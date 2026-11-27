"""Unit and integration tests for deterministic ODCS v3.2.0 export and Parquet Arrow schema parity guards."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import io
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import local_publication
from spicy_regs import odcs_contracts as odcs
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication, r2
from tests.generation_fakes import Store

pytestmark = pytest.mark.usefixtures("ample_disk_space")


def _optional_odcs_json_schema() -> Path | None:
    """Resolve optional ODCS v3.2.0 JSON Schema via env var or relative sibling checkout (no hardcoded user paths)."""
    if env_path := os.environ.get("ODCS_JSON_SCHEMA_PATH"):
        candidate = Path(env_path)
        return candidate if candidate.is_file() else None
    relative_sibling = (
        odcs.REPO_ROOT.parent.parent / "data-design-kit" / "vendor" / "odcs" / "3.2.0" / "odcs-json-schema.json"
    )
    return relative_sibling if relative_sibling.is_file() else None


def _sample_value(dtype: pa.DataType, idx: int = 1):
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return f"val-{idx}"
    if pa.types.is_boolean(dtype):
        return False
    if pa.types.is_integer(dtype):
        return idx
    if pa.types.is_floating(dtype):
        return float(idx)
    if pa.types.is_decimal(dtype):
        return Decimal("0")
    if pa.types.is_date(dtype):
        return date(2025, 1, 1)
    if pa.types.is_timestamp(dtype):
        return datetime(2025, 1, 1, 0, 0, tzinfo=timezone.utc) if dtype.tz else datetime(2025, 1, 1, 0, 0)
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return []
    if pa.types.is_struct(dtype):
        return {child.name: _sample_value(child.type, idx) for child in dtype}
    return None


def _table_for_schema(schema: pa.Schema, count: int = 1) -> pa.Table:
    rows = [{field.name: _sample_value(field.type, i) for field in schema} for i in range(1, count + 1)]
    return pa.Table.from_pylist(rows, schema=schema)


def _prop_by_name(contract: dict, table_name: str, col_name: str) -> dict:
    table = next(s for s in contract["schema"] if s["name"] == table_name)
    return next(p for p in table["properties"] if p["name"] == col_name)


def _custom_map(node: dict, vendor: str) -> dict[str, object]:
    return {cp["property"]: cp["value"] for cp in node.get("customProperties", ()) if cp.get("vendor") == vendor}


def _parquet_bytes(schema: pa.Schema) -> bytes:
    buf = io.BytesIO()
    pq.write_table(_table_for_schema(schema, count=1), buf)
    return buf.getvalue()


def _build_local_gen(
    tmp_path: Path,
    name: str,
    family: str,
    tables: dict[str, pa.Table],
    *,
    partitioned: dict[str, list[str]] | None = None,
) -> tuple[Path, Path]:
    src = tmp_path / f"{name}-src"
    src.mkdir(parents=True, exist_ok=True)
    files: list[Path] = []
    expected_keys: list[str] = []
    for tname, tbl in tables.items():
        if partitioned and f"{tname}.parquet" in partitioned:
            part_dir = src / tname / "year=2025"
            part_dir.mkdir(parents=True, exist_ok=True)
            pq.write_table(tbl, part_dir / "part-000000.parquet")
            files.append(src / tname)
        else:
            p = src / f"{tname}.parquet"
            pq.write_table(tbl, p)
            files.append(p)
        expected_keys.append(f"{tname}.parquet")
    ev = CaptureEvidence(tmp_path / f"{name}-ev", family)
    ev.retain_bytes(f"raw-{name}".encode(), stage="fixture-original", raw_source=True)
    gen_dir = tmp_path / f"{name}-gen"
    build_generation(
        gen_dir,
        family=family,
        files=files,
        expected_keys=expected_keys,
        partitioned=partitioned,
        inputs=ev.inputs(),
    )
    ev.finish()
    return gen_dir, ev.artifact_dir


# ---------------------------------------------------------------------------
# 1. Deterministic ODCS v3.2.0 Export, Schema Validation & CLI
# ---------------------------------------------------------------------------


def test_odcs_export_is_deterministic_and_byte_reproducible(tmp_path: Path):
    out_a = tmp_path / "run-a"
    out_b = tmp_path / "run-b"

    manifest_a = odcs.export_odcs_contracts(out_a, scope="pilot")
    manifest_b = odcs.export_odcs_contracts(out_b, scope="pilot")
    assert manifest_a == manifest_b

    files_a = sorted(p.relative_to(out_a).as_posix() for p in out_a.rglob("*.json"))
    files_b = sorted(p.relative_to(out_b).as_posix() for p in out_b.rglob("*.json"))
    assert files_a == files_b
    for rel in files_a:
        assert (out_a / rel).read_bytes() == (out_b / rel).read_bytes()

    assert odcs.check_odcs_contracts(out_a, scope="pilot") == []


def test_committed_pilot_odcs_contracts_are_up_to_date_and_schema_valid():
    json_schema = _optional_odcs_json_schema()
    assert odcs.check_odcs_contracts(odcs.DEFAULT_ODCS_DIR, scope="pilot", json_schema_path=json_schema) == []
    assert odcs.main(["--scope", "pilot", "--out-dir", str(odcs.DEFAULT_ODCS_DIR), "--check"]) == 0

    manifest = json.loads(odcs.DEFAULT_ODCS_MANIFEST_PATH.read_text(encoding="utf-8"))
    assert manifest["odcs_api_version"] == "v3.2.0"
    assert set(manifest["enrolled_families"]) == set(odcs.PILOT_FAMILIES)
    assert sum(c["table_count"] for c in manifest["contracts"].values()) == 40


def test_full_catalog_odcs_export_passes_v320_validation_with_verified_urns_only(tmp_path: Path):
    json_schema = _optional_odcs_json_schema()
    out_dir = tmp_path / "all-odcs"
    manifest = odcs.export_odcs_contracts(out_dir, scope="all", json_schema_path=json_schema)
    assert len(manifest["contracts"]) >= 90
    assert sum(c["table_count"] for c in manifest["contracts"].values()) >= 280
    domains_present = {entry["domain"] for entry in manifest["contracts"].values()}
    assert domains_present == {"spicycongress", "spicygov", "RefSpec", "SpicyEntities"}

    # Verify across ALL 99 exported contracts:
    # 1. Every refspec.urn is in CANONICAL_REFSPEC_URNS (zero fabricated urn:ref:identifier:*, urn:ref:vocabulary:*, urn:rkaf:us:bill:).
    # 2. Every ddk.conceptKind is strictly in {"identifier", "attribute"} (zero "vocabulary").
    # 3. Zero misattributed REF-042 / REF-024 authority citations.
    emitted_urns: set[str] = set()
    emitted_concept_kinds: set[str] = set()
    for entry in manifest["contracts"].values():
        raw_text = (out_dir / entry["path"]).read_text(encoding="utf-8")
        assert "urn:ref:identifier:" not in raw_text
        assert "urn:ref:vocabulary:" not in raw_text
        assert "urn:rkaf:us:bill:" not in raw_text
        assert "REF-042" not in raw_text
        assert "REF-024" not in raw_text
        contract = json.loads(raw_text)
        for table in contract["schema"]:
            for prop in table["properties"]:
                for cp in prop.get("customProperties", ()):
                    if cp.get("vendor") == "refspec" and cp.get("property") == "urn":
                        assert cp["value"] in odcs.CANONICAL_REFSPEC_URNS
                        emitted_urns.add(cp["value"])
                    if cp.get("vendor") == "ddk" and cp.get("property") == "conceptKind":
                        assert cp["value"] in odcs.DDK_CONCEPT_KINDS
                        emitted_concept_kinds.add(cp["value"])

    assert emitted_concept_kinds == {"identifier", "attribute"}
    assert {
        "urn:rkaf:us:cfr:",
        "urn:rkaf:us:eo:",
        "urn:rkaf:us:frdoc:",
        "urn:rkaf:us:pl:",
        "urn:rkaf:us:regsgov:",
        "urn:rkaf:us:rin:",
        "urn:rkaf:us:usc:",
        "urn:ref:regulations-gov-agency:",
        "urn:ref:atlas-resource-scheme:federal-register-api-topics",
        "urn:ref:atlas-resource-scheme:uei-authority",
        "urn:ref:atlas-resource-scheme:nppes-npi-authority",
    } <= emitted_urns

    gaps = manifest["tracked_catalog_gaps"]
    assert "fec_filing_report_measures" in gaps["derived_child_views"]
    assert "etl_receipts" in gaps["shared_processing_tables"]
    assert "court-opinions" in gaps["local_families_without_inline_arrow_schema"]


def test_odcs_check_detects_drift_and_tampering(tmp_path: Path):
    out_dir = tmp_path / "odcs"
    odcs.export_odcs_contracts(out_dir, scope="pilot")
    amendments_path = out_dir / "families" / "amendments.odcs.json"
    amendments_path.write_text(amendments_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    errors = odcs.check_odcs_contracts(out_dir, scope="pilot")
    assert any("amendments.odcs.json" in err and "drifted" in err for err in errors)
    assert odcs.main(["--scope", "pilot", "--out-dir", str(out_dir), "--check"]) == 1


def test_single_family_export_does_not_prune_other_families_and_validates_slug(tmp_path: Path):
    out_dir = tmp_path / "odcs"
    odcs.export_odcs_contracts(out_dir, scope="pilot")
    members_file = out_dir / "families" / "members.odcs.json"
    assert members_file.is_file()

    # Exporting one family must NOT prune existing sibling contract files.
    odcs.export_odcs_contracts(out_dir, families=["amendments"])
    assert (out_dir / "families" / "amendments.odcs.json").is_file()
    assert members_file.is_file()

    # Invalid family slugs are rejected before touching the filesystem.
    for bad_slug in ("../escape", "UPPER", "bad/slash", "-leading"):
        with pytest.raises(odcs.ODCSContractError, match="Invalid family slug"):
            odcs.export_odcs_contracts(out_dir, families=[bad_slug])


# ---------------------------------------------------------------------------
# 2. DDK Domain Tagging, Verified RefSpec URN Mappings & Relationships
# ---------------------------------------------------------------------------


def test_pilot_ddk_domain_tags_and_secondary_domains():
    expected_domains = {
        "amendments": ("spicycongress", ()),
        "bill-family": ("spicycongress", ("RefSpec",)),
        "committee-rosters": ("spicycongress", ("SpicyEntities",)),
        "members": ("spicycongress", ("SpicyEntities",)),
        "nominations": ("spicycongress", ("spicygov",)),
        "roll-call-votes": ("spicycongress", ()),
        "federal-register": ("spicygov", ("RefSpec",)),
        "rulemaking": ("spicygov", ("RefSpec",)),
    }
    for fam, (primary, secondary) in expected_domains.items():
        loaded = odcs.load_enrolled_family_contract(fam)
        assert loaded is not None
        contract, _ = loaded
        assert contract["domain"] == primary
        assert f"ddk-domain:{primary}" in contract["tags"]
        assert "pilot-wave" in contract["tags"]
        for sec in secondary:
            assert f"ddk-secondary-domain:{sec}" in contract["tags"]
        ddk_custom = _custom_map(contract, "ddk")
        assert ddk_custom["targetDomain"] == primary
        assert ddk_custom["secondaryDomains"] == list(secondary)


def test_canonical_refspec_urn_mappings_rin_regex_and_relationships(tmp_path: Path):
    bill_contract, _ = odcs.load_enrolled_family_contract("bill-family")
    fr_contract, _ = odcs.load_enrolled_family_contract("federal-register")
    rm_contract, _ = odcs.load_enrolled_family_contract("rulemaking")
    members_contract, _ = odcs.load_enrolled_family_contract("members")
    votes_contract, _ = odcs.load_enrolled_family_contract("roll-call-votes")
    rosters_contract, _ = odcs.load_enrolled_family_contract("committee-rosters")

    # Columns without a canonical RefSpec URN omit refspec.urn while keeping refspec.facet and ddk.concept.
    bill_id_prop = _prop_by_name(bill_contract, "congress_bills", "bill_id")
    assert "urn" not in _custom_map(bill_id_prop, "refspec")
    assert _custom_map(bill_id_prop, "refspec")["facet"] == "urn:ref:facet:legal-location"
    assert _custom_map(bill_id_prop, "ddk") == {
        "concept": "urn:ddk:legislative:concept:bill-id",
        "conceptKind": "identifier",
    }

    assert "urn" not in _custom_map(_prop_by_name(members_contract, "members", "bioguide_id"), "refspec")
    assert "urn" not in _custom_map(_prop_by_name(rosters_contract, "committees", "system_code"), "refspec")
    assert "urn" not in _custom_map(_prop_by_name(votes_contract, "roll_call_votes", "vote_id"), "refspec")

    # Public Law retains verified urn:rkaf:us:pl:.
    assert _custom_map(_prop_by_name(bill_contract, "congress_bills", "public_law_number"), "refspec")["urn"] == (
        "urn:rkaf:us:pl:"
    )

    # Documented vocabulary uses valid DDK conceptKind="attribute", omits fabricated URN, keeps taxonomy link.
    action_code_prop = _prop_by_name(bill_contract, "bill_actions", "action_code")
    assert "urn" not in _custom_map(action_code_prop, "refspec")
    assert _custom_map(action_code_prop, "refspec")["facet"] == "urn:ref:facet:code-list-value"
    assert _custom_map(action_code_prop, "ddk")["conceptKind"] == "attribute"
    assert action_code_prop["authoritativeDefinitions"][0]["type"] == "taxonomy"

    # Regulatory / Agency / CFR / RIN / EO / FRDoc / RegsGov verified URNs.
    assert (
        _custom_map(_prop_by_name(fr_contract, "federal_register", "document_number"), "refspec")["urn"]
        == "urn:rkaf:us:frdoc:"
    )
    assert (
        _custom_map(_prop_by_name(fr_contract, "federal_register", "executive_order_number"), "refspec")["urn"]
        == "urn:rkaf:us:eo:"
    )
    assert (
        _custom_map(_prop_by_name(fr_contract, "federal_register", "cfr_references"), "refspec")["urn"]
        == "urn:rkaf:us:cfr:"
    )
    assert (
        _custom_map(_prop_by_name(fr_contract, "federal_register", "regulation_id_numbers"), "refspec")["urn"]
        == "urn:rkaf:us:rin:"
    )
    assert (
        _custom_map(_prop_by_name(fr_contract, "federal_register", "topics"), "refspec")["urn"]
        == "urn:ref:atlas-resource-scheme:federal-register-api-topics"
    )
    assert _custom_map(_prop_by_name(fr_contract, "federal_register", "topics"), "ddk")["conceptKind"] == "attribute"
    # urn:ref:federal-register-agency: MUST NOT be emitted on nested agencies struct arrays or agency_slugs.
    assert "urn" not in _custom_map(_prop_by_name(fr_contract, "federal_register", "agencies"), "refspec")
    assert "urn" not in _custom_map(_prop_by_name(fr_contract, "federal_register", "agency_slugs"), "refspec")

    # Canonical RIN regex from identifiers.py: 4 digits + hyphen + 2 uppercase letters + 2 digits.
    rin_prop = _prop_by_name(fr_contract, "federal_register", "rin")
    rin_pattern = rin_prop["logicalTypeOptions"]["pattern"]
    assert rin_pattern == "^[0-9]{4}-[A-Z]{2}[0-9]{2}$"
    assert re.fullmatch(rin_pattern, "2060-AV12")
    assert not re.fullmatch(rin_pattern, "2060-1234")
    assert not re.fullmatch(rin_pattern, "2060-AAAA")

    assert (
        _custom_map(_prop_by_name(rm_contract, "regulatory_agenda_items", "agenda_item_id"), "refspec")["urn"]
        == "urn:rkaf:us:rin:"
    )
    assert (
        _custom_map(_prop_by_name(rm_contract, "rule_targets", "docket_id"), "refspec")["urn"] == "urn:rkaf:us:regsgov:"
    )
    assert (
        _custom_map(_prop_by_name(rm_contract, "agency_lifecycle_stats", "agency_code"), "refspec")["urn"]
        == "urn:ref:regulations-gov-agency:"
    )

    # Executive orders family carries urn:rkaf:us:eo: and urn:rkaf:us:frdoc:.
    eo_dir = tmp_path / "eo-odcs"
    odcs.export_odcs_contracts(eo_dir, families=["executive-orders"])
    eo_contract, _ = odcs.load_enrolled_family_contract("executive-orders", contracts_dir=eo_dir)
    assert (
        _custom_map(_prop_by_name(eo_contract, "executive_order_observations", "executive_order_number"), "refspec")[
            "urn"
        ]
        == "urn:rkaf:us:eo:"
    )
    assert (
        _custom_map(_prop_by_name(eo_contract, "executive_order_observations", "document_number"), "refspec")["urn"]
        == "urn:rkaf:us:frdoc:"
    )

    # Relationships from table_joins.json (both single-column and composite foreign keys).
    bill_actions = next(s for s in bill_contract["schema"] if s["name"] == "bill_actions")
    assert any(
        rel["from"] == "bill_actions.bill_id" and rel["to"] == "congress_bills.bill_id"
        for rel in bill_actions["relationships"]
    )
    bill_comm_act = next(s for s in bill_contract["schema"] if s["name"] == "bill_committee_activities")
    assert any(
        rel["from"] == ["bill_committee_activities.bill_id", "bill_committee_activities.system_code"]
        and rel["to"] == ["bill_committees.bill_id", "bill_committees.system_code"]
        for rel in bill_comm_act["relationships"]
    )


# ---------------------------------------------------------------------------
# 3. Arrow Type Losslessness, Nullability, Parquet Round-Trip & Audit
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "lossy_type",
    [
        pa.large_list(pa.string()),
        pa.date64(),
        pa.timestamp("ms", tz="UTC"),
        pa.timestamp("ns"),
        pa.time32("s"),
        pa.time64("ns"),
        pa.string_view(),
    ],
)
def test_arrow_type_to_odcs_property_refuses_lossy_arrow_types(lossy_type: pa.DataType):
    with pytest.raises(odcs.ODCSContractError, match="cannot round-trip losslessly"):
        odcs.arrow_type_to_odcs_property(lossy_type, name="col")


def test_odcs_property_to_pyarrow_wraps_invalid_decimal_precision():
    with pytest.raises(odcs.ODCSContractError, match="Invalid Arrow type specification"):
        odcs.odcs_property_to_pyarrow({"name": "bad_dec", "logicalType": "number", "physicalType": "decimal256(99,2)"})


def test_parquet_schema_diff_check_nullability_enforces_nullable_flag():
    expected = pa.schema([pa.field("id", pa.string(), nullable=False), pa.field("val", pa.int64(), nullable=True)])
    actual = pa.schema([pa.field("id", pa.string(), nullable=True), pa.field("val", pa.int64(), nullable=True)])

    assert odcs.parquet_schema_diff(expected, actual, check_nullability=False) == []
    diffs = odcs.parquet_schema_diff(expected, actual, check_nullability=True)
    assert diffs == ["nullability mismatch on ['id (expected nullable=False, got True)']"]


def test_all_40_pilot_tables_round_trip_to_parquet_with_zero_schema_drift(tmp_path: Path):
    pq_root = tmp_path / "pilot-parquets"
    pq_root.mkdir()
    checked = 0

    for fam in odcs.PILOT_FAMILIES:
        contract, _ = odcs.load_enrolled_family_contract(fam)
        declared_odcs_objs = {s["name"]: s for s in contract["schema"]}
        declared_pa = odcs.contract_declared_schemas(contract)
        candidates: dict[str, Path] = {}
        for tname, schema in declared_pa.items():
            path = pq_root / fam / f"{tname}.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(_table_for_schema(schema, count=1), path)
            candidates[tname] = path
            checked += 1

        summary = odcs.verify_parquet_schema_parity(declared_odcs_objs, candidates)
        assert set(summary) == set(declared_pa)
        for tname, info in summary.items():
            assert info["files_checked"] == 1
            assert info["columns"] == list(declared_pa[tname].names)

    assert checked == 40
    audit = odcs.audit_parquet_parity(scope="pilot", parquet_dir=pq_root)
    assert audit["verified_table_count"] == 40
    assert audit["drift_gap_count"] == 0
    assert audit["missing_parquet_count"] == 0
    assert odcs.main(["--scope", "pilot", "--verify-parquet-dir", str(pq_root)]) == 0


def test_build_table_schema_object_rejects_drifted_parquet_footer():
    tm, _ = odcs.load_table_metadata()
    amendments_meta = tm["amendments"]
    valid_schema = odcs._metadata_arrow_schema(amendments_meta)
    obj = odcs.build_table_schema_object("amendments", amendments_meta, verified_parquet_schema=valid_schema)
    assert _custom_map(obj, "spicy-regs")["parquetFooterVerified"] is True

    drifted_schema = valid_schema.append(pa.field("unexpected_col", pa.string()))
    with pytest.raises(odcs.ODCSContractError, match="Parquet footer disagrees.*unexpected_col"):
        odcs.build_table_schema_object("amendments", amendments_meta, verified_parquet_schema=drifted_schema)


# ---------------------------------------------------------------------------
# 4. Security & Fail-Closed Manifest Loading (Traversal, Symlinks, SHA-256, JSON)
# ---------------------------------------------------------------------------


def test_load_enrolled_family_contract_blocks_traversal_symlinks_missing_sha_wrong_family_and_corrupt_json(
    tmp_path: Path,
):
    base = tmp_path / "odcs"
    odcs.export_odcs_contracts(base, families=["irs-form990"])
    manifest_path = base / "manifest.json"
    orig_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    # 1. Path traversal in entry["path"]
    outside = tmp_path / "outside.json"
    outside.write_text((base / "families" / "irs-form990.odcs.json").read_text(encoding="utf-8"), encoding="utf-8")
    trav_manifest = json.loads(json.dumps(orig_manifest))
    trav_manifest["contracts"]["irs-form990"]["path"] = "../outside.json"
    manifest_path.write_text(json.dumps(trav_manifest), encoding="utf-8")
    with pytest.raises(odcs.ODCSContractError, match="escapes containment|unsafe segments"):
        odcs.load_enrolled_family_contract("irs-form990", contracts_dir=base)

    # 2. Missing / null / malformed sha256 fails closed
    for bad_sha in (None, "", "not-a-hex-digest", "A" * 64):
        bad_manifest = json.loads(json.dumps(orig_manifest))
        if bad_sha is None:
            bad_manifest["contracts"]["irs-form990"].pop("sha256", None)
        else:
            bad_manifest["contracts"]["irs-form990"]["sha256"] = bad_sha
        manifest_path.write_text(json.dumps(bad_manifest), encoding="utf-8")
        with pytest.raises(odcs.ODCSContractError, match="requires a valid 64-hex sha256 digest"):
            odcs.load_enrolled_family_contract("irs-form990", contracts_dir=base)

    # 3. Mismatched entry["family"] fails closed
    wrong_entry = json.loads(json.dumps(orig_manifest))
    wrong_entry["contracts"]["irs-form990"]["family"] = "fara"
    manifest_path.write_text(json.dumps(wrong_entry), encoding="utf-8")
    with pytest.raises(odcs.ODCSContractError, match="declares mismatched family 'fara'"):
        odcs.load_enrolled_family_contract("irs-form990", contracts_dir=base)

    # 4. Contract declaring mismatched spicy-regs.family fails closed even when sha256 matches
    amdt_bytes = (odcs.DEFAULT_ODCS_DIR / "families" / "amendments.odcs.json").read_bytes()
    (base / "families" / "irs-form990.odcs.json").write_bytes(amdt_bytes)
    mismatch_manifest = json.loads(json.dumps(orig_manifest))
    mismatch_manifest["contracts"]["irs-form990"]["sha256"] = odcs._sha256_bytes(amdt_bytes)
    manifest_path.write_text(json.dumps(mismatch_manifest), encoding="utf-8")
    with pytest.raises(odcs.ODCSContractError, match="declares family 'amendments', not the expected 'irs-form990'"):
        odcs.load_enrolled_family_contract("irs-form990", contracts_dir=base)

    # 5. Corrupt manifest JSON raises ODCSContractError (never raw JSONDecodeError)
    manifest_path.write_text("{broken json", encoding="utf-8")
    with pytest.raises(odcs.ODCSContractError, match="Failed to read ODCS manifest"):
        odcs.load_enrolled_family_contract("irs-form990", contracts_dir=base)

    # 6. Symlinked contract file and symlinked contracts_dir refused
    clean_dir = tmp_path / "clean-odcs"
    odcs.export_odcs_contracts(clean_dir, families=["irs-form990"])
    target_contract = clean_dir / "families" / "irs-form990.odcs.json"
    real_copy = tmp_path / "real-irs.odcs.json"
    real_copy.write_bytes(target_contract.read_bytes())
    target_contract.unlink()
    target_contract.symlink_to(real_copy)
    with pytest.raises(odcs.ODCSContractError, match="symlink"):
        odcs.load_enrolled_family_contract("irs-form990", contracts_dir=clean_dir)

    symlink_dir = tmp_path / "symlink-odcs"
    symlink_dir.symlink_to(clean_dir)
    with pytest.raises(odcs.ODCSContractError, match="must not be a symlink"):
        odcs.load_enrolled_family_contract("irs-form990", contracts_dir=symlink_dir)


# ---------------------------------------------------------------------------
# 5. Remote HTTP / --verify-r2 / --verify-parquet-dir Fail-Closed Guards
# ---------------------------------------------------------------------------


def test_http_range_file_enforces_206_content_length_and_seek_bounds():
    payload = b"PAR1" + b"x" * 60 + b"PAR1"

    def make_client(handler):
        return httpx.Client(transport=httpx.MockTransport(handler))

    # 1. Server ignoring Range and returning 200 OK must fail closed.
    def handler_200(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(200, headers={"Content-Length": str(len(payload))})
        return httpx.Response(200, content=payload)

    with make_client(handler_200) as client:
        rf = odcs._HTTPRangeFile(client, "https://example.test/a.parquet")
        with pytest.raises(odcs.ODCSContractError, match="Expected HTTP 206 Partial Content.*got 200"):
            rf.read(4)

    # 2. Missing Content-Length on HEAD fails closed.
    def handler_no_len(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, headers={})

    with make_client(handler_no_len) as client:
        with pytest.raises(odcs.ODCSContractError, match="Missing Content-Length"):
            odcs._HTTPRangeFile(client, "https://example.test/a.parquet")

    # 3. Negative seek and invalid whence raise ValueError.
    def handler_206(request: httpx.Request) -> httpx.Response:
        if request.method == "HEAD":
            return httpx.Response(200, headers={"Content-Length": str(len(payload))})
        rng = request.headers["Range"].removeprefix("bytes=")
        start, end = (int(x) for x in rng.split("-"))
        return httpx.Response(206, content=payload[start : end + 1])

    with make_client(handler_206) as client:
        rf = odcs._HTTPRangeFile(client, "https://example.test/a.parquet")
        with pytest.raises(ValueError, match="Negative seek position"):
            rf.seek(-1, io.SEEK_SET)
        with pytest.raises(ValueError, match="Invalid whence"):
            rf.seek(0, 99)
        assert rf.read(4) == b"PAR1"


def test_audit_parquet_parity_r2_fails_closed_on_403_500_and_partial_shard_failures(tmp_path: Path):
    amdt_contract, _ = odcs.load_enrolled_family_contract("amendments")
    amdt_schema = odcs.contract_declared_schemas(amdt_contract)["amendments"]
    valid_pq = _parquet_bytes(amdt_schema)

    # 1. HTTP 403 on publication.v2.json raises HTTPStatusError (never swallowed).
    with httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(403, request=req))) as client:
        with pytest.raises(httpx.HTTPStatusError):
            odcs.audit_parquet_parity(families=["amendments"], r2_base_url="https://r2.test", http_client=client)

    # 2. HTTP 500 on rulemaking latest.json fails closed (never swallowed).
    def handler_rm_500(req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("/publication.v2.json"):
            return httpx.Response(200, json={"format": "spicy-regs-publication", "version": 2, "families": {}})
        return httpx.Response(500, request=req)

    with httpx.Client(transport=httpx.MockTransport(handler_rm_500)) as client:
        with pytest.raises(httpx.HTTPStatusError):
            odcs.audit_parquet_parity(families=["rulemaking"], r2_base_url="https://r2.test", http_client=client)

    # 3. Partial-shard failure: shard 0 succeeds (206), shard 1 fails (500).
    # Table MUST appear in drift_gaps, MUST NOT appear in verified_tables or missing_parquet_tables!
    def handler_partial_shard(req: httpx.Request) -> httpx.Response:
        if req.url.path.endswith("/publication.v2.json"):
            return httpx.Response(
                200,
                json={
                    "format": "spicy-regs-publication",
                    "version": 2,
                    "families": {
                        "amendments": {
                            "prefix": "generations/amendments/" + "a" * 64,
                            "tables": {
                                "amendments.parquet": {
                                    "members": [
                                        {"key": "amendments/part-000000.parquet"},
                                        {"key": "amendments/part-000001.parquet"},
                                    ]
                                }
                            },
                        }
                    },
                },
            )
        if req.url.path.endswith("/materialized/rulemaking/latest.json"):
            return httpx.Response(404, request=req)
        if req.url.path.endswith("part-000000.parquet"):
            if req.method == "HEAD":
                return httpx.Response(200, headers={"Content-Length": str(len(valid_pq))})
            rng = req.headers["Range"].removeprefix("bytes=")
            start, end = (int(x) for x in rng.split("-"))
            return httpx.Response(206, content=valid_pq[start : end + 1])
        return httpx.Response(500, request=req)

    with httpx.Client(transport=httpx.MockTransport(handler_partial_shard)) as client:
        report = odcs.audit_parquet_parity(families=["amendments"], r2_base_url="https://r2.test", http_client=client)
    assert report["verified_table_count"] == 0
    assert report["drift_gap_count"] == 1
    assert report["missing_parquet_count"] == 0
    assert report["drift_gaps"][0]["table"] == "amendments"
    assert "part-000001.parquet" in report["drift_gaps"][0]["member"]

    # 4. CLI returns non-zero (1) on nonexistent --verify-parquet-dir and on empty dir (0 verified tables).
    assert odcs.main(["--scope", "pilot", "--verify-parquet-dir", str(tmp_path / "does-not-exist")]) == 1
    empty_dir = tmp_path / "empty-parquets"
    empty_dir.mkdir()
    assert odcs.main(["--scope", "pilot", "--verify-parquet-dir", str(empty_dir)]) == 1


# ---------------------------------------------------------------------------
# 6. Publish-Time & Dry-Run Fail-Closed Guards in local_publication.py
# ---------------------------------------------------------------------------


def test_local_publication_odcs_guard_passes_and_refuses_all_drift_modes_on_dry_run(tmp_path: Path):
    """Fail closed on column rename, type widening, missing col, extra col, and order drift before any upload."""
    contracts_dir = tmp_path / "odcs-enrolled"
    odcs.export_odcs_contracts(contracts_dir, families=["irs-form990"])
    contract_path = contracts_dir / "families" / "irs-form990.odcs.json"

    declared = local_publication._family_schemas("irs-form990")
    assert declared is not None
    filings_schema = declared["irs990_filings"]

    live = tmp_path / "publication.v2.json"
    live.write_text(json.dumps(publication.empty_index()), encoding="utf-8")

    # 1. Matching candidate passes dry_run with --odcs-contracts-dir and records odcs_schema_parity in receipt.
    ok_gen, ok_ev = _build_local_gen(
        tmp_path, "ok", "irs-form990", {"irs990_filings": _table_for_schema(filings_schema)}
    )
    out_ok = tmp_path / "ok-receipt"
    local_publication.main(
        [
            "--generation",
            str(ok_gen),
            "--family",
            "irs-form990",
            "--evidence",
            str(ok_ev),
            "--output",
            str(out_ok),
            "--dry-run",
            "--scratch-raw",
            "--live-index",
            str(live),
            "--odcs-contracts-dir",
            str(contracts_dir),
            "--require-odcs-contract",
        ]
    )
    receipt = json.loads((out_ok / "dry-run.json").read_text(encoding="utf-8"))
    assert receipt["status"] == "dry_run_accepted"
    assert receipt["odcs_schema_parity"]["contract_id"] == "urn:spicy-regs:odcs:family:irs-form990"
    assert receipt["odcs_schema_parity"]["odcs_api_version"] == "v3.2.0"
    assert receipt["odcs_schema_parity"]["domain"] == "spicygov"
    assert receipt["odcs_schema_parity"]["tables"]["irs990_filings"]["files_checked"] == 1

    # 2. court-opinions (NO inline _family_schemas) is strictly gated by enrolled ODCS contract.
    court_contracts_dir = tmp_path / "odcs-court"
    odcs.export_odcs_contracts(court_contracts_dir, families=["court-opinions"])
    court_contract, _ = odcs.load_enrolled_family_contract("court-opinions", contracts_dir=court_contracts_dir)
    court_schema = odcs.contract_declared_schemas(court_contract)["court_opinions"]
    assert local_publication._family_schemas("court-opinions") is None

    court_ok_gen, court_ok_ev = _build_local_gen(
        tmp_path, "court-ok", "court-opinions", {"court_opinions": _table_for_schema(court_schema)}
    )
    court_out = tmp_path / "court-ok-receipt"
    local_publication.main(
        [
            "--generation",
            str(court_ok_gen),
            "--family",
            "court-opinions",
            "--evidence",
            str(court_ok_ev),
            "--output",
            str(court_out),
            "--dry-run",
            "--scratch-raw",
            "--live-index",
            str(live),
            "--odcs-contracts-dir",
            str(court_contracts_dir),
            "--require-odcs-contract",
        ]
    )
    court_receipt = json.loads((court_out / "dry-run.json").read_text(encoding="utf-8"))
    assert court_receipt["status"] == "dry_run_accepted"
    assert court_receipt["odcs_schema_parity"]["contract_id"] == "urn:spicy-regs:odcs:family:court-opinions"

    renamed_fields = [
        pa.field("opinion_id_renamed", f.type, nullable=f.nullable) if f.name == "opinion_id" else f
        for f in court_schema
    ]
    widened_fields = [
        pa.field(f.name, pa.int64(), nullable=f.nullable) if f.name == "cluster_id" else f for f in court_schema
    ]
    missing_fields = [f for f in court_schema if f.name != "opinion_type"]
    extra_fields = [*list(court_schema), pa.field("unplanned_drift_col", pa.string())]
    reordered_fields = [court_schema.field(1), court_schema.field(0), *list(court_schema)[2:]]

    drift_cases = [
        (
            "rename",
            pa.schema(renamed_fields),
            r"missing columns \['opinion_id'\].*unexpected columns \['opinion_id_renamed'\]",
        ),
        ("type-widening", pa.schema(widened_fields), r"type mismatch.*cluster_id.*expected string, got int64"),
        ("missing-col", pa.schema(missing_fields), r"missing columns \['opinion_type'\]"),
        ("extra-col", pa.schema(extra_fields), r"unexpected columns \['unplanned_drift_col'\]"),
        ("reordered", pa.schema(reordered_fields), r"column order differs"),
    ]
    for label, bad_schema, pattern in drift_cases:
        bad_gen, bad_ev = _build_local_gen(
            tmp_path, f"bad-{label}", "court-opinions", {"court_opinions": _table_for_schema(bad_schema)}
        )
        bad_out = tmp_path / f"bad-{label}-receipt"
        with pytest.raises(publication.PublicationError, match=pattern):
            local_publication.main(
                [
                    "--generation",
                    str(bad_gen),
                    "--family",
                    "court-opinions",
                    "--evidence",
                    str(bad_ev),
                    "--output",
                    str(bad_out),
                    "--dry-run",
                    "--scratch-raw",
                    "--live-index",
                    str(live),
                    "--odcs-contracts-dir",
                    str(court_contracts_dir),
                ]
            )
        assert not bad_out.exists()

    # Numeric type widening (decimal128(38, 0) -> double) refused.
    widen_num_fields = [
        pa.field(f.name, pa.float64(), nullable=f.nullable) if f.name == "revenue_usd" else f for f in filings_schema
    ]
    bad_num_gen, bad_num_ev = _build_local_gen(
        tmp_path, "bad-num", "irs-form990", {"irs990_filings": _table_for_schema(pa.schema(widen_num_fields))}
    )
    with pytest.raises(publication.PublicationError, match=r"revenue_usd.*expected decimal128\(38, 0\), got double"):
        local_publication.main(
            [
                "--generation",
                str(bad_num_gen),
                "--family",
                "irs-form990",
                "--evidence",
                str(bad_num_ev),
                "--output",
                str(tmp_path / "bad-num-receipt"),
                "--dry-run",
                "--scratch-raw",
                "--live-index",
                str(live),
                "--odcs-contract",
                str(contract_path),
            ]
        )


def test_local_publication_refuses_explicit_missing_dir_symlinks_corrupt_parquet_and_tampering(tmp_path: Path):
    contracts_dir = tmp_path / "odcs-enrolled"
    odcs.export_odcs_contracts(contracts_dir, families=["irs-form990"])
    declared = local_publication._family_schemas("irs-form990")
    assert declared is not None
    ok_gen, ok_ev = _build_local_gen(
        tmp_path, "ok", "irs-form990", {"irs990_filings": _table_for_schema(declared["irs990_filings"])}
    )
    live = tmp_path / "live.json"
    live.write_text(json.dumps(publication.empty_index()), encoding="utf-8")

    # 1. Explicit nonexistent --odcs-contracts-dir fails closed even without --require-odcs-contract.
    with pytest.raises(publication.PublicationError, match="ODCS contracts directory does not exist"):
        local_publication.main(
            [
                "--generation",
                str(ok_gen),
                "--family",
                "irs-form990",
                "--evidence",
                str(ok_ev),
                "--output",
                str(tmp_path / "missing-dir"),
                "--dry-run",
                "--scratch-raw",
                "--live-index",
                str(live),
                "--odcs-contracts-dir",
                str(tmp_path / "no-such-dir"),
            ]
        )

    # 2. Explicit --odcs-contracts-dir without manifest.json fails closed even without --require-odcs-contract.
    empty_dir = tmp_path / "empty-odcs-dir"
    empty_dir.mkdir()
    with pytest.raises(publication.PublicationError, match="missing manifest.json"):
        local_publication.main(
            [
                "--generation",
                str(ok_gen),
                "--family",
                "irs-form990",
                "--evidence",
                str(ok_ev),
                "--output",
                str(tmp_path / "empty-dir"),
                "--dry-run",
                "--scratch-raw",
                "--live-index",
                str(live),
                "--odcs-contracts-dir",
                str(empty_dir),
            ]
        )

    # 3. Symlinked explicit --odcs-contract refused.
    symlink_contract = tmp_path / "link.odcs.json"
    symlink_contract.symlink_to(contracts_dir / "families" / "irs-form990.odcs.json")
    with pytest.raises(publication.PublicationError, match="must not be a symlink"):
        local_publication.main(
            [
                "--generation",
                str(ok_gen),
                "--family",
                "irs-form990",
                "--evidence",
                str(ok_ev),
                "--output",
                str(tmp_path / "symlink-contract"),
                "--dry-run",
                "--scratch-raw",
                "--live-index",
                str(live),
                "--odcs-contract",
                str(symlink_contract),
            ]
        )

    # 4. Wrong family's explicit ODCS contract refused.
    pilot_amendments = odcs.DEFAULT_ODCS_DIR / "families" / "amendments.odcs.json"
    with pytest.raises(
        publication.PublicationError, match="declares family 'amendments', not the expected 'irs-form990'"
    ):
        local_publication.main(
            [
                "--generation",
                str(ok_gen),
                "--family",
                "irs-form990",
                "--evidence",
                str(ok_ev),
                "--output",
                str(tmp_path / "wrong-fam"),
                "--dry-run",
                "--scratch-raw",
                "--live-index",
                str(live),
                "--odcs-contract",
                str(pilot_amendments),
            ]
        )

    # 5. --require-odcs-contract refuses an unenrolled family.
    with pytest.raises(publication.PublicationError, match="requires an enrolled ODCS v3.2.0 DataContract"):
        local_publication.main(
            [
                "--generation",
                str(ok_gen),
                "--family",
                "irs-form990",
                "--evidence",
                str(ok_ev),
                "--output",
                str(tmp_path / "unenrolled"),
                "--dry-run",
                "--scratch-raw",
                "--live-index",
                str(live),
                "--odcs-contracts-dir",
                str(odcs.DEFAULT_ODCS_DIR),
                "--require-odcs-contract",
            ]
        )

    # 6. Tampered ODCS contract bytes fail manifest SHA-256 verification.
    contract_path = contracts_dir / "families" / "irs-form990.odcs.json"
    contract_path.write_text(contract_path.read_text(encoding="utf-8") + " ", encoding="utf-8")
    with pytest.raises(publication.PublicationError, match="failed SHA-256 check"):
        local_publication.main(
            [
                "--generation",
                str(ok_gen),
                "--family",
                "irs-form990",
                "--evidence",
                str(ok_ev),
                "--output",
                str(tmp_path / "tampered"),
                "--dry-run",
                "--scratch-raw",
                "--live-index",
                str(live),
                "--odcs-contracts-dir",
                str(contracts_dir),
            ]
        )

    # 7. Corrupt 0-byte Parquet file on inline-schema family raises PublicationError (not raw ArrowInvalid).
    corrupt_dir = tmp_path / "corrupt-gen"
    corrupt_dir.mkdir()
    (corrupt_dir / "irs990_filings.parquet").write_bytes(b"")
    corrupt_root = {"spec": {"family": "irs-form990", "tables": {"irs990_filings.parquet": {"rows": 0}}}}
    with pytest.raises(publication.PublicationError, match="could not read Parquet schema.*irs990_filings"):
        local_publication._verify_family_schemas(corrupt_dir, "irs-form990", corrupt_root)


def test_local_publication_publish_path_refuses_odcs_drift_before_uploading_or_advancing_pointers(
    tmp_path: Path, monkeypatch
):
    """publish() itself fails closed on ODCS drift BEFORE touching credentials, R2, or pointers."""
    court_contracts_dir = tmp_path / "odcs-court"
    odcs.export_odcs_contracts(court_contracts_dir, families=["court-opinions"])
    court_contract, _ = odcs.load_enrolled_family_contract("court-opinions", contracts_dir=court_contracts_dir)
    court_schema = odcs.contract_declared_schemas(court_contract)["court_opinions"]
    drifted_schema = pa.schema([f for f in court_schema if f.name != "opinion_type"])

    bad_gen, bad_ev = _build_local_gen(
        tmp_path, "court-drift", "court-opinions", {"court_opinions": _table_for_schema(drifted_schema)}
    )
    root = json.loads((bad_gen / "artifact.json").read_bytes())

    store = Store()
    endpoint = SimpleNamespace(endpoint_url=r2.VERIFIED_ENDPOINT)
    monkeypatch.setattr(store, "meta", endpoint, raising=False)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr("dotenv.load_dotenv", lambda path: None)
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fixture")

    args = SimpleNamespace(
        generation=bad_gen,
        family="court-opinions",
        evidence=[bad_ev],
        added_table=[],
        expect_prior=None,
        output=tmp_path / "pub-refused",
        env_file=tmp_path / ".env",
        odcs_contract=None,
        odcs_contracts_dir=court_contracts_dir,
        require_odcs_contract=True,
    )
    with pytest.raises(
        publication.PublicationError, match=r"ODCS schema parity check failed.*missing columns \['opinion_type'\]"
    ):
        local_publication.publish(args, root)
    assert store.writes == []
    assert not (tmp_path / "pub-refused").exists()


def test_verify_family_schemas_enforces_committed_pilot_contracts_directly(tmp_path: Path):
    """_verify_family_schemas enforces committed pilot ODCS contracts for hosted pilot families too."""
    fr_contract, _ = odcs.load_enrolled_family_contract("federal-register")
    fr_schema = odcs.contract_declared_schemas(fr_contract)["federal_register"]
    fr_ok_gen, _ = _build_local_gen(
        tmp_path, "fr-ok", "federal-register", {"federal_register": _table_for_schema(fr_schema)}
    )
    fr_ok_root = json.loads((fr_ok_gen / "artifact.json").read_bytes())
    fr_summary = local_publication._verify_family_schemas(fr_ok_gen, "federal-register", fr_ok_root)
    assert fr_summary is not None
    assert fr_summary["contract_id"] == "urn:spicy-regs:odcs:family:federal-register"
    assert fr_summary["domain"] == "spicygov"
    assert fr_summary["tables"]["federal_register"]["files_checked"] == 1

    amdt_contract, _ = odcs.load_enrolled_family_contract("amendments")
    amendments_schema = odcs.contract_declared_schemas(amdt_contract)["amendments"]
    ok_dir = tmp_path / "amdt-ok"
    ok_dir.mkdir()
    pq.write_table(_table_for_schema(amendments_schema), ok_dir / "amendments.parquet")
    ok_root = {"spec": {"family": "amendments", "tables": {"amendments.parquet": {"rows": 1}}}}
    summary = local_publication._verify_family_schemas(ok_dir, "amendments", ok_root)
    assert summary is not None
    assert summary["contract_id"] == "urn:spicy-regs:odcs:family:amendments"
    assert summary["domain"] == "spicycongress"
    assert summary["tables"]["amendments"]["files_checked"] == 1

    widened = pa.schema(
        [pa.field(f.name, pa.int32(), nullable=f.nullable) if f.name == "congress" else f for f in amendments_schema]
    )
    bad_dir = tmp_path / "amdt-bad"
    bad_dir.mkdir()
    pq.write_table(_table_for_schema(widened), bad_dir / "amendments.parquet")
    with pytest.raises(publication.PublicationError, match=r"congress \(expected string, got int32\)"):
        local_publication._verify_family_schemas(bad_dir, "amendments", ok_root)
