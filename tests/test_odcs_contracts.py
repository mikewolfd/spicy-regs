"""Unit and integration tests for deterministic ODCS v3.2.0 export and Parquet Arrow schema parity guards."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
import json
from pathlib import Path
from types import SimpleNamespace

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

_SIBLING_ODCS_JSON_SCHEMA = Path("/Users/mikewolfd/Work/data-design-kit/vendor/odcs/3.2.0/odcs-json-schema.json")


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
    json_schema = _SIBLING_ODCS_JSON_SCHEMA if _SIBLING_ODCS_JSON_SCHEMA.is_file() else None
    assert odcs.check_odcs_contracts(odcs.DEFAULT_ODCS_DIR, scope="pilot", json_schema_path=json_schema) == []
    assert odcs.main(["--scope", "pilot", "--out-dir", str(odcs.DEFAULT_ODCS_DIR), "--check"]) == 0

    manifest = json.loads(odcs.DEFAULT_ODCS_MANIFEST_PATH.read_text(encoding="utf-8"))
    assert manifest["odcs_api_version"] == "v3.2.0"
    assert set(manifest["enrolled_families"]) == set(odcs.PILOT_FAMILIES)
    assert sum(c["table_count"] for c in manifest["contracts"].values()) == 40


def test_full_catalog_odcs_export_passes_v320_validation(tmp_path: Path):
    json_schema = _SIBLING_ODCS_JSON_SCHEMA if _SIBLING_ODCS_JSON_SCHEMA.is_file() else None
    manifest = odcs.export_odcs_contracts(tmp_path / "all-odcs", scope="all", json_schema_path=json_schema)
    assert len(manifest["contracts"]) >= 90
    assert sum(c["table_count"] for c in manifest["contracts"].values()) >= 280
    # All four DDK domains are represented across the full catalog.
    domains_present = {entry["domain"] for entry in manifest["contracts"].values()}
    assert domains_present == {"spicycongress", "spicygov", "RefSpec", "SpicyEntities"}
    # Tracked catalog gaps are explicitly emitted, never silently suppressed.
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


# ---------------------------------------------------------------------------
# 2. DDK Domain Tagging, RefSpec URN Mappings & Foreign-Key Relationships
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


def test_canonical_refspec_urn_mappings_and_relationships_on_pilot_contracts():
    bill_contract, _ = odcs.load_enrolled_family_contract("bill-family")
    fr_contract, _ = odcs.load_enrolled_family_contract("federal-register")
    rm_contract, _ = odcs.load_enrolled_family_contract("rulemaking")
    members_contract, _ = odcs.load_enrolled_family_contract("members")
    votes_contract, _ = odcs.load_enrolled_family_contract("roll-call-votes")
    rosters_contract, _ = odcs.load_enrolled_family_contract("committee-rosters")

    # Legislative RefSpec URNs
    assert (
        _custom_map(_prop_by_name(bill_contract, "congress_bills", "bill_id"), "refspec")["urn"] == "urn:rkaf:us:bill:"
    )
    assert (
        _custom_map(_prop_by_name(bill_contract, "congress_bills", "bill_id"), "ddk")["concept"]
        == "urn:ddk:legislative:concept:bill-id"
    )
    assert (
        _custom_map(_prop_by_name(members_contract, "members", "bioguide_id"), "refspec")["urn"]
        == "urn:ref:identifier:bioguide-id"
    )
    assert (
        _custom_map(_prop_by_name(rosters_contract, "committees", "system_code"), "refspec")["urn"]
        == "urn:ref:identifier:congress-committee-system-code"
    )
    assert (
        _custom_map(_prop_by_name(votes_contract, "roll_call_votes", "vote_id"), "refspec")["urn"]
        == "urn:ref:identifier:congress-roll-call-vote-id"
    )

    # Documented vocabulary URN + authoritativeDefinitions taxonomy link
    action_code_prop = _prop_by_name(bill_contract, "bill_actions", "action_code")
    assert _custom_map(action_code_prop, "refspec")["urn"] == "urn:ref:vocabulary:billstatus-action-code"
    assert action_code_prop.get("authoritativeDefinitions")
    assert action_code_prop["authoritativeDefinitions"][0]["type"] == "taxonomy"

    # Regulatory / Agency / CFR / RIN RefSpec URNs
    assert (
        _custom_map(_prop_by_name(fr_contract, "federal_register", "document_number"), "refspec")["urn"]
        == "urn:ref:identifier:federal-register-document-number"
    )
    assert (
        _custom_map(_prop_by_name(fr_contract, "federal_register", "agencies"), "refspec")["urn"]
        == "urn:ref:federal-register-agency:"
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
        == "urn:ref:vocabulary:federal-register-topics"
    )
    assert (
        _custom_map(_prop_by_name(rm_contract, "regulatory_agenda_items", "agenda_item_id"), "refspec")["urn"]
        == "urn:rkaf:us:rin:"
    )
    assert (
        _custom_map(_prop_by_name(rm_contract, "rule_targets", "docket_id"), "refspec")["urn"]
        == "urn:ref:identifier:regulations-gov-docket-id"
    )
    assert (
        _custom_map(_prop_by_name(rm_contract, "agency_lifecycle_stats", "agency_code"), "refspec")["urn"]
        == "urn:ref:regulations-gov-agency:"
    )

    # Relationships from table_joins.json (both single-column and composite foreign keys)
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
# 3. Positive Pilot Wave Parquet Arrow Schema Parity & Verification
# ---------------------------------------------------------------------------


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

        # Verify directly against ODCS SchemaObject dicts (same input shape as ddk.publication.verify_parquet_schema_parity)
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
# 4. Publish-Time & Dry-Run Fail-Closed Guards in local_publication.py
# ---------------------------------------------------------------------------


def test_local_publication_odcs_guard_passes_and_refuses_all_drift_modes_on_dry_run(tmp_path: Path):
    """Fail closed on column rename, type widening, missing col, extra col, and order drift before any upload."""
    # Export ODCS contract for irs-form990 into a local enrolled contracts directory.
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

    # Helper to test drift on a court-opinions generation (which has NO inline _family_schemas,
    # proving the ODCS guard itself enforces fail-closed Arrow parity independently!).
    court_contracts_dir = tmp_path / "odcs-court"
    odcs.export_odcs_contracts(court_contracts_dir, families=["court-opinions"])
    court_contract, _ = odcs.load_enrolled_family_contract("court-opinions", contracts_dir=court_contracts_dir)
    court_schema = odcs.contract_declared_schemas(court_contract)["court_opinions"]
    assert local_publication._family_schemas("court-opinions") is None

    # Positive court-opinions dry-run governed solely by enrolled ODCS contract.
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

    # Negative drift cases against court-opinions (ODCS guard is the sole schema gate):
    # (a) Column rename
    renamed_fields = [
        pa.field("opinion_id_renamed", f.type, nullable=f.nullable) if f.name == "opinion_id" else f
        for f in court_schema
    ]
    # (b) Type widening / mismatch (string -> int64)
    widened_fields = [
        pa.field(f.name, pa.int64(), nullable=f.nullable) if f.name == "cluster_id" else f for f in court_schema
    ]
    # (c) Missing column
    missing_fields = [f for f in court_schema if f.name != "opinion_type"]
    # (d) Extra unexpected column
    extra_fields = [*list(court_schema), pa.field("unplanned_drift_col", pa.string())]
    # (e) Column order drift
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

    # (f) Type widening on a numeric column via ODCS contract (decimal128(38,0) -> double)
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


def test_local_publication_refuses_tampered_contract_wrong_family_and_unenrolled_when_required(tmp_path: Path):
    contracts_dir = tmp_path / "odcs-enrolled"
    odcs.export_odcs_contracts(contracts_dir, families=["irs-form990"])
    declared = local_publication._family_schemas("irs-form990")
    assert declared is not None
    ok_gen, ok_ev = _build_local_gen(
        tmp_path, "ok", "irs-form990", {"irs990_filings": _table_for_schema(declared["irs990_filings"])}
    )
    live = tmp_path / "live.json"
    live.write_text(json.dumps(publication.empty_index()), encoding="utf-8")

    # 1. Wrong family's explicit ODCS contract refused.
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

    # 2. --require-odcs-contract refuses an unenrolled family.
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

    # 3. Tampered ODCS contract bytes fail manifest SHA-256 verification.
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
    # 1. Receipt-retired pilot family (federal-register) with nested list<struct<...>> columns via build_generation.
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

    # 2. Legislative pilot family (amendments): positive parity + type widening refusal (string -> int32).
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
