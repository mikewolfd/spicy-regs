"""The hand-run publish command: its dry run on a disk store, its refusals and its public readback."""

from datetime import date
from decimal import Decimal
import base64
import hashlib
import json
import os
from pathlib import Path
import shutil
from types import SimpleNamespace

from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactVerificationError
import yaml

from spicy_regs import local_publication as command
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import ARCHIVE_REFERENCE, CaptureEvidence
from spicy_regs.sources import publication, r2
from spicy_regs.sources.disk_store import DiskStore
from tests.generation_fakes import Store

pytestmark = pytest.mark.usefixtures("ample_disk_space")

FAMILY = "irs-form990"


def _dummy_value(dtype: pa.DataType, row_id: str, index: int):
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        return row_id
    if pa.types.is_boolean(dtype):
        return False
    if pa.types.is_integer(dtype):
        return index
    if pa.types.is_floating(dtype):
        return float(index)
    if pa.types.is_decimal(dtype):
        return Decimal("0")
    if pa.types.is_date(dtype):
        return date(2025, 1, 1)
    if pa.types.is_list(dtype):
        return []
    return None


def _table_for_schema(schema: pa.Schema, ids: list[str]) -> pa.Table:
    rows = [
        {field.name: _dummy_value(field.type, item, idx) for field in schema}
        for idx, item in enumerate(ids, start=1)
    ]
    return pa.Table.from_pylist(rows, schema=schema)


def generation(tmp_path, name, tables, *, read=None, family=FAMILY, publication_status="complete-family"):
    """A generation of ``tables`` (table name to its ids) with evidence citing one raw original.

    ``read`` is the published index bytes and prior root bytes a build recorded as what it read.
    """
    declared = command._family_schemas(family) or {}
    files = []
    for table, ids in tables.items():
        path = tmp_path / f"{name}-source" / f"{table}.parquet"
        path.parent.mkdir(exist_ok=True)
        if table in declared:
            arrow_table = _table_for_schema(declared[table], ids)
        else:
            arrow_table = pa.table({"id": ids})
        pq.write_table(arrow_table, path)
        files.append(path)
    evidence = CaptureEvidence(tmp_path / f"{name}-build", family)
    evidence.retain_bytes(f"the original {name} was built from".encode(), stage="fixture-original", raw_source=True)
    directory = tmp_path / name
    index = None
    if read:
        index = publication.parse_index(read[0])
        evidence.inherit_pinned_root(index, read[1])
    artifact = build_generation(directory, family=family, files=files, expected_keys=[path.name for path in files],
                                inputs=evidence.inputs(), read_snapshot=index, publication_status=publication_status)
    evidence.finish()
    return directory, artifact.pin.artifact_digest.removeprefix("sha256:"), evidence.artifact_dir


def run(tmp_path, name, directory, evidence, *extra, family=FAMILY):
    command.main(["--generation", str(directory), "--family", family, "--output", str(tmp_path / name),
                  *(("--evidence", str(evidence)) if evidence else ()), *extra])
    return tmp_path / name


def test_census_acs_cli_registration_keeps_family_and_evidence_guards(tmp_path):
    family = "census-acs"
    directory, digest, evidence = generation(
        tmp_path, "acs", {"census_acs_county_observations": ["001", "003", "005"]}, family=family)
    live = tmp_path / "private-publication.v2.json"
    live.write_text(json.dumps(publication.empty_index()))
    original_index = live.read_bytes()
    dry = ("--dry-run", "--scratch-raw", "--live-index", str(live))

    with pytest.raises(publication.PublicationError, match=f"is family census-acs, not the expected {FAMILY}"):
        run(tmp_path, "accepted-acs", directory, evidence, *dry)
    with pytest.raises(publication.PublicationError, match="published with the source evidence it pins"):
        run(tmp_path, "accepted-acs", directory, None, *dry, family=family)
    assert not (tmp_path / "accepted-acs").exists()

    output = run(tmp_path, "accepted-acs", directory, evidence, *dry, family=family)
    receipt = json.loads((output / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted" and receipt["family"] == family
    assert receipt["artifactDigest"] == "sha256:" + digest
    assert receipt["tables"]["census_acs_county_observations.parquet"]["rows"] == 3
    assert receipt["other_family_changes"] == [] and live.read_bytes() == original_index
    assert not list(tmp_path.glob(".dry-run-store-*"))


def test_usaspending_cli_registration_retains_family_evidence_and_partial_guards(tmp_path):
    family = "usaspending-awards"
    directory, digest, evidence = generation(
        tmp_path, "awards", {"usaspending_transactions": ["a"]}, family=family)
    live = tmp_path / "private-prior.json"
    original_index = json.dumps(publication.empty_index()).encode()
    live.write_bytes(original_index)
    dry = ("--dry-run", "--scratch-raw", "--live-index", str(live))
    with pytest.raises(publication.PublicationError, match=f"not the expected {FAMILY}"):
        run(tmp_path, "wrong-family", directory, evidence, *dry)
    with pytest.raises(publication.PublicationError, match="pass --evidence"):
        run(tmp_path, "missing-evidence", directory, None, *dry, family=family)
    _, _, other_evidence = generation(tmp_path, "other", {"irs990_filings": ["other"]})
    with pytest.raises(publication.PublicationError, match="source evidence is missing or differs"):
        run(tmp_path, "wrong-evidence", directory, other_evidence, *dry, family=family)
    partial, _, partial_evidence = generation(
        tmp_path, "partial-awards", {"usaspending_transactions": ["a"]}, family=family, publication_status="local-partial")
    with pytest.raises(publication.PublicationError, match="local partial candidate cannot be published"):
        run(tmp_path, "partial-refused", partial, partial_evidence, *dry, family=family)
    assert not any((tmp_path / name).exists() for name in (
        "wrong-family", "missing-evidence", "wrong-evidence", "partial-refused"))
    output = run(tmp_path, "registered", directory, evidence, *dry, family=family)
    receipt = json.loads((output / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted" and receipt["family"] == family
    assert receipt["artifactDigest"] == "sha256:" + digest
    assert receipt["other_family_changes"] == [] and live.read_bytes() == original_index
    assert not list(tmp_path.glob(".dry-run-store-*"))


def test_dry_run_runs_the_publication_guards_on_a_disk_store(tmp_path, capsys, monkeypatch):
    first, first_digest, first_evidence = generation(
        tmp_path, "first", {"irs990_filings": ["a", "b"], "irs990_officers": ["p"]})
    store = DiskStore(tmp_path / "live", bucket="live")
    publication.publish_generation(first, client=store, bucket="live", prior_index=publication.empty_index(),
                                   evidence_directories=(first_evidence,))
    live = tmp_path / "publication.v2.json"
    live.write_bytes(store.objects[publication.INDEX_V2_KEY].read_bytes())
    second, second_digest, evidence = generation(
        tmp_path, "second", {"irs990_filings": ["a", "b", "c"], "irs990_officers": ["p"], "irs990_grants": ["x"]})
    dry = ("--dry-run", "--scratch-raw", "--live-index", str(live))
    refused = publication.PublicationError

    with pytest.raises(refused, match=f"is family {FAMILY}, not the expected fara"):
        run(tmp_path, "accepted", second, evidence, *dry, family="fara")
    with pytest.raises(refused, match="published with the source evidence it pins"):
        run(tmp_path, "accepted", second, None, *dry)
    # A rehearsal build must say it is one: without --scratch-raw the dry run holds it to the shared archive.
    with pytest.raises(refused, match="Unset SPICYGOV_RAW_ROOT"):
        run(tmp_path, "accepted", second, evidence, "--dry-run", "--live-index", str(live))
    with pytest.raises(refused, match="Family membership changed; explicit migration is required"):
        run(tmp_path, "accepted", second, evidence, *dry)
    with pytest.raises(refused, match=f"Live {FAMILY} is {first_digest}, not the expected"):
        run(tmp_path, "accepted", second, evidence, *dry, "--added-table", "irs990_grants.parquet",
            "--expect-prior", second_digest)
    with monkeypatch.context() as loosened:
        loosened.setenv("R2_MIN_SIZE_RATIO", "0.01")
        with pytest.raises(refused, match="Unset R2_MIN_SIZE_RATIO: a hand publish keeps the default shrink guard"):
            run(tmp_path, "accepted", second, evidence, *dry, "--added-table", "irs990_grants.parquet")
    # Every refusal above named the same --output and left nothing there, so the corrected command runs as typed.
    assert not (tmp_path / "accepted").exists()

    output = run(tmp_path, "accepted", second, evidence, *dry, "--added-table", "irs990_grants.parquet",
                 "--expect-prior", first_digest[:12])
    receipt = json.loads((output / "dry-run.json").read_text())
    assert json.loads(capsys.readouterr().out) == receipt
    assert receipt["status"] == "dry_run_accepted" and receipt["family"] == FAMILY
    assert receipt["artifactDigest"] == "sha256:" + second_digest
    assert receipt["prefix"] == f"generations/{FAMILY}/{second_digest}"
    assert receipt["prior"]["artifactDigest"] == "sha256:" + first_digest
    assert {key: (table["rows"], table["priorRows"]) for key, table in receipt["tables"].items()} == {
        "irs990_grants.parquet": (1, None), "irs990_filings.parquet": (3, 2), "irs990_officers.parquet": (1, 1)}
    assert receipt["added_tables"] == ["irs990_grants.parquet"] and receipt["other_family_changes"] == []
    assert receipt["raw_archive"] == "scratch: not publishable" and len(receipt["evidence"]) == 1
    # irs990_officers.parquet is byte-identical to the live member, so the publisher copies it server-side.
    assert receipt["store_members_copied"] == 1 and receipt["store_bytes_written"] > 0
    # The store was a rehearsal: only the receipt remains, and the saved live index is as it was.
    assert [path.name for path in output.iterdir()] == ["dry-run.json"]
    assert not list(tmp_path.glob(".dry-run-store-*"))
    assert publication.parse_index(live.read_bytes())["families"][FAMILY]["artifactDigest"] == "sha256:" + first_digest
    with pytest.raises(SystemExit):
        run(tmp_path, "accepted", second, evidence, *dry, "--added-table", "irs990_grants.parquet")

    # A build that recorded the index it read is held to it by the publisher itself: once the family has
    # moved on, naming the new live digest in --expect-prior does not make the old build publishable.
    third, _, third_evidence = generation(tmp_path, "third", {"irs990_filings": ["a", "b", "d"], "irs990_officers": ["p"]},
                                          read=(live.read_bytes(), (first / "artifact.json").read_bytes()))
    publication.publish_generation(second, client=store, bucket="live", prior_index=publication.parse_index(
        live.read_bytes()), evidence_directories=(evidence,), added_tables=frozenset({"irs990_grants.parquet"}))
    moved = tmp_path / "publication-moved.json"
    moved.write_bytes(store.objects[publication.INDEX_V2_KEY].read_bytes())
    with pytest.raises(refused, match="Family changed since the build read its inputs; rebuild before publishing"):
        run(tmp_path, "stale-build", third, third_evidence, "--dry-run", "--scratch-raw", "--live-index", str(moved),
            "--expect-prior", second_digest)


def test_publish_holds_the_target_and_raw_root_then_reads_back_publicly(tmp_path, monkeypatch):
    directory, digest, evidence = generation(tmp_path, "candidate", {"irs990_filings": ["a", "b"]})
    later, later_digest, later_evidence = generation(tmp_path, "later", {"irs990_filings": ["a", "b", "c"]})
    store = Store()
    endpoint = SimpleNamespace(endpoint_url=r2.VERIFIED_ENDPOINT)
    monkeypatch.setattr(store, "meta", endpoint, raising=False)
    monkeypatch.setattr(r2, "get_r2_client", lambda: store)
    monkeypatch.setattr("dotenv.load_dotenv", lambda path: None)
    shared = {"root": tmp_path / "central-raw"}  # the archive every test is given: the builds retained there
    monkeypatch.setattr("spicy_docs.storage.raw_archive.default_raw_archive_root", lambda: shared["root"])
    refused = publication.PublicationError

    with pytest.raises(refused, match="Unset SPICYGOV_RAW_ROOT"):
        run(tmp_path, "receipt", directory, evidence)
    monkeypatch.delenv("SPICYGOV_RAW_ROOT")
    # An evidence directory copied away from its build has no pointer: refused, not assumed shared.
    moved = tmp_path / "moved" / "artifact"
    shutil.copytree(evidence, moved)
    with pytest.raises(refused, match="is not beside the source-archive.json its build wrote"):
        run(tmp_path, "receipt", directory, moved)
    (moved.parent / ARCHIVE_REFERENCE).write_text(json.dumps({"root": str(tmp_path / "scratch-raw")}))
    with pytest.raises(refused, match="built on another raw archive than the shared one"):
        run(tmp_path, "receipt", directory, moved)
    # A pointer that names the shared archive is not enough: each cited original must be in it.
    shared["root"] = tmp_path / "another-shared"
    shared["root"].mkdir()
    (moved.parent / ARCHIVE_REFERENCE).write_text(json.dumps({"root": str(shared["root"])}))
    with pytest.raises(refused, match="1 cited original.s. are not in the shared raw archive"):
        run(tmp_path, "receipt", directory, moved)
    shared["root"] = tmp_path / "central-raw"

    with monkeypatch.context() as loosened:
        loosened.setenv("R2_ALLOW_SHRINK", "1")
        with pytest.raises(refused, match="Unset R2_ALLOW_SHRINK"):
            run(tmp_path, "receipt", directory, evidence)
    with pytest.raises(RuntimeError, match="requires R2 credentials"):
        run(tmp_path, "receipt", directory, evidence)
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "fixture")
    monkeypatch.setattr(endpoint, "endpoint_url", "https://other-account.r2.cloudflarestorage.com")
    with pytest.raises(ValueError, match="differs from the verified target"):
        run(tmp_path, "receipt", directory, evidence)
    assert not any(key.startswith("generations/") or key == publication.INDEX_V2_KEY for key in store.writes)
    assert not (tmp_path / "receipt").exists()

    monkeypatch.setattr(endpoint, "endpoint_url", r2.VERIFIED_ENDPOINT)
    served = []
    monkeypatch.setattr(publication, "load_index", lambda base: served.append(base) or publication.parse_index(
        store.objects[publication.INDEX_V2_KEY]))
    monkeypatch.setattr(publication, "load_family_root", lambda base, entry: publication.family_root(
        store.objects[entry["prefix"] + "/artifact.json"], entry))
    monkeypatch.setattr(publication, "verify_member", lambda base, member: len(store.objects[member.path]))
    receipt = json.loads((run(tmp_path, "receipt", directory, evidence) / "receipt.json").read_text())
    assert receipt["status"] == "published_public_readback" and receipt["prior"] is None
    assert receipt["artifactDigest"] == "sha256:" + digest and receipt["tables"]["irs990_filings.parquet"]["rows"] == 2
    assert receipt["public_index_url"] == "https://data.spicygov.ai/publication.v2.json"
    assert receipt["public_bytes_read"] == len(store.objects[receipt["prefix"] + "/irs990_filings.parquet"])
    assert served == ["https://data.spicygov.ai"]

    # A rerun after an uncertain result writes nothing: the family already is this generation.
    written = list(store.writes)
    again = json.loads((run(tmp_path, "again", directory, evidence) / "receipt.json").read_text())
    assert again["status"] == "already_published_public_readback" and store.writes == written

    # The family is live now. A build made before someone else's publish must not replace it unseen:
    # the publish needs the digest its dry run compared against, and that digest must still be live.
    with pytest.raises(refused, match=f"{FAMILY} is live: pass --expect-prior"):
        run(tmp_path, "later-receipt", later, later_evidence)
    with pytest.raises(refused, match=f"Live {FAMILY} is {digest}, not the expected prior {later_digest}"):
        run(tmp_path, "later-receipt", later, later_evidence, "--expect-prior", later_digest)
    assert store.writes == written
    replaced = json.loads((run(tmp_path, "later-receipt", later, later_evidence, "--expect-prior", digest[:12])
                           / "receipt.json").read_text())
    assert replaced["status"] == "published_public_readback"
    assert replaced["prior"]["artifactDigest"] == "sha256:" + digest
    assert replaced["tables"]["irs990_filings.parquet"] == {**replaced["tables"]["irs990_filings.parquet"], "rows": 3, "priorRows": 2}


def test_only_publisher_originals_must_be_in_the_shared_archive(tmp_path, monkeypatch):
    """A build's own control files and normalized inputs stay with its evidence; a missing original still refuses.

    A House build cites three run-local files per quarter beside each original (ss-coca): counting them as
    originals refused every such build.
    """
    shared = {"root": tmp_path / "central-raw"}  # where conftest's SPICYGOV_RAW_ROOT put the build's originals
    monkeypatch.setattr("spicy_docs.storage.raw_archive.default_raw_archive_root", lambda: shared["root"])
    selection, normalized = tmp_path / "selection.json", tmp_path / "normalized.csv"
    selection.write_text(json.dumps({"files": [{"url": "https://example.gov/q2.csv"}]}))
    normalized.write_text("amount\n1.00\n")
    evidence = CaptureEvidence(tmp_path / "build", FAMILY)
    evidence.retain_bytes(b"the publisher's quarter", stage="fixture-original", raw_source=True)
    evidence.retain_control(selection, stage="fixture-selection")
    evidence.retain_file(normalized, stage="fixture-normalized-input")
    evidence.finish()
    monkeypatch.delenv("SPICYGOV_RAW_ROOT")

    command._require_shared_raw((evidence.artifact_dir,))
    # The pointer names an archive that lacks the original: refused, counting the original alone.
    shared["root"] = tmp_path / "another-shared"
    shared["root"].mkdir()
    (evidence.directory / ARCHIVE_REFERENCE).write_text(json.dumps({"root": str(shared["root"])}))
    with pytest.raises(publication.PublicationError, match=r"^1 cited original\(s\) are not in the shared raw archive"):
        command._require_shared_raw((evidence.artifact_dir,))



def test_census_geocoder_uses_the_cli_with_the_existing_family_gates(tmp_path, capsys):
    """Family admission reaches the real publisher and keeps evidence and prior guards."""
    family = "census-geocoder"
    directory, digest, evidence = generation(tmp_path, "geocoder", {
        "census_geocoder_lookups": ["lookup"], "census_geocoder_geographies": ["06001"],
    }, family=family)
    live = tmp_path / "publication.v2.json"
    live.write_text(json.dumps(publication.empty_index()))
    dry = ("--dry-run", "--scratch-raw", "--live-index", str(live))
    with pytest.raises(publication.PublicationError, match="source evidence it pins"):
        run(tmp_path, "accepted", directory, None, *dry, family=family)
    with pytest.raises(publication.PublicationError, match=f"not the expected {FAMILY}"):
        run(tmp_path, "accepted", directory, evidence, *dry)
    with pytest.raises(publication.PublicationError, match="Live census-geocoder is absent"):
        run(tmp_path, "accepted", directory, evidence, *dry, "--expect-prior", "a" * 64, family=family)
    assert not (tmp_path / "accepted").exists()
    output = run(tmp_path, "accepted", directory, evidence, *dry, family=family)
    receipt = json.loads((output / "dry-run.json").read_text())
    assert json.loads(capsys.readouterr().out) == receipt
    assert receipt["status"] == "dry_run_accepted" and receipt["family"] == family
    assert receipt["artifactDigest"] == "sha256:" + digest
    assert {key: entry["rows"] for key, entry in receipt["tables"].items()} == {
        "census_geocoder_lookups.parquet": 1, "census_geocoder_geographies.parquet": 1,
    }
    assert receipt["evidence"] and receipt["other_family_changes"] == []
    assert publication.parse_index(live.read_bytes()) == publication.empty_index()
    assert not list(tmp_path.glob(".dry-run-store-*"))


def test_open_payments_cli_refuses_a_generation_of_another_family(tmp_path):
    directory, _, evidence = generation(tmp_path, "local", {"irs990_filings": ["a"]})
    with pytest.raises(publication.PublicationError, match="not the expected cms-open-payments"):
        run(tmp_path, "refused", directory, evidence, "--dry-run", family="cms-open-payments")
    assert not (tmp_path / "refused").exists()


def test_schema_parity_guard_refuses_missing_and_mismatched_columns_on_dry_run(tmp_path):
    from spicy_regs.schemas.house_disbursement_pages import PARTITION_COLUMNS, SCHEMAS as HDP_SCHEMAS

    live = tmp_path / "private-publication.v2.json"
    live.write_text(json.dumps(publication.empty_index()))
    dry = ("--dry-run", "--scratch-raw", "--live-index", str(live))

    # 1. Single-file table missing a declared column fails dry_run with PublicationError.
    rows_schema = HDP_SCHEMAS["house_disbursement_pdf_rows"]
    pages_schema = HDP_SCHEMAS["house_disbursement_pdf_pages"]
    broken_rows = pa.schema([field for field in rows_schema if field.name != "reading_method"])

    src = tmp_path / "broken-single-src"
    src.mkdir()
    pq.write_table(_table_for_schema(broken_rows, ["1"]), src / "house_disbursement_pdf_rows.parquet")
    pq.write_table(_table_for_schema(pages_schema, ["1"]), src / "house_disbursement_pdf_pages.parquet")
    ev = CaptureEvidence(tmp_path / "broken-single-ev", "house-disbursement-pages")
    ev.retain_bytes(b"pdf original", stage="fixture-original", raw_source=True)
    gen_dir = tmp_path / "broken-single-gen"
    build_generation(
        gen_dir,
        family="house-disbursement-pages",
        files=[src / "house_disbursement_pdf_rows.parquet", src / "house_disbursement_pdf_pages.parquet"],
        expected_keys=["house_disbursement_pdf_rows.parquet", "house_disbursement_pdf_pages.parquet"],
        inputs=ev.inputs(),
    )
    ev.finish()
    with pytest.raises(publication.PublicationError, match="reading_method"):
        run(tmp_path, "refused-missing-col", gen_dir, ev.artifact_dir, *dry, family="house-disbursement-pages")
    assert not (tmp_path / "refused-missing-col").exists()

    # 2. Partitioned table with a drifted partition member (Nov 6 35/23-col v3 mismatch shape) fails dry_run.
    part_src = tmp_path / "part-src"
    rows_dir = part_src / "house_disbursement_pdf_rows" / "report_year=2025" / "report_start_month=1"
    rows_dir.mkdir(parents=True)
    # Write partition file missing trailing columns.
    drifted_schema = pa.schema(list(rows_schema)[:23])
    drifted_table = _table_for_schema(drifted_schema, ["1"]).set_column(
        rows_schema.get_field_index("report_year"),
        "report_year",
        pa.array([2025], type=pa.int32()),
    ).set_column(
        rows_schema.get_field_index("report_start_month"),
        "report_start_month",
        pa.array([1], type=pa.int32()),
    )
    pq.write_table(drifted_table, rows_dir / "part-000000.parquet")

    pages_dir = part_src / "house_disbursement_pdf_pages" / "report_year=2025" / "report_start_month=1"
    pages_dir.mkdir(parents=True)
    valid_pages = _table_for_schema(pages_schema, ["1"]).set_column(
        pages_schema.get_field_index("report_year"),
        "report_year",
        pa.array([2025], type=pa.int32()),
    ).set_column(
        pages_schema.get_field_index("report_start_month"),
        "report_start_month",
        pa.array([1], type=pa.int32()),
    )
    pq.write_table(valid_pages, pages_dir / "part-000000.parquet")

    ev_part = CaptureEvidence(tmp_path / "part-ev", "house-disbursement-pages")
    ev_part.retain_bytes(b"pdf original", stage="fixture-original", raw_source=True)
    part_gen = tmp_path / "part-gen"
    build_generation(
        part_gen,
        family="house-disbursement-pages",
        files=[part_src / "house_disbursement_pdf_rows", part_src / "house_disbursement_pdf_pages"],
        expected_keys=["house_disbursement_pdf_rows.parquet", "house_disbursement_pdf_pages.parquet"],
        partitioned={
            "house_disbursement_pdf_rows.parquet": PARTITION_COLUMNS,
            "house_disbursement_pdf_pages.parquet": PARTITION_COLUMNS,
        },
        inputs=ev_part.inputs(),
    )
    ev_part.finish()
    with pytest.raises(publication.PublicationError, match="schema parity check failed.*house_disbursement_pdf_rows"):
        run(tmp_path, "refused-part-drift", part_gen, ev_part.artifact_dir, *dry, family="house-disbursement-pages")
    assert not (tmp_path / "refused-part-drift").exists()

    # 3. Matching partitioned house-disbursement-pages generation passes dry_run.
    valid_rows = _table_for_schema(rows_schema, ["1"]).set_column(
        rows_schema.get_field_index("report_year"),
        "report_year",
        pa.array([2025], type=pa.int32()),
    ).set_column(
        rows_schema.get_field_index("report_start_month"),
        "report_start_month",
        pa.array([1], type=pa.int32()),
    )
    pq.write_table(valid_rows, rows_dir / "part-000000.parquet")
    ev_ok = CaptureEvidence(tmp_path / "ok-ev", "house-disbursement-pages")
    ev_ok.retain_bytes(b"pdf original", stage="fixture-original", raw_source=True)
    ok_gen = tmp_path / "ok-gen"
    build_generation(
        ok_gen,
        family="house-disbursement-pages",
        files=[part_src / "house_disbursement_pdf_rows", part_src / "house_disbursement_pdf_pages"],
        expected_keys=["house_disbursement_pdf_rows.parquet", "house_disbursement_pdf_pages.parquet"],
        partitioned={
            "house_disbursement_pdf_rows.parquet": PARTITION_COLUMNS,
            "house_disbursement_pdf_pages.parquet": PARTITION_COLUMNS,
        },
        inputs=ev_ok.inputs(),
    )
    ev_ok.finish()
    out_ok = run(tmp_path, "accepted-hdp", ok_gen, ev_ok.artifact_dir, *dry, family="house-disbursement-pages")
    assert json.loads((out_ok / "dry-run.json").read_text())["status"] == "dry_run_accepted"


def test_local_families_are_disjoint_from_hosted_rollup_workflows():
    """Every family with a hosted rollup-*.yml workflow (scheduled or dispatch-only) is excluded from LOCAL_FAMILIES."""
    workflows_dir = Path(__file__).resolve().parents[1] / ".github" / "workflows"
    rollup_workflows = sorted(workflows_dir.glob("rollup-*.yml"))
    assert rollup_workflows
    hosted_families = set()
    scheduled_hosted = set()
    for workflow_path in rollup_workflows:
        family = workflow_path.stem.removeprefix("rollup-")
        hosted_families.add(family)
        loaded = yaml.safe_load(workflow_path.read_text())
        on = loaded.get("on", loaded.get(True, {}))
        assert "workflow_dispatch" in on or "schedule" in on
        if "schedule" in on:
            scheduled_hosted.add(family)

    assert {"executive-orders", "fec-financial-profiles", "treasury-fiscal"} <= scheduled_hosted
    expected_removed = {
        "administration-policy",
        "executive-orders",
        "fec-financial-profiles",
        "grants-opportunities",
        "house-disbursements",
        "ofac-sanctions",
        "treasury-fiscal",
    }
    assert expected_removed <= hosted_families
    assert command.LOCAL_FAMILIES.isdisjoint(hosted_families)


@pytest.mark.parametrize(
    ("family", "table"),
    [
        ("cms-part-d", "cms_part_d_drug_spending"),
        ("cms-medicaid", "cms_medicaid_drug_spending"),
        ("cms-part-b", "cms_part_b_drug_spending"),
    ],
)
def test_cms_drug_spending_families_are_registered_and_dry_run_accepted(tmp_path, family, table):
    assert family in command.LOCAL_FAMILIES
    assert command._family_schemas(family) is not None
    directory, digest, evidence = generation(tmp_path, family, {table: ["obs-1"]}, family=family)
    live = tmp_path / "publication.v2.json"
    live.write_text(json.dumps(publication.empty_index()))
    output = run(
        tmp_path,
        f"accepted-{family}",
        directory,
        evidence,
        "--dry-run",
        "--scratch-raw",
        "--live-index",
        str(live),
        family=family,
    )
    receipt = json.loads((output / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted" and receipt["family"] == family
    assert receipt["artifactDigest"] == "sha256:" + digest


def test_dry_run_refuses_before_writing_when_projected_free_space_falls_below_40_gib(tmp_path, monkeypatch):
    """Upfront 40 GiB floor refuses before creating parent dirs or .dry-run-store-* scratch."""
    directory, _, evidence = generation(tmp_path, "guarded", {"irs990_filings": ["a", "b"]})
    live = tmp_path / "publication.v2.json"
    live.write_text(json.dumps(publication.empty_index()))
    missing_parent = tmp_path / "not-created-yet"
    output = missing_parent / "dry-run-out"

    probed: list[Path] = []

    def below_floor(path: Path):
        probed.append(Path(path))
        return SimpleNamespace(free=command.DRY_RUN_FREE_FLOOR_BYTES - 1024**3)

    monkeypatch.setattr(shutil, "disk_usage", below_floor)
    with pytest.raises(publication.PublicationError, match="below the 40 GiB floor"):
        command.main([
            "--generation", str(directory), "--family", FAMILY, "--evidence", str(evidence),
            "--dry-run", "--scratch-raw", "--live-index", str(live), "--output", str(output),
        ])
    assert probed == [tmp_path.resolve()]
    assert not missing_parent.exists()
    assert not list(tmp_path.glob(".dry-run-store-*"))

    # Even when current free space exceeds 40 GiB slightly, subtracting projected temp bytes still refuses.
    root = command._candidate(directory, FAMILY, (evidence,))
    args = SimpleNamespace(
        generation=directory, family=FAMILY, evidence=[evidence], added_table=[],
        expect_prior=None, output=output, live_index=live, scratch_raw=True,
    )
    local_sources = command._dry_run_local_sources(directory, FAMILY, root, None, (evidence,))
    projected = command._projected_dry_run_bytes(
        directory, FAMILY, root, (evidence,), live.read_bytes(), local_sources=local_sources,
    )
    assert projected > 1024
    with pytest.raises(publication.PublicationError, match="below the 40 GiB floor"):
        command.dry_run(args, root, disk_usage=lambda _: SimpleNamespace(free=command.DRY_RUN_FREE_FLOOR_BYTES + 1024))
    assert not missing_parent.exists()
    assert not list(tmp_path.glob(".dry-run-store-*"))

    # At exact floor + projected bytes, dry_run is admitted and succeeds.
    receipt = command.dry_run(
        args, root, disk_usage=lambda _: SimpleNamespace(free=command.DRY_RUN_FREE_FLOOR_BYTES + projected),
    )
    assert receipt["status"] == "dry_run_accepted"
    assert not list(missing_parent.glob(".dry-run-store-*"))


def test_dry_run_multipart_verifies_without_duplicating_payload_bytes(tmp_path, monkeypatch):
    """Multipart evidence blobs, changed tables and copied members verify with zero payload bytes written to disk."""
    from spicy_regs.sources import disk_store as disk_store_module

    # Build a prior generation with one large table that stays unchanged across refreshes.
    unchanged_ids = [os.urandom(256).hex() for _ in range(700)]  # > 350 KiB uncompressible
    first, _, first_ev = generation(
        tmp_path, "first", {"irs990_filings": ["a", "b"], "irs990_officers": unchanged_ids},
    )
    live_store = DiskStore(tmp_path / "live-store", bucket="live")
    publication.publish_generation(
        first, client=live_store, bucket="live", prior_index=publication.empty_index(),
        evidence_directories=(first_ev,),
    )
    live = tmp_path / "publication.v2.json"
    live.write_bytes(live_store.objects[publication.INDEX_V2_KEY].read_bytes())

    # Build candidate with a >350 KiB changed table, the unchanged >350 KiB table, and a 1 MiB evidence blob.
    changed_ids = [os.urandom(256).hex() for _ in range(700)]
    declared = command._family_schemas(FAMILY) or {}
    src = tmp_path / "second-source"
    src.mkdir()
    changed_path = src / "irs990_filings.parquet"
    unchanged_path = src / "irs990_officers.parquet"
    pq.write_table(_table_for_schema(declared["irs990_filings"], changed_ids), changed_path)
    shutil.copyfile(first / "irs990_officers.parquet", unchanged_path)

    ev_blob = os.urandom(1024 * 1024)  # 1 MiB uncompressible evidence original
    ev = CaptureEvidence(tmp_path / "second-build", FAMILY)
    ev.retain_bytes(ev_blob, stage="bulk-export", raw_source=True)
    second = tmp_path / "second"
    build_generation(
        second, family=FAMILY, files=[changed_path, unchanged_path],
        expected_keys=["irs990_filings.parquet", "irs990_officers.parquet"], inputs=ev.inputs(),
    )
    ev.finish()

    changed_size = (second / "irs990_filings.parquet").stat().st_size
    unchanged_size = (second / "irs990_officers.parquet").stat().st_size
    assert changed_size > 256 * 1024 and unchanged_size > 256 * 1024

    # Lower PART_BYTES to 64 KiB so both the 1 MiB evidence blob and changed Parquet table take the multipart path.
    monkeypatch.setattr(publication, "PART_BYTES", 64 * 1024)

    observed = SimpleNamespace(upload_parts=0, peak_bytes=0, multipart_dir_created=False, disk_bytes=-1)

    def dir_bytes(root_path: Path) -> int:
        return sum(p.stat().st_size for p in root_path.rglob("*") if p.is_file())

    class ObservingDiskStore(DiskStore):
        def _record(self) -> None:
            observed.peak_bytes = max(observed.peak_bytes, dir_bytes(self.root))
            if (self.root / "multipart").exists():
                observed.multipart_dir_created = True
            observed.disk_bytes = self.disk_bytes_written

        def put_object(self, **kwargs):
            result = super().put_object(**kwargs)
            self._record()
            return result

        def copy_object(self, **kwargs):
            result = super().copy_object(**kwargs)
            self._record()
            return result

        def upload_part(self, **kwargs):
            observed.upload_parts += 1
            result = super().upload_part(**kwargs)
            self._record()
            return result

        def complete_multipart_upload(self, **kwargs):
            result = super().complete_multipart_upload(**kwargs)
            self._record()
            return result

    monkeypatch.setattr(disk_store_module, "DiskStore", ObservingDiskStore)

    output = run(
        tmp_path, "multipart-dry-run", second, ev.artifact_dir,
        "--dry-run", "--scratch-raw", "--live-index", str(live),
    )
    receipt = json.loads((output / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted"
    assert receipt["store_members_copied"] == 1
    assert observed.upload_parts >= 20  # >=16 parts for 1 MiB blob + >=5 parts for changed Parquet
    assert not observed.multipart_dir_created
    # store_bytes_written still reflects what a real R2 publish would upload (excluding the server-side copied table).
    assert receipt["store_bytes_written"] >= len(ev_blob) + changed_size
    assert receipt["store_bytes_written"] < len(ev_blob) + changed_size + unchanged_size
    # Actual local temporary disk writes are strictly metadata only (well below a single 64 KiB part).
    assert receipt["store_disk_bytes_written"] == observed.disk_bytes
    assert observed.peak_bytes < 64 * 1024
    assert receipt["store_disk_bytes_written"] < 64 * 1024

    # Projection reflects zero-copy references vs unreferenced full-copy peak (~2x largest blob).
    root = command._candidate(second, FAMILY, (ev.artifact_dir,))
    local_sources = command._dry_run_local_sources(
        second, FAMILY, root, publication.parse_index(live.read_bytes())["families"][FAMILY], (ev.artifact_dir,),
    )
    zero_copy_proj = command._projected_dry_run_bytes(
        second, FAMILY, root, (ev.artifact_dir,), live.read_bytes(), local_sources=local_sources,
    )
    copy_proj = command._projected_dry_run_bytes(
        second, FAMILY, root, (ev.artifact_dir,), live.read_bytes(), local_sources=None,
    )
    assert zero_copy_proj < 256 * 1024
    assert copy_proj >= 2 * len(ev_blob) + changed_size + unchanged_size


@pytest.mark.parametrize("recompute_md5", [False, True])
def test_dry_run_multipart_preserves_corruption_refusals(tmp_path, monkeypatch, recompute_md5):
    """Damaged multipart bytes still refuse via BadDigest or read-back verification even in zero-copy dry-run mode."""
    from spicy_regs.sources import disk_store as disk_store_module

    ev_blob = os.urandom(256 * 1024)
    declared = command._family_schemas(FAMILY) or {}
    src = tmp_path / "src"
    src.mkdir()
    pq_path = src / "irs990_filings.parquet"
    pq.write_table(_table_for_schema(declared["irs990_filings"], ["a"]), pq_path)
    ev = CaptureEvidence(tmp_path / "build", FAMILY)
    ev.retain_bytes(ev_blob, stage="bulk-export", raw_source=True)
    gen_dir = tmp_path / "gen"
    build_generation(gen_dir, family=FAMILY, files=[pq_path], expected_keys=[pq_path.name], inputs=ev.inputs())
    ev.finish()
    live = tmp_path / "live.json"
    live.write_text(json.dumps(publication.empty_index()))
    monkeypatch.setattr(publication, "PART_BYTES", 64 * 1024)

    class CorruptingDiskStore(DiskStore):
        def upload_part(self, **kwargs):
            if "/blobs/sha256/" in kwargs["Key"] and kwargs["PartNumber"] == 2:
                damaged = bytes([kwargs["Body"][0] ^ 0xFF]) + kwargs["Body"][1:]
                kwargs["Body"] = damaged
                if recompute_md5:
                    kwargs["ContentMD5"] = base64.b64encode(hashlib.md5(damaged, usedforsecurity=False).digest()).decode()
            return super().upload_part(**kwargs)

    monkeypatch.setattr(disk_store_module, "DiskStore", CorruptingDiskStore)
    expected_error = ArtifactVerificationError if recompute_md5 else ClientError
    with pytest.raises(expected_error):
        run(
            tmp_path, "corrupt-dry-run", gen_dir, ev.artifact_dir,
            "--dry-run", "--scratch-raw", "--live-index", str(live),
        )
    assert not (tmp_path / "corrupt-dry-run").exists()
    assert not list(tmp_path.glob(".dry-run-store-*"))
