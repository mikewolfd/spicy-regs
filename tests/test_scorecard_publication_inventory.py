"""Coverage cannot advance from failed, changed or partly reconciled public evidence."""

from copy import deepcopy
from hashlib import sha256
import json
from typing import Any
import shutil

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import ReceiptContext, combine_receipts, failure_receipt, write_dataset
from spicy_regs import etl_policy_registry
from spicy_regs.generations import build_generation
from spicy_regs.scorecards.etl import EARLIER_POLICIES, POLICIES, SOURCE_NAMES, generation_options, write_family
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_scorecard_refresh import edition, tables

from spicy_regs.scorecards.operations import record


@pytest.fixture
def recorder():
    return record

def inputs(tmp_path):
    qualified = dict(
        format_version="scorecard-integration-qualified-scope/1", qualified=True, published=False,
        publisher_id="lcv", scorecard_id="lcv:2025", parser_version="source/1",
        completeness_rule="Complete source-defined snapshot", counts={"scorecard_member_ratings": 3},
        reader_sha256="a" * 64, capture_manifest_sha256="b" * 64,
        native_reference_sha256="c" * 64, source_qualification_sha256="d" * 64,
    )
    raw = json.dumps(qualified).encode()
    (tmp_path / "qualification.json").write_bytes(raw)
    record = dict(
        publisher_id="lcv", scorecard_id="lcv:2025", state="qualified",
        receipt="qualification.json", receipt_sha256=sha256(raw).hexdigest(),
    )
    pending = {**record, "scorecard_id": "lcv:2026", "receipt": "unpublished-missing.json"}
    ledger = dict(editions=[record, pending], readers={"lcv": "lcv"})
    (tmp_path / "integration_qualifications.json").write_text(json.dumps(ledger))
    (tmp_path / "integration_publications.json").write_text(json.dumps(dict(schema_version="1", editions=[], readers={})))
    proof = dict(
        format_version="scorecard-family-publication-readback/1", status="passed",
        source_generation="sha256:" + "e" * 64, public_member_pins_verified=True, hosted_scope_counts_verified=True,
        scopes={"lcv:2025": dict(publisher_id="lcv", parser_version="source/1", counts=qualified["counts"])},
    )
    return record, proof


def test_only_exact_verified_scopes_advance_and_saved_inputs_stay_immutable(tmp_path, recorder):
    _, proof = inputs(tmp_path)
    result = recorder.promote(tmp_path, proof)
    assert [r["scorecard_id"] for r in result["editions"]] == ["lcv:2025"]
    assert recorder.promote(tmp_path, proof) == result
    receipt = recorder.read_receipt(tmp_path, result["editions"][0])
    assert receipt["observed_source_generation"] == proof["source_generation"]
    (tmp_path / "qualification.json").write_text("{}")
    assert recorder.read_receipt(tmp_path, result["editions"][0]) == receipt
    changed = deepcopy(proof)
    changed["scopes"]["lcv:2025"]["counts"]["scorecard_member_ratings"] = 2
    proof_path = tmp_path / receipt["public_readback"]
    proof_path.write_text(json.dumps(changed))
    with pytest.raises(ValueError, match="pin"):
        recorder.read_receipt(tmp_path, result["editions"][0])


def test_qualification_flag_alone_cannot_establish_publication(tmp_path, recorder):
    record, _ = inputs(tmp_path)
    qualified, _ = recorder.document(tmp_path / "qualification.json")
    qualified["published"] = True
    raw = json.dumps(qualified).encode()
    (tmp_path / "qualification.json").write_bytes(raw)
    record.update(state="published", receipt_sha256=sha256(raw).hexdigest())
    with pytest.raises(ValueError, match="complete source scope"):
        recorder.read_receipt(tmp_path, record)


@pytest.mark.parametrize(
    "field,value", [("publisher_id", "hrc"), ("parser_version", "source/2"), ("counts", {"scorecard_member_ratings": 0})]
)
def test_scope_mismatch_does_not_mutate_publication_ledger(tmp_path, recorder, field, value):
    _, proof = inputs(tmp_path)
    before = (tmp_path / "integration_publications.json").read_bytes()
    proof["scopes"]["lcv:2025"][field] = value
    with pytest.raises(ValueError, match="complete public readback"):
        recorder.promote(tmp_path, proof)
    assert (tmp_path / "integration_publications.json").read_bytes() == before


@pytest.mark.parametrize("field,value", [("status", "failed"), ("public_member_pins_verified", False), ("hosted_scope_counts_verified", False)])
def test_incomplete_readback_does_not_mutate_publication_ledger(tmp_path, recorder, field, value):
    _, proof = inputs(tmp_path)
    before = (tmp_path / "integration_publications.json").read_bytes()
    proof[field] = value
    with pytest.raises(ValueError, match="complete public scope"):
        recorder.promote(tmp_path, proof)
    assert (tmp_path / "integration_publications.json").read_bytes() == before


@pytest.mark.parametrize("change", ["generation", "truncated", "count_pin", "different_query"])
def test_hosted_counts_require_the_exact_complete_query_and_generation(tmp_path, recorder, change):
    body: dict[str, Any] = dict(
        sql='SELECT "scorecard_id",count(*) AS rows FROM "scorecards" GROUP BY "scorecard_id" ORDER BY "scorecard_id"',
        columns=["scorecard_id", "rows"], rows=[["lcv:2025", 1]], truncated=False,
        publication={"scorecards": {"artifact_digest": "sha256:" + "a" * 64}},
    )
    if change == "generation":
        body["publication"]["scorecards"]["artifact_digest"] = "sha256:" + "b" * 64
    if change == "truncated":
        body["truncated"] = True
    if change == "different_query":
        body["sql"] += " LIMIT 1"
    raw = json.dumps(dict(structuredContent=body)).encode()
    (tmp_path / "scorecards_scope_counts.json").write_bytes(raw)
    summary = {"calls": [dict(tool="query_sql", label="scorecards_scope_counts", response_sha256=sha256(raw).hexdigest())]}
    if change == "count_pin":
        (tmp_path / "scorecards_scope_counts.json").write_text("{}")
    with pytest.raises(ValueError, match="readback|generation"):
        recorder.hosted_counts(tmp_path, summary, "scorecards", "sha256:" + "a" * 64)


def test_hosted_counts_publication_pins_are_keyed_by_table(tmp_path, recorder):
    body: dict[str, Any] = dict(
        sql='SELECT "scorecard_id",count(*) AS rows FROM "scorecard_members" GROUP BY "scorecard_id" ORDER BY "scorecard_id"',
        columns=["scorecard_id", "rows"], rows=[["lcv:2025", 3]], truncated=False,
        publication={"scorecard_members": {"artifact_digest": "sha256:" + "a" * 64}},
    )
    raw = json.dumps(dict(structuredContent=body)).encode()
    (tmp_path / "scorecard_members_scope_counts.json").write_bytes(raw)
    summary = {"calls": [dict(tool="query_sql", label="scorecard_members_scope_counts", response_sha256=sha256(raw).hexdigest())]}
    assert recorder.hosted_counts(tmp_path, summary, "scorecard_members", "sha256:" + "a" * 64) == {"lcv:2025": 3}


def complete_public_readback(tmp_path, *, snapshot_status="complete", refused_attempt=False, missing_snapshot=False, legacy=False):
    rows = tables(edition("2025"))
    rows["scorecard_snapshots"][0]["completeness_status"] = snapshot_status
    if missing_snapshot:
        rows["scorecard_snapshots"] = []
    source = tmp_path / "source"
    generation_id = "verified-source-generation"
    failures = [failure_receipt(
        POLICIES["scorecard_snapshots"], ReceiptContext(generation_id, "refused-source", "test-v1", [
            dict(source_id="fixture", sha256="sha256:" + "a" * 64)]),
        outcome="refused", raw_fields={"raw_source": {}},
    )] if refused_attempt else []
    files = write_family(source, rows, generation_id=generation_id, attempt_failures=failures)
    options = generation_options(source, SOURCE_NAMES)
    if legacy:
        policy = next(p for p in EARLIER_POLICIES["scorecard_snapshots"] if p.receipt_only)
        context = ReceiptContext(generation_id, "historical-snapshot", "test-v1", [
            dict(source_id="fixture", sha256="sha256:" + "a" * 64)])
        # Produce the admitted historical layout through the ordinary dataset writer.
        legacy_failures = [failure_receipt(policy, context, outcome="refused", raw_fields={"raw_source": {}})] if refused_attempt else []
        _, snapshots = write_dataset([(row, context) for row in rows["scorecard_snapshots"]], tmp_path / "legacy", policy,
                                     failures=legacy_failures)
        receipts = pq.read_table(source / "etl_receipts.parquet")
        rest = tmp_path / "non-snapshot-receipts.parquet"
        pq.write_table(pa.Table.from_pylist([row for row in receipts.to_pylist() if row["dataset"] != "scorecard_snapshots"],
                                           schema=receipts.schema), rest)
        combine_receipts([rest, snapshots], source / "etl_receipts.parquet")
        files = tuple(path for path in files if path.name != "scorecard_snapshots.parquet")
        options["receipt_policies"] = [policy if p.dataset == "scorecard_snapshots" else p for p in options["receipt_policies"]]
        options["schemas"].pop("scorecard_snapshots")
    candidate = tmp_path / "candidate"
    # Simulate the historical producer's exact installed policy, without weakening
    # current generation admission or the proof reader's policy selection.
    with pytest.MonkeyPatch.context() as historical_runtime:
        if legacy:
            installed = etl_policy_registry.installed_policies()
            installed["scorecard_snapshots"] = policy
            historical_runtime.setattr(etl_policy_registry, "installed_policies", lambda: installed)
        artifact = build_generation(candidate, family="scorecards", files=files, expected_keys=[p.name for p in files],
                                    **options)
        index = pub.publish_generation(candidate, client=Store(), bucket="test", prior_index=pub.empty_index())
    root = tmp_path / "readback"
    members = root / "members"
    members.mkdir(parents=True)
    public_files = []
    family = index["families"]["scorecards"]
    for name in (*family["tables"], "etl_receipts.parquet"):
        path = members / name
        shutil.copyfile(candidate / name, path)
        raw = path.read_bytes()
        public_files.append(dict(key=name, sha256="sha256:" + sha256(raw).hexdigest(), bytes=len(raw),
                                 rows=pq.ParquetFile(path).metadata.num_rows))
    summary: dict[str, Any] = dict(status="passed", observed_at="2026-10-08T04:00:00Z", generation=artifact.pin.as_dict(),
                   counts={name: len(values) for name, values in rows.items()}, calls=[],
                   preparation_sha256="a" * 64, publication_receipt_sha256="b" * 64)
    for name in SOURCE_NAMES:
        if name + ".parquet" not in family["tables"]:
            continue
        key = "publisher_id" if name == "scorecard_publishers" else "scorecard_id"
        counts = {}
        for row in rows[name]:
            counts[row[key]] = counts.get(row[key], 0) + 1
        body = dict(sql=f'SELECT "{key}",count(*) AS rows FROM "{name}" GROUP BY "{key}" ORDER BY "{key}"',
                    columns=[key, "rows"], rows=[[scope, count] for scope, count in sorted(counts.items())],
                    truncated=False, publication={name: dict(artifact_digest=artifact.pin.artifact_digest)})
        label = name + "_scope_counts"
        raw = json.dumps(dict(structuredContent=body)).encode()
        (root / (label + ".json")).write_bytes(raw)
        summary["calls"].append(dict(label=label, tool="query_sql", response_sha256=sha256(raw).hexdigest()))
    for name, value in (("summary.json", summary), ("public-index.json", index),
                        ("public-files.json", dict(status="passed", generation=artifact.pin.as_dict(), members=public_files))):
        (root / name).write_text(json.dumps(value))
    return root, rows


@pytest.mark.parametrize("legacy", [False, True])
def test_complete_publication_proof_reconciles_source_files_snapshots_and_hosted_counts(tmp_path, legacy):
    root, rows = complete_public_readback(tmp_path, legacy=legacy)
    proof = record.publication_proof(root)
    assert proof["status"] == "passed"
    assert proof["public_member_pins_verified"] is True
    assert proof["hosted_scope_counts_verified"] is True
    assert proof["scopes"] == {"lcv:2025": dict(publisher_id="lcv", parser_version="test-v1",
                                               counts={name: len(values) for name, values in rows.items()})}


@pytest.mark.parametrize("fault", ["member-bytes", "footer-count", "snapshot-incomplete", "snapshot-refused",
                                    "hosted-pin", "hosted-count", "missing-snapshot"])
@pytest.mark.parametrize("legacy", [False, True])
def test_failed_publication_proof_refuses_and_closes_connections(tmp_path, monkeypatch, fault, legacy):
    root, _ = complete_public_readback(tmp_path, snapshot_status="incomplete" if fault == "snapshot-incomplete" else "complete",
                                      refused_attempt=fault == "snapshot-refused", missing_snapshot=fault == "missing-snapshot", legacy=legacy)
    expected = {
        "member-bytes": "byte and footer pins", "footer-count": "byte and footer pins",
        "snapshot-incomplete": "lack complete", "snapshot-refused": "unfinished acquisition",
        "hosted-pin": "complete source generation", "hosted-count": "Hosted scope counts differ",
        "missing-snapshot": "snapshots and hosted source populations differ",
    }[fault]
    if fault == "member-bytes":
        path = root / "members/scorecards.parquet"
        path.write_bytes(path.read_bytes() + b"corrupt bytes")
    elif fault == "footer-count":
        files = json.loads((root / "public-files.json").read_bytes())
        index = json.loads((root / "public-index.json").read_bytes())
        next(row for row in files["members"] if row["key"] == "scorecards.parquet")["rows"] += 1
        index["families"]["scorecards"]["tables"]["scorecards.parquet"]["rows"] += 1
        (root / "public-files.json").write_text(json.dumps(files))
        (root / "public-index.json").write_text(json.dumps(index))
    elif fault in {"hosted-pin", "hosted-count"}:
        path = root / "scorecards_scope_counts.json"
        payload = json.loads(path.read_bytes())
        if fault == "hosted-pin":
            payload["structuredContent"]["publication"]["scorecards"]["artifact_digest"] = "sha256:" + "f" * 64
        else:
            payload["structuredContent"]["rows"][0][1] += 1
        path.write_text(json.dumps(payload))
        summary = json.loads((root / "summary.json").read_bytes())
        next(call for call in summary["calls"] if call["label"] == "scorecards_scope_counts")["response_sha256"] = sha256(path.read_bytes()).hexdigest()
        (root / "summary.json").write_text(json.dumps(summary))
    opened = []
    connect = duckdb.connect

    def tracked_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        opened.append(connection)
        return connection

    monkeypatch.setattr(record.duckdb, "connect", tracked_connect)
    with pytest.raises(ValueError, match=expected):
        record.publication_proof(root)
    assert opened
    with pytest.raises(duckdb.ConnectionException, match="closed"):
        opened[0].execute("SELECT 1")
