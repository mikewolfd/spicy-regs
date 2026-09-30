"""Caller coverage facts never become provider parse outcomes or successful empty data."""

import hashlib
import json

import duckdb
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.build_fec_observations import MAX_CONTEXT_BYTES, build_fec_observations
from tests.test_fec_observations import _manifest, _query, _rows


def _context(tmp_path, value=None):
    value = value or {
        "version": 1,
        "source_family": "fec_reports",
        "source_authority": "unofficial Senate submission",
        "related_collection_ids": ["committees"],
        "parsing": {"status": "refused", "reader": "native-filing", "reason": "Malformed source quotes"},
        "selection": {"status": "retained", "scope": "one captured original"},
        "relationships": {"status": "not_mapped", "source_record_count": None},
    }
    path = tmp_path / "context.json"
    path.write_text(json.dumps(value))
    return {"path": "context.json", "sha256": "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()}


def _disposition(tmp_path, status="refused"):
    return {
        "collection_id": "coverage:refused-original",
        "source_family": "fec_reports",
        "disposition": {
            "status": status,
            "reason": "Retained source cannot be parsed by the named reader",
            "context": _context(tmp_path),
        },
    }


def test_context_preserves_prior_rows_and_separates_refusal_from_empty_result(tmp_path):
    selected, _ = _query(tmp_path)
    empty, _ = _query(tmp_path, name="empty", records=[], profile="candidate", family="fec_candidates")
    baseline = _rows(build_fec_observations(_manifest(tmp_path, [selected, empty]), tmp_path / "prior"))
    disposition = _disposition(tmp_path)
    paths = build_fec_observations(_manifest(tmp_path, [selected, empty, disposition]), tmp_path / "combined")
    records, collections, relationships = _rows(paths)
    assert records == baseline[0] and collections[:2] == baseline[1] and relationships == baseline[2]
    row = collections[2]
    assert row["record_count"] == row["relationship_count"] == "0"
    assert row["record_outcome"] == "refused"
    for key in (
        "profile",
        "source_system_id",
        "source_state_scope",
        "requested_scope_json",
        "coverage_limits_json",
        "artifact_sha256",
    ):
        assert row[key] is None
    outcome = json.loads(row["collection_outcome_json"])
    assert outcome["providerOutcome"] is None
    assert outcome["receiverDisposition"]["sourceRecordCount"] is None
    assert outcome["receiverDisposition"]["callerContext"]["pin"] == disposition["disposition"]["context"]
    with duckdb.connect() as connection:
        assert connection.execute(
            "SELECT collection_id, record_outcome, profile FROM read_parquet(?) WHERE record_count='0' ORDER BY collection_id",
            [str(paths[1])],
        ).fetchall() == [("coverage:refused-original", "refused", None), ("empty", "empty", "candidate")]


@pytest.mark.parametrize("status", ["retained_unparsed", "unresolved", "inventory_only", "selection_context"])
def test_declared_non_parse_dispositions_are_not_successful_empty_collections(tmp_path, status):
    paths = build_fec_observations(_manifest(tmp_path, [_disposition(tmp_path, status)]), tmp_path / "out")
    assert pq.read_table(paths[0]).num_rows == pq.read_table(paths[2]).num_rows == 0
    row = pq.read_table(paths[1]).to_pylist()[0]
    assert row["record_outcome"] == status and row["profile"] is None


@pytest.mark.parametrize(
    "mutation", ["digest", "missing", "oversized", "unknown_facts", "credential_field", "credential_url"]
)
def test_context_failure_aborts_complete_output_after_prior_valid_collection(tmp_path, mutation):
    selected, _ = _query(tmp_path)
    item = _disposition(tmp_path)
    path = tmp_path / "context.json"
    if mutation == "digest":
        path.write_text("{}")
    elif mutation == "missing":
        path.unlink()
    else:
        if mutation == "oversized":
            path.write_bytes(b" " * (MAX_CONTEXT_BYTES + 1))
        elif mutation == "unknown_facts":
            path.write_text(json.dumps({"version": 1, "arbitrary_log": "not source facts"}))
        elif mutation == "credential_field":
            path.write_text(json.dumps({"version": 1, "source_capture": {"api_key": "secret-value"}}))
        else:
            path.write_text(
                json.dumps(
                    {"version": 1, "source_capture": {"url": "https://api.open.fec.gov/v1/?api_key=secret-value"}}
                )
            )
        item["disposition"]["context"]["sha256"] = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises((ValueError, FileNotFoundError)):
        build_fec_observations(_manifest(tmp_path, [selected, item]), tmp_path / "out", batch_size=1)
    assert not (tmp_path / "out").exists()
    assert not list(tmp_path.glob(".fec-observations-*"))


@pytest.mark.parametrize("mutation", ["parse_mode", "success", "duplicate", "bad_pin"])
def test_disposition_cannot_masquerade_as_source_parse_or_reuse_identity(tmp_path, mutation):
    item = _disposition(tmp_path)
    items = [item]
    if mutation == "parse_mode":
        item["profile"] = "positional"
    elif mutation == "success":
        item["disposition"]["status"] = "empty"
    elif mutation == "duplicate":
        items.append(item)
    else:
        item["disposition"]["context"]["sha256"] = "invalid"
    with pytest.raises(ValueError):
        build_fec_observations(_manifest(tmp_path, items), tmp_path / "out")
    assert not (tmp_path / "out").exists()


def test_selected_input_can_bind_caller_context_without_changing_native_records(tmp_path):
    selected, _ = _query(tmp_path)
    prior = _rows(build_fec_observations(_manifest(tmp_path, [selected]), tmp_path / "prior"))
    selected["context"] = _context(tmp_path)
    current = _rows(build_fec_observations(_manifest(tmp_path, [selected]), tmp_path / "current"))
    assert current[0] == prior[0] and current[2] == prior[2]
    outcome = json.loads(current[1][0]["collection_outcome_json"])
    context = outcome.pop("callerContext")
    assert context["pin"] == selected["context"]
    assert outcome == json.loads(prior[1][0]["collection_outcome_json"])
