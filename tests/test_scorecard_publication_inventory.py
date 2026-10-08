"""Coverage cannot advance from failed, changed or partly reconciled public evidence."""

from copy import deepcopy
from hashlib import sha256
import json
from typing import Any

import pytest


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
