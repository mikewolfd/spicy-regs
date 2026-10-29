"""Byte-content reviews correct usability without rewriting capture history."""

import hashlib
import json
import sqlite3

import pytest

from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.raw_archive import apply_content_review
from spicy_regs.source_catalog.storage import connect, schema


RECEIPT_SHA = "a" * 64
REF = {"manifest": f"acquisitions/sha256/aa/{RECEIPT_SHA}", "sha256": "sha256:" + RECEIPT_SHA}
SHA = "sha256:" + "b" * 64
OTHER_SHA = "sha256:" + "c" * 64


def decision(sha=SHA, **changes):
    return {
        "sha256": sha, "bytes": 12, "classification": "Publisher error body", "role": "failed-response",
        "reason": "Verified original contains Meeting Not Found, not the requested meeting.",
        "evidence": [{"locator": "visible error message", "value": "Meeting Not Found"}],
        "limits": ["No transport status or complete source coverage is inferred."],
        "provenance": {"source_acquisition": "unresolved"}, "scope_complete": False,
        "non_publisher_source": False, "verified_original": True,
        "source_validation": "refused", "source_file_complete": None, **changes,
    }


def review(*decisions):
    return {"format": "spicygov-content-review", "version": 1, "decisions": list(decisions)}


def database(*statuses):
    db = sqlite3.connect(":memory:")
    schema(db)
    db.execute("INSERT INTO collections VALUES('example','Example','{}')")
    for sha in (SHA, OTHER_SHA):
        db.execute("INSERT INTO objects VALUES(?,12)", (sha,))
    for i, status in enumerate(statuses):
        facts = {"receipt_context": {"observed_at": f"2026-01-0{i + 1}T00:00:00Z"},
                 "mapping": {"http_status": 200, "source_validation": None}}
        db.execute("INSERT INTO files VALUES(?,?,?,?,?,?,?,?,?)", (
            str(i), "example", None, "same-name", status, SHA, 12, None, json.dumps(facts),
        ))
    db.execute("INSERT INTO files VALUES('other','example',NULL,'same-name','held',?,12,NULL,'{}')", (OTHER_SHA,))
    return db


def test_review_changes_all_exact_historical_aliases_without_transport_inference():
    db = database("held", "held")
    before = {r[0]: json.loads(r[1]) for r in db.execute("SELECT id,data FROM files")}
    assert apply_content_review(db, review(decision()), receipt_ref=REF)["annotated_aliases"] == 2
    assert list(db.execute("SELECT id,status FROM files ORDER BY id")) == [
        ("0", "refused-source"), ("1", "refused-source"), ("other", "held"),
    ]
    for ident, data in db.execute("SELECT id,data FROM files WHERE sha256=?", (SHA,)):
        facts = json.loads(data)
        assert facts["mapping"] == before[ident]["mapping"]
        assert facts["receipt_context"] == before[ident]["receipt_context"]
        assert facts["content_review"]["receipt"]["sha256"] == REF["sha256"]
    assert apply_content_review(db, review(decision()), receipt_ref=REF)["annotated_aliases"] == 0


def test_review_preserves_missing_receipt_only_failures_and_conflicts():
    statuses = ("missing-bytes", "receipt-only", "failed-response", "conflicting-receipt", "conflicting-metadata")
    db = database(*statuses)
    apply_content_review(db, review(decision()), receipt_ref=REF)
    assert [r[0] for r in db.execute("SELECT status FROM files WHERE sha256=? ORDER BY id", (SHA,))] == list(statuses)


def test_fragment_and_classification_only_review_do_not_claim_accepted_data():
    db = database("held")
    apply_content_review(db, review(decision(source_validation=None, source_file_complete=False)), receipt_ref=REF)
    assert db.execute("SELECT status FROM files WHERE id='0'").fetchone()[0] == "partial-source"
    db = database("held")
    apply_content_review(db, review(decision(source_validation=None)), receipt_ref=REF)
    assert db.execute("SELECT status FROM files WHERE id='0'").fetchone()[0] == "held"
    assert json.loads(db.execute("SELECT data FROM files WHERE id='0'").fetchone()[0])["mapping"]["source_validation"] is None


@pytest.mark.parametrize("changes", [
    {"http_status": 404}, {"source_validation": "accepted"}, {"scope_complete": True},
    {"verified_original": False}, {"source_file_complete": True}, {"non_publisher_source": True, "source_validation": None},
])
def test_invalid_review_cannot_invent_transport_or_acceptance(changes):
    db = database("held")
    with pytest.raises(ValueError, match="Invalid content review decision"):
        apply_content_review(db, review(decision(**changes)), receipt_ref=REF)
    assert db.execute("SELECT status FROM files WHERE id='0'").fetchone()[0] == "held"


def test_late_size_mismatch_does_not_partially_apply_review():
    db = database("held")
    with pytest.raises(ValueError, match="does not match retained originals"):
        apply_content_review(db, review(decision(), decision(OTHER_SHA, bytes=99)), receipt_ref=REF)
    assert db.execute("SELECT status FROM files WHERE id='0'").fetchone()[0] == "held"
    apply_content_review(db, review(decision()), receipt_ref=REF)
    with pytest.raises(ValueError, match="Conflicting content reviews"):
        apply_content_review(db, review(decision(reason="Different content conclusion")), receipt_ref=REF)


def test_rebuild_reads_pinned_review_receipt_after_all_aliases(tmp_path):
    root = tmp_path / "raw"
    store = CentralRawArchiveStore(root)
    body = b"No meeting!!"
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob(sha, len(body), [body])
    for n in (1, 2):
        store.record_receipt(files=[{
            "sha256": sha, "bytes": len(body), "http_status": 200,
            "receipt_context": {"observed_at": f"2026-01-0{n}T00:00:00Z"},
        }], receipt={"run_id": f"capture-{n}"})
    captured = store.record_receipt(files=[], receipt={
        "kind": "local-content-review", "content_review": review(decision("sha256:" + sha)),
    })
    before = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}
    index = tmp_path / "index.sqlite"
    build(root, index)
    # Rejected captures remain custody evidence outside the default usable view.
    with connect(index, readonly=True, scope="all") as db:
        rows = db.execute("SELECT id,status,data FROM files WHERE sha256=?", ("sha256:" + sha,)).fetchall()
        assert len(rows) == 2 and all(r[1] == "refused-source" for r in rows)
        for row in rows:
            facts = json.loads(row[2])
            assert facts["mapping"]["http_status"] == 200
            assert facts["content_review"]["receipt"]["sha256"] == "sha256:" + captured["sha256"]
    assert before == {str(p.relative_to(root)): p.read_bytes() for p in root.rglob("*") if p.is_file()}


@pytest.mark.parametrize(("changes", "expected"), [
    ({}, "refused-source"),
    ({"source_validation": None, "source_file_complete": False}, "partial-source"),
    ({"source_validation": None}, "unclassified-source"),
])
def test_explicit_review_refines_unclassified_content_without_acceptance(changes, expected):
    db = database("unclassified-source", "failed-response")
    before = {r[0]: json.loads(r[1]) for r in db.execute("SELECT id,data FROM files")}
    reviewed = review(decision(**changes))
    apply_content_review(db, reviewed, receipt_ref=REF)
    assert db.execute("SELECT status FROM files WHERE id='0'").fetchone()[0] == expected
    assert db.execute("SELECT status FROM files WHERE id='1'").fetchone()[0] == "failed-response"
    for ident, data in db.execute("SELECT id,data FROM files WHERE sha256=?", (SHA,)):
        facts = json.loads(data)
        assert facts["mapping"] == before[ident]["mapping"]
        assert facts["receipt_context"] == before[ident]["receipt_context"]
    assert apply_content_review(db, reviewed, receipt_ref=REF)["annotated_aliases"] == 0
