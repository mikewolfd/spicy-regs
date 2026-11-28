"""Hosted rollup receipts have permanent custody and catalog parity."""

import gzip
import hashlib
import json
from pathlib import Path

import pytest
import yaml
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts

from spicy_regs.source_catalog import hosted_receipts as hosted
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.storage import connect
from spicy_regs.sources import publication
from tests.generation_fakes import Store, error


def _commit(root, outcome, body=None):
    archive = CentralRawArchiveStore(root)
    files = []
    if body is not None:
        digest = hashlib.sha256(body).hexdigest()
        archive.put_blob(digest, len(body), (body,))
        files = [{"sha256": digest, "bytes": len(body), "source_url": "https://example.test/report.json"}]
    return archive.record_receipt(
        files=files, receipt={"event": "run-outcome", "outcome": outcome, "run_id": outcome, "family": "fixture"}
    )["object"]


@pytest.mark.parametrize("outcome", ["complete", "failed", "unchanged"])
def test_hosted_persistence_is_additive_and_idempotent(tmp_path, outcome):
    store = Store()
    body = b"publisher original"
    original = hosted._blob_key(hashlib.sha256(body).hexdigest())
    receipt = hosted.HOSTED_PREFIX + _commit(tmp_path, outcome, body if outcome != "failed" else None)
    if outcome == "complete":
        store.objects[original] = body  # already uploaded by the generation publisher
    assert hosted.persist_hosted_receipts(tmp_path, client=store, bucket="test") == 1
    assert store.writes == ([original, receipt] if outcome == "unchanged" else [receipt])
    assert json.loads(store.objects[receipt])["receipt"]["outcome"] == outcome
    assert hosted.persist_hosted_receipts(tmp_path, client=store, bucket="test") == 1
    assert store.writes.count(receipt) == 1 and not store.deletes


def test_corrupt_hosted_original_refuses_receipt_commit(tmp_path):
    store = Store()
    body = b"original"
    relative = _commit(tmp_path, "failed", body)
    key = hosted._blob_key(hashlib.sha256(body).hexdigest())
    store.objects[key] = b"corrupted"
    with pytest.raises(publication.PublicationError, match="differs from its pin"):
        hosted.persist_hosted_receipts(tmp_path, client=store, bucket="test")
    assert hosted.HOSTED_PREFIX + relative not in store.objects


@pytest.mark.parametrize("legacy_gzip", [False, True])
def test_hosted_catalog_ingestion_has_local_parity(tmp_path, monkeypatch, legacy_gzip):
    body = b'{"source": "original"}'
    raw = tmp_path / "runner"
    remote = Store()
    success = _commit(raw, "complete", body)
    refusal = _commit(raw, "failed")
    hosted.persist_hosted_receipts(raw, client=remote, bucket="test")
    local = tmp_path / "local"
    digest = hashlib.sha256(body).hexdigest()
    remote.reads.clear()
    if legacy_gzip:
        path = local / f"objects/sha256/{digest[:2]}/{digest}.gz"
        path.parent.mkdir(parents=True)
        path.write_bytes(gzip.compress(body))
        _commit(local, "complete", body)
    _commit(local, "local-only")
    monkeypatch.setattr(hosted, "hosted_connection", lambda: (remote, "test"))
    first = build(local, tmp_path / "with-hosted.sqlite", include_hosted_acquisitions=True)
    local_rebuild = build(local, tmp_path / "only-local.sqlite")

    assert first["generation"] == local_rebuild["generation"]
    receipts = {doc["receipt"]["outcome"]: doc["receipt_object"] for _, doc in iter_acquisition_receipts(local)}
    assert set(receipts) == {"complete", "failed", "local-only"}
    assert receipts["failed"] == refusal
    if not legacy_gzip:
        assert receipts["complete"] == success
    assert len(list(local.glob("objects/sha256/*/*"))) == 1
    assert remote.reads[hosted._blob_key(digest)] == (0 if legacy_gzip else 1)
    with connect(tmp_path / "with-hosted.sqlite", readonly=True) as db:
        assert db.execute("SELECT count(*) FROM objects").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM evidence WHERE kind='acquisition-receipt'").fetchone()[0] == 3
    assert hosted.import_hosted_receipts(local, client=remote, bucket="test") == 2
    assert len(list(iter_acquisition_receipts(local))) == 3


def test_refused_hosted_receipt_read_cannot_become_an_empty_rebuild(tmp_path, monkeypatch):
    local = tmp_path / "local"
    _commit(local, "local-only")
    remote = Store()
    runner = tmp_path / "runner"
    _commit(runner, "failed")
    hosted.persist_hosted_receipts(runner, client=remote, bucket="test")

    def refused(**_):
        raise error("AccessDenied")

    monkeypatch.setattr(remote, "get_object", refused)
    monkeypatch.setattr(hosted, "hosted_connection", lambda: (remote, "test"))
    output = tmp_path / "refused.sqlite"
    with pytest.raises(Exception, match="AccessDenied"):
        build(local, output, include_hosted_acquisitions=True)
    assert not output.exists()


def test_reusable_workflow_persists_receipts_on_failure_success_and_no_op():
    steps = yaml.safe_load(
        (Path(__file__).parents[1] / ".github/workflows/_rollup.yml").read_text()
    )["jobs"]["rollup"]["steps"]
    by_name = {step.get("name"): (i, step) for i, step in enumerate(steps)}
    durable_index, durable = by_name["Persist hosted acquisition receipts to R2"]
    audit_index, audit = by_name["Retain invocation and transferred-input audit"]
    prune_index, prune = by_name["Drop already-uploaded raw source originals before audit upload"]
    assert by_name["Run rollup"][0] < durable_index < prune_index < audit_index
    assert durable["if"] == "always()"  # not conditional on build success, skip_upload, or a retained generation
    assert "-m spicy_regs.source_catalog.hosted_receipts output/raw-source-originals" in durable["run"]
    assert audit["if"] == "always()" and prune["if"] == "success() && !inputs.skip_upload"
    assert {"output/source-evidence/", "output/raw-source-originals/"} <= set(audit["with"]["path"].splitlines())
