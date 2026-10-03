"""Bounded local-file retention through the existing evidence store."""

import hashlib
import json
import os
from pathlib import Path

import pytest
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError, _CHUNK, verify_evidence


def events(evidence):
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]


@pytest.mark.parametrize("size", [0, 1, 4 * _CHUNK + 17])
def test_retained_file_uses_bounded_reads_and_preserves_bytes_and_receipt(tmp_path, monkeypatch, size):
    path = tmp_path / "retained.bin"
    raw = (b"literal\r\n\x00" * (size // 10 + 1))[:size]
    path.write_bytes(raw)
    evidence = CaptureEvidence(tmp_path, "test")
    real_open = Path.open
    reads = []

    class Bounded:
        def __init__(self, stream):
            self.stream = stream

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def read(self, size=-1):
            assert 0 < size <= _CHUNK, "No whole-file or growing read"
            reads.append(size)
            return self.stream.read(size)

        def seek(self, *args):
            return self.stream.seek(*args)

        def fileno(self):
            return self.stream.fileno()

    def opening(selected, *args, **kwargs):
        stream = real_open(selected, *args, **kwargs)
        return Bounded(stream) if selected == path else stream

    monkeypatch.setattr(Path, "open", opening)
    result = evidence.retain_file(path, stage="local", source_scope="retained")
    expected = {"sha256": "sha256:" + hashlib.sha256(raw).hexdigest(), "byte_size": size}
    assert result == expected
    assert len(reads) >= 2 * (size // _CHUNK + 1)
    event = events(evidence)[-1]
    assert {key: event[key] for key in ("event", "stage", "name", "source_scope", "sha256", "byte_size")} == {
        "event": "retained-file",
        "stage": "local",
        "name": path.name,
        "source_scope": "retained",
        **expected,
    }
    assert event["evidence_policy"] == "full" and event["body_retained"] is True
    assert event["response_complete"] is True and len(event["capture_id"]) == 32
    assert event["blob_member"] == "blobs/sha256/" + expected["sha256"].removeprefix("sha256:")
    assert (evidence.store.root / "sha256" / expected["sha256"].removeprefix("sha256:")).read_bytes() == raw
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin).pin == artifact.pin


@pytest.mark.parametrize("mutation", ["grow", "truncate", "rewrite", "replace-path", "same-bytes"])
def test_changed_local_file_cannot_be_journaled_or_seal_complete(tmp_path, monkeypatch, mutation):
    path = tmp_path / "retained.bin"
    raw = b"original\n" * 100
    path.write_bytes(raw)
    evidence = CaptureEvidence(tmp_path, "test")
    put = evidence.store.put_blob

    def changing(*args):
        if mutation == "grow":
            path.write_bytes(raw + b"extra")
        elif mutation == "truncate":
            path.write_bytes(raw[:3])
        elif mutation == "rewrite":
            path.write_bytes(b"X" * len(raw))
        elif mutation == "same-bytes":
            path.write_bytes(raw)
        else:
            replacement = tmp_path / "replacement.bin"
            replacement.write_bytes(raw)
            replacement.replace(path)
        return put(*args)

    monkeypatch.setattr(evidence.store, "put_blob", changing)
    with pytest.raises(SourceEvidenceError):
        evidence.retain_file(path, stage="local")
    assert not any(row["event"] == "retained-file" for row in events(evidence))
    with pytest.raises(SourceEvidenceError, match="retention failed"):
        evidence.seal(outcome="build-complete")


def test_local_retention_refuses_credential_across_chunk_boundary(tmp_path):
    path = tmp_path / "retained.bin"
    key = "synthetic-local-file-credential"
    path.write_bytes(b"x" * (_CHUNK - 4) + key.encode() + b"suffix")
    evidence = CaptureEvidence(tmp_path, "test")
    evidence.credential = key
    with pytest.raises(CredentialRefusedError):
        evidence.retain_file(path, stage="local")
    assert list((evidence.store.root / "sha256").iterdir()) == []
    assert not any(row["event"] == "retained-file" for row in events(evidence))


def test_mutation_during_first_pass_refuses_before_store_write(tmp_path, monkeypatch):
    path = tmp_path / "retained.bin"
    path.write_bytes(b"original")
    evidence = CaptureEvidence(tmp_path, "test")
    original_stat = os.fstat
    changed = False

    def changing(descriptor):
        nonlocal changed
        result = original_stat(descriptor)
        if not changed:
            changed = True
            path.write_bytes(b"modified")
        return result

    monkeypatch.setattr(os, "fstat", changing)
    with pytest.raises(SourceEvidenceError, match="changed"):
        evidence.retain_file(path, stage="local")
    assert list((evidence.store.root / "sha256").iterdir()) == []
    assert not any(row["event"] == "retained-file" for row in events(evidence))


def test_unreadable_local_file_blocks_complete_sealing_and_sealed_file_is_not_opened(tmp_path):
    evidence = CaptureEvidence(tmp_path, "test")
    with pytest.raises(SourceEvidenceError, match="Cannot retain source file bytes"):
        evidence.retain_file(tmp_path / "missing", stage="local")
    with pytest.raises(SourceEvidenceError, match="retention failed"):
        evidence.seal(outcome="build-complete")
    sealed = CaptureEvidence(tmp_path, "test")
    sealed.seal(outcome="build-complete")
    with pytest.raises(SourceEvidenceError, match="sealed"):
        sealed.retain_file(tmp_path / "missing", stage="local")


@pytest.mark.parametrize("policy", ["full", "hash_only", "metadata_only"])
@pytest.mark.parametrize("native_input", [False, True])
def test_retained_bytes_use_admitted_local_receipts_and_respect_policy(tmp_path, policy, native_input):
    body = b"PRIVATE-RETAINED-INPUT-CONTENT"
    digest = hashlib.sha256(body).hexdigest()
    evidence = CaptureEvidence(tmp_path, "test")
    source = evidence.for_source("retained", policy, parser_version="fixture-v1", policy_decision_id="rights-v1")
    if native_input:
        from spicy_regs.transforms.native_legal_references import _retain

        _retain(source, body, role="native-xml", source_locator=body.decode())
    else:
        receipt = source.retain_bytes(body, stage="retained-input", role="selected-text", source_locator=body.decode())
        assert receipt["byte_size"] == len(body)
    row = events(evidence)[-1]
    assert row["event"] == "retained-file"
    assert row["response_complete"] is True
    assert row["body_retained"] is (policy == "full")
    assert len(row["capture_id"]) == 32 and row["capture_id"] not in digest
    artifact = evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin)
    public_bytes = [p.read_bytes() for p in evidence.directory.rglob("*") if p.is_file()]
    if policy == "full":
        assert (evidence.store.root / "sha256" / digest).read_bytes() == body
        assert row["source_locator"] == body.decode()
    else:
        assert "source_locator" not in row and "role" not in row
        assert all(body not in data for data in public_bytes)
    if policy == "metadata_only":
        assert "sha256" not in row
        assert all(digest.encode() not in data for data in public_bytes)
    with pytest.raises(SourceEvidenceError, match="sealed"):
        source.retain_bytes(body, stage="too-late")


@pytest.mark.parametrize("policy", ["full", "hash_only", "metadata_only"])
def test_retained_bytes_refuse_credentials_before_store_or_receipt(tmp_path, policy):
    evidence = CaptureEvidence(tmp_path, "test")
    evidence.credential = "retained-secret"
    source = evidence.for_source("retained", policy, parser_version="fixture-v1", policy_decision_id="rights-v1")
    with pytest.raises(CredentialRefusedError):
        source.retain_bytes(b"prefix-retained-secret-suffix", stage="local")
    assert not any(row["event"] == "retained-file" for row in events(evidence))
    assert list((evidence.store.root / "sha256").iterdir()) == []


def test_failed_retained_byte_write_cannot_seal_complete(tmp_path, monkeypatch):
    evidence = CaptureEvidence(tmp_path, "test")

    def fail(*_):
        raise OSError("storage unavailable")

    monkeypatch.setattr(evidence.store, "put_blob", fail)
    with pytest.raises(SourceEvidenceError, match="Cannot retain"):
        evidence.retain_bytes(b"private input", stage="local")
    assert not any(row["event"] == "retained-file" for row in events(evidence))
    with pytest.raises(SourceEvidenceError, match="cannot seal"):
        evidence.seal(outcome="build-complete")


def test_retained_bytes_receipt_identity_cannot_be_overridden_by_caller_fields(tmp_path):
    evidence = CaptureEvidence(tmp_path, "test")
    receipt = evidence.retain_bytes(
        b"bounded input",
        stage="local",
        sha256="sha256:" + "0" * 64,
        byte_size=999,
        body_retained=False,
        evidence_policy="metadata_only",
        capture_id="0" * 64,
        blob_member="unrelated",
        response_complete=False,
    )
    assert receipt["sha256"] == "sha256:" + hashlib.sha256(b"bounded input").hexdigest()
    assert receipt["byte_size"] == len(b"bounded input")
    assert receipt["body_retained"] is True and receipt["response_complete"] is True
    assert receipt["evidence_policy"] == "full" and len(receipt["capture_id"]) == 32
    artifact = evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin)
