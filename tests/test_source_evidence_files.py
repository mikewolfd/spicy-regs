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
    assert {key: value for key, value in event.items() if key != "recorded_at"} == {
        "event": "retained-file", "stage": "local", "name": path.name, "source_scope": "retained", **expected,
    }
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
