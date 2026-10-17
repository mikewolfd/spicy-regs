"""Bounded local-file retention through the existing evidence store."""

import hashlib
import json
import os
from importlib.metadata import version
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
    assert {key: result[key] for key in expected} == expected
    assert result["response_complete"] is True and result["source_scope"] == "retained"
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


@pytest.mark.parametrize("encoding", ["identity", "gzip"])
def test_full_transport_replays_completed_retained_bytes_without_private_spool(tmp_path, monkeypatch, encoding):
    import gzip
    import httpx
    import spicy_regs.source_evidence as module
    from spicy_docs.transport.captured import CompletedCapture

    body = b"literal response bytes" * 1000
    raw = gzip.compress(body) if encoding == "gzip" else body
    evidence = CaptureEvidence(tmp_path, "test")
    context = evidence.for_source("fixture", "full", parser_version="fixture-v1", policy_decision_id="rights-v1")

    def no_spool(*args, **kwargs):
        raise AssertionError("Full retention must use the maintained private blob writer")

    monkeypatch.setattr(module.tempfile, "NamedTemporaryFile", no_spool)
    transport = context.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-encoding": encoding},
                stream=httpx.ByteStream(raw),
            )
        ),
        stage="native",
        max_bytes=len(raw),
    )
    with httpx.Client(transport=transport) as client:
        with client.stream("GET", "https://source.test/body") as response:
            assert "spicy_docs.completed_capture" not in response.extensions
            assert response.read() == body
            ref = response.extensions["spicy_docs.completed_capture"]
    assert isinstance(ref, CompletedCapture)
    assert ref.sha256 == "sha256:" + hashlib.sha256(raw).hexdigest()
    assert ref.byte_size == len(raw) and b"".join(ref.iter_bytes()) == raw
    assert (ref.publisher_id, ref.evidence_policy, ref.parser_version, ref.policy_decision_id) == (
        "fixture",
        "full",
        "fixture-v1",
        "rights-v1",
    )
    assert ref.capture_id == events(evidence)[-1]["capture_id"]
    evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir)


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
def test_restricted_transport_exposes_no_retained_reference(tmp_path, policy):
    import httpx

    evidence = CaptureEvidence(tmp_path, "test")
    context = evidence.for_source("fixture", policy, parser_version="fixture-v1", policy_decision_id="rights-v1")
    with httpx.Client(
        transport=context.transport(
            httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    stream=httpx.ByteStream(b"private body"),
                )
            ),
            stage="native",
            max_bytes=100,
        )
    ) as client:
        response = client.get("https://source.test/")
    assert response.content == b"private body"
    assert "spicy_docs.completed_capture" not in response.extensions
    assert not list((evidence.store.root / "sha256").iterdir())
    evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir)


@pytest.mark.parametrize("mutation", ["same-size", "growth", "replace", "missing"])
def test_completed_reference_renews_integrity_and_blocks_complete_seal(tmp_path, mutation):
    import httpx

    raw = b"original response"
    evidence = CaptureEvidence(tmp_path, "test")
    with httpx.Client(
        transport=evidence.transport(
            httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    stream=httpx.ByteStream(raw),
                )
            ),
            stage="native",
            max_bytes=100,
        )
    ) as client:
        response = client.get("https://source.test/")
    ref = response.extensions["spicy_docs.completed_capture"]
    path = evidence.store.root / "sha256" / ref.sha256[7:]
    if mutation == "same-size":
        path.write_bytes(b"x" * len(raw))
    elif mutation == "growth":
        path.write_bytes(raw + b"x" * 1_000_000)
    elif mutation == "replace":
        replacement = tmp_path / "replacement"
        replacement.write_bytes(b"x" * len(raw))
        replacement.replace(path)
    else:
        path.unlink()
    with pytest.raises(SourceEvidenceError, match="integrity"):
        b"".join(ref.iter_bytes())
    with pytest.raises(SourceEvidenceError, match="retention failed"):
        evidence.seal(outcome="build-complete")


def test_interrupted_capture_has_no_reference_receipt_blob_or_pending_file(tmp_path):
    import httpx

    class Interrupted(httpx.SyncByteStream):
        def __iter__(self):
            yield b"partial response"
            raise httpx.ReadError("constructed interruption")

    evidence = CaptureEvidence(tmp_path, "test")
    with httpx.Client(
        transport=evidence.transport(
            httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    stream=Interrupted(),
                )
            ),
            stage="native",
            max_bytes=100,
        )
    ) as client:
        with client.stream("GET", "https://source.test/") as response:
            with pytest.raises(httpx.ReadError):
                response.read()
            assert "spicy_docs.completed_capture" not in response.extensions
    assert not any(row["event"] == "capture" for row in events(evidence))
    assert events(evidence)[-1]["response_complete"] is False
    assert not list((evidence.store.root / "sha256").iterdir())
    assert not list((evidence.store.root / ".pending").iterdir())


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
def test_source_stream_error_under_restricted_policy_is_not_a_retention_failure(tmp_path, policy):
    import httpx

    class Broken(httpx.SyncByteStream):
        def __iter__(self):
            yield b"partial response"
            raise ValueError("constructed source error")

    evidence = CaptureEvidence(tmp_path, "test")
    source = evidence.for_source("fixture", policy, parser_version="fixture-v1", policy_decision_id="rights-v1")
    transport = source.transport(
        httpx.MockTransport(lambda _: httpx.Response(200, stream=Broken())), stage="native", max_bytes=100
    )
    with httpx.Client(transport=transport) as client:
        with client.stream("GET", "https://source.test/") as response:
            with pytest.raises(ValueError, match="constructed source error"):
                response.read()
    assert events(evidence)[-1]["event"] == "capture-incomplete"
    assert evidence.retention_failure is None
    verify_evidence(evidence.artifact_dir, expected_pin=evidence.seal(outcome="build-complete").pin)


def test_direct_writer_failure_is_sticky_and_never_exposes_capture(tmp_path, monkeypatch):
    import httpx

    evidence = CaptureEvidence(tmp_path, "test")

    def fail(*args, **kwargs):
        raise OSError("constructed disk failure")

    monkeypatch.setattr(evidence.store, "put_stream", fail)
    with httpx.Client(
        transport=evidence.transport(
            httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    stream=httpx.ByteStream(b"exact response"),
                )
            ),
            stage="native",
            max_bytes=100,
        )
    ) as client:
        with client.stream("GET", "https://source.test/") as response:
            with pytest.raises(SourceEvidenceError, match="Cannot retain"):
                response.read()
            assert "spicy_docs.completed_capture" not in response.extensions
    assert not any(row["event"] == "capture" for row in events(evidence))
    with pytest.raises(SourceEvidenceError, match="retention failed"):
        evidence.seal(outcome="build-complete")


def test_malformed_gzip_has_raw_reference_without_successful_decoding(tmp_path):
    import httpx

    raw = b"not actually gzip"
    evidence = CaptureEvidence(tmp_path, "test")
    with httpx.Client(
        transport=evidence.transport(
            httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    headers={"content-encoding": "gzip"},
                    stream=httpx.ByteStream(raw),
                )
            ),
            stage="native",
            max_bytes=100,
        )
    ) as client:
        with client.stream("GET", "https://source.test/") as response:
            with pytest.raises(httpx.DecodingError):
                response.read()
            ref = response.extensions["spicy_docs.completed_capture"]
    assert b"".join(ref.iter_bytes()) == raw
    assert events(evidence)[-1]["response_complete"] is True


def test_completed_capture_works_with_the_real_download_owner(tmp_path, monkeypatch):
    import httpx
    from spicy_docs.transport.download import BoundedAcquirer
    import spicy_regs.source_evidence as module

    raw = b"PK\x03\x04literal source bytes" * 100
    evidence = CaptureEvidence(tmp_path, "test")
    monkeypatch.setattr(module.tempfile, "NamedTemporaryFile", lambda *a, **k: pytest.fail("duplicate spool"))
    with BoundedAcquirer(
        validate_url=lambda url: url,
        min_interval=0,
        transport=evidence.transport(
            httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    stream=httpx.ByteStream(raw),
                )
            ),
            stage="native",
            max_bytes=len(raw),
        ),
    ) as owner:
        result = owner.download("https://source.test/asset.zip", store=tmp_path / "owner", max_bytes=len(raw))
    assert result["sha256"] == events(evidence)[-1]["sha256"]
    assert (tmp_path / "owner" / result["blob_path"]).read_bytes() == raw
    assert (evidence.store.root / "sha256" / result["sha256"][7:]).read_bytes() == raw


SPOOFED = dict(
    parser_version="spoofed",
    policy_decision_id="spoofed",
    publisher_id="spoofed",
    recorded_at="1999-01-01T00:00:00+00:00",
    event="capture",
    request_body={"sha256": "sha256:" + "0" * 64, "byte_size": 1, "body_retained": True, "blob_member": "unrelated"},
)


@pytest.mark.parametrize("bound_context", [False, True])
def test_retained_file_has_one_receipt_shape_and_caller_cannot_override_identity(tmp_path, bound_context):
    body = b"current retained input"
    path = tmp_path / "current.bin"
    path.write_bytes(body)
    evidence = CaptureEvidence(tmp_path, "test")
    source = (
        evidence.for_source("fixture", "full", parser_version="fixture-v1", policy_decision_id="rights-v1")
        if bound_context
        else evidence
    )
    identity = (
        ("fixture", "fixture-v1", "rights-v1")
        if bound_context
        else (None, version("spicy-docs"), "existing-full-default")
    )
    receipt = source.retain_file(
        path,
        stage="local",
        name="spoofed.bin",
        sha256="sha256:" + "0" * 64,
        byte_size=999,
        body_retained=False,
        response_complete=False,
        evidence_policy="metadata_only",
        capture_id="0" * 64,
        blob_member="unrelated",
        detail="current source context",
        **SPOOFED,
    )
    assert receipt["sha256"] == "sha256:" + hashlib.sha256(body).hexdigest()
    assert receipt["byte_size"] == len(body)
    assert receipt["name"] == path.name and receipt["detail"] == "current source context"
    assert receipt["body_retained"] is True and receipt["response_complete"] is True
    assert receipt["evidence_policy"] == "full" and len(receipt["capture_id"]) == 32
    assert receipt["blob_member"] == "blobs/sha256/" + receipt["sha256"][7:]
    received = source.retain_bytes(b"other retained input", stage="local", detail="bytes context", **SPOOFED)
    assert received["detail"] == "bytes context" and received["response_complete"] is True
    recorded = events(evidence)[-2:]
    assert all(recorded[0][key] == value for key, value in receipt.items())
    assert all(recorded[1][key] == value for key, value in received.items())
    for row in recorded:
        assert row["event"] == "retained-file" and not row["recorded_at"].startswith("1999")
        assert "request_body" not in row
        assert (row["publisher_id"], row["parser_version"], row["policy_decision_id"]) == identity
    evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir)
