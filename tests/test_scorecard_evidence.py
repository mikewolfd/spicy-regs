"""Policy boundaries for source captures, refused bytes and public evidence."""

from dataclasses import FrozenInstanceError, replace
import hashlib
import json
from pathlib import Path

import httpx
import pytest
from spicy_docs.reading.refusals import RefusedResponse, attach_refused_response
from spicy_docs.transport.captured import attach_capture
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.generation_audit import _Run, _body_shapes
from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError, _verify_receipts, verify_evidence
from tests.test_source_evidence import capture, journal

PAYLOAD = b"PRIVATE-SCORECARD-SOURCE-SENTINEL"
REQUEST = b"PRIVATE-REQUEST-SENTINEL"


def context(evidence, policy, publisher="sample"):
    return evidence.for_source(publisher, policy, parser_version="test-parser-v1", policy_decision_id="test-rights-v1")


def assert_private(output, *values):
    for path in output.rglob("*"):
        if path.is_file():
            for value in values:
                assert value not in path.read_bytes(), path


@pytest.mark.parametrize("policy", ["full", "hash_only", "metadata_only"])
def test_capture_policy_applies_to_request_response_and_identity(tmp_path, policy):
    evidence = CaptureEvidence(tmp_path / "output", "scorecards")
    source = context(evidence, policy)
    receipt = source.capture(capture(PAYLOAD, request_body=REQUEST), stage="parse")
    assert receipt["body_retained"] is (policy == "full")
    assert receipt["request_body"]["body_retained"] is (policy == "full")
    assert receipt["byte_size"] == len(PAYLOAD)
    assert len(receipt["capture_id"]) == 32
    evidence.finish()
    verify_evidence(evidence.artifact_dir)
    if policy != "full":
        assert_private(evidence.directory, PAYLOAD, REQUEST)
        assert "blob_member" not in receipt
    if policy == "metadata_only":
        assert "sha256" not in receipt and "sha256" not in receipt["request_body"]
        assert_private(evidence.directory, hashlib.sha256(PAYLOAD).hexdigest().encode())
    else:
        assert receipt["sha256"] == "sha256:" + hashlib.sha256(PAYLOAD).hexdigest()


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
@pytest.mark.parametrize("attached", ["capture", "legacy", "none"])
def test_refusal_and_final_failure_never_republish_payload_or_exception(tmp_path, policy, attached):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    source = context(evidence, policy)
    error = ValueError(PAYLOAD.decode())
    if attached == "capture":
        attach_capture(error, capture(PAYLOAD, request_body=REQUEST))
    elif attached == "legacy":
        attach_refused_response(error, RefusedResponse("https://source.test", "parse", PAYLOAD, "text/plain"))
    source.refusal(error, stage="parse")
    evidence.finish(error)
    assert_private(evidence.directory, PAYLOAD, REQUEST)
    assert all(e.get("message") is None for e in journal(evidence) if e["event"] == "refusal")


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
def test_file_and_stream_payloads_never_enter_upload_roots(tmp_path, monkeypatch, policy):
    output = tmp_path / "output"
    evidence = CaptureEvidence(output, "scorecards")
    source = context(evidence, policy)
    private = tmp_path / "private-source"
    private.write_bytes(PAYLOAD)
    source.retain_file(private, stage="file", ignored_source_excerpt=PAYLOAD.decode())
    import spicy_regs.source_evidence as module

    original = module.tempfile.NamedTemporaryFile
    spools = []

    def spool(*args, **kwargs):
        result = original(*args, **kwargs)
        spools.append(Path(result.name))
        assert not Path(result.name).is_relative_to(output)
        return result

    monkeypatch.setattr(module.tempfile, "NamedTemporaryFile", spool)
    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=httpx.ByteStream(PAYLOAD)))
    with httpx.Client(transport=source.transport(transport, stage="http", max_bytes=1000)) as client:
        assert client.post("https://source.test", content=REQUEST).content == PAYLOAD
    evidence.finish()
    verify_evidence(evidence.artifact_dir)
    assert spools and all(not path.exists() for path in spools)
    assert_private(output, PAYLOAD, REQUEST, b"private-source")
    if policy == "metadata_only":
        assert_private(output, hashlib.sha256(PAYLOAD).hexdigest().encode())


def test_mixed_contexts_are_immutable_and_one_artifact_retains_only_full(tmp_path):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    sources = [context(evidence, p, p) for p in ("full", "hash_only", "metadata_only")]
    for source in sources:
        source.capture(capture((source.policy + "-body").encode()), stage="http")
    with pytest.raises(FrozenInstanceError):
        sources[1].policy = "full"
    artifact = evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin)
    blobs = list((evidence.artifact_dir / "blobs/sha256").iterdir())
    assert [p.read_bytes() for p in blobs] == [b"full-body"]
    with pytest.raises(SourceEvidenceError, match="sealed"):
        sources[1].capture(capture(PAYLOAD), stage="late")


def test_contradictory_context_poison_cannot_be_swallowed(tmp_path):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    context(evidence, "hash_only")
    with pytest.raises(SourceEvidenceError, match="Contradictory"):
        context(evidence, "full")
    with pytest.raises(SourceEvidenceError, match="cannot seal"):
        evidence.seal(outcome="build-complete")


@pytest.mark.parametrize("policy", ["full", "hash_only", "metadata_only"])
def test_credential_refusal_aborts_without_retention(tmp_path, policy):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    evidence.credential = "secret-token"
    source = context(evidence, policy)
    for raw in [capture(b"secret-token"), replace(capture(PAYLOAD), status_code=403)]:
        with pytest.raises(CredentialRefusedError):
            source.capture(raw, stage="http")
    evidence.finish(CredentialRefusedError("secret-token"))
    assert_private(evidence.directory, PAYLOAD, b"secret-token")


@pytest.mark.parametrize("fault", ["orphan", "policy", "size", "digest", "unknown_field", "http_metadata"])
def test_semantic_admission_rejects_structurally_valid_false_receipts(tmp_path, fault):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    source = context(evidence, "hash_only")
    source.capture(capture(PAYLOAD), stage="http")
    rows = journal(evidence)
    event = rows[-1]
    if fault == "orphan":
        evidence._blob(PAYLOAD)
    elif fault == "policy":
        event["body_retained"] = True
    elif fault == "size":
        event["byte_size"] = -1
    elif fault == "digest":
        event["sha256"] = "not-a-digest"
    elif fault == "http_metadata":
        event["status_code"] = "200"
    else:
        event["source_excerpt"] = PAYLOAD.decode()
    (evidence.artifact_dir / "journal.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(SourceEvidenceError):
        evidence.seal(outcome="build-complete")


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
@pytest.mark.parametrize("typed", [False, True])
def test_nonretaining_refusal_cannot_hide_payload_in_untyped_response(tmp_path, policy, typed):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    source = context(evidence, policy)
    source.refusal(ValueError("Source rejected"), stage="parse")
    rows = journal(evidence)
    rows[-1]["response"] = {"source_excerpt": PAYLOAD.decode()}
    if typed:
        rows[-1]["response"].update(event="capture", body_retained=False, byte_size=len(PAYLOAD))
        if policy == "hash_only":
            rows[-1]["response"]["sha256"] = "sha256:" + hashlib.sha256(PAYLOAD).hexdigest()
    (evidence.artifact_dir / "journal.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(SourceEvidenceError):
        evidence.seal(outcome="build-complete")


def test_unknown_version_refuses_legacy_missing_version_keeps_original_semantics(tmp_path):
    from rulespec_artifacts import LocalMemberSource

    evidence = CaptureEvidence(tmp_path, "scorecards")
    context(evidence, "metadata_only").capture(capture(PAYLOAD), stage="http")
    artifact = evidence.seal(outcome="build-complete")
    root = dict(artifact.root)
    root["spec"] = {**root["spec"], "evidence_version": 999}
    with pytest.raises(SourceEvidenceError, match="version"):
        _verify_receipts(LocalMemberSource(evidence.artifact_dir), replace(artifact, root=root))
    del root["spec"]["evidence_version"]
    _verify_receipts(LocalMemberSource(evidence.artifact_dir), replace(artifact, root=root))


def test_audit_states_body_checks_unavailable_by_policy(tmp_path):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    for policy in ("hash_only", "metadata_only"):
        context(evidence, policy, policy).capture(capture(PAYLOAD), stage="http")
    run = _Run(samples=5, secrets={})
    result = _body_shapes(journal(evidence), {"journal.jsonl"}, run)
    assert result["not_assessed_by_policy"] == {"hash_only": 1, "metadata_only": 1}
    assert result["expected_to_observed"] == {}
    assert any("not assessed" in text for text in run.limits)


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
@pytest.mark.parametrize("failure", ["stream", "overflow"])
def test_incomplete_streams_have_no_complete_body_hash_or_payload(tmp_path, policy, failure):
    evidence = CaptureEvidence(tmp_path, "scorecards")
    source = context(evidence, policy)

    class Interrupted(httpx.SyncByteStream):
        def __iter__(self):
            yield PAYLOAD
            if failure == "stream":
                raise httpx.ReadError(PAYLOAD.decode())
            yield PAYLOAD

    transport = httpx.MockTransport(lambda request: httpx.Response(200, stream=Interrupted()))
    try:
        with httpx.Client(transport=source.transport(transport, stage="http", max_bytes=len(PAYLOAD))) as client:
            client.get("https://source.test")
    except (httpx.ReadError, SourceEvidenceError) as error:
        evidence.finish(error)
    else:
        pytest.fail("Interrupted acquisition must refuse")
    records = [r for r in journal(evidence) if r["event"] == "capture-incomplete"]
    assert len(records) == 1 and records[0]["response_complete"] is False
    assert "sha256" not in records[0] and "blob_member" not in records[0]
    assert_private(evidence.directory, PAYLOAD, hashlib.sha256(PAYLOAD).hexdigest().encode())
