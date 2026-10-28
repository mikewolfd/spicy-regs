"""Central originals stay replayable after run outputs disappear; metadata remains local."""

import gzip
import hashlib
import shutil

import httpx
import pytest
from rulespec_artifacts import admit_artifact
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts

from spicy_regs.source_evidence import CaptureEvidence, EvidenceMemberSource, SourceEvidenceError, verify_evidence
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_source_evidence import capture


def receipts(root):
    return [document for _, document in iter_acquisition_receipts(root, verify_objects=True)]


def test_repeated_captures_share_one_original_and_survive_output_removal(tmp_path):
    output = tmp_path / "run-output"
    first, second = [CaptureEvidence(output, "ecfr") for _ in range(2)]
    raw = b"publisher\r\nbytes\x00"
    for evidence in (first, second):
        evidence.capture(capture(raw), stage="title")
        evidence.finish()
        assert not list((evidence.artifact_dir / "blobs").rglob("?" * 64))
        verify_evidence(evidence.artifact_dir)
    archive = first.store.root
    assert len(list((archive / "objects").rglob("?" * 64))) == 1
    shutil.rmtree(output)
    stored = receipts(archive)
    captures = [item for item in stored if item["receipt"]["event"] == "capture"]
    assert len(captures) == 2
    assert {item["files"][0]["family"] for item in captures} == {"ecfr"}
    assert all(item["files"][0]["observed_at"] == "2026-09-19T00:00:00Z" for item in captures)
    assert "period" not in captures[0]["files"][0], "acquisition time is not covered time"
    assert {item["receipt"]["outcome"] for item in stored if item["receipt"]["event"] == "run-outcome"} == {"complete"}
    with CentralRawArchiveStore(archive, create=False).open(hashlib.sha256(raw).hexdigest()) as stream:
        assert stream.read() == raw
    assert not any(path.is_symlink() for path in archive.rglob("*"))


def test_existing_compressed_original_supports_artifact_and_publication(tmp_path):
    evidence = CaptureEvidence(tmp_path / "run", "ecfr")
    raw = b"unchanged native original\n" * 100
    digest = hashlib.sha256(raw).hexdigest()
    stored = evidence.store.root / "objects/sha256" / digest[:2] / (digest + ".gz")
    stored.parent.mkdir(parents=True)
    stored.write_bytes(gzip.compress(raw))
    evidence.capture(capture(raw), stage="held-original")
    artifact = evidence.seal(outcome="build-complete")
    assert not stored.with_suffix("").exists(), "reuse compressed original without a second copy"
    assert not list((evidence.artifact_dir / "blobs").rglob("?" * 64))
    remote = Store()
    pub._publish_evidence(remote, "test", evidence.artifact_dir, artifact)
    assert remote.objects[f"source-evidence/blobs/sha256/{digest}"] == raw
    pub._publish_evidence(remote, "test", evidence.artifact_dir, artifact)
    assert remote.writes.count(f"source-evidence/blobs/sha256/{digest}") == 1


def test_local_derived_inputs_and_lineage_stay_outside_raw_archive(tmp_path):
    evidence = CaptureEvidence(tmp_path / "run", "derived")
    derived = b"PAR1-derived-table"
    evidence.retain_bytes(derived, stage="conversion")
    evidence.finish()
    assert not (evidence.store.root / "objects").exists()
    assert (evidence.local_store.root / "sha256" / hashlib.sha256(derived).hexdigest()).read_bytes() == derived
    assert all(item["files"] == [] for item in receipts(evidence.store.root))
    verify_evidence(evidence.artifact_dir)


def test_explicit_original_file_keeps_provenance_in_central_receipt(tmp_path):
    source = tmp_path / "original.xml"
    source.write_bytes(b"<native/>")
    evidence = CaptureEvidence(tmp_path / "run", "ecfr")
    evidence.retain_file(
        source, raw_source=True, stage="reuse", source_url="https://source.test/title.xml",
        dataset_id="family:ecfr", native_id="title-1", covered_period="requested-as-of:2026-09-19",
        source_version="source-issue-2026-09-19", sha256="sha256:" + "0" * 64,
    )
    evidence.finish()
    [record] = [item for item in receipts(evidence.store.root) if item["receipt"]["event"] == "retained-file"]
    assert record["receipt"]["name"] == "original.xml"
    assert record["files"][0]["original_location"] == str(source.resolve())
    assert record["files"][0]["source_url"] == "https://source.test/title.xml"
    file = record["files"][0]
    assert file["observed_at"] is None and file["acquired_at"] is None
    assert file["sha256"] == hashlib.sha256(source.read_bytes()).hexdigest()
    assert file["dataset_id"] == "family:ecfr" and file["native_id"] == "title-1"
    assert file["covered_period"] == "requested-as-of:2026-09-19"
    assert file["source_version"] == "source-issue-2026-09-19"
    assert not list((evidence.artifact_dir / "blobs").rglob("?" * 64))


def test_archive_and_evidence_can_be_moved_without_original_worktree(tmp_path, monkeypatch):
    evidence = CaptureEvidence(tmp_path / "output", "ecfr")
    evidence.capture(capture(), stage="source")
    artifact = evidence.seal(outcome="build-complete")
    copied = tmp_path / "mirror"
    shutil.copytree(evidence.store.root, copied)
    new_output = tmp_path / "moved-output"
    shutil.move(str(evidence.directory), new_output)
    shutil.rmtree(evidence.store.root)
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(copied))
    assert verify_evidence(new_output / "artifact", expected_pin=artifact.pin).pin == artifact.pin
    assert receipts(copied)


def test_failed_receipt_write_cannot_seal_success_and_failure_is_retained(tmp_path, monkeypatch):
    evidence = CaptureEvidence(tmp_path / "run", "ecfr")
    original = evidence.store.record_receipt

    def fail(**kwargs):
        raise OSError("receipt storage unavailable")

    monkeypatch.setattr(evidence.store, "record_receipt", fail)
    with pytest.raises(SourceEvidenceError):
        evidence.capture(capture(), stage="capture")
    with pytest.raises(SourceEvidenceError, match="cannot seal"):
        evidence.seal(outcome="build-complete")
    monkeypatch.setattr(evidence.store, "record_receipt", original)
    with pytest.raises(SourceEvidenceError, match="Source retention failed"):
        evidence.finish()
    assert admit_artifact(EvidenceMemberSource(evidence.artifact_dir)).root["spec"]["outcome"] == "failed"
    stored = receipts(evidence.store.root)
    assert any(item["receipt"].get("outcome") == "failed" for item in stored)
    [captured] = [item for item in stored if item["receipt"]["event"] == "capture"]
    assert captured["files"][0]["source_url"] == "https://source.test/body"
    assert captured["files"][0]["sha256"] == capture().sha256.removeprefix("sha256:")


def test_interrupted_response_retains_receipt_without_partial_original(tmp_path):
    evidence = CaptureEvidence(tmp_path / "run", "ecfr")

    class Interrupted(httpx.SyncByteStream):
        def __iter__(self):
            yield b"partial"
            raise RuntimeError("connection lost")

    transport = httpx.MockTransport(lambda _: httpx.Response(200, stream=Interrupted()))
    with httpx.Client(transport=evidence.transport(transport, stage="source", max_bytes=100)) as client:
        with pytest.raises(RuntimeError) as caught:
            client.get("https://source.test/title.xml")
    evidence.finish(caught.value)
    stored = receipts(evidence.store.root)
    assert not (evidence.store.root / "objects").exists()
    [incomplete] = [item["receipt"] for item in stored if item["receipt"]["event"] == "capture-incomplete"]
    assert incomplete["bytes_received"] == 7 and incomplete["response_complete"] is False
    assert "sha256" not in incomplete
    assert all(item["files"] == [] for item in stored)


def test_failed_admission_still_commits_failure_receipt(tmp_path):
    evidence = CaptureEvidence(tmp_path / "run", "ecfr")
    evidence._blob(b"orphan-before-receipt")
    with pytest.raises(SourceEvidenceError, match="Unclaimed source body"):
        evidence.finish()
    [outcome] = [item["receipt"] for item in receipts(evidence.store.root)
                 if item["receipt"]["event"] == "run-outcome"]
    assert outcome["outcome"] == "failed" and outcome["artifact"] is None


def test_explicit_archive_root_overrides_default_during_capture_and_admission(tmp_path):
    explicit = tmp_path / "explicit-archive"
    evidence = CaptureEvidence(tmp_path / "output", "ecfr", archive_root=explicit)
    evidence.capture(capture(), stage="source")
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, archive_root=explicit).pin == artifact.pin
    assert receipts(explicit)


def test_local_journal_failure_does_not_orphan_central_original(tmp_path, monkeypatch):
    from pathlib import Path

    evidence = CaptureEvidence(tmp_path / "run", "ecfr")
    original = Path.open

    def fail_journal(path, *args, **kwargs):
        if path == evidence.artifact_dir / "journal.jsonl" and args == ("ab",):
            raise OSError("run output unavailable")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", fail_journal)
    with pytest.raises(SourceEvidenceError, match="journal"):
        evidence.capture(capture(), stage="source")
    [captured] = [item for item in receipts(evidence.store.root) if item["receipt"]["event"] == "capture"]
    assert captured["files"][0]["source_url"] == "https://source.test/body"
    assert captured["files"][0]["sha256"] == capture().sha256.removeprefix("sha256:")


@pytest.mark.parametrize("attached", [False, True])
def test_refused_source_is_retained_without_acquired_dataset_coverage(tmp_path, attached):
    from spicy_docs.reading.refusals import RefusedResponse, attach_refused_response
    from spicy_docs.transport.captured import attach_capture
    from spicy_regs.source_catalog.coverage import coverage
    from spicy_regs.source_catalog.importer import build
    from spicy_regs.source_catalog.storage import connect

    evidence = CaptureEvidence(tmp_path / "run", "gao-reports")
    error = ValueError("Source content failed validation")
    if attached:
        attach_capture(error, capture(b"invalid source document"))
    else:
        attach_refused_response(error, RefusedResponse(
            request_key="https://www.gao.gov/report", stage="read",
            response_bytes=b"invalid source document", media_type="text/plain",
        ))
    evidence.finish(error)
    retained = [file for record in receipts(evidence.store.root) for file in record["files"]]
    assert retained and all(file["source_validation"] == "refused" for file in retained)
    assert {file["http_status"] for file in retained} == {200 if attached else None}
    assert {file["response_complete"] for file in retained} == {True if attached else None}
    index = tmp_path / "catalog.sqlite"
    build(evidence.store.root, index)
    with connect(index) as db:
        assert {row[0] for row in db.execute("SELECT status FROM files")} == {"refused-source"}
        assert coverage(db, "family:gao-reports")["collections"][0]["stages"]["acquired"]["measurements"] == []


def test_refusal_qualifies_earlier_capture_without_rejecting_another_runs_same_bytes(tmp_path):
    from spicy_docs.transport.captured import attach_capture
    from spicy_regs.source_catalog.coverage import coverage
    from spicy_regs.source_catalog.importer import build
    from spicy_regs.source_catalog.storage import connect

    captured = capture(b"publisher bytes used in different contexts")
    refused = CaptureEvidence(tmp_path / "refused", "gao-reports")
    refused.capture(captured, stage="before-parse")
    error = ValueError("Not valid for this acquisition's requested source")
    attach_capture(error, captured)
    refused.finish(error)
    accepted = CaptureEvidence(tmp_path / "accepted", "gao-reports")
    accepted.capture(captured, stage="source")
    accepted.finish()
    index = tmp_path / "catalog.sqlite"
    build(accepted.store.root, index)
    with connect(index) as db:
        statuses = [row[0] for row in db.execute("SELECT status FROM files")]
        assert statuses.count("refused-source") == 2 and statuses.count("held") == 1
        measurements = coverage(db, "family:gao-reports")["collections"][0]["stages"]["acquired"]["measurements"]
        assert sum(item["count"] for item in measurements) == 1
