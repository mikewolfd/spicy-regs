"""Parser-use declarations remain separate from acquisition observations and failed runs."""

from dataclasses import replace
import hashlib

import pytest
from spicy_docs.transport.captured import attach_capture

from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError, verify_evidence
from tests.test_source_evidence import capture, journal
from tests.source_catalog.test_input_proof import selected, source_row


def used(raw=b"original", location="https://source.test/body", kind="json-body"):
    return dict(location=location, sha256="sha256:" + hashlib.sha256(raw).hexdigest(),
                bytes=len(raw), asset_kind=kind)


def manifests(evidence):
    return [event["manifest"] for event in journal(evidence) if event["event"] == "selected-inputs"]


def test_repeated_partial_reads_emit_one_deduplicated_manifest_on_seal(tmp_path):
    e = CaptureEvidence(tmp_path, "fixture")
    e.capture(capture(b"original"), stage="parsed", native_id="publisher-native-id",
              source_version="edition-one", version_kind="publisher-version")
    e.declare_used_inputs([used()], scope_id="fixture:successful-source-files", complete=False)
    e.declare_used_inputs([used()], scope_id="fixture:successful-source-files", complete=False)
    assert not manifests(e)
    e.inputs()
    e.inputs()
    [manifest] = manifests(e)
    assert manifest["complete"] is False
    assert manifest["available"] == manifest["expected"] == []
    [row] = manifest["used"]
    assert (row["source_id"], row["source_version"], row["version_kind"]) == (
        "publisher-native-id", "edition-one", "publisher-version")
    assert (row["sha256"], row["bytes"], row["location"]) == (
        used()["sha256"], used()["bytes"], used()["location"])
    verify_evidence(e.artifact_dir)


def test_exact_central_raw_original_can_be_used_without_an_http_capture(tmp_path):
    original = tmp_path / "source.csv"
    original.write_bytes(b"original")
    e = CaptureEvidence(tmp_path / "output", "fixture")
    e.retain_file(original, stage="parsed-original", raw_source=True, native_id="source-csv")
    e.declare_used_inputs([used(location=str(original.resolve()))], scope_id="one-original")
    e.inputs()
    [manifest] = manifests(e)
    assert manifest["complete"] is True
    assert manifest["used"][0]["source_id"] == "source-csv"


@pytest.mark.parametrize("invalid", ["unknown", "derived", "refused", "refused-after-observation", "http-error"])
def test_unbacked_derived_or_refused_original_cannot_be_declared_used(tmp_path, invalid):
    e = CaptureEvidence(tmp_path / "output", "fixture")
    item = used()
    if invalid == "derived":
        path = tmp_path / "derived.csv"
        path.write_bytes(b"original")
        e.retain_file(path, stage="derived")
        item = used(location=str(path.resolve()))
    elif invalid.startswith("refused"):
        original = capture(b"original")
        if invalid == "refused-after-observation":
            e.capture(original, stage="transport-observed")
        error = ValueError("parse refused")
        attach_capture(error, original)
        e.refusal(error, stage="parser")
    elif invalid == "http-error":
        e.capture(replace(capture(b"original"), status_code=404), stage="http")
    e.declare_used_inputs([item], scope_id="one-original")
    with pytest.raises(SourceEvidenceError, match="lacks a successful recorded"):
        e.inputs()
    assert not manifests(e)
    e.finish(SourceEvidenceError("declaration refused"))
    assert not manifests(e)


@pytest.mark.parametrize("conflict", ["scope", "completeness", "repeat-complete", "direct-first", "direct-last"])
def test_incompatible_scopes_or_direct_selection_refuse_without_a_false_merge(tmp_path, conflict):
    e = CaptureEvidence(tmp_path, "fixture")
    if conflict == "direct-first":
        e.selected_inputs(selected([source_row()]))
        with pytest.raises(SourceEvidenceError, match="conflicting"):
            e.declare_used_inputs([used()], scope_id="one")
        return
    e.declare_used_inputs([used()], scope_id="one", complete=conflict == "repeat-complete")
    with pytest.raises(SourceEvidenceError, match="[Cc]onflicting|Incompatible"):
        if conflict == "direct-last":
            e.selected_inputs(selected([source_row()]))
        else:
            e.declare_used_inputs([used()], scope_id="two" if conflict == "scope" else "one",
                                  complete=conflict != "scope")
    assert not manifests(e)


def test_late_failure_retains_observation_without_success_selection(tmp_path):
    e = CaptureEvidence(tmp_path, "fixture")
    e.capture(capture(b"original"), stage="parser")
    e.declare_used_inputs([used()], scope_id="one-original")
    e.finish(ValueError("later table write failed"))
    assert not manifests(e)
    assert e.artifact.root["spec"]["outcome"] == "failed"
    assert any(event["event"] == "capture" for event in journal(e))


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
def test_restricted_refusal_marker_is_admitted_without_private_text(tmp_path, policy):
    e = CaptureEvidence(tmp_path, "fixture")
    scoped = e.for_source("publisher", policy, parser_version="parser/1", policy_decision_id="rights/1")
    error = ValueError("PRIVATE SOURCE TEXT")
    attach_capture(error, capture(b"PRIVATE SOURCE BODY"))
    scoped.refusal(error, stage="parser")
    e.finish(error)
    assert "PRIVATE SOURCE" not in (e.artifact_dir / "journal.jsonl").read_text()
    assert all(event.get("source_validation") == "refused" for event in journal(e)
               if event["event"] == "refusal")


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
def test_restricted_source_validation_cannot_smuggle_private_text(tmp_path, policy):
    e = CaptureEvidence(tmp_path, "fixture")
    scoped = e.for_source("publisher", policy, parser_version="parser/1", policy_decision_id="rights/1")
    with pytest.raises(SourceEvidenceError, match="fixed refusal marker"):
        scoped.capture(capture(b"body"), stage="parser", source_validation="PRIVATE SOURCE TEXT")
    assert "PRIVATE SOURCE" not in (e.artifact_dir / "journal.jsonl").read_text()


def test_untyped_recorded_version_uses_checksum_without_losing_native_identity(tmp_path):
    e = CaptureEvidence(tmp_path, "fixture")
    e.capture(capture(b"original"), stage="parsed", native_id="edition-2026", source_version="edition-2026")
    e.declare_used_inputs([used()], scope_id="one-original")
    e.inputs()
    [row] = manifests(e)[0]["used"]
    assert row["source_id"] == "edition-2026"
    assert (row["source_version"], row["version_kind"]) == (used()["sha256"], "sha256")


def test_refusal_at_another_location_does_not_reject_identical_successful_bytes(tmp_path):
    e = CaptureEvidence(tmp_path, "fixture")
    e.capture(capture(b"original"), stage="successful-parser")
    error = ValueError("different source refused")
    refused = replace(capture(b"original"), requested_url="https://other.test/body", resolved_url="https://other.test/body")
    attach_capture(error, refused)
    e.refusal(error, stage="parser")
    e.declare_used_inputs([used()], scope_id="one-original")
    e.inputs()
    assert manifests(e)[0]["used"][0]["location"] == "https://source.test/body"


def test_native_identity_cannot_claim_two_different_originals_in_one_version(tmp_path):
    e = CaptureEvidence(tmp_path, "fixture")
    for raw in (b"original", b"changed"):
        e.capture(capture(raw), stage="parsed", native_id="same", source_version="v1", version_kind="publisher-version")
    e.declare_used_inputs([used(), used(b"changed")], scope_id="one-original")
    with pytest.raises(SourceEvidenceError, match="conflicting bytes"):
        e.inputs()
    assert not manifests(e)


def test_used_locator_matches_the_same_credential_redaction_as_capture_receipt(tmp_path):
    e = CaptureEvidence(tmp_path, "fixture")
    e.credential = "private-api-key"
    url = "https://source.test/body?api_key=private-api-key"
    e.capture(replace(capture(b"original"), requested_url=url, resolved_url=url), stage="parser")
    e.declare_used_inputs([used(location=url)], scope_id="one-original")
    e.inputs()
    [row] = manifests(e)[0]["used"]
    assert row["location"] == e._safe(url)
    assert "private-api-key" not in (e.artifact_dir / "journal.jsonl").read_text()
