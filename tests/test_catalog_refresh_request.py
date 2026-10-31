"""Durable-owner notifications never run a refresh or change commit success."""

import json
from pathlib import Path

import pytest

from spicy_regs import catalog_refresh_request as request
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication


def configure(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    raw.mkdir()
    control = tmp_path / "controls/pending.json"
    monkeypatch.setenv(request.REQUEST_ENV, str(control))
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw))
    monkeypatch.delenv(publication.PUBLICATION_MIRROR, raising=False)
    return raw, control


def test_disabled_notification_has_no_filesystem_effect(tmp_path, monkeypatch):
    monkeypatch.delenv(request.REQUEST_ENV, raising=False)
    assert request.notify("publication-complete", {}) is None
    assert list(tmp_path.iterdir()) == []


def test_events_coalesce_as_whole_input_changes_and_newer_event_survives_ack(tmp_path, monkeypatch):
    raw, control = configure(tmp_path, monkeypatch)
    first = request.notify("acquisition-complete", {"family": "first"}, raw_root=raw)
    consumed = request.pending(control, raw)
    assert consumed["request"]["scope"] == "all-committed-inputs"
    second = request.notify("publication-complete", {"family": "second"})
    assert first["requestId"] != second["requestId"]
    assert request.acknowledge(control, consumed["sha256"], raw)["status"] == "pending-newer-request"
    current = request.pending(control, raw)
    assert current["request"]["lastEvent"]["reference"]["family"] == "second"
    assert request.acknowledge(control, current["sha256"], raw)["status"] == "acknowledged"
    assert request.pending(control, raw) is None


def test_capture_finish_notifies_after_retained_run_outcome(tmp_path, monkeypatch):
    raw, control = configure(tmp_path, monkeypatch)
    evidence = CaptureEvidence(tmp_path / "output", "test", archive_root=raw)
    evidence.finish()
    item = request.pending(control, raw)["request"]
    event = item["lastEvent"]
    assert event["reason"] == "acquisition-complete"
    assert Path(event["reference"]["runOutcome"]).is_file()
    assert json.loads(Path(event["reference"]["runOutcome"]).read_text())["outcome"] == "complete"
    assert evidence.catalog_refresh_request["status"] == "pending"


def test_successful_publication_notifies_without_serializing_control_metadata(tmp_path, monkeypatch):
    raw, control = configure(tmp_path, monkeypatch)
    index = {"families": {"test": {"artifactDigest": "sha256:" + "a" * 64}}}
    result = publication._published(index, json.dumps(index).encode(), "test", None)
    assert result == index
    assert result.catalog_refresh_request is not None
    assert result.catalog_refresh_request["status"] == "pending"
    assert request.pending(control, raw)["request"]["lastEvent"]["reason"] == "publication-complete"
    assert "catalog_refresh_request" not in json.dumps(result)


def test_notification_failure_preserves_durable_publication_success(tmp_path, monkeypatch, caplog):
    raw, control = configure(tmp_path, monkeypatch)
    monkeypatch.setenv(request.REQUEST_ENV, str(raw / "must-not-write.json"))
    index = {"families": {"test": {"artifactDigest": "sha256:" + "a" * 64}}}
    result = publication._published(index, json.dumps(index).encode(), "test", None)
    assert result == index
    assert result.catalog_refresh_request is not None
    assert result.catalog_refresh_request["status"] == "signal-failed"
    assert not (raw / "must-not-write.json").exists()
    assert "notification failed" in caplog.text


def test_control_refuses_symlinks_and_wrong_archive(tmp_path, monkeypatch):
    raw, control = configure(tmp_path, monkeypatch)
    request.notify("acquisition-complete", {}, raw_root=raw)
    with pytest.raises(ValueError, match="different raw archive"):
        request.pending(control, tmp_path / "other")
    alias = tmp_path / "alias"
    alias.symlink_to(control)
    with pytest.raises(ValueError, match="regular control path"):
        request.pending(alias, raw)


def test_later_acquisition_signal_preserves_pending_whole_publication_identity(tmp_path, monkeypatch):
    raw, control = configure(tmp_path, monkeypatch)
    digest = "sha256:" + "a" * 64
    request.notify("publication-complete", {"publicationSha256": digest})
    request.notify("acquisition-complete", {"family": "another"}, raw_root=raw)
    item = request.pending(control, raw)["request"]
    assert item["lastEvent"]["reason"] == "acquisition-complete"
    assert item["publicationSha256"] == digest
