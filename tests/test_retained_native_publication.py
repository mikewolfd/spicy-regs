"""Inert accepted-output fixtures check packaging, not native source readers."""

import hashlib
import json
import os
from pathlib import Path

import pytest

from spicy_regs import retained_native_publication as packaging
from spicy_regs.etl_receipts import ReceiptContext, combine_receipts, write_dataset
from spicy_regs.sources import publication
from spicy_regs.sources.retained_native_collections import POLICIES, TABLES, VERSION


def digest(path):
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def pin(path):
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": digest(path)}


def fixture(tmp_path):
    candidate = tmp_path / "accepted"
    candidate.mkdir()
    generation = "fixture-original-receipt-generation"
    outputs, members, receipts = {}, [], []
    for table in TABLES["sec-comments"]:
        policy = POLICIES[table]
        row = dict.fromkeys(policy.subject_schema.names)
        row[policy.identity_fields[0]] = table + "-fixture"
        context = ReceiptContext(
            generation,
            row[policy.identity_fields[0]],
            VERSION,
            [
                {
                    "source_id": "fixture",
                    "source_uri": "https://example.test/source",
                    "sha256": "sha256:" + "0" * 64,
                    "locator": "fixture",
                }
            ],
        )
        subject, receipt = write_dataset([(row, context)], candidate / table, policy, validation_backend="reference")
        info = {"path": str(subject.relative_to(candidate)), "rows": 1, "sha256": digest(subject)}
        outputs[table] = info
        members.append(
            {"table": table, **info, "absolute_path": str(subject), "current_stat_bytes": subject.stat().st_size}
        )
        receipts.append(receipt)
    combined = combine_receipts(receipts, candidate / "etl_receipts.parquet")
    build_path = candidate / "build.json"
    build_path.write_text(
        json.dumps({"generation": generation, "outputs": outputs, "receipts_sha256": digest(combined)})
    )
    selection = candidate / "selection.json"
    selection.write_text(json.dumps({"sources": [{"kind": "sec-comments"}]}))
    acceptance = tmp_path / "root-acceptance.json"
    acceptance.write_text(json.dumps({"state": "inert-fixture-accepted", "commit": "a" * 40}))
    return {
        "local_content_accepted": True,
        "coordinator_commit": "a" * 40,
        "coordinator_acceptance": pin(acceptance),
        "build_metadata": pin(build_path),
        "selection_metadata": pin(selection),
        "candidate_path": str(candidate),
        "accepted_selected_generation": generation,
        "members": members,
        "combined_receipts": {
            "path": str(combined),
            "sha256": digest(combined),
            "current_stat_bytes": combined.stat().st_size,
        },
        "accepted_scope_limits": ["Inert selected outputs only; no source capture or coverage proof"],
    }


def test_preview_keeps_payload_inodes_and_original_receipt_generation(tmp_path, monkeypatch):
    collection = fixture(tmp_path)
    monkeypatch.setattr(packaging, "installed_policies", lambda: {})
    output = tmp_path / "wrapper"
    result = packaging.prepare_selected(
        collection, family="retained-sec-comments", read_snapshot=publication.empty_index(), output=output, preview=True
    )
    assert result["source_generation"] == collection["accepted_selected_generation"]
    assert result["payload_copies"] == result["source_reads"] == 0
    artifact = packaging.generations.verify_generation(output / "generation")
    assert artifact.root["spec"]["publicationStatus"] == "local-partial"
    assert artifact.root["spec"]["etlReceipts"]["generationId"] == collection["accepted_selected_generation"]
    for item in collection["members"]:
        original = Path(item["absolute_path"])
        alias = output / "generation" / (item["table"] + ".parquet")
        assert original.stat().st_ino == alias.stat().st_ino
        assert digest(original) == item["sha256"]
    journal = output / result["evidence_directory"] / "journal.jsonl"
    assert str(tmp_path) not in journal.read_text()
    events = [json.loads(line) for line in journal.read_text().splitlines()]
    package_event = next(event for event in events if event["event"] == "accepted-native-packaging")
    assert package_event["new_capture_claim"] is False and package_event["new_asset_acquisition"] is False
    assert package_event["accepted_proof_controls"] == {
        key: {"bytes": collection[key]["bytes"], "sha256": collection[key]["sha256"]}
        for key in ("coordinator_acceptance", "build_metadata", "selection_metadata")
    }


def test_recorded_code_commit_is_preserved_when_supplement_commit_is_null(tmp_path, monkeypatch):
    collection = fixture(tmp_path)
    acceptance = Path(collection["coordinator_acceptance"]["path"])
    acceptance.write_text(json.dumps({"state": "inert-fixture-accepted", "code_commit": "a" * 40}))
    collection["coordinator_acceptance"] = pin(acceptance)
    collection["coordinator_commit"] = None
    monkeypatch.setattr(packaging, "installed_policies", lambda: {})
    output = tmp_path / "wrapper"
    result = packaging.prepare_selected(
        collection, family="retained-sec-comments", read_snapshot=publication.empty_index(), output=output, preview=True
    )
    events = [
        json.loads(line) for line in (output / result["evidence_directory"] / "journal.jsonl").read_text().splitlines()
    ]
    assert (
        next(event for event in events if event["event"] == "accepted-native-packaging")["accepted_commit"] == "a" * 40
    )


def test_complete_wrapper_requires_registered_exact_policies(tmp_path, monkeypatch):
    collection = fixture(tmp_path)
    monkeypatch.setattr(packaging, "installed_policies", lambda: {})
    with pytest.raises(ValueError, match="must be installed"):
        packaging.prepare_selected(
            collection,
            family="retained-sec-comments",
            read_snapshot=publication.empty_index(),
            output=tmp_path / "missing",
        )
    monkeypatch.setattr(
        packaging, "installed_policies", lambda: {name: POLICIES[name] for name in TABLES["sec-comments"]}
    )
    result = packaging.prepare_selected(
        collection,
        family="retained-sec-comments",
        read_snapshot=publication.empty_index(),
        output=tmp_path / "complete",
    )
    artifact = packaging.generations.verify_generation(tmp_path / "complete/generation")
    assert (
        artifact.root["spec"]["publicationStatus"] == "complete-family" and result["missing_installed_policies"] == []
    )


@pytest.mark.parametrize("fault", ["member-bytes", "metadata-pin", "receipt-generation", "membership", "path"])
def test_changed_accepted_metadata_or_payload_refuses_wrapper(tmp_path, monkeypatch, fault):
    collection = fixture(tmp_path)
    monkeypatch.setattr(packaging, "installed_policies", lambda: {})
    if fault == "member-bytes":
        Path(collection["members"][0]["absolute_path"]).write_bytes(b"changed")
    elif fault == "metadata-pin":
        Path(collection["build_metadata"]["path"]).write_text("{}")
    elif fault == "receipt-generation":
        collection["accepted_selected_generation"] = "other-generation"
    elif fault == "membership":
        collection["members"].pop()
    else:
        collection["members"][0]["absolute_path"] = str(tmp_path / "elsewhere.parquet")
    with pytest.raises(ValueError):
        packaging.prepare_selected(
            collection,
            family="retained-sec-comments",
            read_snapshot=publication.empty_index(),
            output=tmp_path / "wrapper",
            preview=True,
        )
    assert not (tmp_path / "wrapper").exists()


def test_cross_device_link_refuses_without_copy_fallback(tmp_path, monkeypatch):
    collection = fixture(tmp_path)
    monkeypatch.setattr(packaging, "installed_policies", lambda: {})

    def refuse(*args, **kwargs):
        raise OSError(18, "Cross-device link")

    monkeypatch.setattr(os, "link", refuse)
    with pytest.raises(OSError):
        packaging.prepare_selected(
            collection,
            family="retained-sec-comments",
            read_snapshot=publication.empty_index(),
            output=tmp_path / "wrapper",
            preview=True,
        )
    assert not (tmp_path / "wrapper").exists()


def test_existing_destination_and_candidate_child_are_refused(tmp_path, monkeypatch):
    collection = fixture(tmp_path)
    monkeypatch.setattr(packaging, "installed_policies", lambda: {})
    with pytest.raises(FileExistsError):
        packaging.prepare_selected(
            collection,
            family="retained-sec-comments",
            read_snapshot=publication.empty_index(),
            output=Path(collection["candidate_path"]),
            preview=True,
        )
    with pytest.raises(ValueError, match="inside accepted"):
        packaging.prepare_selected(
            collection,
            family="retained-sec-comments",
            read_snapshot=publication.empty_index(),
            output=Path(collection["candidate_path"]) / "new",
            preview=True,
        )


def test_existing_receipt_dataset_ownership_refuses_cold_start(tmp_path, monkeypatch):
    collection = fixture(tmp_path)
    monkeypatch.setattr(packaging, "installed_policies", lambda: {})
    artifact_digest = "sha256:" + "a" * 64
    index = {
        "format": "spicy-regs-publication",
        "version": 2,
        "families": {
            "other": {
                "prefix": "generations/other/" + "a" * 64,
                "logicalId": "urn:fixture:other",
                "artifactDigest": artifact_digest,
                "tables": {},
                "etlReceipts": {
                    "key": "etl_receipts.parquet",
                    "sha256": "sha256:" + "b" * 64,
                    "byteSize": 1,
                    "rows": 1,
                    "columns": [["receipt_id", "VARCHAR"]],
                    "generationId": "other-generation",
                    "datasets": ["sec_rulemakings"],
                },
            }
        },
    }
    with pytest.raises(ValueError, match="receipt dataset"):
        packaging.prepare_selected(
            collection, family="retained-sec-comments", read_snapshot=index, output=tmp_path / "wrapper", preview=True
        )
    assert not (tmp_path / "wrapper").exists()
