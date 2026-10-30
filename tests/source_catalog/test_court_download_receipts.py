"""Local bulk-download receipts must not become publisher source coverage."""

import hashlib
import json
from typing import Any

import pytest

from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs.source_catalog.coverage import coverage
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.raw_content import extract_metadata
from spicy_regs.source_catalog.storage import connect


def bulk_receipt(dataset="citation-map", *, reused=False, body=b"publisher bulk source"):
    """Minimal shape of verified CourtListener receipt 289bdb02...a3ea35."""
    sha = hashlib.sha256(body).hexdigest()
    filename = dataset + "-2026-09-30.csv.bz2"
    url = "https://storage.courtlistener.com/bulk-data/" + filename
    return {
        "selection": {"dataset": dataset, "filename": filename, "bytes": len(body), "url": url},
        "download": {
            "url": url,
            "sha256": "sha256:" + sha,
            "bytes": len(body),
            "blob_path": "sha256/" + sha,
            "reused": reused,
            "downloaded": not reused,
            "response": {"resolved_url": url, "observed_at": "2026-10-19T22:54:50.584860+00:00"},
        },
    }


@pytest.mark.parametrize("dataset", ["citation-map", "citations", "parentheticals"])
@pytest.mark.parametrize("reused", [False, True])
def test_bulk_download_receipt_has_no_source_identity_or_period(dataset, reused):
    result = extract_metadata("family:court-citations", json.dumps(bulk_receipt(dataset, reused=reused)).encode())
    assert result["role"] == "acquisition-receipt"
    assert result["outcome"] == "ok"
    assert result["native_id"] is None and result["dataset_id"] is None
    assert result["periods"] == []


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("download", "downloaded", None),
        ("download", "downloaded", "true"),
        ("download", "downloaded", False),
        ("download", "blob_path", "sha256/" + "0" * 64),
        ("download", "response", {}),
        ("download", "sha256", "unverified"),
        ("selection", "url", "https://publisher.test/different-source"),
        ("selection", "bytes", True),
    ],
)
def test_similar_publisher_fields_need_coherent_local_acquisition_facts(section, field, value):
    document = bulk_receipt()
    document[section][field] = value
    document["bill"] = {"congress": 119, "type": "HR", "number": 1}
    result = extract_metadata("family:bill-family", json.dumps(document).encode())
    assert "role" not in result
    assert result["native_id"] == "hr1-119"


def test_rebuild_keeps_bulk_original_and_counts_receipt_separately(tmp_path):
    root = tmp_path / "raw"
    store = CentralRawArchiveStore(root)
    original = b"publisher bulk source"
    receipt = bulk_receipt(body=original)
    bodies = [original, json.dumps(receipt).encode()]
    descriptors: list[dict[str, Any]] = []
    for body in bodies:
        sha = hashlib.sha256(body).hexdigest()
        store.put_blob(sha, len(body), [body])
        descriptors.append({"sha256": sha, "bytes": len(body), "dataset_id": "family:court-citations"})
    store.record_receipt(files=descriptors, receipt={"status": "success"})

    result = build(root, tmp_path / "index.sqlite")
    assert result["raw_archive"]["totals"]["raw_objects"] == 1
    with connect(tmp_path / "index.sqlite") as db:
        files = {row[0]: tuple(row[1:]) for row in db.execute("SELECT sha256,collection_id,status FROM files")}
        assert files == {
            "sha256:" + descriptors[0]["sha256"]: ("family:court-citations", "held"),
            "sha256:" + descriptors[1]["sha256"]: ("archive:receipts", "receipt-only"),
        }
        measurements = coverage(db, "family:court-citations")["collections"][0]["stages"]["acquired"]["measurements"]
        assert len(measurements) == 1 and measurements[0]["count"] == 1
    for descriptor, body in zip(descriptors, bodies, strict=True):
        with store.open(descriptor["sha256"]) as handle:
            assert handle.read() == body
