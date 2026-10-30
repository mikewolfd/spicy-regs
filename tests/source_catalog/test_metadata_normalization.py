"""Receipt and content metadata share the catalog without sharing provenance."""

import hashlib
import json

import pytest

from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs.source_catalog.coverage import coverage
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.raw_archive import RawImporter, merge_content
from spicy_regs.source_catalog.storage import connect


def retain(root, body, *observations):
    store = CentralRawArchiveStore(root)
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob(sha, len(body), [body])
    store.record_receipt(
        files=[{"sha256": sha, "bytes": len(body), **facts} for facts in observations],
        receipt={"recorded_at": "2026-10-24T12:00:00Z"},
    )
    return sha


def test_normalized_transport_conflicts_and_failures_do_not_become_acquired(tmp_path):
    root = tmp_path / "raw"
    retain(
        root,
        b"source body",
        {"dataset_id": "example", "receipt_context": {"status_code": 500}},
        {"dataset_id": "example", "http_status": 200, "receipt_context": {"status_code": 500}},
        {"dataset_id": "example", "receipt_context": {"capture_complete": False}},
        {"dataset_id": "example", "receipt_context": {"source_file_complete": False}},
    )
    build(root, tmp_path / "index.sqlite")
    with connect(tmp_path / "index.sqlite") as db:
        assert {r[0] for r in db.execute("SELECT status FROM files")} == {
            "failed-response",
            "conflicting-receipt",
            "incomplete-response",
            "partial-source",
        }
        assert coverage(db, "example")["collections"][0]["stages"]["acquired"]["measurements"] == []


def test_aliases_reuse_verified_content_and_keep_acquisition_times(tmp_path, monkeypatch):
    root = tmp_path / "raw"
    body = json.dumps(
        {"data": {"type": "comments", "id": "ACF-2023-0001-0001", "attributes": {"postedDate": "2023-12-04T05:00:00Z"}}}
    ).encode()
    retain(
        root,
        body,
        *[
            {
                "dataset_id": "family:comments-mirror",
                "original_location": "/old/" + str(n),
                "receipt_context": {"observed_at": instant},
            }
            for n, instant in enumerate(["2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"])
        ],
    )
    before = {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob("*") if p.is_file()
    }
    calls = []
    original = RawImporter.read_content

    def read(self, sha, obj, dataset, **kwargs):
        calls.append(sha)
        return original(self, sha, obj, dataset, **kwargs)

    monkeypatch.setattr(RawImporter, "read_content", read)
    result = build(root, tmp_path / "index.sqlite")
    assert len(calls) == 1
    assert result["raw_archive"]["metadata_extraction"]["cache_hits"] == 1
    with connect(tmp_path / "index.sqlite") as db:
        mappings = [json.loads(r[0])["mapping"] for r in db.execute("SELECT data FROM files")]
        assert {m["acquired_at"] for m in mappings} == {"2026-01-01T00:00:00Z", "2026-02-01T00:00:00Z"}
        assert all(m["periods"][0]["label"] == "2023-12-04" for m in mappings)
        assert all({e["origin"] for e in m["evidence"]} == {"receipt", "content"} for m in mappings)
        assert all(m["receipt_evidence_ref"]["sha256"].startswith("sha256:") for m in mappings)
    assert before == {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob("*") if p.is_file()
    }


def test_hash_path_alias_uses_publisher_media_extension_before_reading(tmp_path, monkeypatch):
    root = tmp_path / "raw"
    retain(
        root,
        b"WEBVTT",
        {
            "dataset_id": "family:committee-meetings",
            "original_location": "/old/hash.gz",
            "receipt_source_url": "https://example.test/captions.vtt",
        },
    )
    monkeypatch.setattr(RawImporter, "read_content", lambda *args: pytest.fail("Unsupported media was read"))
    result = build(root, tmp_path / "index.sqlite")
    assert result["raw_archive"]["metadata_extraction"]["unsupported_format_locations"] == 1


def test_content_cap_is_explicit_and_never_qualifies_a_period(tmp_path, monkeypatch):
    root = tmp_path / "raw"
    retain(root, b" " * (1024 * 1024 + 1), {"dataset_id": "family:lobbying-filings"})
    monkeypatch.setattr(RawImporter, "read_content", lambda *args: pytest.fail("Oversize original was read"))
    build(root, tmp_path / "index.sqlite")
    with connect(tmp_path / "index.sqlite") as db:
        mapping = json.loads(db.execute("SELECT data FROM files").fetchone()[0])["mapping"]
        assert mapping["content_metadata"]["outcome"] == "not-read"
        assert mapping["content_metadata"]["scope"]["reason"] == "byte-limit"
        assert mapping["periods"] == []


def test_content_checksum_failure_preserves_previous_catalog(tmp_path):
    root = tmp_path / "raw"
    sha = retain(root, b"original", {"dataset_id": "family:comments-mirror"})
    (root / "objects/sha256" / sha[:2] / sha).write_bytes(b"changed!")
    output = tmp_path / "index.sqlite"
    output.write_bytes(b"previous catalog")
    with pytest.raises(ValueError, match="Original changed"):
        build(root, output)
    assert output.read_bytes() == b"previous catalog"


def test_same_package_namespace_conflict_is_not_qualified_but_different_forms_survive():
    mapping = {
        "native_id": "BILLS-112hr2626ih",
        "periods": [{"kind": "congress", "label": "112"}],
        "dataset_id": "family:bill-family",
        "evidence": [],
    }
    content = {"outcome": "ok", "native_id": "BILLS-113hr2626ih", "periods": [{"kind": "congress", "label": "113"}]}
    result = merge_content(mapping, content, "sha256:x")
    assert result["content_metadata"]["conflict"] is True
    assert result["native_id"] is None and result["periods"] == []
    assert {p["label"] for p in result["candidate_periods"]} == {"112", "113"}
    content["native_id"] = "112/hr/2626"
    result = merge_content(mapping, content, "sha256:x")
    assert result["native_id"] == mapping["native_id"]
    assert result["content_metadata"]["native_id"] == "112/hr/2626"


def test_single_primary_congress_conflict_is_explicit_but_requested_dates_are_separate():
    mapping = {"periods": [{"kind": "congress", "label": "119"}], "evidence": []}
    content = {"outcome": "ok", "primary_records": 1, "periods": [{"kind": "congress", "label": "118"}]}
    result = merge_content(mapping, content, "sha256:x")
    assert result["content_metadata"]["conflict"] is True
    assert result["field_conflicts"]["receipt_content_congress"] == ["118", "119"]
    mapping["periods"] = [{"kind": "requested-publication-range", "label": "2024-01-01/2024-12-31"}]
    content["periods"] = [{"kind": "publication-date", "label": "2024-02-01"}]
    result = merge_content(mapping, content, "sha256:x")
    assert not result["content_metadata"].get("conflict")
    assert len(result["periods"]) == 2


def test_query_can_find_content_identity_inside_multi_record_response(tmp_path, monkeypatch, capsys):
    from spicy_regs.source_catalog.__main__ import main

    root = tmp_path / "raw"
    body = json.dumps(
        {
            "results": [
                {"filing_uuid": "11111111-1111-1111-1111-111111111111", "filing_year": 2024},
                {"filing_uuid": "22222222-2222-2222-2222-222222222222", "filing_year": 2025},
            ]
        }
    ).encode()
    retain(root, body, {"dataset_id": "family:lobbying-filings"})
    output = tmp_path / "index.sqlite"
    build(root, output)
    monkeypatch.setattr(
        "sys.argv", ["catalog", "query", "--index", str(output), "--native-id", "22222222-2222-2222-2222-222222222222"]
    )
    main()
    assert len(json.loads(capsys.readouterr().out)) == 1


def test_receipt_shaped_bytes_do_not_count_as_raw_source(tmp_path):
    root = tmp_path / "raw"
    body = json.dumps(
        {
            "body_path": "publisher.json",
            "sha256": "a" * 64,
            "byte_size": 12,
            "requested_url": "https://publisher.test/source",
            "status_code": 200,
        }
    ).encode()
    retain(root, body, {"dataset_id": "family:committee-meetings", "original_location": "/old/receipt.json"})
    result = build(root, tmp_path / "index.sqlite")
    assert result["raw_archive"]["totals"]["raw_objects"] == 0
    with connect(tmp_path / "index.sqlite") as db:
        row = db.execute("SELECT collection_id,status FROM files").fetchone()
        assert tuple(row) == ("archive:receipts", "receipt-only")
