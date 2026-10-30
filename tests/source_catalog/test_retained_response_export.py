"""Retained source responses stay inspectable without increasing usable holdings."""

from tests.source_catalog.file_helpers import read_file_page

import hashlib
import json
from typing import Any
import runpy
from pathlib import Path

from spicy_regs.source_catalog.exporter import export
from spicy_regs.source_catalog.scope import install_scope
from spicy_regs.source_catalog.storage import connect, schema

SAM = runpy.run_path(str(Path(__file__).with_name("test_captured_errors.py")))["SAM_BODY"]
SAM_SHA = "sha256:" + hashlib.sha256(SAM).hexdigest()


def fixture(tmp_path, *, scoped=True):
    index = tmp_path / "index.sqlite"
    with connect(index, scope="all") as db:
        schema(db)
        db.executemany(
            "INSERT INTO metadata VALUES (?,?)",
            [("generation", json.dumps("sha256:" + "f" * 64)), ("generatedAt", json.dumps("2026-10-28T00:00:00Z"))],
        )
        for cid in ["active", "other-active", "historical"]:
            db.execute("INSERT INTO collections VALUES (?,?,?)", (cid, cid, "{}"))
        shared, excluded = "sha256:" + "b" * 64, "sha256:" + "c" * 64
        cases = [
            ("usable", "active", "held", "sha256:" + "d" * 64, "original-response"),
            ("sam-error", "active", "refused-source", SAM_SHA, "original-response"),
            ("missing", "active", "missing-bytes", "sha256:" + "e" * 64, "original-response"),
            ("unchecked", "active", "unclassified-source", "sha256:" + "0" * 64, "original-response"),
            ("support", "active", "unclassified-source", "sha256:" + "1" * 64, "supporting-original"),
            ("processing", "active", "refused-source", "sha256:" + "2" * 64, "processing-evidence"),
            ("mapping-support", "active", "refused-source", "sha256:" + "5" * 64, "original-response"),
            ("content-processing", "active", "refused-source", "sha256:" + "6" * 64, "original-response"),
            ("receipt", "active", "missing-bytes", "sha256:" + "3" * 64, "acquisition-receipt"),
            ("request", "active", "unclassified-source", "sha256:" + "4" * 64, "request-body"),
            ("excluded-global", "active", "refused-source", excluded, "original-response"),
            ("excluded-scoped", "active", "refused-source", shared, "original-response"),
            ("allowed-other-use", "other-active", "refused-source", shared, "original-response"),
            ("retired", "historical", "refused-source", SAM_SHA, "original-response"),
        ]
        for ident, cid, status, sha, role in cases:
            size = len(SAM) if sha == SAM_SHA else 10
            db.execute("INSERT OR IGNORE INTO objects VALUES (?,?)", (sha, size))
            facts: dict[str, Any] = {
                "role": role,
                "source_url": "https://source.gov/response",
                "receipt_context": {"secret": "never-public"},
                "mapping": {"role": role, "receipt_evidence_ref": {"sha256": "sha256:" + "a" * 64, "entry": 1}},
            }
            if ident == "mapping-support":
                facts["mapping"]["role"] = "supporting-original"
            if ident == "content-processing":
                facts["mapping"]["content_metadata"] = {"role": "processing-evidence"}
            if status != "missing-bytes":
                facts["encoded_bytes"] = size
            db.execute(
                "INSERT INTO files VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    ident,
                    cid,
                    None,
                    ident,
                    status,
                    sha,
                    size,
                    None if status == "missing-bytes" else "/private/original",
                    json.dumps(facts),
                ),
            )
        if scoped:
            install_scope(
                db,
                {
                    "format": "spicy-source-scope",
                    "version": 2,
                    "collections": [
                        {"id": cid, "disposition": disposition, "reason": "Reviewed fixture."}
                        for cid, disposition in [
                            ("active", "active"),
                            ("other-active", "active"),
                            ("historical", "retained"),
                        ]
                    ],
                    "excluded_objects": [
                        {"sha256": excluded, "reason": "Excluded in all source uses."},
                        {"sha256": shared, "collection_id": "active", "reason": "Wrong source attribution here."},
                    ],
                },
            )
        db.commit()
    return index


def test_scoped_response_pages_preserve_main_counts_and_scope_exclusions(tmp_path):
    index = fixture(tmp_path)
    root = tmp_path / "public"
    manifest = export(index, root, page_size=2)
    assert {item["id"] for item in manifest["collections"]} == {"active", "other-active"}
    collection = next(item for item in manifest["collections"] if item["id"] == "active")
    assert collection["holdings"] == {"aliases": 1, "objects": 1, "decodedBytes": 10, "missing": 0}
    detail = json.loads((root / collection["detailPath"]).read_text())
    assert detail["files"]["total"] == 1
    acquired = detail["coverage"]["stages"]["acquired"]["measurements"]
    assert len(acquired) == 1 and acquired[0]["count"] == 1
    assert acquired[0]["unit"] == "retained-file-record"
    assert detail["retainedFiles"]["total"] == 3
    assert [item["rows"] for item in detail["retainedFiles"]["pages"]] == [2, 1]
    pages = [read_file_page(root / part["path"]) for part in detail["retainedFiles"]["pages"]]
    assert all(page["generation"] == manifest["generation"] and page["collectionId"] == "active" for page in pages)
    files = {item["id"]: item for page in pages for item in page["files"]}
    assert set(files) == {"sam-error", "missing", "unchecked"}
    assert files["sam-error"]["sha256"] == SAM_SHA and files["sam-error"]["bytes"] == 154
    assert files["sam-error"]["status"] == "refused-source"
    assert files["missing"]["provenance"]["availability"] == "missing"
    assert files["unchecked"]["status"] == "unclassified-source"
    assert all(item["provenance"]["sourceUrls"] == ["https://source.gov/response"] for item in files.values())
    body = json.dumps(pages)
    assert "/private" not in body and "never-public" not in body
    other = next(item for item in manifest["collections"] if item["id"] == "other-active")
    other_detail = json.loads((root / other["detailPath"]).read_text())
    assert other_detail["files"]["total"] == 0 and other_detail["retainedFiles"]["total"] == 1
    other_page = read_file_page(root / other_detail["retainedFiles"]["pages"][0]["path"])
    assert other_page["files"][0]["id"] == "allowed-other-use"
    with connect(index, scope="all") as db:
        assert db.execute("SELECT count(*) FROM main.files").fetchone()[0] == 14


def test_legacy_unscoped_exports_do_not_invent_reviewed_response_inventory(tmp_path):
    index = fixture(tmp_path, scoped=False)
    root = tmp_path / "public"
    manifest = export(index, root)
    for collection in manifest["collections"]:
        detail = json.loads((root / collection["detailPath"]).read_text())
        assert "retainedFiles" not in detail
