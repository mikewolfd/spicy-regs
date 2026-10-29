"""Structured receipt dates remain usable through the public offline query path."""

import hashlib
import json
import sys

from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs.source_catalog.__main__ import main
from spicy_regs.source_catalog.importer import build


def test_exact_date_query_finds_body_with_structured_receipt_period(tmp_path, monkeypatch, capsys):
    root = tmp_path / "raw"
    store = CentralRawArchiveStore(root)
    body = b"<html><body>Federal Register: January 4, 1994; 94-9</body></html>"
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob(sha, len(body), [body])
    store.record_receipt(files=[{
        "sha256": sha,
        "bytes": len(body),
        "dataset_id": "family:federal-register",
        "native_id": "94-9",
        "original_location": "/retained/94-9.html",
        "receipt_context": {
            "period": {"kind": "date", "label": "1994-01-04",
                       "basis": "Companion publisher metadata publication_date."},
        },
    }], receipt={"recorded_at": "2026-10-24T12:00:00Z"})
    index = tmp_path / "index.sqlite"
    build(root, index)
    monkeypatch.setattr(sys, "argv", [
        "source-catalog", "query", "--index", str(index),
        "--collection", "family:federal-register", "--native-id", "94-9",
        "--period", "1994-01-04",
    ])
    main()
    rows = json.loads(capsys.readouterr().out)
    assert len(rows) == 1 and rows[0]["sha256"] == "sha256:" + sha
    mapping = json.loads(rows[0]["data"])["mapping"]
    assert [(p["kind"], p["label"]) for p in mapping["periods"]] == [("date", "1994-01-04")]
    assert "completeness unknown" in mapping["periods"][0]["basis"]
