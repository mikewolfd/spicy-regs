"""Read a few immutable originals without replaying or selecting the shared archive."""

import hashlib
import json
import sys

import pytest
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts

from spicy_regs.source_catalog.__main__ import main
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.raw_archive import RawImporter
from spicy_regs.source_catalog.selection import select


def _files(store, version, *copies):
    files = []
    for url, body in copies:
        digest = hashlib.sha256(body).hexdigest()
        store.put_blob(digest, len(body), [body])
        files.append({
            "sha256": digest, "bytes": len(body), "family": "crs-report-files",
            "native_id": "R41330", "source_version": version, "source_url": url,
            "edition": int(version), "literal_cover_date": "May 10, 2024",
            "role": "source-body", "source_validation": "accepted",
        })
    return store.record_receipt(files=files, receipt={"historical_api_update_date": None})["object"]


def test_receipt_slice_preserves_numbered_editions_alternate_copies_and_exact_file_ordinal(tmp_path, monkeypatch, capsys):
    raw = tmp_path / "raw"
    store = CentralRawArchiveStore(raw)
    main_url = "https://www.congress.gov/crs_external_products/R/PDF/R41330/R41330."
    newer = _files(store, "47", (main_url + "47.pdf", b"newer original"))
    older = _files(store, "46", (main_url + "46.pdf", b"same edition bytes"),
                   ("https://web.archive.org/web/20260127000000id_/" + main_url + "46.pdf", b"same edition bytes"))
    (raw / "snapshot.json").write_text("not a snapshot")
    monkeypatch.setattr(RawImporter, "receipt_paths", lambda _: pytest.fail("receipt slice scanned the archive"))
    index = tmp_path / "candidate.sqlite"

    def query(version):
        monkeypatch.setattr(sys, "argv", ["source_catalog", "query", "--index", str(index), "--scope", "all",
                                         "--collection", "family:crs-reports", "--native-id", "R41330",
                                         "--source-version", version])
        main()
        return json.loads(capsys.readouterr().out)

    report = build(raw, index, acquisition_receipts=[newer])
    assert report["scope"] == "selected-acquisition-receipts"
    assert not any(path.endswith("snapshot.json") for path in report["inputs"])
    prior = query("47")
    assert len(prior) == 1 and not query("46")

    report = build(raw, index, acquisition_receipts=[newer, older])
    assert query("47") == prior
    copies = query("46")
    assert len(copies) == 2 and copies[0]["sha256"] == copies[1]["sha256"]
    assert len({row["id"] for row in copies}) == 2
    files = [json.loads(row["data"]) for row in copies]
    assert {f["literal_cover_date"] for f in files} == {"May 10, 2024"}
    archived = next(f for f in files if f["source_url"].startswith("https://web.archive.org"))
    link = archived["mapping"]["receipt_evidence_ref"]
    assert link == {"manifest": older, "entry": 2, "sha256": "sha256:" + older.rsplit("/", 1)[-1]}
    (_, original), = iter_acquisition_receipts(raw, receipt_keys=[link["manifest"]])
    assert original["files"][link["entry"] - 1]["source_url"] == archived["source_url"]
    with store.open(copies[0]["sha256"]) as stream:
        assert stream.read() == b"same edition bytes"

    repeated = build(raw, index, acquisition_receipts=[older, newer])
    assert repeated["generation"] == report["generation"] and query("46") == copies
    completed = index.read_bytes()
    missing = "acquisitions/sha256/00/" + "0" * 64
    for keys, error, reason in (([newer, newer], ValueError, "unique canonical"),
                                ([newer, missing], FileNotFoundError, None)):
        with pytest.raises(error, match=reason):
            build(raw, index, acquisition_receipts=keys)
        assert index.read_bytes() == completed
    (raw / older).write_bytes(b"damaged receipt")
    with pytest.raises(ValueError, match="immutable identity"):
        build(raw, index, acquisition_receipts=[newer, older])
    assert index.read_bytes() == completed

    cache, public = tmp_path / "cache", tmp_path / "public"
    cache.mkdir()
    public.mkdir()
    with pytest.raises(ValueError, match="receipt slice cannot replace the shared catalog"):
        select(index, index.with_suffix(".sqlite.reconciliation.json"), tmp_path / "staged", cache, public, raw,
               expected_generation=report["generation"], allow_inventory_change=True)
