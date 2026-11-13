"""Full selected-feed accounting and ordinary native merge controls."""

import hashlib
import json

import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.web_archive import COMMON_CRAWL
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts
from tests.test_cbo_archive_cards import held
from tests.test_bill_family_cbo import CARD, NOTICE, _item
from spicy_regs.transforms.cbo_archive_selection import (
    load_selection,
    read_selected_cards,
    merge_card_rows,
    acquire_missing_pages,
)
from spicy_regs.transforms.bill_family_cbo import latest_capture


def selection(tmp_path):
    manifest, raw = held(tmp_path, [("62720", CARD, "2026-10-17T05:18:39Z"), ("62589", NOTICE, "2026-10-17T06:19:02Z")])
    document = json.loads(manifest.read_text())
    store = CentralRawArchiveStore(raw)
    crawl, pattern = "CC-MAIN-2026-43", "www.cbo.gov/publication/*"
    rows = []
    for _, record in iter_acquisition_receipts(raw, receipt_keys=document["receipt_keys"]):
        receipt = record["receipt"]
        rows.append(
            {
                "url": receipt["original_publisher_url"],
                "timestamp": receipt["archive_timestamp"],
                "status": "200",
                "mime": "text/html",
                "digest": receipt["index_digest"],
                "filename": receipt["warc_filename"],
                "offset": "0",
                "length": str(receipt["warc_length"]),
            }
        )
    indexes = []
    for dataset, body, native in [
        ("cc-index", "\n".join(json.dumps(row) for row in rows).encode(), crawl + ":" + pattern + ":0"),
        ("cc-index-pages", b'{"pages":1}', crawl + ":" + pattern),
    ]:
        sha = hashlib.sha256(body).hexdigest()
        store.put_blob(sha, len(body), [body])
        indexes.append(
            store.record_receipt(
                files=[{"sha256": sha, "bytes": len(body)}],
                receipt={
                    "dataset_id": dataset,
                    "crawl_id": crawl,
                    "original_location": pattern,
                    "native_id": native,
                    "status_code": 200,
                    "requested_url": "https://index.commoncrawl.org/CC-MAIN-2026-43-index",
                },
            )["object"]
        )
    feed = (
        "<response>"
        + _item(0, "S. 4429, Example", 62720, "S. 4429")
        + _item(1, "Suspension", 62589, None)
        + _item(2, "Unarchived", 62721, None)
        + "</response>"
    ).encode()
    sha = hashlib.sha256(feed).hexdigest()
    store.put_blob(sha, len(feed), [feed])
    document.update(
        congress=119,
        feed={"sha256": sha, "bytes": len(feed)},
        index_receipt_keys=indexes,
        crawls=[crawl],
        patterns=[pattern],
    )
    manifest.write_text(json.dumps(document))
    return manifest, raw


def test_complete_selected_population_and_native_merge_keep_prior_on_unread(tmp_path):
    manifest, raw = selection(tmp_path)
    output = tmp_path / "output"
    output.mkdir()
    rows = read_selected_cards(manifest, archive_root=raw, congresses=[119], output_dir=output)
    report = json.loads((output / "cbo_archive_population.json").read_text())
    assert report["population"] == 3 and report["counts"] == {"card": 1, "absent": 1, "neither-observed": 1}
    assert not report["unread_indexes"] and not report["source_accepted"]
    path = merge_card_rows(output, rows, download_prior=lambda *_: False)
    original = pq.read_table(path)
    assert original.to_pylist()[0]["windows"][2]["revenues_millions"] == 1234
    next_run = tmp_path / "next"
    next_run.mkdir()

    def prior(key, target):
        target.write_bytes(path.read_bytes())
        return True

    kept = merge_card_rows(next_run, [], download_prior=prior)
    assert pq.read_table(kept).equals(original)
    assert read_selected_cards(None, archive_root=raw, congresses=[119], output_dir=output) == ()


def test_resume_reuses_exact_warc_coordinates_without_network(tmp_path):
    manifest, raw = selection(tmp_path)
    selected = load_selection(manifest, raw)

    class NeverNetwork:
        def acquire_cc_record(self, *_):
            raise AssertionError("retained original was fetched again")

    keys = acquire_missing_pages(
        selected, acquirer=NeverNetwork(), evidence=None, checkpoint_path=tmp_path / "resume.json"
    )
    assert keys == selected.document["receipt_keys"]


def test_missing_receipt_and_wrong_congress_do_not_become_empty_success(tmp_path):
    manifest, raw = selection(tmp_path)
    with pytest.raises(ValueError, match="outside"):
        read_selected_cards(manifest, archive_root=raw, congresses=[118], output_dir=tmp_path)
    doc = json.loads(manifest.read_text())
    doc["receipt_keys"].append("acquisitions/sha256/00/" + "0" * 64)
    manifest.write_text(json.dumps(doc))
    with pytest.raises(FileNotFoundError):
        read_selected_cards(manifest, archive_root=raw, congresses=[119], output_dir=tmp_path)
    assert not (tmp_path / "cbo_archive_population.json").exists()


def test_latest_index_selection_is_order_independent_and_conflicts_refuse():
    from dataclasses import replace
    from spicy_docs.sources.web_archive import ArchiveCandidate

    first = ArchiveCandidate(
        COMMON_CRAWL,
        "https://www.cbo.gov/publication/62720",
        "20261017051839",
        200,
        "text/html",
        "A" * 32,
        filename="file",
        offset=0,
    )
    same = replace(first, original_url=first.original_url + "?tracking=1", offset=1)
    assert latest_capture([same, first]) == latest_capture([first, same]) == first
    with pytest.raises(ValueError, match="Conflicting"):
        latest_capture([first, replace(same, digest="B" * 32)])


def test_interruption_after_retention_before_checkpoint_recovers_journal(tmp_path):
    from spicy_docs.transport.captured import CapturedBodyResponse
    from spicy_regs.source_evidence import CaptureEvidence

    manifest, raw = selection(tmp_path)
    selected = load_selection(manifest, raw)
    records = list(iter_acquisition_receipts(raw, receipt_keys=selected.document["receipt_keys"]))
    _, held_card = next(record for record in records if record[1]["receipt"]["native_id"] == "62720")
    item = held_card["files"][0]
    store = CentralRawArchiveStore(raw)
    with store.open(item["sha256"]) as stream:
        body = stream.read()
    evidence = CaptureEvidence(tmp_path / "capture", "cbo-web-archive", archive_root=raw)
    evidence.capture(
        CapturedBodyResponse(
            "https://data.commoncrawl.org/retained.warc.gz",
            "https://data.commoncrawl.org/retained.warc.gz",
            206,
            "application/gzip",
            "2026-11-13T11:00:00Z",
            body,
        ),
        stage="cbo-archive-page",
        **held_card["receipt"],
    )
    keys = [key for key in selected.document["receipt_keys"] if key != held_card["receipt_object"]]
    checkpoint = tmp_path / "resume.json"
    checkpoint.write_text(
        json.dumps(
            {
                "feed": selected.document["feed"],
                "receipt_keys": keys,
                "capture_journals": [str(evidence.artifact_dir.relative_to(tmp_path) / "journal.jsonl")],
            }
        )
    )
    selected.document["receipt_keys"] = keys

    class NeverNetwork:
        def acquire_cc_record(self, *_):
            raise AssertionError("retained capture was fetched after interruption")

    resumed = acquire_missing_pages(selected, acquirer=NeverNetwork(), evidence=None, checkpoint_path=checkpoint)
    assert len(resumed) == 2


def test_native_receipt_round_trip_and_invalid_amount_rejection(tmp_path):
    import copy
    import pyarrow as pa
    from spicy_regs.congress_receipts import write_congress_dataset, restore_processing_input

    manifest, raw = selection(tmp_path)
    output = tmp_path / "output"
    output.mkdir()
    rows = read_selected_cards(manifest, archive_root=raw, congresses=[119], output_dir=output)
    path = merge_card_rows(output, rows, download_prior=lambda *_: False)
    original = pq.read_table(path)
    subject, receipts = write_congress_dataset(
        path, tmp_path / "native", dataset="cbo_estimate_cards", generation_id="g"
    )
    restored = restore_processing_input(
        subject, receipts, tmp_path / "restored.parquet", dataset="cbo_estimate_cards", generation_id="g"
    )
    assert pq.read_table(restored).equals(original)
    bad = copy.deepcopy(rows[0])
    bad["windows"][0]["revenues_millions"] = 999
    invalid = tmp_path / "invalid.parquet"
    pq.write_table(pa.Table.from_pylist([bad], schema=original.schema), invalid)
    rejected, errors = write_congress_dataset(
        invalid, tmp_path / "rejected", dataset="cbo_estimate_cards", generation_id="bad"
    )
    assert pq.read_metadata(rejected).num_rows == 0
    assert "refused" in pq.read_table(errors)["outcome"].to_pylist()
