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


def test_build_bill_family_preserves_prior_cbo_estimate_cards_when_manifest_unset(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_bill_family import build_bill_family
    from tests.test_bill_family import StubBodyAcquirer, StubBulkAcquirer, _priors

    manifest, raw = selection(tmp_path)
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    for key in ("GEMINI_API_KEY", "GOOGLE_API_KEY", "R2_PUBLIC_URL"):
        monkeypatch.delenv(key, raising=False)

    monkeypatch.setenv("BILL_FAMILY_CBO_ARCHIVE_MANIFEST", str(manifest))
    first = tmp_path / "first"
    first.mkdir()
    build_bill_family(
        first,
        bulk_acquirer=StubBulkAcquirer(),
        body_acquirer=StubBodyAcquirer(),
        max_version_fetches=10,
        cbo_archive_root=raw,
        **_priors(None),
    )
    seeded = pq.read_table(first / "cbo_estimate_cards.parquet").to_pylist()
    assert {row["publication_id"] for row in seeded} == {"62720"}

    monkeypatch.delenv("BILL_FAMILY_CBO_ARCHIVE_MANIFEST", raising=False)
    second = tmp_path / "second"
    second.mkdir()
    build_bill_family(
        second,
        bulk_acquirer=StubBulkAcquirer(),
        body_acquirer=StubBodyAcquirer(),
        max_version_fetches=10,
        **_priors(first),
    )
    preserved = pq.read_table(second / "cbo_estimate_cards.parquet").to_pylist()
    assert preserved == seeded


def _minimal_cbo_pdf(*, date_line: str, title: str, subtitle: str) -> bytes:
    import pymupdf

    doc = pymupdf.open()
    page = doc.new_page(width=612, height=792)
    lines = [
        "Congressional Budget Office",
        "Cost Estimate",
        date_line,
        title,
        subtitle,
        "By Fiscal Year, Millions of Dollars",
        "2026 2026-2031 2026-2036",
        "Direct Spending (Outlays) 0 0 0",
        "Revenues 0 0 0",
        "Increase or Decrease (-) in the Deficit 0 0 0",
        "Spending Subject to Appropriation (Outlays) 0 * not estimated",
        "Increases net direct spending in any of the four consecutive 10-year periods beginning in 2037? No",
        "Statutory pay-as-you-go procedures apply? No",
        "Mandate Effects",
        "Increases on-budget deficits in any of the four consecutive 10-year periods beginning in 2037? No",
        "Contains intergovernmental mandate? No",
        "Contains private-sector mandate? No",
        "* = between zero and $500,000.",
    ]
    y = 60
    for line in lines:
        page.insert_text((48, y), line, fontsize=10)
        y += 16
    return doc.tobytes()


def test_pdf_only_corroborated_rows_are_strictly_additive_and_mismatches_refuse(tmp_path):
    import io
    from warcio.statusandheaders import StatusAndHeaders
    from warcio.warcwriter import WARCWriter
    from spicy_docs.sources.cbo_pdf_cards import FEED_CONTENT_CORROBORATED_RULE
    from spicy_docs.sources.web_archive import payload_sha1, read_warc_response
    from spicy_regs.congress_receipts import restore_processing_input, write_congress_dataset

    manifest, raw = selection(tmp_path)
    store = CentralRawArchiveStore(raw)
    doc = json.loads(manifest.read_text())

    # Add two PDF-only feed items: 62593 (exact content match) and 62594 (mismatched date -> must refuse)
    good_title = "S. 3733, Passport Library Fee Act"
    good_desc = "As ordered reported by the Senate Committee on Foreign Relations on June 17, 2026"
    bad_title = "S. 3734, Mismatched Date Act"
    bad_desc = "As ordered reported by the Senate Committee on Finance on June 17, 2026"
    feed = (
        "<response>"
        + _item(0, "S. 4429, Example", 62720, "S. 4429")
        + _item(1, "Suspension", 62589, None)
        + f'<item key="2"><Title>{good_title}</Title><Date>Fri, 24 Jul 2026 12:49:00 -0400</Date>'
        + "<Link>https://www.cbo.gov/publication/62593</Link>"
        + f"<Description>{good_desc}</Description><Bill_Number>S. 3733</Bill_Number></item>"
        + f'<item key="3"><Title>{bad_title}</Title><Date>Fri, 24 Jul 2026 12:49:00 -0400</Date>'
        + "<Link>https://www.cbo.gov/publication/62594</Link>"
        + f"<Description>{bad_desc}</Description><Bill_Number>S. 3734</Bill_Number></item>"
        + "</response>"
    ).encode()
    feed_sha = hashlib.sha256(feed).hexdigest()
    store.put_blob(feed_sha, len(feed), [feed])

    crawl = "CC-MAIN-2026-43"
    pdf_pattern = "www.cbo.gov/system/files/*"
    pdf_index_rows = []
    pdf_receipt_keys = []
    for pid, path, pdf_bytes in [
        (
            "62593",
            "/system/files/2026-07/s3733.pdf",
            _minimal_cbo_pdf(date_line="July 24, 2026", title=good_title, subtitle=good_desc),
        ),
        (
            "62594",
            "/system/files/2026-07/s3734.pdf",
            _minimal_cbo_pdf(date_line="July 25, 2026", title=bad_title, subtitle=bad_desc),
        ),
    ]:
        url = "https://www.cbo.gov" + path
        stream = io.BytesIO()
        writer = WARCWriter(stream, gzip=True)
        record = writer.create_warc_record(
            url,
            "response",
            payload=io.BytesIO(pdf_bytes),
            http_headers=StatusAndHeaders("200 OK", [("Content-Type", "application/pdf")], protocol="HTTP/1.1"),
            warc_headers_dict={"WARC-Date": "2026-08-15T04:19:49Z"},
        )
        writer.write_record(record)
        warc_body = stream.getvalue()
        warc_sha = hashlib.sha256(warc_body).hexdigest()
        store.put_blob(warc_sha, len(warc_body), [warc_body])
        warc_resp = read_warc_response(warc_body)
        warc_file = f"crawl-data/{crawl}/segments/example/warc/{pid}.warc.gz"
        pdf_index_rows.append(
            {
                "url": url,
                "timestamp": "20260815041949",
                "status": "200",
                "mime": "application/pdf",
                "digest": payload_sha1(pdf_bytes),
                "filename": warc_file,
                "offset": "0",
                "length": str(len(warc_body)),
            }
        )
        receipt = {
            "authority": "archive",
            "archive_operator": "Common Crawl",
            "dataset_id": "cbo-estimate-pdf",
            "native_id": pid,
            "original_publisher_url": url,
            "archive_timestamp": "20260815041949",
            "index_sha256": "sha256:" + "2" * 64,
            "index_digest": payload_sha1(pdf_bytes),
            "payload_sha1": payload_sha1(pdf_bytes),
            "warc_filename": warc_file,
            "crawl_id": crawl,
            "warc_offset": 0,
            "warc_length": len(warc_body),
            "warc_record_id": warc_resp.record_id,
        }
        pdf_receipt_keys.append(
            store.record_receipt(files=[{"sha256": warc_sha, "bytes": len(warc_body)}], receipt=receipt)["object"]
        )

    idx_body = "\n".join(json.dumps(r) for r in pdf_index_rows).encode()
    idx_sha = hashlib.sha256(idx_body).hexdigest()
    store.put_blob(idx_sha, len(idx_body), [idx_body])
    pages_body = b'{"pages":1}'
    pages_sha = hashlib.sha256(pages_body).hexdigest()
    store.put_blob(pages_sha, len(pages_body), [pages_body])
    for dataset, sha, size, native in [
        ("cc-index", idx_sha, len(idx_body), f"{crawl}:{pdf_pattern}:0"),
        ("cc-index-pages", pages_sha, len(pages_body), f"{crawl}:{pdf_pattern}"),
    ]:
        doc["index_receipt_keys"].append(
            store.record_receipt(
                files=[{"sha256": sha, "bytes": size}],
                receipt={
                    "dataset_id": dataset,
                    "crawl_id": crawl,
                    "original_location": pdf_pattern,
                    "native_id": native,
                    "status_code": 200,
                    "requested_url": "https://index.commoncrawl.org/CC-MAIN-2026-43-index",
                },
            )["object"]
        )
    doc["patterns"].append(pdf_pattern)
    doc["feed"] = {"sha256": feed_sha, "bytes": len(feed)}

    # Without pdf_only_receipt_keys, both PDF-only items remain pdf-identity-unresolved
    manifest.write_text(json.dumps(doc))
    out_base = tmp_path / "base"
    out_base.mkdir()
    base_rows = read_selected_cards(manifest, archive_root=raw, congresses=[119], output_dir=out_base)
    assert [r["publication_id"] for r in base_rows] == ["62720"]

    # With pdf_only_receipt_keys, 62593 is added additively while 62594 (wrong date) stays unresolved
    doc["pdf_only_receipt_keys"] = pdf_receipt_keys
    manifest.write_text(json.dumps(doc))
    out_add = tmp_path / "additive"
    out_add.mkdir()
    add_rows = read_selected_cards(manifest, archive_root=raw, congresses=[119], output_dir=out_add)
    by_pid = {r["publication_id"]: r for r in add_rows}
    assert set(by_pid) == {"62720", "62593"}
    assert by_pid["62720"] == base_rows[0]  # Existing HTML-backed row is bit-for-bit unchanged
    pdf_row = by_pid["62593"]
    assert pdf_row["card_source"] == "pdf-native"
    assert pdf_row["page_number"] == 1
    assert pdf_row["identity_rule"] == FEED_CONTENT_CORROBORATED_RULE
    assert pdf_row["extraction_method"] == "spicy_docs.sources.cbo_pdf_cards.read_native_pdf_card"
    assert pdf_row["schema_version"] == "cbo-estimate-card/1"
    assert pdf_row["windows"][1]["spending_subject_to_appropriation_outlays_literal"] == "*"
    assert pdf_row["windows"][1]["spending_subject_to_appropriation_outlays_millions"] is None
    report = json.loads((out_add / "cbo_archive_population.json").read_text())
    assert report["counts"] == {"card": 1, "absent": 1, "pdf-card": 1, "pdf-identity-unresolved": 1}

    # Merge + native receipt round-trip passes cleanly
    merged_path = merge_card_rows(out_add, add_rows, download_prior=lambda *_: False)
    subject, receipts = write_congress_dataset(
        merged_path, tmp_path / "native-add", dataset="cbo_estimate_cards", generation_id="g-add"
    )
    restored = restore_processing_input(
        subject, receipts, tmp_path / "restored-add.parquet", dataset="cbo_estimate_cards", generation_id="g-add"
    )
    assert pq.read_table(restored).equals(pq.read_table(merged_path))
