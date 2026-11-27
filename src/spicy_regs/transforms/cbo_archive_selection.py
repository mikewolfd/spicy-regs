"""Explicit archived CBO population through the ordinary bill-family build.

Acquisition and table reading share the same pinned feed/index selection. Neither
operation chooses shared catalog/publication state. Bill/month PDFs remain gaps.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace
from urllib.parse import urlsplit

from spicy_docs.reading.pdf_bytes import check_pdf_bytes
from spicy_docs.sources.cbo import (
    CboCostEstimatesFeed,
    CboSourceError,
    archived_publication_id,
    card_row,
    cbo_feed_bills,
    feed_item_pub_date,
    parse_cbo_cost_estimates_feed,
    select_estimate_pdf,
)
from spicy_docs.sources.web_archive import cc_record_locator, parse_cc_index, parse_cc_num_pages, read_warc_response
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts

from spicy_regs.transforms.bill_family_cbo import ArchiveSweep, latest_capture, _receipt
from spicy_regs.transforms.cbo_archive_cards import WARC_RECORD_ID, read_retained_cards

MAX_SELECTION_BYTES = 64 * 1024 * 1024
MAX_PDF_ONLY_BYTES = 32 * 1024 * 1024


def _original(store, item):
    size = item["bytes"]
    if type(size) is not int or not 0 <= size <= MAX_SELECTION_BYTES:
        raise ValueError("CBO selected original exceeds the input bound")
    with store.open(item["sha256"]) as stream:
        body = stream.read(size + 1)
    if len(body) != size or hashlib.sha256(body).hexdigest() != item["sha256"]:
        raise ValueError("CBO selected original differs from its retained pin")
    return body


@dataclass(frozen=True)
class Selection:
    document: dict
    feed: CboCostEstimatesFeed
    sweep: ArchiveSweep
    archive_root: Path


def load_selection(manifest_path: Path, archive_root: Path) -> Selection:
    document = json.loads(manifest_path.read_text())
    congress = document["congress"]
    if type(congress) is not int or congress < 108:
        raise ValueError("CBO selection must name its feed Congress")
    store = CentralRawArchiveStore(archive_root, create=False)
    feed_pin = document["feed"]
    feed = parse_cbo_cost_estimates_feed(_original(store, feed_pin))
    candidates, listed_by, counts, read = set(), {}, {}, set()
    total = feed_pin["bytes"]
    for _, record in iter_acquisition_receipts(archive_root, receipt_keys=document["index_receipt_keys"]):
        receipt = record["receipt"]
        if receipt.get("dataset_id") not in {"cc-index", "cc-index-pages"} or len(record["files"]) != 1:
            raise ValueError("CBO index selection requires explicit index originals")
        item = record["files"][0]
        total += item["bytes"]
        if total > MAX_SELECTION_BYTES:
            raise ValueError("CBO feed/index selection exceeds the 64-MiB bound")
        body = _original(store, item)
        crawl, pattern = receipt["crawl_id"], receipt["original_location"]
        if crawl not in document["crawls"] or pattern not in document["patterns"]:
            raise ValueError("CBO index receipt lies outside the declared population")
        if receipt.get("status_code", receipt.get("http_status")) != 200:
            continue
        if receipt["dataset_id"] == "cc-index-pages":
            pages = parse_cc_num_pages(body)
            if (crawl, pattern) in counts and counts[crawl, pattern] != pages:
                raise ValueError("Conflicting Common Crawl index page counts")
            counts[crawl, pattern] = pages
        else:
            page = int(receipt["native_id"].rsplit(":", 1)[1])
            parsed = parse_cc_index(body, crawl_id=crawl)
            read.add((crawl, pattern, page))
            index = SimpleNamespace(sha256="sha256:" + item["sha256"], requested_url=receipt["requested_url"])
            for candidate in parsed:
                candidates.add(candidate)
                listed_by[candidate] = index
    unread = []
    for crawl in document["crawls"]:
        for pattern in document["patterns"]:
            count = counts.get((crawl, pattern))
            if count is None or any((crawl, pattern, page) not in read for page in range(count)):
                unread.append((crawl, pattern, "unread retained index"))
    sweep = ArchiveSweep(
        tuple(sorted(candidates, key=lambda c: (c.original_url, c.timestamp, c.filename or "", c.offset or 0))),
        listed_by,
        tuple(unread),
    )
    return Selection(document, feed, sweep, archive_root)


def _groups(selection):
    pages, pdfs = defaultdict(list), defaultdict(list)
    for candidate in selection.sweep.candidates:
        identifier = archived_publication_id(candidate.original_url)
        if identifier is not None:
            pages[identifier].append(candidate)
        elif candidate.status == 200 and candidate.mime == "application/pdf":
            pdfs[urlsplit(candidate.original_url).path].append(candidate)
    return pages, pdfs


def _read_retained_pdf_only_cards(
    selection: Selection,
    *,
    feed_bills,
    pdfs: Mapping[str, list],
    items_by_pid: Mapping[str, object],
    congress: int,
) -> tuple[dict[str, dict], list[dict]]:
    """Read explicitly selected PDF-only WARC receipts with dual-backend native verification and feed corroboration.

    Only receipts named in ``pdf_only_receipt_keys`` are read. Missing or damaged
    originals and weak archive identity abort; content-corroboration or parser
    refusals stay explicit outcomes and never emit unverified card rows.
    """
    from spicy_docs.extraction.pages import DefaultReader
    from spicy_docs.extraction.pypdf import PypdfReader
    from spicy_docs.sources.cbo_pdf_cards import identity_from_feed_pdf, parse_pdf_card_text

    pdf_keys = selection.document.get("pdf_only_receipt_keys")
    if not pdf_keys:
        return {}, []
    if not isinstance(pdf_keys, list) or not 1 <= len(pdf_keys) <= 1000:
        raise ValueError("pdf_only_receipt_keys must list between 1 and 1000 retained PDF receipts")
    store = CentralRawArchiveStore(selection.archive_root, create=False)
    captures = defaultdict(list)
    observed_bytes = 0
    for _, document in iter_acquisition_receipts(selection.archive_root, receipt_keys=pdf_keys):
        receipt = document["receipt"]
        if receipt.get("dataset_id") != "cbo-estimate-pdf" or len(document["files"]) != 1:
            raise ValueError("Expected one CBO estimate PDF original per pdf_only_receipt_keys entry")
        item_file = document["files"][0]
        size = item_file["bytes"]
        if size > MAX_PDF_ONLY_BYTES - observed_bytes:
            raise ValueError("Retained PDF-only CBO originals exceed the 32-MiB input bound")
        with store.open(item_file["sha256"]) as stream:
            body = stream.read(size + 1)
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        if len(body) != size or digest.removeprefix("sha256:") != item_file["sha256"]:
            raise ValueError("Retained PDF-only CBO original differs from its receipt")
        observed_bytes += size
        response = read_warc_response(body)
        string_fields = (
            "native_id",
            "original_publisher_url",
            "archive_timestamp",
            "warc_record_id",
            "index_digest",
            "payload_sha1",
            "index_sha256",
            "warc_filename",
            "crawl_id",
        )
        if not all(isinstance(receipt.get(name), str) and receipt[name].strip() for name in string_fields):
            raise ValueError("Weak or conflicting CBO PDF archive identity")
        identifier = receipt["native_id"]
        timestamp = receipt["archive_timestamp"]
        if (
            receipt.get("authority") != "archive"
            or receipt.get("archive_operator") != "Common Crawl"
            or not re.fullmatch(r"[1-9][0-9]{0,8}", identifier)
            or WARC_RECORD_ID.fullmatch(response.record_id) is None
            or item_file.get("native_id", identifier) != identifier
            or response.target_uri != receipt.get("original_publisher_url")
            or not re.fullmatch(r"[0-9]{14}", timestamp)
            or datetime.fromisoformat(response.warc_date.replace("Z", "+00:00")).strftime("%Y%m%d%H%M%S") != timestamp
            or response.record_id != receipt.get("warc_record_id")
            or response.payload_digest != receipt.get("index_digest", "").removeprefix("sha1:")
            or response.payload_digest != receipt.get("payload_sha1")
            or response.http_status != 200
            or type(receipt.get("warc_length")) is not int
            or receipt["warc_length"] != size
            or type(receipt.get("warc_offset")) is not int
            or receipt["warc_offset"] < 0
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", receipt.get("index_sha256", ""))
            or not receipt["warc_filename"].startswith("crawl-data/" + receipt["crawl_id"] + "/")
        ):
            raise ValueError("Weak or conflicting CBO PDF archive identity")
        cc_record_locator(receipt["warc_filename"])
        check_pdf_bytes(response.payload, error_type=ValueError, label=f"archived CBO PDF {identifier}")
        provenance = {
            name: receipt[name]
            for name in (
                "authority",
                "archive_operator",
                "original_publisher_url",
                "archive_timestamp",
                "index_sha256",
                "index_digest",
                "crawl_id",
                "warc_filename",
                "warc_offset",
                "warc_length",
            )
        }
        provenance.update(
            source_sha256=digest,
            source_locator=response.record_id + "#payload/page=1",
            receipt_object=document["receipt_object"],
        )
        captures[identifier].append((timestamp, digest, response, provenance))

    qualified_rows: dict[str, dict] = {}
    pdf_outcomes: list[dict] = []
    for identifier, versions in sorted(captures.items()):
        feed_item = items_by_pid.get(identifier)
        if feed_item is None:
            raise ValueError(f"Retained PDF-only CBO receipt is outside the selected feed: {identifier}")
        by_timestamp = defaultdict(set)
        for timestamp, digest, _, _ in versions:
            by_timestamp[timestamp].add(digest)
        if any(len(digests) != 1 for digests in by_timestamp.values()):
            raise ValueError(f"Conflicting CBO PDF originals at one archive timestamp: {identifier}")
        timestamp, digest, response, provenance = max(versions, key=lambda v: (v[0], v[1], v[3]["receipt_object"]))
        pdf_path = urlsplit(response.target_uri).path
        indexed = pdfs.get(pdf_path, ())
        chosen = latest_capture(indexed)
        if chosen is None or any(
            provenance[key] != value
            for key, value in {
                "warc_filename": chosen.filename,
                "warc_offset": chosen.offset,
                "warc_length": chosen.length,
                "index_digest": chosen.digest,
                "archive_timestamp": chosen.timestamp,
            }.items()
        ):
            pdf_outcomes.append(
                {
                    **provenance,
                    "publication_id": identifier,
                    "outcome": "not-retained",
                    "reason": "PDF receipt does not match latest indexed PDF capture",
                }
            )
            continue
        try:
            with DefaultReader().open(response.payload, "application/pdf") as document:
                pymupdf_text = document.page(1).native().text
            with PypdfReader().open(response.payload) as document:
                pypdf_text = document.read_page(1)
            identity = identity_from_feed_pdf(
                feed_item,
                feed_bills,
                pdf_url=response.target_uri,
                pdf_text=pymupdf_text,
                source_sha256=digest,
                source_locator=response.record_id + "#payload",
            )
            card_mu = parse_pdf_card_text(pymupdf_text, identity=identity)
            card_pp = parse_pdf_card_text(pypdf_text, identity=identity)
            if any(
                getattr(card_mu, field) != getattr(card_pp, field)
                for field in ("periods", "measures", "answers", "source_unit")
            ):
                raise CboSourceError("PDF-only native backends disagree on card values")
        except CboSourceError as refused:
            pdf_outcomes.append(
                {**provenance, "publication_id": identifier, "outcome": "refused", "reason": str(refused)}
            )
            continue
        timestamps = [candidate.timestamp for candidate in indexed if candidate.status == 200]
        row = card_row(
            card_mu,
            card_source="pdf-native",
            **provenance,
            congress=congress,
            publication_date=feed_item_pub_date(feed_item),
            publication_title=feed_item.title,
            publication_context=feed_item.description,
            captures=len(timestamps),
            earliest_timestamp=min(timestamps),
            page_number=1,
            extraction_method="spicy_docs.sources.cbo_pdf_cards.read_native_pdf_card",
            identity_rule=identity.rule,
        )
        qualified_rows[identifier] = row
        pdf_outcomes.append({**provenance, "publication_id": identifier, "outcome": "pdf-card", "reason": None})
    return qualified_rows, pdf_outcomes


def read_selected_cards(manifest_path, *, archive_root=None, congresses, output_dir, evidence=None):
    """Read every selected feed item, expose gaps and add qualified cards to the family.

    No manifest means no archive read, so prior cards remain. A supplied but broken
    selection aborts; it cannot masquerade as an empty successful population.
    """
    if manifest_path is None:
        return ()
    archive_root = Path(archive_root or os.environ.get("SPICYGOV_RAW_ROOT", Path.home() / "Work/corpora/spicyregs/raw"))
    selection = load_selection(Path(manifest_path), archive_root)
    congress = selection.document["congress"]
    if congress not in congresses:
        raise ValueError("Selected CBO feed Congress is outside this bill-family run")
    table, retained = read_retained_cards(Path(manifest_path), archive_root)
    held = {row["publication_id"]: row for row in table.to_pylist()}
    observed = {row["publication_id"]: row for row in retained["outcomes"]}
    pages, pdfs = _groups(selection)
    feed_bills = cbo_feed_bills(selection.feed, congress)
    bills = feed_bills.by_publication()
    items_by_pid = {item.publication_id: item for item in selection.feed.items}
    pdf_only_rows, pdf_only_outcomes = _read_retained_pdf_only_cards(
        selection, feed_bills=feed_bills, pdfs=pdfs, items_by_pid=items_by_pid, congress=congress
    )
    pdf_only_observed = {entry["publication_id"]: entry for entry in pdf_only_outcomes}
    rows, outcomes = [], []
    for item in selection.feed.items:
        identifier = item.publication_id
        chosen = latest_capture(pages.get(identifier, ()))
        pdf = select_estimate_pdf((), item, bills.get(identifier, ()), pdfs)
        outcome = {"publication_id": identifier, "bill_number": item.bill_number, "pdf_candidate": pdf}
        if chosen is None:
            if identifier in pdf_only_rows and pdf is not None:
                rows.append(pdf_only_rows[identifier])
                outcome.update(outcome="pdf-card", reason=None)
            elif identifier in pdf_only_observed:
                outcome.update(
                    outcome="pdf-identity-unresolved",
                    reason=pdf_only_observed[identifier]["reason"],
                )
            else:
                outcome.update(
                    outcome="pdf-identity-unresolved" if pdf else "neither-observed",
                    reason="bill/month is a candidate, not publication identity"
                    if pdf
                    else "no status-200 page or bill/month PDF in read indexes",
                )
        else:
            actual = observed.get(identifier)
            if actual is None or any(
                actual[key] != value
                for key, value in {
                    "warc_filename": chosen.filename,
                    "warc_offset": chosen.offset,
                    "warc_length": chosen.length,
                    "index_digest": chosen.digest,
                    "archive_timestamp": chosen.timestamp,
                }.items()
            ):
                outcome.update(outcome="not-retained", reason="latest indexed page original is not selected")
            else:
                outcome.update(outcome=actual["outcome"], reason=actual["reason"])
                if actual["outcome"] == "card":
                    row = held[identifier]
                    timestamps = [candidate.timestamp for candidate in pages[identifier] if candidate.status == 200]
                    row.update(
                        congress=congress,
                        publication_date=feed_item_pub_date(item),
                        publication_title=item.title,
                        publication_context=item.description,
                        captures=len(timestamps),
                        earliest_timestamp=min(timestamps),
                    )
                    rows.append(row)
        outcomes.append(outcome)
    if extra := held.keys() - {item.publication_id for item in selection.feed.items}:
        raise ValueError(f"Retained CBO cards are outside the selected feed: {sorted(extra)}")
    report = {
        "schema": "cbo-archive-population/1",
        "manifest_sha256": hashlib.sha256(Path(manifest_path).read_bytes()).hexdigest(),
        "feed": selection.document["feed"],
        "congress": congress,
        "population": len(outcomes),
        "counts": dict(Counter(row["outcome"] for row in outcomes)),
        "outcomes": outcomes,
        "unread_indexes": selection.sweep.unread,
        "retained": retained,
        "retained_pdf_only": pdf_only_outcomes,
        "source_accepted": False,
        "limits": "Full selected feed accounted; unretained originals, unresolved PDF identities and unread indexes remain gaps.",
    }
    path = Path(output_dir) / "cbo_archive_population.json"
    path.write_text(json.dumps(report, indent=2) + "\n")
    if evidence is not None:
        evidence.retain_file(path, stage="cbo-archive-population", raw_source=False)
    return rows


def acquire_missing_pages(selection, *, acquirer, evidence, checkpoint_path):
    """Resume by exact WARC coordinates before each permitted missing-page request.

    Caller supplies an admitted, bounded WebArchiveAcquirer. Each successful
    retained capture updates the owned checkpoint, including before interruption.
    Existing bytes are validated by the same retained-card reader before reuse.
    """
    checkpoint_path = Path(checkpoint_path).resolve()
    keys = list(selection.document["receipt_keys"])
    journals = []
    failures = []
    store = CentralRawArchiveStore(selection.archive_root)

    def reference(event):
        return store.record_receipt(
            files=[{"sha256": event["sha256"], "bytes": event["byte_size"]}],
            receipt=event,
        )["object"]

    if checkpoint_path.exists():
        checkpoint = json.loads(checkpoint_path.read_text())
        if checkpoint["feed"] != selection.document["feed"]:
            raise ValueError("CBO resume checkpoint belongs to another feed")
        keys = list(dict.fromkeys([*keys, *checkpoint["receipt_keys"]]))
        journals = checkpoint.get("capture_journals", [])
        # CaptureEvidence fsyncs each event before returning. Replay that exact
        # owned journal to recover a capture retained just before interruption
        # prevented the checkpoint update. Missing or damaged bytes refuse.
        for location in journals:
            journal = (checkpoint_path.parent / location).resolve()
            if not journal.is_relative_to(checkpoint_path.parent):
                raise ValueError("CBO resume journal must stay in the owned run directory")
            for line in journal.read_text().splitlines():
                event = json.loads(line)
                if event.get("event") == "capture" and event.get("dataset_id") == "cbo-publication-page":
                    key = reference(event)
                    if key not in keys:
                        keys.append(key)

    if evidence is not None:
        owner = getattr(evidence, "root", evidence)
        location = str((owner.artifact_dir / "journal.jsonl").resolve().relative_to(checkpoint_path.parent))
        journals = list(dict.fromkeys([*journals, location]))

    def checkpoint(reused=0):
        document = {
            **selection.document,
            "receipt_keys": keys,
            "reused": reused,
            "capture_journals": journals,
            "failures": failures,
        }
        temporary = checkpoint_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(document, indent=2) + "\n")
        temporary.replace(checkpoint_path)

    checkpoint()  # Write the current journal identity before the first request.
    # Reuse the maintained receipt/WARC/card validation, including observed absence.
    validation = checkpoint_path.with_suffix(".validation.json")
    validation.write_text(json.dumps({"receipt_keys": keys}))
    _, report = read_retained_cards(validation, selection.archive_root)
    held = {(row["warc_filename"], row["warc_offset"], row["warc_length"]): row for row in report["inputs"]}
    pages, _ = _groups(selection)
    reused = 0
    for item in selection.feed.items:
        chosen = latest_capture(pages.get(item.publication_id, ()))
        if chosen is None:
            continue
        coordinate = chosen.filename, chosen.offset, chosen.length
        if coordinate in held:
            original = held[coordinate]
            if (original["index_digest"], original["original_publisher_url"], original["archive_timestamp"]) != (
                chosen.digest,
                chosen.original_url,
                chosen.timestamp,
            ):
                raise ValueError("Conflicting CBO identity at retained WARC coordinates")
            reused += 1
            continue
        if evidence is None:
            raise ValueError("Missing CBO originals require an admitted CaptureEvidence acquisition")
        try:
            record = acquirer.acquire_cc_record(chosen)
            event = evidence.capture(
                record.capture,
                stage="cbo-archive-page",
                **_receipt(record, selection.sweep, native_id=item.publication_id, dataset_id="cbo-publication-page"),
            )
        except Exception as error:
            failures.append({"publication_id": item.publication_id, "error_type": type(error).__name__})
            checkpoint(reused)
            evidence.refusal(error, stage="cbo-archive-page")
            raise
        keys.append(reference(event))
        checkpoint(reused)
    checkpoint(reused)
    return keys


def merge_card_rows(output_dir, rows, *, download_prior, prior_present=None):
    """Use the maintained native Parquet merge; an older manifest cannot undo a revision."""
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_docs.schemas.cbo_estimate_cards import CBO_ESTIMATE_CARDS
    from spicy_docs.sources.cbo import validate_card
    from spicy_regs.contract_types import arrow_schema
    from spicy_regs.transforms.table_merge import merge_local_prior, prior_scratch_path

    name = CBO_ESTIMATE_CARDS.name
    prior = prior_scratch_path(output_dir, name)
    have_prior = prior_present is not False and (prior.exists() or download_prior(name + ".parquet", prior))
    held = {row["publication_id"]: row for row in pq.read_table(prior).to_pylist()} if have_prior else {}
    fresh = []
    for row in rows:
        validate_card(row)
        previous = held.get(row["publication_id"])
        if previous is not None:
            if previous["archive_timestamp"] > row["archive_timestamp"]:
                continue
            if (
                previous["archive_timestamp"] == row["archive_timestamp"]
                and previous["source_sha256"] != row["source_sha256"]
            ):
                raise ValueError("Conflicting CBO originals at the same archive timestamp")
        fresh.append(row)
    new = Path(output_dir) / ("_" + name + "_new.parquet")
    output = Path(output_dir) / (name + ".parquet")
    pq.write_table(pa.Table.from_pylist(fresh, schema=arrow_schema(CBO_ESTIMATE_CARDS)), new, compression="zstd")
    with duckdb.connect() as connection:
        merge_local_prior(
            connection,
            columns=CBO_ESTIMATE_CARDS.columns,
            identity=CBO_ESTIMATE_CARDS.identity,
            order_by="archive_timestamp",
            prior_file=prior if have_prior else None,
            new_file=new,
            out_file=output,
        )
    for scratch in (prior, new):
        scratch.unlink(missing_ok=True)
    return output
