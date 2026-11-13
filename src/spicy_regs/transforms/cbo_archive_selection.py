"""Explicit archived CBO population through the ordinary bill-family build.

Acquisition and table reading share the same pinned feed/index selection. Neither
operation chooses shared catalog/publication state. Bill/month PDFs remain gaps.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import hashlib
import json
import os
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit

from spicy_docs.sources.cbo import (
    CboCostEstimatesFeed,
    archived_publication_id,
    cbo_feed_bills,
    feed_item_pub_date,
    parse_cbo_cost_estimates_feed,
    select_estimate_pdf,
)
from spicy_docs.sources.web_archive import parse_cc_index, parse_cc_num_pages
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, iter_acquisition_receipts

from spicy_regs.transforms.bill_family_cbo import ArchiveSweep, latest_capture, _receipt
from spicy_regs.transforms.cbo_archive_cards import read_retained_cards

MAX_SELECTION_BYTES = 64 * 1024 * 1024


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
    bills = cbo_feed_bills(selection.feed, congress).by_publication()
    rows, outcomes = [], []
    for item in selection.feed.items:
        identifier = item.publication_id
        chosen = latest_capture(pages.get(identifier, ()))
        pdf = select_estimate_pdf((), item, bills.get(identifier, ()), pdfs)
        outcome = {"publication_id": identifier, "bill_number": item.bill_number, "pdf_candidate": pdf}
        if chosen is None:
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
    from spicy_docs.schemas.cbo_estimate_cards import CBO_ESTIMATE_CARDS, validate_card
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
    return output
