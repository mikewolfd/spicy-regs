"""``cbo_cost_estimates`` from both of its routes: each bill's BILLSTATUS and CBO's own per-Congress feed.

BILLSTATUS states no CBO estimate for the 112th and 113th Congresses, and elsewhere lacks a few that CBO's feed
lists (spicy-docs 0.52.0, "CBO's own feed is read for every Congress"). So the bill family reads the keyless feed
of every Congress it is scoped to, once a run, shapes its items through spicy-docs'
``build_cbo_feed_cost_estimates``, and merges them with the BILLSTATUS rows through ``merge_cbo_cost_estimates``,
which keeps the BILLSTATUS row wherever both routes state one ``(bill_id, publication_id)``.

Three facts come from this host, because spicy-docs reads no table:

* ``law_bills``, ``laws.law_id`` to ``laws.bill_id`` from the published ``laws`` table, lets a title leading
  with a public law name that law's bill (``title_bill_id``, and ``found_by`` ``title_law``). It is read
  best-effort, like the ``laws`` join on ``congress_bills``: without it those titles stay unmapped.
* A feed row's report citations are its bill's own BILLSTATUS ``<committeeReports>``: this run's parsed status
  where the run read the bill's folder, else the citations the bill's published rows carry, re-shaped from their
  ``citation`` strings (a round trip that reproduces all 12,732 live rows, 2026-09-29). A bill neither names
  publishes NULL there, as the contract says.
* Precedence against rows this run did not rebuild: a feed row is merged with the prior BILLSTATUS rows of the
  bills this run did not re-read, so a prior BILLSTATUS row still wins its identity.

The replacement scope is ``(bill_id, source)``: a re-read bill replaces its prior BILLSTATUS rows, and a Congress
whose feed was read replaces every prior feed row of that Congress, so an item CBO withdraws or re-files leaves.
A Congress whose feed could not be read keeps its prior feed rows. Cost: one small request per scoped Congress
(the twelve feeds are 0.36-0.59 MB, 2026-09-28) and one pass over the prior table's rows of those Congresses.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit

from loguru import logger
from spicy_docs.interpretation.bill_family import FamilyRefusal, build_cbo_feed_cost_estimates
from spicy_docs.schemas.cost_estimate_tables import (
    CBO_COST_ESTIMATES,
    CBO_FEED,
    ESTIMATE_SOURCES,
    merge_cbo_cost_estimates,
)
from spicy_docs.sources.cbo import CboAcquirer, CboBudget, CboSourceError
from spicy_docs.sources.congress.bill_status import BillIdentity
from spicy_docs.transport.credentials import scrub_credential


if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

#: One feed request a Congress; each measured feed is under 0.6 MB, so 8 MB bounds a feed with room to grow.
CBO_FEED_BUDGET = CboBudget(max_requests=1, max_bytes=8 * 1024 * 1024, timeout_seconds=60.0,
                            min_request_interval_seconds=1.0)

#: The feed exists from the 108th Congress on, the same floor as BILLSTATUS bulk.
CBO_FEED_FLOOR = 108


def law_bills_from(path: Path | None) -> dict[str, str] | None:
    """``laws.law_id`` -> ``laws.bill_id`` from the published ``laws`` table, or ``None`` without a readable one.

    Best-effort, as the ``laws`` join on ``congress_bills`` is: an absent or unreadable table leaves a title that
    leads with a public law unmapped rather than failing the run.
    """
    if path is None:
        return None
    import duckdb

    try:
        rows = duckdb.sql(
            f"SELECT law_id, bill_id FROM read_parquet('{path}') WHERE law_id IS NOT NULL AND bill_id IS NOT NULL"
        ).fetchall()
    except duckdb.Error as error:
        logger.warning("Bill family: laws unreadable, so no CBO title reads through a law: {}", error)
        return None
    return dict(rows)


def prior_citations(path: Path | None, congresses: Collection[int]) -> dict[BillIdentity, tuple[str, ...]]:
    """Each bill's report citations as its published rows state them, for the bills of ``congresses``.

    The rows carry the bill's own BILLSTATUS citations, shaped; the ``citation`` strings re-shape to the same
    JSON. A row with NULL citations names nothing, so the bill is left for its next status read.
    """
    if path is None or not congresses:
        return {}
    import duckdb

    wanted = ", ".join(f"'{congress}'" for congress in sorted(congresses))
    found: dict[BillIdentity, tuple[str, ...]] = {}
    for bill, cited in duckdb.sql(
        f"SELECT bill_id, any_value(report_citations_json) FROM read_parquet('{path}') "
        f"WHERE congress IN ({wanted}) AND report_citations_json IS NOT NULL GROUP BY bill_id"
    ).fetchall():
        found[BillIdentity.from_bill_id(bill)] = tuple(entry["citation"] for entry in json.loads(cited))
    return found


@dataclass(frozen=True, slots=True)
class FeedRead:
    """What this run read from CBO's feeds: the estimate rows, every item as listed, refusals, the Congresses read."""

    rows: tuple[dict[str, Any], ...]
    refusals: tuple[FamilyRefusal, ...]
    congresses: frozenset[int]
    #: ``cbo_feed_items``: one row per item of each feed read, whether or not it names a bill (spicy-docs 0.54.0).
    items: tuple[dict[str, Any], ...] = ()


def read_cbo_feeds(
    congresses: Iterable[int],
    *,
    citations: Mapping[BillIdentity, Sequence[str]],
    law_bills: Mapping[str, str] | None,
    acquirer: Any = None,
    evidence: CaptureEvidence | None = None,
) -> FeedRead:
    """Fetch and shape each Congress's feed once; a feed that cannot be read is logged and keeps its prior rows."""
    scoped = sorted(congress for congress in set(congresses) if congress >= CBO_FEED_FLOOR)
    if not scoped:
        return FeedRead((), (), frozenset())
    items: list[dict[str, Any]] = []
    if acquirer is None:
        acquirer = CboAcquirer(
            budget=CboBudget(
                max_requests=len(scoped),
                max_bytes=CBO_FEED_BUDGET.max_bytes,
                timeout_seconds=CBO_FEED_BUDGET.timeout_seconds,
                min_request_interval_seconds=CBO_FEED_BUDGET.min_request_interval_seconds,
            ),
            transport=None if evidence is None else evidence.transport(stage="cbo-feed",
                                                                       max_bytes=CBO_FEED_BUDGET.max_bytes),
        )
    rows: list[dict[str, Any]] = []
    refusals: list[FamilyRefusal] = []
    read: set[int] = set()
    for congress in scoped:
        try:
            feed = acquirer.acquire_per_congress_feed(congress).feed
        except (CboSourceError, OSError) as error:
            if evidence is not None:
                evidence.refusal(error, stage="cbo-feed")
            logger.warning("Bill family: CBO's {} feed unread, so its prior feed rows stand: {}", congress,
                           scrub_credential(str(error), ""))
            continue
        tables = build_cbo_feed_cost_estimates(feed, congress, report_citations=citations, law_bills=law_bills)
        rows.extend(tables.cbo_cost_estimates)
        items.extend(tables.cbo_feed_items)
        refusals.extend(tables.refusals)
        read.add(congress)
        logger.info("Bill family: CBO's {} feed — {:,} items, {:,} estimate rows, {:,} refused", congress,
                    len(feed.items), len(tables.cbo_cost_estimates), len(tables.refusals))
    return FeedRead(tuple(rows), tuple(refusals), frozenset(read), tuple(items))


type CostEstimateScope = tuple[tuple[str, str], set[tuple[str, str]]]


def cost_estimate_publication(
    billstatus: Sequence[Mapping[str, Any]],
    feed: FeedRead,
    *,
    reread: Collection[str],
    prior: Path | None,
) -> tuple[list[Mapping[str, Any]], CostEstimateScope]:
    """This run's ``cbo_cost_estimates`` rows and the ``(bill_id, source)`` scope they replace.

    ``billstatus`` are the rows this run's status reads shaped, ``reread`` the bills whose BILLSTATUS outcome was
    listed (their prior BILLSTATUS rows are replaced). A feed row survives only where ``merge_cbo_cost_estimates``
    keeps it over this run's BILLSTATUS rows and the prior BILLSTATUS rows of every bill not re-read.
    """
    held: list[dict[str, Any]] = []
    prior_feed_bills: set[str] = set()
    if prior is not None and feed.congresses:
        import duckdb

        wanted = ", ".join(f"'{congress}'" for congress in sorted(feed.congresses))
        relation = duckdb.sql(f"SELECT * FROM read_parquet('{prior}') WHERE congress IN ({wanted})")
        for row in relation.to_arrow_table().to_pylist():
            if row["source"] == CBO_FEED:
                prior_feed_bills.add(row["bill_id"])
            elif row["bill_id"] not in reread:
                held.append(row)
    fresh = [dict(row) for row in billstatus]
    feed_rows = list(feed.rows)
    kept = {CBO_COST_ESTIMATES.key(row): row for row in merge_cbo_cost_estimates([*fresh, *held, *feed_rows])}
    added = [row for row in feed_rows if kept[CBO_COST_ESTIMATES.key(row)] is row]
    if feed_rows:
        logger.info("Bill family: {:,} of {:,} CBO feed rows state an estimate no BILLSTATUS row states",
                    len(added), len(feed_rows))
    scope = {(bill, source) for bill in reread for source in ESTIMATE_SOURCES if source != CBO_FEED}
    scope |= {(bill, CBO_FEED) for bill in prior_feed_bills}
    return [*fresh, *added], (("bill_id", "source"), scope)


# ---------------------------------------------------------------------------
# The archive route (ticket ss-7ovu): CBO's estimate pages and PDFs as Common Crawl captured them. cbo.gov is never
# requested; spicy-docs' WebArchiveAcquirer refuses any host but the archives', and one acquirer's budget bounds a
# whole run. Every capture is retained through CaptureEvidence, so the raw store holds the archive's own WARC member
# (the original) and a receipt naming the archive coordinates; the decoded payload is derived and not stored twice.

#: The two prefixes one Common Crawl index query per crawl reads: the publication pages and every PDF.
CBO_ARCHIVE_PATTERNS = ("www.cbo.gov/publication/*", "www.cbo.gov/system/files/*")
#: Bounds of the first bounded acquisition (owner approval for ss-7ovu D12): 20 pages, 20 PDFs.
CBO_ARCHIVE_MAX_PAGES = 20
CBO_ARCHIVE_MAX_PDFS = 20
#: The publisher binding of every archived capture: retained in full, as the owner accepted the archive route (Q21).
CBO_ARCHIVE_PUBLISHER = "cbo-web-archive"
CBO_ARCHIVE_POLICY = "cbo-archive-route-q21/1"


def cbo_archive_evidence(evidence: Any) -> Any:
    """The run's evidence bound to the archive route: full retention under the owner's Q21 decision."""
    from importlib.metadata import version

    return evidence.for_source(CBO_ARCHIVE_PUBLISHER, "full", parser_version=version("spicy-docs"),
                               policy_decision_id=CBO_ARCHIVE_POLICY)


@dataclass(frozen=True, slots=True)
class ArchiveSweep:
    """Every index row the sweep read, the retained index answer that listed each, and the crawls left unread."""

    candidates: tuple[Any, ...]
    listed_by: Mapping[Any, Any]
    unread: tuple[tuple[str, str, str], ...]


def sweep_cbo_archive_index(crawls: Sequence[str], *, acquirer: Any, evidence: Any, reserve: int,
                            patterns: Sequence[str] = CBO_ARCHIVE_PATTERNS) -> ArchiveSweep:
    """Read each crawl's index once per prefix, newest crawl first, keeping ``reserve`` requests for the captures.

    ``showNumPages`` precedes each query so a multi-page answer is read whole, never silently cut at page 0. A
    crawl whose index fails (Common Crawl answers 504 under load) is recorded as a refusal and listed in ``unread``;
    the sweep stops before a crawl it could not finish within the budget left above ``reserve``.
    """
    import httpx
    from spicy_docs.sources.web_archive import WebArchiveError

    candidates: list[Any] = []
    listed_by: dict[Any, Any] = {}
    unread: list[tuple[str, str, str]] = []
    for crawl in crawls:
        for pattern in patterns:
            if acquirer.remaining_requests - reserve < 2:
                unread.append((crawl, pattern, "budget"))
                continue
            try:
                pages, counted = acquirer.acquire_cc_num_pages(crawl, pattern)
                evidence.capture(counted, stage="cbo-archive-index", authority="archive",
                                 archive_operator="Common Crawl", crawl_id=crawl, dataset_id="cc-index-pages",
                                 native_id=f"{crawl}:{pattern}", original_location=pattern)
                if acquirer.remaining_requests - reserve < pages:
                    unread.append((crawl, pattern, "budget"))
                    continue
                for page in range(pages):
                    read = acquirer.acquire_cc_index(crawl, pattern, page=page)
                    evidence.capture(read.capture, stage="cbo-archive-index", authority="archive",
                                     archive_operator="Common Crawl", crawl_id=crawl, dataset_id="cc-index",
                                     native_id=f"{crawl}:{pattern}:{page}", original_location=pattern,
                                     index_rows=len(read.candidates))
                    candidates.extend(read.candidates)
                    listed_by.update(dict.fromkeys(read.candidates, read.capture))
            except (WebArchiveError, httpx.HTTPError, OSError) as error:
                # The index answers 502/504 under load (5 of 22 index requests, 2026-10-30): a gap, not a stop.
                evidence.refusal(error, stage="cbo-archive-index")
                unread.append((crawl, pattern, type(error).__name__))
    return ArchiveSweep(tuple(candidates), listed_by, tuple(unread))


def latest_capture(candidates: Iterable[Any]) -> Any | None:
    """The capture a row is read from: the latest status-200 one, a URL without a query before one with."""
    usable = [candidate for candidate in candidates if candidate.status == 200]
    return max(usable, key=lambda c: ("?" not in c.original_url, c.timestamp), default=None)


@dataclass(frozen=True, slots=True)
class ArchivedCard:
    """One sampled publication: its page capture's outcome, the card where gated, and its PDF where archived."""

    publication_id: str
    outcome: str
    reason: str | None = None
    card: Any = None
    page: Any = None
    pdf: Any = None
    pdf_path: str | None = None
    match_rule: str | None = None
    captures: int = 0


def _receipt(record: Any, sweep: ArchiveSweep, *, native_id: str, dataset_id: str) -> dict[str, Any]:
    candidate = record.candidate
    return {**record.receipt_fields(sweep.listed_by.get(candidate)), "native_id": native_id,
            "dataset_id": dataset_id, "source_record_key": f"{candidate.crawl_id}:{candidate.filename}:"
            f"{candidate.offset}"}


def read_cbo_archive_cards(items: Sequence[Any], congress: int, sweep: ArchiveSweep, *, acquirer: Any,
                           evidence: Any, max_pages: int = CBO_ARCHIVE_MAX_PAGES,
                           max_pdfs: int = CBO_ARCHIVE_MAX_PDFS) -> tuple[ArchivedCard, ...]:
    """Fetch, retain and gate the archived page of each item, then its archived PDF; at most the stated counts.

    One dict join of the sweep's rows by publication id and by PDF path, so the cost is linear in index rows plus
    sampled items. Each outcome is ``card``, ``absent`` (a genuine page without a card), ``refused`` (the gate or
    a digest check said no), ``unarchived`` or ``not-fetched`` (archived, past ``max_pages``); a refusal is retained as evidence and never becomes a card.
    """
    import httpx
    from spicy_docs.reading.pdf_bytes import check_pdf_bytes
    from spicy_docs.sources.cbo import (
        CboCardAbsent,
        CboCostEstimatesFeed,
        archived_publication_id,
        cbo_feed_bills,
        parse_cbo_estimate_card,
        select_estimate_pdf,
    )
    from spicy_docs.sources.web_archive import WebArchiveError

    pages: dict[str, list[Any]] = {}
    pdfs: dict[str, list[Any]] = {}
    for candidate in sweep.candidates:
        identifier = archived_publication_id(candidate.original_url)
        if identifier is not None:
            pages.setdefault(identifier, []).append(candidate)
        elif candidate.mime == "application/pdf" and candidate.status == 200:
            pdfs.setdefault(urlsplit(candidate.original_url).path, []).append(candidate)
    item_bills = cbo_feed_bills(CboCostEstimatesFeed(tuple(items)), congress).by_publication()
    results: list[ArchivedCard] = []
    fetched_pages = fetched_pdfs = 0
    for item in items:
        chosen = latest_capture(pages.get(item.publication_id, ()))
        if chosen is None:
            results.append(ArchivedCard(item.publication_id, "unarchived"))
            continue
        if fetched_pages >= max_pages:
            results.append(ArchivedCard(item.publication_id, "not-fetched", "page budget spent"))
            continue
        fetched_pages += 1
        captures = sum(1 for c in pages[item.publication_id] if c.status == 200)
        try:
            page = acquirer.acquire_cc_record(chosen)
            evidence.capture(page.capture, stage="cbo-archive-page",
                             **_receipt(page, sweep, native_id=item.publication_id, dataset_id="cbo-publication-page"))
        except (WebArchiveError, httpx.HTTPError, OSError) as error:
            evidence.refusal(error, stage="cbo-archive-page")
            results.append(ArchivedCard(item.publication_id, "refused", str(error), captures=captures))
            continue
        try:
            card = parse_cbo_estimate_card(page.response.payload, publication_id=item.publication_id,
                                           archived_url=page.response.target_uri)
        except CboCardAbsent as absent:
            results.append(ArchivedCard(item.publication_id, "absent", str(absent), page=page, captures=captures))
            continue
        except CboSourceError as refused:
            evidence.event("source-validation", stage="cbo-archive-page", native_id=item.publication_id,
                           outcome="refused", message=str(refused))
            results.append(ArchivedCard(item.publication_id, "refused", str(refused), page=page, captures=captures))
            continue
        chosen_pdf = select_estimate_pdf(card.documents, item, item_bills.get(item.publication_id, ()), pdfs)
        pdf = None
        if chosen_pdf is not None and fetched_pdfs < max_pdfs:
            fetched_pdfs += 1
            try:
                pdf = acquirer.acquire_cc_record(latest_capture(pdfs[chosen_pdf[0]]))
                evidence.capture(pdf.capture, stage="cbo-archive-pdf",
                                 **_receipt(pdf, sweep, native_id=item.publication_id, dataset_id="cbo-estimate-pdf"),
                                 match_rule=chosen_pdf[1])
            except (WebArchiveError, httpx.HTTPError, OSError) as error:
                evidence.refusal(error, stage="cbo-archive-pdf")
                pdf = None
            else:
                # Retained before the check, as the page is: the spent request's bytes are the refusal's evidence.
                try:
                    check_pdf_bytes(pdf.response.payload, error_type=CboSourceError, label="archived CBO estimate PDF")
                except CboSourceError as refused:
                    evidence.event("source-validation", stage="cbo-archive-pdf", native_id=item.publication_id,
                                   outcome="refused", message=str(refused))
                    pdf = None
        results.append(ArchivedCard(item.publication_id, "card", card=card, page=page, pdf=pdf,
                                    pdf_path=None if chosen_pdf is None else chosen_pdf[0],
                                    match_rule=None if chosen_pdf is None else chosen_pdf[1], captures=captures))
    return tuple(results)


__all__ = [
    "CBO_ARCHIVE_MAX_PAGES",
    "CBO_ARCHIVE_MAX_PDFS",
    "CBO_ARCHIVE_PATTERNS",
    "CBO_FEED_BUDGET",
    "CBO_FEED_FLOOR",
    "ArchiveSweep",
    "ArchivedCard",
    "FeedRead",
    "cbo_archive_evidence",
    "cost_estimate_publication",
    "latest_capture",
    "law_bills_from",
    "prior_citations",
    "read_cbo_archive_cards",
    "read_cbo_feeds",
    "sweep_cbo_archive_index",
]
