"""Transform: build ``gao_reports.parquet`` from the GAO reports RSS feed, plus GovInfo's and GAO's own listings on request.

Produces a native subject table and shared receipts keyed on ``report_id`` (e.g.
``gao-26-107974``) — the Government Accountability Office oversight layer over
the rulemakings this dataset tracks. ``source`` names the route that supplied
each row: ``gao_rss`` (this feed), ``gao_repair`` (an explicit repair,
:mod:`spicy_regs.transforms.build_gao_target`), ``upstream_copy`` (the one-time
copy of upstream's rows the fork never captured), ``govinfo`` (GovInfo's closed
GAOREPORTS collection, :mod:`spicy_regs.sources.gao_govinfo`), ``gao_listing``
(GAO's own Month in Review and Annual Index, :mod:`spicy_regs.sources.gao_listing`),
or ``gao_major_rule_listing`` and ``gao_major_rule_index`` (GAO's two listings of
its major-rule reports, read with that walk).

**Incremental accumulator.** The GAO RSS feed is a recent-items window, not the
full archive, and GAO's bulk/search surfaces are bot-blocked (see
:mod:`spicy_regs.sources.gao_reports`). So rather than a watermark-bounded
re-fetch, each run parses the whole feed and appends previously unseen products
to the prior published table, deduping on ``report_id`` and preferring the
fresh row; over successive runs the table grows into a rolling history. Because
the merge is append-only against a growing table it never shrinks the output,
so it stays clear of the R2 catastrophic-shrink guard. ``agencies_json`` and
``topics_json`` are pinned but reserved: the feed carries no structured
agency/topic tags, so they default to ``[]`` until a later enrichment pass.

**One read of the prior, one row per product.** The prior table is read once into
memory, by ``report_id``. Each step below reads a product's row as the run has it
so far (its own rows over the prior's) and writes a whole row back, so two steps
that touch one product in one run compose. O(prior) memory and time; only the
capped MODS and product-page reads make a request per row.

**GovInfo history.** ``govinfo_history`` adds one walk of the GAOREPORTS
listing (17 keyed requests) to the run. Its rows fill only product ids no row
holds yet, so they never replace a row from another route or undo a GovInfo
row's MODS read. The collection is closed, so one run adds the history and
later feed runs carry it forward in the prior.

**GovInfo MODS.** ``govinfo_mods`` reads the MODS of up to
:data:`MODS_PER_RUN` history rows still unread (``report_number`` NULL), in
``report_id`` order, and fills ``abstract``, ``topics_json``, ``product_type``
and ``report_number`` from it (:mod:`spicy_regs.sources.gao_govinfo_mods`).
Each published run is durable progress, so a failed or capped run resumes at
the next unread row. A package GovInfo serves no MODS for stays unread.

**Held rows are merged cell by cell.** A feed item for a product already held
never replaces its row: it sets the cells it states (title, date, abstract,
url) on its own and upstream-copied rows, fills only NULL cells on rows another
route supplied, and never empties a cell or fills one with its placeholders.
The row keeps its ``source``. So a listing-held product the feed re-reads keeps
its report number, topics and product type.

**GAO's listing.** ``listing_run`` names a finished SpicyDocs walk of GAO's Month
in Review and Annual Index pages; the walk itself runs outside the rollup. Its
rows follow the GovInfo rule, one helper for both: they fill only product ids no
row holds yet, this run's feed and GovInfo rows included, so a later walk adds
new products and leaves held rows, a MODS read among them, alone. On a row it
holds, the listing fills only a NULL ``report_number``, never a stated one. Its
legal decisions go to ``gao_decisions`` (:data:`DECISIONS_OUTPUT`), which a run
without a walk carries forward unchanged.

**GAO's major-rule listings.** ``major_rule_run`` and ``old_index_run`` name
finished SpicyDocs walks of "Reports on Major Rules" and of GAO's index of
2000-12-15, read with ``listing_run`` and merged into its products by SpicyDocs
before the rule above applies (:mod:`spicy_regs.sources.gao_listing`): the
major-rule reports of 1996-2008 and twelve ordinary products join as rows. Two
cells of a held row can change. Where the listing's own row holds a heading the
Month in Review cut (it ends ``...``) and the merged heading goes on from
there, the title is made whole. And a report those listings state is labelled
Federal Agency Major Rule Report whatever route holds its row, the owner's
"trust GAO's listing" (2026-10-04): nine GovInfo rows on 2026-10-05, GAO-01-1024R
among them, which keep every other GovInfo cell.

**Major-rule letters.** ``major_rule_letters`` names captures of the reports'
product pages and PDFs, read by reference
(:mod:`spicy_regs.sources.gao_major_rule_letters`). Every row labelled Federal
Agency Major Rule Report, and every row GAO's major-rule listings name whatever
route holds it, takes its agency clause, RINs and Federal Register citations
from its letter, once per reader rule. The reading itself, each blank's reason
included, is a receipt field.

**GAO's product pages.** ``product_pages`` reads the newest pending rows' pages
through Zyte for the counts, page count and subject terms, the page outranking
the R package (:mod:`spicy_regs.sources.gao_product_pages`), and reads a new
major-rule report's letter from the same page. Off unless asked for, and then
only under a reader rule that module admits, after its known pages read as they
must, and for a product's first week against ``gao_recommendations``, read from
the published table. ``product_pages_undo`` puts back what a named day's or
rule's reads replaced.

**GAO's decision pages.** ``decision_pages`` names a local capture of the decision
pages (:mod:`spicy_regs.sources.gao_decision_pages`), read by reference with the
walk: each decision's caption completes a number list the listing cut and states
the day GAO decided it. A page the capture lacks, one with no caption and one the
reader refuses keep the listing's values, each counted (refusals by reason) in the
run journal, and the capture is an input of the generation by the receipts read.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from contextlib import nullcontext
from datetime import date
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import (
    gao_govinfo,
    gao_listing,
    gao_major_rule_letters,
    gao_product_pages,
    gao_r_package,
    r2,
)
from spicy_regs.transforms.government_receipts import internal_prior, receipt_builder
from spicy_regs.transforms.government_source_shapes import LEGACY_COLUMNS
from spicy_regs.sources.gao_decision_pages import DecisionPageCapture
from spicy_regs.sources.gao_reports import GaoReportsReader
from spicy_regs.transforms.table_merge import merge_local_prior

OUTPUT = "gao_reports.parquet"

#: The rows this builder writes, before they become the native subject and its receipt: one declaration, shared with
#: the mapper (``government_source_shapes``). The three counts are BIGINT and the rest VARCHAR; ``report_id`` is the
#: primary / dedup key.
COLUMNS = LEGACY_COLUMNS["gao_reports"]
#: The counts are whole numbers; every other column is VARCHAR.
COUNT_COLUMNS = ("recommendation_count", "matters_for_congress_count", "page_count")
_SCHEMA = pa.schema([(c, pa.int64() if c in COUNT_COLUMNS else pa.string()) for c in COLUMNS])

#: GAO's legal decisions from its own listing: spicy-docs' ``gao_decisions`` contract from 0.54.0 (DRY X1).
DECISIONS_OUTPUT = "gao_decisions.parquet"
#: The contract's two last columns, read from ``decision_status`` on every merged row each run, never carried.
DECISION_READINGS = ("outcome", "outcome_rule")
#: What a decision page's caption states, which a later listing read without the page keeps (``_build_decisions``).
PAGE_READINGS = ("b_numbers_json", "b_numbers_truncated", "decided_date")
#: Columns renamed in place, to the name a published prior still spells them with. ``released_date`` was
#: ``decision_date`` until 2026-10-03 (owner decision): it is the date GAO released the decision, not the date
#: GAO decided it (B-424477, decided 2026-08-07, released 2026-08-28).
DECISION_RENAMES = {"released_date": "decision_date"}

# GAO's reports RSS feed carries published products (reports & testimonies).
# The feed does not tag a finer product type, so we default to this label.
_DEFAULT_REPORT_TYPE = "Report"

SOURCE_FEED = "gao_rss"
SOURCE_REPAIR = "gao_repair"
SOURCE_UPSTREAM = "upstream_copy"

#: The cells a feed item states. Its report_type, agencies_json and topics_json are placeholders (``Report``,
#: ``[]``), and it states no product type or number.
_FEED_STATED = ("title", "published_date", "abstract", "url")
#: Routes whose stated cells a later feed read refreshes: the feed's own rows, and upstream's copies of its feed.
_FEED_REFRESHES = frozenset({SOURCE_FEED, SOURCE_UPSTREAM})
#: The route every route of ours outranks: the one-time copy of the CetiAlphaFive/gao R package. A later read of
#: ours takes a package row over, and the package's cells fill only what ours leave NULL.
_LOWEST = gao_r_package.SOURCE

#: MODS reads per run: at the three-a-second pace, about 37 minutes, so two
#: runs read the whole history and a failed run loses at most one batch.
MODS_PER_RUN = 6_500
#: How the Month in Review ends a heading it cut, at about 200 characters.
_CUT = "..."


def _published_date(pub_date: str | None) -> str | None:
    """Parse the RFC-822 ``pubDate`` to an ISO date string, or None.

    Falls back to None (rather than raising) on an unparseable value so one odd
    item can't fail the run.
    """
    if not pub_date:
        return None
    try:
        return parsedate_to_datetime(pub_date).date().isoformat()
    except (TypeError, ValueError):
        return None


def _shape(item: dict) -> dict:
    """Map one raw GAO RSS item onto the published column shape."""
    return {
        # spicy-docs reads the id off the item's link and refuses any link that is not the product's canonical URL.
        "report_id": item["product_id"],
        "title": item.get("title"),
        "report_type": _DEFAULT_REPORT_TYPE,
        "published_date": _published_date(item.get("pub_date")),
        "abstract": item.get("description"),
        # Reserved — the RSS feed carries no structured agency/topic tags.
        "agencies_json": "[]",
        "topics_json": "[]",
        "url": item.get("link"),
        "source": SOURCE_FEED,
        **dict.fromkeys(COLUMNS[COLUMNS.index("product_type"):]),
    }


def _held_rows(prior_file: Path) -> dict[str, dict]:
    """The prior table by ``report_id``, read once; a column the prior predates is None."""
    return {row["report_id"]: {**dict.fromkeys(COLUMNS), **row} for row in pq.read_table(prior_file).to_pylist()}


def _table(held: Mapping[str, dict], changed: Mapping[str, dict]) -> Iterator[dict]:
    """Every row as the run has it so far: its own, then the prior's it has not touched."""
    yield from changed.values()
    yield from (row for report_id, row in held.items() if report_id not in changed)


def _unheld(report_id: str, held: Mapping[str, dict], changed: Mapping[str, dict]) -> bool:
    """Whether no row of ours holds the product: none of this run's, and the prior's only from the lowest route."""
    return report_id not in changed and (report_id not in held or held[report_id]["source"] == _LOWEST)


def _over_lowest(held: Mapping[str, dict], rows: list[dict]) -> tuple[list[dict], int]:
    """Rows of ours for products the lowest route holds, each with its NULL cells filled from that route's row."""
    taken = {row["report_id"] for row in rows
             if row["source"] != _LOWEST and held.get(row["report_id"], row)["source"] == _LOWEST}
    merged = [
        {column: row.get(column) if row.get(column) is not None else held[row["report_id"]].get(column)
         for column in COLUMNS} if row["report_id"] in taken else row
        for row in rows
    ]
    return merged, len(taken)


def _feed_over_held(held: Mapping[str, dict], rows: list[dict]) -> tuple[list[dict], Counter[str]]:
    """Feed rows, each merged cell by cell over the row already held for its product, if any.

    A held row keeps every cell and its ``source``. The feed sets the cells it states: all of them on its own or an
    upstream-copied row, only the NULL ones on a row another route supplied (the listing, GovInfo, a repair).
    It never empties a cell, and never fills one with its placeholders.
    """
    counts: Counter[str] = Counter()
    merged = []
    for row in rows:
        old = held.get(row["report_id"])
        if old is None or old["source"] == _LOWEST:
            merged.append(row)
            continue
        counts["held"] += 1
        new = dict(old)
        for column in _FEED_STATED:
            if row[column] is not None and (old[column] is None or old["source"] in _FEED_REFRESHES):
                counts[f"set_{column}"] += new[column] != row[column]
                new[column] = row[column]
        merged.append(new)
    return merged, counts


def _govinfo_additions(
    held: Mapping[str, dict],
    changed: Mapping[str, dict],
    reader: gao_govinfo.PackageDiscoverySource,
    evidence: CaptureEvidence | None,
) -> list[dict]:
    """GovInfo rows for ids no row holds yet."""
    rows, counts = gao_govinfo.read_history(reader)
    added = [row for row in rows if _unheld(row["report_id"], held, changed)]
    counts["already_held"] = len(rows) - len(added)
    logger.info("GAO reports: GovInfo history {}", dict(counts))
    if evidence:
        evidence.event("govinfo-history", collection=gao_govinfo.COLLECTION, listed_since=gao_govinfo.LISTED_SINCE,
                       page_size=gao_govinfo.PAGE_SIZE, max_pages=gao_govinfo.MAX_PAGES, **counts)
    return added


def _cut_of(held: str | None, whole: str | None) -> bool:
    """Whether ``held`` is the Month in Review's cut of ``whole``: it ends ``...`` and ``whole`` goes on from there.

    The same cut heading read again is not whole: seven non-major-rule products' headings are cut in the Month in
    Review itself, and no other listing states them.
    """
    if not held or not whole or whole == held or not held.endswith(_CUT):
        return False
    stem = held.removesuffix(_CUT)
    return len(whole) > len(stem) and whole.startswith(stem)


def _listing_rows(
    held: Mapping[str, dict],
    changed: dict[str, dict],
    directory: Path,
    evidence: CaptureEvidence | None,
    pages: Path | None = None,
    major_rules: Path | None = None,
    old_index: Path | None = None,
) -> tuple[list[dict], gao_listing.MajorRuleReports]:
    """Fold GAO's listings into ``changed``; return their decisions and what the major-rule listings state.

    A listed product no row holds yet joins as the listing's row. On a row already held the listing fills a NULL
    ``report_number``, makes whole a title its own row holds cut, and labels a report GAO's major-rule listings
    name. Each decision is read against its captured page where ``pages`` names a capture, and each listing page
    read is retained as evidence. O(listed products).
    """
    rows, counts, run, stated = gao_listing.read_listing(directory, evidence, major_rules=major_rules,
                                                         old_index=old_index)
    added = [row for row in rows if _unheld(row["report_id"], held, changed)]
    counts["already_held"] = len(rows) - len(added)
    for row in rows:
        old = changed.get(row["report_id"]) or held.get(row["report_id"])
        if old is None or old["source"] == _LOWEST:
            continue  # the listing's own row joins, taking a package row over; see _over_lowest
        new = {}
        if old.get("report_number") is None and row["report_number"] is not None:
            new["report_number"] = row["report_number"]
            counts["report_number_filled"] += 1
        if old["source"] == gao_listing.SOURCE and _cut_of(old["title"], row["title"]):
            new["title"] = row["title"]
            counts["titles_made_whole"] += 1
        # "Trust GAO's listing" (owner, 2026-10-04): a report its major-rule listings state is one, whatever product
        # type the route that holds the row gave it (GovInfo's MODS calls GAO-01-1024R Correspondence).
        if row["report_id"] in stated.numbers and old.get("product_type") != row["product_type"]:
            new["product_type"] = row["product_type"]
            counts["major_rule_labelled"] += 1
        if new:
            changed[row["report_id"]] = {**old, **new}
    changed.update((row["report_id"], row) for row in added)
    decisions = _decision_rows(run, pages, evidence)
    counts.update(decision_rows=len(decisions), unnumbered_left_out=sum(o.product_number is None for o in run.others))
    logger.info("GAO reports: GAO listing {} (unfinished scopes left for a later run: {})", dict(counts),
                list(run.incomplete_scopes))
    if evidence:
        evidence.event("gao-listing", scopes_read=list(run.complete_scopes),
                       scopes_unfinished=list(run.incomplete_scopes), **counts)
    return decisions, stated


def _letter_reads(
    held: Mapping[str, dict],
    changed: dict[str, dict],
    letters: gao_major_rule_letters.LetterPages,
    named: Mapping[str, str],
    evidence: CaptureEvidence | None,
) -> None:
    """Read each major-rule report's letter not yet read under the current rule, where ``letters`` holds its page.

    A report is a row labelled Federal Agency Major Rule Report, whatever route holds it: the listing step has
    labelled every report GAO's major-rule listings name. The letter must carry the number those listings state
    (``named``, by id) where they state one, since a GovInfo row prints its number another way (``GAO/OGC-97-44``).
    O(rows) to find the reports, one page read per unread report.
    """
    from spicy_docs.sources.gao.major_rule_letters import LETTER_RULE
    from spicy_docs.sources.gao.month_in_review import MAJOR_RULE_REPORT
    from spicy_docs.sources.gao.native import gao_product_url

    counts: Counter[str] = Counter()
    for row in list(_table(held, changed)):
        report_id = row["report_id"]
        if row.get("product_type") != MAJOR_RULE_REPORT:
            continue
        counts["reports"] += 1
        if not gao_major_rule_letters.unread(row):
            counts["already_read"] += 1
            continue
        number = named.get(report_id) or row.get("report_number")
        page = letters.body(gao_product_url(report_id))
        if page is None or number is None:
            counts["page_not_held"] += 1
            continue
        cells = gao_major_rule_letters.letter_cells(page, product_id=report_id, product_number=number, pages=letters)
        gao_major_rule_letters.tally(counts, cells)
        changed[report_id] = {**row, **cells}
    captures = letters.record()
    logger.info("GAO reports: major-rule letters under {} {} from {}", LETTER_RULE, dict(counts), captures)
    if evidence:
        evidence.event(gao_major_rule_letters.STAGE, rule=LETTER_RULE, captures=captures, **counts)


def _product_page_reads(
    held: Mapping[str, dict],
    changed: dict[str, dict],
    acquirer: Any,
    evidence: CaptureEvidence | None,
    *,
    on: date,
    export: gao_product_pages.OpenRecommendations | None,
) -> None:
    """Read the known pages, then the newest pending rows' product pages, each setting what it states.

    The pass commits whole or not at all: a known page that reads otherwise stops it before a row is asked for, and
    a second refusal that says a table may not be whole stops it with nothing it read written. ``on`` is the day,
    and ``export`` GAO's open recommendations as the host holds them, for a product's first week. O(rows) to choose
    the pending ones, then at most :data:`gao_product_pages.PAGES_PER_RUN` requests, one per row.
    """
    from spicy_docs.sources.gao.month_in_review import MAJOR_RULE_REPORT
    from spicy_docs.sources.gao.product_details import (
        PRODUCT_PAGE_DETAILS_RULE,
        GaoProductDetailsError,
        GaoProductPageUnavailableError,
    )

    def stop(why: str, **fields: Any) -> None:
        logger.error("GAO reports: product pages under {} stopped, nothing written: {}", PRODUCT_PAGE_DETAILS_RULE, why)
        if evidence:
            evidence.event(gao_product_pages.STAGE, rule=PRODUCT_PAGE_DETAILS_RULE, stopped=why, **fields)

    for product_id, expected in gao_product_pages.KNOWN_PAGES:
        failure, witness = gao_product_pages.known_page_failure(acquirer, product_id, expected)
        if evidence and isinstance(witness, Exception):
            evidence.refusal(witness, stage=gao_product_pages.KNOWN_STAGE)
        elif evidence:
            evidence.capture(witness, stage=gao_product_pages.KNOWN_STAGE)
        if failure is not None:
            return stop(f"known page {failure}")
    wanted = gao_product_pages.newest_first(_table(held, changed))
    counts: Counter[str] = Counter(pending=len(wanted))
    read: dict[str, dict] = {}
    refusals: list[dict[str, str]] = []
    failures = requests = not_whole = 0
    for row in wanted:
        if requests >= gao_product_pages.PAGES_PER_RUN:
            break
        report_id = row["report_id"]
        if (wait := gao_product_pages.not_yet(row, on, export)) is not None:
            counts[wait] += 1
            continue
        requests += 1
        try:
            details, capture = acquirer.acquire_details(report_id)
        except GaoProductPageUnavailableError as error:
            failures = 0
            counts[gao_product_pages.UNAVAILABLE] += 1
            if evidence:
                evidence.refusal(error, stage=gao_product_pages.STAGE)
            read[report_id] = {**row, **gao_product_pages.unavailable_cells(error.capture)}
            continue
        except GaoProductDetailsError as error:
            reason = getattr(error, "reason", gao_product_pages.ACQUISITION)
            if reason == gao_product_pages.ACQUISITION:
                counts["failed"] += 1
                failures += 1
                if failures >= gao_product_pages.MAX_CONSECUTIVE_FAILURES:
                    counts["stopped_after_failures"] = 1
                    break
                continue
            # A refusal writes nothing for the product and leaves it for a later run; its reason is journaled.
            failures = 0
            counts[gao_product_pages.REFUSED] += 1
            refusals.append({"report_id": report_id, "reason": reason})
            if evidence:
                evidence.refusal(error, stage=gao_product_pages.STAGE)
            not_whole += reason in gao_product_pages.THEME_REASONS
            if not_whole > 1:
                return stop("more than one page refused as not whole: GAO's theme may have changed",
                            refusals=refusals, read_and_discarded=len(read))
            continue
        failures = 0
        if evidence:
            evidence.capture(capture, stage=gao_product_pages.STAGE)
        if not gao_product_pages.agrees(row, details, on, export):
            counts["export_disagrees"] += 1
            continue
        cells = gao_product_pages.page_cells(row, details, capture)
        counts[gao_product_pages.READ] += 1
        for column in gao_product_pages.FILL_COLUMNS:
            counts[f"set_{column}"] += column in cells
            counts[f"outranked_{column}"] += column in cells and row.get(column) not in (None, cells[column])
        # A major-rule report listed after the letters' capture: the page just read prints its letter.
        if (row.get("product_type") == MAJOR_RULE_REPORT and row.get("report_number")
                and gao_major_rule_letters.unread(row)):
            cells |= gao_major_rule_letters.letter_cells(capture.body, product_id=report_id,
                                                         product_number=row["report_number"])
            counts["letters_read"] += 1
        read[report_id] = {**row, **cells}
    changed.update(read)
    counts["left_pending"] = counts["pending"] - len(read)
    logger.info("GAO reports: product pages under {} {} refusals {}", PRODUCT_PAGE_DETAILS_RULE, dict(counts),
                refusals)
    if evidence:
        evidence.event(gao_product_pages.STAGE, rule=PRODUCT_PAGE_DETAILS_RULE,
                       per_run=gao_product_pages.PAGES_PER_RUN, on=on.isoformat(),
                       export_as_of=None if export is None else export.as_of, refusals=refusals, **counts)


def _undo_page_reads(
    held: Mapping[str, dict], changed: dict[str, dict], selector: str, evidence: CaptureEvidence | None
) -> None:
    """Put back what every product-page read ``selector`` names replaced: a rule's, or a day's. O(rows), no request."""
    counts: Counter[str] = Counter()
    for row in list(_table(held, changed)):
        restored = gao_product_pages.undone(row, selector)
        if restored is None:
            continue
        counts["rows"] += 1
        counts["letters"] += row.get(gao_major_rule_letters.RECEIPT) != restored.get(gao_major_rule_letters.RECEIPT)
        for column in gao_product_pages.FILL_COLUMNS:
            counts[f"restored_{column}"] += row.get(column) != restored.get(column)
        changed[row["report_id"]] = restored
    logger.info("GAO reports: product-page reads named by {} undone {}", selector, dict(counts))
    if evidence:
        evidence.event("gao-product-page-undo", selector=selector, **counts)


def _mods_reads(
    held: Mapping[str, dict], changed: dict[str, dict], acquirer: Any, evidence: CaptureEvidence | None
) -> None:
    """The prior's next :data:`MODS_PER_RUN` unread history rows, each updated from its package's MODS."""
    from spicy_regs.sources.gao_govinfo_mods import GaoModsUnavailableError

    pending = sorted((changed.get(report_id, row) for report_id, row in held.items()
                      if row["source"] == gao_govinfo.SOURCE and row.get("report_number") is None),
                     key=lambda row: row["report_id"])
    counts: Counter[str] = Counter(pending=len(pending))
    for row in pending[:MODS_PER_RUN]:
        package_id = gao_govinfo.package_id_of(row)
        try:
            facts, capture = acquirer.capture(package_id)
        except GaoModsUnavailableError as error:
            counts["unavailable"] += 1
            if evidence:
                evidence.refusal(error, stage="govinfo-mods")
            continue
        if evidence:
            evidence.capture(capture, stage="govinfo-mods")
        read = {**row, "abstract": facts.abstract, "product_type": facts.product_type,
                "report_number": facts.report_number,
                "topics_json": json.dumps(list(facts.topics), ensure_ascii=False) if facts.topics else None}
        changed[row["report_id"]] = read
        counts["read"] += 1
        for column in ("abstract", "product_type", "report_number", "topics_json"):
            counts[f"with_{column}"] += read[column] is not None
        if counts["read"] % 500 == 0:
            logger.info("GAO reports: read {:,} of {:,} selected MODS", counts["read"], min(len(pending), MODS_PER_RUN))
    counts["left_unread"] = len(pending) - counts["read"]
    logger.info("GAO reports: GovInfo MODS {}", dict(counts))
    if evidence:
        evidence.event("govinfo-mods", per_run=MODS_PER_RUN, **counts)


@receipt_builder
def build_gao_reports(
    output_dir: Path,
    *,
    max_records: int | None = None,
    evidence: CaptureEvidence | None = None,
    govinfo_history: bool = False,
    govinfo: gao_govinfo.PackageDiscoverySource | None = None,
    govinfo_mods: bool = False,
    mods: Any = None,
    listing_run: Path | None = None,
    decision_pages: Path | None = None,
    major_rule_run: Path | None = None,
    old_index_run: Path | None = None,
    major_rule_letters: Sequence[Path] = (),
    product_pages: bool = False,
    pages: Any = None,
    product_pages_undo: str | None = None,
    today: date | None = None,
) -> tuple[Path, Path]:
    """Build ``gao_reports.parquet`` (append-only merge with the prior table).

    ``govinfo_history`` also walks GovInfo's GAOREPORTS listing, through
    ``govinfo`` when a caller supplies the reader; ``govinfo_mods`` reads the
    next batch of history rows' MODS, through ``mods`` when a caller supplies it;
    ``listing_run`` also reads a finished walk of GAO's own listing from that directory,
    and ``decision_pages`` a capture of its decisions' pages, read with that walk.
    ``major_rule_run`` and ``old_index_run`` are finished walks of GAO's two major-rule
    listings, read with ``listing_run`` and refused without it; ``major_rule_letters``
    are captures of those reports' pages and PDFs. ``product_pages`` reads pending rows'
    product pages, through ``pages`` when a caller supplies the acquirer, and only under a
    reader rule the pass admits; ``today`` is the day that pass takes it to be, GAO's
    own unless a caller states one. ``product_pages_undo`` names a reader rule, a day,
    or a day and hour, whose product-page reads are put back first.
    Returns ``gao_reports.parquet`` and ``gao_decisions.parquet``.
    """
    import duckdb

    if listing_run is None and (major_rule_run is not None or old_index_run is not None):
        raise ValueError("GAO's major-rule listings are read with a Month in Review walk, whose rows stand first")
    if product_pages_undo is not None:
        product_pages_undo = gao_product_pages.undo_selector(product_pages_undo)
    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_gao_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means a fresh start).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        prior_file = internal_prior("gao_reports", prior_file)
    if have_prior:
        logger.info("GAO reports: accumulating onto prior table {}", prior_file)
    else:
        logger.info("GAO reports: no prior table found — starting fresh")
    held = _held_rows(prior_file) if have_prior else {}

    # 2. Fetch + shape the current feed window, then each route asked for, into this run's rows by product.
    reader = GaoReportsReader(max_records=max_records, evidence=evidence)
    rows, feed_counts = _feed_over_held(held, [_shape(item) for item in reader.iter_records()])
    logger.info("GAO reports: fetched {:,} items this run {}", len(rows), dict(feed_counts))
    changed = {row["report_id"]: row for row in rows}
    if product_pages_undo is not None:
        _undo_page_reads(held, changed, product_pages_undo, evidence)
    if govinfo_history:
        with nullcontext(govinfo) if govinfo is not None else gao_govinfo.discovery_reader(evidence) as source:
            changed.update((row["report_id"], row) for row in _govinfo_additions(held, changed, source, evidence))
    if govinfo_mods and have_prior:
        if mods is None:
            from spicy_regs.sources.gao_govinfo_mods import GaoModsAcquirer

            mods = GaoModsAcquirer()
        with mods as acquirer:
            _mods_reads(held, changed, acquirer, evidence)
    decisions: list[dict] = []
    stated = gao_listing.MajorRuleReports()
    if listing_run is not None:
        decisions, stated = _listing_rows(held, changed, listing_run, evidence, decision_pages, major_rule_run,
                                          old_index_run)
    letters = gao_major_rule_letters.LetterPages(major_rule_letters, stated.pages)
    if letters:
        _letter_reads(held, changed, letters, stated.numbers, evidence)
    if product_pages and (refusal := gao_product_pages.reader_refusal()) is not None:
        # Asked for, and not run: the switch alone does not turn the pass on.
        logger.warning("GAO reports: product pages not read: {}", refusal)
        if evidence:
            evidence.event(gao_product_pages.STAGE, not_run=refusal)
    elif product_pages:
        export = gao_product_pages.open_recommendations(output_dir)
        with nullcontext(pages) if pages is not None else gao_product_pages.page_acquirer(evidence) as acquirer:
            _product_page_reads(held, changed, acquirer, evidence, on=today or gao_product_pages.today(),
                                export=export)
    rows, taken_over = _over_lowest(held, list(changed.values()))
    if taken_over:
        logger.info("GAO reports: {:,} rows of ours take over the R package's rows, which fill their NULLs", taken_over)
    new_file = output_dir / "_gao_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")

    # 3. Merge prior + new, dedup on report_id preferring the new row.
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    merge_local_prior(
        con,
        columns=COLUMNS,
        identity="report_id",
        order_by="published_date DESC, report_id",
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=out_file,
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("GAO reports: {:,} rows", total)
    return out_file, _build_decisions(output_dir, decisions, evidence)


def _decision_rows(
    run: gao_listing.ListingRun, pages: Path | None = None, evidence: CaptureEvidence | None = None
) -> list[dict]:
    """One contract row per listed decision: every B-numbered one and every numbered other (a docket has no B-number).

    Shaped by spicy-docs, with the outcome its sentence states; each listed item carries its first listing page. With
    ``pages``, each decision's page is read from that capture by spicy-docs' caption reader, whose File list replaces
    the listing's numbers and whose Date fills ``decided_date``; a page not held, without a caption or refused keeps
    the listing's values. O(listed decisions), one page read each.
    """
    from spicy_docs.interpretation.gao_decisions import decision_outcome
    from spicy_docs.schemas.gao_decision_tables import GAO_SITE, shape_gao_decision
    from spicy_docs.sources.gao.decision_pages import GaoDecisionPageError, read_decision_caption

    listed = [*run.decisions, *(other for other in run.others if other.product_number is not None)]
    capture = None if pages is None else DecisionPageCapture(pages)
    counts: Counter[str] = Counter()
    refused: list[dict[str, str]] = []
    rows = []
    for item in listed:
        caption = None
        if capture is not None:
            url = GAO_SITE + item.link
            body = capture.page(url)
            if body is None:
                counts["not_held"] += 1
            else:
                try:
                    caption = read_decision_caption(body, product_id=unquote(item.link.removeprefix("/products/")),
                                                    listed=getattr(item, "decision_numbers", ()))
                except GaoDecisionPageError as error:
                    refused.append({"url": url, "reason": error.reason})
                else:
                    counts["read" if caption is not None else "no_caption"] += 1
        rows.append(shape_gao_decision(item, outcome=decision_outcome(item.status), caption=caption))
    if capture is not None:
        digest, size = capture.record()
        logger.info("GAO decisions: pages from capture {} ({}): {} read, {} without a caption, {} not held, {} refused "
                    "{}", capture.campaign, digest, counts["read"], counts["no_caption"], counts["not_held"],
                    len(refused), dict(Counter(entry["reason"] for entry in refused)))
        if evidence:
            evidence.event("gao-decision-pages", campaign=capture.campaign, receipts_sha256=digest, receipts_bytes=size,
                           read=counts["read"], no_caption=counts["no_caption"], not_held=counts["not_held"],
                           refused=refused)
    return rows


def _page_readings(prior_file: Path) -> dict[tuple[str, str], tuple[str, str, str]]:
    """Each prior row's page reading, by (number, page), where a caption was read; none from a prior predating them."""
    import duckdb

    if "decided_date" not in pq.read_schema(prior_file).names:
        return {}
    rows = duckdb.sql(f"SELECT decision_number, url, {', '.join(PAGE_READINGS)} FROM read_parquet('{prior_file}') "
                      "WHERE decided_date IS NOT NULL").fetchall()
    return {(number, url): tuple(values) for number, url, *values in rows}


@receipt_builder
def _build_decisions(output_dir: Path, decisions: list[dict], evidence: CaptureEvidence | None = None) -> Path:
    """``gao_decisions.parquet``: the prior table with this run's listed decisions over it, every outcome read again.

    The merge (on number and page) carries the stated columns; a prior that spells ``released_date`` as
    ``decision_date`` (before 2026-10-03) is read renamed. A listed decision this run read no caption for keeps the
    page reading a prior row holds (:data:`PAGE_READINGS`): the listing never restates a page's File list or Date, so
    a later walk without the capture does not fall back to a cut list and no date. ``outcome`` and ``outcome_rule`` are
    then read from each merged row's ``decision_status`` by spicy-docs' closed table, so a new table version reaches
    every held row without a re-read of the walk; sentences it does not read are counted in the run journal. O(rows).
    """
    import duckdb
    from spicy_docs.interpretation.gao_decisions import GAO_OUTCOME_RULE, decision_outcome, unmapped_sentences
    from spicy_docs.schemas.gao_decision_tables import GAO_DECISIONS

    stated = tuple(column for column in GAO_DECISIONS.columns if column not in DECISION_READINGS)
    schema = pa.schema([(column, pa.string()) for column in stated])
    out_file = output_dir / DECISIONS_OUTPUT
    prior_file = output_dir / "_gao_decisions_prior.parquet"
    new_file = output_dir / "_gao_decisions_new.parquet"
    merged_file = output_dir / "_gao_decisions_merged.parquet"
    have_prior = prior_file.exists() or r2.download(DECISIONS_OUTPUT, prior_file)
    if have_prior:
        prior_file = internal_prior("gao_decisions", prior_file)
    read = _page_readings(prior_file) if have_prior else {}
    rows = [{column: row[column] for column in stated} for row in decisions]
    for row in rows:
        held = read.get((row["decision_number"], row["url"])) if row["decided_date"] is None else None
        if held is not None:
            row.update(zip(PAGE_READINGS, held, strict=True))
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), new_file, compression="zstd")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    merge_local_prior(
        con,
        columns=stated,
        identity=GAO_DECISIONS.identity,
        order_by="released_date DESC, decision_number, url",
        renamed=DECISION_RENAMES,
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=merged_file,
    )
    con.close()
    table = pq.read_table(merged_file)
    statuses = table.column("decision_status").to_pylist()
    findings = [decision_outcome(status) for status in statuses]
    table = table.append_column("outcome", pa.array([f.outcome for f in findings], pa.string()))
    table = table.append_column("outcome_rule", pa.array([f.rule for f in findings], pa.string()))
    pq.write_table(table, out_file, compression="zstd", row_group_size=50_000)
    for scratch in (prior_file, new_file, merged_file):
        scratch.unlink(missing_ok=True)
    unmapped = unmapped_sentences(statuses)
    read = sum(f.outcome is not None for f in findings)
    logger.info("GAO decisions: {:,} rows, {:,} with an outcome under {}; {:,} stated sentences the table does not "
                "read", table.num_rows, read, GAO_OUTCOME_RULE, sum(unmapped.values()))
    if evidence:
        evidence.event("gao-decision-outcomes", rule=GAO_OUTCOME_RULE, rows=table.num_rows, with_outcome=read,
                       stated=sum(status is not None for status in statuses), unmapped_sentences=dict(unmapped))
    return out_file
