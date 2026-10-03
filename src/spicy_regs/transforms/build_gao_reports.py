"""Transform: build ``gao_reports.parquet`` from the GAO reports RSS feed, plus GovInfo's and GAO's own listings on request.

Produces an 11-column all-VARCHAR schema keyed on ``report_id`` (e.g.
``gao-26-107974``) — the Government Accountability Office oversight layer over
the rulemakings this dataset tracks. ``source`` names the route that supplied
each row: ``gao_rss`` (this feed), ``gao_repair`` (an explicit repair,
:mod:`spicy_regs.transforms.build_gao_target`), ``upstream_copy`` (the one-time
copy of upstream's rows the fork never captured), ``govinfo`` (GovInfo's closed
GAOREPORTS collection, :mod:`spicy_regs.sources.gao_govinfo`) or ``gao_listing``
(GAO's own Month in Review and Annual Index, :mod:`spicy_regs.sources.gao_listing`).

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
"""

from __future__ import annotations

import json
from collections import Counter
from contextlib import nullcontext
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import gao_govinfo, gao_listing, gao_r_package, r2
from spicy_regs.sources.gao_reports import GaoReportsReader
from spicy_regs.transforms.table_merge import merge_local_prior

OUTPUT = "gao_reports.parquet"

# The published schema: 18 columns in a fixed order, the three counts BIGINT and the rest VARCHAR. ``report_id``
# is the primary / dedup key.
COLUMNS = (
    "report_id",
    "title",
    "report_type",
    "published_date",
    "abstract",
    "agencies_json",
    "topics_json",
    "url",
    "source",
    "product_type",
    "report_number",
    # From the CetiAlphaFive/gao R package (2026-09-29), for the reports it lists; a later GAO product-page reader
    # fills new reports. NULL where no route states them.
    "requester_type",
    "requester_committees_json",
    "requester_members_json",
    "recommendation_count",
    "matters_for_congress_count",
    "page_count",
    "subject_terms_json",
)
#: The counts are whole numbers; every other column is VARCHAR.
COUNT_COLUMNS = ("recommendation_count", "matters_for_congress_count", "page_count")
_SCHEMA = pa.schema([(c, pa.int64() if c in COUNT_COLUMNS else pa.string()) for c in COLUMNS])

#: GAO's legal decisions from its own listing: all VARCHAR, keyed on the number as GAO spells it and the page.
DECISIONS_OUTPUT = "gao_decisions.parquet"
#: Columns renamed in place, to the name a published prior still spells them with. ``released_date`` was
#: ``decision_date`` until 2026-10-03 (owner decision): it is the date GAO released the decision, not the date
#: GAO decided it (B-424477, decided 2026-08-07, released 2026-08-28).
DECISION_RENAMES = {"released_date": "decision_date"}
DECISION_COLUMNS = (
    "decision_number",
    "b_numbers_json",
    "decision_type",
    "title",
    "released_date",
    "topics_json",
    "url",
    "listing_page",
    "source",
)
_DECISION_SCHEMA = pa.schema([(c, pa.string()) for c in DECISION_COLUMNS])

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


def _fill_only(rows: list[dict], counts: Counter[str], *, prior_file: Path | None, held: set[str]) -> list[dict]:
    """``rows`` for ids no row holds yet, this run's (``held``) or the prior's; counts the rest as ``already_held``."""
    held = set(held)
    if prior_file is not None:
        prior = pq.read_table(prior_file, columns=["report_id", "source"]).to_pylist()
        held |= {row["report_id"] for row in prior if row["source"] != _LOWEST}
    added = [row for row in rows if row["report_id"] not in held]
    counts["already_held"] = len(rows) - len(added)
    return added


def _over_lowest(prior_file: Path | None, rows: list[dict]) -> tuple[list[dict], int]:
    """Rows of ours for products the lowest route holds, each with its NULL cells filled from that route's row."""
    if prior_file is None or not rows:
        return rows, 0
    ids = {row["report_id"] for row in rows}
    lowest = {row["report_id"]: row for row in pq.read_table(prior_file).to_pylist()
              if row["report_id"] in ids and row["source"] == _LOWEST}
    merged = [
        {column: row.get(column) if row.get(column) is not None else lowest[row["report_id"]].get(column)
         for column in COLUMNS} if row["report_id"] in lowest else row
        for row in rows
    ]
    return merged, sum(row["report_id"] in lowest for row in rows)


def _feed_over_held(prior_file: Path | None, rows: list[dict]) -> tuple[list[dict], Counter[str]]:
    """Feed rows, each merged cell by cell over the row already held for its product, if any.

    A held row keeps every cell and its ``source``. The feed sets the cells it states: all of them on its own or an
    upstream-copied row, only the NULL ones on a row another route supplied (the listing, GovInfo, a repair).
    It never empties a cell, and never fills one with its placeholders.
    """
    counts: Counter[str] = Counter()
    if prior_file is None or not rows:
        return rows, counts
    ids = {row["report_id"] for row in rows}
    held = {row["report_id"]: row for row in pq.read_table(prior_file).to_pylist() if row["report_id"] in ids}
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
    prior_file: Path | None,
    feed_ids: set[str],
    reader: gao_govinfo.PackageDiscoverySource,
    evidence: CaptureEvidence | None,
) -> list[dict]:
    """GovInfo rows for ids no row holds yet."""
    rows, counts = gao_govinfo.read_history(reader)
    added = _fill_only(rows, counts, prior_file=prior_file, held=feed_ids)
    logger.info("GAO reports: GovInfo history {}", dict(counts))
    if evidence:
        evidence.event("govinfo-history", collection=gao_govinfo.COLLECTION, listed_since=gao_govinfo.LISTED_SINCE,
                       page_size=gao_govinfo.PAGE_SIZE, max_pages=gao_govinfo.MAX_PAGES, **counts)
    return added


def _listing_rows(
    prior_file: Path | None, rows_now: list[dict], directory: Path, evidence: CaptureEvidence | None
) -> tuple[list[dict], list[dict]]:
    """The listing's product rows for ids no row holds yet, held rows whose NULL ``report_number`` it fills, and its
    decisions. ``rows_now`` (this run's rows) is filled in place; each page read is retained as evidence.
    """
    rows, counts, run = gao_listing.read_listing(directory, evidence)
    added = _fill_only(rows, counts, prior_file=prior_file, held={row["report_id"] for row in rows_now})
    numbers = {row["report_id"]: row["report_number"] for row in rows if row["report_number"] is not None}
    for row in rows_now:
        if row.get("report_number") is None and row["report_id"] in numbers:
            row["report_number"] = numbers[row["report_id"]]
            counts["report_number_filled"] += 1
    filled = []
    if prior_file is not None:
        now = {row["report_id"] for row in rows_now}
        for row in pq.read_table(prior_file).to_pylist():
            if row["source"] == _LOWEST:
                continue  # the listing's own row takes a package row over; see _over_lowest
            if row["report_id"] in numbers and row["report_id"] not in now and row.get("report_number") is None:
                filled.append({**row, "report_number": numbers[row["report_id"]]})
        counts["report_number_filled"] += len(filled)
    decisions, decision_counts = gao_listing.decision_rows(run)
    counts.update(decision_counts)
    logger.info("GAO reports: GAO listing {} (unfinished scopes left for a later run: {})", dict(counts),
                list(run.incomplete_scopes))
    if evidence:
        evidence.event("gao-listing", scopes_read=list(run.complete_scopes),
                       scopes_unfinished=list(run.incomplete_scopes), **counts)
    return added + filled, decisions


def _mods_reads(prior_file: Path, acquirer: Any, evidence: CaptureEvidence | None) -> list[dict]:
    """The next :data:`MODS_PER_RUN` unread history rows, each updated from its package's MODS."""
    from spicy_regs.sources.gao_govinfo_mods import GaoModsUnavailableError

    prior = pq.read_table(prior_file).to_pylist()
    pending = sorted((row for row in prior if row["source"] == gao_govinfo.SOURCE and row.get("report_number") is None),
                     key=lambda row: row["report_id"])
    counts: Counter[str] = Counter(pending=len(pending))
    read = []
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
        read.append({**row, "abstract": facts.abstract, "product_type": facts.product_type,
                     "report_number": facts.report_number,
                     "topics_json": json.dumps(list(facts.topics), ensure_ascii=False) if facts.topics else None})
        counts["read"] += 1
        for column in ("abstract", "product_type", "report_number", "topics_json"):
            counts[f"with_{column}"] += read[-1][column] is not None
        if counts["read"] % 500 == 0:
            logger.info("GAO reports: read {:,} of {:,} selected MODS", counts["read"], min(len(pending), MODS_PER_RUN))
    counts["left_unread"] = len(pending) - counts["read"]
    logger.info("GAO reports: GovInfo MODS {}", dict(counts))
    if evidence:
        evidence.event("govinfo-mods", per_run=MODS_PER_RUN, **counts)
    return read


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
) -> tuple[Path, Path]:
    """Build ``gao_reports.parquet`` (append-only merge with the prior table).

    ``govinfo_history`` also walks GovInfo's GAOREPORTS listing, through
    ``govinfo`` when a caller supplies the reader; ``govinfo_mods`` reads the
    next batch of history rows' MODS, through ``mods`` when a caller supplies it;
    ``listing_run`` also reads a finished walk of GAO's own listing from that directory.
    Returns ``gao_reports.parquet`` and ``gao_decisions.parquet``.
    """
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_gao_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means a fresh start).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("GAO reports: accumulating onto prior table {}", prior_file)
    else:
        logger.info("GAO reports: no prior table found — starting fresh")

    # 2. Fetch + shape the current feed window (and GovInfo's history when asked) into a "new rows" parquet.
    reader = GaoReportsReader(max_records=max_records, evidence=evidence)
    rows, feed_counts = _feed_over_held(prior_file if have_prior else None, [_shape(item) for item in reader.iter_records()])
    logger.info("GAO reports: fetched {:,} items this run {}", len(rows), dict(feed_counts))
    if govinfo_history:
        feed_ids = {row["report_id"] for row in rows}
        with nullcontext(govinfo) if govinfo is not None else gao_govinfo.discovery_reader(evidence) as source:
            rows += _govinfo_additions(prior_file if have_prior else None, feed_ids, source, evidence)
    if govinfo_mods and have_prior:
        if mods is None:
            from spicy_regs.sources.gao_govinfo_mods import GaoModsAcquirer

            mods = GaoModsAcquirer()
        with mods as acquirer:
            rows += _mods_reads(prior_file, acquirer, evidence)
    decisions: list[dict] = []
    if listing_run is not None:
        listed, decisions = _listing_rows(prior_file if have_prior else None, rows, listing_run, evidence)
        rows += listed
    rows, taken_over = _over_lowest(prior_file if have_prior else None, rows)
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
    return out_file, _build_decisions(output_dir, decisions)


def _build_decisions(output_dir: Path, decisions: list[dict]) -> Path:
    """``gao_decisions.parquet``: the prior table, with this run's listed decisions over it on the same number and page."""
    import duckdb

    out_file = output_dir / DECISIONS_OUTPUT
    prior_file = output_dir / "_gao_decisions_prior.parquet"
    new_file = output_dir / "_gao_decisions_new.parquet"
    have_prior = prior_file.exists() or r2.download(DECISIONS_OUTPUT, prior_file)
    table = pa.Table.from_pylist(decisions, schema=_DECISION_SCHEMA) if decisions else _DECISION_SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    merge_local_prior(
        con,
        columns=DECISION_COLUMNS,
        identity=("decision_number", "url"),
        order_by="released_date DESC, decision_number, url",
        renamed=DECISION_RENAMES,
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=out_file,
    )
    con.close()
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)
    logger.info("GAO decisions: {:,} rows", pq.ParquetFile(out_file).metadata.num_rows)
    return out_file
