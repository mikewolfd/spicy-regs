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

**GAO's listing.** ``listing_run`` names a finished SpicyDocs walk of GAO's Month
in Review and Annual Index pages; the walk itself runs outside the rollup. Its
rows follow the GovInfo rule, one helper for both: they fill only product ids no
row holds yet, this run's feed and GovInfo rows included, so a later walk adds
new products and leaves held rows, a MODS read among them, alone.
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

from spicy_regs.sources import gao_govinfo, gao_listing, r2
from spicy_regs.sources.gao_reports import GaoReportsReader
from spicy_regs.transforms.table_merge import merge_local_prior

OUTPUT = "gao_reports.parquet"

# The published schema: 11 columns, all VARCHAR, in a fixed order. ``report_id``
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
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])

# GAO's reports RSS feed carries published products (reports & testimonies).
# The feed does not tag a finer product type, so we default to this label.
_DEFAULT_REPORT_TYPE = "Report"

SOURCE_FEED = "gao_rss"
SOURCE_REPAIR = "gao_repair"

#: MODS reads per run: at the three-a-second pace, about 37 minutes, so two
#: runs read the whole history and a failed run loses at most one batch.
MODS_PER_RUN = 6_500


def _report_id(link: str | None) -> str | None:
    """Extract the ``gao-##-######`` id from a product URL, or None.

    Links look like ``https://www.gao.gov/products/gao-26-107974``; the id is the
    last path segment, lowercased.
    """
    if not link:
        return None
    segment = link.rstrip("/").rsplit("/", 1)[-1].strip().lower()
    return segment or None


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
        "report_id": _report_id(item.get("link")),
        "title": item.get("title"),
        "report_type": _DEFAULT_REPORT_TYPE,
        "published_date": _published_date(item.get("pub_date")),
        "abstract": item.get("description"),
        # Reserved — the RSS feed carries no structured agency/topic tags.
        "agencies_json": "[]",
        "topics_json": "[]",
        "url": item.get("link"),
        "source": SOURCE_FEED,
        "product_type": None,
        "report_number": None,
    }


def _fill_only(rows: list[dict], counts: Counter[str], *, prior_file: Path | None, held: set[str]) -> list[dict]:
    """``rows`` for ids no row holds yet, this run's (``held``) or the prior's; counts the rest as ``already_held``."""
    held = set(held)
    if prior_file is not None:
        held |= set(pq.read_table(prior_file, columns=["report_id"])["report_id"].to_pylist())
    added = [row for row in rows if row["report_id"] not in held]
    counts["already_held"] = len(rows) - len(added)
    return added


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


def _listing_additions(
    prior_file: Path | None, held: set[str], directory: Path, evidence: CaptureEvidence | None
) -> list[dict]:
    """GAO listing rows for ids no row holds yet; each page read is retained as evidence."""
    rows, counts, run = gao_listing.read_listing(directory, evidence)
    added = _fill_only(rows, counts, prior_file=prior_file, held=held)
    logger.info("GAO reports: GAO listing {} (unfinished scopes left for a later run: {})", dict(counts),
                list(run.incomplete_scopes))
    if evidence:
        evidence.event("gao-listing", scopes_read=list(run.complete_scopes),
                       scopes_unfinished=list(run.incomplete_scopes), **counts)
    return added


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
) -> Path:
    """Build ``gao_reports.parquet`` (append-only merge with the prior table).

    ``govinfo_history`` also walks GovInfo's GAOREPORTS listing, through
    ``govinfo`` when a caller supplies the reader; ``govinfo_mods`` reads the
    next batch of history rows' MODS, through ``mods`` when a caller supplies it;
    ``listing_run`` also reads a finished walk of GAO's own listing from that directory.
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
    rows = [_shape(item) for item in reader.iter_records()]
    logger.info("GAO reports: fetched {:,} items this run", len(rows))
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
    if listing_run is not None:
        held_now = {row["report_id"] for row in rows}
        rows += _listing_additions(prior_file if have_prior else None, held_now, listing_run, evidence)
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
    return out_file
