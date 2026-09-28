"""Transform: build ``gao_reports.parquet`` from the GAO reports RSS feed, and GovInfo's GAO history on request.

Produces a 9-column all-VARCHAR schema keyed on ``report_id`` (e.g.
``gao-26-107974``) — the Government Accountability Office oversight layer over
the rulemakings this dataset tracks. ``source`` names the route that supplied
each row: ``gao_rss`` (this feed), ``gao_repair`` (an explicit repair,
:mod:`spicy_regs.transforms.build_gao_target`), ``upstream_copy`` (the one-time
copy of upstream's rows the fork never captured) or ``govinfo`` (GovInfo's closed
GAOREPORTS collection, :mod:`spicy_regs.sources.gao_govinfo`).

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
listing (17 keyed requests) to the run. Its rows fill product ids that no other
source holds and refresh earlier GovInfo rows, but never replace a row from
another route, which carries more. The collection is closed, so one run adds the
history and later feed runs carry it forward in the prior.
"""

from __future__ import annotations

import hashlib
import json
from contextlib import nullcontext
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import gao_govinfo, r2
from spicy_regs.sources.gao_reports import GaoReportsReader
from spicy_regs.transforms.table_merge import merge_local_prior

OUTPUT = "gao_reports.parquet"

# The published schema: 9 columns, all VARCHAR, in a fixed order. ``report_id``
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
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])

# GAO's reports RSS feed carries published products (reports & testimonies).
# The feed does not tag a finer product type, so we default to this label.
_DEFAULT_REPORT_TYPE = "Report"

SOURCE_FEED = "gao_rss"
SOURCE_REPAIR = "gao_repair"
SOURCE_UPSTREAM = "upstream_copy"

#: The one-time copy of upstream's rows (owner decision 2026-09-28; commit
#: 92786e7, published as generation e0b5049a, the script since deleted). Every
#: copied report was published in this window, and the copied rows digest to the
#: import's reviewed ``ROWS_SHA256`` under the import's row digest.
UPSTREAM_COPY_WINDOW = ("2026-07-13", "2026-09-14")
UPSTREAM_COPY_ROWS_SHA256 = "sha256:ec48702a097b6ede03c9b4e0100e8f619add6903813363b39af687e9368a7e0c"

#: How a prior written before ``source`` existed is labelled, once. The feed
#: writes a ``Report`` type and ``[]`` tag lists into every row (:func:`_shape`),
#: and so did upstream's copied rows, told apart by their window and checked by
#: their digest. The explicit repair leaves type, abstract and tags NULL. A
#: prior row matching none refuses, so no row's route is guessed. Delete this
#: once the published table carries ``source``.
_PRIOR_SOURCE_SQL = f"""
    CASE
        WHEN report_type = '{_DEFAULT_REPORT_TYPE}' AND agencies_json = '[]' AND topics_json = '[]' THEN
            CASE WHEN published_date BETWEEN '{UPSTREAM_COPY_WINDOW[0]}' AND '{UPSTREAM_COPY_WINDOW[1]}'
                THEN '{SOURCE_UPSTREAM}' ELSE '{SOURCE_FEED}' END
        WHEN report_type IS NULL AND abstract IS NULL AND agencies_json IS NULL AND topics_json IS NULL
            THEN '{SOURCE_REPAIR}'
    END
"""


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
    }


def _rows_digest(rows: list[tuple]) -> str:
    """The upstream import's ``rows_digest``: SHA-256 over rows in ``report_id`` order, each a compact JSON array."""
    lines = (json.dumps(list(row), ensure_ascii=False, separators=(",", ":")) for row in sorted(rows, key=lambda r: r[0]))
    return "sha256:" + hashlib.sha256("\n".join(lines).encode()).hexdigest()


def label_prior_sources(con, prior_file: Path) -> None:
    """Give a prior written before ``source`` existed its column, by :data:`_PRIOR_SOURCE_SQL`; refuse what it cannot place."""
    if "source" in pq.read_schema(prior_file).names:
        return
    path = str(prior_file).replace("'", "''")
    con.execute(f"CREATE TEMP TABLE _labelled AS SELECT *, {_PRIOR_SOURCE_SQL} AS source FROM read_parquet('{path}')")
    unplaced = [row[0] for row in con.execute("SELECT report_id FROM _labelled WHERE source IS NULL ORDER BY 1").fetchall()]
    if unplaced:
        raise ValueError(f"Prior GAO rows are neither feed- nor repair-shaped; set their source explicitly: {unplaced}")
    prior_columns = ", ".join(f'"{c}"' for c in COLUMNS if c != "source")
    copied = con.execute(f"SELECT {prior_columns} FROM _labelled WHERE source = '{SOURCE_UPSTREAM}'").fetchall()
    if copied and _rows_digest(copied) != UPSTREAM_COPY_ROWS_SHA256:
        raise ValueError("Prior rows in the upstream copy's window are not the reviewed copied rows")
    con.execute(f"COPY _labelled TO '{path}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con.execute("DROP TABLE _labelled")
    logger.info("GAO reports: labelled the prior's rows by source")


def _govinfo_additions(
    prior_file: Path | None,
    feed_ids: set[str],
    reader: gao_govinfo.PackageDiscoverySource,
    evidence: CaptureEvidence | None,
) -> list[dict]:
    """GovInfo rows for ids no other route's row holds; an earlier GovInfo row is refreshed."""
    rows, counts = gao_govinfo.read_history(reader)
    held = set(feed_ids)
    if prior_file is not None:
        prior = pq.read_table(prior_file, columns=["report_id", "source"]).to_pylist()
        held |= {row["report_id"] for row in prior if row["source"] != gao_govinfo.SOURCE}
    added = [row for row in rows if row["report_id"] not in held]
    counts["held_by_another_source"] = len(rows) - len(added)
    logger.info("GAO reports: GovInfo history {}", dict(counts))
    if evidence:
        evidence.event("govinfo-history", collection=gao_govinfo.COLLECTION, listed_since=gao_govinfo.LISTED_SINCE,
                       page_size=gao_govinfo.PAGE_SIZE, max_pages=gao_govinfo.MAX_PAGES, **counts)
    return added


def build_gao_reports(
    output_dir: Path,
    *,
    max_records: int | None = None,
    evidence: CaptureEvidence | None = None,
    govinfo_history: bool = False,
    govinfo: gao_govinfo.PackageDiscoverySource | None = None,
) -> Path:
    """Build ``gao_reports.parquet`` (append-only merge with the prior table).

    ``govinfo_history`` also walks GovInfo's GAOREPORTS listing, through
    ``govinfo`` when a caller supplies the reader.
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

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    if have_prior:
        label_prior_sources(con, prior_file)

    # 2. Fetch + shape the current feed window (and GovInfo's history when asked) into a "new rows" parquet.
    reader = GaoReportsReader(max_records=max_records, evidence=evidence)
    rows = [_shape(item) for item in reader.iter_records()]
    logger.info("GAO reports: fetched {:,} items this run", len(rows))
    if govinfo_history:
        feed_ids = {row["report_id"] for row in rows}
        with nullcontext(govinfo) if govinfo is not None else gao_govinfo.discovery_reader(evidence) as source:
            rows += _govinfo_additions(prior_file if have_prior else None, feed_ids, source, evidence)
    new_file = output_dir / "_gao_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")

    # 3. Merge prior + new, dedup on report_id preferring the new row.
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
