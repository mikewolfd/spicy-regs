"""Transform: build ``federal_register.parquet`` from the FR REST API.

Produces the exact 22 all-VARCHAR columns existing consumers expect (the
``fr-docket-links`` rollup and the UI's ``normalizeFRRow``), so in-repo
ingestion is a drop-in replacement for the former external path.

Incremental by design: best-effort prior from R2, fetch documents published
since its max ``publication_date`` minus a short overlap, then dedup on
``(document_number, publication_date)`` preferring a fresh observation of that
same dated record, so corrections republished under the same number on another
date survive as distinct rows. With no prior table it is a full backfill from
the FR epoch; the overlap cannot recover old records already lost by a
number-only merge — those take an explicit historical replay. Conflicting
observations within one input generation refuse publication rather than
choosing an arbitrary row.

``rin`` is the one column not fetched: the first Regulation Identifier Number
in ``regulation_id_numbers_json``, retained as a compatibility projection — an
empty array and NULL both give NULL — while a consumer wanting every RIN of a
multi-RIN document unnests the array (see docs/regulatory-rins.md).
``modify_date`` is not exposed by the REST API, so fresh rows carry NULL for it
while the merge preserves whatever the prior table had.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.federal_register import FR_EPOCH, FederalRegisterReader

OUTPUT = "federal_register.parquet"

# Re-scan this many days before the last stored publication_date on each run, so
# documents added/corrected after their nominal publication date are picked up.
OVERLAP_DAYS = 7

# The fetched shape: 22 columns, all VARCHAR, in the exact order the existing
# table uses. ``COLUMNS`` appends the one derived column.
FETCHED_COLUMNS = (
    "document_number",
    "title",
    "abstract",
    "document_type",
    "publication_date",
    "effective_on",
    "comments_close_on",
    "signing_date",
    "agencies_json",
    "agency_slugs",
    "docket_ids_json",
    "regulation_id_numbers_json",
    "cfr_references_json",
    "html_url",
    "pdf_url",
    "body_html_url",
    "volume",
    "start_page",
    "end_page",
    "subtype",
    "executive_order_number",
    "modify_date",
)
COLUMNS = (*FETCHED_COLUMNS, "rin")
_SCHEMA = pa.schema([(c, pa.string()) for c in FETCHED_COLUMNS])

#: ``rin`` as a projection of the array, for every row of the merged table.
#: ``[]`` and NULL both give NULL: a join key is present or it is not.
_RIN_SQL = "json_extract_string(regulation_id_numbers_json, '$[0]')"


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (volume/pages/EO # come as ints.)"""
    if value is None:
        return None
    return str(value)


def _shape(doc: dict) -> dict:
    """Map one raw FR API document onto the published column shape."""
    agencies = doc.get("agencies") or []
    slugs = ",".join(a["slug"] for a in agencies if isinstance(a, dict) and a.get("slug"))
    return {
        "document_number": doc.get("document_number"),
        "title": doc.get("title"),
        "abstract": doc.get("abstract"),
        "document_type": doc.get("type"),
        "publication_date": doc.get("publication_date"),
        "effective_on": doc.get("effective_on"),
        "comments_close_on": doc.get("comments_close_on"),
        "signing_date": doc.get("signing_date"),
        "agencies_json": json.dumps(agencies),
        "agency_slugs": slugs or None,
        "docket_ids_json": json.dumps(doc.get("docket_ids") or []),
        "regulation_id_numbers_json": json.dumps(doc.get("regulation_id_numbers") or []),
        "cfr_references_json": json.dumps(doc.get("cfr_references") or []),
        "html_url": doc.get("html_url"),
        "pdf_url": doc.get("pdf_url"),
        "body_html_url": doc.get("body_html_url"),
        "volume": _s(doc.get("volume")),
        "start_page": _s(doc.get("start_page")),
        "end_page": _s(doc.get("end_page")),
        "subtype": doc.get("subtype"),
        "executive_order_number": _s(doc.get("executive_order_number")),
        "modify_date": None,
    }


def _prior_max_publication_date(prior_file: Path) -> date | None:
    """Largest ``publication_date`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(publication_date) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def build_federal_register(
    output_dir: Path,
    *,
    since: date | None = None,
    documents: Callable[[date], Iterable[dict]] | None = None,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> Path:
    """Build ``federal_register.parquet`` (incremental merge with the prior table).

    ``documents`` is the raw API records published since a date; the default
    is the live reader, and a hermetic test passes its own.
    """
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_fr_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or download_prior(OUTPUT, prior_file)
    if have_prior:
        logger.info("FR: merging against prior table {}", prior_file)
    else:
        logger.info("FR: no prior table found — full backfill from {}", FR_EPOCH)

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_publication_date(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else FR_EPOCH
    logger.info("FR: fetching documents published since {}", since)

    # 3. Fetch + shape into a "new rows" parquet.
    fetch = documents or (lambda start: FederalRegisterReader(since=start).iter_records())
    rows = [_shape(doc) for doc in fetch(since)]
    new_file = output_dir / "_fr_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("FR: fetched {:,} documents this run", len(rows))

    # 4. Apply the SpicyDocs public-table primary key (projection 1.1).
    # The columns and their literal values stay unchanged; this is not an IRI
    # normalization or an assertion that two dated printings are one matter.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    # The fetched columns are what both files carry; a prior that already has
    # ``rin`` is not read for it, since the projection below recomputes it.
    cols = ", ".join(FETCHED_COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, file_row_number AS _row, 0 AS _src FROM read_parquet('{prior_file}', file_row_number=true) "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, file_row_number AS _row, 1 AS _src FROM read_parquet('{new_file}', file_row_number=true)"
        )
    else:
        union = (
            f"SELECT {cols}, file_row_number AS _row, 1 AS _src FROM read_parquet('{new_file}', file_row_number=true)"
        )

    try:
        # Materialize the prior ∪ fresh union once: the checks, the winner
        # window and the final COPY each scan it, and re-scanning the wide
        # abstract/JSON payload columns four times per run was the largest
        # cost in this rollup.
        con.execute(f"CREATE TEMP TABLE _union AS {union}")
        invalid = con.execute(
            """SELECT document_number, publication_date FROM _union
                WHERE document_number IS NULL OR trim(document_number) = ''
                   OR publication_date IS NULL
                   OR try_cast(publication_date AS DATE) IS NULL
                   OR cast(try_cast(publication_date AS DATE) AS VARCHAR) <> publication_date
                LIMIT 1"""
        ).fetchone()
        if invalid:
            raise ValueError(f"Federal Register record lacks a complete canonical identity: {invalid!r}")
        conflict = con.execute(
            f"""SELECT document_number, publication_date, _src FROM _union
                GROUP BY document_number, publication_date, _src
                HAVING count(DISTINCT ({cols})) > 1 LIMIT 1"""
        ).fetchone()
        if conflict:
            raise ValueError(f"Federal Register conflicting observations of the same number/date: {conflict!r}")
        # Rank positions, not the wide abstract/JSON payloads. A window over
        # every payload column exceeds the 4GB limit on the retained public
        # table; selecting positions first keeps the same winner semantics.
        con.execute(
            """CREATE TEMP TABLE winners AS
                SELECT _src, _row FROM (
                    SELECT _src, _row, row_number() OVER (
                        PARTITION BY document_number, publication_date ORDER BY _src DESC, _row
                    ) AS _rn FROM _union
                ) WHERE _rn = 1"""
        )
        merged_file = output_dir / "_fr_merged.parquet"
        con.execute(
            f"""
        COPY (
            SELECT {cols}, {_RIN_SQL} AS rin
            FROM _union records JOIN winners USING (_src, _row)
            ORDER BY publication_date DESC, document_number
        ) TO '{merged_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
            """
        )
        merged_file.replace(out_file)
    finally:
        con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Federal Register: {:,} rows", total)
    return out_file
