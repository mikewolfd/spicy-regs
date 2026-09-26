"""Transform: build ``federal_register.parquet`` from the FR REST API.

Produces SpicyDocs' public projection (``FEDERAL_REGISTER_COLUMNS``, all
VARCHAR, through ``project_federal_register_document``) plus the derived
``rin``: one owner for the column list and the value mapping, not a local
copy (consolidation plan B2). ``topics_json`` is the publisher's ``topics``;
a row published before that column existed reads NULL until it is fetched
again, and a full re-read (``since`` at the epoch) refills every row.

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

from collections.abc import Callable, Iterable, Iterator
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2

OUTPUT = "federal_register.parquet"

# The oldest documents the API serves. A backfill with no prior table starts here.
FR_EPOCH = date(1994, 1, 1)

# Re-scan this many days before the last stored publication_date on each run, so
# documents added/corrected after their nominal publication date are picked up.
OVERLAP_DAYS = 7


def published_columns() -> tuple[str, ...]:
    """The published width: SpicyDocs' projection plus the derived ``rin``.

    A function, not a constant, because base installs import this module
    without the source-readers extra that carries SpicyDocs.
    """
    from spicy_docs.schemas.federal_register import FEDERAL_REGISTER_COLUMNS

    return (*FEDERAL_REGISTER_COLUMNS, "rin")

#: ``rin`` as a projection of the array, for every row of the merged table.
#: ``[]`` and NULL both give NULL: a join key is present or it is not.
_RIN_SQL = "json_extract_string(regulation_id_numbers_json, '$[0]')"


def _prior_columns(prior_file: Path, columns: tuple[str, ...]) -> str:
    """``columns`` as selected from the prior table; one it predates reads NULL."""
    held = set(pq.read_schema(prior_file).names)
    return ", ".join(c if c in held else f"CAST(NULL AS VARCHAR) AS {c}" for c in columns)


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


def _live_federal_register_documents(since: date) -> Iterator[dict]:
    """Yield raw FR document dicts published in ``[since, today]`` through the SpicyDocs owner.

    The owner acquires exact API pages with its cap-safe date-window traversal:
    a window whose ``count`` reaches the 10,000-result cap is split, and a
    single day that still reads capped refuses the run rather than publish a
    hole. Pages the owner declares included carry the raw API results unchanged;
    capped probe pages carry none (their halves do), so those are skipped here.

    One traversal: the merge below deduplicates repeated observations and
    refuses conflicting ones itself, so the owner's two-traversal
    reconciliation would double the request budget for a contract this
    transform already checks.
    """
    try:
        from spicy_docs.sources.federal_register import native as federal_register
        from spicy_docs.transport.acquisition import federal_register_fetcher
    except ModuleNotFoundError as error:
        if error.name == "spicy_docs":
            raise RuntimeError(
                "The Federal Register acquisition requires spicy-regs[source-readers]. "
                "Run `uv sync --frozen` in a SpicyRegs checkout."
            ) from None
        raise
    scope = {
        "publishedFrom": since.isoformat(),
        "publishedThrough": date.today().isoformat(),
    }
    with federal_register_fetcher(None) as fetch:
        for page in federal_register.iter_federal_register_pages(fetch, query_scope=scope, traversals=1):
            response = federal_register.parse_page_response(page.response_bytes)
            if not federal_register.federal_register_records_included(response, query_scope=scope, page_window=None):
                continue
            yield from response["results"]


def build_federal_register(
    output_dir: Path,
    *,
    since: date | None = None,
    documents: Callable[[date], Iterable[dict]] | None = None,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> Path:
    """Build ``federal_register.parquet`` (incremental merge with the prior table).

    ``documents`` is the raw API records published since a date; the default is
    SpicyDocs' Federal Register acquisition, and a hermetic test passes its own.
    """
    import duckdb
    from spicy_docs.schemas.federal_register import FEDERAL_REGISTER_COLUMNS, project_federal_register_document

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_fr_prior.parquet"
    schema = pa.schema([(c, pa.string()) for c in FEDERAL_REGISTER_COLUMNS])

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
    fetch = documents or _live_federal_register_documents
    rows = [project_federal_register_document(doc) for doc in fetch(since)]
    new_file = output_dir / "_fr_new.parquet"
    table = pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table()
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

    # The projection's columns are what both sides carry; a prior that already
    # has ``rin`` is not read for it, since the projection below recomputes it,
    # and a column the prior predates (``topics_json``) reads NULL.
    cols = ", ".join(FEDERAL_REGISTER_COLUMNS)
    if have_prior:
        union = (
            f"SELECT {_prior_columns(prior_file, FEDERAL_REGISTER_COLUMNS)}, file_row_number AS _row, 0 AS _src "
            f"FROM read_parquet('{prior_file}', file_row_number=true) "
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
