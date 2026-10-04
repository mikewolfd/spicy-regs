"""Build court docket subjects and receipts from CourtListener search results.

The subject retains docket facts and native lists of parties, attorneys and
firms. Lists preserve order, repetitions, null elements and empty strings.
An unknown list stays NULL; an explicitly empty publisher list stays empty.
Source URLs, capture inputs and publisher record timestamps stay in receipts.

Incremental builds reconstruct private processing inputs from the selected
subject and receipt generation. The dated overlap, bounded fill of unnamed dockets and case-type rule
remain in force. A failed source walk leaves the previous output pair intact.
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.parquet_rows import str_or_none
from spicy_regs.sources import r2
from spicy_regs.sources.courtlistener import (
    CourtListenerDocketIdReader, CourtListenerReader, docket_id_queries, record_json,
)
from spicy_regs.transforms.table_merge import merge_local_prior
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.court_receipts import file_witness, finish_court_output, prior_receipt_selection, restore_processing_input, admit_court_updates

OUTPUT = "court_dockets.parquet"

CL_BASE_URL = "https://www.courtlistener.com"

# Re-scan this many days before the last stored date_filed on each run, so
# dockets indexed/corrected after the watermark are picked up.
OVERLAP_DAYS = 14

# The published schema: all VARCHAR, keyed by cl_docket_id. Array fields are JSON
# strings.
COLUMNS = (
    "cl_docket_id",
    "case_name",
    "case_name_full",
    "court_id",
    "court",
    "court_citation_string",
    "docket_number",
    "date_filed",
    "date_terminated",
    "date_argued",
    "nature_of_suit",
    "cause",
    "jurisdiction_type",
    "jury_demand",
    "assigned_to",
    "referred_to",
    "parties_json",
    "attorneys_json",
    "firms_json",
    "pacer_case_id",
    "date_created",
    "absolute_url",
    "blocked",
    "date_blocked",
    "raw_source_record",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])
#: Recomputed from ``docket_number`` over every merged row.
DERIVED = {"case_type": r"lower(nullif(regexp_extract(docket_number, '^\s*(?:\d+:)?\d{2}-([A-Za-z]{1,4})-\d', 1), ''))"}
PUBLISHED_COLUMNS = tuple(SUBJECT_SCHEMAS['court_dockets'].names)
#: About 38 ids a query and two 20-row pages each: 30 requests, with the daily
#: delta well inside the 50-an-hour and 125-a-day limits.
FILL_QUERIES_PER_RUN = 15



def _array_input(value: object) -> str | None:
    """Retain raw arrays, including malformed variants for receipt-only refusal."""
    return None if value is None else record_json(value)


def _abs_url(docket: dict) -> str | None:
    """Make the docket's site-relative ``docket_absolute_url`` an absolute URL."""
    rel = docket.get("docket_absolute_url")
    if not rel:
        return None
    return f"{CL_BASE_URL}{rel}" if str(rel).startswith("/") else str(rel)


def _shape(docket: dict) -> dict:
    """Map one raw CourtListener RECAP search result onto the published columns."""
    meta = docket.get("meta") or {}
    return {
        "cl_docket_id": str_or_none(docket.get("docket_id")),
        "case_name": docket.get("caseName"),
        "case_name_full": docket.get("case_name_full"),
        "court_id": docket.get("court_id"),
        "court": docket.get("court"),
        "court_citation_string": docket.get("court_citation_string"),
        "docket_number": docket.get("docketNumber"),
        "date_filed": docket.get("dateFiled"),
        "date_terminated": docket.get("dateTerminated"),
        "date_argued": docket.get("dateArgued"),
        "nature_of_suit": docket.get("suitNature"),
        "cause": docket.get("cause"),
        "jurisdiction_type": docket.get("jurisdictionType"),
        "jury_demand": docket.get("juryDemand"),
        "assigned_to": docket.get("assignedTo"),
        "referred_to": docket.get("referredTo"),
        "parties_json": _array_input(docket.get("party")),
        "attorneys_json": _array_input(docket.get("attorney")),
        "firms_json": _array_input(docket.get("firm")),
        "pacer_case_id": str_or_none(docket.get("pacer_case_id")),
        "date_created": meta.get("date_created") if isinstance(meta, dict) else None,
        "absolute_url": _abs_url(docket),
        "blocked": str_or_none(docket.get("blocked")),
        "date_blocked": docket.get("date_blocked"),
        "raw_source_record": record_json(docket),
    }


def _prior_max_date_filed(prior_file: Path) -> date | None:
    """Largest ``date_filed`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(date_filed) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def _unnamed_dockets(prior_file: Path) -> list[str]:
    """The prior's docket ids with NULL party names, in numeric order, so each run takes the next slice."""
    import duckdb

    path = str(prior_file).replace("'", "''")
    rows = duckdb.sql(
        f"SELECT cl_docket_id FROM read_parquet('{path}') WHERE parties_json IS NULL "
        "ORDER BY TRY_CAST(cl_docket_id AS BIGINT), cl_docket_id"
    ).fetchall()
    return [row[0] for row in rows]


def build_courtlistener(
    output_dir: Path,
    *,
    evidence: CaptureEvidence | None = None,
    since: date | None = None,
    max_records: int | None = None,
) -> Path:
    """Build ``court_dockets.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = out_file if out_file.is_symlink() else output_dir / "_cl_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    input_witnesses = [file_witness(prior_file)] if have_prior else []
    receipt_path = None
    if have_prior:
        receipt_path, generation_id = prior_receipt_selection(prior_file, dataset='court_dockets')
        if receipt_path is not None:
            input_witnesses.append(file_witness(receipt_path))
        prior_file = restore_processing_input(prior_file, output_dir / f'.cl-prior-{uuid4().hex}.parquet',
            dataset='court_dockets', schema=_SCHEMA, receipt_path=receipt_path,
            generation_id=generation_id)
    if have_prior:
        logger.info("CourtListener: merging against prior table {}", prior_file)
    else:
        logger.info("CourtListener: no prior table found — full backfill")

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_date_filed(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else None
    logger.info("CourtListener: fetching dockets filed since {}", since or "the beginning")

    # 3. Fetch + shape into a "new rows" parquet: the dated delta, then a
    # bounded slice of the prior's dockets that still lack party names.
    reader = CourtListenerReader(since=since, max_records=max_records, evidence=evidence)
    fresh = {row["cl_docket_id"]: row for row in map(_shape, reader.iter_records())}
    logger.info("CourtListener: fetched {:,} dockets this run", len(fresh))
    if have_prior and max_records is None:
        unnamed = _unnamed_dockets(prior_file)
        queries = docket_id_queries(unnamed)
        for query in queries[:FILL_QUERIES_PER_RUN]:
            for docket in CourtListenerDocketIdReader(query=query, evidence=evidence).iter_records():
                row = _shape(docket)
                fresh.setdefault(row["cl_docket_id"], row)
        filled = sum(1 for docket_id in unnamed if docket_id in fresh)
        logger.info("CourtListener: re-read {:,} of {:,} dockets without party names ({} of {} queries)",
                    filled, len(unnamed), min(len(queries), FILL_QUERIES_PER_RUN), len(queries))
    new_file = output_dir / f".cl-new-{uuid4().hex}.parquet"
    rows = list(fresh.values())
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")

    # Admit fresh rows before replacing prior identities. Refused updates retain
    # their receipt while the last valid subject remains eligible for the merge.
    build_generation_id = uuid4().hex
    new_file, fresh_receipts = admit_court_updates(
        'court_dockets', new_file, output_dir / '.court-updates' / build_generation_id,
        schema=_SCHEMA, generation_id=build_generation_id,
    )

    # 4. Merge admitted updates over prior rows.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    staged = output_dir / f'.cl-merged-{uuid4().hex}.parquet'
    merge_local_prior(
        con,
        columns=COLUMNS,
        identity="cl_docket_id",
        order_by="date_filed DESC, cl_docket_id",
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=staged,
        derived=DERIVED,
    )
    con.close()

    out_file = finish_court_output('court_dockets', staged, output_dir,
        witnesses=[file_witness(staged), *input_witnesses],
        diagnostics={'source_selection_max_records': max_records},
        generation_id=build_generation_id, refused_receipts=fresh_receipts, prior_receipts=receipt_path)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Court dockets: {:,} rows", total)
    return out_file
