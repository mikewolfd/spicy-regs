"""Transform: build ``usaspending_recipients.parquet`` from USASpending.gov.

Produces a pinned 6-column all-VARCHAR schema keyed on ``recipient_id`` — the
federal-award **recipient** reference dimension that sits alongside the
regulations.gov corpus and complements the SAM entity registry and FEC
committee table for resolving organization commenters by their federal
funding. The same UEI can appear at multiple ``recipient_level`` values (parent
``P`` / child ``C`` / standalone ``R``), so ``recipient_id``, unique per level,
is the primary/dedup key; consumers filter by level and join on ``uei``.

Scope is deliberately bounded to the **top-N recipients by all-time federal
award amount**: the endpoint reports ~18M recipients across all history, so a
full walk is infeasible and the largest-funded organizations are both the most
resolution-useful and naturally bounded. There is no watermark — recipients
are a reference dimension, not a time series — so each run fetches the current
top-N and merges it with the prior table, dedup on ``recipient_id`` preferring
the fresh row, which keeps the row count monotonic and clear of the R2
catastrophic-shrink guard.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.usaspending import UsaSpendingRecipientsReader

OUTPUT = "usaspending_recipients.parquet"

# The published schema: 6 columns, all VARCHAR, in a fixed order.
# ``recipient_id`` is the primary / dedup key.
COLUMNS = (
    "recipient_id",
    "uei",
    "duns",
    "name",
    "recipient_level",
    "total_award_amount",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (``amount`` comes as a float.)"""
    if value is None:
        return None
    return str(value)


def _shape(doc: dict) -> dict:
    """Map one raw USASpending recipient onto the published column shape."""
    return {
        "recipient_id": doc.get("id"),
        "uei": doc.get("uei"),
        "duns": doc.get("duns"),
        "name": doc.get("name"),
        "recipient_level": doc.get("recipient_level"),
        "total_award_amount": _s(doc.get("amount")),
    }


def build_usaspending_recipients(output_dir: Path, *, max_pages: int | None = None) -> Path:
    """Build ``usaspending_recipients.parquet`` (top-N merged with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_usaspending_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means a clean build).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("USASpending recipients: merging against prior table {}", prior_file)
    else:
        logger.info("USASpending recipients: no prior table found — clean build")

    # 2. Fetch + shape into a "new rows" parquet.
    reader = (
        UsaSpendingRecipientsReader(max_pages=max_pages) if max_pages is not None else UsaSpendingRecipientsReader()
    )
    rows = [_shape(doc) for doc in reader.iter_records()]
    new_file = output_dir / "_usaspending_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("USASpending recipients: fetched {:,} recipients this run", len(rows))

    # 3. Merge prior + new, dedup on recipient_id preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        union = (
            f"SELECT {cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY recipient_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE recipient_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY TRY_CAST(total_award_amount AS DOUBLE) DESC NULLS LAST, recipient_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("USASpending recipients: {:,} rows", total)
    return out_file
