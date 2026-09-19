"""Shared merge helper for incremental, deduplicated Parquet tables.

Every rollup that ingests an external source and republishes an incremental
table follows the same three steps: best-effort download the prior published
table from R2, dedup the union of prior + freshly fetched rows on an identity
key (preferring the fresh row), and order the result by a version column.
Lifted out of ``build_congress_bills.py`` (its original prior-download + DuckDB
merge, ~lines 135-210) and parameterised over the column tuple, identity, and
version column, so a table with a different key shape than a single
``bill_id`` does not have to copy the SQL. ``build_congress_bills`` now calls
this directly; the behavior-preservation proof is
``tests/test_congress_bills.py::test_build_congress_bills_merges_prior_and_fresh_rows``,
which seeds a prior Parquet, stubs the fetch and the R2 download, and asserts
on the merged output — the other tests in that file cover ``_shape``,
``_bill_id`` and windowing only, never the merge itself.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2


def prior_scratch_path(output_dir: Path, name: str) -> Path:
    """The local path :func:`merge_table` caches/reuses the prior table under.

    Public so a caller that must inspect the prior table *before* merging —
    e.g. computing an incremental fetch window from its max version column —
    can download to this exact path first. When that download *succeeds*,
    :func:`merge_table` finds the file already present and does not re-download
    it. When it fails (no prior table exists yet — a cold start), no file is
    written, and ``merge_table`` cannot tell "not tried" from "tried and
    absent" from the path alone; pass its own result as ``prior_present`` to
    ``merge_table`` so a known-absent prior isn't downloaded a second time.
    """
    return output_dir / f"_{name}_prior.parquet"


def merge_table(
    output_dir: Path,
    *,
    name: str,
    columns: tuple[str, ...],
    identity: tuple[str, ...],
    version_column: str,
    rows: Iterable[Mapping[str, object]],
    remote_key: str,
    download_prior: Callable[[str, Path], bool] = r2.download,
    prior_present: bool | None = None,
) -> Path:
    """Merge freshly fetched ``rows`` against the prior ``remote_key`` table.

    Writes ``output_dir / remote_key``: the union of the prior table (best-effort
    downloaded via ``download_prior``, defaulting to :func:`spicy_regs.sources.r2.download`)
    and ``rows``, deduplicated on ``identity`` (all columns must be non-null;
    the fresh row wins over the prior one on a repeated identity), ordered by
    ``version_column`` descending then ``identity``.

    ``columns`` becomes an all-VARCHAR Arrow schema — every published table in
    this pipeline is string-typed, so callers coerce before calling this. An
    absent prior table (first run, or ``download_prior`` returning ``False``)
    degrades to publishing ``rows`` alone, which is a full backfill, not an
    error.

    ``prior_present``: pass ``False`` when the caller already tried
    :func:`prior_scratch_path` and knows the prior is absent (e.g. it read the
    prior table's max version to size an incremental fetch window first, and
    that download itself returned ``False``) — this skips calling
    ``download_prior`` a second time for the same known-absent object.
    ``True`` or the default ``None`` leave the existing
    "already on disk, else ask ``download_prior``" behavior unchanged.
    """
    import duckdb

    out_file = output_dir / remote_key
    prior_file = prior_scratch_path(output_dir, name)
    new_file = output_dir / f"_{name}_new.parquet"

    schema = pa.schema([(c, pa.string()) for c in columns])

    # 1. Pull the prior table (best effort — absence just means full backfill).
    # ``prior_present is False`` is the caller telling us it already tried and
    # found nothing; anything else falls back to disk-then-download.
    if prior_present is False:
        have_prior = False
    else:
        have_prior = prior_file.exists() or download_prior(remote_key, prior_file)
    if have_prior:
        logger.info("{}: merging against prior table {}", name, prior_file)
    else:
        logger.info("{}: no prior table found — full backfill", name)

    # 2. Shape the freshly fetched rows into a "new rows" parquet.
    row_list = list(rows)
    table = pa.Table.from_pylist(row_list, schema=schema) if row_list else schema.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("{}: {:,} fresh rows this run", name, len(row_list))

    # 3. Merge prior + new, dedup on identity preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(columns)
    key_cols = ", ".join(identity)
    not_null = " AND ".join(f"{c} IS NOT NULL" for c in identity)

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
                    PARTITION BY {key_cols} ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE {not_null}
            )
            WHERE _rn = 1
            ORDER BY {version_column} DESC, {key_cols}
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("{}: {:,} rows", name, total)
    return out_file
