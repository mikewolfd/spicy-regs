"""One-pass streaming writer for Hive-partitioned Parquet trees.

The comment writers used to run one COPY per output partition, re-scanning the
staging table or the monolithic parent for every group (O(groups x rows)).
This module writes the whole tree in one ordered pass: the caller supplies one
SQL query ordered by the group keys, and rows stream through batch reads into
one open Parquet writer per group, opened and finalized as group boundaries
pass. Output bytes carry the same rows, schema and ordering each per-group
COPY produced; the caller keeps its own preflight and index update.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.compute
import pyarrow.parquet as pq


def _batch_keys(batch: pa.RecordBatch, key_columns: tuple[str, ...]) -> np.ndarray:
    """One comparable key string per row, with NULLs rendered as a distinct marker.

    Group boundaries are read from this vector; building it costs one pass per
    batch, not one pass per group.
    """
    strings = [
        pa.compute.fill_null(pa.compute.cast(batch.column(name), pa.string()), "\\x00null")
        for name in key_columns
    ]
    joined = pa.compute.binary_join_element_wise(strings[0], strings[1], "-") if len(strings) == 2 else strings[0]  # ty: ignore[unresolved-attribute]
    for extra in strings[2:]:
        joined = pa.compute.binary_join_element_wise(joined, extra, "-")  # ty: ignore[unresolved-attribute]
    return np.asarray(joined)


def write_partition_stream(
    con: duckdb.DuckDBPyConnection,
    sql: str,
    *,
    key_columns: tuple[str, ...],
    path_of: Callable[[tuple], Path],
    label: str,
    drop_columns: tuple[str, ...] = (),
) -> list[Path]:
    """Run one ordered query and write each group to its ``path_of(key)`` file.

    ``sql`` MUST order rows by ``key_columns`` (grouping keys first, any
    within-group order after). ``key_columns`` that are only partition keys and
    not data columns are listed in ``drop_columns`` and never written. Rows
    stream through ``read_next_batch``; every group is written to a temporary
    file that replaces the final ``part-0.parquet`` only when the group is
    complete, so an interrupted run never leaves a half-written partition
    under its canonical name. Returns the written paths in group order.
    """
    reader = con.execute(sql).fetch_record_batch(1 << 20)
    written: list[Path] = []
    current_key: tuple | None = None
    writer: pq.ParquetWriter | None = None
    temp_path: Path | None = None
    final_path: Path | None = None

    def finalize() -> None:
        nonlocal writer, temp_path, final_path
        if writer is not None:
            writer.close()
            assert temp_path is not None and final_path is not None
            temp_path.replace(final_path)
            written.append(final_path)
            writer, temp_path, final_path = None, None, None

    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if batch.num_rows == 0:
            continue
        keys = _batch_keys(batch, key_columns)
        key_values = [batch.column(name) for name in key_columns]
        if drop_columns:
            batch = batch.drop_columns(list(drop_columns))
        # Group boundaries within this batch: every position where the key
        # changes. The stream is globally ordered, so spans are contiguous.
        starts = [0]
        starts.extend(int(i) for i in np.nonzero(keys[1:] != keys[:-1])[0] + 1)
        starts.append(batch.num_rows)
        for span_start, span_end in zip(starts, starts[1:]):
            if span_end <= span_start:
                continue
            key = tuple(column[span_start].as_py() for column in key_values)
            if key != current_key:
                finalize()
                current_key = key
                final_path = path_of(key)
                assert final_path is not None
                final_path.parent.mkdir(parents=True, exist_ok=True)
                temp_path = final_path.with_suffix(".tmp.parquet")
                writer = pq.ParquetWriter(temp_path, batch.schema, compression="zstd")
            assert writer is not None
            writer.write_batch(batch.slice(span_start, span_end - span_start))
    finalize()
    return written
