"""Bounded Arrow writes that replace the destination only on full consumption."""

from collections.abc import Iterable
from itertools import islice
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq


def write_rows(records: Iterable[dict], destination: Path, schema: pa.Schema, *, batch_size: int = 2_000) -> Path:
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    rows = iter(records)
    with TemporaryDirectory(dir=destination.parent) as scratch:
        temporary = Path(scratch) / "rows.parquet"
        with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            while batch := list(islice(rows, batch_size)):
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
        temporary.replace(destination)
    return destination
