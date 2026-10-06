"""Bounded Arrow writes that replace the destination only on full consumption, and the VARCHAR cell rule."""

from collections.abc import Iterable
from itertools import islice
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as pq


def write_rows(records: Iterable[dict], destination: Path, schema: pa.Schema, *, batch_size: int = 2_000) -> Path:
    """Write ``records`` through ``schema`` in ``batch_size`` batches, replacing ``destination`` only when consumed.

    A non-integer or non-positive ``batch_size`` raises ValueError.
    """
    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    from spicy_regs.native_types import extra_field_checker
    rows = iter(records)
    with TemporaryDirectory(dir=destination.parent) as scratch:
        temporary = Path(scratch) / "rows.parquet"
        with pq.ParquetWriter(temporary, schema, compression="zstd") as writer:
            check_fields = extra_field_checker(schema)
            while batch := list(islice(rows, batch_size)):
                for row in batch:
                    check_fields(row)
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
        temporary.replace(destination)
    return destination


def str_or_none(value: object) -> str | None:
    """A source scalar as an all-VARCHAR table stores it: NULL stays NULL, anything else is ``str(value)``.

    The one spelling of the rule eight ingest transforms each restated as ``_s`` (DRY work list F35). An empty string
    stays empty; the court clusters' rule, which reads it as NULL, is that transform's own.
    """
    return None if value is None else str(value)
