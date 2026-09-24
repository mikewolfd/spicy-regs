"""Transform: write parsed records to a per-agency staging Parquet file."""

from collections.abc import Iterable
from contextlib import ExitStack
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

MAX_BATCH_ROWS = 4096
MAX_BATCH_CHARACTERS = 8 * 1024 * 1024


def write_staging(
    agency: str,
    data_type: str,
    records: Iterable[dict],
    staging_dir: Path,
    schema: dict,
) -> int:
    """Stream bounded row groups; expose the staging file only after a complete read."""
    staging_type_dir = staging_dir / data_type
    staging_type_dir.mkdir(parents=True, exist_ok=True)
    staging_file = staging_type_dir / f"{agency}.parquet"

    temporary = staging_file.with_suffix(".parquet.tmp")  # excluded from every staging *.parquet scan
    rows: list[dict] = []
    characters = total = 0
    try:
        with ExitStack() as resources:
            writer = None

            def flush() -> None:
                nonlocal writer, total
                if not rows:
                    return
                table = pl.DataFrame(rows, schema=schema).to_arrow()
                if writer is None:
                    writer = resources.enter_context(pq.ParquetWriter(temporary, table.schema, compression="zstd"))
                writer.write_table(table)
                total += len(rows)
                rows.clear()

            for record in records:
                rows.append(record)
                characters += sum(len(value) for value in record.values() if isinstance(value, str))
                if len(rows) >= MAX_BATCH_ROWS or characters >= MAX_BATCH_CHARACTERS:
                    flush()
                    characters = 0
            flush()
        if total:
            temporary.replace(staging_file)
        else:
            staging_file.unlink(missing_ok=True)
        return total
    finally:
        temporary.unlink(missing_ok=True)
