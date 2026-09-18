"""Bounded Parquet staging for the two CourtListener opinion tables."""

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


class CourtListenerTableWriter:
    """Append shaped rows to a parquet file in bounded batches."""

    def __init__(self, path: Path, *, schema: pa.Schema, batch_size: int) -> None:
        self.path = path
        self.schema = schema
        self.batch_size = batch_size
        self.rows: list[dict] = []
        self.written = 0
        self._writer: pq.ParquetWriter | None = None

    def add(self, row: dict) -> None:
        self.rows.append(row)
        if len(self.rows) >= self.batch_size:
            self.flush()

    def flush(self) -> None:
        if not self.rows:
            return
        table = pa.Table.from_pylist(self.rows, schema=self.schema)
        if self._writer is None:
            self._writer = pq.ParquetWriter(self.path, self.schema, compression="zstd")
        self._writer.write_table(table)
        self.written += len(self.rows)
        self.rows.clear()

    def close(self) -> None:
        self.flush()
        if self._writer is None:
            pq.write_table(self.schema.empty_table(), self.path, compression="zstd")
        else:
            self._writer.close()

    def abort(self) -> None:
        """Close without flushing and discard this failed attempt's staging file."""
        try:
            if self._writer is not None:
                self._writer.close()
        finally:
            self.rows.clear()
            self.path.unlink(missing_ok=True)
