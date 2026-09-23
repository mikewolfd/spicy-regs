"""Bounded Parquet staging and the disk floor shared by the CourtListener tables."""

import os
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

#: Free space this project refuses to eat into, in bytes. A bulk ingest that
#: would cross it is stopped and recorded rather than run. It is the default for
#: a workstation holding retained evidence; a machine with a different budget,
#: such as a CI runner with tens of GB in total, states its own floor in GiB
#: through ``DISK_FLOOR_ENV``.
DISK_HEADROOM_FLOOR = 100 * 2**30
DISK_FLOOR_ENV = "SPICY_REGS_DISK_FLOOR_GIB"


def disk_floor() -> int:
    """The floor in bytes: ``DISK_FLOOR_ENV`` when set, otherwise ``DISK_HEADROOM_FLOOR``."""
    stated = os.environ.get(DISK_FLOOR_ENV, "").strip()
    return int(float(stated) * 2**30) if stated else DISK_HEADROOM_FLOOR


def check_headroom(needed_bytes: int, *, path: Path | None = None) -> None:
    """Refuse an ingest that would take free space below the project floor.

    Raises rather than warning: a run that silently fills the disk is worse than
    a run that did not happen, and the whole point of recording sizes first is to
    be able to make this call before the bytes arrive.
    """
    usage = shutil.disk_usage(path or Path.home())
    remaining = usage.free - needed_bytes
    floor = disk_floor()
    if remaining < floor:
        raise RuntimeError(
            f"CourtListener bulk: refusing to ingest {needed_bytes / 2**30:.1f} GiB — "
            f"would leave {remaining / 2**30:.1f} GiB free, below the "
            f"{floor / 2**30:.0f} GiB floor "
            f"(currently {usage.free / 2**30:.1f} GiB free)"
        )


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
        """Flush and finish the file, writing an empty table when nothing was added."""
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
