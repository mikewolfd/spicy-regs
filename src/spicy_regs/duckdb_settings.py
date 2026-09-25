"""Validated DuckDB settings shared by the MCP server and the merge transforms."""

import re
from dataclasses import dataclass
from pathlib import Path

#: A size with a unit, as DuckDB 1.5 parses it (``4GB``, ``2048MB``, ``16GiB``, ``1.5 GB``).
#: DuckDB refuses a percentage at ``SET``, and reads a bare number as bytes.
_MEMORY_LIMIT = re.compile(r"\d+(\.\d+)?\s*[KMGT]i?B", re.IGNORECASE)


def memory_limit(raw: str, source: str) -> str:
    """``raw`` stripped, or a ``RuntimeError`` naming ``source``; the value is interpolated into a ``SET``."""
    value = raw.strip()
    if not _MEMORY_LIMIT.fullmatch(value):
        raise RuntimeError(f"{source} is not a valid size: {raw!r}")
    return value


@dataclass(frozen=True)
class ExportResources:
    """Shared comments writer budgets; targets, not hard process-memory ceilings."""

    memory: str = "3GB"
    threads: int = 1
    row_group_rows: int = 20_000
    row_group_bytes: str = "16MB"

    def __post_init__(self) -> None:
        memory_limit(self.memory, "export memory limit")
        memory_limit(self.row_group_bytes, "export row group size")
        if self.threads < 1 or self.row_group_rows < 1:
            raise ValueError("Export threads and row group rows must be positive")

    def configure(self, con, spill_dir: Path) -> None:
        spill_dir.mkdir(parents=True, exist_ok=True)
        for name, value in (
            ("memory_limit", self.memory), ("threads", self.threads),
            ("temp_directory", str(spill_dir)), ("preserve_insertion_order", False),
            ("partitioned_write_max_open_files", 8), ("partitioned_write_flush_threshold", 2048),
        ):
            con.execute(f"SET {name} = ?", [value])

    @property
    def parquet_options(self) -> dict:
        return {"compression": "zstd", "row_group_size": self.row_group_rows,
                "row_group_size_bytes": self.row_group_bytes}
