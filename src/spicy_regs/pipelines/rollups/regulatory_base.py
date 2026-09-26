"""Rollup pipelines: the ETL's dockets and documents as managed families.

The ETL rewrites the bare ``dockets.parquet`` and ``documents.parquet`` after
every sweep batch and keeps reading them as its working copies. Once a sweep
completes, these publish each as its own generation family, so index-aware
readers and DocSpec's by-reference admission see one verified snapshot per
sweep. Bare-URL readers keep reading the working copies.
"""

from pathlib import Path
from typing import ClassVar

import duckdb
import pyarrow.parquet as pq

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import r2


#: DocSpec admits a member by reference only when every row group is at most
#: 256 MiB uncompressed. The writers sort, and DuckDB 1.5 ignores
#: ``ROW_GROUP_SIZE_BYTES`` for sorted output (measured 2026-09-26), so the
#: bound is checked here, where a refusal is visible in the run.
MAX_ROW_GROUP_BYTES = 256 * 2**20


class _BaseTableFamily(RollupPipeline):
    """Publish the ETL's completed working copy of one base table."""

    def build(self, output_dir: Path) -> Path:
        path = output_dir / self.output
        if not r2.download_working_copy(self.output, path):
            raise RuntimeError(f"{self.output}: no working copy on R2 to publish")
        key = RECORD_TYPES[self.name].dedup_key
        rows, distinct, missing = (
            duckdb.connect()
            .from_parquet(str(path))
            .aggregate(f"count(*), count(DISTINCT {key}), count(*) FILTER (WHERE {key} IS NULL)")
            .fetchall()[0]
        )
        if missing or distinct != rows:
            raise RuntimeError(f"{self.output}: {missing} rows lack {key} and {rows - missing - distinct} repeat one")
        metadata = pq.ParquetFile(path).metadata
        largest = max((metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups)), default=0)
        if largest > MAX_ROW_GROUP_BYTES:
            raise RuntimeError(f"{self.output}: a {largest / 2**20:.1f} MiB row group exceeds the admission bound")
        return path


class DocketsFamily(_BaseTableFamily):
    name: ClassVar[str] = "dockets"
    output: ClassVar[str] = "dockets.parquet"


class DocumentsFamily(_BaseTableFamily):
    name: ClassVar[str] = "documents"
    output: ClassVar[str] = "documents.parquet"


dockets_app = make_rollup_app(DocketsFamily)
documents_app = make_rollup_app(DocumentsFamily)
