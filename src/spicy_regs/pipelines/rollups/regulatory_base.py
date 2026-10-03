"""Rollup pipelines: the ETL's dockets, documents and the three attribute tables as managed families.

The ETL rewrites the bare ``dockets.parquet`` and ``documents.parquet`` after
every sweep batch and keeps reading them as its working copies; the same read
merges ``docket_attributes.parquet`` and ``document_attributes.parquet``
(decisions 65-67). Once a sweep completes, these publish each as its own
generation family, so index-aware readers and DocSpec's by-reference admission
see one verified snapshot per sweep. Bare-URL readers keep reading the working
copies. The documents family alone adds two columns, from the reconcile step's
bare outcomes, which the working copy never carries.
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

    def key(self) -> str:
        """The column every row must carry once."""
        return RECORD_TYPES[self.name].dedup_key

    def build(self, output_dir: Path) -> Path:
        path = output_dir / self.output
        if not r2.download_working_copy(self.output, path):
            raise RuntimeError(f"{self.output}: no working copy on R2 to publish")
        key = self.key()
        rows, distinct, missing = (
            duckdb.connect()
            .from_parquet(str(path))
            .aggregate(f"count(*), count(DISTINCT {key}), count(*) FILTER (WHERE {key} IS NULL)")
            .fetchall()[0]
        )
        if missing or distinct != rows:
            raise RuntimeError(f"{self.output}: {missing} rows lack {key} and {rows - missing - distinct} repeat one")
        self.annotate(path)
        metadata = pq.ParquetFile(path).metadata
        largest = max((metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups)), default=0)
        if largest > MAX_ROW_GROUP_BYTES:
            raise RuntimeError(f"{self.output}: a {largest / 2**20:.1f} MiB row group exceeds the admission bound")
        return path

    def annotate(self, path: Path) -> None:
        """Add what the family publishes beyond the working copy; the base tables add nothing."""


class DocketsFamily(_BaseTableFamily):
    name: ClassVar[str] = "dockets"
    output: ClassVar[str] = "dockets.parquet"


class DocumentsFamily(_BaseTableFamily):
    """The ETL's documents, each with what Regulations.gov last said of it (``pipelines.docket_reconcile``)."""

    name: ClassVar[str] = "documents"
    output: ClassVar[str] = "documents.parquet"

    def annotate(self, path: Path) -> None:
        from spicy_regs.pipelines.docket_reconcile import OUTCOMES, with_publisher_status

        outcomes = path.with_name(OUTCOMES)
        with_publisher_status(path, outcomes if r2.download_working_copy(OUTCOMES, outcomes) else None)


class _AttributesFamily(_BaseTableFamily):
    def key(self) -> str:
        from spicy_regs.transforms.regulations_attributes import contract

        (identity,) = contract(self.output.removesuffix(".parquet")).identity
        return identity


class DocketAttributesFamily(_AttributesFamily):
    name: ClassVar[str] = "docket-attributes"
    output: ClassVar[str] = "docket_attributes.parquet"


class DocumentAttributesFamily(_AttributesFamily):
    name: ClassVar[str] = "document-attributes"
    output: ClassVar[str] = "document_attributes.parquet"


class CommentAttributesFamily(_AttributesFamily):
    """Seeded by the comment re-read (``fill-comment-fields attributes``), then kept by the ETL's comment passes."""

    name: ClassVar[str] = "comment-attributes"
    output: ClassVar[str] = "comment_attributes.parquet"


dockets_app = make_rollup_app(DocketsFamily)
documents_app = make_rollup_app(DocumentsFamily)
docket_attributes_app = make_rollup_app(DocketAttributesFamily)
document_attributes_app = make_rollup_app(DocumentAttributesFamily)
comment_attributes_app = make_rollup_app(CommentAttributesFamily)
