"""Republish selected native regulatory subjects with their exact shared receipts.

Scheduled refresh jobs preserve the selected generation's source evidence while
rebinding receipts to the newly sealed generation. No bare processing copy is a
publication input.
"""

from pathlib import Path
from typing import ClassVar

import duckdb
import pyarrow.parquet as pq

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.etl_receipts import select_receipts, rebind_receipt, RECEIPT_SCHEMA
from spicy_regs.transforms.parquet_rows import write_rows
from spicy_regs.transforms.regulations_receipts import policy
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
import shutil


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

    def generation_schemas(self):
        dataset = self.output.removesuffix(".parquet")
        return {dataset: described_schema(policy(dataset).subject_schema)}

    def build(self, output_dir: Path) -> Path:
        path = output_dir / self.output
        dataset = self.output.removesuffix(".parquet")
        selected = SelectedPriors(output_dir / ".selected")
        if selected.get(dataset) is None:
            raise RuntimeError(f"{self.output}: no selected native generation to publish")
        subjects, receipts, _ = selected.selections[dataset]
        if len(subjects) != 1:
            raise ValueError("Base regulatory family requires one subject member")
        shutil.copyfile(subjects[0], path)
        scoped = select_receipts(receipts, output_dir / ".selected-receipts.parquet", dataset=dataset)
        write_rows(
            (
                rebind_receipt(row, generation_id=self.receipt_generation_id)
                for batch in pq.ParquetFile(scoped).iter_batches()
                for row in batch.to_pylist()
            ),
            output_dir / "etl_receipts.parquet",
            RECEIPT_SCHEMA,
        )
        type(self).receipt_policies = (policy(dataset),)
        key = self.key()
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
