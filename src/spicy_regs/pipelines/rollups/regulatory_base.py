"""Apply Regulations.gov reconciliation observations to the published documents table.

All five base regulatory families (``dockets``, ``documents``, and the three
attribute tables) publish directly from the ETL without per-row receipts
(ss-vse3, ss-ojz3). Only ``documents`` keeps a scheduled refresh step, because
``reconcile-dockets`` updates ``docket_reconcile_outcomes.parquet`` on its own
schedule between ETL sweeps. The applied outcomes SHA-256 is recorded in
``documents.parquet``'s Arrow schema metadata so unchanged runs skip cleanly.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import ClassVar

import duckdb
import pyarrow.parquet as pq

from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.base import RollupPipeline, Unchanged, make_rollup_app
from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.transforms.regulations_receipts import policy


#: DocSpec admits a member by reference only when every row group is at most
#: 256 MiB uncompressed. The writers sort, and DuckDB 1.5 ignores
#: ``ROW_GROUP_SIZE_BYTES`` for sorted output (measured 2026-09-26), so the
#: bound is checked here, where a refusal is visible in the run.
MAX_ROW_GROUP_BYTES = 256 * 2**20


def validate_base_table_output(path: Path, *, key: str) -> None:
    """Apply the maintained base-family identity and row-group admission checks."""
    rows, distinct, missing = (
        duckdb.connect()
        .from_parquet(str(path))
        .aggregate(f"count(*), count(DISTINCT {key}), count(*) FILTER (WHERE {key} IS NULL)")
        .fetchall()[0]
    )
    if missing or distinct != rows:
        raise RuntimeError(f"{path.name}: {missing} rows lack {key} and {rows - missing - distinct} repeat one")
    metadata = pq.ParquetFile(path).metadata
    largest = max((metadata.row_group(i).total_byte_size for i in range(metadata.num_row_groups)), default=0)
    if largest > MAX_ROW_GROUP_BYTES:
        raise RuntimeError(f"{path.name}: a {largest / 2**20:.1f} MiB row group exceeds the admission bound")


class DocumentsFamily(RollupPipeline):
    """The ETL's documents, each with what Regulations.gov last said of it (``pipelines.docket_reconcile``)."""

    name: ClassVar[str] = "documents"
    output: ClassVar[str] = "documents.parquet"

    def generation_schemas(self):
        return {"documents": described_schema(policy("documents").subject_schema)}

    def build(self, output_dir: Path) -> Path:
        from spicy_regs.pipelines.docket_reconcile import (
            OUTCOMES,
            fetch_outcomes,
            read_reconcile_outcomes_sha256,
        )
        from spicy_regs.schemas.regulations import DOCUMENT_FAMILY_COLUMNS
        from spicy_regs.subject_catalog import retired_receipt_datasets

        if "documents" not in retired_receipt_datasets():
            raise ValueError(
                "DocumentsFamily: documents still publishes receipts; mark its policy "
                "receipts_retired before it publishes without them"
            )
        selected = SelectedPriors(output_dir / ".selected")
        selection = selected.select("documents")
        if selection is None:
            raise RuntimeError(f"{self.output}: no selected native generation to publish")
        if len(selection.subjects) != 1:
            raise ValueError("Documents family requires one subject member")
        subject = selection.subjects[0]
        fetched = fetch_outcomes(output_dir / OUTCOMES)
        if fetched is None:
            if all(col in pq.read_schema(subject).names for col in DOCUMENT_FAMILY_COLUMNS):
                raise Unchanged("no docket_reconcile_outcomes.parquet to apply; the selected documents generation stands")
        elif read_reconcile_outcomes_sha256(subject) == fetched[1]:
            raise Unchanged("documents already carries the current docket_reconcile_outcomes")

        path = output_dir / self.output
        shutil.copyfile(subject, path)
        self._prefetched_outcomes = fetched
        try:
            self.annotate(path)
        finally:
            self._prefetched_outcomes = None
        if not pq.read_schema(path).remove_metadata().equals(policy("documents").subject_schema):
            raise RuntimeError(f"{self.output}: annotated schema differs from the documents policy schema")
        validate_base_table_output(path, key=RECORD_TYPES["documents"].dedup_key)
        return path

    def annotate(self, path: Path) -> tuple[dict, ...]:
        from spicy_regs.pipelines.docket_reconcile import annotate_documents

        return annotate_documents(path, prefetched=getattr(self, "_prefetched_outcomes", None))


documents_app = make_rollup_app(DocumentsFamily)
