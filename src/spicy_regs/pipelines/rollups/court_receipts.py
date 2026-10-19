"""Court family admission uses the shared generation receipt hook."""
from pathlib import Path
from typing import Sequence

from spicy_regs.court_receipts import generation_options
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.base import RollupPipeline


class CourtReceiptRollup(RollupPipeline):
    """Admit court subjects and receipts through the shared generation runner."""

    def generation_kwargs(self, out_paths: Sequence[Path]) -> dict:
        return generation_options(out_paths)

    def generation_schemas(self) -> dict:
        return {name: described_schema(schema) for name, schema in SUBJECT_SCHEMAS.items()}
