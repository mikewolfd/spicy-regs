"""Court family admission uses the shared generation receipt hook."""
from pathlib import Path
from typing import Sequence

from spicy_regs.court_receipts import generation_options
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.base import RollupPipeline


class CourtReceiptRollup(RollupPipeline):
    """Fail closed until the common runner can admit a subject/receipt pair."""

    def generation_kwargs(self, out_paths: Sequence[Path]) -> dict:
        return generation_options(out_paths)

    def generation_schemas(self) -> dict:
        return {name: described_schema(schema) for name, schema in SUBJECT_SCHEMAS.items()}

    def _run_tables(self, output_dir: Path) -> None:
        if not hasattr(RollupPipeline, 'generation_kwargs') or not hasattr(RollupPipeline, 'generation_schemas'):
            raise RuntimeError('Court receipts require the shared rollup receipt admission hook; '
                               'use build_court_generation for local qualification')
        super()._run_tables(output_dir)
