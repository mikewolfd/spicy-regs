"""Rollup pipeline: fr_docket_links.parquet (Federal Register ↔ docket links)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup as RollupPipeline
from spicy_regs.transforms import build_fr_docket_links


class FrDocketLinksRollup(RollupPipeline):
    """FR publications linked to each docket — replaces the docket page's LIKE scan.

    ``federal_register.parquet`` is produced by a separate ingestion path; this
    rollup reads it from R2 as a base input.
    """

    name: ClassVar[str] = "fr-docket-links"
    inputs: ClassVar[tuple[str, ...]] = ("federal_register.parquet",)
    output: ClassVar[str] = "fr_docket_links.parquet"

    def build(self, output_dir: Path) -> Path:
        return self.build_receipts(output_dir, build_fr_docket_links)


app = make_rollup_app(FrDocketLinksRollup)

if __name__ == "__main__":
    app()
