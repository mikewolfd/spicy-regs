"""Expose the pinned SpicyDocs official FEC inventory through the usual rollup."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.fec_receipts import FecReceiptRollup as RollupPipeline


class FecSourceCatalogRollup(RollupPipeline):
    """Build official source-family discovery metadata; upload remains opt-in."""

    name: ClassVar[str] = "fec-source-catalog"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "fec_source_catalog.parquet"

    def build(self, output_dir: Path) -> Path:
        return self.build_receipts(output_dir)


app = make_rollup_app(FecSourceCatalogRollup)

if __name__ == "__main__":
    app()
