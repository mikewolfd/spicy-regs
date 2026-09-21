"""Expose the pinned SpicyDocs official FEC inventory through the usual rollup."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_fec_source_catalog import build_fec_source_catalog


class FecSourceCatalogRollup(RollupPipeline):
    """Build official source-family discovery metadata; upload remains opt-in."""

    name: ClassVar[str] = "fec-source-catalog"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "fec_source_catalog.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_fec_source_catalog(output_dir)


app = make_rollup_app(FecSourceCatalogRollup)

if __name__ == "__main__":
    app()
