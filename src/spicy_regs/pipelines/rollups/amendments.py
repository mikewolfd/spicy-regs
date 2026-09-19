"""Rollup pipeline: amendments.parquet (Congress.gov amendment list route)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_amendments import build_amendments


class AmendmentsRollup(RollupPipeline):
    """Congressional amendments ingested from the Congress.gov v3 API (api.data.gov key)."""

    name: ClassVar[str] = "amendments"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "amendments.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_amendments(output_dir)


app = make_rollup_app(AmendmentsRollup)

if __name__ == "__main__":
    app()
