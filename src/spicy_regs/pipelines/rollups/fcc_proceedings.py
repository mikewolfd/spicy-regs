"""Rollup pipeline: fcc_proceedings.parquet (FCC ECFS REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — ``build_fcc_proceedings``
walks every ECFS proceeding each run. The base class still handles the
shrink-guarded R2 upload of the single output.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_fcc_proceedings


class FccProceedingsRollup(RollupPipeline):
    """FCC proceedings (docket equivalents) ingested from ECFS (api.data.gov key)."""

    name: ClassVar[str] = "fcc-proceedings"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "fcc_proceedings.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_fcc_proceedings(output_dir)


app = make_rollup_app(FccProceedingsRollup)

if __name__ == "__main__":
    app()
