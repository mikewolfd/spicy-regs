"""Rollup pipeline: federal_register.parquet (federalregister.gov REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_federal_register``.
The base class still handles the shrink-guarded R2 upload of the single output.

``FR_SINCE`` (YYYY-MM-DD) replaces the resume point for a re-read: at the
1994 epoch it refetches every document, so a column added to the projection
(``topics_json``, 2026-09-26) reaches every row.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.env_values import date_env
from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_federal_register


class FederalRegisterRollup(RollupPipeline):
    """Federal Register documents ingested from federalregister.gov (no API key)."""

    name: ClassVar[str] = "federal-register"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "federal_register.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_federal_register(output_dir, since=date_env("FR_SINCE"))


app = make_rollup_app(FederalRegisterRollup)

if __name__ == "__main__":
    app()
