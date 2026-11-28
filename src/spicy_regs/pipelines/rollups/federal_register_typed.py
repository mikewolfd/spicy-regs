"""Rollup pipeline: federal-register-typed (typed Federal Register Layer 1 family).

Streams immutable 31-field API JSON pages from R2 ``source-evidence/blobs/sha256/<hex>``
selected by the pinned ``federal_register.parquet`` generation — zero external
``federalregister.gov`` requests and no per-row ETL receipts. Incremental nightly runs
fetch only changed/new blobs and anti-join merge all four tables in DuckDB by
``(document_number, publication_date)``, asserting exact 1-to-1 key parity with
``federal_register.parquet``.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.env_values import flag_env
from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.federal_register_layer1 import (
    FAMILY,
    OUTPUTS,
    merge_federal_register_typed,
)


class FederalRegisterTypedRollup(RollupPipeline):
    """Typed Federal Register Layer 1 document, agency, CFR, and docket-candidate tables."""

    name: ClassVar[str] = FAMILY
    inputs: ClassVar[tuple[str, ...]] = ("federal_register.parquet",)
    outputs: ClassVar[tuple[str, ...]] = OUTPUTS

    def build(self, output_dir: Path) -> tuple[Path, Path, Path, Path]:
        return merge_federal_register_typed(
            output_dir,
            force_rebuild=flag_env("FR_TYPED_FORCE_REBUILD"),
        )


app = make_rollup_app(FederalRegisterTypedRollup)

if __name__ == "__main__":
    app()
