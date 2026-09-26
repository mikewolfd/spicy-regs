"""Rollup pipeline: cfr_sections.parquet (GovInfo CFR section-metadata ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + incremental
merge with the prior published table happens inside ``build_cfr_sections``.
The base class still handles the shrink-guarded R2 upload of the single output.

Section metadata + citations only (not full section text), with each
package's section granules placed from one download of its annual volume XML.
Requires an api.data.gov key (``DATA_GOV_API_KEY``); a keyless run refuses.
``CFR_REPLACE_ALL=true`` (the workflow's ``replace_all`` dispatch input)
re-places every listed package instead of only the changed ones.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.env_values import flag_env
from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_cfr_sections


class CfrSectionsRollup(RollupPipeline):
    """GovInfo CFR section metadata ingested from api.govinfo.gov (needs a key)."""

    name: ClassVar[str] = "cfr-sections"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "cfr_sections.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_cfr_sections(output_dir, replace_all=flag_env("CFR_REPLACE_ALL"))


app = make_rollup_app(CfrSectionsRollup)

if __name__ == "__main__":
    app()
