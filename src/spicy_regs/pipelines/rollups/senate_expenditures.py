"""Rollup pipeline: senate_expenditures.

One output, and its own rollup rather than a fifth output of ``print-citations``:
its pass is PyMuPDF table detection over multi-thousand-page granule PDFs,
which is a different acquisition and a different cost class from the one keyed
package body the citation families read.
``transforms/build_senate_expenditures.py`` states the whole argument.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_senate_expenditures import build_senate_expenditures


class SenateExpendituresRollup(RollupPipeline):
    """The Secretary of the Senate's ruled expenditure tables, from GovInfo granule PDFs (api.data.gov key)."""

    name: ClassVar[str] = "senate-expenditures"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "senate_expenditures.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_senate_expenditures(output_dir)


app = make_rollup_app(SenateExpendituresRollup)

if __name__ == "__main__":
    app()
