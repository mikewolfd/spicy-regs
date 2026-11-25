"""Rollup pipeline: senate_expenditures.

One output, and its own rollup rather than a fifth output of ``print-citations``:
its pass is PyMuPDF table detection over multi-thousand-page granule PDFs,
which is a different acquisition and a different cost class from the one keyed
package body the citation families read.
``transforms/build_senate_expenditures.py`` states the whole argument.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.legislative_rollups import LegislativeReceiptRollup, family_policies
from spicy_regs.transforms.build_senate_expenditures import build_senate_expenditures


class SenateExpendituresRollup(LegislativeReceiptRollup):
    """The Secretary of the Senate's ruled expenditure tables, from GovInfo granule PDFs (api.data.gov key)."""

    name: ClassVar[str] = "senate-expenditures"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    receipt_policies = family_policies('senate_expenditures')
    output: ClassVar[str] = "senate_expenditures.parquet"

    def build(self, output_dir: Path) -> Path:
        self.acquisition_status: dict = {}
        result = self.build_receipts(
            output_dir,
            build_senate_expenditures,
            evidence=self.source_evidence,
            coverage=self.acquisition_status,
        )
        if self.acquisition_status.get("refused"):
            self.deferred_failure = RuntimeError(
                f"Senate expenditures published with {self.acquisition_status['refused']} refused item(s); "
                "see source evidence refusals"
            )
        return result


app = make_rollup_app(SenateExpendituresRollup)

if __name__ == "__main__":
    app()
