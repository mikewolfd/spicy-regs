"""Rollup pipeline: fec_committee_history.parquet (the FEC's bulk committee master, every cycle).

Owner decision 53. An ingesting rollup: ``inputs`` is empty, and each run reads every cycle's committee master
whole inside ``build_fec_committee_history``, retaining each response as source evidence.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.fec_receipts import FecReceiptRollup as RollupPipeline
from spicy_regs.transforms.build_fec_committee_history import OUTPUT


class FecCommitteeHistoryRollup(RollupPipeline):
    """FEC committees per cycle, 1980 through the current cycle, from the bulk committee master."""

    name: ClassVar[str] = "fec-committee-history"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = OUTPUT

    def build(self, output_dir: Path) -> Path:
        return self.build_receipts(output_dir, evidence=self.source_evidence)


app = make_rollup_app(FecCommitteeHistoryRollup)

if __name__ == "__main__":
    app()
