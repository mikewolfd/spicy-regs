"""Rollup pipeline: fec_candidate_history.parquet (the FEC's bulk candidate master, every cycle).

Decision 53's shape, applied to the candidate master. An ingesting rollup: ``inputs`` is empty, and each run
reads every cycle's candidate master whole inside ``build_fec_candidate_history``, retaining each response as
source evidence.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_fec_candidate_history import OUTPUT, build_fec_candidate_history


class FecCandidateHistoryRollup(RollupPipeline):
    """FEC candidates per cycle, 1980 through the current cycle, from the bulk candidate master."""

    name: ClassVar[str] = "fec-candidate-history"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = OUTPUT

    def build(self, output_dir: Path) -> Path:
        return build_fec_candidate_history(output_dir, evidence=self.source_evidence)


app = make_rollup_app(FecCandidateHistoryRollup)

if __name__ == "__main__":
    app()
