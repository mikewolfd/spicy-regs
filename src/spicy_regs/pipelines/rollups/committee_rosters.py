"""Rollup pipeline: committees and committee_assignments.

Two outputs from one pass: the assignment rows are keyed on the committee
``systemCode`` the list route enumerates, and both chamber files are read
whole in the same run that reads the route.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.subject_receipts import SubjectReceiptRollup as RollupPipeline
from spicy_regs.transforms.build_committee_rosters import build_committee_rosters


class CommitteeRostersRollup(RollupPipeline):
    """Committees from Congress.gov (api.data.gov key) and today's seats from the two chamber roster files."""

    name: ClassVar[str] = "committee-rosters"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = ("committees.parquet", "committee_assignments.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return self.build_receipts(output_dir, build_committee_rosters)


app = make_rollup_app(CommitteeRostersRollup)

if __name__ == "__main__":
    app()
