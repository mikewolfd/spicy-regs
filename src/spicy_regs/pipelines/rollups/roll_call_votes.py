"""Rollup pipeline: roll_call_votes.parquet and member_votes.parquet.

Two outputs from one pass: the roll calls and the per-member positions come
out of the same House Clerk walk, so splitting them into two rollups would
fetch every roll call twice.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_roll_call_votes import build_roll_call_votes


class RollCallVotesRollup(RollupPipeline):
    """House roll-call votes and member positions (Congress.gov linkage, Clerk EVS counts)."""

    name: ClassVar[str] = "roll-call-votes"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = ("roll_call_votes.parquet", "member_votes.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_roll_call_votes(output_dir)


app = make_rollup_app(RollCallVotesRollup)

if __name__ == "__main__":
    app()
