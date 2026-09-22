"""Rollup pipeline: roll_call_votes.parquet and member_votes.parquet.

Two outputs from one pass: the roll calls and the per-member positions come
out of the same House Clerk and Senate LIS walks, so separate rollups would
fetch every roll call twice.

Reads the bill family's published ``bill_vote_references`` best-effort at
merge time for the second of its two linkage sources — a ``soft_input``, so a
run with no family output still acquires the native vote identities. A bill
link remains NULL when neither reference source establishes it. The cron runs
an hour after the family's to reuse any available links.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_roll_call_votes import build_roll_call_votes


class RollCallVotesRollup(RollupPipeline):
    """House and Senate roll calls and member positions with optional bill links."""

    name: ClassVar[str] = "roll-call-votes"
    inputs: ClassVar[tuple[str, ...]] = ()
    soft_inputs: ClassVar[tuple[str, ...]] = ("bill_vote_references.parquet",)
    outputs: ClassVar[tuple[str, ...]] = ("roll_call_votes.parquet", "member_votes.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_roll_call_votes(output_dir)


app = make_rollup_app(RollCallVotesRollup)

if __name__ == "__main__":
    app()
