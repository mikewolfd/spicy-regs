"""Rollup pipeline: roll_call_votes.parquet and member_votes.parquet.

Two outputs from one pass: the roll calls and the per-member positions come
out of the same House Clerk and Senate LIS walks, so separate rollups would
fetch every roll call twice.

Reads the bill family's published ``bill_vote_references`` best-effort at
merge time, its only linkage source — a ``soft_input``, so a run with no family
output still acquires the native vote identities. A bill link remains NULL when
no recorded reference establishes it. Both publishers are keyless, so the run
needs no credential. The cron runs an hour after the family's to reuse any
available links.

A dispatch scopes the Congresses (``BILL_FAMILY_CONGRESSES``), the chambers
(``ROLL_CALL_CHAMBERS``) and the per-run cap (``ROLL_CALL_MAX_VOTES``), which is
how the 101st-107th House backfill runs: House only, newest first under the
cap, each run resuming on what the last published.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_roll_call_votes import build_roll_call_votes, max_votes_from_env


class RollCallVotesRollup(RollupPipeline):
    """House and Senate roll calls and member positions with optional bill links."""

    name: ClassVar[str] = "roll-call-votes"
    inputs: ClassVar[tuple[str, ...]] = ()
    soft_inputs: ClassVar[tuple[str, ...]] = ("bill_vote_references.parquet",)
    outputs: ClassVar[tuple[str, ...]] = ("roll_call_votes.parquet", "member_votes.parquet")
    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_roll_call_votes(output_dir, max_votes=max_votes_from_env(), evidence=self.source_evidence)


app = make_rollup_app(RollCallVotesRollup)

if __name__ == "__main__":
    app()
