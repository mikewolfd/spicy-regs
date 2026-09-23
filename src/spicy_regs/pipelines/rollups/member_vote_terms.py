"""Rollup pipeline: member_vote_terms.parquet (the term each member vote counts toward)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import build_member_vote_terms
from spicy_regs.transforms.build_member_vote_terms import INPUTS, OUTPUT


class MemberVoteTermsRollup(RollupPipeline):
    """Delivery decision 2's half-open term rule over the published votes, members and terms."""

    name: ClassVar[str] = "member-vote-terms"
    inputs: ClassVar[tuple[str, ...]] = INPUTS
    output: ClassVar[str] = OUTPUT

    def build(self, output_dir: Path) -> Path:
        return build_member_vote_terms(output_dir)


app = make_rollup_app(MemberVoteTermsRollup)

if __name__ == "__main__":
    app()
