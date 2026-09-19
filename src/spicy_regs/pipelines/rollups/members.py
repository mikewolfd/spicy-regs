"""Rollup pipeline: members.parquet and member_terms.parquet.

Two outputs from one pass: both tables are read out of the same two roster
captures, and a legislator's terms are only in hand while the roster is.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_members import build_members


class MembersRollup(RollupPipeline):
    """Legislators and their terms, from the @unitedstates community crosswalk."""

    name: ClassVar[str] = "members"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = ("members.parquet", "member_terms.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_members(output_dir)


app = make_rollup_app(MembersRollup)

if __name__ == "__main__":
    app()
