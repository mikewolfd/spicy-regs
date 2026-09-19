"""Rollup pipeline: press_releases.parquet (House and Senate appropriations feeds).

Reads the published ``congress_bills`` table best-effort at merge time to fill
the bill linkage the feeds themselves do not carry — a ``soft_input``, so a
run with no bills table still publishes every release with NULL match columns
rather than failing or asserting a false ``unmatched``. Its cron runs twenty
minutes after the bill family's for that reason.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_press_releases import build_press_releases


class PressReleasesRollup(RollupPipeline):
    """Appropriations committee press releases, captured from both chambers' RSS feeds."""

    name: ClassVar[str] = "press-releases"
    inputs: ClassVar[tuple[str, ...]] = ()
    soft_inputs: ClassVar[tuple[str, ...]] = ("congress_bills.parquet",)
    output: ClassVar[str] = "press_releases.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_press_releases(output_dir)


app = make_rollup_app(PressReleasesRollup)

if __name__ == "__main__":
    app()
