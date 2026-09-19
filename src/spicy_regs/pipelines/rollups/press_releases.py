"""Rollup pipeline: press_releases.parquet (House and Senate appropriations feeds)."""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_press_releases import build_press_releases


class PressReleasesRollup(RollupPipeline):
    """Appropriations committee press releases, captured from both chambers' RSS feeds."""

    name: ClassVar[str] = "press-releases"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "press_releases.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_press_releases(output_dir)


app = make_rollup_app(PressReleasesRollup)

if __name__ == "__main__":
    app()
