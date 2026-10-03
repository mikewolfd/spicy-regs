"""Rollup pipeline: court_docket_groups.parquet, which published court_dockets rows are records of one case.

Run on demand from a machine holding a CourtListener bulk docket edition (``--native-file``; the 2026-06-30 edition
is 8.2 GB, kept privately and never published), so it has no schedule or workflow. ``court_dockets.parquet`` is
primed from its managed family and recorded as this generation's parent, at the bytes the build read; the edition is
named on every row. Publishing goes through the standard generation writer with ``--no-skip-upload``.
"""

import re
from pathlib import Path
from typing import ClassVar

from cyclopts import App
from dotenv import load_dotenv

from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.transforms.build_court_docket_groups import build_court_docket_groups


class CourtDocketGroupsRollup(RollupPipeline):
    """Same-case groups over the published court_dockets, from a local native bulk edition."""

    name: ClassVar[str] = "court-docket-groups"
    inputs: ClassVar[tuple[str, ...]] = ("court_dockets.parquet",)
    output: ClassVar[str] = "court_docket_groups.parquet"

    def __init__(self, *, native_file: Path, edition: str, output_dir: Path | None = None, skip_upload: bool = True):
        super().__init__(output_dir=output_dir, skip_upload=skip_upload)
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", edition):
            raise ValueError(f"edition must be the bulk edition's date, YYYY-MM-DD: {edition!r}")
        if not native_file.is_file():
            raise FileNotFoundError(f"native docket edition not found: {native_file}")
        self.native_file, self.edition = native_file, edition

    def build(self, output_dir: Path) -> Path:
        return build_court_docket_groups(output_dir, dockets_file=output_dir / "court_dockets.parquet",
                                         native_file=self.native_file, edition=self.edition)


app = App(name="run-rollup-court-docket-groups", help=__doc__)


@app.default
def main(*, native_file: Path, edition: str, output_dir: Path | None = None, skip_upload: bool = True) -> None:
    load_dotenv()
    CourtDocketGroupsRollup(native_file=native_file, edition=edition, output_dir=output_dir,
                            skip_upload=skip_upload).run()


if __name__ == "__main__":
    app()
