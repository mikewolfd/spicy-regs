"""Build verified retained FEC tables in a new local generation directory."""

from pathlib import Path
from typing import ClassVar
from uuid import uuid4

from cyclopts import App

from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.transforms.build_fec_observations import OUTPUTS, build_fec_observations


class FecObservationsRollup(RollupPipeline):
    """Build explicitly selected retained FEC observations and metadata companions."""

    name: ClassVar[str] = "fec-observations"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = OUTPUTS

    def __init__(self, *, manifest: Path, output_dir: Path | None = None, skip_upload: bool = True):
        super().__init__(output_dir=output_dir, skip_upload=skip_upload)
        self.manifest = manifest

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_fec_observations(self.manifest, output_dir / f"fec-observations-{uuid4().hex}")


app = App(help=__doc__)


@app.default
def main(*, manifest: Path, output_dir: Path | None = None, skip_upload: bool = True) -> None:
    """Read explicit input selection; write companions, scopes and relationships."""
    FecObservationsRollup(manifest=manifest, output_dir=output_dir, skip_upload=skip_upload).run()


if __name__ == "__main__":
    app()
