"""Build a native legal-reference generation from explicitly pinned retained XML."""

from pathlib import Path
from typing import ClassVar

from cyclopts import App

from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.sources import r2
from spicy_regs.transforms.native_legal_references import OUTPUTS, build_native_legal_references


class NativeLegalReferencesRollup(RollupPipeline):
    name: ClassVar[str] = "native-legal-references"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = OUTPUTS
    retain_source_evidence: ClassVar[bool] = True

    def __init__(self, *, manifest: Path, output_dir: Path | None = None, skip_upload: bool = True):
        super().__init__(output_dir=output_dir, skip_upload=skip_upload)
        self.manifest = manifest

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        if self.source_evidence is None:
            raise ValueError("native reference build requires the rollup source evidence lifecycle")
        return build_native_legal_references(
            self.manifest, output_dir, evidence=self.source_evidence, download_prior=r2.download
        )


app = App(help=__doc__)


@app.default
def main(*, manifest: Path, output_dir: Path | None = None, skip_upload: bool = True) -> None:
    NativeLegalReferencesRollup(manifest=manifest, output_dir=output_dir, skip_upload=skip_upload).run()


if __name__ == "__main__":
    app()
