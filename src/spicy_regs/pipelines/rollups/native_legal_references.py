"""Build a native legal-reference generation from explicitly pinned retained XML."""

from pathlib import Path
from typing import ClassVar

from cyclopts import App

from spicy_regs.legislative_rollups import LegislativeReceiptRollup, family_policies
from spicy_regs.transforms.native_legal_references import build_native_legal_references


class NativeLegalReferencesRollup(LegislativeReceiptRollup):
    name: ClassVar[str] = "native-legal-references"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = ("native_legal_references.parquet",)
    receipt_only_tables = ("native_legal_reference_reads.parquet",)
    receipt_policies = family_policies("native_legal_references", "native_legal_reference_reads")
    retain_source_evidence: ClassVar[bool] = True

    def __init__(self, *, manifest: Path, output_dir: Path | None = None, skip_upload: bool = True):
        super().__init__(output_dir=output_dir, skip_upload=skip_upload)
        self.manifest = manifest

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        evidence = self.source_evidence
        if evidence is None:
            raise ValueError("native reference build requires the rollup source evidence lifecycle")
        def builder(directory, *, download_prior):
            return build_native_legal_references(
                self.manifest, directory, evidence=evidence, download_prior=download_prior
            )

        return self.build_receipts(output_dir, builder)


app = App(help=__doc__)


@app.default
def main(*, manifest: Path, output_dir: Path | None = None, skip_upload: bool = True) -> None:
    NativeLegalReferencesRollup(manifest=manifest, output_dir=output_dir, skip_upload=skip_upload).run()


if __name__ == "__main__":
    app()
