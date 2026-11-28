"""Build a native legal-reference generation from explicitly pinned retained XML."""

from pathlib import Path
from typing import ClassVar

from cyclopts import App

from spicy_regs.legislative_rollups import LegislativeTableRollup
from spicy_regs.pipelines.rollups.base import file_scope_digest
from spicy_regs.transforms.native_legal_references import build_native_legal_references


class NativeLegalReferencesRollup(LegislativeTableRollup):
    name: ClassVar[str] = "native-legal-references"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = (
        "native_legal_references.parquet",
        "native_legal_reference_reads.parquet",
    )
    retain_source_evidence: ClassVar[bool] = True

    def __init__(
        self,
        *,
        manifest: Path,
        output_dir: Path | None = None,
        skip_upload: bool = True,
        resume: bool = False,
        require_resume_candidate: bool = False,
    ):
        super().__init__(
            output_dir=output_dir,
            skip_upload=skip_upload,
            resume=resume,
            require_resume_candidate=require_resume_candidate,
        )
        self.manifest = manifest

    def invocation_scope(self) -> dict:
        scope = super().invocation_scope()
        manifest = getattr(self, "manifest", None)
        scope["native_legal_references"] = {
            "manifest_sha256": file_scope_digest(manifest) if manifest is not None else None,
        }
        return scope

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        evidence = self.source_evidence
        if evidence is None:
            raise ValueError("native reference build requires the rollup source evidence lifecycle")
        def builder(directory, *, download_prior):
            return build_native_legal_references(
                self.manifest, directory, evidence=evidence, download_prior=download_prior
            )

        return self.build_tables(output_dir, builder)


app = App(help=__doc__)


@app.default
def main(
    *,
    manifest: Path,
    output_dir: Path | None = None,
    skip_upload: bool = True,
    resume: bool = False,
) -> None:
    NativeLegalReferencesRollup(
        manifest=manifest, output_dir=output_dir, skip_upload=skip_upload, resume=resume
    ).run()


if __name__ == "__main__":
    app()
