"""Build an explicit local FEC identity/context selection with ETL receipts."""

from pathlib import Path
from cyclopts import App
from spicy_regs.transforms.build_fec_identity_context import build_fec_identity_context
from spicy_regs.transforms.build_fec_identity_rollup import build_fec_identity_rollup

app = App(help=__doc__)


@app.default
def main(*, manifest: Path, output_dir: Path):
    build_fec_identity_context(manifest, output_dir)


@app.command
def rollup(
    table: str,
    *,
    output_dir: Path,
    generation_id: str,
    inputs: tuple[Path, ...] = (),
    prior_bundle: Path | None = None,
    prior_generation_id: str | None = None,
    full_walk: bool = False,
):
    """Run a named maintained producer into a local subject/receipt bundle."""
    options = {"full_walk": full_walk} if table == "fec_committees" else {}
    build_fec_identity_rollup(
        table,
        output_dir,
        generation_id=generation_id,
        inputs=inputs,
        prior_bundle=prior_bundle,
        prior_generation_id=prior_generation_id,
        **options,
    )


if __name__ == "__main__":
    app()
