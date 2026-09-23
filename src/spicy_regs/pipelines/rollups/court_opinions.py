"""Rollup pipeline: court_opinions.parquet, the text-free CourtListener opinion index.

Ingests an external source, so ``inputs`` is empty. It maps every opinion to its cluster, which is
what joins ``court_citation_map`` and ``court_parentheticals`` to decisions. Its only source is the
54.6 GB ``opinions`` export, about 8.6 hours to download at the bucket's rate, so it has no
scheduled workflow: run it where a verified copy is retained, with ``COURTLISTENER_BULK_DIR``
pointing at it.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_court_bulk_tables import OPINIONS, build_court_bulk_tables


class CourtOpinionsRollup(RollupPipeline):
    """Every CourtListener opinion's cluster, type and author, without its text."""

    name: ClassVar[str] = "court-opinions"
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = OPINIONS.output

    def build(self, output_dir: Path) -> Path:
        (built,) = build_court_bulk_tables((OPINIONS,), output_dir)
        return built


app = make_rollup_app(CourtOpinionsRollup)

if __name__ == "__main__":
    app()
