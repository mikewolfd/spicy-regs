"""Rollup pipeline: house_activity_reports, budget_volumes, bill_committee_actions and document_citations.

Four outputs from one pass: the two GovInfo collections share an acquirer and
its pacing budget, and the citations and bill actions are read out of a
document body that is only in hand during the pass that fetched it.
``transforms/build_print_citations.py`` states why these four and not some
other grouping, and why ``senate_expenditures`` is a rollup of its own.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_print_citations import build_print_citations
from spicy_regs.transforms.held_citations import write_citation_reads


class PrintCitationsRollup(RollupPipeline):
    """GovInfo activity reports and budget volumes, what their prints cite, and the actions they state (api.data.gov key)."""

    name: ClassVar[str] = "print-citations"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = (
        "house_activity_reports.parquet",
        "budget_volumes.parquet",
        "bill_committee_actions.parquet",
        "document_citations.parquet",
        # The held-field reads the citation table's checkpoints record, rebuilt from the merged table's metadata
        # so a run of this family never drops what the held-citations rollup read.
        "document_citation_reads.parquet",
    )
    #: The family grows by the reads table on its first run from this code; inert once it is published.
    added_tables: ClassVar[tuple[str, ...]] = ("document_citation_reads.parquet",)

    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        paths = build_print_citations(output_dir, evidence=self.source_evidence)
        return (*paths, write_citation_reads(output_dir, paths[3]))


app = make_rollup_app(PrintCitationsRollup)

if __name__ == "__main__":
    app()
