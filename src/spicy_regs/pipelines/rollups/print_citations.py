"""Rollup pipeline: house_activity_reports, budget_volumes, bill_committee_actions and document_citations.

Four outputs from one pass: the two GovInfo collections share an acquirer and
its pacing budget, and the citations and bill actions are read out of a
document body that is only in hand during the pass that fetched it.
``transforms/build_print_citations.py`` states why these four and not some
other grouping, and why ``senate_expenditures`` is a rollup of its own.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.legislative_rollups import LegislativeReceiptRollup, family_policies
from spicy_regs.transforms.build_print_citations import build_print_citations


class PrintCitationsRollup(LegislativeReceiptRollup):
    """GovInfo activity reports and budget volumes, what their prints cite, and the actions they state (api.data.gov key)."""

    name: ClassVar[str] = "print-citations"
    inputs: ClassVar[tuple[str, ...]] = ()
    receipt_policies = family_policies('house_activity_reports', 'budget_volumes', 'bill_committee_actions', 'document_citations')
    outputs: ClassVar[tuple[str, ...]] = (
        "house_activity_reports.parquet",
        "budget_volumes.parquet",
        "bill_committee_actions.parquet",
        "document_citations.parquet",
    )

    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return self.build_receipts(output_dir, build_print_citations, evidence=self.source_evidence)


app = make_rollup_app(PrintCitationsRollup)

if __name__ == "__main__":
    app()
