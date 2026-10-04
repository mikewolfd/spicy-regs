"""Rollup pipeline: law metadata, law text and relationships to the U.S. Code.

The outputs share one pass: the enumeration on the Congress.gov law route is
what addresses both the PLAW USLM file (the citation) and the two OLRC views
of what each law did to the Code, and the act keys are only in hand while
the list is. Its cron fires before the bill family's, which fills
``congress_bills.statutes_at_large_cite`` from ``laws`` at its merge.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.legislative_rollups import LegislativeReceiptRollup, family_policies
from spicy_regs.transforms.build_laws import build_laws, olrc_access_refused


class LawsRollup(LegislativeReceiptRollup):
    """Enacted laws with their Statutes at Large citation, and the OLRC classification tables (api.data.gov key)."""

    name: ClassVar[str] = "laws"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    receipt_policies = family_policies('laws', 'law_code_sections', 'table3_records', 'law_sections')
    outputs: ClassVar[tuple[str, ...]] = ("laws.parquet", "law_code_sections.parquet", "table3_records.parquet", "law_sections.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return self.build_receipts(output_dir, build_laws, evidence=self.source_evidence)


    def run(self) -> None:
        """Publish the family, then fail the run if OLRC refused this run's Table III or classification read.

        The routes are keyless, so a 401/403 is the publisher blocking the read; the build journals it and publishes
        the rest (every row of the blocked table stands), and the failure here keeps the block visible on the run
        itself rather than only in the next night's ``check_source_refusals``.
        """
        super().run()
        blocked = olrc_access_refused(self.source_evidence) if self.source_evidence is not None else []
        if blocked:
            raise RuntimeError(
                f"OLRC answered 401/403 for {' and '.join(blocked)}: the laws family was published without that read; "
                "see the journaled table3-bulk-refused or classification-refused event"
            )


app = make_rollup_app(LawsRollup)

if __name__ == "__main__":
    app()
