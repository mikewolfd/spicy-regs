"""Rollup pipeline: laws, law_code_sections and table3_records.

Three outputs from one pass: the enumeration on the Congress.gov law route is
what addresses both the PLAW USLM file (the citation) and the two OLRC views
of what each law did to the Code, and the act keys are only in hand while
the list is. Its cron fires before the bill family's, which fills
``congress_bills.statutes_at_large_cite`` from ``laws`` at its merge.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_laws import build_laws


class LawsRollup(RollupPipeline):
    """Enacted laws with their Statutes at Large citation, and the OLRC classification tables (api.data.gov key)."""

    name: ClassVar[str] = "laws"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = ("laws.parquet", "law_code_sections.parquet", "table3_records.parquet")

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_laws(output_dir, evidence=self.source_evidence)


app = make_rollup_app(LawsRollup)

if __name__ == "__main__":
    app()
