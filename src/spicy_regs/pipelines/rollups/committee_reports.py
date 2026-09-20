"""Rollup pipeline: committee_reports, report_sections and hearing_transcripts.

Three outputs from one pass: the two GovInfo collections share an acquirer and
its pacing budget, and the agency blocks are parsed from a report body that is
only in hand during the pass that fetched it.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_committee_reports import build_committee_reports


class CommitteeReportsRollup(RollupPipeline):
    """GovInfo committee reports, their agency blocks, and hearing transcripts (api.data.gov key)."""

    name: ClassVar[str] = "committee-reports"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = (
        "committee_reports.parquet",
        "report_sections.parquet",
        "hearing_transcripts.parquet",
        "hearing_bill_links.parquet",
        "committee_report_reads.parquet",
    )

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_committee_reports(output_dir)


app = make_rollup_app(CommitteeReportsRollup)

if __name__ == "__main__":
    app()
