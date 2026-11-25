"""Rollup pipeline: oira_meetings.parquet (reginfo.gov EO 12866 meetings ingest).

Ingests OIRA's EO 12866 meeting log bulk XML exports via ``spicy_docs.sources.oira``,
retaining each exact XML file once in the shared raw archive and stamping
``source_sha256`` on every row (ss-vse3; no per-row ETL receipts).
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_oira import build_oira_meetings


class OiraMeetingsRollup(RollupPipeline):
    """OIRA EO 12866 meetings ingested from reginfo.gov (no API key)."""

    name: ClassVar[str] = "oira-meetings"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "oira_meetings.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_oira_meetings(output_dir, evidence=self.source_evidence)


app = make_rollup_app(OiraMeetingsRollup)

if __name__ == "__main__":
    app()
