"""Rollup pipeline: oira_reviews.parquet (reginfo.gov EO 12866 reviews ingest).

Like ``unified_agenda``, this rollup ingests an external source rather than
reading base tables from R2, so ``inputs`` is empty. ``build_oira_reviews`` does
a full refresh from reginfo.gov's bulk XML exports via ``spicy_docs.sources.oira``,
retaining each exact XML file once in the shared raw archive and stamping
``source_sha256`` on every row (ss-vse3; no per-row ETL receipts).
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_oira import build_oira_reviews


class OiraReviewsRollup(RollupPipeline):
    """OIRA EO 12866 reviews ingested from reginfo.gov (no API key)."""

    name: ClassVar[str] = "oira-reviews"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "oira_reviews.parquet"

    def build(self, output_dir: Path) -> Path:
        return build_oira_reviews(output_dir, evidence=self.source_evidence)


app = make_rollup_app(OiraReviewsRollup)

if __name__ == "__main__":
    app()
