"""Rollup pipeline: gao_recommendations.parquet (GAO's open-recommendations export, accumulated daily).

An ingesting rollup: ``inputs`` is empty, and ``build_gao_recommendations`` reads the export through Zyte, folds it
into the prior published table and retains the export, its director phones emptied, as source evidence.
``GAO_ALLOW_MASS_CLOSE_REASON``, which only a dispatched run sets, lets the fold past its guard, stating why.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.env_values import text_env
from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.government import GovernmentReceiptRollup
from spicy_regs.transforms.build_gao_recommendations import OUTPUT, build_gao_recommendations


class GaoRecommendationsRollup(GovernmentReceiptRollup):
    """Every GAO recommendation the open-recommendations export has listed, as last listed."""

    name: ClassVar[str] = "gao-recommendations"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = OUTPUT

    def build(self, output_dir: Path) -> Path:
        return build_gao_recommendations(
            output_dir,
            evidence=self.source_evidence, receipt_generation_id=self.receipt_generation_id,
            allow_mass_close_reason=text_env("GAO_ALLOW_MASS_CLOSE_REASON"),
        )


app = make_rollup_app(GaoRecommendationsRollup)

if __name__ == "__main__":
    app()
