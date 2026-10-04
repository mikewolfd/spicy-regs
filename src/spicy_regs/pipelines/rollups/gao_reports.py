"""Rollup pipeline: gao_reports.parquet (GAO reports RSS ingest, plus GovInfo's and GAO's own listings on request).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty — the fetch + append-only
merge with the prior published table happens inside ``build_gao_reports``. The
base class still handles the shrink-guarded R2 upload of the single output.
``GAO_GOVINFO_HISTORY=true`` adds the one walk of GovInfo's closed GAOREPORTS
listing to a run; ``GAO_GOVINFO_MODS=true`` reads the next batch of those rows'
MODS. ``GAO_LISTING_RUN=<directory>`` adds a finished SpicyDocs walk of GAO's Month
in Review and Annual Index, made outside the rollup and read from disk; its legal
decisions go to ``gao_decisions.parquet``, which runs without a walk carry forward.
``GAO_DECISION_PAGES=<directory>`` adds, to such a run, a local capture of those
decisions' pages, read by reference: each page's caption completes a cut number list
and states the decided day (``spicy_regs.sources.gao_decision_pages``).
"""

import os
from pathlib import Path
from typing import ClassVar

from spicy_regs.env_values import flag_env
from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.government import GovernmentReceiptRollup
from spicy_regs.transforms import build_gao_reports


class GaoReportsRollup(GovernmentReceiptRollup):
    """GAO oversight reports ingested from the gao.gov reports RSS feed."""

    name: ClassVar[str] = "gao-reports"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = ("gao_reports.parquet", "gao_decisions.parquet")
    #: GAO's legal decisions from its own listing join the family as their own table (owner, 2026-09-28).
    added_tables: ClassVar[tuple[str, ...]] = ("gao_decisions.parquet",)

    def build(self, output_dir: Path) -> tuple[Path, Path]:
        return build_gao_reports(
            output_dir,
            evidence=self.source_evidence, receipt_generation_id=self.receipt_generation_id,
            govinfo_history=flag_env("GAO_GOVINFO_HISTORY"),
            govinfo_mods=flag_env("GAO_GOVINFO_MODS"),
            listing_run=Path(listing) if (listing := os.environ.get("GAO_LISTING_RUN", "").strip()) else None,
            decision_pages=Path(pages) if (pages := os.environ.get("GAO_DECISION_PAGES", "").strip()) else None,
        )


app = make_rollup_app(GaoReportsRollup)

if __name__ == "__main__":
    app()
