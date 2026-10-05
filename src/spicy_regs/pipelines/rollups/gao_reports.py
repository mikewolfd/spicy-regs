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

The major-rule reports of 1996-2008 arrive the same way, from disk, in one run made
where the walks are held; the published table then carries them, and their letter
columns, to every later run. ``GAO_MAJOR_RULE_RUN=<directory>`` and
``GAO_MAJOR_RULE_OLD_INDEX_RUN=<directory>`` add, to a run with ``GAO_LISTING_RUN``,
finished SpicyDocs walks of "Reports on Major Rules" and of GAO's index of 2000-12-15
(``spicy_regs.sources.gao_listing``). ``GAO_MAJOR_RULE_LETTERS=<directories>`` names
captures of the reports' product pages and PDFs, separated as ``PATH`` is, read by
reference for each report's agency, RINs and Federal Register citations
(``spicy_regs.sources.gao_major_rule_letters``).

``GAO_PRODUCT_PAGES=true`` reads pending rows' product pages through Zyte
(``spicy_regs.sources.gao_product_pages``; needs ``ZYTE_TOKEN``). It is off: the
workflow passes ``false`` and gives this rollup no token. Set to ``true`` it still
reads nothing until the installed SpicyDocs reader's rule is
``gao-product-page-details/2`` or later; each run then reads two known pages first,
and holds a product's first week against ``gao_recommendations`` (a soft input:
this rollup's cron fires after gao-recommendations', so it reads the day's export).
``GAO_PRODUCT_PAGES_UNDO=<day, day and hour, or reader rule>`` puts back what the
product-page reads so named replaced (``2026-10-12``, ``2026-10-12T17``,
``gao-product-page-details/2``); it needs no token and works with the read off.
"""

import os
from pathlib import Path
from typing import ClassVar

from spicy_regs.env_values import flag_env, text_env
from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.government import GovernmentReceiptRollup
from spicy_regs.transforms import build_gao_reports


def _directory(name: str) -> Path | None:
    """The directory an environment variable names, or None when it is unset or blank."""
    return Path(value) if (value := text_env(name)) else None


class GaoReportsRollup(GovernmentReceiptRollup):
    """GAO oversight reports ingested from the gao.gov reports RSS feed."""

    name: ClassVar[str] = "gao-reports"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    #: The product-page pass holds a product's first week against GAO's open recommendations; absent, it waits.
    soft_inputs: ClassVar[tuple[str, ...]] = ("gao_recommendations.parquet",)
    outputs: ClassVar[tuple[str, ...]] = ("gao_reports.parquet", "gao_decisions.parquet")
    #: GAO's legal decisions from its own listing join the family as their own table (owner, 2026-09-28).
    added_tables: ClassVar[tuple[str, ...]] = ("gao_decisions.parquet",)

    def build(self, output_dir: Path) -> tuple[Path, Path]:
        return build_gao_reports(
            output_dir,
            evidence=self.source_evidence, receipt_generation_id=self.receipt_generation_id,
            govinfo_history=flag_env("GAO_GOVINFO_HISTORY"),
            govinfo_mods=flag_env("GAO_GOVINFO_MODS"),
            listing_run=_directory("GAO_LISTING_RUN"),
            decision_pages=_directory("GAO_DECISION_PAGES"),
            major_rule_run=_directory("GAO_MAJOR_RULE_RUN"),
            old_index_run=_directory("GAO_MAJOR_RULE_OLD_INDEX_RUN"),
            major_rule_letters=tuple(
                Path(part.strip()) for part in (text_env("GAO_MAJOR_RULE_LETTERS") or "").split(os.pathsep) if part.strip()
            ),
            product_pages=flag_env("GAO_PRODUCT_PAGES"),
            product_pages_undo=text_env("GAO_PRODUCT_PAGES_UNDO"),
        )


app = make_rollup_app(GaoReportsRollup)

if __name__ == "__main__":
    app()
