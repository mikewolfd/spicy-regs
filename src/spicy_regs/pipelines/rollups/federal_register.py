"""Rollup pipeline: federal_register.parquet (federalregister.gov REST ingest).

Unlike the derived rollups, this one *ingests* an external source rather than
reading base tables from R2, so ``inputs`` is empty: the fetch and the merge
into the pinned published table happen in ``merge_federal_register``. The base
class still handles the shrink-guarded upload of the single output.

It publishes no per-row receipts (ss-vse3; its installed policy is marked
``receipts_retired``). A row's evidence is the run's journal and the retained
API page its ``source_sha256`` names; the receipts of the generations before the
switch stay where they are (``docs/pins/receipt-archive.json``).

``FR_SINCE`` (YYYY-MM-DD) replaces the resume point for a re-read: at the
1994 epoch it refetches every document, so a column added to the projection
(``topics_json``, 2026-09-26) reaches every row. ``FR_UNTIL`` (YYYY-MM-DD)
bounds the upper end of a re-read window so multi-year backfills run in
paced, resumable batches (leaving all rows outside ``[since, until]`` intact
during merge). ``FR_REQUEST_INTERVAL_SECONDS`` sets optional polite pacing
between API pages.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.env_values import date_env, float_env
from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_federal_register import merge_federal_register


class FederalRegisterRollup(RollupPipeline):
    """Federal Register documents ingested from federalregister.gov (no API key)."""

    name: ClassVar[str] = "federal-register"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "federal_register.parquet"

    def build(self, output_dir: Path) -> Path:
        interval = float_env("FR_REQUEST_INTERVAL_SECONDS")
        return merge_federal_register(
            output_dir,
            since=date_env("FR_SINCE"),
            until=date_env("FR_UNTIL"),
            request_interval_seconds=0.0 if interval is None else interval,
            evidence=self.source_evidence,
        )


app = make_rollup_app(FederalRegisterRollup)

if __name__ == "__main__":
    app()
