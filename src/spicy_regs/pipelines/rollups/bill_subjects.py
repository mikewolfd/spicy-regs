"""Rollup pipeline: bill_subjects.parquet (Congress.gov / GPO subject enrichment).

Reads the published ``congress_bills.parquet`` as its input snapshot — the
``fr_docket_links`` shape, where a rollup keys off another published artifact
without owning it — and fetches the per-bill subject assignment the ``/bill``
list payload never carries. The bounded, resumable enrichment itself lives in
``enrich_bill_subjects``; the base class handles priming the input and the
shrink-guarded R2 upload of the single output.

Runs on its own cron, deliberately offset from the congress_bills ingest so each
run enriches against a table that has already been refreshed.
"""

import os
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import enrich_bill_subjects
from spicy_regs.transforms.enrich_bill_subjects import MAX_BILLS_PER_RUN


def _int_env(name: str, default: int) -> int:
    """Read a positive integer override, or fall back to the pinned default."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


class BillSubjectsRollup(RollupPipeline):
    """Per-bill policy area and legislative subjects (Congress.gov / GPO BILLSTATUS)."""

    name: ClassVar[str] = "bill-subjects"
    inputs: ClassVar[tuple[str, ...]] = ("congress_bills.parquet",)
    output: ClassVar[str] = "bill_subjects.parquet"

    def build(self, output_dir: Path) -> Path:
        return enrich_bill_subjects(
            output_dir,
            max_bills=_int_env("BILL_SUBJECTS_MAX", MAX_BILLS_PER_RUN),
        )


app = make_rollup_app(BillSubjectsRollup)

if __name__ == "__main__":
    app()
