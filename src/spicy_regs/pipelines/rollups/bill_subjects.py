"""Rollup pipeline: bill_subjects.parquet (BILLSTATUS / Congress.gov subject enrichment).

Reads the published ``congress_bills.parquet`` as its input snapshot — the
``fr_docket_links`` shape, where a rollup keys off another published artifact
without owning it — and adds the subject assignment its list-level rows never
carry: from BILLSTATUS for the 108th Congress on, from Congress.gov below it.
The bounded, resumable enrichment itself lives in ``enrich_bill_subjects``; the
base class handles priming the input and the shrink-guarded R2 upload of the
single output.

Runs on its own cron, well after the bill family's, so each run enriches
against that day's ``congress_bills``.
"""

import os
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms import enrich_bill_subjects
from spicy_regs.transforms.enrich_bill_subjects import DEADLINE_SECONDS


def _int_env(name: str) -> int | None:
    """Read a positive integer override, or None to use the transform's default cap."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be positive, got {value}")
    return value


class BillSubjectsRollup(RollupPipeline):
    """Per-bill policy area and legislative subjects (GPO BILLSTATUS / Congress.gov)."""

    name: ClassVar[str] = "bill-subjects"
    inputs: ClassVar[tuple[str, ...]] = ("congress_bills.parquet",)
    output: ClassVar[str] = "bill_subjects.parquet"
    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> Path:
        minutes = _int_env("BILL_SUBJECTS_DEADLINE_MINUTES")
        return enrich_bill_subjects(
            output_dir,
            max_bills=_int_env("BILL_SUBJECTS_MAX"),
            deadline_seconds=DEADLINE_SECONDS if minutes is None else minutes * 60,
            evidence=self.source_evidence,
        )


app = make_rollup_app(BillSubjectsRollup)

if __name__ == "__main__":
    app()
