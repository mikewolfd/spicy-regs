"""Rollup pipeline: the thirteen bill-family tables from one acquisition pass.

Like the other ingesting rollups it reads no base table from R2, so ``inputs``
is empty and the incremental merge with each prior published table happens
inside the transform. It declares ``outputs`` rather than ``output`` because one
expensive pass — the BILLSTATUS archives, the GovInfo printings and the model
calls — fills all thirteen; thirteen rollups would repeat that pass thirteen
times. Four further outputs ride along, all published because something reads
them back from R2: ``bill_family_archives`` is this rollup's own processing
state, the BILLSTATUS folder-listing entry retained so the next run can prove a
zip unchanged without downloading it; ``bill_vote_references`` is the roll
calls each bill's actions record, which the ``roll-call-votes`` rollup joins at
merge time to fill ``roll_call_votes.bill_id`` (emitted here because the
BILLSTATUS document is already parsed in this pass); and
``bill_family_backfills``/``bill_family_backfill_walks`` are the pre-BILLSTATUS
backfill's retained resume state — per attempted bill, the list stamp and
whether it was filled or refused; per ``(congress, bill_type)`` walked, the
route's declared total against what was reached. Scope comes from the workflow
inputs ``BILL_FAMILY_CONGRESSES`` and ``BILL_FAMILY_BILL_TYPES``, defaulting to
the current Congress and all eight bill types.
Printing bodies come from the keyless BILLS bulk folders from the 113th Congress
on (``transforms/bill_family_bodies.py``), each zip read once for the printings
it holds; ``BILL_FAMILY_MAX_VERSION_FETCHES`` bounds the per-package route that
remains for older printings and what bulk does not hold, and the pre-108
metadata-backfill request budget; it defaults to 600, and zero disables those
requests without disabling BILLSTATUS or bulk text reads (model-call policy is
separate from this acquisition cap). The ``congress_bills`` merge
reads the published ``laws`` table best-effort to fill
``statutes_at_large_cite`` (``transforms/table_merge.py``) — a ``soft_input``,
absent or stale without failing anything — which is why the ``laws`` rollup's
cron fires before this one's.
"""

import os
from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_bill_family import (
    ARCHIVES_TABLE,
    BACKFILLS_TABLE,
    BACKFILL_WALKS_TABLE,
    FAMILY_TABLES,
    MAX_VERSION_FETCHES,
    VOTE_REFERENCES_TABLE,
    build_bill_family,
)


class BillFamilyRollup(RollupPipeline):
    """Bills, actions, committees, printings, sections, diffs and model tables (BILLSTATUS + GovInfo)."""

    name: ClassVar[str] = "bill-family"
    inputs: ClassVar[tuple[str, ...]] = ()
    soft_inputs: ClassVar[tuple[str, ...]] = ("laws.parquet",)
    outputs: ClassVar[tuple[str, ...]] = tuple(f"{contract}.parquet" for contract, _ in FAMILY_TABLES) + (
        "public_activity_events.parquet",
        f"{ARCHIVES_TABLE}.parquet",
        f"{VOTE_REFERENCES_TABLE}.parquet",
        f"{BACKFILLS_TABLE}.parquet",
        f"{BACKFILL_WALKS_TABLE}.parquet",
    )

    retain_source_evidence: ClassVar[bool] = True

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        raw = os.environ.get("BILL_FAMILY_MAX_VERSION_FETCHES", "").strip()
        if raw and (not raw.isascii() or not raw.isdecimal()):
            raise ValueError("BILL_FAMILY_MAX_VERSION_FETCHES must be a nonnegative integer; zero disables acquisition")
        budget = int(raw) if raw else MAX_VERSION_FETCHES
        return build_bill_family(output_dir, max_version_fetches=budget, evidence=self.source_evidence)


app = make_rollup_app(BillFamilyRollup)

if __name__ == "__main__":
    app()
