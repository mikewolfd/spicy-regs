"""Rollup pipeline: the thirteen bill-family tables from one acquisition pass.

Like the other ingesting rollups this reads no base table from R2, so
``inputs`` is empty; the incremental merge with each prior published table
happens inside the transform. It declares ``outputs`` rather than ``output``
because one expensive pass — the BILLSTATUS archives, the GovInfo printings and
the model calls — fills all thirteen. Thirteen rollups would repeat that pass
thirteen times.

Four further outputs ride along, none a contract table and all published
for the same reason every other output is — something reads them back from R2:

* ``bill_family_archives`` is the rollup's own processing state, the BILLSTATUS
  folder listing entry each run retains so the next one can prove a zip
  unchanged without downloading it.
* ``bill_vote_references`` is the roll calls each bill's actions record, which
  the ``roll-call-votes`` rollup joins against at merge time to fill
  ``roll_call_votes.bill_id``. It is emitted here because the BILLSTATUS
  document is already parsed in this pass; reaching the same fields inside that
  rollup would mean re-acquiring every scoped bill's status.
* ``bill_family_backfills`` and ``bill_family_backfill_congresses`` are the
  pre-BILLSTATUS backfill's own retained state (which bill was filled under
  which list stamp; per Congress walked, the route's declared total against
  what was reached), the resume record that lets the next named run skip every
  bill already filled for one comparison apiece.

Scope comes from the workflow inputs ``BILL_FAMILY_CONGRESSES`` and
``BILL_FAMILY_BILL_TYPES``, defaulting to the current Congress and all eight
bill types.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_bill_family import (
    ARCHIVES_TABLE,
    BACKFILLS_TABLE,
    BACKFILL_CONGRESSES_TABLE,
    FAMILY_TABLES,
    VOTE_REFERENCES_TABLE,
    build_bill_family,
)


class BillFamilyRollup(RollupPipeline):
    """Bills, actions, committees, printings, sections, diffs and model tables (BILLSTATUS + GovInfo)."""

    name: ClassVar[str] = "bill-family"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = tuple(f"{contract}.parquet" for contract, _ in FAMILY_TABLES) + (
        "public_activity_events.parquet",
        f"{ARCHIVES_TABLE}.parquet",
        f"{VOTE_REFERENCES_TABLE}.parquet",
        f"{BACKFILLS_TABLE}.parquet",
        f"{BACKFILL_CONGRESSES_TABLE}.parquet",
    )

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_bill_family(output_dir)


app = make_rollup_app(BillFamilyRollup)

if __name__ == "__main__":
    app()
