"""Rollup pipeline: the thirteen bill-family tables from one acquisition pass.

Like the other ingesting rollups this reads no base table from R2, so
``inputs`` is empty; the incremental merge with each prior published table
happens inside the transform. It declares ``outputs`` rather than ``output``
because one expensive pass — the BILLSTATUS archives, the GovInfo printings and
the model calls — fills all thirteen. Thirteen rollups would repeat that pass
thirteen times.

Scope comes from the workflow inputs ``BILL_FAMILY_CONGRESSES`` and
``BILL_FAMILY_BILL_TYPES``, defaulting to the current Congress and all eight
bill types.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_bill_family import FAMILY_TABLES, build_bill_family


class BillFamilyRollup(RollupPipeline):
    """Bills, actions, committees, printings, sections, diffs and model tables (BILLSTATUS + GovInfo)."""

    name: ClassVar[str] = "bill-family"
    inputs: ClassVar[tuple[str, ...]] = ()
    outputs: ClassVar[tuple[str, ...]] = tuple(f"{contract}.parquet" for contract, _ in FAMILY_TABLES) + (
        "public_activity_events.parquet",
    )

    def build(self, output_dir: Path) -> tuple[Path, ...]:
        return build_bill_family(output_dir)


app = make_rollup_app(BillFamilyRollup)

if __name__ == "__main__":
    app()
