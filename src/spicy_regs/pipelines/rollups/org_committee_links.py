"""Rollup pipeline: org_committee_links.parquet (commenter org → FEC committee).

Derives the name bridge between the regulations.gov corpus and the OpenFEC
committee reference dimension. ``fec_committees.parquet`` and
``fec_committee_history.parquet`` (each committee's stated sponsor by filing
year) are *ingest* rollups' outputs treated as base inputs, following the
``fr_docket_links`` precedent over ``federal_register.parquet``: an ingest table
has no upstream dependency inside this repo, so reading it adds only a
cron-ordering preference, which the workflow handles by running after the FEC
ingests.
``comments.parquet`` is deliberately *not* declared as an input — at ~3.3 GB it
would dominate the job and the transform needs five narrow columns from it — so
it is read straight from the public bucket with Parquet projection pushdown
(see :mod:`spicy_regs.transforms.build_org_committee_links`); a local copy in
``output_dir`` is still preferred when one exists.
"""

from pathlib import Path
from typing import ClassVar

from spicy_regs.pipelines.rollups.base import make_rollup_app
from spicy_regs.pipelines.rollups.fec_receipts import FecReceiptRollup as RollupPipeline


class OrgCommitteeLinksRollup(RollupPipeline):
    """Commenter organizations name-matched to FEC committees/PACs."""

    name: ClassVar[str] = "org-committee-links"
    inputs: ClassVar[tuple[str, ...]] = ("fec_committees.parquet", "fec_committee_history.parquet")
    remote_inputs: ClassVar[tuple[str, ...]] = ("comments.parquet",)
    output: ClassVar[str] = "org_committee_links.parquet"

    def build(self, output_dir: Path) -> Path:
        return self.build_receipts(output_dir)


app = make_rollup_app(OrgCommitteeLinksRollup)

if __name__ == "__main__":
    app()
