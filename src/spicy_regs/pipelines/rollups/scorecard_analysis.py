"""Publish exact scorecard identity links from immutable congressional inputs."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
from os import getenv
from pathlib import Path
from typing import ClassVar

from spicy_regs.scorecards.etl import selected_table_entry
from spicy_regs.scorecards.etl import LINK_NAMES, POLICIES, verified_receipt_download
from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.sources import publication
from spicy_regs.transforms.build_scorecard_analysis import INPUTS, OFFICIAL_TABLES, OUTPUTS, build_scorecard_analysis


class ScorecardAnalysisRollup(RollupPipeline):
    """One analysis family, pinned independently from source scorecard refreshes."""

    name: ClassVar[str] = "scorecard-analysis"
    inputs: ClassVar[tuple[str, ...]] = INPUTS
    outputs: ClassVar[tuple[str, ...]] = OUTPUTS
    receipt_policies: ClassVar[tuple] = tuple(POLICIES[name] for name in LINK_NAMES)

    def _prime(self, output_dir: Path, snapshot: Mapping | None = None) -> dict[str, dict]:
        """Verify every member, including partitioned bill tables, under one index."""
        self.__dict__.pop("_scorecard_input_pins", None)
        self.__dict__.pop("_scorecard_input_paths", None)
        self._scorecard_source_receipt = None
        self._scorecard_source_generation = None
        self._official_receipts = {}
        if snapshot is None:
            raise publication.PublicationError("Scorecard analysis requires a captured publication index")
        public_url = getenv("R2_PUBLIC_URL")
        parents, paths = {}, {}
        for key in self.inputs:
            owner = publication.table_owner(snapshot, key)
            if owner is None:
                raise publication.PublicationError(f"Scorecard analysis requires managed input {key}")
            parents[key] = publication.table_pin(snapshot, key)
            members = publication.table_members(snapshot, key)
            paths[key.removesuffix(".parquet")] = []
            for member in members:
                target = output_dir / member.key
                if target.is_file():
                    with target.open("rb") as stream:
                        digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
                    if digest != member.sha256 or target.stat().st_size != member.byte_size:
                        raise publication.PublicationError(f"Local analysis input differs from its pin: {member.key}")
                else:
                    if not public_url:
                        raise publication.PublicationError("Missing analysis input and R2_PUBLIC_URL")
                    target.parent.mkdir(parents=True, exist_ok=True)
                    if not publication.fetch_member(public_url, member, target, key):
                        raise publication.PublicationError(f"Missing pinned analysis input: {member.key}")
                paths[key.removesuffix(".parquet")].append(target)
        source_owner = selected_table_entry(snapshot, "scorecards.parquet")
        for source_key in ("scorecard_members.parquet", "scorecard_items.parquet"):
            if selected_table_entry(snapshot, source_key)["artifactDigest"] != source_owner["artifactDigest"]:
                raise publication.PublicationError("Scorecard analysis source inputs must share one generation")
        if "etlReceipts" in source_owner:
            self._scorecard_source_receipt = verified_receipt_download(
                snapshot, output_dir / "source-etl-receipts.parquet", public_url=public_url
            )
            self._scorecard_source_generation = source_owner["etlReceipts"]["generationId"]
        for name in OFFICIAL_TABLES:
            owner = selected_table_entry(snapshot, name + ".parquet")
            if "etlReceipts" in owner:
                receipt = verified_receipt_download(
                    snapshot, output_dir / (name + "-etl-receipts.parquet"), public_url=public_url, dataset=name
                )
                self._official_receipts[name] = (receipt, owner["etlReceipts"]["generationId"])
        self._scorecard_input_pins = {key.removesuffix(".parquet"): value for key, value in parents.items()}
        self._scorecard_input_paths = paths
        return parents

    def build(self, output_dir: Path) -> tuple[Path, Path]:
        if not hasattr(self, "_scorecard_input_pins"):
            raise publication.PublicationError("Prime verified scorecard analysis inputs before building")
        return build_scorecard_analysis(
            output_dir,
            input_pins=self._scorecard_input_pins,
            input_paths=self._scorecard_input_paths,
            source_receipt_path=self._scorecard_source_receipt,
            source_generation_id=self._scorecard_source_generation,
            official_receipts=self._official_receipts,
            receipt_generation_id=self.receipt_generation_id,
        )


app = make_rollup_app(ScorecardAnalysisRollup)

if __name__ == "__main__":
    app()
