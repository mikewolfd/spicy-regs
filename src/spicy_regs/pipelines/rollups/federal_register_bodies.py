"""Rollup pipeline: ``federal_register_document_bodies.parquet`` (significant FR Rules & Proposed Rules).

Reads the declared v1 population (``document_type IN ('Rule', 'Proposed Rule') AND significant = true
AND publication_date >= since``, default ``2021-01-01``) from the pinned ``federal_register.parquet``
ingest generation, carries forward previously acquired bodies without re-fetching, and acquires
remaining bodies through SpicyDocs' ``body_captures`` (preferring official publisher XML with bounded
GovInfo HTML fallback). Raw XML/HTML bytes stay in R2 ``source-evidence/blobs/sha256/<hex>``; no
per-row ETL receipts are published.

Environment controls:
- ``FR_BODIES_SINCE`` (YYYY-MM-DD, default ``2021-01-01``): publication-date floor.
- ``FR_BODIES_MAX_NEW`` (positive integer, optional): cap newly attempted documents per batch for
  resumable bootstrap runs.
- ``FR_BODIES_RETRY_FAILED`` (``true``/``false``, default ``false``): re-attempt prior ``refused`` rows.
- ``FR_BODIES_REQUEST_INTERVAL_SECONDS`` (float >= 0, default ``0.4``): polite request spacing.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar

from spicy_regs.env_values import date_env, flag_env, float_env, int_env
from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_federal_register_bodies import (
    DEFAULT_MIN_REQUEST_INTERVAL_SECONDS,
    DEFAULT_SINCE,
    FAMILY,
    OUTPUT,
    build_federal_register_bodies,
)


class FederalRegisterBodiesRollup(RollupPipeline):
    """Offline full text for EO 12866 significant Federal Register Rules and Proposed Rules (2021-present)."""

    name: ClassVar[str] = FAMILY
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ("federal_register.parquet",)
    output: ClassVar[str] = OUTPUT
    #: Injectable HTTPX transport for hermetic tests; ``None`` uses live polite acquisition.
    transport: ClassVar[Any] = None
    #: Optional per-class pacing override for hermetic tests; ``None`` uses env or 0.4s default.
    min_request_interval_seconds: ClassVar[float | None] = None

    def _prime(self, output_dir: Path, snapshot: Mapping | None = None) -> dict[str, dict]:
        parents = super()._prime(output_dir, snapshot)
        self._primed_parents = parents
        return parents

    def _source_pin(self) -> dict[str, str] | None:
        parents = getattr(self, "_primed_parents", None) or {}
        fr_parent = parents.get("federal_register.parquet")
        if not isinstance(fr_parent, Mapping):
            return None
        digest = fr_parent.get("artifactDigest") or fr_parent.get("sha256")
        if not isinstance(digest, str) or not digest:
            return None
        family = fr_parent.get("family") or "federal-register"
        return {
            "logicalId": f"urn:spicy-regs:family:{family}",
            "artifactDigest": digest,
        }

    def build(self, output_dir: Path) -> Path:
        since = date_env("FR_BODIES_SINCE") or DEFAULT_SINCE
        max_new = int_env("FR_BODIES_MAX_NEW")
        retry_failed = flag_env("FR_BODIES_RETRY_FAILED")
        env_interval = float_env("FR_BODIES_REQUEST_INTERVAL_SECONDS")
        interval = (
            env_interval
            if env_interval is not None
            else (
                self.min_request_interval_seconds
                if self.min_request_interval_seconds is not None
                else DEFAULT_MIN_REQUEST_INTERVAL_SECONDS
            )
        )
        return build_federal_register_bodies(
            output_dir,
            fr_path=output_dir / "federal_register.parquet",
            since=since,
            max_new=max_new,
            retry_failed=retry_failed,
            min_request_interval_seconds=interval,
            source_pin=self._source_pin(),
            transport=self.transport,
            evidence=self.source_evidence,
            raise_if_unchanged=True,
        )


app = make_rollup_app(FederalRegisterBodiesRollup)

if __name__ == "__main__":
    app()
