"""Rollup pipeline: sam_entities.parquet (SAM.gov Entity API v4 ingest).

Unlike the derived rollups this one *ingests* an external source, so ``inputs``
is empty — the bounded fetch and the incremental merge with the prior published
table happen inside ``build_sam_entities``, and the base class still handles the
shrink-guarded R2 upload of the single output. The active registry (~765K
entities) is far past the ~5K synchronous pagination ceiling, so full coverage
comes from SAM's bulk extract walked over ``registrationDate`` year windows (see
:mod:`spicy_docs.sources.sam` / `spicy_docs.sources.sam_extract`); to keep each *scheduled* run bounded
while still converging on full coverage, the default run fetches a single
rotating year window chosen from the run date, and the transform's merge
accretes each window into the prior table across runs. Env overrides, read here
so the shared rollup CLI stays minimal: ``SAM_INGEST_MODE`` (``extract``
default or ``partition``), ``SAM_SINCE_YEAR``/``SAM_UNTIL_YEAR`` (both required
together for an explicit range; e.g. ``2000``..current for a full backfill),
and ``SAM_MAX_RECORDS`` (blank/``0`` = unbounded).
"""

import os
from datetime import date
from pathlib import Path
from typing import ClassVar

from loguru import logger

from spicy_regs.pipelines.rollups.base import RollupPipeline, make_rollup_app
from spicy_regs.transforms.build_sam_entities import MIN_REGISTRATION_YEAR as _MIN_REGISTRATION_YEAR
from spicy_regs.transforms import build_sam_entities


def _rotating_year(today: date) -> int:
    """Pick one registrationDate year to fetch this run: the current year every other day.

    The current year gains registrations daily (4,357 in the three days after the
    2026 load of 2026-09-23), while an older year changes only as registrations
    expire. Even ordinal days re-read the current year; odd days cycle through
    ``[_MIN_REGISTRATION_YEAR, today.year - 1]``, each older year once per
    ``2 * span`` days. Deterministic from the date, with no persisted cursor.
    """
    day = today.toordinal()
    if day % 2 == 0:
        return today.year
    return _MIN_REGISTRATION_YEAR + (day // 2) % (today.year - _MIN_REGISTRATION_YEAR)


def _int_env(name: str) -> int | None:
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be an integer or blank") from None


class SamEntitiesRollup(RollupPipeline):
    """Federal entity registry ingested from the SAM.gov Entity API (api.data.gov key)."""

    name: ClassVar[str] = "sam-entities"
    retain_source_evidence: ClassVar[bool] = True
    inputs: ClassVar[tuple[str, ...]] = ()
    output: ClassVar[str] = "sam_entities.parquet"

    def build(self, output_dir: Path) -> Path:
        mode = os.environ.get("SAM_INGEST_MODE", "extract").strip() or "extract"
        since = _int_env("SAM_SINCE_YEAR")
        until = _int_env("SAM_UNTIL_YEAR")
        max_records = _int_env("SAM_MAX_RECORDS")  # blank/0 -> unbounded within the window(s)
        if (since is None) != (until is None):
            raise ValueError("Set both SAM_SINCE_YEAR and SAM_UNTIL_YEAR for an explicit selection")
        if max_records is not None and max_records < 0:
            raise ValueError("SAM_MAX_RECORDS must be nonnegative")

        if since is None and until is None:
            # Default scheduled run: one bounded, rotating year window.
            since = until = _rotating_year(date.today())
            logger.info("SAM rollup: scheduled default — rotating year window {}", since)
        else:
            logger.info("SAM rollup: explicit year range {}..{} (max_records={})", since, until, max_records)

        return build_sam_entities(
            output_dir,
            evidence=self.source_evidence,
            mode=mode,
            since_year=since,
            until_year=until,
            max_records=max_records if max_records else None,
        )


app = make_rollup_app(SamEntitiesRollup)

if __name__ == "__main__":
    app()
