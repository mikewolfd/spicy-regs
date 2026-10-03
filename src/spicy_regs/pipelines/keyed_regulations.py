"""What the steps that ask the keyed Regulations.gov API share: the reader, and the bare outcome file each keeps.

``fill-docket-gaps`` and ``reconcile-dockets`` ask the publisher through SpicyDocs' ``RegulationsGovApiReader`` with
the api.data.gov key in ``DATA_GOV_API_KEY``, and each records every answer in its own bare Parquet object on R2, one
row per asked id, the latest answer replacing the earlier one.
"""

from __future__ import annotations

import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from spicy_regs.sources import r2

if TYPE_CHECKING:
    from spicy_docs.sources.regulations_gov.api import RegulationsGovApiReader


def keyed_reader(*, max_requests: int, min_request_interval_seconds: float) -> RegulationsGovApiReader:
    """The keyed API reader; ``max_requests`` bounds the attempts of each request, its retries included."""
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.regulations_gov.api import RegulationsGovApiReader

    key = os.environ.get("DATA_GOV_API_KEY")
    if not key:
        raise RuntimeError("DATA_GOV_API_KEY is required to ask the Regulations.gov API")
    budget = PagedJsonBudget(
        max_requests=max_requests,
        max_page_bytes=1 << 20,
        timeout_seconds=60,
        min_request_interval_seconds=min_request_interval_seconds,
    )
    return RegulationsGovApiReader(budget=budget, api_key=key)


def load_outcomes(output_dir: Path, key: str, schema: Mapping[str, Any]) -> tuple[Path, pl.DataFrame]:
    """The step's recorded answers: the local copy, else R2's bare object, else none yet."""
    path = output_dir / key
    if not path.exists():
        r2.download_working_copy(key, path)
    return path, pl.read_parquet(path) if path.exists() else pl.DataFrame(schema=schema)


def save_outcomes(
    path: Path, prior: pl.DataFrame, rows: Sequence[Mapping[str, Any]], *, key: str, skip_upload: bool
) -> pl.DataFrame:
    """Replace each answered id's row with its new answer, keep every other, and upload unless ``skip_upload``."""
    fresh = pl.DataFrame(list(rows), schema=prior.schema)
    merged = pl.concat([prior.join(fresh.select(key), on=key, how="anti"), fresh]).sort(key)
    merged.write_parquet(path)
    if not skip_upload:
        r2.upload_file(path, remote_key=path.name)
    return merged
