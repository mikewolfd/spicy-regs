"""Transform: build ``amendments.parquet`` from the Congress.gov amendment route.

One row per amendment, keyed on its *own* publisher identity —
``(congress, amendment_type, amendment_number)`` — not on the bill it amends.
An amendment can amend another amendment, so the amended bill and amended
amendment stay as foreign-key columns rather than serving as the key.

``status`` is deliberately absent from the contract: BillTrax hardcoded it to
``"Proposed"`` for every row, which is a field that never carried information.

Needs an api.data.gov key, resolved from the environment by the same helper
the existing Congress.gov ingest uses. A keyless run raises rather than
publishing a truncated table.
"""

from __future__ import annotations

from pathlib import Path

from loguru import logger
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.congress_activity_tables import shape_amendment
from spicy_docs.sources.congress.listing import (
    LIST_ROUTES,
    MAX_LIMIT,
    CongressListingReader,
    list_route_url,
)

from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import congresses_from_env
from spicy_regs.transforms.table_merge import merge_contract_table

BUDGET = PagedJsonBudget(
    max_requests=2_000,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Pages of ``MAX_LIMIT`` records per Congress. The 119th had ~5,600 amendments
#: at 2026-09, so 100 pages of 250 is ~4x headroom; a Congress that exceeded it
#: would be logged as a short walk by the reader rather than silently cut.
MAX_PAGES = 100


def build_amendments(output_dir: Path, *, reader: CongressListingReader | None = None) -> Path:
    """Build ``amendments.parquet`` for the scoped Congresses."""
    if reader is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Amendments need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = CongressListingReader(budget=BUDGET, api_key=api_key)

    route = LIST_ROUTES["amendment"]
    rows: list[dict] = []
    for congress in congresses_from_env():
        url = list_route_url(route, congress=congress, limit=MAX_LIMIT, sort="updateDate desc")
        count = 0
        for page in reader.records(route, url, max_pages=MAX_PAGES):
            for record in page.records:
                rows.append(shape_amendment(record))
                count += 1
        logger.info("Amendments: Congress {} — {:,} amendments", congress, count)

    logger.info("Amendments: {:,} rows this run", len(rows))
    return merge_contract_table(output_dir, "amendments", rows)
