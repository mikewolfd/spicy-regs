"""Transform: build ``amendments.parquet`` from the Congress.gov amendment route.

One row per amendment, keyed on its *own* publisher identity —
``(congress, amendment_type, amendment_number)`` — not on the bill it amends.
An amendment can amend another amendment, so the amended bill and amended
amendment stay as foreign-key columns rather than serving as the key.

``status`` is deliberately absent from the contract: BillTrax hardcoded it to
``"Proposed"`` for every row, which is a field that never carried information.

Incremental on the same pattern as ``build_congress_bills``: the window starts
at the prior published table's max ``update_date`` minus a short overlap and
runs forward at most :data:`MAX_WINDOW_DAYS`, so a steady-state run asks for
the last few days rather than re-walking every amendment of the Congress. The
route honours ``fromDateTime``/``toDateTime``, so the bound is the server's,
not a client-side filter over a full walk. With no prior table the window is
open and the run is a full backfill. ``AMENDMENTS_SINCE``/``AMENDMENTS_UNTIL``
drive a chunk explicitly.

Each amendment's list record is overlaid with its detail record, which alone
states the sponsor and the amended bill or amendment: one paced detail request
per amendment in the window.

Needs an api.data.gov key, resolved from the environment by the same helper
the existing Congress.gov ingest uses. A keyless run raises rather than
publishing a truncated table.
"""

from __future__ import annotations

import os
from collections.abc import Callable
from datetime import date, datetime, timedelta
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

from spicy_regs.sources import r2
from spicy_regs.sources.pooled_walk import pool_passes
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import congresses_from_env
from spicy_regs.transforms.congress_walk import ListingSource
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

NAME = "amendments"
OUTPUT = "amendments.parquet"


BUDGET = PagedJsonBudget(
    max_requests=5,  # measured retry bound: see fork-execution-2026-09-21/retry-measurement-2026-09-22
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    # Every amendment in the window costs one detail request; api.data.gov allows 5,000 an
    # hour per key, so a full Congress (7,066 in the 119th) paces under that bound.
    min_request_interval_seconds=0.75,
)

#: Pages of ``MAX_LIMIT`` records per Congress. The 119th had ~5,600 amendments
#: at 2026-09, so 100 pages of 250 is ~4x headroom; a Congress that exceeded it
#: would be logged as a short walk by the reader rather than silently cut.
MAX_PAGES = 100

#: Re-ask this many days before the stored watermark, so an amendment updated
#: after the previous run's cutoff is still picked up.
OVERLAP_DAYS = 3

#: Largest span one run will ask for, so a deep backfill converges over
#: successive runs instead of timing out and persisting nothing.
MAX_WINDOW_DAYS = 90


def _prior_max_update_date(prior_file: Path) -> date | None:
    """Largest ``update_date`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(update_date) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def _date_env(name: str) -> date | None:
    """Parse a ``YYYY-MM-DD`` env var, or None when unset; a malformed value raises ValueError naming it."""
    raw = os.environ.get(name, "").strip()
    if not raw:
        return None
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be YYYY-MM-DD, got {raw!r}") from exc


def _instant(day: date | None, *, end: bool = False) -> str | None:
    """The route takes an instant, not a date."""
    if day is None:
        return None
    return datetime.combine(day, datetime.max.time() if end else datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%SZ")


#: Sort orders for successive passes over one query. A single offset-paged pass sorted by
#: ``updateDate`` repeats about as many records as it skips, so its row count still equals the
#: declared count: on 2026-09-23 the 119th Congress walk returned its declared 7,066 rows but
#: only 7,013 distinct amendments, and the published table was 52 short. The two orders drop
#: different records; one pass of each, pooled by identity, reached all 7,066.
POOLED_SORTS = ("updateDate desc", "updateDate asc", "updateDate desc", "updateDate asc")


def _pooled_walk(reader: ListingSource, route, *, congress, since: date | None, until: date | None) -> list[dict]:
    """Every list record the query declares, pooled over :data:`POOLED_SORTS` passes; the later ``updateDate`` wins."""

    def passes():
        for sort in POOLED_SORTS:
            url = list_route_url(
                route,
                congress=congress,
                limit=MAX_LIMIT,
                sort=sort,
                from_datetime=_instant(since),
                to_datetime=_instant(until, end=True),
            )
            records, declared = [], None
            for page in reader.records(route, url, max_pages=MAX_PAGES):
                if getattr(page, "declared_count", None) is not None:
                    declared = page.declared_count
                records.extend(dict(record) for record in page.records)
            yield records, declared

    return pool_passes(
        passes(),
        key=lambda r: (str(r.get("congress")), str(r.get("type") or "").lower(), str(r.get("number"))),
        newer=lambda held, r: str(r.get("updateDate") or "") >= str(held.get("updateDate") or ""),
        label=f"Amendments: Congress {congress}",
    )


def _with_detail(reader: ListingSource, record: dict) -> dict:
    """The list record overlaid with its detail record.

    The list route states no sponsors and no amended bill or amendment; the detail route
    states them and omits ``latestAction`` and ``description``, which the list states
    (measured 2026-09-23), so each keeps what the other lacks. A detail that answers
    nothing refuses the run rather than publishing a row without its sponsor.
    """
    route = LIST_ROUTES["amendment-detail"]
    url = list_route_url(
        route,
        congress=int(record["congress"]),
        amendment_type=str(record["type"]).lower(),
        number=int(record["number"]),
    )
    details = [detail for page in reader.records(route, url, max_pages=1) for detail in page.records]
    if len(details) != 1:
        raise RuntimeError(f"Amendments: detail for {url} answered {len(details)} records")
    return {**record, **details[0]}


def build_amendments(
    output_dir: Path,
    *,
    reader: ListingSource | None = None,
    since: date | None = None,
    until: date | None = None,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> Path:
    """Build ``amendments.parquet`` for the scoped Congresses, incrementally."""
    if reader is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Amendments need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = CongressListingReader(budget=BUDGET, api_key=api_key)

    # 1. The prior table, to the path the merge below reuses in place.
    prior_file = prior_scratch_path(output_dir, NAME)
    have_prior = prior_file.exists() or download_prior(OUTPUT, prior_file)

    # 2. The window: from the stored watermark minus an overlap, forward at
    # most one catch-up span.
    since = since or _date_env("AMENDMENTS_SINCE")
    until = until or _date_env("AMENDMENTS_UNTIL")
    if since is None and have_prior:
        watermark = _prior_max_update_date(prior_file)
        since = (watermark - timedelta(days=OVERLAP_DAYS)) if watermark else None
    if since is not None:
        if until is not None and until < since:
            raise ValueError(f"AMENDMENTS_UNTIL {until} precedes since {since}")
        until = min(until or date.today(), since + timedelta(days=MAX_WINDOW_DAYS))
    logger.info(
        "Amendments: fetching amendments updated {} through {}",
        since or "the beginning",
        until or "now",
    )

    route = LIST_ROUTES["amendment"]
    rows: list[dict] = []
    for congress in congresses_from_env():
        pooled = _pooled_walk(reader, route, congress=congress, since=since, until=until)
        rows.extend(shape_amendment(_with_detail(reader, record)) for record in pooled)
        logger.info("Amendments: Congress {} — {:,} amendments in window", congress, len(pooled))

    logger.info("Amendments: {:,} rows this run", len(rows))
    return merge_contract_table(output_dir, "amendments", rows, prior_present=have_prior)
