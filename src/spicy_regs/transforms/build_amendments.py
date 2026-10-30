"""Transform: build ``amendments.parquet`` from the Congress.gov amendment route.

One row per amendment, keyed on its *own* publisher identity —
``(congress, amendment_type, amendment_number)`` — not on the bill it amends.
An amendment can amend another amendment, so the amended bill and amended
amendment stay as foreign-key columns rather than serving as the key.

``status`` is deliberately absent from the contract: BillTrax hardcoded it to
``"Proposed"`` for every row, which is a field that never carried information.

Incremental by an ``update_date`` watermark: the window starts
at the prior published table's max ``update_date`` minus a short overlap and
runs forward at most :data:`MAX_WINDOW_DAYS`, so a steady-state run asks for
the last few days rather than re-walking every amendment of the Congress. The
route honours ``fromDateTime``/``toDateTime``, so the bound is the server's,
not a client-side filter over a full walk. With no prior table the window is
open and the run is a full backfill. ``AMENDMENTS_SINCE``/``AMENDMENTS_UNTIL``
drive a chunk explicitly.

The list is read with ``CongressListingReader.pooled``. A single offset walk
sorted by ``updateDate`` repeats about as many records as it skips, so its row
count still equals the declared count: on 2026-09-23 the 119th Congress walk
returned its declared 7,066 rows but only 7,013 distinct amendments, and the
published table was 52 short. Whole walks, alternating order and page size, are
pooled by the amendment's identity (the newest ``updateDate`` kept) until one
is clean or the pool holds exactly the declared count; one walk of each order
reached all 7,066.

Each amendment's list record is overlaid with its detail record, which alone
states the sponsor and the amended bill or amendment: one paced detail request
per amendment in the window.

Needs an api.data.gov key, resolved from the environment by the same helper
the existing Congress.gov ingest uses. A keyless run raises rather than
publishing a truncated table.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from datetime import date, datetime, timedelta
from operator import itemgetter
from pathlib import Path
from typing import TYPE_CHECKING, Any

from loguru import logger
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.congress_activity_tables import shape_amendment
from spicy_docs.sources.congress.listing import (
    LIST_ROUTES,
    MAX_LIMIT,
    CongressListingReader,
    list_route_url,
)

from spicy_regs.env_values import date_env
from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key, list_identity
from spicy_regs.transforms.congress_scope import congresses_from_env
from spicy_regs.transforms.congress_walk import ListingSource
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

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

#: Pages per walk of one Congress. The 119th declared 7,095 amendments on
#: 2026-09-23, 32 pages at the smallest page a pooled walk asks for (223), so
#: 100 is ~3x headroom; a Congress past it refuses at the page bound rather
#: than being cut.
MAX_PAGES = 100

#: Re-ask this many days before the stored watermark, so an amendment updated
#: after the previous run's cutoff is still picked up.
OVERLAP_DAYS = 3

#: Largest span one run will ask for, so a deep backfill converges over
#: successive runs instead of timing out and persisting nothing.
MAX_WINDOW_DAYS = 90


def _prior_watermarks(prior_file: Path) -> dict[str, date]:
    """Largest ``update_date`` per Congress in the prior table, in one scan; empty when absent."""
    if not prior_file.exists():
        return {}
    import duckdb

    marks = {}
    for congress, stamp in duckdb.sql(
        f"SELECT congress, max(update_date) FROM read_parquet('{prior_file}') GROUP BY congress"
    ).fetchall():
        try:
            marks[str(congress)] = date.fromisoformat(str(stamp)[:10])
        except ValueError:
            continue
    return marks


def max_details_from_env(var: str = "AMENDMENTS_MAX_DETAILS") -> int | None:
    """Optional per-run detail request cap from ``AMENDMENTS_MAX_DETAILS``."""
    raw = os.environ.get(var, "").strip()
    if not raw:
        return None
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{var} must be a non-negative integer, got {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{var} must be a non-negative integer, got {raw!r}")
    return value


def _amendment_key(record: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(record["congress"]),
        str(record["type"]).lower(),
        str(record["number"]),
    )


def _held_amendment_state(
    prior_file: Path, congresses: Sequence[int]
) -> tuple[set[tuple[str, str, str, str | None]], dict[tuple[str, str, str], str | None], list[dict[str, Any]]]:
    """Return ``(enriched_at_stamp, held_stamps, unenriched_records)`` from ``prior_file`` for ``congresses``."""
    if not prior_file.exists() or not congresses:
        return set(), {}, []
    import pyarrow.parquet as pq

    scoped = {str(int(c)) for c in congresses}
    enriched: set[tuple[str, str, str, str | None]] = set()
    held_stamps: dict[tuple[str, str, str], str | None] = {}
    unenriched: list[dict[str, Any]] = []
    for row in pq.read_table(prior_file).to_pylist():
        congress = str(row.get("congress") or "")
        if congress not in scoped:
            continue
        key = (congress, str(row.get("amendment_type") or "").lower(), str(row.get("amendment_number") or ""))
        stamp = row.get("update_date")
        held_stamps[key] = stamp
        has_detail = any(
            row.get(col) is not None
            for col in ("sponsor_bioguide_id", "sponsor_full_name", "amended_bill_id", "amended_amendment_id")
        )
        if has_detail:
            enriched.add((*key, stamp))
        else:
            latest_date = row.get("latest_action_date")
            latest_text = row.get("latest_action_text")
            unenriched.append(
                {
                    "congress": int(congress) if congress.isdecimal() else congress,
                    "type": key[1].upper(),
                    "number": key[2],
                    "purpose": row.get("purpose"),
                    "description": row.get("description"),
                    "proposedDate": row.get("proposed_date"),
                    "submittedDate": row.get("submitted_date"),
                    "chamber": row.get("chamber"),
                    "updateDate": stamp,
                    "latestAction": (
                        {"actionDate": latest_date, "text": latest_text}
                        if latest_date is not None or latest_text is not None
                        else None
                    ),
                    "url": row.get("url"),
                }
            )
    return enriched, held_stamps, unenriched


def _instant(day: date | None, *, end: bool = False) -> str | None:
    """The route takes an instant, not a date."""
    if day is None:
        return None
    return datetime.combine(day, datetime.max.time() if end else datetime.min.time()).strftime("%Y-%m-%dT%H:%M:%SZ")


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
    reader: CongressListingReader | None = None,
    since: date | None = None,
    until: date | None = None,
    max_details: int | None = None,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence: CaptureEvidence | None = None,
    receipt_build=None,
) -> Path:
    """Build ``amendments.parquet`` for the scoped Congresses, incrementally."""
    if receipt_build is not None:
        return receipt_build.run(build_amendments, output_dir, **{
            key: value for key, value in locals().items() if key not in {"output_dir", "receipt_build"}
        })
    if reader is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Amendments need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        if evidence is not None:
            evidence.credential = api_key
        reader = CongressListingReader(
            budget=BUDGET, api_key=api_key,
            transport=None if evidence is None else evidence.transport(stage="amendment-response", max_bytes=BUDGET.max_page_bytes),
        )

    # 1. The prior table, to the path the merge below reuses in place.
    prior_file = prior_scratch_path(output_dir, NAME)
    have_prior = prior_file.exists() or download_prior(OUTPUT, prior_file)

    # 2. The window: explicit since/until when set, or per-Congress watermark
    # from the prior table minus an overlap, forward at most one catch-up span.
    explicit_since = since or date_env("AMENDMENTS_SINCE")
    explicit_until = until or date_env("AMENDMENTS_UNTIL")
    if max_details is None:
        max_details = max_details_from_env()
    if explicit_since is not None and explicit_until is not None and explicit_until < explicit_since:
        raise ValueError(f"AMENDMENTS_UNTIL {explicit_until} precedes since {explicit_since}")

    congresses = congresses_from_env()
    route = LIST_ROUTES["amendment"]
    rows: list[dict] = []
    listed_records: dict[tuple[str, str, str], dict[str, Any]] = {}
    last_since: date | None = explicit_since
    last_until: date | None = explicit_until

    watermarks = _prior_watermarks(prior_file) if explicit_since is None and have_prior else {}
    for congress in congresses:
        congress_since = explicit_since
        congress_until = explicit_until
        if congress_since is None and have_prior:
            watermark = watermarks.get(str(int(congress)))
            congress_since = (watermark - timedelta(days=OVERLAP_DAYS)) if watermark else None
        if congress_since is not None:
            if congress_until is not None and congress_until < congress_since:
                raise ValueError(f"AMENDMENTS_UNTIL {congress_until} precedes since {congress_since}")
            congress_until = min(congress_until or date.today(), congress_since + timedelta(days=MAX_WINDOW_DAYS))
        last_since, last_until = congress_since, congress_until
        logger.info(
            "Amendments: Congress {} — fetching amendments updated {} through {}",
            congress,
            congress_since or "the beginning",
            congress_until or "now",
        )
        url = list_route_url(
            route,
            congress=congress,
            limit=MAX_LIMIT,
            sort="updateDate desc",
            from_datetime=_instant(congress_since),
            to_datetime=_instant(congress_until, end=True),
        )
        pooled = reader.pooled(route, url, key=list_identity, version=itemgetter("updateDate"), max_pages=MAX_PAGES)
        if max_details is None:
            rows.extend(shape_amendment(_with_detail(reader, dict(record))) for record in pooled.records)
        else:
            for record in pooled.records:
                item = dict(record)
                listed_records[_amendment_key(item)] = item
        logger.info(
            "Amendments: Congress {} — {:,} amendments in window, in {} walk(s)",
            congress,
            len(pooled.records),
            pooled.passes,
        )

    if max_details is not None:
        enriched, held_stamps, unenriched_prior = (
            _held_amendment_state(prior_file, congresses) if have_prior else (set(), {}, [])
        )
        candidates: list[tuple[tuple[str, str, str], dict[str, Any], bool]] = []
        for key, record in listed_records.items():
            stamp = record.get("updateDate")
            if (*key, stamp) in enriched:
                continue
            candidates.append((key, record, True))
        for record in unenriched_prior:
            key = _amendment_key(record)
            if key not in listed_records:
                candidates.append((key, record, False))
        candidates.sort(
            key=lambda item: (str(item[1].get("updateDate") or ""), item[0]),
            reverse=True,
        )
        for position, (key, record, from_list) in enumerate(candidates):
            if position < max_details:
                rows.append(shape_amendment(_with_detail(reader, dict(record))))
            elif from_list and key not in held_stamps:
                rows.append(shape_amendment(dict(record)))

    logger.info("Amendments: {:,} rows this run", len(rows))
    if evidence is not None:
        evidence.event("amendment-selection", since=None if last_since is None else last_since.isoformat(),
                       until=None if last_until is None else last_until.isoformat(), rows=len(rows))
    return merge_contract_table(output_dir, "amendments", rows, prior_present=have_prior, download_prior=download_prior)
