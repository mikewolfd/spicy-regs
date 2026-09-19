"""Transform: build ``committees`` and ``committee_assignments``.

Two tables from three publishers, in one pass because the second is keyed on
what the first enumerates:

* **``committees``** — the Congress.gov ``committee/{congress}`` list route,
  walked whole per scoped Congress with no ``sort`` (its ``sort_honored`` is
  carried from the data map, not probed, so nothing here relies on order),
  one row per ``systemCode``; the ``committee/{chamber}/{system_code}``
  detail record — history, subcommittees, parent, currency and that day's
  counts — is folded onto the row where captured, one keyed request per
  committee under the per-run cap. ``shape_committee`` refuses a detail whose
  ``systemCode`` is not the row's, so a fold never lands on the wrong row.
* **``committee_assignments``** — who sits where *today*, from the House
  Clerk's ``MemberData.xml`` and the Senate's ``cvc_member_data.xml``: two
  keyless requests, for the current Congress only, because that is the only
  Congress the files describe. The House file states its Congress and the
  reader proves it against the request; the Senate file states none, so its
  rows carry the caller's Congress with ``congress_basis = caller``. A
  vacancy has no member and no row; a seated member whose only assignment is
  the file's ``<committee rank=""/>`` placeholder has none either.

**The route over-declares.** Measured 2026-09-19, ``committee/119`` declared
238 and served 236 on its one terminal page with no continuation, and
spicy-docs' reader refused the walk. :func:`walk_route` reads past that one
terminal-page refusal — both numbers logged, what was served published — and
lets every other refusal fail the run.

**Incremental.** The list is one page and is re-walked whole every run; the
detail is what is not re-read. Per listed committee: a row already published
with its detail and the same list ``update_date`` is left standing (no
request, no fresh row); one not yet published, or published without a
detail, or whose list row moved, is asked for, newest ``update_date`` first,
under :data:`MAX_DETAILS_PER_RUN`. When the cap or a refusal stops the ask, a
committee not yet published gets its list row with ``detail_captured =
false``, and one already published keeps its prior row until the next run
reaches it — a fresh list-only row must never overwrite a held detail,
because the merge replaces rows whole. A ``401``/``403`` aborts the run.

The assignment tables are snapshots: each file captured this run replaces
every prior row for its chamber and Congress (:func:`retire_prior_rows`), so
a seat the file no longer lists is gone on the next capture, and an earlier
Congress keeps its last capture. A chamber whose file was not established
this run — a transport failure or a refused body — keeps its prior rows; a
keyless ``401``/``403`` (``CommitteeRosterRefusedError``) aborts.

Needs an api.data.gov key for the two Congress.gov routes; the chamber files
are keyless.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, NamedTuple, Protocol

import httpx
from loguru import logger
from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonSourceError
from spicy_docs.schemas.roster_tables import shape_committee, shape_house_assignment, shape_senate_assignment
from spicy_docs.schemas.tables import TableContractError, text
from spicy_docs.sources.congress.committee_rosters import (
    CommitteeRosterAcquirer,
    CommitteeRosterBudget,
    CommitteeRosterError,
    CommitteeRosterRefusedError,
)
from spicy_docs.sources.congress.listing import LIST_ROUTES, MAX_LIMIT, CongressListingReader, list_route_url
from spicy_docs.transport.credentials import scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import congresses_from_env, current_congress
from spicy_regs.transforms.congress_walk import ListingSource, walk_route
from spicy_regs.transforms.table_merge import merge_contract_table, published_table, retire_prior_rows


class RosterSource(Protocol):
    """What this transform needs of the chamber-file acquirer."""

    def acquire_house(self, *, congress: int, session: int | None = ..., max_bytes: int | None = ...) -> Any: ...

    def acquire_senate(self, *, max_bytes: int | None = ...) -> Any: ...


LIST_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Two requests, paced; the House file is ~557 KB, the Senate file ~68 KB
#: (measured 2026-09-19), well under the acquirer's own per-file defaults.
ROSTER_BUDGET = CommitteeRosterBudget(
    max_requests=4,
    max_bytes=4 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=1.0,
)

#: Pages of ``MAX_LIMIT`` per Congress; the 119th's 236 committees fit in one.
MAX_PAGES = 10

#: Detail records asked for per run: a whole Congress in one run (~1 minute
#: at the pacing above), bounding a multi-Congress backfill.
MAX_DETAILS_PER_RUN = 300

NAME = "committees"
ASSIGNMENTS = "committee_assignments"
TRUE = "true"


class HeldCommittee(NamedTuple):
    update_date: str | None
    detail_captured: str | None


def _held_committees(prior_file: Path | None) -> dict[str, HeldCommittee]:
    if prior_file is None:
        return {}
    import duckdb

    rows = duckdb.sql(f"SELECT system_code, update_date, detail_captured FROM read_parquet('{prior_file}')").fetchall()
    return {str(code): HeldCommittee(update_date, captured) for code, update_date, captured in rows}


def _list_committees(reader: ListingSource, congresses: tuple[int, ...]) -> list[Mapping[str, Any]]:
    """Every committee the route lists for the scoped Congresses, one record per ``systemCode``, newest first.

    A committee sits in every Congress it existed in, so a multi-Congress
    scope lists the same code more than once; the record with the larger
    ``updateDate`` is the one kept, which is the row the merge would keep too.
    """
    route = LIST_ROUTES["committee"]
    by_code: dict[str, Mapping[str, Any]] = {}
    for congress in congresses:
        walk = walk_route(
            reader,
            route,
            list_route_url(route, congress=congress, limit=MAX_LIMIT),
            max_pages=MAX_PAGES,
            label=f"Committees: Congress {congress}",
        )
        for record in walk.records:
            code = text(record.get("systemCode"))
            if not code:
                logger.warning("Committees: a list record states no systemCode; skipped")
                continue
            held = by_code.get(code)
            if held is None or (text(record.get("updateDate")) or "") > (text(held.get("updateDate")) or ""):
                by_code[code] = record
    return sorted(by_code.values(), key=lambda record: text(record.get("updateDate")) or "", reverse=True)


def _detail(reader: ListingSource, record: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """One committee's detail record, or ``None`` when the route did not establish one.

    A ``401``/``403`` propagates and aborts the run; a refusal by the reader
    or a transport failure is this committee's gap, retried next run.
    """
    route = LIST_ROUTES["committee-detail"]
    code = str(record.get("systemCode"))
    try:
        url = list_route_url(route, chamber=str(record.get("chamber") or "").lower(), system_code=code)
        for page in reader.records(route, url, max_pages=1):
            for detail in page.records:
                return detail
    except (PagedJsonSourceError, httpx.HTTPError, ConnectionError) as error:
        logger.warning("Committees: detail for {} not established: {}", code, scrub_credential(str(error), ""))
    return None


def _committee_rows(
    listed: list[Mapping[str, Any]], held: Mapping[str, HeldCommittee], reader: ListingSource, max_details: int
) -> list[dict]:
    rows: list[dict] = []
    unchanged = deferred = folded = 0
    remaining = max_details
    for record in listed:
        code = str(record.get("systemCode"))
        update_date = text(record.get("updateDate"))
        prior = held.get(code)
        if prior is not None and prior.detail_captured == TRUE and prior.update_date == update_date:
            unchanged += 1
            continue
        detail = None
        if remaining > 0:
            remaining -= 1
            detail = _detail(reader, record)
        elif remaining == 0:
            logger.warning("Committees: per-run detail cap reached — the next run resumes where this one stopped")
            remaining = -1
        try:
            row = shape_committee(record, detail) if detail is not None else None
        except TableContractError as error:
            logger.warning("Committees: detail for {} refused by the contract: {}", code, error)
            row = None
        if row is None:
            if prior is not None:
                # The prior row — its detail included — stands until a run reaches this committee again.
                deferred += 1
                continue
            try:
                row = shape_committee(record)
            except TableContractError as error:
                logger.warning("Committees: list record {} refused by the contract: {}", code, error)
                continue
        else:
            folded += 1
        rows.append(row)
    logger.info(
        "Committees: {:,} listed — {:,} rows this run ({:,} with a detail), {:,} already folded and unchanged, {:,} held over",
        len(listed),
        len(rows),
        folded,
        unchanged,
        deferred,
    )
    return rows


def _assignment_rows(rosters: RosterSource, congress: int, prior_file: Path | None) -> list[dict]:
    """Today's seats from both chamber files; a file not established this run leaves its chamber's prior rows."""
    rows: list[dict] = []
    try:
        house = rosters.acquire_house(congress=congress)
    except CommitteeRosterRefusedError:
        raise
    except (CommitteeRosterError, httpx.HTTPError, ConnectionError) as error:
        logger.warning("Committee assignments: House file not established: {}", scrub_credential(str(error), ""))
    else:
        roster = house.roster
        observed_at = house.capture.observed_at
        placeholders = sum(member.placeholder_assignments for member in roster.members)
        kinds: Counter[str] = Counter()
        for member in roster.members:
            if member.vacant:
                continue
            for assignment in member.assignments:
                kinds[assignment.kind] += 1
                rows.append(shape_house_assignment(member, assignment, roster=roster, observed_at=observed_at))
        if prior_file is not None:
            retire_prior_rows(prior_file, congress=str(congress), chamber="house")
        logger.info(
            "Committee assignments: House file states Congress {} session {}, published {} — {:,} seats, {:,} vacant,"
            " {:,} assignments {}, {:,} placeholders",
            roster.congress,
            roster.session,
            roster.publish_date,
            len(roster.members),
            roster.vacancies,
            sum(kinds.values()),
            dict(kinds),
            placeholders,
        )
    try:
        senate = rosters.acquire_senate()
    except CommitteeRosterRefusedError:
        raise
    except (CommitteeRosterError, httpx.HTTPError, ConnectionError) as error:
        logger.warning("Committee assignments: Senate file not established: {}", scrub_credential(str(error), ""))
    else:
        roster = senate.roster
        observed_at = senate.capture.observed_at
        before = len(rows)
        for senator in roster.senators:
            for assignment in senator.committees:
                rows.append(
                    shape_senate_assignment(
                        senator, assignment, congress=congress, roster=roster, observed_at=observed_at
                    )
                )
        if prior_file is not None:
            retire_prior_rows(prior_file, congress=str(congress), chamber="senate")
        logger.info(
            "Committee assignments: Senate file updated {} (states no Congress; rows carry {} from the caller) — {:,} senators, {:,} seats",
            roster.last_update_date,
            congress,
            len(roster.senators),
            len(rows) - before,
        )
    return rows


def build_committee_rosters(
    output_dir: Path,
    *,
    reader: ListingSource | None = None,
    rosters: RosterSource | None = None,
    max_details: int = MAX_DETAILS_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> tuple[Path, Path]:
    """Build ``committees.parquet`` and ``committee_assignments.parquet``."""
    if reader is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Committees need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = CongressListingReader(budget=LIST_BUDGET, api_key=api_key)
    rosters = rosters or CommitteeRosterAcquirer(budget=ROSTER_BUDGET)

    priors = {name: published_table(output_dir, name, download_prior) for name in (NAME, ASSIGNMENTS)}

    # 1. The enumeration, whole, then the detail fold under its cap.
    listed = _list_committees(reader, congresses_from_env())
    committee_rows = _committee_rows(listed, _held_committees(priors[NAME]), reader, max_details)

    # 2. Today's seats, for the Congress the files describe.
    assignment_rows = _assignment_rows(rosters, current_congress(), priors[ASSIGNMENTS])

    return (
        merge_contract_table(output_dir, NAME, committee_rows, prior_present=priors[NAME] is not None),
        merge_contract_table(output_dir, ASSIGNMENTS, assignment_rows, prior_present=priors[ASSIGNMENTS] is not None),
    )
