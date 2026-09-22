"""Scoping shared by the Congress-sourced transforms: which Congresses, which types.

The bill family, amendments and roll-call votes are all bounded by "which
Congress(es), which bill types" rather than by a date window, because their
sources are addressed that way — the BILLSTATUS bulk archive is one zip per
``(congress, bill_type)``, and the House vote route's path is
``house-vote/{congress}/{session}``. Stating the rule once here keeps three
transforms from each carrying their own copy of it.

``uv run`` reads the workflow inputs from the environment
(``BILL_FAMILY_CONGRESSES``, ``BILL_FAMILY_BILL_TYPES``); an unset or blank
value means "the default", never "none", so a cron with no inputs still runs.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

#: Congress 1 convened in 1789 and each runs two calendar years, session 1 in
#: the odd year. This is the inverse of the rule ``spicy_docs.sources.congress
#: .votes`` states for building Clerk URLs (``_FIRST_SESSION_YEAR``); it is
#: private there, so it is restated — once — rather than reached into.
FIRST_CONGRESS_YEAR = 1789

#: How long after a Congress boundary the outgoing Congress stays in the
#: default scope. A Congress convenes 3 January of an odd year, so this window
#: carries the outgoing Congress through mid-February. Chosen by the plan's
#: "first weeks of a new Congress" instruction rather than by a measurement of
#: the publisher's correction lag; a correction-lag measurement would replace
#: this placeholder.
CONGRESS_BOUNDARY_OVERLAP_DAYS = 45

#: Every bill and resolution type the BILLSTATUS bulk archive publishes a
#: folder for. The default scope is all eight.
DEFAULT_BILL_TYPES: tuple[str, ...] = ("hr", "s", "hjres", "sjres", "hres", "sres", "hconres", "sconres")


def current_congress(today: date | None = None) -> int:
    """The Congress sitting on ``today`` (default: the system date).

    A Congress convenes on 3 January of an odd year, so the first two days of
    an odd year still belong to the previous one. Getting that wrong would
    point a January run at a Congress with no bills in it yet.
    """
    day = today or date.today()
    year = day.year if (day.year % 2 == 1 and day >= date(day.year, 1, 3)) or day.year % 2 == 0 else day.year - 1
    return (year - FIRST_CONGRESS_YEAR) // 2 + 1


def session_of(congress: int, year: int) -> int:
    """Which session of ``congress`` falls in ``year`` (1 for the odd year, 2 for the even)."""
    first_year = FIRST_CONGRESS_YEAR + 2 * (congress - 1)
    session = year - first_year + 1
    if session not in (1, 2):
        raise ValueError(f"year {year} is not in Congress {congress} ({first_year}-{first_year + 1})")
    return session


def default_congresses(today: date | None = None) -> tuple[int, ...]:
    """The default Congress scope: current, plus the outgoing one across a boundary.

    From 3 January of an odd year the outgoing Congress drops out of
    ``current_congress``, so a late correction to a December record would
    never be re-read. While ``today`` sits in the first
    ``CONGRESS_BOUNDARY_OVERLAP_DAYS`` after the boundary, the default names
    both Congresses; after the window it names only the current one.
    """
    day = today or date.today()
    congress = current_congress(day)
    convenes = date(FIRST_CONGRESS_YEAR + 2 * (congress - 1), 1, 3)
    if congress > 1 and day < convenes + timedelta(days=CONGRESS_BOUNDARY_OVERLAP_DAYS):
        return (congress, congress - 1)
    return (congress,)


def congresses_from_env(var: str = "BILL_FAMILY_CONGRESSES", *, today: date | None = None) -> tuple[int, ...]:
    """Congress numbers from a comma-separated env var, defaulting to :func:`default_congresses`."""
    raw = os.environ.get(var, "").strip()
    if not raw:
        return default_congresses(today)
    congresses = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            congresses.append(int(part))
        except ValueError as exc:
            raise ValueError(f"{var} must be comma-separated Congress numbers, got {part!r}") from exc
    if not congresses:
        return default_congresses(today)
    return tuple(congresses)


def bill_types_from_env(var: str = "BILL_FAMILY_BILL_TYPES") -> tuple[str, ...]:
    """Bill types from a comma-separated env var, defaulting to all eight."""
    raw = os.environ.get(var, "").strip()
    if not raw:
        return DEFAULT_BILL_TYPES
    types = tuple(part.strip().lower() for part in raw.split(",") if part.strip())
    return types or DEFAULT_BILL_TYPES


def sessions_of(congress: int, today: date | None = None) -> tuple[int, ...]:
    """The sessions of ``congress`` that have begun by ``today``.

    A route addressed at a session that has not convened answers nothing, so
    asking only for the sessions that exist keeps an empty walk from reading
    like a source outage.
    """
    day = today or date.today()
    first_year = FIRST_CONGRESS_YEAR + 2 * (congress - 1)
    return tuple(session for session in (1, 2) if day.year >= first_year + session - 1)


#: The daily Congressional Record numbers one volume per calendar year, and
#: the API addresses it by that volume: ``volume = year - 1854``, pinned by
#: volume 172 being 2026 (spicy-docs fixture
#: ``congress-daily-congressional-record-detail.json``, issue 172/148 dated
#: 2026-09-18) and by volume 141 being 1995, the first year the API serves.
#: The rule does not hold for the 19th-century volumes, which ran several to
#: a Congress; nothing here scopes a Congress that old.
RECORD_VOLUME_OFFSET = 1854


def record_volumes(congress: int, today: date | None = None) -> tuple[int, ...]:
    """The Record volumes of ``congress``'s sessions that have begun by ``today``, oldest first."""
    first_year = FIRST_CONGRESS_YEAR + 2 * (congress - 1)
    return tuple(first_year + session - 1 - RECORD_VOLUME_OFFSET for session in sessions_of(congress, today))
