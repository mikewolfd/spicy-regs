"""Walk one Congress.gov list route whole, keeping the declared total beside what was served.

Two routes this repository reads are complete enumerations per Congress —
``law/{congress}`` and ``committee/{congress}`` — read the same way every run:
every page to the terminal one, no window, no ``sort``. spicy-docs' reader
*refuses* at the terminal page when the publisher's declared count and the
observed total disagree, which is the right answer for a walk that may have
been cut short.

The ``committee`` route over-declares: measured 2026-09-19,
``committee/119`` declared 238 and served 236 on its one terminal page with no
continuation, and the reader refused the walk. That is not a short walk — the
publisher's count and list disagree, and only the list can be published — so
:func:`walk_route` consumes the walk and, for that one refusal alone
(spicy-docs' typed ``DeclaredCountMismatch``, read by its numbers rather than
its message), keeps both numbers on the result and returns what was served.
Every other refusal — a repeated continuation, a count that changed mid-walk,
the page bound, a ``401``/``403`` — propagates and fails the run, and the two
numbers are logged at WARNING so a run that met the over-declaration says so.

The guard is narrow in kind *and* in size: the refusal always carries the
served count, so "1 served of 238" would satisfy a kind-only predicate exactly
as 236 of 238 does; :data:`MAX_OVER_DECLARATION` bounds the shortfall to a
handful of entries — the shape of a count that includes what the list omits,
not of a page missing from the walk.

:class:`PerRunCap` sits here because both walkers bound their per-record leg
the same way: a cap that says once, at WARNING, when it stopped that leg.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from loguru import logger
from spicy_docs.reading.paged_json import DeclaredCountMismatch, PagedJsonSourceError


class ListingSource(Protocol):
    """What a Congress.gov list walk needs of a reader.

    Structural, for the same reason the bill family's acquirer seams are: a
    hermetic test serves fixture pages, and naming the concrete reader here
    would make that untypeable.
    """

    def records(self, route: Any, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


class PerRunCap:
    """A per-run request cap for one leg of a rollup, saying once when it stopped that leg."""

    def __init__(self, limit: int, label: str) -> None:
        self.remaining = limit
        self.label = label
        self.exhausted_logged = False

    def take(self) -> bool:
        """Spend one unit of the cap, logging once at WARNING when it is exhausted and returning False thereafter."""
        if self.remaining <= 0:
            if not self.exhausted_logged:
                logger.warning("{}: per-run cap reached — the next run resumes where this one stopped", self.label)
                self.exhausted_logged = True
            return False
        self.remaining -= 1
        return True


#: How far the declared total may exceed what the terminal page served before
#: the walk is read as short rather than over-declared. The measured delta is
#: 2 of 238; a handful of entries the count includes and the list omits is
#: that finding, and a page's worth (``MAX_LIMIT`` is 250) is not.
MAX_OVER_DECLARATION = 8


@dataclass(frozen=True, slots=True)
class RouteWalk:
    """Every record a whole walk served, with the publisher's declared total for the query."""

    records: tuple[Mapping[str, Any], ...]
    declared: int | None
    #: True when the terminal page was reached and the declared total exceeded
    #: what was served: the publisher's count disagreed with its own list.
    over_declared: bool


def _terminal_over_declaration(error: PagedJsonSourceError, served: int) -> bool:
    """Whether a refusal is the terminal over-declaration this walk keeps what it served for."""
    return (
        isinstance(error, DeclaredCountMismatch)
        and error.observed == served
        and 0 < error.declared - served <= MAX_OVER_DECLARATION
    )


def walk_route(reader: ListingSource, route: Any, url: str, *, max_pages: int, label: str) -> RouteWalk:
    """Every page of one list query, or a refusal — except a bounded over-declaration, which is recorded."""
    records: list[Mapping[str, Any]] = []
    declared: int | None = None
    try:
        for page in reader.records(route, url, max_pages=max_pages):
            if declared is None:
                declared = page.declared_count
            records.extend(page.records)
    except PagedJsonSourceError as error:
        if not _terminal_over_declaration(error, len(records)):
            raise
        logger.warning(
            "{}: the route declared {} records and served {:,} on its terminal page — publishing what it served",
            label,
            declared,
            len(records),
        )
        return RouteWalk(tuple(records), declared, True)
    logger.info("{}: {:,} records served, {} declared", label, len(records), declared)
    return RouteWalk(tuple(records), declared, False)
