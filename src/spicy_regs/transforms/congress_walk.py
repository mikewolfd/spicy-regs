"""Walk one Congress.gov list route whole, keeping the declared total beside what was served.

Two routes this repository reads are complete enumerations per Congress —
``law/{congress}`` and ``committee/{congress}`` — and a complete enumeration is
read the same way every run: every page to the terminal one, no window, no
``sort``. spicy-docs' reader walks that and *refuses* at the terminal page when
the publisher's declared count and the observed total disagree, which is the
right answer for a walk that may have been cut short.

The ``committee`` route over-declares. Measured 2026-09-19 (receipt
``roster-comparison-2026-09-19/``): ``committee/119`` declared 238 and served
236 on its one terminal page with no continuation, and the reader refused the
walk. That is not a short walk — the publisher's count and the publisher's
list disagree with each other, and only the list can be published. So
:func:`walk_route` consumes the walk and, for that one refusal alone, keeps
both numbers on the result and returns what was served; every other refusal —
a repeated continuation, a count that changed mid-walk, the page bound, a
``401``/``403`` — propagates and fails the run, as the A11 backfill's list
walk does. The two numbers are logged at WARNING so a run that met the
over-declaration says so.
"""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from loguru import logger
from spicy_docs.reading.paged_json import PagedJsonSourceError


class ListingSource(Protocol):
    """What a Congress.gov list walk needs of a reader.

    Structural, for the same reason the bill family's acquirer seams are: a
    hermetic test serves fixture pages, and naming the concrete reader here
    would make that untypeable.
    """

    def records(self, route: Any, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


#: The reader attaches its traversal context under this key
#: (``spicy_docs.reading.paged_json.PagedJsonReader``'s ``context_key``).
TRAVERSAL_CONTEXT = "paged_json_acquisition"
#: The one terminal-page refusal read past; the reader's own wording.
COUNT_MISMATCH = "declared and observed record counts differ"


@dataclass(frozen=True, slots=True)
class RouteWalk:
    """Every record a whole walk served, with the publisher's declared total for the query."""

    records: tuple[Mapping[str, Any], ...]
    declared: int | None
    #: True when the terminal page was reached and the declared total exceeded
    #: what was served: the publisher's count disagreed with its own list.
    over_declared: bool


def _terminal_count_mismatch(error: PagedJsonSourceError, served: int) -> bool:
    context = error.__dict__.get(TRAVERSAL_CONTEXT)
    return (
        isinstance(context, Mapping)
        and context.get("operation") == "traversal"
        and str(error).endswith(COUNT_MISMATCH)
        and context.get("observedCount") == served
    )


def walk_route(reader: ListingSource, route: Any, url: str, *, max_pages: int, label: str) -> RouteWalk:
    """Every page of one list query, or a refusal — except the over-declaration, which is recorded."""
    records: list[Mapping[str, Any]] = []
    declared: int | None = None
    try:
        for page in reader.records(route, url, max_pages=max_pages):
            if declared is None:
                declared = page.declared_count
            records.extend(page.records)
    except PagedJsonSourceError as error:
        if not _terminal_count_mismatch(error, len(records)):
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
