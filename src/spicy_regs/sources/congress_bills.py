"""Reader connector for the Congress.gov public REST API (v3).

Brings congressional bill ingestion *in-repo* as an external source complementary
to the regulations.gov ``dockets``/``documents`` view — the legislative record
that sits upstream of the rulemakings this dataset tracks.

The reader is a *pure source*: it yields raw bill payloads (dicts) exactly as the
list endpoint returns them. Shaping them into the published ten-column schema is
the job of
:func:`~spicy_regs.transforms.build_congress_bills.build_congress_bills`.

**The walk is spicy-docs' job now (gap D2 / SR01,
``docs/research/closing-the-gaps-2026-09-19.md`` in spicy-docs).** Pagination,
the declared-vs-observed count check, and the ``sort``/window wire encoding
used to be a hand-rolled ``offset``/``limit`` loop in this module — see this
file's git history for that version. It is retired in favour of
:class:`spicy_docs.sources.congress.listing.CongressListingReader` over the
``bill`` route (``docs/sources/listings.md`` in spicy-docs; ``sort_honored=True``
there is a 2026-09-19 measurement, not an assumption). That reader walks the
publisher's continuations to its terminal page and *refuses* — raises, not
logs — if a page repeats its continuation, if the declared count changes
mid-walk, or if the declared and observed totals disagree at the end. This
module keeps only what stays this repo's own job: the fetch window
(``since``/``until``, computed by the caller from the prior table's watermark,
an overlap, and
:data:`~spicy_regs.transforms.build_congress_bills.MAX_WINDOW_DAYS`), and
resolving the api.data.gov key from this repo's own env fallback chain
(:func:`_resolve_api_key`) — handed to the spicy-docs reader, which sends it
only as a request header (``X-Api-Key``), never a query parameter.

**Why the window used to matter so much here.** Congress.gov's ``sort``
parameter was once sent as ``updateDate%2Bdesc`` — the API accepts that and
answers ``200``, but silently ignores it, so rows arrived in arbitrary order.
A client-side "stop at the first out-of-window row" check took the second row
of the first page as the watermark and froze this table for 510 days,
"succeeding" at publishing one bill on every run of that freeze without ever
failing loudly. spicy-docs' ``bill_list_url`` sends the correct
``sort=updateDate desc`` (a literal space, not a plus) and bounds the window
server-side with ``fromDateTime``/``toDateTime``; the window is still this
repo's to compute, but no longer this repo's to encode onto the wire or to
police for completeness — that is exactly the refusal spicy-docs' walk now
does on our behalf.

**Refuse-and-retry, not warn-and-publish.** This reader's refusal is
absolute and stays that way: it never weakens the check above, never
retries internally, and never publishes a walk it could not complete in
full — a window asked for and not fully received must never look like a
completed run. But that refusal has an operational cost this reader cannot
see or absorb on its own: a nightly window closes at "now," a walk over it
takes minutes, and a bill the publisher edits *during* that walk can move
its own ``updateDate`` past ``toDateTime`` and shrink the declared count out
from under a request already in flight — a transient publisher-side race,
not a truncated or out-of-order walk. Retrying the identical window is the
caller's job, not this reader's:
:func:`~spicy_regs.transforms.build_congress_bills._fetch_bills` asks a
fresh reader for the same window a few times, with a short pause, before
giving up. A refusal that survives every retry propagates unchanged and
fails the run loudly; it means the window was asked for and, after every
attempt, still not fully received — the run publishes nothing, and the next
scheduled run tries again from the same watermark.

**Shared with the bill family's pre-BILLSTATUS backfill.** :func:`listing_reader`
is the one place this repository constructs spicy-docs' reader *for the
``bill`` route* — the budget, the header-only key and the lazy import (the
amendments and roll-call transforms construct the same class for their own
routes) — and :func:`bill_detail` is one bill's detail record fetched through
that same reader instance (``capture_validated``, the primitive every
spicy-docs source makes its single bounded, evidenced request with).
``build_bill_family``'s backfill of the 82nd–107th Congresses walks
``bill/{congress}/{type}`` with the first and builds status from the second;
neither is a second walk implementation of the route.

**API key.** Congress.gov requires an api.data.gov key. The same key works
across regulations.gov, Congress.gov, and GovInfo, so we resolve it from a
fallback chain of the env vars this repo already uses (:func:`_resolve_api_key`
— shared with :mod:`spicy_regs.sources.bill_subjects` and every other
Congress.gov/GovInfo consumer here). If no key is set the reader logs a clear
warning and yields nothing — a keyless CI run is a no-op, not a crash, and
does not require spicy-docs' optional ``source-readers`` extra to even be
installed, since the import happens only once a key is in hand.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from datetime import date
from typing import TYPE_CHECKING, Any

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

if TYPE_CHECKING:
    from spicy_docs.sources.congress.bill_status import BillIdentity
    from spicy_docs.sources.congress.listing import CongressListingReader
    from spicy_docs.transport.captured import CapturedBodyResponse

API_BASE = "https://api.congress.gov/v3"

# The API wants a full RFC3339 instant.
_FROM_DATETIME_FMT = "%Y-%m-%dT00:00:00Z"
_TO_DATETIME_FMT = "%Y-%m-%dT00:00:00Z"

# Env vars checked in order for the api.data.gov key (one key works across
# regulations.gov, Congress.gov, and GovInfo). Shared by every Congress.gov/
# GovInfo consumer in this repo: bill_subjects, build_bill_family,
# build_amendments, build_committee_reports, build_roll_call_votes.
API_KEY_ENV_VARS = (
    "API_GOV",  # the shared api.data.gov key, under the name RefSpec/.env uses
    "DATA_GOV_API_KEY",
    "CONGRESS_GOV_API_KEY",
    "REGULATIONS_GOV_API_KEY",
)

_PROGRESS_EVERY = 5_000

# Backstop against a runaway loop, not an expected limit: at spicy-docs'
# MAX_LIMIT (250 rows/page) this clears a full-archive backfill — ~430k bills
# as of 2026-08, and the old offset walk this replaces paged fine past 238k —
# with ample headroom for growth. Hitting it raises a PagedJsonSourceError
# from the spicy-docs reader; it is never a quiet stop.
_MAX_PAGES = 2_000

# One page fetch's request budget (including its own retries — spicy-docs
# resets the request count per page, not per walk) and pacing. Mirrors the
# budget already used by every other Congress.gov/GovInfo listing consumer in
# this repo (build_amendments, build_committee_reports, build_roll_call_votes).
_MAX_REQUESTS_PER_PAGE = 500
_MAX_PAGE_BYTES = 8 * 1024 * 1024
_TIMEOUT_SECONDS = 60.0
_MIN_REQUEST_INTERVAL_SECONDS = 0.2


def listing_reader(api_key: str, transport: httpx.BaseTransport | None = None) -> CongressListingReader:
    """spicy-docs' reader over the Congress.gov list routes, with this repo's page budget and header-only key.

    Imported lazily on purpose: base CLI/MCP installations import this module
    for ``API_BASE``/``API_KEY_ENV_VARS``/``_resolve_api_key`` without the
    optional owner wheel, and only a caller with a key in hand ever gets here.
    The budget's ``max_requests`` is a *per-request* bound — the reader resets
    it on every ``capture_validated`` call, so it caps one page's or one
    detail's attempts including retries, never a walk — which is why no caller
    widens it for a longer walk.
    """
    try:
        from spicy_docs.reading.paged_json import PagedJsonBudget
        from spicy_docs.sources.congress.listing import CongressListingReader
    except ModuleNotFoundError as error:
        if error.name == "spicy_docs":
            raise RuntimeError(
                "Congress bills require spicy-regs[source-readers]. Run `uv sync --frozen` in a SpicyRegs checkout."
            ) from None
        raise
    budget = PagedJsonBudget(
        max_requests=_MAX_REQUESTS_PER_PAGE,
        max_page_bytes=_MAX_PAGE_BYTES,
        timeout_seconds=_TIMEOUT_SECONDS,
        min_request_interval_seconds=_MIN_REQUEST_INTERVAL_SECONDS,
    )
    return CongressListingReader(budget=budget, api_key=api_key, transport=transport)


def bill_detail_url(identity: BillIdentity) -> str:
    """``bill/{congress}/{type}/{number}`` for one validated identity; the key is never part of it."""
    return f"{API_BASE}/bill/{identity.congress}/{identity.bill_type}/{identity.number}?format=json"


def bill_detail(reader: CongressListingReader, identity: BillIdentity) -> tuple[Mapping[str, Any], CapturedBodyResponse]:
    """One bill's detail record, as the publisher states it, proven to be the bill asked for.

    One ``capture_validated`` call on the same reader the list walk uses, so
    the request is bounded, paced and evidenced the way every page is, and the
    key travels as the same header. The response must be a JSON object whose
    ``bill`` names this identity's Congress, type and number — a ``200`` for
    a different bill, or an empty object, is refused rather than shaped, since
    an empty success is not absence. Numbers are read the way the reader reads
    them (decimal, not float), so the record compares equal to a list page's.
    """
    from typing import cast

    from spicy_docs.reading.json_input import load_decimal_json
    from spicy_docs.reading.paged_json import PagedJsonSourceError, PagedJsonUnavailableError

    url = bill_detail_url(identity)

    def read(capture: CapturedBodyResponse, _limit: int) -> Mapping[str, Any]:
        # ``load_decimal_json`` is typed ``object``; the cast states the shape
        # the very next line proves rather than leaving it to inference.
        document = cast(
            Mapping[str, Any],
            load_decimal_json(capture.body, source=reader.family.label, error_type=PagedJsonSourceError),
        )
        bill = document.get("bill") if isinstance(document, Mapping) else None
        if not isinstance(bill, Mapping):
            raise PagedJsonSourceError(f"{reader.family.label} bill detail response omitted its bill object")
        stated = (str(bill.get("congress")), str(bill.get("type") or "").lower(), str(bill.get("number")))
        if stated != (str(identity.congress), identity.bill_type, str(identity.number)):
            raise PagedJsonSourceError(f"{reader.family.label} bill detail identity differs from the requested bill")
        return bill

    bill, capture = reader.capture_validated(
        url,
        media_types=reader.family.media_types,
        parse=read,
        max_bytes=_MAX_PAGE_BYTES,
        unavailable=PagedJsonUnavailableError,
        context={"operation": "bill-detail", "family": reader.family.name, "url": url},
    )
    return bill, capture


def _resolve_api_key() -> str | None:
    """Return the first api.data.gov key set in :data:`API_KEY_ENV_VARS`, or None.

    The same api.data.gov key is valid across regulations.gov, Congress.gov, and
    GovInfo, so we accept whichever the environment already provides.
    """
    for var in API_KEY_ENV_VARS:
        value = os.environ.get(var)
        if value:
            return value
    return None


class CongressBillsReader(Reader):
    """Yields raw Congress.gov bill dicts, newest ``updateDate`` first.

    ``since``/``until`` become the ``fromDateTime``/``toDateTime`` bounds on the
    request, so the server decides what is in the window. The walk itself —
    pagination, retries, and refusing an inconsistent or incomplete traversal —
    is :class:`spicy_docs.sources.congress.listing.CongressListingReader`'s job.
    With no key configured the reader yields nothing.
    """

    def __init__(
        self,
        *,
        since: date | None = None,
        until: date | None = None,
        api_key: str | None = None,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.since = since
        self.until = until
        self.api_key = api_key or _resolve_api_key()
        self.transport = transport

    def iter_records(self) -> Iterator[dict]:
        if not self.api_key:
            logger.warning(
                "Congress bills: no API key found (set one of {}) — yielding nothing",
                ", ".join(API_KEY_ENV_VARS),
            )
            return
        logger.info(
            "Congress bills: fetching bills updated {} through {}",
            self.since or "the beginning",
            self.until or "now",
        )
        # Base CLI/MCP installations can import source names (and this whole
        # module — API_BASE/API_KEY_ENV_VARS/_resolve_api_key are shared by
        # other, keyless-safe callers) without the optional owner wheel.
        # Actually walking the API requires the source-readers extra, which
        # listing_reader imports only now, with a key in hand.
        reader = listing_reader(self.api_key, self.transport)
        from spicy_docs.sources.congress.listing import bill_list_url

        url = bill_list_url(
            from_datetime=self.since.strftime(_FROM_DATETIME_FMT) if self.since else None,
            to_datetime=self.until.strftime(_TO_DATETIME_FMT) if self.until else None,
        )
        seen = 0
        with reader:
            for page in reader.bills(url, max_pages=_MAX_PAGES):
                for bill in page.records:
                    seen += 1
                    if seen % _PROGRESS_EVERY == 0:
                        logger.info("Congress bills: {:,} bills so far...", seen)
                    # page.records is typed Mapping[str, Any] (spicy-docs reads
                    # every row that way); this reader's contract is dict, same
                    # as every raw payload build_congress_bills._shape() reads.
                    yield dict(bill)
        logger.info("Congress bills: yielded {:,} bills", seen)
