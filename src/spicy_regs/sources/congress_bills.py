"""Shared Congress.gov v3 helpers: the api.data.gov key, the ``bill`` route's reader, one bill's detail.

Every Congress.gov/GovInfo consumer in this repository resolves its key through
:func:`_resolve_api_key` (the same api.data.gov key works across
regulations.gov, Congress.gov and GovInfo); the reader built by
:func:`listing_reader` sends it only as the ``X-Api-Key`` header, never a query
parameter. With no key set, callers log and publish nothing, and a base
CLI/MCP install imports this module without spicy-docs' optional
``source-readers`` extra, because :func:`listing_reader` imports it only once a
key is in hand.

**The walk is spicy-docs' job** (gap D2 / SR01,
``docs/research/closing-the-gaps-2026-09-19.md`` in spicy-docs):
:class:`spicy_docs.sources.congress.listing.CongressListingReader` walks the
publisher's continuations and refuses a repeated continuation, a declared count
that moves mid-walk, or a terminal declared/observed mismatch. A walk that
agrees with its count proves only the count — the list sorted by
``updateDate`` shifts while it is read, so one walk can repeat one record and
skip another — which is why a windowed list read goes through
``CongressListingReader.pooled`` keyed by :func:`list_identity` (the
amendments rollup is the live case). The ``sort`` encoding once sent as
``updateDate%2Bdesc`` was silently ignored by the API and froze the retired
list writer for 510 days; spicy-docs' ``bill_list_url`` sends
``sort=updateDate desc`` and bounds a window server-side.

:func:`listing_reader` is the one place this repository constructs that reader
*for the ``bill`` route*, and :func:`bill_detail` is one bill's detail record
fetched through the same instance (``capture_validated``, the single bounded,
evidenced request every spicy-docs source makes). ``build_bill_family``'s
backfill of the 82nd–107th Congresses walks ``bill/{congress}/{type}`` with the
first and builds status from the second.

The archive-wide list writer that used to live here beside these helpers
(``CongressBillsReader``, ``run-rollup-congress-bills``) was retired by plan A1
(decision 31): the bill family refreshes ``congress_bills`` for the current
Congress every day from BILLSTATUS.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_docs.sources.congress.bill_status import BillIdentity
    from spicy_docs.sources.congress.listing import CongressListingReader
    from spicy_docs.transport.captured import CapturedBodyResponse

API_BASE = "https://api.congress.gov/v3"

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

# One page fetch's request budget (including its own retries — spicy-docs
# resets the request count per page, not per walk) and pacing. Mirrors the
# budget already used by every other Congress.gov/GovInfo listing consumer in
# this repo (build_amendments, build_committee_reports, build_roll_call_votes).
_MAX_REQUESTS_PER_PAGE = 5  # measured retry bound: see fork-execution-2026-09-21/retry-measurement-2026-09-22
_MAX_PAGE_BYTES = 8 * 1024 * 1024
_TIMEOUT_SECONDS = 60.0
_MIN_REQUEST_INTERVAL_SECONDS = 0.2


def listing_reader(
    api_key: str, transport: httpx.BaseTransport | None = None, *, evidence: CaptureEvidence | None = None
) -> CongressListingReader:
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
    if evidence is not None:
        from spicy_regs.sources.retained import RetainedCongressListingReader

        return RetainedCongressListingReader(budget=budget, api_key=api_key, transport=transport, evidence=evidence)
    return CongressListingReader(budget=budget, api_key=api_key, transport=transport)


def bill_detail_url(identity: BillIdentity) -> str:
    """``bill/{congress}/{type}/{number}`` for one validated identity; the key is never part of it."""
    return f"{API_BASE}/bill/{identity.congress}/{identity.bill_type}/{identity.number}?format=json"


def bill_detail(
    reader: CongressListingReader, identity: BillIdentity
) -> tuple[Mapping[str, Any], CapturedBodyResponse]:
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


def list_identity(record: Mapping[str, Any]) -> tuple[object, str, object]:
    """A bill or amendment list record's own identity: its Congress, lowercased type and number.

    A missing part stays ``None`` (the type blank), so a pooled walk refuses the
    record instead of collapsing every such record into one identity.
    """
    return record.get("congress"), str(record.get("type") or "").lower(), record.get("number")


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
