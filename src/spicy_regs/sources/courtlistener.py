"""CourtListener docket and opinion searches through the SpicyDocs page reader.

The provider owns requests, exact response parsing and cursor validation. Failed
or incomplete walks raise before consumers replace their prior tables. Docket
searches select APA/review-of-agency cases (nature of suit 899); opinion searches
provide cluster metadata for catch-up, not full opinion bodies. An optional
COURTLISTENER_API_TOKEN travels only in a request header.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import date
from typing import Literal

import httpx

from spicy_regs.sources.base import Reader

API_BASE = "https://www.courtlistener.com/api/rest/v4"
APA_NATURE_OF_SUIT = "899"
API_TOKEN_ENV_VAR = "COURTLISTENER_API_TOKEN"
_MAX_PAGES = 5_000
_MAX_REQUESTS_PER_PAGE = 6


class CourtListenerError(ValueError):
    """The selected CourtListener search cannot be established."""


def _resolve_api_token() -> str | None:
    return os.environ.get(API_TOKEN_ENV_VAR, "").strip() or None


class CourtListenerReader(Reader):
    """Select RECAP dockets; a record cap is an explicit prefix, not a full walk."""

    kind: Literal["r", "o"] = "r"
    identity_field = "docket_id"
    court: str | None = None

    def __init__(
        self,
        *,
        since: date | None = None,
        nature_of_suit: str = APA_NATURE_OF_SUIT,
        max_records: int | None = None,
        api_token: str | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if max_records is not None and (type(max_records) is not int or max_records < 1):
            raise ValueError("max_records must be a positive integer")
        self.since = since
        self.nature_of_suit = nature_of_suit
        self.max_records = max_records
        self.api_token = api_token if api_token is not None else _resolve_api_token()
        self.verbose = verbose
        self.transport = transport

    def iter_records(self) -> Iterator[dict]:
        try:
            from spicy_docs.reading.paged_json import PagedJsonBudget
            from spicy_docs.sources.courtlistener.search import CourtListenerSearchReader, search_url
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "CourtListener search requires spicy-regs[source-readers]. "
                    "Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise
        url = search_url(
            kind=self.kind,
            filed_after=self.since.isoformat() if self.since else None,
            court=self.court,
            nature_of_suit=self.nature_of_suit if self.kind == "r" else None,
        )
        budget = PagedJsonBudget(
            max_requests=_MAX_REQUESTS_PER_PAGE,
            max_page_bytes=16 * 1024 * 1024,
            timeout_seconds=60,
            min_request_interval_seconds=0,
        )
        seen = set()
        yielded = 0
        with CourtListenerSearchReader(budget=budget, api_key=self.api_token, transport=self.transport) as reader:
            for page in reader.search(url, max_pages=_MAX_PAGES):
                if page.declared_count is None:
                    raise CourtListenerError("CourtListener search omitted its declared count")
                # Validate the whole received page even when a record cap stops
                # midway through it; malformed rows are not successful selection.
                records = []
                for record in page.records:
                    identity = record.get(self.identity_field)
                    if type(identity) is not int or identity <= 0 or identity in seen:
                        raise CourtListenerError("CourtListener search has a missing, invalid or repeated identity")
                    seen.add(identity)
                    records.append(dict(record))
                if page.next_url is None and len(seen) != page.declared_count:
                    raise CourtListenerError("CourtListener terminal page disagrees with its declared count")
                if page.next_url is not None and not records:
                    raise CourtListenerError("CourtListener continuation follows an empty page")
                for record in records:
                    yield record
                    yielded += 1
                    if self.max_records is not None and yielded >= self.max_records:
                        return


class CourtListenerOpinionSearchReader(CourtListenerReader):
    """Select opinion cluster metadata; full text remains a bulk-source task."""

    kind: Literal["r", "o"] = "o"
    identity_field = "cluster_id"

    def __init__(
        self,
        *,
        since: date | None = None,
        court: str | None = None,
        max_records: int | None = None,
        api_token: str | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        super().__init__(
            since=since, max_records=max_records, api_token=api_token, verbose=verbose, transport=transport
        )
        self.court = court
