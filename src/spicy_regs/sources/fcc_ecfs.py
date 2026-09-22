"""FCC ECFS selections through SpicyDocs' evidenced page reader.

The provider owns transport, response parsing and inclusive-day URL semantics.
The host subdivides date windows that exceed the publisher's result ceiling.
A single day still hitting that ceiling refuses: two sorted slices cannot prove
coverage when source timestamps tie. Source-confirmed empty windows are valid.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import date, timedelta
from typing import TYPE_CHECKING

import httpx

from spicy_regs.sources.base import Reader

if TYPE_CHECKING:
    from spicy_docs.sources.fcc_ecfs import FccEcfsReader

API_BASE = "https://publicapi.fcc.gov/ecfs"
API_KEY_ENV_VARS = ("API_GOV", "DATA_GOV_API_KEY", "FCC_API_KEY", "REGULATIONS_GOV_API_KEY")
PER_PAGE = 250
MAX_RESULT_WINDOW = 10_000
ECFS_EPOCH = date(1990, 1, 1)
_MAX_REQUESTS_PER_PAGE = 5


class FccEcfsError(ValueError):
    """The selected date window cannot be completely traversed."""


def _resolve_api_key() -> str | None:
    for name in API_KEY_ENV_VARS:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return None


class _EcfsReader(Reader):
    """Shared date-window walk; subclasses set the endpoint, record key, date and identity fields."""

    endpoint: str
    record_key: str
    date_field: str
    identity_field: str

    def __init__(
        self,
        *,
        since: date | None = None,
        until: date | None = None,
        api_key: str | None = None,
        per_page: int = PER_PAGE,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if isinstance(per_page, bool) or not isinstance(per_page, int) or per_page < 1:
            raise ValueError("per_page must be a positive integer")
        self.since = since or ECFS_EPOCH
        self.until = until or date.today()
        self.api_key = api_key if api_key is not None else _resolve_api_key()
        self.per_page = min(per_page, PER_PAGE)
        self.transport = transport
        self._reader: FccEcfsReader | None = None

    def _extra_params(self) -> dict[str, str]:
        return {}

    def iter_records(self) -> Iterator[dict]:
        if not self.api_key:
            raise FccEcfsError("ECFS requires an api.data.gov key")
        if self.since > self.until:
            raise FccEcfsError("ECFS selection start must not follow its end")
        try:
            from spicy_docs.reading.paged_json import PagedJsonBudget
            from spicy_docs.sources.fcc_ecfs import FccEcfsReader
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "FCC ECFS requires spicy-regs[source-readers]. Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise
        budget = PagedJsonBudget(
            max_requests=_MAX_REQUESTS_PER_PAGE,
            max_page_bytes=16 * 1024 * 1024,
            timeout_seconds=60,
            min_request_interval_seconds=0,
        )
        with FccEcfsReader(budget=budget, api_key=self.api_key, transport=self.transport) as reader:
            self._reader = reader
            try:
                yield from self._fetch_window(self.since, self.until)
            finally:
                self._reader = None

    def _fetch_window(self, gte: date, lte: date) -> Iterator[dict]:
        """Yield one window, bisecting an over-ceiling span; a single over-ceiling day refuses."""
        records, exhausted = self._page_window(gte, lte, ascending=True)
        if not exhausted:
            if gte == lte:
                raise FccEcfsError(f"ECFS {self.endpoint} reaches the result ceiling on {gte}; narrow the selection")
            mid = gte + (lte - gte) // 2
            yield from self._fetch_window(gte, mid)
            yield from self._fetch_window(mid + timedelta(days=1), lte)
            return
        yield from records

    def _page_window(self, gte: date, lte: date, *, ascending: bool) -> tuple[list[dict], bool]:
        """Page one window; return its records and whether the walk exhausted it rather than the ceiling.

        A missing or repeated identity refuses the window.
        """
        from spicy_docs.reading.paged_json import with_query
        from spicy_docs.sources.fcc_ecfs import filings_url, proceedings_url

        assert self._reader is not None
        if self.endpoint == "proceedings":
            url = proceedings_url(
                created_from=gte.isoformat(), created_to=lte.isoformat(), limit=self.per_page, descending=not ascending
            )
        else:
            url = filings_url(
                received_from=gte.isoformat(),
                received_to=lte.isoformat(),
                limit=self.per_page,
                descending=not ascending,
            )
        for key, value in self._extra_params().items():
            url = with_query(url, key, value)
        records = []
        seen = set()
        while True:
            page = self._reader.page(url, records_key=self.record_key)
            for record in page.records:
                identity = record.get(self.identity_field)
                if not isinstance(identity, (str, int)) or isinstance(identity, bool) or not str(identity).strip():
                    raise FccEcfsError(f"ECFS {self.endpoint} record omitted its {self.identity_field}")
                identity = str(identity)
                if identity in seen:
                    raise FccEcfsError(f"ECFS {self.endpoint} repeated an identity within one window")
                seen.add(identity)
                records.append(dict(record))
            if page.next_url is None:
                return records, True
            if len(records) + self.per_page > MAX_RESULT_WINDOW:
                return records, False
            url = page.next_url


class FccEcfsProceedingsReader(_EcfsReader):
    """Created-date selection; proceedings lacking dates are outside this scope."""

    endpoint = "proceedings"
    record_key = "proceeding"
    date_field = "date_proceeding_created"
    identity_field = "name"


class FccEcfsFilingsReader(_EcfsReader):
    """Received-date selection, optionally repeated for named proceedings."""

    endpoint = "filings"
    record_key = "filing"
    date_field = "date_received"
    identity_field = "id_submission"

    def __init__(
        self,
        *,
        since: date | None = None,
        until: date | None = None,
        api_key: str | None = None,
        per_page: int = PER_PAGE,
        proceedings: tuple[str, ...] = (),
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        super().__init__(since=since, until=until, api_key=api_key, per_page=per_page, transport=transport)
        self.proceedings = proceedings
        self._current_proceeding: str | None = None

    def _extra_params(self) -> dict[str, str]:
        return {"proceedings.name": self._current_proceeding} if self._current_proceeding else {}

    def iter_records(self) -> Iterator[dict]:
        if not self.proceedings:
            yield from super().iter_records()
            return
        try:
            for name in self.proceedings:
                self._current_proceeding = name
                yield from super().iter_records()
        finally:
            self._current_proceeding = None
