"""CRS report lists through the SpicyDocs Congress.gov reader.

SpicyDocs owns requests, page parsing, declared counts and continuations. CRS
ignores the requested sort, so the source window is explicit and a locally old
row never ends the walk. Failed or incomplete walks raise before the transform
can replace its prior table. Successful empty windows remain valid.
"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from datetime import date

import httpx

from spicy_regs.sources.base import Reader

API_BASE = "https://api.congress.gov/v3"
PER_PAGE = 250
API_KEY_ENV_VARS = ("API_GOV", "DATA_GOV_API_KEY", "CONGRESS_GOV_API_KEY", "REGULATIONS_GOV_API_KEY")
_MAX_PAGES = 400
_MAX_REQUESTS_PER_PAGE = 5


class CrsReportsError(ValueError):
    """The selected CRS report list cannot be established."""


def _resolve_api_key() -> str | None:
    for var in API_KEY_ENV_VARS:
        value = os.environ.get(var, "").strip()
        if value:
            return value
    return None


class CrsReportsReader(Reader):
    """Yield raw CRS report dicts, filtering locally to ``since`` or later.

    Requires an api.data.gov key; a page omitting its declared count or carrying a missing or
    repeated ``id`` raises ``CrsReportsError``. ``per_page`` is clamped to ``PER_PAGE``.
    """

    def __init__(
        self,
        *,
        since: date | None = None,
        per_page: int = PER_PAGE,
        api_key: str | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if isinstance(per_page, bool) or not isinstance(per_page, int) or per_page < 1:
            raise ValueError("per_page must be a positive integer")
        self.since = since
        self.per_page = min(per_page, PER_PAGE)
        self.api_key = api_key if api_key is not None else _resolve_api_key()
        self.verbose = verbose
        self.transport = transport

    def iter_records(self) -> Iterator[dict]:
        if not self.api_key:
            raise CrsReportsError("CRS reports require an api.data.gov key")
        try:
            from spicy_docs.reading.paged_json import PagedJsonBudget
            from spicy_docs.sources.congress.listing import CongressListingReader, crs_report_list_url
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "CRS reports require spicy-regs[source-readers]. Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise
        url = crs_report_list_url(
            from_datetime=f"{self.since.isoformat()}T00:00:00Z" if self.since else None,
            limit=self.per_page,
        )
        budget = PagedJsonBudget(
            max_requests=_MAX_REQUESTS_PER_PAGE,
            max_page_bytes=16 * 1024 * 1024,
            timeout_seconds=60,
            min_request_interval_seconds=0,
        )
        seen = set()
        with CongressListingReader(budget=budget, api_key=self.api_key, transport=self.transport) as reader:
            for page in reader.crs_reports(url, max_pages=_MAX_PAGES):
                if page.declared_count is None:
                    raise CrsReportsError("CRS page omitted its declared count")
                for report in page.records:
                    identity = report.get("id")
                    if not isinstance(identity, str) or not identity.strip() or identity in seen:
                        raise CrsReportsError("CRS page contains a missing or repeated report identity")
                    seen.add(identity)
                    # Do not rely on the server's ignored sort parameter.
                    if self.since is None or not _older_than(report, self.since):
                        yield dict(report)


def _older_than(report: Mapping, since: date) -> bool:
    """True when ``updateDate`` parses to a date before ``since``; missing or invalid dates are not older."""
    raw = report.get("updateDate")
    if not raw:
        return False
    try:
        return date.fromisoformat(str(raw)[:10]) < since
    except ValueError:
        return False
