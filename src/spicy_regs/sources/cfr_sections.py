"""GovInfo annual CFR granule listings through SpicyDocs' bounded reader.

SpicyRegs owns the selected year window and table mapping. SpicyDocs owns the
GovInfo locators, HTTP capture, header-only credential, opaque continuations,
retry bounds and terminal count checks. Missing credentials and failed or
incomplete listings raise before the builder can replace its prior output.
An explicit zero-count terminal listing is a valid empty selection.

These are list-level metadata, not section bodies or native part ancestry.
A section identifier's numeric prefix does not establish its enclosing part.
"""

from __future__ import annotations

import datetime as dt
import os
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING

import httpx
from loguru import logger

from spicy_regs.sources.base import Reader

if TYPE_CHECKING:
    from spicy_docs.reading.paged_json import JsonPage
    from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader

API_BASE = "https://api.govinfo.gov"
BULKDATA_BASE = "https://www.govinfo.gov/bulkdata/CFR"
COLLECTION = "CFR"
PAGE_SIZE = 100
API_KEY_ENV_VARS = ("API_GOV", "DATA_GOV_API_KEY", "GOVINFO_API_KEY", "REGULATIONS_GOV_API_KEY")
_MAX_PAGES = 2_000
_MAX_REQUESTS_PER_PAGE = 5
_MAX_PAGE_BYTES = 8 * 1024 * 1024
_PROGRESS_EVERY = 5_000


class CfrSectionsError(ValueError):
    """The selected CFR listing cannot establish complete, identified rows."""


def _resolve_api_key() -> str | None:
    """Return the first nonempty configured api.data.gov key."""
    for name in API_KEY_ENV_VARS:
        value = os.environ.get(name)
        if value and value.strip():
            return value.strip()
    return None


def _identity(record: Mapping, field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise CfrSectionsError(f"CFR row requires a nonempty {field}")
    return value


def _identified_rows(pages: Iterator[JsonPage], field: str) -> Iterator[dict]:
    """Require counted listings and distinct source identities, without dropping rows."""
    seen = set()
    for page in pages:
        if page.declared_count is None:
            raise CfrSectionsError("CFR listing omitted its declared count")
        for record in page.records:
            identity = _identity(record, field)
            if identity in seen:
                raise CfrSectionsError(f"CFR listing repeats a {field}")
            seen.add(identity)
            yield dict(record)


class CfrSectionsReader(Reader):
    """Yield raw annual CFR granules and their enclosing package's source facts.

    The default selection is last year through this year. A caller can select
    a wider explicit window; completing that window says nothing about earlier
    editions. ``transport`` permits hermetic replay through the owner reader.
    """

    def __init__(
        self,
        *,
        since_year: int | None = None,
        until_year: int | None = None,
        api_key: str | None = None,
        page_size: int = PAGE_SIZE,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        current_year = dt.date.today().year
        self.until_year = until_year if until_year is not None else current_year
        self.since_year = since_year if since_year is not None else current_year - 1
        for year in (self.since_year, self.until_year):
            if isinstance(year, bool) or not isinstance(year, int) or not 1 <= year <= 9999:
                raise CfrSectionsError("CFR selection years must be integers from 1 through 9999")
        if self.since_year > self.until_year:
            raise CfrSectionsError("CFR selection ends before it starts")
        self.api_key = api_key if api_key is not None else _resolve_api_key()
        self.page_size = page_size
        self.verbose = verbose
        self.transport = transport
        self._source: GovInfoDiscoveryReader | None = None
        self._seen = 0

    def iter_records(self) -> Iterator[dict]:
        if not isinstance(self.api_key, str) or not self.api_key.strip():
            raise CfrSectionsError("CFR requires an api.data.gov key; set DATA_GOV_API_KEY")
        from spicy_docs.reading.paged_json import PagedJsonBudget
        from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader

        budget = PagedJsonBudget(
            max_requests=_MAX_REQUESTS_PER_PAGE,
            max_page_bytes=_MAX_PAGE_BYTES,
            timeout_seconds=60.0,
            min_request_interval_seconds=0.2,
        )
        self._seen = 0
        logger.info("CFR: fetching {} granules (editions {}..{})", COLLECTION, self.since_year, self.until_year)
        with GovInfoDiscoveryReader(budget=budget, api_key=self.api_key, transport=self.transport) as source:
            self._source = source
            try:
                for package in self._iter_packages():
                    yield from self._iter_granules(package)
            finally:
                self._source = None
        logger.info("CFR: yielded {:,} granules", self._seen)

    def _iter_packages(self) -> Iterator[dict]:
        from spicy_docs.sources.govinfo.discovery import published_url

        assert self._source is not None
        url = published_url(
            f"{self.since_year:04d}-01-01",
            f"{self.until_year:04d}-12-31",
            collections=[COLLECTION],
            page_size=self.page_size,
        )
        for package in _identified_rows(self._source.packages(url, max_pages=_MAX_PAGES), "packageId"):
            if not package["packageId"].startswith("CFR-"):
                raise CfrSectionsError("CFR listing returned a package outside its collection")
            yield package

    def _iter_granules(self, package: dict) -> Iterator[dict]:
        from spicy_docs.sources.govinfo.discovery import package_granules_url

        assert self._source is not None
        package_id = package["packageId"]
        url = package_granules_url(package_id, page_size=self.page_size)
        for granule in _identified_rows(self._source.granules(url, max_pages=_MAX_PAGES), "granuleId"):
            if not granule["granuleId"].startswith(f"{package_id}-"):
                raise CfrSectionsError("CFR granule identity differs from its requested package")
            self._seen += 1
            if self._seen % _PROGRESS_EVERY == 0:
                logger.info("CFR: {:,} granules so far...", self._seen)
            yield {
                **granule,
                "_package_id": package_id,
                "_package_last_modified": package.get("lastModified"),
                "_package_title": package.get("title"),
                "_package": dict(package),
            }
