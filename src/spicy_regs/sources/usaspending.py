"""Explicit top-page recipient selections through SpicyDocs' USAspending reader.

A successful configured page cap completes this selected scope, not the entire
recipient population. Failed pages and inconsistent continuation/count metadata
raise before the transform writes. Short pages with a next page still continue.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import TYPE_CHECKING

import httpx

from spicy_regs.sources.base import Reader

if TYPE_CHECKING:
    from spicy_docs.sources.usaspending import AwardType

API_BASE = "https://api.usaspending.gov/api/v2"
# An inert host default keeps construction independent of source-readers.
# The owner request builder still validates the selected request limit.
PER_PAGE = 100
# Provider-supported 100 rows per request: at most 10,000 selected rows per
# default invocation. max_pages remains an explicit operational scope control;
# the transform merges all prior rows even when they leave this selected ranking.
DEFAULT_MAX_PAGES = 100
_MAX_PAGES_HARD = 5_000
_MAX_REQUESTS_PER_PAGE = 5


class UsaSpendingError(ValueError):
    """The selected recipient pages cannot be established."""


class UsaSpendingRecipientsReader(Reader):
    """Walk the selected top-page recipient ranking, validating identity and continuation metadata.

    ``max_pages`` bounds this selected scope, not the whole population. A missing, changed or
    inconsistent count/continuation, or a missing or repeated ``id``, raises ``UsaSpendingError``.
    """

    def __init__(
        self,
        *,
        per_page: int = PER_PAGE,
        max_pages: int = DEFAULT_MAX_PAGES,
        award_type: AwardType = "all",
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        for name, value in (("per_page", per_page), ("max_pages", max_pages)):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        self.per_page = min(per_page, PER_PAGE)
        self.max_pages = min(max_pages, _MAX_PAGES_HARD)
        self.award_type = award_type
        self.verbose = verbose
        self.transport = transport

    def iter_records(self) -> Iterator[dict]:
        try:
            from spicy_docs.reading.paged_json import PagedJsonBudget
            from spicy_docs.sources.usaspending import RESULTS_KEY, UsaspendingRecipientsReader, recipients_request
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "USAspending requires spicy-regs[source-readers]. Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise
        url, body = recipients_request(limit=self.per_page, award_type=self.award_type)
        budget = PagedJsonBudget(
            max_requests=_MAX_REQUESTS_PER_PAGE,
            max_page_bytes=16 * 1024 * 1024,
            timeout_seconds=60,
            min_request_interval_seconds=0,
        )
        observed = 0
        declared = None
        seen = set()
        with UsaspendingRecipientsReader(budget=budget, transport=self.transport) as reader:
            for index in range(self.max_pages):
                if body is None:
                    raise UsaSpendingError("USAspending continuation omitted its request body")
                page = reader.page(url, records_key=RESULTS_KEY, body=body, page_index=index)
                metadata = json.loads(page.capture.body).get("page_metadata")
                if (
                    not isinstance(metadata, dict)
                    or type(metadata.get("hasNext")) is not bool
                    or "next" not in metadata
                ):
                    raise UsaSpendingError("USAspending page omitted its continuation metadata")
                if metadata["hasNext"] != (page.next_url is not None):
                    raise UsaSpendingError("USAspending hasNext and next disagree")
                if page.declared_count is None or (declared is not None and page.declared_count != declared):
                    raise UsaSpendingError("USAspending declared count is missing or changed")
                declared = page.declared_count
                observed += len(page.records)
                if len(page.records) > self.per_page or observed > declared:
                    raise UsaSpendingError("USAspending returned more records than requested or declared")
                if page.next_url is not None:
                    if not page.records or page.next_body is None or page.next_body["page"] != body["page"] + 1:
                        raise UsaSpendingError("USAspending continuation skips a page or follows an empty page")
                elif observed != declared:
                    raise UsaSpendingError("USAspending terminal page disagrees with the declared count")
                for record in page.records:
                    identity = record.get("id")
                    if not isinstance(identity, str) or not identity.strip() or identity in seen:
                        raise UsaSpendingError("USAspending page contains a missing or repeated recipient identity")
                    seen.add(identity)
                    yield dict(record)
                if page.next_url is None:
                    return
                url, body = page.next_url, page.next_body
        # The requested top-page selection is complete even when more pages exist.
