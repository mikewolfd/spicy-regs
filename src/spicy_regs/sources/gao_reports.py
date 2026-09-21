"""GAO's recent-product RSS window through the SpicyDocs feed acquirer.

The provider validates the complete feed before selected items are yielded.
HTTP, XML, identity and feed-shape failures raise, preserving the prior table.
An explicitly empty RSS channel remains a successful empty observation.
"""

from __future__ import annotations

from collections.abc import Iterator

import httpx
from spicy_docs.sources.gao.rss import GAO_REPORTS_FEED_URL, GaoFeedAcquirer, GaoFeedBudget

from spicy_regs.sources.base import Reader

RSS_URL = GAO_REPORTS_FEED_URL
_MAX_REQUESTS = 5


class GaoReportsReader(Reader):
    def __init__(
        self,
        *,
        url: str = RSS_URL,
        max_records: int | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if url != RSS_URL:
            raise ValueError("GAO reports reader requires the provider's reports RSS route")
        if max_records is not None and (
            isinstance(max_records, bool) or not isinstance(max_records, int) or max_records < 1
        ):
            raise ValueError("max_records must be a positive integer or None")
        self.max_records = max_records
        self.verbose = verbose
        self.transport = transport

    def iter_records(self) -> Iterator[dict]:
        budget = GaoFeedBudget(
            max_requests=_MAX_REQUESTS, max_bytes=4 * 1024 * 1024, timeout_seconds=60, min_request_interval_seconds=0
        )
        with GaoFeedAcquirer(budget=budget, transport=self.transport) as reader:
            feed = reader.acquire_reports_feed().feed
        items = feed.items if self.max_records is None else feed.items[: self.max_records]
        for item in items:
            yield {"title": item.title, "link": item.link, "description": item.description, "pub_date": item.pub_date}
