"""GAO's recent-product RSS window through the SpicyDocs feed acquirer.

The provider validates the complete feed before selected items are yielded.
HTTP, XML, identity and feed-shape failures raise, preserving the prior table.
An explicitly empty RSS channel remains a successful empty observation.
"""

from __future__ import annotations

from collections.abc import Iterator

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

import httpx

from spicy_regs.sources.base import Reader

# An inert constructor default keeps base imports independent of source-readers.
# The acquirer owns the actual feed request; tests check this default agrees.
RSS_URL = "https://www.gao.gov/rss/reports.xml"
_MAX_REQUESTS = 5


class GaoReportsReader(Reader):
    """Yield GAO report items (title, link, description, pub_date) from the reports RSS feed.

    Refuses any ``url`` other than ``RSS_URL``; ``max_records`` yields only the feed's first items.
    """

    def __init__(
        self,
        *,
        url: str = RSS_URL,
        max_records: int | None = None,
        verbose: bool = False,
        transport: httpx.BaseTransport | None = None,
        evidence: CaptureEvidence | None = None,
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
        self.evidence = evidence

    def iter_records(self) -> Iterator[dict]:
        try:
            from spicy_docs.sources.gao.rss import GaoFeedAcquirer, GaoFeedBudget
        except ModuleNotFoundError as error:
            if error.name == "spicy_docs":
                raise RuntimeError(
                    "GAO reports require spicy-regs[source-readers]. Run `uv sync --frozen` in a SpicyRegs checkout."
                ) from None
            raise
        budget = GaoFeedBudget(
            max_requests=_MAX_REQUESTS, max_bytes=4 * 1024 * 1024, timeout_seconds=60, min_request_interval_seconds=0
        )
        transport = self.transport
        if self.evidence:
            self.evidence.event("selection", stage="gao", url=RSS_URL, max_records=self.max_records)
            transport = self.evidence.transport(transport, stage="gao-response", max_bytes=budget.max_bytes)
        with GaoFeedAcquirer(budget=budget, transport=transport) as reader:
            feed = reader.acquire_reports_feed().feed
        items = feed.items if self.max_records is None else feed.items[: self.max_records]
        for item in items:
            yield {"title": item.title, "link": item.link, "description": item.description, "pub_date": item.pub_date}
