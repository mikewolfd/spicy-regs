"""GAO's reports and testimony from 2009 on, read from GAO's own Month in Review and Annual Index listing.

GAO lists every product it issued, by month and by year, at ``www.gao.gov/reports-testimonies/month-in-review``.
SpicyDocs walks those pages through Zyte (``spicy_docs.sources.gao.month_in_review``). It makes one request every 420
seconds, the site's ``Crawl-delay``, under a hard budget, keeps each page's bytes and resumes from its own receipts.
A full walk takes days, so it runs outside the rollup. The rollup reads a finished walk's directory: SpicyDocs
re-checks every page against its receipt and re-parses it, and each page read is retained as this run's evidence.
Only years and months whose every page was read count; the rest wait for a later run.

A listed product states its product number, GAO's title as ``label: heading``, the topic headings it sits under,
and its "Published" and "Publicly Released" dates. The row keys on the product id the listing links, gao.gov's
own, lowercased, like every other route's rows, and keeps the product number as the page prints it
(``GAO-26-108426``) as ``report_number``, the form users search for. It takes the title, and the public-release date, the date the feed
and upstream's rows carry (equal on all 33 August 2026 products both held). ``topics_json`` holds the topic headings
in listed order, and the type comes from the number, as :func:`gao_govinfo.report_type` rules. B-numbered legal
decisions are listed too; like GovInfo's Comptroller General decisions, they are outside this table. So is every
entry numbered neither ``GAO-`` nor ``B-``: a Contract Appeals Board docket (``2020-02``), a ``P`` number, or a page
with no number at all (an Antideficiency Act report), which SpicyDocs keeps apart as ``others``.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol

from spicy_regs.sources.gao_govinfo import report_type

SOURCE = "gao_listing"
#: A teaser's label is the product's subject ("College Athletics") unless it has none, when GAO shows its product type
#: there. These are the type names finer than ``report_type``: the three the 2009-2026 walk showed as labels
#: (Federal Agency Major Rule Report on 707 products, Correspondence on 201, Other Written Product on 55) and MODS's
#: other names for GAO's product types. "Report" and "Testimony", also seen as labels, say no more than ``report_type``.
FINER_PRODUCT_TYPES = frozenset(
    {
        "Federal Agency Major Rule Report",
        "Correspondence",
        "Other Written Product",
        "Letter Report",
        "Chapter Report",
        "Briefing Report",
        "Fact Sheet",
        "Staff Study",
        "Oral Presentation",
    }
)
PRODUCT_URL = "https://www.gao.gov/products/{}"
#: A walk directory as ``python -m spicy_docs.sources.gao.month_in_review walk`` writes it.
RECEIPTS = "receipts.jsonl"
BLOBS = "blobs"
EVIDENCE_STAGE = "gao-listing"


class ListingRun(Protocol):
    """What this module needs of SpicyDocs' ``GaoListingRun``."""

    @property
    def pages(self) -> Sequence[Any]: ...
    @property
    def products(self) -> Sequence[Any]: ...
    @property
    def decisions(self) -> Sequence[Any]: ...
    @property
    def others(self) -> Sequence[Any]: ...
    @property
    def complete_scopes(self) -> Sequence[str]: ...
    @property
    def incomplete_scopes(self) -> Sequence[str]: ...


class PageEvidence(Protocol):
    """The one call this module makes of the rollup's ``CaptureEvidence``."""

    def capture(self, capture: Any, *, stage: str) -> None: ...


def listing_rows(run: ListingRun) -> tuple[list[dict], Counter[str]]:
    """One row per listed product, and counts of what the run read and left out."""
    rows = [
        {
            "report_id": product.product_id,
            "title": product.title,
            "report_type": report_type(product.product_id),
            "published_date": product.released or product.published,
            "abstract": None,
            "agencies_json": None,
            "topics_json": json.dumps(list(product.topics), ensure_ascii=False),
            "url": PRODUCT_URL.format(product.product_id),
            "source": SOURCE,
            "product_type": product.label if product.label in FINER_PRODUCT_TYPES else None,
            "report_number": product.product_number,
        }
        for product in run.products
    ]
    counts: Counter[str] = Counter(
        pages=len(run.pages),
        complete_scopes=len(run.complete_scopes),
        incomplete_scopes=len(run.incomplete_scopes),
        decisions_left_out=len(run.decisions),
        others_left_out=len(run.others),
        rows=len(rows),
    )
    return rows, counts


def read_listing(directory: Path, evidence: PageEvidence | None = None) -> tuple[list[dict], Counter[str], ListingRun]:
    """Read a finished walk's verified pages, retaining each into ``evidence``, and shape its products."""
    from spicy_docs.sources.gao.month_in_review import read_listing_run

    run = read_listing_run(directory / RECEIPTS, directory / BLOBS)
    if evidence is not None:
        for retained in run.pages:
            evidence.capture(retained.capture, stage=EVIDENCE_STAGE)
    rows, counts = listing_rows(run)
    return rows, counts, run
