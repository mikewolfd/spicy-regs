"""GAO's reports, testimony and legal decisions from 2009 on, read from GAO's own Month in Review and Annual Index.

GAO lists every product it issued, by month and by year, at ``www.gao.gov/reports-testimonies/month-in-review``.
SpicyDocs walks those pages through Zyte (``spicy_docs.sources.gao.month_in_review``), keeping each page's bytes and
resuming from its own receipts. The walk runs outside the rollup: the backfill of 2009-2025 and January-August 2026
took about 80 minutes with three workers on 2026-09-28, while the library default, one request every 420 seconds
(the site's crawl delay), would take days. The rollup reads a finished walk's directory. SpicyDocs re-checks every
page against its receipt and re-parses it, and each page read is retained as this run's evidence. Only years and
months whose every page was read count; the rest wait for a later run.

SpicyDocs classes an entry by its number, with the owner's one exception:
- ``GAO-`` is a product, and every Federal Agency Major Rule Report is a product too. Those were GAO-numbered to
  February 2017 and B-numbered from April 2017, so ``gao_reports`` holds every major-rule report from 2009 on.
  GovInfo's history holds none for 1996-2008, since its B-numbered packages are left out.
- ``B-`` is a legal decision; those become ``gao_decisions`` rows.
- Any other number, a Contract Appeals Board docket (``2020-02``) or a ``P`` number, is one of the "others". The
  numbered others are Contract Appeals Board decisions, listed as Other Decisions, so they are decisions too; the
  others with no number at all (an Antideficiency Act compilation, forum materials) are left out and counted.

A product row keys on the page the listing links, gao.gov's own product id, lowercased, like every other route's
rows. It takes the title (``label: heading``, the feed's own titles), the public-release date (equal to the feed's
and upstream's on all 33 August 2026 products both held), the topic headings in listed order as ``topics_json``, a
type from the number as :func:`gao_govinfo.report_type` rules, and the product number as the page prints it
(``GAO-26-108426``, ``B-331093``) as ``report_number``. A decision row keys on its number as GAO spells it and its
page.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import unquote

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
SITE = "https://www.gao.gov"
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


def decision_rows(run: ListingRun) -> tuple[list[dict], Counter[str]]:
    """One row per listed decision: every B-numbered decision and every numbered other, each with its listing page.

    ``listing_page`` is the first retained walk page that listed the decision, the evidence a reader can open.
    """
    first_page: dict[str, str] = {}
    for retained in run.pages:
        for entry in retained.page.entries:
            first_page.setdefault(unquote(entry.link), retained.capture.requested_url)
    # A numbered other (a Contract Appeals Board docket) states no B-number, so its list is empty.
    decided = [(item, list(item.decision_numbers)) for item in run.decisions]
    decided += [(other, []) for other in run.others if other.product_number is not None]
    rows = [
        {
            "decision_number": item.product_number,
            "b_numbers_json": json.dumps(b_numbers, ensure_ascii=False),
            "decision_type": item.label,
            "title": item.heading,
            "decision_date": item.released or item.published,
            "topics_json": json.dumps(list(item.topics), ensure_ascii=False),
            "url": SITE + item.link,
            "listing_page": first_page.get(unquote(item.link)),
            "source": SOURCE,
        }
        for item, b_numbers in decided
    ]
    counts: Counter[str] = Counter(
        decision_rows=len(rows), unnumbered_left_out=sum(other.product_number is None for other in run.others)
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
