"""GAO's reports, testimony and legal decisions from 2009 on, read from GAO's own Month in Review and Annual Index,
and its major-rule reports from 1996 on, read from its two listings of them.

GAO lists every product it issued, by month and by year, at ``www.gao.gov/reports-testimonies/month-in-review``.
SpicyDocs walks those pages through Zyte (``spicy_docs.sources.gao.month_in_review``), keeping each page's bytes and
resuming from its own receipts. The walk runs outside the rollup: the backfill of 2009-2025 and January-August 2026
took about 80 minutes with three workers on 2026-09-28, while the library default, one request every 420 seconds
(the site's crawl delay), would take days. The rollup reads a finished walk's directory. SpicyDocs re-checks every
page against its receipt and re-parses it, and each page read is retained as this run's evidence. Only years and
months whose every page was read count; the rest wait for a later run.

SpicyDocs classes an entry by its number, with the owner's one exception:
- ``GAO-`` is a product, and every Federal Agency Major Rule Report is a product too. Those were GAO-numbered to
  February 2017 and B-numbered from April 2017, so this listing holds every major-rule report from 2009 on.
  The ones before come from GAO's two major-rule listings, below.
- ``B-`` is a legal decision; those become ``gao_decisions`` rows.
- Any other number, a Contract Appeals Board docket (``2020-02``) or a ``P`` number, is one of the "others". The
  numbered others are Contract Appeals Board decisions, listed as Other Decisions, so they are decisions too; the
  others with no number at all (an Antideficiency Act compilation, forum materials) are left out and counted.

A product row keys on the page the listing links, gao.gov's own product id, lowercased, like every other route's
rows. It takes the title (``label: heading``, the feed's own titles), the public-release date (equal to the feed's
and upstream's on all 33 August 2026 products both held), the topic headings in listed order as ``topics_json``, a
type from the number as :func:`gao_govinfo.report_type` rules, and the product number as the page prints it
(``GAO-26-108426``, ``B-331093``) as ``report_number``. A decision row is spicy-docs' ``gao_decisions`` contract,
shaped from the listed decision by ``build_gao_reports``.

**The major-rule reports before 2009** (owner decisions 2026-09-28 and 2026-10-04) are on two more of GAO's
listings, each walked by SpicyDocs outside the rollup into a directory of the same shape, and read with a Month in
Review walk, whose rows stand first:

- "Reports on Major Rules", GAO's Congressional Review Act listing (``sources.gao.major_rule_reports``): the
  GAO-numbered reports from November 2000 on, OGC-99-12, and in its tail twelve AIMD- and GGD- products that are no
  major-rule reports and join as ordinary products with no label;
- GAO's own index of 2000-12-15, from the Internet Archive, and each listed report's product page
  (``sources.gao.major_rule_old_index``): the 277 OGC-numbered reports of April 1996 to November 2000.

SpicyDocs merges the three (``merge_with_month_in_review``). A report the Month in Review lists keeps its row; the
merge only makes whole a heading the Month in Review cut at about 200 characters. A report only these listings
state is a row labelled Federal Agency Major Rule Report on the listing's word, dated by GAO's issue date (the one
date they state), under its own ``source``: :data:`MAJOR_RULE_SOURCE` or :data:`OLD_INDEX_SOURCE`. They state no
topic, so ``topics_json`` is NULL there, never an empty list: no listing row held one before (0 of 13,437 on
2026-10-05), and a NULL lets a lower route's topic stand. A walk that is not complete adds nothing.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from importlib.metadata import version
from pathlib import Path
from typing import Any, Protocol

from spicy_regs.sources.gao_govinfo import report_type

SOURCE = "gao_listing"
#: The route of a report only "Reports on Major Rules" states, and of one only GAO's index of 2000-12-15 states.
MAJOR_RULE_SOURCE = "gao_major_rule_listing"
OLD_INDEX_SOURCE = "gao_major_rule_index"
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
#: A walk directory as ``python -m spicy_docs.sources.gao.month_in_review walk`` writes it.
RECEIPTS = "receipts.jsonl"
BLOBS = "blobs"
EVIDENCE_STAGE = "gao-listing"
MAJOR_RULE_STAGE = "gao-major-rule-listing"
OLD_INDEX_STAGE = "gao-major-rule-index"


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

    def capture(self, capture: Any, *, stage: str) -> object: ...
    def for_source(self, publisher_id: str, policy: str, *, parser_version: str,
                   policy_decision_id: str) -> Any: ...


@dataclass(frozen=True, slots=True)
class MajorRuleReports:
    """What the major-rule listings a run read state beyond rows; empty where a run read none."""

    #: Each report they state, by product id, with its number: GAO's word that the page is a major-rule report.
    numbers: Mapping[str, str] = field(default_factory=dict)
    #: The old index walk's retained product pages by URL: each prints its report's letter.
    pages: Mapping[str, bytes] = field(default_factory=dict)


def listing_rows(run: ListingRun, products: Sequence[Any] | None = None) -> tuple[list[dict], Counter[str]]:
    """One row per listed product, and counts of what the run read and left out.

    ``products`` are the run's own with the major-rule listings merged in, where a run read those.
    """
    from spicy_docs.sources.gao import major_rule_old_index, major_rule_reports
    from spicy_docs.sources.gao.native import gao_product_url

    # A product's first scope names the listing whose row it is: the Month in Review's scopes come first.
    sources = {major_rule_reports.SCOPE_KEY: MAJOR_RULE_SOURCE, major_rule_old_index.SCOPE_KEY: OLD_INDEX_SOURCE}
    rows = [
        {
            "report_id": product.product_id,
            "title": product.title,
            "report_type": report_type(product.product_id),
            "published_date": product.released or product.published,
            "abstract": None,
            "agencies_json": None,
            "topics_json": json.dumps(list(product.topics), ensure_ascii=False) if product.topics else None,
            "url": gao_product_url(product.product_id),
            "source": sources.get(product.scopes[0], SOURCE) if product.scopes else SOURCE,
            "product_type": product.label if product.label in FINER_PRODUCT_TYPES else None,
            "report_number": product.product_number,
        }
        for product in (run.products if products is None else products)
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


def product_page_evidence(evidence):
    """Keep product-page digests and request metadata; contact-bearing bodies stay local."""
    return None if evidence is None else evidence.for_source(
        "gao-product-pages", "hash_only", parser_version=version("spicy-docs"),
        policy_decision_id="gao-product-pages-contacts-hash-only/1",
    )


def _retain(pages: Sequence[Any], evidence: PageEvidence | None, stage: str) -> None:
    if evidence is not None:
        for retained in pages:
            evidence.capture(retained.capture, stage=stage)


def _major_rule_reports(
    listed: Sequence[Any], major_rules: Path | None, old_index: Path | None, evidence: PageEvidence | None
) -> tuple[Sequence[Any], MajorRuleReports, Counter[str]]:
    """The Month in Review's products with each finished major-rule walk merged in, and what those walks state.

    O(reports): each walk is re-read and digest-checked once by SpicyDocs, and each page retained once.
    """
    from spicy_docs.sources.gao.major_rule_old_index import read_old_index_run
    from spicy_docs.sources.gao.major_rule_reports import REPORT, merge_with_month_in_review, read_major_rule_run

    counts: Counter[str] = Counter()
    reports: list[Any] = []
    pages: dict[str, bytes] = {}
    if major_rules is not None:
        walk = read_major_rule_run(major_rules / RECEIPTS, major_rules / BLOBS)
        counts.update(major_rule_listing_pages=len(walk.pages), major_rule_listing_unfinished=not walk.complete)
        if walk.complete:
            reports += walk.reports
            _retain(walk.pages, evidence, MAJOR_RULE_STAGE)
    if old_index is not None:
        page_evidence = product_page_evidence(evidence)
        index = read_old_index_run(old_index / RECEIPTS, old_index / BLOBS)
        counts.update(old_index_pages=len(index.pages), old_index_unfinished=not index.complete)
        if index.complete:
            reports += index.reports
            _retain(index.pages[:1], evidence, OLD_INDEX_STAGE)
            _retain(index.pages[1:], page_evidence, OLD_INDEX_STAGE)
            # pages[0] is the index's own capture; the rest pair with its reports.
            pages = {page.capture.requested_url: page.capture.body for page in index.pages[1:]}
    merged = merge_with_month_in_review(listed, reports)
    numbers = {report.product_id: report.product_number for report in reports if report.kind == REPORT}
    counts.update(major_rule_reports=len(numbers), only_major_rule_listings=len(merged) - len(listed))
    return merged, MajorRuleReports(numbers, pages), counts


def read_listing(
    directory: Path,
    evidence: PageEvidence | None = None,
    *,
    major_rules: Path | None = None,
    old_index: Path | None = None,
) -> tuple[list[dict], Counter[str], ListingRun, MajorRuleReports]:
    """Read a finished walk's verified pages, retaining each into ``evidence``, and shape its products.

    ``major_rules`` and ``old_index`` name finished walks of GAO's two major-rule listings, merged into the Month in
    Review's products before they are shaped; the fourth value is what those walks state beyond rows.
    """
    from spicy_docs.sources.gao.month_in_review import read_listing_run

    run = read_listing_run(directory / RECEIPTS, directory / BLOBS)
    _retain(run.pages, evidence, EVIDENCE_STAGE)
    products, stated, more = None, MajorRuleReports(), Counter()
    if major_rules is not None or old_index is not None:
        products, stated, more = _major_rule_reports(run.products, major_rules, old_index, evidence)
    rows, counts = listing_rows(run, products)
    counts.update(more)  # not ``+``, which drops a count of zero
    return rows, counts, run, stated
