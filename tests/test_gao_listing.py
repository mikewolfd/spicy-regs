"""GAO's own listing to gao_reports rows: identity, title, release date, topics, type, and retained pages.

The product fields below are those SpicyDocs' Month in Review reader stated for August 2026 pages retained on
2026-09-28 (``gao-26-108640`` under two topics); the testimony and fallback rows are shaped by hand.
"""

from __future__ import annotations

import json
import sys
from types import ModuleType, SimpleNamespace

from spicy_regs.sources import gao_listing
from spicy_regs.sources.gao_listing import SOURCE, listing_rows, read_listing

#: The retained Month in Review page a test's decisions were listed on.
LISTING_PAGE = "https://www.gao.gov/reports-testimonies/month-in-review/2026/August?page=3"


def _product(product_id: str, *, released: str | None = "2026-08-05", published: str | None = "2026-07-14",
             topics: tuple[str, ...] = ("Education",), title: str | None = None, label: str | None = "Label",
             number: str | None = None, scopes: tuple[str, ...] = ("2026-08",)) -> SimpleNamespace:
    return SimpleNamespace(product_id=product_id, title=title or f"{label}: Heading of {product_id}",
                           released=released, published=published, topics=topics, label=label,
                           product_number=number or product_id.upper(), scopes=scopes)


def _decision(link: str, number: str | None, *, numbers: tuple[str, ...] = (), label: str = "Bid Protest Decision",
              heading: str = "Acme Corp.", released: str | None = "2026-08-18",
              topics: tuple[str, ...] = ("Bid Protest Decision",), status: str | None = None,
              cut: bool = False) -> SimpleNamespace:
    """SpicyDocs' ``GaoListedDecision``, with the outcome sentence and first listing page it carries from 0.54.0, and
    from round 6 whether the listing cut its number list."""
    return SimpleNamespace(link=link, product_number=number, decision_numbers=numbers or ((number,) if number else ()),
                           label=label, heading=heading, published=None, released=released, topics=topics,
                           scopes=("2026-08",), status=status, listing_page=LISTING_PAGE, decision_numbers_cut=cut)


def _other(link: str, number: str | None, *, label: str = "Legal Other Decision",
           topics: tuple[str, ...] = ("Other Decision",)) -> SimpleNamespace:
    """SpicyDocs' ``GaoListedOther``: no B-numbers of its own."""
    return SimpleNamespace(link=link, product_number=number, label=label, heading="Acme Corp.", published=None,
                           released="2026-08-18", topics=topics, scopes=("2026-08",), status=None,
                           listing_page=LISTING_PAGE)


def _page(url: str, *links: str) -> SimpleNamespace:
    """A retained listing page: its capture's URL and the teasers' links, in order."""
    return SimpleNamespace(capture=SimpleNamespace(requested_url=url),
                           page=SimpleNamespace(entries=[SimpleNamespace(link=link) for link in links]))


def _run(*products, decisions=(), others=(), pages=(), incomplete=()) -> SimpleNamespace:
    return SimpleNamespace(products=products, decisions=decisions, others=others, pages=pages,
                           complete_scopes=("2026-08",), incomplete_scopes=incomplete)


def test_a_listed_product_becomes_one_row_keyed_on_its_gao_product_id():
    college = _product("gao-26-108640", topics=("Auditing and Financial Management", "Education"),
                       title="College Athletics: Most Programs Spend More Than They Generate in Revenue",
                       label="College Athletics")
    (row,), counts = listing_rows(_run(college, decisions=[object(), object()], others=[object()]))
    assert row == {
        "report_id": "gao-26-108640",
        "title": "College Athletics: Most Programs Spend More Than They Generate in Revenue",
        "report_type": "Report",
        "published_date": "2026-08-05",
        "abstract": None,
        "agencies_json": None,
        "topics_json": '["Auditing and Financial Management", "Education"]',
        "url": "https://www.gao.gov/products/gao-26-108640",
        "source": SOURCE,
        "product_type": None,
        "report_number": "GAO-26-108640",
    }
    assert counts["rows"] == 1 and counts["decisions_left_out"] == 2 and counts["others_left_out"] == 1
    assert counts["complete_scopes"] == 1


def test_testimony_is_typed_by_its_number_and_an_unreleased_product_takes_its_published_date():
    (testimony, unreleased), _ = listing_rows(_run(_product("gao-09-431t"), _product("gao-09-445", released=None)))
    assert testimony["report_type"] == "Testimony" and unreleased["report_type"] == "Report"
    assert unreleased["published_date"] == "2026-07-14"


def test_report_number_is_printed_as_the_page_prints_it_and_a_type_label_is_the_product_type():
    """The number as printed keys what users search; a label that is one of GAO's finer types is the product type."""
    rows, _ = listing_rows(_run(
        _product("gao-14-253r", label="Federal Agency Major Rule Report"),
        _product("gao-20-576r", label="Correspondence"),
        _product("gao-16-75sp-0", label="Other Written Product", number="GAO-16-75SP"),
        _product("gao-09-431t", label="Testimony"),
        _product("gao-15-1r", label="Report"),
    ))
    assert [(row["report_number"], row["product_type"]) for row in rows] == [
        ("GAO-14-253R", "Federal Agency Major Rule Report"),
        ("GAO-20-576R", "Correspondence"),
        ("GAO-16-75SP", "Other Written Product"),
        ("GAO-09-431T", None),
        ("GAO-15-1R", None),
    ]


def test_topic_headings_keep_gaos_spelling_in_json():
    (row,), _ = listing_rows(_run(_product("gao-26-1", topics=("Science & Technology", "Veterans"))))
    assert json.loads(row["topics_json"]) == ["Science & Technology", "Veterans"]


def test_read_listing_reads_the_walk_directory_and_retains_every_page(tmp_path, monkeypatch):
    pages = [SimpleNamespace(capture=f"capture-{index}") for index in range(3)]
    calls = []

    def read_listing_run(receipts, store):
        calls.append((receipts, store))
        return _run(_product("gao-26-1"), pages=pages)

    reader = ModuleType("spicy_docs.sources.gao.month_in_review")
    vars(reader)["read_listing_run"] = read_listing_run
    monkeypatch.setitem(sys.modules, "spicy_docs.sources.gao.month_in_review", reader)

    class Evidence:
        def __init__(self):
            self.captures = []

        def capture(self, capture, *, stage):
            self.captures.append((capture, stage))

    evidence = Evidence()
    rows, counts, run, stated = read_listing(tmp_path, evidence)
    assert calls == [(tmp_path / gao_listing.RECEIPTS, tmp_path / gao_listing.BLOBS)]
    assert evidence.captures == [(f"capture-{index}", "gao-listing") for index in range(3)]
    assert [row["report_id"] for row in rows] == ["gao-26-1"] and counts["pages"] == 3 and run.pages == pages
    # No major-rule walk was named, so the run states nothing beyond its rows.
    assert stated == gao_listing.MajorRuleReports() and "major_rule_reports" not in counts


# The major-rule reports before 2009 (owner decisions 2026-09-28 and 2026-10-04): GAO's two listings of them, merged
# into the Month in Review's products by spicy-docs' own rule. The dataclasses and the merge are the real ones.

MAJOR = "Federal Agency Major Rule Report"
INDEX_URL = "https://web.archive.org/web/20001215164700id_/http://www.gao.gov:80/decisions/majrule/majrule.htm"


def _walks(monkeypatch, *, listed, reports, old=(), complete=True, old_complete=True):
    """Stub the three walk readers; return the calls they receive."""
    from spicy_docs.sources.gao import major_rule_old_index, major_rule_reports, month_in_review

    calls = []

    def read_listing_run(receipts, store):
        calls.append(("month-in-review", receipts.parent))
        return _run(*listed, pages=[SimpleNamespace(capture="mir-page")])

    def read_major_rule_run(receipts, store):
        calls.append(("reports-on-major-rules", receipts.parent))
        pages: tuple = (SimpleNamespace(capture="cra-page"),)
        return major_rule_reports.GaoMajorRuleRun(pages, tuple(reports), 0, complete)

    def read_old_index_run(receipts, store):
        calls.append(("old-index", receipts.parent))
        pages: tuple = (SimpleNamespace(capture=SimpleNamespace(requested_url=INDEX_URL, body=b"index")), *(
            SimpleNamespace(capture=SimpleNamespace(
                requested_url=f"https://www.gao.gov/products/{report.product_id}", body=report.product_id.encode()))
            for report in old))
        if not old_complete:
            return major_rule_old_index.GaoOldIndexRun((), (), (), (), False)
        return major_rule_old_index.GaoOldIndexRun(pages, (), tuple(old), (), True)

    monkeypatch.setattr(month_in_review, "read_listing_run", read_listing_run)
    monkeypatch.setattr(major_rule_reports, "read_major_rule_run", read_major_rule_run)
    monkeypatch.setattr(major_rule_old_index, "read_old_index_run", read_old_index_run)
    return calls


def _listed(product_id, number, heading, *, released="2017-05-02", topics=("Health Care",)):
    from spicy_docs.sources.gao.month_in_review import GaoListedProduct

    return GaoListedProduct(product_id, number, MAJOR, heading, None, released, topics, ("2017",))


def _report(product_id, number, title, published, scope="reports-on-major-rules"):
    from spicy_docs.sources.gao.major_rule_reports import GaoMajorRuleReport

    return GaoMajorRuleReport(product_id, number, title, published, (0,), scope)


class _Retained:
    def __init__(self):
        self.captures = []

    def capture(self, capture, *, stage):
        self.captures.append((getattr(capture, "requested_url", capture), stage))


def test_the_major_rule_listings_join_the_month_in_reviews_products_under_their_own_source(tmp_path, monkeypatch):
    """A report both state keeps the Month in Review's row, its cut heading made whole; one only a major-rule
    listing states is that listing's row, dated by GAO's issue date, with no topic."""
    whole = "Department of Health and Human Services: Medicare Program; Changes to the Whole Heading"
    calls = _walks(
        monkeypatch,
        listed=[_listed("b-331093", "B-331093", whole[:60] + "...")],
        reports=[_report("b-331093", "B-331093", whole, "2017-04-28"),
                 _report("gao-01-193r", "GAO-01-193R", "Department of Agriculture: Sugar Program", "2000-11-27"),
                 _report("ogc-99-12", "OGC-99-12", "Three Agencies: Health Insurance Portability", "1998-10-01"),
                 _report("aimd-00-159r", "AIMD-00-159R", "Audit Review of a Chartered Corporation", "2000-05-05"),
                 _report("b-286338", "B-286338", "An Opinion on Whether an Action Is a Rule", "2000-10-17")],
        old=[_report("ogc-96-9", "OGC-96-9", "NRC Revision of Fee Schedules", "1996-04-26", "majrule-index-2000-12-15"),
             _report("ogc-99-12", "OGC-99-12", "Three Agencies: Health Insurance Portability", "1998-10-01",
                     "majrule-index-2000-12-15")],
    )
    evidence = _Retained()
    rows, counts, _, stated = read_listing(tmp_path / "mir", evidence, major_rules=tmp_path / "cra",
                                           old_index=tmp_path / "early")
    assert calls == [("month-in-review", tmp_path / "mir"), ("reports-on-major-rules", tmp_path / "cra"),
                     ("old-index", tmp_path / "early")]
    by_id = {row["report_id"]: row for row in rows}
    assert list(by_id) == ["b-331093", "gao-01-193r", "ogc-99-12", "aimd-00-159r", "ogc-96-9"]  # no row for B-286338
    assert by_id["b-331093"] | {} == {
        "report_id": "b-331093", "title": f"{MAJOR}: {whole}", "report_type": "Report", "published_date": "2017-05-02",
        "abstract": None, "agencies_json": None, "topics_json": '["Health Care"]',
        "url": "https://www.gao.gov/products/b-331093", "source": SOURCE, "product_type": MAJOR,
        "report_number": "B-331093"}
    assert by_id["gao-01-193r"] | {} == {
        "report_id": "gao-01-193r", "title": f"{MAJOR}: Department of Agriculture: Sugar Program",
        "report_type": "Report", "published_date": "2000-11-27", "abstract": None, "agencies_json": None,
        "topics_json": None, "url": "https://www.gao.gov/products/gao-01-193r",
        "source": gao_listing.MAJOR_RULE_SOURCE, "product_type": MAJOR, "report_number": "GAO-01-193R"}
    # The first listing that states a report names its route; the old index's own reports are its rows.
    assert {key: row["source"] for key, row in by_id.items()} == {
        "b-331093": SOURCE, "gao-01-193r": gao_listing.MAJOR_RULE_SOURCE, "ogc-99-12": gao_listing.MAJOR_RULE_SOURCE,
        "aimd-00-159r": gao_listing.MAJOR_RULE_SOURCE, "ogc-96-9": gao_listing.OLD_INDEX_SOURCE}
    # An AIMD- product in the listing's tail is an ordinary row: no label, the heading alone as its title.
    other = by_id["aimd-00-159r"]
    assert (other["product_type"], other["title"]) == (None, "Audit Review of a Chartered Corporation")
    # What the listings state beyond rows: which pages are major-rule reports, and the old index's product pages.
    assert stated.numbers == {"gao-01-193r": "GAO-01-193R", "ogc-99-12": "OGC-99-12", "ogc-96-9": "OGC-96-9"}
    assert stated.pages == {"https://www.gao.gov/products/ogc-96-9": b"ogc-96-9",
                            "https://www.gao.gov/products/ogc-99-12": b"ogc-99-12"}
    assert evidence.captures == [
        ("mir-page", "gao-listing"), ("cra-page", "gao-major-rule-listing"), (INDEX_URL, "gao-major-rule-index"),
        ("https://www.gao.gov/products/ogc-96-9", "gao-major-rule-index"),
        ("https://www.gao.gov/products/ogc-99-12", "gao-major-rule-index")]
    assert (counts["rows"], counts["only_major_rule_listings"], counts["major_rule_reports"]) == (5, 4, 3)
    # A finished walk is journaled as finished, not left out: a count of zero is still stated.
    assert {key: counts[key] for key in ("major_rule_listing_unfinished", "old_index_unfinished") if key in counts} == {
        "major_rule_listing_unfinished": 0, "old_index_unfinished": 0}


def test_a_major_rule_walk_that_is_not_finished_adds_nothing_and_is_counted(tmp_path, monkeypatch):
    _walks(monkeypatch, listed=[_listed("b-331093", "B-331093", "A Heading")],
           reports=[_report("gao-01-193r", "GAO-01-193R", "Department of Agriculture: Sugar Program", "2000-11-27")],
           old=[_report("ogc-96-9", "OGC-96-9", "NRC Revision of Fee Schedules", "1996-04-26",
                        "majrule-index-2000-12-15")],
           complete=False, old_complete=False)
    evidence = _Retained()
    rows, counts, _, stated = read_listing(tmp_path, evidence, major_rules=tmp_path, old_index=tmp_path)
    assert [row["report_id"] for row in rows] == ["b-331093"]
    assert (counts["major_rule_listing_unfinished"], counts["old_index_unfinished"]) == (1, 1)
    assert stated == gao_listing.MajorRuleReports() and evidence.captures == [("mir-page", "gao-listing")]


def test_a_listing_that_states_no_topic_leaves_the_cell_null():
    """An empty list would say GAO lists the product under no topic, and would stand over a lower route's topic."""
    (row,), _ = listing_rows(_run(_product("gao-01-193r", topics=(), scopes=("reports-on-major-rules",))))
    assert row["topics_json"] is None and row["source"] == gao_listing.MAJOR_RULE_SOURCE


def test_the_scope_keys_are_spicy_docs_own():
    from spicy_docs.sources.gao import major_rule_old_index, major_rule_reports

    assert (major_rule_reports.SCOPE_KEY, major_rule_old_index.SCOPE_KEY) == (
        "reports-on-major-rules", "majrule-index-2000-12-15")
