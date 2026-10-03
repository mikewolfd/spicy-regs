"""GAO's own listing to gao_reports rows: identity, title, release date, topics, type, and retained pages.

The product fields below are those SpicyDocs' Month in Review reader stated for August 2026 pages retained on
2026-09-28 (``gao-26-108640`` under two topics); the testimony and fallback rows are shaped by hand.
"""

from __future__ import annotations

import json
import sys
from types import ModuleType, SimpleNamespace

from spicy_regs.sources import gao_listing
from spicy_regs.sources.gao_listing import SOURCE, decision_rows, listing_rows, read_listing


def _product(product_id: str, *, released: str | None = "2026-08-05", published: str | None = "2026-07-14",
             topics: tuple[str, ...] = ("Education",), title: str | None = None, label: str = "Label",
             number: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(product_id=product_id, title=title or f"{label}: Heading of {product_id}",
                           released=released, published=published, topics=topics, label=label,
                           product_number=number or product_id.upper())


def _decision(link: str, number: str | None, *, numbers: tuple[str, ...] = (), label: str = "Bid Protest Decision",
              heading: str = "Acme Corp.", released: str | None = "2026-08-18",
              topics: tuple[str, ...] = ("Bid Protest Decision",)) -> SimpleNamespace:
    return SimpleNamespace(link=link, product_number=number, decision_numbers=numbers or ((number,) if number else ()),
                           label=label, heading=heading, published=None, released=released, topics=topics,
                           scopes=("2026-08",))


def _other(link: str, number: str | None, *, label: str = "Legal Other Decision",
           topics: tuple[str, ...] = ("Other Decision",)) -> SimpleNamespace:
    """SpicyDocs' ``GaoListedOther``: no B-numbers of its own."""
    return SimpleNamespace(link=link, product_number=number, label=label, heading="Acme Corp.", published=None,
                           released="2026-08-18", topics=topics, scopes=("2026-08",))


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


def test_decisions_become_rows_keyed_on_their_number_and_page_with_the_page_that_listed_them():
    """Every B-numbered decision and every numbered other (a Contract Appeals Board docket) is a decision row."""
    joint = _decision("/products/b-423916.2%2Cb-423916.3", "B-423916.2,B-423916.3",
                      numbers=("B-423916.2", "B-423916.3"))
    docket = _other("/products/2020-02-0", "2020-02")
    numberless = _other("/products/p00459", None, label="Antideficiency Act Report")
    page = "https://www.gao.gov/reports-testimonies/month-in-review/2026/August?page=3"
    run = _run(decisions=[joint], others=[docket, numberless],
               pages=[_page(page, "/products/b-423916.2%2Cb-423916.3", "/products/2020-02-0", "/products/p00459")])
    rows, counts = decision_rows(run)
    assert rows == [
        {"decision_number": "B-423916.2,B-423916.3", "b_numbers_json": '["B-423916.2", "B-423916.3"]',
         "decision_type": "Bid Protest Decision", "title": "Acme Corp.", "released_date": "2026-08-18",
         "topics_json": '["Bid Protest Decision"]', "url": "https://www.gao.gov/products/b-423916.2%2Cb-423916.3",
         "listing_page": page, "source": SOURCE},
        {"decision_number": "2020-02", "b_numbers_json": "[]", "decision_type": "Legal Other Decision",
         "title": "Acme Corp.", "released_date": "2026-08-18", "topics_json": '["Other Decision"]',
         "url": "https://www.gao.gov/products/2020-02-0", "listing_page": page, "source": SOURCE},
    ]
    assert counts["decision_rows"] == 2 and counts["unnumbered_left_out"] == 1


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
    rows, counts, run = read_listing(tmp_path, evidence)
    assert calls == [(tmp_path / gao_listing.RECEIPTS, tmp_path / gao_listing.BLOBS)]
    assert evidence.captures == [(f"capture-{index}", "gao-listing") for index in range(3)]
    assert [row["report_id"] for row in rows] == ["gao-26-1"] and counts["pages"] == 3 and run.pages == pages
