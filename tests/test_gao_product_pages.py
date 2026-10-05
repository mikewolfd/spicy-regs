"""The product-page read, by the reader's importer note: what switches it on, the known pages it reads first, which
rows it asks for and when, what a read sets and records, what a refusal does, and how a read is taken back.

The acquirer is a stand-in handing back spicy-docs' own ``GaoProductDetails``; what its reader reads from a page is
tested in spicy-docs. A major-rule report's page is GAO's own (``fixtures/gao_major_rule_letters``). The reader's
rule is set to ``/2`` for every test here, the first the pass runs under; the installed one may be earlier.
"""

from __future__ import annotations

import json
from datetime import date, datetime
from importlib import import_module
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml
from spicy_docs.sources.gao import product_details
from spicy_docs.sources.gao.product_details import (
    GaoProductDetails,
    GaoProductDetailsError,
    GaoProductPageUnavailableError,
)
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.sources import gao_product_pages as product_pages
from tests.government_fakes import literal_table

module = import_module("spicy_regs.transforms.build_gao_reports")
WORKFLOWS = Path(__file__).resolve().parents[1] / ".github/workflows"
MAJOR = "Federal Agency Major Rule Report"
RULE = "gao-product-page-details/2"
OBSERVED = "2026-10-20T17:20:05Z"
#: The day the pass takes it to be: a month after the rows below were released, so none is in its first week.
TODAY = date(2026, 10, 20)
FULL: dict[str, Any] = {"recommendation_count": 1, "matters_for_congress_count": 0, "page_count": 40,
                        "subject_terms_json": '["Audits"]'}


def _full(**cells: Any) -> dict[str, Any]:
    """A row's four page columns, all stated, with ``cells`` over them."""
    return {**FULL, **cells}


@pytest.fixture(autouse=True)
def reader_rule(monkeypatch):
    monkeypatch.setattr(product_details, "PRODUCT_PAGE_DETAILS_RULE", RULE)


def _row(report_id: str, released: str | None = "2026-09-20", source: str = "gao_rss", **cells) -> dict:
    return {**dict.fromkeys(module.COLUMNS), "report_id": report_id, "title": f"Title {report_id}",
            "report_type": "Report", "published_date": released, "url": f"https://www.gao.gov/products/{report_id}",
            "source": source, **cells}


def _capture(report_id: str, body: bytes = b"<html>page</html>", status: int = 200,
             observed: str = OBSERVED) -> CapturedBodyResponse:
    url = f"https://www.gao.gov/products/{report_id}"
    return CapturedBodyResponse(url, url, status, "text/html; charset=UTF-8", observed, body)


def _page(report_id: str, recommendations: int = 0, matters: int = 0, pages: int | None = None,
          terms: tuple[str, ...] | None = None, body: bytes = b"<html>page</html>", observed: str = OBSERVED,
          topics: tuple[str, ...] | None = None, agencies: tuple[str, ...] | None = None):
    details = GaoProductDetails(report_id, recommendations, matters, agencies, pages, topics, terms)
    return details, _capture(report_id, body, observed=observed)


def _known(product_id: str, recommendations: int, matters: int, pages: int | None, terms: int | None):
    return _page(product_id, recommendations, matters, pages, None if terms is None else tuple(map(str, range(terms))))


def _reading(rule: str = RULE, **fields) -> str:
    return json.dumps({"rule": rule, "outcome": "read", "sha256": "sha256:" + "1" * 64, **fields})


def _refused(reason: str) -> GaoProductDetailsError:
    return GaoProductDetailsError(f"GAO product page refused: {reason}", reason)


def _failed() -> GaoProductDetailsError:
    """A request that gave no page to read: a proxy error or a timeout."""
    return GaoProductDetailsError("Zyte answered 520")


class Pages:
    """The acquirer: each product's answer, a ``(details, capture)`` pair or the error it raises.

    The known pages are answered apart, as the pass requires them unless a test says otherwise.
    """

    def __init__(self, answers: dict | None = None, known: dict | None = None):
        self.answers, self.asked, self.known_asked = answers or {}, [], []
        self.known = {product_id: _known(product_id, *expected) for product_id, expected in product_pages.KNOWN_PAGES}
        self.known.update(known or {})

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    def acquire_details(self, product_id: str):
        if product_id in self.known and product_id not in self.answers:
            self.known_asked.append(product_id)
            answer = self.known[product_id]
        else:
            self.asked.append(product_id)
            answer = self.answers[product_id]
        if isinstance(answer, Exception):
            raise answer
        return answer


class Journal:
    def __init__(self):
        self.events, self.captures, self.refusals, self.credential = {}, [], [], ""

    def event(self, name, **fields):
        self.events[name] = fields

    def capture(self, capture, *, stage):
        self.captures.append((capture.requested_url, stage))

    def refusal(self, error, *, stage):
        self.refusals.append((getattr(error, "reason", None), stage))

    def retain_file(self, path, **fields):
        pass


def _export(tmp_path, as_of: date, rows: dict[str, int]) -> None:
    """The published ``gao_recommendations`` table as the pass finds it: a row per recommendation, stamped ``as_of``."""
    listed = [report_id for report_id, count in rows.items() for _ in range(count)] or ["gao-00-1"]
    table = pa.table({"report_id": listed, "last_seen": pa.array([as_of] * len(listed), pa.date32()),
                      "recommendation": ["text"] * len(listed)})
    pq.write_table(table, tmp_path / "_gao_recommendations.parquet")


def _build(tmp_path, monkeypatch, prior, pages=None, *, evidence=None, **more) -> dict[str, dict]:
    """One run over ``prior`` with no feed item, on :data:`TODAY`; its output becomes the next run's prior."""
    if prior is not None:
        pq.write_table(pa.Table.from_pylist(prior, schema=module._SCHEMA), tmp_path / "_gao_prior.parquet")

    class NoFeed:
        def __init__(self, **_):
            pass

        def iter_records(self):
            return iter(())

    monkeypatch.setattr(module, "GaoReportsReader", NoFeed)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    out, _ = module.build_gao_reports(tmp_path, pages=pages, evidence=evidence, **{"today": TODAY, **more})
    rows = {row["report_id"]: row for row in literal_table(out).to_pylist()}
    out.rename(tmp_path / "_gao_prior.parquet")
    return rows


# What switches the pass on: the caller's flag and the reader's rule, both.


@pytest.mark.parametrize(
    ("rule", "admitted"),
    [
        ("gao-product-page-details/1", False),  # reads 0 from a page it did not read whole
        ("gao-product-page-details/2", True),
        ("gao-product-page-details/10", True),  # compared as a number, not as text
        ("gao-product-page-details", False),
        ("gao-product-page-details/two", False),
        ("another-reader/3", False),
    ],
)
def test_the_pass_runs_only_under_reader_rule_2_or_later(monkeypatch, rule, admitted):
    monkeypatch.setattr(product_details, "PRODUCT_PAGE_DETAILS_RULE", rule)
    refusal = product_pages.reader_refusal()
    assert (refusal is None) is admitted
    # A refusal names the rule it found and the first one the pass runs under.
    assert admitted or (rule in str(refusal) and "gao-product-page-details/2" in str(refusal))


def test_asked_for_under_rule_1_the_pass_reads_and_writes_nothing_and_the_journal_says_why(tmp_path, monkeypatch):
    """The switch alone does not turn the pass on: not a known-page request, not a cell, and the run still builds."""
    monkeypatch.setattr(product_details, "PRODUCT_PAGE_DETAILS_RULE", "gao-product-page-details/1")
    pages, journal = Pages({"gao-26-1": _page("gao-26-1", 4)}), Journal()
    rows = _build(tmp_path, monkeypatch, [_row("gao-26-1")], pages, evidence=journal, product_pages=True)
    assert (pages.known_asked, pages.asked) == ([], []) and rows["gao-26-1"] == _row("gao-26-1")
    assert "gao-product-page-details/1" in journal.events["gao-product-page"]["not_run"]


def test_the_pass_is_off_unless_the_builder_is_asked_for_it(tmp_path, monkeypatch):
    pages = Pages()
    rows = _build(tmp_path, monkeypatch, [_row("gao-26-1")], pages)
    assert (pages.known_asked, pages.asked) == ([], []) and rows["gao-26-1"] == _row("gao-26-1")


def test_switched_on_without_a_zyte_token_the_run_fails_and_publishes_nothing(tmp_path, monkeypatch):
    from spicy_docs.sources.zyte import ZyteTransportError

    monkeypatch.delenv("ZYTE_TOKEN", raising=False)
    with pytest.raises(ZyteTransportError):
        _build(tmp_path, monkeypatch, [_row("gao-26-1")], product_pages=True)
    assert not (tmp_path / "gao_reports.parquet").exists()


def test_the_acquirer_is_bounded_to_the_known_pages_and_the_cap_spaced_and_its_token_scrubbed(monkeypatch):
    from spicy_docs.transport import zyte

    real, budgets = zyte.ZyteBudget, []

    def budget(limit):
        budgets.append(limit)
        return real(limit)

    monkeypatch.setattr(zyte, "ZyteBudget", budget)
    monkeypatch.setenv("ZYTE_TOKEN", "zyte-fixture-token")
    journal = Journal()
    with product_pages.page_acquirer(journal) as acquirer:  # ty: ignore[invalid-argument-type]
        assert acquirer.budget.min_request_interval_seconds == product_pages.SPACING_SECONDS
    assert budgets == [product_pages.PAGES_PER_RUN + 2]
    assert journal.credential == "zyte-fixture-token"


# The known pages.


def test_each_pass_reads_the_known_pages_first_and_keeps_them_as_evidence(tmp_path, monkeypatch):
    """gao-04-49 prints the recommendations view and ggd-91-26 the matters view; each also states a page count and
    subject terms. The values are the importer note's."""
    assert product_pages.KNOWN_PAGES == (("gao-04-49", (239, 0, 148, 10)), ("ggd-91-26", (0, 35, 200, 10)))
    pages, journal = Pages({"gao-26-1": _page("gao-26-1", 4)}), Journal()
    rows = _build(tmp_path, monkeypatch, [_row("gao-26-1")], pages, evidence=journal, product_pages=True)
    assert (pages.known_asked, pages.asked) == (["gao-04-49", "ggd-91-26"], ["gao-26-1"])
    assert rows["gao-26-1"]["recommendation_count"] == 4
    assert journal.captures[:2] == [("https://www.gao.gov/products/gao-04-49", "gao-product-page-known"),
                                    ("https://www.gao.gov/products/ggd-91-26", "gao-product-page-known")]


@pytest.mark.parametrize(
    ("known", "said"),
    [
        ({"gao-04-49": _known("gao-04-49", 0, 0, 148, 10)}, "gao-04-49 states (0, 0, 148, 10)"),  # a renamed wrapper
        ({"gao-04-49": _known("gao-04-49", 238, 0, 148, 10)}, "gao-04-49 states (238, 0, 148, 10)"),
        ({"gao-04-49": _known("gao-04-49", 239, 1, 148, 10)}, "gao-04-49 states (239, 1, 148, 10)"),
        ({"gao-04-49": _known("gao-04-49", 239, 0, None, 10)}, "gao-04-49 states (239, 0, None, 10)"),  # label reworded
        ({"gao-04-49": _known("gao-04-49", 239, 0, 148, None)}, "gao-04-49 states (239, 0, 148, None)"),  # terms as links
        ({"ggd-91-26": _known("ggd-91-26", 0, 0, 200, 10)}, "ggd-91-26 states (0, 0, 200, 10)"),  # the matters view
        ({"gao-04-49": _refused("paged")}, "gao-04-49 was not read: paged"),
        ({"ggd-91-26": _failed()}, "ggd-91-26 was not read: acquisition"),
    ],
)
def test_a_known_page_that_reads_otherwise_stops_the_pass_before_it_reads_or_writes_a_row(
        tmp_path, monkeypatch, known, said):
    prior = [_row("gao-26-2", "2026-09-30"), _row("gao-26-1", "2026-09-20", **_full(page_count=None))]
    pages = Pages({"gao-26-2": _page("gao-26-2", 4), "gao-26-1": _page("gao-26-1", 1)}, known)
    journal = Journal()
    rows = _build(tmp_path, monkeypatch, prior, pages, evidence=journal, product_pages=True)
    assert pages.asked == [] and list(rows.values()) == prior
    event = journal.events["gao-product-page"]
    assert said in event["stopped"] and "read" not in event
    # What each known page answered is kept: its page, or the refusal.
    assert {stage for _, stage in journal.captures + journal.refusals} == {"gao-product-page-known"}


# Which rows, and when.


@pytest.mark.parametrize(
    ("cells", "expected"),
    [
        ({}, True),  # a report the R package does not list: all four NULL
        (_full(page_count=None), True),  # the package states no page count
        (FULL, False),
        ({"product_page_json": _reading()}, False),  # read: what is still NULL the page does not state
        ({"product_page_json": json.dumps({"rule": RULE, "outcome": "unavailable"})}, False),
        ({"product_page_json": _reading("gao-product-page-details/3")}, False),  # a value written is not read again,
        (_full(page_count=None, product_page_json=_reading("gao-product-page-details/1")), False),  # under any rule
    ],
)
def test_a_row_is_pending_while_a_cell_is_null_and_no_read_of_its_page_is_recorded(cells, expected):
    assert product_pages.pending(_row("gao-26-1", **cells)) is expected


def test_pending_rows_are_asked_newest_first_an_undated_row_last():
    rows = [_row("gao-20-1", "2020-01-05"), _row("gao-26-2", "2026-09-30"), _row("gao-26-1", "2026-09-30"),
            _row("ggd-1", None), _row("gao-26-9", "2026-10-01", **FULL)]
    assert [row["report_id"] for row in product_pages.newest_first(rows)] == ["gao-26-1", "gao-26-2", "gao-20-1", "ggd-1"]


def test_a_run_reads_its_cap_of_the_newest_pending_pages_and_the_next_run_goes_on(tmp_path, monkeypatch):
    monkeypatch.setattr(product_pages, "PAGES_PER_RUN", 2)
    prior = [_row("gao-26-3", "2026-09-30"), _row("gao-26-2", "2026-09-20"), _row("gao-20-381", "2020-04-01"),
             _row("gao-10-1", "2010-01-01", **FULL)]
    pages = Pages({"gao-26-3": _page("gao-26-3", 4, 1, 65, ("Cost control", "Financial management")),
                   "gao-26-2": _page("gao-26-2"),
                   "gao-20-381": _page("gao-20-381", pages=12)})
    journal = Journal()
    first = _build(tmp_path, monkeypatch, prior, pages, evidence=journal, product_pages=True)
    assert pages.asked == ["gao-26-3", "gao-26-2"]
    read = first["gao-26-3"]
    assert (read["recommendation_count"], read["matters_for_congress_count"], read["page_count"],
            read["subject_terms_json"]) == (4, 1, 65, '["Cost control", "Financial management"]')
    # The rule is recorded beside the values, with the bytes read and what each column held before.
    assert json.loads(read["product_page_json"]) == {
        "rule": RULE, "outcome": "read", "url": "https://www.gao.gov/products/gao-26-3",
        "sha256": _capture("gao-26-3").sha256, "observed_at": OBSERVED,
        "stated": ["matters_for_congress_count", "page_count", "recommendation_count", "subject_terms_json"],
        "before": dict.fromkeys(product_pages.FILL_COLUMNS)}
    # A page states both counts always, 0 where it prints neither heading; what it does not state stays NULL.
    bare = first["gao-26-2"]
    assert (bare["recommendation_count"], bare["matters_for_congress_count"], bare["page_count"],
            bare["subject_terms_json"]) == (0, 0, None, None)
    assert first["gao-20-381"] == prior[2] and first["gao-10-1"] == prior[3]
    event = journal.events["gao-product-page"]
    assert (event["rule"], event["per_run"], event["on"], event["pending"], event["read"], event["left_pending"]) == (
        RULE, 2, "2026-10-20", 3, 2, 1)
    assert journal.captures[2:] == [("https://www.gao.gov/products/gao-26-3", "gao-product-page"),
                                    ("https://www.gao.gov/products/gao-26-2", "gao-product-page")]

    # The next run: the two read are not asked again, though one still lacks a page count; the third is read.
    second = _build(tmp_path, monkeypatch, None, pages, product_pages=True)
    assert pages.asked[2:] == ["gao-20-381"]
    assert second["gao-26-2"] == bare and second["gao-26-3"] == read and second["gao-20-381"]["page_count"] == 12
    pages.asked.clear()
    _build(tmp_path, monkeypatch, None, pages, product_pages=True)
    assert pages.asked == [] and len(pages.known_asked) == 6  # the known pages are read every pass


# A product's first week.


def test_a_product_is_not_read_on_its_release_day(tmp_path, monkeypatch):
    """No page was read on its release day, so none is written from one: read from the day after GAO dates it."""
    _export(tmp_path, TODAY, {})
    prior = [_row("gao-27-1", "2026-10-21"), _row("gao-27-2", "2026-10-20"), _row("gao-26-9", "2026-10-11")]
    pages, journal = Pages({"gao-26-9": _page("gao-26-9", 0, 0, 12)}), Journal()
    rows = _build(tmp_path, monkeypatch, prior, pages, evidence=journal, product_pages=True)
    assert pages.asked == ["gao-26-9"] and rows["gao-27-2"] == prior[1] and rows["gao-27-1"] == prior[0]
    assert journal.events["gao-product-page"]["released_today"] == 2


def test_in_its_first_week_a_page_is_written_only_where_it_agrees_with_gaos_open_recommendations(tmp_path, monkeypatch):
    """The page's recommendations and matters together must equal the product's rows in the export; a page that
    differs writes nothing and is asked again."""
    prior = [_row("gao-27-4", "2026-10-19"), _row("gao-27-3", "2026-10-16"), _row("gao-27-2", "2026-10-14"),
             _row("gao-27-1", "2026-10-13"), _row("gao-26-9", "2026-10-12")]
    pages = Pages({
        "gao-27-4": _page("gao-27-4", 3, 1, 40),  # 4 rows listed: agrees
        "gao-27-3": _page("gao-27-3", 0, 0, 12),  # none listed, none printed: agrees
        "gao-27-2": _page("gao-27-2", 0, 0, 30),  # 2 rows listed, the page prints none yet
        "gao-27-1": _page("gao-27-1", 5, 0, 50),  # the page prints 5, 4 listed
        "gao-26-9": _page("gao-26-9", 7, 0, 60),  # eight days old: no second witness is asked for
    })
    journal = Journal()

    def run(rows):
        _export(tmp_path, TODAY, {"gao-27-4": 4, "gao-27-2": 2, "gao-27-1": 4})
        return _build(tmp_path, monkeypatch, rows, pages, evidence=journal, product_pages=True)

    rows = run(prior)
    assert pages.asked == ["gao-27-4", "gao-27-3", "gao-27-2", "gao-27-1", "gao-26-9"]
    assert {key: row["recommendation_count"] for key, row in rows.items()} == {
        "gao-27-4": 3, "gao-27-3": 0, "gao-27-2": None, "gao-27-1": None, "gao-26-9": 7}
    assert rows["gao-27-2"] == prior[2] and rows["gao-27-1"] == prior[3]  # not a cell, not a record
    event = journal.events["gao-product-page"]
    assert (event["export_disagrees"], event["read"], event["export_as_of"]) == (2, 3, "2026-10-20")
    assert not (tmp_path / "_gao_recommendations.parquet").exists()  # scratch, not an output

    pages.asked.clear()
    run(None)
    assert pages.asked == ["gao-27-2", "gao-27-1"]


@pytest.mark.parametrize("stamped", [None, date(2026, 10, 15), date(2026, 10, 16)])
def test_in_its_first_week_a_product_waits_for_an_export_stamped_after_its_release_day(tmp_path, monkeypatch, stamped):
    """An export from before the release lists nothing of the product, so it and a page that prints none would
    agree about nothing. Without one stamped later, the page is not asked for."""
    if stamped is not None:
        _export(tmp_path, stamped, {})
    # Released one, five, seven and eight days before: the last is past its first week and waits for nothing.
    prior = [_row("gao-27-3", "2026-10-19"), _row("gao-27-2", "2026-10-15"), _row("gao-27-1", "2026-10-13"),
             _row("gao-26-9", "2026-10-12")]
    pages = Pages({row["report_id"]: _page(row["report_id"], 0, 0, 12) for row in prior})
    journal = Journal()
    rows = _build(tmp_path, monkeypatch, prior, pages, evidence=journal, product_pages=True)
    expected = {None: ["gao-26-9"], date(2026, 10, 15): ["gao-27-1", "gao-26-9"],
                date(2026, 10, 16): ["gao-27-2", "gao-27-1", "gao-26-9"]}[stamped]
    assert pages.asked == expected and rows["gao-27-3"] == prior[0]
    assert journal.events["gao-product-page"]["waiting_for_export"] == 4 - len(expected)


def test_a_row_that_waits_does_not_use_the_runs_cap(tmp_path, monkeypatch):
    monkeypatch.setattr(product_pages, "PAGES_PER_RUN", 2)
    prior = [_row("gao-27-9", "2026-10-20"), _row("gao-27-8", "2026-10-18"), _row("gao-26-3", "2026-09-03"),
             _row("gao-26-2", "2026-09-02"), _row("gao-26-1", "2026-09-01")]
    pages = Pages({key: _page(key) for key in ("gao-26-3", "gao-26-2", "gao-26-1")})
    _build(tmp_path, monkeypatch, prior, pages, product_pages=True)
    assert pages.asked == ["gao-26-3", "gao-26-2"]


def test_the_export_is_the_published_recommendations_table_or_nothing(tmp_path, monkeypatch):
    monkeypatch.setattr(product_pages.r2, "download", lambda *_: False)
    assert product_pages.open_recommendations(tmp_path) is None
    _export(tmp_path, date(2026, 10, 19), {"gao-27-1": 3, "gao-27-2": 1})
    assert product_pages.open_recommendations(tmp_path) == product_pages.OpenRecommendations(
        "2026-10-19", {"gao-27-1": 3, "gao-27-2": 1})

    def download(key, path):
        assert key == "gao_recommendations.parquet"
        _export(path.parent, date(2026, 10, 20), {"gao-27-9": 2})
        return True

    monkeypatch.setattr(product_pages.r2, "download", download)
    assert product_pages.open_recommendations(tmp_path) == product_pages.OpenRecommendations(
        "2026-10-20", {"gao-27-9": 2})


def test_the_day_is_gaos_unless_a_caller_states_one(tmp_path, monkeypatch):
    """GAO dates a product in Washington's time, so an evening run there is still the release day."""
    assert product_pages.today() == datetime.now(product_pages.GAO_ZONE).date()
    assert product_pages.GAO_ZONE.key == "America/New_York"
    pages = Pages({"gao-26-1": _page("gao-26-1", 4)})
    for day, asked in ((date(2026, 9, 20), []), (date(2026, 10, 20), ["gao-26-1"])):  # its release day, then later
        monkeypatch.setattr(product_pages, "today", lambda day=day: day)
        _build(tmp_path, monkeypatch, [_row("gao-26-1", "2026-09-20")], pages, product_pages=True, today=None)
        assert pages.asked == asked


# What a read sets.


def test_a_page_read_value_outranks_the_r_packages_and_a_value_the_page_does_not_state_is_kept(tmp_path, monkeypatch):
    """The owner's ruling (2026-10-04). The row stays the package's: a page read sets cells, not the route."""
    package = _row("gao-20-381", "2020-04-01", "gao_r_package", abstract="Package summary.", recommendation_count=2,
                   matters_for_congress_count=0, subject_terms_json='["Old term"]', topics_json='["Health Care"]',
                   agencies_json='["Department of Labor"]')
    pages = Pages({"gao-20-381": _page("gao-20-381", 3, 0, 65, topics=("Auditing",), agencies=("Treasury",))})
    journal = Journal()
    rows = _build(tmp_path, monkeypatch, [package], pages, evidence=journal, product_pages=True)
    row = rows["gao-20-381"]
    assert (row["recommendation_count"], row["page_count"], row["subject_terms_json"]) == (3, 65, '["Old term"]')
    assert (row["source"], row["abstract"]) == ("gao_r_package", "Package summary.")
    # The page's topic and affected agencies are not read into the table: its one topic is not the table's several.
    assert (row["topics_json"], row["agencies_json"]) == ('["Health Care"]', '["Department of Labor"]')
    reading = json.loads(row["product_page_json"])
    assert reading["stated"] == ["matters_for_congress_count", "page_count", "recommendation_count"]
    # What each column the page set held before, so the read can be taken back exactly.
    assert reading["before"] == {"recommendation_count": 2, "matters_for_congress_count": 0, "page_count": None}
    event = journal.events["gao-product-page"]
    assert (event["outranked_recommendation_count"], event["outranked_matters_for_congress_count"]) == (1, 0)


def test_a_new_major_rule_report_takes_its_letter_from_the_page_just_read(tmp_path, monkeypatch):
    """A report the listing added after the letters' capture; one whose letter is read already is left alone."""
    from tests.test_gao_major_rule_letters import page

    held = json.dumps({"rule": "gao-major-rule-letter/3", "outcome": "read", "source": "product-page",
                       "sha256": "sha256:" + "0" * 64})
    prior = [
        _row("gao-04-193r", "2026-10-01", "gao_listing", product_type=MAJOR, report_number="GAO-04-193R"),
        _row("gao-01-300r", "2026-09-30", "gao_listing", product_type=MAJOR, report_number="GAO-01-300R",
             major_rule_agency="Held agency", major_rule_letter_json=held),
        _row("gao-26-1", "2026-09-29"),
    ]
    pages = Pages({"gao-04-193r": _page("gao-04-193r", pages=3, body=page("gao-04-193r")),
                   "gao-01-300r": _page("gao-01-300r", body=page("gao-01-300r")),
                   "gao-26-1": _page("gao-26-1", body=page("gao-04-193r"))})
    journal = Journal()
    rows = _build(tmp_path, monkeypatch, prior, pages, evidence=journal, product_pages=True)
    new = rows["gao-04-193r"]
    assert (new["page_count"], new["major_rule_rins_json"], new["major_rule_fr_citations_json"]) == (
        3, '["0910-AC40"]', '["68-58894"]')
    assert json.loads(new["major_rule_letter_json"])["outcome"] == "read"
    assert (rows["gao-01-300r"]["major_rule_agency"], rows["gao-01-300r"]["major_rule_letter_json"]) == (
        "Held agency", held)
    assert rows["gao-26-1"]["major_rule_letter_json"] is None  # no major-rule report: no letter is looked for
    assert journal.events["gao-product-page"]["letters_read"] == 1


# Refusals, pages GAO does not serve, and requests that fail.


def test_a_refusal_writes_nothing_for_the_product_journals_its_reason_and_is_asked_again(tmp_path, monkeypatch):
    prior = [_row("gao-26-4", "2026-09-04"), _row("gao-26-3", "2026-09-03"), _row("gao-26-2", "2026-09-02"),
             _row("gao-26-1", "2026-09-01")]
    pages = Pages({
        "gao-26-4": _refused("incomplete"),  # a page that does not end
        "gao-26-3": _refused("unread-section"),  # one page refused as not whole is one page
        "gao-26-2": _failed(),
        "gao-26-1": _page("gao-26-1", 1),
    })
    journal = Journal()
    rows = _build(tmp_path, monkeypatch, prior, pages, evidence=journal, product_pages=True)
    assert [rows[key] for key in ("gao-26-4", "gao-26-3", "gao-26-2")] == prior[:3]  # not a cell, not a record
    assert rows["gao-26-1"]["recommendation_count"] == 1
    event = journal.events["gao-product-page"]
    assert event["refusals"] == [{"report_id": "gao-26-4", "reason": "incomplete"},
                                 {"report_id": "gao-26-3", "reason": "unread-section"}]
    assert (event["refused"], event["failed"], event["read"], event["left_pending"]) == (2, 1, 1, 3)
    assert journal.refusals == [("incomplete", "gao-product-page"), ("unread-section", "gao-product-page")]

    pages.asked.clear()
    _build(tmp_path, monkeypatch, None, pages, product_pages=True)
    assert pages.asked == ["gao-26-4", "gao-26-3", "gao-26-2"]


@pytest.mark.parametrize(
    "reasons", [("paged", "paged"), ("unread-section", "conflicting-row-count"), ("conflicting-row-count", "paged")]
)
def test_a_second_page_refused_as_not_whole_stops_the_pass_and_nothing_it_read_is_written(
        tmp_path, monkeypatch, reasons):
    """More than one such refusal is GAO's theme changing, and what the pass read before it may be wrong as well."""
    prior = [_row(f"gao-26-{n}", f"2026-09-0{n}") for n in (5, 4, 3, 2, 1)]
    pages = Pages({"gao-26-5": _page("gao-26-5", 4, 0, 30), "gao-26-4": _refused(reasons[0]),
                   "gao-26-3": _page("gao-26-3", 0, 0, 9), "gao-26-2": _refused(reasons[1]),
                   "gao-26-1": _page("gao-26-1", 2)})
    journal = Journal()
    rows = _build(tmp_path, monkeypatch, prior, pages, evidence=journal, product_pages=True)
    assert pages.asked == ["gao-26-5", "gao-26-4", "gao-26-3", "gao-26-2"] and list(rows.values()) == prior
    event = journal.events["gao-product-page"]
    assert "theme" in event["stopped"] and event["read_and_discarded"] == 2
    assert [entry["reason"] for entry in event["refusals"]] == list(reasons)


def test_refusals_for_other_reasons_and_failed_requests_do_not_stop_the_pass(tmp_path, monkeypatch):
    """Only a page refused as not whole speaks of GAO's theme; one of those is one page."""
    prior = [_row(f"gao-26-{n}", f"2026-09-0{n}") for n in (6, 5, 4, 3, 2, 1)]
    pages = Pages({"gao-26-6": _failed(), "gao-26-5": _failed(), "gao-26-4": _refused("incomplete"),
                   "gao-26-3": _refused("changed-view"), "gao-26-2": _refused("paged"),
                   "gao-26-1": _page("gao-26-1", 2)})
    rows = _build(tmp_path, monkeypatch, prior, pages, product_pages=True)
    assert pages.asked == [f"gao-26-{n}" for n in (6, 5, 4, 3, 2, 1)]
    assert rows["gao-26-1"]["recommendation_count"] == 2


def test_a_product_gao_serves_no_page_for_is_recorded_and_not_asked_for_again(tmp_path, monkeypatch):
    """GAO answers 404 for an id it does not serve (opa-97-2, a GovInfo spelling). That is no refusal."""
    gone = _capture("opa-97-2", b"Not Found", 404)
    pages = Pages({"opa-97-2": GaoProductPageUnavailableError(gone)})
    journal = Journal()
    rows = _build(tmp_path, monkeypatch, [_row("opa-97-2", "1997-01-01", "govinfo")], pages, evidence=journal,
                  product_pages=True)
    row = rows["opa-97-2"]
    assert json.loads(row["product_page_json"]) == {
        "rule": RULE, "outcome": "unavailable", "url": "https://www.gao.gov/products/opa-97-2",
        "sha256": gone.sha256, "observed_at": OBSERVED}
    assert all(row[column] is None for column in product_pages.FILL_COLUMNS)
    assert (journal.events["gao-product-page"]["unavailable"], journal.events["gao-product-page"]["refusals"]) == (1, [])
    pages.asked.clear()
    _build(tmp_path, monkeypatch, None, pages, product_pages=True)
    assert pages.asked == []


def test_failed_requests_in_a_row_end_the_pass_and_the_run_still_builds(tmp_path, monkeypatch):
    monkeypatch.setattr(product_pages, "MAX_CONSECUTIVE_FAILURES", 2)
    prior = [_row(f"gao-26-{n}", f"2026-09-0{n}") for n in (5, 4, 3, 2, 1)]
    pages = Pages({"gao-26-5": _page("gao-26-5", 4), **{row["report_id"]: _failed() for row in prior[1:]}})
    journal = Journal()
    rows = _build(tmp_path, monkeypatch, prior, pages, evidence=journal, product_pages=True)
    assert pages.asked == ["gao-26-5", "gao-26-4", "gao-26-3"]
    # What was read before the requests began to fail is kept: a failed request says nothing of GAO's pages.
    assert rows["gao-26-5"]["recommendation_count"] == 4 and list(rows.values())[1:] == prior[1:]
    event = journal.events["gao-product-page"]
    assert (event["failed"], event["stopped_after_failures"], event["left_pending"]) == (2, 1, 4)


# Taking a read back.


def test_a_days_reads_are_undone_exactly_with_the_pass_off_and_the_rows_are_pending_again(tmp_path, monkeypatch):
    """A bad run is reversed without a request: each column takes back what it held, and the record is cleared."""
    from tests.test_gao_major_rule_letters import page

    package = _row("gao-20-381", "2020-04-01", "gao_r_package", recommendation_count=2, matters_for_congress_count=0,
                   subject_terms_json='["Old term"]')
    new = _row("gao-26-1", "2026-10-05")
    rule = _row("gao-04-193r", "2026-10-04", "gao_listing", product_type=MAJOR, report_number="GAO-04-193R")
    kept = _row("gao-26-2", "2026-10-03")
    # A major-rule report whose letter came from the letters' capture, not from the page read that day.
    captured = json.dumps({"rule": "gao-major-rule-letter/3", "outcome": "read", "sha256": "sha256:" + "0" * 64})
    lettered = _row("gao-01-300r", "2026-10-02", "gao_listing", product_type=MAJOR, report_number="GAO-01-300R",
                    major_rule_agency="Federal Communications Commission (FCC)", major_rule_letter_json=captured)
    pages = Pages({
        "gao-01-300r": _page("gao-01-300r", 0, 0, 5, body=page("gao-01-300r"), observed="2026-10-19T17:22:20Z"),
        "gao-20-381": _page("gao-20-381", 0, 3, 65, observed="2026-10-19T17:22:11Z"),
        "gao-26-1": _page("gao-26-1", 0, 0, 9, ("A term",), observed="2026-10-19T17:22:14Z"),
        "gao-04-193r": _page("gao-04-193r", pages=3, body=page("gao-04-193r"), observed="2026-10-19T17:22:17Z"),
        "gao-26-2": _page("gao-26-2", 1, 0, 4, observed="2026-10-18T17:22:00Z"),
    })
    read = _build(tmp_path, monkeypatch, [package, new, rule, kept, lettered], pages, product_pages=True)
    # The wrong value a bad day leaves: 0 recommendations over the package's 2.
    assert (read["gao-20-381"]["recommendation_count"], read["gao-04-193r"]["major_rule_rins_json"]) == (
        0, '["0910-AC40"]')

    journal = Journal()
    undone = _build(tmp_path, monkeypatch, None, evidence=journal, product_pages_undo="2026-10-19")
    assert undone["gao-20-381"] == package and undone["gao-26-1"] == new
    # The letter read from that day's page bytes goes with it; a letter read from elsewhere and another day's
    # page read stay.
    assert undone["gao-04-193r"] == rule and undone["gao-26-2"] == read["gao-26-2"]
    assert undone["gao-01-300r"] == lettered
    event = journal.events["gao-product-page-undo"]
    assert (event["selector"], event["rows"], event["letters"], event["restored_page_count"],
            event["restored_recommendation_count"]) == ("2026-10-19", 4, 1, 4, 4)
    assert [row["report_id"] for row in product_pages.newest_first(undone.values())] == [
        "gao-26-1", "gao-04-193r", "gao-01-300r", "gao-20-381"]


@pytest.mark.parametrize(
    "selector", ["2026-10-19T17", "2026-10-19T17:22:11Z", "gao-product-page-details/2"]
)
def test_an_undo_can_name_an_hour_one_read_or_every_read_under_a_rule(tmp_path, monkeypatch, selector):
    rows = [_row("gao-26-1", **_full(page_count=9),
                 product_page_json=_reading(observed_at="2026-10-19T17:22:11Z", before={"page_count": None})),
            _row("gao-26-2", **_full(page_count=9),
                 product_page_json=_reading("gao-product-page-details/3", observed_at="2026-10-19T03:00:00Z",
                                            before={"page_count": None}))]
    undone = _build(tmp_path, monkeypatch, rows, product_pages_undo=selector)
    assert (undone["gao-26-1"]["page_count"], undone["gao-26-1"]["product_page_json"]) == (None, None)
    assert undone["gao-26-2"] == rows[1]


@pytest.mark.parametrize("selector", ["2026", "2026-10", "20261019", "yesterday", "", "gao-major-rule-letter/3"])
def test_an_undo_refuses_anything_wider_than_a_day_that_is_not_this_readers_rule(tmp_path, selector):
    with pytest.raises(ValueError, match="GAO_PRODUCT_PAGES_UNDO"):
        module.build_gao_reports(tmp_path, product_pages_undo=selector)


# The rollup and its workflow.


def test_the_rollup_passes_the_switch_and_the_undo_only_as_its_environment_states_them(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups import gao_reports as rollup

    calls = []
    monkeypatch.setattr(rollup, "build_gao_reports",
                        lambda *_, **kwargs: calls.append((kwargs["product_pages"], kwargs["product_pages_undo"])))
    for switch, undo in (("", ""), ("false", ""), ("true", ""), ("", "2026-10-19")):
        monkeypatch.setenv("GAO_PRODUCT_PAGES", switch)
        monkeypatch.setenv("GAO_PRODUCT_PAGES_UNDO", undo)
        rollup.GaoReportsRollup(output_dir=tmp_path).build(tmp_path)
    assert calls == [(False, None), (False, None), (True, None), (False, "2026-10-19")]
    # The read of GAO's open recommendations is stated; test_hosted_rollups holds its cron after that table's.
    assert rollup.GaoReportsRollup.soft_inputs == ("gao_recommendations.parquet",)


def test_the_workflow_keeps_the_read_off_and_gives_the_job_no_zyte_token():
    """The switch is one value in the caller. While it is false the reusable workflow passes the flag as false and
    its ZYTE_TOKEN expression is empty for this rollup."""
    caller = yaml.safe_load((WORKFLOWS / "rollup-gao-reports.yml").read_text())
    assert caller["jobs"]["run"]["with"]["gao_product_pages"] is False
    reusable = yaml.safe_load((WORKFLOWS / "_rollup.yml").read_text())
    declared = reusable[True]["workflow_call"]["inputs"]["gao_product_pages"]
    assert (declared["default"], declared["type"]) == (False, "boolean")
    [run] = [step for step in reusable["jobs"]["rollup"]["steps"] if step.get("name") == "Run rollup"]
    assert run["env"]["GAO_PRODUCT_PAGES"] == "${{ inputs.gao_product_pages }}"
    assert "inputs.command == 'run-rollup-gao-reports' && inputs.gao_product_pages)" in run["env"]["ZYTE_TOKEN"]


def test_the_undo_is_a_manual_input_that_a_scheduled_run_never_sets():
    caller = yaml.safe_load((WORKFLOWS / "rollup-gao-reports.yml").read_text())
    assert caller[True]["workflow_dispatch"]["inputs"]["product_pages_undo"]["default"] == ""
    assert caller["jobs"]["run"]["with"]["gao_product_pages_undo"] == (
        "${{ github.event_name == 'workflow_dispatch' && inputs.product_pages_undo || '' }}")
    reusable = yaml.safe_load((WORKFLOWS / "_rollup.yml").read_text())
    assert reusable[True]["workflow_call"]["inputs"]["gao_product_pages_undo"]["default"] == ""
    [run] = [step for step in reusable["jobs"]["rollup"]["steps"] if step.get("name") == "Run rollup"]
    assert run["env"]["GAO_PRODUCT_PAGES_UNDO"] == "${{ inputs.gao_product_pages_undo }}"
