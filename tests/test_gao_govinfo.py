"""GovInfo GAOREPORTS listing rows to gao_reports rows: identity, type, scope and twins.

Every expected product id below was read from its gao.gov product page on
2026-09-28, and every package id from GovInfo's GAOREPORTS listing that day.
"""

from __future__ import annotations

import pytest
from spicy_docs.sources.gao.native import MAX_PRODUCT_ID_LENGTH

from spicy_regs.sources import gao_govinfo
from spicy_regs.sources.gao_govinfo import GaoGovInfoError, is_report, read_history, report_id, report_rows, report_type


def _package(package_id: str, doc_class: str = "REPORT", **fields) -> dict:
    return {"packageId": package_id, "docClass": doc_class, "title": f"Title of {package_id}",
            "dateIssued": "1998-06-05", **fields}


@pytest.mark.parametrize(
    ("package_id", "product_id"),
    [
        ("GAOREPORTS-GAO-08-919R", "gao-08-919r"),
        ("GAOREPORTS-T-RCED-94-121", "t-rced-94-121"),
        ("GAOREPORTS-GAO-01-1171t", "gao-01-1171t"),
        # GovInfo's spelling of the "GAO/" citation prefix; gao.gov drops it.
        ("GAOREPORTS-GAO-HEHS-00-73", "hehs-00-73"),
        ("GAOREPORTS-GAO-RCED-95-87R", "rced-95-87r"),
        ("GAOREPORTS-GAO-T-GGD-98-185", "t-ggd-98-185"),
        # A joint report's "/" is "-" on GovInfo; gao.gov joins the division codes.
        ("GAOREPORTS-RCED-AIMD-94-221FS", "rcedaimd-94-221fs"),
        ("GAOREPORTS-GGD-RCED-94-272", "ggdrced-94-272"),
        ("GAOREPORTS-T-RCED-AIMD-95-131", "t-rcedaimd-95-131"),
        # spicy-docs 0.54.0's grammar admits the dots GAO's own product URLs carry (aimd-10.1.22).
        ("GAOREPORTS-FILE-291573.7", "file-291573.7"),
    ],
)
def test_a_package_id_maps_to_the_gao_product_id(package_id, product_id):
    assert report_id(package_id) == product_id


@pytest.mark.parametrize(
    "package_id",
    [
        "CRPT-118hrpt1",
        "GAOREPORTS-GAO-IMTEC-11_1_1",
        # SpicyDocs' product-id grammar is the one in force, its length bound included (256 from round 6).
        "GAOREPORTS-" + "A" * (MAX_PRODUCT_ID_LENGTH + 1),
    ],
)
def test_a_package_outside_the_collection_or_without_a_product_reading_refuses(package_id):
    with pytest.raises(GaoGovInfoError):
        report_id(package_id)


@pytest.mark.parametrize(
    ("product_id", "expected"),
    [
        ("t-rced-94-121", "Testimony"),
        ("t-rcedaimd-95-131", "Testimony"),
        ("gao-02-677t", "Testimony"),
        ("aimd-98-305t", "Testimony"),  # MODS types it Testimony too
        ("gao-08-919r", "Report"),
        ("nsiad-00-228br", "Report"),
        ("gao-04-387sp", "Report"),
    ],
)
def test_testimony_is_typed_by_its_number(product_id, expected):
    assert report_type(product_id) == expected


def test_decisions_and_b_numbers_are_out_of_scope_and_an_unknown_class_refuses():
    assert is_report(_package("GAOREPORTS-GAO-08-919R"))
    assert not is_report(_package("GAOREPORTS-B-400379", "COMPTROLLERDECISION"))
    # GovInfo classes this one REPORT; gao.gov labels it a decision.
    assert not is_report(_package("GAOREPORTS-B-291868"))
    with pytest.raises(GaoGovInfoError, match="docClass"):
        is_report(_package("GAOREPORTS-GAO-08-919R", "OPINION"))


@pytest.mark.parametrize("reverse", [False, True])
def test_twins_keep_the_plainer_spelling_in_either_order(reverse):
    packages = [
        _package("GAOREPORTS-GAO-RCED-95-87R", title="CRP Payments"),
        _package("GAOREPORTS-RCED-95-87R", title="Resources, Community, and Economic Development Division"),
        _package("GAOREPORTS-GAO-01-1171t"),
        _package("GAOREPORTS-GAO-01-1171T"),
    ]
    rows, counts = report_rows(reversed(packages) if reverse else packages)
    by_id = {row["report_id"]: row for row in rows}
    assert set(by_id) == {"rced-95-87r", "gao-01-1171t"}
    assert by_id["rced-95-87r"]["url"] == "https://www.govinfo.gov/app/details/GAOREPORTS-RCED-95-87R"
    assert by_id["gao-01-1171t"]["url"].endswith("GAOREPORTS-GAO-01-1171T")
    assert counts["twins_collapsed"] == 2


def test_rows_carry_source_and_the_listing_fields_and_leave_the_rest_null():
    rows, counts = report_rows([
        _package("GAOREPORTS-T-RCED-94-121", title="U.S. Department of Agriculture", dateIssued="1994-02-09"),
        _package("GAOREPORTS-B-400379", "COMPTROLLERDECISION"),
    ])
    assert rows == [{
        "report_id": "t-rced-94-121",
        "title": "U.S. Department of Agriculture",
        "report_type": "Testimony",
        "published_date": "1994-02-09",
        "abstract": None,
        "agencies_json": None,
        "topics_json": None,
        "url": "https://www.govinfo.gov/app/details/GAOREPORTS-T-RCED-94-121",
        "source": "govinfo",
        "product_type": None,
        "report_number": None,
    }]
    assert counts == {"listed": 2, "decisions_left_out": 1, "rows": 1}


class _Page:
    def __init__(self, records):
        self.records = tuple(records)


class ListingReader:
    """Serves the collection listing in pages and records the URL asked for."""

    def __init__(self, *pages: list[dict]):
        self._pages = pages
        self.urls: list[str] = []

    def packages(self, url, *, max_pages=1):
        self.urls.append(url)
        return iter(_Page(page) for page in self._pages)


def test_read_history_walks_the_whole_collection_route_at_the_largest_page():
    reader = ListingReader([_package("GAOREPORTS-GAO-08-919R")], [_package("GAOREPORTS-T-RCED-94-121")])
    rows, _ = read_history(reader)
    assert [row["report_id"] for row in rows] == ["gao-08-919r", "t-rced-94-121"]
    assert reader.urls == [
        "https://api.govinfo.gov/collections/GAOREPORTS/1990-01-01T00:00:00Z?offsetMark=*&pageSize=1000"
    ]
    assert gao_govinfo.PAGE_SIZE == 1000
