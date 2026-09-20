"""Hermetic tests for the senate-expenditures transform's own seam.

No network and no PDF: the discovery reader, the granule acquirer and the page
extractor are all stubbed. The extractor is stubbed rather than run because
what PyMuPDF's ``find_tables()`` recovers from a real Senate volume, and what
``shape_senate_expenditure_rows`` makes of it, are established in spicy-docs
against two committed page ranges of two real reports. Re-running that here
would need a megabyte of fixture to re-assert someone else's measurement.

What this repository owns, and what these tests hold: which CDOC packages are
these reports, which granule files are read and which are skipped, that the
page cap is applied lazily and reported as the contract asks, that one
``page_context`` serves every table on a page, that a refusal is a counted
file and a credential refusal aborts, and that the rows land under the
contract's schema.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.transforms.build_senate_expenditures import (
    MAX_PAGES_PER_FILE,
    NAME,
    REPORT_TITLE,
    build_senate_expenditures,
)

PACKAGE = "GPO-CDOC-119sdoc3"
PART_ONE = f"{PACKAGE}-1"
PART_TWO = f"{PACKAGE}-2"

#: The nine-column summary grid, with the two header labels that make row 0 a
#: header row and one entry row under it. The classification is spicy-docs'; a
#: real shape is used so the shaper is exercised rather than mocked.
HEADER = ("APPROPRIATION TITLE", "NO.", "FUNDS AVAILABLE AS OF October 1, 2024")
ENTRY = ("SENATE POLICY COMMITTEES\n2024", "0100", "1,234.56")

PAGE_TEXT = "SUMMARY OF TRANSACTIONS BY APPROPRIATIONS\nSENATOR TIM KAINE\nFunding Year 2024\nSALARIES\nA-7"


class _Box:
    x0, y0, x1, y1 = 0.1, 0.1, 0.9, 0.9


class _Table:
    def __init__(self, page: int):
        self.page = page
        self.bbox = _Box()
        self.cells = (HEADER, ENTRY)
        self.row_count = 2
        self.column_count = 3


class _PageResult:
    def __init__(self, number: int, page_count: int, tables: int = 1):
        self.metadata = {"page": number, "page_count": page_count}
        self.text = PAGE_TEXT
        self.tables = tuple(_Table(number) for _ in range(tables))


class _Extractor:
    """A page stream that records how far it was actually read.

    ``extracted`` is the point: the transform must close the generator at the
    cap, so a 1,335-page volume costs eighty pages and not 1,335.
    """

    def __init__(self, page_count: int, tables_per_page: int = 1):
        self.page_count = page_count
        self.tables_per_page = tables_per_page
        self.extracted = 0
        self.contexts = 0

    def extract(self, source, *, media_type):
        for number in range(1, self.page_count + 1):
            self.extracted += 1
            yield _PageResult(number, self.page_count, self.tables_per_page)


class _Page:
    def __init__(self, records):
        self.records = tuple(records)
        self.declared_count = len(self.records)
        self.next_url = None


class _Reader:
    def __init__(self, packages, granules):
        self._packages = packages
        self._granules = granules
        self.granule_calls: list[str] = []

    def packages(self, url, *, max_pages=1):
        return iter([_Page(self._packages)])

    def granules(self, url, *, max_pages=1):
        package_id = url.split("/packages/", 1)[1].split("/", 1)[0]
        self.granule_calls.append(package_id)
        if isinstance(self._granules, Exception):
            raise self._granules
        return iter([_Page(self._granules.get(package_id, []))])


class _Body:
    class _Identity:
        media_type = "application/pdf"

    def __init__(self):
        self.format = "pdf"
        self.body = self._Identity()
        self.body_capture = type("C", (), {"body": b"%PDF-1.4 not really, the extractor is stubbed"})()


class _Acquirer:
    def __init__(self, refuse: dict[str, Exception] | None = None):
        self.refuse = refuse or {}
        self.asked: list[str] = []

    def acquire_granule(self, package_id, granule_id, *, max_bytes=None):
        self.asked.append(granule_id)
        if granule_id in self.refuse:
            raise self.refuse[granule_id]
        return _Body()


def _no_download(remote_key: str, local_path: Path) -> bool:
    return False


def _listing(package_id=PACKAGE, title="Report of the Secretary of the Senate: October 1, 2024 to March 31, 2025"):
    return {"packageId": package_id, "title": title}


def _granule(granule_id, granule_class="CONTENT"):
    return {"granuleId": granule_id, "granuleClass": granule_class, "title": granule_id}


def _build(tmp_path, reader, acquirer, extractor, **kwargs):
    return build_senate_expenditures(
        tmp_path, reader=reader, acquirer=acquirer, extractor=extractor, download_prior=_no_download, **kwargs
    )


def _rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


# --------------------------------------------------------------------------
# The title rule.
# --------------------------------------------------------------------------

#: The five titles the live ``published`` CDOC window 2024-01-01..2026-09-20
#: served on 2026-09-20 under a ``GPO-CDOC-`` id, abbreviated.
REAL_REPORT_TITLES = (
    "Report of the Secretary of the Senate: October 1, 2025 to March 31, 2026",
    "Report of the Secretary of the Senate: April 1, 2025 to September 30, 2025",
    "Report of the Secretary of the Senate: October 1, 2024 to March 31, 2025",
    "Report of the Secretary of the Senate: April 1, 2024 to September 30, 2024",
    "Report of the Secretary of the Senate: October 1, 2023 to March 31, 2024",
)

#: Titles from the same CDOC walk that are not this report. 291 rows were
#: walked and 5 matched, so the rule's selectivity is the thing to hold.
OTHER_CDOC_TITLES = (
    "Report of the Secretary of the Treasury on the State of the Finances",
    "Senate Manual Containing the Standing Rules",
    "Memorial Addresses and Other Tributes",
)


@pytest.mark.parametrize("title", REAL_REPORT_TITLES)
def test_the_title_rule_matches_every_real_report(title):
    assert REPORT_TITLE.search(title) is not None


@pytest.mark.parametrize("title", OTHER_CDOC_TITLES)
def test_the_title_rule_refuses_the_rest_of_the_collection(title):
    assert REPORT_TITLE.search(title) is None


# --------------------------------------------------------------------------
# The pass itself.
# --------------------------------------------------------------------------


def test_rows_publish_under_the_contract_from_every_part(tmp_path):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=3))

    assert out.name == f"{NAME}.parquet"
    assert pq.read_table(out).schema.names == list(TABLE_CONTRACTS[NAME].columns)
    rows = _rows(out)
    assert rows, "the stubbed grid has a header row and an entry row"
    assert {row["file_name"] for row in rows} == {f"{PART_ONE}.pdf", f"{PART_TWO}.pdf"}
    assert {row["package_id"] for row in rows} == {PACKAGE}
    # The page count is the PDF's own: a granule summary states no extent.
    assert {row["page_count"] for row in rows} == {"3"}


def test_the_full_report_is_not_read_twice_under_a_second_file_name(tmp_path):
    """A granule whose id is the package id is the Full Report, which duplicates Part I."""
    reader = _Reader([_listing()], {PACKAGE: [_granule(PACKAGE), _granule(PART_ONE)]})
    acquirer = _Acquirer()
    _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    assert acquirer.asked == [PART_ONE]


def test_a_granule_that_is_not_content_is_not_fetched(tmp_path):
    reader = _Reader([_listing()], {PACKAGE: [_granule("GPO-CDOC-119sdoc3-0", "METADATA"), _granule(PART_ONE)]})
    acquirer = _Acquirer()
    _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    assert acquirer.asked == [PART_ONE]


def test_a_package_that_is_not_this_report_is_never_asked_for_granules(tmp_path):
    reader = _Reader(
        [
            _listing("GPO-CDOC-119sdoc9", "Senate Manual Containing the Standing Rules"),
            _listing("CDOC-119sdoc3", "Report of the Secretary of the Senate: a bare CDOC id"),
            _listing(),
        ],
        {PACKAGE: [_granule(PART_ONE)]},
    )
    acquirer = _Acquirer()
    _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    # The bare `CDOC-` id matches the title but not the reprint prefix, and the
    # Senate Manual matches the prefix but not the title. Only one is asked for.
    assert reader.granule_calls == [PACKAGE]


def test_the_page_cap_stops_the_extraction_rather_than_slicing_it(tmp_path):
    """The generator is closed at the cap, so pages past it are never extracted.

    Slicing a fully-materialized stream would give the same rows and cost the
    whole 1,335-page table detection, which is the whole point of the cap.
    """
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    extractor = _Extractor(page_count=MAX_PAGES_PER_FILE + 500)
    out = _build(tmp_path, reader, _Acquirer(), extractor)

    assert extractor.extracted == MAX_PAGES_PER_FILE
    rows = _rows(out)
    assert {row["pages_read"] for row in rows} == {str(MAX_PAGES_PER_FILE)}
    assert {row["page_count"] for row in rows} == {str(MAX_PAGES_PER_FILE + 500)}
    # Every count over a capped read is a floor, and the row says so.
    assert {row["pages_capped"] for row in rows} == {"true"}


def test_a_short_file_is_not_capped(tmp_path):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    extractor = _Extractor(page_count=5)
    out = _build(tmp_path, reader, _Acquirer(), extractor)
    assert extractor.extracted == 5
    assert {row["pages_capped"] for row in _rows(out)} == {"false"}


def test_every_table_on_a_page_gets_its_own_ordinal_and_the_pages_own_digest(tmp_path):
    """``table_ordinal`` is per page, and one page text is hashed once for all its tables."""
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    out = _build(tmp_path, reader, _Acquirer(), _Extractor(page_count=1, tables_per_page=3))
    rows = _rows(out)
    assert sorted({row["table_ordinal"] for row in rows}) == ["0", "1", "2"]
    assert len({row["text_sha256"] for row in rows}) == 1, "one page, one text digest"


def test_a_file_already_published_is_not_read_again(tmp_path):
    """The print is a fixed artifact: re-reading it would spend table detection for nothing."""
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    out.rename(tmp_path / f"_{NAME}_prior.parquet")

    acquirer.asked.clear()
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    assert acquirer.asked == []


def test_a_refused_file_is_counted_and_its_sibling_still_publishes(tmp_path):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    acquirer = _Acquirer(refuse={PART_ONE: PagedJsonSourceError("the publisher served its error page")})
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    assert acquirer.asked == [PART_ONE, PART_TWO]
    assert {row["file_name"] for row in _rows(out)} == {f"{PART_TWO}.pdf"}


def test_a_refused_granule_list_does_not_stop_the_walk(tmp_path):
    reader = _Reader([_listing()], PagedJsonSourceError("the granule route refused"))
    out = _build(tmp_path, reader, _Acquirer(), _Extractor(page_count=2))
    assert _rows(out) == []


def test_a_credential_refusal_aborts_the_run(tmp_path):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    acquirer = _Acquirer(refuse={PART_ONE: CredentialRefusedError("the key was refused")})
    with pytest.raises(CredentialRefusedError):
        _build(tmp_path, reader, acquirer, _Extractor(page_count=2))


def test_the_per_run_cap_bounds_the_packages(tmp_path):
    second = "GPO-CDOC-119sdoc6"
    reader = _Reader(
        [_listing(), _listing(second, "Report of the Secretary of the Senate: October 1, 2025 to March 31, 2026")],
        {PACKAGE: [_granule(PART_ONE)], second: [_granule(f"{second}-1")]},
    )
    acquirer = _Acquirer()
    _build(tmp_path, reader, acquirer, _Extractor(page_count=2), max_packages=1)
    assert acquirer.asked == [PART_ONE]
