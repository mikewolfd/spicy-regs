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

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.transport.credentials import CredentialRefusedError

import spicy_regs.transforms.build_senate_expenditures as transform
from spicy_regs.transforms.build_senate_expenditures import (
    MAX_PAGES_PER_FILE,
    NAME,
    build_senate_expenditures,
)
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints

PACKAGE = "GPO-CDOC-119sdoc3"
PART_ONE = f"{PACKAGE}-1"
PART_TWO = f"{PACKAGE}-2"

#: The nine-column summary grid, with the two header labels that make row 0 a
#: header row and one entry row under it. The classification is spicy-docs'; a
#: real shape is used so the shaper is exercised rather than mocked.
HEADER = ("APPROPRIATION TITLE", "NO.", "FUNDS AVAILABLE AS OF October 1, 2024")
ENTRY = ("SENATE POLICY COMMITTEES\n2024", "0100", "1,234.56")

ORG_HEADER = ("TOTAL FUNDING YTD ($)", "NET EXPENDITURES FOR THE PERIOD OF 10/01/2024 THRU 03/31/2025")
ORG_ENTRY = ("ORGANIZATION TOTALS $100.00", None)
PAYEE_HEADER = ("DOCUMENT NO.", "DATE POSTED", "PAYEE NAME")
PAYEE_ENTRY = ("D12345 10/15/2024 ACME CORP 42.50", None, None)

PAGE_TEXT = "SUMMARY OF TRANSACTIONS BY APPROPRIATIONS\nSENATOR TIM KAINE\nFunding Year 2024\nSALARIES\nA-7"
B_PAGE_TEXT = "DETAILED AND SUMMARY STATEMENT OF EXPENDITURES\nSENATOR TIM KAINE\nFunding Year 2024\nSALARIES\nB-10"


class _Box:
    x0, y0, x1, y1 = 0.1, 0.1, 0.9, 0.9


class _Table:
    def __init__(self, page: int, cells: tuple[tuple[str | None, ...], ...] = (HEADER, ENTRY)):
        self.page = page
        self.bbox = _Box()
        self.cells = cells
        self.row_count = len(cells)
        self.column_count = len(cells[0]) if cells else 0


class _PageResult:
    def __init__(
        self,
        number: int,
        page_count: int,
        tables: int | tuple[_Table, ...] = 1,
        *,
        text: str = PAGE_TEXT,
    ):
        self.metadata = {"page": number, "page_count": page_count}
        self.text = text
        self.tables = tuple(_Table(number) for _ in range(tables)) if isinstance(tables, int) else tables


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
        marker = source.decode("utf-8", errors="ignore")
        text = PAGE_TEXT if PART_TWO not in marker else PAGE_TEXT.replace("A-7", "A-8")
        for number in range(1, self.page_count + 1):
            self.extracted += 1
            yield _PageResult(number, self.page_count, self.tables_per_page, text=text)


class _Page:
    def __init__(self, records):
        self.records = tuple(records)
        self.declared_count = len(self.records)
        self.next_url = None


class _Reader:
    """Serves one package page and per-package granule pages; an ``Exception`` value for ``granules`` is raised."""

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

    def __init__(self, granule_id: str = ""):
        self.format = "pdf"
        self.body = self._Identity()
        payload = (
            f"%PDF-1.4 {granule_id} not really, the extractor is stubbed".encode()
            if granule_id
            else b"%PDF-1.4 not really, the extractor is stubbed"
        )
        self.body_capture = type("C", (), {"body": payload})()


class _Acquirer:
    """Serves a PDF body per granule, raising the configured refusal for chosen granule ids."""

    def __init__(self, refuse: dict[str, Exception] | None = None):
        self.refuse = refuse or {}
        self.asked: list[str] = []
        self.prefers: list[tuple[str, ...] | None] = []

    def acquire_granule(self, package_id, granule_id, *, prefer=None, max_bytes=None):
        self.asked.append(granule_id)
        self.prefers.append(None if prefer is None else tuple(prefer))
        if granule_id in self.refuse:
            raise self.refuse[granule_id]
        return _Body(granule_id)


def _no_download(remote_key: str, local_path: Path) -> bool:
    return False


def _listing(
    package_id=PACKAGE,
    title="Report of the Secretary of the Senate: October 1, 2024 to March 31, 2025",
    modified="2026-09-20T00:00:00Z",
):
    return {"packageId": package_id, "title": title, "lastModified": modified}


def _granule(granule_id, granule_class="CONTENT"):
    return {"granuleId": granule_id, "granuleClass": granule_class, "title": granule_id}


def _build(tmp_path, reader, acquirer, extractor, **kwargs):
    """Run the transform over the given stubs and no prior download."""
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
    from spicy_docs.sources.govinfo.senate_expenditure_reports import is_senate_expenditure_report

    assert is_senate_expenditure_report("GPO-CDOC-119sdoc6", title)


@pytest.mark.parametrize("title", OTHER_CDOC_TITLES)
def test_the_title_rule_refuses_the_rest_of_the_collection(title):
    from spicy_docs.sources.govinfo.senate_expenditure_reports import is_senate_expenditure_report

    assert not is_senate_expenditure_report("GPO-CDOC-119sdoc6", title)


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
    """A successful prefix with unchanged source, policy, and rule can be skipped."""
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


def test_a_fully_held_package_does_not_consume_a_cap_slot(tmp_path):
    """The permanent stall this rollup shipped with, and the fix, on one run pair.

    Five packages match and the cap is four. The first version counted a
    package against the cap as soon as its granule list answered — before the
    held-file check — so the second run re-listed the four published packages,
    reached the cap on them, and broke before the fifth. The fifth was
    therefore unreachable by **any** number of runs, while
    ``docs/tables/senate_expenditures.md`` promised it would be read next run.
    Measured on the 2026-09-20 receipt, where the fifth was
    ``GPO-CDOC-118sdoc11``.

    Run one fills four; run two must reach the fifth and nothing else.
    """
    packages = [f"GPO-CDOC-119sdoc{n}" for n in (3, 5, 6)] + ["GPO-CDOC-118sdoc13", "GPO-CDOC-118sdoc11"]
    listings = [_listing(package) for package in packages]
    granules = {package: [_granule(f"{package}-1")] for package in packages}

    acquirer = _Acquirer()
    out = _build(tmp_path, _Reader(listings, granules), acquirer, _Extractor(page_count=2), max_packages=4)
    first = {row["package_id"] for row in _rows(out)}
    assert len(first) == 4, "the cap is the cap"
    assert packages[4] not in first, "the fifth is what the second run must reach"

    out.rename(tmp_path / f"_{NAME}_prior.parquet")
    acquirer.asked.clear()
    reader = _Reader(listings, granules)
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2), max_packages=4)

    assert acquirer.asked == [f"{packages[4]}-1"], "only the unread package costs a body fetch"
    assert {row["package_id"] for row in _rows(out)} == set(packages), "all five are published"
    # The held packages still cost their granule list — the window is walked
    # whole every run so a refusal is retried — and that is all they cost.
    assert reader.granule_calls == packages


def test_a_partly_held_package_still_reads_its_unread_file(tmp_path):
    """A package is not all-or-nothing: Part II is read while Part I is skipped.

    The held check is per file because the identity is per file, so a package
    whose second part the publisher added later is picked up without re-reading
    the first.
    """
    granules = {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]}
    acquirer = _Acquirer()
    out = _build(tmp_path, _Reader([_listing()], granules), acquirer, _Extractor(page_count=2))

    # A prior holding Part I alone, as if Part II had just been published.
    table = pq.read_table(out)
    kept = [row for row in table.to_pylist() if row["file_name"] == f"{PART_ONE}.pdf"]
    assert kept, "the fixture must actually hold Part I"
    checkpoints = [row for row in read_checkpoints(out, NAME) if row["file_name"] == f"{PART_ONE}.pdf"]
    metadata = checkpoint_metadata(out, NAME, checkpoints)
    prior_table = pa.Table.from_pylist(kept, schema=table.schema).replace_schema_metadata(metadata)
    pq.write_table(prior_table, tmp_path / f"_{NAME}_prior.parquet")
    out.unlink()

    acquirer.asked.clear()
    out = _build(tmp_path, _Reader([_listing()], granules), acquirer, _Extractor(page_count=2))
    assert acquirer.asked == [PART_TWO], "only the unheld part is fetched"
    assert {row["file_name"] for row in _rows(out)} == {f"{PART_ONE}.pdf", f"{PART_TWO}.pdf"}


def _prior(out: Path) -> None:
    out.replace(out.with_name(f"_{NAME}_prior.parquet"))


def test_changed_rule_replaces_all_old_file_rows_then_skips_stable_rerun(tmp_path, monkeypatch):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    old_rows = _rows(out)
    assert len(old_rows) == 4
    _prior(out)
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "003")

    class CorrectedExtractor(_Extractor):
        def extract(self, source, *, media_type):
            for page in super().extract(source, media_type=media_type):
                page.text += "\nCorrected page reading"
                page.tables[0].cells = (HEADER,)
                page.tables[0].row_count = 1
                yield page

    out = _build(tmp_path, reader, acquirer, CorrectedExtractor(page_count=2))
    corrected = _rows(out)
    assert len(corrected) == 2
    assert {row["row_kind"] for row in corrected} == {"header"}
    assert not {row["text_sha256"] for row in old_rows} & {row["text_sha256"] for row in corrected}
    _prior(out)
    acquirer.asked.clear()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    assert acquirer.asked == []
    assert _rows(out) == corrected


def test_zero_result_removes_previous_findings_but_refused_sibling_keeps_its_rows(tmp_path, monkeypatch):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    out = _build(tmp_path, reader, _Acquirer(), _Extractor(page_count=2))
    old_sibling = [row for row in _rows(out) if row["file_name"] == f"{PART_TWO}.pdf"]
    assert old_sibling
    _prior(out)
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "003")
    acquirer = _Acquirer(refuse={PART_TWO: OSError("temporary read failure")})
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2, tables_per_page=0))
    assert _rows(out) == old_sibling
    reads = {row["file_name"]: row for row in read_checkpoints(out, NAME)}
    assert reads[f"{PART_ONE}.pdf"]["processing_version"]["reader"] == "003"
    assert reads[f"{PART_TWO}.pdf"]["processing_version"]["reader"] == "002"

    _prior(out)
    acquirer.asked.clear()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2, tables_per_page=0))
    assert acquirer.asked == [PART_TWO], "empty success is stable; the failed sibling remains retryable"
    assert _rows(out) == old_sibling


def test_a_successful_empty_file_is_checkpointed_in_an_empty_output(tmp_path):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2, tables_per_page=0))
    assert _rows(out) == []
    assert len(read_checkpoints(out, NAME)) == 1
    _prior(out)
    acquirer.asked.clear()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    assert acquirer.asked == []
    assert _rows(out) == []


def test_raising_page_limit_replaces_the_old_prefix_with_the_larger_prefix(tmp_path, monkeypatch):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    monkeypatch.setattr(transform, "MAX_PAGES_PER_FILE", 2)
    out = _build(tmp_path, reader, _Acquirer(), _Extractor(page_count=5))
    assert {row["pages_read"] for row in _rows(out)} == {"2"}
    _prior(out)
    monkeypatch.setattr(transform, "MAX_PAGES_PER_FILE", 4)
    extractor = _Extractor(page_count=5)
    out = _build(tmp_path, reader, _Acquirer(), extractor)
    assert extractor.extracted == 4
    assert len(_rows(out)) == 8
    assert {row["pages_read"] for row in _rows(out)} == {"4"}
    assert {row["pages_capped"] for row in _rows(out)} == {"true"}


@pytest.mark.parametrize("changed", ["package", "granule"])
def test_publisher_changes_trigger_a_fresh_read(tmp_path, changed):
    listing = _listing()
    granule = _granule(PART_ONE) | {"lastModified": "2026-09-19T00:00:00Z"}
    reader = _Reader([listing], {PACKAGE: [granule]})
    out = _build(tmp_path, reader, _Acquirer(), _Extractor(page_count=2))
    _prior(out)
    (listing if changed == "package" else granule)["lastModified"] = "2026-09-21T00:00:00Z"
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=1))
    assert acquirer.asked == [PART_ONE]
    assert len(_rows(out)) == 2


def test_without_a_publisher_version_each_run_rechecks_the_file(tmp_path):
    reader = _Reader([_listing(modified=None)], {PACKAGE: [_granule(PART_ONE)]})
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    _prior(out)
    acquirer.asked.clear()
    _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    assert acquirer.asked == [PART_ONE]


@pytest.mark.parametrize("failure", ["truncated", "exception", "wrong-page", "empty-stream"])
def test_incomplete_page_stream_preserves_prior_results_and_checkpoint(tmp_path, monkeypatch, failure):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    out = _build(tmp_path, reader, _Acquirer(), _Extractor(page_count=2))
    expected = _rows(out)
    expected_reads = read_checkpoints(out, NAME)
    _prior(out)
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "003")

    class IncompleteExtractor:
        def extract(self, source, *, media_type):
            if failure == "empty-stream":
                return
            yield _PageResult(2 if failure == "wrong-page" else 1, 2)
            if failure == "exception":
                raise OSError("extraction interrupted")

    out = _build(tmp_path, reader, _Acquirer(), IncompleteExtractor())
    assert _rows(out) == expected
    assert read_checkpoints(out, NAME) == expected_reads


def test_legacy_rows_without_checkpoints_are_reprocessed(tmp_path):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    out = _build(tmp_path, reader, _Acquirer(), _Extractor(page_count=2))
    pq.write_table(pq.read_table(out).replace_schema_metadata(None), tmp_path / f"_{NAME}_prior.parquet")
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=1))
    assert acquirer.asked == [PART_ONE]
    assert len(_rows(out)) == 2


def test_late_credential_abort_preserves_the_original_prior_file(tmp_path, monkeypatch):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    out = _build(tmp_path, reader, _Acquirer(), _Extractor(page_count=2))
    expected_bytes = out.read_bytes()
    _prior(out)
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "003")
    acquirer = _Acquirer(refuse={PART_TWO: CredentialRefusedError("key refused after first file")})
    with pytest.raises(CredentialRefusedError):
        _build(tmp_path, reader, acquirer, _Extractor(page_count=2, tables_per_page=0))
    assert acquirer.asked == [PART_ONE, PART_TWO]
    assert (tmp_path / f"_{NAME}_prior.parquet").read_bytes() == expected_bytes


@pytest.mark.parametrize("legacy", [False, True])
def test_stale_prior_files_outside_discovery_are_corrected(tmp_path, monkeypatch, legacy):
    granules = {PACKAGE: [_granule(PART_ONE)]}
    out = _build(tmp_path, _Reader([_listing()], granules), _Acquirer(), _Extractor(page_count=2))
    if legacy:
        pq.write_table(pq.read_table(out).replace_schema_metadata(None), tmp_path / f"_{NAME}_prior.parquet")
    else:
        _prior(out)
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "003")
    reader = _Reader([], granules)
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2, tables_per_page=0))
    assert reader.granule_calls == [PACKAGE]
    assert acquirer.asked == [PART_ONE]
    assert _rows(out) == []
    checkpoint = read_checkpoints(out, NAME)[0]
    assert checkpoint["last_modified"] is None, "an old source date is not evidence about the new read"
    assert checkpoint["processing_version"]["reader"] == "003"
    _prior(out)
    reader.granule_calls.clear()
    acquirer.asked.clear()
    _build(tmp_path, reader, acquirer, _Extractor(page_count=2))
    assert reader.granule_calls == [], "a current rule needs no correction outside the discovery scope"
    assert acquirer.asked == []


def test_an_outside_discovery_file_absent_from_granules_keeps_its_prior_result(tmp_path, monkeypatch):
    out = _build(
        tmp_path, _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]}), _Acquirer(), _Extractor(page_count=2)
    )
    expected = _rows(out)
    expected_reads = read_checkpoints(out, NAME)
    _prior(out)
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "003")
    out = _build(tmp_path, _Reader([], {PACKAGE: []}), _Acquirer(), _Extractor(page_count=2))
    assert _rows(out) == expected
    assert read_checkpoints(out, NAME) == expected_reads


def test_missing_outside_discovery_sibling_does_not_reread_the_corrected_file(tmp_path, monkeypatch):
    granules = {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]}
    out = _build(tmp_path, _Reader([_listing()], granules), _Acquirer(), _Extractor(page_count=2))
    old_sibling = [row for row in _rows(out) if row["file_name"] == f"{PART_TWO}.pdf"]
    assert old_sibling
    _prior(out)
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "003")
    reader = _Reader([], {PACKAGE: [_granule(PART_ONE)]})
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=1))
    assert acquirer.asked == [PART_ONE]
    corrected = _rows(out)
    assert [row for row in corrected if row["file_name"] == f"{PART_TWO}.pdf"] == old_sibling
    _prior(out)

    acquirer.asked.clear()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=1, tables_per_page=0))
    assert acquirer.asked == [], "the absent sibling must not cause another read of the corrected file"
    assert _rows(out) == corrected
    checkpoints = {row["file_name"]: row for row in read_checkpoints(out, NAME)}
    assert checkpoints[f"{PART_ONE}.pdf"]["processing_version"]["reader"] == "003"
    assert checkpoints[f"{PART_TWO}.pdf"]["processing_version"]["reader"] == "002", "missing sibling stays pending"
    _prior(out)

    out = _build(tmp_path, _Reader([], granules), acquirer, _Extractor(page_count=1, tables_per_page=0))
    assert acquirer.asked == [PART_TWO], "the pending sibling is retried when it returns"
    assert {row["file_name"] for row in _rows(out)} == {f"{PART_ONE}.pdf"}


def test_acquire_granule_requests_pdf_preference_explicitly(tmp_path):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    acquirer = _Acquirer()
    _build(tmp_path, reader, acquirer, _Extractor(page_count=1))
    assert acquirer.prefers == [("pdf",)]


@pytest.mark.parametrize("error", [TypeError("internal bug"), KeyError("missing_key")])
def test_internal_type_or_key_error_is_not_swallowed_as_a_refusal(tmp_path, error):
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]})
    acquirer = _Acquirer(refuse={PART_ONE: error})
    with pytest.raises(type(error)):
        _build(tmp_path, reader, acquirer, _Extractor(page_count=1))


def test_rollup_surfaces_refusal_as_deferred_failure_after_publishing_healthy_rows(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.senate_expenditures import SenateExpendituresRollup

    assert SenateExpendituresRollup.retain_source_evidence is True
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    acquirer = _Acquirer(refuse={PART_ONE: PagedJsonSourceError("granule refused")})

    def stub_build(output_dir: Path, **kwargs):
        assert kwargs["evidence"] is not None
        kwargs["download_prior"] = _no_download
        return build_senate_expenditures(
            output_dir,
            reader=reader,
            acquirer=acquirer,
            extractor=_Extractor(page_count=2),
            **kwargs,
        )

    monkeypatch.setattr("spicy_regs.pipelines.rollups.senate_expenditures.build_senate_expenditures", stub_build)
    pipeline = SenateExpendituresRollup(output_dir=tmp_path, skip_upload=True)
    with pytest.raises(RuntimeError, match="1 refused"):
        pipeline.run()
    [generation] = list((tmp_path / "generations").iterdir())
    assert {row["file_name"] for row in _rows(generation / "senate_expenditures.parquet")} == {f"{PART_TWO}.pdf"}


# --------------------------------------------------------------------------
# Part ordering and Part I / Part II appropriation_summary deduplication.
# --------------------------------------------------------------------------


class _ScriptedExtractor:
    """Serves explicit per-granule page definitions keyed by granule id embedded in ``_Body``."""

    def __init__(self, pages_by_granule: dict[str, list[tuple[str, tuple[tuple[tuple[str | None, ...], ...], ...]]]]):
        self._pages_by_granule = pages_by_granule

    def extract(self, source: bytes, *, media_type: str):
        marker = source.decode("utf-8", errors="ignore")
        granule_id = next((gid for gid in sorted(self._pages_by_granule, key=len, reverse=True) if gid in marker), "")
        spec = self._pages_by_granule[granule_id]
        page_count = len(spec)
        for number, (text, tables_cells) in enumerate(spec, start=1):
            tables = tuple(_Table(number, cells) for cells in tables_cells)
            yield _PageResult(number, page_count, tables, text=text)


def _summary_page(label: str, entry: tuple[str, str, str] = ENTRY) -> tuple[str, tuple[tuple[tuple[str | None, ...], ...], ...]]:
    text = f"SUMMARY OF TRANSACTIONS BY APPROPRIATIONS\nSENATOR TIM KAINE\nFunding Year 2024\nSALARIES\n{label}"
    return text, ((HEADER, entry),)


def _org_page(label: str, *, text: str | None = None) -> tuple[str, tuple[tuple[tuple[str | None, ...], ...], ...]]:
    page_text = text or f"DETAILED AND SUMMARY STATEMENT OF EXPENDITURES\nSENATOR TIM KAINE\nFunding Year 2024\nSALARIES\n{label}"
    return page_text, ((ORG_HEADER, ORG_ENTRY),)


def _payee_page(label: str, *, text: str | None = None) -> tuple[str, tuple[tuple[tuple[str | None, ...], ...], ...]]:
    page_text = text or f"DETAILED AND SUMMARY STATEMENT OF EXPENDITURES\nSENATOR TIM KAINE\nFunding Year 2024\nSALARIES\n{label}"
    return page_text, ((PAYEE_HEADER, PAYEE_ENTRY),)


def test_part_granules_are_processed_in_deterministic_numeric_order(tmp_path):
    """Granules returned out of order (e.g. -10, -2, -1) are read in numeric part order -1, -2, -10."""
    part_ten = f"{PACKAGE}-10"
    shared_a1 = _summary_page("A-1")
    reader = _Reader([_listing()], {PACKAGE: [_granule(part_ten), _granule(PART_TWO), _granule(PART_ONE)]})
    acquirer = _Acquirer()
    extractor = _ScriptedExtractor(
        {
            PART_ONE: [shared_a1, _org_page("B-1")],
            PART_TWO: [shared_a1, _payee_page("B-200")],
            part_ten: [shared_a1, _payee_page("B-1000")],
        }
    )
    coverage: dict = {}
    out = _build(tmp_path, reader, acquirer, extractor, coverage=coverage)

    assert acquirer.asked == [PART_ONE, PART_TWO, part_ten]
    rows = _rows(out)
    summary_rows = [row for row in rows if row["grid_kind"] == "appropriation_summary"]
    assert {row["file_name"] for row in summary_rows} == {f"{PART_ONE}.pdf"}
    assert coverage["duplicate_summary_pages_skipped"] == 2
    assert coverage["duplicate_summary_pages_by_file"] == {f"{PART_TWO}.pdf": 1, f"{part_ten}.pdf": 1}


def test_duplicate_appropriation_summary_pages_are_skipped_and_recorded_in_evidence(tmp_path):
    """Byte-identical Part I/II summary pages are emitted once and recorded in CaptureEvidence + coverage."""
    from spicy_regs.source_evidence import CaptureEvidence

    shared_a1 = _summary_page("A-1")
    shared_a2 = _summary_page("A-2", ("OTHER ACCOUNT\n2024", "0200", "5,678.90"))
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_TWO), _granule(PART_ONE)]})
    extractor = _ScriptedExtractor(
        {
            PART_ONE: [shared_a1, shared_a2, _org_page("B-1")],
            PART_TWO: [shared_a1, shared_a2, _org_page("B-1200"), _payee_page("B-1201")],
        }
    )
    evidence = CaptureEvidence(tmp_path / "audit", "senate-expenditures", archive_root=tmp_path / "raw")
    coverage: dict = {}
    out = _build(tmp_path, reader, _Acquirer(), extractor, evidence=evidence, coverage=coverage)

    rows = _rows(out)
    summary_rows = [row for row in rows if row["grid_kind"] == "appropriation_summary"]
    assert {row["file_name"] for row in summary_rows} == {f"{PART_ONE}.pdf"}
    assert {row["printed_page"] for row in summary_rows} == {"A-1", "A-2"}
    b_rows = [row for row in rows if row["grid_kind"] in {"organization_detail", "payee_detail"}]
    assert {row["file_name"] for row in b_rows} == {f"{PART_ONE}.pdf", f"{PART_TWO}.pdf"}
    assert {row["printed_page"] for row in b_rows} == {"B-1", "B-1200", "B-1201"}

    assert coverage["duplicate_summary_pages_skipped"] == 2
    assert coverage["duplicate_summary_pages_by_file"] == {f"{PART_TWO}.pdf": 2}
    journal_events = [
        json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines() if line.strip()
    ]
    coverage_events = [e for e in journal_events if e["event"] == "acquisition-coverage"]
    assert len(coverage_events) == 1
    assert coverage_events[0]["duplicate_summary_pages_skipped"] == 2
    assert coverage_events[0]["duplicate_summary_pages_by_file"] == {f"{PART_TWO}.pdf": 2}

    checkpoints = {row["file_name"]: row for row in read_checkpoints(out, NAME)}
    assert checkpoints[f"{PART_ONE}.pdf"]["duplicate_summary_pages_skipped"] == 0
    assert checkpoints[f"{PART_TWO}.pdf"]["duplicate_summary_pages_skipped"] == 2


def test_non_identical_appropriation_summary_pages_are_never_dropped(tmp_path):
    """Only exact text_sha256 matches are skipped; a distinct Part II summary page is preserved."""
    shared_a1 = _summary_page("A-1")
    distinct_a2 = _summary_page("A-2", ("PART II UNIQUE SUMMARY\n2024", "0300", "99.00"))
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    extractor = _ScriptedExtractor(
        {
            PART_ONE: [shared_a1],
            PART_TWO: [shared_a1, distinct_a2],
        }
    )
    coverage: dict = {}
    out = _build(tmp_path, reader, _Acquirer(), extractor, coverage=coverage)

    summary_rows = [row for row in _rows(out) if row["grid_kind"] == "appropriation_summary"]
    assert {(row["file_name"], row["printed_page"]) for row in summary_rows} == {
        (f"{PART_ONE}.pdf", "A-1"),
        (f"{PART_TWO}.pdf", "A-2"),
    }
    assert coverage["duplicate_summary_pages_skipped"] == 1


def test_identical_appropriation_summary_pages_across_different_packages_are_never_dropped(tmp_path):
    """Deduplication is strictly per package_id; identical summary text in another package is kept."""
    second_pkg = "GPO-CDOC-119sdoc6"
    shared_a1 = _summary_page("A-1")
    reader = _Reader(
        [
            _listing(PACKAGE),
            _listing(second_pkg, "Report of the Secretary of the Senate: October 1, 2025 to March 31, 2026"),
        ],
        {PACKAGE: [_granule(PART_ONE)], second_pkg: [_granule(f"{second_pkg}-1")]},
    )
    extractor = _ScriptedExtractor({PART_ONE: [shared_a1], f"{second_pkg}-1": [shared_a1]})
    coverage: dict = {}
    out = _build(tmp_path, reader, _Acquirer(), extractor, coverage=coverage)

    summary_rows = [row for row in _rows(out) if row["grid_kind"] == "appropriation_summary"]
    assert {row["package_id"] for row in summary_rows} == {PACKAGE, second_pkg}
    assert coverage["duplicate_summary_pages_skipped"] == 0


def test_b_pages_are_never_dropped_even_with_identical_page_text(tmp_path):
    """organization_detail and payee_detail rows are never deduplicated, even when page text repeats."""
    shared_summary = _summary_page("A-1")
    shared_org = _org_page("B-10", text=B_PAGE_TEXT)
    shared_payee = _payee_page("B-11", text=B_PAGE_TEXT)
    # Mixed page: shares exact summary page text, carrying both a duplicate summary table and a B-page table.
    mixed_page = (shared_summary[0], ((HEADER, ENTRY), (ORG_HEADER, ORG_ENTRY)))
    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    extractor = _ScriptedExtractor(
        {
            PART_ONE: [shared_summary, shared_org, shared_payee],
            PART_TWO: [mixed_page, shared_org, shared_payee],
        }
    )
    coverage: dict = {}
    out = _build(tmp_path, reader, _Acquirer(), extractor, coverage=coverage)

    rows = _rows(out)
    summary_rows = [row for row in rows if row["grid_kind"] == "appropriation_summary"]
    assert {row["file_name"] for row in summary_rows} == {f"{PART_ONE}.pdf"}
    org_part2 = [row for row in rows if row["file_name"] == f"{PART_TWO}.pdf" and row["grid_kind"] == "organization_detail"]
    payee_part2 = [row for row in rows if row["file_name"] == f"{PART_TWO}.pdf" and row["grid_kind"] == "payee_detail"]
    # Part II retains both the mixed page's organization_detail table and its own identical B-pages.
    assert len(org_part2) == 4
    assert len(payee_part2) == 2
    assert coverage["duplicate_summary_pages_skipped"] == 1


def test_held_part_one_deduplicates_newly_read_part_two_without_refetching_part_one(tmp_path):
    """Incremental merge: Part I held in prior seeds seen hashes so newly read Part II skips duplicate summaries."""
    shared_a1 = _summary_page("A-1")
    shared_a2 = _summary_page("A-2", ("OTHER ACCOUNT\n2024", "0200", "5,678.90"))
    extractor = _ScriptedExtractor(
        {
            PART_ONE: [shared_a1, shared_a2, _org_page("B-1")],
            PART_TWO: [shared_a1, shared_a2, _payee_page("B-1201")],
        }
    )

    # Run 1: only Part I exists.
    out = _build(tmp_path, _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]}), _Acquirer(), extractor)
    _prior(out)

    # Run 2: Part II arrives; Part I stays held.
    acquirer = _Acquirer()
    coverage: dict = {}
    out = _build(
        tmp_path,
        _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]}),
        acquirer,
        extractor,
        coverage=coverage,
    )
    assert acquirer.asked == [PART_TWO], "held Part I must not be refetched"
    rows = _rows(out)
    summary_rows = [row for row in rows if row["grid_kind"] == "appropriation_summary"]
    assert {row["file_name"] for row in summary_rows} == {f"{PART_ONE}.pdf"}
    assert {row["printed_page"] for row in summary_rows} == {"A-1", "A-2"}
    assert {row["file_name"] for row in rows if row["grid_kind"] == "payee_detail"} == {f"{PART_TWO}.pdf"}
    assert coverage["duplicate_summary_pages_skipped"] == 2
    assert coverage["skipped"] == 1


def test_part_one_recovery_after_prior_refusal_reconciles_held_part_two_duplicates(tmp_path):
    """If Part I refused on Run 1 and Part II published summaries, Run 2 reconciles both to zero duplicates."""
    shared_a1 = _summary_page("A-1")
    extractor = _ScriptedExtractor(
        {
            PART_ONE: [shared_a1, _org_page("B-1")],
            PART_TWO: [shared_a1, _payee_page("B-1201")],
        }
    )
    granules = {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]}

    # Run 1: Part I refuses; Part II still publishes healthy rows (including A-1).
    out = _build(
        tmp_path,
        _Reader([_listing()], granules),
        _Acquirer(refuse={PART_ONE: PagedJsonSourceError("temporary refusal")}),
        extractor,
    )
    assert {row["file_name"] for row in _rows(out) if row["grid_kind"] == "appropriation_summary"} == {
        f"{PART_TWO}.pdf"
    }
    _prior(out)

    # Run 2: Part I succeeds; held Part II overlaps Part I's summary hashes so Part II is refreshed cleanly.
    acquirer = _Acquirer()
    coverage: dict = {}
    out = _build(tmp_path, _Reader([_listing()], granules), acquirer, extractor, coverage=coverage)
    assert acquirer.asked == [PART_ONE, PART_TWO]
    rows = _rows(out)
    summary_rows = [row for row in rows if row["grid_kind"] == "appropriation_summary"]
    assert {row["file_name"] for row in summary_rows} == {f"{PART_ONE}.pdf"}
    assert {row["file_name"] for row in rows if row["grid_kind"] == "payee_detail"} == {f"{PART_TWO}.pdf"}
    assert coverage["duplicate_summary_pages_skipped"] == 1
    assert coverage["skipped"] == 0
    _prior(out)

    # Run 3: steady state — neither part is refetched.
    acquirer.asked.clear()
    steady_coverage: dict = {}
    out = _build(tmp_path, _Reader([_listing()], granules), acquirer, extractor, coverage=steady_coverage)
    assert acquirer.asked == []
    assert steady_coverage["skipped"] == 2
    assert steady_coverage["duplicate_summary_pages_skipped"] == 0


def test_rule_bump_rereads_both_parts_and_removes_preexisting_part_two_duplicates(tmp_path, monkeypatch):
    """Upgrading from a prior generation carrying duplicate Part I/II summaries cleans both deterministically."""
    shared_a1 = _summary_page("A-1")
    extractor = _ScriptedExtractor(
        {
            PART_ONE: [shared_a1, _org_page("B-1")],
            PART_TWO: [shared_a1, _payee_page("B-1201")],
        }
    )
    granules = {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]}

    # Build a synthetic pre-dedupe prior holding duplicate A-1 rows in BOTH Part I and Part II under old rule 001.
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "001")
    out_p1 = _build(tmp_path, _Reader([_listing()], {PACKAGE: [_granule(PART_ONE)]}), _Acquirer(), extractor)
    table_p1 = pq.read_table(out_p1)
    cp_p1 = read_checkpoints(out_p1, NAME)
    out_p1.unlink()
    out_p2 = _build(tmp_path, _Reader([_listing()], {PACKAGE: [_granule(PART_TWO)]}), _Acquirer(), extractor)
    table_p2 = pq.read_table(out_p2)
    cp_p2 = read_checkpoints(out_p2, NAME)
    combined = pa.concat_tables([table_p1, table_p2])
    metadata = checkpoint_metadata(out_p2, NAME, [*cp_p1, *cp_p2])
    pq.write_table(combined.replace_schema_metadata(metadata), tmp_path / f"_{NAME}_prior.parquet")
    out_p2.unlink()

    # Now upgrade to current READ_POLICY_VERSION ("002") and re-run.
    monkeypatch.undo()
    acquirer = _Acquirer()
    coverage: dict = {}
    out = _build(tmp_path, _Reader([_listing()], granules), acquirer, extractor, coverage=coverage)
    assert acquirer.asked == [PART_ONE, PART_TWO]
    rows = _rows(out)
    summary_rows = [row for row in rows if row["grid_kind"] == "appropriation_summary"]
    assert {row["file_name"] for row in summary_rows} == {f"{PART_ONE}.pdf"}
    assert {row["file_name"] for row in rows if row["grid_kind"] == "organization_detail"} == {f"{PART_ONE}.pdf"}
    assert {row["file_name"] for row in rows if row["grid_kind"] == "payee_detail"} == {f"{PART_TWO}.pdf"}
    assert coverage["duplicate_summary_pages_skipped"] == 1


def test_failed_part_one_stream_does_not_poison_seen_hashes_for_part_two(tmp_path):
    """An uncommitted mid-stream failure on Part I must not suppress Part II's summary pages."""
    shared_a1 = _summary_page("A-1")

    class FailingPartOneExtractor:
        def extract(self, source: bytes, *, media_type: str):
            marker = source.decode("utf-8", errors="ignore")
            if PART_ONE in marker:
                yield _PageResult(1, 2, (_Table(1, shared_a1[1][0]),), text=shared_a1[0])
                raise OSError("truncated stream")
            yield _PageResult(1, 2, (_Table(1, shared_a1[1][0]),), text=shared_a1[0])
            yield _PageResult(2, 2, (_Table(2, (PAYEE_HEADER, PAYEE_ENTRY)),), text=B_PAGE_TEXT)

    reader = _Reader([_listing()], {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]})
    coverage: dict = {}
    out = _build(tmp_path, reader, _Acquirer(), FailingPartOneExtractor(), coverage=coverage)
    rows = _rows(out)
    summary_rows = [row for row in rows if row["grid_kind"] == "appropriation_summary"]
    assert {row["file_name"] for row in summary_rows} == {f"{PART_TWO}.pdf"}
    assert coverage["refused"] == 1
    assert coverage["duplicate_summary_pages_skipped"] == 0
