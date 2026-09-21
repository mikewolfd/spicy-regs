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
    REPORT_TITLE,
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


def _listing(
    package_id=PACKAGE,
    title="Report of the Secretary of the Senate: October 1, 2024 to March 31, 2025",
    modified="2026-09-20T00:00:00Z",
):
    return {"packageId": package_id, "title": title, "lastModified": modified}


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
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "002")

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
    _prior(out)
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "002")
    acquirer = _Acquirer(refuse={PART_TWO: OSError("temporary read failure")})
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2, tables_per_page=0))
    assert _rows(out) == old_sibling
    reads = {row["file_name"]: row for row in read_checkpoints(out, NAME)}
    assert reads[f"{PART_ONE}.pdf"]["processing_version"]["reader"] == "002"
    assert reads[f"{PART_TWO}.pdf"]["processing_version"]["reader"] == "001"

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
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "002")

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
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "002")
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
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "002")
    reader = _Reader([], granules)
    acquirer = _Acquirer()
    out = _build(tmp_path, reader, acquirer, _Extractor(page_count=2, tables_per_page=0))
    assert reader.granule_calls == [PACKAGE]
    assert acquirer.asked == [PART_ONE]
    assert _rows(out) == []
    checkpoint = read_checkpoints(out, NAME)[0]
    assert checkpoint["last_modified"] is None, "an old source date is not evidence about the new read"
    assert checkpoint["processing_version"]["reader"] == "002"
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
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "002")
    out = _build(tmp_path, _Reader([], {PACKAGE: []}), _Acquirer(), _Extractor(page_count=2))
    assert _rows(out) == expected
    assert read_checkpoints(out, NAME) == expected_reads


def test_missing_outside_discovery_sibling_does_not_reread_the_corrected_file(tmp_path, monkeypatch):
    granules = {PACKAGE: [_granule(PART_ONE), _granule(PART_TWO)]}
    out = _build(tmp_path, _Reader([_listing()], granules), _Acquirer(), _Extractor(page_count=2))
    old_sibling = [row for row in _rows(out) if row["file_name"] == f"{PART_TWO}.pdf"]
    _prior(out)
    monkeypatch.setattr(transform, "READ_POLICY_VERSION", "002")
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
    assert checkpoints[f"{PART_ONE}.pdf"]["processing_version"]["reader"] == "002"
    assert checkpoints[f"{PART_TWO}.pdf"]["processing_version"]["reader"] == "001", "missing sibling stays pending"
    _prior(out)

    out = _build(tmp_path, _Reader([], granules), acquirer, _Extractor(page_count=1, tables_per_page=0))
    assert acquirer.asked == [PART_TWO], "the pending sibling is retried when it returns"
    assert {row["file_name"] for row in _rows(out)} == {f"{PART_ONE}.pdf"}
