"""Hermetic tests for the laws transform: the enumeration, the PLAW leg, the two OLRC legs, and what a run re-asks.

No network. The list reader, the USLM acquirer and the OLRC acquirer are
stubbed over the wheel's own fixtures (``tests/fixtures/congress_laws/``),
parsed through the wheel's own readers, so what is proved here is this
repository's seam — which laws are asked about, in what order, and what is
published when a leg is not established — and never a shaper rule.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.govinfo.uslm import (
    PublicLawSelection,
    UslmSourceError,
    public_law_xml_locator,
    validate_public_law_xml,
)
from spicy_docs.sources.govinfo.uslm_acquisition import UslmSourceUnavailableError
from spicy_docs.sources.uscode import UsCodeSourceError, parse_table3_page
from spicy_docs.sources.uscode.classification import parse_classification_index, parse_classification_table
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.transforms.build_laws import build_laws
from tests.test_incremental_rollups import no_download, seed

FIXTURES = Path(__file__).parent / "fixtures" / "congress_laws"
LIST_PAGE = json.loads((FIXTURES / "congress-law-list.json").read_text())
LAW_119_1 = json.loads((FIXTURES / "congress-law-119-1.json").read_text())
USLM_BYTES = (FIXTURES / "plaw-119publ1.xml").read_bytes()
INDEX_BYTES = (FIXTURES / "classification-tables-index.shtml").read_bytes()
TABLE_BYTES = (FIXTURES / "classification-tbl119pl_2nd-head.htm").read_bytes()
TABLE3_BYTES = (FIXTURES / "table3-111_226-head.htm").read_bytes()
OBSERVED_AT = "2026-09-19T00:00:00Z"
#: The three listed rows plus the one law the USLM fixture states, as one page.
LISTED_119 = [*LIST_PAGE["bills"], LAW_119_1]


class _Capture:
    def __init__(self, body: bytes):
        self.body = body
        self.observed_at = OBSERVED_AT

    @property
    def sha256(self) -> str:
        return "sha256:" + hashlib.sha256(self.body).hexdigest()


class _Page:
    def __init__(self, records, declared):
        self.records = tuple(records)
        self.declared_count = declared


class StubListingReader:
    """Serves one page per Congress it holds; nothing for the rest."""

    def __init__(self, by_congress):
        self.by_congress = by_congress
        self.urls: list[str] = []

    def records(self, route, url, *, max_pages=100):
        self.urls.append(url)
        congress = int(url.split("/law/")[1].split("?")[0])
        listed = self.by_congress.get(congress)
        return iter((_Page(listed, LIST_PAGE["pagination"]["count"]),)) if listed else iter(())


def _not_found(url: str) -> CapturedBodyResponse:
    return CapturedBodyResponse(url, url, 404, "text/html", OBSERVED_AT, b"")


class StubUslm:
    """Holds PLAW-119publ1 only; other numbers are unavailable, refused or absent as configured."""

    def __init__(self, *, unavailable=(), refused=(), error=None):
        self.unavailable = set(unavailable)
        self.refused = set(refused)
        self.error = error
        self.selections: list[PublicLawSelection] = []

    def acquire_public_law(self, selection, *, max_bytes=None):
        self.selections.append(selection)
        if self.error is not None:
            raise self.error
        if selection.number in self.unavailable:
            raise UslmSourceUnavailableError(_not_found(public_law_xml_locator(selection)))
        if selection.number in self.refused:
            raise UslmSourceError("stub: the body was not the expected shape")
        if selection == PublicLawSelection(119, "public", 1):
            meta = validate_public_law_xml(USLM_BYTES, selection=selection, final_url=public_law_xml_locator(selection))
            return SimpleNamespace(metadata=meta, capture=_Capture(USLM_BYTES))
        raise AssertionError(f"the stub holds no PLAW for {selection}")


class StubOlrc:
    def __init__(self, *, table3_pages=None, index_error=None):
        self.table3_pages = {"111-226": TABLE3_BYTES} if table3_pages is None else table3_pages
        self.index_error = index_error
        self.tables: list[tuple[int, int, str]] = []
        self.acts: list[str] = []

    def acquire_classification_index(self, *, max_bytes=None):
        if self.index_error is not None:
            raise self.index_error
        return SimpleNamespace(result=parse_classification_index(INDEX_BYTES), capture=_Capture(INDEX_BYTES))

    def acquire_classification_table(self, congress, session, *, order="public-law", max_bytes=None, max_rows=65536):
        self.tables.append((congress, session, order))
        if (congress, session) != (119, 2):
            raise UsCodeSourceError("stub: not captured")
        table = parse_classification_table(TABLE_BYTES, congress=congress, session=session, order=order)
        return SimpleNamespace(result=table, capture=_Capture(TABLE_BYTES))

    def acquire_table3_act(self, key, *, max_bytes=None, max_rows=65536):
        self.acts.append(key)
        body = self.table3_pages.get(key)
        if body is None:
            raise UsCodeSourceError("stub: the page is cut off inside the site menu")
        return SimpleNamespace(result=parse_table3_page(body, key=key), capture=_Capture(body))


def _rows(path: Path) -> list[dict]:
    return pq.read_table(path).to_pylist()


def _by_law(path: Path) -> dict[str, dict]:
    return {row["law_id"]: row for row in _rows(path)}


@pytest.fixture
def scoped(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")


def _build(tmp_path, *, reader=None, uslm=None, olrc=None, **kw):
    return build_laws(
        tmp_path,
        reader=reader or StubListingReader({119: LISTED_119}),
        uslm=uslm or StubUslm(unavailable={110, 109, 104}),
        olrc=olrc or StubOlrc(),
        download_prior=no_download,
        **kw,
    )


def test_a_cold_start_publishes_every_listed_law_with_its_uslm_outcome(tmp_path, scoped):
    uslm = StubUslm(unavailable={110, 109, 104})
    laws, _, _ = _build(tmp_path, uslm=uslm)

    # Newest first, every listed law asked about, none twice.
    assert [s.number for s in uslm.selections] == [110, 109, 104, 1]
    rows = _by_law(laws)
    assert set(rows) == {"119-public-110", "119-public-109", "119-public-104", "119-public-1"}
    captured = rows["119-public-1"]
    assert captured["uslm_outcome"] == "captured"
    assert captured["statutes_at_large_cite"] == "139 Stat. 3"
    assert captured["uslm_sha256"] == "sha256:" + hashlib.sha256(USLM_BYTES).hexdigest()
    assert captured["uslm_observed_at"] == OBSERVED_AT and captured["bill_id"] == "119-s-5"
    lagging = rows["119-public-110"]
    assert lagging["uslm_outcome"] == "unavailable"
    assert lagging["statutes_at_large_cite"] is None and lagging["uslm_sha256"] is None
    assert lagging["bill_id"] == "119-s-307" and lagging["package_id"] == "PLAW-119publ110"


def test_the_classification_leg_reads_the_public_law_order_the_index_links(tmp_path, scoped):
    olrc = StubOlrc()
    _, sections, _ = _build(tmp_path, olrc=olrc)

    # The index links both sessions of the 119th in both orders; only the
    # public-law order is asked for, and the 1st session's refusal is that
    # table's gap, not the run's.
    assert olrc.tables == [(119, 2, "public-law"), (119, 1, "public-law")]
    rows = _rows(sections)
    assert len(rows) == 9
    assert {(r["congress"], r["session"], r["table_order"]) for r in rows} == {("119", "2", "public-law")}
    assert [r["seq"] for r in rows] == [str(i) for i in range(9)]
    assert rows[0]["law_id"] == "119-public-70" and rows[0]["observed_at"] == OBSERVED_AT


def test_a_session_page_replaces_its_prior_rows_and_leaves_other_sessions(tmp_path, scoped):
    seed(
        tmp_path,
        "law_code_sections",
        [{"congress": "119", "session": "2", "seq": str(i), "observed_at": "2026-09-01"} for i in range(20)]
        + [{"congress": "119", "session": "1", "seq": "0", "observed_at": "2026-09-01"}],
    )
    _, sections, _ = _build(tmp_path)
    rows = _rows(sections)
    assert sum(1 for r in rows if r["session"] == "2") == 9, "the twenty stale positions are gone"
    assert sum(1 for r in rows if r["session"] == "1") == 1, "the session not read this run keeps its rows"


def test_an_index_failure_reads_no_session_table_and_is_not_a_record(tmp_path, scoped):
    olrc = StubOlrc(index_error=ConnectionError("stub: timed out"))
    _, sections, _ = _build(tmp_path, olrc=olrc)
    assert olrc.tables == [] and _rows(sections) == []


def test_table_iii_reads_unread_acts_oldest_first_and_stops_at_the_lag(tmp_path, monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "111,119")
    # The 111th is not on the stub route; its laws are known from the prior table.
    seed(tmp_path, "laws", [{"law_id": "111-public-226", "congress": "111", "law_type": "public", "number": "226"}])
    olrc = StubOlrc()
    _, _, table3 = _build(tmp_path, olrc=olrc)

    # Newest Congress first; within it the oldest unread act, which Table III
    # does not hold yet, stops that Congress's walk; the 111th's act is read.
    assert olrc.acts == ["119-1", "111-226"]
    rows = _rows(table3)
    assert {r["act_key"] for r in rows} == {"111-226"} and len(rows) == 4
    assert rows[0]["release_point"] == "119-73" and rows[0]["observed_at"] == OBSERVED_AT
    assert [r["seq"] for r in rows] == ["0", "1", "2", "3"]


def test_table_iii_skips_acts_already_published_and_private_laws(tmp_path, monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "111,119")
    seed(
        tmp_path,
        "laws",
        [
            {"law_id": "111-public-226", "congress": "111", "law_type": "public", "number": "226"},
            {"law_id": "111-private-1", "congress": "111", "law_type": "private", "number": "1"},
        ],
    )
    seed(tmp_path, "table3_records", [{"act_key": "111-226", "seq": "0", "observed_at": "2026-09-01"}])
    olrc = StubOlrc()
    _, _, table3 = _build(tmp_path, olrc=olrc)
    assert olrc.acts == ["119-1"], "a held act is not re-read and a private law is never requested"
    assert len(_rows(table3)) == 1, "the held act's prior rows stand"


def test_table_iii_honours_its_own_cap(tmp_path, scoped):
    olrc = StubOlrc()
    _build(tmp_path, olrc=olrc, max_table3=0)
    assert olrc.acts == []


def test_a_captured_law_is_not_re_read_unless_its_list_row_moved(tmp_path, scoped):
    seed(
        tmp_path,
        "laws",
        [
            # Same update_date as the list row: left standing, not asked about.
            {
                "law_id": "119-public-1",
                "congress": "119",
                "law_type": "public",
                "number": "1",
                "update_date": LAW_119_1["updateDate"],
                "uslm_outcome": "captured",
                "statutes_at_large_cite": "139 Stat. 3",
                "title": "the prior row",
            },
            # Captured, but the list row has moved since: asked about again.
            {
                "law_id": "119-public-109",
                "congress": "119",
                "law_type": "public",
                "number": "109",
                "update_date": "2026-01-01",
                "uslm_outcome": "captured",
                "statutes_at_large_cite": "140 Stat. 1",
            },
            # The bulk lag last run: asked about again whatever the list row did.
            {
                "law_id": "119-public-110",
                "congress": "119",
                "law_type": "public",
                "number": "110",
                "update_date": LIST_PAGE["bills"][0]["updateDate"],
                "uslm_outcome": "unavailable",
            },
        ],
    )
    uslm = StubUslm(unavailable={110, 109, 104})
    laws, _, _ = _build(tmp_path, uslm=uslm)
    assert [s.number for s in uslm.selections] == [110, 109, 104]
    rows = _by_law(laws)
    assert rows["119-public-1"]["title"] == "the prior row", "no fresh row was written over the held one"
    assert rows["119-public-1"]["statutes_at_large_cite"] == "139 Stat. 3"
    # The re-read said the folder no longer serves it: that is the publisher's answer today.
    assert rows["119-public-109"]["uslm_outcome"] == "unavailable"
    assert rows["119-public-109"]["statutes_at_large_cite"] is None
    assert rows["119-public-110"]["uslm_outcome"] == "unavailable"


def test_the_cap_leaves_held_rows_standing_and_lists_the_rest_as_not_requested(tmp_path, scoped):
    seed(
        tmp_path,
        "laws",
        [
            {
                "law_id": "119-public-110",
                "congress": "119",
                "law_type": "public",
                "number": "110",
                "update_date": "2026-01-01",
                "uslm_outcome": "unavailable",
                "title": "the prior row",
            }
        ],
    )
    uslm = StubUslm()
    laws, _, _ = _build(tmp_path, uslm=uslm, max_uslm=0)
    assert uslm.selections == []
    rows = _by_law(laws)
    assert (
        rows["119-public-110"]["title"] == "the prior row" and rows["119-public-110"]["uslm_outcome"] == "unavailable"
    )
    for law_id in ("119-public-109", "119-public-104", "119-public-1"):
        assert rows[law_id]["uslm_outcome"] == "not_requested" and rows[law_id]["statutes_at_large_cite"] is None


def test_a_transport_failure_or_refused_body_is_never_published_as_absence(tmp_path, scoped):
    uslm = StubUslm(unavailable={110, 109}, refused={104, 1})
    laws, _, _ = _build(tmp_path, uslm=uslm)
    rows = _by_law(laws)
    assert rows["119-public-110"]["uslm_outcome"] == "unavailable"
    assert rows["119-public-104"]["uslm_outcome"] == "not_requested"
    assert (
        rows["119-public-1"]["uslm_outcome"] == "not_requested"
        and rows["119-public-1"]["statutes_at_large_cite"] is None
    )


def test_a_credential_refusal_aborts_the_run(tmp_path, scoped):
    with pytest.raises(CredentialRefusedError):
        _build(tmp_path, uslm=StubUslm(error=CredentialRefusedError("stub: 403")))


def test_the_published_shapes_are_the_contracts(tmp_path, scoped):
    from spicy_docs.schemas import TABLE_CONTRACTS

    for name, path in zip(("laws", "law_code_sections", "table3_records"), _build(tmp_path), strict=True):
        assert path.name == f"{name}.parquet"
        assert pq.read_table(path).schema.names == list(TABLE_CONTRACTS[name].columns)
