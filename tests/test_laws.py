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
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace

import httpx
import pyarrow.parquet as pq
import pytest
from loguru import logger
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

from spicy_docs.schemas.law_tables import USLM_READER_VERSION
from spicy_docs.sources.uscode.table3 import TABLE3_READER_VERSION
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints

from spicy_regs.transforms.build_laws import (
    OLRC_BUDGET,
    TABLE3_STOP_AFTER,
    _page_rows,
    _rows_digest,
    _table3_rows,
    build_laws,
    olrc_acquirer,
)
from spicy_regs.transforms.congress_walk import PerRunCap
from tests.test_incremental_rollups import no_download, seed

FIXTURES = Path(__file__).parent / "fixtures" / "congress_laws"
LIST_PAGE = json.loads((FIXTURES / "congress-law-list.json").read_text())
LAW_119_1 = json.loads((FIXTURES / "congress-law-119-1.json").read_text())
USLM_BYTES = (FIXTURES / "plaw-119publ1.xml").read_bytes()
INDEX_BYTES = (FIXTURES / "classification-tables-index.shtml").read_bytes()
TABLE_BYTES = (FIXTURES / "classification-tbl119pl_2nd-head.htm").read_bytes()
TABLE3_BYTES = (FIXTURES / "table3-111_226-head.htm").read_bytes()
#: The fixture as the wheel's reader reads it: its page names 111-227 as the next act.
TABLE3_PAGE = parse_table3_page(TABLE3_BYTES, key="111-226")
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


def _stated(key: str | None) -> str | None:
    """A key as a Table III page states it, with an en dash."""
    return key and key.replace("-", "\u2013")


class StubOlrc:
    """Serves a page for each act in ``chain`` (act -> the next act its page names), each stating ``release_point``.

    The reader refuses the ``refusing`` acts, and every other act fails in transport.
    """

    def __init__(self, *, chain=None, release_point="119-73", refusing=(), index_error=None):
        self.chain = {"111-226": "111-227"} if chain is None else chain
        self.release_point = release_point
        self.refusing = set(refusing)
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
        if key in self.refusing:
            raise UsCodeSourceError("stub: Table III page states no act of its own")
        if key not in self.chain:
            raise ConnectionError("stub: Body source transport failed while acquiring a response")
        page = replace(
            TABLE3_PAGE,
            key=key,
            stated_key=_stated(key),
            next_act=_stated(self.chain[key]),
            release_point=self.release_point,
        )
        return SimpleNamespace(result=page, capture=_Capture(TABLE3_BYTES))


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


def test_table_iii_walks_the_newest_congress_first_and_a_failure_ends_only_its_chain(tmp_path, monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "111,119")
    # The 111th is not on the stub route; its laws are known from the prior table.
    seed(tmp_path, "laws", [{"law_id": "111-public-226", "congress": "111", "law_type": "public", "number": "226"}])
    olrc = StubOlrc()
    _, _, table3 = _build(tmp_path, olrc=olrc)

    # Cold, each chain starts at the lowest act the laws table states. 119-1
    # fails, which ends the 119th's chain; the 111th's act is read, and its
    # page names 111-227, which the laws table does not state.
    assert olrc.acts == ["119-1", "111-226"]
    rows = _rows(table3)
    assert {r["act_key"] for r in rows} == {"111-226"} and len(rows) == 4
    assert rows[0]["release_point"] == "119-73" and rows[0]["observed_at"] == OBSERVED_AT
    assert [r["seq"] for r in rows] == ["0", "1", "2", "3"]


def test_table_iii_asks_a_held_act_only_for_its_chain_and_never_a_private_law(tmp_path, monkeypatch):
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
    # The held act has no checkpoint, so it is re-read first; the 119th has no held act and no 118th seed, so it
    # starts cold at the lowest act the laws table states, as it always has. No private law is asked.
    assert olrc.acts == ["111-226", "119-1"], "the stale held act, then the cold start, and no private law"
    assert len(_rows(table3)) == len(TABLE3_PAGE.records)
    assert all(r["observed_at"] == OBSERVED_AT for r in _rows(table3)), "the stale act's rows are replaced whole"


def test_table_iii_honours_its_own_cap(tmp_path, scoped):
    olrc = StubOlrc()
    _build(tmp_path, olrc=olrc, max_table3=0)
    assert olrc.acts == []


def _last_read_digest(olrc, key):
    """The shaped-row digest a checkpoint records for ``key``'s page, or ``None`` when the stub serves none."""
    try:
        acquired = olrc.acquire_table3_act(key)
    except (UsCodeSourceError, ConnectionError):
        return None
    return _rows_digest(_page_rows(acquired.result, OBSERVED_AT))


def _walk_111(tmp_path, monkeypatch, *, chain, numbers=range(220, 231), held=(220,), **olrc_kw):
    """Walk the 111th's public laws ``numbers``, as the laws table states them, with ``held`` acts published.

    Returns the acts asked and the act keys whose rows this run published; ``olrc_kw["log"]``, a list,
    collects the run's log lines.
    """
    log = olrc_kw.pop("log", None)
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "111")
    seed(
        tmp_path,
        "laws",
        [{"law_id": f"111-public-{n}", "congress": "111", "law_type": "public", "number": str(n)} for n in numbers],
    )
    if held:
        seed(
            tmp_path, "table3_records", [{"act_key": f"111-{n}", "seq": "0", "observed_at": "2026-09-01"} for n in held]
        )
        # Each held act was last read under the current rule and its page has not changed since.
        pages = StubOlrc(chain=chain, **{k: v for k, v in olrc_kw.items() if k != "refusing"})
        path = tmp_path / "_table3_records_prior.parquet"
        table = pq.read_table(path)
        metadata = checkpoint_metadata(path, "laws-table3", [
            {"act_key": f"111-{n}", "reader_version": TABLE3_READER_VERSION, "sha256": _Capture(TABLE3_BYTES).sha256,
             "records_sha256": _last_read_digest(pages, f"111-{n}"), "observed_at": OBSERVED_AT, "outcome": "complete"}
            for n in held])
        pq.write_table(table.replace_schema_metadata({k.encode(): v.encode() for k, v in metadata.items()}), path)
    olrc = StubOlrc(chain=chain, **olrc_kw)
    sink = logger.add(log.append, format="{message}") if log is not None else None
    try:
        _, _, table3 = _build(tmp_path, olrc=olrc)
    finally:
        if sink is not None:
            logger.remove(sink)
    return olrc.acts, {r["act_key"] for r in _rows(table3) if r["observed_at"] == OBSERVED_AT}


def test_table_iii_follows_the_chain_from_the_highest_held_act_to_its_end(tmp_path, monkeypatch):
    # 119-4 names 119-12 and 119-12 names 119-18: the chain skips the acts the table holds no page for.
    chain = {"111-222": "111-224", "111-224": "111-227", "111-227": None}
    asked, read = _walk_111(tmp_path, monkeypatch, chain=chain, held=(220, 222))
    assert asked == ["111-222", "111-224", "111-227"], "no number the chain does not name is asked"
    assert read == {"111-224", "111-227"}


@pytest.mark.parametrize(
    ("following", "numbers", "release_point", "reason"),
    [
        (None, range(220, 231), "119-73", "names no next public law"),
        ("111-226", range(220, 226), "119-73", "names an act outside the caller's bound"),
        ("111-226", range(220, 231), "111-224", "names an act past the release point it states"),
        ("112-226", range(220, 231), "119-73", "names an act in another Congress"),
        ("111-221", range(220, 231), "119-73", "names an act that does not follow it"),
    ],
    ids=["names-no-next-act", "past-the-laws-table", "past-the-release-point", "another-congress", "does-not-follow"],
)
def test_table_iii_chain_ends_where_the_page_leads_nowhere_it_may_ask(
    tmp_path, monkeypatch, following, numbers, release_point, reason
):
    # Every act a wrong turn would reach is served, so a missing guard shows up as an extra ask.
    chain = {"111-220": "111-223", "111-223": following, "111-226": None, "111-221": None, "112-226": None}
    log: list[str] = []
    asked, read = _walk_111(tmp_path, monkeypatch, chain=chain, numbers=numbers, release_point=release_point, log=log)
    assert asked == ["111-220", "111-223"] and read == {"111-223"}
    ends = [line for line in log if line.startswith("Laws: Table III chain for Congress 111 ends at 111-223: ")]
    assert ends and ends[0].startswith(f"Laws: Table III chain for Congress 111 ends at 111-223: its page {reason} (")


def test_a_chain_that_ends_on_its_own_spends_no_cap(tmp_path, monkeypatch):
    """The cap is spent per request: the 111th's two acts and the 110th's one fit a cap of three exactly."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "110,111")
    seed(
        tmp_path,
        "laws",
        [
            {"law_id": f"{c}-public-{n}", "congress": str(c), "law_type": "public", "number": str(n)}
            for c, n in ((111, 220), (111, 223), (110, 1))
        ],
    )
    olrc = StubOlrc(chain={"111-220": "111-223", "111-223": None, "110-1": None})
    _, _, table3 = _build(tmp_path, olrc=olrc, max_table3=3)
    assert olrc.acts == ["111-220", "111-223", "110-1"]
    assert {r["act_key"] for r in _rows(table3)} == {"111-220", "111-223", "110-1"}


@pytest.mark.parametrize(
    ("chain", "asked"),
    [
        ({}, ["111-1", "110-1", "109-1"]),
        ({"109-1": None}, ["111-1", "110-1", "109-1", "108-1", "107-1"]),
    ],
    ids=["three-refusals-stop-the-walk", "a-page-read-resets-the-count"],
)
def test_a_page_the_reader_refuses_counts_toward_the_stop(tmp_path, monkeypatch, chain, asked):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "107,108,109,110,111")
    congresses = range(107, 112)
    seed(
        tmp_path,
        "laws",
        [{"law_id": f"{c}-public-1", "congress": str(c), "law_type": "public", "number": "1"} for c in congresses],
    )
    olrc = StubOlrc(chain=chain, refusing={f"{c}-1" for c in congresses} - set(chain))
    _build(tmp_path, olrc=olrc)
    assert olrc.acts == asked


@pytest.mark.parametrize("scope", ["110", "110,111"], ids=["first-congress", "after-another-congress"])
def test_a_start_the_walker_refuses_before_any_request_is_counted_as_that_start(tmp_path, monkeypatch, scope):
    """A laws row numbered 0 starts the 110th's chain at ``110-0``, which the walker refuses unasked.

    The failure is the start's own, never the previous Congress's last act, and needs no act asked.
    """
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", scope)
    seed(
        tmp_path,
        "laws",
        [
            {"law_id": f"{c}-public-{n}", "congress": str(c), "law_type": "public", "number": str(n)}
            for c, n in ((110, 0), (111, 226))
        ],
    )
    olrc = StubOlrc()
    log: list[str] = []
    sink = logger.add(log.append, format="{message}")
    try:
        _build(tmp_path, olrc=olrc)
    finally:
        logger.remove(sink)
    assert olrc.acts == (["111-226"] if "111" in scope else [])
    assert any(line.startswith("Laws: Table III chain for Congress 110 stops at 110-0: ") for line in log)
    assert any("failed ['110-0']" in line for line in log)


def test_a_chain_act_that_drops_is_retried_then_counted(tmp_path, monkeypatch):
    """Through the rollup's own acquirer: a dropped connection is retried by the transport, then is a failure."""
    monkeypatch.setattr("time.sleep", lambda seconds: None)  # the retry backoff
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "108,109,110,111")
    stated = [(111, 226), (111, 227), (110, 1), (109, 1), (108, 1)]
    seed(
        tmp_path,
        "laws",
        [{"law_id": f"{c}-public-{n}", "congress": str(c), "law_type": "public", "number": str(n)} for c, n in stated],
    )
    asked: list[str] = []

    class Dropped(httpx.SyncByteStream):
        def __iter__(self):
            yield TABLE3_BYTES[:4096]
            raise httpx.RemoteProtocolError("peer closed connection without sending complete message body")

    def answer(request: httpx.Request) -> httpx.Response:
        asked.append(request.url.path)
        served = request.url.path == "/table3/111_226.htm"
        stream = httpx.ByteStream(TABLE3_BYTES) if served else Dropped()
        return httpx.Response(200, headers={"content-type": "text/html;charset=UTF-8"}, stream=stream)

    unpaced = replace(OLRC_BUDGET, min_request_interval_seconds=0.0)
    _, _, table3 = _build(tmp_path, olrc=olrc_acquirer(httpx.MockTransport(answer), budget=unpaced))
    attempts = OLRC_BUDGET.max_requests
    # 111-226 is read and names 111-227, which drops on every attempt; 110-1
    # and 109-1 drop too, and three failures in a row leave 108-1 unasked.
    assert [path for path in asked if path.startswith("/table3/")] == (
        ["/table3/111_226.htm"]
        + ["/table3/111_227.htm"] * attempts
        + ["/table3/110_1.htm"] * attempts
        + ["/table3/109_1.htm"] * attempts
    )
    assert {r["act_key"] for r in _rows(table3)} == {"111-226"}


def test_table_iii_walk_stops_at_its_deadline():
    olrc = StubOlrc(chain={"111-220": "111-223", "111-223": "111-226", "111-226": None})
    ticks = iter([0.0, 0.0, 0.0, 60.0])  # the start, then before each request
    rows = _table3_rows(
        olrc,
        {(111, n) for n in range(220, 231)},
        {"111-220"},
        PerRunCap(300, "Laws: Table III pages"),
        deadline_seconds=60.0,
        clock=lambda: next(ticks),
    )
    assert olrc.acts == ["111-220", "111-223"] and {r["act_key"] for r in rows} == {"111-223"}


def test_the_citation_reaches_congress_bills_from_a_law_the_run_captured(tmp_path, scoped):
    """End to end: the laws rollup captures 119-1's PLAW, and the next congress_bills merge carries its citation."""
    from spicy_docs.schemas import TABLE_CONTRACTS

    from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path

    laws, _, _ = _build(tmp_path)
    assert _by_law(laws)["119-public-1"]["statutes_at_large_cite"] == "139 Stat. 3"
    # The published laws table is what the next congress_bills writer downloads.
    laws.rename(prior_scratch_path(tmp_path, "laws"))
    bill = {c: None for c in TABLE_CONTRACTS["congress_bills"].columns}
    bill.update(
        {"bill_id": "119-s-5", "congress": "119", "bill_type": "s", "bill_number": "5", "update_date": "2026-09-01"}
    )
    lagging = dict(bill, bill_id="119-s-307", bill_number="307")
    out = merge_contract_table(tmp_path, "congress_bills", [bill, lagging], download_prior=no_download)
    rows = {r["bill_id"]: r for r in pq.read_table(out).to_pylist()}
    assert rows["119-s-5"]["statutes_at_large_cite"] == "139 Stat. 3"
    assert rows["119-s-307"]["statutes_at_large_cite"] is None, "119-110's PLAW lagged; nothing is guessed"


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
                "uslm_reader_version": USLM_READER_VERSION,
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
                "uslm_reader_version": USLM_READER_VERSION,
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
    # The failed reread does not erase its previously validated native body.
    assert rows["119-public-109"]["uslm_outcome"] == "captured"
    assert rows["119-public-109"]["statutes_at_large_cite"] == "140 Stat. 1"
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
    assert rows["119-public-104"]["uslm_outcome"] == "request_failed"
    assert (
        rows["119-public-1"]["uslm_outcome"] == "request_failed"
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


class RepairOlrc(StubOlrc):
    def __init__(self, *, empty=(), failing=()):
        super().__init__(chain={"119-37": "119-73", "119-73": None}, refusing=failing)
        self.empty = set(empty)

    def acquire_table3_act(self, key, **kwargs):
        if key != "119-37":
            result = super().acquire_table3_act(key, **kwargs)
        else:
            self.acts.append(key)
            if key in self.refusing:
                raise ConnectionError("fixture failed")
            body = (FIXTURES / "table3-119_37.htm").read_bytes()
            result = SimpleNamespace(result=parse_table3_page(body, key=key), capture=_Capture(body))
        if key in self.empty:
            result.result = replace(result.result, records=())
        return result


def _repair_seed(tmp_path):
    seed(tmp_path, "laws", [{"law_id": f"119-public-{n}", "congress": "119", "law_type": "public", "number": str(n)}
                            for n in (37, 73)])
    seed(tmp_path, "table3_records", [
        {"act_key": "119-37", "seq": str(i), "act_section": "old-filtered", "observed_at": "2026-09-01"}
        for i in range(106)] + [{"act_key": "119-73", "seq": "0", "act_section": "old-frontier"},
                               {"act_key": "118-1", "seq": "0", "act_section": "unrelated"}])


def test_stale_held_act_is_replaced_whole_before_forward_discovery(tmp_path, scoped):
    _repair_seed(tmp_path)
    olrc = RepairOlrc()
    _, _, path = _build(tmp_path, reader=StubListingReader({}), olrc=olrc, max_table3=1)
    assert olrc.acts == ["119-37"], "older stale act must not disappear behind maximum held act 119-73"
    rows = _rows(path)
    repaired = sorted((r for r in rows if r["act_key"] == "119-37"), key=lambda r: int(r["seq"]))
    assert len(repaired) == 110
    assert [r["seq"] for r in repaired] == [str(i) for i in range(110)]
    assert sum(r["act_section"] is None for r in repaired) == 4
    assert not any(r["act_section"] == "old-filtered" for r in repaired)
    assert next(r for r in rows if r["act_key"] == "119-73")["act_section"] == "old-frontier"
    assert next(r for r in rows if r["act_key"] == "118-1")["act_section"] == "unrelated"
    checkpoint = next(r for r in read_checkpoints(path, "laws-table3") if r["act_key"] == "119-37")
    assert checkpoint["reader_version"] == TABLE3_READER_VERSION
    assert checkpoint["sha256"] == _Capture((FIXTURES / "table3-119_37.htm").read_bytes()).sha256


@pytest.mark.parametrize("empty", [False, True])
def test_failed_reread_preserves_rows_but_successful_empty_removes_them(tmp_path, scoped, empty):
    _repair_seed(tmp_path)
    source = RepairOlrc(empty={"119-37"} if empty else (), failing=() if empty else {"119-37"})
    _, _, path = _build(tmp_path, reader=StubListingReader({}), olrc=source, max_table3=1)
    rows = [r for r in _rows(path) if r["act_key"] == "119-37"]
    assert len(rows) == (0 if empty else 106)
    checkpoint = next(r for r in read_checkpoints(path, "laws-table3") if r["act_key"] == "119-37")
    assert checkpoint["outcome"] == ("complete" if empty else "refused")
    assert (checkpoint.get("reader_version") == TABLE3_READER_VERSION) is empty


def test_bounded_reread_resumes_past_a_failed_old_act(tmp_path, scoped):
    import shutil
    _repair_seed(tmp_path)
    first = RepairOlrc(failing={"119-37"})
    _, _, path = _build(tmp_path, reader=StubListingReader({}), olrc=first, max_table3=1)
    shutil.copyfile(path, tmp_path / "_table3_records_prior.parquet")
    shutil.copyfile(tmp_path / "laws.parquet", tmp_path / "_laws_prior.parquet")
    second = RepairOlrc(failing={"119-37"})
    _build(tmp_path, reader=StubListingReader({}), olrc=second, max_table3=1)
    assert first.acts == ["119-37"] and second.acts == ["119-73"]


def test_new_congress_uses_only_the_previous_native_link_as_seed(tmp_path, scoped):
    seed(tmp_path, "table3_records", [{"act_key": "118-273", "seq": "0", "act_section": "held"}])
    seed(tmp_path, "laws", [{"law_id": "119-public-12", "congress": "119", "law_type": "public", "number": "12"}])
    source = StubOlrc(chain={"118-273": "119-12", "119-12": None})
    _, _, path = _build(tmp_path, reader=StubListingReader({}), olrc=source)
    assert source.acts == ["118-273", "119-12"]
    assert {r["act_key"] for r in _rows(path)} == {"118-273", "119-12"}


@pytest.mark.parametrize("number", [1, 2])
def test_private_law_read_is_partial_and_stands_under_the_current_rule(tmp_path, scoped, number):
    """A validated private law states no Statutes citation: ``captured_partial`` is its whole truth, not a retry."""
    from spicy_regs.transforms.build_laws import _law_rows, HeldLaw
    from spicy_docs.schemas.law_tables import shape_law

    body = (FIXTURES / f"plaw-119pvtl{number}.xml").read_bytes()
    selection = PublicLawSelection(119, "private", number)
    meta = validate_public_law_xml(body, selection=selection, final_url=public_law_xml_locator(selection))
    asked = []

    class Source:
        def acquire_public_law(self, selection, *, max_bytes=None):
            asked.append(selection)
            return SimpleNamespace(metadata=meta, capture=_Capture(body))

    law = {"number": f"119-{number}", "type": "Private Law"}
    plain = shape_law(LAW_119_1, law)
    old_rule = {str(plain["law_id"]): HeldLaw(plain["update_date"], "not_requested", None)}
    rows = _law_rows([(LAW_119_1, law, plain)], old_rule, Source(), PerRunCap(1, "test"))
    assert rows[0]["uslm_outcome"] == "captured_partial"
    assert rows[0]["uslm_reason"] == "statutes_citation_not_stated"
    assert rows[0]["approved_date"] == "2026-03-26"
    assert rows[0]["uslm_sha256"] == _Capture(body).sha256
    assert json.loads(rows[0]["uslm_citable_as_json"]) == [f"Private Law 119–{number}"]

    current = {str(plain["law_id"]): HeldLaw(plain["update_date"], "captured_partial", USLM_READER_VERSION)}
    assert _law_rows([(LAW_119_1, law, plain)], current, Source(), PerRunCap(1, "test")) == []
    assert len(asked) == 1, "an unchanged partial read under the current rule is not asked again"


@pytest.mark.parametrize("kind", ["html", "wrong-identity", "unavailable", "transport"])
def test_native_refusals_keep_truthful_states_and_evidence(tmp_path, kind):
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.transforms.build_laws import _uslm_row
    from spicy_docs.schemas.law_tables import shape_law
    from spicy_docs.sources.govinfo.uslm_acquisition import UslmAcquirer, UslmAcquisitionBudget
    body = b'<html><body>not the requested native XML</body></html>' if kind == "html" else USLM_BYTES
    def answer(request):
        if kind == "transport":
            raise httpx.ConnectError("fixture network failure")
        return httpx.Response(404 if kind == "unavailable" else 200, stream=httpx.ByteStream(body),
                              headers={"content-type": "text/html" if kind == "html" else "application/xml"})
    source = UslmAcquirer(budget=UslmAcquisitionBudget(max_requests=1, max_bytes=1024 * 1024, timeout_seconds=10, min_request_interval_seconds=0),
                          transport=httpx.MockTransport(answer))
    law = {"number": "119-2" if kind == "wrong-identity" else "119-1", "type": "Public Law"}
    evidence = CaptureEvidence(tmp_path, "laws")
    row = _uslm_row(source, LAW_119_1, law, shape_law(LAW_119_1, law), evidence)
    expected = {"html": ("captured_refused", "native_shape_refused"),
                "wrong-identity": ("captured_refused", "native_identity_refused"),
                "unavailable": ("unavailable", "source_unavailable"),
                "transport": ("request_failed", "request_failed")}[kind]
    assert (row["uslm_outcome"], row["uslm_reason"]) == expected
    assert row["uslm_title"] is None and row["approved_date"] is None
    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    assert any(e["event"] == "refusal" for e in events)
    if kind != "transport":
        assert any(e["event"] == "capture" for e in events)
    source.close()


def test_uslm_reader_version_invalidates_an_unchanged_complete_row():
    from spicy_regs.transforms.build_laws import _law_rows, HeldLaw
    from spicy_docs.schemas.law_tables import shape_law
    law = LAW_119_1["laws"][0]
    plain = shape_law(LAW_119_1, law)
    source = StubUslm()
    rows = _law_rows([(LAW_119_1, law, plain)],
                     {str(plain["law_id"]): HeldLaw(plain["update_date"], "captured", "old-rule")},
                     source, PerRunCap(1, "test"))
    assert len(source.selections) == 1 and rows[0]["uslm_reader_version"] == USLM_READER_VERSION


def test_current_rule_without_capture_proof_does_not_complete_a_table3_read():
    from spicy_regs.transforms.build_laws import _table3_current
    assert not _table3_current({"reader_version": TABLE3_READER_VERSION, "outcome": "complete"})
    assert not _table3_current({"reader_version": TABLE3_READER_VERSION, "outcome": "refused",
                               "sha256": _Capture(TABLE3_BYTES).sha256, "observed_at": OBSERVED_AT})


def _current(olrc, *keys):
    """Checkpoints saying each act was last read under the current rule, from the page ``olrc`` serves now."""
    return {key: {"act_key": key, "reader_version": TABLE3_READER_VERSION, "sha256": _Capture(TABLE3_BYTES).sha256,
                  "records_sha256": _last_read_digest(olrc, key), "observed_at": OBSERVED_AT, "outcome": "complete"}
            for key in keys}


def _cold_walk(chain, release_point, acts, *, seed_fails=False):
    olrc = StubOlrc(chain=chain, release_point=release_point, refusing=("119-73",) if seed_fails else ())
    checkpoints = _current(StubOlrc(chain=chain, release_point=release_point), "119-73")
    evaluated: set[str] = set()
    _table3_rows(olrc, acts, {"119-73"}, PerRunCap(10, "test"), checkpoints=checkpoints, evaluated=evaluated)
    return olrc.acts, evaluated


def test_a_new_congress_starts_where_the_previous_congress_page_says_the_chain_continues():
    asked, read = _cold_walk({"119-73": "120-2", "120-2": None}, "120-5", {(119, 73), (120, 1), (120, 2)})
    assert asked == ["119-73", "120-2"], "the seed's own link, not the lowest act, and no guess"
    assert read == {"120-2"}


def test_a_new_congress_is_not_asked_while_the_release_point_is_still_in_the_previous_one():
    asked, read = _cold_walk({"119-73": "119-74", "119-74": None}, "119-73", {(119, 73), (119, 74), (120, 1)})
    assert "120-1" not in asked, "Table III holds none of the 120th yet"
    assert read == set()


def test_a_new_congress_starts_cold_when_the_seed_names_another_congress_past_the_release():
    # The regression the review found: an incomplete 119 frontier must not silently suppress the 120th.
    asked, read = _cold_walk({"119-73": "119-74", "120-1": None}, "120-3", {(119, 73), (120, 1), (120, 3)})
    assert asked[:2] == ["119-73", "120-1"]
    assert "120-1" in read


def test_a_seed_that_cannot_be_read_falls_back_to_the_cold_start():
    asked, read = _cold_walk({"120-1": None}, "120-3", {(119, 73), (120, 1)}, seed_fails=True)
    assert asked[:2] == ["119-73", "120-1"]
    assert read == {"120-1"}


def test_stale_rereads_share_the_walk_s_consecutive_failure_stop():
    stale = {f"111-{n}" for n in range(220, 230)}
    olrc = StubOlrc(chain={})  # every act fails in transport: an OLRC outage
    checkpoints: dict[str, dict] = {}
    _table3_rows(olrc, {(111, n) for n in range(220, 230)}, stale, PerRunCap(50, "test"), checkpoints=checkpoints,
                 evaluated=set())
    # The stale queue stops after TABLE3_STOP_AFTER failures; the forward walk's own first failure then stops the run.
    assert len(olrc.acts) == TABLE3_STOP_AFTER + 1, "an outage costs a few requests, not one per stale act"
    assert all(checkpoints[key]["outcome"] == "refused" for key in olrc.acts[:TABLE3_STOP_AFTER])


def test_an_unchanged_frontier_page_is_not_published_again_but_a_changed_one_is():
    chain = {"111-226": "111-227"}
    acts = {(111, 226), (111, 227)}
    for release_point, published in (("119-73", set()), ("119-80", {"111-226"})):
        checkpoints = _current(StubOlrc(chain=chain, release_point="119-73"), "111-226")
        evaluated: set[str] = set()
        _table3_rows(StubOlrc(chain=chain, release_point=release_point), acts, {"111-226"}, PerRunCap(10, "test"),
                     checkpoints=checkpoints, evaluated=evaluated)
        assert evaluated == published, f"release point {release_point}"


@pytest.mark.parametrize("failure", ["transport", "html"])
def test_a_failed_reread_never_replaces_a_validated_prior_row(scoped, failure):
    from spicy_regs.transforms.build_laws import _law_rows, HeldLaw
    from spicy_docs.schemas.law_tables import shape_law
    from spicy_docs.sources.govinfo.uslm_acquisition import UslmAcquirer, UslmAcquisitionBudget

    def answer(request):
        if failure == "transport":
            raise httpx.ConnectError("fixture network failure")
        return httpx.Response(200, stream=httpx.ByteStream(b"<html>blocked</html>"), headers={"content-type": "text/html"})

    law = LAW_119_1["laws"][0]
    plain = shape_law(LAW_119_1, law)
    held = {str(plain["law_id"]): HeldLaw("an older list stamp", "captured", USLM_READER_VERSION)}
    budget = UslmAcquisitionBudget(max_requests=1, max_bytes=1024 * 1024, timeout_seconds=10,
                                   min_request_interval_seconds=0)
    with UslmAcquirer(budget=budget, transport=httpx.MockTransport(answer)) as source:
        assert _law_rows([(LAW_119_1, law, plain)], held, source, PerRunCap(1, "test")) == []


def test_metadata_the_contract_refuses_is_captured_refused_without_its_fields(scoped):
    from spicy_regs.transforms.build_laws import _uslm_row
    from spicy_docs.schemas.law_tables import shape_law

    selection = PublicLawSelection(119, "public", 1)
    meta = validate_public_law_xml(USLM_BYTES, selection=selection, final_url=public_law_xml_locator(selection))

    class Source:
        def acquire_public_law(self, selection, *, max_bytes=None):
            return SimpleNamespace(metadata=meta, capture=_Capture(USLM_BYTES))

    law = {"number": "119-2", "type": "Public Law"}  # the list row names another law than the validated file
    row = _uslm_row(Source(), LAW_119_1, law, shape_law(LAW_119_1, law))
    assert (row["uslm_outcome"], row["uslm_reason"]) == ("captured_refused", "contract_refused")
    assert row["uslm_title"] is None and row["statutes_at_large_cite"] is None


def test_stale_acts_that_keep_failing_never_stop_the_forward_walk():
    """Three held acts that always fail end only the stale queue; the frontier still advances."""
    chain = {"119-73": "119-74", "119-74": None}
    acts = {(119, n) for n in (10, 11, 12, 73, 74)}
    held = {"119-10", "119-11", "119-12", "119-73"}
    checkpoints = _current(StubOlrc(chain=chain, release_point="119-80"), "119-73")
    for _ in range(2):  # every run, not only the first
        olrc = StubOlrc(chain=chain, release_point="119-80")
        evaluated: set[str] = set()
        _table3_rows(olrc, acts, held, PerRunCap(20, "test"), checkpoints=checkpoints, evaluated=evaluated)
        assert olrc.acts[:3] == ["119-10", "119-11", "119-12"]
        assert "119-74" in olrc.acts and "119-74" in evaluated
