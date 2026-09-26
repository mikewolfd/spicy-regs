"""Hermetic tests for the laws transform: the enumeration, the PLAW leg, the two OLRC legs, and what a run re-asks.

No network. The list reader, the USLM acquirer and the OLRC acquirer are
stubbed over the wheel's own fixtures (``tests/fixtures/congress_laws/``),
parsed through the wheel's own readers, so what is proved here is this
repository's seam — which laws are asked about, in what order, and what is
published when a leg is not established — and never a shaper rule.
"""

from __future__ import annotations

import hashlib
import io
import json
import shutil
import zipfile
from collections import Counter
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
from spicy_docs.sources.uscode import UsCodeSourceError, parse_table3_page, read_table3_bulk_archive
from spicy_docs.sources.uscode.classification import parse_classification_index, parse_classification_table
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_docs.schemas.law_tables import USLM_READER_VERSION, shape_table3_record
from spicy_regs.transforms.read_checkpoints import read_checkpoints

from spicy_regs.transforms.build_laws import (
    OLRC_BUDGET,
    TABLE3_MAX_RETIRED_PER_RELEASE,
    _bulk_rows,
    _table3_rows,
    build_laws,
    olrc_acquirer,
    table3_rule,
)
from spicy_regs.transforms.congress_walk import PerRunCap
from tests.test_incremental_rollups import no_download, seed

FIXTURES = Path(__file__).parent / "fixtures" / "congress_laws"
LIST_PAGE = json.loads((FIXTURES / "congress-law-list.json").read_text())
LAW_119_1 = json.loads((FIXTURES / "congress-law-119-1.json").read_text())
USLM_BYTES = (FIXTURES / "plaw-119publ1.xml").read_bytes()
INDEX_BYTES = (FIXTURES / "classification-tables-index.shtml").read_bytes()
TABLE_BYTES = (FIXTURES / "classification-tbl119pl_2nd-head.htm").read_bytes()
#: Whole ``<act>`` fragments of ``fulldump@119-73.xml``, verbatim and in the file's order: 118-2, 119-30 and 119-53
#: (the two acts the page walk never reached), 119-37 in twelve fragments, 87-845 across volumes 76 and 76A, and the
#: chapter act 1789-08-07:9.
BULK_EXCERPT = (FIXTURES / "table3-bulk-119-73-excerpt.xml").read_bytes()
OBSERVED_AT = "2026-09-19T00:00:00Z"
#: The three listed rows plus the one law the USLM fixture states, as one page.
LISTED_119 = [*LIST_PAGE["bills"], LAW_119_1]


def _Capture(body: bytes, observed_at: str = OBSERVED_AT) -> CapturedBodyResponse:
    """An owner capture of ``body``, as the acquirers return one (evidence refuses anything else)."""
    return CapturedBodyResponse("https://stub.invalid/", "https://stub.invalid/", 200, None, observed_at, body)


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


def _bulk_zip(member: bytes = BULK_EXCERPT, release_point: str = "119-73", name: str | None = None) -> bytes:
    """The bulk zip OLRC serves: one ``fulldump@<release point>.xml`` member (or ``name``), with a fixed timestamp."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(zipfile.ZipInfo(name or f"fulldump@{release_point}.xml", date_time=(2026, 1, 23, 0, 0, 0)),
                         member)
    return buffer.getvalue()


class StubOlrc:
    """Serves the classification fixtures and one Table III bulk zip, read through the wheel's own bulk reader.

    ``bulk`` is the zip's member, or an exception every bulk read raises; ``member_name`` renames the member.
    """

    def __init__(self, *, bulk: bytes | Exception = BULK_EXCERPT, release_point="119-73", observed_at=OBSERVED_AT,
                 index_error=None, member_name=None):
        self.bulk = bulk
        self.release_point = release_point
        self.member_name = member_name
        self.observed_at = observed_at
        self.index_error = index_error
        self.tables: list[tuple[int, int, str]] = []
        self.bulk_reads = 0

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

    def acquire_table3_bulk(self, *, release_point=None, max_bytes=None):
        self.bulk_reads += 1
        if isinstance(self.bulk, Exception):
            raise self.bulk
        body = _bulk_zip(self.bulk, self.release_point, self.member_name)
        return SimpleNamespace(result=read_table3_bulk_archive(body), capture=_Capture(body, self.observed_at))


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


def _log_of(build):
    """Run ``build`` and return its log lines."""
    log: list[str] = []
    sink = logger.add(log.append, format="{message}")
    try:
        build()
    finally:
        logger.remove(sink)
    return log


def _again(tmp_path):
    """Make this run's outputs the next run's published priors."""
    for name in ("laws", "table3_records"):
        shutil.copyfile(tmp_path / f"{name}.parquet", tmp_path / f"_{name}_prior.parquet")


def _laws(tmp_path, *keys):
    seed(tmp_path, "laws", [{"law_id": f"{c}-public-{n}", "congress": str(c), "law_type": "public", "number": str(n)}
                            for c, n in (key.split("-") for key in keys)])


def test_table_iii_is_one_bulk_read_for_every_congress_the_laws_table_holds(tmp_path, scoped):
    _laws(tmp_path, "118-2")
    olrc = StubOlrc()
    _, _, table3 = _build(tmp_path, olrc=olrc)

    assert olrc.bulk_reads == 1
    rows = _rows(table3)
    # The 119th is scoped and the 118th is held; the 87th is neither, and a chapter act is no public law.
    assert Counter(r["act_key"] for r in rows) == {"118-2": 1, "119-30": 1, "119-37": 110, "119-53": 1}
    act = sorted((r for r in rows if r["act_key"] == "119-37"), key=lambda r: int(r["seq"]))
    assert [r["seq"] for r in act] == [str(i) for i in range(110)], "positions, though the file's sequence skips"
    assert [r["seq"] for r in act if r["act_section"] is None] == ["3", "4", "5", "17"]
    assert (act[17]["usc_title"], act[17]["usc_section"], act[17]["status"]) == ("2", "60a nt", "Elim.")
    (lost,) = (r for r in rows if r["act_key"] == "119-30")
    assert {k: lost[k] for k in ("stated_key", "congress", "act_date", "statutes_at_large_volume", "release_point",
                                 "act_section", "record_volume", "record_page", "usc_title", "usc_section")} == {
        "stated_key": "119-30", "congress": "119", "act_date": "2025-07-24", "statutes_at_large_volume": "139",
        "release_point": "119-73", "act_section": None, "record_volume": "139", "record_page": "473",
        "usc_title": "16", "usc_section": "668dd nt"}
    assert lost["observed_at"] == OBSERVED_AT
    (checkpoint,) = [c for c in read_checkpoints(table3, "laws-table3") if c["congress"] == "119"]
    assert checkpoint["rule"] == table3_rule() and checkpoint["release_point"] == "119-73"
    assert set(checkpoint["acts"]) == {"119-30", "119-37", "119-53"}


def test_the_bulk_states_what_the_act_s_own_page_states():
    """Act 119-37 two ways: its page (with the four blank act sections) and its twelve bulk fragments."""
    page = parse_table3_page((FIXTURES / "table3-119_37.htm").read_bytes(), key="119-37")
    from_page = [shape_table3_record(record, page=page, seq=seq, observed_at=OBSERVED_AT)
                 for seq, record in enumerate(page.records)]
    from_bulk = _bulk_rows(_bulk_zip(), {119}, release_point="119-73", observed_at=OBSERVED_AT)[119]["119-37"]
    rendition = ("stated_key", "congress", "act_date", "statutes_at_large_volume")
    assert [{k: v for k, v in row.items() if k not in rendition} for row in from_bulk] == [
        {k: v for k, v in row.items() if k not in rendition} for row in from_page]
    # The act-level context is the same statement in each source's own spelling.
    assert {tuple(row[k] for k in rendition) for row in from_page} == {("119\u201337", "119th Cong.", "Nov. 12, 2025",
                                                                        "139 Stat.")}
    assert {tuple(row[k] for k in rendition) for row in from_bulk} == {("119-37", "119", "2025-11-12", "139")}


def test_a_fragment_s_own_volume_goes_with_its_records_and_seq_runs_through_the_act():
    rows = _bulk_rows(_bulk_zip(), {87}, release_point="119-73", observed_at=OBSERVED_AT)[87]["87-845"]
    assert [r["seq"] for r in rows] == [str(i) for i in range(15)], "the 76A fragment's sequence restarts at 0"
    assert [r["statutes_at_large_volume"] for r in rows] == ["76"] + ["76A"] * 14
    # A record carries its fragment's volume only where it states a page, as a page's Statutes link would.
    assert (rows[0]["record_page"], rows[0]["record_volume"]) == (None, None)
    assert {r["record_volume"] for r in rows[1:]} == {"76A"}


def test_an_unchanged_bulk_derives_nothing(tmp_path, scoped):
    _, _, first = _build(tmp_path)
    published, checkpoints = _rows(first), read_checkpoints(first, "laws-table3")
    _again(tmp_path)
    olrc = StubOlrc(observed_at="2026-09-20T00:00:00Z")
    log = _log_of(lambda: _build(tmp_path, olrc=olrc))
    assert olrc.bulk_reads == 1, "the zip states no validator, so it is fetched; nothing else is"
    assert any("Laws: Table III bulk unchanged at release point 119-73" in line for line in log)
    table3 = tmp_path / "table3_records.parquet"
    assert _rows(table3) == published, "no row is published again, not even with a new observed_at"
    assert read_checkpoints(table3, "laws-table3") == checkpoints


def _fragment(member: bytes, act_id: bytes) -> tuple[int, int]:
    """Where one ``<act>`` fragment of the excerpt starts and ends, its trailing newline included."""
    start = member.index(b"<act id='" + act_id)
    return start, member.index(b"</act>", start) + len(b"</act>\n")


#: 119-53's one record moves from Elim. to Rep.
MOVED = (lambda start, end: BULK_EXCERPT[:start] + BULK_EXCERPT[start:end].replace(b">Elim.<", b">Rep.<")
         + BULK_EXCERPT[end:])(*_fragment(BULK_EXCERPT, b"4d1d41a4"))


def _without(member: bytes, *act_ids: bytes) -> bytes:
    for act_id in act_ids:
        start, end = _fragment(member, act_id)
        member = member[:start] + member[end:]
    return member


def _journal(evidence) -> list[dict]:
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]


def _rerun(tmp_path, olrc):
    """Publish the first run, then run ``olrc`` over it with evidence; the second run's journal and rows."""
    from spicy_regs.source_evidence import CaptureEvidence

    _, _, first = _build(tmp_path)
    before = (_rows(first), read_checkpoints(first, "laws-table3"))
    _again(tmp_path)
    evidence = CaptureEvidence(tmp_path, "laws")
    _, _, table3 = _build(tmp_path, olrc=olrc, evidence=evidence)
    return before, (_rows(table3), read_checkpoints(table3, "laws-table3")), _journal(evidence)


def test_a_corrected_bulk_publishes_only_the_acts_it_changed(tmp_path, scoped):
    later = "2026-09-20T00:00:00Z"
    _, (rows, _), journal = _rerun(tmp_path, StubOlrc(bulk=MOVED, observed_at=later))
    (moved,) = (r for r in rows if r["act_key"] == "119-53")
    assert (moved["status"], moved["observed_at"]) == ("Rep.", later)
    assert {r["observed_at"] for r in rows if r["act_key"] != "119-53"} == {OBSERVED_AT}, "unchanged rows stand"
    assert not any(e["event"] in ("rows-retired", "table3-bulk-refused") for e in journal)


@pytest.mark.parametrize(
    ("member", "release_point", "reason"),
    [
        (_without(MOVED, b"41c326fe"), "119-73", "acts-dropped-without-release-point-advance"),
        (_without(MOVED, b"41c326fe", b"4d1d41a4"), "119-80", "acts-dropped-past-bound"),
        (MOVED, "119-60", "release-point-regressed"),
    ],
    ids=["drop-at-the-same-release-point", "drop-more-than-the-bound", "release-point-regressed"],
)
def test_a_read_that_would_lose_acts_or_go_back_is_refused_whole(tmp_path, scoped, member, release_point, reason):
    """Table III only grows (0 drops in 182,775 act-transitions since 2020), so these read as a bad file, not news.

    The refused read publishes nothing at all, not even 119-53's corrected status, and keeps every checkpoint.
    """
    olrc = StubOlrc(bulk=member, release_point=release_point, observed_at="2026-09-20T00:00:00Z")
    before, after, journal = _rerun(tmp_path, olrc)
    assert after == before
    (refused,) = (e for e in journal if e["event"] == "table3-bulk-refused")
    assert (refused["reason"], refused["release_point"], refused["published_release_point"]) == (
        reason, release_point, "119-73")
    assert not any(e["event"] == "rows-retired" for e in journal)
    if "dropped" in refused:
        assert len(refused["dropped"]) > (TABLE3_MAX_RETIRED_PER_RELEASE if reason.endswith("bound") else 0)


def test_a_release_point_advance_retires_an_act_the_file_dropped_and_journals_it(tmp_path, scoped):
    olrc = StubOlrc(bulk=_without(MOVED, b"41c326fe"), release_point="119-80", observed_at="2026-09-20T00:00:00Z")
    (prior, _), (rows, checkpoints), journal = _rerun(tmp_path, olrc)
    assert "119-30" not in {r["act_key"] for r in rows}
    assert {r["release_point"] for r in rows} == {"119-80"}, "an advance restates every act"
    (checkpoint,) = checkpoints
    assert checkpoint["release_point"] == "119-80" and "119-30" not in checkpoint["acts"]
    (retired,) = (e for e in journal if e["event"] == "rows-retired")
    assert (retired["table"], retired["key"], retired["scope"]) == ("table3_records", ["act_key"],
                                                                    {"release_point": "119-80"})
    # What the audit reconciles: the act keys the prior held and the output does not, derived here without SQL.
    assert retired["rows"] == [[key] for key in sorted({r["act_key"] for r in prior} - {r["act_key"] for r in rows})]
    assert retired["rows"] == [["119-30"]]


@pytest.mark.parametrize(("release_point", "retired"), [("119-73", None), ("119-80", [["119-2"]])])
def test_a_page_era_act_with_no_bulk_checkpoint_is_held_all_the_same(tmp_path, scoped, release_point, retired):
    """The first bulk run finds only the page walk's per-act checkpoints, which it does not read; the rows say what
    is held. 119-2 is published and the file does not list it: refused at the same release point, retired past it."""
    from spicy_regs.source_evidence import CaptureEvidence

    seed(tmp_path, "table3_records", [{"act_key": "119-2", "seq": "0", "release_point": "119-73",
                                       "observed_at": "2026-09-01"}])
    evidence = CaptureEvidence(tmp_path, "laws")
    _, _, table3 = _build(tmp_path, olrc=StubOlrc(release_point=release_point), evidence=evidence)
    journal = _journal(evidence)
    held = "119-2" in {r["act_key"] for r in _rows(table3)}
    if retired is None:
        assert held and next(e for e in journal if e["event"] == "table3-bulk-refused")["dropped"] == ["119-2"]
    else:
        assert not held and next(e for e in journal if e["event"] == "rows-retired")["rows"] == retired


@pytest.mark.parametrize("change", ["release", "code"])
def test_a_spicy_docs_release_string_derives_nothing_and_its_code_derives_again(tmp_path, scoped, monkeypatch,
                                                                                change):
    """Keyed on SpicyDocs' code as print citations and committee reports are: a version-only pin changes nothing."""
    import importlib.metadata

    from spicy_regs.transforms import build_laws as module

    _, _, first = _build(tmp_path)
    published = _rows(first)
    _again(tmp_path)
    if change == "release":
        real = importlib.metadata.version
        monkeypatch.setattr(importlib.metadata, "version", lambda name: "9.9.9" if name == "spicy-docs" else real(name))
    else:
        monkeypatch.setattr(module, "spicy_docs_code", lambda: "0" * 64)
    log = _log_of(lambda: _build(tmp_path, olrc=StubOlrc(observed_at="2026-09-20T00:00:00Z")))
    table3 = tmp_path / "table3_records.parquet"
    assert any(("unchanged at release point" if change == "release" else "0 published []") in line for line in log)
    assert _rows(table3) == published, "the same file derives the same rows, so nothing is published either way"
    assert {c["rule"] for c in read_checkpoints(table3, "laws-table3")} == {module.table3_rule()}


def test_a_congress_that_left_the_laws_scope_keeps_its_table_iii_rows_current(tmp_path, monkeypatch):
    """Table III lags enactment by months, so the 119th keeps its rows current after the 120th replaces it in scope.

    There is no chain to seed and no start to guess: the 120th's acts appear in the file when Table III holds them.
    """
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "120")
    _laws(tmp_path, "119-30", "119-111")
    olrc = StubOlrc()
    log = _log_of(lambda: _build(tmp_path, reader=StubListingReader({}), olrc=olrc))
    assert olrc.bulk_reads == 1
    assert {r["act_key"] for r in _rows(tmp_path / "table3_records.parquet")} == {"119-30", "119-37", "119-53"}
    assert any("derived for Congresses [119, 120]" in line for line in log)


def test_a_failed_bulk_read_leaves_every_row_and_checkpoint_standing(tmp_path, scoped):
    _, _, first = _build(tmp_path)
    published, checkpoints = _rows(first), read_checkpoints(first, "laws-table3")
    _again(tmp_path)
    _build(tmp_path, olrc=StubOlrc(bulk=ConnectionError("stub: incomplete chunked read")))
    table3 = tmp_path / "table3_records.parquet"
    assert _rows(table3) == published
    assert read_checkpoints(table3, "laws-table3") == checkpoints


def test_the_bulk_is_fetched_through_the_rollup_s_own_acquirer_retried_and_retained(tmp_path, monkeypatch):
    """A zip dropped mid-way is retried like any transport failure; the one served is kept as evidence."""
    from spicy_regs.source_evidence import CaptureEvidence

    monkeypatch.setattr("time.sleep", lambda seconds: None)  # the retry backoff
    served = _bulk_zip()
    asked: list[str] = []

    class Dropped(httpx.SyncByteStream):
        def __iter__(self):
            yield served[:4096]
            raise httpx.RemoteProtocolError("peer closed connection without sending complete message body")

    def answer(request: httpx.Request) -> httpx.Response:
        asked.append(request.url.path)
        # The publisher states no Content-Type or Content-Length for the zip.
        return httpx.Response(200, stream=Dropped() if len(asked) == 1 else httpx.ByteStream(served))

    evidence = CaptureEvidence(tmp_path, "laws")
    unpaced = replace(OLRC_BUDGET, min_request_interval_seconds=0.0)
    evaluated: set[str] = set()
    with olrc_acquirer(httpx.MockTransport(answer), budget=unpaced) as olrc:
        read = _table3_rows(olrc, {119}, set(), None, {}, evaluated, evidence)
    assert asked == ["/table3/table3-xml-bulk.zip"] * 2
    assert evaluated == {"119-30", "119-37", "119-53"} and len(read.rows) == 112 and read.retiring == []
    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    (capture,) = (e for e in events if e["event"] == "capture")
    assert (capture["stage"], capture["sha256"]) == ("table3:bulk", "sha256:" + hashlib.sha256(served).hexdigest())
    assert any(e["event"] == "table3-bulk" and e["published"] == ["119-30", "119-37", "119-53"] for e in events)


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
