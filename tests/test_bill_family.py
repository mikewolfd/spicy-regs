"""Hermetic end-to-end test for the bill-family transform.

No network and no model: the two acquirers are stubbed with the fixture bytes
copied from spicy-docs (see ``tests/fixtures/govinfo_bills/README.md``), and
both model seams stay unwired because no key is set. What this establishes is
that the transform drives ``build_bill_family`` correctly and publishes all
thirteen tables plus its own archive state — not that any rule inside it is
right, which is spicy-docs' own test's job.

119 HR 6028 is used because it offers two consecutive printings, which is the
smallest input that reaches the section and diff tables as well as the
status-derived ones.

``StubBulkAcquirer`` reproduces ``BulkStatusAcquirer``'s ``unchanged_since``
contract rather than just recording the argument: reading the listing only when
given something to compare against, refusing an entry that names a different
file, and returning ``skipped_unchanged`` with no archive and no zip capture.
A stub that always served the zip could not tell a wired skip from an unwired
one, which is the whole point of the second-run test below.
"""

from __future__ import annotations

import json
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from loguru import logger
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.congress.bill_status import BillIdentity, parse_bill_status
from spicy_docs.sources.congress.bill_status import BillSourceError
from spicy_docs.sources.congress.bulk_status import BulkListingEntry
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.transforms.build_bill_family import (
    ARCHIVE_COLUMNS,
    ARCHIVES_TABLE,
    FAMILY_TABLES,
    VOTE_REFERENCE_COLUMNS,
    VOTE_REFERENCES_TABLE,
    build_bill_family,
    engine_stamp,
)
from tests.pdf_fixtures import make_multiline_pdf

FIXTURES = Path(__file__).parent / "fixtures" / "govinfo_bills"
IDENTITY = BillIdentity(congress=119, bill_type="hr", number=6028)
OBSERVED_AT = "2026-09-19T00:00:00Z"
BULKDATA = "https://www.govinfo.gov/bulkdata/BILLSTATUS"

#: The printings the fixture bill offers, by the package id each resolves to.
TEXT_FIXTURES = {
    "BILLS-119hr6028ih": "text-119hr6028ih.xml",
    "BILLS-119hr6028eh": "text-119hr6028eh.xml",
}


def _capture(url: str, body: bytes) -> CapturedBodyResponse:
    return CapturedBodyResponse(
        requested_url=url,
        resolved_url=url,
        status_code=200,
        content_type="text/xml",
        observed_at=OBSERVED_AT,
        body=body,
    )


class _Member:
    def __init__(self, status):
        self.status = status
        self.identity = status.identity
        self.refusal = None


class _Archive:
    def __init__(self, members):
        self.members = tuple(members)
        self.parsed_count = len(self.members)
        self.refused_count = 0


class _Acquisition:
    def __init__(self, archive, capture, *, skipped_unchanged=False, listing_capture=None, listing_entry=None):
        self.archive = archive
        self.capture = capture
        self.skipped_unchanged = skipped_unchanged
        self.listing_capture = listing_capture
        self.listing_entry = listing_entry


class _Listing:
    def __init__(self, zip_entry):
        self.zip_entry = zip_entry


class _ListingAcquisition:
    def __init__(self, listing, capture):
        self.listing = listing
        self.capture = capture


def zip_entry(congress: int, bill_type: str, *, size: int = 31_656_886) -> BulkListingEntry:
    """The folder's own zip entry as GovInfo's bulkdata listing states it."""
    name = f"BILLSTATUS-{congress}-{bill_type}.zip"
    return BulkListingEntry(
        name=name,
        display_label=name,
        just_file_name=name,
        link=f"{BULKDATA}/{congress}/{bill_type}/{name}",
        folder=False,
        formatted_last_modified_time="18-Sep-2026 20:26",
        modified_at=datetime(2026, 9, 18, 20, 26, tzinfo=UTC),
        mime_type="application/zip",
        file_extension="zip",
        formatted_size="30 MB",
        size=size,
    )


class StubBulkAcquirer:
    """Serves the one fixture bill for (119, hr) and an empty archive otherwise.

    Honors ``unchanged_since`` the way ``BulkStatusAcquirer`` does, so
    ``zip_downloads`` records only the folders whose zip bytes were really
    read and ``listings`` records the small listing requests.
    """

    def __init__(self, entry=zip_entry, status: bytes | None = None):
        self.calls: list[tuple[int, str]] = []
        self.zip_downloads: list[tuple[int, str]] = []
        self.listings: list[tuple[int, str]] = []
        self._entry = entry
        self._status = status

    def _listing_capture(self, congress: int, bill_type: str) -> CapturedBodyResponse:
        return _capture(f"{BULKDATA.replace('/bulkdata/', '/bulkdata/json/')}/{congress}/{bill_type}", b"{}")

    def list_archives(self, congress: int, bill_type: str):
        self.listings.append((congress, bill_type))
        return _ListingAcquisition(
            _Listing(self._entry(congress, bill_type)), self._listing_capture(congress, bill_type)
        )

    def acquire(self, congress: int, bill_type: str, *, unchanged_since: BulkListingEntry | None = None):
        self.calls.append((congress, bill_type))
        entry = self._entry(congress, bill_type)
        listing_capture = None
        if unchanged_since is not None:
            self.listings.append((congress, bill_type))
            listing_capture = self._listing_capture(congress, bill_type)
            if (unchanged_since.name, unchanged_since.link) != (entry.name, entry.link):
                raise BillSourceError("unchanged_since names a different file than this folder's own zip entry")
            if (unchanged_since.modified_at, unchanged_since.size) == (entry.modified_at, entry.size):
                return _Acquisition(
                    None, None, skipped_unchanged=True, listing_capture=listing_capture, listing_entry=entry
                )

        self.zip_downloads.append((congress, bill_type))
        body = self._status if self._status is not None else (FIXTURES / "status-119hr6028.xml").read_bytes()
        capture = _capture(entry.link, body)
        members = [_Member(parse_bill_status(body, identity=IDENTITY))] if (congress, bill_type) == (119, "hr") else []
        return _Acquisition(
            _Archive(members),
            capture,
            listing_capture=listing_capture,
            listing_entry=entry if unchanged_since is not None else None,
        )


class _BodyIdentity:
    def __init__(self, media_type: str, byte_size: int):
        self.media_type = media_type
        self.byte_size = byte_size


class _Package:
    """The three facts `body_text` reads off a fetched body, plus the capture."""

    def __init__(self, fmt: str, capture: CapturedBodyResponse, *, media_type: str = "text/xml"):
        self.format = fmt
        self.body_capture = capture
        self.body = _BodyIdentity(media_type, len(capture.body))


class StubBodyAcquirer:
    """Serves a printing's XML from the fixtures; refuses a package it has none for."""

    def __init__(self):
        self.requested: list[str] = []

    def acquire(self, package_id: str, *, max_bytes=None):
        self.requested.append(package_id)
        name = TEXT_FIXTURES.get(package_id)
        if name is None:
            raise LookupError(f"no fixture for {package_id}")
        url = f"https://www.govinfo.gov/content/pkg/{package_id}/xml/{package_id}.xml"
        return _Package("xml", _capture(url, (FIXTURES / name).read_bytes()))


def _no_prior(remote_key: str, local_path: Path) -> bool:
    return False


def _prior_from(published: Path):
    """A ``download_prior`` that serves an earlier run's published files."""

    def download(remote_key: str, local_path: Path) -> bool:
        source = published / remote_key
        if not source.exists():
            return False
        shutil.copyfile(source, local_path)
        return True

    return download


@pytest.fixture
def family(tmp_path, monkeypatch):
    """One bill-family run over the fixture bill, keyless and offline."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    # No model key: the three model-backed tables must come back empty.
    for name in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
        monkeypatch.delenv(name, raising=False)

    paths = build_bill_family(
        tmp_path,
        bulk_acquirer=StubBulkAcquirer(),
        body_acquirer=StubBodyAcquirer(),
        download_prior=_no_prior,
    )
    return {path.stem: path for path in paths}


OWN_TABLES = {ARCHIVES_TABLE: ARCHIVE_COLUMNS, VOTE_REFERENCES_TABLE: VOTE_REFERENCE_COLUMNS}


def test_every_family_table_is_published(family):
    expected = {contract for contract, _ in FAMILY_TABLES} | {"public_activity_events", *OWN_TABLES}
    assert set(family) == expected
    assert len(family) == 15


def test_each_published_table_matches_its_contract_schema_or_its_own(family):
    """The thirteen contracts are spicy-docs'; the last two are this transform's."""
    for name, columns in OWN_TABLES.items():
        assert pq.read_table(family[name]).schema.names == list(columns), name


def test_each_published_table_matches_its_contract_schema(family):
    for name, path in family.items():
        if name in OWN_TABLES:
            continue
        contract = TABLE_CONTRACTS[name]
        assert pq.read_table(path).schema.names == list(contract.columns), name


def test_a_bill_with_no_recorded_votes_publishes_no_references(family):
    """The fixture bill's two actions record no roll call, so the table is empty rather than absent."""
    assert pq.read_table(family[VOTE_REFERENCES_TABLE]).to_pylist() == []


def test_the_bill_and_its_printings_are_there(family):
    bills = pq.read_table(family["congress_bills"]).to_pylist()
    assert [row["bill_id"] for row in bills] == ["119-hr-6028"]
    # The frozen prefix is filled from the same status as the appended columns.
    assert bills[0]["congress"] == "119"
    assert bills[0]["bill_type"] == "hr"

    versions = pq.read_table(family["bill_versions"]).to_pylist()
    # version_slug yields the full slug; the GovInfo suffix is what the
    # package id uses, which is why TEXT_FIXTURES is keyed the other way.
    assert {row["version_code"] for row in versions} == {"introduced-in-house", "engrossed-in-house"}
    assert all(row["bill_id"] == "119-hr-6028" for row in versions)
    # Bodies were fetched, so the capture columns are real rather than NULL.
    assert all(row["sha256"] and row["byte_size"] for row in versions)


def test_sections_are_parsed_and_every_parent_exists(family):
    sections = pq.read_table(family["bill_sections"]).to_pylist()
    assert sections, "the XML printings must yield sections"
    parents = {
        (row["bill_id"], row["version_code"], row["source"])
        for row in pq.read_table(family["bill_versions"]).to_pylist()
    }
    for row in sections:
        assert (row["bill_id"], row["version_code"], row["source"]) in parents


def test_the_consecutive_pair_is_actually_compared(family):
    """The reason the fixture is a pair: the diff tables must be non-empty.

    ``financial_changes`` is deliberately not asserted here — this pair changes
    no dollar figure, so it yields none, and asserting it would be asserting
    the fixture rather than the transform.
    """
    diffs = pq.read_table(family["section_diffs"]).to_pylist()
    assert len(diffs) == 1, "one consecutive pair means one comparison"
    assert diffs[0]["bill_id"] == "119-hr-6028"
    assert diffs[0]["engine_revision"], "every diff row names the engine that produced it"
    assert pq.read_table(family["section_diff_items"]).to_pylist(), "the comparison must settle items"


def test_the_model_tables_are_empty_without_a_key(family):
    for name in ("section_classifications", "bill_summaries", "diff_summaries"):
        assert pq.read_table(family[name]).to_pylist() == [], name


def test_the_first_run_reports_every_bill_as_added(family):
    """With no prior table, every bill is new — and that is what the events say."""
    events = pq.read_table(family["public_activity_events"]).to_pylist()
    assert events, "a first run over one bill must detect it"
    kinds = {row["event_type"] for row in events}
    assert "bill_added" in kinds
    assert all(row["bill_id"] == "119-hr-6028" for row in events)
    assert all(row["detected_at"] for row in events)


def test_only_the_scoped_archive_is_fetched(tmp_path, monkeypatch):
    """The scope env vars bound the walk; nothing outside them is requested."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr,s")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    bulk = StubBulkAcquirer()
    build_bill_family(tmp_path, bulk_acquirer=bulk, body_acquirer=StubBodyAcquirer(), download_prior=_no_prior)
    assert bulk.calls == [(119, "hr"), (119, "s")]


def test_the_version_fetch_cap_is_honored(tmp_path, monkeypatch):
    """Past the cap a printing still gets a row, with its body columns NULL."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    body = StubBodyAcquirer()
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path,
            bulk_acquirer=StubBulkAcquirer(),
            body_acquirer=body,
            max_version_fetches=1,
            download_prior=_no_prior,
        )
    }
    assert len(body.requested) == 1
    versions = pq.read_table(paths["bill_versions"]).to_pylist()
    assert len(versions) == 2, "both printings are still published"
    assert sum(1 for row in versions if row["sha256"]) == 1
    assert sum(1 for row in versions if row["sha256"] is None) == 1


def test_the_engine_stamp_carries_the_vendored_revision():
    """A wheel install states no commit, so the vendored one must be supplied."""
    stamp = engine_stamp()
    assert stamp.name == "deltatrack"
    assert stamp.version
    assert len(stamp.revision) == 40, "the pinned DeltaTrack commit belongs in every diff row"


# --------------------------------------------------------------------------- #
# Incremental behavior: a steady-state run must not re-fetch what it holds.
# --------------------------------------------------------------------------- #
def _seed_prior(output_dir: Path, table: str, rows: list[dict]) -> None:
    """Write a prior published table where the merge and the index will find it."""
    import pyarrow as pa
    from spicy_regs.transforms.table_merge import prior_scratch_path

    contract = TABLE_CONTRACTS[table]
    filled = [{c: None for c in contract.columns} | row for row in rows]
    pq.write_table(
        pa.Table.from_pylist(filled, schema=pa.schema([(c, pa.string()) for c in contract.columns])),
        prior_scratch_path(output_dir, table),
    )


def _status_text_date() -> str | None:
    from spicy_docs.sources.congress.bill_status import parse_bill_status

    body = (FIXTURES / "status-119hr6028.xml").read_bytes()
    return parse_bill_status(body, identity=IDENTITY).update_date_including_text


@pytest.fixture
def scoped(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)


def test_an_unchanged_bill_costs_no_requests(tmp_path, scoped):
    """The steady state: the publisher's text stamp is unchanged, so nothing is fetched."""
    _seed_prior(
        tmp_path,
        "congress_bills",
        [{"bill_id": "119-hr-6028", "update_date_including_text": _status_text_date()}],
    )
    body = StubBodyAcquirer()
    build_bill_family(tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=body, download_prior=_no_prior)
    assert body.requested == [], "an unchanged bill must not fetch a single printing"


def test_a_changed_bill_is_rebuilt(tmp_path, scoped):
    """A different text stamp means the publisher changed something; re-read it."""
    _seed_prior(
        tmp_path,
        "congress_bills",
        [{"bill_id": "119-hr-6028", "update_date_including_text": "1999-01-01T00:00:00Z"}],
    )
    body = StubBodyAcquirer()
    build_bill_family(tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=body, download_prior=_no_prior)
    assert len(body.requested) == 2


def test_printings_already_held_are_not_refetched(tmp_path, scoped):
    """Both printings published: a changed bill re-reads its status, not its bodies."""
    _seed_prior(
        tmp_path,
        "congress_bills",
        [{"bill_id": "119-hr-6028", "update_date_including_text": "1999-01-01T00:00:00Z"}],
    )
    _seed_prior(
        tmp_path,
        "bill_versions",
        [
            {"bill_id": "119-hr-6028", "version_code": code, "source": "govinfo", "sha256": "sha256:x"}
            for code in ("introduced-in-house", "engrossed-in-house")
        ],
    )
    body = StubBodyAcquirer()
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=body, download_prior=_no_prior
        )
    }
    assert body.requested == [], "a held printing needs no second fetch"
    # And the published rows are the prior ones, not degraded re-emissions.
    versions = pq.read_table(paths["bill_versions"]).to_pylist()
    assert len(versions) == 2
    assert all(row["sha256"] == "sha256:x" for row in versions), "a held row must not be overwritten with NULLs"


def test_a_new_printing_pulls_its_neighbour_so_the_diff_still_happens(tmp_path, scoped):
    """Only the predecessor is held; a diff needs both sides, so both are fetched."""
    _seed_prior(
        tmp_path,
        "congress_bills",
        [{"bill_id": "119-hr-6028", "update_date_including_text": "1999-01-01T00:00:00Z"}],
    )
    _seed_prior(
        tmp_path,
        "bill_versions",
        [{"bill_id": "119-hr-6028", "version_code": "introduced-in-house", "source": "govinfo"}],
    )
    body = StubBodyAcquirer()
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=body, download_prior=_no_prior
        )
    }
    assert set(body.requested) == {"BILLS-119hr6028ih", "BILLS-119hr6028eh"}
    assert pq.read_table(paths["section_diffs"]).to_pylist(), "the new pair must still be compared"


def test_needed_printings_picks_the_unheld_and_their_neighbours():
    from spicy_regs.transforms.build_bill_family import _needed_printings

    codes = ["a", "b", "c", "d"]
    assert _needed_printings(codes, held=set(codes)) == set()
    assert _needed_printings(codes, held={"a", "b", "c"}) == {2, 3}
    assert _needed_printings(codes, held=()) == {0, 1, 2, 3}


# --------------------------------------------------------------------------- #
# The bulk-listing skip: a folder whose zip has not moved is not downloaded.
# --------------------------------------------------------------------------- #
def _run(output_dir: Path, bulk: StubBulkAcquirer, download_prior) -> dict[str, Path]:
    output_dir.mkdir(exist_ok=True)
    paths = build_bill_family(
        output_dir, bulk_acquirer=bulk, body_acquirer=StubBodyAcquirer(), download_prior=download_prior
    )
    return {path.stem: path for path in paths}


def test_a_cold_folder_retains_the_listing_entry_it_did_not_need(tmp_path, scoped):
    """`acquire` reads no listing with nothing to compare, so the run asks for one itself.

    Without this, the retained table would stay empty and the skip could never
    fire on any later run — the failure would look exactly like a working
    pipeline that just never saves anything.
    """
    bulk = StubBulkAcquirer()
    paths = _run(tmp_path / "run", bulk, _no_prior)

    assert bulk.zip_downloads == [(119, "hr")], "a cold folder downloads its zip"
    assert bulk.listings == [(119, "hr")], "and asks for the listing once, to retain the entry"
    rows = pq.read_table(paths[ARCHIVES_TABLE]).to_pylist()
    assert len(rows) == 1
    expected = zip_entry(119, "hr")
    assert rows[0]["name"] == expected.name
    assert rows[0]["link"] == expected.link
    assert rows[0]["size"] == str(expected.size)
    assert rows[0]["modified_at"] == expected.modified_at.isoformat()
    assert rows[0]["congress"] == "119"
    assert rows[0]["bill_type"] == "hr"


def test_a_second_run_over_an_unchanged_listing_makes_no_zip_request(tmp_path, scoped):
    """The whole point of the retained entry: the zip is proved unchanged, not re-read."""
    first = tmp_path / "run1"
    _run(first, StubBulkAcquirer(), _no_prior)

    second = StubBulkAcquirer()
    paths = _run(tmp_path / "run2", second, _prior_from(first))

    assert second.calls == [(119, "hr")], "the folder is still visited"
    assert second.listings == [(119, "hr")], "through its listing, which is the cheap half"
    assert second.zip_downloads == [], "and the zip itself is never requested"
    # The entry is retained again, so a third run can skip on the same evidence.
    assert len(pq.read_table(paths[ARCHIVES_TABLE]).to_pylist()) == 1


def test_a_moved_zip_is_downloaded_again(tmp_path, scoped):
    """The comparison has to be able to say no, or the skip is just a cache that never expires."""
    first = tmp_path / "run1"
    _run(first, StubBulkAcquirer(), _no_prior)

    # The publisher rebuilt the zip: same name and link, different size.
    moved = StubBulkAcquirer(entry=lambda c, t: zip_entry(c, t, size=31_658_670))
    _run(tmp_path / "run2", moved, _prior_from(first))

    assert moved.zip_downloads == [(119, "hr")]
    assert moved.listings == [(119, "hr")], "the listing is read once, inside acquire"


def test_a_retained_entry_naming_another_file_does_not_wedge_the_rollup(tmp_path, scoped):
    """A stale row is recognised before a request, so the fallback costs one listing read.

    ``acquire`` would refuse this entry by name — but only after reading the
    folder listing, and its refusal carries no listing to reuse, so passing it
    anyway would buy the same cold download for two reads instead of one.
    """
    first = tmp_path / "run1"
    _run(first, StubBulkAcquirer(entry=lambda c, t: zip_entry(c, "sres")), _no_prior)

    renamed = StubBulkAcquirer()
    paths = _run(tmp_path / "run2", renamed, _prior_from(first))

    assert renamed.zip_downloads == [(119, "hr")], "the zip is fetched rather than the run failing"
    assert renamed.listings == [(119, "hr")], "and the folder listing is read exactly once"
    rows = pq.read_table(paths[ARCHIVES_TABLE]).to_pylist()
    assert {row["name"] for row in rows} == {"BILLSTATUS-119-hr.zip"}, "and the bad row is replaced"


# --------------------------------------------------------------------------- #
# Recorded votes: the fifteenth output, read off the bill's own actions.
# --------------------------------------------------------------------------- #
#: chamber, session, roll number, date, url, and the optional seventh field.
#: The Senate entry carries a `<fullActionName>`; the House one does not, which
#: is the pair the publisher's own measurement found (absent on 58 of 58 JSON
#: entries, documented by the BILLSTATUS guide, so both states are real).
RECORDED_VOTES = {
    # The Senate action (index 0 in the fixture) and the House floor action (index 1).
    "Received in the Senate.": (
        "Senate",
        "2",
        "00312",
        "2026-06-09T20:14:02Z",
        "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1192/vote_119_2_00312.xml",
        "MOTION TO CONSIDER",
    ),
    "Motion to reconsider laid on the table Agreed to without objection.": (
        "House",
        "2",
        "306",
        "2026-06-08T19:48:09Z",
        "https://clerk.house.gov/evs/2026/roll306.xml",
        None,
    ),
}


def _voted_status() -> bytes:
    """The fixture bill with one recorded vote on each of its two actions.

    Derived from the fixture rather than committed beside it, the way the
    PDF-only status is: only a `<recordedVotes>` block is added under each
    action's `<text>`, in the guide's own element spelling, so the parser
    reads it exactly as it reads a real BILLSTATUS. One vote per chamber, so
    both URL grammars and both chamber spellings are exercised.
    """
    raw = (FIXTURES / "status-119hr6028.xml").read_text()
    for action_text, (chamber, session, roll, date, url, full_name) in RECORDED_VOTES.items():
        named = "" if full_name is None else f"<fullActionName>{full_name}</fullActionName>"
        block = (
            f"<text>{action_text}</text>\n        <recordedVotes><recordedVote>"
            f"<chamber>{chamber}</chamber><congress>119</congress><date>{date}</date>{named}"
            f"<rollNumber>{roll}</rollNumber><sessionNumber>{session}</sessionNumber><url>{url}</url>"
            f"</recordedVote></recordedVotes>"
        )
        assert f"<text>{action_text}</text>" in raw
        raw = raw.replace(f"<text>{action_text}</text>", block, 1)
    return raw.encode()


def test_recorded_votes_on_a_bills_actions_are_published_as_references(tmp_path, scoped):
    """Each `recordedVotes` entry becomes one row naming the bill, the roll call and the action it sat on."""
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path,
            bulk_acquirer=StubBulkAcquirer(status=_voted_status()),
            body_acquirer=StubBodyAcquirer(),
            download_prior=_no_prior,
        )
    }
    rows = pq.read_table(paths[VOTE_REFERENCES_TABLE]).to_pylist()
    # Keyed by the action each entry sat on, because the merge publishes in
    # identity order (chamber sorts before session), not publisher order.
    by_action = {row["action_index"]: row for row in rows}
    assert [
        (by_action[i]["chamber"], by_action[i]["congress"], by_action[i]["session"], by_action[i]["roll_number"])
        for i in ("0", "1")
    ] == [
        ("senate", "119", "2", "312"),
        ("house", "119", "2", "306"),
    ], "chamber lowercased, numbers read as integers (the Senate's leading zeros go)"
    for row in rows:
        assert row["bill_id"] == "119-hr-6028"
        assert row["url"] and row["date"] and row["observed_at"] == OBSERVED_AT

    # The optional seventh field, both ways: `_full_action_name` finds the entry
    # again on its action by the key the reference states, so a publisher that
    # resumes sending it is carried rather than silently dropped, and one that
    # does not send it yields NULL rather than a guess.
    assert by_action["0"]["full_action_name"] == "MOTION TO CONSIDER"
    assert by_action["1"]["full_action_name"] is None


def test_references_are_keyed_by_action_so_one_roll_call_on_two_actions_is_two_rows(tmp_path, scoped):
    """The publisher attaches a passage vote to every floor action it settled; both are kept."""
    house = RECORDED_VOTES["Motion to reconsider laid on the table Agreed to without objection."]
    same_on_both = {text: house for text in RECORDED_VOTES}
    raw = (FIXTURES / "status-119hr6028.xml").read_text()
    for action_text, (chamber, session, roll, date, url, _full_name) in same_on_both.items():
        block = (
            f"<text>{action_text}</text><recordedVotes><recordedVote><chamber>{chamber}</chamber>"
            f"<congress>119</congress><date>{date}</date><rollNumber>{roll}</rollNumber>"
            f"<sessionNumber>{session}</sessionNumber><url>{url}</url></recordedVote></recordedVotes>"
        )
        raw = raw.replace(f"<text>{action_text}</text>", block, 1)
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path,
            bulk_acquirer=StubBulkAcquirer(status=raw.encode()),
            body_acquirer=StubBodyAcquirer(),
            download_prior=_no_prior,
        )
    }
    rows = pq.read_table(paths[VOTE_REFERENCES_TABLE]).to_pylist()
    assert sorted(r["action_index"] for r in rows) == ["0", "1"]
    assert {r["roll_number"] for r in rows} == {"306"}


# --------------------------------------------------------------------------- #
# The PDF rendition: reachable since 0.21.1 put PDF last in the preference
# rather than outside it, and the one branch that fills the cleanup_* columns.
# --------------------------------------------------------------------------- #
def _pdf_only_status() -> bytes:
    """The fixture bill with every printing offered only as PDF.

    The real case this stands in for is the pre-113th corpus, which offers no
    XML (spicy-docs `docs/research/pdf-only-corpus-2026-09-19.md`). Derived
    from the fixture rather than committed beside it so the two cannot drift:
    only the format URLs move, and `format_name` reads the rendition from the
    URL folder exactly as it does for the XML original.
    """
    raw = (FIXTURES / "status-119hr6028.xml").read_text()
    return re.sub(r"<url>(\S+?)/xml/(\S+?)\.xml</url>", r"<url>\1/pdf/\2.pdf</url>", raw).encode()


#: Two pages, each three real physical lines with its own gutter number
#: (1-3, a consecutive run starting at 1) and a genuine hyphen-wrap on the
#: first line, corroborated by its gutter number — the shape a real GPO
#: gutter-numbered page has under PyMuPDF's line-grouped extraction (a
#: content line immediately followed by its own bare digit line; see
#: ``spicy_docs.extraction.gpo_normalize``'s module docstring). No real fixture
#: from ``spicy-docs``' own ``tests/fixtures/gpo_pdf_text/README.md``
#: provenance is both under 200 KB and gutter-numbered — its two documents
#: under 200 KB (the ENR bill at 196,785 bytes, the committee report at
#: 199,803 bytes) are never GPO line-numbered by GPO's own print convention,
#: and its two gutter-numbered documents (223,439 and 206,513 bytes) both
#: exceed 200 KB — so this synthetic PDF stands in, built to exercise
#: ``is_gpo_layout`` for real rather than merely asserting its output is not
#: None.
PDF_GPO_PAGES = [
    [
        "Introduc-",
        "1",
        "ing this measure to amend the Act.",
        "2",
        "Additional provision text follows here today.",
        "3",
    ],
    [
        "Further findings support the measure describ-",
        "1",
        "ed in section one, presented here today now.",
        "2",
        "Enacted this day pursuant to the authority granted.",
        "3",
    ],
]


class StubPdfBodyAcquirer:
    """Serves each printing as a real, minimal, genuinely GPO-gutter-numbered PDF."""

    def __init__(self):
        self.requested: list[str] = []

    def acquire(self, package_id: str, *, max_bytes=None):
        self.requested.append(package_id)
        url = f"https://www.govinfo.gov/content/pkg/{package_id}/pdf/{package_id}.pdf"
        body = make_multiline_pdf(PDF_GPO_PAGES)
        capture = CapturedBodyResponse(
            requested_url=url,
            resolved_url=url,
            status_code=200,
            content_type="application/pdf",
            observed_at=OBSERVED_AT,
            body=body,
        )
        return _Package("pdf", capture, media_type="application/pdf")


@pytest.fixture
def pdf_family(tmp_path, scoped):
    """One run over a bill whose printings are offered only as PDF, with the warnings it logged."""
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        paths = build_bill_family(
            tmp_path,
            bulk_acquirer=StubBulkAcquirer(status=_pdf_only_status()),
            body_acquirer=StubPdfBodyAcquirer(),
            download_prior=_no_prior,
        )
    finally:
        logger.remove(sink)
    return {path.stem: path for path in paths}, messages


def test_a_pdf_printing_is_fetched_and_published_as_one(pdf_family):
    """PDF is last in the preference, not outside it: the body columns are real."""
    paths, _ = pdf_family
    versions = pq.read_table(paths["bill_versions"]).to_pylist()
    assert len(versions) == 2
    assert {row["format_name"] for row in versions} == {"pdf"}
    for row in versions:
        assert row["source"] == "govinfo"
        assert row["sha256"] and row["byte_size"] and row["observed_at"] and row["resolved_url"]


def test_the_pdf_cleanup_record_reaches_the_cleanup_columns(pdf_family):
    """`body_text`'s PDF branch is the GPO normalizer, and its record is what these columns are.

    They were NULL on every row before the 0.21.1 adoption: the transform
    never ran `body_text` at all. They went silently null again, in a
    different way, when this repository's PDF branch ran through
    `PypdfPageExtractor` instead of `body_text`'s default PyMuPDF extractor:
    pypdf glues a GPO gutter number onto the end of its content line
    ("Representa-1") rather than emitting it as PyMuPDF does — its own
    physical line immediately after — so `is_gpo_layout`'s adjacency
    detector never fires on a pypdf-read page, `cleanup_line_numbers` stays
    `False` on a genuinely numbered document, and `hyphen_rejoin_count` stays
    `0` since rejoin is gated on the layout verdict (measured
    `docs/research/gpo-normalizer-vs-upstream-2026-09-19.md` in spicy-docs:
    0 of 6, then 0 of 12, real gutter numbers rejoined under pypdf, vs 6 of 6
    and 12 of 12 under PyMuPDF). `PDF_GPO_PAGES` is genuinely gutter-numbered
    (see its own docstring), so asserting the layout verdict and the rejoin
    count here — not just that the columns are non-null — is what would catch
    a regression back to an extractor that defeats the normalizer.
    """
    paths, _ = pdf_family
    versions = pq.read_table(paths["bill_versions"]).to_pylist()
    for row in versions:
        assert row["cleanup_line_numbers"] == "true", "PDF_GPO_PAGES is gutter-numbered; the layout must be detected"
        assert row["cleanup_gpo_footers"] is not None
        assert row["cleanup_spacing_normalized"] is not None
        assert row["cleanup_hyphen_rejoins"] == "2", "one gutter-corroborated hyphen wrap per page, two pages"
        pages = json.loads(row["cleanup_json"])
        assert [page["page"] for page in pages] == [1, 2], "one entry per PDF page, one-based"
        # The two fields 0.21.1 added to GpoPageCleanup, serialized upstream.
        assert all("running_footer_lines" in page and "content_lines" in page for page in pages)


def test_a_pdf_only_pair_is_refused_by_name_not_silently_skipped(pdf_family):
    """No XML means no section tree, so the pair cannot be diffed — and says so."""
    paths, messages = pdf_family
    assert pq.read_table(paths["bill_sections"]).to_pylist() == [], "a PDF printing has no section tree"
    assert pq.read_table(paths["section_diffs"]).to_pylist() == [], "so the consecutive pair yields no diff"
    refusals = [line for line in messages if "refusals by table" in line]
    assert refusals, "the refusal must be reported, not left as an empty table"
    assert "section_diffs" in refusals[0]
