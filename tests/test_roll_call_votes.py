"""Hermetic test for the roll-call transform's two linkage sources.

No network: the listing reader and the Clerk acquirer are stubbed, and the
``bill_vote_references`` table the bill family publishes is seeded where the
transform's best-effort download looks for it.

The headline case is the measured sample — the 58 ``recordedVotes`` entries
spicy-docs read off 20 bills of the 119th Congress
(``tests/fixtures/congress_votes/README.md``) — which must resolve to 34
distinct roll calls with no two references disagreeing. That number is a
property of real publisher data, so a change in how this transform reads or
indexes the references moves it; a synthetic fixture could not fail the same
way.
"""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.interpretation.vote_matching import VoteMatchError, index_vote_references, read_vote_key
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.congress.votes import SenateVoteMenu, SenateVoteMenuEntry, VoteRefusedError, VoteSourceError

from spicy_regs.transforms.build_bill_family import VOTE_REFERENCE_COLUMNS, VOTE_REFERENCES_TABLE
from spicy_regs.transforms.build_roll_call_votes import (
    _recorded_vote_references,
    build_roll_call_votes,
)
from spicy_regs.transforms.table_merge import prior_scratch_path

SAMPLE = Path(__file__).parent / "fixtures" / "congress_votes" / "recorded-votes-119-sample.json"
OBSERVED_AT = "2026-09-19T00:00:00Z"


def _sample_rows() -> list[dict]:
    """The 58 measured entries as the bill family would have published them.

    Normalized through ``read_vote_key``, the same reader the family's own
    ``recorded_vote_references`` call goes through, so the chamber is
    lowercased and the numbers are read as integers exactly as they would be
    on a real run. ``action_index`` is the entry's position in its bill's
    list, which reproduces the measured shape: the publisher attaches one
    roll call to each floor action it settled, so a bill's entries repeat.
    """
    sample = json.loads(SAMPLE.read_text())
    rows = []
    for bill in sample["bills"]:
        for action_index, entry in enumerate(bill["votes"]):
            key = read_vote_key(entry)
            bill_type = "".join(c for c in bill["bill"] if c.isalpha())
            number = "".join(c for c in bill["bill"] if c.isdigit())
            rows.append(
                {
                    "bill_id": f"{key.congress}-{bill_type}-{number}",
                    "chamber": key.chamber,
                    "congress": str(key.congress),
                    "session": str(key.session),
                    "roll_number": str(key.roll_number),
                    "action_index": str(action_index),
                    "url": entry["url"],
                    "date": entry["date"],
                    "full_action_name": None,
                    "observed_at": OBSERVED_AT,
                }
            )
    return rows


def _seed_references(output_dir: Path, rows: list[dict]) -> None:
    """Write the family's published reference table where the transform will find it."""
    pq.write_table(
        pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in VOTE_REFERENCE_COLUMNS])),
        prior_scratch_path(output_dir, VOTE_REFERENCES_TABLE),
    )


def _no_prior(remote_key: str, local_path: Path) -> bool:
    return False


class _Page:
    def __init__(self, records):
        self.records = tuple(records)


class StubListingReader:
    """Serves each ``house-vote`` record on the session its own path addresses.

    The transform walks one URL per session of the Congress, so a stub that
    ignored the path would serve every record on both walks. A duplicated
    record names the same bill as itself and so cannot conflict with itself —
    but it does duplicate any *genuine* disagreement, because the listing
    reference that loses to a recorded vote then loses to it twice, and
    ``conflict_count`` reads 2 where the input stated one disagreement. The
    served rows are held under a different name than the Protocol's ``records``
    method, which they would shadow.
    """

    def __init__(self, listed=()):
        self.listed = listed

    def records(self, route, url, *, max_pages=100):
        session = int(url.rsplit("?", 1)[0].rsplit("/", 1)[-1])
        page = [record for record in self.listed if record["sessionNumber"] == session]
        return iter((_Page(page),)) if page else iter(())


class _Tallied:
    """The facts ``shape_roll_call_vote`` and ``shape_member_vote`` read off a Clerk file."""

    def __init__(self, locator):
        self.congress = locator.congress
        self.chamber = locator.chamber
        self.session = locator.session
        self.roll_number = locator.roll_number
        self.date = "2025-09-08"
        self.source_url = locator.url()
        self.question = "On Passage"
        self.result = "Passed"
        self.tallies = {"yea-total": 220, "nay-total": 210}
        self.member_votes = ()


class _Acquisition:
    def __init__(self, vote):
        self.vote = vote


class StubVoteAcquirer:
    def __init__(self, senate_rolls=()):
        self.requested: list[tuple[str, int]] = []
        self.senate_rolls = senate_rolls

    def list_senate_votes(self, congress, session):
        # Empty tuples isolate House-only unit selections; the real provider
        # refuses an empty published XML menu, covered by the failure tests.
        entries = tuple(
            SenateVoteMenuEntry(n, "1-Jan", None, None, None, None, {}, "Fixture")
            for n in self.senate_rolls
            if session == 1
        )
        return SimpleNamespace(menu=SenateVoteMenu(congress, session, 2025 + session - 1, entries))

    def acquire(self, locator, *, crosswalk=None):
        self.requested.append((locator.chamber, locator.roll_number))
        return _Acquisition(_Tallied(locator))


@pytest.fixture
def scoped(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")


def _house_listing_record(roll_number: int, *, bill_type: str = "HR", number: str = "3424") -> dict:
    return {
        "congress": 119,
        "sessionNumber": 1,
        "rollCallNumber": roll_number,
        "legislationType": bill_type,
        "legislationNumber": number,
        "sourceDataURL": f"https://clerk.house.gov/evs/2025/roll{roll_number}.xml",
        "startDate": "2025-09-08T18:56:00-04:00",
    }


# --------------------------------------------------------------------------- #
# The measured sample.
# --------------------------------------------------------------------------- #
def test_the_measured_sample_resolves_thirty_four_roll_calls_with_no_conflicts(tmp_path, scoped):
    """58 entries, 34 distinct roll calls, zero disagreements — read back through this transform.

    The 58-to-34 collapse is the publisher's own shape: a passage vote is
    recorded on both the "On passage" action and the "Motion to reconsider"
    that follows it. Zero conflicts is the stronger half of the claim — no
    roll call in the sample is claimed by two different bills — and it is what
    makes `conflict_count` zero on every row rather than merely unset.
    """
    rows = _sample_rows()
    assert len(rows) == 58, "the fixture is the measured sample, not a reduction of it"
    _seed_references(tmp_path, rows)

    references = _recorded_vote_references(tmp_path, (119,), _no_prior)
    assert len(references) == 58, "every measured entry is read back as a reference"

    index = index_vote_references(references)
    assert len(index.by_vote) == 34
    assert index.conflicts == ()
    assert {key.chamber for key in index.by_vote} == {"house", "senate"}
    assert sum(1 for key in index.by_vote if key.chamber == "senate") == 6


def test_the_sample_is_not_read_for_a_congress_out_of_scope(tmp_path, monkeypatch):
    """The scope bounds the read; the table accumulates across every Congress the family has run."""
    _seed_references(tmp_path, _sample_rows())
    assert _recorded_vote_references(tmp_path, (118,), _no_prior) == ()


# --------------------------------------------------------------------------- #
# The published rows.
# --------------------------------------------------------------------------- #
def test_a_recorded_vote_reference_fills_the_linkage_columns(tmp_path, scoped):
    """The four match columns come off the reference that won, per row."""
    rows = [row for row in _sample_rows() if row["chamber"] == "house" and row["roll_number"] == "240"]
    assert rows, "roll 240 is in the sample"
    _seed_references(tmp_path, rows)

    acquirer = StubVoteAcquirer()
    paths = build_roll_call_votes(
        tmp_path,
        reader=StubListingReader(),
        acquirer=acquirer,
        overlap=0,
        download_prior=_no_prior,
    )
    published = pq.read_table(paths[0]).to_pylist()
    assert len(published) == 1, "one roll call, however many actions recorded it"
    row = published[0]
    assert row["bill_id"] == "119-hr-3424"
    assert row["match_rule"] == "bill_action_recorded_vote"
    assert row["match_action_index"] == "0", "the first reference indexed for this vote wins"
    assert row["match_url"] == "https://clerk.house.gov/evs/2025/roll240.xml"
    assert row["conflict_count"] == "0"
    assert row["yea"] == "220", "the linkage does not disturb the tally half"


def test_the_listing_still_links_a_vote_no_reference_names(tmp_path, scoped):
    """With no family run published, the House listing's own linkage is unchanged."""
    acquirer = StubVoteAcquirer()
    paths = build_roll_call_votes(
        tmp_path,
        reader=StubListingReader([_house_listing_record(240)]),
        acquirer=acquirer,
        overlap=0,
        download_prior=_no_prior,
    )
    row = pq.read_table(paths[0]).to_pylist()[0]
    assert row["bill_id"] == "119-hr-3424"
    assert row["match_rule"] == "house_vote_legislation"
    assert row["match_action_index"] is None, "a listing reference sits on no action"
    assert row["conflict_count"] == "0"


def test_the_bills_own_action_wins_over_the_listing(tmp_path, scoped):
    """Both publishers name roll 240; the one written on the bill's action is the join."""
    _seed_references(tmp_path, [row for row in _sample_rows() if row["roll_number"] == "240"])
    paths = build_roll_call_votes(
        tmp_path,
        # The listing names a different bill for the same roll call.
        reader=StubListingReader([_house_listing_record(240, number="9999")]),
        acquirer=StubVoteAcquirer(),
        overlap=0,
        download_prior=_no_prior,
    )
    row = pq.read_table(paths[0]).to_pylist()[0]
    assert row["bill_id"] == "119-hr-3424"
    assert row["match_rule"] == "bill_action_recorded_vote"
    assert row["conflict_count"] == "1", "the losing reference is counted on this row, not dropped"


def test_a_conflict_is_counted_on_the_row_it_belongs_to(tmp_path, scoped):
    """Per vote, not per run: an uncontested roll call must not inherit another's count."""
    _seed_references(tmp_path, [row for row in _sample_rows() if row["roll_number"] in ("240", "241")])
    paths = build_roll_call_votes(
        tmp_path,
        reader=StubListingReader(
            [
                _house_listing_record(240, number="9999"),  # disagrees
                _house_listing_record(241, number="3425"),  # agrees
            ]
        ),
        acquirer=StubVoteAcquirer(),
        overlap=0,
        download_prior=_no_prior,
    )
    by_roll = {row["roll_number"]: row for row in pq.read_table(paths[0]).to_pylist()}
    assert by_roll["240"]["conflict_count"] == "1"
    assert by_roll["241"]["conflict_count"] == "0", "one contested roll call does not contest the others"


def test_senate_references_do_not_substitute_for_menu_enumeration(tmp_path, scoped):
    """Bill-scoped references alone cannot establish a Senate acquisition population."""
    senate = [row for row in _sample_rows() if row["chamber"] == "senate"]
    assert senate, "the sample carries Senate roll calls"
    _seed_references(tmp_path, senate)

    acquirer = StubVoteAcquirer()
    paths = build_roll_call_votes(
        tmp_path,
        reader=StubListingReader(),
        acquirer=acquirer,
        overlap=0,
        download_prior=_no_prior,
    )
    assert acquirer.requested == [], "no Senate roll call is fetched"
    assert pq.read_table(paths[0]).to_pylist() == []


@pytest.mark.parametrize("link", [None, "malformed"])
def test_unlinked_house_identity_is_acquired_once(tmp_path, scoped, link):
    record: dict[str, object] = {"congress": 119, "sessionNumber": 1, "rollCallNumber": 7}
    if link:
        record.update(legislationType="not-a-bill", legislationNumber="1")
    acquirer = StubVoteAcquirer()
    paths = build_roll_call_votes(
        tmp_path, reader=StubListingReader([record, record]), acquirer=acquirer, download_prior=_no_prior
    )
    [row] = pq.read_table(paths[0]).to_pylist()
    assert acquirer.requested == [("house", 7)]
    assert row["bill_id"] is None and row["match_rule"] == "unmatched"
    assert row["yea"] == "220"


def test_invalid_listing_identity_refuses_without_writing_outputs(tmp_path, scoped):
    acquirer = StubVoteAcquirer()
    with pytest.raises(VoteMatchError, match="rollCallNumber"):
        build_roll_call_votes(
            tmp_path,
            reader=StubListingReader([{"congress": 119, "sessionNumber": 1}]),
            acquirer=acquirer,
            download_prior=_no_prior,
        )
    assert acquirer.requested == []
    assert not (tmp_path / "roll_call_votes.parquet").exists()


def test_both_chambers_keep_same_roll_number_and_native_lis_identity(tmp_path, scoped):
    class Members(StubVoteAcquirer):
        def acquire(self, locator, *, crosswalk=None):
            acquired = super().acquire(locator)
            senate = locator.chamber == "senate"
            acquired.vote.member_votes = (
                SimpleNamespace(
                    lis_id="S001" if senate else None,
                    bioguide_id=None if senate else "A000001",
                    name="Member",
                    party="D",
                    state="CA",
                    vote="Yea",
                    vote_normalized="yea",
                ),
            )
            return acquired

    acquirer = Members(senate_rolls=(7, 7))
    paths = build_roll_call_votes(
        tmp_path, reader=StubListingReader([_house_listing_record(7)]), acquirer=acquirer, download_prior=_no_prior
    )
    votes = pq.read_table(paths[0]).to_pylist()
    members = pq.read_table(paths[1]).to_pylist()
    assert {r["vote_id"] for r in votes} == {"119-house-1-7", "119-senate-1-7"}
    assert sorted(acquirer.requested) == [("house", 7), ("senate", 7)]
    senate = next(row for row in members if row["chamber"] == "senate")
    assert senate["member_key"] == "lis:S001" and senate["lis_id"] == "S001" and senate["bioguide_id"] is None
    assert next(row for row in votes if row["chamber"] == "senate")["bill_id"] is None


@pytest.mark.parametrize(
    "error", [VoteSourceError("menu lists no votes"), VoteRefusedError("https://www.senate.gov/menu")]
)
def test_senate_menu_failure_preserves_existing_outputs(tmp_path, scoped, error):
    class FailedMenu(StubVoteAcquirer):
        def list_senate_votes(self, congress, session):
            raise error

    retained = tmp_path / "roll_call_votes.parquet"
    retained.write_bytes(b"prior output")
    acquirer = FailedMenu()
    with pytest.raises(type(error)):
        build_roll_call_votes(
            tmp_path,
            reader=StubListingReader([_house_listing_record(7)]),
            acquirer=acquirer,
            download_prior=_no_prior,
        )
    assert acquirer.requested == [] and retained.read_bytes() == b"prior output"


def test_small_cap_prioritizes_unseen_votes_over_both_chamber_refreshes(tmp_path, scoped):
    import shutil

    reader = StubListingReader([_house_listing_record(n) for n in (1, 2)])
    requested = []
    for attempt in range(4):
        run = tmp_path / str(attempt)
        run.mkdir()

        def prior(remote, local):
            p = tmp_path / str(attempt - 1) / remote
            if not p.exists():
                return False
            shutil.copyfile(p, local)
            return True

        acquirer = StubVoteAcquirer(senate_rolls=(1, 2))
        paths = build_roll_call_votes(
            run, reader=reader, acquirer=acquirer, max_votes=1, overlap=25, download_prior=prior
        )
        requested.extend(acquirer.requested)
    assert len(requested) == len(set(requested)) == 4
    assert len(pq.read_table(paths[0]).to_pylist()) == 4
    # Once the backfill is held, each chamber still receives its own refresh.
    attempt = 4
    run = tmp_path / str(attempt)
    run.mkdir()
    acquirer = StubVoteAcquirer(senate_rolls=(1, 2))
    build_roll_call_votes(run, reader=reader, acquirer=acquirer, max_votes=2, overlap=1, download_prior=prior)
    assert set(acquirer.requested) == {("house", 2), ("senate", 2)}


def test_successful_reacquisition_replaces_only_its_member_children(tmp_path, scoped, monkeypatch):
    from spicy_regs.sources import r2

    monkeypatch.setattr(r2, "download", lambda *_: pytest.fail("ignored injected prior reader"))
    columns = TABLE_CONTRACTS["member_votes"].columns
    old = [
        {
            "vote_id": "119-house-1-7",
            "member_key": "old",
            "congress": "119",
            "chamber": "house",
            "session": "1",
            "roll_number": "7",
        },
        {
            "vote_id": "119-house-1-8",
            "member_key": "keep",
            "congress": "119",
            "chamber": "house",
            "session": "1",
            "roll_number": "8",
        },
        {
            "vote_id": "119-house-1-6",
            "member_key": "capped",
            "congress": "119",
            "chamber": "house",
            "session": "1",
            "roll_number": "6",
        },
        {
            "vote_id": "119-house-1-9",
            "member_key": "unselected",
            "congress": "119",
            "chamber": "house",
            "session": "1",
            "roll_number": "9",
        },
    ]

    def prior(remote, local):
        if remote != "member_votes.parquet":
            return False
        pq.write_table(pa.Table.from_pylist(old, schema=pa.schema([(c, pa.string()) for c in columns])), local)
        return True

    class FailedVote(StubVoteAcquirer):
        def acquire(self, locator, *, crosswalk=None):
            if locator.roll_number == 8:
                raise VoteSourceError("retained failed scope")
            acquired = super().acquire(locator)
            acquired.vote.member_votes = (
                SimpleNamespace(
                    lis_id=None,
                    bioguide_id="A000001",
                    name="New member",
                    party="D",
                    state="CA",
                    vote="Yea",
                    vote_normalized="yea",
                ),
            )
            return acquired

    paths = build_roll_call_votes(
        tmp_path,
        reader=StubListingReader([_house_listing_record(n) for n in (6, 7, 8)]),
        acquirer=FailedVote(),
        download_prior=prior,
        max_votes=2,
    )
    rows = pq.read_table(paths[1]).to_pylist()
    assert {(r["vote_id"], r["member_key"]) for r in rows} == {
        ("119-house-1-8", "keep"),
        ("119-house-1-7", "A000001"),
        ("119-house-1-6", "capped"),
        ("119-house-1-9", "unselected"),
    }


def test_the_reference_scratch_file_is_not_left_beside_the_outputs(tmp_path, scoped):
    _seed_references(tmp_path, _sample_rows())
    build_roll_call_votes(
        tmp_path,
        reader=StubListingReader(),
        acquirer=StubVoteAcquirer(),
        overlap=0,
        download_prior=_no_prior,
    )
    assert not prior_scratch_path(tmp_path, VOTE_REFERENCES_TABLE).exists()


def test_a_malformed_reference_row_costs_that_row_only(tmp_path, scoped):
    """One unreadable row must not lose the linkage for every other vote in the run."""
    rows = [row for row in _sample_rows() if row["roll_number"] == "240"]
    rows.append({**rows[0], "bill_id": "not-a-bill-key", "roll_number": "241", "action_index": "0"})
    _seed_references(tmp_path, rows)

    references = _recorded_vote_references(tmp_path, (119,), _no_prior)
    assert len(references) == len(rows) - 1
    assert all(reference.bill.number == 3424 for reference in references)


def test_the_published_table_still_matches_its_contract(tmp_path, scoped):
    _seed_references(tmp_path, [row for row in _sample_rows() if row["roll_number"] == "240"])
    paths = build_roll_call_votes(
        tmp_path,
        reader=StubListingReader(),
        acquirer=StubVoteAcquirer(),
        overlap=0,
        download_prior=_no_prior,
    )
    assert pq.read_table(paths[0]).schema.names == list(TABLE_CONTRACTS["roll_call_votes"].columns)
