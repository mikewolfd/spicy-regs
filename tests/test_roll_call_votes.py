"""Hermetic test for the roll-call transform's two linkage sources.

No network: both chambers' indexes and the vote acquirer are stubbed, and the
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

import hashlib
import json
import re
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.interpretation.vote_matching import index_vote_references, read_vote_key
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.congress.votes import (
    ClerkVoteIndex,
    ClerkVoteIndexEntry,
    SenateVoteMenu,
    SenateVoteMenuEntry,
    VoteRefusedError,
    VoteSourceError,
    parse_clerk_vote,
    parse_senate_vote,
    vote_day,
)

from spicy_regs.transforms.build_bill_family import VOTE_REFERENCE_COLUMNS, VOTE_REFERENCES_TABLE
from spicy_regs.transforms.build_roll_call_votes import (
    _recorded_vote_references,
    build_roll_call_votes,
)
from spicy_regs.transforms.table_merge import prior_scratch_path

FIXTURES = Path(__file__).parent / "fixtures" / "congress_votes"
SAMPLE = FIXTURES / "recorded-votes-119-sample.json"
#: Real publisher bodies (README beside them): House 119-1-240 and Senate 119-1-1.
REAL_BODIES = {
    ("house", 240): (FIXTURES / "clerk-roll240.xml", parse_clerk_vote),
    ("senate", 1): (FIXTURES / "senate-vote-119-1-00001.xml", parse_senate_vote),
}
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


class _Tallied:
    """The facts ``shape_roll_call_vote`` and ``shape_member_vote`` read off a vote file, date in its chamber's spelling."""

    def __init__(self, locator):
        self.congress = locator.congress
        self.chamber = locator.chamber
        self.session = locator.session
        self.roll_number = locator.roll_number
        self.date = "8-Sep-2025" if locator.chamber == "house" else "September 8, 2025,  06:56 PM"
        self.source_url = locator.url()
        self.question = "On Passage"
        self.result = "Passed"
        self.tallies = {"yea-total": 220, "nay-total": 210}
        self.member_votes = ()
        # The real reader's Clerk shape: a captured House file keeps its legis-num (here none).
        self.publisher = "clerk" if locator.chamber == "house" else "senate-lis"
        self.legis_num = None

    @property
    def day(self):
        return vote_day(self.chamber, self.date)


class _Acquisition:
    """One acquired vote as the transform's acquirer returns it."""

    def __init__(self, vote):
        self.vote = vote


class StubVoteAcquirer:
    """Serves both chambers' session indexes and their acquisitions, recording every locator requested."""

    def __init__(self, senate_rolls=(), house_rolls=(), withheld_senate_rolls=()):
        self.requested: list[tuple[str, int]] = []
        self.senate_rolls = senate_rolls
        self.house_rolls = house_rolls
        self.withheld_senate_rolls = withheld_senate_rolls

    def list_house_votes(self, congress, session):
        # As with the menu, empty tuples isolate reference-driven selections;
        # the real reader refuses an empty or gapped Clerk index.
        entries = tuple(
            ClerkVoteIndexEntry(n, "8-Sep", None, None, None, None)
            for n in sorted(self.house_rolls, reverse=True)
            if session == 1
        )
        return SimpleNamespace(index=ClerkVoteIndex(congress, session, 1789 + 2 * (congress - 1) + session - 1, entries))

    def list_senate_votes(self, congress, session):
        # Empty tuples isolate House-only unit selections; the real provider
        # refuses an empty published XML menu, covered by the failure tests.
        entries = tuple(
            SenateVoteMenuEntry(n, "1-Jan", None, None, (), None, {}, "Fixture")
            for n in self.senate_rolls
            if session == 1
        ) + tuple(
            SenateVoteMenuEntry(n, "23-Oct", None, None, (), None, {}, "Vote data is unavailable due to secret session.",
                                data_available=False)
            for n in self.withheld_senate_rolls
            if session == 1
        )
        return SimpleNamespace(menu=SenateVoteMenu(congress, session, 2025 + session - 1, entries))

    def acquire(self, locator, *, crosswalk=None):
        self.requested.append((locator.chamber, locator.roll_number))
        return _Acquisition(_Tallied(locator))


@pytest.fixture
def scoped(monkeypatch):
    """Scope the bill family to the 119th Congress."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")


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
    assert references is not None
    assert len(references) == 58, "every measured entry is read back as a reference"

    index = index_vote_references(references)
    assert len(index.by_vote) == 34
    assert index.conflicts == ()
    assert {key.chamber for key in index.by_vote} == {"house", "senate"}
    assert sum(1 for key in index.by_vote if key.chamber == "senate") == 6


@pytest.mark.parametrize("earlier_index", [8, 9])
def test_recorded_action_indices_are_numeric_in_published_linkage(tmp_path, scoped, earlier_index):
    """The retained 119th-Congress cases contain index 10 beside 8 or 9."""
    base = next(row for row in _sample_rows() if row["chamber"] == "house" and row["roll_number"] == "240")
    _seed_references(tmp_path, [base | {"action_index": "10"}, base | {"action_index": str(earlier_index)}])
    paths = build_roll_call_votes(
        tmp_path,
        acquirer=StubVoteAcquirer(),
        download_prior=_no_prior,
    )
    row = pq.read_table(paths[0]).to_pylist()[0]
    assert row["match_action_index"] == str(earlier_index)
    assert row["bill_id"] == base["bill_id"]
    assert row["match_url"] == base["url"]
    assert row["conflict_count"] == "0"


def test_numeric_action_ties_keep_bill_order_and_malformed_reference_refusal(tmp_path):
    base = _sample_rows()[0]
    _seed_references(
        tmp_path,
        [
            base | {"action_index": "invalid", "bill_id": "119-hr-1"},
            base | {"action_index": "10", "bill_id": "119-hr-2"},
            base | {"action_index": "8", "bill_id": "119-hr-3"},
            base | {"action_index": "8", "bill_id": "119-hr-2"},
            base | {"action_index": None, "bill_id": "119-hr-1"},
        ],
    )
    references = _recorded_vote_references(tmp_path, (119,), _no_prior)
    assert references is not None
    assert [(reference.action_index, reference.bill.number) for reference in references] == [
        (8, 2),
        (8, 3),
        (10, 2),
        (None, 1),
    ]
    index = index_vote_references(references)
    assert next(iter(index.by_vote.values())).bill.number == 2
    assert len(index.conflicts) == 2


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


def test_a_roll_call_no_recorded_reference_names_is_published_unmatched(tmp_path, scoped):
    """No bill action records it and its file states no measure: the roll call keeps its tally, and no bill."""
    acquirer = StubVoteAcquirer(house_rolls=(240,))
    paths = build_roll_call_votes(tmp_path, acquirer=acquirer, overlap=0, download_prior=_no_prior)
    row = pq.read_table(paths[0]).to_pylist()[0]
    assert acquirer.requested == [("house", 240)]
    assert (row["bill_id"], row["match_rule"], row["match_action_index"]) == (None, "unmatched", None)
    assert row["conflict_count"] == "0" and row["yea"] == "220"


def test_a_conflict_is_counted_on_the_row_it_belongs_to(tmp_path, scoped):
    """Per vote, not per run: an uncontested roll call must not inherit another's count."""
    sample = [row for row in _sample_rows() if row["roll_number"] in ("240", "241")]
    disagreeing = next(row for row in sample if row["roll_number"] == "240") | {
        "bill_id": "119-hr-9999", "action_index": "99",
    }
    _seed_references(tmp_path, [*sample, disagreeing])
    paths = build_roll_call_votes(tmp_path, acquirer=StubVoteAcquirer(), overlap=0, download_prior=_no_prior)
    by_roll = {row["roll_number"]: row for row in pq.read_table(paths[0]).to_pylist()}
    assert by_roll["240"]["bill_id"] == "119-hr-3424", "the earliest action's reference wins"
    assert by_roll["240"]["conflict_count"] == "1", "the losing reference is counted on this row, not dropped"
    assert by_roll["241"]["conflict_count"] == "0", "one contested roll call does not contest the others"


def test_senate_references_do_not_substitute_for_menu_enumeration(tmp_path, scoped):
    """Bill-scoped references alone cannot establish a Senate acquisition population."""
    senate = [row for row in _sample_rows() if row["chamber"] == "senate"]
    assert senate, "the sample carries Senate roll calls"
    _seed_references(tmp_path, senate)

    acquirer = StubVoteAcquirer()
    paths = build_roll_call_votes(
        tmp_path,
        acquirer=acquirer,
        overlap=0,
        download_prior=_no_prior,
    )
    assert acquirer.requested == [], "no Senate roll call is fetched"
    assert pq.read_table(paths[0]).to_pylist() == []


def test_a_house_roll_call_a_reference_also_names_is_acquired_once(tmp_path, scoped):
    """The index and a recorded reference reaching one roll call select it once."""
    _seed_references(tmp_path, [row for row in _sample_rows() if row["roll_number"] == "240"])
    acquirer = StubVoteAcquirer(house_rolls=(240,))
    paths = build_roll_call_votes(tmp_path, acquirer=acquirer, download_prior=_no_prior)
    [row] = pq.read_table(paths[0]).to_pylist()
    assert acquirer.requested == [("house", 240)]
    assert row["bill_id"] == "119-hr-3424" and row["yea"] == "220"


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

    acquirer = Members(senate_rolls=(7, 7), house_rolls=(7,))
    paths = build_roll_call_votes(tmp_path, acquirer=acquirer, download_prior=_no_prior)
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
    acquirer = FailedMenu(house_rolls=(7,))
    with pytest.raises(type(error)):
        build_roll_call_votes(tmp_path, acquirer=acquirer, download_prior=_no_prior)
    assert acquirer.requested == [] and retained.read_bytes() == b"prior output"


@pytest.mark.parametrize("error", [VoteSourceError("index skips roll 100"), VoteRefusedError("https://clerk.house.gov")])
def test_clerk_index_failure_preserves_existing_outputs(tmp_path, scoped, error):
    """A refused or gapped House index cannot establish the House population, so nothing is written."""

    class FailedIndex(StubVoteAcquirer):
        def list_house_votes(self, congress, session):
            raise error

    retained = tmp_path / "roll_call_votes.parquet"
    retained.write_bytes(b"prior output")
    acquirer = FailedIndex(senate_rolls=(1,))
    with pytest.raises(type(error)):
        build_roll_call_votes(tmp_path, acquirer=acquirer, download_prior=_no_prior)
    assert acquirer.requested == [] and retained.read_bytes() == b"prior output"


def test_the_clerk_index_is_the_house_population_before_the_115th(tmp_path, monkeypatch):
    """Congress.gov's listing never reached before the 115th; the Clerk's own index is the House population."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "110")
    acquirer = StubVoteAcquirer(house_rolls=(1, 2, 3), senate_rolls=(1, 2))
    paths = build_roll_call_votes(
        tmp_path, acquirer=acquirer, download_prior=_no_prior, open_congresses=(119,)
    )
    assert sorted(acquirer.requested) == [("house", 1), ("house", 2), ("house", 3), ("senate", 1), ("senate", 2)]
    rows = pq.read_table(paths[0]).to_pylist()
    assert sorted(row["vote_id"] for row in rows) == [
        "110-house-1-1", "110-house-1-2", "110-house-1-3", "110-senate-1-1", "110-senate-1-2",
    ]
    assert {row["source_url"] for row in rows if row["chamber"] == "house"} == {
        f"https://clerk.house.gov/evs/2007/roll{n:03d}.xml" for n in (1, 2, 3)
    }


def test_a_closed_congress_is_read_once_and_resumes_only_what_is_missing(tmp_path, monkeypatch):
    """Held roll calls of a closed Congress are never re-read for corrections; a capped backfill resumes."""
    import shutil

    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "110")
    requested = []
    for attempt in range(3):
        run = tmp_path / str(attempt)
        run.mkdir()

        def prior(remote, local):
            previous = tmp_path / str(attempt - 1) / remote
            if not previous.exists():
                return False
            shutil.copyfile(previous, local)
            return True

        acquirer = StubVoteAcquirer(house_rolls=(1, 2, 3), senate_rolls=(1, 2))
        build_roll_call_votes(
            run, acquirer=acquirer, max_votes=3, overlap=25, download_prior=prior,
            open_congresses=(119,),
        )
        requested.append(sorted(acquirer.requested))
    # Newest first under the cap, then the rest, then nothing: no overlap re-read of a closed Congress.
    assert requested == [
        [("house", 2), ("house", 3), ("senate", 2)],
        [("house", 1), ("senate", 1)],
        [],
    ]


def test_small_cap_prioritizes_unseen_votes_over_both_chamber_refreshes(tmp_path, scoped):
    import shutil

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

        acquirer = StubVoteAcquirer(senate_rolls=(1, 2), house_rolls=(1, 2))
        paths = build_roll_call_votes(
            run, acquirer=acquirer, max_votes=1, overlap=25, download_prior=prior,
            open_congresses=(119,),
        )
        requested.extend(acquirer.requested)
    assert len(requested) == len(set(requested)) == 4
    assert len(pq.read_table(paths[0]).to_pylist()) == 4
    # Once the backfill is held, each chamber still receives its own refresh.
    attempt = 4
    run = tmp_path / str(attempt)
    run.mkdir()
    acquirer = StubVoteAcquirer(senate_rolls=(1, 2), house_rolls=(1, 2))
    build_roll_call_votes(run, acquirer=acquirer, max_votes=2, overlap=1, download_prior=prior, open_congresses=(119,))
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
        acquirer=FailedVote(house_rolls=(6, 7, 8)),
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
        acquirer=StubVoteAcquirer(),
        overlap=0,
        download_prior=_no_prior,
    )
    assert not prior_scratch_path(tmp_path, VOTE_REFERENCES_TABLE).exists()


@pytest.mark.parametrize("references_available", [True, False])
@pytest.mark.parametrize("refresh_native", [True, False])
def test_held_links_refresh_independently_of_native_fetch_and_preserve_members(
    tmp_path, scoped, references_available, refresh_native,
):
    from spicy_docs.schemas.congress_activity_tables import shape_roll_call_vote
    from spicy_docs.sources.congress.votes import VoteLocator

    old = shape_roll_call_vote(
        _Tallied(VoteLocator(chamber="senate", congress=119, session=1, roll_number=212)),
        tally={"yea-total": 220, "nay-total": 210}, member_vote_count=0,
    )
    old.update(bill_id="119-hr-1", match_rule="bill_action_recorded_vote", match_action_index="21",
               match_url="https://www.senate.gov/old", conflict_count="0")
    member = {**{name: old[name] for name in ("vote_id", "congress", "chamber", "session", "roll_number")},
              "member_key": "keep", "position": "Yea"}

    def prior(remote, local):
        name = remote.removesuffix(".parquet")
        values = {"roll_call_votes": [old], "member_votes": [member]}.get(name)
        if values is None:
            return False
        pq.write_table(pa.Table.from_pylist(values, schema=pa.schema([
            (column, pa.string()) for column in TABLE_CONTRACTS[name].columns
        ])), local)
        return True

    if references_available:
        _seed_references(tmp_path, [{
            **_sample_rows()[0], "congress": "119", "chamber": "senate", "session": "1",
            "roll_number": "212", "bill_id": "119-hr-1", "action_index": "17",
        }])
    acquirer = StubVoteAcquirer(senate_rolls=(212,))
    paths = build_roll_call_votes(
        tmp_path, acquirer=acquirer, download_prior=prior,
        overlap=1 if refresh_native else 0, max_votes=1 if refresh_native else 0, open_congresses=(119,),
    )
    actual = pq.read_table(paths[0]).to_pylist()[0]
    assert actual["match_action_index"] == ("17" if references_available else "21")
    relationship = {"bill_id", "match_rule", "match_action_index", "match_url", "conflict_count"}
    assert {k: v for k, v in actual.items() if k not in relationship} == {
        k: v for k, v in old.items() if k not in relationship
    }
    # Link-only corrections cannot replace a native roster. A successful native
    # reread still replaces it with the source's (here empty) complete roster.
    members = pq.read_table(paths[1]).to_pylist()
    assert ([row["member_key"] for row in members]) == ([] if refresh_native else ["keep"])
    assert acquirer.requested == ([("senate", 212)] if refresh_native else [])


def _held_row(roll: int, *, congress: int = 119) -> dict:
    """A held House roll call as a prior run published it, with a recorded-vote link at action 21."""
    from spicy_docs.schemas.congress_activity_tables import shape_roll_call_vote
    from spicy_docs.sources.congress.votes import VoteLocator

    row = shape_roll_call_vote(
        _Tallied(VoteLocator(chamber="house", congress=congress, session=1, roll_number=roll)),
        tally={"yea-total": 220, "nay-total": 210},
        member_vote_count=0,
    )
    row.update(bill_id=f"{congress}-hr-1", match_rule="bill_action_recorded_vote", match_action_index="21",
               match_url="https://clerk.house.gov/old", conflict_count="0")
    return row


def _published(tables: dict[str, list[dict]]):
    """A ``download_prior`` serving each named contract table's rows."""

    def download(remote: str, local: Path) -> bool:
        name = remote.removesuffix(".parquet")
        if name not in tables:
            return False
        schema = pa.schema([(column, pa.string()) for column in TABLE_CONTRACTS[name].columns])
        pq.write_table(pa.Table.from_pylist(tables[name], schema=schema), local)
        return True

    return download


def _events(evidence, name: str) -> list[dict]:
    lines = (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()
    return [event for event in map(json.loads, lines) if event["event"] == name]


@pytest.mark.parametrize("refresh_native", [False, True])
def test_a_missing_reference_input_never_changes_a_held_house_link(tmp_path, scoped, refresh_native):
    """No ``bill_vote_references`` file: an absent optional input must not act as a deletion.

    Neither the held vote nor the same vote re-read from its file may lose the
    recorded link it was published with.
    """
    from spicy_regs.source_evidence import CaptureEvidence

    old = _held_row(240)
    member = {**{name: old[name] for name in ("vote_id", "congress", "chamber", "session", "roll_number")},
              "member_key": "keep", "position": "Yea"}
    evidence = CaptureEvidence(tmp_path / "audit", "roll-call-votes")
    acquirer = StubVoteAcquirer(house_rolls=(240,))
    paths = build_roll_call_votes(
        tmp_path,
        acquirer=acquirer,
        download_prior=_published({"roll_call_votes": [old], "member_votes": [member]}),
        overlap=1 if refresh_native else 0,
        max_votes=1 if refresh_native else 0,
        evidence=evidence,
        open_congresses=(119,),
    )
    assert pq.read_table(paths[0]).to_pylist() == [old]
    assert acquirer.requested == ([("house", 240)] if refresh_native else [])
    members = [row["member_key"] for row in pq.read_table(paths[1]).to_pylist()]
    assert members == ([] if refresh_native else ["keep"])
    assert [event["available"] for event in _events(evidence, "vote-reference-input")] == [False]
    [linkage] = _events(evidence, "held-vote-linkage")
    assert linkage["input_available"] is False and linkage["relinked"] == []
    assert linkage["unresolved_prior_preserved"] == ["119-house-1-240"]


def test_recorded_references_relink_only_held_votes_in_scope_and_count_their_conflicts(tmp_path, scoped):
    """Two recorded references disagree on one held vote; the others stay byte-for-byte.

    The winner is the recorded reference at the lowest action (17), exactly
    what a fresh acquisition would publish, and ``conflict_count`` counts the
    disagreeing one. A held vote the input does not name, and one
    outside the scoped Congresses that the input does name, keep every field.
    """
    import hashlib as _hashlib

    from spicy_regs.source_evidence import CaptureEvidence

    relinked, unresolved, out_of_scope = _held_row(240), _held_row(241), _held_row(240, congress=118)
    base = {**_sample_rows()[0], "chamber": "house", "session": "1", "roll_number": "240"}
    _seed_references(tmp_path, [
        base | {"congress": "119", "bill_id": "119-hr-2", "action_index": "30", "url": "https://clerk.house.gov/b"},
        base | {"congress": "119", "bill_id": "119-hr-1", "action_index": "17", "url": "https://clerk.house.gov/a"},
        base | {"congress": "118", "bill_id": "118-hr-5", "action_index": "3", "url": "https://clerk.house.gov/c"},
    ])
    digest = "sha256:" + _hashlib.sha256(prior_scratch_path(tmp_path, VOTE_REFERENCES_TABLE).read_bytes()).hexdigest()
    evidence = CaptureEvidence(tmp_path / "audit", "roll-call-votes")
    pin = {"logicalId": "urn:test:bill-family", "artifactDigest": "sha256:" + "a" * 64}
    evidence.read_snapshot = {"families": {"bill-family": {
        **pin, "tables": {f"{VOTE_REFERENCES_TABLE}.parquet": {"sha256": digest}},
    }}}
    members = [
        {**{name: row[name] for name in ("vote_id", "congress", "chamber", "session", "roll_number")},
         "member_key": row["vote_id"], "position": "Yea"}
        for row in (relinked, unresolved, out_of_scope)
    ]
    paths = build_roll_call_votes(
        tmp_path,
        acquirer=StubVoteAcquirer(),
        download_prior=_published({"roll_call_votes": [relinked, unresolved, out_of_scope], "member_votes": members}),
        overlap=0,
        max_votes=0,
        evidence=evidence,
    )
    rows = {row["vote_id"]: row for row in pq.read_table(paths[0]).to_pylist()}
    link = {"bill_id": "119-hr-1", "match_rule": "bill_action_recorded_vote", "match_action_index": "17",
            "match_url": "https://clerk.house.gov/a", "conflict_count": "1"}
    assert rows == {
        "119-house-1-240": relinked | link,
        "119-house-1-241": unresolved,
        "118-house-1-240": out_of_scope,
    }
    positions = {(row["vote_id"], row["member_key"], row["position"]) for row in pq.read_table(paths[1]).to_pylist()}
    assert positions == {(row["vote_id"], row["member_key"], row["position"]) for row in members}
    [reference_input] = _events(evidence, "vote-reference-input")
    assert reference_input["sha256"] == digest
    assert reference_input["generation"] == {"family": "bill-family", **pin}
    [linkage] = _events(evidence, "held-vote-linkage")
    assert [(item["vote_id"], item["previous"]["match_action_index"], item["match_action_index"])
            for item in linkage["relinked"]] == [("119-house-1-240", "21", "17")]
    assert linkage["unresolved_prior_preserved"] == ["119-house-1-241"]
    assert linkage["held_in_scope"] == 2


def test_a_malformed_reference_row_costs_that_row_only(tmp_path, scoped):
    """One unreadable row must not lose the linkage for every other vote in the run."""
    rows = [row for row in _sample_rows() if row["roll_number"] == "240"]
    rows.append({**rows[0], "bill_id": "not-a-bill-key", "roll_number": "241", "action_index": "0"})
    _seed_references(tmp_path, rows)

    references = _recorded_vote_references(tmp_path, (119,), _no_prior)
    assert references is not None
    assert len(references) == len(rows) - 1
    assert all(reference.bill.number == 3424 for reference in references)


def test_the_published_table_still_matches_its_contract(tmp_path, scoped):
    _seed_references(tmp_path, [row for row in _sample_rows() if row["roll_number"] == "240"])
    paths = build_roll_call_votes(
        tmp_path,
        acquirer=StubVoteAcquirer(),
        overlap=0,
        download_prior=_no_prior,
    )
    assert pq.read_table(paths[0]).schema.names == list(TABLE_CONTRACTS["roll_call_votes"].columns)


def test_candidate_capture_merges_legacy_rows_and_is_held_on_resume(tmp_path, scoped):
    import shutil

    legacy_columns = [
        c
        for c in TABLE_CONTRACTS["roll_call_votes"].columns
        if c not in {"tally_kind", "documents_json", "amendments_json"}
    ]
    # A Senate row: it predates tally_kind and needs no legis_num to be held.
    legacy = {
        "vote_id": "119-senate-1-1",
        "congress": "119",
        "chamber": "senate",
        "session": "1",
        "roll_number": "1",
        "yea": "1",
        "vote_date": "January 3, 2025,  12:00 PM",
    }

    def legacy_prior(remote, local):
        if remote != "roll_call_votes.parquet":
            return False
        pq.write_table(
            pa.Table.from_pylist([legacy], schema=pa.schema([(c, pa.string()) for c in legacy_columns])), local
        )
        return True

    class CandidateAcquirer(StubVoteAcquirer):
        def acquire(self, locator, *, crosswalk=None):
            acquired = super().acquire(locator)
            acquired.vote.tally_kind = "candidates"
            acquired.vote.tallies = {"Literal candidate": 1, "Present": 0, "Not Voting": 0}
            acquired.vote.member_votes = (
                SimpleNamespace(
                    lis_id=None,
                    bioguide_id="A000001",
                    name="Member",
                    party="D",
                    state="CA",
                    vote="Literal candidate",
                    vote_normalized=None,
                ),
            )
            return acquired

    first = tmp_path / "first"
    first.mkdir()
    acquirer = CandidateAcquirer(house_rolls=(2,), senate_rolls=(1,))
    paths = build_roll_call_votes(first, acquirer=acquirer, overlap=0, download_prior=legacy_prior)
    rows = {row["roll_number"]: row for row in pq.read_table(paths[0]).to_pylist()}
    assert acquirer.requested == [("house", 2)]
    assert rows["1"]["yea"] == "1" and rows["1"]["tally_kind"] is None
    assert rows["1"]["documents_json"] is rows["1"]["amendments_json"] is None
    assert rows["2"]["tally_kind"] == "candidates" and rows["2"]["member_vote_count"] == "1"
    assert all(rows["2"][field] is None for field in ("yea", "nay", "present", "not_voting"))
    assert json.loads(rows["2"]["tallies_json"]) == {"Literal candidate": 1, "Present": 0, "Not Voting": 0}
    member = pq.read_table(paths[1]).to_pylist()[0]
    assert member["position"] == "Literal candidate" and member["position_normalized"] is None

    def captured_prior(remote, local):
        source = first / remote
        if not source.exists():
            return False
        shutil.copyfile(source, local)
        return True

    second = tmp_path / "second"
    second.mkdir()
    resumed = CandidateAcquirer(house_rolls=(2,), senate_rolls=(1,))
    again = build_roll_call_votes(second, acquirer=resumed, overlap=0, download_prior=captured_prior)
    assert resumed.requested == []
    assert pq.read_table(again[0]).to_pylist() == pq.read_table(paths[0]).to_pylist()
    assert pq.read_table(again[1]).to_pylist() == pq.read_table(paths[1]).to_pylist()


@pytest.mark.parametrize(
    "overrides",
    [
        {"tallies_json": None},
        {"tallies_json": "{}"},
        {"tallies_json": "bad json"},
        {"tallies_json": "[1]"},
        {"tallies_json": '{"Candidate":true}'},
        {"tallies_json": '{"Candidate":-1}'},
        {"tallies_json": '{"Candidate":2}'},
        {"tallies_json": '{"":1}'},
        {"member_vote_count": "0"},
        {"member_vote_count": None},
        {"source_url": None},
        {"tally_kind": None},
        {"tally_kind": "unknown"},
        {"yea": "1"},
    ],
)
def test_incomplete_or_malformed_candidate_row_remains_retryable(tmp_path, overrides):
    from spicy_regs.transforms.build_roll_call_votes import _held_votes

    row = {
        "congress": "119",
        "chamber": "house",
        "session": "1",
        "roll_number": "2",
        "tally_kind": "candidates",
        "tallies_json": '{"Candidate":1}',
        "member_vote_count": "1",
        "source_url": "https://clerk.house.gov/evs/2025/roll002.xml",
        **overrides,
    }
    path = tmp_path / "prior.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [row], schema=pa.schema([(c, pa.string()) for c in TABLE_CONTRACTS["roll_call_votes"].columns])
        ),
        path,
    )
    assert _held_votes(path) == set()


# --------------------------------------------------------------------------- #
# vote_day: the chamber's printed day, fresh from the file or backfilled.
# --------------------------------------------------------------------------- #
class RealBodyAcquirer(StubVoteAcquirer):
    """Serves the two real publisher bodies, parsed by spicy-docs, instead of a stub vote."""

    def acquire(self, locator, *, crosswalk=None):
        self.requested.append((locator.chamber, locator.roll_number))
        path, parse = REAL_BODIES[(locator.chamber, locator.roll_number)]
        return _Acquisition(parse(path.read_bytes(), locator))


def _real_run(output_dir, download_prior, *, acquirer=None, overlap=25):
    acquirer = acquirer or RealBodyAcquirer(senate_rolls=(1,), house_rolls=(240,))
    paths = build_roll_call_votes(
        output_dir,
        acquirer=acquirer,
        overlap=overlap,
        download_prior=download_prior,
    )
    return {row["vote_id"]: row for row in pq.read_table(paths[0]).to_pylist()}


def test_a_real_body_publishes_its_chambers_printed_day(tmp_path, scoped):
    rows = _real_run(tmp_path, _no_prior)
    assert {vote_id: (row["vote_date"], row["vote_day"]) for vote_id, row in rows.items()} == {
        "119-house-1-240": ("8-Sep-2025", "2025-09-08"),
        "119-senate-1-1": ("January 9, 2025,  02:54 PM", "2025-01-09"),
    }


def test_a_prior_published_before_vote_day_is_backfilled_without_a_refetch(tmp_path, scoped):
    """Held rows gain the day the shaper would have given them; linkage-only and unreadable rows stay NULL.

    The prior is the real bodies' own published rows with ``vote_day`` removed
    (as every row published before the column existed looks), plus three
    House roll calls the index no longer lists: one linkage-only (no
    tally, though its date is readable), one held with a Congress.gov UTC
    instant, and one held with no date at all.
    """
    first = tmp_path / "first"
    first.mkdir()
    published = _real_run(first, _no_prior)
    legacy_columns = [c for c in TABLE_CONTRACTS["roll_call_votes"].columns if c != "vote_day"]

    def unreached(roll, *, yea, vote_date):
        key = {"congress": "119", "chamber": "house", "session": "1", "roll_number": str(roll)}
        return {
            "vote_id": f"119-house-1-{roll}",
            **key,
            "yea": yea,
            "tally_kind": "positions" if yea else None,
            "vote_date": vote_date,
        }

    prior = [{c: row[c] for c in legacy_columns} for row in published.values()] + [
        unreached(7, yea=None, vote_date="8-Sep-2025"),
        unreached(8, yea="220", vote_date="2025-09-08T22:56:00Z"),
        unreached(9, yea="220", vote_date=None),
    ]

    def legacy_prior(remote, local):
        if remote != "roll_call_votes.parquet":
            return False
        schema = pa.schema([(c, pa.string()) for c in legacy_columns])
        pq.write_table(pa.Table.from_pylist(prior, schema=schema), local)
        return True

    second = tmp_path / "second"
    second.mkdir()
    acquirer = RealBodyAcquirer(senate_rolls=(1,), house_rolls=(240,))
    rows = _real_run(second, legacy_prior, acquirer=acquirer, overlap=0)
    assert acquirer.requested == [], "both real roll calls are held, so neither is fetched again"
    assert {vote_id: row["vote_day"] for vote_id, row in rows.items()} == {
        "119-house-1-240": "2025-09-08",
        "119-senate-1-1": "2025-01-09",
        "119-house-1-7": None,  # linkage-only: never filled, however readable its date
        "119-house-1-8": None,  # a UTC instant is not the chamber's printed day
        "119-house-1-9": None,  # the file printed no date
    }
    assert all(rows[vote_id] == row for vote_id, row in published.items()), "backfill equals a fresh shape"


def test_the_copied_publisher_bodies_match_the_digests_their_readme_records():
    readme = (FIXTURES / "README.md").read_text().splitlines()
    for path, _ in REAL_BODIES.values():
        [row] = [line for line in readme if line.startswith(f"| `{path.name}` |")]
        assert re.findall(r"`([0-9a-f]{64})`", row) == [hashlib.sha256(path.read_bytes()).hexdigest()], path.name


def test_a_prior_that_has_vote_day_fills_only_its_nulls_and_is_not_rewritten_when_complete(tmp_path):
    """Every run after the first: the column exists, so the fill replaces it in place."""
    from spicy_regs.transforms.build_roll_call_votes import _held_votes, _repair_held_votes

    columns = TABLE_CONTRACTS["roll_call_votes"].columns

    def row(roll, *, yea, vote_date, day):
        key = {"congress": "119", "chamber": "house", "session": "1", "roll_number": str(roll)}
        return {
            "vote_id": f"119-house-1-{roll}",
            **key,
            "yea": yea,
            "tally_kind": "positions" if yea else None,
            "vote_date": vote_date,
            "vote_day": day,
            "legis_num": "QUORUM" if yea else None,
        }

    prior = tmp_path / "prior.parquet"
    rows = [
        row(1, yea="220", vote_date="8-Sep-2025", day=None),  # held with NULL: filled
        row(2, yea="220", vote_date="9-Sep-2025", day="2025-09-01"),  # a stated day is never rewritten
        row(3, yea=None, vote_date="10-Sep-2025", day=None),  # linkage-only: left NULL
    ]
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in columns])), prior)

    def repair():
        # No recorded-vote input: only vote_day can change.
        return _repair_held_votes(prior, _held_votes(prior), {}, None, (119,))

    repair()
    table = pq.read_table(prior)
    assert table.column_names == list(columns), "vote_day keeps its contract position"
    assert {r["roll_number"]: r["vote_day"] for r in table.to_pylist()} == {
        "1": "2025-09-08",
        "2": "2025-09-01",
        "3": None,
    }

    before = (prior.read_bytes(), prior.stat().st_ino, prior.stat().st_mtime_ns)
    repair()
    assert (prior.read_bytes(), prior.stat().st_ino, prior.stat().st_mtime_ns) == before, "nothing to fill, no rewrite"


def test_the_shared_workflow_withholds_the_api_key_from_this_keyless_rollup():
    """Every test above runs with the keys stripped (``conftest.isolate_env``); the job is not handed one either."""
    import yaml

    workflow = yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/_rollup.yml").read_text())
    [step] = [step for step in workflow["jobs"]["rollup"]["steps"] if step.get("name") == "Run rollup"]
    assert step["env"]["DATA_GOV_API_KEY"] == (
        "${{ inputs.command != 'run-rollup-roll-call-votes' && secrets.DATA_GOV_API_KEY || '' }}"
    )


# --------------------------------------------------------------------------- #
# The vote file's own statement of its measure.
# --------------------------------------------------------------------------- #
FILE_RULE = "vote_file_legislation"


def test_the_vote_file_links_what_no_bill_action_records(tmp_path, scoped):
    """The Clerk's legis-num and the Senate's document link each real file, under their own rule and URL."""
    rows = _real_run(tmp_path, _no_prior)
    assert {vote_id: (row["bill_id"], row["match_rule"], row["match_action_index"]) for vote_id, row in rows.items()} == {
        "119-house-1-240": ("119-hr-3424", FILE_RULE, None),
        "119-senate-1-1": ("119-s-5", FILE_RULE, None),
    }
    assert all(row["match_url"] == row["source_url"] and row["conflict_count"] == "0" for row in rows.values())
    assert rows["119-house-1-240"]["legis_num"] == "H R 3424" and rows["119-senate-1-1"]["legis_num"] is None


def test_the_file_chooses_between_two_bills_actions_that_record_one_vote(tmp_path, scoped):
    """A lower action index in another bill's list is no evidence; the bill the file names wins, and the other counts."""
    base = {**_sample_rows()[0], "congress": "119", "chamber": "senate", "session": "1", "roll_number": "1"}
    _seed_references(tmp_path, [
        base | {"bill_id": "119-s-99", "action_index": "0", "url": "https://www.senate.gov/a"},
        base | {"bill_id": "119-s-5", "action_index": "30", "url": "https://www.senate.gov/b"},
    ])
    rows = _real_run(tmp_path, _no_prior)
    senate = rows["119-senate-1-1"]
    assert (senate["bill_id"], senate["match_rule"], senate["match_action_index"], senate["match_url"]) == (
        "119-s-5", "bill_action_recorded_vote", "30", "https://www.senate.gov/b",
    )
    assert senate["conflict_count"] == "1"


def test_a_bills_own_action_wins_over_a_disagreeing_file_and_the_file_is_counted(tmp_path, scoped):
    base = next(row for row in _sample_rows() if row["chamber"] == "house" and row["roll_number"] == "240")
    _seed_references(tmp_path, [base | {"bill_id": "119-hr-1", "action_index": "4"}])
    house = _real_run(tmp_path, _no_prior)["119-house-1-240"]
    assert (house["bill_id"], house["match_rule"], house["conflict_count"]) == ("119-hr-1", "bill_action_recorded_vote", "1")


def test_held_rows_relink_from_their_own_columns_without_a_fetch(tmp_path, scoped):
    """No file is fetched: the House row's legis_num and the Senate row's documents link them; a recorded link stays."""
    stated_house = _held_row(7) | {"legis_num": "H R 7", **dict.fromkeys(("bill_id", "match_action_index", "match_url"))}
    stated_house |= {"match_rule": "unmatched", "conflict_count": "0"}
    senate = {**_held_row(9), "vote_id": "119-senate-1-9", "chamber": "senate", "legis_num": None,
              "documents_json": json.dumps([{"congress": 119, "type": "S.Res.", "number": "30"}]),
              "amendments_json": "[]", "bill_id": None, "match_rule": "unmatched", "match_action_index": None,
              "match_url": None, "conflict_count": "0"}
    recorded_link = _held_row(8) | {"legis_num": "H R 9999"}  # its action's link stands; the family no longer names it
    acquirer = StubVoteAcquirer()
    paths = build_roll_call_votes(
        tmp_path, acquirer=acquirer, overlap=0, max_votes=0,
        download_prior=_published({"roll_call_votes": [stated_house, senate, recorded_link], "member_votes": []}),
    )
    rows = {row["vote_id"]: row for row in pq.read_table(paths[0]).to_pylist()}
    assert acquirer.requested == []
    assert (rows["119-house-1-7"]["bill_id"], rows["119-house-1-7"]["match_rule"]) == ("119-hr-7", FILE_RULE)
    assert rows["119-house-1-7"]["match_url"] == stated_house["source_url"]
    assert (rows["119-senate-1-9"]["bill_id"], rows["119-senate-1-9"]["match_rule"]) == ("119-sres-30", FILE_RULE)
    assert rows["119-house-1-8"] == recorded_link


def test_a_house_row_published_before_legis_num_is_read_once_more_then_held(tmp_path, scoped):
    """Its own statement is in no column, so its file is read again once; the re-read row is held from then on."""
    import shutil

    legacy = {**_held_row(240), "legis_num": None}
    first = tmp_path / "first"
    first.mkdir()
    acquirer = RealBodyAcquirer(house_rolls=(240,))
    rows = _real_run(first, _published({"roll_call_votes": [legacy], "member_votes": []}), acquirer=acquirer, overlap=0)
    assert acquirer.requested == [("house", 240)]
    assert rows["119-house-1-240"]["legis_num"] == "H R 3424"

    def captured(remote, local):
        source = first / remote
        if not source.exists():
            return False
        shutil.copyfile(source, local)
        return True

    second = tmp_path / "second"
    second.mkdir()
    resumed = RealBodyAcquirer(house_rolls=(240,))
    assert _real_run(second, captured, acquirer=resumed, overlap=0) == rows
    assert resumed.requested == []


def test_a_vote_the_senate_menu_withholds_is_journaled_and_never_read(tmp_path, scoped):
    """116-2-216's file is served as 0-0 with every senator "Not Voting"; the menu says that is not its record."""
    from spicy_regs.source_evidence import CaptureEvidence

    evidence = CaptureEvidence(tmp_path / "audit", "roll-call-votes")
    acquirer = StubVoteAcquirer(senate_rolls=(1, 2), withheld_senate_rolls=(3,))
    paths = build_roll_call_votes(
        tmp_path, acquirer=acquirer, download_prior=_no_prior, evidence=evidence, open_congresses=(119,)
    )
    assert sorted(acquirer.requested) == [("senate", 1), ("senate", 2)]
    assert sorted(row["roll_number"] for row in pq.read_table(paths[0]).to_pylist()) == ["1", "2"]
    [event] = _events(evidence, "vote-withheld")
    assert (event["chamber"], event["roll_number"]) == ("senate", 3)
    assert event["statement"] == "Vote data is unavailable due to secret session."
