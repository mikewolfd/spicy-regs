"""Hermetic tests for the House name crosswalk: the rule over real crosswalk facts, then the fill over a small table.

The members, terms and affiliations below are the published crosswalk's own rows for the people named (measured
2026-10-04), so each case is a label the Clerk printed and the member it did or did not name. The nicknames are the
source project's own ``name.nickname`` values, which ``members`` does not publish yet.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS

from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.member_name_crosswalk import (
    RULE_VERSION,
    SOURCES,
    fill_house_name_bioguide_ids,
    resolve_name_keys,
)
from spicy_regs.transforms.table_merge import prior_scratch_path
from tests.test_roll_call_votes import _events

#: bioguide id: (first, last, other_names_json, [(term_index, start, end, state, party)]).
CROSSWALK = {
    "A000014": ("Neil", "Abercrombie", None, [("1", "1991-01-03", "1993-01-03", "HI", "Democrat")]),
    "J000256": ("Walter", "Jones", None, [("12", "1989-01-03", "1991-01-03", "NC", "Democrat"),
                                          ("13", "1991-01-03", "1992-09-15", "NC", "Democrat")]),
    "J000255": ("Walter", "Jones", None, [("0", "1995-01-04", "1997-01-03", "NC", "Republican")]),
    "L000243": ("Norman", "Lent", None, [("10", "1991-01-03", "1993-01-03", "NY", "Republican-Conservative")]),
    "S000124": ("James", "Scheuer", None, [("12", "1991-01-03", "1993-01-03", "NY", "Democrat-Liberal")]),
    "T000058": ("William", "Tauzin", None, [("8", "1995-01-04", "1997-01-03", "LA", "Republican")]),
    "C000345": ("Helen", "Chenoweth-Hage", None, [("0", "1995-01-04", "1997-01-03", "ID", "Republican")]),
    "L000035": ("Blanche", "Lincoln", '[{"middle":null,"last":"Lambert","end":"1995-01-03"}]',
                [("0", "1993-01-05", "1995-01-03", "AR", "Democrat"), ("1", "1995-01-04", "1997-01-03", "AR", "Democrat")]),
    "D000136": ("Thomas", "Davis", None, [("3", "2001-01-03", "2003-01-03", "VA", "Republican")]),
    "D000597": ("Jo Ann", "Davis", None, [("0", "2001-01-03", "2003-01-03", "VA", "Republican")]),
    "D000299": ("Lincoln", "Diaz-Balart", None, [("5", "2003-01-07", "2005-01-03", "FL", "Republican")]),
    "D000600": ("Mario", "Diaz-Balart", None, [("0", "2003-01-07", "2005-01-03", "FL", "Republican")]),
    "C000134": ("Walter", "Capps", None, [("0", "1997-01-07", "1997-10-28", "CA", "Democrat")]),
    "C001036": ("Lois", "Capps", None, [("0", "1998-03-10", "1999-01-03", "CA", "Democrat")]),
    "B000622": ("Sonny", "Bono", None, [("1", "1997-01-07", "1998-01-05", "CA", "Republican")]),
    "B001228": ("Mary", "Bono Mack", '[{"last":"Bono","end":"2007-12-17"}]',
                [("0", "1998-04-07", "1999-01-03", "CA", "Republican")]),
    "H000323": ("J.", "Hastert", None, [("6", "1999-01-06", "2001-01-03", "IL", "Republican")]),
    "J000032": ("Sheila", "Jackson Lee", None, [("0", "1995-01-04", "1997-01-03", "TX", "Democrat")]),
    "V000081": ("Nydia", "Velázquez", None, [("0", "1993-01-05", "1995-01-03", "NY", "Democrat")]),
    "N000147": ("Eleanor", "Norton", None, [("1", "1993-01-05", "1995-01-03", "DC", "Democrat")]),
    "S000527": ("Dennis", "Smith", None, [("4", "1989-01-03", "1991-01-03", "OR", "Republican")]),
    "S000607": ("Robert", "Smith", None, [("3", "1989-01-03", "1991-01-03", "OR", "Republican")]),
    "M000720": ("Dan", "Miller", None, [("4", "2001-01-03", "2003-01-03", "FL", "Republican")]),
    "M001144": ("Jeff", "Miller", None, [("0", "2001-10-23", "2003-01-03", "FL", "Republican")]),
    # Not real members: a state's own Norton and Lee, to show a delegate's row and a whole surname keep to their own,
    # three Roes of one state, party and Congress, and a member whose one term states no end.
    "X000001": ("Pat", "Norton", None, [("0", "1993-01-05", "1995-01-03", "NY", "Democrat")]),
    "X000002": ("Pat", "Lee", None, [("0", "1995-01-04", "1997-01-03", "TX", "Democrat")]),
    "X000003": ("Ann", "Roe", None, [("0", "2001-01-03", "2003-01-03", "OH", "Republican")]),
    "X000004": ("Ann", "Roe", None, [("0", "2001-01-03", "2003-01-03", "OH", "Republican")]),
    "X000005": ("Ann Marie", "Roe", None, [("0", "2001-01-03", "2003-01-03", "OH", "Republican")]),
    "X000006": ("Pat", "Open", None, [("0", "1991-01-03", None, "HI", "Democrat")]),
    # Two more: one whose two terms of one Congress meet on a day, and one whose dated other name is her own surname.
    "X000007": ("Pat", "Twice", None, [("0", "1991-01-03", "1992-06-01", "HI", "Democrat"),
                                       ("1", "1992-06-01", "1993-01-03", "HI", "Democrat")]),
    "X000008": ("Pat", "Same", '[{"last":"Same","end":"1995-01-03"}]', [("0", "1993-01-05", "1995-01-03", "HI", "Democrat")]),
    # And two Poes: one whose first name is Tom (and who was Quill until 2002), one whose nickname is.
    "X000009": ("Tom", "Poe", '[{"last":"Quill","end":"2002-06-01"}]', [("0", "2001-01-03", "2003-01-03", "WA", "Democrat")]),
    "X000010": ("Thomas", "Poe", None, [("0", "2001-01-03", "2003-01-03", "WA", "Democrat")]),
}
#: ``name.nickname`` as the source project states it for the real members above; X000010's is made up with him.
NICKNAMES = {"D000136": "Tom", "S000527": "Denny", "T000058": "Billy", "X000010": "Tom"}
#: Lincoln's Senate term (the crosswalk's own row): a senator is no candidate for a House label.
SENATE_TERMS = [("L000035", "2", "1999-01-06", "2005-01-03", "AR", "Democrat")]
#: The crosswalk's dated affiliations for Tauzin's 104th term, which states one party (Republican) for the term.
AFFILIATIONS = [("T000058", "8", "Democrat"), ("T000058", "8", "Republican")]


def _members(only=None, nicknames=True):
    """The crosswalk's member rows; ``nicknames`` false is the table as published today, without the column."""
    return [{"bioguide_id": b, "name_first": first, "name_last": last, "other_names_json": other}
            | ({"name_nickname": NICKNAMES.get(b)} if nicknames else {})
            for b, (first, last, other, _) in CROSSWALK.items() if only is None or b in only]


def _terms(only=None):
    house = [(b, "rep", *term) for b, (_, _, _, terms) in CROSSWALK.items() for term in terms]
    senate = [(b, "sen", *term) for b, *term in SENATE_TERMS]
    return [{"bioguide_id": b, "term_index": index, "term_type": kind, "term_start": start, "term_end": end,
             "term_state": state, "term_party": party}
            for b, kind, index, start, end, state, party in house + senate if only is None or b in only]


def _affiliations():
    return [{"bioguide_id": b, "term_index": index, "party": party} for b, index, party in AFFILIATIONS]


def _resolve(*key, only=None, affiliations=True, nicknames=True):
    [resolution] = resolve_name_keys(
        [key], _members(only, nicknames), _terms(only), _affiliations() if affiliations else ()
    ).values()
    return resolution


@pytest.mark.parametrize(
    ("key", "bioguide", "rule"),
    [
        (("102", "Abercrombie", "D", "HI"), "A000014", "surname"),
        # One label, two men: the father sat as a Democrat through 1992, the son as a Republican from 1995.
        (("102", "Jones (NC)", "D", "NC"), "J000256", "surname"),
        (("104", "Jones (NC)", "R", "NC"), "J000255", "surname"),
        # The Clerk prints no accent and its own hyphen, and sometimes a second space.
        (("104", "Jackson-Lee (TX)", "D", "TX"), "J000032", "surname"),
        (("103", "Velazquez", "D", "NY"), "V000081", "surname"),
        (("102", "Abercrombie  (HI)", "D", "HI"), "A000014", "surname"),
        # A dated other_names surname, while it was hers; a surname that is also her own is not a former one.
        (("103", "Lambert", "D", "AR"), "L000035", "former_surname"),
        (("104", "Lincoln", "D", "AR"), "L000035", "surname"),
        (("103", "Same", "D", "HI"), "X000008", "surname"),
        # The crosswalk's two New York fusion labels are the Clerk's R and D.
        (("102", "Lent", "R", "NY"), "L000243", "surname"),
        (("102", "Scheuer", "D", "NY"), "S000124", "surname"),
        # A member who changed party mid-term matches under both letters, through the term's dated affiliations.
        (("104", "Tauzin", "D", "LA"), "T000058", "surname"),
        (("104", "Tauzin", "R", "LA"), "T000058", "surname"),
        # One part of a compound surname, only because no whole surname matches.
        (("104", "Chenoweth", "R", "ID"), "C000345", "surname_part"),
        # Several members of one surname, state and party: the printed first name chooses.
        (("107", "Davis, Jo Ann", "R", "VA"), "D000597", "surname+first_name"),
        (("107", "Davis, Thomas M.", "R", "VA"), "D000136", "surname+first_word"),
        (("108", "Diaz-Balart, L.", "R", "FL"), "D000299", "surname+first_initial"),
        (("108", "Diaz-Balart, M.", "R", "FL"), "D000600", "surname+first_initial"),
        # A printed first name that is the member's stated nickname chooses too, after the rules that read name_first.
        (("107", "Davis, Tom", "R", "VA"), "D000136", "surname+nickname"),
        (("101", "Smith, Denny (OR)", "R", "OR"), "S000527", "surname+nickname"),
        (("107", "Poe, Tom", "D", "WA"), "X000009", "surname+first_name"),
        (("107", "Poe, Thomas", "D", "WA"), "X000010", "surname+first_name"),
        # A delegate's row states XX; the label's qualifier, or any delegate jurisdiction, is the state.
        (("103", "Norton (DC)", "D", "XX"), "N000147", "surname"),
        (("103", "Norton", "D", "XX"), "N000147", "surname"),
        (("103", "Norton", "D", "NY"), "X000001", "surname"),
        # The strongest first-name rule any member meets decides: two more Roes are Ann, but only in their first word.
        (("107", "Roe, Ann Marie", "R", "OH"), "X000005", "surname+first_name"),
    ],
)
def test_a_label_one_member_answers_to_resolves_with_its_rule(key, bioguide, rule):
    resolution = _resolve(*key)
    assert {member: found["rule"] for member, found in resolution["members"].items()} == {bioguide: rule}
    assert (resolution["reason"], resolution["candidates"]) == (None, [bioguide])
    assert resolution["first_named"] is ("," in key[1])


@pytest.mark.parametrize(
    ("key", "members"),
    [
        # Both served in the 105th, one after the other: the vote's day will say which.
        (("105", "Capps", "D", "CA"), {"C000134": "surname", "C001036": "surname"}),
        # Each member's rule names the surname that matched: his own, and the one she carried until 2007.
        (("105", "Bono", "R", "CA"), {"B000622": "surname", "B001228": "former_surname"}),
        # A bare surname cannot choose between two, with or without its state.
        (("107", "Davis (VA)", "R", "VA"), {"D000136": "surname", "D000597": "surname"}),
        (("101", "Smith (OR)", "R", "OR"), {"S000527": "surname", "S000607": "surname"}),
        # Two Roes are Ann: the whole name chooses neither, the weaker rule that would pick the third is not tried,
        # and a first name that chooses no one narrows nothing.
        (("107", "Roe, Ann", "R", "OH"), dict.fromkeys(("X000003", "X000004", "X000005"), "surname")),
        (("107", "Roe, A.", "R", "OH"), dict.fromkeys(("X000003", "X000004", "X000005"), "surname")),
    ],
)
def test_a_label_several_members_answer_to_keeps_them_all_for_the_votes_day(key, members):
    resolution = _resolve(*key)
    assert {member: found["rule"] for member, found in resolution["members"].items()} == members
    assert (resolution["reason"], resolution["candidates"]) == (None, sorted(members))
    assert all(found["terms"] for found in resolution["members"].values())
    assert resolution["first_named"] is ("," in key[1])


def test_without_the_nickname_column_a_nickname_chooses_no_one():
    """``members`` as published today states no nickname: the label keeps both Davises, as it did before the step."""
    for key in (("107", "Davis, Tom", "R", "VA"), ("101", "Smith, Denny (OR)", "R", "OR")):
        resolution = _resolve(*key, nicknames=False)
        assert len(resolution["members"]) == 2 and {found["rule"] for found in resolution["members"].values()} == {"surname"}
    # A nickname the column leaves NULL, or one that is not the printed name, chooses no one either.
    assert len(_resolve("107", "Davis, Tommy", "R", "VA")["members"]) == 2
    assert len(_resolve("107", "Roe, Tom", "R", "OH")["members"]) == 3


@pytest.mark.parametrize(
    ("key", "reason", "candidates"),
    [
        # The Clerk printed the Speaker D on one roll call of 1999: the one Hastert is not that party.
        (("106", "Hastert", "D", "IL"), "party_differs", ["H000323"]),
        (("104", "Jones (NC)", "D", "NC"), "party_differs", ["J000255"]),
        (("106", "Hastert", "L", "IL"), "party_differs", ["H000323"]),
        # No one of that surname in that state in that Congress.
        (("102", "Abercrombie", "D", "CA"), "no_member", []),
        (("104", "Lambert", "D", "AR"), "no_member", []),
        # A term that ended the day a Congress began does not seat its member in it.
        (("103", "Abercrombie", "D", "HI"), "no_member", []),
        (("103", "Jones (NC)", "D", "NC"), "no_member", []),
        # A whole surname in that state keeps a part from being tried, whatever its party.
        (("104", "Lee", "R", "TX"), "party_differs", ["X000002"]),
        (("103", "Norton (GU)", "D", "XX"), "no_member", []),
        # A senator of that surname, state and party, and a term that states no end, seat no one in the House.
        (("106", "Lincoln", "D", "AR"), "no_member", []),
        (("102", "Open", "D", "HI"), "no_member", []),
        (("102", "Jones (NC)", "D", "SC"), "state_conflict", []),
        (("102", "Jones (N.C.)", "D", "NC"), "unread_label", []),
        (("102", "Jones (NC) (D)", "D", "NC"), "unread_label", []),
    ],
)
def test_a_label_no_member_answers_to_stays_unresolved_with_its_reason(key, reason, candidates):
    assert _resolve(*key) == {"reason": reason, "candidates": candidates, "members": {}, "first_named": False}


def test_a_whole_surname_wins_over_a_part_and_a_party_change_needs_its_affiliation():
    assert RULE_VERSION == "house-name-crosswalk-v4", "a rule change moves the version on purpose, and says why"
    assert list(_resolve("104", "Lee", "D", "TX")["members"]) == ["X000002"], "Lee is not Jackson Lee while a Lee sits"
    assert _resolve("104", "Lee", "D", "TX", only={"J000032"})["members"]["J000032"]["rule"] == "surname_part"
    # With one Davis seated the first name decides nothing, so the rule does not claim it did.
    assert _resolve("107", "Davis, Jo Ann", "R", "VA", only={"D000597"})["members"]["D000597"]["rule"] == "surname"
    # Without the dated affiliation the term states Republican alone, and the D rows of 1995 match no one.
    assert _resolve("104", "Tauzin", "D", "LA", affiliations=False)["reason"] == "party_differs"


def test_a_resolution_names_the_terms_it_matched_and_every_key_is_answered():
    keys = [("102", "Jones (NC)", "D", "NC"), ("101", "Jones (NC)", "D", "NC"), ("102", "Nobody", "D", "NC")]
    resolved = resolve_name_keys(keys, _members(), _terms(), _affiliations())
    assert list(resolved) == keys
    assert resolved[keys[0]]["members"]["J000256"]["terms"] == [
        {"term_index": "13", "term_start": "1991-01-03", "term_end": "1992-09-15"}]
    assert resolved[keys[1]]["members"]["J000256"]["terms"] == [
        {"term_index": "12", "term_start": "1989-01-03", "term_end": "1991-01-03"}]


# --------------------------------------------------------------------------- #
# The fill: one decision per key, joined to its rows on the vote's day.
# --------------------------------------------------------------------------- #
VOTES = TABLE_CONTRACTS["member_votes"].columns


def _row(vote_id, name, party, state, date, *, key=None, chamber="house", bioguide=None):
    congress, _, session, roll = vote_id.split("-")
    return dict.fromkeys(VOTES) | {
        "vote_id": vote_id, "congress": congress, "chamber": chamber, "session": session, "roll_number": roll,
        "member_key": key or f"name:{name}", "bioguide_id": bioguide, "member_name": name, "party": party,
        "state": state, "position": "Yea", "position_normalized": "yea", "vote_date": date,
    }


def _write(path: Path, rows: list[dict], columns) -> None:
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in columns])), path)


class _Published:
    """A ``download_prior`` serving the three crosswalk tables: each one asked for, and the bytes it was given."""

    def __init__(self, only=None, *, without=(), nicknames=True):
        published = tuple(column for column in TABLE_CONTRACTS["members"].columns if column != "name_nickname")
        self.tables = {
            "members": (_members(only, nicknames), (*published, "name_nickname") if nicknames else published),
            "member_terms": (_terms(only), TABLE_CONTRACTS["member_terms"].columns),
            "member_party_affiliations": (_affiliations(), TABLE_CONTRACTS["member_party_affiliations"].columns),
        }
        self.without = without
        self.asked: list[str] = []
        self.sha256: dict[str, str] = {}

    def __call__(self, remote: str, local: Path) -> bool:
        self.asked.append(remote)
        name = remote.removesuffix(".parquet")
        if name not in self.tables or name in self.without:
            return False
        rows, columns = self.tables[name]
        _write(local, [dict.fromkeys(columns) | row for row in rows], columns)
        self.sha256[name] = "sha256:" + hashlib.sha256(local.read_bytes()).hexdigest()
        return True


def _evidence(tmp_path) -> CaptureEvidence:
    return CaptureEvidence(tmp_path / "audit", "roll-call-votes")


def _event(evidence) -> dict:
    [event] = _events(evidence, "member-name-crosswalk")
    return event


def _fill(tmp_path, rows, download=None, evidence=None):
    out = tmp_path / "member_votes.parquet"
    if rows is not None:
        _write(out, rows, VOTES)
    count = fill_house_name_bioguide_ids(tmp_path, out, download or _Published(), evidence=evidence)
    return count, {(r["vote_id"], r["member_key"]): r["bioguide_id"] for r in pq.read_table(out).to_pylist()}


ROWS = [
    _row("102-house-1-1", "Abercrombie", "D", "HI", "3-JAN-1991"),
    _row("102-house-1-1", "Jones (NC)", "D", "NC", "3-JAN-1991"),
    # Jones died 15 September 1992: his term's last day is his, a row dated after it is not (and the id it held
    # goes, since the fill is derived, not coalesced), and neither is a row whose printed date spicy-docs refuses.
    _row("102-house-2-399", "Jones (NC)", "D", "NC", "15-Sep-1992"),
    _row("102-house-2-400", "Jones (NC)", "D", "NC", "16-Sep-1992", bioguide="J000256"),
    _row("102-house-2-400", "Abercrombie", "D", "HI", "16-Sep-1992"),
    _row("102-house-2-401", "Jones (NC)", "D", "NC", "31-Feb-1992"),
    # Two members answer to Capps in the 105th. Walter died 28 October 1997 and Lois took the seat 10 March 1998: a
    # row is his on his day and hers on hers, and no one's on a day between them.
    _row("105-house-1-9", "Capps", "D", "CA", "5-Feb-1997"),
    _row("105-house-1-10", "Capps", "D", "CA", "5-Feb-1997", bioguide="C000134"),
    _row("105-house-1-640", "Capps", "D", "CA", "13-Nov-1997"),
    _row("105-house-2-50", "Capps", "D", "CA", "17-Mar-1998"),
    # Both Davises of Virginia sat on this day, and one of them is stated to go by Tom.
    _row("107-house-1-5", "Davis, Tom", "R", "VA", "7-Feb-2001"),
    # Three Roes sat too, and nothing printed says which Ann this is.
    _row("107-house-1-5", "Roe, Ann", "R", "OH", "7-Feb-2001"),
    # What the fill must leave alone: a stated id, and the Senate's own name: row.
    _row("108-house-1-5", "Abercrombie", "D", "HI", "8-Jan-2003", key="A000014", bioguide="A000014"),
    _row("102-senate-1-1", "Abercrombie", "D", "HI", "January 3, 1991,  02:00 PM", chamber="senate"),
]


def test_the_fill_sets_each_house_name_row_from_its_key_and_its_day_and_journals_the_resolutions(tmp_path):
    evidence, download = _evidence(tmp_path), _Published()
    count, ids = _fill(tmp_path, ROWS, download, evidence)
    assert ids == {
        ("102-house-1-1", "name:Abercrombie"): "A000014",
        ("102-house-1-1", "name:Jones (NC)"): "J000256",
        ("102-house-2-399", "name:Jones (NC)"): "J000256",
        ("102-house-2-400", "name:Jones (NC)"): None,
        ("102-house-2-400", "name:Abercrombie"): "A000014",
        ("102-house-2-401", "name:Jones (NC)"): None,
        ("105-house-1-9", "name:Capps"): "C000134",
        ("105-house-1-10", "name:Capps"): "C000134",
        ("105-house-1-640", "name:Capps"): None,
        ("105-house-2-50", "name:Capps"): "C001036",
        ("107-house-1-5", "name:Davis, Tom"): "D000136",
        ("107-house-1-5", "name:Roe, Ann"): None,
        ("108-house-1-5", "A000014"): "A000014",
        ("102-senate-1-1", "name:Abercrombie"): None,
    }
    assert count == 8
    # The rewrite keeps the table's merge order: newest printed date first, then the row's identity.
    order = [(r["vote_date"], r["vote_id"], r["member_key"]) for r in pq.read_table(tmp_path / "member_votes.parquet").to_pylist()]
    assert order == sorted(order, key=lambda row: ([-ord(ch) for ch in row[0]], row[1].split("-"), row[2]))
    assert sorted(download.asked) == sorted(f"{name}.parquet" for name in SOURCES)
    assert [prior_scratch_path(tmp_path, name).exists() for name in SOURCES] == [False] * 3, "no scratch input is left"

    event = _event(evidence)
    assert (event["rule_version"], event["input_available"], event["nicknames"]) == (RULE_VERSION, True, True)
    assert (event["keys"], event["rows"], event["rows_resolved"], event["rows_changed"], event["unchanged"]) == (5, 12, 8, 8, 0)
    # Each input is journaled by the digest of the bytes read; no read snapshot names a generation for them here.
    assert event["inputs"] == {name: {"sha256": download.sha256[name], "generation": None} for name in SOURCES}
    assert event["resolved"] == [
        {"congress": "102", "member_name": "Abercrombie", "party": "D", "state": "HI", "bioguide_id": "A000014",
         "rule": "surname", "rows": 2, "rows_changed": 2,
         "terms": [{"term_index": "1", "term_start": "1991-01-03", "term_end": "1993-01-03"}]},
        {"congress": "102", "member_name": "Jones (NC)", "party": "D", "state": "NC", "bioguide_id": "J000256",
         "rule": "surname", "rows": 2, "rows_changed": 2,
         "terms": [{"term_index": "13", "term_start": "1991-01-03", "term_end": "1992-09-15"}]},
        # One key, two members, each with the rows of their own days; the row that already held his id did not change.
        {"congress": "105", "member_name": "Capps", "party": "D", "state": "CA", "bioguide_id": "C000134",
         "rule": "surname+seating_day", "rows": 2, "rows_changed": 1,
         "terms": [{"term_index": "0", "term_start": "1997-01-07", "term_end": "1997-10-28"}]},
        {"congress": "105", "member_name": "Capps", "party": "D", "state": "CA", "bioguide_id": "C001036",
         "rule": "surname+seating_day", "rows": 1, "rows_changed": 1,
         "terms": [{"term_index": "0", "term_start": "1998-03-10", "term_end": "1999-01-03"}]},
        {"congress": "107", "member_name": "Davis, Tom", "party": "R", "state": "VA", "bioguide_id": "D000136",
         "rule": "surname+nickname", "rows": 1, "rows_changed": 1,
         "terms": [{"term_index": "3", "term_start": "2001-01-03", "term_end": "2003-01-03"}]},
    ]
    assert event["unresolved"] == [
        {"congress": "102", "member_name": "Jones (NC)", "party": "D", "state": "NC", "reason": "outside_term",
         "candidates": ["J000256"], "rows": 2},
        {"congress": "105", "member_name": "Capps", "party": "D", "state": "CA", "reason": "outside_term",
         "candidates": ["C000134", "C001036"], "rows": 1},
        {"congress": "107", "member_name": "Roe, Ann", "party": "R", "state": "OH", "reason": "several_seated",
         "candidates": ["X000003", "X000004", "X000005"], "rows": 1},
    ]


def test_a_second_fill_changes_nothing_and_does_not_rewrite_the_table(tmp_path):
    _fill(tmp_path, ROWS)
    out = tmp_path / "member_votes.parquet"
    before = (out.stat().st_ino, out.stat().st_mtime_ns, out.read_bytes())
    evidence, download = _evidence(tmp_path), _Published()
    # The run's read snapshot names the generation of an input only when the bytes read are the ones it published.
    download("members.parquet", tmp_path / "published.parquet")
    generation = {"logicalId": "urn:members", "artifactDigest": "sha256:" + "a" * 64}
    evidence.read_snapshot = {"families": {"members": generation | {"tables": {
        "members.parquet": {"sha256": download.sha256["members"]}, "member_terms.parquet": {"sha256": "sha256:" + "0" * 64},
    }}}}
    count, _ = _fill(tmp_path, None, download, evidence)
    assert count == 8 and (out.stat().st_ino, out.stat().st_mtime_ns, out.read_bytes()) == before
    event = _event(evidence)
    assert (event["rows_changed"], event["resolved"], event["unchanged"]) == (0, [], 5)
    assert event["inputs"]["members"]["generation"] == {"family": "members"} | generation
    assert event["inputs"]["member_terms"] == {"sha256": download.sha256["member_terms"], "generation": None}
    assert [entry["reason"] for entry in event["unresolved"]] == ["outside_term", "outside_term", "several_seated"]


def test_a_plain_label_seating_several_is_the_member_its_roll_calls_first_named_labels_leave_over(tmp_path):
    """The Clerk prints a first name only to tell one of several apart, and lists a member once. The day is asked
    first; the roll call's own first-named labels only where the day seats several; and a label that itself prints
    a first name is never given what is left."""
    rows = [
        # The 101st's two Smiths of Oregon, both seated all of 1990: Denny by his nickname, the plain label the other.
        _row("101-house-2-10", "Smith, Denny (OR)", "R", "OR", "24-Jan-1990"),
        _row("101-house-2-10", "Smith (OR)", "R", "OR", "24-Jan-1990"),
        # On a roll call that lists only the plain label there is no one to be told apart from.
        _row("101-house-2-11", "Smith (OR)", "R", "OR", "25-Jan-1990"),
        # Miller (FL) before Jeff Miller was seated: the day alone decides, whatever other roll calls print.
        _row("107-house-1-5", "Miller (FL)", "R", "FL", "7-Feb-2001"),
        # After, beside Jeff's first name alone, it is the Miller left over.
        _row("107-house-1-400", "Miller (FL)", "R", "FL", "1-Nov-2001"),
        _row("107-house-1-400", "Miller, Jeff", "R", "FL", "1-Nov-2001"),
        # Beside both first names no one is left for it, and beside one Roe of three, two are.
        _row("107-house-1-401", "Miller (FL)", "R", "FL", "2-Nov-2001"),
        _row("107-house-1-401", "Miller, Dan", "R", "FL", "2-Nov-2001"),
        _row("107-house-1-401", "Miller, Jeff", "R", "FL", "2-Nov-2001"),
        _row("107-house-1-401", "Roe (OH)", "R", "OH", "2-Nov-2001"),
        _row("107-house-1-401", "Roe, Ann Marie", "R", "OH", "2-Nov-2001"),
        # A first-named label is not a plain one: with Ann Marie listed, Ann is still one of two, and with Jo Ann
        # listed a first name no Davis is stated to go by is still unexplained.
        _row("107-house-1-401", "Roe, Ann", "R", "OH", "2-Nov-2001"),
        _row("107-house-1-401", "Davis, Tommy", "R", "VA", "2-Nov-2001"),
        _row("107-house-1-401", "Davis, Jo Ann", "R", "VA", "2-Nov-2001"),
        # Only a first-named label sets a member aside: a plain label that is one Poe's former surname does not.
        _row("107-house-1-401", "Quill", "D", "WA", "2-Nov-2001"),
        _row("107-house-1-401", "Poe", "D", "WA", "2-Nov-2001"),
    ]
    evidence = _evidence(tmp_path)
    count, ids = _fill(tmp_path, rows, evidence=evidence)
    assert ids == {
        ("101-house-2-10", "name:Smith, Denny (OR)"): "S000527",
        ("101-house-2-10", "name:Smith (OR)"): "S000607",
        ("101-house-2-11", "name:Smith (OR)"): None,
        ("107-house-1-5", "name:Miller (FL)"): "M000720",
        ("107-house-1-400", "name:Miller (FL)"): "M000720",
        ("107-house-1-400", "name:Miller, Jeff"): "M001144",
        ("107-house-1-401", "name:Miller (FL)"): None,
        ("107-house-1-401", "name:Miller, Dan"): "M000720",
        ("107-house-1-401", "name:Miller, Jeff"): "M001144",
        ("107-house-1-401", "name:Roe (OH)"): None,
        ("107-house-1-401", "name:Roe, Ann Marie"): "X000005",
        ("107-house-1-401", "name:Roe, Ann"): None,
        ("107-house-1-401", "name:Davis, Tommy"): None,
        ("107-house-1-401", "name:Davis, Jo Ann"): "D000597",
        ("107-house-1-401", "name:Quill"): "X000009",
        ("107-house-1-401", "name:Poe"): None,
    }
    assert count == 10
    event = _event(evidence)
    assert [(e["member_name"], e["bioguide_id"], e["rule"], e["rows"]) for e in event["resolved"]] == [
        ("Smith (OR)", "S000607", "surname+remaining_member", 1),
        ("Smith, Denny (OR)", "S000527", "surname+nickname", 1),
        ("Davis, Jo Ann", "D000597", "surname+first_name", 1),
        ("Miller (FL)", "M000720", "surname+remaining_member", 1),
        ("Miller (FL)", "M000720", "surname+seating_day", 1),
        ("Miller, Dan", "M000720", "surname+first_name", 1),
        ("Miller, Jeff", "M001144", "surname+first_name", 2),
        ("Quill", "X000009", "former_surname", 1),
        ("Roe, Ann Marie", "X000005", "surname+first_name", 1),
    ]
    assert [(e["member_name"], e["reason"], e["rows"]) for e in event["unresolved"]] == [
        ("Smith (OR)", "several_seated", 1), ("Davis, Tommy", "several_seated", 1), ("Miller (FL)", "several_seated", 1),
        ("Poe", "several_seated", 1), ("Roe (OH)", "several_seated", 1), ("Roe, Ann", "several_seated", 1),
    ]


def test_a_members_table_without_the_nickname_column_fills_as_before_it(tmp_path):
    """Until ``members`` publishes the column no nickname is read, so Denny and Tom name no one, and with Denny
    unnamed the plain Smith has no one to be told apart from. The journal says the inputs stated none."""
    rows = [
        _row("101-house-2-10", "Smith, Denny (OR)", "R", "OR", "24-Jan-1990"),
        _row("101-house-2-10", "Smith (OR)", "R", "OR", "24-Jan-1990"),
        _row("107-house-1-5", "Davis, Tom", "R", "VA", "7-Feb-2001"),
        _row("107-house-1-5", "Davis, Jo Ann", "R", "VA", "7-Feb-2001"),
    ]
    evidence = _evidence(tmp_path)
    count, ids = _fill(tmp_path, rows, _Published(nicknames=False), evidence)
    assert count == 1 and [key for key, bioguide in ids.items() if bioguide] == [("107-house-1-5", "name:Davis, Jo Ann")]
    event = _event(evidence)
    assert event["nicknames"] is False
    assert [(e["member_name"], e["reason"]) for e in event["unresolved"]] == [
        ("Smith (OR)", "several_seated"), ("Smith, Denny (OR)", "several_seated"), ("Davis, Tom", "several_seated"),
    ]


def test_a_day_two_terms_of_one_member_share_seats_one_member(tmp_path):
    """The seating day counts members, not terms: the day one term of hers ends and the next begins is still hers."""
    count, ids = _fill(tmp_path, [_row("102-house-2-150", "Twice", "D", "HI", "1-Jun-1992")])
    assert (count, ids) == (1, {("102-house-2-150", "name:Twice"): "X000007"})


def test_expired_former_surname_withdraws_a_held_id_on_the_votes_day(tmp_path):
    rows = [
        _row("107-house-2-100", "Quill", "D", "WA", "1-Jun-2002"),
        _row("107-house-2-101", "Quill", "D", "WA", "2-Jul-2002", bioguide="X000009"),
    ]
    evidence = _evidence(tmp_path)
    count, ids = _fill(tmp_path, rows, evidence=evidence)
    assert count == 1
    assert ids[("107-house-2-100", "name:Quill")] == "X000009"
    assert ids[("107-house-2-101", "name:Quill")] is None
    assert [(entry["member_name"], entry["reason"], entry["rows"]) for entry in _event(evidence)["unresolved"]] == [
        ("Quill", "outside_term", 1),
    ]


def test_two_rows_of_one_roll_call_that_resolve_to_one_member_both_stay_null(tmp_path):
    """The Clerk lists a member once. With the crosswalk holding only one of Virginia's two Davises, both of the
    Clerk's labels resolve to her; one of them is someone else, so neither is filled, on that roll call only."""
    rows = [
        _row("107-house-1-5", "Davis, Jo Ann", "R", "VA", "7-Feb-2001"),
        _row("107-house-1-5", "Davis, Tom", "R", "VA", "7-Feb-2001"),
        _row("107-house-1-6", "Davis, Jo Ann", "R", "VA", "8-Feb-2001"),
    ]
    evidence = _evidence(tmp_path)
    count, ids = _fill(tmp_path, rows, _Published(only={"D000597"}), evidence)
    assert count == 1 and ids == {
        ("107-house-1-5", "name:Davis, Jo Ann"): None,
        ("107-house-1-5", "name:Davis, Tom"): None,
        ("107-house-1-6", "name:Davis, Jo Ann"): "D000597",
    }
    assert [(entry["member_name"], entry["reason"], entry["rows"]) for entry in _event(evidence)["unresolved"]] == [
        ("Davis, Jo Ann", "listed_twice", 1), ("Davis, Tom", "listed_twice", 1),
    ]


@pytest.mark.parametrize("missing", SOURCES)
def test_an_unpublished_crosswalk_table_leaves_the_rows_as_merged(tmp_path, missing):
    held = [_row("102-house-1-1", "Abercrombie", "D", "HI", "3-JAN-1991", bioguide="A000014"),
            _row("102-house-1-2", "Abercrombie", "D", "HI", "3-JAN-1991")]
    out = tmp_path / "member_votes.parquet"
    _write(out, held, VOTES)
    before = out.read_bytes()
    evidence = _evidence(tmp_path)
    assert fill_house_name_bioguide_ids(tmp_path, out, _Published(without={missing}), evidence=evidence) == 0
    assert out.read_bytes() == before, "a held id is neither cleared nor a NULL filled without the whole crosswalk"
    event = _event(evidence)
    assert (event["rule_version"], event["input_available"], event["missing"]) == (RULE_VERSION, False, [missing])
    assert [prior_scratch_path(tmp_path, name).exists() for name in SOURCES] == [False] * 3


def test_a_table_with_no_name_rows_asks_for_no_crosswalk(tmp_path):
    download = _Published()
    count, ids = _fill(tmp_path, [row for row in ROWS if not (row["chamber"] == "house" and row["member_key"].startswith("name:"))], download)
    assert (count, download.asked) == (0, [])
    assert ids == {("108-house-1-5", "A000014"): "A000014", ("102-senate-1-1", "name:Abercrombie"): None}


def test_a_selected_input_read_failure_aborts_instead_of_publishing_held_ids(tmp_path):
    held = [_row("105-house-1-640", "Capps", "D", "CA", "13-Nov-1997", bioguide="C000134")]
    out = tmp_path / "member_votes.parquet"
    _write(out, held, VOTES)
    before = out.read_bytes()

    def missing_receipt(key, destination):
        raise FileNotFoundError("selected members receipt is missing")

    with pytest.raises(FileNotFoundError, match="receipt is missing"):
        fill_house_name_bioguide_ids(tmp_path, out, missing_receipt)
    assert out.read_bytes() == before
    assert not (tmp_path / "_member_votes_name_filled.parquet").exists()


def test_native_selection_is_bound_to_the_restored_crosswalk_bytes(tmp_path):
    from spicy_regs.congress_receipts import write_congress_dataset
    from spicy_regs.pipelines.rollups.subject_receipts import SelectedPriors
    from spicy_regs.selected_generations import SelectedDataset, remember_selection

    selected = []
    for name, (rows, columns) in _Published(nicknames=False).tables.items():
        raw = tmp_path / (name + "-raw.parquet")
        _write(raw, [dict.fromkeys(columns) | row for row in rows], columns)
        subject, receipt = write_congress_dataset(raw, tmp_path / (name + "-native"), dataset=name, generation_id="native")
        assert subject is not None
        selected.append(SelectedDataset(name, (subject,), receipt, "native"))
    remember_selection(tmp_path, selected)
    prior = SelectedPriors(tmp_path / "reader", root=tmp_path, public_url="")
    work = tmp_path / "work"
    work.mkdir()
    out = work / "member_votes.parquet"
    _write(out, [_row("102-house-1-1", "Abercrombie", "D", "HI", "3-JAN-1991")], VOTES)
    evidence = _evidence(tmp_path)
    assert fill_house_name_bioguide_ids(
        work, out, prior.download, evidence=evidence, selected_input=prior.input_provenance
    ) == 1
    for name, entry in _event(evidence)["inputs"].items():
        binding = entry["selection"]
        assert (binding["dataset"], binding["generationId"]) == (name, "native")
        assert binding["processing"]["sha256"] == entry["sha256"]
        assert len(binding["subjects"]) == 1 and binding["receipts"]["byteSize"] > 0

    before = out.read_bytes()
    with pytest.raises(ValueError, match="differs from selected native input"):
        fill_house_name_bioguide_ids(
            work, out, prior.download, selected_input=lambda _: {"processing": {"sha256": "wrong"}}
        )
    assert out.read_bytes() == before
