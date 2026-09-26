"""Hermetic tests for delivery decision 2: the term each member vote counts toward."""

from __future__ import annotations

import importlib
import tomllib
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_regs import data_dictionary as dd
from spicy_regs.pipelines.rollups.member_vote_terms import MemberVoteTermsRollup
from spicy_regs.transforms.build_member_vote_terms import COLUMNS, build_member_vote_terms

REPO_ROOT = Path(__file__).resolve().parents[1]


def _write(path: Path, rows: list[dict], columns: list[str]) -> None:
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in columns])), path)


def _vote(vote_id, key, chamber, date, *, bioguide=None, lis=None):
    return {
        "vote_id": vote_id,
        "member_key": key,
        "chamber": chamber,
        "bioguide_id": bioguide,
        "lis_id": lis,
        "vote_date": date,
    }


def _term(bioguide, index, kind, start, end):
    return {"bioguide_id": bioguide, "term_index": index, "term_type": kind, "term_start": start, "term_end": end}


def _build(tmp_path, votes, terms, members=()):
    _write(
        tmp_path / "member_votes.parquet",
        votes,
        ["vote_id", "member_key", "chamber", "bioguide_id", "lis_id", "vote_date"],
    )
    _write(
        tmp_path / "member_terms.parquet", terms, ["bioguide_id", "term_index", "term_type", "term_start", "term_end"]
    )
    _write(tmp_path / "members.parquet", list(members), ["bioguide_id", "lis_id"])
    out = build_member_vote_terms(tmp_path)
    assert pq.read_schema(out).names == list(COLUMNS)
    return {(r["vote_id"], r["member_key"]): r for r in pq.read_table(out).to_pylist()}


def test_half_open_terms_with_a_unique_inclusive_end_fallback(tmp_path):
    rows = _build(
        tmp_path,
        [
            _vote("119-house-1-1", "A000370", "house", "3-Jan-2025", bioguide="A000370"),  # boundary day
            _vote("119-house-1-56", "T000489", "house", "4-Mar-2025", bioguide="T000489"),  # its term's end day
            _vote("119-house-1-1", "G000578", "house", "3-Jan-2025", bioguide="G000578"),  # after the term ended
            _vote("119-senate-1-1", "S421", "senate", "January 9, 2025,  02:54 PM", lis="S421"),
            _vote("119-senate-1-1", "S999", "senate", "January 9, 2025,  02:54 PM", lis="S999"),
            _vote("119-house-1-2", "O000001", "house", "7-Jan-2025", bioguide="O000001"),
        ],
        [
            _term("A000370", "5", "rep", "2023-01-03", "2025-01-03"),
            _term("A000370", "6", "rep", "2025-01-03", "2027-01-03"),
            _term("T000489", "0", "rep", "2023-01-03", "2025-03-04"),
            _term("G000578", "3", "rep", "2023-01-03", "2025-01-02"),
            _term("V000137", "0", "sen", "2025-01-03", "2025-01-09"),
            _term("V000137", "1", "rep", "2025-01-01", "2027-01-03"),  # the other chamber's term never counts
            _term("O000001", "0", "rep", "2025-01-03", None),  # an unknown end is not open-ended
        ],
        members=[{"bioguide_id": "V000137", "lis_id": "S421"}],
    )
    assert [(k, r["term_match"], r["term_index"]) for k, r in rows.items()] == [
        (("119-house-1-1", "A000370"), "half_open", "6"),
        (("119-house-1-56", "T000489"), "inclusive_end", "0"),
        (("119-house-1-1", "G000578"), "unmatched", None),
        (("119-senate-1-1", "S421"), "inclusive_end", "0"),
        (("119-senate-1-1", "S999"), "unresolved_member", None),
        (("119-house-1-2", "O000001"), "unmatched", None),
    ]
    assert rows[("119-senate-1-1", "S421")]["bioguide_id"] == "V000137"
    assert rows[("119-house-1-1", "A000370")]["vote_day"] == "2025-01-03"


def test_two_terms_under_one_rule_are_ambiguous_not_chosen(tmp_path):
    rows = _build(
        tmp_path,
        [_vote("v", "M", "house", "10-Feb-2025", bioguide="M")],
        [_term("M", "0", "rep", "2025-01-03", "2027-01-03"), _term("M", "1", "rep", "2025-02-01", "2027-01-03")],
    )
    assert rows[("v", "M")]["term_match"] == "ambiguous" and rows[("v", "M")]["term_index"] is None


@pytest.mark.parametrize(
    "chamber,literal",
    [("house", "2025-01-03"), ("house", "3-January-2025"), ("senate", "3-Jan-2025"), ("senate", "January 9, 2025")],
)
def test_a_date_outside_its_chambers_spelling_refuses_the_build(tmp_path, chamber, literal):
    with pytest.raises(ValueError, match="own spelling"):
        _build(tmp_path, [_vote("v", "M", chamber, literal, bioguide="M")], [])
    assert not (tmp_path / "member_vote_terms.parquet").exists()


@pytest.mark.parametrize("literal", [None, "", "  "])
def test_a_vote_whose_file_prints_no_date_is_undated_not_matched(tmp_path, literal):
    """No day means no term: the row is kept, its member still resolved, and nothing is guessed."""
    rows = _build(
        tmp_path,
        [
            _vote("119-house-1-9", "A000370", "house", literal, bioguide="A000370"),
            _vote("119-senate-1-9", "S421", "senate", literal, lis="S421"),
            _vote("119-house-1-1", "A000370", "house", "3-Jan-2025", bioguide="A000370"),
        ],
        [
            _term("A000370", "6", "rep", "2025-01-03", "2027-01-03"),
            _term("V000137", "0", "sen", "2025-01-03", "2031-01-03"),
        ],
        members=[{"bioguide_id": "V000137", "lis_id": "S421"}],
    )
    assert {k: (r["bioguide_id"], r["vote_day"], r["term_match"], r["term_index"]) for k, r in rows.items()} == {
        ("119-house-1-9", "A000370"): ("A000370", None, "undated", None),
        ("119-senate-1-9", "S421"): ("V000137", None, "undated", None),
        ("119-house-1-1", "A000370"): ("A000370", "2025-01-03", "half_open", "6"),
    }


def test_a_stated_bioguide_id_stands_before_the_lis_crosswalk(tmp_path):
    """spicy-docs' rule order: a Bioguide id the row states wins, even one members does not hold."""
    rows = _build(
        tmp_path,
        [
            _vote("119-senate-1-1", "S421", "senate", "January 9, 2025,  02:54 PM", bioguide="X000001", lis="S421"),
            _vote("119-senate-1-1", "S422", "senate", "January 9, 2025,  02:54 PM", lis="S422"),
        ],
        [
            _term("X000001", "0", "sen", "2025-01-03", "2031-01-03"),
            _term("V000137", "0", "sen", "2025-01-03", "2031-01-03"),
        ],
        members=[{"bioguide_id": "V000137", "lis_id": "S421"}, {"bioguide_id": None, "lis_id": "S422"}],
    )
    assert {k[1]: (r["bioguide_id"], r["term_match"]) for k, r in rows.items()} == {
        "S421": ("X000001", "half_open"),
        "S422": (None, "unresolved_member"),
    }


def test_rows_stream_out_in_input_order_and_whole_row_groups(tmp_path, monkeypatch):
    """The streamed writer cuts row groups where ``pq.write_table`` would and keeps ``member_votes``' order."""
    # The package re-exports the function under the module's name, so reach the module itself.
    module = importlib.import_module("spicy_regs.transforms.build_member_vote_terms")
    monkeypatch.setattr(module, "_ROW_GROUP", 2)
    votes = [_vote(f"119-house-1-{n}", "M", "house", "10-Feb-2025", bioguide="M") for n in (5, 3, 9, 1, 7)]
    rows = _build(tmp_path, votes, [_term("M", "0", "rep", "2025-01-03", "2027-01-03")])
    assert [key[0] for key in rows] == [vote["vote_id"] for vote in votes]
    metadata = pq.ParquetFile(tmp_path / "member_vote_terms.parquet").metadata
    assert [metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)] == [2, 2, 1]


def test_a_repeated_vote_row_or_a_shared_lis_id_refuses(tmp_path):
    vote = _vote("v", "M", "house", "10-Feb-2025", bioguide="M")
    with pytest.raises(ValueError, match="appears twice"):
        _build(tmp_path, [vote, vote], [])
    with pytest.raises(ValueError, match="names two members"):
        _build(
            tmp_path, [vote], [], members=[{"bioguide_id": "A", "lis_id": "S1"}, {"bioguide_id": "B", "lis_id": "S1"}]
        )


def test_the_rollup_is_registered_scheduled_and_described():
    scripts = tomllib.loads((REPO_ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    assert f"run-rollup-{MemberVoteTermsRollup.name}" in scripts
    workflow = yaml.safe_load((REPO_ROOT / ".github/workflows/rollup-member-vote-terms.yml").read_text())
    assert workflow["jobs"]["run"]["with"]["command"] == "run-rollup-member-vote-terms"
    assert dd.expected_schemas()["member_vote_terms"] == [(c, "VARCHAR") for c in COLUMNS]
    assert "member_vote_terms" in dd.MCP_QUERYABLE and (REPO_ROOT / "docs/tables/member_vote_terms.md").exists()
