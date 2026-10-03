"""Round-5 chaos repairs at the server boundary: citation order and paging, read statuses and kind scopes.

Evidence: corpora/mcp-chaos-2026-10-02/round5/phase2-server.md (S5-1..S5-4, freshness) and phase3-review.md (the
recorded decisions: three read statuses from existing parent columns; meaning text moved into the artifacts).
"""

from __future__ import annotations

import asyncio

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server as server
from spicy_regs.citation_resolution import SOURCE_TABLES
from tests.test_mcp_relationships import citation_connection
from tests.test_mcp_server import _tool_data


def _resolve(con, monkeypatch, **arguments):
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    return _tool_data(server.build_server(), "resolve_document_citations",
                      {"document_kind": "govinfo_package", "document_key": "CRPT-example", **arguments})


def _cite(con, *rows, key="CRPT-example", digest="digest"):
    """Add (cite_kind, target_key, span_start) occurrences to a held report."""
    con.executemany("INSERT INTO document_citations VALUES "
                    f"('govinfo_package', '{key}', '{digest}', ?, ?, 'true', ?, '003')", rows)


def _report(con, key, digest="other", pages="9", rules="rules", rows="0"):
    con.execute("INSERT INTO house_activity_reports VALUES (?, ?, ?, ?, ?)", [key, digest, pages, rules, rows])


# S5-1: offsets order numerically; one kind and a page are selectable; every kind's count is stated.

def test_occurrences_follow_text_offsets_within_a_kind(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("public_law", "114-public-254", "18732"), ("public_law", "114-public-254", "111876"))
        result = _resolve(con, monkeypatch)
    assert [row["span_start"] for row in result["occurrences"]] == ["1", "2", "18732", "111876"]


def test_cite_kind_selects_one_kind_and_counts_every_held_kind(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("bill_number", "114-hr-1", "5"), ("bill_number", "114-hr-2", "7"))
        result = _resolve(con, monkeypatch, cite_kind="Public_Law", max_occurrences=1)
    assert [row["cite_kind"] for row in result["occurrences"]] == ["public_law"]
    selection = result["coverage"]["occurrence_selection"]
    assert selection["cite_kind"] == "public_law" and selection["status"] == "capped"
    assert result["coverage"]["cite_kind_counts"] == {"bill_number": 2, "public_law": 2}


def test_offset_pages_through_a_kind_without_overlap(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("public_law", "114-public-254", "30"))
        pages = [_resolve(con, monkeypatch, cite_kind="public_law", max_occurrences=2, offset=offset)
                 for offset in (0, 2)]
    assert [[row["span_start"] for row in page["occurrences"]] for page in pages] == [["1", "2"], ["30"]]
    assert [page["truncated"] for page in pages] == [True, False]
    assert [page["coverage"]["occurrence_selection"]["status"] for page in pages] == ["capped", "last_page"]
    # The rows before this page were not looked up; no occurrence reason records that.
    assert pages[1]["coverage"]["partial"] is True and pages[1]["coverage"]["reason_counts"] == {}


def test_a_routed_kind_the_document_does_not_hold_is_a_complete_empty_selection(monkeypatch):
    with citation_connection() as con:
        result = _resolve(con, monkeypatch, cite_kind="crs_report_id")
    assert result["occurrences"] == []
    assert result["coverage"]["occurrence_selection"]["status"] == "complete_held_selection"
    assert result["coverage"]["cite_kind_counts"] == {"public_law": 2}
    assert result["coverage"]["partial"] is False


def test_a_kind_neither_routed_nor_held_is_refused_naming_the_held_kinds(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError) as refused:
        _resolve(con, monkeypatch, cite_kind="public_laws")
    assert "'public_laws'" in str(refused.value) and "holds: public_law" in str(refused.value)


# S5-1 EXPAND: the page boundary is stable, the end is stated, and nothing is guessed.

def test_rows_tied_on_kind_span_and_key_page_apart_by_their_text(monkeypatch):
    """Rows of two held texts of one document share kind, span and key; the identity's digest orders them."""
    with citation_connection() as con:
        _cite(con, ("public_law", "114-public-254", "1"), digest="digest-0")
        pages = [_resolve(con, monkeypatch, max_occurrences=1, offset=offset)["occurrences"] for offset in (0, 1, 2)]
    assert [(row["span_start"], row["text_sha256"]) for [row] in pages] == [
        ("1", "digest"), ("1", "digest-0"), ("2", "digest")]


def test_an_offset_past_the_end_is_an_empty_last_page_that_is_partial(monkeypatch):
    with citation_connection() as con:
        result = _resolve(con, monkeypatch, offset=50)
    assert result["occurrences"] == [] and result["truncated"] is False
    assert result["coverage"]["occurrence_selection"] | {"meaning": None} == {
        "status": "last_page", "cite_kind": None, "offset": 50, "max_occurrences": 100, "meaning": None}
    assert result["coverage"]["partial"] is True


def test_a_span_that_is_not_an_integer_is_refused_rather_than_ordered_as_text(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError, match="span"):
        _cite(con, ("public_law", "114-public-254", "12a"))
        _resolve(con, monkeypatch)


def test_cite_kind_is_a_parameter_not_sql(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError, match="neither a kind this document holds"):
        _resolve(con, monkeypatch, cite_kind="public_law' OR 1=1 --")


def test_an_unknown_key_is_refused_for_the_key_before_any_cite_kind(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError, match="case-sensitive"):
        _resolve(con, monkeypatch, document_key="CRPT-missing", cite_kind="public_laws")


# S5-2: not held, not read and read with none found are three answers, and a key nothing holds is refused.

def test_a_key_nothing_holds_is_refused_naming_the_held_spelling(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError) as refused:
        _resolve(con, monkeypatch, document_key="crpt-example")
    message = str(refused.value)
    assert "'crpt-example'" in message and "house_activity_reports" in message and "'CRPT-example'" in message
    assert "case-sensitive" in message


def test_a_key_held_under_another_kind_is_refused_naming_that_kind(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError) as refused:
        con.execute("CREATE TABLE budget_volumes AS SELECT * FROM house_activity_reports LIMIT 0")
        _resolve(con, monkeypatch, document_kind="budget_volume")
    message = str(refused.value)
    assert "budget_volumes" in message and "govinfo_package 'CRPT-example'" in message


def test_citations_of_a_document_no_longer_held_read_not_held(monkeypatch):
    with citation_connection() as con:
        con.execute("DELETE FROM house_activity_reports")
        result = _resolve(con, monkeypatch)
    assert result["source_read"]["status"] == "not_held"
    assert {row["source_status"] for row in result["occurrences"]} == {"unread_source"}


def test_a_held_document_without_a_digest_stays_missing_digest(monkeypatch):
    with citation_connection() as con:
        con.execute("UPDATE house_activity_reports SET text_sha256 = NULL")
        assert _resolve(con, monkeypatch)["source_read"]["status"] == "missing_digest"


def test_a_report_read_with_no_findings_is_read_none_found(monkeypatch):
    with citation_connection() as con:
        _report(con, "CRPT-quiet")
        result = _resolve(con, monkeypatch, document_key="CRPT-quiet")
    assert result["occurrences"] == [] and result["source_read"]["status"] == "read_none_found"
    assert result["coverage"]["occurrence_selection"]["status"] == "complete_held_selection"
    assert result["coverage"]["partial"] is False


@pytest.mark.parametrize("unrecorded", ["digest", "pages", "rules", "rows"])
def test_a_report_without_a_read_record_is_not_read_and_partial(monkeypatch, unrecorded):
    with citation_connection() as con:
        _report(con, "CRPT-quiet", **{unrecorded: None})
        result = _resolve(con, monkeypatch, document_key="CRPT-quiet")
    assert result["occurrences"] == [] and result["source_read"]["status"] == "not_read"
    assert result["coverage"]["partial"] is True


def test_a_report_whose_read_states_rows_that_are_not_held_is_refused(monkeypatch):
    with citation_connection() as con, pytest.raises(ToolError, match="states 5 citation rows"):
        _report(con, "CRPT-quiet", rows="5")
        _resolve(con, monkeypatch, document_key="CRPT-quiet")


@pytest.mark.parametrize("body", ["Pursuant to 5 U.S.C. 553.", None])
def test_a_held_field_with_no_rows_is_not_read_because_its_table_records_no_read(monkeypatch, body):
    """Held fields are read only when an operator selects them; the source table records no such read."""
    with citation_connection() as con:
        con.execute("CREATE TABLE comments (comment_id VARCHAR, comment VARCHAR)")
        con.execute("INSERT INTO comments VALUES ('EPA-HQ-0001', ?)", [body])
        result = _resolve(con, monkeypatch, document_kind="comment_inline", document_key="EPA-HQ-0001")
    assert result["source_read"] == {"table": "comments", "status": "not_read"}
    assert result["coverage"]["partial"] is True


# S5-3 and S5-4: what the client is told.

def _citation_tool():
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "resolve_document_citations"]
    return tool


def test_the_schema_names_the_table_each_kind_reads():
    meaning = _citation_tool().input_schema["properties"]["document_kind"]["description"]
    assert all(f"{kind}: {table}" in meaning for kind, table in SOURCE_TABLES.items())


def test_the_description_defines_partial_the_key_rule_and_the_read_statuses():
    description = _citation_tool().description or ""
    assert "coverage.partial" in description and "case-sensitive" in description
    assert all(status in description for status in ("read_none_found", "not_read", "not_held"))
    assert "govinfo_package covers only" in description
