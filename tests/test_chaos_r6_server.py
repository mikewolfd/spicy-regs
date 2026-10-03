"""Round-6 chaos repairs at the server boundary: strict arguments, read status, lineage, reply size, agency text.

Evidence: corpora/mcp-chaos-2026-10-02/round6/phase2-server.md (H1, H2, H4, M8, L7, L9, L10) and phase3-review.md
(the review's changes and the owner's decisions: refuse an oversized reply with its remedy, and shrink replies).
"""

from __future__ import annotations

import asyncio
import json

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server as server
from tests.test_chaos_r5_server import (DOCS_LIVE, HELD, _cite, _documents_parent, _held_field_reads, _index,
                                        _lineage, _resolve, _resolve_held)
from tests.test_mcp_relationships import citation_connection, merged_occurrences
from tests.test_mcp_server import _tool_data

#: One valid call per tool, so a test can add a single bad argument to it.
VALID = {
    "list_sources": {},
    "describe_table": {"table": "laws"},
    "query_sql": {"sql": "SELECT 1"},
    "lookup_agency": {"namespace": "regulations.gov:agency", "identifier": "OPM"},
    "resolve_document_citations": {"document_kind": "govinfo_package", "document_key": "CRPT-example"},
}


def _call(name, arguments):
    return asyncio.run(server.build_server().call_tool(name, arguments))


def _refusal(monkeypatch, name, arguments) -> str:
    monkeypatch.setattr(server, "_get_connection", lambda: pytest.fail(f"{name} ran with a bad argument"))
    with pytest.raises(ToolError) as refused:
        _call(name, arguments)
    return str(refused.value)


# H4, L9, L7: an argument a tool does not declare, or a value outside its bound, is refused in written words.

def test_every_tool_advertises_that_it_takes_no_other_argument():
    tools = asyncio.run(server.build_server().list_tools())
    assert {tool.name for tool in tools} == set(VALID)
    for tool in tools:
        assert tool.input_schema.get("additionalProperties") is False, tool.name
        assert "title" not in tool.input_schema and not any("title" in field for field in
                                                            tool.input_schema["properties"].values()), tool.name


@pytest.mark.parametrize("name", sorted(VALID))
def test_every_tool_refuses_an_argument_it_does_not_declare(monkeypatch, name):
    message = _refusal(monkeypatch, name, {**VALID[name], "offest": 50})
    assert "offest is not an argument." in message and f"{name} takes" in message


def test_a_misspelled_argument_is_refused_naming_the_arguments(monkeypatch):
    """zubair's offest ran page 0 with nothing said (round 6)."""
    message = _refusal(monkeypatch, "query_sql", {"sql": "SELECT 1", "offest": 50})
    assert message == ("Error executing tool query_sql: offest is not an argument. "
                       "query_sql takes max_cell_chars, max_rows, sql.")


def test_a_bound_is_stated_in_plain_words(monkeypatch):
    message = _refusal(monkeypatch, "query_sql", {"sql": "SELECT 1", "max_rows": 501})
    assert message == ("Error executing tool query_sql: max_rows: Input should be less than or equal to 500. "
                       "query_sql takes max_cell_chars, max_rows, sql.")


def test_an_unknown_argument_a_bad_value_and_a_missing_one_are_said_together(monkeypatch):
    message = _refusal(monkeypatch, "query_sql", {"max_rows": 0, "limit": 5})
    assert message == ("Error executing tool query_sql: limit is not an argument; sql is required; max_rows: Input "
                       "should be greater than or equal to 1. query_sql takes max_cell_chars, max_rows, sql.")


def test_an_unsupported_namespace_is_refused_naming_the_supported_ones(monkeypatch):
    """Reverses rounds 4-5: an unsupported namespace answered success with status unsupported_namespace."""
    message = _refusal(monkeypatch, "lookup_agency", {"namespace": "sec.gov:cik", "identifier": "OPM"})
    assert "namespace: Input should be 'regulations.gov:agency' or 'federal_register_agency'" in message
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "lookup_agency"]
    assert tool.input_schema["properties"]["namespace"]["enum"] == ["regulations.gov:agency", "federal_register_agency"]


# M8: a held field's read recorded before rule sets were kept is a read, and status words are defined where used.

def test_a_zero_row_read_recorded_without_a_rule_set_or_time_is_read_none_found(monkeypatch):
    """Every published document_citation_reads row states no rule set and no read time (round 6: 13 of 13, the
    dictionary calls that a read recorded before the table existed); court opinion 11264529 read 0 rows."""
    with citation_connection() as con:
        _held_field_reads(con, (HELD, None, None, 0))
        result = _resolve_held(con, monkeypatch)
    assert result["source_read"] == {"table": "comments", "status": "read_none_found"}
    assert result["coverage"]["partial"] is False


def test_an_undated_read_stating_rows_that_are_not_held_still_refuses(monkeypatch):
    """Reads of one text that state no time are ordered by their rows, so one stating rows is never hidden by a 0."""
    with citation_connection() as con, pytest.raises(ToolError, match="states 4 citation rows"):
        _held_field_reads(con, (HELD, None, None, 0), (HELD, None, None, 4))
        _resolve_held(con, monkeypatch)


def test_the_reply_defines_exactly_the_target_status_words_it_uses(monkeypatch):
    with citation_connection() as con:
        _cite(con, ("public_law", "119-public-999", "30"), ("crs_report_id", "R12345", "40"))
        result = _resolve(con, monkeypatch)
        found = _resolve(con, monkeypatch, cite_kind="public_law", max_occurrences=2)
    used = {row["target_status"] for row in merged_occurrences(result)}
    assert used == {"found", "missing", "not_checked"} == set(result["target_status_meaning"])
    assert "may not cover" in result["target_status_meaning"]["missing"]
    assert "reason" in result["target_status_meaning"]["not_checked"]
    assert set(found["target_status_meaning"]) == {"found"}
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "resolve_document_citations"]
    assert "target_status_meaning" in (tool.description or "")


# H2: inputs_current is three-valued, a family's own earlier output is its prior generation, an export parent is
# compared with the receipt the connection pinned, and a snapshot table is never stale for a source it may not read.

def _described(name="discovery_signals"):
    return _tool_data(server.build_server(), "describe_table", {"table": name})["publication"]


def test_a_current_input_and_an_unknown_one_are_unknown_not_current(monkeypatch):
    parents = {**_documents_parent(DOCS_LIVE, "sha256:" + "d" * 64), "comments.parquet": {"etag": '"e"', "byteSize": 9}}
    con, _ = _lineage(monkeypatch, parents)
    with con:
        described = _described()
    assert [item["input_table_current"] for item in described["inputs"]] == [None, True]
    assert described["inputs_current"] is None


def test_a_lagging_input_is_stale_whatever_else_is_unknown(monkeypatch):
    con, _ = _lineage(monkeypatch, {**_documents_parent(), "comments.parquet": {"etag": '"e"', "byteSize": 9}})
    with con:
        assert _described()["inputs_current"] is False


def test_the_familys_own_earlier_output_is_its_prior_generation_never_a_stale_input(monkeypatch):
    """gao-reports records its own previous generation as a parent of both its tables (round 6: read as a lag)."""
    prior = "sha256:" + "9" * 64
    own = {f"{table}.parquet": {"family": "discovery-signals", "artifactDigest": prior, "sha256": "sha256:" + "8" * 64,
                                "byteSize": 1} for table in ("discovery_signals", "signal_history")}
    con, _ = _lineage(monkeypatch, own)
    with con:
        described = _described()
    assert described["prior_generation"] == prior
    assert "inputs" not in described and "inputs_current" not in described
    con, _ = _lineage(monkeypatch, {**own, **_documents_parent()})
    with con:
        described = _described()
    assert described["prior_generation"] == prior and [item["table"] for item in described["inputs"]] == ["documents"]
    assert described["inputs_current"] is False


def _exports(matches=True, **receipt):
    return {"comments_index": {
        "rows": 3 if matches else None,
        "rows_basis": "comments_export_receipt" if matches else "export_receipt_does_not_match_object",
        "export_receipt": {"receipt_sha256": "sha256:" + "1" * 64, "sha256": "sha256:" + "7" * 64, "etag": '"x"',
                           "bytes": 9, "catalog_snapshot_id": "5", **receipt}}}


@pytest.mark.parametrize(("parent", "exports", "live", "current"), [
    ({"sha256": "sha256:" + "7" * 64}, _exports(), "sha256:" + "7" * 64, True),
    ({"sha256": "sha256:" + "6" * 64}, _exports(), "sha256:" + "7" * 64, False),
    ({"etag": '"x"'}, _exports(), '"x"', True),
    ({"etag": '"x"'}, _exports(matches=False), None, None),  # the file moved since its receipt: nothing to compare
    ({"sha256": "sha256:" + "7" * 64}, {}, None, None),  # no receipt pinned
])
def test_an_export_parent_is_compared_with_the_receipts_pin_of_the_same_kind(monkeypatch, parent, exports, live,
                                                                              current):
    con, _ = _lineage(monkeypatch, {"comments_index.parquet": {**parent, "byteSize": 9}})
    monkeypatch.setattr(server, "_export_rows", lambda cursor: exports)
    with con:
        described = _described()
    assert described["inputs"] == [{"table": "comments_index", "family": None, "built_from": next(iter(parent.values())),
                                    "live": live, "input_table_current": current}]
    assert described["inputs_current"] is current


def _snapshot(con, sources, previous="snapshot_" + "5" * 32):
    """Pin a rulemaking snapshot holding comment_periods, whose manifest records ``sources``."""
    manifest = {"snapshot_id": "snapshot_" + "6" * 32, "run_id": "r", "asserted_at": "2026-10-03T16:06:23Z",
                "inputs": {"previous_snapshot_id": previous, "sources": sources},
                "stages": [{"name": "comment-periods", "depends_on": ["proceedings"],
                            "outputs": ["comment_periods.parquet"]}]}
    pinned = {"snapshot_id": manifest["snapshot_id"], "manifest": manifest,
              "tables": {"comment_periods.parquet": {"sha256": "4" * 64, "rows": 2}}}
    con.execute("CREATE TABLE _spicy_rulemaking (snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_rulemaking VALUES (?)", [json.dumps(pinned)])
    con.execute("CREATE TABLE comment_periods (id VARCHAR)")


def test_a_snapshot_table_is_never_stale_for_a_source_it_may_not_have_read(monkeypatch):
    """The manifest records sources per snapshot and stages without theirs, so which sources a table read is unknown:
    a lagging source leaves every table of the snapshot unknown, never false (phase3-review item 3)."""
    con, _ = _lineage(monkeypatch, {})
    with con:
        _snapshot(con, {"documents.parquet": {"sha256": "f" * 64, "bytes": 1},
                        "unified_agenda.parquet": {"sha256": "a" * 64, "bytes": 1}})
        described = _described("comment_periods")
    assert described["prior_generation"] == "snapshot_" + "5" * 32
    assert described["snapshot_inputs"] == [
        {"table": "documents", "built_from": "sha256:" + "f" * 64, "live": "sha256:" + "d" * 64,
         "input_table_current": False},
        {"table": "unified_agenda", "built_from": "sha256:" + "a" * 64, "live": None, "input_table_current": None}]
    assert "inputs" not in described and described["inputs_current"] is None


def test_a_snapshot_whose_sources_are_all_live_is_current(monkeypatch):
    con, _ = _lineage(monkeypatch, {})
    with con:
        _snapshot(con, {"documents.parquet": {"sha256": "d" * 64, "bytes": 1}})
        described = _described("comment_periods")
    assert described["snapshot_inputs"] == [{"table": "documents", "built_from": "sha256:" + "d" * 64,
                                             "live": "sha256:" + "d" * 64, "input_table_current": True}]
    assert described["inputs_current"] is True


@pytest.mark.parametrize(("instant", "basis"), [
    ("2026-10-03T16:13:56Z", "last_object_write"),  # written by the 2026-10-03 backfill
    ("2026-10-01T03:12:26Z", "last_object_write"),
    ("2026-10-03T16:13:56.500000Z", "pointer_move"),  # compared as instants, not as text
    ("2026-10-03T16:41:21Z", "pointer_move"),
    (None, None),
])
def test_published_at_names_what_was_observed(monkeypatch, instant, basis):
    with citation_connection() as con:
        family = {"publishedAt": instant} if instant else {}
        con.execute("UPDATE _spicy_publication SET snapshot = ?", [json.dumps(_index(**family))])
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        described = _described("laws")
    assert (described["published_at"], described["published_at_basis"]) == (instant, basis)


@pytest.mark.parametrize("name", ["describe_table", "query_sql"])
def test_the_texts_say_inputs_are_what_the_producer_recorded(name):
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == name]
    text = " ".join((tool.description or "").split())  # a client reads the wrapped lines as one paragraph
    assert "the parents its producer recorded (none recorded is not none; a read that bypassed the download helper is " \
           "not recorded)" in text
