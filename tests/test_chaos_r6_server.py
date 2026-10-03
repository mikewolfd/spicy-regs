"""Round-6 chaos repairs at the server boundary: strict arguments, read status, lineage, reply size, agency text.

Evidence: corpora/mcp-chaos-2026-10-02/round6/phase2-server.md (H1, H2, H4, M8, L7, L9, L10) and phase3-review.md
(the review's changes and the owner's decisions: refuse an oversized reply with its remedy, and shrink replies).
"""

from __future__ import annotations

import asyncio

import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server as server

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
