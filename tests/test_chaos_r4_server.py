"""Round-4 chaos repairs at the server boundary: registered descriptions, kind refusals, release summaries, export rows.

Evidence: corpora/mcp-chaos-2026-10-02/round4/phase2-server.md (S1-S4, S10, the double send) and phase3-review.md.
"""

from __future__ import annotations

import asyncio
import inspect
import json

import duckdb
import pytest
from mcp.server.mcpserver.exceptions import ToolError
from starlette.testclient import TestClient

from spicy_regs import mcp_server as server
from spicy_regs.citation_resolution import SOURCE_TABLES
from spicy_regs.citation_sources import TEXT_SOURCES
from tests.test_mcp_query_results import call
from tests.test_mcp_relationships import citation_connection
from tests.test_mcp_server import _tool_data

# The client's cap is 2,048 characters (Claude Code's MAX_MCP_DESCRIPTION_LENGTH default); keep a margin.
DESCRIPTION_BUDGET = 2_000


# S3 and the double send: what the client is sent.

def test_every_registered_description_fits_the_client_cap_without_indentation():
    tools = asyncio.run(server.build_server().list_tools())
    for tool in tools:
        description = tool.description or ""
        assert len(description) <= DESCRIPTION_BUDGET, (tool.name, len(description))
        assert not any(line[:1].isspace() for line in description.splitlines()), tool.name


def test_registered_descriptions_are_the_cleaned_docstrings():
    tools = {tool.name: tool.description or "" for tool in asyncio.run(server.build_server().list_tools())}
    assert tools["query_sql"].startswith("Run read-only SQL")
    assert tools["query_sql"] == inspect.cleandoc(tools["query_sql"])


def test_text_content_is_the_compact_json_of_the_structured_content(monkeypatch):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE comments_index AS SELECT 'FDA' AS agency_code, 1.5 AS row_count")
        server._install_relationship_views(con)
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        with TestClient(server.build_app()) as client:
            for name, arguments in (("query_sql", {"sql": "SELECT 1 AS one, 'é' AS accent, 0.1 AS tenth"}),
                                    ("describe_table", {"table": "comments_index"}), ("list_sources", {})):
                result = call(client, name, arguments)
                [content] = result["content"]
                assert content["text"] == json.dumps(result["structuredContent"], separators=(",", ":"),
                                                     ensure_ascii=False)


# S1: resolve_document_citations refuses a kind no writer emits.

def test_supported_kinds_are_exactly_the_writers_kinds():
    from spicy_docs.schemas.budget_volume_tables import BUDGET_VOLUME
    from spicy_docs.schemas.document_citation_tables import GOVINFO_PACKAGE

    assert set(SOURCE_TABLES) == {GOVINFO_PACKAGE, BUDGET_VOLUME} | set(TEXT_SOURCES)


@pytest.mark.parametrize(("kind", "closest"), [
    ("federal_register", None),
    ("court_opinion", "court_opinion_derived_pdf"),
    ("house_activity_reports", "govinfo_package"),
    ("house_activity_report", "govinfo_package"),
    ("budget_volumes", "budget_volume"),
])
def test_an_unsupported_kind_is_refused_naming_the_supported_kinds(monkeypatch, kind, closest):
    with citation_connection() as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        with pytest.raises(ToolError) as refused:
            _tool_data(server.build_server(), "resolve_document_citations",
                       {"document_kind": kind, "document_key": "CRPT-example"})
    message = str(refused.value)
    assert repr(kind) in message and "govinfo_package" in message and "comment_inline" in message
    if closest is not None:
        assert f"closest supported kind is {closest!r}" in message


def test_kinds_compare_case_insensitively(monkeypatch):
    with citation_connection() as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        result = _tool_data(server.build_server(), "resolve_document_citations",
                            {"document_kind": "Govinfo_Package", "document_key": "CRPT-example"})
    assert result["document_kind"] == "govinfo_package"
    assert len(result["occurrences"]) == 2 and result["source_read"]["status"] == "read"


def test_the_schema_enumerates_the_supported_kinds():
    [tool] = [t for t in asyncio.run(server.build_server().list_tools()) if t.name == "resolve_document_citations"]
    assert tool.input_schema["properties"]["document_kind"]["enum"] == sorted(SOURCE_TABLES)
