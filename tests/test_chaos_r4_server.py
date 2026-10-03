"""Round-4 chaos repairs at the server boundary: registered descriptions, kind refusals, release summaries, export rows.

Evidence: corpora/mcp-chaos-2026-10-02/round4/phase2-server.md (S1-S4, S10, the double send) and phase3-review.md.
"""

from __future__ import annotations

import asyncio
import inspect
import json

import duckdb
from starlette.testclient import TestClient

from spicy_regs import mcp_server as server
from tests.test_mcp_query_results import call

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
