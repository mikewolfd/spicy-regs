"""HTTP reproductions of silent query-result loss and false success flags."""

import json

import duckdb
import pytest
from starlette.testclient import TestClient

from spicy_regs import mcp_server


@pytest.fixture
def client(monkeypatch):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE fcc_filings AS SELECT 'DISSEMINATED' AS filing_status")
        con.execute("CREATE TABLE fcc_proceedings AS SELECT 'OPENALL' AS filing_status")
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        with TestClient(mcp_server.build_app()) as http:
            yield http


def call(client, name, arguments):
    response = client.post(
        "/mcp",
        headers={"Accept": "application/json, text/event-stream", "MCP-Protocol-Version": "2025-06-18"},
        json={"jsonrpc": "2.0", "id": "test", "method": "tools/call", "params": {"name": name, "arguments": arguments}},
    )
    assert response.status_code == 200
    messages = [json.loads(line[5:]) for line in response.text.splitlines() if line.startswith("data:")]
    return messages[-1]["result"]


@pytest.mark.parametrize(("name", "arguments", "message"), [
    ("describe_table", {"table": "blind_nonexistent_table_37a1"}, "Unknown table"),
    ("query_sql", {"sql": "SELECT 1", "max_rows": 0}, "greater than or equal to 1"),
    ("query_sql", {"sql": "SELECT 1", "max_rows": -1}, "greater than or equal to 1"),
    ("query_sql", {"sql": "SELECT 1", "max_rows": 501}, "less than or equal to 500"),
    ("query_sql", {"sql": "DELETE FROM blind_nonexistent_table_37a1 WHERE FALSE"}, "read-only"),
    ("query_sql", {"sql": "SELECT 1; DROP TABLE blind_nonexistent_table_37a1"}, "read-only"),
    ("query_sql", {"sql": "EXPLAIN ANALYZE DELETE FROM fcc_filings"}, "read-only"),
])
def test_refused_requests_set_mcp_error_flag(client, name, arguments, message):
    result = call(client, name, arguments)
    assert result["isError"] is True
    assert message in result["content"][0]["text"]


@pytest.mark.parametrize("sql", [
    "SELECT 1 AS value, 2 AS value LIMIT 1",
    "SELECT f.filing_status, p.filing_status FROM fcc_filings f CROSS JOIN fcc_proceedings p LIMIT 1",
])
def test_duplicate_names_require_aliases_instead_of_silently_losing_values(client, sql):
    result = call(client, "query_sql", {"sql": sql})
    assert result["isError"] is True
    assert "Duplicate result column names" in result["content"][0]["text"]
    assert "AS aliases" in result["content"][0]["text"]


def test_aliased_join_preserves_both_statuses(client):
    result = call(client, "query_sql", {"sql": (
        "SELECT f.filing_status AS submission_status, p.filing_status AS proceeding_status "
        "FROM fcc_filings f CROSS JOIN fcc_proceedings p LIMIT 1"
    )})
    assert result["isError"] is False
    assert result["structuredContent"]["rows"] == [
        {"submission_status": "DISSEMINATED", "proceeding_status": "OPENALL"}
    ]


@pytest.mark.parametrize(("size", "truncated"), [(0, False), (1, False), (2, False), (3, True)])
def test_truncation_measures_an_omitted_row_not_just_reaching_the_cap(client, size, truncated):
    result = call(client, "query_sql", {"sql": f"SELECT i FROM range({size}) t(i) ORDER BY i", "max_rows": 2})
    assert result["isError"] is False
    data = result["structuredContent"]
    assert data["rows"] == [{"i": i} for i in range(min(size, 2))]
    assert data["row_count_shown"] == min(size, 2)
    assert data["truncated"] is truncated


@pytest.mark.parametrize(("sql", "named"), [
    ("SELECT 1 AS x", set()),
    ("SELECT filing_status FROM fcc_filings", {"fcc_filings"}),
    ("WITH f AS (SELECT filing_status FROM fcc_filings) "
     "SELECT f.filing_status, (SELECT count(*) FROM fcc_proceedings) AS n FROM f", {"fcc_filings", "fcc_proceedings"}),
])
def test_a_query_reports_the_version_of_only_the_tables_it_names(client, sql, named):
    # Every table's version on every call made a one-row answer 54K characters on the deployed endpoint.
    result = call(client, "query_sql", {"sql": sql})
    assert result["isError"] is False
    data = result["structuredContent"]
    assert set(data["publication"]) == named
    assert "connection_publication" not in data


def test_a_misspelled_table_is_answered_with_close_names(client):
    result = call(client, "describe_table", {"table": "fcc_filing"})
    assert result["isError"] is True
    assert "Close names: fcc_filings" in result["content"][0]["text"]


def test_discovery_lists_tables_without_per_table_pins_or_audits(client):
    # Pins and ledger audits for every table made list_sources 164K characters; describe_table carries them.
    result = call(client, "list_sources", {})
    assert result["isError"] is False
    data = result["structuredContent"]
    assert {entry["table"] for entry in data["tables"]} == {"fcc_filings", "fcc_proceedings"}
    assert set(data["tables"][0]) == {"table", "label", "coverage"}
    assert not {"publication", "qualification", "datasets", "declared_tables"} & set(data)
