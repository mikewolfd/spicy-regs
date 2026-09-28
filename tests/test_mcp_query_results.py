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
    ("SELECT filing_status FROM FCC_Filings", {"fcc_filings"}),
    ("WITH fcc_filings AS (SELECT 'CTE' AS filing_status) SELECT filing_status FROM fcc_filings", set()),
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


def test_an_unparseable_statement_is_refused_with_the_parsers_message(client):
    result = call(client, "query_sql", {"sql": "SELEC filing_status FROM fcc_filings"})
    assert result["isError"] is True
    assert 'syntax error at or near "SELEC"' in result["content"][0]["text"]


def test_query_sql_reply_names_the_statement_it_answers(client):
    """A reply read out of context (a client's spill file) must say which statement it answers.

    describe_table echoes table and resolve_document_citations echoes its keys; query_sql echoed only max_rows,
    so a reader handed another call's spilled reply saw foreign columns and could not tell whose they were.
    """
    for sql, max_rows in (("SELECT count(*) AS n FROM fcc_filings", 5), ("SELECT filing_status FROM fcc_proceedings", 25)):
        reply = call(client, "query_sql", {"sql": sql, "max_rows": max_rows})["structuredContent"]
        assert (reply["sql"], reply["max_rows"]) == (sql, max_rows)
        assert next(iter(reply)) == "sql"  # first key: visible in the first bytes of a spilled single-line file


@pytest.mark.parametrize(("sql", "named"), [
    pytest.param("WITH fcc_filings AS (SELECT 9 AS x) SELECT x FROM fcc_filings", set(), id="a CTE shadows the view"),
    pytest.param("WITH fcc_filings AS (SELECT * FROM fcc_filings) SELECT * FROM fcc_filings", {"fcc_filings"},
                 id="a CTE body reads the view it shadows"),
    pytest.param("WITH fcc_filings AS (SELECT 9 AS x), g AS (SELECT x FROM fcc_filings) SELECT x FROM g", set(),
                 id="a later CTE sees an earlier one"),
    pytest.param("WITH g AS (SELECT * FROM fcc_filings), fcc_filings AS (SELECT 9 AS x) SELECT * FROM g",
                 {"fcc_filings"}, id="an earlier CTE does not see a later one"),
    pytest.param("WITH RECURSIVE fcc_filings(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM fcc_filings WHERE n < 3) "
                 "SELECT n FROM fcc_filings", set(), id="a recursive CTE reads itself"),
    pytest.param("SELECT (WITH fcc_filings AS (SELECT 9 AS x) SELECT x FROM fcc_filings), "
                 "(SELECT count(*) FROM fcc_filings)", {"fcc_filings"}, id="a CTE is scoped to its own subquery"),
    pytest.param('SELECT * FROM "fcc_filings"', {"fcc_filings"}, id="quoted"),
    pytest.param("SELECT * FROM FCC_Filings", {"fcc_filings"}, id="any case"),
    pytest.param("SELECT * FROM main.fcc_filings", {"fcc_filings"}, id="schema qualified"),
    pytest.param("SELECT * FROM memory.main.fcc_filings", {"fcc_filings"}, id="catalog qualified"),
    pytest.param("WITH fcc_filings AS (SELECT 9 AS x) SELECT * FROM main.fcc_filings", {"fcc_filings"},
                 id="a qualified name is never a CTE"),
    pytest.param("SELECT * FROM information_schema.tables", set(), id="another schema"),
    pytest.param("SELECT filing_status FROM fcc_filings UNION SELECT filing_status FROM fcc_proceedings",
                 {"fcc_filings", "fcc_proceedings"}, id="union"),
    pytest.param("SELECT * FROM read_parquet('https://example.test/fcc_filings.parquet')", set(),
                 id="a direct file read names no published table"),
    pytest.param("SELEC nonsense FROM fcc_filings", set(), id="unparseable"),
])
def test_tables_named_follows_sql_scope(sql, named):
    """The pins a reply carries come from these names: a false one claims a read that never happened."""
    with duckdb.connect() as con:
        assert mcp_server._tables_named(con, sql) == named
