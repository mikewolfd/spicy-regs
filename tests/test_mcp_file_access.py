"""Query the real configured local dataset while refusing unselected files."""

import asyncio
from pathlib import Path

import duckdb
import pytest
from mcp.server.fastmcp.exceptions import ToolError

from spicy_regs import mcp_server
from tests.test_mcp_server import _tool_data


@pytest.fixture
def selected_dataset(tmp_path, monkeypatch):
    selected = tmp_path / "selected"
    selected.mkdir()
    path = selected / "agency_stats.parquet"
    with duckdb.connect() as con:
        con.execute("COPY (SELECT 'EPA' AS agency_code, 7 AS docket_count) TO ? (FORMAT PARQUET)", [str(path)])
    outside = tmp_path / "outside.txt"
    outside.write_text("constructed outside fixture")
    link = selected / "outside-link"
    link.symlink_to(outside)
    monkeypatch.setattr(mcp_server, "DATA_DIR", selected)
    mcp_server._reset_connection_cache()
    yield mcp_server.build_server(), selected, outside
    mcp_server._reset_connection_cache()


def test_selected_parquet_view_remains_queryable(selected_dataset):
    server, _, _ = selected_dataset
    result = _tool_data(server, "query_sql", {"sql": "SELECT agency_code,docket_count FROM agency_stats LIMIT 1"})
    assert result["rows"] == [{"agency_code": "EPA", "docket_count": 7}]


@pytest.mark.parametrize("shape", ["direct", "nested", "traversal", "symlink", "glob", "blob"])
def test_unselected_file_reads_are_refused_by_duckdb(selected_dataset, shape):
    server, selected, outside = selected_dataset
    paths = {"traversal": selected / ".." / outside.name, "symlink": selected / "outside-link"}
    target = paths.get(shape, outside)
    target_sql = str(target).replace("'", "''")
    if shape == "glob":
        sql = f"SELECT * FROM glob('{target_sql}') LIMIT 1"
    else:
        function = "read_blob" if shape == "blob" else "read_text"
        sql = f"SELECT content FROM {function}('{target_sql}') LIMIT 1"
        if shape == "nested":
            sql = f"WITH x AS ({sql}) SELECT * FROM x LIMIT 1"
    with pytest.raises(ToolError, match="disabled by configuration"):
        asyncio.run(server.call_tool("query_sql", {"sql": sql}))


def test_os_metadata_reproduction_is_refused_when_present(selected_dataset):
    if not Path("/etc/os-release").is_file():
        return  # Linux container smoke runs this exact deployed reproduction.
    server, _, _ = selected_dataset
    with pytest.raises(ToolError, match="disabled by configuration"):
        asyncio.run(server.call_tool("query_sql", {"sql": "SELECT content FROM read_text('/etc/os-release') LIMIT 1"}))


def test_query_cannot_relax_external_access(selected_dataset):
    server, _, _ = selected_dataset
    with pytest.raises(ToolError, match="read-only"):
        asyncio.run(server.call_tool("query_sql", {"sql": "SET enable_external_access=true; SELECT 1"}))
    con = mcp_server._get_connection()
    with pytest.raises(duckdb.Error):
        con.execute("SET enable_external_access=true")
    assert con.execute("SELECT current_setting('allow_persistent_secrets')").fetchone() == (False,)
