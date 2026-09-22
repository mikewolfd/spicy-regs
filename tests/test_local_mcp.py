"""Exercise CLI download receipts through the real local DuckDB/MCP reader.

Pins the managed-batch exposure — selected members only, pins rehashed and
verified — alongside the refusals: a damaged, incomplete or switched batch must
never fall back to stale root files or an unverified member.
"""

import builtins
import json

import pytest

from spicy_regs import mcp_server
from spicy_regs.local_data import local_selection
from spicy_regs.sources import publication
from tests.test_cli_generations import _download, _remote
from tests.test_mcp_server import _tool_data


def serve(monkeypatch, directory):
    monkeypatch.setattr(mcp_server, "DATA_DIR", directory)
    monkeypatch.setattr(mcp_server, "TABLES", ("a", "b", "dockets", "other"))
    monkeypatch.setattr(publication, "load_index", lambda _: pytest.fail("Local reader fetched remote index"))
    con = mcp_server._build_connection()
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    return con, mcp_server.build_server()


@pytest.mark.parametrize("location", ["root", "current", "batch"])
def test_local_batch_exposes_selected_members_and_verified_pins(tmp_path, monkeypatch, location):
    index, bodies, _, _ = _remote(monkeypatch, legacy=True)
    batch = _download(tmp_path, "a", "dockets")
    # Neither an unselected family sibling nor stale root files join this batch.
    (batch / "b.parquet").write_bytes(bodies["b"])
    (tmp_path / "a.parquet").write_bytes(bodies["b"])
    (tmp_path / "other.parquet").write_bytes(bodies["b"])
    directory = {"root": tmp_path, "current": tmp_path / "current", "batch": batch}[location]
    con, server = serve(monkeypatch, directory)
    try:
        sources = _tool_data(server, "list_sources", {})
        assert sources["tables"] == ["a", "dockets"]
        assert set(sources["publication"]) == {"a", "dockets"}
        assert sources["selected_directory"] == str(batch)
        pin = sources["publication"]["a"]
        assert pin["status"] == "managed_download"
        assert pin["artifact_digest"] == index["families"]["pair"]["artifactDigest"]
        assert "rehashed" in pin["verification"]
        assert sources["publication"]["dockets"] == {"status": "local_unversioned"}
        rows = _tool_data(server, "query_sql", {"sql": "SELECT id FROM a"})
        assert rows["rows"] == [{"id": "old-a"}]
        assert rows["connection_publication"]["a"] == pin
        assert _tool_data(server, "describe_table", {"table": "a"})["publication"] == pin
    finally:
        con.close()


@pytest.mark.parametrize("damage", ["content", "missing", "symlink", "selection-key", "selection-status", "incomplete"])
def test_damaged_batch_refuses_instead_of_falling_back(tmp_path, monkeypatch, damage):
    _, bodies, _, _ = _remote(monkeypatch)
    batch = _download(tmp_path, "a")
    (tmp_path / "a.parquet").write_bytes(bodies["a"])
    if damage == "content":
        (batch / "a.parquet").write_bytes(bodies["b"])
    elif damage == "missing":
        (batch / "a.parquet").unlink()
    elif damage == "symlink":
        (batch / "a.parquet").unlink()
        (batch / "a.parquet").symlink_to(tmp_path / "a.parquet")
    else:
        metadata = json.loads((batch / "download.json").read_text())
        if damage == "selection-key":
            metadata["selected"]["a"]["key"] = "a.parquet"
        elif damage == "selection-status":
            metadata["selected"]["a"]["status"] = "legacy-unversioned"
        else:
            metadata["status"] = "incomplete"
        (batch / "download.json").write_text(json.dumps(metadata))
    with pytest.raises(RuntimeError):
        serve(monkeypatch, tmp_path)


def test_current_switch_keeps_existing_connection_and_refreshes_next_one(tmp_path, monkeypatch):
    _remote(monkeypatch)
    old = _download(tmp_path, "a", "b")
    monkeypatch.setenv("SPICY_REGS_DATA_DIR", str(tmp_path / "current"))
    configured = mcp_server._resolve_data_dir()
    assert configured == tmp_path / "current"
    con, server = serve(monkeypatch, configured)
    _remote(monkeypatch, "new")
    new = _download(tmp_path, "a", "b")
    try:
        old_result = _tool_data(server, "query_sql", {"sql": "SELECT a.id AS a, b.id AS b FROM a, b"})
        assert old_result["rows"] == [{"a": "old-a", "b": "old-b"}]
        assert old_result["selected_directory"] == str(old)
        new_con, new_server = serve(monkeypatch, configured)
        try:
            result = _tool_data(new_server, "query_sql", {"sql": "SELECT a.id AS a, b.id AS b FROM a, b"})
            assert result["rows"] == [{"a": "new-a", "b": "new-b"}]
            assert result["selected_directory"] == str(new)
        finally:
            new_con.close()
    finally:
        con.close()


@pytest.mark.parametrize("damage", ["rewrite", "replace", "remove"])
def test_source_changes_after_connection_are_refused(tmp_path, monkeypatch, damage):
    _, bodies, _, _ = _remote(monkeypatch)
    batch = _download(tmp_path, "a")
    con, server = serve(monkeypatch, tmp_path)
    path = batch / "a.parquet"
    if damage == "rewrite":
        path.write_bytes(bodies["b"])
    elif damage == "replace":
        other = batch / "replacement"
        other.write_bytes(bodies["a"])
        other.replace(path)
    else:
        path.unlink()
    try:
        with pytest.raises(Exception, match="Local download member"):
            _tool_data(server, "query_sql", {"sql": "SELECT * FROM a"})
    finally:
        con.close()


def test_source_change_during_statement_is_refused(tmp_path, monkeypatch):
    _, bodies, _, _ = _remote(monkeypatch)
    batch = _download(tmp_path, "a")
    con, _ = serve(monkeypatch, tmp_path)
    try:
        with pytest.raises(RuntimeError, match="changed after verification"):
            with mcp_server._statement_timeout(con.cursor()):
                (batch / "a.parquet").write_bytes(bodies["b"])
    finally:
        con.close()


def test_current_cannot_escape_download_runs(tmp_path):
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (tmp_path / "current").symlink_to(elsewhere)
    for path in (tmp_path, tmp_path / "current"):
        with pytest.raises(RuntimeError, match="outside download-runs"):
            local_selection(path)


def test_local_batch_checks_declared_schema_after_byte_verification(tmp_path, monkeypatch):
    _remote(monkeypatch)
    batch = _download(tmp_path, "a")
    metadata = json.loads((batch / "download.json").read_text())
    metadata["publication"]["families"]["pair"]["tables"]["a.parquet"]["columns"] = [["wrong", "VARCHAR"]]
    (batch / "download.json").write_text(json.dumps(metadata))
    with pytest.raises(RuntimeError, match="schema differs"):
        serve(monkeypatch, tmp_path)


def test_managed_local_mcp_needs_no_optional_provider(tmp_path, monkeypatch):
    _remote(monkeypatch)
    _download(tmp_path, "a")
    original = builtins.__import__

    def base_only(name, *args, **kwargs):
        if name.split(".")[0] in {"spicy_docs", "rulespec_artifacts"}:
            raise ImportError(f"Optional module unavailable: {name}")
        return original(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", base_only)
    con, server = serve(monkeypatch, tmp_path)
    try:
        assert _tool_data(server, "query_sql", {"sql": "SELECT id FROM a"})["rows"] == [{"id": "old-a"}]
    finally:
        con.close()
