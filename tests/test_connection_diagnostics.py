"""Maintained phase tracking and real in-process SDK status dispatch, without source reads."""
import asyncio
import json
import threading
from time import monotonic

import pytest
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import CallToolResult, TextContent

from spicy_regs import connection_diagnostics as diagnostics, mcp_server


@pytest.fixture(autouse=True)
def reset_diagnostics():
    with diagnostics._lock:
        assert not diagnostics._active
        diagnostics._recent.clear()
        diagnostics._untracked = 0
    yield
    with diagnostics._lock:
        assert not diagnostics._active
        assert diagnostics._untracked == 0
        diagnostics._recent.clear()


def test_sdk_registers_strict_argument_free_status(monkeypatch):
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: pytest.fail("status opened a connection"))
    server = mcp_server.build_server()

    async def run():
        tools = await server.list_tools()
        tool = next(tool for tool in tools if tool.name == "connection_status")
        assert tool.input_schema["properties"] == {}
        assert not tool.input_schema.get("required")
        assert tool.input_schema["additionalProperties"] is False
        result = await server.call_tool("connection_status", {})
        assert isinstance(result, CallToolResult)
        assert result.structured_content is not None
        assert set(result.structured_content) == {"active", "recent", "untracked_active_phases", "scope"}
        assert result.structured_content["active"] == []
        assert isinstance(result.content[0], TextContent)
        assert json.loads(result.content[0].text) == result.structured_content
        with pytest.raises(ToolError):
            await server.call_tool("connection_status", {"unexpected": True})

    asyncio.run(run())


def test_sdk_status_answers_while_real_cold_builder_holds_connection_lock(monkeypatch):
    """One data worker occupies the only tool slot; status dispatch stays on the event loop."""
    entered, release = threading.Event(), threading.Event()
    monkeypatch.setattr(mcp_server, "TOOL_CONCURRENCY", 1)
    monkeypatch.setattr(mcp_server, "DATA_DIR", None)
    monkeypatch.setattr(mcp_server, "_cached_connection", None)
    monkeypatch.setattr(mcp_server, "_cached_connection_at", 0)
    monkeypatch.setattr(mcp_server, "_refreshing", False)
    monkeypatch.setattr(mcp_server, "_resolve_catalog_config", lambda: None)

    def blocked_publication():
        entered.set()
        assert release.wait(3), "test did not release the builder"
        raise ValueError("synthetic private publication refusal")

    monkeypatch.setattr(mcp_server, "_read_publication", blocked_publication)
    server = mcp_server.build_server()

    async def run():
        pending = asyncio.create_task(server.call_tool("query_sql", {"sql": "SELECT 1", "max_rows": 1}))
        try:
            deadline = monotonic() + 2
            while not entered.is_set() and monotonic() < deadline:
                await asyncio.sleep(0.005)
            assert entered.is_set()
            assert not pending.done()
            assert mcp_server._connection_lock.locked()
            result = await asyncio.wait_for(server.call_tool("connection_status", {}), 0.5)
            assert isinstance(result, CallToolResult)
            assert result.structured_content is not None
            active = result.structured_content["active"]
            assert [record["phase"] for record in active] == ["connection_build"]
            assert active[0]["operation_seconds"] >= active[0]["phase_seconds"] >= 0
            assert not pending.done()
        finally:
            release.set()
            with pytest.raises(ToolError, match="synthetic private publication refusal"):
                await asyncio.wait_for(pending, 2)
        result = await server.call_tool("connection_status", {})
        assert isinstance(result, CallToolResult)
        assert result.structured_content is not None
        assert result.structured_content["active"] == []
        assert any(row["phase"] == "connection_build" and row["error_type"] == "ValueError"
                   for row in result.structured_content["recent"])
        assert "private publication refusal" not in json.dumps(result.structured_content)

    asyncio.run(run())


def test_waiter_does_not_replace_builder_and_nested_phase_restores():
    entered, release = threading.Event(), threading.Event()

    def build():
        with diagnostics.phase("connection_build"):
            entered.set()
            assert release.wait(2)

    worker = threading.Thread(target=build)
    worker.start()
    try:
        assert entered.wait(1)
        with diagnostics.phase("connection_lock"):
            assert {row["phase"] for row in diagnostics.snapshot()["active"]} == {
                "connection_build", "connection_lock"}
            with diagnostics.phase("view_schema", "known_table"):
                assert "view_schema" in {row["phase"] for row in diagnostics.snapshot()["active"]}
            assert "connection_lock" in {row["phase"] for row in diagnostics.snapshot()["active"]}
    finally:
        release.set()
        worker.join(2)
    assert not worker.is_alive()


def test_exception_identity_preserved_and_message_excluded():
    error = ValueError("private SQL URL credential")
    with pytest.raises(ValueError) as caught:
        with diagnostics.phase("connection_build"):
            raise error
    assert caught.value is error
    state = diagnostics.snapshot()
    assert state["recent"][-1]["error_type"] == "ValueError"
    assert "private SQL URL credential" not in json.dumps(state)


def test_history_bounded_and_returned_records_detached():
    for _ in range(80):
        with diagnostics.phase("view_schema", "known_table"):
            pass
    state = diagnostics.snapshot()
    assert len(state["recent"]) == 32
    state["recent"][0]["phase"] = "changed"
    assert diagnostics.snapshot()["recent"][0]["phase"] == "view_schema"


def test_untracked_overflow_is_explicit_and_clears(monkeypatch):
    monkeypatch.setattr(diagnostics, "_max_active", 0)
    with diagnostics.phase("tool", "list_sources"):
        state = diagnostics.snapshot()
        assert state["active"] == []
        assert state["untracked_active_phases"] == 1
    assert diagnostics.snapshot()["untracked_active_phases"] == 0


def test_observed_tool_preserves_value_and_error():
    value = {"same": object()}
    assert mcp_server._run_observed_tool(lambda: value, {}) is value
    error = LookupError("preserved")

    def refused():
        raise error

    with pytest.raises(LookupError) as caught:
        mcp_server._run_observed_tool(refused, {})
    assert caught.value is error
