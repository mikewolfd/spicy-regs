"""Discovery stays compact while selected descriptions retain exact evidence."""

import asyncio
from copy import deepcopy
import json

import duckdb
import pytest

from spicy_regs import mcp_server as server
from tests.test_fec_release import digest
from tests.test_mcp_fec_release import configure, connection
from tests.test_mcp_server import _tool_data


def test_discovery_summarizes_mixed_release_states_and_names_each_view(tmp_path, monkeypatch):
    specs, index, _, consumer, _ = configure(tmp_path, monkeypatch)
    index["families"]["members"]["artifactDigest"] = digest("advanced-parent")
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        discovery = _tool_data(mcp, "list_sources", {})
        summary = discovery["fec_release"]
        assert summary["status_counts"] == {"compatible": 1, "disabled": 1}
        assert "views" not in summary
        assert summary["consumer"] == consumer
        assert summary["raw_query_financial_qualification"] == "not_inferred"
        assert "describe_table" in summary["details"]
        available = {name for group in discovery["relationship_views"] for name in group["views"]}
        assert specs[1].view.name in available
        assert specs[0].view.name not in available
        assert specs[0].view.name in discovery["unavailable_tables"]
        assert "fec_receipts" in {entry["table"] for entry in discovery["tables"]}
        selected = _tool_data(mcp, "describe_table", {"table": specs[0].view.name})
        assert selected["relationship"]["release_compatibility"]["reasons"]
        assert selected["relationship"]["release_compatibility"]["receipt_sha256"] == summary["receipt_sha256"]


@pytest.mark.parametrize("available", [True, False])
def test_selected_description_carries_evidence_once_without_changing_pinned_state(tmp_path, monkeypatch, available):
    specs, index, _, _, _ = configure(tmp_path, monkeypatch)
    if not available:
        index["families"]["members"]["artifactDigest"] = digest("advanced-parent")
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        name = specs[0].view.name
        relationships = deepcopy(server._connection_relationships(con))
        publication = deepcopy(server._publication_status(con))
        captured_release = deepcopy(server._pinned_record(con, "_spicy_fec_release"))
        expected = relationships[name]
        description = _tool_data(mcp, "describe_table", {"table": name})
        assert description["available"] is available
        assert description["metadata"] == expected["metadata"]
        assert "metadata" not in description["relationship"]
        assert description["relationship"]["status"] == expected["status"]
        assert description["relationship"]["reason"] == expected["reason"]
        assert description["relationship"]["dependencies"] == expected["dependencies"]
        evidence_owner = "publication" if available else "relationship"
        other = "relationship" if available else "publication"
        assert description[evidence_owner]["release_compatibility"] == expected["release_compatibility"]
        assert "release_compatibility" not in description[other]
        assert "qualification" in description and "joins" in description
        if available:
            assert description["columns"]
            queried = _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM {name}"})
            assert queried["publication"][name] == description["publication"]
            assert queried["rows"] == [{"id": 1}]
        else:
            assert description["publication"] == {"status": "unavailable"}
            assert description["schema_matches_declared"] is None
        description[evidence_owner]["release_compatibility"]["reasons"].append({"reason": "client edit"})
        description["metadata"]["summary"] = "client edit"
        _tool_data(mcp, "list_sources", {})
        assert server._connection_relationships(con) == relationships
        assert server._publication_status(con) == publication
        assert server._pinned_record(con, "_spicy_fec_release") == captured_release


def test_empty_release_summary_does_not_infer_qualification(monkeypatch):
    with duckdb.connect() as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        discovery = _tool_data(server.build_server(), "list_sources", {})
        summary = discovery["fec_release"]
        assert summary["status_counts"] == {}
        assert summary["receipt_sha256"] is None and summary["consumer"] is None
        assert summary["raw_query_financial_qualification"] == "not_inferred"


def test_reflected_descriptions_explain_summary_and_selected_evidence():
    descriptions = {tool.name: tool.description or "" for tool in asyncio.run(server.build_server().list_tools())}
    assert "status_counts" in descriptions["list_sources"]
    assert "describe_table" in descriptions["list_sources"]
    assert "release_compatibility" in descriptions["describe_table"]
    assert "unavailable" in descriptions["describe_table"]


def test_release_summary_size_does_not_expand_with_dependency_evidence(tmp_path, monkeypatch):
    specs, index, _, _, _ = configure(tmp_path, monkeypatch)
    index["families"]["members"]["artifactDigest"] = digest("advanced-parent")
    with connection(index) as con:
        before = server._fec_release_reply(con)
        captured = deepcopy(server._pinned_record(con, "_spicy_fec_release"))
        relationships = deepcopy(server._connection_relationships(con))
        name = specs[0].view.name
        relationships[name]["release_compatibility"]["reasons"] = [
            {"path": "dependencies.fec_receipts", "reason": "exact_table_pin_mismatch",
             "expected": {"members": ["évidence" * 1000]}, "actual": None}
        ]
    with duckdb.connect() as changed:
        changed.execute("CREATE TABLE _spicy_relationships(snapshot VARCHAR)")
        changed.execute("INSERT INTO _spicy_relationships VALUES (?)", [json.dumps(relationships)])
        # The selected release identity is constant; changing detail cannot enlarge discovery.
        changed.execute("CREATE TABLE _spicy_fec_release(snapshot VARCHAR)")
        changed.execute("INSERT INTO _spicy_fec_release VALUES (?)", [json.dumps(captured)])
        assert server._fec_release_reply(changed) == before
