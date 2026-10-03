"""Exact-pin refusal through the local MCP boundary, using synthetic releases."""

from copy import deepcopy
from dataclasses import replace
import json

import duckdb
import pytest
from starlette.testclient import TestClient

from spicy_regs import fec_release as release, mcp_server as server
from tests.test_fec_release import capture, digest, fixture
from tests.test_mcp_server import _tool_data
from tests.test_mcp_query_results import call


def configure(tmp_path, monkeypatch):
    specs, index, receipt, consumer = fixture(tmp_path)
    raw = json.dumps(receipt, sort_keys=True).encode()
    path = tmp_path / "selected-receipt.json"
    path.write_bytes(raw)
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_FILE", str(path))
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_SHA256", release.sha256(raw))
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", consumer["image_digest"])
    monkeypatch.setattr(server, "FEC_QUALIFIED_VIEWS", specs)
    monkeypatch.setattr(server, "DATA_DIR", None)
    monkeypatch.setattr(server, "TABLES", ("fec_receipts", "fec_reports", "members"))
    monkeypatch.setattr(release, "runtime_consumer", lambda image_digest: {**deepcopy(consumer), "image_digest": image_digest})
    return specs, index, receipt, consumer, path


def connection(index, *, value=1):
    con = duckdb.connect()
    con.execute("CREATE TABLE _spicy_publication(snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(index)])
    for name in ["fec_receipts", "fec_reports", "members"]:
        con.execute(f"CREATE TABLE {name}(id INTEGER)")
        con.execute(f"INSERT INTO {name} VALUES (?)", [value])
    server._install_relationship_views(con)
    server._apply_security_settings(con)
    return con


def test_compatible_mcp_responses_include_exact_receipt_dependency_and_consumer_ids(tmp_path, monkeypatch):
    specs, index, _, consumer, _ = configure(tmp_path, monkeypatch)
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        discovery = _tool_data(mcp, "list_sources", {})
        name = specs[0].view.name
        assert discovery["fec_release"]["status_counts"] == {"compatible": len(specs)}
        described = _tool_data(mcp, "describe_table", {"table": name, "detail": True})
        assert described["available"]
        result = _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM {name}"})
        attestation = result["publication"][name]["release_compatibility"]
        assert result["rows"] == [{"id": 1}]
        assert attestation["receipt_sha256"] == discovery["fec_release"]["receipt_sha256"]
        full = described["publication"]["release_compatibility"]
        assert full["consumer"] == consumer
        assert full["dependencies"]["members"] == release.captured_table(index, "members")
        assert attestation["sql_sha256"] == full["sql_sha256"] and full["interpretation"]["policies"]
        assert "consumer" not in attestation and "dependencies" not in attestation
        raw = _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM fec_receipts"})
        assert "release_compatibility" not in raw["publication"]["fec_receipts"]


def test_discovery_fits_sse_limit_with_many_views_and_partitioned_dependencies(tmp_path, monkeypatch):
    registry_size = len(server.FEC_QUALIFIED_VIEWS)
    specs, index, receipt, _, path = configure(tmp_path, monkeypatch)
    base = specs[0]
    specs = tuple(replace(base, view=replace(base.view, name=f"fec_test_partitioned_{i}"))
                  for i in range(registry_size))
    table = index["families"]["fec-query"]["tables"]["fec_receipts.parquet"]
    del table["sha256"]
    table.update(rows=50, byteSize=5000, partitionColumns=["id"], members=[
        {"key": f"fec_receipts/id=1/part-{i:06}.parquet", "sha256": digest(str(i)),
         "rows": 1, "byteSize": 100, "partition": {"id": "1"}}
        for i in range(50)
    ])
    receipt["output_membership"]["fec_receipts"] = release.captured_table(index, "fec_receipts")
    view_receipt = receipt["views"][base.view.name]
    view_receipt["dependencies"]["fec_receipts"] = release.captured_table(index, "fec_receipts")
    receipt["views"] = {s.view.name: deepcopy(view_receipt) for s in specs}
    raw = json.dumps(receipt).encode()
    path.write_bytes(raw)
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_SHA256", release.sha256(raw))
    monkeypatch.setattr(server, "FEC_QUALIFIED_VIEWS", specs)
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        with TestClient(server.build_app()) as client:
            listed = call(client, "list_sources", {})
            # MCP sends both text and structured content in the same SSE event.
            assert len(json.dumps(listed).encode()) < 1024 * 1024
            summary = listed["structuredContent"]["fec_release"]
            assert summary["status_counts"] == {"compatible": len(specs)}
            assert "views" not in summary
            # Release discovery is bounded independently of view/partition detail.
            assert len(json.dumps(summary).encode()) < 2048
            names = {name for group in listed["structuredContent"]["relationship_views"] for name in group["views"]}
            assert names.issuperset(s.view.name for s in specs)
            described = call(client, "describe_table", {"table": specs[0].view.name, "detail": True})["structuredContent"]
            assert described["publication"]["release_compatibility"]["dependencies"] == view_receipt["dependencies"]


def test_parent_advancement_refuses_affected_query_but_keeps_old_connection_and_unrelated_views(tmp_path, monkeypatch):
    specs, index, _, _, _ = configure(tmp_path, monkeypatch)
    old = connection(index)
    changed = deepcopy(index)
    changed["families"]["members"]["artifactDigest"] = digest("new-parent")
    with connection(changed, value=2) as new:
        monkeypatch.setattr(server, "_get_connection", lambda: new)
        mcp = server.build_server()
        name = specs[0].view.name
        description = _tool_data(mcp, "describe_table", {"table": name})
        assert not description["available"]
        assert "exact_table_pin_mismatch" in description["relationship"]["reason"]
        with pytest.raises(Exception, match="disabled.*exact_table_pin_mismatch"):
            _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM {name}"})
        assert _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM {specs[1].view.name}"})["rows"] == [{"id": 2}]
        assert _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM members"})["rows"] == [{"id": 2}]
        monkeypatch.setattr(server, "_get_connection", lambda: old)
        prior = _tool_data(mcp, "query_sql", {"sql": f"SELECT * FROM {name}"})
        assert prior["rows"] == [{"id": 1}]
        assert prior["publication"][name]["input_publications"]["members"]["artifact_digest"] == index["families"]["members"]["artifactDigest"]
    old.close()


@pytest.mark.parametrize("damage", ["missing_pin", "bad_bytes", "unreadable", "image"])
def test_receipt_or_deployment_failure_leaves_raw_tables_queryable(tmp_path, monkeypatch, damage):
    specs, index, _, _, path = configure(tmp_path, monkeypatch)
    if damage == "missing_pin":
        monkeypatch.delenv("SPICY_REGS_FEC_RELEASE_SHA256")
    elif damage == "bad_bytes":
        path.write_text("{}")
    elif damage == "unreadable":
        monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_FILE", str(tmp_path / "absent"))
    else:
        monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", digest("another-image"))
    with connection(index) as con:
        monkeypatch.setattr(server, "_get_connection", lambda: con)
        mcp = server.build_server()
        listed = _tool_data(mcp, "list_sources", {})
        assert listed["fec_release"]["status_counts"] == {"disabled": len(specs)}
        for spec in specs:
            assert spec.view.name in listed["unavailable_tables"]
            described = _tool_data(mcp, "describe_table", {"table": spec.view.name})
            assert described["relationship"]["release_compatibility"]["status"] == "disabled"
            assert described["relationship"]["release_compatibility"]["reasons"]
        assert _tool_data(mcp, "query_sql", {"sql": "SELECT * FROM fec_receipts"})["rows"] == [{"id": 1}]


def test_refresh_checks_configuration_when_index_unchanged_and_rollback_restores_matched_set(tmp_path, monkeypatch):
    specs, index, _, consumer, _ = configure(tmp_path, monkeypatch)
    old = connection(index)
    publication = server._Publication(index, None)
    monkeypatch.setattr(server, "_read_publication", lambda: publication)
    built = []

    def build(selected=None):
        assert selected == publication
        con = connection(index)
        built.append(con)
        return con

    monkeypatch.setattr(server, "_build_connection", build)
    assert server._refreshed(old) is old
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", digest("wrong-image"))
    incompatible = server._refreshed(old)
    assert incompatible is not old
    name = specs[0].view.name
    assert server._connection_relationships(incompatible)[name]["release_compatibility"]["status"] == "disabled"
    assert old.execute(f"SELECT * FROM {name}").fetchall() == [(1,)]
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", consumer["image_digest"])
    restored = server._refreshed(incompatible)
    assert restored.execute(f"SELECT * FROM {name}").fetchall() == [(1,)]
    assert server._refreshed(restored) is restored
    for con in [old, *built]:
        con.close()


def test_application_sql_cannot_hide_unlisted_dependency_or_write_statement(tmp_path):
    specs, index, receipt, consumer = fixture(tmp_path)
    con = duckdb.connect()
    for name in ["fec_receipts", "fec_reports", "members"]:
        con.execute(f"CREATE TABLE {name}(id INTEGER)")
    for sql in ["SELECT a.id FROM fec_receipts a, members b, fec_reports c", "DROP TABLE members"]:
        modified = replace(specs[0], view=replace(specs[0].view, query=lambda _, sql=sql: sql))
        # Even a matching synthetic receipt cannot enlarge the trusted declaration.
        receipt["views"][modified.view.name]["sql_sha256"] = release.sha256(sql.encode())
        config = capture(tmp_path, (modified,), receipt, consumer)
        result = release.install_views(con, (modified,), config, index, ["fec_receipts", "fec_reports", "members"], {},
                                       read_tables=server._tables_named)
        assert result[modified.view.name]["release_compatibility"]["status"] == "disabled"
        assert con.execute("SELECT count(*) FROM members").fetchone() == (0,)
    con.close()


def test_receipt_cannot_register_extra_sql_or_unknown_view(tmp_path, monkeypatch):
    specs, index, receipt, _, path = configure(tmp_path, monkeypatch)
    receipt["views"]["unregistered"] = {**deepcopy(receipt["views"][specs[0].view.name]), "sql": "SELECT * FROM members"}
    raw = json.dumps(receipt).encode()
    path.write_bytes(raw)
    monkeypatch.setenv("SPICY_REGS_FEC_RELEASE_SHA256", release.sha256(raw))
    with connection(index) as con:
        assert "unregistered" not in server._connection_relationships(con)
        with pytest.raises(duckdb.CatalogException):
            con.execute("SELECT * FROM unregistered")
        assert con.execute(f"SELECT * FROM {specs[1].view.name}").fetchall() == [(1,)]


def test_runtime_measurement_failure_disables_qualified_views_but_preserves_raw_access(tmp_path, monkeypatch):
    specs, index, _, _, _ = configure(tmp_path, monkeypatch)

    def unreadable(_):
        raise OSError("installation unreadable")

    monkeypatch.setattr(release, "runtime_consumer", unreadable)
    with connection(index) as con:
        state = server._connection_relationships(con)[specs[0].view.name]
        assert "consumer_measurement_unavailable" in state["reason"]
        assert con.execute("SELECT * FROM fec_receipts").fetchall() == [(1,)]


def test_refresh_parent_change_and_partial_rollback_wait_for_all_pins(tmp_path, monkeypatch):
    specs, index, _, consumer, _ = configure(tmp_path, monkeypatch)
    old = connection(index)
    live = deepcopy(index)
    live["families"]["members"]["artifactDigest"] = digest("advanced-parent")
    monkeypatch.setattr(server, "_read_publication", lambda: server._Publication(live, None))

    def build(selected=None):
        assert selected is not None
        return connection(selected.index)

    monkeypatch.setattr(server, "_build_connection", build)
    advanced = server._refreshed(old)
    name = specs[0].view.name
    assert server._connection_relationships(advanced)[name]["release_compatibility"]["status"] == "disabled"
    # Data rollback alone cannot admit a mismatched running image.
    live = index
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", digest("wrong-image"))
    intermediate = server._refreshed(advanced)
    assert server._connection_relationships(intermediate)[name]["release_compatibility"]["status"] == "disabled"
    monkeypatch.setenv("SPICY_REGS_CONSUMER_IMAGE_DIGEST", consumer["image_digest"])
    restored = server._refreshed(intermediate)
    assert restored.execute(f"SELECT * FROM {name}").fetchall() == [(1,)]
    assert old.execute(f"SELECT * FROM {name}").fetchall() == [(1,)]
    for con in [old, advanced, intermediate, restored]:
        con.close()
