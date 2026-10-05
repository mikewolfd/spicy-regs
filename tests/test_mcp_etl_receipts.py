"""A native family keeps its exact processing evidence accessible through generic MCP."""

from copy import deepcopy
from hashlib import sha256
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.local_data import receipt_local_key, selection_record
from spicy_regs.sources import publication as pub
from tests.test_generation_mcp import connection_fixture
from tests.test_mcp_server import _listed, _records, _tool_data
from tests.test_publication_index_metadata import index


def retained(tmp_path):
    value = index()
    family = value["families"]["members"]
    source = tmp_path / "members.parquet"
    receipts = tmp_path / "etl_receipts.parquet"
    pq.write_table(pa.table({"bioguide_id": ["A000001"]}), source)
    pq.write_table(pa.table({
        "dataset": ["members", "scorecard_snapshots"],
        "generation_id": ["test-generation", "test-generation"],
        "outcome": ["accepted", "observed"],
        "processing_json": ['{"source_url":"https://publisher.test/score"}', '{"complete":true}'],
    }), receipts)
    family["tables"]["members.parquet"].update(
        sha256="sha256:" + sha256(source.read_bytes()).hexdigest(), byteSize=source.stat().st_size,
    )
    family["etlReceipts"] = dict(
        key="etl_receipts.parquet", sha256="sha256:" + sha256(receipts.read_bytes()).hexdigest(),
        byteSize=receipts.stat().st_size, rows=2,
        columns=[[field.name, "VARCHAR"] for field in pq.ParquetFile(receipts).schema_arrow],
        generationId="test-generation", datasets=["members", "scorecard_snapshots"],
    )
    return value, source, receipts


def test_receipt_pointer_is_admitted_without_breaking_legacy_readers(tmp_path):
    value, _, _ = retained(tmp_path)
    assert pub.parse_index(json.dumps(value).encode()) == value
    legacy = pub.derive_v1(value)
    assert "etlReceipts" not in legacy["families"]["members"]
    assert legacy["families"]["members"]["tables"] == value["families"]["members"]["tables"]
    assert pub.parse_index(json.dumps(legacy).encode()) == legacy
    assert pub.receipt_members(value)[0].path.endswith("/etl_receipts.parquet")


@pytest.mark.parametrize("damage", ["key", "hash", "columns", "duplicate-columns", "datasets", "extra", "owner"])
def test_untrusted_receipt_descriptor_is_refused(tmp_path, damage):
    value, _, _ = retained(tmp_path)
    receipt = value["families"]["members"]["etlReceipts"]
    if damage == "key":
        receipt["key"] = "../etl_receipts.parquet"
    elif damage == "hash":
        receipt["sha256"] = "bad"
    elif damage == "columns":
        receipt["columns"] = [["dataset", None]]
    elif damage == "duplicate-columns":
        receipt["columns"] *= 2
    elif damage == "datasets":
        receipt["datasets"] = ["members", "members"]
    elif damage == "extra":
        receipt["unknown"] = True
    else:
        other = deepcopy(value["families"]["members"])
        other["artifactDigest"] = "sha256:" + "c" * 64
        other["prefix"] = "generations/other/" + "c" * 64
        other["tables"] = {}
        value["families"]["other"] = other
    with pytest.raises(pub.PublicationError):
        pub.parse_index(json.dumps(value).encode())


def test_remote_generic_mcp_lists_queries_and_pins_processing_receipts(tmp_path, monkeypatch):
    value, source, receipts = retained(tmp_path)
    receipt_member = pub.receipt_members(value)[0]
    locations = {pub.single_member(value, "members.parquet").path: source, receipt_member.path: receipts}
    connection_fixture(monkeypatch, value, locations, tables=("members",))
    con = mcp_server._build_connection()
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    server = mcp_server.build_server()
    try:
        assert "etl_receipts" in {row["table"] for row in _listed(_tool_data(server, "list_sources", {}))}
        described = _tool_data(server, "describe_table", {"table": "etl_receipts"})
        assert described["available"]
        # The reply is the receipts page for a reader who asks the server: it carries the guide a table's note points at.
        assert "## What a receipt does not tell you" in described["metadata"]["guide"]
        pin = described["publication"]
        assert pin["status"] == "managed_receipts"
        assert pin["families"]["members"]["artifact_digest"] == value["families"]["members"]["artifactDigest"]
        assert pin["families"]["members"]["generation_id"] == "test-generation"
        result = _tool_data(server, "query_sql", {"sql": "SELECT processing_json FROM etl_receipts WHERE outcome='observed'"})
        assert _records(result) == [{"processing_json": '{"complete":true}'}]
        assert result["publication"]["etl_receipts"]["rows"] == 2
    finally:
        con.close()


@pytest.mark.parametrize("damage", ["missing", "schema"])
def test_remote_receipt_failure_refuses_entire_connection(tmp_path, monkeypatch, damage):
    value, source, receipts = retained(tmp_path)
    locations = {pub.single_member(value, "members.parquet").path: source}
    if damage == "schema":
        pq.write_table(pa.table({"wrong": ["schema"]}), receipts)
        locations[pub.receipt_members(value)[0].path] = receipts
    connection_fixture(monkeypatch, value, locations, tables=("members",))
    with pytest.raises(RuntimeError, match="ETL receipt member unavailable"):
        mcp_server._build_connection()


def downloaded(tmp_path, monkeypatch, value, receipts):
    """A complete local download of ``members`` holding its family's receipt member; returns that member's path."""
    member = next(m for m in pub.receipt_members(value) if m.path.startswith(value["families"]["members"]["prefix"]))
    local = tmp_path / receipt_local_key(member)
    local.parent.mkdir(parents=True)
    local.write_bytes(receipts.read_bytes())
    (tmp_path / "download.json").write_text(json.dumps(dict(
        version=1, status="complete", publication=value,
        selected={"members": selection_record(value, "members")},
    )))
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(mcp_server, "TABLES", ("members",))
    monkeypatch.setattr(pub, "load_index", lambda _: pytest.fail("Local receipts fetched a remote index"))
    return local


def test_local_receipts_are_rehashed_and_never_fetched_remotely(tmp_path, monkeypatch):
    value, source, receipts = retained(tmp_path)
    local = downloaded(tmp_path, monkeypatch, value, receipts)
    con = mcp_server._build_connection()
    assert con.execute("SELECT count(*) FROM etl_receipts").fetchone() == (2,)
    con.close()
    local.write_bytes(b"changed evidence")
    with pytest.raises(RuntimeError, match="receipt member differs"):
        mcp_server._build_connection()


def test_local_receipt_pin_and_schema_check_cover_only_selected_families(tmp_path, monkeypatch):
    value, source, receipts = retained(tmp_path)
    other = deepcopy(value["families"]["members"])
    other.update(prefix="generations/other/" + "c" * 64, logicalId="urn:test:other",
                 artifactDigest="sha256:" + "c" * 64,
                 tables={"other_rows.parquet": dict(value["families"]["members"]["tables"]["members.parquet"])})
    other["etlReceipts"].update(columns=[["unrelated", "VARCHAR"]], datasets=["other_rows"], generationId="other")
    value["families"]["other"] = other
    downloaded(tmp_path, monkeypatch, value, receipts)
    con = mcp_server._build_connection()
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    server = mcp_server.build_server()
    try:
        pin = _tool_data(server, "describe_table", {"table": "etl_receipts"})["publication"]
        assert pin["status"] == "managed_receipts"
        assert set(pin["families"]) == {"members"}
        listed = {row["table"]: row for row in _listed(_tool_data(server, "list_sources", {}))}
        assert listed["etl_receipts"]["rows"] == 2
    finally:
        con.close()
