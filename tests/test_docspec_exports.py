"""Boundary tests; the opt-in DocSpec fixture supplies the actual native oracle."""

import hashlib
import json
from copy import deepcopy
from pathlib import Path

import duckdb
import httpx
import pytest
from starlette.testclient import TestClient

from spicy_regs import docspec_exports, mcp_server
from tests.test_mcp_query_results import call


@pytest.fixture
def admitted(tmp_path, monkeypatch):
    directory = tmp_path / "selected"
    directory.mkdir()
    path = directory / "comments.parquet"
    with duckdb.connect() as con:
        con.execute("COPY (SELECT 1::BIGINT AS id, 'public' AS public) TO ? (FORMAT PARQUET)", [str(path)])
    manifest = tmp_path / "private-manifest.json"
    manifest.write_text("source metadata is not an SQL input")
    source = {"dataset": "comments", "version": "initial", "manifest": {
        "owner": "comments", "path": str(manifest), "digest": "b" * 64, "bytes": manifest.stat().st_size}}
    # The boundary fixture uses a synthetic hash response. Actual BLAKE3 and
    # native canonical pins are qualified by the opt-in Rust→MCP fixture.
    pin = {"format": 1, "id": "export", "dataset": "comments", "source": source,
           "object": {"owner": "comments", "path": "original.parquet",
                      "digest": hashlib.sha256(path.read_bytes()).hexdigest(), "bytes": path.stat().st_size}}
    release = {"dataset": {"dataset": "comments"}, "id": "initial", "manifest": source["manifest"]}
    document = {"version": 1, "metadata_url": "http://127.0.0.1:12345",
                "binary": str(Path(__file__).resolve()),
                "tables": {"comments": {"file": path.name, "export": pin}}}
    configuration = tmp_path / "bindings.json"
    configuration.write_text(json.dumps(document))
    reads, hashes = [], []
    records = {"/datasets/comments/exports/export": pin, "/datasets/comments/versions/initial": release}

    def respond(request):
        reads.append(request.url.path)
        return httpx.Response(200, json=records[request.url.path])

    original = httpx.Client
    monkeypatch.setattr(docspec_exports.httpx, "Client", lambda **options: original(transport=httpx.MockTransport(respond), **options))

    def observed_hash(binary, member):
        hashes.append(str(member))
        return {"digest": hashlib.sha256(member.read_bytes()).hexdigest(), "bytes": member.stat().st_size}

    monkeypatch.setattr(docspec_exports, "_hash", observed_hash)
    monkeypatch.setenv("SPICY_REGS_DOCSPEC_EXPORTS", str(configuration))
    monkeypatch.setattr(mcp_server, "DATA_DIR", directory)
    monkeypatch.setattr(mcp_server, "_CONNECTION_TTL_SECONDS", 0.0)
    mcp_server._reset_connection_cache()
    yield directory, configuration, document, records, reads, hashes, manifest
    mcp_server._reset_connection_cache()


def test_exact_pin_survives_ttl_without_rehash_or_catalog_lookup(admitted):
    directory, configuration, document, _, reads, hashes, manifest = admitted
    with TestClient(mcp_server.build_app()) as client:
        first = call(client, "query_sql", {"sql": "SELECT id,public FROM comments"})
        assert first["isError"] is False
        reply = first["structuredContent"]
        binding = reply["publication"]["comments"]
        assert binding["status"] == "docspec_immutable_export"
        assert binding["export"] == document["tables"]["comments"]["export"]
        assert binding["evidence"] == {"scope": "release", "version": "initial", "manifest_blake3": "b" * 64, "row_mapping": False}
        assert reply["rows"] == [[1, "public"]]
        retained = mcp_server._get_connection()
        configuration.write_text(json.dumps(document, indent=2))
        for _ in range(2):
            assert mcp_server._get_connection() is retained
            assert call(client, "query_sql", {"sql": "SELECT id FROM comments"})["isError"] is False
        assert (len(reads), len(hashes)) == (2, 1)
        refused = call(client, "query_sql", {"sql": f"SELECT content FROM read_text('{manifest}')"})
        assert refused["isError"] is True
        assert "disabled by configuration" in refused["content"][0]["text"]
        assert (len(reads), len(hashes)) == (2, 1)
    assert directory.joinpath("comments.parquet").is_file()


@pytest.mark.parametrize("failure", ["pin", "release", "bytes", "external"])
def test_wrong_pin_release_bytes_or_external_trust_refuses_before_query(admitted, failure):
    directory, configuration, document, records, reads, hashes, _ = admitted
    if failure == "pin":
        document = deepcopy(document)
        document["tables"]["comments"]["export"]["object"]["digest"] = "c" * 64
        configuration.write_text(json.dumps(document))
    elif failure == "release":
        records["/datasets/comments/versions/initial"] = {**records["/datasets/comments/versions/initial"], "manifest": {"digest": "c" * 64}}
    elif failure == "bytes":
        path = directory / "comments.parquet"
        original = path.read_bytes()
        path.write_bytes(b"X" + original[1:])
    else:
        document["metadata_url"] = "http://example.com:12345"
        configuration.write_text(json.dumps(document))
    with pytest.raises(RuntimeError, match="DocSpec"):
        mcp_server._build_connection()
    assert len(hashes) == (1 if failure == "bytes" else 0)
    if failure == "external":
        assert reads == []


def test_unmanaged_mode_remains_unversioned_without_binding(admitted, monkeypatch):
    _, _, _, _, reads, hashes, _ = admitted
    monkeypatch.delenv("SPICY_REGS_DOCSPEC_EXPORTS")
    con = mcp_server._build_connection()
    assert mcp_server._publication_status(con)["publication"]["comments"]["status"] == "local_unversioned"
    assert reads == hashes == []
    con.close()


@pytest.mark.parametrize("hardlink", [False, True])
def test_duplicate_local_member_refuses_before_admission(admitted, hardlink):
    directory, configuration, document, _, reads, hashes, _ = admitted
    document["tables"]["alias"] = deepcopy(document["tables"]["comments"])
    if hardlink:
        directory.joinpath("alias.parquet").hardlink_to(directory / "comments.parquet")
        document["tables"]["alias"]["file"] = "alias.parquet"
    configuration.write_text(json.dumps(document))
    with pytest.raises(RuntimeError, match="each local file once"):
        mcp_server._build_connection()
    assert reads == hashes == []


def test_hash_request_cancellation_kills_and_reaps_private_subprocess(tmp_path, monkeypatch):
    import threading
    import subprocess
    from time import monotonic
    from spicy_regs.runtime_bounds import RequestBound

    binary = tmp_path / "synthetic-binary"
    binary.write_bytes(b"synthetic")
    class Process:
        returncode = None
        killed = reaped = False
        def __enter__(self): return self
        def __exit__(self, *args): assert self.reaped
        def wait(self, timeout=None):
            if timeout is None:
                self.reaped = True
                self.returncode = -9
                return -9
            threading.Event().wait(timeout)
            raise subprocess.TimeoutExpired("synthetic", timeout)
        def kill(self): self.killed = True
    process = Process()
    monkeypatch.setattr(docspec_exports.subprocess, "Popen", lambda *a, **k: process)
    started = monotonic()
    with pytest.raises(TimeoutError, match="export cancelled"):
        with RequestBound(.08, "export cancelled").scope():
            docspec_exports._hash(binary, tmp_path / "selected.parquet")
    assert process.killed and process.reaped and monotonic() - started < 1
