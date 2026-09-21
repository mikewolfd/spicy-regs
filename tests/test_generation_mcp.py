"""Serve actual local Parquet behind captured immutable remote URLs."""

import json
import re

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.sources import publication as pub
from tests.generation_fakes import Store
from tests.test_generation_publication import build, publish


def connection_fixture(tmp_path, monkeypatch, index, locations):
    inner = duckdb.connect()
    seen = []
    closed = []

    class Connection:
        def execute(self, sql, parameters=None):
            if sql.startswith(("INSTALL ", "LOAD ", "SET ")):
                return self
            if sql.startswith("CREATE VIEW "):
                [url] = re.findall(r"read_parquet\('([^']+)'\)", sql)
                seen.append(url)
                key = url.removeprefix(mcp_server.R2_BASE_URL + "/")
                if key not in locations:
                    raise duckdb.IOException("missing object")
                sql = sql.replace(url, str(locations[key]))
            return inner.execute(sql, parameters) if parameters is not None else inner.execute(sql)

        def close(self):
            closed.append(True)
            inner.close()

    monkeypatch.setattr(mcp_server.duckdb, "connect", lambda: Connection())
    monkeypatch.setattr(pub, "load_index", lambda url: index)
    monkeypatch.setattr(mcp_server, "_apply_security_settings", lambda con: None)
    monkeypatch.setattr(mcp_server, "TABLES", ("a", "b", "legacy", "missing"))
    return inner, seen, closed


def test_views_and_status_use_one_captured_generation(tmp_path, monkeypatch):
    directory, _ = build(tmp_path)
    index = publish(Store(), directory)
    legacy = tmp_path / "legacy.parquet"
    pq.write_table(pa.table({"id": ["legacy"]}), legacy)
    locations = {pub.table_location(index, key)[0]: directory / key for key in ("a.parquet", "b.parquet")}
    locations["legacy.parquet"] = legacy
    inner, seen, closed = connection_fixture(tmp_path, monkeypatch, index, locations)
    mcp_server._build_connection()
    assert not closed
    status = mcp_server._publication_status(inner.cursor())
    assert status["tables"] == ["a", "b", "legacy"]
    assert status["publication"]["legacy"] == {"status": "legacy_unversioned"}
    assert status["publication"]["a"]["artifact_digest"] == index["families"]["test"]["artifactDigest"]
    assert inner.execute("SELECT * FROM a JOIN b USING (id)").fetchall() == [("one",)]
    assert seen[:2] == [
        mcp_server.R2_BASE_URL + "/" + pub.table_location(index, key)[0] for key in ("a.parquet", "b.parquet")
    ]
    inner.close()


@pytest.mark.parametrize("bad", ["missing", "schema"])
def test_missing_or_wrong_schema_managed_member_refuses_connection(tmp_path, monkeypatch, bad):
    directory, _ = build(tmp_path)
    index = publish(Store(), directory)
    if bad == "schema":
        index = json.loads(json.dumps(index))
        index["families"]["test"]["tables"]["a.parquet"]["columns"] = [["wrong", "VARCHAR"]]
    locations = {} if bad == "missing" else {pub.table_location(index, "a.parquet")[0]: directory / "a.parquet"}
    _, _, closed = connection_fixture(tmp_path, monkeypatch, index, locations)
    with pytest.raises(RuntimeError, match="Published"):
        mcp_server._build_connection()
    assert closed == [True]
