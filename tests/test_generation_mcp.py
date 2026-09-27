"""Pins that MCP views and publication status read one captured generation, with local Parquet standing in for R2."""

import json
import re

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import data_dictionary as dd
from spicy_regs import mcp_server
from spicy_regs.ontology.common import write_parquet_rows
from spicy_regs.pipelines.rulemaking_dataset import RulemakingDatasetPipeline
from spicy_regs.sources import publication as pub
from spicy_regs.transforms.build_agency_lifecycle_stats import SCHEMA as AGENCY_LIFECYCLE_STATS_SCHEMA
from spicy_regs.transforms.build_comment_periods import COLUMNS as COMMENT_PERIOD_COLUMNS
from spicy_regs.transforms.build_lifecycles import EVENT_SCHEMA, LIFECYCLE_SCHEMA
from spicy_regs.transforms.build_proceedings import COLUMNS as PROCEEDING_COLUMNS
from spicy_regs.transforms.build_regulatory_agenda import ITEM_COLUMNS, RELATIONSHIP_COLUMNS
from spicy_regs.transforms.build_rule_targets import COLUMNS as RULE_TARGET_COLUMNS
from tests.generation_fakes import Store
from tests.test_generation_publication import build, publish


def serve_documents(monkeypatch, documents: dict) -> list[str]:
    """Answer the publication module's bounded GETs from ``documents`` (URL to JSON); returns every URL read."""
    reads = []

    def get(url, *, allow_missing, **_):
        reads.append(url)
        if url in documents:
            return json.dumps(documents[url]).encode()
        if allow_missing:
            return None
        raise pub.PublicationError(f"missing {url}")

    monkeypatch.setattr(pub, "_bounded_get", get)
    return reads


def connection_fixture(monkeypatch, index, locations, *, tables=("a", "b", "legacy", "missing"), documents=None):
    """Patch DuckDB connections that rewrite captured R2 URLs to ``locations``, with the index, pointer and table scope.

    Returns every connection built; each keeps the URLs its statements read. With no ``documents`` the rulemaking
    pointer answers 404, as it does before the dataset's first publication.
    """
    connect = duckdb.connect
    built = []

    class Connection:
        def __init__(self):
            self.inner, self.seen, self.closed = connect(), [], False
            built.append(self)

        def execute(self, sql, parameters=None):
            if sql.startswith(("INSTALL ", "LOAD ", "SET ")):
                return self
            for url in re.findall("'(" + re.escape(mcp_server.R2_BASE_URL) + "/[^']+)'", sql):
                self.seen.append(url)
                key = url.removeprefix(mcp_server.R2_BASE_URL + "/")
                if key not in locations:
                    raise duckdb.IOException("missing object")
                sql = sql.replace(url, str(locations[key]))
            return self.inner.execute(sql, parameters) if parameters is not None else self.inner.execute(sql)

        def cursor(self):
            return self.inner.cursor()

        def close(self):
            self.closed = True
            self.inner.close()

        def __enter__(self):
            return self

        def __exit__(self, *_):
            self.close()

    monkeypatch.setattr(mcp_server.duckdb, "connect", Connection)
    monkeypatch.setattr(pub, "load_index", lambda url: index)
    serve_documents(monkeypatch, documents or {})
    monkeypatch.setattr(mcp_server, "_apply_security_settings", lambda con, allowed_paths=None: None)
    monkeypatch.setattr(mcp_server, "TABLES", tables)
    return built


def test_views_and_status_use_one_captured_generation(tmp_path, monkeypatch):
    directory, _ = build(tmp_path)
    index = publish(Store(), directory)
    legacy = tmp_path / "legacy.parquet"
    pq.write_table(pa.table({"id": ["legacy"]}), legacy)
    locations = {pub.single_member(index, key).path: directory / key for key in ("a.parquet", "b.parquet")}
    locations["legacy.parquet"] = legacy
    built = connection_fixture(monkeypatch, index, locations)
    mcp_server._build_connection()
    [con] = built
    assert not con.closed
    status = mcp_server._publication_status(con.cursor())
    assert status["tables"] == ["a", "b", "legacy"]
    assert status["publication"]["legacy"] == {"status": "legacy_unversioned"}
    assert status["publication"]["a"]["artifact_digest"] == index["families"]["test"]["artifactDigest"]
    assert con.inner.execute("SELECT * FROM a JOIN b USING (id)").fetchall() == [("one",)]
    assert con.seen[:2] == [
        mcp_server.R2_BASE_URL + "/" + pub.single_member(index, key).path for key in ("a.parquet", "b.parquet")
    ]
    con.inner.close()


@pytest.mark.parametrize("bad", ["missing", "schema"])
def test_missing_or_wrong_schema_managed_member_refuses_connection(tmp_path, monkeypatch, bad):
    directory, _ = build(tmp_path)
    index = publish(Store(), directory)
    if bad == "schema":
        index = json.loads(json.dumps(index))
        index["families"]["test"]["tables"]["a.parquet"]["columns"] = [["wrong", "VARCHAR"]]
    locations = {} if bad == "missing" else {pub.single_member(index, "a.parquet").path: directory / "a.parquet"}
    built = connection_fixture(monkeypatch, index, locations)
    with pytest.raises(RuntimeError, match="Published"):
        mcp_server._build_connection()
    assert [con.closed for con in built] == [True]


def test_admitted_table_without_dictionary_is_available_but_helpers_are_not(tmp_path, monkeypatch):
    from tests.test_mcp_server import _tool_data

    directory, _ = build(tmp_path, keys=("extra.parquet",))
    index = publish(Store(), directory)
    inner = duckdb.connect()
    inner.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
    inner.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps(index)])
    inner.execute(f"CREATE VIEW extra AS SELECT * FROM read_parquet('{directory / 'extra.parquet'}')")
    inner.execute("CREATE TABLE unrelated_helper (id VARCHAR)")
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: inner)
    server = mcp_server.build_server()
    result = _tool_data(server, "list_sources", {})
    assert result["tables"] == ["extra"]
    assert result["publication"]["extra"]["status"] == "managed_generation"
    described = _tool_data(server, "describe_table", {"table": "extra"})
    assert described["available"] is True
    assert described["columns"][0]["column_name"] == "id"
    assert described["declared_columns"] == []
    assert described["schema_matches_declared"] is None
    inner.close()


# The materialized rulemaking dataset publishes under its own pointer, not the publication index.
SNAPSHOT_A = "snapshot_" + "a" * 32
SNAPSHOT_B = "snapshot_" + "b" * 32
#: Each rulemaking table's writer: an all-VARCHAR column list (``write_parquet_rows``) or its own Arrow schema.
WRITERS = {
    "rule_targets": RULE_TARGET_COLUMNS,
    "proceedings": PROCEEDING_COLUMNS,
    "regulatory_agenda_items": ITEM_COLUMNS,
    "agenda_item_proceedings": RELATIONSHIP_COLUMNS,
    "comment_periods": COMMENT_PERIOD_COLUMNS,
    "rulemaking_lifecycles": LIFECYCLE_SCHEMA,
    "lifecycle_events": EVENT_SCHEMA,
    "agency_lifecycle_stats": AGENCY_LIFECYCLE_STATS_SCHEMA,
}


def rulemaking_documents(snapshot_id: str, tables=tuple(WRITERS), **artifact) -> dict:
    """The pointer and manifest the pipeline publishes for ``tables``, by URL; ``artifact`` overrides every record."""
    prefix = f"materialized/rulemaking/snapshots/{snapshot_id}"
    pointer = {
        "format_version": 2,
        "dataset": "rulemaking",
        "snapshot_id": snapshot_id,
        "manifest_key": f"{prefix}/manifest.json",
    }
    records = {
        f"{table}.parquet": {
            "remote_key": f"{prefix}/{table}.parquet",
            "sha256": "0" * 64,
            "bytes": 1,
            "rows": 1,
            "visibility": "public",
            **artifact,
        }
        for table in tables
    }
    manifest = {"format_version": 2, "dataset": "rulemaking", "snapshot_id": snapshot_id, "artifacts": records}
    base = mcp_server.R2_BASE_URL
    return {f"{base}/{pub.SNAPSHOT_POINTER}": pointer, f"{base}/{prefix}/manifest.json": manifest}


def write_rulemaking(tmp_path, snapshot_id: str, marker: str) -> dict:
    """Every rulemaking table as its own writer writes it, ``rule_targets`` holding one docket named ``marker``.

    Returns the snapshot's object keys mapped to the files, for :func:`connection_fixture`.
    """
    directory = tmp_path / snapshot_id
    directory.mkdir()
    for table, shape in WRITERS.items():
        path = directory / f"{table}.parquet"
        if isinstance(shape, pa.Schema):
            pq.write_table(pa.Table.from_pylist([], schema=shape), path)
        else:
            write_parquet_rows(path, columns=shape, rows=[{"docket_id": marker}] if table == "rule_targets" else [])
    prefix = f"materialized/rulemaking/snapshots/{snapshot_id}"
    return {f"{prefix}/{table}.parquet": directory / f"{table}.parquet" for table in WRITERS}


def test_the_rulemaking_tables_are_the_pipelines_public_outputs():
    public = tuple(key.removesuffix(".parquet") for key in RulemakingDatasetPipeline.published_outputs)
    assert tuple(WRITERS) == dd.RULEMAKING_TABLES == public
    assert set(public) <= set(mcp_server.TABLES) & set(dd.TABLES) & dd.MCP_QUERYABLE


def test_rulemaking_views_read_the_pointers_snapshot_and_a_moved_pointer_at_the_next_build(tmp_path, monkeypatch):
    locations = {**write_rulemaking(tmp_path, SNAPSHOT_A, "EPA-A"), **write_rulemaking(tmp_path, SNAPSHOT_B, "EPA-B")}
    documents = rulemaking_documents(SNAPSHOT_A)
    built = connection_fixture(
        monkeypatch, pub.empty_index(), locations, tables=dd.RULEMAKING_TABLES, documents=documents
    )
    reads = serve_documents(monkeypatch, documents)
    monkeypatch.setattr(mcp_server, "_cached_connection", None)  # restored, with the TTL, after the test
    monkeypatch.setattr(mcp_server, "_cached_connection_at", 0.0)
    pointer = f"{mcp_server.R2_BASE_URL}/{pub.SNAPSHOT_POINTER}"

    assert mcp_server._get_connection() is mcp_server._get_connection()
    [first] = built
    assert reads.count(pointer) == 1  # once per connection build, not per call
    prefix = f"{mcp_server.R2_BASE_URL}/materialized/rulemaking/snapshots"
    assert first.seen == [f"{prefix}/{SNAPSHOT_A}/{table}.parquet" for table in dd.RULEMAKING_TABLES]

    documents.update(rulemaking_documents(SNAPSHOT_B))  # the publisher moves the pointer
    query = "SELECT docket_id FROM rule_targets"
    mcp_server._get_connection()
    assert len(built) == 1 and first.inner.execute(query).fetchall() == [("EPA-A",)]  # within the TTL
    monkeypatch.setattr(mcp_server, "_CONNECTION_TTL_SECONDS", 0.0)
    mcp_server._get_connection()
    [_, second] = built
    assert reads.count(pointer) == 2
    assert second.seen == [f"{prefix}/{SNAPSHOT_B}/{table}.parquet" for table in dd.RULEMAKING_TABLES]
    assert second.inner.execute(query).fetchall() == [("EPA-B",)]
    assert first.inner.execute(query).fetchall() == [("EPA-A",)]  # a connection keeps the snapshot it pinned
    status = mcp_server._publication_status(second.cursor())
    assert [name for name in status["tables"] if name in dd.RULEMAKING_TABLES] == list(dd.RULEMAKING_TABLES)
    assert all(status["publication"][name]["status"] == "derived_view"
               for name in status["tables"] if name not in dd.RULEMAKING_TABLES)
    assert status["publication"]["lifecycle_events"] == {
        "status": "rulemaking_snapshot",
        "snapshot_id": SNAPSHOT_B,
        "sha256": "sha256:" + "0" * 64,
    }
    assert "pinned by the pointer" in status["verification"]
    assert [con.closed for con in built] == [False, False]


def test_each_rulemaking_view_describes_as_the_dictionary_declares(tmp_path, monkeypatch):
    """The writers' own files, read through the views, carry the declared types, DATE, INTEGER and BOOLEAN included."""
    from tests.test_mcp_server import _tool_data

    locations = write_rulemaking(tmp_path, SNAPSHOT_A, "EPA-A")
    built = connection_fixture(
        monkeypatch,
        pub.empty_index(),
        locations,
        tables=dd.RULEMAKING_TABLES,
        documents=rulemaking_documents(SNAPSHOT_A),
    )
    mcp_server._build_connection()
    [con] = built
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    server = mcp_server.build_server()
    declared = dd.expected_schemas()
    mcp_server._parsed_pin.cache_clear()
    for table in dd.RULEMAKING_TABLES:
        described = _tool_data(server, "describe_table", {"table": table})
        assert described["schema_matches_declared"] is True, (table, described["schema_differences"])
        actual = [(column["column_name"], column["column_type"]) for column in described["columns"]]
        assert (
            actual == declared[table] == [(c["column_name"], c["column_type"]) for c in described["declared_columns"]]
        )
        assert described["publication"]["status"] == "rulemaking_snapshot"
    # Source selection, rulemaking selection and the relationship registry are each parsed once.
    assert mcp_server._parsed_pin.cache_info().misses == 3
    typed = dict(declared["rulemaking_lifecycles"])
    assert (typed["proposal_date"], typed["duration_days"], typed["pre_2008_coverage"]) == (
        "DATE",
        "INTEGER",
        "BOOLEAN",
    )
    assert {kind for _, kind in declared["agency_lifecycle_stats"]} == {"VARCHAR", "INTEGER", "BOOLEAN", "DATE"}
    assert {kind for table in dd.RULEMAKING_TABLES[:5] for _, kind in declared[table]} == {"VARCHAR"}


def test_an_unreadable_snapshot_member_refuses_the_connection(tmp_path, monkeypatch):
    locations = write_rulemaking(tmp_path, SNAPSHOT_A, "EPA-A")
    del locations[f"materialized/rulemaking/snapshots/{SNAPSHOT_A}/lifecycle_events.parquet"]
    built = connection_fixture(
        monkeypatch,
        pub.empty_index(),
        locations,
        tables=dd.RULEMAKING_TABLES,
        documents=rulemaking_documents(SNAPSHOT_A),
    )
    with pytest.raises(RuntimeError, match="member unavailable: lifecycle_events"):
        mcp_server._build_connection()
    assert [con.closed for con in built] == [True]


def test_no_published_pointer_leaves_the_rulemaking_tables_unavailable(tmp_path, monkeypatch):
    built = connection_fixture(monkeypatch, pub.empty_index(), {}, tables=dd.RULEMAKING_TABLES)
    mcp_server._build_connection()
    [con] = built
    assert mcp_server._publication_status(con.cursor())["tables"] == []
    assert con.seen == [f"{mcp_server.R2_BASE_URL}/{table}.parquet" for table in dd.RULEMAKING_TABLES]  # legacy keys


def _moved(documents: dict, key: str, change) -> dict:
    """``documents`` with ``change`` applied to a copy of the pointer or manifest."""
    documents = json.loads(json.dumps(documents))
    [url] = [url for url in documents if url.endswith(key)]
    change(documents[url])
    return documents


def _artifact(field: str, value):
    return lambda manifest: manifest["artifacts"]["rule_targets.parquet"].__setitem__(field, value)


@pytest.mark.parametrize(
    "change",
    [
        pytest.param(
            ("latest.json", lambda p: p.__setitem__("manifest_key", "materialized/other/manifest.json")),
            id="pointer names another manifest",
        ),
        pytest.param(("latest.json", lambda p: p.__setitem__("format_version", 3)), id="unreadable pointer format"),
        pytest.param(("latest.json", lambda p: p.__setitem__("dataset", "other")), id="another dataset's pointer"),
        pytest.param(("manifest.json", lambda m: m.__setitem__("format_version", 1)), id="manifest of another format"),
        pytest.param(("manifest.json", lambda m: m.__setitem__("dataset", "other")), id="manifest of another dataset"),
        pytest.param(
            ("manifest.json", lambda m: m.__setitem__("snapshot_id", SNAPSHOT_B)), id="manifest names another"
        ),
        pytest.param(
            ("manifest.json", _artifact("remote_key", "rule_targets.parquet")), id="artifact outside snapshot"
        ),
        pytest.param(
            ("manifest.json", _artifact("remote_key", "x') UNION SELECT 1 --")), id="artifact key with a quote"
        ),
    ],
)
def test_an_inconsistent_or_unsafe_snapshot_refuses(monkeypatch, change):
    key, edit = change
    serve_documents(monkeypatch, _moved(rulemaking_documents(SNAPSHOT_A), key, edit))
    with pytest.raises(pub.PublicationError):
        pub.load_rulemaking_snapshot(mcp_server.R2_BASE_URL)


@pytest.mark.parametrize("snapshot_id", ["x') UNION SELECT 1 --", "a/../../b", "Snapshot_A", "snapshot_a\n"])
def test_a_self_consistent_snapshot_with_an_unsafe_id_refuses(monkeypatch, snapshot_id):
    """Pointer, manifest and keys agree with one another, so only the id's own grammar can refuse it."""
    serve_documents(monkeypatch, rulemaking_documents(snapshot_id))
    with pytest.raises(pub.PublicationError):
        pub.load_rulemaking_snapshot(mcp_server.R2_BASE_URL)


@pytest.mark.parametrize("table", ['a" AS SELECT 1; --', "a'b", "../publication", "Rule_Targets"])
def test_a_self_consistent_public_artifact_with_an_unsafe_name_refuses(monkeypatch, table):
    """The key sits at its own name under the snapshot, so only the table-name grammar can refuse it."""
    serve_documents(monkeypatch, rulemaking_documents(SNAPSHOT_A, tables=(table,)))
    with pytest.raises(pub.PublicationError):
        pub.load_rulemaking_snapshot(mcp_server.R2_BASE_URL)


def test_the_index_wins_over_the_snapshot_for_a_table_both_name(tmp_path, monkeypatch):
    """A managed generation keeps its table; the snapshot serves the rest, declared or not."""
    directory, _ = build(tmp_path)
    index = publish(Store(), directory)
    rulemaking = write_rulemaking(tmp_path, SNAPSHOT_A, "EPA-A")
    snapshot_key = f"materialized/rulemaking/snapshots/{SNAPSHOT_A}/rule_targets.parquet"
    locations = {pub.single_member(index, key).path: directory / key for key in ("a.parquet", "b.parquet")}
    locations[snapshot_key] = rulemaking[snapshot_key]
    documents = rulemaking_documents(SNAPSHOT_A, tables=("a", "rule_targets"))
    built = connection_fixture(monkeypatch, index, locations, tables=("a",), documents=documents)
    mcp_server._build_connection()
    [con] = built
    base = mcp_server.R2_BASE_URL
    assert con.seen == [
        f"{base}/{pub.single_member(index, 'a.parquet').path}",
        f"{base}/{pub.single_member(index, 'b.parquet').path}",
        f"{base}/{snapshot_key}",
    ]
    status = mcp_server._publication_status(con.cursor())
    assert status["tables"] == ["a", "b", "rule_targets"]  # rule_targets is not in TABLES here
    assert status["publication"]["a"]["status"] == "managed_generation"
    assert status["publication"]["rule_targets"]["status"] == "rulemaking_snapshot"
    urls = pub.published_urls(base)
    assert (urls["a"], urls["rule_targets"]) == (
        [f"{base}/{pub.single_member(index, 'a.parquet').path}"],
        [f"{base}/{snapshot_key}"],
    )


def test_dictionary_discovery_reads_the_rulemaking_tables_through_the_pointer(tmp_path, monkeypatch):
    """``check --source r2`` DESCRIBEs each snapshot table at the URL the pointer names, typed as its writer wrote it."""
    locations = write_rulemaking(tmp_path, SNAPSHOT_A, "EPA-A")
    connection_fixture(monkeypatch, pub.empty_index(), locations, documents=rulemaking_documents(SNAPSHOT_A))
    with pytest.raises(dd.SchemaDiscoveryError) as error:  # the legacy base objects are not served here
        dd.discover_schemas("r2", mcp_server.R2_BASE_URL)
    expected = dd.expected_schemas()
    assert error.value.schemas == {table: expected[table] for table in dd.RULEMAKING_TABLES}
    assert all(f"[{name}]" in str(error.value) for name in ("dockets", "documents", "comments", "comments_index"))


def test_only_artifacts_the_manifest_marks_public_are_served(monkeypatch):
    documents = rulemaking_documents(SNAPSHOT_A)
    documents = _moved(documents, "manifest.json", _artifact("visibility", "internal"))
    documents = _moved(documents, "manifest.json", lambda m: m["artifacts"]["proceedings.parquet"].pop("visibility"))
    serve_documents(monkeypatch, documents)
    snapshot = pub.load_rulemaking_snapshot(mcp_server.R2_BASE_URL + "/")
    assert snapshot is not None and snapshot["snapshot_id"] == SNAPSHOT_A
    assert set(snapshot["tables"]) == {f"{table}.parquet" for table in dd.RULEMAKING_TABLES[2:]}
    serve_documents(monkeypatch, {})
    assert pub.load_rulemaking_snapshot(mcp_server.R2_BASE_URL) is None
