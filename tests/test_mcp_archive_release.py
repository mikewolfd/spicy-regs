"""The MCP's archive binding (L2-A4): scoped admission, list_row, verification, the metadata cache and the tools."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import pytest

from spicy_regs import archive_release
from spicy_regs import mcp_server as server
from spicy_regs.sources import publication

#: One meeting (event 112474) with two documents and a witness, the shape hearing_documents and witness_documents read.
FIXTURE_ROWS = {
    "list": "SELECT * FROM (VALUES ('a.pdf', ['112474'], ['w1']), ('b.pdf', ['112474'], []), ('c.pdf', ['999'], []))"
            " t(filename, meeting_keys, witness_keys)",
    "detail": "SELECT * FROM (VALUES ('\\x01'::BLOB, 'CHRG-1'), ('\\x02'::BLOB, NULL), ('\\x03'::BLOB, NULL)) t(link_key, package_id)",
    "links": "SELECT '\\x01'::BLOB AS link_key, 0::BIGINT AS list_row",
    "urls": "SELECT '\\x01'::BLOB AS lookup_key, 0::BIGINT AS list_row",
    "sources": "SELECT 0::BIGINT AS list_row, 0 AS ordinal, 'https://docs.house.gov/a.pdf' AS url",
    "recoveries": "SELECT 0::BIGINT AS list_row, 0 AS ordinal",
    "meetings": "SELECT * FROM (VALUES (112474::BIGINT, 'House', 'Hearing'), (999::BIGINT, NULL, 'Other')) t(event_id, chamber, title)",
    "witnesses": "SELECT 'w1' AS witness_key, 112474::BIGINT AS event_id, 'A Witness' AS name, 'CHRG-1' AS package_id",
    "recordings": "SELECT 'yt:x' AS video_key, [112474::BIGINT] AS event_ids",
}
RECIPES: list[dict[str, Any]] = [
    {"id": "hearing_documents", "title": "Documents for a meeting", "requires": ["list", "meetings"],
     "sql": "SELECT l.list_row,l.filename,m.event_id FROM \"list\" l JOIN meetings m "
            "ON list_contains(l.meeting_keys,CAST(m.event_id AS VARCHAR)) WHERE m.event_id=? ORDER BY l.list_row"},
    {"id": "committee_roster", "title": "Committee seats", "requires": ["rosters", "committees", "members"],
     "sql": "SELECT 1"},
]


def write_release(root: Path) -> str:
    """A complete release under ``root/releases/<id>/`` whose id the consumer recomputes; returns that id."""
    staging = root / "staging"
    staging.mkdir(parents=True)
    entries, schema_files = [], []
    with duckdb.connect() as con:
        for stem, sql in FIXTURE_ROWS.items():
            path = staging / f"{stem}.parquet"
            con.execute(f"COPY ({sql}) TO '{path}' (FORMAT PARQUET)")
            columns = [row[0] for row in con.execute(f"DESCRIBE {sql}").fetchall()]
            [(rows,)] = con.execute(f"SELECT count(*) FROM ({sql})").fetchall()
            entry = {"name": f"{stem}.parquet", "columns": [{"name": name, "type": "x", "meaning": f"{stem} {name}"}
                                                            for name in columns],
                     "purpose": f"{stem} purpose", "row_meaning": f"one {stem}"}
            if stem in ("list", "detail"):
                entry["virtual_columns"] = [{"name": "list_row", "type": "int64", "meaning": "row position"}]
            schema_files.append(entry)
            entries.append((path, rows))
    schema = {"schema_version": 1, "files": schema_files, "recipes": RECIPES,
              "relationships": [{"id": "document_detail", "source": {"table": "list", "fields": ["list_row"]},
                                 "target": {"table": "detail", "fields": ["list_row"]}},
                                {"id": "meeting_votes", "source": {"table": "meetings", "fields": ["event_id"]},
                                 "target": {"external": "spicy-regs roll-call-votes"}},
                                {"id": "roster_member", "source": {"table": "rosters", "fields": ["bioguide_id"]},
                                 "target": {"table": "members", "fields": ["bioguide_id"]}}]}
    (staging / "schema.json").write_text(json.dumps(schema))
    entries.append((staging / "schema.json", None))
    files = [{"name": path.name, "sha256": hashlib.sha256(path.read_bytes()).hexdigest(), "size": path.stat().st_size,
              "rows": rows, "row_range": None, "shard": None, "producer": "archive"} for path, rows in entries]
    release_id = archive_release.release_identity(1, files)
    manifest = {"schema_version": 1, "release_id": release_id, "files": files}
    (staging / "manifest.json").write_text(json.dumps(manifest))
    target = root / "releases" / release_id
    target.parent.mkdir(parents=True)
    staging.rename(target)
    return release_id


@pytest.fixture
def release(tmp_path, monkeypatch):
    """A fixture release, pinned as ``bound`` and served from a local origin through the remote reader."""
    origin = tmp_path / "archive"
    release_id = write_release(origin)
    monkeypatch.setattr(archive_release, "ORIGIN", str(origin))
    monkeypatch.setattr(archive_release, "load_pins", lambda: {"version": 1, "updated_at": "x", "bound": release_id,
                                                               "releases": [release_id]})
    monkeypatch.setattr(server, "DATA_DIR", None)
    monkeypatch.setattr(server, "TABLES", ())
    monkeypatch.setattr(server, "R2_BASE_URL", str(tmp_path / "r2"))
    monkeypatch.setattr(server, "_resolve_catalog_config", lambda: None)
    monkeypatch.setattr(server, "load_public_http", lambda *args: None)
    return origin, release_id


def build(requested=None):
    token = server._requested_relations.set(requested)
    try:
        return server._build_connection(server._Publication(publication.empty_index(), None))
    finally:
        server._requested_relations.reset(token)


def registered(con):
    return {row[0] for row in con.execute("SELECT table_name FROM information_schema.tables").fetchall()}


def test_unscoped_build_binds_every_view_with_list_row_and_runs_the_published_recipe(release):
    con = build()
    try:
        assert set(archive_release.VIEWS) <= registered(con)
        assert con.execute("SELECT list_row, filename FROM archive_list ORDER BY list_row").fetchall() == [
            (0, "a.pdf"), (1, "b.pdf"), (2, "c.pdf")]
        assert con.execute("SELECT list_row, package_id FROM archive_detail WHERE package_id IS NOT NULL").fetchall() == [
            (0, "CHRG-1")]
        recipe = RECIPES[0]["sql"].replace('"list"', "archive_list").replace("meetings m", "archive_meetings m")
        assert con.execute(recipe, [112474]).fetchall() == [(0, "a.pdf", 112474), (1, "b.pdf", 112474)]
    finally:
        con.close()


def test_a_query_naming_no_archive_view_reads_nothing_from_the_archive(release, monkeypatch):
    def refuse(url):
        raise AssertionError(f"archive read for a non-archive query: {url}")

    monkeypatch.setattr(archive_release, "_get", refuse)
    con = build({"etl_receipts"})
    try:
        assert not registered(con) & {*archive_release.VIEWS, "_spicy_archive"}
    finally:
        con.close()


def test_a_query_naming_one_archive_view_admits_only_that_file(release):
    con = build({"archive_meetings"})
    try:
        assert registered(con) & set(archive_release.VIEWS) == {"archive_meetings"}
        assert con.execute("SELECT count(*) FROM archive_meetings").fetchone() == (2,)
        with pytest.raises(duckdb.Error):
            con.execute(f"SELECT * FROM read_parquet('{release[0]}/releases/{release[1]}/list.parquet')")
    finally:
        con.close()


def test_manifest_and_schema_are_read_once_per_process(release, monkeypatch):
    reads = []
    get = archive_release._get
    monkeypatch.setattr(archive_release, "_get", lambda url: reads.append(url) or get(url))
    for _ in range(2):
        build({"archive_meetings"}).close()
    assert [url.rsplit("/", 1)[1] for url in reads] == ["manifest.json", "schema.json"]


def test_a_file_missing_a_published_column_refuses_the_query_that_names_it(tmp_path, release, monkeypatch):
    origin = tmp_path / "other"

    def drop_title(target):
        with duckdb.connect() as con:
            con.execute(f"COPY (SELECT event_id, chamber FROM '{target}/meetings.parquet') TO '{target}/m.parquet' (FORMAT PARQUET)")
        (target / "m.parquet").replace(target / "meetings.parquet")

    # The manifest pins the rewritten bytes, so only the column check can catch the change.
    release_id = write_release(origin)
    drop_title(origin / "releases" / release_id)
    entries = json.loads((origin / "releases" / release_id / "manifest.json").read_text())["files"]
    meetings = next(item for item in entries if item["name"] == "meetings.parquet")
    data = (origin / "releases" / release_id / "meetings.parquet").read_bytes()
    meetings.update(sha256=hashlib.sha256(data).hexdigest(), size=len(data))
    new_id = archive_release.release_identity(1, entries)
    (origin / "releases" / release_id).rename(origin / "releases" / new_id)
    (origin / "releases" / new_id / "manifest.json").write_text(
        json.dumps({"schema_version": 1, "release_id": new_id, "files": entries}))
    monkeypatch.setattr(archive_release, "ORIGIN", str(origin))
    monkeypatch.setattr(archive_release, "load_pins", lambda: {"bound": new_id})
    with pytest.raises(RuntimeError, match="archive_meetings"):
        build({"archive_meetings"})


def test_an_unreachable_archive_is_reported_unavailable_in_discovery_and_refuses_a_named_query(release, monkeypatch):
    def down(url):
        raise archive_release.ArchiveReleaseError("down")

    monkeypatch.setattr(archive_release, "_get", down)
    con = build()
    try:
        record = server._pinned_record(con.cursor(), "_spicy_archive")
        assert record is not None
        assert record["status"] == "unavailable" and record["release_id"] == release[1]
        assert not registered(con) & set(archive_release.VIEWS)
    finally:
        con.close()
    with pytest.raises(RuntimeError, match="Archive release .* unavailable"):
        build({"archive_list"})


def test_a_pruned_file_under_a_cached_manifest_degrades_discovery_but_refuses_a_named_query(release):
    origin, release_id = release
    build().close()  # caches the manifest and schema for the process, as a long-running server does
    (origin / "releases" / release_id / "recordings.parquet").unlink()
    con = build()
    try:
        record = server._pinned_record(con.cursor(), "_spicy_archive")
        assert record is not None and record["status"] == "unavailable" and "archive_recordings" in record["reason"]
        assert not registered(con) & set(archive_release.VIEWS)
    finally:
        con.close()
    with pytest.raises(RuntimeError, match="archive_recordings"):
        build({"archive_recordings"})


def test_local_copy_is_hashed_and_a_corrupt_byte_refuses(release):
    origin, release_id = release
    assert archive_release.bind(release_id, str(origin), hash_files=True).hashed
    path = origin / "releases" / release_id / "witnesses.parquet"
    data = bytearray(path.read_bytes())
    data[len(data) // 2] ^= 1
    path.write_bytes(bytes(data))
    with pytest.raises(archive_release.ArchiveReleaseError, match="witnesses.parquet does not match"):
        archive_release.bind(release_id, str(origin), hash_files=True)


def test_local_data_dir_without_a_copy_never_reads_the_network(tmp_path, monkeypatch):
    monkeypatch.setattr(server, "DATA_DIR", tmp_path)
    monkeypatch.setattr(archive_release, "_get", lambda url: pytest.fail(f"read {url}"))
    binding, record = server._bind_archive(None)
    assert binding is None and record["status"] == "unavailable"


def test_local_data_dir_copy_binds_hashed(release, monkeypatch):
    origin, release_id = release
    monkeypatch.setattr(server, "DATA_DIR", origin.parent)
    binding, record = server._bind_archive(None)
    assert binding.hashed and record["trust"] == archive_release.LOCAL_TRUST


def test_archive_urls_are_owned_immutable_inputs_so_the_metadata_cache_stays_on(release, monkeypatch):
    origin, release_id = release
    monkeypatch.setattr(server, "R2_BASE_URL", "https://fixture.invalid")
    binding = archive_release.bind(release_id, str(origin), hash_files=False)._replace(origin="https://archive.invalid")
    urls = [binding.url(stem) for stem in binding.files]
    index = publication.empty_index()
    assert server._immutable_metadata_inputs(index, None, urls) is None
    assert server._immutable_metadata_inputs(index, None, urls, binding) == dict.fromkeys(urls, "archive_release")
    assert server._immutable_metadata_inputs(index, None, [*urls, "https://archive.invalid/other"], binding) is None


def test_tools_describe_the_release_and_pin_query_replies(release, monkeypatch):
    con = build()
    monkeypatch.setattr(server, "_get_connection", lambda: con)
    tools: Any = server.build_server()

    def call(name, arguments):
        return asyncio.run(tools.call_tool(name, arguments)).structured_content

    try:
        listed = call("list_sources", {"subject": "archive"})["archive"]
        assert listed["release_id"] == release[1] and listed["status"] == "bound"
        assert {item["id"]: item["available_here"] for item in listed["recipes"]} == {
            "hearing_documents": True, "committee_roster": False}
        assert {item["table"] for item in listed["views"] if item["available"]} == set(archive_release.VIEWS)
        described = call("describe_table", {"table": "archive_list"})
        assert described["publication"]["rows"] == 3 and described["publication"]["trust"] == archive_release.REMOTE_TRUST
        assert [c["column_name"] for c in described["columns"]][-1] == "list_row"
        assert [item["id"] for item in described["relationships"]] == ["document_detail"]
        assert [item["id"] for item in described["recipes"]] == ["hearing_documents"]
        meetings = call("describe_table", {"table": "archive_meetings"})
        assert [(item["id"], item["available_here"]) for item in meetings["relationships"]] == [("meeting_votes", True)]
        reply = call("query_sql", {"sql": "SELECT count(*) AS n FROM archive_witnesses"})
        assert reply["rows"] == [[1]]
        assert reply["publication"]["archive_witnesses"]["status"] == "archive_release"
        assert reply["publication"]["archive_witnesses"]["release_id"] == release[1]
    finally:
        con.close()
