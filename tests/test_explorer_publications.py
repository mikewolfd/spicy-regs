"""Separate publications must bind real schemas to exact current files."""
from copy import deepcopy
import io
import json
import re

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import explorer_publications as publications
from spicy_regs.explorer_metadata import build_bundle

BASE = "https://data.example.org"


@pytest.fixture
def file():
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": ["x"], "count": pa.array([1], type=pa.int32())}), buffer)
    raw = buffer.getvalue()
    return raw, {"path": "comments.parquet", "byteSize": len(raw), "rows": 1,
                 "etag": '"one"', "sha256": "sha256:" + "a" * 64}


def client_for(raw, *, change=None):
    seen = []
    def request(req):
        seen.append((req.method, req.headers.get("range")))
        headers = {"etag": '"one"', "content-length": str(len(raw))}
        status, body = 200, b""
        if req.method == "GET":
            assert req.headers["if-match"] == '"one"'
            match = re.fullmatch(r"bytes=(\d+)-(\d+)", req.headers["range"])
            assert match is not None
            start, end = map(int, match.groups())
            body = raw[start:end + 1]
            status = 206
            headers |= {"content-range": f"bytes {start}-{end}/{len(raw)}", "content-length": str(len(body))}
        if change:
            status, headers, body = change(req, seen, status, headers, body)
        return httpx.Response(status, headers=headers, content=body)
    return httpx.Client(transport=httpx.MockTransport(request)), seen


def test_footer_reads_only_metadata_and_observes_actual_schema(file):
    raw, member = file
    client, seen = client_for(raw)
    assert publications.parquet_schema(client, BASE, member) == [["id", "VARCHAR"], ["count", "INTEGER"]]
    assert [method for method, _ in seen] == ["HEAD", "GET", "GET", "HEAD"]
    assert all(byte_range for method, byte_range in seen if method == "GET")


@pytest.mark.parametrize("fault", ["size", "version", "ignored-range", "wrong-range", "truncated", "changed"])
def test_footer_rejects_wrong_or_changing_objects(file, fault):
    raw, member = file
    def change(req, seen, status, headers, body):
        if fault == "size" and req.method == "HEAD":
            headers["content-length"] = "999"
        if fault == "version" or fault == "changed" and len(seen) == 4:
            headers["etag"] = '"two"'
        if req.method == "GET":
            if fault == "ignored-range":
                status, body = 200, raw
            if fault == "wrong-range":
                headers["content-range"] = "bytes 0-7/999"
            if fault == "truncated":
                body = body[:-1]
        return status, headers, body
    client, _ = client_for(raw, change=change)
    with pytest.raises(ValueError):
        publications.parquet_schema(client, BASE, member)


def test_footer_rows_and_limit_are_checked(file, monkeypatch):
    raw, member = file
    client, _ = client_for(raw)
    with pytest.raises(ValueError, match="rows differ"):
        publications.parquet_schema(client, BASE, {**member, "rows": 2})
    monkeypatch.setattr(publications, "FOOTER_LIMIT", 1)
    with pytest.raises(ValueError, match="reader limit"):
        publications.parquet_schema(client, BASE, member)


def empty_index():
    return {"format": "spicy-regs-publication", "version": 2, "families": {}}


def identity(member):
    return {"kind": "comments", "family": "comments", "members": [member]}


def cached_bundle(extra):
    return {"format": "spicy-regs-explorer-metadata", "version": 1, "extra_tables": extra,
            "tables": {name: {key: item[key] for key in ("publicationSchema", "publicationIdentity", "family")}
                       for name, item in extra.items()}}


def test_exact_cached_schema_skips_footer_but_checks_mutable_version(file, monkeypatch):
    raw, member = file
    client, seen = client_for(raw)
    identities = {"comments": identity(member)}
    first = publications._describe(identities, BASE, {}, client)
    seen.clear()
    monkeypatch.setattr(publications, "parquet_schema", lambda *_: pytest.fail("Unchanged files must not be rescanned"))
    assert publications._describe(identities, BASE, cached_bundle(first), client) == first
    assert seen == [("HEAD", None)]


def test_immutable_cache_reuse_and_changed_identity_invalidation(file, monkeypatch):
    _, member = file
    immutable: dict = {"kind": "rulemaking", "family": "rulemaking", "snapshotId": "snapshot_a",
                 "members": [{k: v for k, v in member.items() if k != "etag"}]}
    calls = []
    monkeypatch.setattr(publications, "parquet_schema", lambda *args: calls.append(args) or [["id", "VARCHAR"]])
    first = publications._describe({"records": immutable}, BASE, {}, None)
    assert len(calls) == 1
    publications._describe({"records": immutable}, BASE, cached_bundle(first), None)
    assert len(calls) == 1
    changed = deepcopy(immutable)
    changed["members"][0]["sha256"] = "sha256:" + "b" * 64
    publications._describe({"records": changed}, BASE, cached_bundle(first), None)
    assert len(calls) == 2


@pytest.mark.parametrize("cache", [{"extra_tables": None}, {"extra_tables": []},
                                  {"extra_tables": {"records": []}}, {"extra_tables": {"records": None}}])
def test_malformed_cache_is_rebuilt_without_inventing_membership(file, monkeypatch, cache):
    _, member = file
    calls = []
    monkeypatch.setattr(publications, "parquet_schema", lambda *a: calls.append(a) or [["id", "VARCHAR"]])
    result = publications._describe({"records": identity(member)}, BASE, cache, None)
    assert set(result) == {"records"}
    assert len(calls) == 1


@pytest.mark.parametrize("fault", ["format", "version", "schema", "identity", "family"])
def test_inconsistent_cached_schema_is_a_miss(file, monkeypatch, fault):
    _, member = file
    calls = []
    monkeypatch.setattr(publications, "parquet_schema", lambda *a: calls.append(a) or [["id", "VARCHAR"]])
    first = publications._describe({"records": identity(member)}, BASE, {}, None)
    previous = cached_bundle(first)
    if fault in ("format", "version"):
        previous[fault] = "unknown"
    else:
        key = {"schema": "publicationSchema", "identity": "publicationIdentity", "family": "family"}[fault]
        previous["tables"]["records"][key] = "unknown"
    result = publications._describe({"records": identity(member)}, BASE, previous, None)
    assert result["records"]["publicationSchema"] == [["id", "VARCHAR"]]
    assert len(calls) == 2


def test_separate_catalog_membership_is_not_borrowed_from_metadata(file, monkeypatch):
    _, member = file
    snapshot = {"snapshot_id": "snapshot_a", "tables": {
        "records.parquet": {"remote_key": "materialized/rulemaking/snapshots/snapshot_a/records.parquet",
                            "bytes": member["byteSize"], "rows": 1, "sha256": "a" * 64}}}
    monkeypatch.setattr(publications, "load_rulemaking_snapshot", lambda *a, **k: snapshot)
    monkeypatch.setattr(publications, "load_comments_publication", lambda *a, **k: None)
    monkeypatch.setattr(publications, "parquet_schema", lambda *a: [["id", "VARCHAR"]])
    extra = publications.other_tables(empty_index(), BASE, client=object(), previous={"extra_tables": {"ghost": {}}})
    assert set(extra) == {"records"}
    data = empty_index()
    data["families"] = {"new-owner": {"tables": {"records.parquet": {"columns": [["new_id", "VARCHAR"]]}}}}
    assert publications.other_tables(data, BASE, client=object()) == {}


def test_separate_table_enables_composite_join_without_changing_main_identity(file, monkeypatch):
    _, member = file
    monkeypatch.setattr(publications, "parquet_schema", lambda *a: [["id", "VARCHAR"], ["edition", "VARCHAR"]])
    extra = publications._describe({"comments": identity(member)}, BASE, {}, None)
    index = empty_index()
    index["families"] = {"dockets": {"artifactDigest": "sha256:" + "c" * 64,
                                     "tables": {"dockets.parquet": {"columns": [["id", "VARCHAR"], ["edition", "VARCHAR"]]}}}}
    declaration = {"child": "comments", "child_columns": ["id", "edition"], "parent": "dockets",
                   "parent_columns": ["id", "edition"], "kind": "unmeasured"}
    bundle = build_bundle(index, extra_tables=extra, descriptions={"comments": {}, "dockets": {}},
                          registry={"sources": {}}, join_record={"joins": [declaration]}, audit={})
    assert bundle["joins"] == [declaration]
    assert bundle["omittedJoins"] == []
    assert bundle["publication"]["families"] == {"dockets": "sha256:" + "c" * 64}
    assert bundle["tables"]["comments"]["publicationIdentity"] == extra["comments"]["publicationIdentity"]
    extra["comments"]["descriptor"]["members"][0]["rows"] = 2
    with pytest.raises(ValueError, match="identity"):
        build_bundle(index, extra_tables=extra)


def test_shared_publication_loaders_accept_authoritative_reader(file):
    from spicy_regs.sources.publication import load_comments_publication, load_rulemaking_snapshot
    _, member = file
    pointer = {"format_version": 2, "dataset": "rulemaking", "snapshot_id": "snapshot_a",
               "manifest_key": "materialized/rulemaking/snapshots/snapshot_a/manifest.json"}
    manifest = {**pointer, "artifacts": {"records.parquet": {
        "visibility": "public", "remote_key": "materialized/rulemaking/snapshots/snapshot_a/records.parquet"}}}
    receipt = {"format_version": 1, "source": {"table_uuid": "uuid", "snapshot_id": 1, "schema_id": 1},
               "files": {name + ".parquet": {"sha256": member["sha256"][7:], "etag": member["etag"],
                                                "rows": member["rows"], "bytes": member["byteSize"]}
                         for name in ("comments", "comments_index")}}
    records = {"materialized/rulemaking/latest.json": pointer, pointer["manifest_key"]: manifest,
               "comments-publication.json": receipt}
    def read(url, **kwargs):
        return json.dumps(records[url.removeprefix(BASE + "/")]).encode()
    snapshot = load_rulemaking_snapshot(BASE, read=read)
    comments = load_comments_publication(BASE, read=read)
    assert snapshot is not None and comments is not None
    assert snapshot["snapshot_id"] == "snapshot_a"
    assert comments["receipt"] == receipt
