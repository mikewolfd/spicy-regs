"""Behavioral catalog tests using retained-format fixtures and isolated outputs."""

import csv
import gzip
import hashlib
import json

import pytest

from spicy_regs.source_catalog import build, export, validate_public
from spicy_regs.source_catalog.storage import Inputs, connect


def write(root, name, value):
    path = root / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value))
    return path


@pytest.fixture
def corpus(tmp_path):
    root = tmp_path / "corpus"
    root.mkdir()
    write(
        root,
        "catalog.json",
        {
            "families": {
                "test": {
                    "tables": ["test.parquet"],
                    "current_generation": "sha256:output",
                    "source_artifacts": ["sha256:artifact"],
                    "baselines": ["baseline"],
                }
            },
            "lineage": [
                {"family": "test", "digest": "sha256:output", "inputs": [{"artifactDigest": "sha256:artifact"}]}
            ],
            "missing_historical_roots": [{"family": "test", "digest": "sha256:missing"}],
        },
    )
    for name, fields, rows in [
        (
            "sources.tsv",
            ["kind", "name", "identity", "status", "path"],
            [
                {
                    "kind": "source-evidence",
                    "name": "test",
                    "identity": "sha256:artifact",
                    "status": "bytes-collected",
                    "path": "source-evidence/artifact",
                }
            ],
        ),
        (
            "table-inputs.tsv",
            ["table", "family", "parent_tables", "baseline_paths"],
            [
                {
                    "table": "test.parquet",
                    "family": "test",
                    "parent_tables": "parent.parquet",
                    "baseline_paths": "baseline",
                }
            ],
        ),
    ]:
        with (root / name).open("w") as stream:
            writer = csv.DictWriter(stream, fieldnames=fields, delimiter="\t")
            writer.writeheader()
            writer.writerows(rows)
    body = b"original decoded bytes"
    sha = hashlib.sha256(body).hexdigest()
    stored = root / "source-evidence/blobs/sha256" / (sha + ".gz")
    stored.parent.mkdir(parents=True)
    stored.write_bytes(gzip.compress(body))
    write(
        root,
        "collections/unintegrated/files.json",
        {
            "id": "unintegrated",
            "files": [
                {
                    "path": "original-a.txt",
                    "object": "source-evidence/blobs/sha256/" + sha,
                    "sha256": sha,
                    "bytes": len(body),
                },
                {
                    "path": "original-b.txt",
                    "object": "source-evidence/blobs/sha256/" + sha,
                    "sha256": sha,
                    "bytes": len(body),
                },
                {"path": "gone.txt", "status": "verified", "sha256": "b" * 64, "bytes": 4},
            ],
        },
    )
    write(
        root,
        "source-evidence/artifact/artifact.json",
        {"artifactDigest": "sha256:artifact", "memberManifests": [{"objectKey": "members.json"}]},
    )
    write(
        root,
        "source-evidence/artifact/members.json",
        {
            "members": [
                {"objectKey": "same.txt", "sha256": sha, "byteSize": len(body)},
                {"objectKey": "same.txt", "sha256": sha, "byteSize": len(body)},
            ]
        },
    )
    return root


def test_rebuild_preserves_aliases_dedup_and_missing(corpus, tmp_path):
    first, second = tmp_path / "first.sqlite", tmp_path / "second.sqlite"
    config = write(tmp_path, "config.json", {"collections": [], "routes": []})
    one = build(corpus, first, config=config)
    two = build(corpus, second, config=config)
    assert one["generation"] == two["generation"]
    db = connect(first, readonly=True)
    assert db.execute("SELECT count(*) FROM files").fetchone()[0] == 6
    assert db.execute("SELECT count(*) FROM objects").fetchone()[0] == 2
    assert db.execute("SELECT count(*) FROM files WHERE status='missing-bytes'").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM files WHERE status='receipt-only'").fetchone()[0] == 1
    assert db.execute("SELECT count(*) FROM files WHERE name='same.txt'").fetchone()[0] == 2
    db.close()
    manifest = export(first, tmp_path / "public", page_size=1)
    internal = export(first, tmp_path / "internal", internal=True, page_size=1)
    assert manifest["generation"] == internal["generation"]
    assert manifest["totals"]["objects"] == 1
    assert manifest["totals"]["aliases"] == 6
    assert internal["audience"] == "internal"
    for path in (tmp_path / "public").rglob("*.json"):
        validate_public(json.loads(path.read_text()))
        assert str(corpus) not in path.read_text()
    assert any("localPath" in path.read_text() for path in (tmp_path / "internal").rglob("*.json"))


def test_failed_pin_validation_preserves_active_index(corpus, tmp_path, monkeypatch):
    output = tmp_path / "active.sqlite"
    config = write(tmp_path, "config.json", {})
    build(corpus, output, config=config)
    before = output.read_bytes()

    def changed(self):
        raise RuntimeError("Input changed during build")

    monkeypatch.setattr(Inputs, "check", changed)
    with pytest.raises(RuntimeError, match="changed"):
        build(corpus, output, config=config)
    assert output.read_bytes() == before
    assert json.loads(output.with_suffix(".sqlite.reconciliation.json").read_text())["status"] == "failed"
    assert not list(tmp_path.glob("*.tmp"))


def test_rebuild_scope_exports_current_sources_and_keeps_saved_inventory(corpus, tmp_path):
    config = write(tmp_path, "scope.json", {
        "catalog_scope": {
            "format": "spicy-source-scope", "version": 1,
            "collections": [{"id": "corpus:test", "disposition": "active", "reason": "Published test family."}],
            "excluded_objects": [],
        },
    })
    first, second = tmp_path / "scoped.sqlite", tmp_path / "rebuilt.sqlite"
    one = build(corpus, first, config=config)
    two = build(corpus, second, config=config)
    assert one["generation"] == two["generation"]
    with connect(first, readonly=True) as db:
        active = {r[0] for r in db.execute("SELECT id FROM files")}
        assert {r[0] for r in db.execute("SELECT id FROM collections")} == {"corpus:test"}
    with connect(first, readonly=True, scope="retained") as db:
        retained = {r[0] for r in db.execute("SELECT id FROM files")}
    with connect(first, readonly=True, scope="all") as db:
        assert active | retained == {r[0] for r in db.execute("SELECT id FROM files")}
        assert len(active) + len(retained) == 6
    manifest = export(first, tmp_path / "scoped-public", page_size=2)
    assert manifest["totals"]["aliases"] == len(active)
    assert [c["id"] for c in manifest["collections"]] == ["corpus:test"]


def test_discovery_unknown_and_failed_scope_survive(corpus, tmp_path):
    discovery = write(
        tmp_path,
        "discovery.json",
        {
            "format": "spicy-source-discovery",
            "version": 1,
            "collections": [
                {"id": "publisher:dataset", "title": "Advertised only", "raw": {"secret": "/Users/private"}}
            ],
            "routes": [
                {
                    "id": "route",
                    "collection_id": "publisher:dataset",
                    "access_url": "https://publisher.test/api?api_key=secret",
                }
            ],
            "items": [{"id": "item", "collection_id": "publisher:dataset", "name": "advertised.zip"}],
            "observations": [
                {
                    "id": "obs",
                    "collection_id": "publisher:dataset",
                    "status": "request-failed",
                    "scope": {"query": "2020"},
                }
            ],
        },
    )
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}), discovery=[discovery])
    manifest = export(output, tmp_path / "public")
    summary = next(c for c in manifest["collections"] if c["id"] == "publisher:dataset")
    assert "known-not-acquired" in summary["statuses"]
    assert "request-failed" in summary["statuses"]
    detail = json.loads((tmp_path / "public" / summary["detailPath"]).read_text())
    assert "accessUrl" not in detail["routes"][0]
    assert detail["observations"][0]["status"] == "request-failed"
    assert "raw" not in json.dumps(detail)


def test_privacy_guard_rejects_internal_fields_and_secrets():
    for value in [{"localPath": "x"}, {"name": "/Users/private"}, {"url": "https://example.test/?token=secret"}]:
        with pytest.raises(ValueError):
            validate_public(value)


def test_public_bulk_listing_preserves_scope_without_allowing_credentials():
    from spicy_regs.source_catalog.exporter import public_url

    url = "https://com-courtlistener-storage.s3-us-west-2.amazonaws.com/list.html?prefix=bulk-data/"
    assert public_url(url) == url
    assert public_url(url + "&token=private") is None
    assert public_url(url.replace("bulk-data/", "other/")) is None
    assert public_url(url.replace("com-courtlistener-storage", "other-bucket")) is None


def test_native_blob_reference_and_requested_period(corpus, tmp_path):
    existing = json.loads((corpus / "collections/unintegrated/files.json").read_text())["files"][0]
    write(
        corpus,
        "source-native/native/artifact.json",
        {
            "artifactDigest": "sha256:native",
            "spec": {"family": "test", "sourceSystemId": "publisher-api"},
            "memberManifests": [{"objectKey": "native.json"}],
        },
    )
    write(
        corpus,
        "source-native/native/native.json",
        {
            "members": [
                {
                    "blobRef": "sha256:" + existing["sha256"],
                    "byteSize": existing["bytes"],
                    "role": "source-native-records",
                }
            ]
        },
    )
    scope = corpus / "source-native/native/records/scopes.jsonl"
    scope.parent.mkdir(parents=True)
    scope.write_text(
        json.dumps({"scopeId": "day", "fields": {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-13"}})
        + "\n"
    )
    output = tmp_path / "index.sqlite"
    build(
        corpus,
        output,
        config=write(
            tmp_path,
            "config.json",
            {
                "collections": [{"id": "mapped", "title": "Mapped"}],
                "routes": [{"id": "exact-route", "collection_id": "mapped"}],
                "source_routes": {"publisher-api": "exact-route"},
            },
        ),
    )
    db = connect(output, readonly=True)
    row = db.execute("SELECT status FROM files WHERE name LIKE 'sha256:%' AND collection_id='mapped'").fetchone()
    assert row[0] != "missing-bytes"
    period = json.loads(db.execute("SELECT data FROM evidence WHERE kind='periods'").fetchone()[0])
    assert period["basis"] == "requested-scope"
    assert period["start"] == "2026-04-13"
    assert period["route_id"] == "exact-route"
    assert db.execute("SELECT route_id FROM files WHERE collection_id='mapped'").fetchone()[0] == "exact-route"
    db.close()


def test_discovery_versions_and_observation_fanout(corpus, tmp_path):
    envelope = {
        "format": "spicy-source-discovery",
        "version": 1,
        "collections": [{"id": "a", "title": "A"}, {"id": "b", "title": "B"}],
        "items": [{"id": "same", "version_id": "v1", "collection_id": "a", "status": "advertised"}],
        "observations": [{"id": "obs", "collection_ids": ["a", "b"], "status": "partial-enumeration"}],
    }
    first = write(tmp_path, "d1.json", envelope)
    envelope["items"][0]["version_id"] = "v2"
    second = write(tmp_path, "d2.json", envelope)
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}), discovery=[first, second])
    db = connect(output, readonly=True)
    assert db.execute("SELECT count(*) FROM files WHERE collection_id='a'").fetchone()[0] == 2
    assert db.execute("SELECT count(*) FROM evidence WHERE kind='observations'").fetchone()[0] == 2
    assert db.execute("SELECT DISTINCT status FROM files WHERE collection_id='a'").fetchone()[0] == "known-not-acquired"
    db.close()


def test_export_collision_preserves_active_manifest(corpus, tmp_path):
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}))
    root = tmp_path / "public"
    manifest = export(output, root, page_size=1)
    before = (root / "manifest.json").read_bytes()
    detail = root / manifest["collections"][0]["detailPath"]
    detail.write_text('{"bad":"existing immutable partition"}')
    with pytest.raises(ValueError, match="collision"):
        export(output, root, page_size=1)
    assert (root / "manifest.json").read_bytes() == before


@pytest.mark.parametrize(
    "body",
    [
        '{"nested":{"members":[{"wrong":true}]}}',
        '{"members":[{"a":1}{"b":2}]}',
        '{"members":[{"a":1},]}',
        '{"members":[{"a":1}]} garbage',
        '{"members":[{"a":1}]',
    ],
)
def test_streaming_manifest_refuses_malformed_root(tmp_path, body):
    from spicy_regs.source_catalog.storage import array_items

    path = tmp_path / "bad.json"
    path.write_text(body)
    with pytest.raises(ValueError):
        list(array_items(Inputs(tmp_path), path, "members"))


def test_streaming_manifest_reads_top_level_only(tmp_path):
    from spicy_regs.source_catalog.storage import array_items

    path = write(tmp_path, "members.json", {"nested": {"members": ["wrong"]}, "members": [{"right": 1}], "tail": 3})
    assert list(array_items(Inputs(tmp_path), path, "members")) == [{"right": 1}]


@pytest.mark.parametrize(
    "path", ["/tmp/private.txt", "/Volumes/private.txt", "/opt/private.txt", "C:\\private\\x", "\\\\server\\secret"]
)
def test_privacy_guard_rejects_absolute_local_paths(path):
    with pytest.raises(ValueError):
        validate_public({"name": path})


def test_possession_changes_catalog_generation(corpus, tmp_path):
    output = tmp_path / "index.sqlite"
    config = write(tmp_path, "config.json", {})
    before = build(corpus, output, config=config)
    next((corpus / "source-evidence/blobs/sha256").glob("*.gz")).unlink()
    after = build(corpus, output, config=config)
    assert before["generation"] != after["generation"]
    db = connect(output, readonly=True)
    assert db.execute("SELECT count(*) FROM files WHERE status='missing-bytes'").fetchone()[0] == 5
    db.close()


def test_observation_raw_document_stored_once(corpus, tmp_path):
    raw = {"large": "x" * 100000}
    discovery = write(
        tmp_path,
        "discovery.json",
        {
            "format": "spicy-source-discovery",
            "version": 1,
            "collections": [{"id": "a"}, {"id": "b"}],
            "observations": [{"id": "obs", "collection_ids": ["a", "b"], "status": "partial-enumeration", "raw": raw}],
        },
    )
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}), discovery=[discovery])
    db = connect(output, readonly=True)
    snapshots = list(db.execute("SELECT data FROM evidence WHERE kind='discovery_snapshot'"))
    assert len(snapshots) == 1
    assert json.loads(snapshots[0][0])["raw"] == raw
    for row in db.execute("SELECT data FROM evidence WHERE kind='observations'"):
        value = json.loads(row[0])
        assert "raw" not in value
        assert value["observation_ref"] == "obs"
    db.close()


def test_unassigned_inputs_have_public_destination_reason(corpus, tmp_path):
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}))
    manifest = export(output, tmp_path / "public")
    collection = next(row for row in manifest["collections"] if row["id"] == "corpus:unintegrated")
    detail = json.loads((tmp_path / "public" / collection["detailPath"]).read_text())
    assert detail["planned"][0]["status"] == "unassigned"
    assert detail["planned"][0]["reason"]
    assert "table" not in detail["planned"][0]


def test_errors_visible_without_fabricating_dataset(corpus, tmp_path):
    discovery = write(
        tmp_path,
        "d.json",
        {
            "format": "spicy-source-discovery",
            "version": 1,
            "observations": [{"id": "failure", "status": "parse-error", "scope": "catalog index"}],
            "errors": [
                {
                    "observation_id": "failure",
                    "path": "dataset[0]",
                    "message": "Malformed dataset metadata",
                    "raw": {"secret": "/tmp/private"},
                }
            ],
        },
    )
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}), discovery=[discovery])
    manifest = export(output, tmp_path / "public")
    collection = next(row for row in manifest["collections"] if row["id"] == "inventory-observation:failure")
    assert collection["title"] == "Unresolved inventory observation"
    assert "does not establish that a dataset exists" in collection["description"]
    assert collection["holdings"]["objects"] == 0
    detail = json.loads((tmp_path / "public" / collection["detailPath"]).read_text())
    assert any(
        row["note"] == "Malformed dataset metadata" and row["status"] == "parse-error"
        for row in detail["observations"]
        if row.get("note")
    )
    assert any(row["id"] == "failure" for row in detail["observations"])


def test_publisher_name_and_associated_error_public_display(corpus, tmp_path):
    discovery = write(
        tmp_path,
        "d.json",
        {
            "format": "spicy-source-discovery",
            "version": 1,
            "collections": [
                {"id": "publisher:a", "title": "A", "publisher": {"name": "Publisher name", "internal": "/tmp/private"}}
            ],
            "observations": [{"id": "obs", "collection_ids": ["publisher:a"], "status": "partial-enumeration"}],
            "errors": [{"observation_id": "obs", "message": "One distribution has no URL"}],
        },
    )
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}), discovery=[discovery])
    manifest = export(output, tmp_path / "public")
    collection = next(row for row in manifest["collections"] if row["id"] == "publisher:a")
    assert collection["publisher"] == "Publisher name"
    detail = json.loads((tmp_path / "public" / collection["detailPath"]).read_text())
    assert any(
        row.get("note") == "One distribution has no URL" and row["status"] == "partial-enumeration"
        for row in detail["observations"]
    )


def test_possession_does_not_assert_integration_absence(corpus, tmp_path):
    path = corpus / "collections/unintegrated/files.json"
    manifest = json.loads(path.read_text())
    manifest["files"][0]["status"] = "verified"
    path.write_text(json.dumps(manifest))
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}))
    db = connect(output, readonly=True)
    assert db.execute("SELECT DISTINCT status FROM files WHERE name='same.txt'").fetchone()[0] == "held"
    assert db.execute("SELECT status FROM files WHERE name='original-a.txt'").fetchone()[0] == "verified"
    assert db.execute("SELECT status FROM files WHERE name='gone.txt'").fetchone()[0] == "missing-bytes"
    db.close()
    exported = export(output, tmp_path / "public")
    collection = next(row for row in exported["collections"] if row["id"] == "corpus:unintegrated")
    assert "integration-unresolved" in collection["statuses"]
    assert "held-not-integrated" not in collection["statuses"]


def test_explicit_not_integrated_source_status_is_preserved(corpus, tmp_path):
    path = corpus / "collections/unintegrated/files.json"
    manifest = json.loads(path.read_text())
    manifest["status"] = "held-not-integrated"
    path.write_text(json.dumps(manifest))
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}))
    exported = export(output, tmp_path / "public")
    collection = next(row for row in exported["collections"] if row["id"] == "corpus:unintegrated")
    assert "held-not-integrated" in collection["statuses"]
    assert "integration-unresolved" not in collection["statuses"]


def test_single_target_raw_observation_does_not_collide_with_snapshot(corpus, tmp_path):
    discovery = write(
        tmp_path,
        "d.json",
        {
            "format": "spicy-source-discovery",
            "version": 1,
            "collections": [{"id": "publisher:a", "title": "A"}],
            "observations": [
                {
                    "id": "obs",
                    "collection_ids": ["publisher:a"],
                    "status": "partial-enumeration",
                    "raw": {"whole": "catalog"},
                }
            ],
        },
    )
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}), discovery=[discovery])
    db = connect(output, readonly=True)
    snapshots = list(db.execute("SELECT id,data FROM evidence WHERE kind='discovery_snapshot'"))
    assert len(snapshots) == 1
    assert snapshots[0]["id"].startswith("snapshot:")
    assert json.loads(snapshots[0]["data"])["id"] == "obs"
    local = db.execute("SELECT data FROM evidence WHERE id='obs' AND collection_id='publisher:a'").fetchone()
    assert local is not None
    assert json.loads(local[0])["observation_ref"] == "obs"
    db.close()
    manifest = export(output, tmp_path / "public")
    collection = next(row for row in manifest["collections"] if row["id"] == "publisher:a")
    detail = json.loads((tmp_path / "public" / collection["detailPath"]).read_text())
    assert detail["observations"][0]["id"] == "obs"


def test_directory_inventory_summary_is_evidence_not_file(corpus, tmp_path):
    directory = corpus / "rebuild-seeds/test/generation"
    directory.mkdir(parents=True)
    baseline = []
    for name in ["test.parquet", "etl_receipts.parquet"]:
        path = directory / name
        body = name.encode()
        path.write_bytes(body)
        baseline.append(
            {
                "family": "test",
                "relative_path": str(path.relative_to(corpus)),
                "sha256": hashlib.sha256(body).hexdigest(),
                "bytes": len(body),
            }
        )
    write(corpus, "metadata/baseline-files.json", baseline)
    with (corpus / "sources.tsv").open("a") as stream:
        stream.write("retained-baseline\ttest\tsha256:generation\tbytes-collected\trebuild-seeds/test/generation\n")
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}))
    db = connect(output, readonly=True)
    assert db.execute("SELECT count(*) FROM files WHERE name='rebuild-seeds/test/generation'").fetchone()[0] == 0
    assert db.execute("SELECT count(*) FROM files WHERE name LIKE 'rebuild-seeds/test/generation/%'").fetchone()[0] == 2
    summary = [json.loads(row[0]) for row in db.execute("SELECT data FROM evidence WHERE kind='source_inventory'")]
    assert any(row["kind"] == "retained-baseline" and row["path"] == "rebuild-seeds/test/generation" for row in summary)
    db.close()


def test_retained_labels_preserve_ids_and_reviewed_titles(corpus, tmp_path):
    path = corpus / "collections/unintegrated/files.json"
    manifest = json.loads(path.read_text())
    manifest["label"] = "Descriptive retained collection label"
    path.write_text(json.dumps(manifest))
    output = tmp_path / "index.sqlite"
    config = write(tmp_path, "config.json", {})
    build(corpus, output, config=config)
    db = connect(output, readonly=True)
    row = db.execute("SELECT id,title FROM collections WHERE id='corpus:unintegrated'").fetchone()
    assert row["title"] == "Descriptive retained collection label"
    assert row["id"] == "corpus:unintegrated"
    db.close()
    write(
        tmp_path,
        "config.json",
        {"collections": [{"id": "reviewed", "title": "Reviewed source title", "source_names": ["unintegrated"]}]},
    )
    build(corpus, output, config=config)
    db = connect(output, readonly=True)
    assert db.execute("SELECT title FROM collections WHERE id='reviewed'").fetchone()[0] == "Reviewed source title"
    db.close()


def test_matrix_aggregates_only_assigned_route_counts(corpus, tmp_path, monkeypatch, capsys):
    from spicy_regs.source_catalog.__main__ import main

    discovery = write(
        tmp_path,
        "d.json",
        {
            "format": "spicy-source-discovery",
            "version": 1,
            "collections": [{"id": "a"}, {"id": "b"}],
            "routes": [
                {"id": "route-a", "collection_id": "a"},
                {"id": "route-b", "collection_id": "a"},
                {"id": "route-empty", "collection_id": "a"},
                {"id": "other-collection", "collection_id": "b"},
            ],
            "items": [
                {"id": "a1", "collection_id": "a", "route_id": "route-a", "status": "advertised"},
                {"id": "a2", "collection_id": "a", "route_id": "route-a", "status": "receipt-only"},
                {"id": "b1", "collection_id": "a", "route_id": "route-b", "status": "advertised"},
                {"id": "unassigned", "collection_id": "a", "status": "advertised"},
                {"id": "other", "collection_id": "b", "route_id": "other-collection", "status": "advertised"},
            ],
        },
    )
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=write(tmp_path, "config.json", {}), discovery=[discovery])
    monkeypatch.setattr("sys.argv", ["source-catalog", "matrix", "--index", str(output), "--collection", "a"])
    main()
    result = json.loads(capsys.readouterr().out)
    inventories = {row["route_id"]: row["inventory"] for row in result}
    assert inventories == {
        "route-a": {"known-not-acquired": 1, "receipt-only": 1},
        "route-b": {"known-not-acquired": 1},
        "route-empty": {},
    }
    assert all(row["collection_id"] == "a" for row in result)
    monkeypatch.setattr("sys.argv", ["source-catalog", "matrix", "--index", str(output)])
    main()
    result = json.loads(capsys.readouterr().out)
    assert next(row for row in result if row["route_id"] == "other-collection")["inventory"] == {
        "known-not-acquired": 1
    }


def test_matrix_preserves_scoped_relationships_and_cross_collection_policy(corpus, tmp_path, monkeypatch, capsys):
    from spicy_regs.source_catalog.__main__ import main

    relationship = {
        "id": "comparison",
        "collection_id": "source",
        "kind": "identity-overlap",
        "from_id": "bulk",
        "to_id": "api",
        "scope": "2024 committee IDs only",
        "note": "Field differences remain; no replacement or other-cycle equivalence.",
    }
    policy = {
        "id": "selection",
        "collection_id": "destination",
        "table": "history",
        "route_id": "bulk",
        "status": "preferred-source",
        "reason": "Cycle-specific source facts",
    }
    config = write(
        tmp_path,
        "config.json",
        {
            "routes": [{"id": route, "collection_id": "source"} for route in ["bulk", "api", "unknown"]],
            "relationships": [relationship],
            "planned": [policy],
        },
    )
    output = tmp_path / "index.sqlite"
    build(corpus, output, config=config)
    monkeypatch.setattr("sys.argv", ["source-catalog", "matrix", "--index", str(output), "--collection", "source"])
    main()
    rows = {row["route_id"]: row for row in json.loads(capsys.readouterr().out)}
    assert rows["bulk"]["relationships"] == [relationship]
    assert rows["api"]["relationships"] == [relationship]
    assert rows["bulk"]["selection_policies"] == [policy]
    assert rows["api"]["selection_policies"] == []
    assert rows["unknown"]["relationships"] == []
    assert rows["unknown"]["comparison"].startswith("unresolved")
    assert rows["bulk"]["period"]["kind"] == "unknown"


def test_inventory_overlays_select_metadata_keep_canonical_objects_and_pins(corpus, tmp_path):
    before = {name: (corpus / name).read_bytes() for name in ["catalog.json", "table-inputs.tsv", "sources.tsv"]}
    catalog = json.loads(before["catalog.json"])
    catalog["families"]["test"]["historical_baseline_notes"] = [{"table": "legacy.parquet"}]
    catalog_path = write(tmp_path, "overlay/catalog.json", catalog)
    table_path = tmp_path / "overlay/table-inputs.tsv"
    table_path.write_text("table\tfamily\tbaseline_paths\nlegacy.parquet\ttest\tretained-baseline\n")
    config = write(tmp_path, "config.json", {})
    output = tmp_path / "candidate.sqlite"
    result = build(corpus, output, config=config, catalog_input=catalog_path, table_inputs_input=table_path)
    assert (
        result["inputs"][str(catalog_path.resolve())]["sha256"]
        == "sha256:" + hashlib.sha256(catalog_path.read_bytes()).hexdigest()
    )
    assert (
        result["inputs"][str(table_path.resolve())]["sha256"]
        == "sha256:" + hashlib.sha256(table_path.read_bytes()).hexdigest()
    )
    assert str(corpus / "catalog.json") not in result["inputs"]
    assert str(corpus / "table-inputs.tsv") not in result["inputs"]
    assert str(corpus / "sources.tsv") in result["inputs"]
    db = connect(output, readonly=True)
    tables = [json.loads(row[0]) for row in db.execute("SELECT data FROM evidence WHERE kind='table_input'")]
    assert [row["table"] for row in tables] == ["legacy.parquet"]
    assert db.execute("SELECT count(*) FROM files WHERE local_path LIKE ?", (str(corpus) + "/%",)).fetchone()[0] > 0
    assert db.execute("SELECT id FROM tables WHERE id='legacy'").fetchone() is None
    db.close()
    for name, data in before.items():
        assert (corpus / name).read_bytes() == data
    catalog["families"]["test"]["historical_baseline_notes"][0]["note"] = "new retained evidence"
    write(tmp_path, "overlay/catalog.json", catalog)
    other = build(
        corpus, tmp_path / "other.sqlite", config=config, catalog_input=catalog_path, table_inputs_input=table_path
    )
    assert other["generation"] != result["generation"]


@pytest.mark.parametrize(
    "value",
    [
        [],
        {},
        {"families": []},
        {"families": {"test": []}},
        {"families": {"test": {"tables": "bad"}}},
        {"families": {"test": {"source_artifacts": "bad"}}},
        {"families": {}, "version": 999},
        {"families": {}, "format": "unknown", "version": 2},
    ],
)
def test_malformed_catalog_overlay_preserves_existing_output(corpus, tmp_path, value):
    output = tmp_path / "active.sqlite"
    config = write(tmp_path, "config.json", {})
    build(corpus, output, config=config)
    before = output.read_bytes()
    overlay = write(tmp_path, "bad.json", value)
    with pytest.raises(ValueError):
        build(corpus, output, config=config, catalog_input=overlay)
    assert output.read_bytes() == before
    assert json.loads(output.with_suffix(".sqlite.reconciliation.json").read_text())["status"] == "failed"


@pytest.mark.parametrize(
    "text",
    [
        "table\ttable\tfamily\na\tb\ttest\n",
        "table\tfamily\t\na\ttest\t\n",
        "table\na\n",
        "table\tfamily\na\n",
        "table\tfamily\na\ttest\textra\n",
        "table\tfamily\n\ttest\n",
        'table\tfamily\n"unterminated\ttest\n',
    ],
)
def test_malformed_table_overlay_refuses_activation(corpus, tmp_path, text):
    overlay = tmp_path / "bad.tsv"
    overlay.write_text(text)
    config = write(tmp_path, "config.json", {})
    output = tmp_path / "candidate.sqlite"
    with pytest.raises((ValueError, csv.Error)):
        build(corpus, output, config=config, table_inputs_input=overlay)
    assert not output.exists()
    assert json.loads(output.with_suffix(".sqlite.reconciliation.json").read_text())["status"] == "failed"


def test_missing_invalid_encoding_and_drifting_overlays_refuse(corpus, tmp_path, monkeypatch):
    config = write(tmp_path, "config.json", {})
    output = tmp_path / "candidate.sqlite"
    with pytest.raises(FileNotFoundError):
        build(corpus, output, config=config, catalog_input=tmp_path / "missing.json")
    with pytest.raises(IsADirectoryError):
        build(corpus, output, config=config, table_inputs_input=tmp_path)
    overlay = tmp_path / "bad.tsv"
    overlay.write_bytes(b"table\tfamily\n\xff\ttest\n")
    with pytest.raises(UnicodeDecodeError):
        build(corpus, output, config=config, table_inputs_input=overlay)
    catalog = write(tmp_path, "selected.json", json.loads((corpus / "catalog.json").read_text()))
    original = Inputs.check

    def drift(self):
        catalog.write_text(catalog.read_text() + " ")
        original(self)

    monkeypatch.setattr(Inputs, "check", drift)
    with pytest.raises(RuntimeError, match="Input changed during build"):
        build(corpus, output, config=config, catalog_input=catalog)
    assert not output.exists()
    assert not list(tmp_path.glob("*.tmp"))


def test_overlay_cli_routes_selected_paths(corpus, tmp_path, monkeypatch, capsys):
    import sys
    from spicy_regs.source_catalog.__main__ import main

    config = write(tmp_path, "config.json", {})
    catalog = write(tmp_path, "selected.json", json.loads((corpus / "catalog.json").read_text()))
    tables = tmp_path / "selected.tsv"
    tables.write_bytes((corpus / "table-inputs.tsv").read_bytes())
    output = tmp_path / "candidate.sqlite"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "source_catalog",
            "build",
            "--corpus",
            str(corpus),
            "--output",
            str(output),
            "--config",
            str(config),
            "--catalog-input",
            str(catalog),
            "--table-inputs-input",
            str(tables),
        ],
    )
    main()
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "validated"
    assert str(catalog.resolve()) in result["inputs"]
    assert str(tables.resolve()) in result["inputs"]


def publication_selection(tmp_path, *, cross_family=False, partitioned=False):
    """A retained response and independently pinned capture, never a live fetch."""
    from spicy_regs.source_catalog.storage import digest

    family = "comments" if cross_family else "test"
    descriptor: dict[str, object] = {"byteSize": 4, "rows": 2, "sha256": "sha256:" + "c" * 64}
    if partitioned:
        descriptor.pop("sha256")
        descriptor["partitionColumns"] = ["year"]
        descriptor["members"] = [
            {
                "key": "test/year=2026/part.parquet",
                "byteSize": 4,
                "rows": 2,
                "sha256": "sha256:" + "d" * 64,
                "partition": {"year": "2026"},
            }
        ]
    publication = write(
        tmp_path,
        "publication.v2.json",
        {
            "format": "spicy-regs-publication",
            "version": 2,
            "families": {
                family: {
                    "artifactDigest": "sha256:" + "b" * 64,
                    "prefix": "generations/test/" + "b" * 64,
                    "publishedAt": "2026-10-10T01:36:31Z",
                    "tables": {"test.parquet": descriptor},
                }
            },
        },
    )
    headers = tmp_path / "headers.txt"
    headers.write_text("HTTP/2 200\ndate: Sat, 10 Oct 2026 19:54:12 GMT\n")

    def pin(path):
        return {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}

    capture_at = "2026-10-10T19:54:12.354210+00:00"
    capture = write(
        tmp_path,
        "capture.json",
        {
            "observed_at": capture_at,
            "observations": [
                {
                    "exit_code": 0,
                    "bytes": publication.stat().st_size,
                    "sha256": digest(publication).removeprefix("sha256:"),
                    "url": "https://public.example/publication.v2.json",
                }
            ],
        },
    )
    bindings = write(
        tmp_path,
        "bindings.json",
        {
            "format": "B-successor-retained-publication-row-bindings",
            "version": 1,
            "publication_input": pin(publication),
            "header_input": pin(headers),
            "client_capture_at": capture_at,
            "response_http_date": {"utc": "2026-10-10T19:54:12+00:00"},
            "rows": [
                {
                    "publication_family": family,
                    "table": "test.parquet",
                    "artifact_digest": "sha256:" + "b" * 64,
                    "artifact_prefix": "generations/test/" + "b" * 64,
                    "member_sha256": descriptor.get("sha256"),
                    "member_bytes": 4,
                    "member_rows": 2,
                    "partition_members": descriptor.get("members"),
                    "partition_columns": descriptor.get("partitionColumns"),
                    "family_published_at": "2026-10-10T01:36:31Z",
                    "descriptor_locator": ["families", family, "tables", "test.parquet"],
                    "v4_inventory_bindings": [
                        {"collection_id": "family:test", "inventory_family": "test", "artifact_digest": "sha256:output"}
                    ],
                }
            ],
        },
    )
    return write(
        tmp_path,
        "selection.json",
        {
            "format": "spicy-retained-publication-selection",
            "version": 1,
            "publication_url": "https://public.example/publication.v2.json",
            "publication": pin(publication),
            "headers": pin(headers),
            "capture": pin(capture),
            "row_bindings": pin(bindings),
            "evidence_inputs": [],
        },
    )


@pytest.mark.parametrize("cross_family,partitioned", [(False, False), (True, False), (False, True)])
def test_retained_publication_preserves_inventory_and_scoped_coverage(corpus, tmp_path, cross_family, partitioned):
    from spicy_regs.source_catalog.coverage import coverage

    config = write(tmp_path, "config.json", {"collections": [{"id": "family:test", "families": ["test"]}]})
    original = tmp_path / "original.sqlite"
    build(corpus, original, config=config)
    with connect(original, readonly=True) as db:
        old_tables = [tuple(row) for row in db.execute("SELECT * FROM tables ORDER BY 1")]
        old_coverage = coverage(db, "family:test")
    selection = publication_selection(tmp_path, cross_family=cross_family, partitioned=partitioned)
    output = tmp_path / "selected.sqlite"
    result = build(corpus, output, config=config, publication_observations=[selection])
    with connect(output, readonly=True) as db:
        assert [tuple(row) for row in db.execute("SELECT * FROM tables ORDER BY 1")] == old_tables
        selected_coverage = coverage(db, "family:test")
        selected_coverage.pop("catalog_generation")
        old_coverage.pop("catalog_generation")
        if cross_family:
            assert selected_coverage == old_coverage
        else:
            selected_stages = selected_coverage["collections"][0]["stages"]
            old_stages = old_coverage["collections"][0]["stages"]
            new_measurements = selected_stages["published"]["measurements"]
            assert len(new_measurements) == len(old_stages["published"]["measurements"]) + 1
            assert all(item in new_measurements for item in old_stages["published"]["measurements"])
            fresh = next(item for item in new_measurements if item["version"] == "sha256:" + "b" * 64)
            assert fresh["scope"]["captured_at"] == "2026-10-10T19:54:12.354210+00:00"
            assert fresh["period"] == {"kind": "family-publication-instant", "label": "2026-10-10T01:36:31Z"}
            for stage in ["expected", "advertised", "acquired", "processed"]:
                assert selected_stages[stage] == old_stages[stage]
            assert selected_coverage["collections"][0]["completeness_denominator"] is None
    assert str(selection.resolve()) in result["inputs"]
    manifest = export(output, tmp_path / "public")
    cid = "publication:comments" if cross_family else "family:test"
    entry = next(item for item in manifest["collections"] if item["id"] == cid)
    detail = json.loads((tmp_path / "public" / entry["detailPath"]).read_text())
    observation = detail["publicationObservations"][0]
    assert observation["associationStatus"] == ("unqualified" if cross_family else "exact-family")
    assert observation["captureAt"] != observation["serverDate"] != observation["familyPublishedAt"]
    assert observation["artifactDigest"] == "sha256:" + "b" * 64
    assert ("members" in observation["tables"][0]) == partitioned
    if cross_family:
        historical = next(item for item in manifest["collections"] if item["id"] == "family:test")
        assert "publicationObservations" not in json.loads((tmp_path / "public" / historical["detailPath"]).read_text())
    if not cross_family:
        assert any(item["version"] == "sha256:" + "b" * 64 for item in detail["coverage"]["stages"]["published"]["measurements"])
    validate_public(detail)


@pytest.mark.parametrize(
    "fault", ["pin", "capture", "descriptor", "inventory", "duplicate", "missing", "partition", "timestamp"]
)
def test_retained_publication_rejects_invalid_selection_atomically(corpus, tmp_path, fault):
    from spicy_regs.source_catalog.storage import digest

    config = write(tmp_path, "config.json", {"collections": [{"id": "family:test", "families": ["test"]}]})
    output = tmp_path / "active.sqlite"
    build(corpus, output, config=config)
    before = output.read_bytes()
    selection_path = publication_selection(tmp_path, partitioned=fault == "partition")
    selection = json.loads(selection_path.read_text())
    if fault == "pin":
        selection["publication"]["sha256"] = "sha256:" + "f" * 64
    else:
        key = "capture" if fault in ["capture", "timestamp"] else "row_bindings"
        path = tmp_path / ("capture.json" if fault in ["capture", "timestamp"] else "bindings.json")
        value = json.loads(path.read_text())
        if fault == "timestamp":
            value["observed_at"] = "2026-10-10 19:54:12+00:00"
        elif fault == "capture":
            value["observations"][0]["bytes"] += 1
        elif fault == "descriptor":
            value["rows"][0]["member_rows"] += 1
        elif fault == "inventory":
            value["rows"][0]["v4_inventory_bindings"][0]["inventory_family"] = "other"
        elif fault == "duplicate":
            value["rows"].append(value["rows"][0])
        elif fault == "missing":
            value["rows"] = []
        elif fault == "partition":
            value["rows"][0]["partition_members"][0]["rows"] += 1
        path.write_text(json.dumps(value))
        selection[key] = {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}
    selection_path.write_text(json.dumps(selection))
    with pytest.raises(ValueError):
        build(corpus, output, config=config, publication_observations=[selection_path])
    assert output.read_bytes() == before
    assert json.loads(output.with_suffix(".sqlite.reconciliation.json").read_text())["status"] == "failed"


def test_identical_publication_bytes_keep_distinct_client_captures(corpus, tmp_path):
    from spicy_regs.source_catalog.storage import digest

    config = write(tmp_path, "config.json", {"collections": [{"id": "family:test", "families": ["test"]}]})
    first = publication_selection(tmp_path)
    selection = json.loads(first.read_text())
    capture = json.loads((tmp_path / "capture.json").read_text())
    bindings = json.loads((tmp_path / "bindings.json").read_text())
    capture["observed_at"] = bindings["client_capture_at"] = "2026-10-11T19:54:12+00:00"
    for key, name, value in [
        ("capture", "later-capture.json", capture),
        ("row_bindings", "later-bindings.json", bindings),
    ]:
        path = write(tmp_path, name, value)
        selection[key] = {"path": str(path), "sha256": digest(path), "bytes": path.stat().st_size}
    later = write(tmp_path, "later-selection.json", selection)
    output = tmp_path / "captures.sqlite"
    build(corpus, output, config=config, publication_observations=[first, later, first])
    with connect(output, readonly=True) as db:
        observations = [
            json.loads(row[0]) for row in db.execute("SELECT data FROM evidence WHERE kind='publication_observation'")
        ]
    assert len(observations) == 2
    assert len({item["id"] for item in observations}) == 2
    assert {item["captureAt"] for item in observations} == {
        "2026-10-10T19:54:12.354210+00:00",
        "2026-10-11T19:54:12+00:00",
    }


def test_retained_publication_requires_selected_response_url(corpus, tmp_path):
    config = write(tmp_path, "config.json", {"collections": [{"id": "family:test", "families": ["test"]}]})
    selection_path = publication_selection(tmp_path)
    selection = json.loads(selection_path.read_text())
    selection["publication_url"] = "https://other.example/publication.v2.json"
    selection_path.write_text(json.dumps(selection))
    with pytest.raises(ValueError, match="digest-matched response"):
        build(corpus, tmp_path / "refused.sqlite", config=config, publication_observations=[selection_path])


@pytest.mark.parametrize(
    "value",
    ["https://user:password@example.invalid/path", "https://public.invalid/?token=private", "/Users/private/label"],
)
def test_retained_publication_partition_labels_fail_closed_for_private_values(value):
    from spicy_regs.source_catalog.exporter import public_publication

    item = {
        "id": "publication:test",
        "tables": [
            {
                "table": "test.parquet",
                "rows": 1,
                "bytes": 1,
                "partitionColumns": ["label"],
                "members": [
                    {
                        "key": "test/part.parquet",
                        "sha256": "sha256:" + "a" * 64,
                        "rows": 1,
                        "bytes": 1,
                        "partition": {"label": value},
                    }
                ],
            }
        ],
    }
    with pytest.raises(ValueError):
        public_publication(item)


def test_additive_discovery_candidate_preserves_selected_scope_without_publication(corpus, tmp_path):
    from spicy_regs.source_catalog.coverage import coverage

    config = write(tmp_path, "config.json", {"collections": [{"id": "family:test", "families": ["test"]}]})
    candidate = {
        "id": "accepted-candidate:new-proof",
        "collection_id": "family:test",
        "qualification_input": {"sha256": "sha256:" + "a" * 64},
        "qualification_scope": "selected metadata; historical coverage unknown",
        "local_candidate_outputs": [{"table": "metadata", "sha256": "sha256:" + "d" * 64, "rows": 401}],
    }
    discovery = write(tmp_path, "discovery.json", {"format": "spicy-source-discovery", "version": 1, "planned": [candidate]})
    output = tmp_path / "candidate.sqlite"
    result = build(corpus, output, config=config, discovery=[discovery])
    with connect(output, readonly=True) as db:
        stages = coverage(db, "family:test")["collections"][0]["stages"]
        measurement = stages["processed"]["measurements"][0]
        assert measurement["count"] == 401
        assert measurement["version"] == candidate["local_candidate_outputs"][0]["sha256"]
        assert measurement["scope"]["qualification_scope"] == candidate["qualification_scope"]
        assert stages["published"]["status"] == stages["expected"]["status"] == "unknown"
    assert str(discovery.resolve()) in result["inputs"]
    candidate.pop("collection_id")
    write(tmp_path, "discovery.json", {"format": "spicy-source-discovery", "version": 1, "planned": [candidate]})
    with pytest.raises(ValueError, match="requires a collection or route binding"):
        build(corpus, tmp_path / "unbound.sqlite", config=config, discovery=[discovery])


def test_additive_candidate_route_binding_and_immutable_evidence_ids(corpus, tmp_path):
    from spicy_regs.source_catalog.coverage import coverage

    candidate = {
        "id": "candidate:old", "collection_id": "family:test",
        "qualification_input": {"sha256": "sha256:" + "a" * 64},
        "qualification_scope": "selected metadata",
        "local_candidate_outputs": [{"table": "metadata", "sha256": "sha256:" + "c" * 64, "rows": 3}],
    }
    config = write(tmp_path, "config.json", {
        "collections": [{"id": "family:test", "families": ["test"]}],
        "routes": [{"id": "selected-route", "collection_id": "family:test"}],
        "planned": [candidate],
    })
    route_candidate = dict(candidate, route_id="selected-route")
    route_candidate.pop("collection_id")
    route_candidate["local_candidate_outputs"] = [{"table": "metadata", "sha256": "sha256:" + "d" * 64, "rows": 4}]
    discovery = write(tmp_path, "discovery.json", {
        "format": "spicy-source-discovery", "version": 1,
        "planned": [route_candidate, dict(route_candidate, id="candidate:new")],
    })
    output = tmp_path / "route.sqlite"
    build(corpus, output, config=config, discovery=[discovery])
    with connect(output, readonly=True) as db:
        measurements = coverage(db, "family:test")["collections"][0]["stages"]["processed"]["measurements"]
        by_id = {m["evidence_ids"][0]: m for m in measurements}
        assert by_id["candidate:old"]["count"] == 3
        assert by_id["candidate:old"]["version"] == "sha256:" + "c" * 64
        assert by_id["candidate:new"]["count"] == 4
        assert by_id["candidate:new"]["route_id"] == "selected-route"
    route_candidate["route_id"] = "missing-route"
    write(tmp_path, "discovery.json", {"format": "spicy-source-discovery", "version": 1, "planned": [route_candidate]})
    with pytest.raises(ValueError, match="requires a collection or route binding"):
        build(corpus, tmp_path / "unknown-route.sqlite", config=config, discovery=[discovery])


def test_grown_baseline_manifest_retains_exact_historical_aggregate(corpus, tmp_path):
    baseline = [{"sha256": "sha256:prior", "relative_path": "prior.parquet", "family": "test"}]
    write(corpus, "metadata/baseline-files.json", baseline)
    config = write(tmp_path, "config.json", {})
    original = tmp_path / "original.sqlite"
    original_result = build(corpus, original, config=config)
    with connect(original, readonly=True) as db:
        prior = tuple(db.execute("SELECT * FROM evidence WHERE kind='baseline-files' AND collection_id IS NULL").fetchone())
    write(corpus, "metadata/baseline-files.json", baseline + [{"family": "test", "relative_path": "additional.parquet", "sha256": "sha256:new"}])
    from spicy_regs.source_catalog.storage import stable
    assert prior[0] != "baseline-files:" + stable(None, json.loads(prior[3]))
    history = {"evidence_id": prior[0], "value": json.loads(prior[3])["value"],
               "data_sha256": "sha256:" + hashlib.sha256(prior[3].encode()).hexdigest(),
               "prior_catalog_generation": original_result["generation"],
               "source_locator": {"database": str(original), "table": "evidence", "id": prior[0]}}
    discovery = write(tmp_path, "history.json", {"format": "spicy-source-discovery", "version": 1, "baseline_history": [history, history]})
    output = tmp_path / "successor.sqlite"
    build(corpus, output, config=config, discovery=[discovery])
    with connect(output, readonly=True) as db:
        aggregates = [tuple(row) for row in db.execute("SELECT * FROM evidence WHERE kind='baseline-files' AND collection_id IS NULL")]
        assert prior in aggregates
        assert len(aggregates) == 2
        assert db.execute("SELECT count(*) FROM files WHERE name='prior.parquet'").fetchone()[0] == 1
        assert db.execute("SELECT count(*) FROM files WHERE name='additional.parquet'").fetchone()[0] == 1


@pytest.mark.parametrize("history", [{"value": []}, ["not a baseline manifest"], [None]])
def test_historical_baseline_metadata_refuses_invalid_shapes(corpus, tmp_path, history):
    discovery = write(tmp_path, "history.json", {"format": "spicy-source-discovery", "version": 1, "baseline_history": history})
    with pytest.raises(ValueError, match="Historical baseline metadata"):
        build(corpus, tmp_path / "refused.sqlite", config=write(tmp_path, "config.json", {}), discovery=[discovery])


@pytest.mark.parametrize("change", ["checksum", "locator", "identity", "collision"])
def test_historical_baseline_metadata_refuses_changed_identity_or_value(corpus, tmp_path, change):
    baseline = []
    write(corpus, "metadata/baseline-files.json", baseline)
    config = write(tmp_path, "config.json", {})
    original = tmp_path / "original.sqlite"
    result = build(corpus, original, config=config)
    with connect(original, readonly=True) as db:
        prior = tuple(db.execute("SELECT * FROM evidence WHERE kind='baseline-files' AND collection_id IS NULL").fetchone())
    history = {"value": baseline, "evidence_id": prior[0], "data_sha256": "sha256:" + hashlib.sha256(prior[3].encode()).hexdigest(),
               "prior_catalog_generation": result["generation"], "source_locator": {"database": str(original), "table": "evidence", "id": prior[0]}}
    if change == "checksum":
        history["data_sha256"] = "sha256:" + "0" * 64
    elif change == "locator":
        history["source_locator"]["id"] = "another-row"
    elif change == "identity":
        history["evidence_id"] = "publication:unrelated"
    else:
        history["value"] = ["changed value"]
        history["data_sha256"] = "sha256:" + hashlib.sha256(json.dumps({"value": history["value"]}, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    discovery = write(tmp_path, "history.json", {"format": "spicy-source-discovery", "version": 1, "baseline_history": [history]})
    with pytest.raises(ValueError, match="Historical"):
        build(corpus, tmp_path / "refused.sqlite", config=config, discovery=[discovery])
