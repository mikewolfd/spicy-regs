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
