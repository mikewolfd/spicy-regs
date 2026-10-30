"""The source catalog rebuilds from portable raw manifests and receipts alone."""

from tests.source_catalog.file_helpers import read_file_page


import gzip
import hashlib
import json
import shutil

import pytest

from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.raw_archive import holdings
from spicy_regs.source_catalog.storage import connect


def base_archive(root):
    root.mkdir()
    body = b"<billStatus>original bytes</billStatus>"
    sha = hashlib.sha256(body).hexdigest()
    object_name = f"objects/sha256/{sha[:2]}/{sha}.gz"
    path = root / object_name
    path.parent.mkdir(parents=True)
    path.write_bytes(gzip.compress(body, mtime=0))
    obj = {
        "sha256": sha,
        "bytes": len(body),
        "object": object_name,
        "encoding": "gzip",
        "stored_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "stored_bytes": path.stat().st_size,
    }
    raw = {
        "sha256": sha,
        "bytes": len(body),
        "object": object_name,
        "original_location": "/removed/worktree/BILLSTATUS-119-hr42.xml",
        "receipt_context": {"family": "bill-family"},
        "receipt_status": "provisional",
    }
    indexes = {}
    for name, rows in {
        "objects": [obj],
        "files": [raw, dict(raw, original_location="/another/BILLSTATUS-119-hr42.xml")],
        "receipts": [],
        "provisional-receipts": [{"raw_sha256": sha, "acquired_at": None}],
        "unavailable-receipts": [],
    }.items():
        name += ".jsonl.gz"
        payload = gzip.compress(b"".join((json.dumps(row) + "\n").encode() for row in rows), mtime=0)
        (root / name).write_bytes(payload)
        indexes[name] = hashlib.sha256(payload).hexdigest()
    (root / "snapshot.json").write_text(
        json.dumps({"format": "spicygov-raw-files-1", "recorded_at": "2026-10-24T12:00:00Z", "indexes": indexes})
    )
    return raw, obj


def test_raw_rebuild_deduplicates_and_preserves_period_semantics(tmp_path):
    root = tmp_path / "raw"
    raw, _ = base_archive(root)
    config = tmp_path / "registry.json"
    config.write_text("{}")
    first = build(root, tmp_path / "one.sqlite", config=config)
    second = build(root, tmp_path / "two.sqlite", config=config)
    assert first["generation"] == second["generation"]
    assert first["raw_archive"]["totals"] == {
        "file_locations": 2,
        "raw_objects": 1,
        "mapped_raw_objects": 1,
        "raw_objects_with_period": 1,
    }
    with connect(tmp_path / "one.sqlite") as db:
        facts = json.loads(db.execute("SELECT data FROM files LIMIT 1").fetchone()[0])
        assert facts["mapping"]["acquired_at"] is None
        assert facts["mapping"]["periods"][0]["kind"] == "congress"
        assert facts["mapping"]["periods"][0]["label"] == "119"
        assert db.execute("SELECT sha256 FROM files LIMIT 1").fetchone()[0] == "sha256:" + raw["sha256"]
    # No old source location is needed when the archive is copied to another machine.
    shutil.copytree(root, tmp_path / "mirror")
    mirrored = build(tmp_path / "mirror", tmp_path / "mirror.sqlite", config=config)
    assert mirrored["raw_archive"] == first["raw_archive"]


def test_raw_build_refuses_mutated_indexes_symlinks_and_in_archive_output(tmp_path):
    root = tmp_path / "raw"
    _, obj = base_archive(root)
    with pytest.raises(ValueError, match="outside"):
        build(root, root / "catalog.sqlite")
    output = tmp_path / "active.sqlite"
    output.write_bytes(b"previous catalog")
    target = root / obj["object"]
    outside = tmp_path / "outside.gz"
    target.rename(outside)
    target.symlink_to(outside)
    with pytest.raises(ValueError, match="symlink"):
        build(root, output)
    assert output.read_bytes() == b"previous catalog"
    target.unlink()
    outside.rename(target)
    with (root / "files.jsonl.gz").open("ab") as stream:
        stream.write(b"mutation")
    with pytest.raises(ValueError, match="checksum|pin|digest"):
        build(root, output)
    assert output.read_bytes() == b"previous catalog"


def test_support_role_preserves_original_but_excludes_usable_coverage(tmp_path):
    from spicy_regs.source_catalog.coverage import coverage

    root = tmp_path / "raw"
    raw, _ = base_archive(root)
    path = root / "files.jsonl.gz"
    with gzip.open(path, "wt") as stream:
        for role in ("original-response", "supporting-original", "discovery-index"):
            stream.write(json.dumps(dict(raw, role=role)) + "\n")
    snapshot = json.loads((root / "snapshot.json").read_text())
    snapshot["indexes"][path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    (root / "snapshot.json").write_text(json.dumps(snapshot))
    config = tmp_path / "config.json"
    config.write_text("{}")
    index = tmp_path / "scope.sqlite"
    build(root, index, config=config)
    with connect(index, readonly=True) as db:
        assert [r[0] for r in db.execute("SELECT status FROM files ORDER BY status")] == [
            "held", "supporting-evidence", "supporting-evidence",
        ]
        assert db.execute("SELECT count(DISTINCT sha256) FROM files").fetchone()[0] == 1
        acquired = coverage(db, "family:bill-family")["collections"][0]["stages"]["acquired"]
        assert sum(item["count"] for item in acquired["measurements"]) == 1


def test_legacy_fec_processing_context_is_retained_without_source_coverage(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    from spicy_regs.source_catalog.coverage import coverage
    from spicy_regs.source_catalog.scope import install_scope

    root = tmp_path / "raw"
    store = CentralRawArchiveStore(root)
    artifact = "a" * 64
    context = {
        "version": 1,
        "source_capture": {"original_sha256": "sha256:" + "b" * 64},
        "selection": {"observation_part": 10},
        "parsing": {"status": "parsed", "facts": ["decoded workbook cell"] * 4000},
        "coverage_limits": ["Native workbook bytes remain authoritative."],
    }
    derived = json.dumps(context).encode()
    assert 64 * 1024 < len(derived) < 1024 * 1024
    small_context = json.dumps(dict(context, parsing={"status": "parsed"})).encode()
    context_variants = {
        "member-context": {"version": 1, "bulk_group": "Contributions", "member": {"name": "itcont.txt"},
                           "parsing": {"state": "unparsed"}, "coverage_limits": ["Selected member only."]},
        "selection-context": {"version": 1, "bulk_group": "Campaigns", "selection": {"census_axis": "bulk"},
                              "related_collection_ids": [], "coverage_limits": []},
        "dictionary-context": {"version": 1, "source_capture": {"sha256": "sha256:" + "c" * 64},
                               "bulk_group": "Dictionaries", "evidence": [], "parsing": {"status": "parsed"}},
        "empty-limits": dict(context, coverage_limits=[]),
        "authority-context": {"version": 1, "source_family": "fec_ao", "source_authority": {"kind": "official"},
                              "selection": {"native_parse_requested": False}, "related_collection_ids": [],
                              "coverage_limits": []},
        "timing-context": {"version": 1, "source_capture": {"acquisition_day": "2026-09-12"},
                           "evidence": [], "coverage_limits": ["Exact acquisition timestamp unknown."]},
    }
    files = []
    for name, body, role, producer in [
        ("context", derived, "evidence", True),
        ("small-context", small_context, "evidence", True),
        *[(name, json.dumps(value).encode(), "evidence", True) for name, value in context_variants.items()],
        # A proved content kind applies to every exact-byte alias; receipt roles stay intact.
        ("other-role", small_context, "original-response", True),
        ("publisher", b'{"results": [{"committee_id": "C00000001"}], "pagination": {"page": 1, "pages": 1}}', "evidence", True),
        ("source-capture", b"PK intact source response capture", "source-acquisition-evidence", True),
        ("unknown-version", json.dumps(dict(context, version=2)).encode(), "evidence", True),
        ("boolean-version", json.dumps(dict(context, version=True)).encode(), "evidence", True),
        ("float-version", json.dumps(dict(context, version=1.0)).encode(), "evidence", True),
        ("other-producer", derived, "evidence", False),
        ("unknown-fields", json.dumps(dict(context, results=[])).encode(), "evidence", True),
        ("oversize", b" " * (1024 * 1024) + derived, "evidence", True),
    ]:
        sha = hashlib.sha256(body).hexdigest()
        store.put_blob(sha, len(body), [body])
        member = f"blobs/sha256/{sha}"
        files.append({
            "sha256": sha, "bytes": len(body), "name": name,
            "receipt_source_url": "https://example.test/context.json" if len(body) < 64 * 1024
            else "https://example.test/eFilingFormats.zip",
            "original_location": f"/old/source-evidence/{artifact}/{member}" if producer else "/other/record.json",
            "receipt_context": {
                "artifact": "sha256:" + artifact, "declared_role": role,
                "family": "fec-query", "member": member,
            },
        })
    store.record_receipt(files=files, receipt={"status": "captured"})
    config = tmp_path / "config.json"
    config.write_text("{}")
    index = tmp_path / "index.sqlite"
    build(root, index, config=config)
    with connect(index, scope="all") as db:
        rows = {row["name"]: row for row in db.execute("SELECT * FROM files")}
        supporting = {"context", "small-context", "other-role", "other-producer", *context_variants}
        assert all(rows[name]["status"] == "supporting-evidence" for name in supporting)
        uncertain = {"unknown-version", "boolean-version", "float-version", "unknown-fields", "oversize"}
        assert all(rows[name]["status"] == "unclassified-source" for name in uncertain)
        assert all(row["status"] == "held" for name, row in rows.items() if name not in supporting | uncertain)
        facts = json.loads(rows["context"]["data"])
        assert facts["mapping"]["role"] == "evidence"
        assert facts["receipt_context"] == files[0]["receipt_context"]
        assert facts["mapping"]["content_metadata"]["role"] == "processing-evidence"
        assert facts["mapping"]["content_metadata"]["raw_sha256"] == rows["context"]["sha256"]
        assert json.loads(rows["oversize"]["data"])["mapping"]["content_metadata"]["outcome"] == "not-read"
        acquired = coverage(db, "family:fec-query")["collections"][0]["stages"]["acquired"]
        assert sum(item["count"] for item in acquired["measurements"]) == len(files) - len(supporting | uncertain)
        install_scope(db, {
            "format": "spicy-source-scope", "version": 1, "excluded_objects": [],
            "collections": [{"id": "family:fec-query", "disposition": "active", "reason": "Current consumer."}],
        })
        db.commit()
    with connect(index, readonly=True, scope="active") as db:
        assert {r[0] for r in db.execute("SELECT name FROM files")} == set(rows) - (supporting | uncertain)
    with connect(index, readonly=True, scope="retained") as db:
        assert {r[0] for r in db.execute("SELECT name FROM files")} == supporting | uncertain


def test_acquisition_only_archive_rebuilds_without_prior_catalog(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    root = tmp_path / "raw"
    store = CentralRawArchiveStore(root)
    body = b"publisher response"
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob("sha256:" + sha, len(body), [body])
    store.record_receipt(
        files=[
            {
                "sha256": sha,
                "bytes": len(body),
                "dataset_id": "publisher:example",
                "native_id": "edition-1",
                "covered_period": "2025",
                "original_location": "https://example.test/source",
            }
        ],
        receipt={"recorded_at": "2026-10-24T12:00:00Z", "status": "success"},
    )
    result = build(root, tmp_path / "catalog.sqlite")
    assert result["raw_archive"]["totals"]["raw_objects"] == 1
    with connect(tmp_path / "catalog.sqlite") as db:
        assert holdings(db, "publisher:example")["periods"][0]["kind"] == "declared-covered-period"
        assert not db.execute("SELECT * FROM lineage").fetchall()


def test_failed_responses_and_request_bodies_are_not_acquired_dataset_coverage(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    from spicy_regs.source_catalog.coverage import coverage

    root = tmp_path / "raw"
    store = CentralRawArchiveStore(root)
    files = []
    for body, metadata in [
        (b"upstream failure", {"http_status": 500}),
        (b"malformed original", {"http_status": 200, "source_validation": "refused"}),
        (b"partial original", {"response_complete": False}),
        (b"range fragment", {"role": "source-fragment", "partial": True, "response_complete": True}),
        (b"request parameters", {"role": "request-body"}),
    ]:
        sha = hashlib.sha256(body).hexdigest()
        store.put_blob(sha, len(body), [body])
        files.append({"sha256": sha, "bytes": len(body), "dataset_id": "publisher:example", **metadata})
    store.record_receipt(files=files, receipt={"status": "failed"})
    build(root, tmp_path / "catalog.sqlite")
    with connect(tmp_path / "catalog.sqlite") as db:
        assert {row[0] for row in db.execute("SELECT status FROM files")} == {
            "failed-response",
            "receipt-only",
            "refused-source",
            "incomplete-response",
            "partial-source",
        }
        assert coverage(db, "publisher:example")["collections"][0]["stages"]["acquired"]["measurements"] == []


def test_later_refusal_applies_to_same_run_but_not_independent_capture(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    from spicy_regs.source_catalog.coverage import coverage

    root = tmp_path / "raw"
    store = CentralRawArchiveStore(root)
    body = b"native source response"
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob(sha, len(body), [body])
    original = {"sha256": sha, "bytes": len(body), "dataset_id": "publisher:example"}
    for run_id, fields in [
        ("run-one", {}),
        ("run-one", {"source_validation": "refused"}),
        ("run-two", {}),
    ]:
        store.record_receipt(files=[{**original, **fields}], receipt={"run_id": run_id})
    build(root, tmp_path / "catalog.sqlite")
    with connect(tmp_path / "catalog.sqlite") as db:
        assert [
            tuple(row) for row in db.execute("SELECT status,count(*) FROM files GROUP BY status ORDER BY status")
        ] == [("held", 1), ("refused-source", 2)]
        acquired = coverage(db, "publisher:example")["collections"][0]["stages"]["acquired"]["measurements"]
        assert acquired[0]["count"] == 1
        # Holdings describes physical custody, including refused originals.
        assert holdings(db)["totals"]["raw_objects"] == 1


def test_source_url_without_a_filename_has_a_stable_display_name(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    from spicy_regs.source_catalog.exporter import export

    root = tmp_path / "raw"
    store = CentralRawArchiveStore(root)
    body = b"publisher homepage"
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob(sha, len(body), [body])
    store.record_receipt(
        files=[
            {
                "sha256": sha,
                "bytes": len(body),
                "dataset_id": "publisher:example",
                "original_location": "https://example.test/",
            }
        ],
        receipt={"status": "captured"},
    )
    # This naming fixture has its own publisher, independent of live-use policy.
    config = tmp_path / "registry.json"
    config.write_text("{}")
    build(root, tmp_path / "index.sqlite", config=config)
    with connect(tmp_path / "index.sqlite") as db:
        assert db.execute("SELECT name FROM files").fetchone()[0] == "sha256:" + sha
    manifest = export(tmp_path / "index.sqlite", tmp_path / "public")
    collection = next(row for row in manifest["collections"] if row["id"] == "publisher:example")
    detail = json.loads((tmp_path / "public" / collection["detailPath"]).read_text())
    page = read_file_page(tmp_path / "public" / detail["files"]["pages"][0]["path"])
    assert page["files"][0]["name"] == "sha256:" + sha
