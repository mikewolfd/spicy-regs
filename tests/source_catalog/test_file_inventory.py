"""Native observations retain searchable facts, receipt pointers and strict bounds."""

import hashlib
import io
import json
from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from spicy_regs.source_catalog import file_inventory as native
from spicy_regs.source_catalog.exporter import export
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.storage import connect

GEN = "sha256:" + "a" * 64


def record(**fields):
    row = {field.name: None for field in native.schema(GEN, "source", "active")}
    row.update(
        alias_id="an-existing-nonraw-alias",
        dataset_id="source",
        inventory_kind="active",
        name="original.json",
        status="held",
        provenance_basis="unknown",
        availability="local",
    )
    row.update(fields)
    return row


def test_codec_preserves_native_values_and_has_only_declared_footer_metadata():
    row = record(
        provenance_basis="receipt-backed",
        content_sha256=bytes.fromhex("b" * 64),
        original_bytes=2**53 + 1,
        captured_at=datetime(2026, 10, 28, 1, 2, 3, 456789, tzinfo=UTC),
        source_urls=["https://example.gov/request", "https://example.gov/result"],
        periods=[
            {"kind": "year", "label": "2020", "basis": "Publisher record", "start": "2020", "end": "2020"},
            {"kind": "congress", "label": "116", "basis": "Native congress", "start": None, "end": None},
        ],
    )
    body = native.encode([row], generation=GEN, dataset_id="source", inventory_kind="active")
    assert native.validate(body, generation=GEN, dataset_id="source", inventory_kind="active") == [row]
    file = pq.ParquetFile(io.BytesIO(body))
    assert {key.decode() for key in file.metadata.metadata} == native.METADATA_KEYS
    assert b"ARROW:schema" not in file.metadata.metadata
    assert file.schema_arrow.field("original_bytes").type.bit_width == 64


@pytest.mark.parametrize(
    "fields",
    [
        {"name": "/Users/private/file"},
        {"source_url": "https://example.gov/?token=private"},
        {"source_url": "https://example.gov/a", "source_urls": ["https://example.gov/a", "https://example.gov/b"]},
        {"receipt_key": "../escape", "receipt_sha256": bytes(32), "receipt_entry": 1, "receipt_kind": "frozen-index"},
        {"original_receipt_pointer": "/Users/private"},
        {
            "period_kind": "year",
            "period_label": "2020",
            "periods": [{"kind": "year", "label": "2020"}, {"kind": "year", "label": "2021"}],
        },
    ],
)
def test_native_privacy_and_ambiguous_duplicate_fields_fail(fields):
    body = native.encode([record(**fields)], generation=GEN, dataset_id="source", inventory_kind="active")
    with pytest.raises(ValueError):
        native.validate(body, generation=GEN, dataset_id="source", inventory_kind="active")


def test_codec_rejects_wrong_scope_metadata_and_duplicate_observations():
    body = native.encode([record()], generation=GEN, dataset_id="source", inventory_kind="active")
    with pytest.raises(ValueError):
        native.validate(body, generation=GEN, dataset_id="other", inventory_kind="active")
    duplicate = native.encode([record(), record()], generation=GEN, dataset_id="source", inventory_kind="active")
    with pytest.raises(ValueError, match="duplicate"):
        native.validate(duplicate, generation=GEN, dataset_id="source", inventory_kind="active")


@pytest.mark.parametrize(
    "timestamp", ["2026-10-28", "2026-10-28T01:02:03", "2026-10-28T01:02:03.123456789Z", "not-a-date"]
)
def test_naive_invalid_and_unsupported_precision_are_unknown_with_reason(timestamp):
    class References:
        def indexed(self, ref):
            return None

    row = native.native_row(
        {"id": "alias", "name": "source", "status": "held", "sha256": None, "bytes": 0},
        {
            "receipt_object": "acquisitions/sha256/aa/" + "a" * 64,
            "mapping": {"acquired_at": timestamp, "receipt_evidence_ref": {"sha256": GEN, "entry": 1}},
        },
        dataset_id="source",
        publisher=None,
        inventory_kind="active",
        references=References(),
    )
    assert row["captured_at"] is None
    assert "capture-time-invalid-or-unsupported" in row["conflict_fields"]


def test_archive_refs_resolve_exact_receipt_entry_and_original_pointer_without_raw_rebuild(tmp_path):
    root = tmp_path / "raw"
    store = CentralRawArchiveStore(root)
    original_receipt = b'{"originals":[{"observed_at":"2026-09-14T22:05:06.284509+00:00"}]}'
    sha = "sha256:" + hashlib.sha256(original_receipt).hexdigest()
    store.put_blob(sha, len(original_receipt), [original_receipt])
    store.record_receipt(
        files=[{"sha256": sha, "bytes": len(original_receipt), "role": "acquisition-receipt"}],
        receipt={"status": "retained-original"},
    )
    body = b'{"publisher":"literal response"}'
    body_sha = "sha256:" + hashlib.sha256(body).hexdigest()
    store.put_blob(body_sha, len(body), [body])
    acquisition = store.record_receipt(
        files=[
            {
                "sha256": body_sha,
                "bytes": len(body),
                "dataset_id": "source",
                "native_id": "source-record-42",
                "source_version": "reviewed-v1",
                "source_url": "https://example.gov/original",
                "acquired_at": "2026-09-14T22:05:06.284509+00:00",
                "receipt_context": {"original_receipt_sha256": sha, "original_receipt_pointer": "/originals/0"},
                "covered_period": {"kind": "year", "label": "2020", "start": "2020-01-01", "end": "2020-12-31"},
            }
        ],
        receipt={"status": "captured"},
    )
    config = tmp_path / "registry.json"
    config.write_text("{}")
    index = tmp_path / "index.sqlite"
    build(root, index, config=config)
    with connect(index, readonly=True, scope="all") as db:
        before = db.execute("SELECT count(*) FROM main.files").fetchone()[0]
    output = tmp_path / "public"
    manifest = export(index, output)
    assert manifest["version"] == 3
    collection = next(c for c in manifest["collections"] if c["id"] == "source")
    detail = json.loads((output / collection["detailPath"]).read_text())
    part = detail["files"]["pages"][0]
    assert part["format"] == "parquet" and part["bytes"] == (output / part["path"]).stat().st_size
    assert part["sha256"] == "sha256:" + hashlib.sha256((output / part["path"]).read_bytes()).hexdigest()
    rows = native.validate(
        (output / part["path"]).read_bytes(),
        generation=manifest["generation"],
        dataset_id="source",
        inventory_kind="active",
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["source_id"] == "source-record-42" and row["source_version"] == "reviewed-v1"
    assert (
        row["receipt_key"] == acquisition["object"]
        and row["receipt_kind"] == "acquisition"
        and row["receipt_entry"] == 1
    )
    assert row["original_receipt_key"] == store.describe_blob(sha, len(original_receipt))["object"]
    assert row["original_receipt_pointer"] == "/originals/0" and row["original_receipt_sha256"] == bytes.fromhex(
        sha[7:]
    )
    assert row["captured_at"] == datetime(2026, 9, 14, 22, 5, 6, 284509, tzinfo=UTC)
    with connect(index, readonly=True, scope="all") as db:
        assert db.execute("SELECT count(*) FROM main.files").fetchone()[0] == before


def test_compressed_byte_bound_splits_shards_without_dropping_aliases(tmp_path, monkeypatch):
    import random

    from tests.source_catalog.test_export_reporting import index_with_collection

    index = index_with_collection(tmp_path)
    randomness = random.Random(1729)
    expected = set()
    with connect(index) as db:
        for i in range(8):
            alias = f"alias-{i}"
            expected.add(alias)
            name = randomness.randbytes(18000).hex()
            db.execute(
                "INSERT INTO files VALUES(?,?,NULL,?,?,NULL,NULL,NULL,?)", (alias, "publication", name, "held", "{}")
            )
        db.commit()
    monkeypatch.setattr(native, "MAX_BYTES", 32000)
    output = tmp_path / "public"
    manifest = export(index, output, page_size=5000)
    detail = json.loads((output / manifest["collections"][0]["detailPath"]).read_text())
    pages = detail["files"]["pages"]
    assert len(pages) > 1
    seen = []
    for page in pages:
        assert page["bytes"] <= 32000 and page["rows"] <= 5000
        rows = native.validate(
            (output / page["path"]).read_bytes(),
            generation=manifest["generation"],
            dataset_id="publication",
            inventory_kind="active",
        )
        seen.extend(row["alias_id"] for row in rows)
    assert len(seen) == len(expected) and set(seen) == expected


def test_existing_route_identity_remains_a_native_searchable_column():
    class References:
        def indexed(self, ref):
            return None

    row = native.native_row(
        {"id": "alias", "route_id": "route-current", "name": "source", "status": "held", "sha256": None, "bytes": 0},
        {},
        dataset_id="source",
        publisher=None,
        inventory_kind="active",
        references=References(),
    )
    assert row["route_id"] == "route-current"
    body = native.encode([row], generation=GEN, dataset_id="source", inventory_kind="active")
    assert (
        native.validate(body, generation=GEN, dataset_id="source", inventory_kind="active")[0]["route_id"]
        == "route-current"
    )
