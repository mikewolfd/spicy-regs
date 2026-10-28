"""Bounded semantic checks: missing/different/ambiguous versions and asset roles."""
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.source_catalog.coverage_diff import compare


class CoverageDiffTests(unittest.TestCase):
    def test_complete_inventory_requires_finite_scope(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            rows = [{"key": "source-1", "etag": "v1", "size": 1}]
            expected = self.write(root, "expected", rows)
            actual = self.write(root, "actual", rows, role="available")
            value = json.loads(expected.read_text())
            value["scope"]["finite"] = False
            expected.write_text(json.dumps(value))
            with self.assertRaisesRegex(ValueError, "finite scope"):
                compare(expected, actual, root / "out")

    def write(self, root, name, rows, *, asset="json-body", role="expected", scope="fixture"):
        file = root / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([
            ("key", pa.string()), ("etag", pa.string()), ("size", pa.int64())])), file)
        descriptor = root / f"{name}.json"
        descriptor.write_text(json.dumps({
            "path": str(file), "format": "parquet", "role": role,
            "columns": {"source_id": "key", "source_version": "etag", "bytes": "size"},
            "asset_kind": asset, "version_kind": "etag", "complete": True,
            "scope": {"id": scope, "finite": True},
        }))
        return descriptor

    def test_version_and_identity_classification(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            def row(key, version="v1", size=1):
                return {"key": key, "etag": version, "size": size}
            expected = self.write(root, "expected", [row(key) for key in
                ("same", "missing", "stale", "unknown", "conflict", "size-conflict", "duplicate")])
            actual = self.write(root, "actual", [row("same"), row("stale", "v2"), row("unknown", None),
                row("conflict"), row("conflict", "v2"), row("size-conflict", size=2),
                row("duplicate"), row("duplicate"), row("unexpected")], role="used")
            summary = compare(expected, actual, root / "out", memory_limit="64MB", threads=1)
            rows = {row["source_id"]: row for row in pq.read_table(root / "out/diff.parquet").to_pylist()}
            self.assertEqual({key: row["status"] for key, row in rows.items()}, {
                "same": "same", "missing": "missing", "stale": "stale", "unknown": "unknown",
                "conflict": "unknown", "size-conflict": "unknown", "duplicate": "same", "unexpected": "unexpected"})
            self.assertEqual(summary["measurements"]["actual"], {
                "input_rows": 9, "identities": 7, "duplicate_identities": 2, "duplicate_rows": 2})
            self.assertEqual(rows["duplicate"]["actual_rows"], 2)
            self.assertEqual({key: row["identity_status"] for key, row in rows.items()}, {
                "same": "present", "missing": "missing", "stale": "present", "unknown": "present",
                "conflict": "present", "size-conflict": "present", "duplicate": "present", "unexpected": "unexpected"})
            self.assertEqual(summary["identity_parity"], "different")
            self.assertEqual(summary["identity_counts"], [
                {"asset_kind": "json-body", "status": "missing", "identities": 1},
                {"asset_kind": "json-body", "status": "present", "identities": 6},
                {"asset_kind": "json-body", "status": "unexpected", "identities": 1},
            ])

    def test_identity_parity_does_not_require_version_columns(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            rows = [{"key": "source-1", "etag": None, "size": 1}]
            expected = self.write(root, "expected", rows)
            actual = self.write(root, "actual", rows * 3, role="available")
            for descriptor in (expected, actual):
                spec = json.loads(descriptor.read_text())
                del spec["columns"]["source_version"]
                del spec["version_kind"]
                descriptor.write_text(json.dumps(spec))
            summary = compare(expected, actual, root / "out", memory_limit="64MB", threads=1)
            self.assertEqual(summary["identity_parity"], "equal")
            self.assertEqual(summary["identity_counts"], [
                {"asset_kind": "json-body", "status": "present", "identities": 1}])
            self.assertEqual(summary["counts"], [{"asset_kind": "json-body", "status": "unknown",
                                                 "reason": "version-unrecorded", "identities": 1}])
            self.assertEqual(summary["measurements"]["actual"], {
                "input_rows": 3, "identities": 1, "duplicate_identities": 1, "duplicate_rows": 2})

    def test_incomplete_inventories_cannot_qualify_parity(self):
        for incomplete in ("expected", "actual", "both"):
            with self.subTest(incomplete=incomplete), TemporaryDirectory() as folder:
                root = Path(folder)
                rows = [{"key": "source-1", "etag": "v1", "size": 1}]
                expected = self.write(root, "expected", rows)
                actual = self.write(root, "actual", rows, role="available")
                for descriptor in (expected, actual):
                    if incomplete in (descriptor.stem, "both"):
                        spec = json.loads(descriptor.read_text())
                        spec["complete"] = False
                        descriptor.write_text(json.dumps(spec))
                summary = compare(expected, actual, root / "out", memory_limit="64MB", threads=1)
                self.assertEqual(summary["identity_parity"], "unqualified")
                self.assertEqual(summary["identity_counts"], [
                    {"asset_kind": "json-body", "status": "present", "identities": 1}])
                self.assertEqual(summary["counts"][0]["status"], "same")

    def test_empty_complete_inventories_have_equal_identity_parity(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            expected = self.write(root, "expected", [])
            actual = self.write(root, "actual", [], role="available")
            summary = compare(expected, actual, root / "out", memory_limit="64MB", threads=1)
            self.assertEqual(summary["identity_parity"], "equal")
            self.assertEqual(summary["identity_counts"], [])
            self.assertEqual(summary["measurements"]["actual"], {
                "input_rows": 0, "identities": 0, "duplicate_identities": 0, "duplicate_rows": 0})

    def test_json_metadata_does_not_cover_attachment_body(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            rows = [{"key": "source-1", "etag": "v1", "size": 1}]
            expected = self.write(root, "expected", rows, asset="attachment-body")
            actual = self.write(root, "actual", rows, role="available")
            summary = compare(expected, actual, root / "out", memory_limit="64MB", threads=1)
            self.assertEqual({row["status"] for row in summary["counts"]}, {"missing", "unexpected"})
            self.assertEqual(summary["identity_parity"], "different")
            self.assertEqual(summary["identity_counts"], [
                {"asset_kind": "attachment-body", "status": "missing", "identities": 1},
                {"asset_kind": "json-body", "status": "unexpected", "identities": 1},
            ])

    def test_different_scopes_refuse(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            rows = [{"key": "source-1", "etag": "v1", "size": 1}]
            expected = self.write(root, "expected", rows)
            actual = self.write(root, "actual", rows, role="available", scope="another-generation")
            with self.assertRaisesRegex(ValueError, "scopes differ"):
                compare(expected, actual, root / "out")

    def test_pin_mismatch_refuses_and_filters_limit_scope(self):
        with TemporaryDirectory() as folder:
            root = Path(folder)
            rows = [{"key": "selected", "etag": "v1", "size": 1},
                    {"key": "outside", "etag": "v2", "size": 2}]
            expected = self.write(root, "expected", rows)
            actual = self.write(root, "actual", rows, role="available")
            for descriptor in (expected, actual):
                spec = json.loads(descriptor.read_text())
                spec["filters"] = {"key": "selected"}
                descriptor.write_text(json.dumps(spec))
            summary = compare(expected, actual, root / "filtered", memory_limit="64MB", threads=1)
            self.assertEqual(summary["measurements"]["expected"]["identities"], 1)
            spec = json.loads(expected.read_text())
            spec.update(sha256="sha256:" + "0"*64, bytes=Path(spec["path"]).stat().st_size)
            expected.write_text(json.dumps(spec))
            with self.assertRaisesRegex(ValueError, "pin differs"):
                compare(expected, actual, root / "refused")


if __name__ == "__main__":
    unittest.main()
