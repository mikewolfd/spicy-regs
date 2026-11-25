"""Pinned PyIceberg comments export across evolved nested attachment schemas."""

from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

if TYPE_CHECKING:
    from pyiceberg.manifest import DataFile
    from pyiceberg.table import FileScanTask

from scripts import publish_comments_mirror_guarded as guard
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.policy_migrations import earlier_policy
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, r2
from spicy_regs.sources import regulatory_catalog as native
from spicy_regs.transforms.regulations_receipts import policy


def _v0_schema() -> pa.Schema:
    return earlier_policy("comments", "regulations-native-v2").subject_schema


def _v1_schema() -> pa.Schema:
    return policy("comments").subject_schema


def _base_row(comment_id: str, agency: str, docket_id: str, posted_date: str, attachments: Any) -> dict[str, Any]:
    row: dict[str, Any] = dict.fromkeys(_v1_schema().names)
    row.update(
        comment_id=comment_id,
        docket_id=docket_id,
        agency_code=agency,
        title=f"Title {comment_id}",
        comment=f"Body {comment_id}",
        posted_date=posted_date,
        modify_date=posted_date,
        attachments=attachments,
        text_content=f"Text {comment_id}",
        text_extraction_status="ok",
        duplicate_comments=0,
        comment_text=f"Body {comment_id}",
    )
    return row


def _v0_row(comment_id: str, agency: str, docket_id: str, posted_date: str, attachments: Any) -> dict[str, Any]:
    row = _base_row(comment_id, agency, docket_id, posted_date, attachments)
    return {name: row[name] for name in _v0_schema().names}


def _make_data_file(file_path: str, record_count: int, *, delete: bool = False, equality_delete: bool = False) -> DataFile:
    pytest.importorskip("pyiceberg")
    from pyiceberg.manifest import DataFile, DataFileContent, FileFormat

    content = (
        DataFileContent.EQUALITY_DELETES
        if equality_delete
        else DataFileContent.POSITION_DELETES
        if delete
        else DataFileContent.DATA
    )
    return DataFile.from_args(
        content=content,
        file_path=file_path,
        file_format=FileFormat.PARQUET,
        record_count=record_count,
        file_size_in_bytes=1024,
    )


def _make_task(data_file: DataFile, delete_files: set[DataFile] | None = None) -> FileScanTask:
    pytest.importorskip("pyiceberg")
    from pyiceberg.table import FileScanTask

    return FileScanTask(data_file, delete_files=delete_files)


class _FakeR2Client:
    def __init__(self, objects: dict[tuple[str, str], Path]) -> None:
        self._objects = objects
        self.downloads: list[tuple[str, str]] = []

    def download_file(self, bucket: str, key: str, filename: str) -> None:
        self.downloads.append((bucket, key))
        src = self._objects[(bucket, key)]
        Path(filename).write_bytes(src.read_bytes())


class _FakeTable:
    def __init__(self, table_uuid: str, snapshot_id: int, schema_id: int, tasks: list[FileScanTask]) -> None:
        self.metadata = SimpleNamespace(
            table_uuid=table_uuid,
            current_snapshot_id=snapshot_id,
            current_schema_id=schema_id,
        )
        self._tasks = tasks
        self.scanned_snapshot: int | None = None

    def scan(self, *, snapshot_id: int):
        self.scanned_snapshot = snapshot_id
        tasks = self._tasks

        class _Scan:
            def plan_files(self):
                return iter(tasks)

        return _Scan()


def _configure_r2_env(monkeypatch: pytest.MonkeyPatch, bucket: str = "spicy-regs") -> None:
    monkeypatch.setenv("R2_CATALOG_URI", "https://catalog.example/iceberg")
    monkeypatch.setenv("R2_CATALOG_WAREHOUSE", "wh")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "tok")
    monkeypatch.setenv("R2_BUCKET_NAME", bucket)
    monkeypatch.setenv("R2_ENDPOINT", "https://r2.example")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "ak")
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "sk")


def test_export_public_comments_mixed_schema_v0_v1_and_position_deletes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Export mixes schema_id 0 and 1 files (with NULL, [], null element, multi-element lists) + position deletes."""
    _configure_r2_env(monkeypatch)

    v0_rows = [
        _v0_row("EPA-01", "EPA", "EPA-1", "2025-01-10T00:00:00Z", None),
        _v0_row("EPA-02-del", "EPA", "EPA-1", "2025-01-10T00:00:00Z", []),
        _v0_row(
            "EPA-03",
            "EPA",
            "EPA-1",
            "2025-01-11T00:00:00Z",
            [
                {
                    "title": "Attachment 1",
                    "formats": [{"url": "https://example.invalid/1.pdf", "format": "pdf", "size": 123}, None],
                    "attachment_id": "A1",
                    "restrict_reason": None,
                    "restrict_reason_type": None,
                },
                None,
                {
                    "title": "Attachment 2",
                    "formats": None,
                    "attachment_id": "A2",
                    "restrict_reason": "PII",
                    "restrict_reason_type": "privacy",
                },
            ],
        ),
        _v0_row("FDA-04", "FDA", "FDA-1", "2025-02-01T00:00:00Z", []),
    ]
    v1_rows = [
        _base_row("EPA-05-del", "EPA", "EPA-1", "2026-01-01T00:00:00Z", None),
        _base_row("EPA-06-del", "EPA", "EPA-1", "2026-01-01T00:00:00Z", []),
        _base_row(
            "EPA-07",
            "EPA",
            "EPA-2",
            "2026-01-05T00:00:00Z",
            [
                {
                    "title": "Modern Attachment",
                    "formats": [{"url": "https://example.invalid/2.pdf", "format": "pdf", "size": 456}],
                    "attachment_id": "A3",
                    "restrict_reason": None,
                    "restrict_reason_type": None,
                    "agency_note": "Note",
                    "authors": ["Alice", None, "Bob"],
                    "description": "Desc",
                    "doc_abstract": "Abstract",
                    "doc_order": 2,
                    "modify_date": "2026-01-05T00:00:00Z",
                    "publication": "FR",
                }
            ],
        ),
    ]

    v0_file = tmp_path / "src_v0.parquet"
    v1_file = tmp_path / "src_v1.parquet"
    del_file = tmp_path / "src_del.parquet"

    # Write v0 through DuckDB so list child names are 'element' (matching Parquet/Iceberg files on R2).
    with duckdb.connect() as dcon:
        dcon.register("v0_src", pa.Table.from_pylist(v0_rows, schema=_v0_schema()))
        dcon.execute(f"COPY v0_src TO '{v0_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    pq.write_table(pa.Table.from_pylist(v1_rows, schema=_v1_schema()), v1_file)

    v0_uri = "s3://spicy-regs/warehouse/comments/data/v0.parquet"
    v1_uri = "s3://spicy-regs/warehouse/comments/data/v1.parquet"
    del_uri = "s3://spicy-regs/warehouse/comments/data/pos_del.parquet"
    pq.write_table(
        pa.table({"file_path": [v0_uri, v1_uri, v1_uri], "pos": pa.array([1, 0, 1], type=pa.int64())}),
        del_file,
    )

    fake_r2 = _FakeR2Client(
        {
            ("spicy-regs", "warehouse/comments/data/v0.parquet"): v0_file,
            ("spicy-regs", "warehouse/comments/data/v1.parquet"): v1_file,
            ("spicy-regs", "warehouse/comments/data/pos_del.parquet"): del_file,
        }
    )
    monkeypatch.setattr(r2, "get_r2_client", lambda: fake_r2)

    del_df = _make_data_file(del_uri, 3, delete=True)
    # Intentionally attach the shared delete file to both tasks and put v1 before v0 to check deterministic sort.
    tasks = [
        _make_task(_make_data_file(v1_uri, len(v1_rows)), delete_files={del_df}),
        _make_task(_make_data_file(v0_uri, len(v0_rows)), delete_files={del_df}),
    ]
    pinned = iceberg.CatalogPairSnapshot(
        "uuid-comments",
        99,
        1,
        iceberg.CatalogSnapshot("uuid-receipts", 10, 0),
    )
    fake_table = _FakeTable(pinned.table_uuid, pinned.snapshot_id, pinned.schema_id, tasks)
    monkeypatch.setattr("pyiceberg.catalog.rest.RestCatalog", lambda *a, **kw: SimpleNamespace(load_table=lambda _: fake_table))

    # Local DuckDB connection simulates attached catalog metadata + empty receipt count (4 accepted rows).
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
    native.ensure_native(con, COMMENT)
    with con.cursor() as c:
        c.execute(
            f"INSERT INTO {native.receipts_table()} (receipt_id, dataset, outcome) "
            "SELECT 'r' || i, 'comments', 'accepted' FROM range(4) t(i)"
        )
    monkeypatch.setattr(iceberg, "_connect", lambda: con)
    monkeypatch.setattr(native, "_remote_catalog", lambda _: True)
    monkeypatch.setattr(iceberg, "_read_pair_snapshot", lambda *a, **kw: pinned)
    monkeypatch.setattr(
        iceberg,
        "_table_metadata",
        lambda *a, **kw: {
            "current-snapshot-id": pinned.snapshot_id,
            "snapshots": [
                {
                    "snapshot-id": pinned.snapshot_id,
                    "summary": {"operation": "append", "total-records": "7", "total-delete-files": "1"},
                }
            ],
        },
    )
    monkeypatch.setattr(
        native,
        "_copy",
        lambda *a, **kw: pytest.fail("DuckDB native._copy must be bypassed for remote Iceberg catalog"),
    )

    out_dir = tmp_path / "export"
    result = iceberg.export_public_comments(
        out_dir,
        COMMENT,
        snapshot=pinned,
        resources=ExportResources("128MB", 1, row_group_rows=2, row_group_bytes="1MB"),
    )

    # Shared delete file must be downloaded exactly once despite appearing on both tasks.
    assert fake_r2.downloads == [
        ("spicy-regs", "warehouse/comments/data/pos_del.parquet"),
        ("spicy-regs", "warehouse/comments/data/v0.parquet"),
        ("spicy-regs", "warehouse/comments/data/v1.parquet"),
    ]
    assert fake_table.scanned_snapshot == pinned.snapshot_id
    assert not list(out_dir.glob("comments-export-*"))

    monolith = pq.read_table(result["comments"])
    assert monolith.schema.equals(_v1_schema())
    by_id = {row["comment_id"]: row for row in monolith.to_pylist()}
    assert set(by_id) == {"EPA-01", "EPA-03", "FDA-04", "EPA-07"}
    assert by_id["EPA-01"]["attachments"] is None
    assert by_id["FDA-04"]["attachments"] == []
    assert by_id["EPA-03"]["attachments"] == [
        {
            "title": "Attachment 1",
            "formats": [{"url": "https://example.invalid/1.pdf", "format": "pdf", "size": 123}, None],
            "attachment_id": "A1",
            "restrict_reason": None,
            "restrict_reason_type": None,
            "agency_note": None,
            "authors": None,
            "description": None,
            "doc_abstract": None,
            "doc_order": None,
            "modify_date": None,
            "publication": None,
        },
        None,
        {
            "title": "Attachment 2",
            "formats": None,
            "attachment_id": "A2",
            "restrict_reason": "PII",
            "restrict_reason_type": "privacy",
            "agency_note": None,
            "authors": None,
            "description": None,
            "doc_abstract": None,
            "doc_order": None,
            "modify_date": None,
            "publication": None,
        },
    ]
    assert by_id["EPA-07"]["attachments"][0]["authors"] == ["Alice", None, "Bob"]
    assert by_id["EPA-07"]["attachments"][0]["doc_order"] == 2

    index_rows = {
        (r["agency_code"], r["docket_id"], r["year"], r["month"]): r["row_count"]
        for r in pq.read_table(result["index"]).to_pylist()
    }
    assert index_rows == {
        ("EPA", "EPA-1", 2025, 1): 2,
        ("EPA", "EPA-2", 2026, 1): 1,
        ("FDA", "FDA-1", 2025, 2): 1,
    }


def test_copy_pinned_iceberg_snapshot_refuses_equality_deletes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _configure_r2_env(monkeypatch)
    monkeypatch.setattr(r2, "get_r2_client", lambda: pytest.fail("Must not download any file on equality deletes"))
    pinned = iceberg.CatalogSnapshot("uuid-comments", 99, 1)
    eq_del = _make_data_file(
        "s3://spicy-regs/warehouse/comments/data/eq_del.parquet", 1, equality_delete=True
    )
    tasks = [_make_task(_make_data_file("s3://spicy-regs/warehouse/comments/data/v1.parquet", 1), delete_files={eq_del})]
    fake_table = _FakeTable(pinned.table_uuid, pinned.snapshot_id, pinned.schema_id, tasks)
    monkeypatch.setattr("pyiceberg.catalog.rest.RestCatalog", lambda *a, **kw: SimpleNamespace(load_table=lambda _: fake_table))

    with pytest.raises(ValueError, match="unsupported Iceberg delete file content"):
        iceberg._copy_pinned_iceberg_snapshot(
            COMMENT,
            pinned,
            "default_native",
            tmp_path / "subjects.parquet",
            work_dir=tmp_path,
            resources=ExportResources("64MB", 1),
        )


@pytest.mark.parametrize(
    "bad_uri",
    [
        "s3://other-bucket/warehouse/comments/data/f.parquet",
        "https://spicy-regs.r2.dev/warehouse/comments/data/f.parquet",
        "s3://spicy-regs/warehouse/comments/data/f.parquet?versionId=1",
        "s3://spicy-regs/warehouse/comments/data/f.parquet#frag",
        "s3://spicy-regs/",
    ],
)
def test_copy_pinned_iceberg_snapshot_refuses_bad_s3_paths(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bad_uri: str
) -> None:
    _configure_r2_env(monkeypatch)
    monkeypatch.setattr(r2, "get_r2_client", lambda: pytest.fail("Must not download bad S3 path"))
    pinned = iceberg.CatalogSnapshot("uuid-comments", 99, 1)
    tasks = [_make_task(_make_data_file(bad_uri, 1))]
    fake_table = _FakeTable(pinned.table_uuid, pinned.snapshot_id, pinned.schema_id, tasks)
    monkeypatch.setattr("pyiceberg.catalog.rest.RestCatalog", lambda *a, **kw: SimpleNamespace(load_table=lambda _: fake_table))

    with pytest.raises(ValueError, match="outside s3://spicy-regs/"):
        iceberg._copy_pinned_iceberg_snapshot(
            COMMENT,
            pinned,
            "default_native",
            tmp_path / "subjects.parquet",
            work_dir=tmp_path,
            resources=ExportResources("64MB", 1),
        )


@pytest.mark.parametrize(
    ("table_uuid", "snapshot_id", "schema_id"),
    [("other-uuid", 99, 1), ("uuid-comments", 100, 1), ("uuid-comments", 99, 2)],
)
def test_copy_pinned_iceberg_snapshot_refuses_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, table_uuid: str, snapshot_id: int, schema_id: int
) -> None:
    pytest.importorskip("pyiceberg")
    _configure_r2_env(monkeypatch)
    pinned = iceberg.CatalogSnapshot("uuid-comments", 99, 1)
    fake_table = _FakeTable(table_uuid, snapshot_id, schema_id, [])
    monkeypatch.setattr("pyiceberg.catalog.rest.RestCatalog", lambda *a, **kw: SimpleNamespace(load_table=lambda _: fake_table))

    with pytest.raises(RuntimeError, match="Catalog snapshot changed before export"):
        iceberg._copy_pinned_iceberg_snapshot(
            COMMENT,
            pinned,
            "default_native",
            tmp_path / "subjects.parquet",
            work_dir=tmp_path,
            resources=ExportResources("64MB", 1),
        )


def test_copy_pinned_iceberg_snapshot_refuses_empty_task_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pyiceberg")
    _configure_r2_env(monkeypatch)
    pinned = iceberg.CatalogSnapshot("uuid-comments", 99, 1)
    fake_table = _FakeTable(pinned.table_uuid, pinned.snapshot_id, pinned.schema_id, [])
    monkeypatch.setattr("pyiceberg.catalog.rest.RestCatalog", lambda *a, **kw: SimpleNamespace(load_table=lambda _: fake_table))

    with pytest.raises(RuntimeError, match="planned zero data files"):
        iceberg._copy_pinned_iceberg_snapshot(
            COMMENT,
            pinned,
            "default_native",
            tmp_path / "subjects.parquet",
            work_dir=tmp_path,
            resources=ExportResources("64MB", 1),
        )


def test_align_comments_batch_refuses_unexpected_and_missing_required_fields() -> None:
    target_schema = _v1_schema()
    valid_row = _base_row("c1", "EPA", "D1", "2025-01-01", [])
    valid_batch = pa.Table.from_pylist([valid_row], schema=target_schema).to_batches()[0]
    aligned = iceberg._align_comments_batch_to_schema(valid_batch, target_schema)
    assert aligned.schema.equals(target_schema)

    # Unexpected top-level column must refuse.
    extra_top = valid_batch.append_column("extra_col", pa.array(["x"]))
    with pytest.raises(ValueError, match="Unexpected columns"):
        iceberg._align_comments_batch_to_schema(extra_top, target_schema)

    # Unexpected nested struct field in attachments must refuse.
    bad_attachments = pa.array(
        [[{"title": "t", "formats": None, "attachment_id": "a", "restrict_reason": None,
           "restrict_reason_type": None, "surprise_field": "drop_me"}]],
        type=pa.list_(
            pa.struct(
                [
                    ("title", pa.string()),
                    ("formats", pa.list_(pa.struct([("url", pa.string()), ("format", pa.string()), ("size", pa.int64())]))),
                    ("attachment_id", pa.string()),
                    ("restrict_reason", pa.string()),
                    ("restrict_reason_type", pa.string()),
                    ("surprise_field", pa.string()),
                ]
            )
        ),
    )
    idx = valid_batch.schema.get_field_index("attachments")
    bad_nested = valid_batch.set_column(idx, "attachments", bad_attachments)
    with pytest.raises(ValueError, match="unexpected struct fields.*surprise_field"):
        iceberg._align_comments_batch_to_schema(bad_nested, target_schema)

    # Missing non-optional attachment field (e.g. 'title') must refuse.
    missing_title = pa.array(
        [[{"formats": None, "attachment_id": "a", "restrict_reason": None, "restrict_reason_type": None}]],
        type=pa.list_(
            pa.struct(
                [
                    ("formats", pa.list_(pa.struct([("url", pa.string()), ("format", pa.string()), ("size", pa.int64())]))),
                    ("attachment_id", pa.string()),
                    ("restrict_reason", pa.string()),
                    ("restrict_reason_type", pa.string()),
                ]
            )
        ),
    )
    missing_req = valid_batch.set_column(idx, "attachments", missing_title)
    with pytest.raises(ValueError, match="missing required struct field 'title'"):
        iceberg._align_comments_batch_to_schema(missing_req, target_schema)


def test_copy_diagnostics_records_pyiceberg_copy_and_restores_hook(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from scripts import read_comment_attachment_witnesses as witness

    subject = {"table_uuid": "uuid-comments", "snapshot_id": 99, "schema_id": 1}
    args = SimpleNamespace(
        workdir=tmp_path,
        expected_source_sha="a" * 40,
        skip_upload=True,
        diagnostic_native_snapshot=subject,
    )
    pair = iceberg.CatalogPairSnapshot(**subject, receipts=iceberg.CatalogSnapshot("uuid-receipts", 10, 0))
    monkeypatch.setattr(guard.iceberg, "_qualified", lambda _: "reg_catalog.default_native.comments")
    monkeypatch.setattr(guard.iceberg, "_connect", duckdb.connect)
    monkeypatch.setattr(witness, "runtime_identity", lambda con: {"engine_version": [{"library_version": "1.5.5"}]})

    original = iceberg._copy_pinned_iceberg_snapshot
    calls: list[Path] = []

    def fake_pyiceberg(record_type, current, namespace, path, *, work_dir, resources):
        saved = json.loads((tmp_path / "comments-publish-diagnostic.json").read_text())
        assert saved["phase"] == "native_copy"
        assert saved["copy_backend"] == "pyiceberg"
        calls.append(path)
        return path

    monkeypatch.setattr(iceberg, "_copy_pinned_iceberg_snapshot", fake_pyiceberg)
    resources = ExportResources()
    with guard.copy_diagnostics(args, {"snapshot": subject}):
        iceberg._copy_pinned_iceberg_snapshot(
            COMMENT,
            pair,
            "default_native",
            tmp_path / "comments.parquet",
            work_dir=tmp_path,
            resources=resources,
        )
    assert iceberg._copy_pinned_iceberg_snapshot is fake_pyiceberg
    assert iceberg._copy_pinned_iceberg_snapshot is not original
    assert calls == [tmp_path / "comments.parquet"]
    saved = json.loads((tmp_path / "comments-publish-diagnostic.json").read_text())
    assert saved["phase"] == "validated_no_upload"
    assert saved["native_copy_calls"] == 1
    assert saved["copy_backend"] == "pyiceberg"
    assert saved["copy_transaction_pair"] == asdict(pair)


def test_comments_mirror_workflow_syncs_comments_reader_frozen() -> None:
    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/_comments-mirror.yml").read_text())
    publish_runs = "\n".join(step.get("run", "") for step in workflow["jobs"]["publish"]["steps"])
    assert "uv sync --frozen --extra operational-runner --extra comments-reader" in publish_runs
    qualify_text = json.dumps(workflow["jobs"]["qualify-debugger"])
    assert "secrets." not in qualify_text and "R2_" not in qualify_text
