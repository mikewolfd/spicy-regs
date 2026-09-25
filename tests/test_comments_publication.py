"""Mirror retry/no-op behavior and the agency-first physical layout."""

from dataclasses import asdict, replace
import hashlib
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines import comments_mirror as mirror
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from spicy_regs.transforms.partition_comments import assemble_comments, sort_comment_agencies, stage_comment_agencies


def build(con, root, resources):
    resources.configure(con, root / "spill")
    from tempfile import TemporaryDirectory
    from pathlib import Path

    with TemporaryDirectory(dir=root) as work:
        staging = Path(work) / "staging"
        stage_comment_agencies(con, "SELECT * FROM source", staging, resources=resources)
        partitions = sort_comment_agencies(staging, root, resources=resources)
    flat = assemble_comments(con, partitions, root, con.sql("SELECT * FROM source").columns, resources=resources)
    index = iceberg._build_comments_index(con, COMMENT, root, source_sql="SELECT * FROM source")
    return {"comments": flat, "partitions": partitions, "index": index}


def test_partitioning_preserves_values_nulls_schema_and_order(tmp_path):
    text = "wide source value " * 150_000
    source = pa.table({"comment_id": ["z", "a", "b", "c"], "agency_code": ["EPA", "EPA", "012", "EPA"],
                       "docket_id": ["EPA-1", "EPA-1", None, None],
                       "posted_date": ["2026-09-01", "2026-09-01", None, None],
                       "text_content": [text, "new", None, "unknown docket"]})
    stale = tmp_path / "comments/agency/agency_code=STALE/part-0.parquet"
    stale.parent.mkdir(parents=True)
    stale.write_bytes(b"stale")
    with duckdb.connect() as con:
        con.register("source", source)
        result = build(con, tmp_path, ExportResources("128MB", 1, row_group_bytes="1MB"))
        flat = pq.read_table(result["comments"])
        assert flat.schema == source.schema
        assert sorted(flat.to_pylist(), key=lambda r: r["comment_id"]) == sorted(source.to_pylist(), key=lambda r: r["comment_id"])
        epa = pq.ParquetFile(result["partitions"] / "agency_code=EPA/part-0.parquet").read()
        assert epa["comment_id"].to_pylist() == ["a", "z", "c"]
        assert "agency_code" not in epa.column_names
        assert not stale.exists()
        assert (result["partitions"] / "agency_code=012/part-0.parquet").exists()


@pytest.mark.parametrize("agency", [None, "__HIVE_DEFAULT_PARTITION__", "../bad"])
def test_invalid_partition_preserves_previous_files(tmp_path, agency):
    old = tmp_path / "comments/agency/agency_code=EPA/part-0.parquet"
    old.parent.mkdir(parents=True)
    old.write_bytes(b"old")
    with duckdb.connect() as con:
        con.register("source", pa.table({"comment_id": ["a"], "agency_code": pa.array([agency], type=pa.string()),
                                         "docket_id": ["EPA-1"], "posted_date": ["2026-09-01"]}))
        with pytest.raises(ValueError, match="invalid coordinates"):
            build(con, tmp_path, ExportResources("64MB", 1))
    assert old.read_bytes() == b"old"
    assert not list(tmp_path.glob("comments-build-*"))


@pytest.fixture
def publication(tmp_path, monkeypatch):
    snapshot = iceberg.CatalogSnapshot("table-uuid", 41, 0)
    state: dict = {"snapshot": snapshot, "objects": {}, "receipt": None, "builds": 0, "uploads": [], "fail": None, "agency": "EPA", "text": "first"}
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "test")
    monkeypatch.setattr(mirror, "resolve_r2_base_url", lambda: "https://public.example")
    monkeypatch.setattr(mirror, "MIN_EXPECTED_ROWS", 1)
    monkeypatch.setattr(iceberg, "catalog_snapshot", lambda _: state["snapshot"])
    monkeypatch.setattr(mirror.r2, "read_json_object", lambda _: state["receipt"])
    monkeypatch.setattr(mirror.r2, "object_version", lambda key: state["objects"].get(key))
    monkeypatch.setattr(mirror.r2, "public_object_version", lambda url: state["objects"].get(url.removeprefix("https://public.example/")))
    state["objects"]["comments.parquet"] = {"etag": "prior", "bytes": 99}
    monkeypatch.setattr(mirror, "validate_export", lambda *a, **kw: mirror.PublicPredecessor(state["objects"]["comments.parquet"]["etag"], frozenset({"EPA"})))
    monkeypatch.setattr(mirror.r2, "preflight_uploads", lambda *a: None)

    def export(root, rt, **kw):
        state["builds"] += 1
        root.mkdir(parents=True, exist_ok=True)
        with duckdb.connect() as con:
            con.register("source", pa.table({"comment_id": ["a"], "agency_code": [state["agency"]], "text_content": [state["text"]],
                                             "docket_id": pa.array([None], type=pa.string()),
                                             "posted_date": pa.array([None], type=pa.string())}))
            return build(con, root, ExportResources("64MB", 1))

    def upload(path, remote_key=None, **kw):
        key = remote_key or path.name
        if state["fail"] == key:
            raise OSError("interrupted upload")
        data = path.read_bytes()
        state["objects"][key] = {"etag": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
        state["uploads"].append(key)
        if key == mirror.RECEIPT_KEY:
            state["receipt"] = json.loads(data)

    def verify(path, key, base_url):
        if state["fail"] == "readback":
            raise OSError("readback failed")
        return {**state["objects"][key], "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}

    def partitions(root, files):
        for path in files:
            upload(path, path.relative_to(root).as_posix())
        upload(root / "comments_index.parquet")

    monkeypatch.setattr(iceberg, "export_public_comments", export)
    monkeypatch.setattr(mirror.r2, "upload_file", upload)
    monkeypatch.setattr(mirror.r2, "upload_comment_partitions", partitions)
    monkeypatch.setattr(mirror.r2, "verify_public_file", verify)
    return state, tmp_path


def test_exact_retry_uses_metadata_and_text_only_change_rebuilds(publication):
    state, root = publication
    assert mirror.publish_comments_mirror(root)
    uploads = list(state["uploads"])
    assert uploads[-1] == mirror.RECEIPT_KEY
    assert state["receipt"]["source"] == asdict(state["snapshot"])
    assert not mirror.publish_comments_mirror(root)
    assert state["builds"] == 1 and state["uploads"] == uploads
    state["snapshot"] = replace(state["snapshot"], snapshot_id=42)
    state["text"] = "repaired attachment text"
    assert mirror.publish_comments_mirror(root)
    assert state["builds"] == 2
    assert pq.read_table(root / "comments.parquet")["text_content"].to_pylist() == [state["text"]]


@pytest.mark.parametrize("change", ["missing", "replaced", "schema", "table", "format", "inventory"])
def test_changed_publication_rebuilds(publication, change):
    state, root = publication
    mirror.publish_comments_mirror(root)
    key = "comments/agency/agency_code=EPA/part-0.parquet"
    if change == "missing":
        del state["objects"][key]
    elif change == "replaced":
        state["objects"][key] = {"etag": "different", "bytes": state["objects"][key]["bytes"]}
    elif change == "schema":
        state["snapshot"] = replace(state["snapshot"], schema_id=1)
    elif change == "table":
        state["snapshot"] = replace(state["snapshot"], table_uuid="new-table")
    elif change == "format":
        state["receipt"]["format_version"] = 0
    elif change == "inventory":
        del state["receipt"]["files"][key]
    assert mirror.publish_comments_mirror(root)
    assert state["builds"] == 2


@pytest.mark.parametrize("failure", ["comments_index.parquet", "readback"])
def test_failed_publication_does_not_advance_receipt_and_retry_finishes(publication, failure):
    state, root = publication
    mirror.publish_comments_mirror(root)
    original = state["receipt"]
    state["snapshot"] = replace(state["snapshot"], snapshot_id=42)
    state["fail"] = failure
    with pytest.raises(OSError):
        mirror.publish_comments_mirror(root)
    assert state["receipt"] is original
    state["fail"] = None
    assert mirror.publish_comments_mirror(root)
    assert state["receipt"]["source"]["snapshot_id"] == 42


def test_skip_upload_builds_without_changing_receipt(publication):
    state, root = publication
    mirror.publish_comments_mirror(root)
    original, uploads = state["receipt"], list(state["uploads"])
    assert mirror.publish_comments_mirror(root, skip_upload=True)
    assert state["builds"] == 2 and state["uploads"] == uploads
    assert state["receipt"] is original
    assert json.loads((root / "comments-build.json").read_text())["source"] == asdict(state["snapshot"])


def test_moving_catalog_refuses_before_upload(publication, monkeypatch):
    state, root = publication
    values = iter([state["snapshot"], replace(state["snapshot"], snapshot_id=42)])
    monkeypatch.setattr(iceberg, "catalog_snapshot", lambda _: next(values))
    with pytest.raises(RuntimeError, match="Catalog changed"):
        mirror.publish_comments_mirror(root)
    assert state["uploads"] == []


def test_force_can_recover_from_an_unreadable_receipt(publication, monkeypatch):
    state, root = publication
    monkeypatch.setattr(mirror.r2, "read_json_object", lambda _: pytest.fail("force read corrupt receipt"))
    assert mirror.publish_comments_mirror(root, force=True)
    assert state["receipt"] is not None


def test_agency_move_clears_old_url_and_keeps_a_valid_receipt(publication):
    state, root = publication
    mirror.publish_comments_mirror(root)
    state["snapshot"] = replace(state["snapshot"], snapshot_id=42)
    state["agency"] = "FDA"
    assert mirror.publish_comments_mirror(root)
    old = "comments/agency/agency_code=EPA/part-0.parquet"
    new = "comments/agency/agency_code=FDA/part-0.parquet"
    assert pq.ParquetFile(root / old).metadata.num_rows == 0
    assert state["receipt"]["files"][old]["rows"] == 0
    assert state["receipt"]["files"][new]["rows"] == 1
    assert not mirror.publish_comments_mirror(root)
