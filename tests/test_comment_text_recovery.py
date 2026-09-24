"""Text failures survive raw checkpoints and fresh hosts; retries preserve other fields."""

import json

import duckdb
import polars as pl
import pytest

from spicy_regs import manifest as manifest_module
from spicy_regs.pipelines import regulations
from spicy_regs.pipelines.comment_text import PENDING_TEXT_FILE, PendingCommentText
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, r2
from spicy_regs.sources.derived_text import DerivedTextUnavailable
from spicy_regs.transforms.comment_partitions import comment_partition_path
from spicy_regs.transforms.derived_text_pool import DerivedTextPool, TextResult
from tests.test_backfill_derived_text import _seed_catalog
from tests.test_derived_text import ACF, _FakeS3Resource, _store, derived_key
from tests.test_regulations_pipeline import _comment_key, _comment_payload


@pytest.mark.parametrize("mode", ["parquet", "catalog", "chunked"])
def test_text_retry_after_manifest_on_fresh_host(tmp_path, monkeypatch, mode):
    identity = f"{ACF[1]}-0004"
    raw_key = _comment_key(identity, ACF[1], agency=ACF[0])
    payload = _comment_payload(identity, ACF[1], "2025-01-01")
    payload["data"]["attributes"].update(agencyId=ACF[0], title="Preserved source title")
    payload["data"]["relationships"] = {"attachments": {"data": [{"id": "a", "type": "attachments"}]}}
    payload["included"] = [{"id": "a", "type": "attachments", "attributes": {
        "title": "Attachment", "fileFormats": [{"fileUrl": "https://example.org/a.pdf", "format": "pdf", "size": 10}],
    }}]
    store = _store() | {raw_key: json.dumps(payload).encode()}
    failed = True
    gets = []

    class Resource(_FakeS3Resource):
        def Object(self, name, key):  # noqa: N802
            gets.append(key)
            obj = super().Object(name, key)
            if failed and key.startswith("derived-data/"):
                obj.e_tag = '"changed-after-listing"'
            return obj

    remote = {}
    uploads = []

    def download(key, path):
        if key not in remote:
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(remote[key])
        return True

    def upload(path, remote_key=None):
        key = remote_key or path.name
        uploads.append(key)
        remote[key] = path.read_bytes()

    monkeypatch.setattr(regulations.mirrulations, "s3_resource", lambda: Resource(store))
    monkeypatch.setattr(r2, "download_from_r2", download)
    monkeypatch.setattr(manifest_module, "download_from_r2", download)
    monkeypatch.setattr(r2, "preflight_uploads", lambda *args: None)
    monkeypatch.setattr(r2, "upload_file", upload)

    def connect(rt):
        con = duckdb.connect()
        con.execute(f"ATTACH '{tmp_path / 'catalog.duckdb'}' AS {iceberg._CATALOG_ALIAS}")
        iceberg._ensure_table(con, rt)
        return con

    monkeypatch.setattr(iceberg, "_connect_for_table", connect)

    def run(directory):
        regulations.RegulationsPipeline(
            output_dir=directory, agency=ACF[0], only_comments=True,
            allow_fresh_start=True, use_iceberg=mode != "parquet", chunk_size=1 if mode == "chunked" else 0,
            text_workers=2, skip_upload=False,
        ).run()

    first = tmp_path / "first"
    run(first)
    assert raw_key in set(pl.read_parquet(first / "manifest.parquet")["key"])
    pending = pl.read_parquet(first / PENDING_TEXT_FILE).to_dicts()
    assert pending[0]["comment_id"] == identity
    assert pending[0]["phase"] == "fetch" and pending[0]["attempts"] == 1
    assert json.loads(pending[0]["source_json"])["attachments"][0]["etag"]
    assert uploads[-1] == "manifest.parquet"
    assert uploads.index(PENDING_TEXT_FILE) < uploads.index("manifest.parquet")

    # The raw object is deliberately unavailable now: successful text recovery
    # must use the checkpoint and persisted row, never re-ingest the source JSON.
    del store[raw_key]
    failed = False
    gets.clear()
    second = tmp_path / "second"
    run(second)
    assert raw_key not in gets
    assert derived_key(*ACF, "pypdf", "0004", 1) in gets
    assert pl.read_parquet(second / PENDING_TEXT_FILE).is_empty()
    if mode == "parquet":
        rows = pl.read_parquet(list((second / "comments").rglob("part-0.parquet")), hive_partitioning=False)
    else:
        with connect(COMMENT) as con:
            rows = con.execute(f"SELECT * FROM {iceberg._qualified(COMMENT)}").pl()
    row = rows.to_dicts()[0]
    assert row["text_content"] == "Wisconsin DCF comment body"
    assert row["text_extraction_status"] == "derived"
    assert row["title"] == "Preserved source title"
    assert row["modify_date"] == payload["data"]["attributes"]["modifyDate"]
    third = tmp_path / "third"
    third.mkdir()
    assert PendingCommentText(third).rows == {}


def test_retry_keeps_other_agencies_and_current_text_and_survives_failed_write(tmp_path, monkeypatch):
    monkeypatch.setattr(r2, "download", lambda *args: False)
    pending = PendingCommentText(tmp_path)
    rows = [{"agency_code": ACF[0], "docket_id": ACF[1], "comment_id": f"{ACF[1]}-{suffix}",
             "text_content": text, "text_extraction_status": status, "posted_date": "2025-01-01"}
            for suffix, text, status in [("0004", None, None), ("0015", "PDF result", "ok")]]
    other = {**rows[0], "agency_code": "EPA", "docket_id": "EPA-2026-0001", "comment_id": "EPA-2026-0001-0001"}
    for row in [*rows, other]:
        pending.observe(TextResult(row, "failed", error=DerivedTextUnavailable("transport", phase="fetch")))
    pending.save()
    # A chunk may commit after a successful inline fetch whose metadata loses
    # deduplication. Its old retry must survive a crash before the final upsert.
    pending = PendingCommentText(tmp_path)
    pending.observe(TextResult(rows[0], "derived"))
    pending.save()
    assert rows[0]["comment_id"] in PendingCommentText(tmp_path).rows
    before = pending.path.read_bytes()

    def connect(rt):
        con = duckdb.connect()
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        _seed_catalog(con, rows)
        return con

    monkeypatch.setattr(iceberg, "_connect_for_table", connect)
    real_upsert = iceberg.upsert_comment_text

    def refuse(*args):
        raise RuntimeError("catalog unavailable")

    monkeypatch.setattr(iceberg, "upsert_comment_text", refuse)
    with DerivedTextPool(lambda: _FakeS3Resource(_store()), max_workers=2) as pool:
        with pytest.raises(RuntimeError, match="catalog unavailable"):
            PendingCommentText(tmp_path).retry(pool, tmp_path, [ACF[0]], use_iceberg=True)
    assert pending.path.read_bytes() == before

    verified = []

    def upsert(con, rt, agency, updates):
        real_upsert(con, rt, agency, updates)
        verified.extend(con.execute(f"SELECT comment_id, text_content FROM {iceberg._qualified(rt)}").fetchall())

    monkeypatch.setattr(iceberg, "upsert_comment_text", upsert)
    with DerivedTextPool(lambda: _FakeS3Resource(_store()), max_workers=2) as pool:
        resumed = PendingCommentText(tmp_path)
        # A raw row can lose the metadata merge by modify_date. Its successful
        # inline fetch must not retire a prior retry until persisted text exists.
        resumed.observe(TextResult(rows[0], "derived"))
        resumed.retry(pool, tmp_path, [ACF[0]], use_iceberg=True)
        resumed.save()
    assert dict(verified)[rows[1]["comment_id"]] == "PDF result"
    assert dict(verified)[rows[0]["comment_id"]] == "Wisconsin DCF comment body"
    assert set(resumed.rows) == {other["comment_id"]}


def test_inline_access_refusal_never_advances_either_checkpoint(tmp_path, monkeypatch):
    from spicy_docs.sources.mirrulations import MirrulationsAccessRefusedError

    row = _comment_payload(f"{ACF[1]}-0004", ACF[1], "2025-01-01", agency=ACF[0])
    row["included"] = [{"id": "a", "type": "attachments", "attributes": {
        "fileFormats": [{"fileUrl": "https://example.org/a.pdf", "format": "pdf"}],
    }}]
    key = _comment_key(row["data"]["id"], ACF[1], agency=ACF[0])
    raw = _FakeS3Resource({key: json.dumps(row).encode()})
    monkeypatch.setattr(r2, "download_from_r2", lambda *args: False)
    monkeypatch.setattr(manifest_module, "download_from_r2", lambda *args: False)
    # Pool resources are constructed before the agency reader's resource.
    resources = iter([_FakeS3Resource(_store(), listing_status=403), raw])
    monkeypatch.setattr(regulations.mirrulations, "s3_resource", lambda: next(resources))
    with pytest.raises(MirrulationsAccessRefusedError):
        regulations.RegulationsPipeline(
            output_dir=tmp_path, agency=ACF[0], only_comments=True, allow_fresh_start=True, text_workers=1,
        ).run()
    assert not (tmp_path / "manifest.parquet").exists()
    assert not (tmp_path / PENDING_TEXT_FILE).exists()
    assert not (tmp_path / "comments").exists()


@pytest.mark.parametrize("use_iceberg", [False, True])
def test_null_docket_retry_matches_only_the_same_relationship(tmp_path, monkeypatch, use_iceberg):
    monkeypatch.setattr(r2, "download", lambda *args: False)
    actual = [
        {**dict.fromkeys(COMMENT.schema), "comment_id": identity, "agency_code": "ODNI",
         "docket_id": docket, "text_content": "Already recovered", "text_extraction_status": "ok"}
        for identity, docket in [("unchanged", None), ("moved", "ODNI-known")]
    ]
    pending = PendingCommentText(tmp_path)
    for row in actual:
        pending.observe(TextResult({**row, "docket_id": None}, "failed", error=DerivedTextUnavailable("retry", phase="fetch")))
        path = comment_partition_path(tmp_path / "comments", "ODNI", row["docket_id"], None, None)
        path.parent.mkdir(parents=True, exist_ok=True)
        pl.DataFrame([row], schema=COMMENT.schema).write_parquet(path)
    pending.save()

    def connect(rt):
        con = duckdb.connect()
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        _seed_catalog(con, actual)
        return con

    monkeypatch.setattr(iceberg, "_connect_for_table", connect)
    with DerivedTextPool(lambda: _FakeS3Resource({}), max_workers=1) as pool:
        resumed = PendingCommentText(tmp_path)
        assert resumed.retry(pool, tmp_path, ["ODNI"], use_iceberg=use_iceberg) == []
        resumed.save()
    assert set(resumed.rows) == {"moved"}
    assert pl.read_parquet(list((tmp_path / "comments").rglob("part-0.parquet")), hive_partitioning=False).sort(
        "comment_id"
    ).to_dicts() == sorted(actual, key=lambda row: row["comment_id"])
