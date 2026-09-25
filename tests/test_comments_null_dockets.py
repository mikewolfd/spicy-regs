"""Source-null docket links survive ingestion, retries, counts and publication."""

from hashlib import sha256
import json
from pathlib import Path

import duckdb
import polars as pl
import pyarrow.parquet as pq
import pytest

from spicy_regs.comments_health import check_comments
from spicy_regs.pipelines import regulations
from spicy_regs.schemas import COMMENT, DOCKET
from spicy_regs.sources import iceberg
from spicy_regs.transforms import merge_comments_partitioned, update_comments_index, write_staging
from spicy_regs.transforms.build_agency_stats import build_agency_stats
from spicy_regs.transforms.build_feed_summary import build_feed_summary
from spicy_regs.transforms.comment_partitions import HIVE_NULL
from tests.test_regulations_pipeline import _FakeS3Resource


FIXTURES = Path(__file__).parent / "fixtures/comments_null_dockets"


def reviewed_sources():
    store = {}
    rows = []
    for receipt in json.loads((FIXTURES / "source-receipts.json").read_text()):
        raw = (FIXTURES / receipt["retained_file"]).read_bytes()
        assert sha256(raw).hexdigest() == receipt["sha256"]
        store[receipt["source"].removeprefix("s3://mirrulations/")] = raw
        rows.append(COMMENT.extract(json.loads(raw)))
    return store, rows


@pytest.mark.parametrize("mode", ["parquet", "catalog", "chunked"])
def test_reviewed_comments_survive_ingestion_and_public_mirror(tmp_path, monkeypatch, mode):
    store, expected = reviewed_sources()
    assert all(row["docket_id"] is None for row in expected)
    monkeypatch.setattr(regulations.mirrulations, "s3_resource", lambda: _FakeS3Resource(store))
    monkeypatch.setattr(regulations.r2, "download", lambda *args, **kwargs: False)

    def connect(rt):
        con = duckdb.connect()
        con.execute(f"ATTACH '{tmp_path / 'catalog.duckdb'}' AS {iceberg._CATALOG_ALIAS}")
        iceberg._ensure_table(con, rt)
        return con

    monkeypatch.setattr(iceberg, "_connect_for_table", connect)
    output = tmp_path / "output"
    for _ in range(2):
        regulations.RegulationsPipeline(
            output_dir=output, agency="ODNI", only_comments=True, allow_fresh_start=True,
            use_iceberg=mode != "parquet", chunk_size=1 if mode == "chunked" else 0,
            enrich_text=False, skip_upload=True,
        ).run()
        assert set(pl.read_parquet(output / "manifest.parquet")["key"]) == set(store)
        if mode == "parquet":
            index = pl.read_parquet(output / "comments_index.parquet")
            assert index["row_count"].sum() == len(expected)
            assert index["docket_id"].null_count() == len(index)
        else:
            assert not (output / "comments_index.parquet").exists()

    with connect(COMMENT) as con:
        if mode == "parquet":
            pattern = str(output / "comments/agency_code=*/docket_id=*/year=*/month=*/part-0.parquet")
            assert iceberg.seed_comments_from_parquet(con, pattern, COMMENT) == len(expected)
        got = con.execute(f"SELECT * FROM {iceberg._qualified(COMMENT)}").pl()
        assert got.sort("comment_id").to_dicts() == sorted(expected, key=lambda row: row["comment_id"])
    monkeypatch.setattr(iceberg, "_connect", lambda: connect(COMMENT))
    monkeypatch.setattr(iceberg, "_read_snapshot", lambda *a: iceberg.CatalogSnapshot("local", 1, 0))
    monkeypatch.setattr(iceberg, "_snapshot_query", lambda rt, _: f"SELECT * FROM {iceberg._qualified(rt)}")
    result = iceberg.export_public_comments(output, COMMENT)

    # Exercise the same per-agency files and count check used by publication.
    directory = result["partitions"]
    with duckdb.connect() as con:
        mirrored = f"SELECT * FROM read_parquet('{directory}/agency_code=*/part-0.parquet', hive_partitioning=true)"
        index_sql = f"SELECT * FROM read_parquet('{output / 'comments_index.parquet'}')"
        assert check_comments(con, mirrored, index_sql) == []
        assert con.execute(mirrored).pl().select(got.columns).sort("comment_id").to_dicts() == got.sort("comment_id").to_dicts()

    # An agency with no docket records still gets its comments counted. A
    # docket-only feed cannot manufacture docket rows for these comments.
    pl.DataFrame(schema=DOCKET.schema).write_parquet(output / "dockets.parquet")
    assert pl.read_parquet(build_agency_stats(output)).to_dicts() == [
        {"agency_code": "ODNI", "docket_count": 0, "document_count": 0, "comment_count": len(expected)}
    ]
    assert pl.read_parquet(build_feed_summary(output)).is_empty()


def test_null_partition_merge_index_and_source_correction_are_retryable(tmp_path):
    _, rows = reviewed_sources()
    rows.append({**rows[0], "comment_id": "known-docket", "docket_id": "ODNI-known"})
    rows.append({**rows[0], "comment_id": "unknown-date", "posted_date": None})
    stage = tmp_path / "stage"
    output = tmp_path / "output"
    write_staging("ODNI", "comments", rows, stage, COMMENT.schema)
    for attempt, correction in enumerate([False, False, True, True]):
        changed = merge_comments_partitioned(
            stage, output, COMMENT.schema, COMMENT.dedup_key,
            source_correction=correction, download_existing=False,
        )
        index = update_comments_index(output, changed)
        assert pl.read_parquet(index)["row_count"].sum() == len(rows)
        assert any(f"docket_id={HIVE_NULL}" in str(path) for path in changed)
        got = [row for path in changed for row in pq.ParquetFile(path).read().to_pylist()]
        assert sorted(got, key=lambda row: row["comment_id"]) == sorted(rows, key=lambda row: row["comment_id"])
        if attempt == 1:
            rows[0]["title"] = "Corrected source title at the same source timestamp"
            write_staging("ODNI", "comments", rows, stage, COMMENT.schema)

    # A known/unknown relationship change is still a move across partitions;
    # bounded corrections must refuse it without leaving an old duplicate.
    before = {path: path.read_bytes() for path in output.rglob("*.parquet")}
    rows[0]["docket_id"] = "ODNI-new"
    write_staging("ODNI", "comments", rows, stage, COMMENT.schema)
    with pytest.raises(ValueError, match="cannot relocate"):
        merge_comments_partitioned(
            stage, output, COMMENT.schema, COMMENT.dedup_key, source_correction=True, download_existing=False,
        )
    assert {path: path.read_bytes() for path in output.rglob("*.parquet")} == before
