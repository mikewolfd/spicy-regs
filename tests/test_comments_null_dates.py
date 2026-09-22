"""Unknown posted dates survive comment partitions, catalog seeds and consumers."""

import duckdb
import polars as pl
import pyarrow.parquet as pq
import pytest

from scripts.check_comments_freshness import _FRESHNESS_SQL
from spicy_regs.schemas import COMMENT, DOCKET
from spicy_regs.sources import iceberg
from spicy_regs.transforms import merge_comments_partitioned, update_comments_index, write_staging
from spicy_regs.transforms.partition_comments import partition_comments
from spicy_regs.transforms.build_feed_summary import build_feed_summary
from spicy_regs.transforms.build_agency_stats import build_agency_stats


def comment(identity, date):
    return {
        **dict.fromkeys(COMMENT.schema),
        "comment_id": identity,
        "agency_code": "EPA",
        "docket_id": "EPA-1",
        "posted_date": date,
        "modify_date": "2026-01-01",
    }


def test_unknown_dates_survive_partition_merge_catalog_seed_and_agency_mirror(tmp_path):
    output = tmp_path / "output"
    output.mkdir()
    stage = tmp_path / "stage"
    rows = [comment("unknown", None), comment("known", "2025-01-01")]
    write_staging("EPA", "comments", rows, stage, COMMENT.schema)
    changed = merge_comments_partitioned(stage, output, COMMENT.schema, COMMENT.dedup_key, download_existing=False)
    assert sum(pq.ParquetFile(p).metadata.num_rows for p in changed) == 2
    # Ordinary retries must also find the unknown-date partition, not add rows.
    changed = merge_comments_partitioned(stage, output, COMMENT.schema, COMMENT.dedup_key, download_existing=False)
    index = update_comments_index(output, changed)
    assert pl.read_parquet(index)["row_count"].sum() == 2
    assert pl.read_parquet(index).filter(pl.col("year").is_null())["row_count"].to_list() == [1]

    pl.DataFrame(
        [{**dict.fromkeys(DOCKET.schema), "docket_id": "EPA-1", "agency_code": "EPA"}], schema=DOCKET.schema
    ).write_parquet(output / "dockets.parquet")
    assert pl.read_parquet(build_feed_summary(output))["comment_count"].to_list() == [2]
    assert pl.read_parquet(build_agency_stats(output))["comment_count"].to_list() == [2]

    with duckdb.connect() as con:
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        iceberg._ensure_table(con, COMMENT)
        pattern = str(output / "comments/agency_code=*/docket_id=*/year=*/month=*/part-0.parquet")
        assert iceberg.seed_comments_from_parquet(con, pattern, COMMENT) == 2
        got = con.execute(
            f"SELECT comment_id,posted_date FROM {iceberg._qualified(COMMENT)} ORDER BY comment_id"
        ).fetchall()
        assert got == [("known", "2025-01-01"), ("unknown", None)]
        iceberg._export_parquet(con, COMMENT, output)
    directory = partition_comments(output)
    [part] = directory.glob("agency_code=*/part-0.parquet")
    mirrored = {r["comment_id"]: r["posted_date"] for r in pq.ParquetFile(part).read().to_pylist()}
    assert mirrored == {"known": "2025-01-01", "unknown": None}


@pytest.mark.parametrize("bad", ["not-a-date", "infinity"])
def test_ordinary_merge_refuses_malformed_dates_before_writes(tmp_path, bad):
    stage = tmp_path / "stage"
    output = tmp_path / "output"
    write_staging("EPA", "comments", [comment("bad", bad)], stage, COMMENT.schema)
    with pytest.raises(ValueError, match="invalid coordinates"):
        merge_comments_partitioned(stage, output, COMMENT.schema, COMMENT.dedup_key, download_existing=False)
    assert not output.exists()


def test_freshness_handles_unknown_only_agencies_and_still_flags_missing_or_lagging_rows():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE comments (comment_id VARCHAR, agency_code VARCHAR, posted_date VARCHAR)")
        con.execute("CREATE TABLE comments_index (agency_code VARCHAR, year BIGINT, month BIGINT, row_count BIGINT)")
        con.execute("INSERT INTO comments VALUES ('u','EPA',NULL),('k','FDA','2025-01-01')")
        con.execute("INSERT INTO comments_index VALUES ('EPA',NULL,NULL,1),('FDA',2025,2,1),('MISSING',NULL,NULL,1)")
        got = con.execute(_FRESHNESS_SQL.format(idx_where="", rows_where="")).fetchall()
        assert {row[0] for row in got} == {"FDA", "MISSING"}


def test_partial_null_partition_coordinates_refuse_index_replacement(tmp_path):
    part = tmp_path / "comments/agency_code=EPA/docket_id=EPA-1/year=__HIVE_DEFAULT_PARTITION__/month=1/part-0.parquet"
    part.parent.mkdir(parents=True)
    pl.DataFrame([comment("unknown", None)], schema=COMMENT.schema).write_parquet(part)
    index = tmp_path / "comments_index.parquet"
    index.write_bytes(b"retained prior index")
    with pytest.raises(ValueError, match="both be NULL"):
        update_comments_index(tmp_path, [part])
    assert index.read_bytes() == b"retained prior index"
