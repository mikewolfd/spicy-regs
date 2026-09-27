"""Unknown posted dates survive catalog seeds, the comments index, the agency mirror and consumers."""

import duckdb
import polars as pl
import pyarrow.parquet as pq
import pytest

from spicy_regs.comments_health import check_comments
from spicy_regs.schemas import COMMENT, DOCKET
from spicy_regs.sources import iceberg
from spicy_regs.transforms import write_staging
from spicy_regs.transforms.comment_partitions import validate_staged_comments
from spicy_regs.transforms.partition_comments import partition_comments
from spicy_regs.transforms.build_feed_summary import build_feed_summary
from spicy_regs.transforms.build_agency_stats import build_agency_stats


def comment(identity, date):
    """Build one comment row with schema columns defaulted null and the identity, agency, docket, and dates set."""
    return {
        **dict.fromkeys(COMMENT.schema),
        "comment_id": identity,
        "agency_code": "EPA",
        "docket_id": "EPA-1",
        "posted_date": date,
        "modify_date": "2026-01-01",
    }


def test_unknown_dates_survive_catalog_seed_index_and_agency_mirror(tmp_path):
    """An unknown date keeps its own NULL index group and survives the seed, the rollups and the mirror."""
    output = tmp_path / "output"
    output.mkdir()
    source = tmp_path / "source.parquet"
    pl.DataFrame([comment("unknown", None), comment("known", "2025-01-01")], schema=COMMENT.schema).write_parquet(
        source
    )

    with duckdb.connect() as con:
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        iceberg._ensure_table(con, COMMENT)
        assert iceberg.seed_comments_from_parquet(con, str(source), COMMENT) == 2
        got = con.execute(
            f"SELECT comment_id,posted_date FROM {iceberg._qualified(COMMENT)} ORDER BY comment_id"
        ).fetchall()
        assert got == [("known", "2025-01-01"), ("unknown", None)]
        index = iceberg._build_comments_index(con, COMMENT, output)
        iceberg._export_parquet(con, COMMENT, output)
    assert pl.read_parquet(index)["row_count"].sum() == 2
    assert pl.read_parquet(index).filter(pl.col("year").is_null())["row_count"].to_list() == [1]

    pl.DataFrame(
        [{**dict.fromkeys(DOCKET.schema), "docket_id": "EPA-1", "agency_code": "EPA"}], schema=DOCKET.schema
    ).write_parquet(output / "dockets.parquet")
    assert pl.read_parquet(build_feed_summary(output))["comment_count"].to_list() == [2]
    assert pl.read_parquet(build_agency_stats(output))["comment_count"].to_list() == [2]

    directory = partition_comments(output)
    [part] = directory.glob("agency_code=*/part-0.parquet")
    mirrored = {r["comment_id"]: r["posted_date"] for r in pq.ParquetFile(part).read().to_pylist()}
    assert mirrored == {"known": "2025-01-01", "unknown": None}


@pytest.mark.parametrize("bad", ["not-a-date", "infinity"])
def test_staged_comments_refuse_malformed_dates_before_any_merge(tmp_path, bad):
    stage = tmp_path / "stage"
    write_staging("EPA", "comments", [comment("bad", bad)], stage, COMMENT.schema)
    with pytest.raises(ValueError, match="invalid coordinates"):
        validate_staged_comments(stage)


def test_freshness_handles_unknown_only_agencies_and_still_flags_missing_or_lagging_rows():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE comments (comment_id VARCHAR, agency_code VARCHAR, docket_id VARCHAR, posted_date VARCHAR)")
        con.execute("CREATE TABLE comments_index (agency_code VARCHAR, docket_id VARCHAR, year BIGINT, month BIGINT, row_count BIGINT)")
        con.execute("INSERT INTO comments VALUES ('u','EPA','EPA-1',NULL),('k','FDA','FDA-1','2025-01-01')")
        con.execute("INSERT INTO comments_index VALUES ('EPA','EPA-1',NULL,NULL,1),('FDA','FDA-1',2025,2,1),('MISSING','M-1',NULL,NULL,1)")
        errors = check_comments(con, 'SELECT * FROM comments', 'SELECT * FROM comments_index')
        assert any('FDA/' in error for error in errors)
        assert any('MISSING/' in error for error in errors)
        assert not any('EPA/' in error or 'unique IDs' in error for error in errors)

