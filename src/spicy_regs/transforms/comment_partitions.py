"""Shared comments partition rules that preserve unknown source relationships and dates."""

from pathlib import Path

import duckdb


HIVE_NULL = "__HIVE_DEFAULT_PARTITION__"


def validate_staged_comments(staging_dir: Path) -> None:
    """Check all staged comments before any persistent table is merged."""
    files = sorted((staging_dir / "comments").glob("*.parquet"))
    if not files:
        return
    with duckdb.connect() as con:
        con.from_parquet([str(path) for path in files], union_by_name=True).create_view("staged_comments")
        validate_comment_coordinates(con, "SELECT * FROM staged_comments")


def validate_comment_coordinates(con: duckdb.DuckDBPyConnection, source_sql: str) -> None:
    """Validate a trusted internal source query before any partition/index writes.

    NULL docket IDs and posted dates use Hive's null partition marker.
    Comment and agency identities remain required. Non-NULL docket IDs must
    be safe path segments and cannot collide with the reserved NULL marker.
    """
    result = con.execute(f"""
        SELECT count(*) FROM ({source_sql})
        WHERE (posted_date IS NOT NULL AND (
                  try_cast(posted_date AS TIMESTAMP) IS NULL
                  OR NOT isfinite(try_cast(posted_date AS TIMESTAMP))))
           OR comment_id IS NULL OR trim(comment_id) = ''
           OR agency_code IS NULL OR agency_code = '{HIVE_NULL}'
           OR NOT regexp_full_match(agency_code, '[A-Za-z0-9_-]+')
           OR (docket_id IS NOT NULL AND (
               trim(docket_id, '"') = '{HIVE_NULL}'
               OR NOT regexp_full_match(trim(docket_id, '"'), '[A-Za-z0-9_-]+')))
    """).fetchone()
    assert result is not None
    if result[0]:
        raise ValueError(f"cannot partition comments: {result[0]:,} rows have missing or invalid coordinates")


def comment_partition_path(comments_dir: Path, agency: str, docket: str | None, year: int | None, month: int | None) -> Path:
    """Use standard Hive NULL markers without inventing a docket or source date."""
    return (
        comments_dir
        / f"agency_code={agency}"
        / f"docket_id={HIVE_NULL if docket is None else docket}"
        / f"year={HIVE_NULL if year is None else year}"
        / f"month={HIVE_NULL if month is None else month}"
        / "part-0.parquet"
    )


def partition_date_value(value: str) -> int | None:
    """Decode Hive's marker as a real NULL in the numeric index column."""
    return None if value == HIVE_NULL else int(value)
