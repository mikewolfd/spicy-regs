"""Shared comments partition rules that preserve unknown source dates."""

from pathlib import Path

import duckdb


HIVE_NULL = "__HIVE_DEFAULT_PARTITION__"


def validate_comment_coordinates(con: duckdb.DuckDBPyConnection, source_sql: str) -> None:
    """Validate a trusted internal source query before any partition/index writes.

    NULL posted dates are source facts and use Hive's null partition marker.
    Malformed or nonfinite non-NULL dates and unsafe/missing identities refuse.
    """
    result = con.execute(f"""
        SELECT count(*) FROM ({source_sql})
        WHERE (posted_date IS NOT NULL AND (
                  try_cast(posted_date AS TIMESTAMP) IS NULL
                  OR NOT isfinite(try_cast(posted_date AS TIMESTAMP))))
           OR agency_code IS NULL OR docket_id IS NULL
           OR NOT regexp_full_match(agency_code, '[A-Za-z0-9_-]+')
           OR NOT regexp_full_match(trim(docket_id, '"'), '[A-Za-z0-9_-]+')
    """).fetchone()
    assert result is not None
    if result[0]:
        raise ValueError(f"cannot partition comments: {result[0]:,} rows have missing or invalid coordinates")


def comment_partition_path(comments_dir: Path, agency: str, docket: str, year: int | None, month: int | None) -> Path:
    """Use standard Hive NULL markers without inventing a source date."""
    return (
        comments_dir
        / f"agency_code={agency}"
        / f"docket_id={docket}"
        / f"year={HIVE_NULL if year is None else year}"
        / f"month={HIVE_NULL if month is None else month}"
        / "part-0.parquet"
    )


def partition_date_value(value: str) -> int | None:
    """Decode Hive's marker as a real NULL in the numeric index column."""
    return None if value == HIVE_NULL else int(value)
