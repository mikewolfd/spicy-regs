#!/usr/bin/env python3
"""Migrate comments.parquet → partitioned comments structure.

Reads the monolithic comments.parquet and writes it into Hive-partitioned
files at:

    comments/agency_code={A}/docket_id={D}/year={Y}/month={M}/part-0.parquet

Also builds the comments_index.parquet used by the frontend and feed summary.
NULL posted dates use Hive null year/month partitions and remain NULL in the index.
Malformed dates and missing or unsafe agency/docket coordinates refuse before writes.

Usage:
    uv run python scripts/migrate_comments_partitioned.py [--output-dir output]
"""

from pathlib import Path
import sys

import duckdb
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.comment_partitions import comment_partition_path, validate_comment_coordinates


def validate_partition_coordinates(comments_file: Path) -> None:
    """Refuse malformed coordinates while retaining source NULL posted dates."""
    with duckdb.connect() as con:
        con.execute("SET memory_limit='4GB'")
        con.execute("SET threads=2")
        path = str(comments_file).replace("'", "''")
        validate_comment_coordinates(con, f"SELECT * FROM read_parquet('{path}')")


def migrate(output_dir: Path) -> None:
    """Write every partition, then rebuild the comments index.

    Validates coordinates before creating anything and exits 1 when
    ``comments.parquet`` is missing.
    """
    comments_file = output_dir / "comments.parquet"
    if not comments_file.exists():
        logger.error("comments.parquet not found in {}", output_dir)
        sys.exit(1)

    # Run before creating or replacing anything: the old WHERE clause silently
    # omitted rows with a native NULL date while still reporting success.
    validate_partition_coordinates(comments_file)
    comments_dir = output_dir / "comments"
    comments_dir.mkdir(parents=True, exist_ok=True)

    total_rows = pq.ParquetFile(comments_file).metadata.num_rows
    logger.info("Migrating {:,} rows from comments.parquet...", total_rows)

    # Get the list of columns from the file
    schema = pq.read_schema(comments_file)
    col_names = [f.name for f in schema]
    col_select = ", ".join(f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in col_names)

    # Discover all partitions
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con.execute(f"SET temp_directory='{spill_dir}'")

    logger.info("Discovering partitions...")
    partitions = con.execute(f"""
        SELECT DISTINCT
            agency_code,
            TRIM(docket_id, '"') AS docket_id,
            EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP))::INT AS year,
            EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))::INT AS month
        FROM read_parquet('{comments_file}')
    """).fetchall()

    logger.info("Found {:,} partitions to write", len(partitions))

    # Write each partition
    written_files: list[Path] = []

    for i, (agency, docket, year, month) in enumerate(partitions):
        partition_file = comment_partition_path(comments_dir, agency, docket, year, month)
        partition_file.parent.mkdir(parents=True, exist_ok=True)

        docket_escaped = str(docket).replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT {col_select}
                FROM read_parquet('{comments_file}')
                WHERE agency_code = '{agency}'
                  AND TRIM(docket_id, '"') = '{docket_escaped}'
                  AND EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP)) IS NOT DISTINCT FROM {"NULL" if year is None else year}
                  AND EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP)) IS NOT DISTINCT FROM {"NULL" if month is None else month}
                ORDER BY posted_date
            ) TO '{partition_file}'
            (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

        written_files.append(partition_file)
        if len(written_files) % 1000 == 0:
            logger.info("  written {:,}/{:,} partitions", len(written_files), len(partitions))

    con.close()

    logger.info("Written {:,} partition files", len(written_files))

    # Build the index
    logger.info("Building comments index...")
    from spicy_regs.transforms import update_comments_index

    index_path = update_comments_index(output_dir, written_files)
    logger.info("Index written to {}", index_path)

    logger.info("Migration complete!")
    logger.info("You can now upload with:")
    logger.info("  uv run python -m spicy_regs.sources.r2 {}", output_dir / "comments_index.parquet")
    logger.info("  And upload the comments/ directory to R2")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Migrate comments to partitioned format")
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    args = parser.parse_args()
    migrate(args.output_dir)
