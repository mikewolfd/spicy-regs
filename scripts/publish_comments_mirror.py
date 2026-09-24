#!/usr/bin/env python3
"""Publish the public comments read-mirror from the Iceberg catalog.

The browser UI reads comments as public Parquet on R2 (it can't reach the
credentialed R2 Data Catalog). Once the ETL routes comments through Iceberg it
writes rows into the catalog and only republishes the small
``comments_index.parquet`` — so the public surface the UI actually reads goes
stale:

* ``comments.parquet``                                  — the flat monolith (UI full scans)
* ``comments/agency/agency_code={X}/part-0.parquet``    — the per-agency tree (UI agency/docket views)

This regenerates that whole mirror from the catalog (the write-side system of
record) and uploads it, restoring the dual model for comments so the UI serves
current data without ever touching the catalog.

Reads the catalog (``R2_CATALOG_*``) and writes public Parquet (``R2_*``). It is
read-only against the catalog; the only writes are the public mirror files. Runs
after successful ETL publication, plus manual dispatch — see
``.github/workflows/_regulations-refresh.yml``. Before upload it verifies unique
IDs, index and partition coverage, and retention of previously published IDs.

Usage:
    uv run python scripts/publish_comments_mirror.py
    uv run python scripts/publish_comments_mirror.py --skip-upload   # build locally only
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb
import httpx
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.comments_health import check_comments, check_retained_ids
from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.sources import iceberg, r2
from spicy_regs.transforms import partition_comments

# The mirror runs with R2_ALLOW_SHRINK=1 (see the workflow): re-exporting from the
# catalog legitimately changes on-disk size — the sorted, freshly-compressed
# partitions can be several times *smaller* than the old published files while
# holding *more* rows — so r2's byte-size shrink guard produces false positives.
# Identity conservation and index/partition reconciliation replace that check.
# This additional floor catches a catastrophically small export early.
MIN_EXPECTED_ROWS = 1_000_000


def validate_export(output_dir: Path, previous_url: str) -> str:
    """Verify the export and conservation against one unchanged public predecessor.

    Only identity columns are read from the prior mirror. The before/after ETag
    check refuses a predecessor that changed during validation. Scheduled and
    manual publication also share the catalog-writer concurrency group.
    """
    response = httpx.head(previous_url, follow_redirects=True, timeout=60)
    response.raise_for_status()
    etag = response.headers.get("etag")
    if not etag:
        raise RuntimeError("Prior comments mirror has no ETag; cannot verify its population")
    with duckdb.connect() as con:
        con.execute("SET memory_limit='4GB'; SET threads=2; SET preserve_insertion_order=false")
        con.from_parquet(str(output_dir / "comments.parquet")).create_view("candidate")
        con.from_parquet(str(output_dir / "comments_index.parquet")).create_view("candidate_index")
        con.from_parquet(previous_url).create_view("previous")
        errors = check_comments(con, "SELECT * FROM candidate", "SELECT * FROM candidate_index")
        errors += check_retained_ids(con, "SELECT * FROM previous", "SELECT * FROM candidate")
        if errors:
            raise RuntimeError("Refusing comments publication: " + "; ".join(errors))
    response = httpx.head(previous_url, follow_redirects=True, timeout=60)
    response.raise_for_status()
    if response.headers.get("etag") != etag:
        raise RuntimeError("Prior comments mirror changed during validation; retry on a stable predecessor")
    return etag


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument(
        "--skip-upload", action="store_true", help="Build the mirror locally but don't publish to R2"
    )
    args = parser.parse_args()

    if not iceberg.is_configured():
        logger.error("R2 Data Catalog is not configured (R2_CATALOG_*); cannot export the mirror")
        return 1

    output_dir: Path = args.output_dir
    comments_rt = RECORD_TYPES["comments"]

    # 1. Monolith + index straight from the catalog.
    result = iceberg.export_public_comments(output_dir, comments_rt)

    # Refuse a catastrophically small export before scanning identities.
    n_rows = pq.ParquetFile(result["comments"]).metadata.num_rows
    if n_rows < MIN_EXPECTED_ROWS:
        logger.error(
            "Exported monolith has only {:,} rows (< {:,} floor); the catalog read looks "
            "broken — refusing to publish and overwrite the live files",
            n_rows,
            MIN_EXPECTED_ROWS,
        )
        return 1
    logger.info("Exported monolith has {:,} rows", n_rows)
    previous_url = resolve_r2_base_url().rstrip("/") + "/comments.parquet"
    predecessor_etag = validate_export(output_dir, previous_url)
    logger.info("Validated comments identity conservation against {}", predecessor_etag)

    # 2. Derive the per-agency tree the UI reads for scoped queries from that monolith.
    partition_dir = partition_comments(output_dir)
    agency_files = sorted(partition_dir.glob("agency_code=*/part-0.parquet"))
    logger.info("Built {} agency partition(s)", len(agency_files))
    # Reject stale files left by an earlier export, dropped agencies and changed
    # rows at the same total count. Compare the actual files that will be sent.
    with duckdb.connect() as con:
        con.execute("SET memory_limit='4GB'; SET threads=2; SET preserve_insertion_order=false")
        con.from_parquet([str(p) for p in agency_files], hive_partitioning=True).create_view("partitions")
        con.from_parquet(str(result["index"])).create_view("candidate_index")
        errors = check_comments(con, "SELECT * FROM partitions", "SELECT * FROM candidate_index")
        con.from_parquet(str(result["comments"])).create_view("candidate")
        errors += check_retained_ids(con, "SELECT * FROM candidate", "SELECT * FROM partitions")
        if errors:
            raise RuntimeError("Invalid comments partitions: " + "; ".join(errors))

    if args.skip_upload:
        logger.info("--skip-upload set; mirror left in {}", output_dir)
        return 0

    # 3. Publish: monolith, then the partitions + refreshed index.
    response = httpx.head(previous_url, follow_redirects=True, timeout=60)
    response.raise_for_status()
    if response.headers.get("etag") != predecessor_etag:
        raise RuntimeError("Prior comments mirror changed before publication; refusing overwrite")
    r2.upload_file(result["comments"], remote_key="comments.parquet")
    r2.upload_comment_partitions(output_dir, agency_files)
    logger.info("Published comments mirror: monolith + {} partition(s) + index", len(agency_files))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
