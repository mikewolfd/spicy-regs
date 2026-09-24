#!/usr/bin/env python3
"""Seed the Iceberg catalog ``comments`` table from the published comments on R2.

One-time loader for the catalog cutover (see PR #89). The partitioned
``comments/`` tree on R2 is already current; this copies it into the R2 Data
Catalog ``comments`` table so the MCP servers can serve row-level reads from the
catalog instead of the frozen monolithic ``comments.parquet``. The fork never
produced that tree, so ``--source-key comments.parquet`` reads each agency's
rows from the monolithic object instead. Loading per agency keeps memory bounded
and gives progress. After loading it rebuilds ``comments_index.parquet`` from
the catalog so a quick diff against the published index confirms the load is
complete.

Needs both credential sets in the environment:

* R2 S3 keys (to read the source partitions):
  ``R2_ACCESS_KEY_ID``, ``R2_SECRET_ACCESS_KEY``, ``R2_ENDPOINT``,
  ``R2_BUCKET_NAME`` (default ``spicy-regs``)
* R2 Data Catalog creds (to write the table):
  ``R2_CATALOG_URI``, ``R2_CATALOG_WAREHOUSE``, ``R2_CATALOG_TOKEN``
  (+ optional ``R2_CATALOG_NAMESPACE``)

Usage:
    uv run python scripts/seed_comments_catalog.py
    uv run python scripts/seed_comments_catalog.py --agency OMB        # one agency
    uv run python scripts/seed_comments_catalog.py --source-key comments.parquet  # the fork
    uv run python scripts/seed_comments_catalog.py --append            # add to a non-empty table
    uv run python scripts/seed_comments_catalog.py --upload-index      # publish the rebuilt index
"""

from __future__ import annotations

import argparse
import sys
from os import getenv
from pathlib import Path

from dotenv import load_dotenv
from loguru import logger

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, r2

# A monolithic source is read once per agency, and each read touches only the row
# groups whose agency_code range holds that agency. So the whole load costs about
# one pass over the file only while the file is sorted by agency_code; the fork's
# comments.parquet is (195 row groups, one out of order, 329 touches for 133
# agencies on 2026-09-24). Unsorted, it would be read once per agency: refuse.
MAX_SOURCE_PASSES = 2


def row_group_touches(con, source: str, agencies: list[str]) -> tuple[int, int]:
    """``(row groups, row groups read)`` when each agency loads alone, from agency_code stats.

    A row group without statistics counts against every agency, so an
    unreadable footer reads as unsorted rather than as cheap.
    """
    ranges = con.execute(
        f"SELECT stats_min_value, stats_max_value FROM parquet_metadata('{iceberg._sql_str(source)}') "
        "WHERE path_in_schema = 'agency_code'"
    ).fetchall()
    touches = sum(1 for agency in agencies for lo, hi in ranges if lo is None or hi is None or lo <= agency <= hi)
    return len(ranges), touches


def _agencies(con, bucket: str, only: str | None) -> list[str]:
    """Agencies to load, from the published index (or just ``only`` when given)."""
    if only:
        return [only]
    index_uri = f"s3://{bucket}/comments_index.parquet"
    rows = con.execute(
        f"SELECT DISTINCT agency_code FROM read_parquet('{iceberg._sql_str(index_uri)}') "
        "WHERE agency_code IS NOT NULL ORDER BY agency_code"
    ).fetchall()
    return [r[0] for r in rows]


def _expected_counts(con, bucket: str) -> dict[str, int]:
    """Per-agency expected row count, from the published index (sum of row_count).

    Drives the resume skip: an agency whose catalog count already equals its
    expected count is fully loaded, so a re-run leaves it untouched instead of
    re-inserting its (potentially thousands of) partition files.
    """
    index_uri = f"s3://{bucket}/comments_index.parquet"
    rows = con.execute(
        f"SELECT agency_code, CAST(SUM(row_count) AS BIGINT) "
        f"FROM read_parquet('{iceberg._sql_str(index_uri)}') "
        "WHERE agency_code IS NOT NULL GROUP BY agency_code"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


def main() -> int:
    load_dotenv()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", help="Load only this agency_code (default: all)")
    parser.add_argument(
        "--source-key",
        help="Read every agency from this monolithic object (e.g. comments.parquet) "
        "instead of the comments/ partition tree",
    )
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument(
        "--append",
        action="store_true",
        help="Allow loading into a non-empty catalog table (default: refuse). "
        "The load is idempotent per agency (each agency's rows are replaced), "
        "so this is safe for resuming an interrupted run.",
    )
    parser.add_argument(
        "--upload-index",
        action="store_true",
        help="Publish the rebuilt comments_index.parquet to R2 after loading",
    )
    args = parser.parse_args()

    bucket = getenv("R2_BUCKET_NAME", "spicy-regs")

    con = iceberg._connect()  # iceberg + httpfs loaded, catalog attached
    try:
        iceberg.create_s3_secret(con)
        iceberg._ensure_table(con, COMMENT)

        existing = con.execute(f"SELECT count(*) FROM {iceberg._qualified(COMMENT)}").fetchone()[0]
        if existing and not args.append:
            logger.error(
                "Catalog comments table already has {:,} rows. Re-run with --append "
                "to load anyway — the load is idempotent per agency (each agency's "
                "rows are replaced), so this is safe.",
                existing,
            )
            return 1

        agencies = _agencies(con, bucket, args.agency)
        expected = _expected_counts(con, bucket)
        logger.info("Seeding {} agency partition set(s) into the catalog", len(agencies))

        if args.source_key:
            source_uri = f"s3://{bucket}/{args.source_key}"
            groups, touches = row_group_touches(con, source_uri, agencies)
            logger.info(
                "{}: {} row groups; loading {} agencies reads {} of them", source_uri, groups, len(agencies), touches
            )
            if touches > MAX_SOURCE_PASSES * groups:
                logger.error(
                    "{} is not sorted by agency_code: the per-agency loads would read it {:.1f} times over. "
                    "Rewrite it ordered by agency_code first.",
                    source_uri,
                    touches / max(groups, 1),
                )
                return 1

        qualified = iceberg._qualified(COMMENT)
        # Resume check in one grouped scan: the catalog is unpartitioned, so a
        # per-agency count(*) would re-read the whole table once per agency.
        present: dict[str, int] = {}
        if agencies:
            agency_list = ", ".join(f"'{iceberg._sql_str(agency)}'" for agency in agencies)
            present = dict(
                con.execute(
                    f"SELECT agency_code, count(*) FROM {qualified} "
                    f"WHERE agency_code IN ({agency_list}) GROUP BY agency_code"
                ).fetchall()
            )
        loaded = skipped = 0
        for i, agency in enumerate(agencies, 1):
            safe = iceberg._sql_str(agency)
            # Resume skip: if this agency's rows are already fully present
            # (catalog count matches the index), leave it untouched. Each
            # agency loads via a single atomic INSERT, so a nonzero count that
            # matches means it completed — no partial-agency state to worry about.
            want = expected.get(agency)
            have = present.get(agency, 0)
            if want is not None and have == want and have > 0:
                logger.info("  [{}/{}] {}: already loaded ({:,} rows) — skipping", i, len(agencies), agency, have)
                skipped += 1
                continue
            source = (
                f"s3://{bucket}/{args.source_key}"
                if args.source_key
                else f"s3://{bucket}/comments/agency_code={safe}/docket_id=*/year=*/month=*/part-0.parquet"
            )
            try:
                # Replacing makes a re-run idempotent; an agency with no rows
                # yet skips the DELETE, which would scan the whole table.
                total = iceberg.seed_comments_from_parquet(con, source, COMMENT, agency, replace=have > 0)
                loaded += 1
            except Exception as exc:  # noqa: BLE001 — keep going, report at the end
                logger.warning("  [{}/{}] {}: skipped ({})", i, len(agencies), agency, exc)
                continue
            logger.info("  [{}/{}] {}: catalog now holds {:,} rows", i, len(agencies), agency, total)

        total = con.execute(f"SELECT count(*) FROM {qualified}").fetchone()[0]
        logger.info(
            "Load complete: {:,} rows in the catalog comments table ({} agencies loaded, {} already-current skipped)",
            total,
            loaded,
            skipped,
        )

        index_file = iceberg._build_comments_index(con, COMMENT, args.output_dir)
        logger.info("Rebuilt comments index at {}", index_file)
    finally:
        con.close()

    if args.upload_index:
        logger.info("Uploading rebuilt comments index to R2...")
        r2.upload_file(index_file, remote_key="comments_index.parquet")

    logger.info("Done. Verify with: uv run python scripts/check_comments_freshness.py --surface catalog")
    return 0


if __name__ == "__main__":
    sys.exit(main())
