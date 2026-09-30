#!/usr/bin/env python3
"""Verify comments uniqueness and agency/docket/month coverage against the public index.

Check public files anonymously and, when explicitly requested, the raw catalog.
A deduplicating consumer view cannot establish physical catalog integrity.
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from time import monotonic
from urllib.parse import quote

import duckdb

from spicy_regs.comments_health import check_comments, check_retained_ids
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.public_url import resolve_r2_base_url
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg


@contextmanager
def _stage(label: str):
    """Keep completed stages visible if the runner later times out."""
    started = monotonic()
    print(f"START {label}", flush=True)
    yield
    print(f"DONE {label}: {monotonic() - started:.1f}s", flush=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", help="Restrict to one agency")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--surface", choices=("public", "catalog", "both"), default="public")
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit must be positive")
    base = resolve_r2_base_url().rstrip('/').replace("'", "''")
    where = " WHERE agency_code = '" + args.agency.replace("'", "''") + "'" if args.agency else ""
    index = f"SELECT * FROM read_parquet('{base}/comments_index.parquet'){where}"
    surfaces = ("public", "catalog") if args.surface == "both" else (args.surface,)
    failed = False
    for surface in surfaces:
        con = None
        try:
            with _stage(f"{surface}: connect"):
                con = iceberg._connect() if surface == "catalog" else duckdb.connect()
            # Both surfaces read the public index (and the public surface the 4 GB comments.parquet) over r2.dev,
            # which answers 429 under load: ETL 36389677283's verify step failed on one.
            load_public_http(con)
            con.execute("SET memory_limit='4GB'; SET threads=2; SET preserve_insertion_order=false")
            if surface == "catalog":
                if iceberg.dedupe_recovery_pending(con, RECORD_TYPES["comments"]):
                    raise RuntimeError("Unfinished comments dedupe; catalog is not ready")
                source = iceberg._qualified(RECORD_TYPES["comments"])
            else:
                source = f"read_parquet('{base}/comments.parquet')"
            with _stage(f"{surface}: IDs and index coverage"):
                errors = check_comments(con, f"SELECT * FROM {source}{where}", index, limit=args.limit)
            if surface == "public":
                agencies = [row[0] for row in con.execute(f"SELECT DISTINCT agency_code FROM ({index})").fetchall()]
                if not agencies or any(agency is None for agency in agencies):
                    raise RuntimeError("Public comments index has no valid agency inventory")
                urls = [f"{resolve_r2_base_url().rstrip('/')}/comments/agency/agency_code={quote(agency, safe='')}/part-0.parquet"
                        for agency in agencies]
                con.from_parquet(urls, hive_partitioning=True).create_view("public_partitions")
                partitions = "SELECT * FROM public_partitions"
                with _stage("public: agency partition IDs and index coverage"):
                    errors += check_comments(con, partitions, index, limit=args.limit)
                with _stage("public: retained IDs in agency partitions"):
                    errors += check_retained_ids(con, f"SELECT * FROM {source}{where}", partitions)
            for error in errors:
                print(f"FAIL {surface}: {error}", flush=True)
            if not errors:
                print(f"OK {surface}: unique IDs and complete index coverage", flush=True)
            failed |= bool(errors)
        except (duckdb.Error, RuntimeError, OSError) as exc:
            print(f"FAIL {surface}: verification incomplete: {exc}", flush=True)
            failed = True
        finally:
            if con is not None:
                con.close()
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
