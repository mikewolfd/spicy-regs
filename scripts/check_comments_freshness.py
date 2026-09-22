#!/usr/bin/env python3
"""Flag staleness between ``comments_index`` and the row-level ``comments`` surface.

The comment *counts* (``comments_index``, ``feed_summary``, ``agency_stats``) and
the row-level ``comments`` table are produced by different steps. If they drift —
the index advertises comments for a month the row data doesn't actually contain —
then count queries look current while ``SELECT ... FROM comments`` returns nothing
for recent dockets. That exact gap (OMB-2026-0034 showed ~39k June-2026 comments
in the index but zero rows in ``comments``) is what this check catches.

It runs two checks, each of which can independently flag:

1. Freshness — per ``agency_code``, the latest ``(year, month)`` in
   ``comments_index`` vs the latest ``posted_date`` month actually materialized in
   the ``comments`` read surface; flags agencies whose rows lag the index.
2. Uniqueness — per ``agency_code``, ``count(*)`` vs ``count(distinct comment_id)``
   in the ``comments`` surface; flags agencies carrying duplicate rows (which
   inflate counts and repeat comments — the one-time seed did this for agencies it
   loaded more than once, and the freshness check alone can't see it).

Both run against whatever the MCP server is configured to read: the R2 Data
Catalog (Iceberg) when ``R2_CATALOG_*`` is set, otherwise the public monolithic
``comments.parquet``. So they validate the *actual* surface users query.

Usage:
    uv run python scripts/check_comments_freshness.py
    uv run python scripts/check_comments_freshness.py --agency OMB
    uv run python scripts/check_comments_freshness.py --limit 50

Exit code is 1 when any agency is flagged (so it can drive a cron/CI alert), 0
otherwise.
"""

from __future__ import annotations

import argparse
import sys

from spicy_regs import mcp_server

# Per-agency, one pass over the comments rows: the index's latest (year, month)
# as YYYYMM vs the latest posted_date month actually present, plus the row and
# distinct-id counts so the uniqueness check needs no second scan. A frugal
# aggregation — MAX/COUNT are streaming and the GROUP BY is ~hundreds of
# agencies — so it stays well within memory even over tens of millions of rows.
_FRESHNESS_SQL = """
WITH idx AS (
    SELECT agency_code,
           MAX(year * 100 + month) AS idx_max_ym,
           CAST(SUM(row_count) AS BIGINT) AS idx_rows
    FROM comments_index
    {idx_where}
    GROUP BY agency_code
),
rows AS (
    SELECT agency_code,
           MAX(
               EXTRACT(YEAR FROM CAST(posted_date AS TIMESTAMP)) * 100
               + EXTRACT(MONTH FROM CAST(posted_date AS TIMESTAMP))
           ) AS rows_max_ym,
           COUNT(*) AS actual_rows,
           COUNT(DISTINCT comment_id) AS distinct_ids
    FROM comments
    WHERE agency_code IS NOT NULL
    {rows_where}
    GROUP BY agency_code
)
SELECT i.agency_code,
       i.idx_max_ym,
       r.rows_max_ym,
       i.idx_rows,
       COALESCE(r.actual_rows, 0) AS actual_rows,
       COALESCE(r.distinct_ids, 0) AS distinct_ids
FROM idx i
LEFT JOIN rows r USING (agency_code)
ORDER BY i.idx_rows DESC
"""


def _fmt_ym(ym: int | None) -> str:
    if ym is None:
        return "—"
    return f"{ym // 100:04d}-{ym % 100:02d}"


def _check_comments(con, idx_where: str, rows_where: str, limit: int) -> tuple[bool, bool]:
    """One pass reports both freshness lag and duplicate rows. Returns (stale, duplicated)."""
    rows = con.execute(_FRESHNESS_SQL.format(idx_where=idx_where, rows_where=rows_where)).fetchall()

    stale_rows = [
        r
        for r in rows
        if (r[4] == 0 and r[2] is None)  # the index knows the agency; comment rows are absent
        or (r[1] is not None and (r[2] is None or r[2] < r[1]))  # rows absent a month or lag the index
    ]
    duplicated_rows = sorted((r for r in rows if r[4] > r[5]), key=lambda r: r[4] - r[5], reverse=True)
    total_dupes = sum(r[4] - r[5] for r in duplicated_rows)

    print(f"STALE: {len(stale_rows)} agency partition(s) lag the comments index\n")
    print(f"{'agency':<10} {'index_to':<9} {'rows_to':<9} {'index_rows':>12} {'actual_rows':>12}")
    print("-" * 56)
    for agency, idx_ym, rows_ym, idx_rows, actual_rows, _distinct in stale_rows[:limit]:
        print(f"{agency:<10} {_fmt_ym(idx_ym):<9} {_fmt_ym(rows_ym):<9} {idx_rows:>12,} {actual_rows:>12,}")
    if len(stale_rows) > limit:
        print(f"... and {len(stale_rows) - limit} more (raise --limit to see them)")

    if duplicated_rows:
        print(f"\nDUPLICATED: {len(duplicated_rows)} agency(ies) carry duplicate comment rows ({total_dupes:,} duplicate rows)\n")
        print(f"{'agency':<10} {'rows':>14} {'distinct_ids':>14} {'factor':>8}")
        print("-" * 48)
        for agency, _idx, _rows_ym, _idx_rows, rows_n, distinct in duplicated_rows[:limit]:
            factor = rows_n / distinct if distinct else 0
            print(f"{agency:<10} {rows_n:>14,} {distinct:>14,} {factor:>7.2f}x")
        if len(duplicated_rows) > limit:
            print(f"... and {len(duplicated_rows) - limit} more (raise --limit to see them)")
    else:
        print("OK (uniqueness): every agency's comment rows are unique by comment_id.")

    return bool(stale_rows), bool(duplicated_rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--agency", help="Restrict the check to a single agency_code")
    parser.add_argument("--limit", type=int, default=100, help="Max flagged agencies to print (default 100)")
    args = parser.parse_args()

    if args.agency:
        safe = args.agency.replace("'", "''")
        idx_where = f"WHERE agency_code = '{safe}'"
        rows_where = f"AND agency_code = '{safe}'"
    else:
        idx_where = ""
        rows_where = ""

    # A one-off script that owns and closes its own connection — use the builder
    # directly, not the shared cached connection the MCP tools reuse.
    con = mcp_server._build_connection()
    try:
        stale, duplicated = _check_comments(con, idx_where, rows_where, args.limit)
    finally:
        con.close()

    return 1 if (stale or duplicated) else 0


if __name__ == "__main__":
    sys.exit(main())
