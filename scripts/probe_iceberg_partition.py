#!/usr/bin/env python3
"""Probe the R2 Data Catalog for partitioned Iceberg table support (one-shot, safe).

Creates a scratch table ``probe_iceberg_partition_<pid>`` in the catalog's
namespace with ``PARTITION BY (agency_code)``, inserts a few rows, reads them
back through an agency predicate, checks the plan carries the predicate, and
drops the table in a ``finally`` block. Prints PASS/FAIL and exits nonzero on
failure. Loads ``.env`` beside the checkout; with no credentials it prints SKIP
and exits 0 so a bare local run never fails the gate.
"""

from __future__ import annotations

import os
import sys

from spicy_regs.sources import iceberg


def main() -> int:
    from dotenv import load_dotenv

    load_dotenv()
    if not iceberg.is_configured():
        print("SKIP: R2 Data Catalog is not configured (missing env vars)")
        return 0
    table = f"probe_iceberg_partition_{os.getpid()}"
    qualified = f'{iceberg._CATALOG_ALIAS}."{iceberg._namespace()}"."{table}"'
    con = None
    try:
        con = iceberg._connect()
        con.execute(
            f'CREATE TABLE {qualified} '
            f'("comment_id" VARCHAR, "agency_code" VARCHAR, "posted_date" VARCHAR) '
            f'PARTITION BY ("agency_code");'
        )
        con.execute(
            f"INSERT INTO {qualified} VALUES "
            f"('c1','EPA','2025-01-01'),('c2','EPA','2025-01-02'),('c3','FAA','2025-02-01');"
        )
        rows = con.execute(
            f"SELECT comment_id FROM {qualified} WHERE agency_code = 'EPA' ORDER BY comment_id"
        ).fetchall()
        assert [r[0] for r in rows] == ["c1", "c2"], rows
        plan = str(con.execute(f"EXPLAIN SELECT * FROM {qualified} WHERE agency_code = 'EPA'").fetchall())
        pruned = "agency_code" in plan
        print(f"PASS: partitioned create/insert/read round trip; predicate in plan: {pruned}")
        return 0 if pruned else 1
    except Exception as error:  # noqa: BLE001 — report and fail; the scratch table is dropped below
        print(f"FAIL: {error!r}")
        return 1
    finally:
        if con is not None:
            try:
                con.execute(f"DROP TABLE IF EXISTS {qualified};")
            except Exception:  # noqa: BLE001 — cleanup is best-effort by design
                pass
            con.close()


if __name__ == "__main__":
    sys.exit(main())
