#!/usr/bin/env python3
"""Probe the catalog's checked MERGE replacement on a throwaway table (safe, one-shot).

The unit tests run :func:`spicy_regs.sources.iceberg.replace_rows` against an
in-memory DuckDB, which accepts statements the Iceberg engine once refused
(f70e2e1). This drives the same function against the real R2 Data Catalog:
insert, scoped update with an expected prior, refusal of a prior changed by a
second connection, and rollback of an injected failure after the MERGE. Every
outcome is read back on a fresh connection. The table
``probe_catalog_replace_<run>`` is dropped in ``finally``; the probe never
touches ``comments`` or a published table, so it does not move the snapshot
the mirror export pins.

Without catalog credentials it prints SKIP and exits 0, unless ``--require``
is passed (the integration workflow passes it whenever secrets are provided).
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING

from spicy_regs.schemas import RecordType
from spicy_regs.sources import iceberg

if TYPE_CHECKING:
    import duckdb


def _record_type(name: str) -> RecordType:
    columns = ("id", "agency_code", "modify_date", "value")
    return RecordType(name=name, schema=dict.fromkeys(columns, str), dedup_key="id", extract=lambda raw: raw)


def run_probe(connect: "Callable[[], duckdb.DuckDBPyConnection]", table: str) -> list[str]:
    """Return the passed checks; raise AssertionError on the first failed one."""
    record = _record_type(table)
    qualified = iceberg._qualified(record)
    passed: list[str] = []

    def read_back() -> list[tuple]:
        check = connect()
        try:
            return check.execute(f"SELECT id, agency_code, value FROM {qualified} ORDER BY id, agency_code").fetchall()
        finally:
            check.close()

    con = connect()
    try:
        iceberg._ensure_table(con, record)
        con.execute("CREATE OR REPLACE TEMP TABLE probe_fresh AS SELECT * FROM (VALUES "
                    "('a','EPA','2026-01-01','v1'),('b','FAA','2026-01-01','v1')) t(id,agency_code,modify_date,value)")
        iceberg.replace_rows(con, record, "probe_fresh")
        assert read_back() == [("a", "EPA", "v1"), ("b", "FAA", "v1")], read_back()
        passed.append("insert")

        con.execute(f"CREATE OR REPLACE TEMP TABLE probe_prior AS SELECT * FROM {qualified} WHERE agency_code='EPA'")
        con.execute("CREATE OR REPLACE TEMP TABLE probe_fresh AS SELECT * REPLACE ('v2' AS value) FROM probe_prior")
        iceberg.replace_rows(con, record, "probe_fresh", expected_prior="probe_prior", scope={"agency_code": "EPA"})
        iceberg.replace_rows(con, record, "probe_fresh", scope={"agency_code": "EPA"})  # idempotent replay
        assert read_back() == [("a", "EPA", "v2"), ("b", "FAA", "v1")], read_back()
        passed.append("scoped update with expected prior and replay")

        con.execute(f"CREATE OR REPLACE TEMP TABLE probe_prior AS SELECT * FROM {qualified} WHERE id='a'")
        con.execute("CREATE OR REPLACE TEMP TABLE probe_fresh AS SELECT * REPLACE ('v3' AS value) FROM probe_prior")
        other = connect()
        try:
            other.execute(f"UPDATE {qualified} SET value='concurrent' WHERE id='a'")
        finally:
            other.close()
        try:
            iceberg.replace_rows(con, record, "probe_fresh", expected_prior="probe_prior")
        except RuntimeError as error:
            assert "prior changed" in str(error), error
        else:
            raise AssertionError("a prior changed by another connection was overwritten")
        assert read_back() == [("a", "EPA", "concurrent"), ("b", "FAA", "v1")], read_back()
        passed.append("changed prior refused")

        class FailAfterMerge:
            def execute(self, sql, *args, **kwargs):
                result = con.execute(sql, *args, **kwargs)
                if sql.lstrip().startswith("MERGE INTO"):
                    raise RuntimeError("injected failure after MERGE")
                return result

        con.execute(f"CREATE OR REPLACE TEMP TABLE probe_fresh AS SELECT * REPLACE ('v4' AS value) FROM {qualified}")
        try:
            iceberg.replace_rows(FailAfterMerge(), record, "probe_fresh")
        except RuntimeError as error:
            assert "injected failure" in str(error), error
        else:
            raise AssertionError("the injected failure did not propagate")
        assert read_back() == [("a", "EPA", "concurrent"), ("b", "FAA", "v1")], read_back()
        passed.append("post-MERGE failure rolled back")
        return passed
    finally:
        try:
            con.execute(f"DROP TABLE IF EXISTS {qualified}")
        finally:
            con.close()


def main(argv: list[str] | None = None) -> int:
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description="Probe the catalog's checked MERGE replacement on a throwaway table.")
    parser.add_argument("--require", action="store_true", help="fail instead of skipping without credentials")
    args = parser.parse_args(argv)
    load_dotenv()
    if not iceberg.is_configured():
        print(("FAIL" if args.require else "SKIP") + ": R2 Data Catalog is not configured (missing env vars)")
        return 1 if args.require else 0
    run = os.environ.get("GITHUB_RUN_ID", str(os.getpid())) + "_" + os.environ.get("GITHUB_RUN_ATTEMPT", "0")
    try:
        passed = run_probe(iceberg._connect, f"probe_catalog_replace_{run}")
    except Exception as error:  # noqa: BLE001 — report and fail; the scratch table is dropped in run_probe
        print(f"FAIL: {error!r}")
        return 1
    print("PASS: " + "; ".join(passed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
