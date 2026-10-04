#!/usr/bin/env python3
"""Probe the catalog's checked MERGE replacement in a disposable native namespace (safe, one-shot).

The unit tests run :func:`spicy_regs.sources.iceberg.replace_rows` against an
in-memory DuckDB, which accepts statements the Iceberg engine once refused
(f70e2e1). This drives the same function against the real R2 Data Catalog:
insert, scoped update with an expected prior, refusal of a prior changed by a
second connection, and rollback of an injected failure after the MERGE. Every
outcome is read back on a fresh connection. The native ``comments`` and ``etl_receipts`` tables live in the disposable
``probe_catalog_replace_<run>_native`` namespace and are dropped in ``finally``.
The probe never touches the configured production namespace or its snapshots.

Without catalog credentials it prints SKIP and exits 0, unless ``--require``
is passed (the integration workflow passes it whenever secrets are provided).
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import regulatory_catalog as native
from spicy_regs.sources import iceberg

if TYPE_CHECKING:
    import duckdb


def run_probe(connect: "Callable[[], duckdb.DuckDBPyConnection]", namespace: str) -> list[str]:
    """Exercise the supported comment/receipt writer in an isolated namespace."""
    import re
    if not re.fullmatch(r'probe_catalog_replace_[A-Za-z0-9_]+', namespace):
        raise ValueError('Probe requires an explicit disposable probe_catalog_replace_ namespace')
    previous = os.environ.get('R2_CATALOG_NAMESPACE')
    os.environ['R2_CATALOG_NAMESPACE'] = namespace
    try:
        return _run_native_probe(connect)
    finally:
        if previous is None:
            os.environ.pop('R2_CATALOG_NAMESPACE', None)
        else:
            os.environ['R2_CATALOG_NAMESPACE'] = previous


def _run_native_probe(connect) -> list[str]:
    import polars as pl
    record = COMMENT
    qualified = iceberg._qualified(record)
    passed: list[str] = []

    def read_back():
        with connect() as check:
            checked = native.processing_table(check, record)
            return check.execute(f"SELECT comment_id, agency_code, text_content FROM {checked} ORDER BY comment_id").fetchall()

    con = connect()
    owns_storage = False
    try:
        if native._exists(con, qualified) or native._exists(con, native.receipts_table()):
            raise ValueError('Probe namespace already contains data; use a fresh namespace')
        owns_storage = True
        iceberg._ensure_table(con, record)
        rows = [{**dict.fromkeys(record.schema), 'comment_id': identity, 'agency_code': agency,
                 'modify_date': '2026-01-01', 'text_content': 'v1'} for identity, agency in [('a', 'EPA'), ('b', 'FAA')]]
        con.register('probe_input', pl.DataFrame(rows, schema=record.schema).to_arrow())
        con.execute('CREATE TEMP TABLE probe_fresh AS SELECT * FROM probe_input')
        iceberg.replace_rows(con, record, 'probe_fresh')
        assert read_back() == [('a', 'EPA', 'v1'), ('b', 'FAA', 'v1')]
        passed.append('insert')

        prior = native.processing_table(con, record, where="agency_code='EPA'")
        con.execute(f'CREATE TEMP TABLE probe_prior AS SELECT * FROM {prior}')
        con.execute("CREATE OR REPLACE TEMP TABLE probe_fresh AS SELECT * REPLACE ('v2' AS text_content) FROM probe_prior")
        iceberg.replace_rows(con, record, 'probe_fresh', expected_prior='probe_prior', scope={'agency_code': 'EPA'})
        iceberg.replace_rows(con, record, 'probe_fresh', scope={'agency_code': 'EPA'})
        assert read_back() == [('a', 'EPA', 'v2'), ('b', 'FAA', 'v1')]
        passed.append('scoped update with expected prior and replay')

        prior = native.processing_table(con, record, where="comment_id='a'")
        con.execute(f'CREATE OR REPLACE TEMP TABLE probe_prior AS SELECT * FROM {prior}')
        con.execute("CREATE OR REPLACE TEMP TABLE probe_fresh AS SELECT * REPLACE ('v3' AS text_content) FROM probe_prior")
        with connect() as other:
            selected = native.processing_table(other, record, where="comment_id='a'")
            other.execute(f"CREATE TEMP TABLE concurrent_input AS SELECT * REPLACE ('concurrent' AS text_content) FROM {selected}")
            iceberg.replace_rows(other, record, 'concurrent_input')
        try:
            iceberg.replace_rows(con, record, 'probe_fresh', expected_prior='probe_prior')
        except RuntimeError as error:
            assert 'changed after preparation' in str(error), error
        else:
            raise AssertionError('a prior changed by another connection was overwritten')
        assert read_back() == [('a', 'EPA', 'concurrent'), ('b', 'FAA', 'v1')]
        passed.append('changed prior refused')

        class FailAfterMerge:
            def execute(self, sql, *args, **kwargs):
                result = con.execute(sql, *args, **kwargs)
                if sql.lstrip().startswith('MERGE INTO'):
                    raise RuntimeError('injected failure after MERGE')
                return result

        selected = native.processing_table(con, record)
        con.execute(f"CREATE OR REPLACE TEMP TABLE probe_fresh AS SELECT * REPLACE ('v4' AS text_content) FROM {selected}")
        try:
            iceberg.replace_rows(FailAfterMerge(), record, 'probe_fresh')
        except RuntimeError as error:
            assert 'injected failure' in str(error), error
        else:
            raise AssertionError('the injected failure did not propagate')
        assert read_back() == [('a', 'EPA', 'concurrent'), ('b', 'FAA', 'v1')]
        passed.append('post-MERGE failure rolled back')
        return passed
    finally:
        try:
            if owns_storage:
                con.execute(f'DROP TABLE IF EXISTS {qualified}')
                con.execute(f'DROP TABLE IF EXISTS {native.receipts_table()}')
                con.execute(f'DROP SCHEMA IF EXISTS {iceberg._CATALOG_ALIAS}."{native.namespace()}"')
        finally:
            con.close()


def main(argv: list[str] | None = None) -> int:
    from dotenv import load_dotenv

    parser = argparse.ArgumentParser(description="Probe checked subject/receipt replacement in a disposable native namespace.")
    parser.add_argument("--require", action="store_true", help="fail instead of skipping without credentials")
    args = parser.parse_args(argv)
    load_dotenv()
    if not iceberg.is_configured():
        print(("FAIL" if args.require else "SKIP") + ": R2 Data Catalog is not configured (missing env vars)")
        return 1 if args.require else 0
    run = os.environ.get("GITHUB_RUN_ID", str(os.getpid())) + "_" + os.environ.get("GITHUB_RUN_ATTEMPT", "0")
    try:
        passed = run_probe(iceberg._connect, f"probe_catalog_replace_{run}")
    except Exception as error:  # noqa: BLE001 — report and fail; the scratch subject and receipt tables are dropped in run_probe
        print(f"FAIL: {error!r}")
        return 1
    print("PASS: " + "; ".join(passed))
    return 0


if __name__ == "__main__":
    sys.exit(main())
