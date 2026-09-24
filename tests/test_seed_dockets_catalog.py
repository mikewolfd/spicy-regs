"""The seed preflight reads an empty or populated catalog without changing it."""

import duckdb
import polars as pl
import pytest

from scripts import seed_dockets_catalog as seed
from scripts.seed_dockets_catalog import counts
from spicy_regs.schemas import DOCKET
from spicy_regs.sources import iceberg


def test_counts_leave_missing_namespace_and_table_absent(tmp_path):
    source = tmp_path / "dockets.parquet"
    pl.DataFrame({"docket_id": ["EPA-1", "EPA-1", "EPA-2", None]}).write_parquet(source)
    with duckdb.connect() as con:
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        before = con.execute("SELECT * FROM information_schema.schemata").fetchall()
        assert counts(con, str(source)) == (0, 4, 2)
        assert con.execute("SELECT * FROM information_schema.tables").fetchall() == []
        assert con.execute("SELECT * FROM information_schema.schemata").fetchall() == before


def test_counts_preserve_catalog_rows_and_ignore_null_and_repeated_keys(tmp_path):
    source = tmp_path / "dockets.parquet"
    pl.DataFrame({"docket_id": ["EPA-1", "EPA-2", "EPA-2", None]}).write_parquet(source)
    with duckdb.connect() as con:
        con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
        iceberg._ensure_table(con, DOCKET)
        table = iceberg._qualified(DOCKET)
        con.execute(f"INSERT INTO {table} (docket_id, title) VALUES ('EPA-1', 'Newer'), (NULL, 'Unkeyed')")
        before = con.execute(f"SELECT * FROM {table}").fetchall()
        assert counts(con, str(source)) == (2, 4, 1)
        assert con.execute(f"SELECT * FROM {table}").fetchall() == before


@pytest.mark.parametrize("args, source_rows, status", [(["--dry-run"], 279124, 0), ([], 12, 1)])
def test_cli_dry_run_and_short_source_leave_catalog_untouched(tmp_path, monkeypatch, args, source_rows, status):
    catalog = tmp_path / "catalog.duckdb"
    con = duckdb.connect()
    con.execute(f"ATTACH '{catalog}' AS {iceberg._CATALOG_ALIAS}")
    monkeypatch.setattr(iceberg, "_connect", lambda: con)
    monkeypatch.setattr(iceberg, "create_s3_secret", lambda con: None)
    monkeypatch.setattr(seed, "counts", lambda con, uri: (0, source_rows, source_rows))
    monkeypatch.setattr("sys.argv", ["seed_dockets_catalog.py", *args])
    assert seed.main() == status
    with duckdb.connect(str(catalog)) as check:
        assert check.execute("SHOW TABLES").fetchall() == []
        assert check.execute("SELECT schema_name FROM information_schema.schemata WHERE schema_name='default'").fetchall() == []
