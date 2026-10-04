"""Native migration qualifies PDF diagnostics without adding them to subjects."""

import duckdb
import pytest

from spicy_regs.schemas import COMMENT, DOCUMENT
from spicy_regs.sources import iceberg
from spicy_regs.sources.regulatory_catalog import processing_table, receipts_table

FIELD = "pdf_extraction_results_json"


@pytest.fixture
def catalog(tmp_path, monkeypatch):
    database = tmp_path / "catalog.duckdb"

    def connect():
        con = duckdb.connect()
        con.execute(f"ATTACH '{database}' AS {iceberg._CATALOG_ALIAS}")
        return con

    monkeypatch.setattr(iceberg, "_connect", connect)
    return connect


@pytest.mark.parametrize("record_type", [DOCUMENT, COMMENT])
def test_missing_pdf_diagnostics_migrate_as_unread_with_paired_receipts(catalog, record_type):
    with catalog() as con:
        con.execute(f"CREATE SCHEMA {iceberg._schema_ref()}")
        columns = [name for name in record_type.schema if name != FIELD]
        ddl = ", ".join(f'"{name}" VARCHAR' for name in columns)
        con.execute(f'CREATE TABLE {iceberg._schema_ref()}."{record_type.name}" ({ddl})')
        con.execute(
            f'INSERT INTO {iceberg._schema_ref()}."{record_type.name}" ("{record_type.dedup_key}") VALUES (\'kept\')'
        )
    with iceberg._connect_for_table(record_type) as con:
        assert FIELD not in iceberg._column_types(con, record_type)
        assert con.execute(f"SELECT {FIELD} FROM {processing_table(con, record_type)}").fetchall() == [(None,)]
        assert con.execute(f"SELECT count(*) FROM {receipts_table()}").fetchone()[0] == 1
    with iceberg._connect_for_table(record_type) as con:
        assert con.execute(f"SELECT count(*) FROM {receipts_table()}").fetchone()[0] == 1


@pytest.mark.parametrize("bad_type", ["INTEGER", "BOOLEAN"])
def test_wrong_legacy_diagnostic_type_refuses_without_migrating(catalog, bad_type):
    with catalog() as con:
        con.execute(f"CREATE SCHEMA {iceberg._schema_ref()}")
        con.execute(f"CREATE TABLE {iceberg._schema_ref()}.comments (comment_id VARCHAR, {FIELD} {bad_type})")
        con.execute(f"INSERT INTO {iceberg._schema_ref()}.comments (comment_id) VALUES ('kept')")
    with pytest.raises(ValueError, match="incompatible types"):
        iceberg._connect_for_table(COMMENT)
    with catalog() as con:
        assert con.execute(f"SELECT comment_id FROM {iceberg._schema_ref()}.comments").fetchall() == [("kept",)]
        assert not con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='default_native'"
        ).fetchall()


def test_migration_transaction_refusal_keeps_old_rows(catalog, monkeypatch):
    with catalog() as con:
        con.execute(f"CREATE SCHEMA {iceberg._schema_ref()}")
        con.execute(f"CREATE TABLE {iceberg._schema_ref()}.comments (comment_id VARCHAR)")
        con.execute(f"INSERT INTO {iceberg._schema_ref()}.comments VALUES ('kept')")
    from spicy_regs.sources import regulatory_catalog

    real_stage = regulatory_catalog._stage

    def refused(*args, **kwargs):
        real_stage(*args, **kwargs)
        raise duckdb.Error("source conversion refused")

    monkeypatch.setattr(regulatory_catalog, "_stage", refused)
    with pytest.raises(duckdb.Error, match="source conversion refused"):
        iceberg._connect_for_table(COMMENT)
    with catalog() as con:
        assert con.execute(f"SELECT * FROM {iceberg._schema_ref()}.comments").fetchall() == [("kept",)]
        assert not con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='default_native'"
        ).fetchall()
