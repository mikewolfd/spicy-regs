"""The catalog replacement probe's own logic, against a local stand-in catalog.

The integration workflow runs it against the real catalog; this pins that the
probe's expectations match the write path and that it always drops its table.
"""

import duckdb
import pytest

from scripts import probe_catalog_replace as probe
from spicy_regs.sources import iceberg


@pytest.fixture
def connect():
    base = duckdb.connect()
    base.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS}")
    try:
        yield base.cursor
    finally:
        base.close()


def _tables(connect) -> list[str]:
    return [row[0] for row in connect().execute(
        "SELECT table_name FROM duckdb_tables() WHERE database_name = ?", [iceberg._CATALOG_ALIAS]).fetchall()]


def test_probe_passes_every_check_and_drops_its_table(connect):
    assert probe.run_probe(connect, "probe_catalog_replace_test") == [
        "insert", "scoped update with expected prior and replay", "changed prior refused",
        "post-MERGE failure rolled back",
    ]
    assert _tables(connect) == []


def test_probe_drops_its_table_when_a_check_fails(connect, monkeypatch):
    monkeypatch.setattr(iceberg, "replace_rows", lambda *args, **kwargs: None)
    with pytest.raises(AssertionError):
        probe.run_probe(connect, "probe_catalog_replace_test")
    assert _tables(connect) == []


def test_probe_requires_credentials_only_when_asked(monkeypatch):
    monkeypatch.setattr(iceberg, "is_configured", lambda: False)
    monkeypatch.setattr("dotenv.load_dotenv", lambda *args, **kwargs: False)
    assert probe.main([]) == 0
    assert probe.main(["--require"]) == 1


def test_probe_refuses_existing_native_storage_without_deleting_it(connect, monkeypatch):
    from spicy_regs.schemas import COMMENT
    from spicy_regs.sources import regulatory_catalog
    monkeypatch.setenv('R2_CATALOG_NAMESPACE', 'probe_catalog_replace_held')
    with connect() as con:
        regulatory_catalog.ensure_native(con, COMMENT)
        before = con.execute(f'SELECT * FROM {regulatory_catalog.receipts_table()}').fetchall()
    with pytest.raises(ValueError, match='already contains data'):
        probe.run_probe(connect, 'probe_catalog_replace_held')
    with connect() as con:
        assert con.execute(f'SELECT * FROM {regulatory_catalog.receipts_table()}').fetchall() == before


def test_probe_refuses_production_namespace_before_connecting():
    with pytest.raises(ValueError, match='disposable'):
        probe.run_probe(lambda: pytest.fail('production namespace was opened'), 'default')
