"""Native catalog initialization and schema admission have no legacy migration path."""
import duckdb
import pytest
from spicy_regs.schemas import COMMENT, DOCUMENT
from spicy_regs.sources import iceberg
from spicy_regs.sources import regulatory_catalog as native


@pytest.mark.parametrize('record_type', [COMMENT, DOCUMENT])
def test_uninitialized_native_rows_are_not_selected(record_type):
    with duckdb.connect() as connection:
        connection.execute("ATTACH ':memory:' AS reg_catalog")
        connection.execute(f'CREATE SCHEMA reg_catalog."{native.namespace()}"')
        connection.execute(f'CREATE TABLE {native.qualified(record_type)} ({native._ddl(native.policy(record_type.name).subject_schema)})')
        with pytest.raises(ValueError, match='initialization receipt'):
            native.processing_table(connection, record_type)
        native.ensure_native(connection, record_type)
        assert native.initialized(connection, record_type.name)
        assert connection.execute(f'SELECT count(*) FROM {native.processing_table(connection, record_type)}').fetchone() == (0,)


def test_wrong_existing_native_schema_refuses_without_repair():
    with duckdb.connect() as connection:
        connection.execute("ATTACH ':memory:' AS reg_catalog")
        native.ensure_native(connection, COMMENT)
        connection.execute(f'ALTER TABLE {native.qualified(COMMENT)} ADD COLUMN obsolete_raw VARCHAR')
        with pytest.raises(ValueError, match='schema differs'):
            native.ensure_native(connection, COMMENT)
        assert 'obsolete_raw' in iceberg._column_types(connection, COMMENT)
