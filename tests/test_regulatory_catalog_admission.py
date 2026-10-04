"""Native catalog admission is enforced by the real subject/receipt writer."""
from dataclasses import replace

import duckdb

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg


def test_unregistered_fixture_table_keeps_generic_catalog_algorithm():
    fixture = replace(COMMENT, name='fixture_comments')
    with duckdb.connect() as connection:
        connection.execute("ATTACH ':memory:' AS reg_catalog")
        iceberg._ensure_table(connection, fixture)
        assert connection.execute('SELECT count(*) FROM reg_catalog.default.fixture_comments').fetchone() == (0,)
