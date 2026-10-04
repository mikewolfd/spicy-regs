"""Native catalog admission is enforced by the real subject/receipt writer."""
from dataclasses import replace

import duckdb
import pytest

from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg


def test_unregistered_dataset_is_refused_before_catalog_mutation():
    fixture = replace(COMMENT, name='fixture_comments')
    with duckdb.connect() as connection:
        connection.execute("ATTACH ':memory:' AS reg_catalog")
        with pytest.raises(ValueError, match="Unsupported regulatory catalog dataset"):
            iceberg._ensure_table(connection, fixture)
        assert connection.execute("SELECT table_name FROM information_schema.tables WHERE table_catalog='reg_catalog'").fetchall() == []
