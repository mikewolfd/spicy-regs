"""Batch readers of the public endpoint retry a 429 instead of failing the refresh."""

import duckdb
import pytest

from spicy_regs.duckdb_settings import PUBLIC_HTTP_RETRIES, load_public_http


@pytest.mark.integration
def test_load_public_http_applies_the_retry_settings():
    """Live: httpfs installs, and DuckDB reads back each retry setting as set (they exist only once it loads)."""
    with duckdb.connect() as con:
        load_public_http(con)
        applied = dict(con.execute("SELECT name, value FROM duckdb_settings() WHERE name IN "
                                   "('http_retries', 'http_retry_wait_ms', 'http_retry_backoff')").fetchall())
    assert {name: float(value) for name, value in applied.items()} == PUBLIC_HTTP_RETRIES
