"""Inject a DuckDB failure into a bulk loader's result stream after it has produced work."""

from contextlib import contextmanager
from itertools import islice

import duckdb
import pytest

from spicy_regs import etl_bulk

#: An operational failure (FAILED, exit 2) and one the input data caused (REFUSED, exit 1), with what each must raise.
FAILURES = [
    pytest.param(duckdb.OutOfMemoryException("injected"), False, id="operational"),
    pytest.param(duckdb.InvalidInputException("injected"), True, id="invalid-input"),
]


def assert_raised_as(caught, failure, invalid):
    """``caught`` carries ``failure`` as its cause and never asked for a row retry; only invalid input is a refusal."""
    assert caught.value.__cause__ is failure
    assert "no row fallback" in str(caught.value)
    assert isinstance(caught.value, etl_bulk.BulkInputError) is invalid
    assert isinstance(caught.value, ValueError) is invalid


class _Result:
    def __init__(self, result, failure, batches, delivered):
        self._result, self._failure, self._batches, self._delivered = result, failure, batches, delivered

    def __getattr__(self, name):
        return getattr(self._result, name)

    def to_arrow_reader(self, *arguments, **options):
        reader = self._result.to_arrow_reader(*arguments, **options)

        def stream():
            for batch in islice(reader, self._batches):
                self._delivered.append(batch.num_rows)
                yield batch
            raise self._failure

        return stream()


class _Connection:
    def __init__(self, connection, failure, batches, delivered):
        self._connection, self._failure, self._batches, self._delivered = connection, failure, batches, delivered

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def execute(self, *arguments, **options):
        return _Result(self._connection.execute(*arguments, **options), self._failure, self._batches, self._delivered)


def fail_streams_after(monkeypatch, failure, batches=1):
    """Make every result stream a bulk connection opens raise ``failure`` once ``batches`` batches were read.

    Returns the row counts of the batches delivered before the failure, so a test can show that work had been done.
    """
    real, delivered = etl_bulk.bulk_connection, []

    @contextmanager
    def connection(*arguments, **options):
        with real(*arguments, **options) as (con, root):
            yield _Connection(con, failure, batches, delivered), root

    monkeypatch.setattr(etl_bulk, "bulk_connection", connection)
    return delivered
