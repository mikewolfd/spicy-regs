"""Connection refresh and refusal boundaries for the PDF column migration.

Actual Iceberg DDL and read/write behavior are separately qualified against the
local REST catalog. This fake reproduces its stale connection schema after ADD.
"""

import duckdb
import pytest

from spicy_regs.schemas import COMMENT, DOCUMENT
from spicy_regs.sources import iceberg

FIELD = "pdf_extraction_results_json"


class CachedConnection:
    def __init__(self, field_type, *, refusal=None):
        self.field_type = field_type
        self.refusal = refusal
        self.commands = []
        self.closed = False

    def execute(self, sql):
        assert not self.closed
        self.commands.append(sql)
        if self.refusal is not None and sql.startswith("ALTER TABLE"):
            raise self.refusal
        return self

    def fetchall(self):
        # ADD does not change this cached schema, matching the Iceberg finding.
        return [(FIELD, self.field_type)] if self.field_type is not None else []

    def close(self):
        assert not self.closed
        self.closed = True


@pytest.mark.parametrize("record_type", [DOCUMENT, COMMENT])
def test_added_column_reopens_once_and_only_verifies_on_the_fresh_connection(monkeypatch, record_type):
    old = CachedConnection(None)
    fresh = CachedConnection("VARCHAR")
    connections = iter([old, fresh])
    monkeypatch.setattr(iceberg, "_connect", lambda: next(connections))
    actual = iceberg._connect_for_table(record_type)
    assert actual is fresh
    assert old.closed and not fresh.closed
    assert sum(sql.startswith("ALTER TABLE") for sql in old.commands) == 1
    assert fresh.commands == [f"DESCRIBE {iceberg._qualified(record_type)}"]
    actual.close()


@pytest.mark.parametrize("record_type", [DOCUMENT, COMMENT])
def test_current_table_retains_one_connection_without_alter(monkeypatch, record_type):
    current = CachedConnection("VARCHAR")
    connections = iter([current])
    monkeypatch.setattr(iceberg, "_connect", lambda: next(connections))
    assert iceberg._connect_for_table(record_type) is current
    assert not current.closed
    assert not any(sql.startswith("ALTER TABLE") for sql in current.commands)
    current.close()


def test_ddl_refusal_closes_connection_and_propagates_without_retry(monkeypatch):
    refusal = duckdb.Error("catalog refused schema update")
    old = CachedConnection(None, refusal=refusal)
    connections = iter([old])
    monkeypatch.setattr(iceberg, "_connect", lambda: next(connections))
    with pytest.raises(duckdb.Error) as caught:
        iceberg._connect_for_table(COMMENT)
    assert caught.value is refusal
    assert old.closed


def test_reopen_refusal_leaves_completed_ddl_for_next_run(monkeypatch):
    old = CachedConnection(None)
    refusal = duckdb.Error("catalog temporarily unavailable")
    attempts = 0

    def connect():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            return old
        raise refusal

    monkeypatch.setattr(iceberg, "_connect", connect)
    with pytest.raises(duckdb.Error) as caught:
        iceberg._connect_for_table(COMMENT)
    assert caught.value is refusal
    assert attempts == 2
    assert old.closed
    assert sum(sql.startswith("ALTER TABLE") for sql in old.commands) == 1


@pytest.mark.parametrize("field_type", [None, "INTEGER"])
def test_unexpected_schema_after_reopen_closes_without_a_second_alter(monkeypatch, field_type):
    old = CachedConnection(None)
    fresh = CachedConnection(field_type)
    connections = iter([old, fresh])
    monkeypatch.setattr(iceberg, "_connect", lambda: next(connections))
    with pytest.raises(ValueError, match="must be VARCHAR after migration"):
        iceberg._connect_for_table(COMMENT)
    assert old.closed and fresh.closed
    assert fresh.commands == [f"DESCRIBE {iceberg._qualified(COMMENT)}"]


def test_wrong_existing_type_closes_without_ddl(monkeypatch):
    current = CachedConnection("INTEGER")
    monkeypatch.setattr(iceberg, "_connect", lambda: current)
    with pytest.raises(ValueError, match="must be VARCHAR"):
        iceberg._connect_for_table(COMMENT)
    assert current.closed
    assert not any(sql.startswith("ALTER TABLE") for sql in current.commands)
