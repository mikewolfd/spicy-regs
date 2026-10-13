"""Actual local SQL schemas stay checked while selected installs avoid repeat reads."""
from dataclasses import replace
from types import SimpleNamespace

import duckdb
import pytest

from spicy_regs import fec_release
from spicy_regs.fec_receipt_adapter import ReceiptAdapter
from spicy_regs.relationship_views.sql_views import SQLView, install_sql_views


class ObservedConnection:
    def __init__(self, connection):
        self.connection = connection
        self.describes = []

    def execute(self, sql, *args):
        if sql.startswith("DESCRIBE "):
            self.describes.append(sql)
        return self.connection.execute(sql, *args)

    def extract_statements(self, sql):
        return self.connection.extract_statements(sql)


def selected(monkeypatch, views, connection, *, prepare=None, refused=()):
    calls = []

    def check(spec, *args):
        calls.append(spec.view.name)
        return {"status": "disabled" if spec.view.name in refused else "compatible",
                "reasons": [{"path": "receipt", "reason": "bad pin"}] if spec.view.name in refused else []}

    monkeypatch.setattr(fec_release, "check_view", check)
    specs = [SimpleNamespace(view=view) for view in views]
    config = {"views": {view.name: {"sql": view.query({})} for view in views}}
    result = fec_release.install_views(
        connection, specs, config, {"families": {}},
        {table for view in views for table in view.required}, {},
        read_tables=lambda con, sql: con.connection.get_table_names(sql), prepare=prepare)
    assert calls == [view.name for view in views]
    return result


def view(name, table="source", columns=("id",)):
    return SQLView(name, {table: columns}, lambda _: f'SELECT id FROM "{table}"', "Synthetic", ("id",))


@pytest.mark.parametrize("adapter", [False, True])
def test_107_dependency_checks_use_25_actual_reads_with_identical_results(monkeypatch, adapter):
    views = []
    for i in range(72):
        tables = [f"t{i % 25}"] + ([f"t{(i + 1) % 25}"] if i < 35 else [])
        sql = f"SELECT a.id FROM {tables[0]} a"
        if len(tables) == 2:
            sql += f" JOIN {tables[1]} b ON a.id=b.id"
        views.append(SQLView(f"v{i}", {t: ("id",) for t in tables}, lambda _, sql=sql: sql,
                             "Synthetic", ("id",)))
    assert sum(len(v.required) for v in views) == 107
    with duckdb.connect(config={"threads": 1}) as raw:
        con = ObservedConnection(raw)
        for i in range(25):
            raw.execute(f"CREATE TABLE t{i} AS SELECT 1 AS id")
        baseline = selected(monkeypatch, views, con, prepare=lambda spec, sql: spec)
        assert len(con.describes) == 107
        con.describes.clear()
        prepare = ReceiptAdapter(con, {"families": {}}, "unused").prepare_qualified if adapter else None
        actual = selected(monkeypatch, views, con, prepare=prepare)
        assert actual == baseline
        assert len(con.describes) == 25
        assert all(item["status"] == "available" for item in actual.values())
        assert raw.execute("SELECT * FROM v71").fetchall() == [(1,)]


def test_schema_observations_do_not_cross_install_or_connection(monkeypatch):
    for columns, expected in [("id INTEGER", "available"), ("other INTEGER", "unsupported")]:
        with duckdb.connect(config={"threads": 1}) as raw:
            raw.execute(f"CREATE TABLE source ({columns})")
            con = ObservedConnection(raw)
            assert selected(monkeypatch, [view("v")], con)["v"]["status"] == expected
            assert len(con.describes) == 1
            selected(monkeypatch, [view("v")], con)
            assert len(con.describes) == 2


@pytest.mark.parametrize("adapted", [False, True])
def test_preparation_replacement_rechecks_actual_physical_columns(monkeypatch, adapted):
    with duckdb.connect(config={"threads": 1}) as raw:
        raw.execute("CREATE TABLE source (id INTEGER)")
        raw.execute("CREATE TABLE private_source (id INTEGER)")
        con = ObservedConnection(raw)
        calls = 0

        def prepare(spec, sql):
            nonlocal calls
            calls += 1
            table = "private_source" if adapted else "source"
            if calls == 2:
                raw.execute(f"DROP TABLE {table}")
                raw.execute(f"CREATE TABLE {table} (other INTEGER)")
            return replace(spec, required={table: ("id",)},
                           query=lambda _: f"SELECT id FROM {table}") if adapted else spec

        if adapted:
            owner = ReceiptAdapter(con, {"families": {"native": {"tables": {}, "etlReceipts": {"datasets": ["source"]}}}},
                                   "unused")
            monkeypatch.setattr(owner, "prepare", prepare)
            selected_prepare = owner.prepare_qualified
        else:
            selected_prepare = prepare
        result = selected(monkeypatch, [view("v1"), view("v2")], con, prepare=selected_prepare)
        assert result["v1"]["status"] == "available"
        assert result["v2"]["status"] == "unsupported"
        assert result["v2"]["release_compatibility"]["status"] == "disabled"
        assert len(con.describes) == 2


def test_replaced_output_is_described_again_before_later_dependency():
    with duckdb.connect(config={"threads": 1}) as raw:
        raw.execute("CREATE TABLE source (id INTEGER)")
        raw.execute("CREATE VIEW changed AS SELECT id FROM source")
        con = ObservedConnection(raw)
        schemas = {}
        assert install_sql_views(con, {"changed"}, [view("first", "changed")], _schemas=schemas)["first"]["status"] == "available"
        replacement = SQLView("changed", {"source": ("id",)}, lambda _: "SELECT id AS other FROM source",
                              "Synthetic replacement", ("other",))
        install_sql_views(con, {"source"}, [replacement], _schemas=schemas)
        result = install_sql_views(con, {"changed"}, [view("last", "changed")], _schemas=schemas)
        assert result["last"]["status"] == "unsupported"
        assert con.describes.count('DESCRIBE "changed"') == 2


def test_failed_preparation_does_not_supply_schema_to_a_later_install(monkeypatch):
    with duckdb.connect(config={"threads": 1}) as raw:
        raw.execute("CREATE TABLE source (id INTEGER)")
        con = ObservedConnection(raw)
        calls = 0

        def prepare(spec, sql):
            nonlocal calls
            calls += 1
            if calls == 2:
                raw.execute("DROP TABLE source")
                raw.execute("CREATE TABLE source (other INTEGER)")
                raise ValueError("preparation failed after replacement")
            return spec

        with pytest.raises(ValueError, match="preparation failed"):
            selected(monkeypatch, [view("v1"), view("v2")], con, prepare=prepare)
        assert selected(monkeypatch, [view("retry")], con)["retry"]["status"] == "unsupported"


def test_bad_receipt_and_missing_table_keep_refusals(monkeypatch):
    with duckdb.connect(config={"threads": 1}) as raw:
        con = ObservedConnection(raw)
        result = selected(monkeypatch, [view("bad")], con, refused={"bad"})
        assert result["bad"]["status"] == "unavailable" and con.describes == []
        with pytest.raises(duckdb.CatalogException):
            selected(monkeypatch, [view("missing")], con)
        assert install_sql_views(con, set(), [view("absent")])["absent"]["status"] == "unavailable"
