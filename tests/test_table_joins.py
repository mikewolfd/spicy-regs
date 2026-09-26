"""Pins the declared cross-table joins: valid against the dictionary, bundled fresh, served by MCP, held live."""

import dataclasses

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import check_table_joins as live
from spicy_regs import data_dictionary as dd
from spicy_regs import table_joins
from tests.test_mcp_server import _tool_data
from tests.test_table_qualification import _index, _serve


def test_every_join_names_declared_tables_and_columns():
    assert table_joins.declaration_errors(dd.expected_schemas()) == []


def test_a_join_naming_an_unknown_column_or_missing_its_reason_is_refused(monkeypatch):
    bad = (
        table_joins._join("documents", "no_such_column", "dockets", "docket_id", 1, 0),
        table_joins._join("dockets", "rin", "unified_agenda", "rin", 10, 9, "scope"),
    )
    monkeypatch.setattr(table_joins, "JOINS", bad)
    errors = table_joins.declaration_errors(dd.expected_schemas())
    assert any("documents.no_such_column is not a declared column" in error for error in errors)
    assert any("a scope join must state its reason" in error for error in errors)


def test_a_stale_bundle_fails_the_dictionary_check(tmp_path, monkeypatch, capsys):
    record = tmp_path / "table_joins.json"
    monkeypatch.setattr(table_joins, "RECORD", record)
    record.write_bytes(dd.joins_bytes())
    assert dd.joins_errors() == []
    moved = dataclasses.replace(table_joins.JOINS[0], baseline_missing=1)
    monkeypatch.setattr(table_joins, "JOINS", (moved, *table_joins.JOINS[1:]))
    assert dd.cmd_check(dd.build_parser().parse_args(["check"])) == 1
    assert "table_joins.json is stale" in capsys.readouterr().err


def test_the_bundled_joins_are_a_fresh_build_of_the_declarations():
    assert dd.joins_errors() == []


def test_the_contract_shape_carries_no_measurement():
    shaped = table_joins.references()
    assert shaped["documents"] == [{"child_columns": ["docket_id"], "parent_table": "dockets",
                                    "parent_columns": ["docket_id"]}]
    assert all(set(entry) == {"child_columns", "parent_table", "parent_columns"}
               for entries in shaped.values() for entry in entries)


def test_describe_table_lists_the_joins_a_table_makes_and_receives(monkeypatch):
    described = _tool_data(_serve(monkeypatch, _index(), bundled=True), "describe_table", {"table": "dockets"})
    joins = described["joins"]
    assert {join["child"] for join in joins["incoming"]} >= {"documents", "comments", "rule_targets"}
    assert [join["parent"] for join in joins["outgoing"]] == ["unified_agenda"]
    assert joins["outgoing"][0]["kind"] == "scope" and joins["outgoing"][0]["reason"]
    assert joins["baseline"]["receipts"]


def _tables(tmp_path, child_ids, parent_ids) -> dict[str, str]:
    paths = {}
    for name, ids in (("child", child_ids), ("parent", parent_ids)):
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({"key_id": pa.array(ids, pa.string())}), path)
        paths[name] = str(path)
    return paths


def _declared(keys: int, missing: int, kind: str = "complete") -> table_joins.Join:
    return table_joins.Join("child", ("key_id",), "parent", ("key_id",), keys, missing, kind, "a reason")


@pytest.mark.parametrize(("child", "declared", "status"), [
    (["a", "b", None], _declared(2, 0), "OK"),
    (["a", "b", "orphan"], _declared(2, 0), "BELOW"),
    (["a", "orphan"], _declared(2, 1, "scope"), "OK"),
    (["a"], _declared(0, 0, "empty"), "UNBASELINED"),
    ([None], _declared(0, 0, "empty"), "EMPTY"),
    ([None], _declared(2, 0), "EMPTIED"),
])
def test_the_live_check_holds_each_join_to_its_floor(tmp_path, child, declared, status):
    paths = _tables(tmp_path, child, ["a", "b"])
    result = live.measure(duckdb.connect(), declared, paths.__getitem__)
    assert result["status"] == status
    if status == "BELOW":
        assert result["examples"] == ["orphan"] and result["missing"] == 1
    assert (status in live.FAILING) == (status in {"BELOW", "UNBASELINED", "EMPTIED"})


def test_a_composite_join_needs_every_column_to_match(tmp_path):
    child = tmp_path / "child.parquet"
    parent = tmp_path / "parent.parquet"
    pq.write_table(pa.table({"bill_id": ["b1", "b1"], "version_code": ["ih", "enr"]}), child)
    pq.write_table(pa.table({"bill_id": ["b1"], "version_code": ["ih"]}), parent)
    join = table_joins.Join("child", ("bill_id", "version_code"), "parent", ("bill_id", "version_code"), 2, 0)
    result = live.measure(duckdb.connect(), join, {"child": str(child), "parent": str(parent)}.__getitem__)
    assert (result["status"], result["missing"], result["examples"]) == ("BELOW", 1, ["b1|enr"])


def test_the_live_check_names_the_ledger_destination_or_refuses(tmp_path, capsys):
    ledger = tmp_path / "ledger.md"
    ledger.write_text("No destination stated here.\n", encoding="utf-8")
    assert live.main(["--ledger", str(ledger)]) == 2
    assert "public data destinations" in capsys.readouterr().err
