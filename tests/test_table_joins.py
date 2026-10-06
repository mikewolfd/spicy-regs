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


@pytest.mark.parametrize("child", ["fec_legal_parties", "fec_legal_events", "fec_legal_documents"])
def test_legal_child_joins_name_exact_observations_and_zero_orphan_baselines(child):
    """The independent retained-publication measurement admits no orphan or repeated parent key."""
    outgoing = [join for join in table_joins.joins_for(child)["outgoing"] if join["parent"] == "fec_legal_matters"]
    assert len(outgoing) == 1
    join = outgoing[0]
    assert (join["child_columns"], join["parent"], join["parent_columns"]) == (
        ["matter_record_id"], "fec_legal_matters", ["record_id"],
    )
    assert (join["kind"], join["expected_cardinality"]) == ("complete", "one")
    measurement = join["measurement"]
    assert measurement["measured_on"] == "2026-10-02"
    assert measurement["source_generation"] == (
        "sha256:d76de786fccdd502efc8f20b65cc38863cce0084defad63770fb1f2b997a3153"
    )
    assert (measurement["keys"], measurement["missing"], measurement["parent_duplicate_keys"]) == (184, 0, 0)
    assert measurement["child_nonnull_rows"] == measurement["child_input_rows"] == measurement["inner_join_rows"]
    assert "2026-10-02" in join["reason"] and "2026-09-27" not in join["reason"]
    assert join in table_joins.joins_for("fec_legal_matters")["incoming"]


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
    assert {"child_columns": ["docket_id"], "parent_table": "dockets", "parent_columns": ["docket_id"]} in shaped["documents"]
    assert all(set(entry) == {"child_columns", "parent_table", "parent_columns"}
               for entries in shaped.values() for entry in entries)



def test_the_spicy_docs_contracts_reference_the_identity_joins_declared_here():
    """Every provider reference is declared; local declarations may add relationships.

    The producer's schema references are a minimum, not an exhaustive registry
    of relationships between tables built by this repository. Every local
    addition still has schema validation, an operational baseline and live CI.
    """
    from spicy_docs.schemas import TABLE_CONTRACTS

    declared = {(j.child, j.child_columns, j.parent, j.parent_columns): j for j in table_joins.JOINS}
    referenced = {(contract.name, tuple(ref.child_columns), ref.parent_table, tuple(ref.parent_columns))
                  for contract in TABLE_CONTRACTS.values() for ref in contract.references}
    retired = {(j.child, j.child_columns, j.parent, j.parent_columns) for j in table_joins.RETIRED_PROCESSING_JOINS}
    translated = set()
    for child, child_columns, parent, parent_columns in referenced - retired:
        rename = table_joins._NATIVE_JOIN_FIELDS
        translated.add((child, tuple(rename.get((child, c), c) for c in child_columns),
                        parent, tuple(rename.get((parent, c), c) for c in parent_columns)))
    assert translated <= declared.keys()

def test_describe_table_lists_the_joins_a_table_makes_and_receives(monkeypatch):
    described = _tool_data(_serve(monkeypatch, _index(), bundled=True), "describe_table", {"table": "dockets"})
    joins = described["joins"]
    assert {join["child"] for join in joins["incoming"]} >= {"documents", "comments", "rule_targets"}
    assert [join["parent"] for join in joins["outgoing"]] == ["unified_agenda"]
    assert joins["outgoing"][0]["kind"] == "scope" and joins["outgoing"][0]["reason"]
    assert joins["baseline"]["receipts"]
    # A public reply names no path on a maintainer's machine.
    assert not [receipt for receipt in joins["baseline"]["receipts"] if receipt.startswith(("~", "/"))]


def _tables(tmp_path, child_ids, parent_ids) -> dict[str, list[str]]:
    paths = {}
    for name, ids in (("child", child_ids), ("parent", parent_ids)):
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({"key_id": pa.array(ids, pa.string())}), path)
        paths[name] = [str(path)]
    return paths


def _declared(keys: int, missing: int, kind: str = "complete") -> table_joins.Join:
    return table_joins.Join("child", ("key_id",), "parent", ("key_id",), keys, missing, kind, "a reason")


@pytest.mark.parametrize(("child", "declared", "status"), [
    (["a", "b", None], _declared(2, 0), "OK"),
    (["a", "b", "orphan"], _declared(2, 0), "LAG"),
    (["a", "b", "o1", "o2", "o3", "o4"], _declared(2, 0), "BELOW"),
    (["a", "orphan"], _declared(2, 0, "scope"), "LAG"),
    (["a", "o1", "o2", "o3", "o4"], _declared(2, 0, "scope"), "BELOW"),
    (["a", "orphan"], _declared(2, 1, "scope"), "OK"),
    (["a"], _declared(0, 0, "empty"), "UNBASELINED"),
    ([None], _declared(0, 0, "empty"), "EMPTY"),
    ([None], _declared(2, 0), "EMPTIED"),
])
def test_the_live_check_holds_each_join_to_its_floor(tmp_path, child, declared, status):
    paths = _tables(tmp_path, child, ["a", "b"])
    result = live.measure(duckdb.connect(), declared, paths.__getitem__)
    assert result["status"] == status
    if status == "LAG":
        assert result["examples"] == ["orphan"] and result["missing"] == 1
    assert (status in live.FAILING) == (status in {"BELOW", "UNBASELINED", "EMPTIED"})


@pytest.mark.parametrize(("missing", "status"), [(9_216, "LAG"), (9_218, "LAG"), (9_219, "BELOW")])
def test_a_growing_design_join_lags_on_its_rate_not_its_baseline_count(missing, status):
    """court_dockets on 2026-09-26: two new PACER dockets without an opinion took 19.7037% to 19.7003%."""
    declared = _declared(11_475, 9_214, "design")
    assert declared.admitted_missing(11_477) == 9_215
    assert live.verdict(declared, 11_477, missing)["status"] == status


def test_a_composite_join_needs_every_column_to_match(tmp_path):
    child = tmp_path / "child.parquet"
    parent = tmp_path / "parent.parquet"
    pq.write_table(pa.table({"bill_id": ["b1", "b1"], "version_code": ["ih", "enr"]}), child)
    pq.write_table(pa.table({"bill_id": ["b1"], "version_code": ["ih"]}), parent)
    join = table_joins.Join("child", ("bill_id", "version_code"), "parent", ("bill_id", "version_code"), 2, 0)
    result = live.measure(duckdb.connect(), join, {"child": [str(child)], "parent": [str(parent)]}.__getitem__)
    assert (result["status"], result["missing"], result["examples"]) == ("LAG", 1, ["b1|enr"])


def test_the_live_check_names_the_ledger_destination_or_refuses(tmp_path, capsys):
    ledger = tmp_path / "ledger.md"
    ledger.write_text("No destination stated here.\n", encoding="utf-8")
    assert live.main(["--ledger", str(ledger)]) == 2
    assert "public data destinations" in capsys.readouterr().err


def test_the_live_check_reads_rulemaking_tables_through_the_snapshot_pointer(monkeypatch):
    """Managed tables resolve through the index, rulemaking tables through their pointer, the rest to legacy keys."""
    from tests.test_check_ledger_pins import BASE, INDEX, SNAPSHOT, _serve

    monkeypatch.setattr(live.publication, "load_index", lambda url: INDEX)
    _serve(monkeypatch)
    url_of = live.table_urls(BASE + "/")
    assert url_of("rule_targets") == [f"{BASE}/materialized/rulemaking/snapshots/{SNAPSHOT}/rule_targets.parquet"]
    assert url_of("nominations") == [f"{BASE}/{INDEX['families']['nominations']['prefix']}/nominations.parquet"]
    assert url_of("dockets") == [f"{BASE}/dockets.parquet"]
    assert url_of("_proceedings_state") == [f"{BASE}/_proceedings_state.parquet"]  # internal: never a snapshot URL


@pytest.mark.parametrize("child", ["fec_legal_parties", "fec_legal_events", "fec_legal_documents"])
def test_legal_capture_keys_preserve_rows_when_logical_matter_ids_repeat(tmp_path, child):
    join = next(join for join in table_joins.JOINS if join.child == child and join.parent == "fec_legal_matters")
    parent = tmp_path / "matters.parquet"
    children = tmp_path / "children.parquet"
    pq.write_table(pa.table({"record_id": ["capture-1", "capture-2"], "matter_id": ["MUR-8195"] * 2}), parent)
    pq.write_table(pa.table({
        "matter_record_id": ["capture-1", "capture-1", "capture-2"], "matter_id": ["MUR-8195"] * 3,
    }), children)
    paths = {child: [str(children)], "fec_legal_matters": [str(parent)]}
    with duckdb.connect() as con:
        result = live.measure(con, join, paths.__getitem__)
        assert (result["missing"], result["parent_duplicate_keys"]) == (0, 0)
        assert result["child_nonnull_rows"] == result["child_input_rows"] == result["inner_join_rows"] == 3
        unsafe = dataclasses.replace(join, child_columns=("matter_id",), parent_columns=("matter_id",))
        multiplied = live.measure(con, unsafe, paths.__getitem__)
        assert (multiplied["status"], multiplied["inner_join_rows"]) == ("MULTIPLICITY", 6)


@pytest.mark.parametrize(("children", "parents", "status", "missing", "nonnull"), [
    (["a", "orphan"], ["a"], "LAG", 1, 2),
    (["a", None], ["a"], "OK", 0, 1),
    (["a"], ["a", "a"], "MULTIPLICITY", 0, 1),
    ([], ["a"], "EMPTIED", 0, 0),
])
def test_legal_join_measurements_expose_incomplete_or_nonunique_selections(
    tmp_path, children, parents, status, missing, nonnull,
):
    join = next(join for join in table_joins.JOINS if join.child == "fec_legal_parties")
    paths = _tables(tmp_path, children, parents)
    selected = dataclasses.replace(join, child="child", child_columns=("key_id",),
                                   parent="parent", parent_columns=("key_id",))
    with duckdb.connect() as con:
        result = live.measure(con, selected, paths.__getitem__)
    assert (result["status"], result["missing"], result["child_nonnull_rows"]) == (status, missing, nonnull)
    # LAG remains operational tolerance; it does not satisfy the retained zero-orphan acceptance above.
    if status == "LAG":
        assert result["missing"] > 0 and status not in live.FAILING


def test_combining_legal_children_requires_separate_aggregation():
    with duckdb.connect() as con:
        con.execute("CREATE TABLE matters(record_id VARCHAR); INSERT INTO matters VALUES ('capture-1')")
        for table, count in (("parties", 2), ("events", 3), ("documents", 2)):
            con.execute(f"CREATE TABLE {table} AS SELECT 'capture-1' AS matter_record_id FROM range({count})")
        assert con.execute("""
            SELECT count(*) FROM matters m
            JOIN parties p ON p.matter_record_id = m.record_id
            JOIN events e ON e.matter_record_id = m.record_id
            JOIN documents d ON d.matter_record_id = m.record_id
        """).fetchone() == (12,)
        assert con.execute("""
            SELECT p.n, e.n, d.n FROM matters m
            JOIN (SELECT matter_record_id, count(*) n FROM parties GROUP BY ALL) p ON p.matter_record_id = m.record_id
            JOIN (SELECT matter_record_id, count(*) n FROM events GROUP BY ALL) e ON e.matter_record_id = m.record_id
            JOIN (SELECT matter_record_id, count(*) n FROM documents GROUP BY ALL) d ON d.matter_record_id = m.record_id
        """).fetchone() == (2, 3, 2)


def test_mcp_legal_join_discovery_uses_the_generated_registry(monkeypatch):
    from spicy_regs import mcp_server

    monkeypatch.setattr(mcp_server, "_joins", table_joins.joins_record)
    server = _serve(monkeypatch, _index(), bundled=True)
    parent = _tool_data(server, "describe_table", {"table": "fec_legal_matters"})["joins"]
    expected = {"fec_legal_parties", "fec_legal_events", "fec_legal_documents"}
    assert {join["child"] for join in parent["incoming"]} == expected
    for child in expected:
        result = _tool_data(server, "describe_table", {"table": child, "detail": True})
        assert result["available"] is False
        assert result["qualification"]["status"] != "recorded"
        assert result["joins"]["outgoing"] == table_joins.joins_for(child)["outgoing"]


# --------------------------------------------------------------------------- #
# Round 6 (implementer B): the joins the column prose promised, each measured through check_table_joins.
# --------------------------------------------------------------------------- #
def _only_join(child: str, parent: str) -> table_joins.Join:
    (join,) = [join for join in table_joins.SOURCE_JOINS if join.child == child and join.parent == parent]
    return join


def test_lobbyists_join_their_filing_directly_with_a_full_measurement():
    join = _only_join("lobbying_activity_lobbyists", "lobbying_filings")
    assert join.child_columns == ("filing_uuid",) and join.kind == "complete"
    assert (join.baseline_keys, join.baseline_missing) == (1_696_361, 0) and join.measurement is not None


def test_every_fec_typed_table_with_a_collection_names_a_held_collection():
    from spicy_regs.fec_receipt_adapter import processing_declarations
    schemas = {name: item["columns"] for name, item in processing_declarations().items()}
    with_collection = {table for table in dd.FEC_TYPED_TABLES if "collection_id" in dict(schemas[table])}
    declared = {join.child for join in table_joins.SOURCE_JOINS
                if join.parent == "fec_collections" and join.child_columns == ("collection_id",)}
    assert with_collection <= declared
    assert all(_only_join(table, "fec_collections").kind == "complete" for table in with_collection)


@pytest.mark.parametrize(("child", "column", "parent"), [
    ("fec_independent_expenditures", "collection_id", "fec_filing_report_observations"),
    ("fec_reported_financial_summaries", "committee_native_id", "fec_committees"),
    ("fec_reported_financial_summaries", "candidate_native_id", "fec_candidate_history"),
    ("fec_contribution_aggregates", "candidate_native_id", "fec_candidate_history"),
])
def test_the_partial_fec_routes_are_declared_as_scope_joins_with_their_reason(child, column, parent):
    (join,) = [join for join in table_joins.SOURCE_JOINS
               if join.child == child and join.parent == parent and join.child_columns == (column,)]
    assert join.kind == "scope" and join.baseline_missing > 0 and join.reason


def test_a_guarantor_reaches_its_loan_by_its_back_reference():
    join = _only_join("fec_loan_guarantors", "fec_loans")
    assert join.child_columns == ("collection_id", "back_reference_transaction_id")
    assert join.parent_columns == ("collection_id", "transaction_id") and join.baseline_missing == 0


def test_no_join_reason_names_a_path_on_a_maintainers_machine():
    assert [join.name for join in table_joins.SOURCE_JOINS if table_joins.maintainer_path(join.reason)] == []


def test_incoming_measurement_evidence_is_retrievable_from_the_child(monkeypatch):
    from spicy_regs import mcp_server

    monkeypatch.setattr(mcp_server, "_joins", table_joins.joins_record)
    record = table_joins.joins_record()
    for declared in record["joins"]:
        parent = mcp_server._table_joins(declared["parent"], measurements=True)
        incoming = next(j for j in parent["incoming"]
                        if (j["child"], j["child_columns"], j.get("parent_columns", parent.get("incoming_parent_columns"))) ==
                        (declared["child"], declared["child_columns"], declared["parent_columns"]))
        assert parent["incoming_parent"] == declared["parent"]
        if declared["measurement"]:
            detail = incoming["measurement"]
            assert detail["status"] == "see_child_description" and detail["tool"] == "describe_table"
            child = mcp_server._table_joins(detail["arguments"]["table"], measurements=detail["arguments"]["detail"])
            assert declared in child["outgoing"]
        else:
            assert "measurement" not in incoming
        for key in ("kind", "baseline_keys", "baseline_missing"):
            assert incoming[key] == declared[key]
        assert incoming.get("reason", "") == declared["reason"]
        assert incoming.get("floor_pct") == declared["floor_pct"]
        assert incoming.get("measured_via") == declared["measured_via"]
        assert incoming.get("expected_cardinality", "unspecified") == declared["expected_cardinality"]


def test_incoming_parent_keys_are_factored_only_when_every_complete_key_matches(monkeypatch):
    from spicy_regs import mcp_server

    records = [table_joins.record(table_joins.Join("child_a", ("a",), "parent", ("id",), 1, 0)),
               table_joins.record(table_joins.Join("child_b", ("b",), "parent", ("id",), 1, 0))]
    monkeypatch.setattr(mcp_server, "_joins", lambda: {"basis": "test", "baseline": {}, "joins": records})
    shared = mcp_server._table_joins("parent", measurements=True)
    assert shared["incoming_parent_columns"] == ["id"]
    assert all("parent_columns" not in j for j in shared["incoming"])
    records[1]["parent_columns"] = ["alternate_id"]
    mixed = mcp_server._table_joins("parent", measurements=True)
    assert "incoming_parent_columns" not in mixed
    assert [j["parent_columns"] for j in mixed["incoming"]] == [["id"], ["alternate_id"]]
