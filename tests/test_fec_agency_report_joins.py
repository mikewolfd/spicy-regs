"""Report editions connect observations without merging or multiplying their rows."""

import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import check_table_joins as live
from spicy_regs import table_joins


GENERATION = "sha256:784b23da0ea9ee7807e09715d38508cd8e0cd654af1047ae6a5b1fc63efb7300"
CHILDREN = [
    ("fec_agency_report_documents", 98, 121),
    ("fec_agency_report_text", 60, 811),
    ("fec_report_metrics", 107, 3287),
]


def _join(child):
    return next(join for join in table_joins.JOINS
                if join.child == child and join.parent == "fec_agency_reports")


@pytest.mark.parametrize("child,keys,rows", CHILDREN)
def test_report_navigation_publishes_full_pinned_measurements_in_both_directions(child, keys, rows):
    outgoing = [join for join in table_joins.joins_for(child)["outgoing"]
                if join["parent"] == "fec_agency_reports"]
    assert len(outgoing) == 1
    join = outgoing[0]
    assert (join["child_columns"], join["parent_columns"], join["kind"],
            join["expected_cardinality"]) == (["report_id"], ["report_id"], "complete", "one")
    assert (join["baseline_keys"], join["baseline_missing"]) == (keys, 0)
    measured = join["measurement"]
    assert (measured["scope"], measured["measured_on"], measured["selected_generation"]) == (
        "full_selected_inputs", "2026-10-06", GENERATION,
    )
    assert (measured["child_input_rows"], measured["child_nonnull_rows"],
            measured["inner_join_rows"], measured["left_join_nonnull_rows"]) == (rows,) * 4
    assert measured["child_repeated_key_rows"] == rows - keys
    assert (measured["missing"], measured["parent_input_rows"], measured["parent_distinct_keys"],
            measured["parent_duplicate_keys"], measured["max_matched_parent_multiplicity"]) == (0, 124, 124, 0, 1)
    for side, name in (("child", child), ("parent", "fec_agency_reports")):
        selected = measured["selected_inputs"][side]
        assert (selected["id"], selected["family"], selected["artifactDigest"]) == (name, "fec-query", GENERATION)
        assert len(selected["members"]) == 1
        member = selected["members"][0]
        assert member["url"] == measured[f"{side}_urls"][0]
        assert member["sha256"].startswith("sha256:") and len(member["sha256"]) == 71
        assert member["byteSize"] > 0
        assert member["rows"] == (rows if side == "child" else 124)
    assert join in table_joins.joins_for("fec_agency_reports")["incoming"]
    bundle = json.loads(table_joins.RECORD.read_text())
    assert [entry for entry in bundle["joins"]
            if entry["child"] == child and entry["parent"] == "fec_agency_reports"] == [join]
    assert join in [entry for entry in bundle["joins"] if entry["parent"] == "fec_agency_reports"]
    assert {"child_columns": ["report_id"], "parent_table": "fec_agency_reports",
            "parent_columns": ["report_id"]} in bundle["references"][child]


@pytest.mark.parametrize("child,keys,rows", CHILDREN)
@pytest.mark.parametrize("children,parents,status,missing,joined", [
    (["edition-1", "edition-1", "edition-2", None], ["edition-1", "edition-2"], "OK", 0, 3),
    (["edition-1", "missing-1", "missing-2", "missing-3", "missing-4"], ["edition-1"], "BELOW", 4, 1),
    (["edition-1", "edition-1"], ["edition-1", "edition-1"], "MULTIPLICITY", 0, 4),
])
def test_report_join_keeps_repeated_observations_and_reports_invalid_parent_selections(
    tmp_path, child, keys, rows, children, parents, status, missing, joined,
):
    paths = {}
    for table, ids in ((child, children), ("fec_agency_reports", parents)):
        path = tmp_path / f"{table}.parquet"
        pq.write_table(pa.table({"report_id": pa.array(ids, pa.string())}), path)
        paths[table] = [str(path)]
    with duckdb.connect() as con:
        result = live.measure(con, _join(child), paths.__getitem__)
    assert (result["status"], result["missing"], result["inner_join_rows"]) == (status, missing, joined)
    assert result["child_input_rows"] == len(children)
    assert result["child_nonnull_rows"] == sum(value is not None for value in children)
    assert result["parent_duplicate_keys"] == (1 if status == "MULTIPLICITY" else 0)
    if status == "OK":
        assert result["inner_join_rows"] == result["child_nonnull_rows"]
    else:
        assert result["status"] in live.FAILING
