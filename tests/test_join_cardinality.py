"""Full identity matching and raw-row amplification are distinct from key coverage."""
from dataclasses import replace

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.check_table_joins import measure
from spicy_regs.table_joins import JOINS, Join


@pytest.mark.parametrize("columns, child_key, parent_keys", [
    (("bill_id","version_code","source"), ("119-hr-1","ih","govinfo"),
     [("119-hr-1","ih","govinfo"),("119-hr-1","ih","congress")]),
    (("package_id","part_id"), ("CRPT-119hrpt1","1"),
     [("CRPT-119hrpt1","1"),("CRPT-119hrpt2","1")]),
    (("congress","chamber","event_id"), ("119","house","1"),
     [("119","house","1"),("118","house","1"),("119","senate","1")]),
])
def test_reused_component_does_not_amplify_full_key(tmp_path, columns, child_key, parent_keys):
    paths = {}
    for name, rows in [("child",[child_key,child_key]),("parent",parent_keys)]:
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({c:[row[i] for row in rows] for i,c in enumerate(columns)}),path)
        paths[name]=[str(path)]
    join = Join("child",columns,"parent",columns,1,0,expected_cardinality="one")
    with duckdb.connect() as con:
        result=measure(con,join,paths.__getitem__)
        assert result["keys"]==1 and result["missing"]==0
        assert result["child_input_rows"]==result["inner_join_rows"]==2
        assert result["max_parent_multiplicity"]==1
        # Removing the scope/provider column recreates a many-match reference.
        short = replace(join,child_columns=columns[:-1],parent_columns=columns[:-1])
        if columns[0]=="package_id":
            short=replace(join,child_columns=("part_id",),parent_columns=("part_id",))
        elif columns[0]=="congress":
            short=replace(join,child_columns=("event_id",),parent_columns=("event_id",))
        amplified=measure(con,short,paths.__getitem__)
        assert amplified["status"]=="MULTIPLICITY"
        assert amplified["inner_join_rows"]>result["inner_join_rows"]


def test_full_selected_receipts_cover_new_keys_and_populated_attributes():
    measured=[j for j in JOINS if j.measurement]
    assert {j.child for j in measured} >= {"bill_sections","section_diffs","report_sections","hearing_transcripts",
                                         "document_attributes","docket_attributes"}
    for join in measured:
        assert join.measurement is not None
        assert join.expected_cardinality=="one"
        assert join.measurement["scope"]=="full_selected_inputs"
        assert (join.baseline_keys, join.baseline_missing) == (
            join.measurement["keys"], join.measurement["missing"])
        assert 0 <= join.baseline_missing <= join.baseline_keys
        assert join.kind in {"complete", "scope", "design", "empty"}
        assert 0 <= join.measurement["max_parent_multiplicity"] <= 1
        assert join.measurement["parent_duplicate_keys"] == 0
        assert all('/generations/' in url for key in ['child_urls','parent_urls'] for url in join.measurement[key])
