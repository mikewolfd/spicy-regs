"""Hermetic tests for the same-case docket groups side table (small synthetic editions)."""
from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.build_court_docket_groups import SCHEMA, build_court_docket_groups, group_members


def _member(docket_id, pacer, name="United States v. Doe"):
    return {"id": docket_id, "pacer_case_id": pacer, "case_name": name}


def test_parent_is_the_numerically_lowest_pacer_id():
    """String order would rank "100000" below "99999"; the parent must be the lower number."""
    assert group_members([_member("a", "100000"), _member("b", "99999")]) == ("b", "doppeldocket")


def test_close_pacer_ids_are_doppeldockets_and_wide_ones_refiled():
    assert group_members([_member("a", "616724"), _member("b", "616726"), _member("c", "616729")]) == ("a", "doppeldocket")
    assert group_members([_member("a", "616724"), _member("b", "616730")]) == ("a", "refiled")


def test_reused_numbers_missing_pacer_ids_and_singletons_stay_ungrouped():
    assert group_members([_member("a", "1", "Healy v. Dukes"), _member("b", "2", "G.C.P. v. DHS")]) is None
    assert group_members([_member("a", ""), _member("b", None)]) is None
    assert group_members([_member("a", "1")]) is None


def test_captions_compare_case_insensitively_and_non_numeric_ids_do_not_order():
    assert group_members([_member("a", "7", "SAMMA v. DOD"), _member("b", "x9", "Samma v. DoD")]) == ("a", "doppeldocket")


def _write(path, rows, schema):
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def test_build_uses_only_published_members_and_every_parent_resolves(tmp_path):
    published = tmp_path / "court_dockets.parquet"
    native = tmp_path / "dockets.parquet"
    string = pa.string()
    _write(published, [
        {"cl_docket_id": "10", "court_id": "dcd", "docket_number": "1:20-cv-1"},
        {"cl_docket_id": "11", "court_id": "dcd", "docket_number": "1:20-cv-1"},
        {"cl_docket_id": "20", "court_id": "txwd", "docket_number": "7:13-cr-96"},
    ], pa.schema([("cl_docket_id", string), ("court_id", string), ("docket_number", string)]))
    _write(native, [
        {"id": "10", "court_id": "dcd", "docket_number": "1:20-cv-1", "pacer_case_id": "100000", "case_name": "A v. B"},
        {"id": "11", "court_id": "dcd", "docket_number": "1:20-cv-1", "pacer_case_id": "99999", "case_name": "a v. b"},
        # A native sibling outside the published selection never joins a group.
        {"id": "12", "court_id": "dcd", "docket_number": "1:20-cv-1", "pacer_case_id": "5", "case_name": "A v. B"},
        {"id": "20", "court_id": "txwd", "docket_number": "7:13-cr-96", "pacer_case_id": "616724", "case_name": "C"},
        {"id": "21", "court_id": "txwd", "docket_number": "7:13-cr-96", "pacer_case_id": "616726", "case_name": "C"},
    ], pa.schema([(name, string) for name in ("id", "court_id", "docket_number", "pacer_case_id", "case_name")]))
    out = build_court_docket_groups(tmp_path / "out", dockets_file=published, native_file=native, edition="2026-06-30")
    table = pq.read_table(out)
    assert table.schema == SCHEMA
    assert table.to_pylist() == [
        {"cl_docket_id": "10", "parent_cl_docket_id": "11", "confidence_tier": "doppeldocket", "group_size": 2,
         "edition": "2026-06-30", "rule_version": "1"},
        {"cl_docket_id": "11", "parent_cl_docket_id": "11", "confidence_tier": "doppeldocket", "group_size": 2,
         "edition": "2026-06-30", "rule_version": "1"},
    ]
