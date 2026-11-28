"""A key projection is read once without changing counts, filters, or cardinality."""
from collections import Counter

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import check_table_joins as live
from spicy_regs.table_joins import Join


def test_cached_projections_measure_every_scope_from_one_scan(tmp_path):
    data = {
        "child": {"id": [2, 2, 2, 10, 3, 4, 42, 5, None, 1],
                  "part": ["x", "x", "x", "a", "a", "b", "c", "d", "z", None],
                  "kind": ["A", "A", "B", "A", "A", "A", "A", None, "A", "A"]},
        "parent": {"id": [2, 2, None, 11],
                   "part": ["x", "x", "z", None]},
    }
    for name, columns in data.items():
        pq.write_table(pa.table(columns), tmp_path / f"{name}.parquet")

    composite = ("id", "part")
    joins = [
        Join("child", composite, "parent", composite, 1, 0, child_filter=(("kind", "A"),)),
        Join("child", composite, "parent", composite, 1, 0, child_filter=(("kind", "B"),)),
        Join("parent", composite, "child", composite, 1, 0, expected_cardinality="one"),
        Join("child", ("id",), "parent", ("id",), 1, 0),
    ]
    urls = Counter()

    def resolve(name):
        urls[name] += 1
        return [str(tmp_path / f"{name}.parquet")]

    with duckdb.connect() as con:
        con.execute("SET enable_logging = true; SET enabled_log_types = 'QueryLog'")
        cached = live.check(con, joins, resolve)
        assert con.execute("SELECT table_name FROM duckdb_tables() WHERE temporary").fetchall() == []
        assert urls == {"child": 1, "parent": 1}
        scans = con.execute("SELECT message FROM duckdb_logs() WHERE starts_with(message, ?)",
                            ["CREATE TEMP TABLE"]).fetchall()
        assert len(scans) == 4  # two ordered key projections on each table
        assert all(sql.count("read_parquet") == 1 for (sql,) in scans)
        for join, result in zip(joins, cached, strict=True):
            assert result == live.measure(con, join, resolve)
            row = con.execute(result["sql"]).fetchone()
            assert row is not None
            assert (row[0], row[1], row[2] or [], *row[3:]) == (
                result["keys"], result["missing"], result["examples"], *(result[k] for k in live._METRICS)
            )

    filtered, other_filter, reverse, _ = cached
    assert (filtered["keys"], filtered["missing"], filtered["resolved_pct"]) == (5, 4, 20.0)
    assert filtered["examples"] == ["3|a", "4|b", "10|a"]  # typed key order, bounded to three
    assert (filtered["child_input_rows"], filtered["child_nonnull_rows"]) == (8, 6)
    assert (filtered["parent_input_rows"], filtered["parent_duplicate_keys"]) == (4, 1)
    assert (filtered["inner_join_rows"], filtered["left_join_nonnull_rows"]) == (4, 8)
    assert (other_filter["keys"], other_filter["missing"], other_filter["inner_join_rows"]) == (1, 0, 2)
    assert reverse["status"] == "MULTIPLICITY"


def test_key_cache_drops_live_projections_if_the_run_is_interrupted(tmp_path):
    path = tmp_path / "keys.parquet"
    pq.write_table(pa.table({"id": [1]}), path)
    first = Join("a", ("id",), "parent", ("id",), 1, 0)
    second = Join("b", ("id",), "parent", ("id",), 1, 0)

    def stop_after_first(result):
        assert result["keys"] == 1
        raise InterruptedError("stop")

    with duckdb.connect() as con:
        with pytest.raises(InterruptedError, match="stop"):
            live.check(con, [first, second], lambda _: [str(path)], on_result=stop_after_first)
        assert con.execute("SELECT table_name FROM duckdb_tables() WHERE temporary").fetchall() == []
