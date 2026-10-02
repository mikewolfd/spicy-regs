"""Exact lookup and interpretation boundaries over tiny physical Parquet groups."""

from decimal import Decimal
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.relationship_views.fec_financial_meaning import financial_rule_sql
from tests.test_fec_financial_policy import bulk


FIRST = "sha256:" + "1" * 64
SECOND = "sha256:" + "e" * 64


@pytest.fixture
def decisions(tmp_path):
    observations = [
        bulk(record_id=FIRST, amount=Decimal("125.25"), amount_raw="125.25"),
        bulk(record_id=SECOND, amount=Decimal("-30.00"), amount_raw="-30.00"),
        bulk(record_id=None),
        bulk(record_id="malformed"),
    ]
    table = pa.Table.from_pylist(observations)
    table = table.cast(
        pa.schema([pa.field(f.name, pa.string() if pa.types.is_null(f.type) else f.type) for f in table.schema])
    )
    path = tmp_path / "observations.parquet"
    pq.write_table(table, path, row_group_size=1)
    with duckdb.connect(config={"threads": 1, "memory_limit": "128MB", "max_temp_directory_size": "0B"}) as con:
        con.read_parquet(str(path)).create_view("observations")
        con.execute(
            "CREATE VIEW decisions AS "
            + financial_rule_sql("observations", "bulk_source_analysis", columns=table.column_names)
        )
        yield con


@pytest.mark.parametrize(
    "predicate,expected",
    [
        (f"target_record_id = '{FIRST}'", [(FIRST, "eligible", Decimal("125.25"), False)]),
        (
            f"target_record_id IN ('{FIRST}', '{SECOND}')",
            [(FIRST, "eligible", Decimal("125.25"), False), (SECOND, "eligible", Decimal("-30"), False)],
        ),
        ("target_record_id = 'sha256:" + "7" * 64 + "'", []),
    ],
)
def test_exact_financial_lookup_ignores_unselected_malformed_identity(decisions, predicate, expected):
    assert decisions.execute(
        "SELECT target_record_id, status, value, current_financial_total_qualified "
        f"FROM decisions WHERE {predicate} ORDER BY target_record_id"
    ).fetchall() == expected


def test_exact_lookup_pushes_literal_id_into_parquet_scan(decisions):
    plan = json.loads(decisions.execute(
        "EXPLAIN (FORMAT JSON) SELECT status, value FROM decisions "
        f"WHERE target_record_id IN ('{FIRST}', '{SECOND}')"
    ).fetchone()[1])

    def scans(nodes):
        for node in nodes:
            if node["name"] in {"READ_PARQUET", "PARQUET_SCAN"}:
                yield node
            yield from scans(node["children"])

    filters = [scan["extra_info"].get("Filters", "") for scan in scans(plan)]
    assert filters
    assert any("record_id" in str(value) and FIRST in str(value) and SECOND in str(value) for value in filters)
    assert all("CASE" not in str(value) for value in filters)


def test_literal_keys_and_observation_count_do_not_interpret_money(decisions):
    assert decisions.execute("SELECT count(*) FROM decisions").fetchone() == (4,)
    assert decisions.execute("SELECT target_record_id FROM decisions ORDER BY target_record_id NULLS FIRST").fetchall() == [
        (None,), ("malformed",), (FIRST,), (SECOND,)
    ]


@pytest.mark.parametrize("predicate", ["target_record_id IS NULL", "target_record_id = 'malformed'"])
@pytest.mark.parametrize("projection", ["*", "status", "value", "sum(value)", "current_financial_total_qualified"])
def test_selected_malformed_identity_refuses_financial_interpretation(decisions, predicate, projection):
    with pytest.raises(duckdb.InvalidInputException, match="SHA-256"):
        decisions.execute(f"SELECT {projection} FROM decisions WHERE {predicate}").fetchall()


def test_financial_projection_without_key_still_refuses_malformed_evidence(decisions):
    with pytest.raises(duckdb.InvalidInputException, match="SHA-256"):
        decisions.execute("SELECT status, value FROM decisions").fetchall()
