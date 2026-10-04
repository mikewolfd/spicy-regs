"""Identity query shapes retain observations, uncertain values and evidence links."""

import duckdb
import pytest

from spicy_regs.relationship_views.fec_identity_query import FEC_IDENTITY_QUERY_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views

SPECS = {s.name: s for s in FEC_IDENTITY_QUERY_VIEWS}
EVIDENCE = {
    "record_id": "record-1",
    "collection_id": "capture-1",
    "source_record_id": "source-1",
    "source_sha256": "sha256:exact",
    "source_locator_json": '{"offset":17}',
    "source_authority": "official-fec",
    "source_namespace": "test",
    "mapping_status": "partial",
    "source_cycle": 2026,
}


def source(connection, view, rows, types=None):
    """Only declare required dependencies, catching undeclared reads in each spec."""
    spec = SPECS[view]
    table, columns = next(iter(spec.required.items()))
    types = {"source_cycle": "INTEGER", **(types or {})}
    connection.execute(
        f'CREATE TABLE "{table}" (' + ",".join(f'"{c}" {types.get(c, "VARCHAR")}' for c in columns) + ")"
    )
    for row in rows:
        values = {**EVIDENCE, **row}
        connection.execute(
            f'INSERT INTO "{table}" VALUES (' + ",".join("?" for _ in columns) + ")", [values.get(c) for c in columns]
        )
    result = install_sql_views(connection, [table], [spec], {table: {"generation": "sha256:selected"}})
    assert result[view]["status"] == "available"
    return table


@pytest.fixture
def connection():
    with duckdb.connect() as connection:
        yield connection


def test_cycles_preserve_repeat_ordinal_and_invalid_elements(connection):
    source(
        connection,
        "fec_candidate_cycles",
        [
            {"candidate_id": "H2AK01158", "cycles_json": '[2026,2026,null,"2026",2026.5,{},false]'},
            {"record_id": "bad-array", "cycles_json": "bad"},
        ],
    )
    rows = connection.execute(
        "SELECT source_ordinal,cycle,parsing_status FROM fec_candidate_cycles ORDER BY source_ordinal"
    ).fetchall()
    assert rows == [
        (0, 2026, "reported"),
        (1, 2026, "reported"),
        (2, None, "json_null"),
        (3, None, "unsupported_value"),
        (4, None, "unsupported_value"),
        (5, None, "unsupported_value"),
        (6, None, "unsupported_value"),
    ]
    assert connection.execute("SELECT DISTINCT source_locator_json FROM fec_candidate_cycles").fetchall() == [
        (EVIDENCE["source_locator_json"],)
    ]


def test_memberships_do_not_fabricate_targets_or_drop_unsupported_values(connection):
    source(
        connection,
        "fec_committee_candidate_links",
        [{"committee_id": "C00000000", "candidate_ids_json": '["H2AK01158","H2AK01158","invalid",null]'}],
    )
    assert connection.execute(
        "SELECT source_ordinal,candidate_id,parsing_status FROM fec_committee_candidate_links ORDER BY source_ordinal"
    ).fetchall() == [
        (0, "H2AK01158", "reported"),
        (1, "H2AK01158", "reported"),
        (2, None, "unsupported_value"),
        (3, None, "json_null"),
    ]


def test_only_independent_array_children_exist():
    assert len(SPECS) == 7
    assert all("source_ordinal" in spec.identity_columns for spec in SPECS.values())
    assert not any(name.endswith(("_records", "_registrations")) for name in SPECS)


def test_sponsor_links_read_compact_promoted_array_without_native_object(connection):
    source(
        connection,
        "fec_committee_sponsor_candidate_links",
        [{"committee_id": "C00000000", "sponsor_candidate_ids_json": '["H2AK01158","H2AK01158",null]'}],
    )
    assert connection.execute(
        "SELECT source_ordinal,candidate_id,parsing_status FROM fec_committee_sponsor_candidate_links ORDER BY source_ordinal"
    ).fetchall() == [(0, "H2AK01158", "reported"), (1, "H2AK01158", "reported"), (2, None, "json_null")]


def test_install_checks_compact_array_schema_without_scanning(connection):
    spec = SPECS["fec_candidate_cycles"]
    columns = spec.required["fec_candidate_api_observations"]
    connection.execute(
        "CREATE VIEW fec_candidate_api_observations AS SELECT "
        + ",".join(f"error('must not scan')::VARCHAR AS \"{c}\"" for c in columns)
    )
    status = install_sql_views(connection, ["fec_candidate_api_observations"], [spec])
    assert status[spec.name]["status"] == "available"
