"""One dataset list, complete child meanings, and no source scans at installation."""

import asyncio
from dataclasses import replace
from typing import Any

import pyarrow as pa
from spicy_regs.subject_catalog import policies

import duckdb
import pytest

from tests.test_mcp_server import _listed, _records

from spicy_regs import mcp_server
from spicy_regs.data_dictionary import build_fec_child_metadata, expected_schemas, load_descriptions
from spicy_regs.fec_query_catalog import child_column_descriptions
from spicy_regs.relationship_views import FEC_QUERY_VIEWS
from spicy_regs.relationship_views.sql_views import annotate_views, install_sql_views


def test_primary_and_necessary_children_share_discovery(monkeypatch):
    con: Any = duckdb.connect()
    table = "fec_filing_text_observations"
    schema = policies()[table].subject_schema
    values = {"record_id": "observation", "text_fragments": [{"field_position": 2, "text": "Narrative"}] * 2}
    con.register("fixture", pa.Table.from_pylist([values], schema=schema))
    con.execute(f"CREATE TABLE {table} AS SELECT * FROM fixture")
    con.unregister("fixture")
    mcp_server._install_relationship_views(con)
    monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
    server: Any = mcp_server.build_server()
    sources = asyncio.run(server.call_tool("list_sources", {})).structured_content
    assert "query_views" not in sources
    names = {item["table"]: item for item in _listed(sources)}
    assert names[table]["category"] == "query_data"
    assert names["fec_filing_text"]["role"] == "child_query"
    assert names["fec_filing_text"]["source_tables"] == [table]
    assert "fec_candidate_records" not in names
    described = asyncio.run(server.call_tool("describe_table", {"table": "fec_filing_text"})).structured_content
    meanings = {c["column_name"]: c["description"] for c in described["columns"]}
    assert "Zero-based" in meanings["source_ordinal"]
    assert meanings["text"]
    assert all("its meaning and source grain" not in v for v in meanings.values())
    result = asyncio.run(
        server.call_tool(
            "query_sql", {"sql": "SELECT source_ordinal, text FROM fec_filing_text ORDER BY source_ordinal"}
        )
    ).structured_content
    assert _records(result) == [{"source_ordinal": 0, "text": "Narrative"}, {"source_ordinal": 1, "text": "Narrative"}]


def test_role_is_explicit_and_installation_does_not_evaluate_rows():
    con: Any = duckdb.connect()
    reads = []

    def observe(value: str) -> str:
        reads.append(value)
        return value

    con.create_function("observe", observe, side_effects=True)
    spec = replace(next(s for s in FEC_QUERY_VIEWS if s.name == "fec_filing_text"), rule_version="future-version")
    table = "fec_filing_text_observations"
    schema = policies()[table].subject_schema
    con.register("empty", pa.Table.from_batches([], schema=schema))
    con.execute(f"CREATE VIEW {table} AS SELECT * REPLACE (observe(record_id) AS record_id) FROM empty")
    installed = install_sql_views(con, [table], [spec])
    annotate_views(installed)
    assert installed[spec.name]["status"] == "available"
    assert installed[spec.name]["metadata"]["view_role"] == "child_query"
    assert "unique_pairs" not in installed[spec.name]["metadata"]["coverage_semantics"]
    assert reads == []


def test_every_child_has_complete_field_meanings_and_new_fields_require_them():
    metadata = build_fec_child_metadata(load_descriptions(), expected_schemas())
    assert set(metadata) == {s.name for s in FEC_QUERY_VIEWS}
    for entry in metadata.values():
        assert set(entry["column_descriptions"]) == {c["column_name"] for c in entry["columns"]}
        assert entry["source_tables"] and entry["grain"] and entry["identity_columns"]
    with pytest.raises(ValueError, match="needs column meanings"):
        child_column_descriptions(FEC_QUERY_VIEWS[0], [("new_measure", "DOUBLE")], {})


def test_no_same_grain_mirrors_remain():
    removed = {
        "fec_candidate_records",
        "fec_committee_records",
        "fec_committee_master_records",
        "fec_committee_history_records",
        "fec_candidate_registrations",
        "fec_committee_registrations",
        "fec_lobbyist_records",
        "fec_collection_summary",
        "fec_filing_announcements",
        "fec_meetings",
        "fec_source_pages",
        "fec_document_leads",
        "fec_receipt_samples",
        "fec_disbursement_samples",
        "fec_filing_records",
        "fec_filing_reports",
        "fec_matters",
        "fec_matter_documents",
        "fec_report_text",
    }
    assert not removed.intersection(s.name for s in FEC_QUERY_VIEWS)
