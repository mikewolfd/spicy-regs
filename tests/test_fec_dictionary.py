"""Declared retained FEC schemas and meaning through the existing dictionary/MCP owners."""

import asyncio
import base64
import hashlib
import importlib
import json
import subprocess
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import data_dictionary as dd, mcp_server

RESOURCE = dd.REPO_ROOT / "data_dictionary/fec_typed_schemas.json"


def tool_data(server, name, arguments):
    response = asyncio.run(server.call_tool(name, arguments))
    data = getattr(response, "structured_content", None)
    assert isinstance(data, dict), "Expected a completed structured MCP reply"
    return data


def schemas():
    document = json.loads(RESOURCE.read_text())
    return document, {
        table: pa.ipc.read_schema(pa.BufferReader(base64.b64decode(entry["arrow_schema_ipc_base64"], validate=True)))
        for table, entry in document["tables"].items()
    }


def test_supported_schema_declaration_matches_exact_bytes_and_parquet_types(tmp_path):
    """Exercise every declared union, including nested/decimal types, through actual Parquet."""
    document, declared = schemas()
    assert document["format_version"] == 2
    assert set(declared) == set(dd.FEC_TYPED_TABLES)
    for field in ("baseline_union_receipt_sha256", "source_generation_pin"):
        assert document[field].startswith("sha256:") and len(document[field]) == 71
    with duckdb.connect() as con:
        con.execute("SET memory_limit='64MB'")
        con.execute("SET max_temp_directory_size='0B'")
        for table, schema in declared.items():
            item = document["tables"][table]
            blob = base64.b64decode(item["arrow_schema_ipc_base64"], validate=True)
            assert "sha256:" + hashlib.sha256(blob).hexdigest() == item["arrow_schema_sha256"]
            path = tmp_path / f"{table}.parquet"
            pq.write_table(pa.Table.from_batches([], schema=schema), path)
            actual = [(r[0], r[1]) for r in con.execute(
                "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)", [str(path)]
            ).fetchall()]
            assert actual == dd.fec_typed_schemas()[table]
            assert pq.ParquetFile(path).schema_arrow.equals(schema, check_metadata=False)


def test_current_mapper_schemas_remain_compatible_with_the_complete_declared_union():
    """Schema-only producer check; no source files or captured data bodies are read."""
    _, declared = schemas()
    producer_schemas = []
    for module_name in (
        "fec_agency", "fec_research_context", "fec_legal", "fec_identity_observations",
        "fec_committee_observations", "fec_candidate_observations",
    ):
        module = importlib.import_module("spicy_regs.transforms." + module_name)
        producer_schemas.extend(module.SCHEMAS.items())
        for table, schema in module.SCHEMAS.items():
            assert declared[table].equals(schema, check_metadata=False), table
    from spicy_regs.transforms import fec_api_financial, fec_bulk_financial, fec_filing_forms, fec_summaries
    from spicy_regs.transforms.fec_bulk_selection import SELECTION_SCHEMA
    from spicy_regs.transforms.fec_filing_associations import ASSOCIATION_SCHEMA
    from spicy_regs.transforms.fec_filing_definitions import DEFINITION_SCHEMA
    from spicy_regs.transforms.fec_filing_schedules import retained_filing_mappings
    from spicy_regs.transforms.fec_query import EVIDENCE_SCHEMA, RECEIPT_SCHEMA

    producer_schemas.extend((value.table, value.schema) for value in vars(fec_bulk_financial).values()
                            if isinstance(value, fec_bulk_financial.BulkMapping))
    producer_schemas.extend((value.fields.table, value.fields.schema) for value in fec_api_financial.MAPPINGS)
    producer_schemas.extend((value.fields.table, value.schema)
                            for value in (*retained_filing_mappings(), *fec_filing_forms.remaining_filing_mappings()))
    producer_schemas.extend([
        ("fec_receipts", RECEIPT_SCHEMA), ("fec_record_evidence", EVIDENCE_SCHEMA),
        ("fec_filing_definition_evidence", EVIDENCE_SCHEMA), ("fec_filing_definitions", DEFINITION_SCHEMA),
        ("fec_filing_header_associations", ASSOCIATION_SCHEMA), ("fec_collection_selection", SELECTION_SCHEMA),
        ("fec_reported_financial_summaries", fec_summaries.SUMMARY_SCHEMA),
        ("fec_contribution_aggregates", fec_summaries.AGGREGATE_SCHEMA),
        ("fec_filing_report_observations", fec_filing_forms.FILING_REPORT_SCHEMA),
        ("fec_filing_text_observations", fec_filing_forms.FILING_TEXT_SCHEMA),
    ])
    for table, producer in producer_schemas:
        assert pa.unify_schemas([declared[table], producer]).equals(declared[table], check_metadata=False), table


def test_dictionary_retains_observation_grains_financial_limits_and_exact_evidence_routes():
    metadata = json.loads(dd.DEFAULT_MCP_METADATA_PATH.read_text())
    for table in dd.FEC_TYPED_TABLES:
        item = metadata[table]
        assert item["identity_columns"] and item["grain"]
        assert item["kind"] == "sampled"
        assert "does not establish complete FEC history or current publication" in item["coverage"]
        assert [(c["column_name"], c["column_type"]) for c in item["columns"]] == dd.fec_typed_schemas()[table]
    header_columns = {c["column_name"]: c["description"] for c in metadata["fec_filing_header_associations"]["columns"]}
    assert "Native header source_record_id" in header_columns["target_record_id"]
    assert "fec_source_records" in header_columns["target_table"]
    assert "candidate" in header_columns["filing_observation_ids"]
    assert "publisher-calculated" in metadata["fec_electioneering_communications"]["summary"]
    assert "not a filer-reported allocation" in next(c["description"] for c in metadata[
        "fec_electioneering_communications"]["columns"] if c["column_name"] == "allocated_candidate_amount")
    assert "no blanket memo exclusion" in metadata["fec_source_records"]["data_quality"]
    assert "not automatically" in metadata["fec_quality_notices"]["summary"]
    assert "value_operator and bound_value" in metadata["fec_report_metrics"]["summary"]
    assert "cancellation status" in metadata["fec_research_meeting_observations"]["summary"]
    assert "context_column" in {c["column_name"] for c in metadata["fec_record_evidence"]["columns"]}
    assert "definition_set_id" in metadata["fec_receipts"]["summary"]
    assert "no entity rows are invented" in metadata["fec_research_response_outcomes"]["summary"]


def test_mcp_describes_all_retained_declarations_without_claiming_availability(monkeypatch):
    with duckdb.connect() as con:
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        mcp_server._table_metadata.cache_clear()
        server = mcp_server.build_server()
        listed = tool_data(server, "list_sources", {})
        assert set(dd.FEC_TYPED_TABLES) <= set(listed["unavailable_tables"])
        for table in dd.FEC_TYPED_TABLES:
            result = tool_data(server, "describe_table", {"table": table})
            assert result["available"] is False
            assert result["publication"]["status"] == "unavailable"
            assert result["schema_matches_declared"] is None
            assert result["metadata"]["identity_columns"]
            assert [(c["column_name"], c["column_type"]) for c in result["columns"]] == dd.fec_typed_schemas()[table]


def test_mcp_reports_actual_schema_drift_for_a_retained_typed_table(monkeypatch):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE fec_report_metrics(record_id VARCHAR, value DOUBLE, new_field VARCHAR)")
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        result = tool_data(server, "describe_table", {"table": "fec_report_metrics"})
        assert result["schema_matches_declared"] is False
        differences = result["schema_differences"]
        assert differences["unexpected_columns"] == ["new_field"]
        assert "bound_value" in differences["missing_columns"]
        assert differences["type_differences"] == [dict(column="value", declared="DECIMAL(38,9)", actual="DOUBLE")]


def test_installed_fec_metadata_does_not_import_source_processors():
    program = '''
import asyncio, importlib.abc, sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'pyarrow', 'polars', 'spicy_docs'} or fullname == 'spicy_regs.data_dictionary':
            raise AssertionError(fullname)
sys.meta_path.insert(0, Block())
import duckdb
from spicy_regs import mcp_server
con = duckdb.connect()
mcp_server._get_connection = lambda: con
mcp = mcp_server.build_server()
response = asyncio.run(mcp.call_tool('describe_table', {'table': 'fec_receipts'})).structured_content
assert response['metadata']['identity_columns'] == ['record_id']
assert any(c['column_name'] == 'amount' and c['column_type'] == 'DECIMAL(38,9)' for c in response['columns'])
con.close()
'''
    result = subprocess.run([sys.executable, "-c", program], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
