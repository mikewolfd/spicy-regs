"""Derived views and citation lookup through the actual MCP request boundary."""

import json

import duckdb
import pytest

from spicy_regs import mcp_server
from tests.test_mcp_server import _tool_data


def test_discovery_exposes_derived_dependencies_and_unsupported_old_schema(monkeypatch):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE members (bioguide_id VARCHAR, fec_ids_json VARCHAR, observed_at VARCHAR, roster VARCHAR)")
        con.execute("INSERT INTO members VALUES ('C000127', '[\"S8WA00194\",\"S8WA00194\"]', '2026-09-27', 'current')")
        con.execute("CREATE TABLE comments (comment_id VARCHAR, docket_id VARCHAR)")
        mcp_server._install_relationship_views(con)
        mcp_server._apply_security_settings(con)
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        discovered = _tool_data(server, "list_sources", {})
        registry = mcp_server._connection_relationships(con.cursor())
        member_views = [name for name, value in registry.items() if value["dependencies"] == ["members"]]
        available = [name for name in member_views if registry[name]["status"] == "available"]
        assert available
        name = next(n for n in available if n.endswith("_occurrences"))
        # A family's views share one summary and are listed together, once.
        [group] = [group for group in discovered["relationship_views"] if name in group["views"]]
        assert set(group["views"]) == set(available)
        assert group["summary"] == registry[name]["metadata"]["summary"]
        assert name not in [entry["table"] for entry in discovered["tables"]]
        described = _tool_data(server, "describe_table", {"table": name})
        assert described["relationship"]["dependencies"] == ["members"]
        assert described["publication"]["status"] == "derived_view"
        assert described["publication"]["input_publications"]["members"]["status"].endswith("unversioned")
        assert described["available"] and described["metadata"]["rule_version"]
        # Columns are described on request, not at connection build.
        assert "columns" not in registry[name]["metadata"]
        assert described["schema_matches_declared"] is True
        ordinal = next(c for c in described["columns"] if c["column_name"] == "source_ordinal")
        assert ordinal["column_type"] == "BIGINT" and "Zero-based position" in ordinal["description"]
        result = _tool_data(server, "query_sql", {"sql": f'SELECT * FROM "{name}" ORDER BY source_ordinal'})
        assert [r["source_ordinal"] for r in result["rows"]] == [0, 1]
        assert all(r["target_status"] == "not_checked" for r in result["rows"])
        unsupported = _tool_data(server, "describe_table", {"table": "comment_document_references"})
        assert not unsupported["available"]
        assert unsupported["relationship"]["status"] == "unsupported"


def citation_connection():
    con = duckdb.connect()
    con.execute("CREATE TABLE document_citations (document_kind VARCHAR, document_key VARCHAR, text_sha256 VARCHAR, "
                "cite_kind VARCHAR, target_key VARCHAR, target_resolved VARCHAR, span_start VARCHAR, rule_version VARCHAR)")
    con.execute("INSERT INTO document_citations VALUES "
                "('govinfo_package','CRPT-example','digest','public_law','114-public-254','true','1','003'), "
                "('govinfo_package','CRPT-example','digest','public_law','114-public-254','true','2','003')")
    con.execute("CREATE TABLE house_activity_reports (package_id VARCHAR, text_sha256 VARCHAR)")
    con.execute("INSERT INTO house_activity_reports VALUES ('CRPT-example','digest')")
    con.execute("CREATE TABLE laws(law_id VARCHAR, congress VARCHAR, law_type VARCHAR, number VARCHAR)")
    con.execute("INSERT INTO laws VALUES ('114-public-254','114','public','254')")
    con.execute("CREATE TABLE _spicy_publication(snapshot VARCHAR)")
    con.execute("INSERT INTO _spicy_publication VALUES (?)", [json.dumps({"families": {
        "laws": {"artifactDigest": "sha256:" + "a" * 64, "tables": {"laws.parquet": {}}}
    }})])
    return con


@pytest.mark.parametrize("name", ["fcc_native_observations", "fcc_native_proceeding_links"])
def test_fcc_description_explains_complete_publisher_key(monkeypatch, name):
    with duckdb.connect() as con:
        con.execute("CREATE TABLE fcc_filings (id_submission VARCHAR, native_fields_json VARCHAR, "
                    "native_fields_sha256 VARCHAR)")
        con.execute("CREATE TABLE fcc_proceedings (name VARCHAR, id_proceeding VARCHAR)")
        mcp_server._install_relationship_views(con)
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        described = _tool_data(mcp_server.build_server(), "describe_table", {"table": name})
        meaning = next(c["description"] for c in described["columns"]
                       if c["column_name"] == "native_proceeding_id")
        assert described["available"]
        assert all(term in meaning for term in ("reused", "observed_name", "native_proceeding_id",
                                                "fcc_proceedings.name", "id_proceeding", "alone"))
        assert "column_descriptions" not in described["metadata"]


def test_citation_tool_is_bounded_and_keeps_extraction_separate(monkeypatch):
    with citation_connection() as con:
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        server = mcp_server.build_server()
        result = _tool_data(server, "resolve_document_citations", {
            "document_kind": "govinfo_package", "document_key": "CRPT-example", "max_occurrences": 1,
        })
        assert result["truncated"] is True
        assert result["coverage"]["partial"] is True
        assert result["coverage"]["occurrence_selection"]["status"] == "capped"
        assert result["acquisition_queue"]["coverage"]["partial"] is True
        assert result["source_read"]["status"] == "read"
        assert result["occurrences"][0]["target_status"] == "found"
        assert result["occurrences"][0]["target_resolved"] == "true"
        assert result["coverage"]["distinct_target_keys_read"] == 1
        # Selection values remain parameters, not executable SQL fragments.
        empty = _tool_data(server, "resolve_document_citations", {
            "document_kind": "govinfo_package", "document_key": "CRPT-example' OR 1=1 --",
        })
        assert empty["occurrences"] == []


@pytest.mark.parametrize("damage", ["ambiguous", "stale", "missing_column", "missing_finding_digest"])
def test_source_problems_never_become_successful_target_checks(monkeypatch, damage):
    with citation_connection() as con:
        if damage == "ambiguous":
            con.execute("INSERT INTO house_activity_reports VALUES ('CRPT-example','other')")
        elif damage == "stale":
            con.execute("UPDATE house_activity_reports SET text_sha256='other'")
        elif damage == "missing_finding_digest":
            con.execute("ALTER TABLE document_citations DROP COLUMN text_sha256")
        else:
            con.execute("ALTER TABLE house_activity_reports DROP COLUMN text_sha256")
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: con)
        result = _tool_data(mcp_server.build_server(), "resolve_document_citations", {
            "document_kind": "govinfo_package", "document_key": "CRPT-example",
        })
        assert all(row["target_status"] == "not_checked" for row in result["occurrences"])
        assert result["source_read"]["status"] == {
            "ambiguous": "ambiguous", "stale": "read", "missing_column": "read_failure", "missing_finding_digest": "read",
        }[damage]
