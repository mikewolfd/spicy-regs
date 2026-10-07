"""Derived views and citation lookup through the actual MCP request boundary."""

import json

import duckdb
import pytest
from mcp.server.mcpserver.exceptions import ToolError

from spicy_regs import mcp_server
from tests.test_mcp_server import _listed, _records, _tool_data
from tests.citation_fixtures import prepare_citation_inputs


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
        assert group["summary"] == registry[group["views"][0]]["metadata"]["label"].removesuffix(" occurrences")
        assert name not in [entry["table"] for entry in _listed(discovered)]
        described = _tool_data(server, "describe_table", {"table": name})
        assert described["metadata"]["summary"] == registry[name]["metadata"]["summary"]
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
        rows = _records(result)
        assert [r["source_ordinal"] for r in rows] == [0, 1]
        assert all(r["target_status"] == "not_checked" for r in rows)
        unsupported = _tool_data(server, "describe_table", {"table": "comment_document_references"})
        assert not unsupported["available"]
        assert unsupported["relationship"]["status"] == "unsupported"


def merged_occurrences(result: dict) -> list[dict]:
    """A citation reply's occurrences with the fields it states once merged back, as its occurrence_fields say."""
    fields = result["occurrence_fields"]
    rows = [{**fields["shared"], **fields["by_cite_kind"].get(row["cite_kind"], {}), **row}
            for row in result["occurrences"]]
    return [{**{field: row[other] for field, other in fields["same_as"].items() if other in row}, **row} for row in rows]


def citation_connection():
    con = duckdb.connect()
    con.execute("CREATE TABLE document_citations (document_kind VARCHAR, document_key VARCHAR, text_sha256 VARCHAR, "
                "cite_kind VARCHAR, target_key VARCHAR, target_resolved VARCHAR, span_start VARCHAR, rule_version VARCHAR)")
    con.execute("INSERT INTO document_citations VALUES "
                "('govinfo_package','CRPT-example','sha256:1111111111111111111111111111111111111111111111111111111111111111','public_law','114-public-254','true','1','003'), "
                "('govinfo_package','CRPT-example','sha256:1111111111111111111111111111111111111111111111111111111111111111','public_law','114-public-254','true','2','003')")
    # The read record the print-citations rollup writes beside each report it read (pages, rules, rows produced).
    con.execute("CREATE TABLE house_activity_reports (package_id VARCHAR, text_sha256 VARCHAR, pages_read VARCHAR, "
                "rule_set_version VARCHAR, citation_rows VARCHAR)")
    con.execute("INSERT INTO house_activity_reports VALUES ('CRPT-example','sha256:1111111111111111111111111111111111111111111111111111111111111111','9','rules','2')")
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
        import pyarrow as pa
        from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS
        con.register("_fcc_fixture", pa.Table.from_batches([], schema=SUBJECT_SCHEMAS["fcc_filings"]))
        con.execute("CREATE TABLE fcc_filings AS SELECT * FROM _fcc_fixture")
        con.unregister("_fcc_fixture")
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
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: prepare_citation_inputs(con))
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
        # Selection values remain parameters, not executable SQL fragments: the key matches no row and is refused.
        with pytest.raises(ToolError, match="no govinfo_package citation row has document_key"):
            _tool_data(server, "resolve_document_citations", {
                "document_kind": "govinfo_package", "document_key": "CRPT-example' OR 1=1 --",
            })


@pytest.mark.parametrize("damage", ["ambiguous", "stale", "missing_column", "missing_finding_digest"])
def test_source_problems_never_become_successful_target_checks(monkeypatch, damage):
    with citation_connection() as con:
        if damage == "ambiguous":
            con.execute("INSERT INTO house_activity_reports VALUES ('CRPT-example','sha256:2222222222222222222222222222222222222222222222222222222222222222','9','rules','2')")
        elif damage == "stale":
            con.execute("UPDATE house_activity_reports SET text_sha256='sha256:2222222222222222222222222222222222222222222222222222222222222222'")
        elif damage == "missing_finding_digest":
            con.execute("ALTER TABLE document_citations DROP COLUMN text_sha256")
        else:
            con.execute("ALTER TABLE house_activity_reports DROP COLUMN text_sha256")
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: prepare_citation_inputs(con))
        arguments = {"document_kind": "govinfo_package", "document_key": "CRPT-example"}
        server = mcp_server.build_server()
        if damage in {"ambiguous", "missing_finding_digest"}:
            with pytest.raises(ToolError, match="Duplicate|missing subject identity"):
                _tool_data(server, "resolve_document_citations", arguments)
            return
        result = _tool_data(server, "resolve_document_citations", arguments)
        assert all(row["target_status"] == "not_checked" for row in merged_occurrences(result))
        assert result["source_read"]["status"] == {
            "stale": "read", "missing_column": "missing_digest",
        }[damage]
