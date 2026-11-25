"""MCP server statutory citation tests: disposition enrichment in resolve_document_citations and lookup helpers."""

from __future__ import annotations

import json

import duckdb

from spicy_regs import mcp_server
from tests.citation_fixtures import prepare_citation_inputs
from tests.test_mcp_relationships import merged_occurrences
from tests.test_mcp_server import _tool_data


def test_resolve_document_citations_enriches_historical_usc_sections(monkeypatch) -> None:
    with duckdb.connect() as con:
        con.execute(
            "CREATE TABLE document_citations (document_kind VARCHAR, document_key VARCHAR, text_sha256 VARCHAR, "
            "cite_kind VARCHAR, target_key VARCHAR, target_resolved VARCHAR, span_start VARCHAR, rule_version VARCHAR)"
        )
        digest = "sha256:" + "1" * 64
        con.executemany(
            "INSERT INTO document_citations VALUES ('govinfo_package', 'CRPT-example', ?, 'usc_section', ?, 'true', ?, '003')",
            [
                (digest, "5-553", "1"),
                (digest, "49A-1432", "20"),
                (digest, "10-593", "40"),
            ],
        )
        con.execute(
            "CREATE TABLE house_activity_reports (package_id VARCHAR, text_sha256 VARCHAR, pages_read VARCHAR, "
            "rule_set_version VARCHAR, citation_rows VARCHAR)"
        )
        con.execute("INSERT INTO house_activity_reports VALUES ('CRPT-example', ?, '9', 'rules', '3')", [digest])
        con.execute(
            "CREATE TABLE uscode_edition_sections (observation_id VARCHAR, edition_id VARCHAR, "
            "source_kind VARCHAR, source_context VARCHAR, native_identifier VARCHAR)"
        )
        con.execute(
            "INSERT INTO uscode_edition_sections VALUES "
            "('o1', 'rp-119-102', 'release-point-corpus', 'main-body-candidate', '/us/usc/t5/s553')"
        )
        con.execute("CREATE TABLE _spicy_publication (snapshot VARCHAR)")
        con.execute(
            "INSERT INTO _spicy_publication VALUES (?)",
            [
                json.dumps(
                    {
                        "families": {
                            "uscode-editions": {
                                "artifactDigest": "sha256:" + "a" * 64,
                                "tables": {"uscode_edition_sections.parquet": {}},
                            }
                        }
                    }
                )
            ],
        )
        mcp_server._install_relationship_views(con)
        monkeypatch.setattr(mcp_server, "_get_connection", lambda: prepare_citation_inputs(con))
        reply = _tool_data(
            mcp_server.build_server(),
            "resolve_document_citations",
            {"document_kind": "govinfo_package", "document_key": "CRPT-example"},
        )

    rows = {row["target_key"]: row for row in merged_occurrences(reply)}
    assert rows["5-553"]["target_status"] == "found"
    assert "disposition" not in rows["5-553"]

    assert rows["49A-1432"]["target_status"] == "missing"
    assert rows["49A-1432"]["disposition"]["verdict"] == "exists-as-recodified"
    assert {s["target_key"] for s in rows["49A-1432"]["disposition"]["successors"]} == {
        "49-44701",
        "49-44702",
        "49-44706",
        "49-44914",
    }
    assert "usc-1994-title49-disposition.parquet" in rows["49A-1432"]["disposition"]["provenance"]["table_pins"]

    assert rows["10-593"]["target_status"] == "missing"
    assert rows["10-593"]["disposition"]["successors"][0]["target_key"] == "10-12203"


def test_mcp_statutory_helpers_resolve_and_refuse() -> None:
    disp = mcp_server.resolve_usc_disposition(42, "3711")
    assert disp["recodification"] == "title-34-2017"
    assert {s["target_key"] for s in disp["successors"]} == {"34-10101"}

    assert mcp_server.resolve_act_citation("section 3 of the Small Business Act")["unresolved_reason"] == "act_section_ambiguous"
    assert mcp_server.resolve_act_citation(act_name="Small Business Act", section="3", enclosing_section="2")["target_key"] == "15-632"
    assert mcp_server.resolve_act_citation("section 107(a) of CERCLA")["target_key"] == "42-9607"
    assert mcp_server.resolve_act_citation("section 706 of the Civil Rights Act of 1964")["target_key"] == "42-2000e-5"
    assert mcp_server.resolve_act_citation("Civil Rights Act section 706")["unresolved_reason"] == "act_not_in_index"


def test_mcp_server_exposes_and_runs_statutory_citation_tools() -> None:
    server = mcp_server.build_server()
    registered = [tool.name for tool in server._tool_manager.list_tools()]
    assert "resolve_usc_disposition" in registered
    assert "resolve_act_citation" in registered

    disp = _tool_data(server, "resolve_usc_disposition", {"former_title": 42, "former_section": "3711"})
    assert disp["recodification"] == "title-34-2017"
    assert {s["target_key"] for s in disp["successors"]} == {"34-10101"}

    disp_app = _tool_data(server, "resolve_usc_disposition", {"former_title": "49A", "former_section": "1432"})
    assert disp_app["verdict"] == "exists-as-recodified"
    assert disp_app["appendix"] is True

    ambiguous = _tool_data(server, "resolve_act_citation", {"text": "section 3 of the Small Business Act"})
    assert ambiguous["unresolved_reason"] == "act_section_ambiguous"

    structured = _tool_data(
        server,
        "resolve_act_citation",
        {"act_name": "Small Business Act", "section": "3", "enclosing_section": "2"},
    )
    assert structured["target_key"] == "15-632"

    cercla = _tool_data(server, "resolve_act_citation", {"text": "section 107(a) of CERCLA"})
    assert cercla["target_key"] == "42-9607"
