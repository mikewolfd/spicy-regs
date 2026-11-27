"""Consumer-surface tests for statutory U.S.C. disposition and named-Act citation resolution."""

from __future__ import annotations

import duckdb

from spicy_regs.citation_resolution import (
    ACT_SECTION_ROUTE,
    LEGACY_USC_SECTION_ROUTE,
    NATIVE_RESOLUTION_RULE,
    NATIVE_ROUTES,
    RESOLUTION_RULE,
    ROUTES,
    STATUTORY_ROUTES,
    USC_DISPOSITION_ROUTE,
    resolve_act_citation,
    resolve_citations,
    resolve_usc_disposition,
)
from spicy_regs.ontology.act_resolution import ENCLOSING_SOURCE_IDENTITY_RULE, act_resolution_pins
from spicy_regs.ontology.usc_dispositions import DISPOSITION_RULE, disposition_table_digests
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.relationship_views.uscode import USCODE_VIEWS


def test_resolve_usc_disposition_returns_source_backed_successors_and_provenance() -> None:
    assert USC_DISPOSITION_ROUTE.table == "usc_disposition_tables"
    disp = resolve_usc_disposition(49, "1432", appendix=True)
    assert disp["route"] == "usc_disposition"
    assert disp["rule"] == DISPOSITION_RULE == "usc-disposition-tables/2"
    assert disp["verdict"] == "exists-as-recodified"
    assert disp["recodification"] == "title-49-1994"
    assert {s["target_key"] for s in disp["successors"]} == {"49-44701", "49-44702", "49-44706", "49-44914"}
    assert disp["provenance"]["table_pins"] == {
        "usc-1994-title49-disposition.parquet": disposition_table_digests()["usc-1994-title49-disposition.parquet"]
    }
    assert disp["provenance"]["recodifications"][0]["deeming_provision"] == "Pub. L. 103-272, § 6(b)"

    # String appendix spelling ("49A" / "49 App.") agrees with appendix=True and keeps 49-3102 separate.
    assert resolve_usc_disposition("49A", "3102")["verdict"] == "not-in-table"
    pos = resolve_usc_disposition("49", "3102")
    assert pos["verdict"] == "exists-as-recodified"
    assert [s["target_key"] for s in pos["successors"]] == ["49-31502"]

    # Editorial reclassification (Title 34) preserves its distinction in provenance.
    t34 = resolve_usc_disposition(42, "3711")
    assert t34["recodification"] == "title-34-2017"
    assert t34["provenance"]["recodifications"][0]["is_editorial_reclassification"] is True
    assert {s["target_key"] for s in t34["successors"]} == {"34-10101"}


def test_resolve_act_citation_and_act_section_route_with_provenance() -> None:
    assert ACT_SECTION_ROUTE.table == "usc_act_index"
    pins = act_resolution_pins()

    # Negative control: bare Small Business Act § 3 refuses cleanly.
    sba_bare = resolve_act_citation("section 3 of the Small Business Act")
    assert sba_bare["resolved"] is False
    assert sba_bare["status"] == "refused"
    assert sba_bare["unresolved_reason"] == "act_section_ambiguous"
    assert sba_bare["table3_key"] == "85-536"
    assert sba_bare["provenance"]["table_pins"] == pins
    assert sba_bare["provenance"]["rules"]["enclosing_source_identity"] == ENCLOSING_SOURCE_IDENTITY_RULE

    # Aligned Small Business Act § 2[3] resolves to 15-632.
    for sba_ok in (
        resolve_act_citation(act_name="Small Business Act", section="3", enclosing_section="2"),
        resolve_act_citation("section 2[3] of the Small Business Act"),
    ):
        assert sba_ok["resolved"] is True
        assert sba_ok["target_key"] == "15-632"
        assert sba_ok["iri"] == "urn:rkaf:us:usc:15:632"

    # CERCLA and Civil Rights Act of 1964 resolve; bare Civil Rights Act refuses.
    cercla = resolve_act_citation("section 107(a) of CERCLA")
    assert cercla["resolved"] is True and cercla["target_key"] == "42-9607"

    cra = resolve_act_citation("section 706 of the Civil Rights Act of 1964")
    assert cra["resolved"] is True and cra["target_key"] == "42-2000e-5"

    cra_bare = resolve_act_citation("Civil Rights Act section 706")
    assert cra_bare["resolved"] is False and cra_bare["unresolved_reason"] == "act_not_in_index"

    # Route-based lookup via STATUTORY_ROUTES.
    with duckdb.connect() as con:
        occurrences = [
            {"cite_kind": "act_section", "target_key": "section 107(a) of CERCLA", "span_start": "0"},
            {"cite_kind": "act_section", "target_key": "section 3 of the Small Business Act", "span_start": "30"},
            {"cite_kind": "act_section", "target_key": "Civil Rights Act section 706", "span_start": "70"},
        ]
        resolved = resolve_citations(con, occurrences, {}, routes=STATUTORY_ROUTES)["occurrences"]
    assert resolved[0]["target_status"] == "found"
    assert resolved[0]["candidate_keys"] == [{"usc_title": "42", "usc_section": "9607"}]
    assert resolved[0]["target_snapshot"]["table_pins"] == pins
    assert resolved[1]["target_status"] == "ambiguous"
    assert resolved[1]["reason"] == "act_section_ambiguous"
    assert resolved[2]["target_status"] == "missing"
    assert resolved[2]["reason"] == "act_not_in_index"


def test_resolve_citations_enriches_missing_usc_sections_and_keeps_native_rule_unchanged() -> None:
    with duckdb.connect() as con:
        con.execute(
            "CREATE TABLE uscode_edition_sections (observation_id VARCHAR, edition_id VARCHAR, "
            "source_kind VARCHAR, source_context VARCHAR, native_identifier VARCHAR)"
        )
        con.execute(
            "INSERT INTO uscode_edition_sections VALUES "
            "('o1', 'rp-119-102', 'release-point-corpus', 'main-body-candidate', '/us/usc/t5/s553')"
        )
        install_sql_views(con, ("uscode_edition_sections",), USCODE_VIEWS)
        con.execute("CREATE TABLE law_code_sections (usc_title VARCHAR, usc_section_key VARCHAR, congress VARCHAR, session VARCHAR, seq VARCHAR)")
        pins = {"uscode_edition_sections": {"status": "managed_generation"}, "law_code_sections": {"status": "managed_generation"}}
        items = [
            {"cite_kind": "usc_section", "target_key": "5-553", "target_resolved": "true", "span_start": "1"},
            {"cite_kind": "usc_section", "target_key": "49A-1432", "target_resolved": "true", "span_start": "10"},
            {"cite_kind": "usc_section", "target_key": "10-593", "target_resolved": "true", "span_start": "20"},
            {"cite_kind": "usc_section", "target_key": "42-3711", "target_resolved": "true", "span_start": "30"},
        ]

        res = resolve_citations(con, items, pins)
        assert res["resolution_rule"] == RESOLUTION_RULE == "selected-target-lookup/2"
        live, r49, r10, r42 = res["occurrences"]
        assert live["target_status"] == "found" and "disposition" not in live
        assert r49["target_status"] == "missing"
        assert {s["target_key"] for s in r49["disposition"]["successors"]} == {"49-44701", "49-44702", "49-44706", "49-44914"}
        assert r10["disposition"]["successors"][0]["target_key"] == "10-12203"
        assert r42["disposition"]["successors"][0]["target_key"] == "34-10101"

        native = resolve_citations(
            con, items, pins, routes=NATIVE_ROUTES, rule=NATIVE_RESOLUTION_RULE, enrich_dispositions=False,
        )
        assert native["resolution_rule"] == "selected-target-lookup/2"
        assert native["occurrences"][0]["target_status"] == "found"
        assert native["occurrences"][0]["candidate_keys"] == [{"usc_title": "5", "usc_section_key": "553"}]
        assert all("disposition" not in row for row in native["occurrences"])

        legacy = resolve_citations(
            con, items, pins, routes={**ROUTES, "usc_section": LEGACY_USC_SECTION_ROUTE}, rule="selected-target-lookup/1",
        )
        assert legacy["resolution_rule"] == "selected-target-lookup/1"
        assert all("disposition" not in row for row in legacy["occurrences"])
