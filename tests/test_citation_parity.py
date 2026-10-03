"""The citation copies on the serving side of the lightweight-import boundary, held equal to spicy-docs.

The serving modules cannot import spicy-docs (``tests/test_mcp_lightweight_imports.py``), so
``citation_resolution`` and ``identifiers`` restate its kinds, key spellings and RIN shape as literals;
this test, under the source-readers install, is what binds each copy to its owner (DRY scout S2,
corpora/mcp-chaos-2026-10-02/round5/phase2-dry.md; work-list F04).
"""

from __future__ import annotations

from types import SimpleNamespace

import duckdb
import pytest
from spicy_docs.interpretation.citations import DOCUMENT_CITATION_KINDS, canonical_alnum, find_citations
from spicy_docs.interpretation.identifier_shapes import published_rin
from spicy_docs.schemas.budget_volume_tables import BUDGET_VOLUME
from spicy_docs.schemas.document_citation_tables import GOVINFO_PACKAGE
from spicy_docs.schemas.law_tables import law_id
from spicy_docs.schemas.tables import bill_id, usc_section_key

from spicy_regs.citation_resolution import CITE_KINDS, ROUTES, SOURCE_TABLES, resolve_citations
from spicy_regs.citation_sources import TEXT_SOURCES
from spicy_regs.identifiers import normalize_rin
from spicy_regs.transforms.build_cfr_sections import _cfr_ref


def test_document_kinds_are_exactly_the_writers_kinds():
    assert set(SOURCE_TABLES) == {GOVINFO_PACKAGE, BUDGET_VOLUME} | set(TEXT_SOURCES)


def test_cite_kinds_are_spicy_docs_citation_kinds():
    assert CITE_KINDS == DOCUMENT_CITATION_KINDS


#: One printed example per routed kind; ``45Q`` also exercises the case fold both sides apply to a section.
PRINTED = ("H.R. 2617; Public Law 117-328; 136 Stat. 4459; 5 U.S.C. 553; 26 U.S.C. 45Q; 40 CFR 60.1; 88 FR 12345; "
           "RIN 2060-AV12; GAO-23-105520; CRS Report R47101; Docket No. EPA-HQ-OAR-2021-0317; 410 U.S. 113; "
           "the Committee on the Judiciary.")

#: The same targets as their tables hold them, each key built by its producer where one builds it (spicy-docs'
#: bill_id, law_id and usc_section_key, the cfr_sections transform's cfr_ref); the rest are the publisher's own ids.
TARGETS = {
    "congress_bills": [{"bill_id": bill_id(SimpleNamespace(congress=117, bill_type="hr", number=2617))}],
    "laws": [{"law_id": law_id(117, "public", 328), "congress": "117", "law_type": "public", "number": "328",
              "statutes_at_large_volume": "136", "statutes_at_large_page": "4459"}],
    "law_code_sections": [{"usc_title": title, "usc_section_key": usc_section_key(section), "congress": "117",
                           "session": "2", "seq": str(seq)} for seq, (title, section) in enumerate((("5", "553"),
                                                                                                    ("26", "45Q")))],
    "cfr_sections": [{"cfr_ref": _cfr_ref(40, 60, 1), "package_id": "CFR-2025-title40-vol7",
                      "granule_id": "CFR-2025-title40-vol7-sec60-1"}],
    "federal_register": [{"volume": "88", "start_page": "12345", "document_number": "2023-01234",
                          "publication_date": "2023-02-28"}],
    "unified_agenda": [{"rin": "2060-AV12", "agenda_edition": "202504"}],
    "gao_reports": [{"report_id": "gao-23-105520", "report_number": "GAO-23-105520"}],
    "crs_reports": [{"report_id": "R47101"}],
    "dockets": [{"docket_id": "EPA-HQ-OAR-2021-0317"}],
    "committees": [{"system_code": "hsju00"}],
    "court_citations": [{"volume": "410", "page": "113", "reporter": "U.S.", "cluster_id": "108713"}],
}


def test_every_route_finds_the_key_spicy_docs_prints():
    findings = find_citations(PRINTED, congress=117,
                              committees=[(canonical_alnum("Committee on the Judiciary"), "hsju00")])
    assert set(ROUTES) == {finding.kind for finding in findings}, "each routed kind needs a printed example"
    assert {route.table for route in ROUTES.values()} == set(TARGETS)
    occurrences = [{"cite_kind": finding.kind, "target_key": finding.target_key,
                    "target_resolved": str(finding.target_resolved).lower()} for finding in findings]
    with duckdb.connect() as con:
        for table, rows in TARGETS.items():
            con.execute(f'CREATE TABLE "{table}" ({", ".join(f"{column} VARCHAR" for column in rows[0])})')
            con.executemany(f'INSERT INTO "{table}" VALUES ({", ".join("?" * len(rows[0]))})',
                            [list(row.values()) for row in rows])
        pins = {table: {"status": "managed_generation"} for table in TARGETS}
        resolved = resolve_citations(con, occurrences, pins)["occurrences"]
    assert [(row["cite_kind"], row["target_key"], row["target_status"]) for row in resolved
            if row["target_status"] != "found"] == []


#: RIN shapes the two readers must agree on. A dash spelled another way (``2060–AV12``) is where they differ:
#: published_rin folds it, normalize_rin refuses it, and whether to fold is the owner's question (work-list F04).
RIN_SHAPES = ["2060-AV12", "2060-av12", " 2060-AV12 ", "0648-XC39", "1625-AAOO", "2060-XXXX", "2060AV12",
              "2060--AV12", "2060-AV123", "RIN 2060-AV12", "", None]


@pytest.mark.parametrize("value", RIN_SHAPES)
def test_the_rin_shape_is_spicy_docs_published_rin(value):
    assert normalize_rin(value) == published_rin(value)


def test_the_dash_fold_is_the_one_recorded_difference():
    assert (normalize_rin("2060–AV12"), published_rin("2060–AV12")) == (None, "2060-AV12")
