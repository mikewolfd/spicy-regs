"""usc_citation_verdicts: every held (title, section, place) key once, checked honestly, never read as absent."""

import hashlib
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.schemas.native_reference_rows import NATIVE_LEGAL_REFERENCE_RULE
from spicy_regs.citation_resolution import NATIVE_RESOLUTION_RULE, ROUTES, resolve_citations
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.rollups.native_legal_references import NativeLegalReferencesRollup
from spicy_regs.relationship_views.reference import REFERENCE_TABLES, reference_paths
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.relationship_views.uscode import USCODE_VIEWS


def _table(con, name, fields, records):
    con.execute(f'CREATE TABLE {name} (' + ','.join(f'{field} VARCHAR' for field in fields) + ')')
    if records:
        con.executemany(f'INSERT INTO {name} VALUES (' + ','.join('?' for _ in fields) + ')', records)


#: Release-point rows shaped as generation 792cbaa7 holds them: an en dash, an upper-case suffix, a combined heading,
#: range stubs (one spanning lettered sections, one crossing a digit count, two whose bounds carry a dash number,
#: one crossing a letter count), an OLRC duplicate numbering (both same-status 10-130g and multi-status 10-2891),
#: withdrawn/reserved sections (repealed 10-2304, transferred 25-450/50-414, omitted 2-72a-3, reserved 10-3003),
#: an exact section overlapping a historical range stub (26-4221 inside 26-4220...4225), an appendix identifier of
#: another shape, a notes row with no identifier, and an annual-edition row that must not count as release-point.
RELEASE = [
    ("o1", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t5/s553"),
    ("o2", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t42/s2000e–5"),
    ("o3", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t26/s45Q"),
    ("o4", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t12/s5 /us/usc/t12/s6"),
    ("o5", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t25/s2601...2606"),
    ("o6", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t12/s33...34c"),
    ("o7", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t10/s130g"),
    ("o8", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t10/s130g"),
    ("o9", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t18a/pl/91/538/s1"),
    ("o10", "rp-119-102", "release-point-corpus", "notes-or-quoted-content", None),
    ("o12", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t15/s99...101"),
    ("o13", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t16/s698v...698v–10"),
    ("o14", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t16/s1857f-5a...1857f-6c"),
    ("o15", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t42/s2689r...2689aa"),
    ("o16", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t10/s2304"),
    ("o17", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t25/s450"),
    ("o18", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t50/s414"),
    ("o19", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t2/s72a-3"),
    ("o20", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t10/s3003"),
    ("o21", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t10/s2891"),
    ("o22", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t10/s2891"),
    ("o23", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t26/s4221"),
    ("o24", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t26/s4220...4225"),
    ("o25", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t2/s184"),
    ("o11", "annual-2000", "annual", "main-body-candidate", "/us/usc/t7/s1"),
]

CITATIONS = [  # (cite_kind, target_key, target_resolved) -> always usc_place = 'unstated'
    ("usc_section", "5-553", "true"), ("usc_section", "5-553", "true"),
    ("usc_section", "42-2000e-5", "true"),
    ("usc_section", "26-45q", "true"),
    ("usc_section", "12-6", "true"),
    ("usc_section", "25-2601", "true"),  # a stub's own low bound: spanned, never printed
    ("usc_section", "25-2603", "true"),
    ("usc_section", "12-34b", "true"),  # inside 33...34c only in Code order (stem, then suffix)
    ("usc_section", "12-34d", "true"),
    ("usc_section", "15-100", "true"),  # inside 99...101 by stem; a text comparison would miss it
    ("usc_section", "16-698v-4", "true"),  # inside 698v...698v-10 only when the dash number compares as a number
    ("usc_section", "16-698w", "true"),
    ("usc_section", "16-1857f-6a", "true"),  # inside 1857f-5a...1857f-6c
    ("usc_section", "16-1857f-7", "true"),
    ("usc_section", "42-2689s", "true"),  # inside 2689r...2689aa only when shorter letters sort first
    ("usc_section", "42-2689ab", "true"),
    ("usc_section", "50A-541", "true"),  # appendix title, present in 1994-2014 annual editions
    ("usc_section", "5A-10", "true"),  # appendix title, present in 1994-2021 annual editions
    ("usc_section", "5-10", "true"),  # negative control: non-appendix must NOT match 5A-10
    ("usc_section", "50A-1634", "true"),  # appendix annual range stub (1633..1636)
    ("usc_section", "50-1634", "true"),  # negative control: non-appendix must NOT match 50A's annual range
    ("usc_section", "7-1", "true"),  # held only in an annual row in synthetic RELEASE (current in 119-102 oracle)
    ("usc_section", "10-130g", "true"),
    # Legal status controls from usc_oracle_sections
    ("usc_section", "10-2304", "true"),  # repealed
    ("usc_section", "25-450", "true"),  # transferred
    ("usc_section", "50-414", "true"),  # transferred
    ("usc_section", "2-72a-3", "true"),  # omitted
    ("usc_section", "10-3003", "true"),  # reserved
    ("usc_section", "10-2891", "true"),  # duplicate OLRC numbering: ['current', 'repealed'] -> 'multiple'
    ("usc_section", "26-4221", "true"),  # current section overlapping historical repealed stub 26-4220...4225
    # 1994-2024 annual-only controls
    ("usc_section", "41-5", "true"),  # annual-only live section (1994-2009)
    ("usc_section", "42-300aaa-5", "true"),  # annual-only range stub (1994-2008, 300aaa...300aaa-13)
    # Negative controls: cross-title ranges and letter-suffix vs subsection
    ("usc_section", "1-2603", "true"),  # inside Title 25 stub 2601...2606, NOT Title 1
    ("usc_section", "1-300aaa-5", "true"),  # inside Title 42 annual range 300aaa...300aaa-13, NOT Title 1
    ("usc_section", "2-184a", "true"),  # 2-184 has subsection (a), but letter-suffix repair is unsupported
    ("usc_section", "9-99", "false"),  # unsettled spelling: never a key
    ("public_law", "117-328", "true"),
]

CLASSIFICATIONS = [  # (usc_title, usc_section_key, usc_place, action)
    ("5", "553", "note", "nt new"),
    ("5", "553", "section", "amd"),
    ("12", "6", "preceding", "prec new"),
    ("25", "2601", "note_preceding", "nt prec new"),  # must stay not_checked even though 25-2601 sits in a range stub
    ("22", "10801", "section", "new"),
    ("22", "10802", "note", "nt new"),  # unprinted note: must stay not_checked_non_section_place (never classified_as_new)
    ("22", "10803", "section", "nt new"),  # unprinted section with 'nt new': exact action='new' kills substring mutant
    ("8", "1101", "note", "nt new"),  # real OLRC 119-1 control: note stays separate from live section 8-1101
    ("14", "319", "section", "tr to 14/336"),  # not at release point, present 2018-2024 in annual oracle -> annual_only
    ("10", "130g", None, "amd"),  # NULL place defensively coalesces to unstated
]

TABLE3 = [  # (usc_title, usc_section_key, usc_place)
    ("5", "553", "section"),
    ("5", "553", "note"),
    ("12", "6", "section"),
    ("12", "6", ""),  # blank place defensively coalesces to unstated
]

NATIVE_REFS = [  # (interpretation_status, target_candidates list of dicts)
    ("native_section_href", [{"target_kind": "usc_section", "target_key": "5-553", "target_resolved": True}]),
    ("native_bill_usc_cite", [{"target_kind": "usc_section", "target_key": "5-553", "target_resolved": True}]),
    ("native_bill_usc_cite", [{"target_kind": "usc_section", "target_key": "26-45Q", "target_resolved": True}]),
    ("partial_text_findings", [
        {"target_kind": "usc_section", "target_key": "5-553", "target_resolved": True},
        {"target_kind": "usc_section", "target_key": "9-99", "target_resolved": False},  # unsettled: excluded
        {"target_kind": "public_law", "target_key": "119-public-1", "target_resolved": True},  # non-usc: excluded
        None,
    ]),
    ("unsupported_href", []),
    (None, None),
]

# key: (citations, classifications, table3, native_refs, release rows, stubs, verdict, reason)
EXPECTED = {
    ("5", "553", "section"): (0, 1, 1, 2, 1, None, "printed_at_release_point", None),
    ("5", "553", "unstated"): (2, 0, 0, 1, 1, None, "printed_at_release_point", None),
    ("5", "553", "note"): (0, 1, 1, 0, 0, None, "not_checked_non_section_place", "non_section_place"),
    ("42", "2000e-5", "unstated"): (1, 0, 0, 0, 1, None, "printed_at_release_point", None),
    ("26", "45q", "unstated"): (1, 0, 0, 0, 1, None, "printed_at_release_point", None),
    ("26", "45q", "section"): (0, 0, 0, 1, 1, None, "printed_at_release_point", None),
    ("12", "6", "unstated"): (1, 0, 1, 0, 1, None, "printed_at_release_point", None),
    ("12", "6", "section"): (0, 0, 1, 0, 1, None, "printed_at_release_point", None),
    ("12", "6", "preceding"): (0, 1, 0, 0, 0, None, "not_checked_non_section_place", "non_section_place"),
    ("25", "2601", "unstated"): (1, 0, 0, 0, 0, ["2601...2606"], "in_range_stub", None),
    ("25", "2601", "note_preceding"): (0, 1, 0, 0, 0, None, "not_checked_non_section_place", "non_section_place"),
    ("25", "2603", "unstated"): (1, 0, 0, 0, 0, ["2601...2606"], "in_range_stub", None),
    ("12", "34b", "unstated"): (1, 0, 0, 0, 0, ["33...34c"], "in_range_stub", None),
    ("12", "34d", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("15", "100", "unstated"): (1, 0, 0, 0, 0, ["99...101"], "in_range_stub", None),
    ("16", "698v-4", "unstated"): (1, 0, 0, 0, 0, ["698v...698v-10"], "in_range_stub", None),
    ("16", "698w", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("16", "1857f-6a", "unstated"): (1, 0, 0, 0, 0, ["1857f-5a...1857f-6c"], "in_range_stub", None),
    ("16", "1857f-7", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("42", "2689s", "unstated"): (1, 0, 0, 0, 0, ["2689r...2689aa"], "in_range_stub", None),
    ("42", "2689ab", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("50A", "541", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "appendix_title"),
    ("5A", "10", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "appendix_title"),
    ("5", "10", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("50A", "1634", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "appendix_title"),
    ("50", "1634", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("7", "1", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("10", "130g", "unstated"): (1, 1, 0, 0, 2, None, "printed_at_release_point", None),
    ("10", "2304", "unstated"): (1, 0, 0, 0, 1, None, "printed_at_release_point", None),
    ("25", "450", "unstated"): (1, 0, 0, 0, 1, None, "printed_at_release_point", None),
    ("50", "414", "unstated"): (1, 0, 0, 0, 1, None, "printed_at_release_point", None),
    ("2", "72a-3", "unstated"): (1, 0, 0, 0, 1, None, "printed_at_release_point", None),
    ("10", "3003", "unstated"): (1, 0, 0, 0, 1, None, "printed_at_release_point", None),
    ("10", "2891", "unstated"): (1, 0, 0, 0, 2, None, "printed_at_release_point", None),
    ("26", "4221", "unstated"): (1, 0, 0, 0, 1, None, "printed_at_release_point", None),
    ("41", "5", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "annual_only"),
    ("42", "300aaa-5", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "annual_only"),
    ("1", "2603", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("1", "300aaa-5", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("2", "184a", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("22", "10801", "section"): (0, 1, 0, 0, 0, None, "not_printed_at_release_point", "classified_as_new"),
    ("22", "10802", "note"): (0, 1, 0, 0, 0, None, "not_checked_non_section_place", "non_section_place"),
    ("22", "10803", "section"): (0, 1, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("8", "1101", "note"): (0, 1, 0, 0, 0, None, "not_checked_non_section_place", "non_section_place"),
    ("14", "319", "section"): (0, 1, 0, 0, 0, None, "not_printed_at_release_point", "annual_only"),
}


@pytest.fixture
def con():
    con = duckdb.connect()
    _table(con, "uscode_edition_sections",
           ("observation_id", "edition_id", "source_kind", "source_context", "native_identifier"), RELEASE)
    _table(con, "document_citations", ("cite_kind", "target_key", "target_resolved"), CITATIONS)
    _table(con, "law_code_sections", ("usc_title", "usc_section_key", "usc_place", "action"), CLASSIFICATIONS)
    _table(con, "table3_records", ("usc_title", "usc_section_key", "usc_place"), TABLE3)
    con.execute("""
        CREATE TABLE native_legal_references (
            interpretation_status VARCHAR,
            target_candidates STRUCT(target_kind VARCHAR, target_key VARCHAR, target_resolved BOOLEAN)[]
        )
    """)
    con.executemany("INSERT INTO native_legal_references VALUES (?, ?)", NATIVE_REFS)
    tables = (
        "uscode_edition_sections", "document_citations", "law_code_sections",
        "table3_records", "native_legal_references",
    )
    installed = install_sql_views(con, tables, USCODE_VIEWS)
    assert {name: entry["status"] for name, entry in installed.items()} == {
        "usc_citation_verdicts": "available", "usc_release_point_sections": "available"}
    assert installed["usc_citation_verdicts"]["metadata"]["rule_version"] == "usc-release-point-verdict-v3"
    assert installed["usc_citation_verdicts"]["metadata"]["identity_columns"] == [
        "usc_title", "usc_section_key", "usc_place"]
    return con


def test_each_key_reads_its_release_point_verdict_and_observed_reason(con):
    rows = con.execute("""SELECT usc_title, usc_section_key, usc_place, document_citation_count,
        law_code_section_count, table3_record_count, native_legal_reference_count,
        release_point_rows, release_point_range_stubs, verdict, reason
        FROM usc_citation_verdicts""").fetchall()
    assert {(title, key, place): tuple(rest) for title, key, place, *rest in rows} == EXPECTED
    assert con.execute("SELECT DISTINCT rule_version FROM usc_citation_verdicts").fetchall() == [
        ("usc-release-point-verdict-v3",)
    ]


def test_oracle_legal_status_distinguishes_current_withdrawn_reserved_multiple_and_ranges(con):
    rows = {
        (t, k, p): (st, sts, src, ap, ao, fy, ly, cnt, v, r)
        for t, k, p, st, sts, src, ap, ao, fy, ly, cnt, v, r in con.execute("""
            SELECT usc_title, usc_section_key, usc_place,
                   oracle_status, oracle_statuses, oracle_status_source,
                   annual_presence, annual_only, annual_first_year, annual_last_year, annual_edition_count,
                   verdict, reason
            FROM usc_citation_verdicts
        """).fetchall()
    }
    # Current live law
    assert rows[("5", "553", "section")] == (
        "current", ["current"], "oracle_section", "annual_section", False, 1994, 2024, 31,
        "printed_at_release_point", None,
    )
    # Printed at release point, but withdrawn or reserved in OLRC oracle
    assert rows[("10", "2304", "unstated")][:3] == ("repealed", ["repealed"], "oracle_section")
    assert rows[("12", "6", "unstated")][:3] == ("repealed", ["repealed"], "oracle_section")
    assert rows[("25", "450", "unstated")][:3] == ("transferred", ["transferred"], "oracle_section")
    assert rows[("50", "414", "unstated")][:3] == ("transferred", ["transferred"], "oracle_section")
    assert rows[("2", "72a-3", "unstated")][:3] == ("omitted", ["omitted"], "oracle_section")
    assert rows[("10", "3003", "unstated")][:3] == ("reserved", ["reserved"], "oracle_section")
    # Duplicate OLRC numbering with conflicting statuses stays one row and reports 'multiple'
    assert rows[("10", "2891", "unstated")][:3] == ("multiple", ["current", "repealed"], "oracle_section")
    # Exact section status wins over overlapping repealed range stub (26-4220...4225)
    assert rows[("26", "4221", "unstated")][:3] == ("current", ["current"], "oracle_section")
    # Range-stub statuses come from usc_oracle_ranges
    assert rows[("25", "2603", "unstated")] == (
        "repealed", ["repealed"], "oracle_range", "annual_range_stub", False, 1994, 2024, 31,
        "in_range_stub", None,
    )
    assert rows[("12", "34b", "unstated")][:3] == ("transferred", ["transferred"], "oracle_range")
    # 16-698v-4: live in annual sections 2000-2013 and in annual range stubs 2014-2024
    assert rows[("16", "698v-4", "unstated")] == (
        "repealed", ["repealed"], "oracle_range", "annual_section_and_range_stub", False, 2000, 2024, 25,
        "in_range_stub", None,
    )
    # Synthetic partial RELEASE omitting 7-1 must NOT claim annual_only when 119-102 oracle has 7-1 current
    assert rows[("7", "1", "unstated")] == (
        "current", ["current"], "oracle_section", "annual_section", False, 1994, 2024, 31,
        "not_printed_at_release_point", "not_printed",
    )


def test_annual_only_presence_and_negative_controls(con):
    rows = {
        (t, k, p): (st, src, ap, ao, fy, ly, cnt, v, r)
        for t, k, p, st, src, ap, ao, fy, ly, cnt, v, r in con.execute("""
            SELECT usc_title, usc_section_key, usc_place,
                   oracle_status, oracle_status_source,
                   annual_presence, annual_only, annual_first_year, annual_last_year, annual_edition_count,
                   verdict, reason
            FROM usc_citation_verdicts
        """).fetchall()
    }
    # 1994-2024 annual-only live section (former Title 41) and recently transferred section (14-319)
    assert rows[("41", "5", "unstated")] == (
        None, None, "annual_section", True, 1994, 2009, 16, "not_printed_at_release_point", "annual_only",
    )
    assert rows[("14", "319", "section")] == (
        None, None, "annual_section", True, 2018, 2024, 7, "not_printed_at_release_point", "annual_only",
    )
    # 1994-2024 annual-only range stub (42-300aaa...300aaa-13)
    assert rows[("42", "300aaa-5", "unstated")] == (
        None, None, "annual_range_stub", True, 1994, 2008, 15, "not_printed_at_release_point", "annual_only",
    )
    # Appendix titles preserve reason='appendix_title' while surfacing exact annual-only presence
    assert rows[("50A", "541", "unstated")] == (
        None, None, "annual_section", True, 1994, 2014, 21, "not_printed_at_release_point", "appendix_title",
    )
    assert rows[("5A", "10", "unstated")] == (
        None, None, "annual_section", True, 1994, 2021, 28, "not_printed_at_release_point", "appendix_title",
    )
    assert rows[("50A", "1634", "unstated")][:4] == (None, None, "annual_range_stub", True)
    assert rows[("50A", "1634", "unstated")][7:] == ("not_printed_at_release_point", "appendix_title")

    # Cross-title negative controls: NEVER match another title's range stub or another title's appendix
    for neg_key in (("1", "2603", "unstated"), ("1", "300aaa-5", "unstated"),
                    ("5", "10", "unstated"), ("50", "1634", "unstated")):
        assert rows[neg_key] == (
            None, None, "none", False, None, None, 0, "not_printed_at_release_point", "not_printed",
        ), neg_key

    # Letter-suffix vs subsection negative control: 2-184a is NEVER repaired to 2-184(a)
    assert rows[("2", "184a", "unstated")] == (
        None, None, "none", False, None, None, 0, "not_printed_at_release_point", "not_printed",
    )

    # nt-new negative controls:
    # - unprinted note with action='nt new' stays not_checked_non_section_place with NULL oracle/annual columns
    assert rows[("22", "10802", "note")] == (
        None, None, None, None, None, None, None, "not_checked_non_section_place", "non_section_place",
    )
    assert rows[("8", "1101", "note")] == (
        None, None, None, None, None, None, None, "not_checked_non_section_place", "non_section_place",
    )
    # - unprinted section with action='nt new' does NOT match action='new' (kills substring mutant)
    assert rows[("22", "10803", "section")] == (
        None, None, "none", False, None, None, 0, "not_printed_at_release_point", "not_printed",
    )
    assert rows[("22", "10801", "section")] == (
        None, None, "none", False, None, None, 0, "not_printed_at_release_point", "classified_as_new",
    )


def test_oracle_publication_pins_and_locked_connection_execution(con):
    [(raw_pins,)] = con.execute("SELECT DISTINCT oracle_publication_json FROM usc_citation_verdicts").fetchall()
    expected_pins = {
        name: REFERENCE_TABLES[name][1]
        for name in ("usc_oracle_sections", "usc_oracle_ranges", "usc_oracle_annual_sections", "usc_oracle_annual_ranges")
    }
    assert json.loads(raw_pins) == expected_pins

    # Works identically after applying MCP's allowed_paths + enable_external_access=false lock.
    con.execute("SET allowed_paths=?", [list(reference_paths())])
    con.execute("SET enable_external_access=false")
    assert con.execute("SELECT count(*) FROM usc_citation_verdicts").fetchone()[0] == len(EXPECTED)


def test_distinct_section_unstated_and_note_places_never_collapse(con):
    rows = {
        row[0]: row[1:]
        for row in con.execute("""
            SELECT usc_place, document_citation_count, law_code_section_count, table3_record_count,
                   native_legal_reference_count, release_point_rows, release_point_editions,
                   release_point_range_stubs, verdict, reason, oracle_status, annual_only
            FROM usc_citation_verdicts
            WHERE usc_title = '5' AND usc_section_key = '553'
        """).fetchall()
    }
    assert set(rows) == {"section", "unstated", "note"}
    assert rows["section"] == (0, 1, 1, 2, 1, ["rp-119-102"], None, "printed_at_release_point", None, "current", False)
    assert rows["unstated"] == (2, 0, 0, 1, 1, ["rp-119-102"], None, "printed_at_release_point", None, "current", False)
    assert rows["note"] == (0, 1, 1, 0, 0, None, None, "not_checked_non_section_place", "non_section_place", None, None)


def test_one_row_per_key_and_every_source_key_is_counted_both_ways(con):
    view = {
        (t, k, p)
        for t, k, p in con.execute("SELECT usc_title, usc_section_key, usc_place FROM usc_citation_verdicts").fetchall()
    }
    assert len(view) == con.execute("SELECT count(*) FROM usc_citation_verdicts").fetchone()[0]
    sources = {(*target.split("-", 1), "unstated") for kind, target, settled in CITATIONS
               if kind == "usc_section" and settled == "true"}
    sources |= {(title, key, place or "unstated") for title, key, place, _ in CLASSIFICATIONS}
    sources |= {(title, key, place or "unstated") for title, key, place in TABLE3}
    for status, candidates in NATIVE_REFS:
        place = "section" if status in {"native_section_href", "native_bill_usc_cite"} else "unstated"
        for cand in candidates or ():
            if cand and cand["target_kind"] == "usc_section" and cand["target_resolved"]:
                t, k = str(cand["target_key"]).split("-", 1)
                sources.add((t.upper(), k.lower(), place))
    assert view == sources


def test_the_view_never_says_absent_and_never_marks_non_section_places_printed(con):
    verdicts = {row[0] for row in con.execute("SELECT DISTINCT verdict FROM usc_citation_verdicts").fetchall()}
    assert verdicts == {
        "printed_at_release_point", "in_range_stub",
        "not_printed_at_release_point", "not_checked_non_section_place",
    }
    bad_non_section = con.execute("""
        SELECT count(*)
        FROM usc_citation_verdicts
        WHERE usc_place IN ('note', 'preceding', 'note_preceding')
          AND (verdict <> 'not_checked_non_section_place'
               OR reason <> 'non_section_place'
               OR release_point_rows <> 0
               OR release_point_editions IS NOT NULL
               OR release_point_range_stubs IS NOT NULL
               OR oracle_status IS NOT NULL
               OR oracle_statuses IS NOT NULL
               OR oracle_status_source IS NOT NULL
               OR annual_presence IS NOT NULL
               OR annual_only IS NOT NULL
               OR annual_first_year IS NOT NULL
               OR annual_last_year IS NOT NULL
               OR annual_edition_count IS NOT NULL)
    """).fetchone()[0]
    assert bad_non_section == 0


def test_the_route_finds_what_the_release_point_prints_or_spans(con):
    """resolve_document_citations' usc_section route agrees with the verdict view key for key."""
    occurrences = [{"cite_kind": kind, "target_key": key, "target_resolved": settled}
                   for kind, key, settled in CITATIONS if kind == "usc_section" and settled == "true"]
    resolved = resolve_citations(con, occurrences, {"uscode_edition_sections": {"status": "managed_generation"}})
    assert ROUTES["usc_section"].pinned_by == "uscode_edition_sections"
    outcomes = {tuple(row["target_key"].split("-", 1)): (row["target_status"], row.get("match_basis"))
                for row in resolved["occurrences"]}
    by_verdict = {"printed_at_release_point": ("found", "printed_section"), "in_range_stub": ("found", "range_stub"),
                  "not_printed_at_release_point": ("missing", None)}
    expected_outcomes = {
        (title, key): by_verdict[expected[6]]
        for (title, key, place), expected in EXPECTED.items()
        if place == "unstated" and expected[0]
    }
    # 26-4221 is both printed at the release point and spanned by historical range stub 26-4220...4225.
    expected_outcomes[("26", "4221")] = ("found", "printed_section_and_range_stub")
    assert outcomes == expected_outcomes
    # Cross-title range negative control: 1-2603 MUST be missing even though 25-2601...2606 spans 2603.
    assert outcomes[("1", "2603")] == ("missing", None)
    # Letter-suffix negative control: 2-184a MUST be missing even though 2-184 is printed.
    assert outcomes[("2", "184a")] == ("missing", None)
    # An OLRC duplicate numbering is one target, not two.
    [duplicate] = [row for row in resolved["occurrences"] if row["target_key"] == "10-130g"]
    assert duplicate["candidate_keys"] == [{"usc_title": "10", "usc_section_key": "130g"}]
    assert resolved["resolution_rule"] == "selected-target-lookup/2"


def test_sealed_native_legal_references_generation_integrates_with_verdicts(tmp_path, monkeypatch):
    """Read sealed receipt-free target candidates through the U.S. Code citation verdicts."""
    assert NATIVE_RESOLUTION_RULE == "selected-target-lookup/2"
    assert NATIVE_LEGAL_REFERENCE_RULE == "native-legal-reference/004"
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    bill_xml = b'<bill><external-xref legal-doc="usc" parsable-cite="usc/5/553"/></bill>'
    law_xml = (
        b'<pLaw xmlns="http://schemas.gpo.gov/xml/uslm">'
        b'<ref href="/us/usc/t5/s553"/><ref href="/us/pl/119/1"/>'
        b'<ref href="/us/usc/t5/s553/nt"/>'  # note href stays unsupported_href, never collides with section
        b'<ref href="/us/usc/t10/s2304"/>'  # printed at release point, repealed in OLRC oracle
        b'<ref href="/us/usc/t41/s5"/>'  # annual-only (1994-2009), missing at release point
        b'</pLaw>'
    )
    usc_xml = b'<section><sourceCredit>(5 U.S.C. 553; Pub. L. 119-1.)</sourceCredit></section>'

    rp_target = inputs / "usc_release_point_sections.parquet"
    pq.write_table(
        pa.table({"usc_title": ["5", "10"], "usc_section_key": ["553", "2304"]}),
        rp_target,
    )

    def write_src(name, body):
        (inputs / name).write_bytes(body)
        return "sha256:" + hashlib.sha256(body).hexdigest()

    manifest = inputs / "manifest.json"
    manifest.write_text(json.dumps({
        "targets": {
            "usc_release_point_sections": {
                "path": rp_target.name,
                "sha256": "sha256:" + hashlib.sha256(rp_target.read_bytes()).hexdigest(),
            }
        },
        "sources": [
            {
                "path": "bill.xml", "sha256": write_src("bill.xml", bill_xml),
                "source_family": "bill", "source_record_key": "BILLS-119hr1ih",
                "edition": "ih", "source_locator": "synthetic:bill",
            },
            {
                "path": "law.xml", "sha256": write_src("law.xml", law_xml),
                "source_family": "law", "source_record_key": "119-public-1",
                "edition": None, "source_locator": "synthetic:law",
            },
            {
                "path": "usc.xml", "sha256": write_src("usc.xml", usc_xml),
                "source_family": "uscode", "source_record_key": "/us/usc/t5/s553",
                "edition": "119-103", "source_locator": "synthetic:uscode",
            },
        ],
    }))

    output = tmp_path / "output"
    NativeLegalReferencesRollup(manifest=manifest, output_dir=output, skip_upload=True).run()
    gen, = (output / "generations").iterdir()
    artifact = verify_generation(gen)
    assert artifact.pin.artifact_digest == f"sha256:{gen.name}"
    assert "etlReceipts" not in artifact.root["spec"]

    # Confirm sealed native_legal_references carries rule/2 provenance and found status for printed sections.
    sealed_rows = pq.read_table(gen / "native_legal_references.parquet").to_pylist()
    usc_cands = [
        c for row in sealed_rows for c in (row["target_candidates"] or ()) if c and c["target_kind"] == "usc_section"
    ]
    assert {c["resolution_rule"] for c in usc_cands} == {"selected-target-lookup/2"}
    assert {c["target_table_selected"] for c in usc_cands} == {"usc_release_point_sections"}
    by_key = {c["normalized_key"]: c for c in usc_cands}
    assert by_key["5-553"]["target_status"] == "found" and by_key["5-553"]["match_basis"] == "printed_section"
    assert by_key["5-553"]["candidate_keys"][0]["usc_title"] == "5"
    assert by_key["5-553"]["candidate_keys"][0]["usc_section_key"] == "553"
    assert by_key["10-2304"]["target_status"] == "found"
    assert by_key["41-5"]["target_status"] == "missing"

    with duckdb.connect() as con:
        _table(con, "uscode_edition_sections",
               ("observation_id", "edition_id", "source_kind", "source_context", "native_identifier"),
               [("o1", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t5/s553"),
                ("o2", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t10/s2304")])
        _table(con, "document_citations", ("cite_kind", "target_key", "target_resolved"), [])
        _table(con, "law_code_sections", ("usc_title", "usc_section_key", "usc_place", "action"),
               [("5", "553", "note", "nt new")])
        _table(con, "table3_records", ("usc_title", "usc_section_key", "usc_place"), [])
        sealed_parquet = str(gen / "native_legal_references.parquet").replace("'", "''")
        con.execute(f"CREATE VIEW native_legal_references AS SELECT * FROM read_parquet('{sealed_parquet}')")
        installed = install_sql_views(
            con,
            ("uscode_edition_sections", "document_citations", "law_code_sections",
             "table3_records", "native_legal_references"),
            USCODE_VIEWS,
        )
        assert installed["usc_citation_verdicts"]["status"] == "available"
        rows = {
            (t, k, p): (dc, lcs, t3, nlr, rp, v, r, os, ao)
            for t, k, p, dc, lcs, t3, nlr, rp, v, r, os, ao in con.execute("""
                SELECT usc_title, usc_section_key, usc_place,
                       document_citation_count, law_code_section_count, table3_record_count,
                       native_legal_reference_count, release_point_rows, verdict, reason,
                       oracle_status, annual_only
                FROM usc_citation_verdicts
            """).fetchall()
        }
        assert rows == {
            ("5", "553", "section"): (0, 0, 0, 2, 1, "printed_at_release_point", None, "current", False),
            ("5", "553", "unstated"): (0, 0, 0, 1, 1, "printed_at_release_point", None, "current", False),
            ("5", "553", "note"): (0, 1, 0, 0, 0, "not_checked_non_section_place", "non_section_place", None, None),
            ("10", "2304", "section"): (0, 0, 0, 1, 1, "printed_at_release_point", None, "repealed", False),
            ("41", "5", "section"): (0, 0, 0, 1, 0, "not_printed_at_release_point", "annual_only", None, True),
        }
