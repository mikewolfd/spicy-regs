"""usc_citation_verdicts: every held U.S. Code key once, checked against the release point, never read as absent."""

import duckdb
import pytest

from spicy_regs.citation_resolution import ROUTES, resolve_citations
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.relationship_views.uscode import USCODE_VIEWS


def _table(con, name, fields, records):
    con.execute(f'CREATE TABLE {name} (' + ','.join(f'{field} VARCHAR' for field in fields) + ')')
    con.executemany(f'INSERT INTO {name} VALUES (' + ','.join('?' for _ in fields) + ')', records)


#: Release-point rows shaped as generation 792cbaa7 holds them: an en dash, an upper-case suffix, a combined heading,
#: range stubs (one spanning lettered sections, one crossing a digit count, two whose bounds carry a dash number,
#: one crossing a letter count), an OLRC duplicate numbering, an
#: appendix identifier of another shape, a notes row with no identifier, and an annual-edition row that must not count.
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
    ("o11", "annual-2000", "annual", "main-body-candidate", "/us/usc/t7/s1"),
]

CITATIONS = [  # (cite_kind, target_key, target_resolved)
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
    ("usc_section", "50A-541", "true"),
    ("usc_section", "7-1", "true"),  # held only in an annual edition
    ("usc_section", "10-130g", "true"),
    ("usc_section", "9-99", "false"),  # unsettled spelling: never a key
    ("public_law", "117-328", "true"),
]

CLASSIFICATIONS = [  # (usc_title, usc_section_key, action)
    ("5", "553", "nt new"),
    ("22", "10801", "new"),
    ("14", "319", "tr to 14/336"),
]

TABLE3 = [("5", "553"), ("12", "6")]

EXPECTED = {  # key: (citations, classifications, table3, release rows, stubs, verdict, reason)
    ("5", "553"): (2, 1, 1, 1, None, "printed_at_release_point", None),
    ("42", "2000e-5"): (1, 0, 0, 1, None, "printed_at_release_point", None),
    ("26", "45q"): (1, 0, 0, 1, None, "printed_at_release_point", None),
    ("12", "6"): (1, 0, 1, 1, None, "printed_at_release_point", None),
    ("25", "2601"): (1, 0, 0, 0, ["2601...2606"], "in_range_stub", None),
    ("25", "2603"): (1, 0, 0, 0, ["2601...2606"], "in_range_stub", None),
    ("12", "34b"): (1, 0, 0, 0, ["33...34c"], "in_range_stub", None),
    ("12", "34d"): (1, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("15", "100"): (1, 0, 0, 0, ["99...101"], "in_range_stub", None),
    ("16", "698v-4"): (1, 0, 0, 0, ["698v...698v-10"], "in_range_stub", None),
    ("16", "698w"): (1, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("16", "1857f-6a"): (1, 0, 0, 0, ["1857f-5a...1857f-6c"], "in_range_stub", None),
    ("16", "1857f-7"): (1, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("42", "2689s"): (1, 0, 0, 0, ["2689r...2689aa"], "in_range_stub", None),
    ("42", "2689ab"): (1, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("50A", "541"): (1, 0, 0, 0, None, "not_printed_at_release_point", "appendix_title"),
    ("7", "1"): (1, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("10", "130g"): (1, 0, 0, 2, None, "printed_at_release_point", None),
    ("22", "10801"): (0, 1, 0, 0, None, "not_printed_at_release_point", "classified_as_new"),
    ("14", "319"): (0, 1, 0, 0, None, "not_printed_at_release_point", "not_printed"),
}


@pytest.fixture
def con():
    con = duckdb.connect()
    _table(con, "uscode_edition_sections",
           ("observation_id", "edition_id", "source_kind", "source_context", "native_identifier"), RELEASE)
    _table(con, "document_citations", ("cite_kind", "target_key", "target_resolved"), CITATIONS)
    _table(con, "law_code_sections", ("usc_title", "usc_section_key", "action"), CLASSIFICATIONS)
    _table(con, "table3_records", ("usc_title", "usc_section_key"), TABLE3)
    tables = ("uscode_edition_sections", "document_citations", "law_code_sections", "table3_records")
    installed = install_sql_views(con, tables, USCODE_VIEWS)
    assert {name: entry["status"] for name, entry in installed.items()} == {
        "usc_citation_verdicts": "available", "usc_release_point_sections": "available"}
    return con


def test_each_key_reads_its_release_point_verdict_and_observed_reason(con):
    rows = con.execute("""SELECT usc_title, usc_section_key, document_citation_count, law_code_section_count,
        table3_record_count, release_point_rows, release_point_range_stubs, verdict, reason
        FROM usc_citation_verdicts""").fetchall()
    assert {(title, key): tuple(rest) for title, key, *rest in rows} == EXPECTED


def test_one_row_per_key_and_every_source_key_is_counted_both_ways(con):
    view = {(t, k) for t, k in con.execute("SELECT usc_title, usc_section_key FROM usc_citation_verdicts").fetchall()}
    assert len(view) == con.execute("SELECT count(*) FROM usc_citation_verdicts").fetchone()[0]
    sources = {tuple(target.split("-", 1)) for kind, target, settled in CITATIONS
               if kind == "usc_section" and settled == "true"}
    sources |= {(title, key) for title, key, _ in CLASSIFICATIONS} | set(TABLE3)
    assert view == sources


def test_the_view_never_says_absent(con):
    verdicts = {row[0] for row in con.execute("SELECT DISTINCT verdict FROM usc_citation_verdicts").fetchall()}
    assert verdicts == {"printed_at_release_point", "in_range_stub", "not_printed_at_release_point"}


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
    assert outcomes == {key: by_verdict[expected[5]] for key, expected in EXPECTED.items() if expected[0]}
    # An OLRC duplicate numbering is one target, not two.
    [duplicate] = [row for row in resolved["occurrences"] if row["target_key"] == "10-130g"]
    assert duplicate["candidate_keys"] == [{"usc_title": "10", "usc_section_key": "130g"}]
    assert resolved["resolution_rule"] == "selected-target-lookup/2"
