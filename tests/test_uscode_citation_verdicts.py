"""usc_citation_verdicts: every held (title, section, place) key once, checked honestly, never read as absent."""

import hashlib
import json

import duckdb
import pytest

from spicy_docs.schemas.native_reference_rows import NATIVE_LEGAL_REFERENCE_RULE
from spicy_regs.citation_resolution import NATIVE_RESOLUTION_RULE, ROUTES, resolve_citations
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.legislative_receipts import FILE_POLICY, policy, write_legislative_outputs
from spicy_regs.native_types import described_schema
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.relationship_views.uscode import USCODE_VIEWS
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.native_legal_references import OUTPUTS, build_native_legal_references


def _table(con, name, fields, records):
    con.execute(f'CREATE TABLE {name} (' + ','.join(f'{field} VARCHAR' for field in fields) + ')')
    if records:
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
    ("usc_section", "50A-541", "true"),
    ("usc_section", "7-1", "true"),  # held only in an annual edition
    ("usc_section", "10-130g", "true"),
    ("usc_section", "9-99", "false"),  # unsettled spelling: never a key
    ("public_law", "117-328", "true"),
]

CLASSIFICATIONS = [  # (usc_title, usc_section_key, usc_place, action)
    ("5", "553", "note", "nt new"),
    ("5", "553", "section", "amd"),
    ("12", "6", "preceding", "prec new"),
    ("25", "2601", "note_preceding", "nt prec new"),  # must stay not_checked even though 25-2601 sits in a range stub
    ("22", "10801", "section", "new"),
    ("14", "319", "section", "tr to 14/336"),
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
    ("7", "1", "unstated"): (1, 0, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
    ("10", "130g", "unstated"): (1, 1, 0, 0, 2, None, "printed_at_release_point", None),
    ("22", "10801", "section"): (0, 1, 0, 0, 0, None, "not_printed_at_release_point", "classified_as_new"),
    ("14", "319", "section"): (0, 1, 0, 0, 0, None, "not_printed_at_release_point", "not_printed"),
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
    assert installed["usc_citation_verdicts"]["metadata"]["rule_version"] == "usc-release-point-verdict-v2"
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
        ("usc-release-point-verdict-v2",)
    ]


def test_distinct_section_unstated_and_note_places_never_collapse(con):
    rows = {
        row[0]: row[1:]
        for row in con.execute("""
            SELECT usc_place, document_citation_count, law_code_section_count, table3_record_count,
                   native_legal_reference_count, release_point_rows, release_point_editions,
                   release_point_range_stubs, verdict, reason
            FROM usc_citation_verdicts
            WHERE usc_title = '5' AND usc_section_key = '553'
        """).fetchall()
    }
    assert set(rows) == {"section", "unstated", "note"}
    assert rows["section"] == (0, 1, 1, 2, 1, ["rp-119-102"], None, "printed_at_release_point", None)
    assert rows["unstated"] == (2, 0, 0, 1, 1, ["rp-119-102"], None, "printed_at_release_point", None)
    assert rows["note"] == (0, 1, 1, 0, 0, None, None, "not_checked_non_section_place", "non_section_place")


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
               OR release_point_range_stubs IS NOT NULL)
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
    assert outcomes == {
        (title, key): by_verdict[expected[6]]
        for (title, key, place), expected in EXPECTED.items()
        if place == "unstated" and expected[0]
    }
    # An OLRC duplicate numbering is one target, not two.
    [duplicate] = [row for row in resolved["occurrences"] if row["target_key"] == "10-130g"]
    assert duplicate["candidate_keys"] == [{"usc_title": "10", "usc_section_key": "130g"}]
    assert resolved["resolution_rule"] == "selected-target-lookup/2"


def test_sealed_native_legal_references_generation_integrates_with_verdicts(tmp_path):
    """End-to-end sealed legislative-documents/2 native Arrow target_candidates -> usc_citation_verdicts."""
    assert NATIVE_RESOLUTION_RULE == "selected-target-lookup/1"
    assert NATIVE_LEGAL_REFERENCE_RULE == "native-legal-reference/003"

    inputs = tmp_path / "inputs"
    inputs.mkdir()
    bill_xml = b'<bill><external-xref legal-doc="usc" parsable-cite="usc/5/553"/></bill>'
    law_xml = (
        b'<pLaw xmlns="http://schemas.gpo.gov/xml/uslm">'
        b'<ref href="/us/usc/t5/s553"/><ref href="/us/pl/119/1"/>'
        b'<ref href="/us/usc/t5/s553/nt"/>'  # note href stays unsupported_href, never collides with section
        b'</pLaw>'
    )
    usc_xml = b'<section><sourceCredit>(5 U.S.C. 553; Pub. L. 119-1.)</sourceCredit></section>'

    def write_src(name, body):
        (inputs / name).write_bytes(body)
        return "sha256:" + hashlib.sha256(body).hexdigest()

    manifest = inputs / "manifest.json"
    manifest.write_text(json.dumps({
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
        ]
    }))

    raw_out = tmp_path / "raw_out"
    evidence = CaptureEvidence(raw_out, "native-legal-references")
    files = build_native_legal_references(manifest, raw_out, evidence=evidence)
    bundle = tmp_path / "sealed-bundle"
    write_legislative_outputs(files, bundle, generation_id="verdict-integration")
    gen = tmp_path / "generation"
    artifact = build_generation(
        gen,
        family="native-legal-references",
        files=[bundle / "native_legal_references.parquet", bundle / "native_legal_reference_reads.parquet"],
        expected_keys=list(OUTPUTS),
        schemas={name: described_schema(policy(name).subject_schema)
                 for name in ("native_legal_references", "native_legal_reference_reads")},
        inputs=evidence.inputs(),
        receipt_path=bundle / "etl_receipts.parquet",
        receipt_policies=[policy("native_legal_references"), policy("native_legal_reference_reads"), FILE_POLICY],
        receipt_generation_id="verdict-integration",
    )
    verify_generation(gen, expected_pin=artifact.pin)

    with duckdb.connect() as con:
        _table(con, "uscode_edition_sections",
               ("observation_id", "edition_id", "source_kind", "source_context", "native_identifier"),
               [("o1", "rp-119-102", "release-point-corpus", "main-body-candidate", "/us/usc/t5/s553")])
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
            (t, k, p): (dc, lcs, t3, nlr, rp, v, r)
            for t, k, p, dc, lcs, t3, nlr, rp, v, r in con.execute("""
                SELECT usc_title, usc_section_key, usc_place,
                       document_citation_count, law_code_section_count, table3_record_count,
                       native_legal_reference_count, release_point_rows, verdict, reason
                FROM usc_citation_verdicts
            """).fetchall()
        }
        assert rows == {
            ("5", "553", "section"): (0, 0, 0, 2, 1, "printed_at_release_point", None),
            ("5", "553", "unstated"): (0, 0, 0, 1, 1, "printed_at_release_point", None),
            ("5", "553", "note"): (0, 1, 0, 0, 0, "not_checked_non_section_place", "non_section_place"),
        }
