"""U.S. Code section keys the held tables cite or classify, each checked against the held OLRC release point.

The release point is ``uscode_edition_sections`` (``source_kind = 'release-point-corpus'``): the main-body sections it
prints on one date. A key it does not print may still exist: a law enacted after that date, an appendix title it omits,
or an annual edition this host does not hold. The verdict therefore never says absent.

Under ``usc-release-point-verdict-v2``, ``usc_citation_verdicts`` preserves ``usc_place`` in its primary key
``(usc_title, usc_section_key, usc_place)`` across ``document_citations``, ``law_code_sections``, ``table3_records``,
and sealed ``native_legal_references`` (unnested from native Arrow ``target_candidates``). Because
``usc_release_point_sections`` indexes only main-body sections, non-section places (``note``, ``preceding``,
``note_preceding``) are never checked against it and emit ``verdict = 'not_checked_non_section_place'`` with
``reason = 'non_section_place'`` — protecting downstream ``nt new`` negative controls and Wave 4 ``usc_oracle_*``
enrichment (``ss-xpyk``) while keeping ``native_legal_references`` on ``selected-target-lookup/1`` and
``native-legal-reference/003``.
"""

import json

from spicy_regs.identifiers import section_order_sql, usc_section_key_sql

from .core import literal
from .sql_views import SQLView, pin

#: One section in a release-point identifier: its title (an appendix title keeps its letter, ``t18a``) and section.
_SECTION_ID = r"/us/usc/t([0-9]+[a-z]?)/s([^/ ]+)"

#: Each section identifier a held release-point body observation names, one row apiece: a combined heading names
#: several, space-separated. ``section`` is folded as spicy-docs folds a citation's (``usc_section_key``); a range
#: stub's (``2601...2606``) keeps its ``...``. An identifier of another shape (``t18a/pl/91/538/s1``) names no
#: Code section and is left out.
RELEASE_POINT_SECTIONS_SQL = f"""SELECT observation_id, edition_id,
        upper(regexp_extract(identifier, '{_SECTION_ID}', 1)) AS usc_title,
        {usc_section_key_sql(f"regexp_extract(identifier, '{_SECTION_ID}', 2)")} AS section
    FROM (SELECT observation_id, edition_id, unnest(string_split(native_identifier, ' ')) AS identifier
          FROM uscode_edition_sections
          WHERE source_kind = 'release-point-corpus' AND source_context = 'main-body-candidate')
    WHERE regexp_full_match(identifier, '{_SECTION_ID}')"""


_SOURCES = ("document_citations", "law_code_sections", "table3_records", "native_legal_references")


def _query(p):
    sources = literal(json.dumps({table: p.get(table) for table in _SOURCES}, sort_keys=True))
    return f"""WITH cited AS (
        SELECT upper(regexp_extract(target_key, '^([^-]+)-', 1)) AS usc_title,
               {usc_section_key_sql("regexp_extract(target_key, '^[^-]+-(.+)$', 1)")} AS usc_section_key,
               'unstated' AS usc_place,
               'document_citations' AS source_table, false AS classified_new
        FROM document_citations
        WHERE cite_kind = 'usc_section' AND lower(CAST(target_resolved AS VARCHAR)) = 'true'
        UNION ALL
        SELECT upper(usc_title), {usc_section_key_sql("usc_section_key")},
               coalesce(nullif(lower(trim(usc_place)), ''), 'unstated'),
               'law_code_sections', coalesce(action = 'new', false)
        FROM law_code_sections
        UNION ALL
        SELECT upper(usc_title), {usc_section_key_sql("usc_section_key")},
               coalesce(nullif(lower(trim(usc_place)), ''), 'unstated'),
               'table3_records', false
        FROM table3_records
        UNION ALL
        SELECT upper(regexp_extract(c.cand.target_key, '^([^-]+)-', 1)),
               {usc_section_key_sql("regexp_extract(c.cand.target_key, '^[^-]+-(.+)$', 1)")},
               CASE WHEN n.interpretation_status IN ('native_section_href', 'native_bill_usc_cite')
                    THEN 'section'
                    ELSE 'unstated' END,
               'native_legal_references', false
        FROM native_legal_references n, UNNEST(n.target_candidates) AS c(cand)
        WHERE c.cand.target_kind = 'usc_section'
          AND lower(CAST(c.cand.target_resolved AS VARCHAR)) = 'true'),
    keys AS (
        SELECT usc_title, usc_section_key, usc_place,
               count(*) FILTER (WHERE source_table = 'document_citations') AS document_citation_count,
               count(*) FILTER (WHERE source_table = 'law_code_sections') AS law_code_section_count,
               count(*) FILTER (WHERE source_table = 'table3_records') AS table3_record_count,
               count(*) FILTER (WHERE source_table = 'native_legal_references') AS native_legal_reference_count,
               bool_or(classified_new) AS classified_new
        FROM cited WHERE coalesce(usc_title, '') <> '' AND coalesce(usc_section_key, '') <> ''
        GROUP BY usc_title, usc_section_key, usc_place),
    release AS ({RELEASE_POINT_SECTIONS_SQL}),
    printed AS (
        SELECT usc_title, section, count(*) AS release_point_rows, list(DISTINCT edition_id ORDER BY edition_id) AS editions
        FROM release WHERE section NOT LIKE '%...%' GROUP BY usc_title, section),
    stubs AS (
        SELECT usc_title, section AS stub, {section_order_sql("split_part(section, '...', 1)")} AS low,
               {section_order_sql("split_part(section, '...', 2)")} AS high
        FROM release WHERE section LIKE '%...%'),
    exact AS (
        SELECT k.*,
               CASE WHEN k.usc_place IN ('section', 'unstated')
                    THEN coalesce(p.release_point_rows, 0) ELSE 0 END AS release_point_rows,
               CASE WHEN k.usc_place IN ('section', 'unstated')
                    THEN p.editions ELSE NULL END AS release_point_editions
        FROM keys k
        LEFT JOIN printed p
               ON k.usc_place IN ('section', 'unstated')
              AND k.usc_title = p.usc_title
              AND k.usc_section_key = p.section),
    -- The range stage reads only unprinted section/unstated keys and only stub rows, joined on the title alone: a
    -- range term beside the equality key in one ON clause makes DuckDB choose a blockwise nested loop (courts.py).
    ranged AS (
        SELECT e.usc_title, e.usc_section_key, e.usc_place, list(DISTINCT s.stub ORDER BY s.stub) AS stubs
        FROM exact e JOIN stubs s ON e.usc_title = s.usc_title
        WHERE e.usc_place IN ('section', 'unstated')
          AND e.release_point_rows = 0
          AND {section_order_sql("e.usc_section_key")} BETWEEN s.low AND s.high
        GROUP BY e.usc_title, e.usc_section_key, e.usc_place)
    SELECT e.usc_title, e.usc_section_key, e.usc_place,
           e.document_citation_count, e.law_code_section_count,
           e.table3_record_count, e.native_legal_reference_count,
           e.release_point_rows, e.release_point_editions,
           CASE WHEN e.usc_place IN ('section', 'unstated') THEN r.stubs ELSE NULL END AS release_point_range_stubs,
           CASE WHEN e.usc_place NOT IN ('section', 'unstated') THEN 'not_checked_non_section_place'
                WHEN e.release_point_rows > 0 THEN 'printed_at_release_point'
                WHEN r.stubs IS NOT NULL THEN 'in_range_stub'
                ELSE 'not_printed_at_release_point' END AS verdict,
           CASE WHEN e.usc_place NOT IN ('section', 'unstated') THEN 'non_section_place'
                WHEN e.release_point_rows > 0 OR r.stubs IS NOT NULL THEN NULL
                WHEN regexp_full_match(e.usc_title, '[0-9]+[A-Z]') THEN 'appendix_title'
                WHEN e.classified_new THEN 'classified_as_new'
                ELSE 'not_printed' END AS reason,
           {sources} AS source_publication_json,
           {pin(p, 'uscode_edition_sections')} AS target_publication_json,
           'usc-release-point-verdict-v2' AS rule_version
    FROM exact e
    LEFT JOIN ranged r
           ON e.usc_title = r.usc_title
          AND e.usc_section_key = r.usc_section_key
          AND e.usc_place = r.usc_place"""


def _release_point_sections(_p):
    return f"""SELECT usc_title, usc_section_key,
           CASE WHEN usc_section_key LIKE '%...%' THEN {section_order_sql("split_part(usc_section_key, '...', 1)")} END AS range_low,
           CASE WHEN usc_section_key LIKE '%...%' THEN {section_order_sql("split_part(usc_section_key, '...', 2)")} END AS range_high
    FROM (SELECT DISTINCT usc_title, section AS usc_section_key FROM ({RELEASE_POINT_SECTIONS_SQL}))"""


USCODE_VIEWS = (
    SQLView(
        'usc_release_point_sections', {
            'uscode_edition_sections': ('observation_id', 'edition_id', 'source_kind', 'source_context',
                                        'native_identifier'),
        }, _release_point_sections,
        'Each U.S. Code section the held OLRC release point prints, once per (usc_title, usc_section_key), keys '
        'only: the target resolve_document_citations looks a usc_section citation up in. A range stub '
        '(low...high, usually repealed or omitted sections) is one row with its bounds in Code order. An OLRC '
        'duplicate numbering is one row, not two sections.',
        ('usc_title', 'usc_section_key'), rule_version='usc-release-point-sections-v1',
        column_descriptions={
            'usc_title': 'U.S. Code title as the release point keys it; an appendix title keeps its upper-case letter '
                         '(50A, 18A).',
            'usc_section_key': 'Section folded as spicy-docs usc_section_key folds a citation: trimmed, lower-cased, '
                               'every dash spelling an ASCII hyphen; a range stub keeps its low...high.',
            'range_low': 'For a range stub, its first section in Code order (stem, letters length, letters, dash '
                         'number, rest); NULL for a printed section.',
            'range_high': 'For a range stub, its last section in Code order; NULL for a printed section.',
        }),
    SQLView(
        'usc_citation_verdicts', {
            'document_citations': ('cite_kind', 'target_key', 'target_resolved'),
            'law_code_sections': ('usc_title', 'usc_section_key', 'usc_place', 'action'),
            'table3_records': ('usc_title', 'usc_section_key', 'usc_place'),
            'native_legal_references': ('interpretation_status', 'target_candidates'),
            'uscode_edition_sections': ('observation_id', 'edition_id', 'source_kind', 'source_context',
                                        'native_identifier'),
        }, _query,
        'Each U.S. Code (usc_title, usc_section_key, usc_place) key the held text citations, classification tables, '
        'Table III rows and native XML legal references name, with how many times each names it and whether the held '
        'OLRC release point prints that main-body section. Statutory notes and preceding places (note, preceding, '
        'note_preceding) are kept separate from main sections and receive not_checked_non_section_place rather than '
        'joining main-body release-point rows. Not printed is not absent: a section enacted after the release point, '
        'an appendix title it omits, or a section only an annual edition holds reads not printed. A reason is what '
        'was observed, never a correction.',
        ('usc_title', 'usc_section_key', 'usc_place'), rule_version='usc-release-point-verdict-v2',
        column_descriptions={
            'usc_title': 'U.S. Code title as the release point keys it; an appendix title keeps its upper-case letter '
                         '(50A, 18A).',
            'usc_section_key': 'Section folded as spicy-docs usc_section_key folds a citation: trimmed, lower-cased, '
                               'every dash spelling an ASCII hyphen.',
            'usc_place': 'Code place preserved from the naming source: section, note, preceding, note_preceding '
                         '(verbatim from law_code_sections and table3_records, with NULL/blank coalesced to unstated), '
                         'section for exact native XML section targets (native_section_href, native_bill_usc_cite) in '
                         'native_legal_references, and unstated for text-parsed citations in document_citations and '
                         'partial_text_findings in native_legal_references.',
            'document_citation_count': 'Settled usc_section citations in document_citations naming this key '
                                       '(always under usc_place = unstated).',
            'law_code_section_count': 'law_code_sections classification rows naming this (title, section, place) key.',
            'table3_record_count': 'table3_records rows naming this (title, section, place) key.',
            'native_legal_reference_count': 'Settled usc_section candidates in native_legal_references.target_candidates '
                                            'naming this (title, section, place) key.',
            'release_point_rows': 'Held main-body release-point observations whose identifier names this exact section '
                                  '(checked only when usc_place is section or unstated; 0 for non-section places). '
                                  'More than one is an OLRC duplicate numbering, not two sections.',
            'release_point_editions': 'Each held release-point edition_id printing this exact main-body section; '
                                      'NULL when unprinted or for non-section places.',
            'release_point_range_stubs': 'For an unprinted section or unstated key, each release-point range stub '
                                         '(low...high, in Code order: integer stem, letters by length then spelling, '
                                         'the number after a dash, then any rest) spanning it; NULL for non-section '
                                         'places. A stub is the publisher\'s placeholder for a span, usually repealed '
                                         'or omitted, not printed text.',
            'verdict': 'printed_at_release_point, in_range_stub, not_printed_at_release_point (for section and '
                       'unstated keys; checking unstated against main-body sections does not assert the citation '
                       'meant a section rather than a note), or not_checked_non_section_place (for note, preceding '
                       'and note_preceding); never absent.',
            'reason': 'non_section_place for not_checked_non_section_place; for not_printed_at_release_point: '
                      'appendix_title (a lettered title), classified_as_new (a law_code_sections row states action '
                      'new, so a later law may have created it) or not_printed; NULL when printed or in a range stub.',
            'source_publication_json': 'Host-selected publications of document_citations, law_code_sections, '
                                       'table3_records and native_legal_references; JSON null means unprovided.',
        }),
)
