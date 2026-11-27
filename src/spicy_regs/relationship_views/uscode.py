"""U.S. Code section keys the held tables cite or classify, each checked against the held OLRC release point and oracle.

The release point is ``uscode_edition_sections`` (``source_kind = 'release-point-corpus'``): the main-body sections it
prints on one date. A key it does not print may still exist: a law enacted after that date, an appendix title it omits,
or an annual edition this host holds only in the vendored 1994-2024 oracle. The verdict therefore never says absent,
and printed-at-release is never conflated with current-law applicability.

Under ``usc-release-point-verdict-v3``, ``usc_citation_verdicts`` preserves ``usc_place`` in its primary key
``(usc_title, usc_section_key, usc_place)`` across ``document_citations``, ``law_code_sections``, ``table3_records``,
and sealed ``native_legal_references`` (unnested from native Arrow ``target_candidates``), and enriches main-body
(``section`` / ``unstated``) rows with:
- release-point legal status from SHA-256-pinned ``usc_oracle_sections`` and ``usc_oracle_ranges``
  (``current`` vs ``repealed`` / ``transferred`` / ``omitted`` / ``reserved`` / ``renumbered`` / ``vacant`` /
  ``unknown`` / ``multiple``);
- 1994-2024 annual-edition presence from SHA-256-pinned ``usc_oracle_annual_sections`` and
  ``usc_oracle_annual_ranges`` (``annual_presence``, ``annual_only``, year span, and ``reason = 'annual_only'``).

Non-section places (``note``, ``preceding``, ``note_preceding``) never join release-point or oracle main-body tables
and emit ``verdict = 'not_checked_non_section_place'`` with ``reason = 'non_section_place'`` and NULL oracle/annual
columns. Automatic letter-suffix vs. subsection repair remains explicitly unsupported.
"""

import json

from spicy_regs.identifiers import section_order_sql, usc_section_key_sql

from .core import literal
from .reference import reference_table_pins, reference_table_sql
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
_ORACLE_TABLES = (
    "usc_oracle_sections",
    "usc_oracle_ranges",
    "usc_oracle_annual_sections",
    "usc_oracle_annual_ranges",
)


def _query(p):
    sources = literal(json.dumps({table: p.get(table) for table in _SOURCES}, sort_keys=True))
    oracle_pins = literal(json.dumps(reference_table_pins(_ORACLE_TABLES), sort_keys=True))
    oracle_sections_sql = reference_table_sql("usc_oracle_sections")
    oracle_ranges_sql = reference_table_sql("usc_oracle_ranges")
    oracle_annual_sections_sql = reference_table_sql("usc_oracle_annual_sections")
    oracle_annual_ranges_sql = reference_table_sql("usc_oracle_annual_ranges")
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
               bool_or(classified_new) AS classified_new,
               {section_order_sql("usc_section_key")} AS sec_order
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
          AND e.sec_order.stem IS NOT NULL
          AND e.sec_order BETWEEN s.low AND s.high
        GROUP BY e.usc_title, e.usc_section_key, e.usc_place),
    oracle_sec AS (
        SELECT CAST(title AS VARCHAR) AS usc_title,
               {usc_section_key_sql("section")} AS section,
               list(DISTINCT status ORDER BY status) AS statuses
        FROM {oracle_sections_sql}
        GROUP BY 1, 2),
    oracle_rng_spans AS (
        SELECT CAST(title AS VARCHAR) AS usc_title,
               {section_order_sql(usc_section_key_sql("lo"))} AS low,
               {section_order_sql(usc_section_key_sql("hi"))} AS high,
               status
        FROM {oracle_ranges_sql}),
    oracle_exact AS (
        SELECT e.*,
               CASE WHEN e.usc_place IN ('section', 'unstated') THEN o.statuses ELSE NULL END AS sec_statuses
        FROM exact e
        LEFT JOIN oracle_sec o
               ON e.usc_place IN ('section', 'unstated')
              AND e.usc_title = o.usc_title
              AND e.usc_section_key = o.section),
    oracle_rng AS (
        SELECT e.usc_title, e.usc_section_key, e.usc_place,
               list(DISTINCT r.status ORDER BY r.status) AS statuses
        FROM oracle_exact e
        JOIN oracle_rng_spans r ON e.usc_title = r.usc_title
        WHERE e.usc_place IN ('section', 'unstated')
          AND e.sec_statuses IS NULL
          AND e.sec_order.stem IS NOT NULL
          AND e.sec_order BETWEEN r.low AND r.high
        GROUP BY e.usc_title, e.usc_section_key, e.usc_place),
    ann_sec AS (
        SELECT e.usc_title, e.usc_section_key, e.usc_place,
               list(DISTINCT a.year ORDER BY a.year) AS years
        FROM exact e
        JOIN (
            SELECT CAST(title AS VARCHAR) || CASE WHEN appendix THEN 'A' ELSE '' END AS usc_title,
                   {usc_section_key_sql("section")} AS section,
                   CAST(year AS INTEGER) AS year
            FROM {oracle_annual_sections_sql}
        ) a ON e.usc_title = a.usc_title AND e.usc_section_key = a.section
        WHERE e.usc_place IN ('section', 'unstated')
        GROUP BY e.usc_title, e.usc_section_key, e.usc_place),
    ann_rng_spans AS (
        SELECT CAST(title AS VARCHAR) || CASE WHEN appendix THEN 'A' ELSE '' END AS usc_title,
               {section_order_sql(usc_section_key_sql("lo"))} AS low,
               {section_order_sql(usc_section_key_sql("hi"))} AS high,
               list(DISTINCT CAST(year AS INTEGER) ORDER BY CAST(year AS INTEGER)) AS years
        FROM {oracle_annual_ranges_sql}
        GROUP BY 1, 2, 3),
    ann_rng AS (
        SELECT e.usc_title, e.usc_section_key, e.usc_place,
               list(DISTINCT y.yr ORDER BY y.yr) AS years
        FROM exact e
        JOIN ann_rng_spans ar ON e.usc_title = ar.usc_title
        CROSS JOIN UNNEST(ar.years) AS y(yr)
        WHERE e.usc_place IN ('section', 'unstated')
          AND e.sec_order.stem IS NOT NULL
          AND e.sec_order BETWEEN ar.low AND ar.high
        GROUP BY e.usc_title, e.usc_section_key, e.usc_place),
    enriched AS (
        SELECT e.usc_title, e.usc_section_key, e.usc_place,
               e.document_citation_count, e.law_code_section_count,
               e.table3_record_count, e.native_legal_reference_count,
               e.classified_new,
               e.release_point_rows, e.release_point_editions,
               CASE WHEN e.usc_place IN ('section', 'unstated') THEN r.stubs ELSE NULL END AS release_point_range_stubs,
               CASE WHEN e.usc_place NOT IN ('section', 'unstated') THEN NULL
                    ELSE coalesce(e.sec_statuses, orng.statuses) END AS oracle_statuses,
               CASE WHEN e.usc_place NOT IN ('section', 'unstated') THEN NULL
                    WHEN e.sec_statuses IS NOT NULL THEN 'oracle_section'
                    WHEN orng.statuses IS NOT NULL THEN 'oracle_range'
                    ELSE NULL END AS oracle_status_source,
               CASE WHEN e.usc_place NOT IN ('section', 'unstated') THEN NULL
                    WHEN a.years IS NOT NULL AND ar.years IS NOT NULL THEN 'annual_section_and_range_stub'
                    WHEN a.years IS NOT NULL THEN 'annual_section'
                    WHEN ar.years IS NOT NULL THEN 'annual_range_stub'
                    ELSE 'none' END AS annual_presence,
               CASE WHEN e.usc_place NOT IN ('section', 'unstated') THEN NULL
                    ELSE list_distinct(list_concat(
                        coalesce(a.years, CAST([] AS INTEGER[])),
                        coalesce(ar.years, CAST([] AS INTEGER[]))
                    )) END AS all_annual_years
        FROM oracle_exact e
        LEFT JOIN ranged r
               ON e.usc_title = r.usc_title
              AND e.usc_section_key = r.usc_section_key
              AND e.usc_place = r.usc_place
        LEFT JOIN oracle_rng orng
               ON e.usc_title = orng.usc_title
              AND e.usc_section_key = orng.usc_section_key
              AND e.usc_place = orng.usc_place
        LEFT JOIN ann_sec a
               ON e.usc_title = a.usc_title
              AND e.usc_section_key = a.usc_section_key
              AND e.usc_place = a.usc_place
        LEFT JOIN ann_rng ar
               ON e.usc_title = ar.usc_title
              AND e.usc_section_key = ar.usc_section_key
              AND e.usc_place = ar.usc_place)
    SELECT usc_title, usc_section_key, usc_place,
           document_citation_count, law_code_section_count,
           table3_record_count, native_legal_reference_count,
           release_point_rows, release_point_editions, release_point_range_stubs,
           CASE WHEN usc_place NOT IN ('section', 'unstated') THEN 'not_checked_non_section_place'
                WHEN release_point_rows > 0 THEN 'printed_at_release_point'
                WHEN release_point_range_stubs IS NOT NULL THEN 'in_range_stub'
                ELSE 'not_printed_at_release_point' END AS verdict,
           CASE WHEN usc_place NOT IN ('section', 'unstated') THEN 'non_section_place'
                WHEN release_point_rows > 0 OR release_point_range_stubs IS NOT NULL THEN NULL
                WHEN regexp_full_match(usc_title, '[0-9]+[A-Z]') THEN 'appendix_title'
                WHEN classified_new THEN 'classified_as_new'
                WHEN oracle_statuses IS NULL AND annual_presence <> 'none' THEN 'annual_only'
                ELSE 'not_printed' END AS reason,
           CASE WHEN oracle_statuses IS NULL THEN NULL
                WHEN len(oracle_statuses) = 1 THEN oracle_statuses[1]
                ELSE 'multiple' END AS oracle_status,
           oracle_statuses,
           oracle_status_source,
           annual_presence,
           CASE WHEN usc_place NOT IN ('section', 'unstated') THEN NULL
                ELSE (release_point_rows = 0 AND release_point_range_stubs IS NULL
                      AND oracle_statuses IS NULL AND annual_presence <> 'none') END AS annual_only,
           CASE WHEN all_annual_years IS NOT NULL AND len(all_annual_years) > 0
                THEN CAST(list_min(all_annual_years) AS INTEGER) ELSE NULL END AS annual_first_year,
           CASE WHEN all_annual_years IS NOT NULL AND len(all_annual_years) > 0
                THEN CAST(list_max(all_annual_years) AS INTEGER) ELSE NULL END AS annual_last_year,
           CASE WHEN all_annual_years IS NULL THEN NULL
                ELSE CAST(len(all_annual_years) AS BIGINT) END AS annual_edition_count,
           {sources} AS source_publication_json,
           {pin(p, 'uscode_edition_sections')} AS target_publication_json,
           {oracle_pins} AS oracle_publication_json,
           'usc-release-point-verdict-v3' AS rule_version
    FROM enriched"""


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
        'only: the usc_section target of resolve_document_citations and native_legal_references under '
        'selected-target-lookup/2. A range stub (low...high) is one row with bounds in Code order; duplicate '
        'OLRC numberings stay one row.',
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
        'Each U.S. Code (usc_title, usc_section_key, usc_place) key named by held citations, classifications, Table III '
        'or native XML references, with release-point presence, vendored OLRC oracle legal status (current vs '
        'repealed/transferred/omitted/reserved) and 1994-2024 annual-edition presence. Non-section places (note, '
        'preceding, note_preceding) stay separate as not_checked_non_section_place. Not printed is not absent; '
        'letter-suffix repair is unsupported.',
        ('usc_title', 'usc_section_key', 'usc_place'), rule_version='usc-release-point-verdict-v3',
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
                      'exactly new, never nt new), annual_only (absent from the release point and release-point '
                      'oracle, but present in 1994-2024 annual editions) or not_printed; NULL when printed or in a '
                      'range stub.',
            'oracle_status': 'Legal status at OLRC release point 119-102 from vendored usc_oracle_sections (or '
                             'usc_oracle_ranges when spanned by a range stub only): current, repealed, transferred, '
                             'omitted, renumbered, vacant, reserved, unknown, or multiple when duplicate OLRC '
                             'numberings carry more than one status (see oracle_statuses); NULL when absent from '
                             'the release-point oracle or for non-section places.',
            'oracle_statuses': 'Sorted distinct legal statuses from usc_oracle_sections (or usc_oracle_ranges when '
                               'matched by range stub only); preserves all statuses on duplicate OLRC numberings; '
                               'NULL when absent from the release-point oracle or for non-section places.',
            'oracle_status_source': 'Which vendored release-point oracle table supplied oracle_status: '
                                    'oracle_section (usc_oracle_sections) or oracle_range (usc_oracle_ranges); '
                                    'NULL when absent or for non-section places.',
            'annual_presence': '1994-2024 annual-edition presence from vendored usc_oracle_annual_sections and '
                               'usc_oracle_annual_ranges (matching title and appendix flag exactly): annual_section, '
                               'annual_range_stub, annual_section_and_range_stub, or none; NULL for non-section places.',
            'annual_only': 'True when a checked section/unstated key is absent from both the held release point and '
                           'the release-point oracle (oracle_status IS NULL) yet present in 1994-2024 annual editions; '
                           'False otherwise for checked keys; NULL for non-section places.',
            'annual_first_year': 'Earliest 1994-2024 annual edition year printing or spanning this key in the vendored '
                                 'annual oracle; NULL when absent or for non-section places.',
            'annual_last_year': 'Latest 1994-2024 annual edition year printing or spanning this key in the vendored '
                                'annual oracle; NULL when absent or for non-section places.',
            'annual_edition_count': 'Count of distinct 1994-2024 annual edition years matching this key in '
                                    'usc_oracle_annual_sections or usc_oracle_annual_ranges; 0 when checked and '
                                    'absent; NULL for non-section places.',
            'source_publication_json': 'Host-selected publications of document_citations, law_code_sections, '
                                       'table3_records and native_legal_references; JSON null means unprovided.',
            'oracle_publication_json': 'Pinned SHA-256 digests of the four vendored RefSpec oracle tables read by '
                                       'this view (usc_oracle_sections, usc_oracle_ranges, '
                                       'usc_oracle_annual_sections, usc_oracle_annual_ranges).',
        }),
)
