"""Locate companion records without treating recorded digests as verified bytes."""

from .sql_views import SQLView, pin


def evidence(p):
    return f"""WITH companions AS (
        SELECT s.collection_id,s.source_record_id,count(*) AS n,
            list(struct_pack(source_sha256:=source_sha256) ORDER BY source_sha256 NULLS LAST) AS candidates
        FROM fec_source_records s
        JOIN (
            SELECT DISTINCT
                json_extract_string(TRY_CAST(source_locator_json AS JSON),'$.collection_id') AS collection_id,
                json_extract_string(TRY_CAST(source_locator_json AS JSON),'$.source_record_id') AS source_record_id
            FROM fec_relationships
        ) requested ON s.collection_id=requested.collection_id AND s.source_record_id=requested.source_record_id
        GROUP BY s.collection_id,s.source_record_id
    ), observations AS (
        SELECT *,json_extract_string(TRY_CAST(source_locator_json AS JSON),'$.collection_id') AS collection_id,
            json_extract_string(TRY_CAST(source_locator_json AS JSON),'$.source_record_id') AS source_record_id,
            CASE WHEN json_type(TRY_CAST(source_locator_json AS JSON),'$.collection_id')='VARCHAR'
                      AND json_type(TRY_CAST(source_locator_json AS JSON),'$.source_record_id')='VARCHAR'
                      AND trim(json_extract_string(TRY_CAST(source_locator_json AS JSON),'$.collection_id'))<>''
                      AND trim(json_extract_string(TRY_CAST(source_locator_json AS JSON),'$.source_record_id'))<>''
                 THEN 'valid' ELSE 'unsupported' END AS locator_status
        FROM fec_relationships
    )
    SELECT r.*,coalesce(c.n,0) AS target_count,
        CASE WHEN r.locator_status<>'valid' THEN 'unsupported'
             WHEN c.n IS NULL THEN 'missing' WHEN c.n=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
        to_json(c.candidates)::VARCHAR AS companion_candidates_json,
        CASE WHEN c.n=1 THEN CASE WHEN r.source_sha256 IS NULL OR c.candidates[1].source_sha256 IS NULL
             THEN 'unavailable' WHEN r.source_sha256=c.candidates[1].source_sha256 THEN 'matches'
             ELSE 'mismatch' END ELSE 'not_checked' END AS recorded_digest_status,
        'not_checked' AS source_bytes_status,
        {pin(p, 'fec_relationships')} AS source_publication_json,
        {pin(p, 'fec_source_records')} AS target_publication_json,
        'fec-companion-location-v3' AS rule_version
    FROM observations r LEFT JOIN companions c
        ON CASE WHEN r.locator_status='valid' THEN r.collection_id END=c.collection_id
        AND r.source_record_id=c.source_record_id"""


def collection_cycles(p):
    """The cycle FEC files a collection under: the ``bulk-downloads/<even year>/`` directory of its captured URLs."""
    return f"""WITH paths AS (
        SELECT collection_id,source_family,profile,
            list_distinct(coalesce(regexp_extract_all(requested_scope_json,
                'bulk-downloads/((?:19|20)[0-9][0-9])/',1),[])) AS cycles
        FROM fec_collections
    )
    SELECT collection_id,source_family,profile,
        CASE WHEN len(cycles)=1 THEN cycles[1] END AS cycle,
        CASE len(cycles) WHEN 0 THEN 'not_stated' WHEN 1 THEN 'bulk_directory' ELSE 'ambiguous' END AS cycle_status,
        {pin(p, 'fec_collections')} AS source_publication_json,
        'fec-collection-cycle/1' AS rule_version
    FROM paths"""


FEC_VIEWS = (
    SQLView('fec_collection_cycles', {
        'fec_collections': ('collection_id','source_family','profile','requested_scope_json'),
    }, collection_cycles, 'The FEC election cycle each collection was filed under, from the publisher\'s own '
       'bulk-downloads/<year>/ directory in its captured URLs: 2026 means the 2025-2026 cycle. Join collection_id '
       'to fec_source_records to filter records by cycle. cycle is NULL (cycle_status not_stated) for API, legal, '
       'data-dictionary and other files outside a year directory, and for a collection whose captures span two '
       'cycles (ambiguous); no cycle is read from a file name or a query parameter.',
       ('collection_id',), rule_version='fec-collection-cycle/1',
       column_descriptions={
           'cycle': "The even election year from the publisher's bulk-downloads/<year>/ directory; NULL unless exactly "
                    'one directory year appears in the captured URLs.',
           'cycle_status': 'bulk_directory when one year directory appears, not_stated when none, ambiguous when the '
                           'captures span two.',
       }),
    SQLView('fec_relationship_evidence', {
        'fec_relationships': ('source_locator_json','source_sha256','source_fields_json','relationship_type'),
        'fec_source_records': ('collection_id','source_record_id','source_sha256'),
    }, evidence, 'Every source relationship observation locates its companion under collection/record identity. '
       'companion_candidates_json retains every candidate digest, including repeated and null values. '
       'Read full companion evidence directly from fec_source_records using collection_id and source_record_id; '
       'retain all matches, since these coordinates can be ambiguous. Raw bodies, metadata and URLs are not '
       'embedded in candidate arrays, allowing serving queries to avoid reading those large columns. '
       'Explicit empty relationship states remain '
       'observations, not edges. Matching recorded digests is not verification of retained bytes.',
       ('source_locator_json','source_fields_json','relationship_type'), rule_version='fec-companion-location-v3',
       column_descriptions={
           'collection_id': 'The companion collection named in source_locator_json; NULL when the locator is unsupported.',
           'source_record_id': 'The companion record named in source_locator_json; NULL when the locator is unsupported.',
           'locator_status': 'valid when source_locator_json names a non-empty collection_id and source_record_id, '
                             'else unsupported.',
           'companion_candidates_json': 'JSON list of every fec_source_records digest under the locator coordinates; '
                                        'sorted by digest with nulls last; repeated and null digests retained.',
           'recorded_digest_status': 'matches or mismatch of the recorded source_sha256 against the single companion; '
                                     'unavailable when either digest is NULL; not_checked unless exactly one companion.',
           'source_bytes_status': 'Always not_checked: recorded digests are compared, retained bytes never are.',
       }),
)
