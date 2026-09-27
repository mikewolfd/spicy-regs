"""Locate companion records without treating recorded digests as verified bytes."""

from .sql_views import SQLView, pin


def evidence(p):
    return f"""WITH companions AS (
        SELECT s.collection_id,s.source_record_id,count(*) AS n,
            list(struct_pack(source_sha256:=source_sha256)) AS candidates
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
        'fec-companion-location-v2' AS rule_version
    FROM observations r LEFT JOIN companions c
        ON CASE WHEN r.locator_status='valid' THEN r.collection_id END=c.collection_id
        AND r.source_record_id=c.source_record_id"""


FEC_VIEWS = (
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
       ('source_locator_json','source_fields_json','relationship_type'), rule_version='fec-companion-location-v2'),
)
