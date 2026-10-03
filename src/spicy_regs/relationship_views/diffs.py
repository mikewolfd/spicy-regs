"""Resolve both diff endpoints using provider-qualified version and element keys."""

from .sql_views import SQLView, pin

DIFF_KEYS = ('bill_id','from_version_code','from_source','to_version_code','to_source')


def endpoints(p):
    parts = []
    for side in ('from','to'):
        parts.append(f"""SELECT d.*, '{side}' AS endpoint_side,
            d.{side}_version_code AS endpoint_version_code, d.{side}_source AS endpoint_source,
            d.{side}_element_id AS endpoint_element_id, d.{side}_text_sha256 AS endpoint_text_sha256,
            coalesce(t.n,0) AS target_count,
            CASE WHEN d.{side}_element_id IS NULL OR d.{side}_element_id=''
                      OR d.{side}_source IS NULL OR d.{side}_version_code IS NULL THEN 'unsupported'
                 WHEN t.n IS NULL THEN 'missing' WHEN t.n=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
            CASE WHEN t.n IS NULL OR d.{side}_text_sha256 IS NULL OR t.body_sha256 IS NULL THEN 'unavailable'
                 WHEN t.n<>1 THEN 'ambiguous' WHEN d.{side}_text_sha256=t.body_sha256 THEN 'matches'
                 ELSE 'mismatch' END AS text_digest_status,
            {pin(p, 'section_diff_items')} AS source_publication_json,
            {pin(p, 'bill_sections')} AS target_publication_json, 'diff-full-endpoint-v1' AS rule_version
            FROM section_diff_items d LEFT JOIN (
                SELECT bill_id,version_code,source,element_id,count(*) AS n,min(body_sha256) AS body_sha256
                FROM bill_sections GROUP BY bill_id,version_code,source,element_id
            ) t ON d.bill_id=t.bill_id AND d.{side}_version_code=t.version_code
                AND d.{side}_source=t.source AND d.{side}_element_id=t.element_id""")
    return ' UNION ALL '.join(parts)


def version_endpoints(p):
    parts = []
    for side in ('from','to'):
        parts.append(f"""SELECT d.*, '{side}' AS endpoint_side,
            d.{side}_version_code AS endpoint_version_code, d.{side}_source AS endpoint_source,
            coalesce(t.n,0) AS target_count,
            CASE WHEN d.{side}_source IS NULL OR d.{side}_version_code IS NULL THEN 'unsupported'
                 WHEN t.n IS NULL THEN 'missing' WHEN t.n=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
            to_json(t.candidates)::VARCHAR AS candidate_versions_json,
            {pin(p, 'section_diffs')} AS source_publication_json,
            {pin(p, 'bill_versions')} AS target_publication_json, 'diff-full-version-v1' AS rule_version
            FROM section_diffs d LEFT JOIN (
                SELECT bill_id,version_code,source,count(*) AS n,
                    list(struct_pack(sha256:=sha256,resolved_url:=resolved_url,observed_at:=observed_at)) AS candidates
                FROM bill_versions GROUP BY bill_id,version_code,source
            ) t ON d.bill_id=t.bill_id AND d.{side}_version_code=t.version_code AND d.{side}_source=t.source""")
    return ' UNION ALL '.join(parts)


_ENDPOINT_COLUMNS = {
    'endpoint_side': 'Which end of the diff this row resolves: from (the earlier printing) or to (the later); both '
                     'ends of one diff are separate rows.',
    'endpoint_version_code': "This end's version code, copied from from_version_code or to_version_code.",
    'endpoint_source': "This end's provider source (govinfo or congress), copied from from_source or to_source.",
}

DIFF_VIEWS = (
    SQLView('section_diff_version_endpoints', {
        'section_diffs': (*DIFF_KEYS,'engine_name','engine_version','engine_revision','computed_at'),
        'bill_versions': ('bill_id','version_code','source','sha256','resolved_url','observed_at'),
    }, version_endpoints, 'Both complete provider-specific version endpoints, with the original diff engine '
       'name/version/revision and every matching version digest and source URL. Duplicates remain ambiguous; '
       'recorded digest presence does not verify retained bytes.', (*DIFF_KEYS,'endpoint_side'),
       column_descriptions={**_ENDPOINT_COLUMNS,
           'candidate_versions_json': 'JSON list of every bill_versions row matching this end (sha256, resolved_url, '
                                      'observed_at); more than one means ambiguous.'}),

    SQLView('section_diff_endpoints', {
        'section_diff_items': (*DIFF_KEYS,'seq','from_element_id','to_element_id','from_text_sha256','to_text_sha256'),
        'bill_sections': ('bill_id','version_code','source','element_id','body_sha256'),
    }, endpoints, 'Each diff item has independent from/to endpoints on bill, version code, provider source and '
       'element ID. Added/removed missing endpoints remain unsupported, not invented sections. Target existence '
       'and exact body-digest agreement are separate; repeated parent keys are ambiguous.', (*DIFF_KEYS,'seq','endpoint_side'),
       column_descriptions={**_ENDPOINT_COLUMNS,
           'endpoint_element_id': "This end's section element id; NULL on an added or removed side.",
           'endpoint_text_sha256': 'The text digest the diff recorded for this end.',
           'text_digest_status': 'matches or mismatch when exactly one target section exists and both digests are '
                                 'present; ambiguous for repeated targets; unavailable otherwise.'}),
)
