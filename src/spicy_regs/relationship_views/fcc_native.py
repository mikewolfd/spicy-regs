"""Native FCC roles and offered artifacts, independent of legacy name-only arrays."""

from .sql_views import SQLView, pin

_FIELDS = "(VALUES ('proceedings'),('filers'),('authors'),('lawfirms'),('bureaus'),('documents'))"
_REQUIRED = {'fcc_filings': ('id_submission', 'native_fields_json', 'native_fields_sha256')}
_RULE = 'fcc-native-fields/1'
_NATIVE_COLUMNS = {
    'raw_field_json': "The native field's complete JSON as held; NULL when unread or absent.",
    'observation_kind': 'fcc_proceeding for the proceedings field, offered_artifact for documents, participant_role '
                        'for filers, authors, lawfirms and bureaus.',
    'participant_role': 'The native role field this participant came from (filers, authors, lawfirms, bureaus); '
                        'NULL for other fields.',
    'observed_name': "The element's name as the publisher states it; not resolved to a person or organization.",
    'native_proceeding_id': "Publisher numeric proceeding ID, which can be reused across observed names. "
                            "Match BOTH observed_name and native_proceeding_id to fcc_proceedings.name AND "
                            "id_proceeding; the numeric ID alone is not a unique proceeding key.",
    'identifier_namespace': 'fcc_ecfs_proceeding_id for a proceeding element; NULL otherwise.',
    'offered_url': 'The document src URL the publisher offers; not fetched.',
    'filename': "The publisher's filename for a document element.",
    'description': "The publisher's description of a document element.",
    'acquisition_status': 'not_checked for a document element: no URL was fetched; NULL otherwise.',
    'retained_digest': 'Always NULL: no retained bytes are recorded.',
}


def field_states(p):
    return f"""WITH fields AS (
        SELECT s.*,f.source_field,TRY_CAST(s.native_fields_json AS JSON) AS native
        FROM fcc_filings s CROSS JOIN {_FIELDS} f(source_field)
    ) SELECT id_submission,source_field,native_fields_sha256,
        json_extract(native,'$.'||source_field)::VARCHAR AS raw_field_json,
        CASE WHEN native_fields_json IS NULL THEN 'unread_legacy'
             WHEN native IS NULL OR json_type(native)<>'OBJECT' THEN 'unsupported_container'
             WHEN NOT json_exists(native,'$.'||source_field) THEN 'absent'
             WHEN json_type(native,'$.'||source_field)='NULL' THEN 'null'
             WHEN json_type(native,'$.'||source_field)<>'ARRAY' THEN 'unsupported_shape'
             WHEN json_array_length(native,'$.'||source_field)=0 THEN 'empty_array'
             ELSE 'populated_array' END AS field_state,
        {pin(p,'fcc_filings')} AS source_publication_json,'{_RULE}' AS rule_version
        FROM fields"""


def observations(p):
    return f"""WITH fields AS ({field_states(p)})
    SELECT f.*,CAST(e.key AS BIGINT) AS source_ordinal,
        '/'||source_field||'/'||e.key AS source_pointer,e.value::VARCHAR AS raw_value_json,
        CASE WHEN e.type='NULL' THEN 'null_element' WHEN e.type='OBJECT' THEN 'native_object'
             ELSE 'unsupported_element' END AS parsing_status,
        CASE WHEN source_field='proceedings' THEN 'fcc_proceeding'
             WHEN source_field='documents' THEN 'offered_artifact' ELSE 'participant_role' END AS observation_kind,
        CASE WHEN source_field IN ('filers','authors','lawfirms','bureaus') THEN source_field END AS participant_role,
        CASE WHEN json_type(e.value,'$.name')='VARCHAR' THEN json_extract_string(e.value,'$.name') END AS observed_name,
        CASE WHEN source_field='proceedings' AND json_type(e.value,'$.id_proceeding') IN ('VARCHAR','UBIGINT','BIGINT')
             THEN json_extract_string(e.value,'$.id_proceeding') END AS native_proceeding_id,
        CASE WHEN source_field='proceedings' THEN 'fcc_ecfs_proceeding_id' END AS identifier_namespace,
        CASE WHEN source_field='documents' AND json_type(e.value,'$.src')='VARCHAR'
             THEN json_extract_string(e.value,'$.src') END AS offered_url,
        CASE WHEN source_field='documents' THEN json_extract_string(e.value,'$.filename') END AS filename,
        CASE WHEN source_field='documents' THEN json_extract_string(e.value,'$.description') END AS description,
        CASE WHEN source_field='documents' THEN 'not_checked' END AS acquisition_status,
        NULL::VARCHAR AS retained_digest,'not_checked' AS target_status
    FROM fields f,json_each(CASE WHEN field_state IN ('populated_array','empty_array')
        THEN TRY_CAST(raw_field_json AS JSON) ELSE '[]'::JSON END) e"""


def proceeding_links(p):
    return f"""WITH observations AS ({observations(p)}), targets AS (
        SELECT name,id_proceeding,count(*) AS n FROM fcc_proceedings GROUP BY name,id_proceeding
    ) SELECT o.* EXCLUDE(target_status),coalesce(t.n,0) AS target_count,
        CASE WHEN o.observed_name IS NULL OR trim(o.observed_name)='' OR o.native_proceeding_id IS NULL
                  OR trim(o.native_proceeding_id)='' THEN 'unsupported'
             WHEN t.n IS NULL THEN 'missing' WHEN t.n=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
        {pin(p,'fcc_proceedings')} AS target_publication_json
    FROM observations o LEFT JOIN targets t ON o.observed_name=t.name AND o.native_proceeding_id=t.id_proceeding
    WHERE o.source_field='proceedings'"""


FCC_NATIVE_VIEWS = (
    SQLView('fcc_native_field_states', _REQUIRED, field_states,
        'Literal native field states; legacy SQL null is unread. native_fields_sha256 hashes canonical selected-field '
        'JSON, not response bytes. Source capture qualification requires the separately retained publication evidence.',
        ('id_submission','source_field'), rule_version=_RULE, column_descriptions=_NATIVE_COLUMNS),
    SQLView('fcc_native_observations', _REQUIRED, observations,
        'Every native array element retains role, ordinal and raw JSON. Names do not resolve people; proceeding IDs '
        'are FCC-only. Offered artifact URLs do not establish acquisition, format, size, redirect or retained bytes. '
        'Those unsupported fields remain in raw JSON when supplied; retained_digest stays null.',
        ('id_submission','source_field','source_ordinal'), rule_version=_RULE, column_descriptions=_NATIVE_COLUMNS),
    SQLView('fcc_native_proceeding_links', {**_REQUIRED,'fcc_proceedings': ('name','id_proceeding')}, proceeding_links,
        'Native FCC name and proceeding ID must both match the selected proceeding population. All source occurrences '
        'remain, duplicate target identities are ambiguous, and no other docket namespace is searched.',
        ('id_submission','source_field','source_ordinal'), rule_version=_RULE, column_descriptions=_NATIVE_COLUMNS),
)
