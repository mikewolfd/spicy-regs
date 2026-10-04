"""Offered artifacts and source-namespaced vocabulary, with no inferred mappings."""

from .congress import SCALAR, scalar_valid, value
from .core import ArrayRelationship
from .sql_views import SQLView, pin


def artifact(name, table, keys, field, url_field):
    url = value(url_field)
    return ArrayRelationship(
        name, table, keys, field, 'offered_url', url,
        f"e.type = 'OBJECT' AND regexp_full_match({url}, 'https?://.+')",
        'Offered source artifact references with original roles, formats and sizes retained in raw JSON. '
        'No URL fetch or body acquisition is implied. Acquisition status is not checked by this view.',
        details=(('acquisition_status', "'not_checked'"), ('retained_digest', 'NULL::VARCHAR'),
                 ('artifact_role', "'document_content'" if table == 'documents' else "'filing_document'")),
    )


def vocabulary(name, table, keys, field, namespace, object_id=None, label=None):
    key = value(object_id) if object_id else SCALAR
    valid = (f"e.type = 'OBJECT' AND json_type(e.value, '$.{object_id}') IN ('VARCHAR','UBIGINT','BIGINT') "
             f"AND length({key}) > 0") if object_id else scalar_valid()
    return ArrayRelationship(
        name, table, keys, field, namespace, key, valid,
        'Native vocabulary occurrences in the stated provider namespace. Labels do not establish equality '
        'across providers; no RefSpec mapping is fabricated. Publication metadata pins the source selection.',
        details=(('source_namespace', f"'{namespace}'"), ('native_label', value(label) if label else SCALAR)),
    )


ARTIFACT_TOPIC_RELATIONSHIPS = (
    ArrayRelationship(
        'document_attachment_records', 'documents', ('document_id',), 'attachment_records_json',
        'attachment', value('id'), "e.type='OBJECT' AND json_extract_string(e.value,'$.type')='attachments' AND length(json_extract_string(e.value,'$.id'))>0",
        'Explicitly read attachment records; NULL field means unread, empty array means read empty. Restricted records remain present without URLs.',
        details=(('role', "'attachment'"), ('restriction', "json_extract_string(e.value,'$.attributes.restrictReasonType')"),
                 ('formats_json', "CAST(json_extract(e.value,'$.attributes.fileFormats') AS VARCHAR)")),
    ),
    artifact('document_artifacts', 'documents', ('document_id',), 'attachments_json', 'url'),
    artifact('fcc_filing_artifacts', 'fcc_filings', ('id_submission',), 'documents_json', 'src'),
    vocabulary('federal_register_agencies', 'federal_register', ('document_number','publication_date'),
               'agencies_json', 'federal_register_agency', 'id', 'name'),
    vocabulary('federal_register_topics', 'federal_register', ('document_number','publication_date'),
               'topics_json', 'federal_register_topic'),
    vocabulary('bill_subject_terms', 'bill_subjects', ('bill_id',), 'subjects_json', 'congress_subject'),
    vocabulary('lobbying_contacted_entities', 'lobbying_activities', ('filing_uuid','activity_index'),
               'government_entities_json', 'lda_government_entity', 'id', 'name'),
)


def attachment_renditions(publication):
    return f"""SELECT s.document_id, CAST(r.key AS BIGINT) attachment_ordinal,
        json_extract_string(r.value,'$.id') attachment_id, CAST(f.key AS BIGINT) format_ordinal,
        'attachment' AS artifact_role, json_extract_string(f.value,'$.fileUrl') offered_url,
        json_extract_string(f.value,'$.format') format,
        json_extract_string(f.value,'$.size') source_size,
        CAST(r.value AS VARCHAR) raw_attachment_json, CAST(f.value AS VARCHAR) raw_format_json,
        '/attachment_records_json/'||r.key||'/attributes/fileFormats/'||f.key source_pointer,
        CASE WHEN f.type='OBJECT' AND regexp_full_match(json_extract_string(f.value,'$.fileUrl'),'https?://.+')
             THEN 'valid' ELSE 'unsupported_element' END parsing_status,
        'not_checked' acquisition_status, NULL::VARCHAR retained_digest,
        {pin(publication, 'documents')} source_publication_json
        FROM documents s,
        json_each(CASE WHEN json_type(try_cast(s.attachment_records_json AS JSON))='ARRAY'
                       THEN s.attachment_records_json ELSE '[]' END) r,
        json_each(CASE WHEN json_type(r.value,'$.attributes.fileFormats')='ARRAY'
                       THEN json_extract(r.value,'$.attributes.fileFormats') ELSE '[]'::JSON END) f"""


ARTIFACT_SQL_VIEWS = (
    SQLView('document_attachment_renditions', {'documents': ('document_id','attachment_records_json')},
            attachment_renditions, 'Every offered attachment format in source order, independent of document content URLs. No acquisition is implied.',
            ('document_id','attachment_ordinal','format_ordinal')),
)


NATIVE_BILL_SUBJECTS = vocabulary(
    'bill_subject_terms', 'bill_subjects', ('bill_id',), 'subjects', 'congress_subject'
)
