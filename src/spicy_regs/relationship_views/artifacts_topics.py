"""Offered artifacts and source-namespaced vocabulary, with no inferred mappings."""

from .congress import SCALAR, scalar_valid, value
from .core import ArrayRelationship


def artifact(name, table, keys, field, url_field):
    url = value(url_field)
    return ArrayRelationship(
        name, table, keys, field, 'offered_url', url,
        f"e.type = 'OBJECT' AND regexp_full_match({url}, 'https?://.+')",
        'Offered source artifact references with original roles, formats and sizes retained in raw JSON. '
        'No URL fetch or body acquisition is implied. Acquisition status is not checked by this view.',
        details=(('acquisition_status', "'not_checked'"), ('retained_digest', 'NULL::VARCHAR')),
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
