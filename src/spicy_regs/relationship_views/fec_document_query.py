"""Readable FEC document views; original observations remain the evidence layer.

These projections preserve source identities and uncertainty. They do not qualify
filing associations, legal applicability, current records, or financial totals.
"""

from .core import literal
from .sql_views import SQLView, pin

VERSION = "fec-query-shape-v1"
EVIDENCE = (
    "record_id",
    "collection_id",
    "source_record_id",
    "source_sha256",
    "source_locator_json",
    "source_authority",
    "selection_evidence_sha256",
)
FILING = ("filing_key", "filing_link_status", "filing_header_record_id")


def _select(columns, alias="s"):
    return ", ".join(f'{alias}."{column}"' for column in columns)


def _json_status(value):
    return (
        f"CASE WHEN {value} IS NULL THEN 'source_null' "
        f"WHEN json_type({value}) = 'NULL' THEN 'json_null' "
        f"WHEN json_type({value}) = 'ARRAY' THEN "
        f"CASE WHEN json_array_length({value}) = 0 THEN 'empty' ELSE 'reported' END "
        "ELSE 'unsupported_shape' END"
    )


def _array_spec(name, table, array, parent_columns, fields, meaning):
    # to_json supports the native LIST<STRUCT> column without assuming an element
    # schema. A null/unsupported collection is a diagnostic row; empty is no rows.
    def query(publication):
        fields_sql = ", ".join(f"json_extract_string(e.value, '$.{source}') AS {target}" for target, source in fields)
        return f"""WITH parents AS (
            SELECT *, to_json({array}) AS array_json FROM {table}
        ), expanded AS (
            SELECT s.*, e.key AS element_key, e.value AS element_value
            FROM parents s LEFT JOIN LATERAL json_each(
                CASE WHEN json_type(array_json) = 'ARRAY' THEN array_json ELSE '[]'::JSON END
            ) e ON TRUE
            WHERE coalesce(json_type(array_json), '') <> 'ARRAY'
               OR json_array_length(array_json) > 0
        )
        SELECT {_select(parent_columns)}, s.record_id AS parent_record_id,
            try_cast(s.element_key AS BIGINT) AS source_ordinal,
            '{array}' AS source_field,
            CASE WHEN s.element_key IS NOT NULL THEN '/{array}/' || s.element_key
                 ELSE '/{array}' END AS source_pointer,
            {_json_status("s.array_json")} AS collection_status,
            CASE WHEN s.element_key IS NULL THEN 'collection_unavailable'
                 WHEN json_type(e.value) = 'OBJECT' THEN 'reported'
                 WHEN json_type(e.value) = 'NULL' THEN 'json_null'
                 ELSE 'unsupported_shape' END AS parsing_status,
            {fields_sql},
            CASE WHEN json_type(e.value) <> 'OBJECT' THEN cast(e.value AS VARCHAR)
                 END AS unsupported_value_json,
            {literal(VERSION)} AS rule_version,
            {pin(publication, table)} AS source_publication_json
        FROM expanded s LEFT JOIN LATERAL (SELECT s.element_value AS value) e ON TRUE"""

    return SQLView(name, {table: (*parent_columns, array)}, query, meaning, ("record_id", "source_ordinal"), VERSION)


def _citations(publication):
    table = "fec_legal_matters"
    return f"""WITH parents AS (
        SELECT *, try_cast(native_facts_json AS JSON) AS facts FROM {table}
    ), roots AS (
        SELECT s.*, j.key AS citation_kind, '/' || j.key AS collection_pointer,
               j.value AS collection_value
        FROM parents s, LATERAL json_each(facts) j
        WHERE j.key IN ('ao_citations', 'aos_cited_by', 'regulatory_citations', 'statutory_citations')
        UNION ALL
        SELECT s.*, 'citations/' || j.key, '/citations/' || replace(replace(j.key, '~', '~0'), '/', '~1'), j.value
        FROM parents s, LATERAL json_each(
            CASE WHEN json_type(facts, '$.citations') = 'OBJECT'
                 THEN json_extract(facts, '$.citations') ELSE '{{}}'::JSON END) j
        UNION ALL
        SELECT s.*, 'citations', '/citations', json_extract(facts, '$.citations')
        FROM parents s WHERE json_exists(facts, '$.citations')
          AND json_type(facts, '$.citations') <> 'OBJECT'
    )
    SELECT {_select((*EVIDENCE, "matter_id"))}, s.record_id AS matter_record_id,
        s.citation_kind, 'native_facts_json' AS source_field, try_cast(e.key AS BIGINT) AS source_ordinal,
        s.collection_pointer || CASE WHEN e.key IS NULL THEN '' ELSE '/' || e.key END AS source_pointer,
        {_json_status("s.collection_value")} AS collection_status,
        CASE WHEN e.key IS NULL THEN 'collection_unavailable'
             WHEN json_type(e.value) IN ('OBJECT', 'VARCHAR') THEN 'reported'
             WHEN json_type(e.value) = 'NULL' THEN 'json_null'
             ELSE 'unsupported_shape' END AS parsing_status,
        CASE WHEN json_type(e.value) = 'VARCHAR' THEN json_extract_string(e.value, '$')
             ELSE json_extract_string(e.value, '$.text') END AS citation_text,
        json_extract_string(e.value, '$.url') AS url,
        json_extract_string(e.value, '$.title') AS title,
        json_extract_string(e.value, '$.section') AS section,
        json_extract_string(e.value, '$.name') AS name,
        json_extract_string(e.value, '$.no') AS advisory_opinion_number,
        cast(CASE WHEN e.key IS NULL THEN s.collection_value ELSE e.value END AS VARCHAR) AS raw_value_json,
        {literal(VERSION)} AS rule_version, {pin(publication, table)} AS source_publication_json
    FROM roots s LEFT JOIN LATERAL json_each(
        CASE WHEN json_type(s.collection_value) = 'ARRAY' THEN s.collection_value ELSE '[]'::JSON END
    ) e ON TRUE
    WHERE coalesce(json_type(s.collection_value), '') <> 'ARRAY'
       OR json_array_length(s.collection_value) > 0"""


def _subjects(publication):
    table = "fec_legal_matters"
    return f"""WITH RECURSIVE parents AS (
        SELECT *, try_cast(native_facts_json AS JSON) AS facts FROM {table}
    ), roots AS (
        SELECT s.record_id, j.key AS subject_kind, j.value AS collection_value
        FROM parents s, LATERAL json_each(facts) j WHERE j.key IN ('subject', 'subjects')
    ), nodes(record_id, subject_kind, source_pointer, parent_pointer, source_ordinal, depth,
             node, collection_status) AS (
        SELECT s.record_id, s.subject_kind,
            '/' || s.subject_kind || CASE WHEN e.key IS NULL THEN '' ELSE '/' || e.key END,
            NULL::VARCHAR, try_cast(e.key AS BIGINT), 0,
            CASE WHEN e.key IS NULL THEN s.collection_value ELSE e.value END,
            {_json_status("s.collection_value")}
        FROM roots s LEFT JOIN LATERAL json_each(
            CASE WHEN json_type(s.collection_value) = 'ARRAY' THEN s.collection_value ELSE '[]'::JSON END
        ) e ON TRUE
        WHERE coalesce(json_type(s.collection_value), '') <> 'ARRAY'
           OR json_array_length(s.collection_value) > 0
        UNION ALL
        SELECT n.record_id, n.subject_kind,
            n.source_pointer || '/children' || CASE WHEN e.key IS NULL THEN '' ELSE '/' || e.key END,
            n.source_pointer, try_cast(e.key AS BIGINT), n.depth + 1,
            CASE WHEN e.key IS NULL THEN json_extract(n.node, '$.children') ELSE e.value END,
            {_json_status("json_extract(n.node, '$.children')")}
        FROM nodes n LEFT JOIN LATERAL json_each(
            CASE WHEN json_type(n.node, '$.children') = 'ARRAY'
                 THEN json_extract(n.node, '$.children') ELSE '[]'::JSON END
        ) e ON TRUE
        WHERE json_type(n.node) = 'OBJECT' AND json_exists(n.node, '$.children')
          AND (json_type(n.node, '$.children') <> 'ARRAY'
               OR json_array_length(n.node, '$.children') > 0)
    )
    SELECT {_select((*EVIDENCE, "matter_id"))}, s.record_id AS matter_record_id,
        n.subject_kind, 'native_facts_json' AS source_field, n.source_pointer, n.parent_pointer, n.source_ordinal, n.depth,
        n.collection_status,
        CASE WHEN n.source_ordinal IS NULL THEN 'collection_unavailable'
             WHEN json_type(n.node) IN ('OBJECT', 'VARCHAR') THEN 'reported'
             WHEN json_type(n.node) = 'NULL' THEN 'json_null'
             ELSE 'unsupported_shape' END AS parsing_status,
        CASE WHEN json_type(n.node) = 'VARCHAR' THEN json_extract_string(n.node, '$')
             ELSE coalesce(json_extract_string(n.node, '$.text'), json_extract_string(n.node, '$.subject')) END AS subject,
        json_extract_string(n.node, '$.primary_subject_id') AS primary_subject_id,
        json_extract_string(n.node, '$.secondary_subject_id') AS secondary_subject_id,
        cast(CASE WHEN json_type(n.node) = 'OBJECT' THEN json_merge_patch(n.node, '{{"children":null}}') ELSE n.node END AS VARCHAR) AS subject_attributes_json,
        {literal(VERSION)} AS rule_version, {pin(publication, table)} AS source_publication_json
    FROM nodes n JOIN parents s ON n.record_id = s.record_id"""


FEC_DOCUMENT_QUERY_VIEWS = (
    _array_spec(
        "fec_filing_report_measures",
        "fec_filing_report_observations",
        "reported_measures",
        (*EVIDENCE, *FILING, "definition_set_id", "currency", "aggregation_status"),
        (
            ("native_position", "native_position"),
            ("label", "native_label"),
            ("definition_cell", "definition_cell"),
            ("raw_value", "raw_value"),
            ("exact_value", "value"),
            ("value_status", "value_status"),
            ("quantity_kind", "quantity_kind"),
            ("measure_role", "measure_role"),
            ("period_basis", "period_basis"),
        ),
        "Ordered report measures with exact decimal strings and source meanings; totals and subtotals remain distinct. Null collections emit diagnostics.",
    ),
    _array_spec(
        "fec_filing_text",
        "fec_filing_text_observations",
        "text_fragments",
        (
            *EVIDENCE,
            *FILING,
            "text_record_kind",
            "reporting_committee_id",
            "transaction_id",
            "back_reference_transaction_id",
            "back_reference_form_name",
            "current_record_status",
        ),
        (
            ("field_position", "field_position"),
            ("text", "text"),
            ("text_status", "text_status"),
            ("fragment_source_column", "source_column"),
            ("fragment_source_pointer", "source_pointer"),
        ),
        "Ordered filing text fragments, with original text status and unresolved filing associations preserved.",
    ),
    SQLView(
        "fec_legal_citations",
        {"fec_legal_matters": (*EVIDENCE, "matter_id", "native_facts_json")},
        _citations,
        "Source citation occurrences, preserving citation kind, direction, literal values and URLs without legal interpretation.",
        ("record_id", "source_pointer"),
        VERSION,
    ),
    SQLView(
        "fec_legal_subjects",
        {"fec_legal_matters": (*EVIDENCE, "matter_id", "native_facts_json")},
        _subjects,
        "Source subject hierarchy with parent pointers and sibling ordinals; repeated subjects remain distinct.",
        ("record_id", "source_pointer"),
        VERSION,
    ),
)
