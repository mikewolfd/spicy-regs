"""Evidence endpoints derived from the exact source address on typed FEC rows.

The builder validates the endpoint shapes before writing rows. A derived view
avoids storing each typed identity again for every header/dictionary witness.
The serving release must bind the explicitly supplied source-generation pin;
this function does not resolve the latest source family or qualify totals.
"""

import re

from spicy_regs.fec_versions import INDIVIDUAL_SNAPSHOT_POLICY

from .core import literal, quoted

FILING_REFERENCE_POLICY = "fec-retained-filing-reference-resolution/1"


def filing_reference_resolution() -> str:
    """Resolve reported filing references without multiplying their observations.

    One native filing key can have several retained API observations. Match the
    key, expose the number of observations, and leave each observation available
    in fec_filings. No first/latest record is selected. Repeated physical record
    IDs or conflicting filing identity attributes refuse resolution. A resolved
    reference proves neither amendment direction nor replacement membership.
    """
    return f"""WITH source AS (
        SELECT record_id, count(*) AS source_count, min(filing_key) AS filing_key
        FROM fec_filings GROUP BY record_id
    ), target AS (
        SELECT filing_key, count(*) AS target_observation_count,
            count(DISTINCT record_id) AS target_record_count,
            count(DISTINCT source_authority) AS authority_count,
            count(DISTINCT source_namespace) AS namespace_count,
            count(DISTINCT report_number) AS report_number_count,
            count(DISTINCT native_filer_id) AS filer_count,
            count(DISTINCT form_type) AS form_count
        FROM fec_filings WHERE filing_key IS NOT NULL GROUP BY filing_key
    ) SELECT l.* EXCLUDE (target_resolution_status),
        {literal(FILING_REFERENCE_POLICY)} AS resolution_policy_version,
        COALESCE(t.target_observation_count, 0) AS target_observation_count,
        CASE WHEN s.source_count IS NULL THEN 'unresolved_source_observation_absent'
             WHEN s.source_count <> 1 THEN 'unresolved_source_observation_ambiguous'
             WHEN s.filing_key IS DISTINCT FROM l.filing_key THEN 'unresolved_source_identity_mismatch'
             WHEN l.target_filing_key IS NULL THEN 'unresolved_no_file_number'
             WHEN t.filing_key IS NULL THEN 'unresolved_target_not_retained'
             WHEN t.target_record_count <> t.target_observation_count THEN 'unresolved_target_observation_ambiguous'
             WHEN t.authority_count <> 1 OR t.namespace_count <> 1 OR t.report_number_count <> 1
               OR t.filer_count > 1 OR t.form_count > 1 THEN 'unresolved_target_identity_conflict'
             ELSE 'resolved_native_filing_key' END AS target_resolution_status
        FROM fec_filing_links l LEFT JOIN source s ON l.source_filing_record_id = s.record_id
        LEFT JOIN target t ON l.target_filing_key = t.filing_key"""


def typed_record_evidence(table: str, source_generation_pin: str) -> str:
    """SQL for one typed table's record and collection-context witnesses.

    No DISTINCT or source join collapses multiplicity. Endpoint existence and
    cardinality remain release-validation requirements, independent of this view.
    """
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", source_generation_pin):
        raise ValueError("Typed FEC evidence requires the selected source-generation pin")
    common = f"""{literal(table)} AS target_table, record_id AS target_record_id,
        'self' AS target_generation_scope, 'external' AS witness_generation_scope,
        {literal(source_generation_pin)} AS witness_generation_pin"""
    source = quoted(table)
    definition = "json_extract(source_locator_json, '$.field_mapping')"
    kind = f"json_extract_string({definition}, '$.kind')"
    return f"""SELECT {common}, 'primary' AS role, 'source_record' AS endpoint_kind,
            collection_id, source_record_id, NULL::VARCHAR AS context_column, NULL::VARCHAR AS context_pointer,
            source_sha256 AS witness_sha256, source_locator_json AS witness_locator_json
        FROM {source}
        UNION ALL
        SELECT {common}, 'field_definition' AS role, 'source_record' AS endpoint_kind,
            json_extract_string({definition}, '$.collection_id') AS collection_id,
            json_extract_string({definition}, '$.source_record_id') AS source_record_id,
            NULL::VARCHAR AS context_column, NULL::VARCHAR AS context_pointer,
            json_extract_string({definition}, '$.source_sha256') AS witness_sha256,
            json_extract({definition}, '$.source_locator')::VARCHAR AS witness_locator_json
        FROM {source}
        WHERE json_type({definition}) = 'OBJECT' AND {kind} IS DISTINCT FROM 'official-html-dictionary'
        UNION ALL
        SELECT {common}, 'field_definition' AS role, 'collection_context' AS endpoint_kind,
            json_extract_string({definition}, '$.definitions_collection_id') AS collection_id,
            NULL::VARCHAR AS source_record_id, 'collection_outcome_json' AS context_column,
            json_extract_string({definition}, '$.definitions_pointer') AS context_pointer,
            json_extract_string({definition}, '$.source_sha256') AS witness_sha256,
            json_extract({definition}, '$.table_locator')::VARCHAR AS witness_locator_json
        FROM {source} WHERE {kind} = 'official-html-dictionary'"""


def spending_targets(table: str) -> str:
    """Source-reported candidate associations; never allocate a full event amount.

    A row with no candidate identity/name/office attributes supplies no target
    association. Electioneering's supplied candidate share remains distinct from
    its repeated full disbursement amount and from an inferred equal division.
    """
    choices = {
        "fec_independent_expenditures": ("support_oppose_code", "NULL::DECIMAL(38,9)", "'not_reported'"),
        "fec_communication_costs": ("support_oppose_code", "NULL::DECIMAL(38,9)", "'not_reported'"),
        "fec_electioneering_communications": (
            "NULL::VARCHAR",
            "allocated_candidate_amount",
            "allocated_candidate_amount_status",
        ),
        "fec_coordinated_party_expenditures": ("NULL::VARCHAR", "NULL::DECIMAL(38,9)", "'not_reported'"),
    }
    if table not in choices:
        raise ValueError("No qualified candidate-target layout for this FEC table")
    support, allocation, allocation_status = choices[table]
    return f"""SELECT
        'sha256:' || sha256({literal("fec-spending-target/1:" + table + ":")} || record_id) AS record_id,
        'fec-spending-target/1' AS identity_version,
        {literal(table)} AS spending_table, record_id AS spending_record_id,
        'self' AS spending_generation_scope,
        candidate_id, candidate_name, candidate_office, candidate_state, candidate_district,
        {support} AS support_oppose_code,
        CASE WHEN NULLIF(candidate_id, '') IS NOT NULL THEN 'native_identifier_unresolved'
             WHEN NULLIF(candidate_name, '') IS NOT NULL THEN 'name_only'
             ELSE 'attributes_only' END AS target_status,
        {allocation} AS allocated_amount, {allocation_status} AS allocated_amount_status,
        currency, collection_id, source_record_id, source_sha256, source_locator_json,
        mapping_version, mapping_status, selection_evidence_sha256
        FROM {quoted(table)}
        WHERE COALESCE(NULLIF(candidate_id, ''), NULLIF(candidate_name, ''), NULLIF(candidate_office, ''),
                       NULLIF(candidate_state, ''), NULLIF(candidate_district, '')) IS NOT NULL"""


def filing_record_evidence(table: str, source_generation_pin: str) -> str:
    """Join normalized workbook witnesses and the exact header to filing rows.

    A generation validator must require one definition row and its verified
    witnesses per definition_set_id. This SQL does not select latest definitions.
    """
    primary = typed_record_evidence(table, source_generation_pin)
    return f"""{primary}
        UNION ALL
        SELECT {literal(table)}, record_id, 'self', 'external', {literal(source_generation_pin)},
            'filing_header', 'source_record', collection_id, filing_header_record_id,
            NULL::VARCHAR, NULL::VARCHAR, source_sha256, filing_header_locator_json
        FROM {quoted(table)}
        WHERE filing_header_record_id IS NOT NULL OR filing_header_locator_json IS NOT NULL
        UNION ALL
        SELECT {literal(table)}, t.record_id, 'self', e.witness_generation_scope, e.witness_generation_pin,
            e.role, e.endpoint_kind, e.collection_id, e.source_record_id, e.context_column, e.context_pointer,
            e.witness_sha256, e.witness_locator_json
        FROM {quoted(table)} t JOIN fec_filing_definition_evidence e ON e.target_record_id = t.definition_set_id
        WHERE e.target_table = 'fec_filing_definitions'"""


def reported_summary_measures() -> str:
    """Expose source-named measures without storing repeated summary evidence.

    No aliases equate similarly named measures from different source summaries.
    Values and units retain separate NULL, blank, zero and conversion-refusal
    states. This view performs no aggregation or latest-edition selection.
    """
    return """SELECT
        'sha256:' || sha256('fec-summary-measure/1:' || record_id || ':' || item.measure.native_field) AS record_id,
        'fec-summary-measure/1' AS identity_version, record_id AS summary_record_id,
        'self' AS summary_generation_scope, summary_type, mapping_version,
        item.measure.native_field AS native_measure_name, item.measure.value AS amount,
        item.measure.raw_value AS amount_raw, item.measure.value_status AS amount_status,
        item.measure.unit AS unit, source_cycle, period_start, period_end, period_basis,
        candidate_native_id, candidate_name, committee_native_id, committee_name,
        entity_reference_status, mapping_status, selection_evidence_sha256,
        collection_id, source_record_id, source_sha256, source_locator_json
        FROM fec_reported_financial_summaries, UNNEST(measures) AS item(measure)"""


def individual_snapshot_inclusion(source_generation_pin: str) -> str:
    """Per-observation decisions for the proven main/date representation choice.

    This is a retained snapshot policy, not a current-total qualification. An
    unmatched correction or duplicate policy row remains unresolved. Grouping
    policies first preserves exactly one decision per source observation even
    if a malformed policy table escaped the release uniqueness checks.
    """
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", source_generation_pin):
        raise ValueError("FEC snapshot selection requires its exact source-generation pin")
    return f"""WITH p AS (
        SELECT collection_id, source_sha256, count(*) AS policy_count,
            min(record_id) AS policy_record_id, min(policy_version) AS policy_version,
            min(purpose) AS purpose, min(selection_status) AS selection_status,
            min(selection_reason) AS selection_reason,
            min(selected_collection_id) AS selected_collection_id,
            min(equivalence_evidence_sha256) AS equivalence_evidence_sha256
        FROM fec_collection_selection
        WHERE source_generation_pin = {literal(source_generation_pin)}
          AND purpose = 'retained-reported-individual-contribution-snapshot'
        GROUP BY collection_id, source_sha256
    ), checked AS (
        SELECT *, policy_count = 1
            AND policy_version = {literal(INDIVIDUAL_SNAPSHOT_POLICY)}
            AND selection_status IN ('included', 'duplicate') AS supported_policy
        FROM p
    ) SELECT t.record_id AS target_record_id, 'fec_receipts' AS target_table,
        'self' AS target_generation_scope, t.collection_id,
        CASE WHEN p.supported_policy THEN p.selection_status ELSE 'unresolved' END AS selection_status,
        CASE WHEN p.supported_policy THEN p.selection_reason
             WHEN p.policy_count > 1 THEN 'ambiguous-collection-policy'
             WHEN p.policy_count = 1 AND p.policy_version IS DISTINCT FROM {literal(INDIVIDUAL_SNAPSHOT_POLICY)}
                THEN 'unsupported-policy-version'
             WHEN p.policy_count = 1 THEN 'unsupported-policy-status'
             ELSE 'no-qualified-representation-or-correction-applicability' END AS selection_reason,
        CASE WHEN p.policy_count = 1 THEN p.policy_record_id END AS policy_record_id,
        CASE WHEN p.policy_count = 1 THEN p.policy_version END AS policy_version,
        CASE WHEN p.policy_count = 1 THEN p.equivalence_evidence_sha256 END AS evidence_sha256,
        CASE WHEN p.supported_policy THEN p.selected_collection_id END AS selected_collection_id,
        'unqualified' AS current_record_status,
        'retained-reported-individual-contribution-snapshot' AS purpose
        FROM fec_receipts t LEFT JOIN checked p
          ON t.collection_id = p.collection_id AND t.source_sha256 = p.source_sha256
        WHERE t.source_namespace = 'fec-bulk-individual-contributions'"""


TARGET_COLUMNS = {
    "target_table": "The typed FEC table whose observation this row is about.",
    "target_record_id": "That observation's record_id; with target_table, the row's key.",
    "target_generation_scope": "Always self: the target lives in the same selected generation as this view.",
}
EVIDENCE_COLUMNS = {
    **TARGET_COLUMNS,
    "witness_generation_scope": "Always external: the witness is bound to the source generation in witness_generation_pin.",
    "witness_generation_pin": "The selected source-generation digest this witness is bound to.",
    "role": "What the witness is: primary (the source record), field_definition (the layout or dictionary) or "
            "filing_header (the filing's header record).",
    "endpoint_kind": "source_record for a record witness, collection_context for a collection-level definition witness.",
    "collection_id": "The witness's collection: the record's own for a primary or filing_header witness, the definitions "
                     "collection for a field_definition witness.",
    "source_record_id": "The witness's record id within its collection; NULL for a collection-level witness.",
    "context_column": "The collection column holding the context witness; NULL for a record witness.",
    "context_pointer": "JSON pointer to the witness inside context_column; NULL for a record witness.",
    "witness_sha256": "The witness's recorded digest; recorded, not re-verified here.",
    "witness_locator_json": "JSON locator of the witness (collection, record or table) as recorded.",
}
RESOLUTION_COLUMNS = {
    "resolution_policy_version": "Version of the filing-reference resolution rule applied here.",
    "target_observation_count": "Number of fec_filings observations under the referenced filing key; 0 when none is retained.",
    "target_resolution_status": "resolved_native_filing_key, or the unresolved_* reason: source observation absent or "
                                "ambiguous, source identity mismatch, no file number, target not retained, target "
                                "observations ambiguous or conflicting.",
}
INCLUSION_COLUMNS = {
    **TARGET_COLUMNS,
    "target_table": "Always fec_receipts.",
    "selection_status": "included or duplicate from the single supported policy row; unresolved otherwise.",
    "selection_reason": "The policy row's reason when supported; else ambiguous-collection-policy, "
                        "unsupported-policy-version, unsupported-policy-status or "
                        "no-qualified-representation-or-correction-applicability.",
    "policy_record_id": "The single fec_collection_selection row applied; NULL when none or several.",
    "policy_version": "That policy row's version; NULL when none or several.",
    "evidence_sha256": "That policy row's equivalence evidence digest; NULL when none or several.",
    "selected_collection_id": "The collection the supported policy selects as the representation; NULL otherwise.",
    "current_record_status": "Always unqualified: no current-record selection is made.",
    "purpose": "Always retained-reported-individual-contribution-snapshot: the policy purpose this view applies.",
}
SUMMARY_MEASURE_COLUMNS = {
    "record_id": "'sha256:' digest of the summary record_id and the native measure name: this measure row's own key.",
    "identity_version": "Always fec-summary-measure/1.",
    "summary_record_id": "The fec_reported_financial_summaries record_id the measure belongs to.",
    "summary_generation_scope": "Always self: the summary lives in this selected generation.",
    "native_measure_name": "The publisher's own measure field name; names are not equated across summary types.",
    "amount": "Exact decimal value of the measure; NULL when blank or unconvertible.",
    "amount_raw": "The measure text as filed.",
    "amount_status": "Conversion state of the measure value, separate from NULL.",
    "unit": "The measure unit as recorded.",
}
SPENDING_TARGET_COLUMNS = {
    "record_id": "'sha256:' digest keyed on the spending table and its record_id: this association's own key, not the "
                 "source observation's.",
    "identity_version": "Always fec-spending-target/1.",
    "spending_table": "The typed spending table the association comes from.",
    "spending_record_id": "That spending observation's record_id.",
    "spending_generation_scope": "Always self.",
    "support_oppose_code": "The publisher's support/oppose code as the source row states it; NULL for tables that report none.",
    "target_status": "native_identifier_unresolved when a candidate_id is reported, name_only when only a name, "
                     "attributes_only otherwise; no candidate identity is resolved.",
    "allocated_amount": "The publisher's own candidate share for electioneering communications; NULL elsewhere, never an "
                        "inferred division of the full amount.",
    "allocated_amount_status": "Conversion state of allocated_amount; not_reported where the source states no share.",
}
