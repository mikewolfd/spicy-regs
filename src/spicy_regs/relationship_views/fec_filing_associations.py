"""Count-preserving filing associations over a pinned typed FEC release.

Only the small filing-metadata side is grouped. Financial observations remain
one row each, including repeated source observations and unresolved identities.
The serving release must verify namespace definition pins and source endpoints.
No Arrow or source-reader dependency is needed to construct this SQL.
"""

import re

from .core import literal, quoted

POLICY_VERSION = "fec-retained-filing-association/1"
TARGET_IDENTITY_VERSION = "fec-typed-observation/1"
TARGET_MAPPING_VERSION = "fec-identity-observations/1"
FILE_NUMBER_MAPPINGS = {
    "fec-bulk-individual-contributions": "fec-bulk-individual-receipt/1",
    **{
        "fec-bulk-" + name: "fec-bulk-" + name + "/1"
        for name in (
            "other-committee-transactions",
            "committee-to-candidate-transactions",
            "independent-expenditure-csv",
            "communication-cost-csv",
            "operating-expense-ordered-header",
        )
    },
}
FILE_NUMBER_FILER_FIELDS = {
    namespace: (
        "spender_native_id"
        if namespace == "fec-bulk-independent-expenditure-csv"
        else "reporting_entity_id"
        if namespace == "fec-bulk-communication-cost-csv"
        else "reporting_committee_id"
    )
    for namespace in FILE_NUMBER_MAPPINGS
}
_KEY_PREFIX = "urn:fec:filing:official-fec:openfec-file-number:"


def _pin(value):
    if not isinstance(value, str) or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None:
        raise ValueError("Filing associations require an explicit SHA-256 evidence pin")
    return value


def financial_number_association_sql(table, source_generation_pin, namespace_evidence, *, columns):
    """Derive the bounded mapper's decisions without materializing a large copy.

    ``columns`` comes from the admitted typed table schema. An absent optional
    file-number/filer column stays NULL; a union's NULL committee column never
    masks the namespace's actual spender/entity field. Metadata is grouped by
    native filing key before joining, with every conflicting observation counted.

    This returns the association's semantic/provenance columns. The source key is
    (target_table, target_record_id); no redundant stored association ID is made.
    Release admission validates those source IDs and literal locator JSON.
    """
    _pin(source_generation_pin)
    names = set(columns)
    required = {
        "record_id",
        "collection_id",
        "source_record_id",
        "source_sha256",
        "source_authority",
        "source_locator_json",
        "selection_evidence_sha256",
        "source_namespace",
        "mapping_version",
    }
    if not required <= names:
        raise ValueError("Filing association source lacks required typed identity columns")

    def col(name):
        return "t." + quoted(name) if name in names else "NULL::VARCHAR"

    proofs = {key: _pin(value) for key, value in namespace_evidence.items() if value is not None}
    routes = ",\n".join(
        "("
        + ", ".join(
            (
                literal(namespace),
                literal(version),
                literal(proofs[namespace]) if namespace in proofs else "NULL::VARCHAR",
            )
        )
        + ")"
        for namespace, version in FILE_NUMBER_MAPPINGS.items()
    )
    filer = (
        "CASE "
        + " ".join(
            "WHEN t.source_namespace = " + literal(namespace) + " THEN " + col(field)
            for namespace, field in FILE_NUMBER_FILER_FIELDS.items()
        )
        + " END"
    )
    raw = col("report_number")
    return f"""WITH routes(namespace, mapper, definition_pin) AS (VALUES {routes}),
    targets AS (
        SELECT filing_key, count(*) AS n, count(DISTINCT record_id) AS unique_ids,
            list(record_id ORDER BY record_id) AS witness_ids,
            bool_and(COALESCE(source_authority = 'official-fec'
                AND source_namespace = 'fec-openfec-file-number'
                AND identity_version = {literal(TARGET_IDENTITY_VERSION)}
                AND mapping_version = {literal(TARGET_MAPPING_VERSION)}
                AND regexp_full_match(report_number, '-?[1-9][0-9]*')
                AND filing_key = {literal(_KEY_PREFIX)} || report_number, FALSE)) AS identity_valid,
            count(DISTINCT NULLIF(native_filer_id, '')) AS filer_count,
            count(DISTINCT NULLIF(form_type, '')) AS form_count,
            min(NULLIF(native_filer_id, '')) AS native_filer_id
        FROM fec_filings WHERE filing_key IS NOT NULL GROUP BY filing_key
    ), source AS (
        SELECT t.*, {raw} AS association_report_number, {filer} AS association_filer_id,
            r.definition_pin,
            CASE WHEN t.source_authority IS DISTINCT FROM 'official-fec' THEN 'unresolved_source_authority'
                 WHEN r.mapper IS NULL OR t.mapping_version IS DISTINCT FROM r.mapper THEN 'unresolved_native_namespace'
                 WHEN r.definition_pin IS NULL THEN 'unresolved_namespace_evidence'
                 WHEN {raw} IS NULL OR {raw} IN ('', '0', '-0') THEN 'unresolved_no_file_number'
                 WHEN NOT regexp_full_match({raw}, '-?[1-9][0-9]*') THEN 'unresolved_invalid_file_number'
                 ELSE NULL END AS early_status,
            CASE WHEN t.source_authority = 'official-fec' AND t.mapping_version = r.mapper
                AND r.definition_pin IS NOT NULL THEN r.definition_pin END AS namespace_pin,
            CASE WHEN t.source_authority = 'official-fec' AND t.mapping_version = r.mapper
                AND r.definition_pin IS NOT NULL AND regexp_full_match({raw}, '-?[1-9][0-9]*')
                THEN {literal(_KEY_PREFIX)} || {raw} END AS referenced_key
        FROM {quoted(table)} t LEFT JOIN routes r ON t.source_namespace = r.namespace
    ), decisions AS (
        SELECT s.*,
            CASE WHEN s.early_status IS NOT NULL THEN s.early_status
                 WHEN f.n IS NULL THEN 'unresolved_target_not_retained'
                 WHEN f.n <> f.unique_ids THEN 'unresolved_target_observation_ambiguous'
                 WHEN NOT f.identity_valid OR f.filer_count > 1 OR f.form_count > 1 THEN 'unresolved_target_identity_conflict'
                 WHEN NULLIF(s.association_filer_id, '') IS NOT NULL AND f.native_filer_id IS NOT NULL
                    AND s.association_filer_id <> f.native_filer_id THEN 'unresolved_filer_identity_conflict'
                 ELSE 'resolved_native_filing_key' END AS decided_status,
            COALESCE(f.n, 0) AS target_count, COALESCE(f.witness_ids, []::VARCHAR[]) AS target_ids
        FROM source s LEFT JOIN targets f ON s.referenced_key = f.filing_key
    ) SELECT {literal(POLICY_VERSION)} AS policy_version, {literal(table)} AS target_table,
        record_id AS target_record_id, {literal(source_generation_pin)} AS source_generation_pin,
        collection_id, source_record_id, source_sha256, source_authority, source_locator_json,
        'native-file-number' AS association_basis, decided_status AS association_status,
        referenced_key AS referenced_filing_key,
        CASE WHEN decided_status = 'resolved_native_filing_key' THEN referenced_key END AS filing_key,
        selection_evidence_sha256, namespace_pin AS namespace_evidence_sha256,
        association_report_number AS report_number_raw,
        NULL::VARCHAR AS header_record_id, NULL::VARCHAR AS header_locator_json,
        NULL::VARCHAR AS request_url, NULL::VARCHAR AS resolved_url,
        'unknown' AS replacement_mode, 'unqualified' AS membership_completeness,
        'unqualified' AS current_record_status,
        target_count AS filing_observation_count, target_ids AS filing_observation_ids
        FROM decisions"""


def financial_header_association_sql(table, source_generation_pin):
    """One association decision per typed row, preserving original money columns.

    The caller registers the bounded header output as fec_filing_header_associations.
    Exact physical keys and generation bind the join. Duplicate association rows
    refuse instead of expanding a money-bearing row into several observations.
    """
    _pin(source_generation_pin)
    return f"""WITH h AS (
        SELECT collection_id, source_sha256, source_authority, header_record_id,
            min(header_locator_json) AS header_locator_json, count(*) AS n, min(record_id) AS association_record_id,
            min(policy_version) AS policy_version, min(association_basis) AS association_basis,
            min(filing_key) AS filing_key, min(referenced_filing_key) AS referenced_filing_key,
            min(association_status) AS association_status
        FROM fec_filing_header_associations
        WHERE source_generation_pin = {literal(source_generation_pin)}
        GROUP BY collection_id, source_sha256, source_authority, header_record_id
    ), checked AS (
        SELECT *, policy_version = {literal(POLICY_VERSION)}
            AND association_basis = 'native-api-original-url'
            AND association_status IN ('resolved_native_filing_key', 'unresolved_source_authority',
                'unresolved_archive_member_identity', 'unresolved_redirect_identity',
                'unresolved_no_native_url_witness', 'unresolved_target_observation_ambiguous',
                'unresolved_target_identity_conflict')
            AND CASE WHEN association_status = 'resolved_native_filing_key'
                THEN source_authority = 'official-fec'
                    AND regexp_full_match(filing_key, 'urn:fec:filing:official-fec:openfec-file-number:-?[1-9][0-9]*')
                    AND referenced_filing_key = filing_key
                ELSE filing_key IS NULL AND referenced_filing_key IS NULL END AS supported_association
        FROM h
    ) SELECT t.record_id AS target_record_id, {literal(table)} AS target_table,
        t.collection_id, t.source_record_id, t.source_sha256, t.source_authority,
        t.filing_header_record_id, {literal(POLICY_VERSION)} AS association_policy_version,
        CASE WHEN h.n = 1 THEN h.association_record_id END AS association_record_id,
        CASE WHEN h.n = 1 THEN h.policy_version END AS header_association_policy_version,
        CASE WHEN h.n = 1 THEN h.association_status END AS header_association_status,
        CASE WHEN h.n = 1 AND h.supported_association
                  AND t.filing_header_locator_json = h.header_locator_json THEN h.filing_key END AS filing_key,
        CASE WHEN h.n IS NULL THEN 'unresolved_header_association_absent'
             WHEN h.n > 1 THEN 'unresolved_header_association_ambiguous'
             WHEN h.policy_version IS DISTINCT FROM {literal(POLICY_VERSION)} THEN 'unresolved_header_policy_version'
             WHEN NOT COALESCE(h.supported_association, FALSE) THEN 'unresolved_header_policy_semantics'
             WHEN t.filing_header_locator_json IS DISTINCT FROM h.header_locator_json THEN 'unresolved_header_locator_mismatch'
             ELSE h.association_status END AS association_status,
        'unknown' AS replacement_mode, 'unqualified' AS membership_completeness,
        'unqualified' AS current_record_status
        FROM {quoted(table)} t LEFT JOIN checked h
          ON t.collection_id = h.collection_id AND t.source_sha256 = h.source_sha256
          AND t.source_authority = h.source_authority AND t.filing_header_record_id = h.header_record_id"""
