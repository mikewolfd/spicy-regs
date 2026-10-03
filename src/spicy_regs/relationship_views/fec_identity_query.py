"""Readable identity observations, with exact links back to retained evidence.

These views reshape selected observations. They do not select a current record,
merge captures, verify entity identity, or infer an uncertain date's century.
"""

from .core import literal, quoted
from .sql_views import SQLView, pin

RULE_VERSION = "fec-query-shape-v1"
_EVIDENCE = (
    "record_id",
    "collection_id",
    "source_record_id",
    "source_sha256",
    "source_locator_json",
    "source_authority",
    "source_namespace",
    "mapping_status",
    "source_cycle",
)
_CANDIDATES = "fec_candidate_api_observations"
_COMMITTEES = "fec_committee_observations"


def _column_list(columns, alias="s"):
    return ", ".join(f"{alias}.{quoted(c)}" for c in columns)


def _array_view(name, table, parent_id, field, output, kind):
    required = (*_EVIDENCE, parent_id, field)

    def query(publication):
        raw = f"s.{quoted(field)}"
        value = "json_extract_string(e.value, '$')"
        if kind == "INTEGER":
            valid = f"e.type IN ('BIGINT','UBIGINT') AND TRY_CAST({value} AS INTEGER) IS NOT NULL"
            typed = f"TRY_CAST({value} AS INTEGER)"
        else:
            valid = f"e.type='VARCHAR' AND trim({value})<>''"
            if output == "candidate_id":
                valid += f" AND regexp_full_match({value}, '[HSP][0-9A-Z]{{8}}')"
            typed = value
        return f"""WITH source AS (
            SELECT {_column_list(required)}, {raw} AS array_input FROM {quoted(table)} s
        ) SELECT {_column_list((*_EVIDENCE, parent_id))},
            CAST(e.key AS BIGINT) AS source_ordinal,
            {literal(field)} AS source_field,
            CASE WHEN {valid} THEN {typed} END AS {quoted(output)},
            CASE WHEN e.type='NULL' THEN 'json_null' WHEN {valid} THEN 'reported'
                ELSE 'unsupported_value' END AS parsing_status,
            e.value::VARCHAR AS raw_value_json,
            {pin(publication, table)} AS source_publication_json,
            '{RULE_VERSION}' AS rule_version
        FROM source s, json_each(CASE WHEN json_type(TRY_CAST(array_input AS JSON))='ARRAY'
            THEN TRY_CAST(array_input AS JSON) ELSE '[]'::JSON END) e"""

    return SQLView(
        name,
        {table: required},
        query,
        "One row per independently expanded source array element, including repeats and invalid elements. "
        "source_ordinal is zero-based. Empty/missing/malformed arrays emit no elements; inspect the "
        "parent record's array status and source evidence. Election years and districts are never paired "
        "by position, and candidate links do not establish target existence.",
        ("record_id", "source_ordinal"),
        RULE_VERSION,
    )


FEC_IDENTITY_QUERY_VIEWS = (
    _array_view("fec_candidate_cycles", _CANDIDATES, "candidate_id", "cycles_json", "cycle", "INTEGER"),
    _array_view(
        "fec_candidate_election_years", _CANDIDATES, "candidate_id", "election_years_json", "election_year", "INTEGER"
    ),
    _array_view(
        "fec_candidate_election_districts",
        _CANDIDATES,
        "candidate_id",
        "election_districts_json",
        "district",
        "VARCHAR",
    ),
    _array_view(
        "fec_candidate_inactive_election_years",
        _CANDIDATES,
        "candidate_id",
        "inactive_election_years_json",
        "election_year",
        "INTEGER",
    ),
    _array_view("fec_committee_cycles", _COMMITTEES, "committee_id", "cycles_json", "cycle", "INTEGER"),
    _array_view(
        "fec_committee_candidate_links", _COMMITTEES, "committee_id", "candidate_ids_json", "candidate_id", "VARCHAR"
    ),
    _array_view(
        "fec_committee_sponsor_candidate_links",
        _COMMITTEES,
        "committee_id",
        "sponsor_candidate_ids_json",
        "candidate_id",
        "VARCHAR",
    ),
)
