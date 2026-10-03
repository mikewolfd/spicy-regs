"""Thin grouping and child-row navigation over build-shaped FEC context tables.

Feed interpretation, page cleanup and scalar typing belong to the producers.
These views add a response grain or expand independently repeated values only.
"""

from .sql_views import SQLView

VERSION = "fec-query-shape-v1"
CONTEXT = (
    "record_id",
    "collection_id",
    "source_authority",
    "source_url",
    "source_sha256",
    "context_sha256",
    "source_context_pointer",
    "observed_at_json",
)
API_KEY = ("collection_id", "source_sha256", "source_namespace", "source_url")
API_VALUES = ("api_version", "page", "per_page", "reported_count", "reported_pages", "is_count_exact", "observed_count")


def _view(name, table, columns, sql, meaning, identity=("record_id",)):
    return SQLView(name, {table: tuple(columns)}, lambda _: sql, meaning, identity, VERSION)


def _cols(columns, prefix=""):
    return ", ".join(prefix + c for c in columns)


_agreed = ", ".join(
    f"CASE WHEN count(DISTINCT ({_cols(API_VALUES)}, parsing_status))=1 THEN min({c}) END AS {c}" for c in API_VALUES
)
_response_values = ", ".join(
    f"max({c}) FILTER (WHERE response_field='{field}') AS {c}"
    for c, field in (
        ("api_version", "api_version"),
        ("page", "pagination"),
        ("per_page", "pagination"),
        ("reported_count", "pagination"),
        ("reported_pages", "pagination"),
        ("is_count_exact", "pagination"),
        ("observed_count", "results"),
    )
)
_api = _view(
    "fec_api_responses",
    "fec_api_response_controls",
    (*API_KEY, *API_VALUES, "record_id", "response_field", "parsing_status", "query_completeness"),
    f"""WITH fields AS (
      SELECT {_cols(API_KEY)}, response_field, {_agreed}, count(*) AS field_rows,
        count(DISTINCT ({_cols(API_VALUES)}, parsing_status)) AS distinct_values,
        bool_or(parsing_status <> 'parsed' OR parsing_status IS NULL) AS invalid_field,
        count(DISTINCT coalesce(query_completeness, '<SQL-NULL>')) AS completeness_values,
        CASE WHEN count(DISTINCT coalesce(query_completeness, '<SQL-NULL>'))=1 THEN min(query_completeness) END AS completeness
      FROM fec_api_response_controls GROUP BY {_cols(API_KEY)}, response_field
    ) SELECT {_cols(API_KEY)}, {_response_values}, sum(field_rows)::BIGINT AS control_row_count,
       sum(field_rows-1)::BIGINT AS repeated_field_rows,
       CASE WHEN bool_or(distinct_values>1) THEN 'conflicting_fields'
            WHEN bool_or(invalid_field) THEN 'invalid_control'
            WHEN count(*) FILTER (WHERE response_field IN ('api_version','pagination','results'))<>3 THEN 'missing_fields'
            ELSE 'parsed' END AS parsing_status,
       CASE WHEN max(completeness_values)=1 AND count(DISTINCT coalesce(completeness,'<SQL-NULL>'))=1
            THEN min(completeness) END AS query_completeness,
       CASE WHEN max(completeness_values)>1 OR count(DISTINCT coalesce(completeness,'<SQL-NULL>'))>1
            THEN 'conflicting_declarations' ELSE 'source_declaration' END AS query_completeness_status,
       'capture_only_no_population_completeness_claim' AS completeness_status,
       '{VERSION}' AS rule_version
       FROM fields GROUP BY {_cols(API_KEY)}""",
    "One response from build-typed controls. Source control rows retain identities and coordinates. "
    "Repeated conflicting values are explicit; observed counts never imply population completeness.",
    API_KEY,
)

_context_counts = _view(
    "fec_context_output_counts",
    "fec_research_context_dispositions",
    ("collection_id", "context_sha256", "outputs_json"),
    "SELECT collection_id, context_sha256, j.key AS output_table, "
    "CASE WHEN json_type(j.value) IN ('BIGINT','UBIGINT') AND try_cast(j.value AS BIGINT)>=0 "
    "THEN try_cast(j.value AS BIGINT) END AS output_count, "
    "CASE WHEN json_type(j.value) IN ('BIGINT','UBIGINT') AND try_cast(j.value AS BIGINT)>=0 "
    "THEN 'parsed' ELSE 'unsupported_count' END AS parsing_status, '" + VERSION + "' AS rule_version "
    "FROM fec_research_context_dispositions, json_each(CASE WHEN json_type(try_cast(outputs_json AS JSON))='OBJECT' "
    "THEN try_cast(outputs_json AS JSON) ELSE '{}'::JSON END) j",
    "Emitted table counts from each build context; independent child rows preserve context identity.",
    ("collection_id", "context_sha256", "output_table"),
)


def _meeting_child(name, field, expression, extra_columns=()):
    return _view(
        name,
        "fec_research_meeting_observations",
        (*CONTEXT, field, *extra_columns),
        f"SELECT {_cols(CONTEXT, 's.')}, try_cast(j.key AS BIGINT) AS source_ordinal, "
        f"'{field}' AS source_field, '/' || j.key AS source_pointer, {expression}, "
        f"'{VERSION}' AS rule_version FROM fec_research_meeting_observations s, "
        f"json_each(CASE WHEN json_type(try_cast({field} AS JSON))='ARRAY' "
        f"THEN try_cast({field} AS JSON) ELSE '[]'::JSON END) j",
        "Independent ordered children of the build-shaped meeting record; repeated elements remain distinct.",
        ("record_id", "source_ordinal"),
    )


_meeting_dates = _meeting_child(
    "fec_meeting_dates",
    "dates_json",
    "date_status, CASE WHEN json_type(j.value)='VARCHAR' THEN json_extract_string(j.value, '$') END AS date_raw, "
    "CASE WHEN json_type(j.value)='VARCHAR' AND regexp_full_match(json_extract_string(j.value, '$'), '[0-9]{4}-[0-9]{2}-[0-9]{2}') "
    "THEN try_cast(json_extract_string(j.value, '$') AS DATE) END AS meeting_date, "
    "CASE WHEN date_status='source_date_range' AND j.key='0' THEN 'range_start' "
    "WHEN date_status='source_date_range' AND j.key='1' THEN 'range_end' "
    "WHEN date_status='source_single_date' THEN 'single_date' WHEN date_status='source_listed_dates' THEN 'listed_date' "
    "ELSE 'source_date_unspecified_role' END AS date_role, "
    "CASE WHEN json_type(j.value)='VARCHAR' AND regexp_full_match(json_extract_string(j.value, '$'), '[0-9]{4}-[0-9]{2}-[0-9]{2}') "
    "AND try_cast(json_extract_string(j.value, '$') AS DATE) IS NOT NULL THEN 'parsed' ELSE 'unsupported_date' END AS parsing_status",
    ("date_status",),
)
_meeting_links = _meeting_child(
    "fec_meeting_links",
    "links_json",
    "json_extract_string(j.value, '$.url') AS url, json_extract_string(j.value, '$.href') AS href, "
    "json_extract_string(j.value, '$.body_status') AS body_status, "
    "try_cast(json_extract_string(j.value, '$.source_fact_index') AS BIGINT) AS source_fact_index, "
    "CASE WHEN json_type(j.value)='OBJECT' AND json_type(j.value, '$.url')='VARCHAR' "
    "THEN 'parsed' ELSE 'unsupported_link' END AS parsing_status",
)

FEC_CONTEXT_QUERY_VIEWS = (_api, _context_counts, _meeting_dates, _meeting_links)
