"""Explicit Regulations.gov comment references; identifier namespaces stay separate."""

import json
from typing import Any, Iterable, Mapping

from .core import literal

RULE_VERSION = "comment-native-reference-v1"
NAMES = (
    "comment_document_references", "comment_document_references_pairs", "comment_reference_field_states",
    "comment_native_references",
)
REQUIRED = {
    "comment_id", "docket_id", "comment_on_document_id", "comment_on_object_id", "original_document_id",
    "comment_reference_values_json",
}


def install_comment_references(
    connection: Any, available_tables: Iterable[str], publication: Mapping[str, object] | None = None,
) -> dict[str, dict[str, Any]]:
    """Expose held raw-field state; SQL NULL on legacy rows remains unread."""
    status, reason = "available", None
    if "comments" not in set(available_tables):
        status, reason = "unavailable", "Source table is not loaded"
    else:
        columns = {row[0] for row in connection.execute('DESCRIBE "comments"').fetchall()}
        missing = REQUIRED - columns
        if missing:
            status, reason = "unsupported", "Missing source columns: " + ", ".join(sorted(missing))
    pin = (publication or {}).get("comments")
    if status == "available":
        provenance = (
            f"{literal(json.dumps(pin, sort_keys=True))} AS source_publication_json, "
            f"'{RULE_VERSION}' AS rule_version, 'comments' AS source_table"
        )
        # No cross join between references: each field is its own observation.
        selections = []
        for field, kind in (("commentOnDocumentId", "regulations_document"),
                            ("commentOn", "regulations_object"),
                            ("originalDocumentId", "regulations_original_document")):
            raw = "TRY_CAST(comment_reference_values_json AS JSON)"
            value = f"json_extract({raw}, '$.{field}')"
            scalar = f"json_extract_string({raw}, '$.{field}')"
            selections.append(f"""SELECT comment_id, docket_id, comment_reference_values_json AS raw_field_value,
                '{field}' AS source_field, '/data/attributes/{field}' AS source_pointer,
                0::BIGINT AS source_ordinal, CAST({value} AS VARCHAR) AS raw_value_json,
                '{kind}' AS target_kind,
                CASE WHEN json_type({value}) = 'VARCHAR' AND trim({scalar}) <> ''
                     THEN {scalar} ELSE NULL END AS target_key,
                CASE WHEN comment_reference_values_json IS NULL THEN 'unread'
                     WHEN {raw} IS NULL THEN 'malformed_json'
                     WHEN json_type({raw}) <> 'OBJECT' THEN 'unsupported_shape'
                     WHEN NOT json_exists({raw}, '$.{field}') THEN 'absent'
                     WHEN json_type({value}) = 'NULL' THEN 'null'
                     WHEN json_type({value}) <> 'VARCHAR' THEN 'unsupported_shape'
                     WHEN trim({scalar}) = '' THEN 'empty_string'
                     ELSE 'valid' END AS parsing_status, {provenance}
                FROM comments""")
        connection.execute("CREATE OR REPLACE VIEW comment_reference_field_states AS " + " UNION ALL ".join(selections))
        connection.execute("""CREATE OR REPLACE VIEW comment_native_references AS
            SELECT *, CASE WHEN parsing_status = 'valid' AND target_kind = 'regulations_document'
                           THEN 'not_checked' ELSE 'unsupported' END AS target_status
            FROM comment_reference_field_states
            WHERE parsing_status NOT IN ('unread', 'absent')""")
        connection.execute("""CREATE OR REPLACE VIEW comment_document_references AS
            SELECT * FROM comment_native_references WHERE source_field = 'commentOnDocumentId'""")
        connection.execute("""CREATE OR REPLACE VIEW comment_document_references_pairs AS
            SELECT DISTINCT comment_id, target_kind, target_key, target_status, source_table,
                source_field, source_publication_json, rule_version
            FROM comment_document_references WHERE parsing_status = 'valid'""")
    return {name: {
        "status": status, "reason": reason, "dependencies": ["comments"],
        "metadata": {
            "label": name.replace("_", " "), "kind": "derived", "rule_version": RULE_VERSION,
            "source_keys": ["comment_id"], "source_field": "comment_reference_values_json",
            "identity_columns": ["comment_id", "source_field"],
            "input_publications": {"comments": pin},
            "summary": "Explicit native comment references. Parent-document, object and original-document IDs "
            "remain different namespaces. Docket NULL is preserved; no prefix inference or target lookup.",
            "coverage": "Only rows retaining the raw-field map distinguish absent, null and empty values. "
            "Legacy SQL NULL is unread. Non-document namespaces are unsupported for target resolution.",
        },
    } for name in NAMES}
