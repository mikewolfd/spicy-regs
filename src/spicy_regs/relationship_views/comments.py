"""Explicit Regulations.gov comment references; identifier namespaces stay separate."""

import json
from typing import Any, Iterable, Mapping

from .core import literal
from .lineage import column_lineage, table_columns, view_columns_from

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
    lineages: dict[str, dict[str, list[str]]] = {}
    if status == "available":
        provenance = (
            f"{literal(json.dumps(pin, sort_keys=True))} AS source_publication_json, "
            f"'{RULE_VERSION}' AS rule_version, 'comments' AS source_table"
        )
        # One scan of comments joined to the three field names: each field stays
        # its own observation (no cross join between references), and a filter
        # on source_field prunes the field list, not the table.
        fields = ", ".join(f"('{field}', '{kind}')" for field, kind in (
            ("commentOnDocumentId", "regulations_document"),
            ("commentOn", "regulations_object"),
            ("originalDocumentId", "regulations_original_document"),
        ))
        value = "json_extract(c.raw, '$.' || f.source_field)"
        scalar = "json_extract_string(c.raw, '$.' || f.source_field)"
        selects = {}
        selects["comment_reference_field_states"] = f"""SELECT c.comment_id, c.docket_id, c.comment_reference_values_json AS raw_field_value,
                f.source_field, '/data/attributes/' || f.source_field AS source_pointer,
                0::BIGINT AS source_ordinal, CAST({value} AS VARCHAR) AS raw_value_json,
                f.target_kind,
                CASE WHEN json_type({value}) = 'VARCHAR' AND trim({scalar}) <> ''
                     THEN {scalar} ELSE NULL END AS target_key,
                CASE WHEN c.comment_reference_values_json IS NULL THEN 'unread'
                     WHEN c.raw IS NULL THEN 'malformed_json'
                     WHEN json_type(c.raw) <> 'OBJECT' THEN 'unsupported_shape'
                     WHEN NOT json_exists(c.raw, '$.' || f.source_field) THEN 'absent'
                     WHEN json_type({value}) = 'NULL' THEN 'null'
                     WHEN json_type({value}) <> 'VARCHAR' THEN 'unsupported_shape'
                     WHEN trim({scalar}) = '' THEN 'empty_string'
                     ELSE 'valid' END AS parsing_status, {provenance}
            FROM (SELECT comment_id, docket_id, comment_reference_values_json,
                         TRY_CAST(comment_reference_values_json AS JSON) AS raw FROM comments) c
            CROSS JOIN (VALUES {fields}) AS f(source_field, target_kind)"""
        selects["comment_native_references"] = """SELECT *, CASE WHEN parsing_status = 'valid' AND target_kind = 'regulations_document'
                           THEN 'not_checked' ELSE 'unsupported' END AS target_status
            FROM comment_reference_field_states
            WHERE parsing_status NOT IN ('unread', 'absent')"""
        selects["comment_document_references"] = """SELECT * FROM comment_native_references WHERE source_field = 'commentOnDocumentId'"""
        selects["comment_document_references_pairs"] = """SELECT DISTINCT comment_id, target_kind, target_key, target_status, source_table,
                source_field, source_publication_json, rule_version
            FROM comment_document_references WHERE parsing_status = 'valid'"""
        relations = {"comments": table_columns("comments", sorted(columns))}
        # Each view reads the one before it, so lineage threads back to the comments table.
        for name, select in selects.items():
            connection.execute(f'CREATE OR REPLACE VIEW "{name}" AS {select}')
            lineages[name] = column_lineage(connection, select, relations)
            relations[name] = view_columns_from(lineages[name])
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
            **({"column_lineage": lineages[name]} if name in lineages else {}),
        },
    } for name in NAMES}
