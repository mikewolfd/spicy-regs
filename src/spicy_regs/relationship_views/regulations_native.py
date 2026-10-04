"""Existing regulatory navigation names over native subject lists.

Processing/raw-field-state views belong to receipt inspection. This installer
never reconstructs public JSON mirrors or multiplies independent arrays.
Integration selects this installer for receipt-migrated generations only.
"""

from __future__ import annotations

from .core import quoted
from .sql_views import SQLView, install_sql_views

# Existing occurrence/pair names, domain keys and native member expressions.
ARRAYS = (
    (
        "federal_register_rins",
        "federal_register",
        ("document_number", "publication_date"),
        "regulation_id_numbers",
        "rin",
        "item",
    ),
    (
        "federal_register_dockets",
        "federal_register",
        ("document_number", "publication_date"),
        "docket_ids",
        "docket_spelling",
        "item",
    ),
    ("document_additional_rins", "documents", ("document_id",), "additional_rins", "rin", "item"),
    ("proceeding_dockets", "proceedings", ("proceeding_id",), "docket_ids", "regulations_docket", "item"),
    ("proceeding_rins", "proceedings", ("proceeding_id",), "rins", "rin", "item"),
    (
        "proceeding_federal_register",
        "proceedings",
        ("proceeding_id",),
        "fr_document_ids",
        "dated_federal_register",
        "item",
    ),
    ("comment_period_proceedings", "comment_periods", ("comment_period_id",), "proceeding_ids", "proceeding", "item"),
    ("comment_period_dockets", "comment_periods", ("comment_period_id",), "docket_ids", "regulations_docket", "item"),
    ("comment_period_rins", "comment_periods", ("comment_period_id",), "rins", "rin", "item"),
    ("document_artifacts", "documents", ("document_id",), "attachments", "offered_url", "item.url"),
    (
        "document_attachment_records",
        "documents",
        ("document_id",),
        "attachment_records",
        "attachment",
        "item.attachment_id",
    ),
    (
        "federal_register_agencies",
        "federal_register",
        ("document_number", "publication_date"),
        "agencies",
        "federal_register_agency",
        "CAST(item.id AS VARCHAR)",
    ),
    (
        "federal_register_topics",
        "federal_register",
        ("document_number", "publication_date"),
        "topics",
        "federal_register_topic",
        "item",
    ),
)



def native_regulations_views(available_tables):
    """Use the shared view binder for native fields, including column lineage."""
    available = set(available_tables)
    specs = []

    def add(name, required, sql, meaning, keys, descriptions=None):
        specs.append(SQLView(name, required, lambda _, sql=sql: sql, meaning, keys,
                             rule_version="native-subject-navigation/1",
                             column_descriptions=descriptions or {}))

    for name, table, keys, field, kind, value in ARRAYS:
        if table not in available:
            continue
        selected = ", ".join("s." + quoted(k) for k in keys)
        ids = ", ".join(quoted(k) for k in keys)
        pattern = "[0-9]{4}-[A-Z0-9]{4}" if kind == "rin" else "https?://.+" if kind == "offered_url" else ".+"
        valid = f"regexp_full_match({value}, '{pattern}')"
        occurrences = f"""SELECT {selected}, ordinality-1 AS source_ordinal,
            '{kind}' AS target_kind, CASE WHEN {valid} THEN {value} ELSE NULL END AS target_key
            FROM {quoted(table)} s, UNNEST(s.{quoted(field)}) WITH ORDINALITY AS elements(item,ordinality)"""
        meaning = f"Each {field} value on {table} in source order; repeated and null positions remain distinct."
        add(name + "_occurrences", {table: (*keys, field)}, occurrences, meaning, (*keys, "source_ordinal"))
        add(name + "_pairs", {table: (*keys, field)},
            f"SELECT DISTINCT {ids},target_kind,target_key FROM ({occurrences}) WHERE target_key IS NOT NULL",
            f"Distinct {kind} references from {table}.{field}; no target existence or identity claim.",
            (*keys, "target_kind", "target_key"))
    if "documents" in available:
        add("document_attachment_renditions", {"documents": ("document_id", "attachment_records")},
            """SELECT s.document_id,a.ordinality-1 AS attachment_ordinal,a.item.attachment_id,
                   f.ordinality-1 AS format_ordinal,'attachment' AS artifact_role,
                   f.item.url AS offered_url,f.item.format AS format,f.item.size AS source_size
            FROM documents s,
                 UNNEST(s.attachment_records) WITH ORDINALITY AS a(item,ordinality),
                 UNNEST(a.item.file_formats) WITH ORDINALITY AS f(item,ordinality)""",
            "Each offered format of a source attachment; URLs and sizes do not establish acquisition.",
            ("document_id", "attachment_ordinal", "format_ordinal"), {
                "attachment_ordinal": "Zero-based attachment position on the source document.",
                "attachment_id": "Publisher identifier on the attachment record, when stated.",
                "format_ordinal": "Zero-based offered format position within the attachment.",
                "artifact_role": "Always attachment: an offered attachment format.",
                "offered_url": "Publisher-offered format URL; no fetch or retention is asserted.",
                "format": "Publisher-stated attachment format.",
                "source_size": "Publisher-stated format size; not a measured download size.",
            })
    if {"regulatory_agenda_items", "unified_agenda"} <= available:
        add("agenda_item_editions", {"regulatory_agenda_items": ("agenda_item_id", "rin"),
             "unified_agenda": ("rin", "agenda_edition")},
            """SELECT i.agenda_item_id,i.rin,e.agenda_edition,e.target_count
            FROM regulatory_agenda_items i LEFT JOIN
              (SELECT rin,agenda_edition,count(*) AS target_count FROM unified_agenda GROUP BY rin,agenda_edition) e
              ON i.rin=e.rin AND i.rin<>'' """,
            "Agenda editions sharing the exact reported RIN; each edition stays separate.",
            ("agenda_item_id", "agenda_edition"))
    if "comments" in available:
        required = {"comments": ("comment_id", "docket_id", "comment_on_document_id", "comment_on_object_id", "original_document_id")}
        sql = """SELECT comment_id,docket_id,'regulations_document' AS target_kind,comment_on_document_id AS target_key
            FROM comments WHERE comment_on_document_id IS NOT NULL
            UNION ALL SELECT comment_id,docket_id,'regulations_object',comment_on_object_id
            FROM comments WHERE comment_on_object_id IS NOT NULL
            UNION ALL SELECT comment_id,docket_id,'regulations_original_document',original_document_id
            FROM comments WHERE original_document_id IS NOT NULL"""
        add("comment_native_references", required, sql,
            "Publisher document references on each comment, distinguished by reference kind.",
            ("comment_id", "target_kind", "target_key"))
        documents = f"SELECT * FROM ({sql}) WHERE target_kind='regulations_document'"
        add("comment_document_references", required, documents,
            "Comments linked by their publisher-stated comment-on document identifier.", ("comment_id", "target_key"))
        add("comment_document_references_pairs", required,
            f"SELECT DISTINCT comment_id,target_kind,target_key FROM ({documents}) WHERE trim(target_key)<>''",
            "Distinct publisher-stated comment-on document references; no target existence claim.",
            ("comment_id", "target_kind", "target_key"))
    return tuple(specs)


def install_native_regulations_views(connection, available_tables, publication=None):
    return install_sql_views(connection, available_tables, native_regulations_views(available_tables), publication)
