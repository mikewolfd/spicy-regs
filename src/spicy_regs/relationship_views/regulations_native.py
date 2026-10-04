"""Existing regulatory navigation names over native subject lists.

Processing/raw-field-state views belong to receipt inspection. This installer
never reconstructs public JSON mirrors or multiplies independent arrays.
Integration selects this installer for receipt-migrated generations only.
"""

from __future__ import annotations

from .core import quoted

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


def install_native_regulations_views(connection, available_tables):
    """Install native domain views; return explicit migration availability."""
    available = set(available_tables)
    installed = {}
    for name, table, keys, field, kind, value in ARRAYS:
        if table not in available:
            continue
        columns = {r[0] for r in connection.execute(f"DESCRIBE {quoted(table)}").fetchall()}
        if not {*keys, field} <= columns:
            raise ValueError(f"{table}: native subject columns required")
        selected = ", ".join("s." + quoted(k) for k in keys)
        ids = ", ".join(quoted(k) for k in keys)
        pattern = "[0-9]{4}-[A-Z0-9]{4}" if kind == "rin" else "https?://.+" if kind == "offered_url" else ".+"
        valid = f"regexp_full_match({value}, '{pattern}')"
        connection.execute(f"""CREATE OR REPLACE VIEW {quoted(name + "_occurrences")} AS
            SELECT {selected}, ordinality-1 AS source_ordinal, '{kind}' AS target_kind,
                   CASE WHEN {valid} THEN {value} ELSE NULL END AS target_key
            FROM {quoted(table)} s, UNNEST(s.{quoted(field)}) WITH ORDINALITY AS member(item,ordinality)""")
        connection.execute(f"""CREATE OR REPLACE VIEW {quoted(name + "_pairs")} AS
            SELECT DISTINCT {ids},target_kind,target_key FROM {quoted(name + "_occurrences")}
            WHERE target_key IS NOT NULL """)
        installed[name + "_occurrences"] = "native"
        installed[name + "_pairs"] = "native"
        installed[name + "_field_states"] = "receipt-only"
    if "documents" in available:
        connection.execute("""CREATE OR REPLACE VIEW document_attachment_renditions AS
            SELECT s.document_id,a.ordinality-1 AS attachment_ordinal,a.item.attachment_id,
                   f.ordinality-1 AS format_ordinal,'attachment' AS artifact_role,
                   f.item.url AS offered_url,f.item.format AS format,f.item.size AS source_size
            FROM documents s,
                 UNNEST(s.attachment_records) WITH ORDINALITY AS a(item,ordinality),
                 UNNEST(a.item.file_formats) WITH ORDINALITY AS f(item,ordinality)""")
        installed["document_attachment_renditions"] = "native"
    if {"regulatory_agenda_items", "unified_agenda"} <= available:
        connection.execute("""CREATE OR REPLACE VIEW agenda_item_editions AS
            SELECT i.agenda_item_id,i.rin,e.agenda_edition,e.target_count
            FROM regulatory_agenda_items i LEFT JOIN
              (SELECT rin,agenda_edition,count(*) AS target_count FROM unified_agenda GROUP BY rin,agenda_edition) e
              ON i.rin=e.rin AND i.rin<>'' """)
        installed["agenda_item_editions"] = "native"
    if "comments" in available:
        connection.execute("""CREATE OR REPLACE VIEW comment_native_references AS
            SELECT comment_id,docket_id,'regulations_document' AS target_kind,comment_on_document_id AS target_key
            FROM comments WHERE comment_on_document_id IS NOT NULL
            UNION ALL SELECT comment_id,docket_id,'regulations_object',comment_on_object_id
            FROM comments WHERE comment_on_object_id IS NOT NULL
            UNION ALL SELECT comment_id,docket_id,'regulations_original_document',original_document_id
            FROM comments WHERE original_document_id IS NOT NULL""")
        connection.execute("""CREATE OR REPLACE VIEW comment_document_references AS
            SELECT * FROM comment_native_references WHERE target_kind='regulations_document' """)
        connection.execute("""CREATE OR REPLACE VIEW comment_document_references_pairs AS
            SELECT DISTINCT comment_id,target_kind,target_key FROM comment_document_references WHERE trim(target_key)<>'' """)
        installed.update(
            {
                name: "native"
                for name in (
                    "comment_native_references",
                    "comment_document_references",
                    "comment_document_references_pairs",
                )
            }
        )
        installed["comment_reference_field_states"] = "receipt-only"
    installed["lifecycle_date_evidence"] = "receipt-only"
    return installed
