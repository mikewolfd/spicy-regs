"""Published Federal Register table shape shared by its writer and readers."""

FEDERAL_REGISTER_COLUMNS: tuple[str, ...] = (
    "document_number",
    "title",
    "abstract",
    "document_type",
    "publication_date",
    "effective_on",
    "comments_close_on",
    "signing_date",
    "agencies_json",
    "agency_slugs",
    "docket_ids_json",
    "regulation_id_numbers_json",
    "cfr_references_json",
    "topics_json",
    "html_url",
    "pdf_url",
    "body_html_url",
    "volume",
    "start_page",
    "end_page",
    "subtype",
    "executive_order_number",
    "modify_date",
)

# ``topics_json`` is an additive migration.  The current writer emits it, but
# a pinned published table from before its one-time backfill legitimately does
# not have that column.  Readers capture it whenever present and do not invent
# a null source field when it is absent from that exact source version.
FEDERAL_REGISTER_OPTIONAL_COLUMNS: tuple[str, ...] = ("topics_json",)

__all__ = ["FEDERAL_REGISTER_COLUMNS", "FEDERAL_REGISTER_OPTIONAL_COLUMNS"]
