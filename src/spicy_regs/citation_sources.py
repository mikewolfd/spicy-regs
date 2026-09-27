"""Exact held text fields shared by extraction and serving, without ETL imports."""

from dataclasses import dataclass
import json


@dataclass(frozen=True)
class TextSource:
    table: str
    keys: tuple[str, ...]
    field: str
    rendition: str
    derivation: str = "literal-held-field"
    excluded_rules: tuple[str, ...] = ()


TEXT_SOURCES = {
    "court_opinion_derived_pdf": TextSource(
        "court_opinion_pdf_extractions",
        ("opinion_id", "source_sha256"),
        "text_content",
        "pdf",
        "derived_pdf",
        ("case_docket_number",),
    ),
    "communication_authority": TextSource(
        "house_communications", ("congress", "communication_type", "number"), "legal_authority", "txt"
    ),
    "communication_report_nature": TextSource(
        "house_communications", ("congress", "communication_type", "number"), "report_nature", "txt"
    ),
    "communication_record_entry": TextSource(
        "house_communications", ("congress", "communication_type", "number"), "record_entry_text", "txt"
    ),
    "bill_section": TextSource("bill_sections", ("bill_id", "version_code", "source", "seq"), "body", "txt"),
    "report_section": TextSource("report_sections", ("package_id", "part_id", "seq"), "body", "txt"),
    "lobbying_activity": TextSource("lobbying_activities", ("filing_uuid", "activity_index"), "description", "txt"),
    "comment_inline": TextSource("comments", ("comment_id",), "comment", "htm"),
}


def document_key(kind: str, values: tuple[str, ...]) -> str:
    """Keep single native keys literal and encode composite keys without delimiter collisions."""
    spec = TEXT_SOURCES[kind]
    if len(values) != len(spec.keys) or any(not isinstance(value, str) or not value for value in values):
        raise ValueError(f"{kind} requires nonempty values for {spec.keys}")
    return values[0] if len(values) == 1 else json.dumps(values, ensure_ascii=False, separators=(",", ":"))


def key_values(kind: str, key: str) -> tuple[str, ...]:
    spec = TEXT_SOURCES[kind]
    parsed = [key] if len(spec.keys) == 1 else json.loads(key)
    if not isinstance(parsed, list):
        raise ValueError("A composite document key must be a JSON list")
    values = tuple(parsed)
    document_key(kind, values)
    return values


def source_digests(cursor, kind: str, key: str) -> list[tuple[str | None]]:
    """Hash the actual current field under its full identity; keep duplicate rows ambiguous."""
    spec = TEXT_SOURCES[kind]
    values = key_values(kind, key)
    predicate = " AND ".join(f'CAST("{column}" AS VARCHAR) = ?' for column in spec.keys)
    return cursor.execute(
        f'SELECT \'sha256:\' || sha256("{spec.field}") FROM "{spec.table}" WHERE {predicate} LIMIT 2',
        list(values),
    ).fetchall()
