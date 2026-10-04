"""Native child queries for receipt-separated FEC document subjects.

Views only expand one repeated value at a time. They expose domain values and
stable parent keys; null and empty collections remain distinguishable on the
parent. Evidence and parsing failures are read from the selected ETL receipts.
"""

from .sql_views import SQLView

VERSION = "fec-native-document-query/1"


def _array(name, table, field, columns, fields, meaning):
    def sql(_publication):
        parent = ", ".join(f'p."{c}"' for c in columns)
        child = ", ".join(f'e.value."{source}" AS "{target}"' for source, target in fields)
        return f"""SELECT {parent}, e.ordinality - 1 AS source_ordinal, {child}
            FROM {table} p, UNNEST(p.{field}) WITH ORDINALITY AS e(value, ordinality)"""

    return SQLView(name, {table: (*columns, field)}, sql, meaning, ("record_id", "source_ordinal"), VERSION)


def _citations(_publication):
    branches = []
    kinds = {
        "ao_citations": ("no", "name", None, None, None),
        "aos_cited_by": ("no", "name", None, None, None),
        "regulatory_citations": (None, None, "title", "part", "section"),
        "statutory_citations": (None, None, "title", None, "section"),
        "citations.regulations": (None, None, None, None, None),
        "citations.us_code": (None, None, None, None, None),
    }
    for field, (number, name, title, part, section) in kinds.items():

        def col(value):
            return f'e.value."{value}"::VARCHAR' if value else "NULL::VARCHAR"

        linked = field.startswith("citations.")
        branches.append(f"""SELECT p.record_id, p.matter_id, '{field}' AS citation_kind,
            e.ordinality - 1 AS source_ordinal,
            {col(number)} AS advisory_opinion_number, {col(name)} AS name,
            {col(title)} AS title, {col(part)} AS part, {col(section)} AS section,
            {col("text" if linked else None)} AS citation_text,
            {col("url" if linked else None)} AS url
            FROM fec_legal_matters p, UNNEST(p.{field}) WITH ORDINALITY AS e(value, ordinality)""")
    return " UNION ALL ".join(branches)


def _subjects(_publication):
    return """SELECT p.record_id, p.matter_id, 'subject' AS subject_kind,
        n.value.path AS ordinal_path, n.value.node.text AS subject,
        n.value.node.children AS child_ordinals,
        NULL::VARCHAR AS primary_subject_id, NULL::VARCHAR AS secondary_subject_id
        FROM fec_legal_matters p, UNNEST(p.subject) AS n(value)
        UNION ALL
        SELECT p.record_id, p.matter_id, 'subjects' AS subject_kind,
        [cast(n.ordinality - 1 AS INTEGER)] AS ordinal_path, n.value.subject,
        NULL::INTEGER[] AS child_ordinals,
        n.value.primary_subject_id, n.value.secondary_subject_id
        FROM fec_legal_matters p, UNNEST(p.subjects) WITH ORDINALITY AS n(value, ordinality)"""


FEC_NATIVE_DOCUMENT_QUERY_VIEWS = (
    _array(
        "fec_filing_report_measures",
        "fec_filing_report_observations",
        "reported_measures",
        ("record_id", "filing_key", "currency"),
        tuple(
            (n, "exact_value" if n == "value" else n)
            for n in ("native_position", "native_label", "value", "quantity_kind", "measure_role", "period_basis")
        ),
        "Ordered exact report measures; reported components, totals, subtotals and period bases remain distinct.",
    ),
    _array(
        "fec_filing_text",
        "fec_filing_text_observations",
        "text_fragments",
        ("record_id", "filing_key", "text_record_kind", "reporting_committee_id", "transaction_id"),
        (("field_position", "field_position"), ("text", "text")),
        "Ordered narrative fragments, preserving repeats and null elements.",
    ),
    SQLView(
        "fec_legal_citations",
        {
            "fec_legal_matters": (
                "record_id",
                "matter_id",
                "ao_citations",
                "aos_cited_by",
                "regulatory_citations",
                "statutory_citations",
                "citations",
            )
        },
        _citations,
        "Native citation occurrences, keeping direction and sibling order.",
        ("record_id", "citation_kind", "source_ordinal"),
        VERSION,
    ),
    SQLView(
        "fec_legal_subjects",
        {"fec_legal_matters": ("record_id", "matter_id", "subject", "subjects")},
        _subjects,
        "Native subject hierarchy and both publisher subject IDs, preserving repeats.",
        ("record_id", "subject_kind", "ordinal_path"),
        VERSION,
    ),
)
