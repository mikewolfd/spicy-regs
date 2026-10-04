"""FCC navigation over native domain arrays; raw fields and parser states live in receipts."""

from .sql_views import SQLView, pin

_REQUIRED = {"fcc_filings": ("id_submission", "proceedings", "filers", "authors", "lawfirms", "bureaus", "documents")}
_RULE = "fcc-native-domain/2"


def observations(p):
    parts = []
    for field in ("proceedings", "filers", "authors", "lawfirms", "bureaus", "documents"):
        role = f"'{field}'" if field in ("filers", "authors", "lawfirms", "bureaus") else "NULL::VARCHAR"
        name = "v.name" if field != "documents" else "NULL::VARCHAR"
        native_id = "v.id_proceeding" if field == "proceedings" else "NULL::VARCHAR"
        filename = "v.filename" if field == "documents" else "NULL::VARCHAR"
        description = "v.description" if field in ("proceedings", "documents") else "NULL::VARCHAR"
        kind = (
            "fcc_proceeding"
            if field == "proceedings"
            else "offered_artifact"
            if field == "documents"
            else "participant_role"
        )
        parts.append(f"""SELECT s.id_submission, '{field}' AS source_field,
            ordinality - 1 AS source_ordinal, '{kind}' AS observation_kind,
            {role} AS participant_role, {name} AS observed_name,
            {native_id} AS native_proceeding_id, {filename} AS filename,
            {description} AS description
            FROM fcc_filings s, UNNEST(s.{field}) WITH ORDINALITY AS entries(v, ordinality)""")
    return " UNION ALL ".join(parts)


def proceeding_links(p):
    return f"""WITH observations AS ({observations(p)}), targets AS (
        SELECT name, id_proceeding, count(*) AS n FROM fcc_proceedings GROUP BY name,id_proceeding
    ) SELECT o.*, coalesce(t.n,0) AS target_count,
        CASE WHEN o.observed_name IS NULL OR trim(o.observed_name)='' OR o.native_proceeding_id IS NULL
                  OR trim(o.native_proceeding_id)='' THEN 'unsupported'
             WHEN t.n IS NULL THEN 'missing' WHEN t.n=1 THEN 'found' ELSE 'ambiguous' END AS target_status,
        {pin(p, "fcc_proceedings")} AS target_publication_json
    FROM observations o LEFT JOIN targets t ON o.observed_name=t.name AND o.native_proceeding_id=t.id_proceeding
    WHERE o.source_field='proceedings'"""


def memberships(p):
    return """SELECT s.id_submission, ordinality - 1 AS source_ordinal, v.name AS target_key,
        v.id_proceeding AS native_proceeding_id FROM fcc_filings s,
        UNNEST(s.proceedings) WITH ORDINALITY AS entries(v, ordinality)"""


def artifacts(p):
    return """SELECT s.id_submission, ordinality - 1 AS source_ordinal, v.filename, v.description
        FROM fcc_filings s, UNNEST(s.documents) WITH ORDINALITY AS entries(v, ordinality)"""


FCC_NATIVE_VIEWS = (
    SQLView(
        "fcc_native_observations",
        _REQUIRED,
        observations,
        "FCC source roles and document metadata in native array order, including repeats and null elements. "
        "Names do not resolve people. Raw/source URL, parser and acquisition evidence is in etl_receipts.",
        ("id_submission", "source_field", "source_ordinal"),
        rule_version=_RULE,
    ),
    SQLView(
        "fcc_native_proceeding_links",
        {**_REQUIRED, "fcc_proceedings": ("name", "id_proceeding")},
        proceeding_links,
        "Matches both the stated name and FCC numeric proceeding ID. Duplicate target identities remain ambiguous.",
        ("id_submission", "source_field", "source_ordinal"),
        rule_version=_RULE,
    ),
    SQLView(
        "fcc_filing_proceedings_occurrences",
        {"fcc_filings": ("id_submission", "proceedings")},
        memberships,
        "Each proceeding occurrence as named by the filing; no deduplication or inferred namespace.",
        ("id_submission", "source_ordinal"),
        rule_version=_RULE,
    ),
    SQLView(
        "fcc_filing_proceedings_pairs",
        {"fcc_filings": ("id_submission", "proceedings")},
        lambda p: (
            "SELECT DISTINCT id_submission, target_key, native_proceeding_id FROM ("
            + memberships(p)
            + ") WHERE target_key IS NOT NULL AND target_key <> ''"
        ),
        "Distinct named proceedings for navigation; occurrence rows preserve every stated position.",
        ("id_submission", "target_key", "native_proceeding_id"),
        rule_version=_RULE,
    ),
    SQLView(
        "fcc_filing_artifacts",
        {"fcc_filings": ("id_submission", "documents")},
        artifacts,
        "Document filenames and descriptions as offered. Source URLs and acquisition evidence remain in receipts.",
        ("id_submission", "source_ordinal"),
        rule_version=_RULE,
    ),
)
