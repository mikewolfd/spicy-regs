"""FCC navigation over native domain arrays; raw fields and parser states live in receipts."""

from .sql_views import SQLView, pin

_REQUIRED = {"fcc_filings": ("id_submission", "proceedings", "filers", "authors", "lawfirms", "bureaus", "documents")}
_RULE = "fcc-native-domain/3"
_NATIVE_COLUMNS = {
    'observation_kind': 'fcc_proceeding for the proceedings field, offered_artifact for documents, participant_role '
                        'for filers, authors, lawfirms and bureaus.',
    'participant_role': 'The native role field this participant came from (filers, authors, lawfirms, bureaus); '
                        'NULL for other fields.',
    'observed_name': "The element's name as the publisher states it; not resolved to a person or organization.",
    'native_proceeding_id': "Publisher numeric proceeding ID, which can be reused across observed names. "
                            "Match BOTH observed_name and native_proceeding_id to fcc_proceedings.name AND "
                            "id_proceeding; the numeric ID alone is not a unique proceeding key.",
    'identifier_namespace': 'fcc_ecfs_proceeding_id for a proceeding element; NULL otherwise.',
    'offered_url': 'The document src URL the publisher offers; not fetched.',
    'filename': "The publisher's filename for a document element.",
    'description': "The publisher's description of a document element.",
    'acquisition_status': 'not_checked for a document element: no URL was fetched; NULL otherwise.',
    'retained_digest': 'Always NULL: no retained bytes are recorded.',
}



def observations(p):
    parts = []
    for field in ("proceedings", "filers", "authors", "lawfirms", "bureaus", "documents"):
        role = f"'{field}'" if field in ("filers", "authors", "lawfirms", "bureaus") else "NULL::VARCHAR"
        name = "v.name" if field != "documents" else "NULL::VARCHAR"
        native_id = "v.id_proceeding" if field == "proceedings" else "NULL::VARCHAR"
        filename = "v.filename" if field == "documents" else "NULL::VARCHAR"
        description = "v.description" if field in ("proceedings", "documents") else "NULL::VARCHAR"
        offered_url = "v.src" if field == "documents" else "NULL::VARCHAR"
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
            {description} AS description, {offered_url} AS offered_url
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
    return """SELECT s.id_submission, ordinality - 1 AS source_ordinal, v.filename, v.description,
        v.src AS offered_url, 'not_checked' AS acquisition_status
        FROM fcc_filings s, UNNEST(s.documents) WITH ORDINALITY AS entries(v, ordinality)"""


def extraction_results(p):
    return """SELECT s.generation_id, s.id_submission, ordinality - 1 AS attempt_ordinal,
        offered_ordinal, s.documents[offered_ordinal + 1].filename AS filename,
        s.documents[offered_ordinal + 1].description AS description,
        result.url, result.url_status, result.source_sha256, result.digest_status,
        result.status AS extraction_status, result.page_count, result.error,
        result.capture_status, result.text_access_status
        FROM fcc_filings s,
        UNNEST(s.extraction_results) WITH ORDINALITY AS attempts(result, ordinality)
        LEFT JOIN UNNEST(result.offered_ordinals) AS offered(offered_ordinal) ON TRUE"""


FCC_NATIVE_VIEWS = (
    SQLView(
        "fcc_native_observations",
        _REQUIRED,
        observations,
        "FCC source roles and document metadata in native array order, including repeats and null elements. "
        "Names do not resolve people. Each filing's page address on fcc.gov is kept in the filing's receipt; "
        "see etl_receipts.",
        ("id_submission", "source_field", "source_ordinal"),
        rule_version=_RULE, column_descriptions=_NATIVE_COLUMNS,
    ),
    SQLView(
        "fcc_document_extraction_results",
        {"fcc_filings": ("generation_id", "id_submission", "documents", "extraction_results")},
        extraction_results,
        "Latest retained extraction observations. Each matching offered URL position remains visible; unmatched "
        "results have no offered ordinal. Attempt ordinal is the retained array position, not a lifetime count. "
        "Recorded status and digest do not establish captured-byte custody or accessible extracted text.",
        ("generation_id", "id_submission", "attempt_ordinal", "offered_ordinal"),
        rule_version=_RULE,
        column_descriptions={
            "generation_id": "Selected main filing generation; not the original acquisition time.",
            "attempt_ordinal": "Zero-based position in the retained latest result array.",
            "offered_ordinal": "Zero-based document position whose exact src matches the result URL, or NULL.",
            "extraction_status": "Recorded outcome, unchanged from the selected producer input.",
            "capture_status": "Unverified when a digest is recorded; otherwise not_recorded.",
            "text_access_status": "Unverified: these observations contain no qualified public text witness.",
        },
    ),
    SQLView(
        "fcc_native_proceeding_links",
        {**_REQUIRED, "fcc_proceedings": ("name", "id_proceeding")},
        proceeding_links,
        "Matches both the stated name and FCC numeric proceeding ID. Duplicate target identities remain ambiguous.",
        ("id_submission", "source_field", "source_ordinal"),
        rule_version=_RULE, column_descriptions=_NATIVE_COLUMNS,
    ),
    SQLView(
        "fcc_filing_proceedings_occurrences",
        {"fcc_filings": ("id_submission", "proceedings")},
        memberships,
        "Each proceeding occurrence as named by the filing; no deduplication or inferred namespace.",
        ("id_submission", "source_ordinal"),
        rule_version=_RULE, column_descriptions=_NATIVE_COLUMNS,
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
        rule_version=_RULE, column_descriptions=_NATIVE_COLUMNS,
    ),
    SQLView(
        "fcc_filing_artifacts",
        {"fcc_filings": ("id_submission", "documents")},
        artifacts,
        "Each offered document's URL, filename and description in source order, including repeated and null elements. "
        "An offered URL does not establish capture or extraction.",
        ("id_submission", "source_ordinal"),
        rule_version=_RULE, column_descriptions=_NATIVE_COLUMNS,
    ),
)
