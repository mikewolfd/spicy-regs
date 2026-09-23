"""Explicit source rereads: correct mapped fields without erasing enrichment."""

from collections.abc import Sequence

import duckdb


ENRICHMENT_COLUMNS = frozenset({"text_content", "text_extraction_status", "pdf_extraction_results_json"})


def correction_query(
    con: duckdb.DuckDBPyConnection,
    *,
    fresh_sql: str,
    prior_sql: str,
    columns: Sequence[str],
    key: str,
) -> str:
    """Validate an explicit reread and return its source-recency merge query.

    Fresh source fields, including NULL, win at an equal source instant. An
    older or undated reread cannot displace a dated newer prior. Both undated
    rows permit the explicit correction. Only independently produced text and
    extraction fields fall back to the prior, and only together: a fresh row
    with neither text nor status keeps the prior's text, status and results,
    and any other fresh row replaces all three, so text never pairs with
    another fill's status or provenance. Source facts never fall back.
    Conflicting/duplicate input identities and unorderable dates refuse before
    output replacement, so an incomplete correction remains retryable.
    """
    for name, query in (("_correction_fresh", fresh_sql), ("_correction_prior", prior_sql)):
        con.execute(f"CREATE OR REPLACE TEMP VIEW {name} AS {query}")
        bad = con.execute(f'SELECT count(*) FROM {name} WHERE "{key}" IS NULL OR trim("{key}") = \'\'').fetchone()
        duplicates = con.execute(f'SELECT count(*) - count(DISTINCT "{key}") FROM {name}').fetchone()
        if (bad and bad[0]) or (duplicates and duplicates[0]):
            raise ValueError(f"source correction requires distinct nonblank {key} in {name}")

    # Unrelated legacy rows do not have to acquire a new date spelling just to
    # survive a bounded repair. Only fresh and matched prior dates are compared.
    bad_dates = con.execute(f"""
        SELECT count(*) FROM (
            SELECT modify_date FROM _correction_fresh
            UNION ALL
            SELECT p.modify_date FROM _correction_prior p
            JOIN _correction_fresh f USING ("{key}")
        ) WHERE modify_date IS NOT NULL
          AND try_cast(modify_date AS TIMESTAMPTZ) IS NULL
    """).fetchone()
    if bad_dates and bad_dates[0]:
        raise ValueError("source correction cannot order a non-NULL modify_date")

    wins = f'''f."{key}" IS NOT NULL AND (
        p."{key}" IS NULL OR p.modify_date IS NULL OR
        try_cast(f.modify_date AS TIMESTAMPTZ) >= try_cast(p.modify_date AS TIMESTAMPTZ)
    )'''
    unfilled = 'f."text_content" IS NULL AND f."text_extraction_status" IS NULL'
    projection = []
    for column in columns:
        fresh = f'f."{column}"'
        if column in ENRICHMENT_COLUMNS:
            fresh = f'CASE WHEN {unfilled} THEN p."{column}" ELSE {fresh} END'
        projection.append(f'CASE WHEN {wins} THEN {fresh} ELSE p."{column}" END AS "{column}"')
    return f'''SELECT {", ".join(projection)} FROM _correction_fresh f
               FULL OUTER JOIN _correction_prior p ON f."{key}" = p."{key}"'''
