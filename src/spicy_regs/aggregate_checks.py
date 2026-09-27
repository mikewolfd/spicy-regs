"""Scoped aggregate measurements for the existing join checker.

A differing number is a defect only when the selected rollup declares the
selected input bytes. Receipts retain independent pins and exact SQL; a full
file read describes those files, never publisher archive completeness.
"""

from dataclasses import dataclass
from threading import Timer
from time import monotonic

import duckdb

from spicy_regs.sources import publication


@dataclass(frozen=True)
class Aggregate:
    name: str
    output: str
    inputs: tuple[str, ...]
    grain: tuple[str, ...]
    expected: str
    observed: str
    population: str


AGGREGATES = (
    Aggregate(
        "comments-index",
        "comments_index",
        ("comments",),
        ("agency_code", "docket_id", "year", "month"),
        """SELECT agency_code,trim(docket_id,'"') docket_id,
        extract(year FROM CAST(posted_date AS TIMESTAMP)) AS year,
        extract(month FROM CAST(posted_date AS TIMESTAMP)) AS month,count(*) AS value
        FROM comments GROUP BY ALL""",
        "SELECT agency_code,docket_id,year,month,sum(row_count) AS value FROM comments_index GROUP BY ALL",
        "All held comment rows, including NULL cohort keys; strict timestamp conversion as index builder.",
    ),
    *(
        Aggregate(
            "agency-" + metric,
            "agency_stats",
            (source,),
            ("agency_code",),
            f"SELECT agency_code,{expression} AS value FROM {source} WHERE agency_code IS NOT NULL GROUP BY ALL",
            f"SELECT agency_code,sum({metric}_count) AS value FROM agency_stats GROUP BY ALL",
            "All held rows with non-NULL agency; zero-valued agency cells may originate in another input.",
        )
        for metric, source, expression in [
            ("docket", "dockets", "count(*)"),
            ("document", "documents", "count(*)"),
            ("comment", "comments_index", "sum(row_count)"),
        ]
    ),
    Aggregate(
        "monthly-documents",
        "agency_monthly_volume",
        ("documents",),
        ("agency_code", "year", "month", "document_type"),
        """SELECT agency_code,extract(year FROM try_cast(posted_date AS DATE)) AS year,
        extract(month FROM try_cast(posted_date AS DATE)) AS month,document_type,count(*) AS value
        FROM documents WHERE try_cast(posted_date AS DATE) IS NOT NULL
        AND extract(year FROM try_cast(posted_date AS DATE))<>0 GROUP BY ALL""",
        "SELECT agency_code,year,month,document_type,sum(document_count) AS value FROM agency_monthly_volume GROUP BY ALL",
        "Usable posted dates only; NULL, unparseable and year-zero dates omitted; NULL agency/type retained.",
    ),
    Aggregate(
        "feed-dockets",
        "feed_summary",
        ("dockets",),
        ("docket_id",),
        """SELECT trim(docket_id,'"') docket_id,count(*) AS value FROM dockets GROUP BY ALL""",
        "SELECT docket_id,count(*) AS value FROM feed_summary GROUP BY ALL",
        "One feed row per held docket row, including NULL docket identities.",
    ),
    Aggregate(
        "feed-comments",
        "feed_summary",
        ("dockets", "comments_index"),
        ("docket_id",),
        """SELECT d.docket_id,sum(coalesce(c.n,0)) AS value FROM
        (SELECT trim(docket_id,'"') docket_id FROM dockets) d LEFT JOIN
        (SELECT docket_id,sum(row_count) n FROM comments_index GROUP BY ALL) c
        ON d.docket_id=c.docket_id GROUP BY d.docket_id""",
        "SELECT docket_id,sum(comment_count) AS value FROM feed_summary GROUP BY ALL",
        "Comments on held docket rows only; SQL NULL dockets do not match; repeated dockets multiply as builder does.",
    ),
    Aggregate(
        "lifecycle-outcomes",
        "agency_lifecycle_stats",
        ("rulemaking_lifecycles",),
        ("agency_code", "censor_date", "outcome"),
        """WITH source AS (SELECT agency_code,censor_date,outcome FROM rulemaking_lifecycles WHERE outcome IS NOT NULL),
        expanded AS (SELECT * FROM source WHERE agency_code IS NOT NULL UNION ALL
        SELECT NULL agency_code,censor_date,outcome FROM source)
        SELECT agency_code,censor_date,outcome,count(*) AS value FROM expanded GROUP BY ALL""",
        """SELECT agency_code,censor_date,outcome,sum(value) AS value FROM
        (SELECT agency_code,censor_date,'final' outcome,finals AS value FROM agency_lifecycle_stats WHERE stratum='all'
        UNION ALL SELECT agency_code,censor_date,'withdrawn',withdrawals FROM agency_lifecycle_stats WHERE stratum='all'
        UNION ALL SELECT agency_code,censor_date,'censored',censored FROM agency_lifecycle_stats WHERE stratum='all') GROUP BY ALL""",
        "All-stratum non-NULL outcomes only, plus global NULL-agency cell; separate routine strata are overlapping.",
    ),
)
BY_NAME = {check.name: check for check in AGGREGATES}


def comparison_sql(check: Aggregate) -> str:
    """Group both sides before null-aware equality; report before/after totals."""
    on = " AND ".join(f'e."{key}" IS NOT DISTINCT FROM o."{key}"' for key in check.grain)
    return f"""WITH e AS ({check.expected}),o AS ({check.observed}), j AS (
        SELECT coalesce(e.value,0) expected,coalesce(o.value,0) observed
        FROM e FULL OUTER JOIN o ON {on})
        SELECT (SELECT count(*) FROM e) expected_cells,(SELECT count(*) FROM o) observed_cells,
        coalesce((SELECT sum(value) FROM e),0) expected_total,
        coalesce((SELECT sum(value) FROM o),0) observed_total,
        count(*) FILTER (WHERE expected<>observed) mismatched_cells FROM j"""


def qualify(check: Aggregate, pins: dict, root: dict) -> tuple[str, str]:
    """Only verified roots plus matching member digests establish comparable scope."""
    selected = (check.output, *check.inputs)
    if any(not pins.get(table) for table in selected):
        return "UNPINNED", "A selected table lacks a supported managed-index pin; materialized/catalog lineage is not inferred from a URL."
    spec = root.get("spec", {})
    if root.get("artifactDigest") != pins[check.output]["artifactDigest"]:
        return "INCOMPARABLE", "Output root differs from selected output generation."
    if spec.get("publicationStatus") != "complete-family":
        return "INCOMPARABLE", "Output is not an admitted complete-family publication."
    carried = spec.get("carriedForward", {})
    for table in check.inputs:
        key = table + ".parquet"
        parent = spec.get("parents", {}).get(key, {})
        if parent.get("sha256") == pins[table]["sha256"]:
            continue
        if (
            pins[table]["artifactDigest"] == pins[check.output]["artifactDigest"]
            and key not in carried
            and check.output + ".parquet" not in carried
        ):
            continue
        return "INCOMPARABLE", f"{table}: selected input differs from declared parent or lineage is absent."
    return "COMPARABLE", "Selected immutable input bytes match output parent lineage or freshly co-built family."


def measure(
    con,
    check: Aggregate,
    *,
    pins: dict,
    root: dict,
    timeout_seconds: float = 90,
    read_status: str = "full_selected_inputs",
    bind_pinned_views: bool = False,
) -> dict:
    """Measure already-bound pinned views; unsupported/partial scopes do not scan."""
    if not 0 < timeout_seconds <= 90:
        raise ValueError("timeout_seconds must be in (0,90]")
    status, reason = qualify(check, pins, root)
    result: dict = {
        "check": check.name,
        "kind": "aggregate",
        "status": status,
        "reason": reason,
        "grain": list(check.grain),
        "population": check.population,
        "read_status": read_status,
        "publication_pins": pins,
        "sql": comparison_sql(check),
        "coverage": "Selected files only; no source archive, acquisition, or parsing completeness claim.",
    }
    if read_status != "full_selected_inputs":
        result.update(
            status="NOT_MEASURED",
            reason="Failed, capped, acquired-only or parsed-only inputs cannot prove published totals.",
        )
        return result
    if status != "COMPARABLE":
        return result
    start = monotonic()
    timer = Timer(timeout_seconds, con.interrupt)
    timer.start()
    try:
        if bind_pinned_views:
            for table, pin in pins.items():
                assert pin is not None
                con.execute(
                    f'CREATE OR REPLACE TEMP VIEW "{table}" AS SELECT * FROM {publication.parquet_scan(pin["urls"])}'
                )
        row = con.execute(result["sql"]).fetchone()
        assert row is not None
        result.update(
            zip(
                ("expected_cells", "observed_cells", "expected_total", "observed_total", "mismatched_cells"),
                row,
                strict=True,
            )
        )
        result["status"] = "MISMATCH" if row[-1] else ("EMPTY" if not row[0] and not row[1] else "OK")
    except duckdb.InterruptException:
        result.update(status="TIMEOUT", read_status="failed")
    except duckdb.Error as error:
        result.update(status="READ_FAILURE", read_status="failed", error_type=type(error).__name__)
    finally:
        timer.cancel()
    result["elapsed_seconds"] = round(monotonic() - start, 3)
    return result


def check_public(con, base: str, names: list[str], *, timeout_seconds: float = 90) -> dict:
    """Use the join checker's connection and publication resolver, retaining exact selections."""
    if not names or len(names) != len(set(names)) or set(names) - BY_NAME.keys():
        raise ValueError("Select distinct known aggregate checks")
    index = publication.current_index(base)
    tables = {table for name in names for table in (BY_NAME[name].output, *BY_NAME[name].inputs)}
    pins, roots = {}, {}
    for table in sorted(tables):
        owner = publication.table_owner(index, table + ".parquet")
        if owner is None:
            continue
        family, entry = owner
        descriptor = entry["tables"][table + ".parquet"]
        # Split inputs require logical member-set lineage, not a fabricated file digest.
        if "sha256" not in descriptor:
            continue
        urls = [base.rstrip("/") + "/" + member.path for member in publication.table_members(index, table + ".parquet")]
        pins[table] = {
            "family": family,
            "artifactDigest": entry["artifactDigest"],
            "sha256": descriptor["sha256"],
            "urls": urls,
            "rows": descriptor["rows"],
        }
        if table in {BY_NAME[name].output for name in names} and family not in roots:
            _, roots[family] = publication.load_family_root(base, entry)
    results = []
    for name in names:
        check = BY_NAME[name]
        selected = {table: pins.get(table) for table in (check.output, *check.inputs)}
        root = roots.get((pins.get(check.output) or {}).get("family"), {})
        results.append(
            measure(con, check, pins=selected, root=root, timeout_seconds=timeout_seconds, bind_pinned_views=True)
        )
        if results[-1]["status"] in ("TIMEOUT", "READ_FAILURE"):
            break
    return {
        "base": base,
        "publication_index": index,
        "roots": roots,
        "results": results,
        "requested_checks": names,
        "remaining_checks": names[len(results) :],
    }
