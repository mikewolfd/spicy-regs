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


#: A derived count leaves out a posting Regulations.gov removed, and the comments held on it (removed_postings).
_COUNTED = "publisher_status IS DISTINCT FROM 'removed'"


_TRIMMED_DOCKET = """trim(docket_id,'"') docket_id"""


def _removed_comments(key: str) -> str:
    """``gone``: every held comment naming a removed posting, counted by ``key`` as the comments index spells it."""
    return f"""gone AS (SELECT {key},count(*) n FROM comments WHERE comment_on_document_id IN
        (SELECT document_id FROM documents WHERE publisher_status='removed') GROUP BY ALL)"""


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
    Aggregate(
        "agency-docket",
        "agency_stats",
        ("dockets",),
        ("agency_code",),
        "SELECT agency_code,count(*) AS value FROM dockets WHERE agency_code IS NOT NULL GROUP BY ALL",
        "SELECT agency_code,sum(docket_count) AS value FROM agency_stats GROUP BY ALL",
        "All held rows with non-NULL agency; zero-valued agency cells may originate in another input.",
    ),
    Aggregate(
        "agency-document",
        "agency_stats",
        ("documents",),
        ("agency_code",),
        f"SELECT agency_code,count(*) AS value FROM documents WHERE agency_code IS NOT NULL AND {_COUNTED} GROUP BY ALL",
        "SELECT agency_code,sum(document_count) AS value FROM agency_stats GROUP BY ALL",
        "Held rows with non-NULL agency, less postings Regulations.gov removed; zero-valued agency cells may "
        "originate in another input.",
    ),
    Aggregate(
        "agency-comment",
        "agency_stats",
        ("comments_index", "comments", "documents"),
        ("agency_code",),
        f"""WITH {_removed_comments("agency_code")},
        held AS (SELECT agency_code,sum(row_count) n FROM comments_index GROUP BY ALL)
        SELECT agency_code,held.n-coalesce(gone.n,0) AS value FROM held LEFT JOIN gone USING (agency_code)
        WHERE agency_code IS NOT NULL""",
        "SELECT agency_code,sum(comment_count) AS value FROM agency_stats GROUP BY ALL",
        "Indexed comments with non-NULL agency, less those naming a posting Regulations.gov removed, matched over "
        "every comment row; zero-valued agency cells may originate in another input.",
    ),
    Aggregate(
        "monthly-documents",
        "agency_monthly_volume",
        ("documents",),
        ("agency_code", "year", "month", "document_type"),
        f"""SELECT agency_code,extract(year FROM try_cast(posted_date AS DATE)) AS year,
        extract(month FROM try_cast(posted_date AS DATE)) AS month,document_type,count(*) AS value
        FROM documents WHERE try_cast(posted_date AS DATE) IS NOT NULL AND {_COUNTED}
        AND extract(year FROM try_cast(posted_date AS DATE))<>0 GROUP BY ALL""",
        "SELECT agency_code,year,month,document_type,sum(document_count) AS value FROM agency_monthly_volume GROUP BY ALL",
        "Usable posted dates only; NULL, unparseable and year-zero dates and removed postings omitted; NULL "
        "agency/type retained.",
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
        ("dockets", "comments_index", "comments", "documents"),
        ("docket_id",),
        f"""WITH {_removed_comments(_TRIMMED_DOCKET)},
        held AS (SELECT docket_id,sum(row_count) n FROM comments_index GROUP BY ALL)
        SELECT d.docket_id,sum(coalesce(c.n,0)-coalesce(gone.n,0)) AS value FROM
        (SELECT trim(docket_id,'"') docket_id FROM dockets) d LEFT JOIN held c ON d.docket_id=c.docket_id
        LEFT JOIN gone ON d.docket_id=gone.docket_id GROUP BY d.docket_id""",
        "SELECT docket_id,sum(comment_count) AS value FROM feed_summary GROUP BY ALL",
        "Comments on held docket rows only, less those naming a posting Regulations.gov removed; SQL NULL dockets "
        "do not match; repeated dockets multiply as builder does.",
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
    output = pins[check.output]
    if output.get("kind") == "comments-mirror":
        if (check.name != "comments-index" or not root.get("source")
                or any(pins[t].get("kind") != "comments-mirror" for t in selected)
                or any(pins[t].get("receipt_sha256") != output["receipt_sha256"] for t in selected)
                or any(pins[t]["sha256"] != "sha256:" + root["files"][t + ".parquet"]["sha256"] for t in selected)):
            return "INCOMPARABLE", "Comment files do not share the same catalog export receipt."
        return "COMPARABLE", "Both public file versions match one catalog snapshot export receipt."
    if output.get("kind") == "materialized":
        if any(pins[t].get("snapshot_id") != root.get("snapshot_id") for t in selected):
            return "INCOMPARABLE", "Materialized tables belong to different snapshots."
        for table in selected:
            record = root.get("artifacts", {}).get(table + ".parquet", {})
            if record.get("visibility") != "public" or pins[table]["sha256"] != "sha256:" + record.get("sha256", ""):
                return "INCOMPARABLE", "Materialized table differs from its public manifest record."
        stages = {stage["name"]: stage for stage in root.get("stages", [])}
        builders = {key: name for name, stage in stages.items() for key in stage["outputs"]}
        output_stage = builders.get(check.output + ".parquet")
        dependencies = stages.get(output_stage, {}).get("depends_on", [])
        if not output_stage or any(builders.get(t + ".parquet") not in dependencies for t in check.inputs):
            return "INCOMPARABLE", "The manifest does not declare the selected input stages as output dependencies."
        return "COMPARABLE", "Public artifacts share one materialized snapshot and declared build dependency."
    spec = root.get("spec", {})
    if root.get("artifactDigest") != pins[check.output]["artifactDigest"]:
        return "INCOMPARABLE", "Output root differs from selected output generation."
    if spec.get("publicationStatus") != "complete-family":
        return "INCOMPARABLE", "Output is not an admitted complete-family publication."
    carried = spec.get("carriedForward", {})
    for table in check.inputs:
        key = table + ".parquet"
        parent = spec.get("parents", {}).get(key, {})
        if parent.get("sha256") == pins[table]["sha256"] or (
            pins[table].get("kind") == "comments-mirror" and parent.get("etag") == pins[table].get("etag")
            and parent.get("byteSize") == pins[table].get("bytes")
        ):
            continue
        if (
            pins[table].get("artifactDigest") == pins[check.output]["artifactDigest"]
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
    missing = tables - pins.keys()
    exports = sorted(missing & set(publication.COMMENTS_EXPORT_TABLES))
    if exports:
        comments = publication.load_comments_publication(base)
        if comments is not None:
            roots["comments-mirror"] = comments["receipt"]
            pins.update(publication.comments_export_pins(base, comments, exports))
    missing = tables - pins.keys()
    if missing:
        snapshot = publication.load_rulemaking_snapshot(base)
        if snapshot is not None:
            roots["materialized-rulemaking"] = snapshot["manifest"]
            for table in missing:
                record = snapshot["tables"].get(table + ".parquet")
                if record is not None:
                    pins[table] = {**record, "sha256": "sha256:" + record["sha256"], "kind": "materialized",
                                   "family": "materialized-rulemaking", "snapshot_id": snapshot["snapshot_id"],
                                   "urls": [base.rstrip("/") + "/" + record["remote_key"]]}
    results = []
    for name in names:
        check = BY_NAME[name]
        selected = {table: pins.get(table) for table in (check.output, *check.inputs)}
        root = roots.get((pins.get(check.output) or {}).get("family"), {})
        versions_match = publication.mutable_versions_match(selected)
        result = measure(con, check, pins=selected, root=root, timeout_seconds=timeout_seconds,
                         read_status="full_selected_inputs" if versions_match else "moved_public_version",
                         bind_pinned_views=True)
        if versions_match and not publication.mutable_versions_match(selected):
            result.update(status="NOT_MEASURED", read_status="moved_public_version",
                          reason="A fixed public comment file changed during the measurement; rerun from its new receipt.")
            for key in ("expected_cells", "observed_cells", "expected_total", "observed_total", "mismatched_cells"):
                result.pop(key, None)
        results.append(result)
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
