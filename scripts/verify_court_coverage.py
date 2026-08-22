"""Measure court-data coverage against the publisher's own enumeration.

Every number in ``docs/evidence/court-data-coverage.md`` comes from here. The
point is that a coverage claim is only worth what its denominator is worth, so
each check names where its denominator came from:

* **Bulk datasets** — the CourtListener bucket's S3 listing, which is the
  publisher's statement of what exists. Sizes are exact bytes from that listing.
* **APA dockets** — the published ``court_dockets`` table, which is itself the
  full result of a ``nature_of_suit=899`` search.
* **Supreme Court opinions** — the Court's own term index pages.

Run with ``--offline`` to skip the two network denominators and report only what
local artifacts can support.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from spicy_regs.sources.courtlistener_bulk import list_bulk_dumps
from spicy_regs.sources.supreme_court_opinions import TERM_INDEX_URL, parse_term_index, term_code

DATASETS_OF_INTEREST = (
    "opinions",
    "opinion-clusters",
    "dockets",
    "courts",
    "citations",
    "citation-map",
    "parentheticals",
)


def _rows(path: Path) -> int:
    return duckdb.sql(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0]


def bulk_enumeration() -> dict:
    """What the publisher says exists, with exact sizes."""
    objects = list_bulk_dumps()
    latest: dict[str, dict] = {}
    for obj in objects:
        if obj.dataset in DATASETS_OF_INTEREST and obj.dump_date:
            current = latest.get(obj.dataset)
            if current is None or obj.dump_date.isoformat() > current["dump_date"]:
                latest[obj.dataset] = {
                    "dump_date": obj.dump_date.isoformat(),
                    "bytes": obj.size,
                    "gib": round(obj.size / 2**30, 3),
                }
    return {
        "listing_object_count": len(objects),
        "listing_byte_total": sum(o.size for o in objects),
        "latest_dumps": dict(sorted(latest.items())),
    }


def cluster_coverage(clusters: Path, dockets: Path) -> dict:
    """How much of the APA docket set the ingested decisions actually reach."""
    con = duckdb.connect()
    total_clusters = _rows(clusters)
    total_dockets = _rows(dockets)
    matched_dockets = con.execute(
        f"""
        SELECT count(DISTINCT d.cl_docket_id)
        FROM read_parquet('{dockets}') d
        JOIN read_parquet('{clusters}') c ON c.cl_docket_id = d.cl_docket_id
        """
    ).fetchone()[0]
    matched_clusters = con.execute(
        f"""
        SELECT count(*)
        FROM read_parquet('{clusters}') c
        JOIN read_parquet('{dockets}') d ON c.cl_docket_id = d.cl_docket_id
        """
    ).fetchone()[0]
    with_docket = con.execute(
        f"SELECT count(*) FROM read_parquet('{clusters}') WHERE cl_docket_id IS NOT NULL"
    ).fetchone()[0]
    con.close()
    return {
        "clusters_ingested": total_clusters,
        "clusters_with_a_docket_id": with_docket,
        "apa_dockets_published": total_dockets,
        "apa_dockets_with_at_least_one_decision": matched_dockets,
        "apa_dockets_with_no_decision": total_dockets - matched_dockets,
        "clusters_on_apa_dockets": matched_clusters,
    }


def body_coverage(bodies: Path, clusters: Path) -> dict:
    """What the bounded opinion-text slice covers, and what it leaves out."""
    con = duckdb.connect()
    total = _rows(bodies)
    stats = con.execute(
        f"""
        SELECT
            count(*) FILTER (WHERE available_text_fields IS NOT NULL),
            count(*) FILTER (WHERE plain_text IS NOT NULL),
            count(*) FILTER (WHERE html_with_citations IS NOT NULL),
            min(CAST(opinion_id AS BIGINT)),
            max(CAST(opinion_id AS BIGINT))
        FROM read_parquet('{bodies}')
        """
    ).fetchone()
    joined = con.execute(
        f"""
        SELECT count(DISTINCT b.cluster_id)
        FROM read_parquet('{bodies}') b
        JOIN read_parquet('{clusters}') c ON c.cluster_id = b.cluster_id
        """
    ).fetchone()[0]
    con.close()
    return {
        "opinions_ingested": total,
        "opinions_with_any_text": stats[0],
        "opinions_with_plain_text": stats[1],
        "opinions_with_html_with_citations": stats[2],
        "opinion_id_min": stats[3],
        "opinion_id_max": stats[4],
        "clusters_resolved_for_ingested_opinions": joined,
    }


def scotus_coverage(opinions: Path | None, terms: tuple[int, ...]) -> dict:
    """The Court's term index is the denominator for its own opinions."""
    import httpx

    counts: dict[str, int] = {}
    with httpx.Client(
        timeout=60.0,
        follow_redirects=True,
        headers={"User-Agent": "spicy-regs/0.1 court-coverage-check"},
    ) as client:
        for year in terms:
            url = TERM_INDEX_URL.format(term_code=term_code(year))
            response = client.get(url)
            response.raise_for_status()
            counts[str(year)] = len(parse_term_index(response.text, term_year=year))
    published = _rows(opinions) if opinions and opinions.exists() else None
    return {
        "term_index_counts": counts,
        "term_index_total": sum(counts.values()),
        "court_opinions_rows": published,
        "court_opinions_published": published is not None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("output"))
    parser.add_argument("--dockets", type=Path, required=True)
    parser.add_argument("--clusters", type=Path, required=True)
    parser.add_argument("--bodies", type=Path)
    parser.add_argument("--scotus", type=Path)
    parser.add_argument("--terms", type=int, nargs="*", default=[2022, 2023, 2024, 2025])
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()

    report: dict = {}
    if not args.offline:
        report["bulk_enumeration"] = bulk_enumeration()
    report["clusters"] = cluster_coverage(args.clusters, args.dockets)
    if args.bodies and args.bodies.exists():
        report["bodies"] = body_coverage(args.bodies, args.clusters)
    if not args.offline:
        report["supreme_court"] = scotus_coverage(args.scotus, tuple(args.terms))
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
