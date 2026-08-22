"""Capture opinion *text* for the APA docket set specifically.

``court_opinion_bodies`` as first built is a uniform 2.44% sample of the
``opinions`` dump, and APA clusters are 0.011% of the corpus — so the sample
lands 17 opinions on an APA docket, which is the arithmetic working correctly
and is also useless for the question the court data exists to answer. See
``docs/evidence/court-data-coverage-2026-08-22.md`` gap 5.

The fix is not more sampling, it is a *targeted* pass: enumerate the clusters
that sit on an APA docket, then stream the whole dump keeping only opinions in
that set. The read is the same 50.8 GiB / ~8.6 hours either way — the dump is
not indexed and not sorted, so finding 1,155 clusters costs exactly as much as
reading everything. What changes is the output, which is megabytes.

Two bounds are worth stating before the run rather than after:

* **Disk.** The dump is streamed and never landed, so the volume pays only for
  the parquet written. ``estimate_output_bytes`` sizes that from the target
  count and ``check_headroom`` refuses the run if it does not fit.
* **The denominator.** Coverage here is a fraction of *the clusters on an APA
  docket*, not of the corpus. The receipt records the target count so the
  resulting table can never be read as APA-complete when it is not.

Inputs are digest-pinned in the receipt: the published ``court_dockets`` table
(itself the complete result of a ``nature_of_suit=899`` search) and the
``court_opinion_clusters`` table built from the 2026-06-30 dump. Change either
and the target set changes, which is the whole point of pinning them.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import UTC, date, datetime
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.build_court_opinion_bodies import (
    build_court_opinion_bodies,
    estimate_output_bytes,
)

DUMP_DATE = date(2026, 6, 30)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def apa_cluster_ids(clusters: Path, dockets: Path, *, temp_dir: Path) -> list[str]:
    """Cluster ids sitting on an APA (nature-of-suit 899) docket.

    The clusters dump has no ``court_id`` and no nature-of-suit; the APA scope
    lives entirely on the docket side, so this join *is* the definition of the
    target set.
    """
    temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{temp_dir}'")
    rows = con.execute(
        f"""
        SELECT DISTINCT c.cluster_id
        FROM read_parquet('{clusters}') c
        JOIN read_parquet('{dockets}') d ON c.cl_docket_id = d.cl_docket_id
        WHERE c.cluster_id IS NOT NULL
        ORDER BY CAST(c.cluster_id AS BIGINT)
        """
    ).fetchall()
    con.close()
    return [row[0] for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clusters", type=Path, required=True)
    parser.add_argument("--dockets", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dump-date", type=date.fromisoformat, default=DUMP_DATE)
    parser.add_argument(
        "--max-compressed-bytes",
        type=int,
        default=None,
        help="Bound the read for a rehearsal; omit for the full targeted pass.",
    )
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    targets = apa_cluster_ids(
        args.clusters, args.dockets, temp_dir=args.output_dir / ".duckdb_tmp"
    )
    logger.info("APA target set: {:,} clusters", len(targets))

    started = time.time()
    started_at = datetime.now(UTC).isoformat()
    out = build_court_opinion_bodies(
        args.output_dir,
        dump_date=args.dump_date,
        cluster_ids=set(targets),
        max_compressed_bytes=args.max_compressed_bytes,
    )
    elapsed = time.time() - started

    con = duckdb.connect()
    kept, clusters_hit, with_text = con.execute(
        f"""
        SELECT count(*), count(DISTINCT cluster_id),
               count(*) FILTER (WHERE available_text_fields IS NOT NULL)
        FROM read_parquet('{out}')
        """
    ).fetchone()
    con.close()

    receipt = {
        "artifact": out.name,
        "captured_at": started_at,
        "finished_at": datetime.now(UTC).isoformat(),
        "elapsed_seconds": round(elapsed, 1),
        "source": {
            "publisher": "CourtListener bulk data",
            "dataset": "opinions",
            "dump_date": args.dump_date.isoformat(),
            "url": (
                "https://storage.courtlistener.com/bulk-data/"
                f"opinions-{args.dump_date.isoformat()}.csv.bz2"
            ),
        },
        "inputs": {
            "clusters": {"path": str(args.clusters), "sha256": _sha256(args.clusters)},
            "dockets": {"path": str(args.dockets), "sha256": _sha256(args.dockets)},
        },
        "bounds": {
            "filter": "cluster_id in the APA (nature-of-suit 899) docket set",
            "target_clusters": len(targets),
            "max_compressed_bytes": args.max_compressed_bytes,
            "estimated_output_bytes": estimate_output_bytes(
                54_561_543_156, set(targets)
            ),
        },
        "result": {
            "rows": kept,
            "distinct_clusters": clusters_hit,
            "rows_with_any_text": with_text,
            "output_bytes": out.stat().st_size,
            "parquet_rows": pq.ParquetFile(out).metadata.num_rows,
        },
        "coverage": {
            "denominator": "clusters on an APA docket",
            "denominator_count": len(targets),
            "numerator_count": clusters_hit,
            "fraction": round(clusters_hit / len(targets), 4) if targets else None,
        },
    }
    receipt_path = args.output_dir / "apa_opinion_bodies_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    logger.info("Receipt written to {}", receipt_path)
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    main()
