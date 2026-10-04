"""Enrich native court opinion clusters with inline court scope and matching receipts.

The existing docket map supplies court identity and jurisdiction. The disk floor
is checked before rewriting the complete subject and receipt pair.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms._courtlistener_writer import check_headroom, disk_floor
from spicy_regs.transforms.court_scope import CourtScope, court_jurisdictions
from spicy_regs.court_receipts import finish_court_output, file_witness, restore_processing_input, prior_receipt_selection
from spicy_regs.transforms.build_court_opinion_clusters import _SCHEMA as INPUT_SCHEMA

BATCH_ROWS = 25_000


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def backfill(
    *,
    clusters: Path,
    docket_court_map: Path,
    courts_dump: Path,
    output_dir: Path,
    dump_date: date,
) -> dict:
    """Write the scoped table and return its receipt."""
    output_dir.mkdir(parents=True, exist_ok=True)
    check_headroom(clusters.stat().st_size * 3, path=output_dir)
    original_clusters = clusters
    receipt_path, generation_id = prior_receipt_selection(clusters, dataset='court_opinion_clusters')
    clusters = restore_processing_input(clusters, output_dir / f'.clusters-input-{uuid4().hex}.parquet',
        dataset='court_opinion_clusters', schema=INPUT_SCHEMA, receipt_path=receipt_path,
        generation_id=generation_id)
    source = pq.ParquetFile(clusters)
    total_rows = source.metadata.num_rows

    check_headroom(clusters.stat().st_size, path=output_dir)
    logger.info("Court scope backfill: {:,} clusters (floor {:.0f} GiB)", total_rows, disk_floor() / 2**30)
    scope = CourtScope.from_map(docket_court_map, court_jurisdictions(local_file=courts_dump))
    out_schema = INPUT_SCHEMA
    out_file = output_dir / "court_opinion_clusters.parquet"
    staging = out_file.with_suffix(".partial.parquet")
    writer = pq.ParquetWriter(staging, out_schema, compression="zstd")
    jurisdictions: Counter[str] = Counter()
    federal = unknown = 0
    written = 0
    try:
        for batch in source.iter_batches(batch_size=BATCH_ROWS):
            rows = batch.to_pylist()
            shaped = []
            for row in rows:
                court_id, jurisdiction, is_fed = scope.for_docket(row.get("cl_docket_id"))
                jurisdictions[jurisdiction or "<unknown>"] += 1
                if is_fed == "t":
                    federal += 1
                elif is_fed is None:
                    unknown += 1
                enriched = {
                    "court_id": court_id,
                    "court_jurisdiction": jurisdiction,
                    "court_is_federal": is_fed,
                }
                shaped.append({**row, **enriched})
            writer.write_table(pa.Table.from_pylist(shaped, schema=out_schema))
            written += len(shaped)
            if written % (BATCH_ROWS * 8) == 0:
                logger.info("Court scope backfill: {:,} / {:,} rows", written, total_rows)
    finally:
        writer.close()
    # Keep the completed mapper input and bind the rewritten subject to new receipts.
    retained = output_dir / f'.clusters-enriched-source-{uuid4().hex}.parquet'
    staging.replace(retained)
    out_file = finish_court_output('court_opinion_clusters', retained, output_dir,
        witnesses=[file_witness(original_clusters), file_witness(docket_court_map),
                   file_witness(courts_dump), file_witness(retained)], prior_receipts=receipt_path)

    receipt = {
        "artifact": out_file.name,
        "written_at": datetime.now(UTC).isoformat(),
        "inputs": {
            "clusters": {"path": str(original_clusters), "sha256": _sha256(original_clusters)},
            "docket_court_map": {
                "path": str(docket_court_map),
                "sha256": _sha256(docket_court_map),
                "dockets": pq.ParquetFile(docket_court_map).metadata.num_rows,
                "dump_date": dump_date.isoformat(),
            },
            "courts_dump": {
                "path": str(courts_dump),
                "sha256": _sha256(courts_dump),
            },
        },
        "coverage": {
            "denominator": "clusters in court_opinion_clusters",
            "denominator_count": total_rows,
            "rows_written": written,
            "clusters_placed_in_a_court": total_rows - unknown,
            "clusters_with_no_court": unknown,
            "clusters_in_a_federal_court": federal,
            "federal_share": round(federal / total_rows, 6) if total_rows else None,
            "by_jurisdiction": dict(jurisdictions.most_common()),
        },
    }
    receipt_path = output_dir / "cluster_court_scope_receipt.json"
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n")
    logger.info("Receipt written to {}", receipt_path)
    return receipt


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--clusters", type=Path, required=True)
    parser.add_argument("--docket-court-map", type=Path, required=True)
    parser.add_argument("--courts-dump", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--dump-date", type=date.fromisoformat, default=date(2026, 6, 30))
    options = parser.parse_args()
    print(
        json.dumps(
            backfill(
                clusters=options.clusters,
                docket_court_map=options.docket_court_map,
                courts_dump=options.courts_dump,
                output_dir=options.output_dir,
                dump_date=options.dump_date,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
