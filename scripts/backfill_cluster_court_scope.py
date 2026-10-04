"""Enrich retained court opinion clusters with court scope and matching receipts.

The existing docket map supplies court identity and jurisdiction. Scope stays
inline on the cluster subject. The historical separate scope-table mode now
refuses; existing files remain untouched. The disk floor is checked before the
rewrite, and an unreceipted legacy input requires explicit migration selection.
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

SCOPE_COLUMNS = (
    "cluster_id",
    "cl_docket_id",
    "court_id",
    "court_jurisdiction",
    "court_is_federal",
)
_SCOPE_SCHEMA = pa.schema([(c, pa.string()) for c in SCOPE_COLUMNS])
BATCH_ROWS = 250_000


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _fits(needed: int, path: Path) -> bool:
    try:
        check_headroom(needed, path=path)
    except RuntimeError as exc:
        logger.warning("Court scope backfill: {}", exc)
        return False
    return True


def backfill(
    *,
    clusters: Path,
    docket_court_map: Path,
    courts_dump: Path,
    output_dir: Path,
    dump_date: date,
    mode: str = "auto",
    allow_legacy_input: bool = False,
) -> dict:
    """Write the scoped table and return its receipt."""
    output_dir.mkdir(parents=True, exist_ok=True)
    if mode == 'scope':
        raise ValueError('Separate court_cluster_scope output is retired; enrich cluster columns with receipts')
    if mode not in ('auto', 'full'):
        raise ValueError('Unsupported court scope mode')
    check_headroom(clusters.stat().st_size * 3, path=output_dir)
    original_clusters = clusters
    receipt_path, generation_id = prior_receipt_selection(clusters, dataset='court_opinion_clusters')
    clusters = restore_processing_input(clusters, output_dir / f'.clusters-input-{uuid4().hex}.parquet',
        dataset='court_opinion_clusters', schema=INPUT_SCHEMA, receipt_path=receipt_path,
        generation_id=generation_id, allow_legacy=allow_legacy_input)
    source = pq.ParquetFile(clusters)
    total_rows = source.metadata.num_rows

    if mode == "auto":
        check_headroom(clusters.stat().st_size, path=output_dir)
        mode = 'full'
    elif mode == "full":
        check_headroom(clusters.stat().st_size, path=output_dir)
    logger.info(
        "Court scope backfill: {:,} clusters, mode={} (floor {:.0f} GiB)",
        total_rows,
        mode,
        disk_floor() / 2**30,
    )

    args = argparse.Namespace(
        clusters=clusters,
        docket_court_map=docket_court_map,
        courts_dump=courts_dump,
        output_dir=output_dir,
        dump_date=dump_date,
    )
    scope = CourtScope.from_map(args.docket_court_map, court_jurisdictions(local_file=args.courts_dump))

    existing = [field.name for field in source.schema_arrow]
    if mode == "full":
        out_columns = list(existing)
        # Keep the published order: the scope sits next to the join key it is
        # derived from, not bolted onto the end. Inserted as one slice — three
        # separate inserts at the same position reverse them.
        missing = [
            column for column in ("court_id", "court_jurisdiction", "court_is_federal") if column not in out_columns
        ]
        at = out_columns.index("cl_docket_id") + 1
        out_columns[at:at] = missing
        out_schema = pa.schema([source.schema_arrow.field(c) if c in existing else pa.field(c, pa.string())
                                for c in out_columns])
        out_file = args.output_dir / "court_opinion_clusters.parquet"
    else:
        out_columns = list(SCOPE_COLUMNS)
        out_schema = _SCOPE_SCHEMA
        out_file = args.output_dir / "court_cluster_scope.parquet"

    staging = out_file.with_suffix(".partial.parquet")
    writer = pq.ParquetWriter(staging, out_schema, compression="zstd")
    jurisdictions: Counter[str] = Counter()
    federal = unknown = 0
    written = 0
    # Scope mode needs two columns out of thirty-six, and the thirty-four it
    # does not need include syllabus, headmatter and summary — kilobytes of
    # prose per row. Materializing those as Python objects, 250,000 rows at a
    # time, is gigabytes of memory to answer a question about docket ids.
    # Full mode has to carry every column through, so its batches are a tenth
    # the size for the same peak memory.
    full = mode == "full"
    read_columns = None if full else list(SCOPE_COLUMNS[:2])
    batch_rows = BATCH_ROWS // 10 if full else BATCH_ROWS
    try:
        for batch in source.iter_batches(batch_size=batch_rows, columns=read_columns):
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
                if mode == "full":
                    shaped.append({**row, **enriched})
                else:
                    shaped.append(
                        {
                            "cluster_id": row.get("cluster_id"),
                            "cl_docket_id": row.get("cl_docket_id"),
                            **enriched,
                        }
                    )
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
                   file_witness(courts_dump), file_witness(retained)])

    receipt = {
        "artifact": out_file.name,
        "mode": mode,
        "written_at": datetime.now(UTC).isoformat(),
        "inputs": {
            "clusters": {"path": str(args.clusters), "sha256": _sha256(args.clusters)},
            "docket_court_map": {
                "path": str(args.docket_court_map),
                "sha256": _sha256(args.docket_court_map),
                "dockets": pq.ParquetFile(args.docket_court_map).metadata.num_rows,
                "dump_date": args.dump_date.isoformat(),
            },
            "courts_dump": {
                "path": str(args.courts_dump),
                "sha256": _sha256(args.courts_dump),
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
    receipt_path = args.output_dir / "cluster_court_scope_receipt.json"
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
    parser.add_argument("--mode", choices=("auto", "scope", "full"), default="auto")
    parser.add_argument('--allow-legacy-input', action='store_true')
    options = parser.parse_args()
    print(
        json.dumps(
            backfill(
                clusters=options.clusters,
                docket_court_map=options.docket_court_map,
                courts_dump=options.courts_dump,
                output_dir=options.output_dir,
                dump_date=options.dump_date,
                mode=options.mode,
                allow_legacy_input=options.allow_legacy_input,
            ),
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
