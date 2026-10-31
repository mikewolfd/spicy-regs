"""CourtListener opinion bodies as Markdown: one pass over the opinions export, written id-sorted in id blocks.

The export is in no id order (a median 2,000-record stretch spans 96% of the id range;
``work/court-text-measure-20261030/extrapolate.json``), so a streaming write cannot sort it, and staging the whole
export first is the 73 GB of scratch behind decision 6. So the build has two phases:

1. Read the export once (``CourtListenerLocalDump``), convert each record in a process pool
   (``spicy_docs.sources.courtlistener.opinion_markdown.select_source``) and spill the converted rows, partitioned by
   ``opinion_id // SPILL_WIDTH``, through ``pyarrow.dataset.write_dataset`` at zstd 1. O(N) in the export.
2. Walk the spill buckets in id order: read one, sort it by numeric id, and write its subject rows and receipts in
   one pass through ``court_bulk.write_bundle`` into the member of its ``opinion_block``
   (``court_opinion_texts/opinion_block=<k>/part-000000.parquet``), at zstd 12, row groups of at most
   ``ROW_GROUP_TEXT_BYTES`` of Markdown, statistics only on the short columns. O(N log B) for bucket size B.

Peak memory is O(largest spill bucket) in phase 2; peak disk is the whole spill at the end of phase 1, shrinking as
phase 2 deletes each consumed bucket while the members grow.

Readers compute a row's member from its id: ``opinion_block = opinion_id // OPINION_BLOCK_WIDTH`` (stored as decimal
text, as ``opinion_id`` is), so a point read names one member. The width is chosen from the whole 2026-06-30 export
(``lengths-full.parquet``, scaled by the 2026-10-30 Markdown size model): 76 blocks whose largest is projected at
0.84 GB. Spill buckets are a third of a block so the heaviest one (1.99 GB of source HTML) sorts in memory.

Receipts point at the converted value and never copy it: the export's file witness, the record's id and
``source_sha256`` (of the rendering converted), with the converter's identity in the diagnostics. A rendering the
converter refuses becomes a refused receipt carrying the same pointer, never an empty text.

A run bounded by ``max_rows`` reads the export's head, writes a ``bounded`` run receipt beside its output and never
selects or admits a generation.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import resource
import shutil
import time
from collections import Counter, deque
from collections.abc import Callable, Generator, Iterator
from contextlib import closing, nullcontext
from concurrent.futures import Future, ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from spicy_regs import etl_bulk
from spicy_regs.court_receipts import POLICIES, _refusal, _write_bulk_pair, file_witness
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext
from spicy_regs.transforms.table_merge import Partitioning

DATASET = "court_opinion_texts"
#: Published: the member holding opinion ``N`` is ``opinion_block = N // OPINION_BLOCK_WIDTH``.
OPINION_BLOCK_WIDTH = 150_000
#: Ids per spill bucket, the unit sorted in memory; a divisor of the block width so buckets never straddle a member.
SPILL_WIDTH = 50_000
#: Uncompressed Markdown bytes per subject row group at most.
ROW_GROUP_TEXT_BYTES = 8 << 20
#: Subject columns with Parquet statistics; the Markdown's would only bloat the footer.
STATISTICS = ["opinion_id", "cluster_id", "date_modified", "opinion_block", "text_source", "converter_version"]
#: Export columns read: the identity, the subject's native fields and the renderings ``select_source`` orders.
SOURCE_COLUMNS = ("id", "cluster_id", "date_modified", "html_with_citations", "html", "plain_text")
PARTITIONING = Partitioning("opinion_block", "opinion_id", lambda opinion_id: str(int(opinion_id) // OPINION_BLOCK_WIDTH))
PROCESSOR = "spicy_regs/courts/opinion-texts/1"
#: Records per conversion task, so a task's HTML stays in the tens of MB.
_TASK_ROWS = 500
_BUCKET = "spill_bucket"
_SPILL_SCHEMA = pa.schema([
    *((name, pa.string()) for name in POLICIES[DATASET].subject_schema.names),
    ("source_sha256", pa.string()), ("dump_date", pa.string()), (_BUCKET, pa.int32()),
])

Converter = Callable[[dict], Any]


#: The SpicyDocs converter. The pinned reader wheel predates it, so it is imported by name and a run on that wheel
#: refuses with this module named; replace with a static import once a reader wheel carrying it is pinned.
CONVERTER_MODULE = "spicy_docs.sources.courtlistener.opinion_markdown"


def _converter():
    try:
        return importlib.import_module(CONVERTER_MODULE)
    except ModuleNotFoundError as error:
        raise RuntimeError(f"the installed SpicyDocs reader has no {CONVERTER_MODULE}") from error


@dataclass
class _Converted:
    batch: pa.RecordBatch
    refused: list[dict]
    source_bytes: int
    cpu_seconds: float
    by_source: Counter


def _convert(rows: list[dict], dump_date: str, convert: Converter | None, version: str) -> _Converted:
    """One task: each record's Markdown row, or a refusal pointer for a rendering the converter refuses."""
    started = time.process_time()
    if convert is None:
        convert = _converter().select_source
    out: dict[str, list] = {name: [] for name in _SPILL_SCHEMA.names}
    refused, source_bytes = [], 0
    for row in rows:
        opinion_id = row["id"]
        try:
            text = convert(row)
        except ValueError as error:  # OpinionMarkdownError is a ValueError
            column = next((c for c in ("html_with_citations", "html", "plain_text") if (row.get(c) or "").strip()), None)
            value = (row.get(column) or "").encode() if column else b""
            refused.append({"opinion_id": opinion_id, "cluster_id": row["cluster_id"], "text_source": column,
                            "source_sha256": "sha256:" + hashlib.sha256(value).hexdigest(), "error": error})
            continue
        value = (row.get(text.text_source) or "").encode() if text.text_source != "none" else b""
        source_bytes += len(value)
        out["opinion_id"].append(opinion_id)
        out["cluster_id"].append(row["cluster_id"])
        out["date_modified"].append(row["date_modified"])
        out["text_source"].append(text.text_source)
        out["markdown"].append(text.markdown if text.text_source != "none" else None)
        out["converter_version"].append(version)
        out["opinion_block"].append(PARTITIONING.value(opinion_id))
        out["source_sha256"].append("sha256:" + hashlib.sha256(value).hexdigest() if value else None)
        out["dump_date"].append(dump_date)
        out[_BUCKET].append(int(opinion_id) // SPILL_WIDTH)
    batch = pa.RecordBatch.from_pydict(out, schema=_SPILL_SCHEMA)
    return _Converted(batch, refused, source_bytes, time.process_time() - started, Counter(out["text_source"]))


def byte_segments(limit: int = ROW_GROUP_TEXT_BYTES) -> Callable[[pa.Table], Iterator[pa.Table]]:
    """Cut a sorted batch into consecutive slices of at most ``limit`` Markdown bytes (one row may exceed it)."""

    def segments(table: pa.Table) -> Iterator[pa.Table]:
        sizes = pc.fill_null(pc.call_function("binary_length", [table["markdown"].cast(pa.binary())]), 0).to_pylist()
        start, total = 0, 0
        for index, size in enumerate(sizes):
            if index > start and total + size > limit:
                yield table.slice(start, index - start)
                start, total = index, 0
            total += size
        if start < table.num_rows:
            yield table.slice(start, table.num_rows - start)

    return segments


def _check_open_files(buckets: int) -> int:
    soft, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft < 64:
        raise RuntimeError(f"open-file limit {soft} is too low for the spill writer")
    return min(buckets, max(32, soft // 4))


def _member_writer(path: Path) -> pq.ParquetWriter:
    path.parent.mkdir(parents=True, exist_ok=True)
    return pq.ParquetWriter(path, POLICIES[DATASET].subject_schema, compression="zstd", compression_level=12,
                            write_statistics=STATISTICS)


def build_court_opinion_texts(
    local_file: Path,
    output_dir: Path,
    *,
    dump_date: date,
    generation_id: str | None = None,
    max_rows: int | None = None,
    workers: int = 3,
    convert: Converter | None = None,
    parse_workers: int = 4,
    row_group_text_bytes: int = ROW_GROUP_TEXT_BYTES,
    source_id: str | None = None,
) -> dict:
    """Write ``output_dir/court_opinion_texts/`` and ``output_dir/etl_receipts.parquet``; return the run receipt.

    ``workers`` conversion processes (0: convert in this process); ``convert`` replaces the SpicyDocs converter
    (tests); ``max_rows`` bounds the read to the export's head; ``source_id`` names the export in the witness when
    ``local_file`` is a content-addressed raw object rather than the publisher's file name.
    """
    from spicy_docs.sources.courtlistener.local import CourtListenerLocalDump

    started = time.perf_counter()
    generation_id = generation_id or uuid4().hex
    edition = dump_date.isoformat()
    policy = POLICIES[DATASET]
    table_dir = output_dir / DATASET
    spill_dir = output_dir / f".{DATASET}-spill"
    receipt_path = output_dir / "etl_receipts.parquet"
    for path in (table_dir, spill_dir, receipt_path):
        if path.exists():
            raise FileExistsError(f"{path} exists; build into an empty directory")
    output_dir.mkdir(parents=True, exist_ok=True)
    witness = file_witness(local_file, source_id=source_id)
    identity = _converter().converter_identity() if convert is None else {"converter_version": "test"}
    diagnostics = {"converter": identity, "opinion_block_width": OPINION_BLOCK_WIDTH,
                   "bounded_rows": max_rows}
    dump = CourtListenerLocalDump(local_file, columns=list(SOURCE_COLUMNS), parse_workers=parse_workers,
                                  work_dir=output_dir)
    counts = {"rows_read": 0, "refused": 0, "source_bytes": 0, "convert_cpu_seconds": 0.0}
    refusals: list[dict] = []
    by_source: Counter = Counter()

    def tasks() -> Iterator[list[dict]]:
        # Closing the reader's generator early (a bounded run) removes its pieces and stops its threads.
        with closing(cast(Generator[pa.RecordBatch], dump.iter_batches())) as batches:
            for batch in batches:
                rows = batch.to_pylist()
                if max_rows is not None:
                    rows = rows[: max_rows - counts["rows_read"]]
                counts["rows_read"] += len(rows)
                for start in range(0, len(rows), _TASK_ROWS):
                    yield rows[start: start + _TASK_ROWS]
                if max_rows is not None and counts["rows_read"] >= max_rows:
                    return

    def converted(pool: ProcessPoolExecutor | None) -> Iterator[pa.RecordBatch]:
        running: deque[Future] = deque()
        version = identity["converter_version"]

        def tally(result: _Converted) -> pa.RecordBatch:
            refusals.extend(result.refused)
            counts["source_bytes"] += result.source_bytes
            counts["convert_cpu_seconds"] += result.cpu_seconds
            by_source.update(result.by_source)
            return result.batch

        for task in tasks():
            if pool is None:
                yield tally(_convert(task, edition, convert, version))
                continue
            running.append(pool.submit(_convert, task, edition, convert, version))
            if len(running) >= 2 * workers:  # bounded in flight: a task holds tens of MB of HTML
                yield tally(running.popleft().result())
        while running:
            yield tally(running.popleft().result())

    # Phase 1: one pass over the export into id buckets.
    buckets = -(-12_000_000 // SPILL_WIDTH)
    with ProcessPoolExecutor(workers) if workers else nullcontext() as pool:
        ds.write_dataset(
            converted(pool), spill_dir, schema=_SPILL_SCHEMA, format="parquet",
            partitioning=ds.partitioning(pa.schema([(_BUCKET, pa.int32())]), flavor="hive"),
            file_options=ds.ParquetFileFormat().make_write_options(compression="zstd", compression_level=1),
            max_open_files=_check_open_files(buckets), existing_data_behavior="error",
            basename_template="spill-{i}.parquet",
        )
    phase1 = time.perf_counter() - started
    spill_bytes = sum(p.stat().st_size for p in spill_dir.rglob("*.parquet"))
    if max_rows is None and not dump.completed:
        raise RuntimeError(f"{local_file.name} was not read to its end")

    # Phase 2: each bucket sorted by numeric id, into its block's member, subjects and receipts in one pass.
    bucket_dirs = sorted(spill_dir.glob(f"{_BUCKET}=*"), key=lambda p: int(p.name.partition("=")[2]))
    block, member = None, None
    routed = 0
    try:
        with etl_bulk.new_receipt_writer(receipt_path) as receipts:
            for bucket in bucket_dirs:
                table = ds.dataset(bucket, format="parquet").to_table()
                table = table.take(pc.call_function("sort_indices", [pc.cast(table["opinion_id"], pa.int64())]))
                sorted_source = output_dir / f".{DATASET}-sorted.parquet"
                pq.write_table(table, sorted_source, compression="zstd", compression_level=1)
                del table
                bucket_block = str(int(bucket.name.partition("=")[2]) * SPILL_WIDTH // OPINION_BLOCK_WIDTH)
                if bucket_block != block:  # buckets arrive in id order, so a block's member is finished here
                    if member is not None:
                        member.close()
                    block = bucket_block
                    member = _member_writer(table_dir / f"opinion_block={block}" / "part-000000.parquet")
                routed += _write_bulk_pair(
                    DATASET, sorted_source, table_dir / ".unused.parquet", receipt_path,
                    generation_id=generation_id, witnesses=[witness], processor=PROCESSOR, diagnostics=diagnostics,
                    attempt_prefix=f"{edition}:{bucket.name}", receipt_writer=receipts,
                    subject_writer=member, segments=byte_segments(row_group_text_bytes),
                )
                sorted_source.unlink()
                shutil.rmtree(bucket)
            # Refusals follow every accepted receipt, as the row writer orders them; they carry the pointer only.
            for ordinal, refusal in enumerate(refusals):
                error = refusal.pop("error")
                context = ReceiptContext(generation_id, f"{DATASET}:convert:{edition}:{ordinal}", PROCESSOR,
                                         [witness], diagnostics)
                receipts.write_table(pa.Table.from_pylist(
                    [_refusal(policy, context, error, {**refusal, "dump_date": edition})], schema=RECEIPT_SCHEMA))
    finally:
        if member is not None:
            member.close()
    counts["refused"] = len(refusals)
    shutil.rmtree(spill_dir, ignore_errors=True)
    run = {
        "dataset": DATASET, "generation_id": generation_id, "dump_date": edition, "witness": witness,
        "status": "bounded" if max_rows is not None else "complete", "completed_read": dump.completed,
        **counts, "rows_by_text_source": dict(sorted(by_source.items())), "rows_routed_to_row_function": routed, "spill_bytes": spill_bytes,
        "phase1_seconds": round(phase1, 1), "phase2_seconds": round(time.perf_counter() - started - phase1, 1),
        "max_rss_main_process": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss, "diagnostics": diagnostics,
    }
    (output_dir / f"{DATASET}-run.json").write_text(json.dumps(run, indent=2, sort_keys=True, default=str) + "\n")
    return run


def main(argv: list[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(description="Build court_opinion_texts from a retained opinions export.")
    parser.add_argument("export", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--dump-date", type=date.fromisoformat, required=True)
    parser.add_argument("--max-rows", type=int)
    # Three: with the reader's four parse threads the process tree peaked at 6.8 GB on the 100,000-record head
    # (7.3 GB with four), under the 8 GB budget with headroom.
    parser.add_argument("--workers", type=int, default=min(3, os.cpu_count() or 1))
    parser.add_argument("--source-id", help="the publisher's file name, when the export is a raw archive object")
    args = parser.parse_args(argv)
    run = build_court_opinion_texts(args.export, args.output_dir, dump_date=args.dump_date, max_rows=args.max_rows,
                                    workers=args.workers, source_id=args.source_id)
    print(json.dumps(run, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
