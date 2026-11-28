"""CourtListener opinion bodies as Markdown: one pass over the opinions export, written id-sorted in id blocks.

The export is in no id order (a median 2,000-record stretch spans 96% of the id range;
``work/court-text-measure-20261030/extrapolate.json``), so a streaming write cannot sort it, and staging the whole
export first is the 73 GB of scratch behind decision 6. So the build has two phases:

1. Read the export once (``CourtListenerLocalDump`` with 128 MiB pieces), convert each record in a process pool
   (``spicy_docs.sources.courtlistener.opinion_markdown.select_source``) and spill the converted rows, partitioned by
   ``opinion_id // SPILL_WIDTH``, one file per bucket held open for the pass, at ``spill_compression_level`` (default
   zstd 6), each row group about ``SPILL_GROUP_BYTES`` of Markdown. O(N) in the export. ``_complete.json`` marks the
   spill whole.
2. Walk the spill buckets in id order (optionally across ``phase2_workers`` concurrent blocks): read each bucket,
   sort it by numeric ``opinion_id``, verify zero duplicate ``opinion_id``s per block, slice with
   ``byte_segments(row_group_text_bytes)``, and write into the member of its ``opinion_block``
   (``court_opinion_texts/opinion_block=<k>/part-000000.parquet``) via atomic ``.tmp`` rename at zstd 12 with
   statistics only on the nine short columns (omitting ``markdown`` and ``source_text_sha256``). Each block's spill
   buckets are deleted immediately after its member commits; a failed run resumes past already-committed blocks.

Peak memory is O(largest spill bucket * phase2_workers) in phase 2; peak disk is the whole spill at the end of
phase 1, shrinking as phase 2 deletes each committed block's buckets while the members grow.

Readers compute a row's member from its id: ``opinion_block = opinion_id // OPINION_BLOCK_WIDTH`` (stored as decimal
text, as ``opinion_id`` is), so a point read names one member. Spill buckets are a third of a block so the heaviest
bucket sorts comfortably in memory.

Receipts are retired (ss-vse3 / ss-bb8c): every row carries ``source_column`` (the rendering converted, NULL when
``text_source == 'none'``), ``source_text_sha256`` (the per-rendering digest, NULL when ``text_source == 'none'``),
``dump_date`` (ISO export edition date), and ``source_sha256`` (the export file SHA-256, matching ``court_opinions``).
A rendering the converter refuses becomes an exception row in ``court_opinion_texts.exceptions.parquet``
(``court_bulk.EXCEPTION_SCHEMA``, capped at ``court_bulk.MAX_EXCEPTIONS``) carrying pointers only — never raw HTML
or empty text. An opinion without text is a row with ``text_source == 'none'`` and NULL Markdown;
``court_opinion_pdf_extractions`` joins it by ``opinion_id`` where a PDF was extracted.

The run summary ``court_opinion_texts-run.json`` is written last and marks the build done. A run that stopped before
it resumes on the same command: from the spill if phase 1 finished, skipping every block whose
``opinion_block=<k>/part-000000.parquet`` already committed.

A run bounded by ``max_rows`` reads the export's head, writes a ``bounded`` run summary beside its output, and is
intended only for local canary verification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import resource
import shutil
import sys
import time
from collections import Counter, deque
from collections.abc import Callable, Generator, Iterator
from concurrent.futures import Future, ProcessPoolExecutor, ThreadPoolExecutor
from contextlib import closing, nullcontext
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from spicy_docs.sources.courtlistener import opinion_markdown

from spicy_regs.court_bulk import EXCEPTION_SCHEMA, MAX_EXCEPTIONS
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.transforms.table_merge import Partitioning

DATASET = "court_opinion_texts"
SCHEMA = SUBJECT_SCHEMAS[DATASET]
#: Published: the member holding opinion ``N`` is ``opinion_block = N // OPINION_BLOCK_WIDTH``.
OPINION_BLOCK_WIDTH = 150_000
#: Ids per spill bucket, the unit sorted in memory; a divisor of the block width so buckets never straddle a member.
SPILL_WIDTH = 50_000
#: Uncompressed Markdown bytes per subject row group at most.
ROW_GROUP_TEXT_BYTES = 8 << 20
#: Uncompressed CSV piece size passed to ``CourtListenerLocalDump`` (halves rolling piece disk vs 256 MiB default).
PIECE_BYTES = 128 << 20
#: Default zstd compression level for Phase 1 spill files (trades small CPU overhead for ~25% smaller peak spill).
DEFAULT_SPILL_COMPRESSION_LEVEL = 6
#: Default zstd compression level for committed output Parquet members.
DEFAULT_COMPRESSION_LEVEL = 12
#: Subject columns with Parquet statistics; ``markdown`` and ``source_text_sha256`` are omitted to keep footers small.
STATISTICS = [
    "opinion_id",
    "cluster_id",
    "opinion_block",
    "date_modified",
    "text_source",
    "converter_version",
    "source_column",
    "dump_date",
    "source_sha256",
]
#: Export columns read: the identity, the subject's native fields and the renderings ``select_source`` orders.
SOURCE_COLUMNS = ("id", "cluster_id", "date_modified", "html_with_citations", "html", "plain_text")
PARTITIONING = Partitioning("opinion_block", "opinion_id", lambda opinion_id: str(int(opinion_id) // OPINION_BLOCK_WIDTH))
#: Markdown bytes buffered per spill bucket before it is written as one row group (~0.45 GiB max across 230 buckets).
SPILL_GROUP_BYTES = 2 << 20
#: Records per conversion task, so a task's HTML stays in the tens of MB.
_TASK_ROWS = 500
_SPILL_COMPLETE = "_complete.json"
_SPILL_EXCEPTIONS = "_exceptions.parquet"
_RUN = f"{DATASET}-run.json"
_EXCEPTIONS = f"{DATASET}.exceptions.parquet"
_BUCKET = "spill_bucket"
_SPILL_SCHEMA = pa.schema([*SCHEMA, (_BUCKET, pa.int32())])
_SHA256_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
_INT_ID_RE = re.compile(r"[0-9]+\Z")

Converter = Callable[[dict], Any]


@dataclass
class _Converted:
    batch: pa.RecordBatch
    refused: list[dict[str, Any]]
    source_bytes: int
    cpu_seconds: float
    by_source: Counter


def _file_sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def _validate_sha256(digest: str) -> str:
    if not isinstance(digest, str) or not _SHA256_RE.fullmatch(digest):
        raise ValueError("source_sha256 must be 'sha256:' followed by 64 lowercase hex digits")
    return digest


def _convert(
    rows: list[dict[str, Any]],
    start_ordinal: int,
    dump_date: str,
    source_sha256: str,
    convert: Converter | None,
    version: str,
) -> _Converted:
    """One task: each record's Markdown row, or a refusal pointer for a rendering the converter refuses."""
    started = time.process_time()
    if convert is None:
        convert = opinion_markdown.select_source
    out: dict[str, list[Any]] = {name: [] for name in _SPILL_SCHEMA.names}
    refused: list[dict[str, Any]] = []
    source_bytes = 0
    for offset, row in enumerate(rows):
        ordinal = start_ordinal + offset
        opinion_id = row.get("id")
        try:
            if not isinstance(opinion_id, str) or not _INT_ID_RE.fullmatch(opinion_id):
                raise ValueError(f"invalid opinion_id: {opinion_id!r}")
            opinion_num = int(opinion_id)
            text = convert(row)
        except ValueError as error:  # OpinionMarkdownError is a ValueError
            column = next((c for c in ("html_with_citations", "html", "plain_text") if (row.get(c) or "").strip()), None)
            value = (row.get(column) or "").encode() if column else b""
            rendering_sha = "sha256:" + hashlib.sha256(value).hexdigest() if column else None
            pointer = {
                "opinion_id": opinion_id,
                "cluster_id": row.get("cluster_id"),
                "date_modified": row.get("date_modified"),
                "source_column": column,
                "source_text_sha256": rendering_sha,
                "dump_date": dump_date,
                "source_sha256": source_sha256,
            }
            refused.append(
                {
                    "dataset": DATASET,
                    "ordinal": ordinal,
                    "outcome": "refused",
                    "reason": f"{type(error).__name__}: {error}",
                    "source_record": json.dumps(pointer, ensure_ascii=False, sort_keys=True),
                    "source_column": column,
                }
            )
            continue
        column = text.text_source if text.text_source != "none" else None
        value = (row.get(column) or "").encode() if column else b""
        source_bytes += len(value)
        out["opinion_id"].append(opinion_id)
        out["cluster_id"].append(row["cluster_id"])
        out["opinion_block"].append(str(opinion_num // OPINION_BLOCK_WIDTH))
        out["date_modified"].append(row["date_modified"])
        out["text_source"].append(text.text_source)
        out["markdown"].append(text.markdown if column else None)
        out["converter_version"].append(version)
        out["source_column"].append(column)
        out["source_text_sha256"].append("sha256:" + hashlib.sha256(value).hexdigest() if column else None)
        out["dump_date"].append(dump_date)
        out["source_sha256"].append(source_sha256)
        out[_BUCKET].append(opinion_num // SPILL_WIDTH)
    batch = pa.RecordBatch.from_pydict(out, schema=_SPILL_SCHEMA)
    return _Converted(batch, refused, source_bytes, time.process_time() - started, Counter(out["text_source"]))


def _ordered_segment_slices(sizes: list[int], limit: int) -> Iterator[tuple[int, int]]:
    """Yield ``(start, length)`` slices whose cumulative bytes stay <= ``limit`` (one row may exceed it)."""
    start, total = 0, 0
    for index, size in enumerate(sizes):
        if index > start and total + size > limit:
            yield start, index - start
            start, total = index, 0
        total += size
    if start < len(sizes):
        yield start, len(sizes) - start


def byte_segments(limit: int = ROW_GROUP_TEXT_BYTES) -> Callable[[pa.Table], Iterator[pa.Table]]:
    """Cut a sorted batch into consecutive slices of at most ``limit`` Markdown bytes (one row may exceed it)."""

    def segments(table: pa.Table) -> Iterator[pa.Table]:
        sizes = pc.fill_null(pc.call_function("binary_length", [table["markdown"].cast(pa.binary())]), 0).to_pylist()
        for start, length in _ordered_segment_slices(sizes, limit):
            yield table.slice(start, length)

    return segments


def _raise_open_files(needed: int) -> None:
    """Lift the soft open-file limit to ``needed`` (the spill holds a file per bucket open), or refuse."""
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    if soft >= needed:
        return
    if hard != resource.RLIM_INFINITY and hard < needed:
        raise RuntimeError(f"open-file hard limit {hard} is below the {needed} the spill writer keeps open")
    resource.setrlimit(resource.RLIMIT_NOFILE, (needed, hard))


def _member_writer(path: Path, *, compression_level: int = DEFAULT_COMPRESSION_LEVEL) -> pq.ParquetWriter:
    path.parent.mkdir(parents=True, exist_ok=True)
    return pq.ParquetWriter(
        path,
        SCHEMA,
        compression="zstd",
        compression_level=compression_level,
        write_statistics=STATISTICS,
    )


def _rss_bytes() -> int:
    """This process's peak RSS in bytes (``ru_maxrss`` is bytes on macOS, KiB on Linux)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024


def build_court_opinion_texts(
    local_file: Path,
    output_dir: Path,
    *,
    dump_date: date,
    source_sha256: str | None = None,
    max_rows: int | None = None,
    workers: int = 3,
    convert: Converter | None = None,
    parse_workers: int = 4,
    piece_bytes: int = PIECE_BYTES,
    row_group_text_bytes: int = ROW_GROUP_TEXT_BYTES,
    source_id: str | None = None,
    spill_compression_level: int = DEFAULT_SPILL_COMPRESSION_LEVEL,
    phase2_workers: int = 1,
    compression_level: int = DEFAULT_COMPRESSION_LEVEL,
) -> dict[str, Any]:
    """Write ``output_dir/court_opinion_texts/`` (receipts-free); return the run summary.

    ``source_sha256`` accepts an already-verified export file SHA-256 (``sha256:...``) so callers reading from
    ``CentralRawArchiveStore`` do not re-hash the 55 GB archive; when omitted, ``local_file`` is hashed once.
    ``workers`` sets Phase 1 conversion processes (0: convert in this process); ``phase2_workers`` sets concurrent
    Phase 2 block writers (default 1; 3-4 recommended for full builds); ``convert`` replaces the SpicyDocs
    converter (tests); ``max_rows`` bounds the read to the export's head for local canaries.

    A rerun into the same directory after a failure resumes: a complete spill (its ``_complete.json`` written last)
    is not read again, and Phase 2 skips every block whose ``opinion_block=<k>/part-000000.parquet`` already
    committed. The rerun must name the same export digest, converter, bound, dump date, and spill compression level.
    """
    table_dir = output_dir / DATASET
    spill_dir = output_dir / f".{DATASET}-spill"
    exceptions_path = output_dir / _EXCEPTIONS
    run_path = output_dir / _RUN
    if run_path.exists():
        raise FileExistsError(f"{run_path} exists; build into an empty directory")
    if max_rows is not None and (type(max_rows) is not int or max_rows < 1):
        raise ValueError("max_rows must be a positive integer")
    if type(spill_compression_level) is not int or not 1 <= spill_compression_level <= 12:
        raise ValueError("spill_compression_level must be an integer from 1 to 12")
    if type(phase2_workers) is not int or phase2_workers < 1:
        raise ValueError("phase2_workers must be a positive integer")
    if type(compression_level) is not int or not 1 <= compression_level <= 22:
        raise ValueError("compression_level must be an integer from 1 to 22")
    if type(piece_bytes) is not int or piece_bytes < 1:
        raise ValueError("piece_bytes must be a positive integer")
    if type(row_group_text_bytes) is not int or row_group_text_bytes < 1:
        raise ValueError("row_group_text_bytes must be a positive integer")

    export_sha256 = _validate_sha256(source_sha256) if source_sha256 is not None else _file_sha256(local_file)
    resolved_source_id = source_id or local_file.name
    identity = opinion_markdown.converter_identity() if convert is None else {"converter_version": "test"}
    marker = spill_dir / _SPILL_COMPLETE
    resumed = marker.exists()
    if resumed:
        spill = json.loads(marker.read_text())
        if spill.get("spill_compression_level", DEFAULT_SPILL_COMPRESSION_LEVEL) != spill_compression_level:
            raise ValueError(f"{spill_dir} was spilled at another compression level; resume with its recorded level")
        expected = {
            "source_sha256": export_sha256,
            "converter": identity,
            "bounded_rows": max_rows,
            "dump_date": dump_date.isoformat(),
        }
        if {key: spill.get(key) for key in expected} != json.loads(json.dumps(expected)):
            raise ValueError(f"{spill_dir} was spilled from another export, converter or bound; remove it to rebuild")
    else:
        for path in (table_dir, spill_dir, exceptions_path):
            if path.exists():
                raise FileExistsError(f"{path} exists without a complete spill; build into an empty directory")
        output_dir.mkdir(parents=True, exist_ok=True)
        spill = _spill(
            local_file,
            spill_dir,
            source_id=resolved_source_id,
            source_sha256=export_sha256,
            identity=identity,
            dump_date=dump_date,
            max_rows=max_rows,
            workers=workers,
            convert=convert,
            parse_workers=parse_workers,
            piece_bytes=piece_bytes,
            work_dir=output_dir,
            spill_compression_level=spill_compression_level,
        )

    phase2 = time.perf_counter()
    rows_written, blocks = _write_members(
        spill,
        spill_dir,
        table_dir,
        row_group_text_bytes,
        phase2_workers=phase2_workers,
        compression_level=compression_level,
    )
    if spill["counts"]["refused"] > 0:
        staged_exceptions = spill_dir / _SPILL_EXCEPTIONS
        pending_exc = exceptions_path.with_name(f".{exceptions_path.name}.tmp")
        shutil.copyfile(staged_exceptions, pending_exc)
        pending_exc.replace(exceptions_path)
    else:
        exceptions_path.unlink(missing_ok=True)

    diagnostics = _diagnostics(spill)
    run: dict[str, Any] = {
        "dataset": DATASET,
        "dump_date": spill["dump_date"],
        "source_id": spill["source_id"],
        "source_sha256": spill["source_sha256"],
        "export_bytes": spill["export_bytes"],
        "status": "bounded" if max_rows is not None else "complete",
        "completed_read": spill["completed_read"],
        **spill["counts"],
        "rows_written": rows_written,
        "members": len(blocks),
        "opinion_blocks": blocks,
        "exceptions_file": _EXCEPTIONS if spill["counts"]["refused"] > 0 else None,
        "rows_by_text_source": spill["rows_by_text_source"],
        "refused_by_text_source": spill["refused_by_text_source"],
        "spill_bytes": spill["spill_bytes"],
        "spill_compression_level": spill["spill_compression_level"],
        "spill_row_groups": spill["spill_row_groups"],
        "spill_files": spill["spill_files"],
        "phase2_workers": phase2_workers,
        "resumed": resumed,
        "phase1_seconds": spill["phase1_seconds"],
        "phase2_seconds": round(time.perf_counter() - phase2, 1),
        "max_rss_main_process_bytes": _rss_bytes(),
        "diagnostics": diagnostics,
    }
    # The run summary commits the build; the spill (and resume state in it) goes only after it is in place.
    pending = run_path.with_name(f".{run_path.name}.tmp")
    pending.write_text(json.dumps(run, indent=2, sort_keys=True, default=str) + "\n")
    pending.replace(run_path)
    shutil.rmtree(spill_dir)
    return run


def _diagnostics(spill: dict[str, Any]) -> dict[str, Any]:
    return {
        "converter": spill["converter"],
        "opinion_block_width": OPINION_BLOCK_WIDTH,
        "bounded_rows": spill["bounded_rows"],
    }


def _spill(
    local_file: Path,
    spill_dir: Path,
    *,
    source_id: str,
    source_sha256: str,
    identity: dict[str, Any],
    dump_date: date,
    max_rows: int | None,
    workers: int,
    convert: Converter | None,
    parse_workers: int,
    piece_bytes: int,
    work_dir: Path,
    spill_compression_level: int = DEFAULT_SPILL_COMPRESSION_LEVEL,
) -> dict[str, Any]:
    """Phase 1: one pass over the export into id buckets, refusals as exceptions; ``_complete.json`` is written last."""
    from spicy_docs.sources.courtlistener.local import CourtListenerLocalDump

    started = time.perf_counter()
    edition = dump_date.isoformat()
    spill: dict[str, Any] = {
        "source_id": source_id,
        "source_sha256": source_sha256,
        "export_bytes": local_file.stat().st_size,
        "converter": identity,
        "bounded_rows": max_rows,
        "dump_date": edition,
        "spill_compression_level": spill_compression_level,
    }
    dump = CourtListenerLocalDump(
        local_file,
        columns=list(SOURCE_COLUMNS),
        parse_workers=parse_workers,
        piece_bytes=piece_bytes,
        work_dir=work_dir,
    )
    counts: dict[str, Any] = {"rows_read": 0, "refused": 0, "source_bytes": 0, "convert_cpu_seconds": 0.0}
    refusals: list[dict[str, Any]] = []
    by_source: Counter = Counter()

    def tasks() -> Iterator[tuple[int, list[dict[str, Any]]]]:
        # Closing the reader's generator early (a bounded run) removes its pieces and stops its threads.
        with closing(cast(Generator[pa.RecordBatch], dump.iter_batches())) as batches:
            for batch in batches:
                rows = batch.to_pylist()
                if max_rows is not None:
                    rows = rows[: max_rows - counts["rows_read"]]
                batch_start = counts["rows_read"]
                counts["rows_read"] += len(rows)
                for start in range(0, len(rows), _TASK_ROWS):
                    yield batch_start + start, rows[start : start + _TASK_ROWS]
                if max_rows is not None and counts["rows_read"] >= max_rows:
                    return

    def converted(pool: ProcessPoolExecutor | None) -> Iterator[pa.RecordBatch]:
        running: deque[Future[_Converted]] = deque()
        version = identity["converter_version"]

        def tally(result: _Converted) -> pa.RecordBatch:
            refusals.extend(result.refused)
            if len(refusals) > MAX_EXCEPTIONS:
                raise RuntimeError(
                    f"{DATASET}: more than {MAX_EXCEPTIONS:,} rows refused during conversion; "
                    "review the export before building it"
                )
            counts["source_bytes"] += result.source_bytes
            counts["convert_cpu_seconds"] += result.cpu_seconds
            by_source.update(result.by_source)
            return result.batch

        for start_ordinal, task in tasks():
            if pool is None:
                yield tally(_convert(task, start_ordinal, edition, source_sha256, convert, version))
                continue
            running.append(pool.submit(_convert, task, start_ordinal, edition, source_sha256, convert, version))
            if len(running) >= 2 * workers:  # bounded in flight: a task holds tens of MB of HTML
                yield tally(running.popleft().result())
        while running:
            yield tally(running.popleft().result())

    with ProcessPoolExecutor(workers) if workers else nullcontext() as pool:
        _write_spill(converted(pool), spill_dir, compression_level=spill_compression_level)
    if max_rows is None and not dump.completed:
        raise RuntimeError(f"{local_file.name} was not read to its end")

    refused_by_source = dict(sorted(Counter(str(r["source_column"]) for r in refusals).items()))
    if refusals:
        exception_rows = [
            {key: row[key] for key in EXCEPTION_SCHEMA.names}
            for row in sorted(refusals, key=lambda item: item["ordinal"])
        ]
        pq.write_table(
            pa.Table.from_pylist(exception_rows, schema=EXCEPTION_SCHEMA),
            spill_dir / _SPILL_EXCEPTIONS,
            compression="zstd",
        )

    files = list(spill_dir.glob(f"{_BUCKET}=*.parquet"))
    blocks = sorted({int(p.stem.partition("=")[2]) * SPILL_WIDTH // OPINION_BLOCK_WIDTH for p in files})
    counts["refused"] = len(refusals)
    spill.update(
        completed_read=dump.completed,
        counts=counts,
        blocks=blocks,
        rows_by_text_source=dict(sorted(by_source.items())),
        refused_by_text_source=refused_by_source,
        spill_bytes=sum(p.stat().st_size for p in files),
        spill_files=len(files),
        spill_row_groups=sum(pq.ParquetFile(p).metadata.num_row_groups for p in files),
        phase1_seconds=round(time.perf_counter() - started, 1),
    )
    pending_marker = spill_dir / f".{_SPILL_COMPLETE}.tmp"
    pending_marker.write_text(json.dumps(spill, indent=2, sort_keys=True, default=str) + "\n")
    pending_marker.replace(spill_dir / _SPILL_COMPLETE)
    return spill


def _write_spill(
    batches: Iterator[pa.RecordBatch],
    spill_dir: Path,
    *,
    compression_level: int = DEFAULT_SPILL_COMPRESSION_LEVEL,
) -> None:
    """Route converted rows to ``spill_bucket=<k>.parquet``, each bucket's rows buffered to ``SPILL_GROUP_BYTES``.

    Every bucket's writer stays open for the pass (the export is in no id order, so every bucket is written
    throughout), so each bucket is one file. O(N log T) for tasks of T rows.
    """
    spill_dir.mkdir()
    writers: dict[int, pq.ParquetWriter] = {}
    buffered: dict[int, tuple[list[pa.Table], int]] = {}

    def flush(bucket: int) -> None:
        tables, _ = buffered.pop(bucket)
        if bucket not in writers:
            _raise_open_files(len(writers) + 256)
            writers[bucket] = pq.ParquetWriter(
                spill_dir / f"{_BUCKET}={bucket}.parquet",
                SCHEMA,
                compression="zstd",
                compression_level=compression_level,
            )
        writers[bucket].write_table(pa.concat_tables(tables))

    try:
        for batch in batches:
            if batch.num_rows == 0:
                continue
            table = pa.Table.from_batches([batch])
            order = pc.call_function("sort_indices", [table[_BUCKET]])
            keys = table[_BUCKET].take(order).to_pylist()
            sizes = pc.fill_null(pc.call_function("binary_length", [table["markdown"].cast(pa.binary())]), 0)
            sizes = sizes.take(order).to_pylist()
            table = table.drop_columns([_BUCKET])
            start = 0
            for index in range(1, len(keys) + 1):
                if index < len(keys) and keys[index] == keys[start]:
                    continue
                # A copy of the bucket's rows: a slice would pin the whole task's buffers until the bucket flushes.
                tables, size = buffered.get(keys[start], ([], 0))
                tables.append(table.take(order.slice(start, index - start)))
                buffered[keys[start]] = (tables, size + sum(sizes[start:index]))
                if buffered[keys[start]][1] >= SPILL_GROUP_BYTES:
                    flush(keys[start])
                start = index
        for bucket in list(buffered):
            flush(bucket)
    finally:
        for writer in writers.values():
            writer.close()


def _write_block(
    block: int,
    buckets: list[Path],
    table_dir: Path,
    row_group_text_bytes: int,
    compression_level: int,
) -> int:
    """Sort one ``opinion_block``'s spill buckets, assert ID uniqueness, commit atomically, and unlink buckets."""
    member_path = table_dir / f"opinion_block={block}" / "part-000000.parquet"
    pending = member_path.with_name(f".{member_path.name}.tmp")
    if member_path.exists():
        # Block already committed in an earlier attempt before interruption; clean any leftover spill buckets.
        pending.unlink(missing_ok=True)
        for bucket in buckets:
            bucket.unlink(missing_ok=True)
        return pq.ParquetFile(member_path).metadata.num_rows

    if not buckets:
        raise FileNotFoundError(f"{DATASET}: missing spill buckets for uncommitted opinion_block={block}")

    member_path.parent.mkdir(parents=True, exist_ok=True)
    pending.unlink(missing_ok=True)
    written = 0
    previous_max_id: int | None = None
    try:
        with _member_writer(pending, compression_level=compression_level) as member:
            for bucket in buckets:
                table = pq.read_table(bucket)
                if table.num_rows == 0:
                    continue
                ids = pc.cast(table["opinion_id"], pa.int64())
                if ids.null_count:
                    raise ValueError(f"{DATASET}: null opinion_id in opinion_block={block}")
                order = pc.call_function("sort_indices", [ids])
                sorted_ids = ids.take(order)
                if sorted_ids.length() > 1:
                    dup_mask = pc.call_function(
                        "equal", [sorted_ids.slice(0, sorted_ids.length() - 1), sorted_ids.slice(1)]
                    )
                    if pc.call_function("any", [dup_mask]).as_py():
                        dup_indices = pc.call_function("indices_nonzero", [dup_mask]).to_pylist()[:5]
                        examples = [str(sorted_ids[i].as_py()) for i in dup_indices]
                        raise ValueError(
                            f"{DATASET}: duplicate opinion_id in opinion_block={block}, for example {examples}"
                        )
                first_id = int(sorted_ids[0].as_py())
                last_id = int(sorted_ids[sorted_ids.length() - 1].as_py())
                if previous_max_id is not None and first_id <= previous_max_id:
                    raise ValueError(
                        f"{DATASET}: duplicate or out-of-order opinion_id {first_id} in opinion_block={block}"
                    )
                previous_max_id = last_id
                # Slice ``order`` into row-group-sized chunks BEFORE ``table.take`` so dense buckets never
                # concatenate > 2 GiB of ``pa.string()`` bytes into one contiguous Arrow array.
                raw_sizes = pc.fill_null(
                    pc.call_function("binary_length", [table["markdown"].cast(pa.binary())]), 0
                )
                ordered_sizes = raw_sizes.take(order).to_pylist()
                projected = table.select(SCHEMA.names)
                del table, ids, sorted_ids, raw_sizes
                for start, length in _ordered_segment_slices(ordered_sizes, row_group_text_bytes):
                    segment = projected.take(order.slice(start, length)).cast(SCHEMA)
                    member.write_table(segment)
                    written += segment.num_rows
                    del segment
                del projected, order, ordered_sizes
        pending.replace(member_path)
    except BaseException:
        pending.unlink(missing_ok=True)
        raise

    for bucket in buckets:
        bucket.unlink(missing_ok=True)
    return written


def _write_members(
    spill: dict[str, Any],
    spill_dir: Path,
    table_dir: Path,
    row_group_text_bytes: int,
    *,
    phase2_workers: int = 1,
    compression_level: int = DEFAULT_COMPRESSION_LEVEL,
) -> tuple[int, list[int]]:
    """Phase 2: sort each block's buckets by numeric id into its member; delete buckets immediately after commit."""
    buckets_by_block: dict[int, list[Path]] = {}
    for bucket in spill_dir.glob(f"{_BUCKET}=*.parquet"):
        number = int(bucket.stem.partition("=")[2])
        buckets_by_block.setdefault(number * SPILL_WIDTH // OPINION_BLOCK_WIDTH, []).append(bucket)
    blocks = sorted(set(spill.get("blocks", ())) | buckets_by_block.keys())
    work_items = [
        (block, sorted(buckets_by_block.get(block, []), key=lambda p: int(p.stem.partition("=")[2])))
        for block in blocks
    ]

    if phase2_workers == 1 or len(work_items) <= 1:
        total_written = sum(
            _write_block(block, buckets, table_dir, row_group_text_bytes, compression_level)
            for block, buckets in work_items
        )
    else:
        with ThreadPoolExecutor(max_workers=min(phase2_workers, len(work_items))) as pool:
            futures = [
                pool.submit(_write_block, block, buckets, table_dir, row_group_text_bytes, compression_level)
                for block, buckets in work_items
            ]
            total_written = sum(future.result() for future in futures)

    expected_written = int(spill["counts"]["rows_read"]) - int(spill["counts"]["refused"])
    if total_written != expected_written:
        raise RuntimeError(
            f"{DATASET}: Phase 2 wrote {total_written:,} rows, expected {expected_written:,}"
        )
    return total_written, blocks


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build court_opinion_texts from a retained opinions export.")
    parser.add_argument("export", type=Path)
    parser.add_argument("output_dir", type=Path)
    parser.add_argument("--dump-date", type=date.fromisoformat, required=True)
    parser.add_argument("--source-sha256", help="pre-verified 'sha256:<hex>' of the export file")
    parser.add_argument("--max-rows", type=int)
    parser.add_argument("--workers", type=int, default=min(3, os.cpu_count() or 1))
    parser.add_argument("--parse-workers", type=int, default=4)
    parser.add_argument("--piece-bytes", type=int, default=PIECE_BYTES)
    parser.add_argument("--spill-compression-level", type=int, default=DEFAULT_SPILL_COMPRESSION_LEVEL)
    parser.add_argument("--phase2-workers", type=int, default=1)
    parser.add_argument("--compression-level", type=int, default=DEFAULT_COMPRESSION_LEVEL)
    parser.add_argument("--source-id", help="the publisher's file name, when the export is a raw archive object")
    args = parser.parse_args(argv)
    run = build_court_opinion_texts(
        args.export,
        args.output_dir,
        dump_date=args.dump_date,
        source_sha256=args.source_sha256,
        max_rows=args.max_rows,
        workers=args.workers,
        parse_workers=args.parse_workers,
        piece_bytes=args.piece_bytes,
        source_id=args.source_id,
        spill_compression_level=args.spill_compression_level,
        phase2_workers=args.phase2_workers,
        compression_level=args.compression_level,
    )
    print(json.dumps(run, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
