"""Local-publication pipeline for ``court-opinion-texts`` (receipts-free ``courts/2``).

Locates a retained CourtListener ``opinions-<dump_date>.csv.bz2`` archive in ``CentralRawArchiveStore`` (never
downloading or copying the 55 GB archive), retains its central-raw provenance and converter diagnostics via
``CaptureEvidence``, builds id-sorted ``opinion_block`` Parquet members via
:func:`spicy_regs.transforms.build_court_opinion_texts.build_court_opinion_texts`, and seals an immutable
generation with zero-copy ``owned_output_root`` split-table adoption.

Full builds require ``>= 75 GiB`` free on ``--output-dir``'s volume and refuse ``SPICYGOV_RAW_ROOT`` so the
resulting generation passes ``publish-local-generation --dry-run`` on the shared raw archive. Bounded local
canaries (``--max-rows``) bypass the 75 GiB full-build disk gate and respect ``SPICYGOV_RAW_ROOT`` when set.
"""

from __future__ import annotations

import argparse
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
import json
import os
from pathlib import Path
import shutil
from typing import Any

from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.raw_releases import READ_INDEX_HELP, inherit_read_index, raw_original
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_court_opinion_texts import (
    DATASET,
    DEFAULT_COMPRESSION_LEVEL,
    DEFAULT_SPILL_COMPRESSION_LEVEL,
    OPINION_BLOCK_WIDTH,
    PIECE_BYTES,
    ROW_GROUP_TEXT_BYTES,
    SCHEMA,
    Converter,
    _file_sha256,
    build_court_opinion_texts,
)

FAMILY = "court-opinion-texts"
TABLE_KEY = f"{DATASET}.parquet"
EXPORT_STAGE = "courtlistener-bulk-export"
DEFAULT_DUMP_DATE = date(2026, 9, 30)
DEFAULT_SOURCE_SHA256 = "sha256:5e7b9e9b0ced0ab9c80c3c6d4d1fbe05c5d62a77759120061e2e460c069073d8"
BULK_BASE_URL = "https://storage.courtlistener.com/bulk-data"
RAW_ROOT_ENV = "SPICYGOV_RAW_ROOT"
GIB = 1024**3
#: Minimum free disk required on ``output_dir``'s volume before launching a full 10.9M build (~30 GiB + 40 GiB floor).
DEFAULT_MIN_FREE_GIB = 75.0


@dataclass(frozen=True)
class CourtOpinionTextsBuildResult:
    family: str
    generation_dir: Path
    evidence_dir: Path
    run_path: Path
    exceptions_path: Path | None
    artifact_digest: str
    logical_id: str
    evidence_digest: str
    run: dict[str, Any]
    disk_preflight: dict[str, Any] | None


def default_original_location(dump_date: date) -> str:
    return f"{BULK_BASE_URL}/opinions-{dump_date.isoformat()}.csv.bz2"


def _existing_ancestor(path: Path) -> Path:
    probe = Path(path).resolve(strict=False)
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    return probe


def check_disk_headroom(
    output_dir: Path,
    *,
    min_free_gib: float = DEFAULT_MIN_FREE_GIB,
    disk_usage: Callable[[Path], Any] | None = None,
) -> dict[str, Any]:
    """Refuse launching a full build when ``output_dir``'s volume has less than ``min_free_gib`` GiB free."""
    probe = _existing_ancestor(output_dir)
    usage = (shutil.disk_usage if disk_usage is None else disk_usage)(probe)
    free_gib = int(usage.free) / GIB
    if free_gib < min_free_gib:
        raise RuntimeError(
            f"Refusing full {FAMILY} build on {probe}: {free_gib:.2f} GiB free is below the "
            f"{min_free_gib:.1f} GiB pre-flight floor (need >= 30 GiB build + 40 GiB dry-run reserve)"
        )
    return {"probe_path": str(probe), "free_gib": round(free_gib, 3), "min_free_gib": min_free_gib}


def build_court_opinion_texts_generation(
    output_dir: Path,
    *,
    dump_date: date = DEFAULT_DUMP_DATE,
    source_sha256: str = DEFAULT_SOURCE_SHA256,
    original_location: str | None = None,
    export_path: Path | None = None,
    archive_root: Path | None = None,
    allow_scratch_raw: bool = False,
    min_free_gib: float | None = None,
    max_rows: int | None = None,
    workers: int = 3,
    parse_workers: int = 4,
    piece_bytes: int = PIECE_BYTES,
    row_group_text_bytes: int = ROW_GROUP_TEXT_BYTES,
    spill_compression_level: int = DEFAULT_SPILL_COMPRESSION_LEVEL,
    phase2_workers: int = 1,
    compression_level: int = DEFAULT_COMPRESSION_LEVEL,
    convert: Converter | None = None,
    read_index: Path | None = None,
    disk_usage: Callable[[Path], Any] | None = None,
) -> CourtOpinionTextsBuildResult:
    """Build and admit a receipts-free ``court-opinion-texts`` generation from a retained opinions export."""
    is_canary = max_rows is not None
    if is_canary and (type(max_rows) is not int or max_rows < 1):
        raise ValueError("max_rows must be a positive integer")
    if is_canary and archive_root is None and not os.environ.get(RAW_ROOT_ENV):
        raise ValueError(
            f"Bounded {FAMILY} canaries (--max-rows) must pass --archive-root or set {RAW_ROOT_ENV} "
            "so canary runs never write acquisition receipts into the shared raw archive"
        )
    if not is_canary and not allow_scratch_raw and archive_root is None and os.environ.get(RAW_ROOT_ENV):
        raise ValueError(
            f"Unset {RAW_ROOT_ENV} for a full publishable {FAMILY} build "
            "(pass --max-rows for a bounded local canary or --allow-scratch-raw for scratch rehearsal)"
        )

    effective_floor = DEFAULT_MIN_FREE_GIB if (min_free_gib is None and not is_canary) else min_free_gib
    disk_preflight: dict[str, Any] | None = None
    if effective_floor is not None:
        disk_preflight = check_disk_headroom(output_dir, min_free_gib=effective_floor, disk_usage=disk_usage)

    # Validate explicit --export-path digest BEFORE initializing CaptureEvidence so a mismatch never writes a
    # stray blob or acquisition receipt into CentralRawArchiveStore.
    verified_export_path: Path | None = None
    if export_path is not None:
        verified_export_path = Path(export_path).resolve(strict=True)
        if source_sha256:
            observed_sha256 = _file_sha256(verified_export_path)
            if observed_sha256 != source_sha256:
                raise ValueError(
                    f"Export file digest {observed_sha256} differs from expected {source_sha256}"
                )

    # Resolve symlinks (e.g. /tmp -> /private/tmp on macOS) so owned_output_root sees clean real paths.
    resolved_output = Path(output_dir).resolve()
    resolved_output.mkdir(parents=True, exist_ok=True)
    location = original_location or default_original_location(dump_date)
    build_dir = resolved_output / "build"
    generation_dir = resolved_output / "generation"
    if generation_dir.exists():
        raise FileExistsError(f"{generation_dir} exists; build into an empty output directory")

    evidence = CaptureEvidence(resolved_output, FAMILY, archive_root=archive_root)
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        resolved_export = (
            verified_export_path
            if verified_export_path is not None
            else raw_original(evidence.store, source_sha256, None)
        )
        retained = evidence.retain_file(
            resolved_export,
            stage=EXPORT_STAGE,
            raw_source=True,
            original_location=location,
            source_url=location,
            edition=dump_date.isoformat(),
        )
        if source_sha256 and retained["sha256"] != source_sha256:
            raise ValueError(
                f"Retained export digest {retained['sha256']} differs from expected {source_sha256}"
            )

        run = build_court_opinion_texts(
            resolved_export,
            build_dir,
            dump_date=dump_date,
            source_sha256=retained["sha256"],
            source_id=Path(location).name,
            max_rows=max_rows,
            workers=workers,
            convert=convert,
            parse_workers=parse_workers,
            piece_bytes=piece_bytes,
            row_group_text_bytes=row_group_text_bytes,
            spill_compression_level=spill_compression_level,
            phase2_workers=phase2_workers,
            compression_level=compression_level,
        )

        run_path = build_dir / f"{DATASET}-run.json"
        exceptions_candidate = build_dir / f"{DATASET}.exceptions.parquet"
        exceptions_path = exceptions_candidate if run["refused"] > 0 and exceptions_candidate.is_file() else None

        evidence.event(
            "courtlistener-opinion-texts-build",
            table=DATASET,
            edition=dump_date.isoformat(),
            source_sha256=retained["sha256"],
            status=run["status"],
            completed_read=run["completed_read"],
            rows_read=run["rows_read"],
            rows_written=run["rows_written"],
            refused=run["refused"],
            members=run["members"],
            rows_by_text_source=run["rows_by_text_source"],
            refused_by_text_source=run["refused_by_text_source"],
            converter=run["diagnostics"]["converter"],
            opinion_block_width=OPINION_BLOCK_WIDTH,
            bounded_rows=max_rows,
        )
        if exceptions_path is not None:
            evidence.retain_file(
                exceptions_path,
                stage="courtlistener-opinion-texts-exceptions",
                raw_source=False,
                table=DATASET,
            )
        evidence.retain_control(
            run_path,
            stage="courtlistener-opinion-texts-run",
            table=DATASET,
        )

        inputs = evidence.inputs()
        artifact = build_generation(
            generation_dir,
            family=FAMILY,
            files=[build_dir / DATASET],
            expected_keys=[TABLE_KEY],
            schemas={DATASET: described_schema(SCHEMA)},
            partitioned={TABLE_KEY: ["opinion_block"]},
            read_snapshot=read_snapshot,
            inputs=inputs,
            owned_output_root=resolved_output,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    assert evidence.artifact is not None
    return CourtOpinionTextsBuildResult(
        family=FAMILY,
        generation_dir=generation_dir,
        evidence_dir=evidence.artifact_dir,
        run_path=run_path,
        exceptions_path=exceptions_path,
        artifact_digest=artifact.pin.artifact_digest,
        logical_id=artifact.pin.logical_id,
        evidence_digest=evidence.artifact.pin.artifact_digest,
        run=run,
        disk_preflight=disk_preflight,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build and seal a local-publication court-opinion-texts generation from a retained opinions export."
    )
    parser.add_argument("--output-dir", required=True, type=Path, help="Empty directory for build, evidence, and generation")
    parser.add_argument("--dump-date", type=date.fromisoformat, default=DEFAULT_DUMP_DATE)
    parser.add_argument("--source-sha256", default=DEFAULT_SOURCE_SHA256)
    parser.add_argument("--original-location", help="Publisher URL of the retained bulk export")
    parser.add_argument("--export-path", type=Path, help="Explicit path to a retained opinions .csv.bz2 file")
    parser.add_argument("--archive-root", type=Path, help="Override raw archive root (for scratch/canary builds)")
    parser.add_argument("--allow-scratch-raw", action="store_true", help="Allow SPICYGOV_RAW_ROOT on full builds")
    parser.add_argument("--min-free-gib", type=float, help="Override pre-flight disk headroom floor in GiB")
    parser.add_argument("--max-rows", type=int, help="Bounded local canary row limit (never publish live)")
    parser.add_argument("--workers", type=int, default=min(6, os.cpu_count() or 1))
    parser.add_argument("--parse-workers", type=int, default=4)
    parser.add_argument("--piece-bytes", type=int, default=PIECE_BYTES)
    parser.add_argument("--spill-compression-level", type=int, default=DEFAULT_SPILL_COMPRESSION_LEVEL)
    parser.add_argument("--phase2-workers", type=int, default=3)
    parser.add_argument("--compression-level", type=int, default=DEFAULT_COMPRESSION_LEVEL)
    parser.add_argument("--read-index", type=Path, help=READ_INDEX_HELP)
    args = parser.parse_args(argv)

    result = build_court_opinion_texts_generation(
        args.output_dir,
        dump_date=args.dump_date,
        source_sha256=args.source_sha256,
        original_location=args.original_location,
        export_path=args.export_path,
        archive_root=args.archive_root,
        allow_scratch_raw=args.allow_scratch_raw,
        min_free_gib=args.min_free_gib,
        max_rows=args.max_rows,
        workers=args.workers,
        parse_workers=args.parse_workers,
        piece_bytes=args.piece_bytes,
        spill_compression_level=args.spill_compression_level,
        phase2_workers=args.phase2_workers,
        compression_level=args.compression_level,
        read_index=args.read_index,
    )
    summary = {
        "family": result.family,
        "artifact_digest": result.artifact_digest,
        "logical_id": result.logical_id,
        "evidence_digest": result.evidence_digest,
        "generation_dir": str(result.generation_dir),
        "evidence_dir": str(result.evidence_dir),
        "run_path": str(result.run_path),
        "exceptions_path": str(result.exceptions_path) if result.exceptions_path is not None else None,
        "disk_preflight": result.disk_preflight,
        "run": result.run,
    }
    print(json.dumps(summary, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
