"""Build a receipts-free local generation of ``courtlistener_bankruptcy_dockets`` from the retained export."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import READ_INDEX_HELP, inherit_read_index, open_raw, raw_original
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_courtlistener_bankruptcy_dockets import (
    DEFAULT_DECOMPRESSION_THREADS,
    DEFAULT_PARSE_WORKERS,
    DEFAULT_PIECE_BYTES,
    DUMP_DATE,
    EXPECTED_BYTES,
    EXPECTED_ETAG,
    EXPECTED_SHA256,
    FAMILY,
    OUTPUT,
    PUBLISHER_URL,
    SCHEMA,
    TABLE,
    build_courtlistener_bankruptcy_dockets,
)

STAGE_EXPORT = "courtlistener-bankruptcy-export"
BUILD_EVENT = "courtlistener-bankruptcy-build"


def _require_https_url(url: str) -> str:
    if not isinstance(url, str) or not url.strip():
        raise ValueError("CourtListener bankruptcy export source_url must be a non-empty HTTPS URL")
    parsed = urlsplit(url)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError("CourtListener bankruptcy export source_url must be a valid HTTPS URL without credentials")
    return url


def build(
    output_dir: Path,
    *,
    archive_root: Path | None = None,
    read_index: Path | None = None,
    sha256: str = EXPECTED_SHA256,
    byte_size: int = EXPECTED_BYTES,
    dump_date: str = DUMP_DATE,
    source_url: str = PUBLISHER_URL,
    etag: str = EXPECTED_ETAG,
    piece_bytes: int = DEFAULT_PIECE_BYTES,
    parse_workers: int = DEFAULT_PARSE_WORKERS,
    decompression_threads: int = DEFAULT_DECOMPRESSION_THREADS,
) -> tuple[Any, Path, dict[str, Any]]:
    """Verify retained raw export, build typed Parquet, and admit one receipts-free generation."""
    _require_https_url(source_url)
    if not isinstance(etag, str) or not etag.strip():
        raise ValueError("CourtListener bankruptcy export etag must be non-empty")
    if type(byte_size) is not int or byte_size <= 0:
        raise ValueError("CourtListener bankruptcy export byte_size must be a positive integer")

    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("Use a fresh output directory for each CourtListener bankruptcy dockets generation")

    store = open_raw(archive_root)
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        raw_path = raw_original(store, sha256, byte_size)
        retained = evidence.retain_file(
            raw_path,
            stage=STAGE_EXPORT,
            raw_source=True,
            source_url=source_url,
            original_location=source_url,
            etag=etag,
            dataset_id=FAMILY,
            source_version=dump_date,
        )
        if retained.get("sha256") != sha256 or retained.get("byte_size") != byte_size:
            raise ValueError("Retained CourtListener bankruptcy export differs from selected digest or byte size")

        read_snapshot = inherit_read_index(evidence, read_index)
        out_path, stats = build_courtlistener_bankruptcy_dockets(
            raw_path,
            output_dir / "tables",
            dump_date=dump_date,
            source_sha256=sha256,
            piece_bytes=piece_bytes,
            parse_workers=parse_workers,
            decompression_threads=decompression_threads,
        )
        evidence.event(
            BUILD_EVENT,
            source_url=source_url,
            etag=etag,
            source_byte_size=byte_size,
            **stats,
        )
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=[out_path],
            expected_keys=[OUTPUT],
            schemas={TABLE: described_schema(SCHEMA)},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir, stats


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    candidate = commands.add_parser("build")
    candidate.add_argument("--output-dir", required=True, type=Path)
    candidate.add_argument("--archive-root", type=Path)
    candidate.add_argument("--read-index", type=Path, help=READ_INDEX_HELP)
    candidate.add_argument("--sha256", default=EXPECTED_SHA256)
    candidate.add_argument("--byte-size", default=EXPECTED_BYTES, type=int)
    candidate.add_argument("--dump-date", default=DUMP_DATE)
    candidate.add_argument("--source-url", default=PUBLISHER_URL)
    candidate.add_argument("--etag", default=EXPECTED_ETAG)
    candidate.add_argument("--piece-bytes", default=DEFAULT_PIECE_BYTES, type=int)
    candidate.add_argument("--parse-workers", default=DEFAULT_PARSE_WORKERS, type=int)
    candidate.add_argument("--decompression-threads", default=DEFAULT_DECOMPRESSION_THREADS, type=int)

    args = parser.parse_args(argv)
    artifact, evidence_dir, stats = build(
        args.output_dir,
        archive_root=args.archive_root,
        read_index=args.read_index,
        sha256=args.sha256,
        byte_size=args.byte_size,
        dump_date=args.dump_date,
        source_url=args.source_url,
        etag=args.etag,
        piece_bytes=args.piece_bytes,
        parse_workers=args.parse_workers,
        decompression_threads=args.decompression_threads,
    )
    print(
        json.dumps(
            {
                "generation": str(args.output_dir / "generation"),
                "evidence": str(evidence_dir),
                "pin": artifact.pin.as_dict(),
                "stats": stats,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
