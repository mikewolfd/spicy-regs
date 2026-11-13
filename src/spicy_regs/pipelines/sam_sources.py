"""Capture or replay public SAM files into ordinary local table generations.

Source tables remain separate from SAM registrations and USAspending awards.
Every build names one exact original and preserves every observation/revision.
No command changes publication or source-catalog selection.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import download_original, open_raw
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_sam_sources import SCHEMAS, SNAPSHOTS, TABLES, build_sam_source, validate_capture


def acquire(kind, filename, output_dir, *, max_bytes, transport=None):
    from spicy_docs.sources.sam_public import download_url, validate_public_url

    url = download_url(kind, filename)
    with acquisition_evidence("sam-" + kind, output_dir=Path(output_dir)) as evidence:
        descriptor = download_original(evidence,
            lambda acquirer, store: acquirer.download(url, store=store, max_bytes=max_bytes),
            stage="sam-public-file", max_bytes=max_bytes, transport=transport, validate_url=validate_public_url)
    return descriptor


def build(kind, filename, capture, selection, output_dir, *, archive_root=None, encoding="utf-8-sig"):
    from spicy_docs.sources.sam_public import MAX_CSV_BYTES, csv_records, download_url, exclusion_records, file_date

    validate_capture(capture)
    if capture["request_url"] != download_url(kind, filename):
        raise ValueError("SAM original request differs from the selected public filename")
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a new SAM generation output directory")
    evidence = CaptureEvidence(output_dir, "sam-" + kind, archive_root=archive_root)
    store = open_raw(archive_root)
    try:
        original = store.describe_blob(capture["response_sha256"], capture["byte_size"])
        if kind == "opportunities" and capture["byte_size"] > MAX_CSV_BYTES:
            raise ValueError("SAM notice original exceeds its byte bound")
        # describe_blob renews size and digest before parsing. The exact
        # original is also named in the generation's separate source evidence.
        with store.open(capture["response_sha256"]) as stream:
            if kind == "exclusions":
                records = exclusion_records(stream, filename=filename, byte_size=capture["byte_size"])
                snapshot_date = file_date(filename)
            elif kind == "opportunities":
                records = csv_records(stream, kind=kind, encoding=encoding)
                snapshot_date = None  # This rolling filename states no snapshot date.
            else:
                raise ValueError("Unsupported SAM source kind")
            paths, summary = build_sam_source(records, output_dir, kind=kind, capture=capture,
                                             selection=selection, snapshot_date=snapshot_date)
        evidence.event("sam-public-file-read", capture=capture, filename=filename, population=summary)
        if original["encoding"] != "raw":
            raise ValueError("SAM generation evidence requires an unwrapped original")
        evidence.retain_file(store.root / original["object"], stage="sam-public-file", raw_source=True,
                             original_location=capture["request_url"], observed_at=capture["observed_at"])
        tables = [TABLES[kind], SNAPSHOTS[kind]]
        artifact = build_generation(output_dir / "generation", family="sam-" + kind,
            files=paths, expected_keys=[name + ".parquet" for name in tables],
            schemas={name: described_schema(SCHEMAS[name]) for name in tables},
            inputs=evidence.inputs(), owned_output_root=output_dir)
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    acquire_parser = commands.add_parser("acquire")
    acquire_parser.add_argument("--kind", choices=TABLES, required=True)
    acquire_parser.add_argument("--filename", required=True)
    acquire_parser.add_argument("--max-bytes", type=int, required=True)
    acquire_parser.add_argument("--output-dir", type=Path, required=True)
    acquire_parser.add_argument("--capture-out", type=Path, required=True)
    build_parser = commands.add_parser("build")
    build_parser.add_argument("--kind", choices=TABLES, required=True)
    build_parser.add_argument("--filename", required=True)
    build_parser.add_argument("--capture", type=Path, required=True)
    build_parser.add_argument("--selection", type=Path, required=True)
    build_parser.add_argument("--output-dir", type=Path, required=True)
    build_parser.add_argument("--archive-root", type=Path)
    build_parser.add_argument("--encoding", default="utf-8-sig")
    args = parser.parse_args()
    if args.command == "acquire":
        descriptor = acquire(args.kind, args.filename, args.output_dir, max_bytes=args.max_bytes)
        args.capture_out.write_text(json.dumps(descriptor, indent=2) + "\n")
        print(json.dumps(descriptor))
    else:
        artifact, summary = build(args.kind, args.filename, json.loads(args.capture.read_text()),
            json.loads(args.selection.read_text()), args.output_dir, archive_root=args.archive_root,
            encoding=args.encoding)
        print(json.dumps({"artifact": artifact.pin.as_dict(), "summary": summary}, indent=2))


if __name__ == "__main__":
    main()
