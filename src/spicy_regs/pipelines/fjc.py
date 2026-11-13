"""Acquire one FJC Article III flat export and build source-evidenced judicial career tables."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.pipelines.raw_releases import (
    add_snapshot_build, download_original, inherit_read_index, journal_snapshot_releases, snapshot_inputs,
)
from spicy_regs.source_evidence import CaptureEvidence, acquisition_evidence
from spicy_regs.transforms.build_fjc import SCHEMAS, build_fjc

STAGE = "fjc-source-csv"
RELEASE_EVENT = "fjc-source-release"


def capture(evidence, *, transport=None) -> list[dict]:
    """Capture the official flat export once and return its exact observed scope."""
    from spicy_docs.sources import fjc

    descriptor = download_original(
        evidence, lambda acquirer, store: fjc.acquire_snapshot(acquirer, store=store),
        stage=STAGE, max_bytes=fjc.MAX_BYTES, transport=transport, validate_url=fjc.validate_download_url,
    )
    return [fjc.fjc_scope(descriptor)]


def acquire(output_dir: Path, *, transport=None) -> dict:
    """Retain exact publisher bytes and their acquisition receipt in the central raw archive."""
    with acquisition_evidence("fjc-judicial-careers", output_dir=Path(output_dir)) as evidence:
        return {"releases": capture(evidence, transport=transport)}


def build(readers, output_dir: Path, *, archives, read_index: Path | None = None):
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each FJC generation")
    readers = tuple(readers)
    evidence = CaptureEvidence(output_dir, "fjc-judicial-careers")
    try:
        read_snapshot = inherit_read_index(evidence, read_index)
        retained = set()
        for reader in readers:
            capture = reader.collection_outcome["requestedScope"]["capture"]
            ref = capture["response_sha256"]
            if ref not in retained:
                original = Path(archives[ref])
                if original.stat().st_size != capture["byte_size"]:
                    raise ValueError("FJC original differs from the selected size")
                # The request URL, not the local path: this journal is published.
                saved = evidence.retain_file(original, stage=STAGE, raw_source=True, original_location=capture["request_url"])
                if saved.get("sha256") != ref:
                    raise ValueError("FJC original differs from the admitted digest")
                retained.add(ref)
        journal_snapshot_releases(evidence, readers, RELEASE_EVENT)
        tables = build_fjc(readers, output_dir)
        artifact = build_generation(
            output_dir / "generation",
            family="fjc-judicial-careers",
            files=tables,
            expected_keys=tuple(table + ".parquet" for table in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            read_snapshot=read_snapshot,
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    from spicy_docs.sources.fjc_profile import FJC_PROFILE, iter_retained_pages

    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("acquire", help="Capture the official Article III flat export into raw")
    capture.add_argument("--output-dir", required=True, type=Path)
    capture.add_argument("--captures-out", required=True, type=Path, help="Where to write the capture selection")
    add_snapshot_build(commands)
    args = parser.parse_args()
    if args.command == "acquire":
        selection = acquire(args.output_dir)
        args.captures_out.write_text(json.dumps(selection, indent=2, sort_keys=True) + "\n")
        print(json.dumps({"captures": str(args.captures_out), "releases": selection["releases"]}))
        return
    readers, archives = snapshot_inputs(args, FJC_PROFILE, iter_retained_pages)
    artifact, evidence = build(readers, args.output_dir, archives=archives, read_index=args.read_index)
    print(json.dumps({
        "directory": str(args.output_dir / "generation"),
        "sourceEvidence": str(evidence),
        "logicalId": artifact.pin.logical_id,
        "artifactDigest": artifact.pin.artifact_digest,
    }))


if __name__ == "__main__":
    main()
