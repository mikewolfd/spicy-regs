"""Build a local Executive disclosure candidate from reviewed, digest-selected public originals."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_executive_disclosures import SCHEMAS, build_tables, validate_selection


def build(selection: dict, output_dir: Path, *, archive_root: Path | None = None):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    validate_selection(selection)
    output_dir = Path(output_dir)
    if output_dir.exists():
        raise ValueError("use a fresh output directory; prior candidates remain intact")
    evidence = CaptureEvidence(output_dir, "executive-disclosures", archive_root=archive_root)
    try:
        evidence.retain_bytes(json.dumps(selection, sort_keys=True).encode(), stage="reviewed-filing-selection")
        tables = build_tables(selection, output_dir, evidence=evidence,
                              store=CentralRawArchiveStore(archive_root, create=False))
        artifact = build_generation(
            output_dir / "generation", family="executive-disclosures", files=tables,
            expected_keys=tuple(name + ".parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(), owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def acquire(url: str, output_dir: Path):
    """One explicitly selected public posting; a Form 201/agency route refuses before network access."""
    from spicy_docs.sources.executive_disclosures import ExecutiveDisclosureAcquirer
    from spicy_regs.source_evidence import acquisition_evidence

    with acquisition_evidence("executive-disclosures", output_dir=Path(output_dir)) as evidence:
        with ExecutiveDisclosureAcquirer() as reader:
            try:
                capture = reader.acquire_pdf(url)
            except Exception as error:
                evidence.refusal(error, stage="public-filing")
                raise
        evidence.capture(capture, stage="public-filing")
        return {"source_url": url, "source_sha256": capture.sha256, "byte_size": capture.byte_size,
                "acquired_at": capture.observed_at}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("acquire")
    capture.add_argument("--url", required=True)
    capture.add_argument("--output-dir", required=True, type=Path)
    candidate = commands.add_parser("build")
    candidate.add_argument("--selection", required=True, type=Path)
    candidate.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    if args.command == "acquire":
        print(json.dumps(acquire(args.url, args.output_dir)))
        return
    artifact, evidence = build(json.loads(args.selection.read_text()), args.output_dir)
    print(json.dumps({"artifactDigest": artifact.pin.artifact_digest, "sourceEvidence": str(evidence)}))


if __name__ == "__main__":
    main()
