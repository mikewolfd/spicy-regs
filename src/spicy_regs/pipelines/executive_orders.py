"""Build a complete declared EO publication year from admitted metadata and local body captures."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from rulespec_artifacts import ArtifactPin, LocalMemberSource

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_executive_orders import OUTPUT, SCHEMA, TABLE, build_executive_orders


def build(reader, output_dir: Path, *, year: int, body_paths, load_body=None):
    """Retain originals and publish one ordinary local generation; no network calls."""
    if load_body is None:
        from spicy_docs.sources.federal_register.body_captures import load_body_capture

        load_body = load_body_capture
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each executive-order generation")
    evidence = CaptureEvidence(output_dir, "executive-orders")
    try:
        table = build_executive_orders(
            reader, output_dir, year=year, body_paths=body_paths, load_body=load_body, evidence=evidence
        )
        artifact = build_generation(
            output_dir / "generation",
            family="executive-orders",
            files=(table,),
            expected_keys=(OUTPUT,),
            schemas={TABLE: described_schema(SCHEMA)},
            inputs=evidence.inputs(),
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    from spicy_docs.source_native import SourceNativeReleaseReader
    from spicy_docs.sources.federal_register.profile import FEDERAL_REGISTER_PROFILE
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    selection = json.loads(args.selection.read_text())
    item = selection["source"]
    reader = SourceNativeReleaseReader(
        LocalMemberSource(item["release"]),
        blob_source=LocalSourceNativeBlobStore(item["blobs"], create=False),
        profile=FEDERAL_REGISTER_PROFILE,
        accepted_verifier_implementation_ids=frozenset({item["verifierImplementationId"]}),
        expected_pin=ArtifactPin(item["logicalId"], item["artifactDigest"]),
    )
    artifact, evidence = build(
        reader, args.output_dir, year=selection["publication_year"], body_paths=selection["bodies"]
    )
    print(
        json.dumps(
            {
                "directory": str(args.output_dir / "generation"),
                "sourceEvidence": str(evidence),
                "logicalId": artifact.pin.logical_id,
                "artifactDigest": artifact.pin.artifact_digest,
            }
        )
    )


if __name__ == "__main__":
    main()
