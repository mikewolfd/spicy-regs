"""Build a local ACS county generation from an explicitly pinned source release.

The resulting ordinary generation can use sources.publication.publish_generation.
This command does not acquire source data or change any published selection.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from rulespec_artifacts import ArtifactPin, LocalMemberSource

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_census_acs import OUTPUT, SCHEMA, TABLE, build_census_acs


def build(reader, output_dir: Path, *, vintage: int, state: str, counties: tuple[str, ...]):
    """Use the existing source-evidence lineage and generation publication format."""
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each ACS source release")
    from spicy_docs.sources.census_acs_profile import MAX_BUNDLE_BYTES
    evidence = CaptureEvidence(output_dir, "census-acs")
    try:
        table = build_census_acs(reader, output_dir, vintage=vintage, state=state, counties=counties)
        refs = {entry["evidenceBlobRef"] for entry in reader.iter_record_evidence()}
        if len(refs) != 1:
            raise ValueError("ACS county release must retain one complete source query archive")
        evidence.retain_bytes(
            reader.read_evidence(refs.pop(), max_bytes=MAX_BUNDLE_BYTES),
            stage="census-acs-source-release",
            source_logical_id=reader.pin.logical_id, source_artifact_digest=reader.pin.artifact_digest,
            vintage=vintage, state=state, counties=list(counties),
        )
        artifact = build_generation(
            output_dir / "generation", family="census-acs", files=(table,), expected_keys=(OUTPUT,),
            schemas={TABLE: described_schema(SCHEMA)}, inputs=evidence.inputs(),
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-release", type=Path, required=True)
    parser.add_argument("--source-blobs", type=Path, required=True)
    parser.add_argument("--source-logical-id", required=True)
    parser.add_argument("--source-digest", required=True)
    parser.add_argument("--verifier-implementation-id", required=True)
    parser.add_argument("--vintage", type=int, required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--counties", nargs="+", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    from spicy_docs.source_native import SourceNativeReleaseReader
    from spicy_docs.sources.census_acs_profile import ACS5_COUNTY_PROFILE
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    reader = SourceNativeReleaseReader(
        LocalMemberSource(args.source_release),
        blob_source=LocalSourceNativeBlobStore(args.source_blobs),
        profile=ACS5_COUNTY_PROFILE,
        accepted_verifier_implementation_ids=frozenset({args.verifier_implementation_id}),
        expected_pin=ArtifactPin(args.source_logical_id, args.source_digest),
    )
    artifact, evidence = build(reader, args.output_dir, vintage=args.vintage, state=args.state, counties=tuple(args.counties))
    print(json.dumps({"directory": str(args.output_dir / "generation"),
                      "sourceEvidence": str(evidence),
                      "logicalId": artifact.pin.logical_id, "artifactDigest": artifact.pin.artifact_digest}))


if __name__ == "__main__":
    main()
