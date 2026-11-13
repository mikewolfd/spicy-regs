"""Build an ordinary local generation from explicit pinned geography lookup releases."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from rulespec_artifacts import ArtifactPin, LocalMemberSource

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_census_geocoder import SCHEMAS, build_census_geocoder


def build(readers, output_dir):
    """Retain source originals once in the central archive; preserve every outcome."""
    readers = tuple(readers)
    output_dir = Path(output_dir)
    if output_dir.exists() and any(output_dir.iterdir()):
        raise ValueError("Use a fresh output directory for each geocoder candidate generation")
    evidence = CaptureEvidence(output_dir, "census-geocoder")
    try:
        tables = build_census_geocoder(readers, output_dir)
        for reader in readers:
            (item,) = reader.iter_records()
            r = item["record"]
            c = r["capture"]
            from spicy_docs.transport.captured import CapturedBodyResponse

            evidence.capture(
                CapturedBodyResponse(
                    c["request_url"],
                    c["resolved_url"],
                    c["status_code"],
                    c["content_type"],
                    c["observed_at"],
                    r["response_json"].encode(),
                ),
                stage="census-geocoder-source-release",
                source_logical_id=reader.pin.logical_id,
                source_artifact_digest=reader.pin.artifact_digest,
                selection=r["selection"],
                outcome=r["outcome"],
            )
        artifact = build_generation(
            output_dir / "generation",
            family="census-geocoder",
            files=tables,
            expected_keys=tuple(t.name for t in tables),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inputs",
        type=Path,
        required=True,
        help="JSON array of exact source release paths, pins, blob stores and verifier IDs",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    from spicy_docs.source_native import SourceNativeReleaseReader
    from spicy_docs.sources.census_geocoder_profile import GEOCODER_PROFILE
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    entries = json.loads(args.inputs.read_text())
    if not isinstance(entries, list) or not 1 <= len(entries) <= 100:
        raise ValueError("Declare from one to 100 pinned lookup releases")
    readers = [
        SourceNativeReleaseReader(
            LocalMemberSource(Path(e["release"])),
            blob_source=LocalSourceNativeBlobStore(Path(e["blobs"])),
            profile=GEOCODER_PROFILE,
            expected_pin=ArtifactPin(e["logical_id"], e["digest"]),
            accepted_verifier_implementation_ids=frozenset({e["verifier_implementation_id"]}),
        )
        for e in entries
    ]
    artifact, evidence = build(readers, args.output_dir)
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
