"""Build retained OFAC list history from explicitly selected admitted snapshots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from rulespec_artifacts import ArtifactPin, LocalMemberSource

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_ofac import SCHEMAS, build_ofac


def build(readers, output_dir: Path, *, archives):
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each OFAC generation")
    readers = tuple(readers)
    evidence = CaptureEvidence(output_dir, "ofac-sanctions")
    try:
        retained = set()
        for reader in readers:
            capture = reader.collection_outcome["requestedScope"]["capture"]
            ref = capture["response_sha256"]
            if ref not in retained:
                original = Path(archives[ref])
                if original.stat().st_size != capture["byte_size"]:
                    raise ValueError("OFAC original differs from the selected size")
                saved = evidence.retain_file(original, stage="ofac-source-archive")
                if saved.get("sha256") != ref:
                    raise ValueError("OFAC original differs from the admitted digest")
                retained.add(ref)
            evidence.event(
                "ofac-source-release",
                source_logical_id=reader.pin.logical_id,
                source_artifact_digest=reader.pin.artifact_digest,
                collection_outcome=reader.collection_outcome,
            )
        tables = build_ofac(readers, output_dir)
        artifact = build_generation(
            output_dir / "generation",
            family="ofac-sanctions",
            files=tables,
            expected_keys=tuple(table + ".parquet" for table in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
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
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    from spicy_docs.source_native import SourceNativeReleaseReader
    from spicy_docs.sources.ofac_profile import OFAC_PROFILE
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    selection = json.loads(args.selection.read_text())
    readers = [
        SourceNativeReleaseReader(
            LocalMemberSource(item["release"]),
            blob_source=LocalSourceNativeBlobStore(item["blobs"], create=False),
            profile=OFAC_PROFILE,
            accepted_verifier_implementation_ids=frozenset({item["verifierImplementationId"]}),
            expected_pin=ArtifactPin(item["logicalId"], item["artifactDigest"]),
        )
        for item in selection["sources"]
    ]
    artifact, evidence = build(readers, args.output_dir, archives=selection["archives"])
    print(json.dumps({
        "directory": str(args.output_dir / "generation"),
        "sourceEvidence": str(evidence),
        "logicalId": artifact.pin.logical_id,
        "artifactDigest": artifact.pin.artifact_digest,
    }))


if __name__ == "__main__":
    main()
