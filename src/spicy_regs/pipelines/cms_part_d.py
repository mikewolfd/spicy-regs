"""Build a selected CMS Part D benefit year from one admitted local source release."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from rulespec_artifacts import ArtifactPin, LocalMemberSource

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_cms_part_d import IDENTITY_FIELDS, MEASURES, OUTPUT, SCHEMA, TABLE, build_cms_part_d


def build(reader, output_dir: Path, *, year: int):
    """Retain originals once and use the ordinary generation path; acquire nothing."""
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh output directory for each CMS Part D generation")
    evidence = CaptureEvidence(output_dir, "cms-part-d")
    try:
        evidence.event(
            "cms-source-release",
            source_logical_id=reader.pin.logical_id,
            source_artifact_digest=reader.pin.artifact_digest,
            collection_outcome=reader.collection_outcome,
            selected_benefit_year=year,
        )
        retained = set()
        dictionary_seen = False
        for capture in reader.collection_outcome["requestedScope"]["captures"]:
            ref = capture["response_sha256"]
            body = reader.read_evidence(ref, max_bytes=capture["byte_size"])
            if capture["role"] == "dictionary":
                selection = reader.collection_outcome["requestedScope"]["selection"]
                if ref != selection["dictionary_sha256"] or capture["request_url"] != selection["dictionary_url"]:
                    raise ValueError("CMS Part D dictionary differs from its selected source")
                fields = {field["name"] for field in json.loads(body)["data"]["fields"]}
                required = {*IDENTITY_FIELDS, "Tot_Mftr", f"Outlier_Flag_{year}"}
                required.update(f"{prefix}_{year}" for prefix in MEASURES.values())
                if not required <= fields:
                    raise ValueError("CMS Part D dictionary lacks a selected-year field")
                dictionary_seen = True
            if ref not in retained:
                evidence.retain_bytes(
                    body,
                    stage="cms-source-" + capture["role"],
                    evidence_blob_ref=ref,
                    source_url=capture["request_url"],
                )
                retained.add(ref)
        if not dictionary_seen:
            raise ValueError("CMS Part D requires its selected dictionary")
        table = build_cms_part_d(reader, output_dir, year=year)
        artifact = build_generation(
            output_dir / "generation",
            family="cms-part-d",
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
    from spicy_docs.sources.cms_profile import CMS_PROFILE
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

    selection = json.loads(args.selection.read_text())
    item = selection["source"]
    reader = SourceNativeReleaseReader(
        LocalMemberSource(item["release"]),
        blob_source=LocalSourceNativeBlobStore(item["blobs"], create=False),
        profile=CMS_PROFILE,
        accepted_verifier_implementation_ids=frozenset({item["verifierImplementationId"]}),
        expected_pin=ArtifactPin(item["logicalId"], item["artifactDigest"]),
    )
    artifact, evidence = build(reader, args.output_dir, year=selection["benefit_year"])
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
