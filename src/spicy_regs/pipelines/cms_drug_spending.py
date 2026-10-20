"""Retain and publish admitted drug-spending observations through one CMS workflow."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from rulespec_artifacts import ArtifactPin, LocalMemberSource

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.cms_drug_spending import IDENTITY_FIELDS


def build(reader, output_dir: Path, *, year: int, label, family, measures, schema, build_table, dictionary_fields=None):
    """Retain originals once and use the ordinary generation path; acquire nothing."""
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError(f"Use a fresh output directory for each CMS {label} generation")
    evidence = CaptureEvidence(output_dir, family)
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
                    raise ValueError(f"CMS {label} dictionary differs from its selected source")
                fields = {field["name"] for field in json.loads(body)["data"]["fields"]}
                required = (
                    set(dictionary_fields)
                    if dictionary_fields is not None
                    else {
                        *IDENTITY_FIELDS,
                        "Tot_Mftr",
                        f"Outlier_Flag_{year}",
                        *(f"{prefix}_{year}" for prefix in measures.values()),
                    }
                )
                if not required <= fields:
                    raise ValueError(f"CMS {label} dictionary lacks a selected-year field")
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
            raise ValueError(f"CMS {label} requires its selected dictionary")
        table = build_table(reader, output_dir, year=year)
        artifact = build_generation(
            output_dir / "generation",
            family=family,
            files=(table,),
            expected_keys=(table.name,),
            schemas={table.stem: described_schema(schema)},
            inputs=evidence.inputs(),
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main(build, description):
    parser = argparse.ArgumentParser(description=description)
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
