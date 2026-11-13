"""Build a complete, explicitly pinned CMS provider CSV through ordinary generations."""

import argparse
import hashlib
import json
from pathlib import Path

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_cms_provider import SCHEMA, TABLE, build_cms_provider, required_fields


def build(scope, controls, csv_path: Path, output_dir: Path, *, year: int):
    from spicy_docs.sources.cms_provider import provider_records, provider_scope

    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh CMS provider generation directory")
    catalog, dictionary, stats = controls
    if provider_scope(catalog, dictionary, stats, scope["csv"], release_date=scope["selection"]["release_date"]) != scope:
        raise ValueError("CMS provider selection differs from its original controls")
    fields = {f["name"] for f in json.loads(controls[1].body)["data"]["fields"]}
    if not required_fields() <= fields:
        raise ValueError("CMS provider dictionary lacks required measures or context")
    evidence = CaptureEvidence(output_dir, "cms-medicare-provider")
    try:
        for capture, role in zip(controls, ("catalog", "dictionary", "stats"), strict=True):
            evidence.capture(capture, stage="cms-provider-" + role)
        retained = evidence.retain_file(
            csv_path,
            stage="cms-provider-csv",
            raw_source=True,
            source_url=scope["csv"]["request_url"],
            original_location=scope["csv"]["request_url"],
        )
        if retained["sha256"] != scope["csv"]["response_sha256"] or retained["byte_size"] != scope["csv"]["byte_size"]:
            raise ValueError("CMS provider original differs from its selected CSV")
        with csv_path.open("rb") as stream:
            table = build_cms_provider(
                provider_records(stream, scope=scope, dictionary=controls[1].body), output_dir, scope=scope, year=year
            )
        evidence.event("cms-provider-selection", scope=scope, selected_benefit_year=year)
        artifact = build_generation(
            output_dir / "generation",
            family="cms-medicare-provider",
            files=(table,),
            expected_keys=(table.name,),
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
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore
    from spicy_docs.transport.captured import CapturedBodyResponse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    selected = json.loads(args.selection.read_text())
    store, controls = CentralRawArchiveStore(create=False), []
    for c in selected["controls"]:
        with store.open(c["response_sha256"]) as stream:
            body = stream.read(c["byte_size"] + 1)
        if len(body) != c["byte_size"] or "sha256:" + hashlib.sha256(body).hexdigest() != c["response_sha256"]:
            raise ValueError("CMS provider control bytes differ")
        controls.append(
            CapturedBodyResponse(c["request_url"], c["request_url"], 200, "application/json", c["observed_at"], body)
        )
    artifact, evidence = build(selected["scope"], controls, args.csv, args.output_dir, year=selected["benefit_year"])
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
