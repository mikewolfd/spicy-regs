"""Build exact scorecard links from one captured published index; no source reads."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path

from dotenv import load_dotenv

from spicy_regs.data_dictionary import expected_schemas
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.sources import publication, r2
from spicy_regs.transforms.build_scorecard_analysis import INPUTS, OUTPUTS, build_scorecard_analysis


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("Use a fresh analysis directory")
    args.output.mkdir(parents=True)
    load_dotenv(args.env_file)
    client = r2.get_r2_client()
    snapshot, _, _ = publication._stored_index(client, "spicy-regs")
    index_path = args.output / "publication.v2.json"
    index_path.write_text(json.dumps(snapshot, indent=2) + "\n")
    paths, pins, parents = {}, {}, {}
    for key in INPUTS:
        name = key.removesuffix(".parquet")
        pins[name] = parents[key] = publication.table_pin(snapshot, key)
        paths[name] = []
        for member in publication.table_members(snapshot, key):
            if member.sha256 is None or member.byte_size is None:
                raise ValueError("Analysis requires immutable published inputs")
            # Authenticated storage read of the already-published immutable object.
            response = client.get_object(Bucket="spicy-regs", Key=member.path)
            target = args.output / "inputs" / member.key
            target.parent.mkdir(parents=True, exist_ok=True)
            digest, count = sha256(), 0
            with target.open("xb") as output, response["Body"] as body:
                while block := body.read(1024 * 1024):
                    count += len(block)
                    if count > member.byte_size:
                        raise ValueError("Published object exceeded its pinned byte count")
                    digest.update(block)
                    output.write(block)
            if count != member.byte_size or "sha256:" + digest.hexdigest() != member.sha256:
                raise ValueError("Published input differs from its immutable pin")
            paths[name].append(target)
    files = build_scorecard_analysis(args.output, input_pins=pins, input_paths=paths)
    evidence = CaptureEvidence(args.output, "scorecard-analysis")
    evidence.inherit(snapshot, public_url="https://data.spicygov.ai")
    evidence.event("verified-published-inputs", input_pins=parents, source_network_requests=0)
    artifact = build_generation(
        args.output / "generation",
        family="scorecard-analysis",
        files=files,
        expected_keys=OUTPUTS,
        schemas=expected_schemas(),
        read_snapshot=snapshot,
        parents=parents,
        inputs=evidence.inputs(),
    )
    verify_generation(args.output / "generation", expected_pin=artifact.pin)
    verify_evidence(evidence.artifact_dir)
    qualification = json.loads((args.output / "scorecard-analysis-qualification.json").read_bytes())
    report = {
        "status": "prepared_not_published",
        "generation": artifact.pin.as_dict(),
        "generation_directory": str(args.output / "generation"),
        "evidence_directory": str(evidence.artifact_dir),
        "read_snapshot_path": str(index_path),
        "source_network_requests": 0,
        "qualification": qualification,
    }
    (args.output / "preparation.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
