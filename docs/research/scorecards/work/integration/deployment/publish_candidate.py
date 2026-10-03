"""Publish one qualified candidate through the existing verified family CAS.

The explicit command performs a remote write. It refuses runtime drift from the
named implementation commit and retains before/after pointer observations.
"""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from hashlib import sha256
from importlib.metadata import version
import json
import os
from pathlib import Path
import subprocess

from dotenv import load_dotenv

from spicy_regs.generations import verify_generation
from spicy_regs.sources import publication, r2


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--preparation", type=Path, required=True)
    parser.add_argument("--implementation-commit", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    args = parser.parse_args()
    subprocess.run(
        [
            "git",
            "diff",
            "--exit-code",
            args.implementation_commit,
            "--",
            "src/spicy_regs",
            "pyproject.toml",
            "uv.lock",
            "data_dictionary",
        ],
        check=True,
        capture_output=True,
    )
    if args.output.exists():
        raise ValueError("Use a fresh publication receipt directory")
    args.output.mkdir(parents=True)
    prepared = json.loads(args.preparation.read_bytes())
    directory = Path(prepared["generation_directory"])
    artifact = verify_generation(directory)
    if artifact.pin.as_dict() != prepared["generation"]:
        raise ValueError("Prepared generation differs from its qualification pin")
    family = artifact.root["spec"]["family"]
    prior = publication.parse_index(Path(prepared["read_snapshot_path"]).read_bytes())
    load_dotenv(args.env_file)
    client = r2.get_r2_client()
    bucket = os.environ.get("R2_BUCKET_NAME", "spicy-regs")
    if (
        bucket != "spicy-regs"
        or client.meta.endpoint_url != "https://174055408ff1560e60601c4d12c561c4.r2.cloudflarestorage.com"
    ):
        raise ValueError("Deployment account or bucket differs from the verified target")
    before, _, _ = publication._stored_index(client, bucket)
    for key, pin in artifact.root["spec"].get("parents", {}).items():
        if publication.table_pin(before, key) != pin:
            raise ValueError("Analysis input changed after qualification; rebuild before publication")
    (args.output / "before.json").write_text(json.dumps(before, indent=2) + "\n")
    evidence = (Path(prepared["evidence_directory"]),) if prepared.get("evidence_directory") else ()
    after = publication.publish_generation(
        directory, client=client, bucket=bucket, prior_index=prior, evidence_directories=evidence
    )
    observed, _, _ = publication._stored_index(client, bucket)
    if observed["families"][family] != after["families"][family]:
        raise ValueError("Published family changed before authenticated readback")
    unexpected = [
        name for name, entry in before["families"].items() if name != family and observed["families"].get(name) != entry
    ]
    (args.output / "after.json").write_text(json.dumps(observed, indent=2) + "\n")
    receipt = {
        "status": "published_authenticated_readback",
        "observed_at": datetime.now(UTC).isoformat(),
        "family": family,
        "generation": artifact.pin.as_dict(),
        "family_entry": observed["families"][family],
        "implementation_commit": args.implementation_commit,
        "provider_version": version("spicy-docs"),
        "preparation_sha256": sha256(args.preparation.read_bytes()).hexdigest(),
        "other_family_changes_during_publication": unexpected,
        "public_index_url": "https://data.spicygov.ai/publication.v2.json",
        "source_network_requests": 0,
    }
    (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(
        json.dumps(
            {
                key: receipt[key]
                for key in ("status", "family", "generation", "other_family_changes_during_publication")
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
