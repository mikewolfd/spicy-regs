"""Verify explicit retained congressional generations; never fetch or select latest."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pyarrow.parquet as pq
from rulespec_artifacts import LocalMemberSource, iter_member_descriptors

from qualify_lcv import digest, write
from spicy_regs.generations import verify_generation
from spicy_regs.sources import publication


def select(*, baseline: Path, members_receipt: Path, bills: Path, rolls: Path, amendments: Path, output: Path):
    snapshot = publication.parse_index(baseline.read_bytes())
    member_receipt = json.loads(members_receipt.read_bytes())
    paths = {name: [Path(member_receipt["paths"][name])] for name in ("members", "member_terms")}
    paths.update(
        congress_bills=[bills],
        roll_call_votes=[rolls / "roll_call_votes.parquet"],
        amendments=[amendments / "amendments.parquet"],
    )
    selected = []
    for directory in (Path(member_receipt["paths"]["members"]).parent, rolls, amendments):
        artifact = verify_generation(directory)
        family = artifact.root["spec"]["family"]
        members = list(iter_member_descriptors(artifact, LocalMemberSource(directory)))
        previous = snapshot["families"].get(family)
        snapshot["families"][family] = {
            "prefix": f"generations/{family}/{artifact.pin.artifact_digest.removeprefix('sha256:')}",
            "logicalId": artifact.pin.logical_id,
            "artifactDigest": artifact.pin.artifact_digest,
            "tables": publication.table_entries(artifact.root["spec"]["tables"], members),
        }
        selected.append(
            {
                "family": family,
                "directory": str(directory),
                "pin": artifact.pin.as_dict(),
                "baseline_pin": previous["artifactDigest"] if previous else None,
                "selection": "explicit verified retained generation, not a latest claim",
            }
        )
    snapshot = publication.parse_index(json.dumps(snapshot).encode())
    pins, facts = {}, {}
    for name, files in paths.items():
        pin = publication.table_pin(snapshot, name + ".parquet")
        assert len(files) == 1
        file = files[0]
        if digest(file) != pin["sha256"] or file.stat().st_size != pin["byteSize"]:
            raise ValueError(f"Retained {name} differs from selected immutable publication pin")
        pins[name] = pin
        parquet = pq.ParquetFile(file)
        facts[name] = {
            "rows": parquet.metadata.num_rows,
            "columns": parquet.schema_arrow.names,
            "file_sha256": digest(file),
            "byte_size": file.stat().st_size,
        }
    # Read the actual pinned Parquet record, not a prior derived summary.
    vote_reader = pq.ParquetFile(paths["roll_call_votes"][0])
    votes = vote_reader.read().to_pylist()
    zeldin = [row for row in votes if row["vote_id"] == "119-senate-1-24"]
    assert len(zeldin) == 1
    report = {
        "status": "verified_explicit_local_inputs",
        "network_requests": 0,
        "baseline_path": str(baseline),
        "baseline_sha256": digest(baseline),
        "selected_generations": selected,
        "paths": {name: [str(p) for p in files] for name, files in paths.items()},
        "input_pins": pins,
        "selected_index": snapshot,
        "table_facts": facts,
        "direct_official_readback": {
            "vote": zeldin[0],
            "source_path": str(paths["roll_call_votes"][0]),
            "source_sha256": digest(paths["roll_call_votes"][0]),
        },
        "limits": [
            "Bills match the retained 2026-10-03 index. Roll calls and amendments use explicitly selected older verified local generations.",
            "Members and historical terms use the separately qualified local rebuild; none of these selections asserts latest remote coverage.",
            "No congressional source was reacquired by this qualification.",
        ],
    }
    write(output, report)
    print(
        json.dumps(
            {"status": report["status"], "tables": {k: v["rows"] for k, v in facts.items()}, "zeldin": zeldin[0]}
        )
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline", "members-receipt", "bills", "rolls", "amendments", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    select(**vars(parser.parse_args()))


if __name__ == "__main__":
    main()
