#!/usr/bin/env python3
"""Fetch checked attachment stages, combine them, and stage the result for a separate fill."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from pathlib import Path, PurePosixPath

from read_comment_attachment_group import checked_download, digest, publish_stage, require, supervise

from spicy_regs import operational_runner as owner

MIB = 2**20
LIMITS = {"wall_seconds": 120 * 60, "interval_seconds": .2, "rss_bytes": 2 * 2**30,
          "output_bytes": 6 * 2**30, "start_free_bytes": 4 * 2**30, "stop_free_bytes": 4 * 2**30}
SPILL_BYTES = 512 * MIB
CONTROL_LIMIT = 4 * MIB
MEMBER_LIMIT = 64 * MIB
RUNNER_WORKFLOW = "Read comment attachment source groups"
PLAN = r"pinned-[a-z0-9-]+\.json"


def sha256(value):
    require(isinstance(value, str) and re.fullmatch(r"[0-9a-f]{64}", value), "Expected an exact SHA-256")
    return value


def staging_key(value, name):
    key = PurePosixPath(value)
    require(str(key) == value and value.startswith("staging/") and ".." not in key.parts and key.name == name,
            f"Expected a staging key for {name}")
    return key


def download(client, bucket, key, target, pin, limit):
    size = pin["bytes"]
    require(type(size) is int and 0 < size <= limit, f"Invalid pinned byte count for {target.name}")
    checked_download(client, bucket, str(key), target, sha256(pin["sha256"]), size, size)


def local_controls(client, bucket, key, pin, work, reads, master_sha256, expected):
    if not expected:
        require(not key and not pin, "The master has no local groups")
        return {}
    require(key and pin, "Local groups require their exact compact controls manifest")
    controls = work / "local-stages.json"
    base = staging_key(key, controls.name).parent
    checked_download(client, bucket, key, controls, sha256(pin), CONTROL_LIMIT)
    manifest = owner.read_json(controls)
    require(manifest["format"] == "comment-attachment-local-stages/1" and manifest["groups_sha256"] == master_sha256
            and set(manifest["groups"]) == expected, "Local controls do not match the frozen local groups")
    for name, selected in manifest["groups"].items():
        files = selected["files"]
        plans = [file for file in files if re.fullmatch(fr"plan/{PLAN}", file)]
        required = {"result.json", "stage/staging.json", *plans}
        optional = {"stage/reads.parquet.sha256", "stage/source-gaps.parquet"}
        require(len(plans) == 1 and required <= set(files) <= required | optional, f"Unexpected local controls for {name}")
        for file, member in files.items():
            download(client, bucket, base / name / file, reads / name / file, member, CONTROL_LIMIT)
    return {group: selected["stage_key"] for group, selected in manifest["groups"].items()}


def github_controls(run_ids, repository, master_sha256, work, reads, expected):
    prefix = f"comment-attachment-stage-{master_sha256}-"
    found = set()
    for run in run_ids:
        gh = ["gh", "run"]
        info = json.loads(subprocess.check_output(
            [*gh, "view", run, "--repo", repository, "--json", "workflowName,status"], text=True))
        require(info == {"workflowName": RUNNER_WORKFLOW, "status": "completed"}, f"Source run {run} differs or is still running")
        records = subprocess.check_output([
            "gh", "api", "--paginate", f"repos/{repository}/actions/runs/{run}/artifacts?per_page=100",
            "--jq", ".artifacts[] | [.name, .size_in_bytes, .expired] | @tsv",
        ], text=True)
        names = []
        for record in records.splitlines():
            name, size, expired = record.split("\t")
            if not name.startswith(prefix):
                continue
            group = name.removeprefix(prefix)
            require(group in expected and group not in found and expired == "false" and 0 < int(size) <= CONTROL_LIMIT,
                    f"Wrong, repeated, expired or oversized compact artifact: {name}")
            found.add(group)
            names.append(name)
        require(names, f"Source run {run} has no matching compact artifacts")
        target = work / "github" / run
        destination = target / names[0] if len(names) == 1 else target
        subprocess.run([*gh, "download", run, "--repo", repository, "--dir", str(destination),
                        *(arg for name in names for arg in ("--name", name))], check=True)
        for name in names:
            group = name.removeprefix(prefix)
            directory = target / name
            require(directory.is_dir() and not directory.is_symlink(), f"Compact artifact directory missing: {name}")
            files = {str(path.relative_to(directory)) for path in directory.rglob("*") if path.is_file()}
            plans = [file for file in files if re.fullmatch(fr"read/plan/{PLAN}", file)]
            required = {"result.json", "stage/staging.json", "stage/reads.parquet.sha256", *plans}
            require(len(plans) == 1 and required <= files <= required | {"stage/source-gaps.parquet"}
                    and sum(owner.regular(directory / file).st_size for file in files) <= CONTROL_LIMIT,
                    f"Unexpected runner controls for {group}")
            directory.rename(reads / group)
    require(found == expected, f"Missing runner groups: {sorted(expected - found)}")


def collect(args, client, bucket):
    from spicy_regs.pipelines import comment_fields, comment_source_groups

    work = args.workdir
    master = work / "source-groups" / "groups.json"
    base = staging_key(args.groups_key, master.name).parent
    checked_download(client, bucket, args.groups_key, master, sha256(args.groups_sha256), CONTROL_LIMIT)
    receipt = owner.read_json(master)
    groups = {item["group"]: item for item in receipt["groups"]}
    require(receipt["format"] == "comment-source-groups/1" and receipt["profile"] == "attachments"
            and receipt["reader_sha256"] == comment_fields._reader_sha256()
            and receipt["implementation_sha256"] == digest(Path(comment_source_groups.__file__))
            and groups and len(groups) == len(receipt["groups"])
            and all(re.fullmatch(r"(local|runner)-[0-9]{3}", name) for name in groups),
            "Frozen groups do not match the checked attachment runtime")
    reads = work / "group-reads"
    reads.mkdir()
    local = {name for name in groups if name.startswith("local-")}
    local_keys = local_controls(client, bucket, args.local_key, args.local_sha256, work, reads, args.groups_sha256, local)
    github_controls(args.source_runs, args.repository, args.groups_sha256, work, reads, set(groups) - local)
    stages = {}
    for name, group in groups.items():
        directory = reads / name
        result = owner.read_json(directory / "result.json")
        staged = owner.read_json(directory / "stage" / "staging.json")
        stage_file = staged["file"]
        key = f"staging/comment-fields-fill-{sha256(stage_file['sha256'])[:12]}/reads.parquet"
        if name in local:
            require(result["master_receipt_sha256"] == args.groups_sha256 and result["inputs"] == group
                    and result["stage"] == stage_file and local_keys[name] == key, f"Local stage identity differs: {name}")
        else:
            require(result["status"] == "complete" and result["groups_sha256"] == args.groups_sha256
                    and result["group_descriptor"] == group and result["stage_file"] == stage_file
                    and result["upload"]["bucket"] == bucket and result["upload"]["key"] == key
                    and result["upload"]["sha256"] == stage_file["sha256"], f"Runner stage identity differs: {name}")
        require(result["group"] == name and staged["profile"] == "attachments" and stage_file["name"] == "reads.parquet"
                and type(stage_file["bytes"]) is int and 0 < stage_file["bytes"] <= MEMBER_LIMIT
                and group["manifest"]["file"] == "manifest.parquet" and type(group["manifest"]["bytes"]) is int
                and 0 < group["manifest"]["bytes"] <= MEMBER_LIMIT, f"Group stage or manifest bytes refused: {name}")
        stages[name] = (key, stage_file)
    stage_bytes = sum(pin["bytes"] for _, pin in stages.values())
    manifest_bytes = sum(group["manifest"]["bytes"] for group in groups.values())
    # Count staged inputs and estimated output, plus spill and re-encoding overhead.
    headroom = 128 * MIB
    needed = 2 * stage_bytes + manifest_bytes + SPILL_BYTES + headroom
    controls = owner.sizes(work)
    require(needed + max(controls["logical"], controls["allocated"]) <= LIMITS["output_bytes"]
            and shutil.disk_usage(work).free >= needed + LIMITS["stop_free_bytes"], "Aggregate bytes exceed available scratch")
    owner.durable(work / "admission.json", {
        "stage_inputs_bytes": stage_bytes, "manifest_bytes": manifest_bytes, "estimated_stage_output_bytes": stage_bytes,
        "spill_bytes": SPILL_BYTES, "extra_headroom_bytes": headroom, "estimated_additional_bytes": needed,
    })
    for name, group in groups.items():
        download(client, bucket, base / name / "manifest.parquet", master.parent / name / "manifest.parquet",
                 group["manifest"], MEMBER_LIMIT)
        key, pin = stages[name]
        download(client, bucket, key, reads / name / "stage" / "reads.parquet", pin, MEMBER_LIMIT)
    return master, reads


def execute(args):
    from spicy_regs.pipelines.comment_stage_aggregate import aggregate
    from spicy_regs.sources.publication import _put_immutable
    from spicy_regs.sources.r2 import verified_target_client

    from comment_attachment_census import census

    client, bucket = verified_target_client()
    master, reads = collect(args, client, bucket)
    stage = args.workdir / "stage"
    receipt = aggregate(master, args.groups_sha256, reads, stage, max_spill=f"{SPILL_BYTES}B")
    census(stage, stage / "attachment-census.json")
    uploaded = publish_stage(stage, client, bucket)
    controls = {}
    for filename in ("staging.json", "attachment-census.json"):
        path = stage / filename
        checksum = digest(path)
        remote = str(PurePosixPath(uploaded["key"]).parent / "controls" / checksum / filename)
        _put_immutable(client, bucket, remote, path, sha256=f"sha256:{checksum}")
        checked_download(client, bucket, remote, args.workdir / "verified" / filename, checksum,
                         CONTROL_LIMIT, path.stat().st_size)
        controls[filename] = {"key": remote, "sha256": checksum, "bytes": path.stat().st_size}
    owner.durable(args.workdir / "result.json", {
        "groups_sha256": args.groups_sha256, "source_run_ids": args.source_runs, "local_controls_sha256": args.local_sha256,
        "runtime_ref": args.runtime_ref, "rows": receipt["rows"], "source_gaps": receipt.get("source_gaps"),
        "upload": uploaded, "controls": controls,
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--groups-key", required=True)
    parser.add_argument("--groups-sha256", required=True)
    parser.add_argument("--local-key", default="")
    parser.add_argument("--local-sha256", default="")
    parser.add_argument("--source-runs", type=json.loads, required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--runtime-ref", required=True)
    parser.add_argument("--workdir", type=Path, required=True)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    require(re.fullmatch(r"[a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+", args.repository), "Invalid repository")
    require(re.fullmatch(r"[0-9a-f]{40}", args.runtime_ref), "Expected an exact runtime commit")
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1], text=True).strip()
    require(actual == args.runtime_ref, "Checked-out runtime differs")
    runs = args.source_runs
    require(isinstance(runs, list) and len(runs) <= 128 and all(type(run) is int and run > 0 for run in runs)
            and len(set(runs)) == len(runs), "Source runs must be distinct GitHub run IDs")
    args.source_runs = list(map(str, runs))
    if args.worker:
        execute(args)
    else:
        require(not args.workdir.exists(), "Use a fresh aggregate work directory")
        command = [sys.executable, "-B", str(Path(__file__).resolve()), *sys.argv[1:], "--worker"]
        supervise(args, command=command, limits=LIMITS)


if __name__ == "__main__":
    main()
