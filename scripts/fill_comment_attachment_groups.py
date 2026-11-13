#!/usr/bin/env python3
"""Plan whole-file attachment fills, then run one pinned group through the maintained writer."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import sys

if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import read_comment_attachment_group as runner
from spicy_regs import comments_maintenance as maintenance, operational_runner as owner
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines import comment_fields_groups as groups, comment_fields_write as writer
from spicy_regs.sources import iceberg
from spicy_regs.sources.publication import _put_immutable
from spicy_regs.sources.r2 import verified_target_client

GiB = 2**30
LIMITS = {**runner.LIMITS, "wall_seconds": 300 * 60, "rss_bytes": 8 * GiB, "output_bytes": 6 * GiB}
RESOURCES = ExportResources(memory="2GB", threads=2)
MAX_ROWS, MAX_VALUES = 1_000_000, 512 * 2**20
SPILL_BYTES = GiB
require, digest = runner.require, runner.digest


def pin(path):
    return {"bytes": path.stat().st_size, "sha256": digest(path)}


def load(path):
    return json.loads(path.read_text())


def key(value):
    parsed = PurePosixPath(value)
    require(value.startswith("staging/") and ".." not in parsed.parts and str(parsed) == value, "Staging key required")
    return value


def fetch(client, bucket, descriptor, path, maximum):
    require(re.fullmatch(r"[0-9a-f]{64}", descriptor["sha256"]), "Full SHA-256 required")
    runner.checked_download(client, bucket, key(descriptor["key"]), path, descriptor["sha256"], maximum,
                            descriptor.get("bytes"))


def publish(client, bucket, path, prefix):
    """Retain a small checked control alongside the maintained immutable stage upload."""
    identity = pin(path)
    target = key(f"{prefix}/{path.name}")
    _put_immutable(client, bucket, target, path, sha256="sha256:" + identity["sha256"])
    reply = client.get_object(Bucket=bucket, Key=target)
    try:
        import hashlib
        observed, size = hashlib.sha256(), 0
        while block := reply["Body"].read(1 << 20):
            size += len(block)
            require(size <= identity["bytes"], "Stored control exceeds expected bytes")
            observed.update(block)
        require(size == identity["bytes"] and observed.hexdigest() == identity["sha256"], "Stored control differs")
    finally:
        reply["Body"].close()
    return {"key": target, **identity}


def admit(root, *, download=0, reserve=0):
    """Account retained and proposed bytes before work; the watchdog enforces actual growth."""
    held = owner.sizes(root, float("inf"))
    used = max(held["logical"], held["allocated"])
    require(used + download + reserve < LIMITS["output_bytes"] - 65536, "Owned input/recovery/spill admission exceeds 6GiB")
    require(shutil.disk_usage(root).free - download - reserve >= LIMITS["stop_free_bytes"], "Disk reserve admission refused")
    return {"held_bytes": used, "download_bytes": download, "reserved_bytes": reserve}


def make_plan(args, client, bucket):
    stage = args.workdir / "aggregate"
    stage.mkdir(exist_ok=True)
    fetch(client, bucket, args.config["staging"], stage / "staging.json", 16 * 2**20)
    receipt = load(stage / "staging.json")
    file = receipt["file"]
    require(file["sha256"] == args.config["reads"]["sha256"], "Aggregate stage/receipt differ")
    admit(args.workdir, download=file["bytes"], reserve=2 * GiB)
    fetch(client, bucket, {**args.config["reads"], "bytes": file["bytes"]}, stage / "reads.parquet", 4 * GiB)
    planned = groups.plan(args.workdir / "physical", stage, max_rows=MAX_ROWS, max_read_bytes=MAX_VALUES, max_spill="1GB")
    counts = Counter(g["original_file"] for g in planned["groups"])
    split = [name for name, n in counts.items() if n != 1]
    require(not split, "Physical files exceed whole-file bounds; retain plan for separately qualified bounds: " + repr(split[:3]))
    staged = []
    for group in planned["groups"]:
        out = args.workdir / "group"
        admit(args.workdir, reserve=MAX_VALUES + SPILL_BYTES)
        receipt = groups.stage_group(args.workdir / "physical", group["group_id"], out)
        uploaded = runner.publish_stage(out, client, bucket)
        control = publish(client, bucket, out / "staging.json", "staging/comment-attachment-stage-" + digest(out / "staging.json"))
        staged.append({**group, "reads": {"key": uploaded["key"], **pin(out / "reads.parquet")}, "staging": control})
        shutil.rmtree(out)  # Exact stage and receipt were read back from immutable storage.
    bundle = {"format": "comment-physical-fill/1", "runtime_ref": args.runtime_ref,
              "aggregate": args.config, "groups": staged, "physical_plan": pin(args.workdir / "physical/groups.json"),
              "snapshot": planned["snapshot"], "outcomes": planned["outcomes"],
              "journals": str(PurePosixPath(args.config["reads"]["key"]).parent / "journals")}
    target = args.workdir / "plan.json"
    owner.durable(target, bundle)
    prefix = "staging/comment-physical-fill-" + digest(target)
    controls = {name: publish(client, bucket, args.workdir / "physical" / name, prefix + "/physical")
                for name in ("groups.json", "members.parquet", "excluded.parquet")}
    owner.durable(args.workdir / "result.json", {"plan": publish(client, bucket, target, prefix), "controls": controls})


def selected(args, client, bucket):
    target = args.workdir / "plan.json"
    fetch(client, bucket, args.config["plan"], target, 16 * 2**20)
    plan = load(target)
    require(plan["format"] == "comment-physical-fill/1" and plan["runtime_ref"] == args.runtime_ref, "Plan runtime/format differs")
    require(len({g["original_file"] for g in plan["groups"]}) == len(plan["groups"]), "Plan splits a physical file")
    return plan


def choose(args, client, bucket):
    if args.operation == "plan":
        return ["plan"]
    plan = selected(args, client, bucket)
    ids = [g["group_id"] for g in plan["groups"]]
    if args.operation == "fill":
        fetch(client, bucket, args.config["pilot"], args.workdir / "pilot.json", 2**20)
        pilot = load(args.workdir / "pilot.json")
        pilot_group = next((g for g in plan["groups"] if g["group_id"] == pilot["group"]), None)
        require(pilot["pilot"] is True and pilot["status"] == "complete" and pilot["written"]["rows_changed"] > 0
                and pilot["plan_sha256"] == args.config["plan"]["sha256"] and pilot["runtime_ref"] == args.runtime_ref,
                "A successful writing pilot for this exact plan/runtime is required")
        require(pilot_group is not None and pilot["stage_sha256"] == pilot_group["reads"]["sha256"], "Pilot group stage differs")
        choices = args.config.get("groups", [i for i in ids if i != pilot["group"]])
        require(choices and len(set(choices)) == len(choices) and all(i in ids and i != pilot["group"] for i in choices),
                "Remaining groups must be unique known groups excluding the pilot")
    else:
        choices = [args.config["group"]]
        require(choices[0] in ids, "Choose an explicit known physical group")
    require(len(choices) <= 256, "Select at most 256 physical groups per workflow")
    return choices


def pull_journals(workdir, prefix, client):
    """Merge only prefix-compatible histories; never overwrite a newer retained recovery journal."""
    incoming = workdir / "incoming"
    writer.sync_journals(incoming, prefix, push=False, client=client)
    out = writer.fill_dir(workdir / "writer")
    out.mkdir(parents=True, exist_ok=True)
    for path in writer.fill_dir(incoming).glob("*.jsonl"):
        target = out / path.name
        prior = target.read_bytes() if target.exists() else b""
        remote = path.read_bytes()
        require(prior.startswith(remote) or remote.startswith(prior), "Journal histories diverged")
        if len(remote) > len(prior):
            target.write_bytes(remote)
    shutil.rmtree(incoming)


def pending_guard(workdir):
    writer._refuse_committed_failures(workdir)
    prepared_path = writer.fill_dir(workdir) / "prepare.json"
    prepared = load(prepared_path) if prepared_path.exists() else None
    for journal in writer.fill_dir(workdir).glob("write-journal-*.jsonl"):
        lines = writer._lines(journal)
        if lines and lines[-1]["state"] == "pending":
            line = lines[-1]
            require(prepared is not None and line["prepare_id"] == prepared["prepare_id"],
                    "Pending write needs its exact earlier recovery artifact; do not prepare another group")
            require(Path(line["preimage"]).is_file() and digest(Path(line["preimage"])) == line["preimage_sha256"],
                    "Pending write lacks its verified preimage")


def pilot_history(workdir, pilot):
    prepare_id = pilot["prepared"]["prepare_id"]
    require(re.fullmatch(r"[0-9]+-[0-9a-f]{12}-[0-9a-f]{8}", prepare_id), "Pilot prepare identity differs")
    lines = writer._lines(writer._journal(workdir, {"prepare_id": prepare_id}))
    require(lines and all(line.get("prepare_id") == prepare_id for line in lines), "Pilot journal is missing or differs")
    terminal = {line["batch"]: line for line in lines}
    require(not any(line["state"] == "undone" for line in lines)
            and all(line["state"] == "verified" for line in terminal.values())
            and sum(line.get("changed", 0) for line in terminal.values()) == pilot["written"]["rows_changed"],
            "Pilot is undone, unresolved, or differs from its retained success")


def recovery_files(root):
    files = [f for folder in ("writer/fill", "group") for f in (root / folder).rglob("*")
             if f.is_file() and "spill" not in f.parts]
    return files + [root / name for name in ("maintenance-state.json", "prior-maintenance-state.json") if (root / name).is_file()]


def recovery(args, *, verify=False, maintenance_changed=False):
    path = args.workdir / "recovery.json"
    if verify:
        require(path.is_file() and digest(path) == args.config["recovery_sha256"], "Recovery receipt differs")
        data = load(path)
        require(data["identity"] == identity(args), "Recovery runtime/plan/group differs")
        expected_files = data["files"]
        actual = {str(f.relative_to(args.workdir)) for f in recovery_files(args.workdir)}
        if maintenance_changed:
            # The supervisor verified these exact prior settings before restoring and capturing the new guard.
            names = {"maintenance-state.json", "prior-maintenance-state.json"}
            actual -= names
            expected_files = {name: value for name, value in expected_files.items() if name not in names}
        require(actual == set(expected_files), "Recovery contains missing or unlisted files")
        for name, expected in expected_files.items():
            file = args.workdir / name
            require(".." not in Path(name).parts and not Path(name).is_absolute() and not file.is_symlink()
                    and file.is_file() and pin(file) == expected, "Recovery member differs: " + name)
        return data
    files = {str(file.relative_to(args.workdir)): pin(file) for file in recovery_files(args.workdir)}
    data = {"identity": identity(args), "files": files}
    owner.durable(path, data)
    (args.workdir / "recovery.json.sha256").write_text(digest(path) + "  recovery.json\n")
    return data


def identity(args):
    return {"plan_sha256": args.config["plan"]["sha256"], "group": int(args.group), "runtime_ref": args.runtime_ref}


def run_group(args, client, bucket):
    require(int(args.group) in choose(args, client, bucket), "Group is outside the approved operation")
    plan = selected(args, client, bucket)
    choice = [g for g in plan["groups"] if g["group_id"] == int(args.group)]
    require(len(choice) == 1, "Unknown physical group")
    group = choice[0]
    require(group["identities"] <= MAX_ROWS and group["read_value_bytes"] <= MAX_VALUES, "Group exceeds qualified bounds")
    if args.operation in {"resume", "undo"}:
        authorized = args.workdir / "authorized.json"
        recovery(args, verify=True, maintenance_changed=authorized.exists() and load(authorized) == ownership(args))
    elif (args.workdir / "writer/fill/prepare.json").exists():
        raise ValueError("Existing writer state requires explicit resume")
    pull_journals(args.workdir, plan["journals"], client)
    work = args.workdir / "writer"
    if args.operation != "undo":
        pending_guard(work)
        if args.operation == "fill":
            pilot_history(work, load(args.workdir / "pilot.json"))
        prepared_path = writer.fill_dir(work) / "prepare.json"
        if args.operation == "resume" and prepared_path.exists():
            history = writer._lines(writer._journal(work, load(prepared_path)))
            require(not any(line["state"] == "undone" for line in history),
                    "This prepare was undone; use a fresh explicit pilot/prepare instead of resume")
    else:
        maintenance.run("verify", args.workdir / "maintenance-state.json")
        result = writer.undo(work, args.config["undo_batch"], expected_snapshot=args.config["expected_snapshot"])
        owner.durable(args.workdir / "result.json", {**identity(args), "operation": "undo", "status": "complete", "undo": result})
        return
    attempt = writer.fill_dir(work) / "attempt.json"
    attempt.parent.mkdir(parents=True, exist_ok=True)
    if not attempt.exists():
        owner.durable(attempt, {**identity(args), "operation": args.operation})
    else:
        require(all(load(attempt)[name] == value for name, value in identity(args).items()), "Writer attempt identity differs")
    stage = args.workdir / "group"
    needed = sum(g["bytes"] for k, g in group.items() if k in {"reads", "staging"}) if not (stage / "reads.parquet").exists() else 0
    admit(args.workdir, download=needed, reserve=3 * GiB)
    fetch(client, bucket, group["staging"], stage / "staging.json", 2**20)
    fetch(client, bucket, group["reads"], stage / "reads.parquet", MAX_VALUES)
    groups._input(stage)
    with iceberg._connect() as con:
        con.execute("SET max_temp_directory_size='1GB'")
        if args.operation == "resume" and (writer.fill_dir(work) / "prepare.json").exists():
            prepared = load(writer.fill_dir(work) / "prepare.json")
            require(prepared["profile"] == "attachments" and prepared["reads"] == str(stage / "reads.parquet"), "Prepared input differs")
        else:
            if args.operation == "resume":
                # A stopped prepare has no commit/pending journal (checked above). Its work files are rebuildable;
                # preserve all journals/images and the prior artifact, removing only these incomplete query outputs.
                for name in ("work", "spill"):
                    shutil.rmtree(writer.fill_dir(work) / name, ignore_errors=True)
                for name in ("fill.parquet", "files.parquet", "refusals.parquet"):
                    (writer.fill_dir(work) / name).unlink(missing_ok=True)
            prepared = writer.prepare(work, reads=stage / "reads.parquet", profile="attachments", resources=RESOURCES, con=con)
        # Compressed native bytes underestimate restored receipt values. Reserve at least 1GiB for images and
        # twice the touched compressed files, plus source values and bounded spill; the actual sampler remains authoritative.
        preimages = max(GiB, 2 * sum(f["bytes"] for f in prepared["files"].values()) + group["read_value_bytes"])
        admission = admit(args.workdir, reserve=preimages + SPILL_BYTES)
        owner.durable(args.workdir / "admission.json", {**admission, "estimated_preimage_bytes": preimages, "group": group})
        maintenance.run("verify", args.workdir / "maintenance-state.json")
        written = writer.write(work, resources=RESOURCES, con=con)
    total = sum(line.get("changed", 0) for line in writer._lines(writer._journal(work, prepared)) if line["state"] == "verified")
    written = {**written, "new_rows_changed": written["rows_changed"], "rows_changed": total}
    owner.durable(args.workdir / "result.json", {**identity(args), "operation": args.operation, "status": "complete",
                   "pilot": load(writer.fill_dir(work) / "attempt.json")["operation"] == "pilot",
                   "stage_sha256": group["reads"]["sha256"],
                   "prepared": {k: prepared[k] for k in ("rows_to_fill", "snapshot", "prepare_id")}, "written": written})


def execute(args):
    client, bucket = verified_target_client()
    (make_plan if args.operation == "plan" else run_group)(args, client, bucket)


def ownership(args):
    return {**identity(args), "attempt": os.getenv("GITHUB_RUN_ID", f"local-{os.getpid()}")
            + ":" + os.getenv("GITHUB_RUN_ATTEMPT", "1")}


def finalize(args):
    """Safe to repeat from the workflow's always step if the supervisor itself was interrupted."""
    authorized = args.workdir / "authorized.json"
    if not authorized.exists() or load(authorized) != ownership(args):
        # In particular, a rejected downloaded artifact must never be pushed back into shared journal history.
        return
    # A failed repeat must not leave an earlier descriptor claiming to pin the newly retained artifact.
    (args.workdir / "published-result.json").unlink(missing_ok=True)
    try:
        maintenance.run("restore", args.workdir / "maintenance-state.json")
    finally:
        try:
            if (args.workdir / "plan.json").exists():
                require(digest(args.workdir / "plan.json") == args.config["plan"]["sha256"], "Recovery plan differs")
                client, _ = verified_target_client()
                prefix = load(args.workdir / "plan.json")["journals"]
                # Even a verified artifact may lag or conflict with remote history. Check the complete merge
                # again before any push, including when the child's earlier pull was the operation that failed.
                pull_journals(args.workdir, prefix, client)
                writer.sync_journals(args.workdir / "writer", prefix, push=True, client=client)
        finally:
            recovery(args)
    completed = args.workdir / "worker-complete.json"
    result = args.workdir / "result.json"
    if completed.exists() and result.exists() and load(completed) == {**ownership(args), "result_sha256": digest(result)}:
        writer._refuse_committed_failures(args.workdir / "writer")
        require(not any(writer._lines(p)[-1]["state"] == "pending" for p in (args.workdir / "writer/fill").glob("*.jsonl")
                        if writer._lines(p)), "Pending history cannot publish completion")
        client, bucket = verified_target_client()
        final = args.workdir / "completed-result.json"
        owner.durable(final, {**load(result), "recovery": pin(args.workdir / "recovery.json"),
                              "recovery_run_id": os.getenv("GITHUB_RUN_ID"),
                              "recovery_artifact": f"comment-attachment-fill-{args.group}"})
        owner.durable(args.workdir / "published-result.json", publish(client, bucket, final,
                      "staging/comment-physical-fill-result-" + digest(final)))


def guarded(args):
    """The maintenance owner survives watchdog termination of the writer and always attempts both restorations."""
    state = args.workdir / "maintenance-state.json"
    command = [sys.executable, "-B", str(Path(__file__).resolve()), *sys.argv[1:], "--worker"]
    require(os.getenv(iceberg.CATALOG_LOCK_ENV) == "comments-catalog-write", "Hold the exact shared writer lock")
    if args.operation == "plan":
        runner.supervise(args, command=command, limits=LIMITS)
        return
    if args.operation in {"resume", "undo"}:
        recovery(args, verify=True)
    (args.workdir / "worker-complete.json").unlink(missing_ok=True)
    owner.durable(args.workdir / "authorized.json", ownership(args))
    try:
        if state.exists():
            maintenance.run("restore", state)
            state.rename(args.workdir / "prior-maintenance-state.json")
        maintenance.run("suspend", state)
        runner.supervise(args, command=command, limits=LIMITS)
        result = args.workdir / "result.json"
        require(all(load(result)[name] == value for name, value in identity(args).items()), "Worker result identity differs")
        owner.durable(args.workdir / "worker-complete.json", {**ownership(args), "result_sha256": digest(result)})
    finally:
        finalize(args)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-ref", required=True)
    parser.add_argument("--config", required=True, help="Pinned staging/plan descriptors as JSON; see runbook")
    parser.add_argument("--operation", choices=("plan", "pilot", "fill", "resume", "undo"), required=True)
    parser.add_argument("--group", default="plan")
    parser.add_argument("--workdir", required=True, type=Path)
    parser.add_argument("--matrix", action="store_true")
    parser.add_argument("--finalize", action="store_true", help="Repeat recovery retention and maintenance restoration only")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{40}", args.runtime_ref), "Exact runtime commit required")
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1], text=True).strip()
    require(actual == args.runtime_ref, "Checked-out runtime differs")
    args.config = json.loads(args.config)
    args.workdir.mkdir(parents=True, exist_ok=True)
    if args.finalize:
        require(args.operation != "plan", "Planning has no catalog recovery to finalize")
        finalize(args)
    elif args.matrix:
        client, bucket = verified_target_client()
        print(json.dumps(choose(args, client, bucket)))
    elif args.worker:
        execute(args)
    else:
        guarded(args)


if __name__ == "__main__":
    main()
