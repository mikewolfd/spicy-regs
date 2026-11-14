#!/usr/bin/env python3
"""Repair explicitly pinned unknown-size refusals from original bytes without source GETs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import time
from types import SimpleNamespace
import zipfile

import polars as pl

from read_comment_attachment_group import digest, require, publish_stage, supervise, LIMITS, SOURCE_BYTES
from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs import operational_runner as owner
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.pipelines import comment_fields as cf
from spicy_regs.pipelines.comment_fields_evidence import verified_part, write_evidence, commit_checkpoint
from spicy_regs.transforms.regulations_shape import comment_size_normalizations, shape_record
from spicy_regs.transforms import regulations_shape

FORMAT = "comment-attachment-size-repair/1"
REFUSAL = "attachment.fileFormats.size must state a nonnegative 64-bit byte count"
MAX_ARCHIVE_BYTES = 64 * 2**20


def producer(args):
    """Name the actual code; dirty local probes cannot enter remote admission."""
    root = Path(__file__).resolve().parents[1]
    paths = [
        Path(__file__),
        Path(cf.__file__),
        Path(regulations_shape.__file__),
        root / "src/spicy_regs/pipelines/comment_fields_evidence.py",
    ]
    candidate = getattr(args, "candidate", False)
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    require(actual == args.runtime_ref, "Repair runtime differs")
    if not candidate:
        for path in paths:
            committed = subprocess.check_output(["git", "show", f"{actual}:{path.relative_to(root)}"], cwd=root)
            require(hashlib.sha256(committed).hexdigest() == digest(path), "Repair code differs from its commit")
    require(not candidate or not args.publish, "Local candidate repairs cannot publish")
    return {
        "runtime_ref": actual,
        "reader_sha256": cf._reader_sha256(),
        "executor_sha256": digest(Path(__file__)),
        "shape_sha256": digest(Path(regulations_shape.__file__)),
        "candidate_only": candidate,
    }


def finite_seconds(value):
    return type(value) in (int, float) and math.isfinite(value) and value >= 0


def source_usage(value):
    require(
        set(value) == {"attempts", "bytes"} and all(type(n) is int and n >= 0 for n in value.values()),
        "Invalid source usage",
    )
    return value


def pinned_json(path, sha256):
    require(digest(path) == sha256, f"Pinned input differs: {path.name}")
    return owner.read_json(path)


def recovery_header(archive, request, workdir):
    """Refuse corrupt, exhausted or oversized recovery before extracting it."""
    pin = request["previous"]["archive"]
    require(
        archive.is_file()
        and not archive.is_symlink()
        and 0 < pin["bytes"] <= MAX_ARCHIVE_BYTES
        and archive.stat().st_size == pin["bytes"]
        and digest(archive) == pin["sha256"],
        "Original recovery archive differs",
    )
    with zipfile.ZipFile(archive) as source:
        members = source.infolist()
        extracted = sum(item.file_size for item in members)
        require(len(members) <= 4096 and extracted <= MAX_ARCHIVE_BYTES, "Recovery exceeds its extracted byte limit")
        require(len({item.filename for item in members}) == len(members), "Recovery repeats a member")
        for item in members:
            path = Path(item.filename)
            require(
                str(path) == item.filename.rstrip("/")
                and not path.is_absolute()
                and ".." not in path.parts
                and not item.flag_bits & 1
                and item.file_size <= 4 * 2**20
                and not (item.external_attr >> 16) & 0o170000 == 0o120000,
                "Unsafe recovery member",
            )

        def document(name, maximum=65536):
            require(source.getinfo(name).file_size <= maximum, "Oversized recovery control")
            return json.loads(source.read(name))

        execution, usage = document("execution.json"), source_usage(document("source-usage.json"))
        master = document("inputs/groups.json", 512 * 1024)
        [group] = [g for g in master["groups"] if g["group"] == request["group"]]
        require(
            execution["status"] == "failed"
            and execution["limits"] == LIMITS
            and not execution["remaining_processes"]
            and finite_seconds(execution["elapsed_seconds"])
            and execution["peaks"]["rss"] <= LIMITS["rss_bytes"]
            and execution["peaks"]["allocated"] <= LIMITS["output_bytes"]
            and execution["last"]["free"] >= LIMITS["stop_free_bytes"],
            "Prior resource bounds differ or were exhausted",
        )
        supplementary = source_usage(request["supplementary_source_usage"])
        require(finite_seconds(request["supplementary_active_seconds"]), "Invalid supplementary active time")
        remaining = LIMITS["wall_seconds"] - execution["elapsed_seconds"] - request["supplementary_active_seconds"]
        require(remaining > 0, "No cumulative active time remains")
        require(
            usage["attempts"] + supplementary["attempts"] <= 2 * group["keys"]
            and usage["bytes"] + supplementary["bytes"] <= SOURCE_BYTES,
            "Cumulative source budget exhausted",
        )
        reserve = 2 * extracted + 256 * 2**20
        require(
            reserve <= LIMITS["output_bytes"]
            and shutil.disk_usage(workdir).free >= reserve + LIMITS["stop_free_bytes"],
            "Recovery scratch admission refused",
        )
        return {
            "remaining_seconds": remaining,
            "extracted_bytes": extracted,
            "reserved_bytes": reserve,
            "limits": {**LIMITS, "wall_seconds": remaining},
        }


def restore(archive, request, destination):
    header = recovery_header(archive, request, destination.parent)
    require(not destination.exists(), "Recovery destination already exists")
    with zipfile.ZipFile(archive) as source:
        source.extractall(destination)
    return header


def link(source, target):
    target.parent.mkdir(parents=True, exist_ok=True)
    os.link(source, target)


def repair(args):
    started = time.monotonic()
    request = pinned_json(args.request, args.request_sha256)
    actual_producer = producer(args)
    require(
        request["format"] == FORMAT and request["producer"] == actual_producer,
        "Repair request does not name this exact producer",
    )
    old = request["previous"]
    prior = args.workdir / "prior"
    admission = restore(args.archive, request, prior)
    identity = pinned_json(prior / "identity.json", old["identity_sha256"])
    require(identity == old["identity"], "Prior producer identity differs")
    master = pinned_json(prior / "inputs/groups.json", identity["groups_sha256"])
    require(
        identity["group"] == request["group"]
        and master["reader_sha256"] == identity["reader_sha256"]
        and master["format"] == "comment-source-groups/1"
        and master["profile"] == "attachments",
        "Repair group or original reader differs",
    )
    [group] = [item for item in master["groups"] if item["group"] == request["group"]]
    require(group["local"] == 0 and 0 < group["keys"] <= 250_000, "Repair source group is not admitted")
    for name, field in (("manifest.parquet", "manifest"), ("source-pins.parquet", "source_pins")):
        if pin := group[field]:
            require(
                (prior / "inputs" / name).stat().st_size == pin["bytes"]
                and digest(prior / "inputs" / name) == pin["sha256"],
                "Original group input differs",
            )
    [plan] = list((prior / "read/plan").glob("*.parquet"))
    metadata = pinned_json(plan.with_suffix(".json"), old["plan_receipt_sha256"])
    require(
        metadata["reader_sha256"] == identity["reader_sha256"]
        and metadata["profile"] == "attachments"
        and metadata["observe_source"]
        and digest(plan) == metadata["plan_sha256"]
        and metadata["manifest_sha256"] == group["manifest"]["sha256"]
        and metadata["source_pins_sha256"] == (group["source_pins"]["sha256"] if group["source_pins"] else None),
        "Original plan differs",
    )
    planned = pl.read_parquet(plan)
    manifest = pl.read_parquet(prior / "inputs/manifest.parquet")
    require(
        planned["key"].sort().equals(manifest["key"].sort()) and planned["key"].n_unique() == group["keys"],
        "Plan membership differs from the original group",
    )
    previous_execution = owner.read_json(prior / "execution.json")
    require(
        previous_execution["status"] == "failed"
        and not previous_execution["remaining_processes"]
        and previous_execution["limits"] == LIMITS
        and finite_seconds(previous_execution["elapsed_seconds"]),
        "Repair needs a stopped unsuccessful source job",
    )
    usage = source_usage(owner.read_json(prior / "source-usage.json"))
    supplementary = source_usage(request["supplementary_source_usage"])
    total_usage = {name: usage[name] + supplementary[name] for name in ("attempts", "bytes")}
    extra_seconds = request["supplementary_active_seconds"]
    require(
        finite_seconds(extra_seconds)
        and previous_execution["elapsed_seconds"] + extra_seconds < LIMITS["wall_seconds"],
        "No cumulative active time remains",
    )
    require(
        total_usage["attempts"] <= 2 * group["keys"] and total_usage["bytes"] <= SOURCE_BYTES,
        "Original and supplementary source usage exceed the retained budget",
    )
    attempts = (prior / "usage/attempts.bin").read_bytes()
    byte_ledger = (prior / "usage/bytes.bin").read_bytes()
    require(
        set(attempts) <= {ord("1")}
        and len(byte_ledger) % 8 == 0
        and len(attempts) == usage["attempts"]
        and sum(item[0] for item in struct.iter_unpack("<Q", byte_ledger)) == usage["bytes"],
        "Retained source ledger differs",
    )
    replacements = {item["key"]: item for item in request["records"]}
    require(
        0 < len(replacements) == len(request["records"]) <= 5 and set(replacements) <= set(planned["key"]),
        "Repair requires distinct explicit refused keys",
    )
    archive = CentralRawArchiveStore(args.raw_root, create=False)
    read = args.workdir / "read"
    repaired, unchanged, checkpoints, changed = [], [], [], []
    accepted_rows = 0
    seen = set()
    for (agency, chunk), members in planned.group_by(["agency", "chunk"], maintain_order=True):
        keys = members["key"].to_list()
        part = cf.part_path(prior / "read", plan.stem, agency, chunk)
        check = verified_part(part, metadata["plan_sha256"], keys, "attachments")
        require(bool(check), "Every old chunk must have a verified checkpoint")
        accepted_rows += check["keys"] - check["unresolved"]
        target = read / part.relative_to(prior / "read")
        evidence = part.parent / check["evidence_file"]
        selected = set(keys) & replacements.keys()
        if not check["unresolved"]:
            require(not selected, "A selected repair key already succeeded")
            for source in (part, part.with_suffix(".checkpoint.json"), evidence):
                link(source, read / source.relative_to(prior / "read"))
            unchanged.append(
                {
                    "path": str(part.relative_to(prior)),
                    "checkpoint_sha256": digest(part.with_suffix(".checkpoint.json")),
                    **check,
                }
            )
            checkpoints.append(check)
            continue
        before = pl.read_parquet(part)
        observations = pl.read_parquet(evidence)
        failures = observations.filter(pl.col("status") != "read").to_dicts()
        require({item["key"] for item in failures} == selected, "Unselected unresolved records remain")
        added = []
        for failure in failures:
            key, pin = failure["key"], replacements[failure["key"]]
            require(
                failure["status"] == "refused"
                and failure["reason"] == REFUSAL
                and pin["checkpoint_sha256"] == digest(part.with_suffix(".checkpoint.json"))
                and pin["raw_sha256"] == failure["sha256"].hex()
                and pin["raw_bytes"] == failure["size"]
                and pin["etag"] == failure["etag"]
                and pin["version_id"] == failure["version_id"],
                "Repair does not match the exact source refusal",
            )
            with archive.open("sha256:" + pin["raw_sha256"]) as stream:
                body = stream.read(pin["raw_bytes"] + 1)
            require(
                len(body) == pin["raw_bytes"] and hashlib.sha256(body).hexdigest() == pin["raw_sha256"],
                "Retained original differs from failed observation",
            )
            raw = json.loads(body)
            normalizations = comment_size_normalizations(raw)
            require(normalizations and normalizations == pin["normalizations"], "Exact unknown-size occurrences differ")
            row = cf._attachment_part_row(SimpleNamespace(key=key, payload=raw))
            require(row["comment_id"] == cf.comment_key_identity(key), "Repaired source identity differs")
            # Exercise the actual native normalizer as well as the staged source projection.
            native = shape_record("comments", {name: value for name, value in row.items() if name != "key"})
            require(comment_size_normalizations(native["raw_source_record"]), "Normalization lost its literal input")
            added.append(row)
            seen.add(key)
            repaired.append(
                {
                    "key": key,
                    "raw_sha256": pin["raw_sha256"],
                    "raw_bytes": len(body),
                    "prior_checkpoint": check,
                    "normalizations": normalizations,
                    "source_modify_date": raw["data"]["attributes"].get("modifyDate"),
                }
            )
        combined = pl.concat([before, pl.DataFrame(added, schema=before.schema)]).sort("key")
        require(
            combined["key"].sort().to_list() == sorted(keys)
            and combined.filter(~pl.col("key").is_in(list(selected))).sort("key").equals(before.sort("key")),
            "Repair changed prior successful rows or group membership",
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        combined.write_parquet(target, compression="zstd")
        rows = observations.to_dicts()
        for row in rows:
            if row["key"] in selected:
                row.update(status="read", reason=None)
        new_evidence = write_evidence(target, rows)
        commit_checkpoint(target, metadata["plan_sha256"], new_evidence, len(keys), 0, "attachments")
        derived = verified_part(target, metadata["plan_sha256"], keys, "attachments")
        checkpoints.append(derived)
        changed.append(
            {
                "path": str(part.relative_to(prior)),
                "previous_checkpoint": check,
                "previous_checkpoint_sha256": digest(part.with_suffix(".checkpoint.json")),
                "derived_checkpoint": derived,
                "derived_checkpoint_sha256": digest(target.with_suffix(".checkpoint.json")),
                "preserved_rows": len(before),
                "added_keys": sorted(selected),
            }
        )
    require(seen == replacements.keys() and len(checkpoints) == metadata["chunks"], "Repair coverage differs")
    # stage() verifies the derived parts. The original plan is attached afterward:
    # it describes the original reader, never a claim that it produced repaired rows.
    cf.WRITE_RESOURCES = ExportResources(memory="256MB", threads=1)
    staged = cf.stage(read, args.workdir / "stage", profile="attachments")
    require(staged["file"]["bytes"] <= 64 * 2**20, "Derived stage exceeds aggregate member limit")
    staged.update(plan_sha256=metadata["plan_sha256"], observe_source=True, source_checkpoints=checkpoints)
    owner.durable(args.workdir / "stage/staging.json", staged)
    for source in (plan, plan.with_suffix(".json")):
        link(source, read / source.relative_to(prior / "read"))
    elapsed = time.monotonic() - started
    require(
        previous_execution["elapsed_seconds"] + extra_seconds + elapsed <= LIMITS["wall_seconds"],
        "Cumulative active time exceeded",
    )
    require(producer(args) == actual_producer, "Producer changed during repair")
    report = {
        "format": FORMAT,
        "request_sha256": args.request_sha256,
        "previous": old,
        "group": request["group"],
        "groups_sha256": identity["groups_sha256"],
        "producer": actual_producer,
        "resource_admission": admission,
        "plan_role": "Original reader selection; added rows use the explicitly named repair producer",
        "preserved_rows": accepted_rows,
        "added_rows": len(repaired),
        "changed_checkpoints": changed,
        "unchanged_checkpoints": unchanged,
        "repaired_records": repaired,
        "prior_source_usage": usage,
        "supplementary_source_usage": supplementary,
        "source_usage": total_usage,
        "new_source_gets": 0,
        "previous_active_seconds": previous_execution["elapsed_seconds"],
        "repair_seconds": elapsed,
        "supplementary_active_seconds": extra_seconds,
        "checks": {
            "all original checkpoints verified": True,
            "all unselected rows unchanged": True,
            "every selected original matches its failed observation": True,
            "all planned keys staged": staged["rows"] == group["keys"],
            "no unresolved source keys": all(check["unresolved"] == 0 for check in checkpoints),
            "prior accepted rows plus exact additions conserve keys": accepted_rows + len(repaired) == group["keys"],
        },
    }
    require(all(report["checks"].values()), "Repair conservation checks failed")
    owner.durable(args.workdir / "repair.json", report)
    uploaded = None
    if args.publish:
        from spicy_regs.sources.r2 import verified_target_client

        client, bucket = verified_target_client()
        uploaded = publish_stage(args.workdir / "stage", client, bucket)
    result = {
        **report["producer"],
        "groups_sha256": identity["groups_sha256"],
        "group": request["group"],
        "status": "complete",
        "keys": group["keys"],
        "group_descriptor": group,
        "stage_file": staged["file"],
        "upload": uploaded,
        "repair_sha256": digest(args.workdir / "repair.json"),
        "read": {
            "chunks": len(checkpoints),
            "keys": group["keys"],
            "rows": staged["rows"],
            "unresolved": 0,
            "deferred_chunks": 0,
            "stopped": None,
        },
        "source": {
            "planned_keys": group["keys"],
            "read_keys": group["keys"],
            "planned_chunks": len(checkpoints),
            "read_chunks": len(checkpoints),
            "unresolved_keys": 0,
        },
    }
    owner.durable(args.workdir / "source-usage.json", total_usage)
    owner.durable(args.workdir / "result.json", result)
    shutil.copyfile(args.request, args.workdir / "repair-request.json")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("request", "archive", "raw-root", "workdir"):
        parser.add_argument("--" + name, required=True, type=Path)
    parser.add_argument("--request-sha256", required=True)
    parser.add_argument("--runtime-ref", required=True)
    parser.add_argument("--publish", action="store_true")
    parser.add_argument("--candidate", action="store_true", help="Allow pinned uncommitted code for local proof only")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    actual = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1], text=True
    ).strip()
    require(actual == args.runtime_ref, "Repair runtime differs")
    request = pinned_json(args.request, args.request_sha256)
    if args.worker:
        repair(args)
    else:
        require(not args.workdir.exists(), "Use a fresh repair work directory")
        args.workdir.parent.mkdir(parents=True, exist_ok=True)
        header = recovery_header(args.archive, request, args.workdir.parent)
        command = [sys.executable, "-B", str(Path(__file__).resolve()), *sys.argv[1:], "--worker"]
        supervise(args, command=command, limits={**LIMITS, "wall_seconds": header["remaining_seconds"]})


if __name__ == "__main__":
    main()
