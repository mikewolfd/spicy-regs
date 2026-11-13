#!/usr/bin/env python3
"""Read one identity-complete attachment group, retain checkpoints, and stage its verified output."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import re
import signal
import struct
import subprocess
import sys
import threading
import time

from spicy_regs import operational_runner as owner

LIMITS = {"wall_seconds": 90 * 60, "interval_seconds": .1, "rss_bytes": 2 * 2**30,
          "output_bytes": 2**30, "start_free_bytes": 4 * 2**30, "stop_free_bytes": 4 * 2**30}
SOURCE_BYTES = 3 * 2**30
MAX_KEYS = 250_000


def require(value, message):
    if not value:
        raise ValueError(message)


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def checked_download(client, bucket, key, target, sha256, maximum, expected_size=None):
    """Reuse exact retained input; otherwise download bounded bytes into an owned temporary file."""
    if target.exists():
        require(target.is_file() and not target.is_symlink(), "Input is not a regular owned file")
        require(target.stat().st_size <= maximum and digest(target) == sha256, "Retained input differs from its pin")
        require(expected_size is None or target.stat().st_size == expected_size, "Retained input size differs")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    reply = client.get_object(Bucket=bucket, Key=key)
    body = reply["Body"]
    temporary = target.with_suffix(".download")
    size = 0
    try:
        with temporary.open("wb") as stream:
            while block := body.read(min(1 << 20, maximum - size + 1)):
                size += len(block)
                require(size <= maximum, "Input download exceeds its byte allowance")
                stream.write(block)
        require(expected_size is None or size == expected_size, "Input download size differs")
        require(digest(temporary) == sha256, "Input download digest differs")
        temporary.replace(target)
    finally:
        body.close()


def select_group(args, client, bucket):
    from spicy_regs.pipelines import comment_fields as cf, comment_source_groups

    require(re.fullmatch(r"runner-[0-9]{3}", args.group), "Only runner source groups are admitted")
    require(re.fullmatch(r"[0-9a-f]{64}", args.groups_sha256), "groups_sha256 must be a full SHA-256")
    key = PurePosixPath(args.groups_key)
    require(str(key).startswith("staging/") and key.name == "groups.json" and ".." not in key.parts,
            "groups_key must name a staging groups.json")
    inputs = args.workdir / "inputs"
    descriptor = inputs / "groups.json"
    checked_download(client, bucket, args.groups_key, descriptor, args.groups_sha256, 512 * 1024)
    receipt = json.loads(descriptor.read_text())
    require(receipt["format"] == "comment-source-groups/1" and receipt["profile"] == "attachments", "Group format/profile differs")
    require(receipt["implementation_sha256"] == digest(Path(comment_source_groups.__file__)), "Grouping implementation differs")
    require(receipt["reader_sha256"] == cf._reader_sha256(), "Group reader differs")
    selected = [item for item in receipt["groups"] if item["group"] == args.group]
    require(len(selected) == 1, "Source group is missing or repeated")
    group = selected[0]
    require(group["local"] == 0 and 0 < group["keys"] <= min(MAX_KEYS, receipt["max_group_keys"]), "Group custody or size refused")
    require(group["r2"] >= 0 and group["external"] >= 0 and group["r2"] + group["external"] == group["keys"], "Group counts differ")
    for field, filename in (("manifest", "manifest.parquet"), ("source_pins", "source-pins.parquet")):
        pin = group[field]
        if pin is None:
            require(field == "source_pins" and group["r2"] == 0, "Group lacks its source pins")
            continue
        require(pin["file"] == filename and 0 < pin["bytes"] <= 64 * 2**20, "Group member name or size refused")
        checked_download(client, bucket, str(key.parent / args.group / filename), inputs / filename,
                         pin["sha256"], 64 * 2**20, pin["bytes"])
    import polars as pl

    manifest = pl.read_parquet(inputs / "manifest.parquet")
    require(manifest.columns == ["key"] and len(manifest) == group["keys"]
            and manifest["key"].n_unique() == len(manifest), "Group manifest membership differs")
    if group["source_pins"] is not None:
        pins = pl.read_parquet(inputs / "source-pins.parquet")
        require(len(pins) == group["r2"] and pins["raw_custody"].eq("r2").all(), "Runner group contains non-R2 retained pins")
        require(pins["key"].is_in(manifest["key"].implode()).all(), "Source pin is outside this group")
    return group


class SourceBudgetExceeded(BaseException):
    """Stop the whole read; the source reader must not convert a resource stop into another retry."""


class SourceMeter:
    """Count actual SDK HTTP attempts and streamed bytes, retaining counters across resumed jobs."""

    def __init__(self, folder, keys, maximum_bytes=SOURCE_BYTES):
        folder.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()
        self.maximum_attempts, self.maximum_bytes = 2 * keys, maximum_bytes
        self.attempt_file = (folder / "attempts.bin").open("ab", buffering=0)
        byte_path = folder / "bytes.bin"
        self.attempts = self.attempt_file.tell()
        self.bytes = 0
        if byte_path.exists():
            with byte_path.open("rb") as stream:
                while block := stream.read(64 * 1024):
                    require(len(block) % 8 == 0, "Source byte ledger is incomplete")
                    self.bytes += sum(item[0] for item in struct.iter_unpack("<Q", block))
        self.byte_file = byte_path.open("ab", buffering=0)
        require(self.attempts <= self.maximum_attempts and self.bytes <= maximum_bytes, "Retained source usage exceeds its budget")

    def attempt(self, **_):
        with self.lock:
            if self.attempts >= self.maximum_attempts:
                raise SourceBudgetExceeded("Source HTTP attempt limit")
            self.attempt_file.write(b"1")
            self.attempts += 1

    def response(self, parsed, **_):
        if "Body" in parsed:
            parsed["Body"] = MeteredBody(parsed["Body"], self)

    def watch(self, client):
        client.meta.events.register("before-send.s3.GetObject", self.attempt)
        client.meta.events.register("after-call.s3.GetObject", self.response)

    def close(self):
        self.attempt_file.close()
        self.byte_file.close()


class MeteredBody:
    def __init__(self, body, meter):
        self.body, self.meter = body, meter

    def read(self, size=-1):
        # Small comment bodies share a short read/charge critical section; no concurrent read can exceed the allowance.
        with self.meter.lock:
            remaining = self.meter.maximum_bytes - self.meter.bytes
            if remaining <= 0 and size != 0:
                raise SourceBudgetExceeded("Source streamed-byte limit")
            amount = min(size, remaining) if size >= 0 else remaining
            value = self.body.read(amount)
            self.meter.byte_file.write(struct.pack("<Q", len(value)))
            self.meter.bytes += len(value)
            if len(value) == remaining and (size < 0 or size > amount):
                raise SourceBudgetExceeded("Source streamed-byte limit")
            return value

    def close(self):
        self.body.close()


def publish_stage(stage, client, bucket):
    """Use the maintained immutable uploader and full readback, including safe resume after a partial upload."""
    from spicy_regs.sources.publication import _put_immutable

    receipt = json.loads((stage / "staging.json").read_text())
    sha256 = receipt["file"]["sha256"]
    require(receipt["profile"] == "attachments" and digest(stage / "reads.parquet") == sha256, "Staged input differs")
    prefix = f"staging/comment-fields-fill-{sha256[:12]}"
    for name in ("reads.parquet", "reads.parquet.sha256"):
        path = stage / name
        expected = digest(path)
        _put_immutable(client, bucket, f"{prefix}/{name}", path, sha256="sha256:" + expected)
        reply = client.get_object(Bucket=bucket, Key=f"{prefix}/{name}")
        body, actual, size = reply["Body"], hashlib.sha256(), 0
        try:
            while block := body.read(1 << 20):
                size += len(block)
                require(size <= path.stat().st_size, "Stored stage exceeds its size")
                actual.update(block)
        finally:
            body.close()
        require(size == path.stat().st_size and actual.hexdigest() == expected, "Stored stage differs after immutable upload")
    return {"bucket": bucket, "key": f"{prefix}/reads.parquet", "sha256": sha256,
            "journals": f"{prefix}/journals"}


def execute(args):
    from spicy_docs.sources import mirrulations
    from spicy_regs.duckdb_settings import ExportResources
    from spicy_regs.pipelines import comment_fields as cf
    from spicy_regs.sources.publication import EVIDENCE_PREFIX, _S3Members
    from spicy_regs.sources.r2 import verified_target_client

    client, bucket = verified_target_client()
    group = select_group(args, client, bucket)
    identity = {"groups_sha256": args.groups_sha256, "group": args.group, "runtime_ref": args.runtime_ref,
                "executor_sha256": digest(Path(__file__)), "reader_sha256": cf._reader_sha256()}
    record = args.workdir / "identity.json"
    if record.exists():
        require(json.loads(record.read_text()) == identity, "Resume identity differs")
    else:
        owner.durable(record, identity)
    inputs, work, stage = args.workdir / "inputs", args.workdir / "read", args.workdir / "stage"
    cf.require_record_shape()
    cf.PLAN_RESOURCES = ExportResources(memory="256MB", threads=1)
    cf.WRITE_RESOURCES = ExportResources(memory="256MB", threads=1)
    plan = cf.plan(work, inputs / "manifest.parquet", source_pins=(inputs / "source-pins.parquet") if group["source_pins"] else None,
                   observe_source=True, profile="attachments", chunk_keys=1_000, max_spill="256MB")
    source = mirrulations.s3_resource(max_pool_connections=args.workers)
    evidence_client, evidence_bucket = verified_target_client()
    meter = SourceMeter(args.workdir / "usage", group["keys"])
    try:
        meter.watch(source.meta.client)
        meter.watch(evidence_client)
        read = cf.read(work, workers=args.workers, max_rss_mb=1_800, min_free_gb=4, resource=source,
                       evidence_store=_S3Members(evidence_client, evidence_bucket, EVIDENCE_PREFIX))
    finally:
        owner.durable(args.workdir / "source-usage.json", {"attempts": meter.attempts, "bytes": meter.bytes})
        meter.close()
    status = cf.status(work)
    require(not read["deferred_chunks"] and not read["unresolved"] and read["stopped"] is None,
            "Source read stopped or deferred keys; checkpoints retained")
    require(status["read_keys"] == group["keys"] and status["read_chunks"] == status["planned_chunks"]
            and status["unresolved_keys"] == 0, "Source group is incomplete; checkpoints retained")
    if not (stage / "staging.json").exists():
        # Preserve an interrupted stage for review; it cannot satisfy the new stage.
        if stage.exists():
            stage.rename(args.workdir / f"interrupted-stage-{time.time_ns()}")
        cf.stage(work, stage, profile="attachments")
    staged = json.loads((stage / "staging.json").read_text())
    require(staged["plan_sha256"] == cf._pinned_plan(plan)[0] and staged["rows"] == group["keys"]
            and staged["columns"] == list(cf.read_columns("attachments")), "Retained stage does not belong to this group")
    uploaded = publish_stage(stage, client, bucket)
    result = {**identity, "status": "complete", "keys": group["keys"], "group_descriptor": group,
              "stage_file": staged["file"], "read": read, "source": status, "upload": uploaded}
    owner.durable(args.workdir / "result.json", result)


def supervise(args, *, command=None, limits=None):
    args.workdir.mkdir(parents=True, exist_ok=True)
    require(not args.workdir.is_symlink(), "Work directory must not be a symlink")
    started, known, child, leader = time.monotonic(), {}, None, None
    limits = limits or LIMITS
    budget = owner.Budget(args.workdir, {"limits": limits}, started)
    result = {"status": "failed", "limits": limits}
    def interrupted(signum, _frame):
        raise InterruptedError(f"Source-group supervisor received signal {signum}")

    previous_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        budget.tick(force=True)
        with (args.workdir / "execution.log").open("ab") as log:
            command = command or [sys.executable, "-B", str(Path(__file__).resolve()), *sys.argv[1:], "--worker"]
            child = subprocess.Popen(command,
                                     stdout=log, stderr=log, start_new_session=True)
            leader = owner.birth(child.pid)
            known[child.pid] = leader
            budget.identities = lambda: owner.members(leader, known)
            while child.poll() is None:
                budget.tick(force=True)
                time.sleep(.1)
            require(child.returncode == 0, f"Source-group child exited {child.returncode}; checkpoints retained")
            require(not owner.members(leader, known), "Source-group descendants remain")
            budget.tick(force=True)
        result["status"] = "complete"
    except BaseException as error:
        result["error"] = str(error)
        raise
    finally:
        try:
            if child is not None and leader is not None:
                result["remaining_processes"] = owner.close_members(leader, known, args.workdir / "process-closure.json")
                child.wait(timeout=5)
                if result["remaining_processes"]:
                    result.update(status="failed", error="Source-group descendants could not be closed")
        finally:
            signal.signal(signal.SIGTERM, previous_term)
            result.update({"peaks": budget.peaks, "last": budget.last, "elapsed_seconds": time.monotonic() - started})
            owner.durable(args.workdir / "execution.json", result)
    require(not result.get("remaining_processes"), "Source-group descendants could not be closed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--groups-key", required=True)
    parser.add_argument("--groups-sha256", required=True)
    parser.add_argument("--group", required=True)
    parser.add_argument("--runtime-ref", required=True)
    parser.add_argument("--workdir", required=True, type=Path)
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{40}", args.runtime_ref), "runtime_ref must be an exact commit")
    require(1 <= args.workers <= 64, "Workers must be between 1 and 64")
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1], text=True).strip()
    require(actual == args.runtime_ref, "Checked-out runtime differs")
    (execute if args.worker else supervise)(args)


if __name__ == "__main__":
    main()
