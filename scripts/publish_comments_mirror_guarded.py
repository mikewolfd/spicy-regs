#!/usr/bin/env python3
"""Admit and monitor the existing comments publisher using observed native/public object sizes."""
# ruff: noqa: E402
# Fatal diagnostics must precede native imports in the direct child.

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from dataclasses import asdict
import faulthandler
import json
import os
from pathlib import Path
import re
import resource
import shutil
import subprocess
import sys
import time
from typing import Any
from urllib.parse import urlsplit

if __name__ == "__main__":
    faulthandler.enable(all_threads=True)
    # Python stacks only; core dumps exceed evidence bounds and can contain credentials.
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import read_comment_attachment_group as runner
from spicy_regs import operational_runner as owner
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, r2

GiB = 2**30
FREE_FLOOR = 8 * GiB
ACTIVE_SECONDS = 150 * 60
PRELIMITS = {**runner.LIMITS, "wall_seconds": 15 * 60, "rss_bytes": GiB, "output_bytes": 64 * 2**20,
             "start_free_bytes": FREE_FLOOR, "stop_free_bytes": FREE_FLOOR}
require = runner.require


def observe(client, bucket, *, evidence=None, expected_snapshot=None):
    """Only catalog/manifest metadata and object HEADs; no subject or receipt payload scan."""
    namespace = iceberg._catalog_namespace(COMMENT)
    with iceberg._connect() as con:
        con.execute("SET memory_limit='256MB'; SET threads=1")
        before = iceberg._read_snapshot(con, COMMENT, namespace=namespace)
        record = {"namespace": namespace, "snapshot_before": asdict(before), "complete": False}
        if evidence is not None:
            owner.durable(evidence, record)
        require(expected_snapshot is None or asdict(before) == expected_snapshot,
                "Diagnostic native snapshot differs; no payload export permitted")
        table = iceberg._qualified(COMMENT)
        # The maintained file-list predicate includes live data and delete files here. Initialization and
        # declared-schema qualification still belong to the unchanged publisher, after resource admission.
        paths = sorted({row[0] for row in con.execute(
            "SELECT file_path FROM iceberg_metadata(?) WHERE status <> 'DELETED'", [table]).fetchall()})
        require(0 < len(paths) <= 10_000, "Native object inventory is empty or exceeds the metadata bound")

        def head(path):
            parsed = urlsplit(path)
            require(parsed.scheme == "s3" and parsed.netloc == bucket and not parsed.query and not parsed.fragment,
                    "Native object belongs to a different storage target")
            reply = client.head_object(Bucket=bucket, Key=parsed.path.lstrip("/"))
            require(reply["ContentLength"] > 0 and isinstance(reply.get("ETag"), str), "Native object has no usable size/version")
            return {"path": path, "bytes": reply["ContentLength"], "etag": reply["ETag"], "version_id": reply.get("VersionId")}

        with ThreadPoolExecutor(max_workers=4) as pool:
            objects = list(pool.map(head, paths))
        after = iceberg._read_snapshot(con, COMMENT, namespace=namespace)
        if evidence is not None:
            owner.durable(evidence, {**record, "native_objects": objects, "snapshot_after": asdict(after), "complete": True})
        require(after == before, "Catalog moved during size admission; retry under the writer lock")
    previous = client.head_object(Bucket=bucket, Key="comments.parquet")
    require(previous["ContentLength"] > 0 and isinstance(previous.get("ETag"), str), "Prior monolith has no usable size/version")
    return {"namespace": namespace, "snapshot": asdict(before), "native_objects": objects,
            "native_bytes": sum(item["bytes"] for item in objects),
            "prior_monolith": {"key": "comments.parquet", "bytes": previous["ContentLength"],
                               "etag": previous["ETag"], "version_id": previous.get("VersionId")}}


def admission(root, observation):
    # Four possible copy equivalents with 25% growth headroom, plus bounded total spill/control reserve.
    basis = max(observation["native_bytes"], observation["prior_monolith"]["bytes"])
    maximum = 5 * basis + 8 * GiB
    free = shutil.disk_usage(root).free
    return {**observation, "format": "comments-publication-admission/1", "basis_bytes": basis,
            "output_cap_bytes": maximum, "free_floor_bytes": FREE_FLOOR, "required_free_bytes": maximum + FREE_FLOOR,
            "observed_free_bytes": free, "admitted": free >= maximum + FREE_FLOOR,
            "resources": asdict(ExportResources()), "tree_rss_cap_bytes": 8 * GiB, "active_seconds": ACTIVE_SECONDS}


def check_current(client, bucket, receipt):
    """Refuse changed admission inputs before the maintained publisher can start its payload export."""
    with iceberg._connect() as con:
        current = iceberg._read_snapshot(con, COMMENT, namespace=receipt["namespace"])
    require(asdict(current) == receipt["snapshot"], "Admitted native snapshot/schema changed; rerun admission")
    previous = client.head_object(Bucket=bucket, Key="comments.parquet")
    require(previous["ContentLength"] == receipt["prior_monolith"]["bytes"]
            and previous.get("ETag") == receipt["prior_monolith"]["etag"], "Admitted predecessor changed; rerun admission")


def diagnostic_subject(args):
    value = getattr(args, "diagnostic_native_snapshot", None)
    if value is not None:
        from scripts.read_comment_attachment_witnesses import subject_pin
        require(args.skip_upload, "Native COPY diagnostics require --skip-upload")
        return subject_pin(value)
    return None


@contextmanager
def copy_diagnostics(args, receipt):
    """Observe the unchanged copy on its actual connection; never replace its query or sink."""
    from scripts.read_comment_attachment_witnesses import runtime_identity
    from spicy_regs.sources import regulatory_catalog as native

    original = native._copy
    target_sql = f"SELECT * FROM {iceberg._qualified(COMMENT)}"
    report: dict[str, Any] = {"source_commit": args.expected_source_sha, "admitted_subject": receipt["snapshot"],
              "expected_subject": diagnostic_subject(args), "skip_upload": True, "native_copy_calls": 0,
              "python_fatal_stacks": faulthandler.is_enabled(),
              "core_dump_limit_bytes": resource.getrlimit(resource.RLIMIT_CORE)[0],
              "debugger": {"status": "unavailable", "reason": "No credential-free hosted debugger qualification; not invoked"},
              "native_backtrace": False}

    def retain(phase):
        report["phase"] = phase
        require(len(json.dumps(report).encode()) <= 8 * 2**20, "Publisher diagnostic evidence exceeds bound")
        owner.durable(args.workdir / "comments-publish-diagnostic.json", report)

    def observed_copy(con, sql, path, **kwargs):
        if sql != target_sql:
            return original(con, sql, path, **kwargs)
        report["native_copy_calls"] += 1
        require(report["native_copy_calls"] == 1, "Unexpected repeated native COPY in diagnostic")
        report["query"] = sql
        report["output"] = str(Path(path).resolve().relative_to(args.workdir.resolve()))
        report["resources"] = asdict(kwargs["resources"])
        report["parameters_present"] = kwargs.get("parameters") is not None
        retain("native_copy_snapshot")
        actual_pair = iceberg._read_pair_snapshot(con, COMMENT, namespace=iceberg._catalog_namespace(COMMENT))
        report["copy_transaction_pair"] = asdict(actual_pair)
        retain("native_copy_snapshot")
        require(asdict(actual_pair.subject()) == report["expected_subject"] == report["admitted_subject"],
                "Native COPY transaction subject differs from diagnostic admission")
        retain("native_runtime_identity")
        report["runtime"] = runtime_identity(con)
        retain("native_copy")
        result = original(con, sql, path, **kwargs)
        retain("native_copy_complete")
        return result

    retain("publisher_starting")
    setattr(native, "_copy", observed_copy)
    try:
        yield
        require(report["native_copy_calls"] == 1, "Diagnostic publisher did not execute its native COPY")
        retain("validated_no_upload")
    except BaseException as error:
        # Keep the last attempted phase; arbitrary exception text can contain credentials.
        report["outcome"] = "failed"
        report["error_type"] = type(error).__name__
        retain(report["phase"])
        raise
    finally:
        native._copy = original


def worker(args):
    expected_snapshot = diagnostic_subject(args)
    client, bucket = r2.verified_target_client()
    path = args.workdir / "comments-admission.json"
    if args.phase == "admit":
        receipt = admission(args.workdir, observe(client, bucket, evidence=args.workdir / "comments-admit-observation.json",
                                                 expected_snapshot=expected_snapshot))
        owner.durable(path, {**receipt, "source_commit": args.expected_source_sha})
        require(receipt["admitted"], "Insufficient disk for complete comments publication, spill and recovery floor")
        return
    from spicy_regs.pipelines.comments_mirror import publish_comments_mirror

    receipt = json.loads(path.read_text())
    require(receipt["admitted"] and receipt["source_commit"] == args.expected_source_sha, "Publication admission differs")
    require(expected_snapshot is None or receipt["snapshot"] == expected_snapshot, "Diagnostic admission snapshot differs")
    check_current(client, bucket, receipt)
    require(shutil.disk_usage(args.workdir).free >= receipt["required_free_bytes"], "Available disk changed after admission")
    with copy_diagnostics(args, receipt) if expected_snapshot is not None else nullcontext():
        publish_comments_mirror(args.workdir, resources=ExportResources(), skip_upload=args.skip_upload, force=args.force)


def run(args):
    expected_snapshot = diagnostic_subject(args)
    require(os.getenv(iceberg.CATALOG_LOCK_ENV) == "comments-catalog-write", "Hold the shared comments-catalog-write lock")
    args.workdir.mkdir(parents=True, exist_ok=True)
    require(not any((args.workdir / name).exists() for name in
                    ("comments-admission.json", "comments.parquet", "comments", ".comments-generations")),
            "Use a fresh publication directory; preserve previous refusal evidence")
    start = time.monotonic()
    command = [sys.executable, "-B", str(Path(__file__).resolve()), "--expected-source-sha", args.expected_source_sha,
               "--output-dir", str(args.workdir)]
    if args.skip_upload:
        command.append("--skip-upload")
    if args.force:
        command.append("--force")
    if expected_snapshot is not None:
        command.extend(["--diagnostic-native-snapshot", json.dumps(expected_snapshot, sort_keys=True)])

    def watched(phase, limits):
        try:
            runner.supervise(args, command=[*command, "--phase", phase], limits=limits)
        finally:
            for name in ("execution.json", "process-closure.json", "execution.log"):
                path = args.workdir / name
                if path.exists():
                    path.replace(args.workdir / f"comments-{phase}-{name}")

    watched("admit", PRELIMITS)
    receipt = json.loads((args.workdir / "comments-admission.json").read_text())
    limits = {**PRELIMITS, "wall_seconds": ACTIVE_SECONDS - (time.monotonic() - start),
              "rss_bytes": receipt["tree_rss_cap_bytes"], "output_bytes": receipt["output_cap_bytes"]}
    watched("publish", limits)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--expected-source-sha", required=True)
    parser.add_argument("--output-dir", dest="workdir", type=Path, default=Path("output"))
    parser.add_argument("--skip-upload", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--diagnostic-native-snapshot", default="",
                        help="Exact UUID/snapshot/schema JSON for no-upload native COPY diagnostics")
    parser.add_argument("--phase", choices=("admit", "publish"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.diagnostic_native_snapshot = json.loads(args.diagnostic_native_snapshot) if args.diagnostic_native_snapshot else None
    diagnostic_subject(args)
    require(os.getenv(iceberg.CATALOG_LOCK_ENV) == "comments-catalog-write", "Hold the shared comments-catalog-write lock")
    require(re.fullmatch(r"[0-9a-f]{40}", args.expected_source_sha), "Exact source commit required")
    repository = Path(__file__).resolve().parents[1]
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repository, text=True).strip()
    require(actual == args.expected_source_sha, "Checked-out publisher differs from expected source")
    require(not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"], cwd=repository, text=True).strip(),
            "Tracked publisher inputs are modified")
    (worker if args.phase else run)(args)


if __name__ == "__main__":
    main()
