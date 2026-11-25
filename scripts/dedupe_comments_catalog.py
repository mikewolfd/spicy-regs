#!/usr/bin/env python3
"""Read-only duplicate audit of the selected native comments catalog.

Native ingestion admits one subject and receipt per identity. This command never
rebuilds or migrates stored rows; a failed audit requires a qualified rebuild from
retained source evidence, through the ordinary native writer.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import math
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import time
from urllib.parse import urlparse
import zipfile

if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from loguru import logger
from spicy_regs.duckdb_settings import ExportResources
from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg

GiB = 2**30
MAX_JSON = 16 * 2**20
MAX_ARCHIVE = 512 * 2**20
RESOURCES = ExportResources(memory="4GB", threads=1)
SPILL = 3 * GiB
WORKING = 512 * 2**20
LIMITS = {
    "wall_seconds": 900,
    "interval_seconds": 0.1,
    "rss_bytes": 8 * GiB,
    "output_bytes": 6 * GiB,
    "start_free_bytes": 4 * GiB,
    "stop_free_bytes": 4 * GiB,
}
MEMBER = "physical/catalog-index.parquet"
CONTROLS = {
    "execution.json",
    "execution.log",
    "process-closure.json",
    "admission-plan.json",
    "physical/membership-explain.json",
}
REPO = "mikewolfd/spicy-regs"
POLL_ATTEMPTS = 5
POLL_SECONDS = 2.0
_GITHUB_TOKEN_ENVS = frozenset({
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "GH_ENTERPRISE_TOKEN",
    "GITHUB_ENTERPRISE_TOKEN",
    "ACTIONS_RUNTIME_TOKEN",
    "ACTIONS_ID_TOKEN_REQUEST_TOKEN",
})


def require(value: object, message: str) -> None:
    if not value:
        raise ValueError(message)


def pin(path: Path) -> dict[str, int | str]:
    from spicy_regs import operational_runner as owner

    value = owner.file_pin(path)
    return {"bytes": value["bytes"], "sha256": value["sha256"].removeprefix("sha256:")}


def read_request(path: Path) -> dict:
    from spicy_regs import operational_runner as owner

    request = owner.read_json(path)
    require(
        set(request) == {"format", "artifact", "snapshot", "members"}
        and request["format"] == "retained-comment-membership/1",
        "Retained request format differs",
    )
    artifact = request["artifact"]
    require(set(artifact) == {"id", "run_id", "head_sha", "name", "bytes", "sha256"}, "Artifact fields differ")
    require(
        all(type(artifact[k]) is int and artifact[k] > 0 for k in ("id", "run_id", "bytes"))
        and artifact["bytes"] <= MAX_ARCHIVE,
        "Artifact size/identity refused",
    )
    require(
        artifact["name"] == "comment-attachment-fill-plan"
        and re.fullmatch(r"[0-9a-f]{40}", artifact["head_sha"])
        and re.fullmatch(r"[0-9a-f]{64}", artifact["sha256"]),
        "Artifact producer/hash differs",
    )
    subject = request["snapshot"]
    require(
        set(subject) == {"table_uuid", "snapshot_id", "schema_id"}
        and re.fullmatch(r"[0-9a-f-]{36}", subject["table_uuid"])
        and type(subject["snapshot_id"]) is int
        and subject["snapshot_id"] > 0
        and type(subject["schema_id"]) is int
        and subject["schema_id"] >= 0,
        "Snapshot pin refused",
    )
    require(set(request["members"]) == CONTROLS | {MEMBER}, "Exact retained member set required")
    for name, expected in request["members"].items():
        require(
            set(expected) == {"bytes", "crc32", "sha256"}
            and type(expected["bytes"]) is int
            and 0 < expected["bytes"] <= MAX_ARCHIVE
            and re.fullmatch(r"[0-9a-f]{8}", expected["crc32"]),
            "Member size/CRC refused",
        )
        require(
            expected["sha256"] is None
            if name == MEMBER
            else expected["bytes"] <= 256 * 2**10 and re.fullmatch(r"[0-9a-f]{64}", expected["sha256"]),
            "Control pin or newly measured membership hash differs",
        )
    require(sum(m["bytes"] for m in request["members"].values()) <= MAX_ARCHIVE, "Extracted size refused")
    return request


def admission(root: Path, request: dict, phase: str) -> None:
    from spicy_regs import operational_runner as owner

    held = owner.sizes(root, float("inf"))
    proposed = (
        request["artifact"]["bytes"] + sum(m["bytes"] for m in request["members"].values())
        if phase == "acquire"
        else 0
    )
    free = shutil.disk_usage(root).free
    reserved = SPILL + WORKING + MAX_JSON
    owned_ok = max(held["logical"], held["allocated"]) + proposed + reserved < LIMITS["output_bytes"] - 65536
    free_ok = free - proposed - reserved >= LIMITS["stop_free_bytes"]
    record = {
        "phase": phase,
        "status": "admitted" if owned_ok and free_ok else "refused",
        "held": held,
        "proposed_input_bytes": proposed,
        "spill_reserved_bytes": SPILL,
        "working_reserved_bytes": WORKING,
        "report_reserved_bytes": MAX_JSON,
        "free_bytes": free,
        "limits": LIMITS,
    }
    owner.durable(root / f"{phase}-admission.json", record)
    require(owned_ok and free_ok, "Retained audit storage admission refused")


def _api(client, path: str, token: str, *, allow_transient: bool = False) -> dict | None:
    with client.stream(
        "GET",
        f"https://api.github.com/repos/{REPO}/{path}",
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
    ) as reply:
        if allow_transient and reply.status_code in {404, 429, 500, 502, 503, 504}:
            return None
        require(reply.status_code == 200, "GitHub metadata request refused")
        raw = bytearray()
        for block in reply.iter_bytes(64 * 1024):
            require(len(raw) + len(block) <= 128 * 1024, "GitHub metadata exceeds bound")
            raw.extend(block)
    return json.loads(raw)


def _poll_artifact_metadata(client, expected: dict, token: str) -> dict:
    """Poll actions/artifacts/{id} until expired == false, refusing exact pin mismatches immediately."""
    for attempt in range(POLL_ATTEMPTS):
        metadata = _api(client, f"actions/artifacts/{expected['id']}", token, allow_transient=True)
        if metadata is None or metadata.get("expired"):
            if attempt + 1 < POLL_ATTEMPTS:
                time.sleep(POLL_SECONDS)
                continue
            require(False, "Artifact metadata unavailable or expired")
        assert metadata is not None
        require(
            metadata["id"] == expected["id"]
            and metadata["name"] == expected["name"]
            and metadata["size_in_bytes"] == expected["bytes"]
            and not metadata["expired"]
            and metadata["digest"] == "sha256:" + expected["sha256"]
            and metadata["workflow_run"]["id"] == expected["run_id"]
            and metadata["workflow_run"]["head_sha"] == expected["head_sha"],
            "Artifact producer metadata differs",
        )
        return metadata
    raise ValueError("Artifact metadata unavailable or expired")


def _poll_artifact_redirect(client, artifact_id: int, token: str) -> str:
    """Poll actions/artifacts/{id}/zip until HTTP 302 with an allowed Actions storage host."""
    for attempt in range(POLL_ATTEMPTS):
        with client.stream(
            "GET",
            f"https://api.github.com/repos/{REPO}/actions/artifacts/{artifact_id}/zip",
            headers={"Authorization": f"Bearer {token}"},
        ) as reply:
            if reply.status_code in {202, 404, 429, 500, 502, 503, 504} and attempt + 1 < POLL_ATTEMPTS:
                time.sleep(POLL_SECONDS)
                continue
            location = reply.headers.get("Location", "")
            host = urlparse(location)
            require(
                reply.status_code == 302
                and host.scheme == "https"
                and not host.username
                and (host.hostname or "").endswith((".blob.core.windows.net", ".actions.githubusercontent.com")),
                "Artifact redirect refused",
            )
            return location
    raise ValueError("Artifact redirect refused")


def acquire(root: Path, request: dict, request_pin: dict) -> None:
    """Hosted-only acquisition: complete provider ZIP hash precedes any member extraction."""
    import httpx
    from spicy_regs import operational_runner as owner

    require(os.getenv("GITHUB_ACTIONS") == "true" and sys.platform == "linux", "Artifact acquisition is hosted Linux only")
    admission(root, request, "acquire")
    expected = request["artifact"]
    token = os.getenv("GITHUB_TOKEN") or os.getenv("GH_TOKEN")
    require(token, "Artifact read token required only during acquisition")
    assert token is not None
    archive_path = root / "artifact.zip"
    require(not archive_path.exists() and not (root / "inputs").exists(), "Use a fresh retained-audit workdir")
    with httpx.Client(follow_redirects=False, timeout=httpx.Timeout(20, connect=5)) as client:
        metadata = _poll_artifact_metadata(client, expected, token)
        run = _api(client, f"actions/runs/{expected['run_id']}", token)
        assert run is not None
        require(
            run["id"] == expected["run_id"]
            and run["head_sha"] == expected["head_sha"]
            and run["status"] == "completed"
            and run["conclusion"] == "failure"
            and run["path"] == ".github/workflows/fill-comment-attachment-groups.yml",
            "Artifact producer metadata differs",
        )
        owner.durable(root / "producer.json", {"artifact": metadata, "run": run})
        location = _poll_artifact_redirect(client, expected["id"], token)
        sha, size = hashlib.sha256(), 0
        # No Authorization header follows the redirect; never retain the temporary signed URL.
        with client.stream("GET", location) as reply, (root / "artifact.zip.part").open("xb") as stream:
            require(reply.status_code == 200, "Artifact download refused")
            for block in reply.iter_bytes(1 << 20):
                size += len(block)
                require(size <= expected["bytes"], "Artifact exceeds exact byte bound")
                sha.update(block)
                stream.write(block)
        require(size == expected["bytes"] and sha.hexdigest() == expected["sha256"], "Whole artifact ZIP pin differs")
    (root / "artifact.zip.part").rename(archive_path)
    extract(root, request, request_pin)


def extract(root: Path, request: dict, request_pin: dict) -> None:
    """Extract exactly six regular members after verifying the complete archive."""
    from spicy_regs import operational_runner as owner

    archive_path = root / "artifact.zip"
    require(pin(archive_path) == {k: request["artifact"][k] for k in ("bytes", "sha256")}, "Whole artifact ZIP pin differs")
    inputs, members = root / "inputs", {}
    with zipfile.ZipFile(archive_path) as archive:
        infos = archive.infolist()
        require(
            len(infos) == len(request["members"]) and {i.filename for i in infos} == set(request["members"]),
            "Archive contains missing, repeated or unexpected members",
        )
        for info in infos:
            name, expected = PurePosixPath(info.filename), request["members"][info.filename]
            require(
                not name.is_absolute()
                and ".." not in name.parts
                and str(name) == info.filename
                and stat.S_IFMT(info.external_attr >> 16) in (0, stat.S_IFREG)
                and not info.flag_bits & 1
                and info.file_size == expected["bytes"]
                and f"{info.CRC:08x}" == expected["crc32"],
                "Unsafe or changed archive member",
            )
        inputs.mkdir()
        for info in infos:
            target = inputs / info.filename
            target.parent.mkdir(parents=True, exist_ok=True)
            sha, size = hashlib.sha256(), 0
            with archive.open(info) as source, target.open("xb") as stream:
                while block := source.read(1 << 20):
                    size += len(block)
                    require(size <= info.file_size, "Extracted member exceeds pin")
                    sha.update(block)
                    stream.write(block)
            expected = request["members"][info.filename]
            require(
                size == expected["bytes"] and (expected["sha256"] is None or sha.hexdigest() == expected["sha256"]),
                "Extracted member SHA-256 differs",
            )
            target.chmod(0o444)
            members[info.filename] = {"bytes": size, "sha256": sha.hexdigest(), "crc32": f"{info.CRC:08x}"}
    explain = json.loads((inputs / "physical/membership-explain.json").read_text())
    require(
        explain["snapshot"] == request["snapshot"]
        and explain["resources"] == {"memory": "4GB", "threads": 1, "max_spill": "3GB"},
        "Retained COPY snapshot/resources differ",
    )
    require(
        json.loads((inputs / "execution.json").read_text())["status"] == "failed"
        and "A staged comment identity appears more than once in the native catalog"
        in (inputs / "execution.log").read_text(),
        "Expected retained duplicate refusal differs",
    )
    archive_path.chmod(0o444)
    owner.durable(root / "custody.json", {"request": request_pin, "archive": pin(archive_path), "members": members})


def _verified_custody(root: Path, request: dict, request_pin: dict) -> dict:
    from spicy_regs import operational_runner as owner

    custody = owner.read_json(root / "custody.json")
    require(
        custody["request"] == request_pin
        and custody["archive"] == {k: request["artifact"][k] for k in ("bytes", "sha256")},
        "Acquisition request/archive differs",
    )
    require(pin(root / "artifact.zip") == custody["archive"], "Retained archive changed")
    for name, expected in custody["members"].items():
        selected = request["members"].get(name)
        require(
            selected is not None
            and expected["bytes"] == selected["bytes"]
            and expected["crc32"] == selected["crc32"]
            and (name == MEMBER or expected["sha256"] == selected["sha256"]),
            "Custody control pin differs",
        )
        require(pin(root / "inputs" / name) == {k: expected[k] for k in ("bytes", "sha256")}, "Retained input changed")
    require(set(custody["members"]) == set(request["members"]), "Custody membership differs")
    return custody


def _readback_verify(client, bucket: str, key: str, expected_bytes: int, expected_sha256: str) -> None:
    reply = client.get_object(Bucket=bucket, Key=key)
    body, actual, size = reply["Body"], hashlib.sha256(), 0
    try:
        while block := body.read(1 << 20):
            size += len(block)
            require(size <= expected_bytes, f"Stored {key} exceeds expected bytes")
            actual.update(block)
    finally:
        body.close()
    require(size == expected_bytes and actual.hexdigest() == expected_sha256, f"Stored {key} differs from pin")


def backup(root: Path, request: dict, request_pin: dict, runtime_ref: str) -> None:
    """Hosted-only immutable backup of the verified artifact ZIP to R2 staging custody."""
    from spicy_regs import operational_runner as owner
    from spicy_regs.sources.publication import _head, _put_immutable
    from spicy_regs.sources.r2 import verified_target_client

    require(os.getenv("GITHUB_ACTIONS") == "true" and sys.platform == "linux", "Artifact R2 backup is hosted Linux only")
    require(
        not any(_non_storage_credential(name) for name in os.environ),
        "Backup child received GitHub or Iceberg catalog credentials",
    )
    custody = _verified_custody(root, request, request_pin)
    admission(root, request, "backup")
    client, bucket = verified_target_client()
    expected = request["artifact"]
    prefix = f"staging/retained-actions-artifacts/{expected['run_id']}-{expected['id']}-{expected['sha256'][:12]}"
    archive_key = f"{prefix}/artifact.zip"
    custody_key = f"{prefix}/custody.json"
    archive_path = root / "artifact.zip"
    archive_head = _head(client, bucket, archive_key)
    if archive_head is None:
        _put_immutable(client, bucket, archive_key, archive_path, sha256="sha256:" + expected["sha256"])
    _readback_verify(client, bucket, archive_key, expected["bytes"], expected["sha256"])

    r2_custody_path = root / "r2-custody.json"
    producer_path = root / "producer.json"
    owner.durable(
        r2_custody_path,
        {
            "format": "retained-artifact-r2-custody/1",
            "runtime_ref": runtime_ref,
            "request": request_pin,
            "artifact": expected,
            "snapshot": request["snapshot"],
            "archive_key": archive_key,
            "archive": custody["archive"],
            "members": custody["members"],
            "producer_pin": pin(producer_path) if producer_path.exists() else None,
        },
    )
    r2_custody_pin = pin(r2_custody_path)
    custody_head = _head(client, bucket, custody_key)
    if custody_head is None:
        _put_immutable(client, bucket, custody_key, r2_custody_path, sha256="sha256:" + str(r2_custody_pin["sha256"]))
    _readback_verify(client, bucket, custody_key, int(r2_custody_pin["bytes"]), str(r2_custody_pin["sha256"]))
    require(pin(archive_path) == custody["archive"], "Retained archive changed during backup")
    owner.durable(
        root / "backup.json",
        {
            "format": "retained-artifact-r2-backup/1",
            "status": "complete",
            "bucket": bucket,
            "prefix": prefix,
            "archive_key": archive_key,
            "custody_key": custody_key,
            "archive_already_present": archive_head is not None,
            "custody_already_present": custody_head is not None,
            "archive": custody["archive"],
            "custody": r2_custody_pin,
            "runtime_ref": runtime_ref,
        },
    )


def _restricted_connection(path: Path, scratch: Path):
    import duckdb

    con = duckdb.connect(config={"allow_persistent_secrets": False})
    RESOURCES.configure(con, scratch)
    con.execute("SET max_temp_directory_size='3GB'")
    for option in ("autoinstall_known_extensions", "autoload_known_extensions", "allow_unsigned_extensions"):
        con.execute(f"SET {option}=false")
    con.execute("SET allowed_paths=?", [[str(path)]])
    con.execute("SET enable_external_access=false")
    con.execute("SET lock_configuration=true")
    return con


def _credential(name: str) -> bool:
    return name in _GITHUB_TOKEN_ENVS or name.startswith(("R2_", "AWS_", "CLOUDFLARE_"))


def _non_storage_credential(name: str) -> bool:
    return name in _GITHUB_TOKEN_ENVS or name.startswith("R2_CATALOG_")


@contextmanager
def without_credentials():
    removed = {name: os.environ.pop(name) for name in list(os.environ) if _credential(name)}
    try:
        yield
    finally:
        os.environ.update(removed)


@contextmanager
def without_non_storage_credentials():
    removed = {name: os.environ.pop(name) for name in list(os.environ) if _non_storage_credential(name)}
    try:
        yield
    finally:
        os.environ.update(removed)


def query(root: Path, request: dict, request_pin: dict, runtime_ref: str) -> None:
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs import operational_runner as owner

    require(not any(_credential(name) for name in os.environ), "Query child received credentials")
    custody = _verified_custody(root, request, request_pin)
    admission(root, request, "query")
    path = (root / "inputs" / MEMBER).resolve()
    before = pin(path)
    parquet = pq.ParquetFile(path)
    schema = parquet.schema_arrow
    require(
        schema.names == ["comment_id", "modify_date", "_file"]
        and all(pa.types.is_string(field.type) or pa.types.is_large_string(field.type) for field in schema),
        "Membership schema differs",
    )
    footer = {
        "rows": parquet.metadata.num_rows,
        "row_groups": parquet.metadata.num_row_groups,
        "physical_schema": str(parquet.schema),
        "arrow_schema": str(schema),
        "columns": [
            {
                "name": schema.names[i],
                "compressed_bytes": sum(
                    parquet.metadata.row_group(r).column(i).total_compressed_size
                    for r in range(parquet.metadata.num_row_groups)
                ),
                "uncompressed_bytes": sum(
                    parquet.metadata.row_group(r).column(i).total_uncompressed_size
                    for r in range(parquet.metadata.num_row_groups)
                ),
            }
            for i in range(len(schema))
        ],
    }
    source = "read_parquet(?, hive_partitioning=false)"
    statements = {
        "duplicate_ids": (
            f"CREATE TEMP TABLE duplicate_ids AS SELECT comment_id, count(*) AS copies "
            f"FROM {source} GROUP BY comment_id HAVING count(*)>1"
        ),
        "counts": "SELECT count(*), coalesce(sum(copies),0), coalesce(sum(copies-1),0) FROM duplicate_ids",
        "duplicate_rows": (
            f"CREATE TEMP TABLE duplicate_rows AS SELECT n.comment_id,n.modify_date,n._file "
            f"FROM {source} n SEMI JOIN duplicate_ids d ON n.comment_id IS NOT DISTINCT FROM d.comment_id"
        ),
        "raw_value_bytes": (
            "SELECT coalesce(sum(octet_length(encode(coalesce(comment_id,'')))"
            "+octet_length(encode(coalesce(modify_date,'')))"
            "+octet_length(encode(coalesce(_file,'')))),0) FROM duplicate_rows"
        ),
        "ids": "SELECT comment_id,copies FROM duplicate_ids ORDER BY comment_id NULLS FIRST",
        "rows": (
            "SELECT comment_id,modify_date,_file FROM duplicate_rows "
            "ORDER BY comment_id NULLS FIRST,modify_date NULLS FIRST,_file NULLS FIRST"
        ),
    }
    report: dict[str, object] = {
        "format": "retained-comment-identity-audit/1",
        "status": "refused",
        "semantic_verdict": None,
        "runtime_ref": runtime_ref,
        "script": pin(Path(__file__).resolve()),
        "duckdb_version": duckdb.__version__,
        "request": request,
        "request_pin": request_pin,
        "custody": custody,
        "footer": footer,
        "queries": statements,
        "query_parameters": [str(path)],
        "limits": LIMITS,
        "resources": {"memory": RESOURCES.memory, "threads": RESOURCES.threads, "max_spill": "3GB"},
        "scope": "Retained source-matching native membership; not a complete catalog census or accepted plan",
    }
    try:
        with _restricted_connection(path, root / "scratch/spill") as con:
            con.execute(statements["duplicate_ids"], [str(path)])
            counts_row = con.execute(statements["counts"]).fetchone()
            require(counts_row is not None, "Duplicate count query returned no row")
            assert counts_row is not None
            ids, copies, excess = (int(v) for v in counts_row)
            counts = {
                "duplicate_ids": ids,
                "rows_in_duplicate_groups": copies,
                "excess_rows": excess,
                "membership_identity_groups_including_null": footer["rows"] - excess,
            }
            report["counts"] = counts
            owner.durable(root / "counts.json", counts)
            require(ids <= 1000 and copies <= 10_000, "Duplicate identity/copy output bound refused; scalar counts retained")
            con.execute(statements["duplicate_rows"], [str(path)])
            raw_row = con.execute(statements["raw_value_bytes"]).fetchone()
            require(raw_row is not None and int(raw_row[0]) <= MAX_JSON, "Duplicate values exceed report byte allowance; scalar counts retained")
            report["duplicate_ids"] = [
                {"comment_id": row[0], "copies": row[1]} for row in con.execute(statements["ids"]).fetchall()
            ]
            rows = [dict(zip(schema.names, row, strict=True)) for row in con.execute(statements["rows"]).fetchall()]
            require(len(rows) == copies, "Null-aware duplicate rows do not match full scalar count")
            report.update(rows=rows, status="complete")
        require(pin(path) == before, "Membership changed during audit")
        report["input_unchanged"] = True
        compact_held = sum(p.stat().st_size for p in root.glob("*") if p.is_file() and p.suffix in {".json", ".log"})
        require(
            len(json.dumps(report, ensure_ascii=True, indent=2).encode()) + compact_held < MAX_JSON - 65536,
            "Complete report exceeds 16MiB evidence allowance; scalar counts retained",
        )
    except BaseException as error:
        report.update(status="refused", reason=str(error))
        report["input_unchanged"] = pin(path) == before
        report.pop("rows", None)
        report.pop("duplicate_ids", None)
        owner.durable(root / "audit.json", report)
        raise
    owner.durable(root / "audit.json", report)


def _charge_prior(workdir: Path, name: str, limits: dict[str, float | int], label: str) -> float:
    from spicy_regs import operational_runner as owner

    prior = owner.read_json(workdir / name)
    prior_limits = prior.get("limits")
    require(
        prior["status"] == "complete"
        and prior["remaining_processes"] == []
        and isinstance(prior_limits, dict)
        and "wall_seconds" in prior_limits
        and isinstance(prior_limits["wall_seconds"], (int, float))
        and not isinstance(prior_limits["wall_seconds"], bool)
        and {k: v for k, v in prior_limits.items() if k != "wall_seconds"}
        == {k: v for k, v in LIMITS.items() if k != "wall_seconds"}
        and math.isclose(
            float(prior_limits["wall_seconds"]),
            float(limits["wall_seconds"]),
            rel_tol=1e-6,
            abs_tol=1e-3,
        )
        and isinstance(prior["elapsed_seconds"], (int, float))
        and not isinstance(prior["elapsed_seconds"], bool)
        and 0 <= prior["elapsed_seconds"] < 900,
        f"{label} did not close successfully within its exact limits",
    )
    limits["wall_seconds"] = float(limits["wall_seconds"]) - float(prior["elapsed_seconds"])
    require(limits["wall_seconds"] > 0, "Cumulative 900s audit budget exhausted")
    return float(prior["elapsed_seconds"])


def retained(args: argparse.Namespace) -> int:
    from scripts import read_comment_attachment_group as runner
    from spicy_regs import operational_runner as owner

    require(args.workdir is not None and args.runtime_ref is not None, "Retained workdir/runtime required")
    actual = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1], text=True
    ).strip()
    require(bool(re.fullmatch(r"[0-9a-f]{40}", args.runtime_ref)) and actual == args.runtime_ref, "Audit runtime differs")
    args.workdir = args.workdir.resolve()
    require(args.retained_request.resolve() == args.workdir / "request.json", "Request must be owned request.json")
    request = read_request(args.retained_request)
    request_pin = pin(args.retained_request.resolve())
    if args.worker:
        if args.phase == "acquire":
            acquire(args.workdir, request, request_pin)
        elif args.phase == "backup":
            backup(args.workdir, request, request_pin, args.runtime_ref)
        else:
            query(args.workdir, request, request_pin, args.runtime_ref)
        return 0
    limits: dict[str, float | int] = dict(LIMITS)
    prior_elapsed = 0.0
    backup_exec_path = args.workdir / "backup-execution.json"
    if args.phase == "backup":
        require(not backup_exec_path.exists() and not (args.workdir / "execution.json").exists(), "Backup already attempted; retain prior outcome")
        prior_elapsed += _charge_prior(args.workdir, "acquire-execution.json", limits, "Acquisition")
    elif args.phase == "query":
        require(not (args.workdir / "execution.json").exists(), "Query already attempted; retain prior outcome")
        prior_elapsed += _charge_prior(args.workdir, "acquire-execution.json", limits, "Acquisition")
        if backup_exec_path.exists():
            prior_elapsed += _charge_prior(args.workdir, "backup-execution.json", limits, "R2 backup")
            require((args.workdir / "backup.json").exists(), "Backup execution missing backup.json")
    command = [
        sys.executable,
        "-B",
        str(Path(__file__).resolve()),
        "--retained-request",
        str(args.retained_request.resolve()),
        "--workdir",
        str(args.workdir),
        "--runtime-ref",
        args.runtime_ref,
        "--phase",
        args.phase,
        "--worker",
    ]
    try:
        if args.phase == "query":
            with without_credentials():
                runner.supervise(args, command=command, limits=limits)
        elif args.phase == "backup":
            with without_non_storage_credentials():
                runner.supervise(args, command=command, limits=limits)
        else:
            runner.supervise(args, command=command, limits=limits)
    finally:
        if args.phase in {"acquire", "backup"}:
            for name in ("execution.json", "execution.log", "process-closure.json"):
                source = args.workdir / name
                if source.exists():
                    source.rename(source.with_name(f"{args.phase}-{name}"))
    if args.phase == "query":
        execution = owner.read_json(args.workdir / "execution.json")
        elapsed = prior_elapsed + float(execution["elapsed_seconds"])
        require(
            execution["status"] == "complete" and execution["remaining_processes"] == [] and elapsed <= 900,
            "Cumulative supervised audit did not close successfully",
        )
        completion: dict[str, object] = {
            "status": "complete",
            "semantic_verdict": None,
            "runtime_ref": args.runtime_ref,
            "request": request_pin,
            "elapsed_seconds": elapsed,
            "acquisition_execution": pin(args.workdir / "acquire-execution.json"),
            "query_execution": pin(args.workdir / "execution.json"),
            "report": pin(args.workdir / "audit.json"),
        }
        if backup_exec_path.exists():
            completion["backup_execution"] = pin(backup_exec_path)
            completion["backup"] = pin(args.workdir / "backup.json")
        owner.durable(args.workdir / "completion.json", completion)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--retained-request", type=Path, help="Exact Actions artifact/member pins; no native access")
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--runtime-ref")
    parser.add_argument("--phase", choices=("acquire", "backup", "query"), default="query")
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.retained_request is not None:
        return retained(args)
    require(
        args.workdir is None and args.runtime_ref is None and not args.worker and args.phase == "query",
        "Retained-only options require a retained request",
    )
    if not iceberg.is_configured():
        logger.error("R2 Data Catalog is not configured; cannot audit")
        return 1
    con = iceberg._connect()
    try:
        duplicates = iceberg.audit_duplicates(con, RECORD_TYPES["comments"])
        for agency, rows, distinct in duplicates:
            logger.error("{}: {} rows for {} comment identities", agency, rows, distinct)
        if duplicates:
            return 1
        logger.info("Selected native comments contain no repeated agency/comment identities")
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
