"""Exact predecessor evidence permits only the selected original-backed additions."""

import hashlib
import importlib.util
import json
from pathlib import Path
import struct
import subprocess
from types import SimpleNamespace
import zipfile

import polars as pl
import pytest
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from spicy_regs.pipelines import comment_fields as cf, comment_source_groups, comment_stage_aggregate as aggregate
from spicy_regs.pipelines.comment_fields_evidence import EVIDENCE_SCHEMA, commit_checkpoint, file_sha256, write_evidence


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


@pytest.fixture
def executor(monkeypatch):
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location("repair_under_test", scripts / "repair_comment_attachment_group.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def recovery(tmp_path, executor):
    original = Path(__file__).parent / "fixtures/comment_attachment_unknown_sizes/HHS-OASH-2022-0006-0002.json"
    body = original.read_bytes()
    raw = json.loads(body)
    key = "raw-data/HHS/HHS-OASH-2022-0006/text-HHS-OASH-2022-0006/comments/HHS-OASH-2022-0006-0002.json"
    kept_key = key.replace("-0002.json", "-0003.json")
    kept = json.loads(body)
    kept["data"]["id"] = "HHS-OASH-2022-0006-0003"
    kept["included"][0]["attributes"]["fileFormats"][0]["size"] = 7
    previous = tmp_path / "original-recovery"
    inputs = previous / "inputs"
    inputs.mkdir(parents=True)
    pl.DataFrame({"key": [key, kept_key]}).write_parquet(inputs / "manifest.parquet")
    group = {
        "group": "runner-000",
        "keys": 2,
        "local": 0,
        "r2": 0,
        "external": 2,
        "manifest": {
            "file": "manifest.parquet",
            "bytes": (inputs / "manifest.parquet").stat().st_size,
            "sha256": file_sha256(inputs / "manifest.parquet"),
        },
        "source_pins": None,
    }
    master = {
        "format": "comment-source-groups/1",
        "profile": "attachments",
        "reader_sha256": "a" * 64,
        "implementation_sha256": file_sha256(Path(comment_source_groups.__file__)),
        "keys": 2,
        "local_groups": 1,
        "runner_groups": 1,
        "max_group_keys": 250000,
        "groups": [group],
    }
    write_json(inputs / "groups.json", master)
    plan = cf.plan(
        previous / "read",
        inputs / "manifest.parquet",
        observe_source=True,
        profile="attachments",
        chunk_keys=1000,
        max_spill="32MB",
    )
    metadata = json.loads(plan.with_suffix(".json").read_text())
    metadata["reader_sha256"] = master["reader_sha256"]
    write_json(plan.with_suffix(".json"), metadata)
    part = cf.part_path(previous / "read", plan.stem, "HHS", 0)
    part.parent.mkdir(parents=True)
    kept_row = cf._attachment_part_row(SimpleNamespace(key=kept_key, payload=kept))
    pl.DataFrame([kept_row]).write_parquet(part)
    sha = hashlib.sha256(body).hexdigest()
    observations = [
        {
            **dict.fromkeys(EVIDENCE_SCHEMA),
            "key": item,
            "status": status,
            "reason": executor.REFUSAL if status == "refused" else None,
            "size": len(body),
            "sha256": bytes.fromhex(sha),
            "etag": '"fixture"',
        }
        for item, status in [(key, "refused"), (kept_key, "read")]
    ]
    evidence = write_evidence(part, observations)
    commit_checkpoint(part, metadata["plan_sha256"], evidence, 2, 1, "attachments")
    identity = {
        "group": group["group"],
        "groups_sha256": file_sha256(inputs / "groups.json"),
        "reader_sha256": master["reader_sha256"],
        "runtime_ref": "b" * 40,
        "executor_sha256": "c" * 64,
    }
    write_json(previous / "identity.json", identity)
    write_json(
        previous / "execution.json",
        {
            "status": "failed",
            "remaining_processes": [],
            "limits": executor.LIMITS,
            "elapsed_seconds": 1,
            "peaks": {"rss": 1, "allocated": 1},
            "last": {"free": 10 * 2**30},
        },
    )
    write_json(previous / "source-usage.json", {"attempts": 2, "bytes": 100})
    (previous / "usage").mkdir()
    (previous / "usage/attempts.bin").write_bytes(b"11")
    (previous / "usage/bytes.bin").write_bytes(struct.pack("<Q", 100))
    archived = tmp_path / "recovery.zip"
    with zipfile.ZipFile(archived, "w") as archive:
        for path in previous.rglob("*"):
            if path.is_file():
                archive.write(path, str(path.relative_to(previous)))
    raw_root = tmp_path / "raw"
    CentralRawArchiveStore(raw_root).put_blob("sha256:" + sha, len(body), [body])
    runtime = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    args = SimpleNamespace(
        runtime_ref=runtime,
        candidate=True,
        publish=False,
        archive=archived,
        raw_root=raw_root,
        workdir=tmp_path / "runner-000",
        request=tmp_path / "request.json",
    )
    request = {
        "format": executor.FORMAT,
        "group": group["group"],
        "producer": executor.producer(args),
        "previous": {
            "archive": {"bytes": archived.stat().st_size, "sha256": file_sha256(archived)},
            "identity": identity,
            "identity_sha256": file_sha256(previous / "identity.json"),
            "plan_receipt_sha256": file_sha256(plan.with_suffix(".json")),
        },
        "records": [
            {
                "key": key,
                "raw_sha256": sha,
                "raw_bytes": len(body),
                "etag": '"fixture"',
                "version_id": None,
                "checkpoint_sha256": file_sha256(part.with_suffix(".checkpoint.json")),
                "normalizations": executor.comment_size_normalizations(raw),
            }
        ],
        "supplementary_source_usage": {"attempts": 1, "bytes": len(body)},
        "supplementary_active_seconds": 1,
    }
    write_json(args.request, request)
    args.request_sha256 = file_sha256(args.request)
    args.workdir.mkdir()
    return args, request, kept_row, inputs


def admission_for(args, request, target):
    report = json.loads((args.workdir / "repair.json").read_text())
    write_json(
        args.workdir / "execution.json",
        {
            "status": "complete",
            "remaining_processes": [],
            "limits": report["resource_admission"]["limits"],
            "elapsed_seconds": report["repair_seconds"] + 0.1,
            "peaks": {"rss": 1, "allocated": 1},
            "last": {"free": 10 * 2**30},
        },
    )
    files = [
        args.workdir / name
        for name in [
            "result.json",
            "repair.json",
            "repair-request.json",
            "execution.json",
            "stage/staging.json",
            "stage/reads.parquet.sha256",
        ]
    ]
    files += list((args.workdir / "read/plan").glob("*.json"))
    selected = {
        "files": {
            str(p.relative_to(args.workdir)): {"bytes": p.stat().st_size, "sha256": file_sha256(p)} for p in files
        },
        "request_sha256": args.request_sha256,
        "previous_archive_sha256": request["previous"]["archive"]["sha256"],
        "records": [{k: row[k] for k in ("key", "raw_sha256", "raw_bytes")} for row in request["records"]],
    }
    value = {
        "format": "comment-attachment-repair-stages/1",
        "groups_sha256": request["previous"]["identity"]["groups_sha256"],
        "previous_reader_sha256": request["previous"]["identity"]["reader_sha256"],
        "producer": request["producer"],
        "groups": {request["group"]: selected},
    }
    write_json(target, value)
    return value


def test_original_backed_repair_preserves_accepted_row_and_closes_aggregate(executor, recovery, tmp_path):
    args, request, kept, inputs = recovery
    archive_sha = file_sha256(args.archive)
    executor.repair(args)
    rows = pl.read_parquet(args.workdir / "stage/reads.parquet").to_dicts()
    assert next(row for row in rows if row["key"] == kept["key"]) == kept
    added = next(row for row in rows if row["key"] != kept["key"])
    assert json.loads(added["attachments_json"])[0]["formats"][0]["size"] == -1
    report = json.loads((args.workdir / "repair.json").read_text())
    assert report["preserved_rows"] == report["added_rows"] == 1 and report["new_source_gets"] == 0
    assert file_sha256(args.archive) == archive_sha
    admission = tmp_path / "repair-stages.json"
    admission_for(args, request, admission)
    reads = tmp_path / "reads"
    reads.mkdir()
    args.workdir.rename(reads / request["group"])
    (inputs / request["group"]).mkdir()
    (inputs / request["group"] / "manifest.parquet").write_bytes((inputs / "manifest.parquet").read_bytes())
    result = aggregate.aggregate(
        inputs / "groups.json",
        file_sha256(inputs / "groups.json"),
        reads,
        tmp_path / "aggregate",
        repair_admissions=admission,
        repair_admissions_sha256=file_sha256(admission),
        candidate_only=True,
    )
    assert result["rows"] == 2 and result["candidate_only"] is True
    with pytest.raises(ValueError, match="producer"):
        aggregate.repair_admission(
            admission,
            file_sha256(admission),
            json.loads((inputs / "groups.json").read_text()),
            file_sha256(inputs / "groups.json"),
        )


@pytest.mark.parametrize(
    "change,message,before_extract",
    [
        ("archive", "archive differs", True),
        ("attempts", "source budget", True),
        ("time", "active time", True),
        ("raw", "exact source refusal", False),
        ("checkpoint", "exact source refusal", False),
        ("producer", "exact producer", True),
    ],
)
def test_changed_parent_source_identity_and_exhausted_budgets_refuse(
    executor, recovery, change, message, before_extract
):
    args, request, _, _ = recovery
    if change == "archive":
        request["previous"]["archive"]["sha256"] = "0" * 64
    if change == "attempts":
        request["supplementary_source_usage"]["attempts"] = 3
    if change == "time":
        request["supplementary_active_seconds"] = 5400
    if change == "raw":
        request["records"][0]["raw_sha256"] = "0" * 64
    if change == "checkpoint":
        request["records"][0]["checkpoint_sha256"] = "0" * 64
    if change == "producer":
        request["producer"]["reader_sha256"] = "0" * 64
    write_json(args.request, request)
    args.request_sha256 = file_sha256(args.request)
    with pytest.raises(ValueError, match=message):
        executor.repair(args)
    assert not (args.workdir / "result.json").exists()
    if before_extract:
        assert not (args.workdir / "prior").exists()


@pytest.mark.parametrize("change", ["keys", "request", "archive", "producer", "closure"])
def test_aggregate_requires_exact_reviewed_derivation(executor, recovery, tmp_path, change):
    args, request, _, inputs = recovery
    executor.repair(args)
    path = tmp_path / "repair-stages.json"
    admitted = admission_for(args, request, path)
    selected = admitted["groups"][request["group"]]
    if change == "keys":
        selected["records"][0]["key"] += "-unreviewed"
    if change == "request":
        selected["request_sha256"] = "0" * 64
    if change == "archive":
        selected["previous_archive_sha256"] = "0" * 64
    if change == "producer":
        admitted["producer"]["executor_sha256"] = "0" * 64
    if change == "closure":
        execution = args.workdir / "execution.json"
        changed = json.loads(execution.read_text()) | {"status": "failed"}
        write_json(execution, changed)
        selected["files"]["execution.json"].update(bytes=execution.stat().st_size, sha256=file_sha256(execution))
    write_json(path, admitted)
    master = json.loads((inputs / "groups.json").read_text())
    with pytest.raises(ValueError):
        checked = aggregate.repair_admission(
            path, file_sha256(path), master, file_sha256(inputs / "groups.json"), candidate_only=True
        )
        aggregate._group_result(
            args.workdir.parent,
            master["groups"][0],
            file_sha256(inputs / "groups.json"),
            master["reader_sha256"],
            checked["groups"][request["group"]],
            checked["producer"],
        )
