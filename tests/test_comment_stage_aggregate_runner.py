"""The aggregate runner consumes small approved receipts and checked staging objects only."""

import importlib.util
import json
import shutil
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pytest

from spicy_regs.pipelines.comment_fields_evidence import file_sha256
from spicy_regs.sources import r2
from tests.test_comment_stage_aggregate import completed


@pytest.fixture
def runner(monkeypatch):
    scripts = Path(__file__).resolve().parents[1] / "scripts"
    monkeypatch.syspath_prepend(str(scripts))
    spec = importlib.util.spec_from_file_location("aggregate_runner_under_test", scripts / "aggregate_comment_attachment_stages.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def inputs(request, runner, tmp_path, monkeypatch):
    master, digest, reads, _ = request.getfixturevalue(completed.__name__)
    key = "staging/test/source-groups/groups.json"
    objects = {key: master.read_bytes()}
    bucket = "test-bucket"
    groups = json.loads(master.read_text())["groups"]
    local_manifest = {"format": "comment-attachment-local-stages/1", "groups_sha256": digest, "groups": {}}
    runner_groups = {}
    for group in groups:
        name = group["group"]
        work = reads / name
        stage = work / "stage"
        staged = json.loads((stage / "staging.json").read_text())
        stage_sha = staged["file"]["sha256"]
        stage_key = f"staging/comment-fields-fill-{stage_sha[:12]}/reads.parquet"
        objects[stage_key] = (stage / "reads.parquet").read_bytes()
        objects[f"staging/test/source-groups/{name}/manifest.parquet"] = (
            master.parent / name / "manifest.parquet").read_bytes()
        [plan] = work.glob("plan/pinned-*.json")
        files = [work / "result.json", plan, stage / "staging.json", stage / "reads.parquet.sha256"]
        if name.startswith("local-"):
            files.append(stage / "source-gaps.parquet")
            local_pins = {}
            for path in files:
                relative = str(path.relative_to(work))
                objects[f"staging/test/local/{name}/{relative}"] = path.read_bytes()
                local_pins[relative] = {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
            local_manifest["groups"][name] = {"files": local_pins, "stage_key": stage_key}
        else:
            result_path = work / "result.json"
            result = json.loads(result_path.read_text())
            result["upload"] = {"bucket": bucket, "key": stage_key, "sha256": stage_sha}
            result_path.write_text(json.dumps(result))
            runner_groups[name] = {(f"read/plan/{plan.name}" if path == plan else str(path.relative_to(work))):
                                   path.read_bytes() for path in files}

    local_key = "staging/test/local/local-stages.json"
    local_path = tmp_path / "local-stages.json"
    local_path.write_text(json.dumps(local_manifest))
    objects[local_key] = local_path.read_bytes()
    downloads = []

    def get_object(*, Bucket, Key):
        assert Bucket == bucket
        assert Key.startswith("staging/")
        downloads.append(Key)
        return {"Body": BytesIO(objects[Key])}

    def put_object(*, Bucket, Key, Body, **_):
        assert Bucket == bucket
        assert Key.startswith("staging/comment-fields-fill-")
        objects[Key] = Body
        return {}

    monkeypatch.setattr(r2, "verified_target_client", lambda: (SimpleNamespace(
        get_object=get_object, put_object=put_object), bucket))
    prefix = f"comment-attachment-stage-{digest}-"

    def gh_output(command, **_):
        if command[:3] == ["gh", "run", "view"]:
            return json.dumps({"workflowName": runner.RUNNER_WORKFLOW, "status": "completed"})
        assert command[:2] == ["gh", "api"]
        return "\n".join(f"{prefix}{name}\t1000\tfalse" for name in runner_groups)

    def gh_download(command, **_):
        assert command[:3] == ["gh", "run", "download"]
        target = Path(command[command.index("--dir") + 1])
        names = [command[i + 1] for i, value in enumerate(command) if value == "--name"]
        for name in names:
            dest = target / name if len(names) > 1 else target
            for relative, body in runner_groups[name.removeprefix(prefix)].items():
                path = dest / relative
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(body)

    monkeypatch.setattr(runner.subprocess, "check_output", gh_output)
    monkeypatch.setattr(runner.subprocess, "run", gh_download)
    monkeypatch.setattr(runner.shutil, "disk_usage", lambda _: SimpleNamespace(free=20 * 2**30))
    work = tmp_path / "aggregate-runner"
    work.mkdir()
    return SimpleNamespace(workdir=work, groups_key=key, groups_sha256=digest, local_key=local_key,
                           local_sha256=file_sha256(local_path), source_runs=["123"], repository="x/y",
                           runtime_ref="1" * 40), downloads, objects


def test_aggregate_runner_reads_only_pinned_artifacts_and_emits_census(runner, inputs):
    args, downloads, objects = inputs
    runner.execute(args)
    result = json.loads((args.workdir / "result.json").read_text())
    key = result["controls"]["attachment-census.json"]["key"]
    report = json.loads(objects[key])
    assert result["rows"] == report["observed_comment_copies"] == 1
    assert report["source_gaps"]["statuses"] == {"source-missing": 1}
    assert report["source_gaps"]["quarantined_keys"] == 2
    assert all("/parts/" not in key and "/source-pins." not in key for key in downloads)
    assert all(field["wrong_type"] == 0 for field in report["fields"].values())


def test_aggregate_runner_checks_disk_before_large_downloads(runner, inputs, monkeypatch):
    args, downloads, _ = inputs
    monkeypatch.setattr(shutil, "disk_usage", lambda _: SimpleNamespace(free=4 * 2**30))
    with pytest.raises(ValueError, match="available scratch"):
        runner.execute(args)
    assert not any(key.endswith(("/reads.parquet", "/manifest.parquet")) for key in downloads)
    assert not (args.workdir / "result.json").exists()


def test_aggregate_runner_refuses_repeated_group(runner, inputs):
    args, downloads, _ = inputs
    args.source_runs = ["123", "456"]
    with pytest.raises(ValueError, match="repeated"):
        runner.execute(args)
    assert not any(key.endswith(("/reads.parquet", "/manifest.parquet")) for key in downloads)
