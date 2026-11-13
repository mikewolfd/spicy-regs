"""Hosted control exercises the actual maintained stage/prepare/write/undo with offline storage."""

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import fill_comment_attachment_groups as run
from spicy_regs.pipelines import comment_fields as cf, comment_fields_write as writer
from tests.test_comment_fields_write import LISTED, reads, row, seed, table
from tests.test_comment_fields_write import catalog as _catalog
from tests.test_read_comment_attachment_group import Store

catalog = _catalog


class JournalStore(Store):
    def download_file(self, bucket, key, path):
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_bytes(self.objects[key])

    def upload_file(self, path, bucket, key):
        self.objects[key] = Path(path).read_bytes()

    def get_paginator(self, _):
        def paginate(**kwargs):
            return [{"Contents": [{"Key": key} for key in self.objects if key.startswith(kwargs["Prefix"])]}]
        return SimpleNamespace(paginate=paginate)


def setup_plan(tmp_path, catalog):
    seed(catalog, [row("A"), row("B", agency="CMS"), row("C", agency="FDA")])
    source, stage = tmp_path / "source", tmp_path / "stage"
    reads(source, [{"key": "a", "comment_id": "A", "attachments_json": LISTED},
                   {"key": "a(1)", "comment_id": "A", "attachments_json": None},
                   {"key": "b", "comment_id": "B", "attachments_json": LISTED},
                   {"key": "c", "comment_id": "C", "attachments_json": LISTED}])
    cf.stage(source, stage, profile="attachments")
    store = JournalStore({f"staging/aggregate/{name}": (stage / name).read_bytes() for name in ("staging.json", "reads.parquet")})
    config = {field: {"key": f"staging/aggregate/{name}", **run.pin(stage / name)}
              for field, name in (("staging", "staging.json"), ("reads", "reads.parquet"))}
    args = SimpleNamespace(workdir=tmp_path / "plan", config=config, runtime_ref="a" * 40, operation="plan")
    args.workdir.mkdir()
    run.make_plan(args, store, "test")
    return store, run.load(args.workdir / "result.json")["plan"]


def arguments(tmp_path, plan, operation="pilot", group=0):
    root = tmp_path / f"job-{group}"
    root.mkdir(exist_ok=True)
    return SimpleNamespace(workdir=root, config={"plan": plan, "group": group}, runtime_ref="a" * 40,
                           operation=operation, group=str(group))


def test_real_group_write_resume_and_undo_preserve_conflicting_copies(tmp_path, catalog, monkeypatch):
    store, plan = setup_plan(tmp_path, catalog)
    monkeypatch.setattr(run.maintenance, "run", lambda *a: {})
    args = arguments(tmp_path, plan)
    before_gets = len(store.gets)
    run.run_group(args, store, "test")
    assert table(catalog)["B"]["attachments_json"] == LISTED
    assert table(catalog)["A"]["attachments_json"] is None
    assert not any("aggregate/reads" in key for key in store.gets[before_gets:])
    result = run.load(args.workdir / "result.json")
    assert result["pilot"] and result["written"]["rows_changed"] == 1
    run.recovery(args)
    args.config["recovery_sha256"] = run.digest(args.workdir / "recovery.json")
    args.operation = "resume"
    run.run_group(args, store, "test")
    result = run.load(args.workdir / "result.json")
    assert result["pilot"] and result["written"]["rows_changed"] == 1
    assert result["written"]["new_rows_changed"] == 0
    # Restored all-copy group remains quarantined by the existing writer's disagreement rule.
    blocked = arguments(tmp_path, plan, group=1)
    run.run_group(blocked, store, "test")
    assert run.load(blocked.workdir / "result.json")["written"]["rows_changed"] == 0
    assert table(catalog)["A"]["attachments_json"] is None
    run.recovery(args)
    args.config["recovery_sha256"] = run.digest(args.workdir / "recovery.json")
    prepared = run.load(args.workdir / "writer/fill/prepare.json")
    line = writer._lines(writer._journal(args.workdir / "writer", prepared))[-1]
    args.operation = "undo"
    args.config.update(undo_batch=line["batch"], expected_snapshot=line["snapshot_after"]["snapshot_id"])
    run.run_group(args, store, "test")
    assert table(catalog)["B"]["attachments_json"] is None


def test_retained_original_through_physical_group_to_native(tmp_path, catalog, monkeypatch):
    from spicy_regs.schemas import COMMENT
    original = Path(__file__).parent / "fixtures/comments_submitter_fields/EPA-HQ-OW-2022-0114-0017.source.json"
    payload = json.loads(original.read_text())
    extracted = COMMENT.extract(payload)
    identity = payload["data"]["id"]
    key = f"raw-data/EPA/EPA-HQ-OW-2022-0114/text-EPA-HQ-OW-2022-0114/comments/{identity}.json"
    part = cf._attachment_part_row(SimpleNamespace(key=key, payload=payload))
    source, stage = tmp_path / "source", tmp_path / "stage"
    reads(source, [part])
    cf.stage(source, stage, profile="attachments")
    seed(catalog, [row(identity, modify=extracted["modify_date"], title=extracted["title"], comment=extracted["comment"])])
    store = JournalStore({f"staging/aggregate/{name}": (stage / name).read_bytes() for name in ("staging.json", "reads.parquet")})
    config = {field: {"key": f"staging/aggregate/{name}", **run.pin(stage / name)}
              for field, name in (("staging", "staging.json"), ("reads", "reads.parquet"))}
    planning = SimpleNamespace(workdir=tmp_path / "plan", config=config, runtime_ref="a" * 40, operation="plan")
    planning.workdir.mkdir()
    run.make_plan(planning, store, "test")
    args = arguments(tmp_path, run.load(planning.workdir / "result.json")["plan"])
    monkeypatch.setattr(run.maintenance, "run", lambda *a: {})
    run.run_group(args, store, "test")
    final = table(catalog)[identity]
    assert final["attachments_json"] == extracted["attachments_json"]
    assert final["modify_date"] == "2022-05-05T00:43:20Z"
    (tmp_path / "native-output.json").write_text(json.dumps(final, indent=2) + "\n")
    (tmp_path / "original-pin.json").write_text(json.dumps({"path": str(original), **run.pin(original)}))


def test_committed_failure_blocks_other_group_and_retains_preimages(tmp_path, catalog, monkeypatch):
    store, plan = setup_plan(tmp_path, catalog)
    monkeypatch.setattr(run.maintenance, "run", lambda *a: {})
    original = writer._our_commit
    def bad(con, before, changed, **kwargs):
        actual, _ = original(con, before, changed, **kwargs)
        return actual, "planted verification failure"
    monkeypatch.setattr(writer, "_our_commit", bad)
    args = arguments(tmp_path, plan)
    with pytest.raises(writer.FillVerificationError, match="committed"):
        run.run_group(args, store, "test")
    run.recovery(args)
    assert list((args.workdir / "writer/fill/preimage").glob("*-written.parquet"))
    writer.sync_journals(args.workdir / "writer", "staging/aggregate/journals", push=True, client=store)
    other = arguments(tmp_path, plan, group=2)
    with pytest.raises(RuntimeError, match="committed and then failed"):
        run.run_group(other, store, "test")
    assert not (other.workdir / "writer/fill/prepare.json").exists()


def test_pending_without_recovery_and_divergent_journals_refuse(tmp_path):
    store = JournalStore({"staging/journals/write-journal-x.jsonl": b'{"state":"pending","prepare_id":"x"}\n'})
    run.pull_journals(tmp_path, "staging/journals", store)
    with pytest.raises(ValueError, match="exact earlier recovery"):
        run.pending_guard(tmp_path / "writer")
    store.objects["staging/journals/write-journal-x.jsonl"] = b'{"state":"other"}\n'
    with pytest.raises(ValueError, match="diverged"):
        run.pull_journals(tmp_path, "staging/journals", store)


def test_tampered_recovery_and_budget_admission_refuse(tmp_path, monkeypatch):
    args = arguments(tmp_path, {"sha256": "b" * 64})
    path = args.workdir / "writer/fill/prepare.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    run.recovery(args)
    args.config["recovery_sha256"] = run.digest(args.workdir / "recovery.json")
    path.write_text("changed")
    with pytest.raises(ValueError, match="member differs"):
        run.recovery(args, verify=True)
    monkeypatch.setattr(run.shutil, "disk_usage", lambda p: SimpleNamespace(free=4 * run.GiB))
    with pytest.raises(ValueError, match="reserve admission"):
        run.admit(args.workdir, download=1)


@pytest.mark.parametrize("failure", ["supervisor", "suspend"])
def test_cleanup_restores_after_partial_suspend_or_watchdog_stop(tmp_path, monkeypatch, failure):
    args = arguments(tmp_path, {"sha256": "b" * 64})
    monkeypatch.setenv(run.iceberg.CATALOG_LOCK_ENV, "comments-catalog-write")
    calls = []
    def maintenance(operation, path):
        calls.append(operation)
        if operation == failure:
            raise RuntimeError("partial maintenance failure")
    monkeypatch.setattr(run.maintenance, "run", maintenance)
    def supervise(*a, **kw):
        assert Path(kw["command"][2]).name == "fill_comment_attachment_groups.py"
        raise RuntimeError("watchdog stopped writer")
    monkeypatch.setattr(run.runner, "supervise", supervise)
    with pytest.raises(RuntimeError):
        run.guarded(args)
    assert calls == ["suspend", "restore"]
    assert (args.workdir / "recovery.json").exists()


def test_whole_file_plan_refuses_split_and_fill_requires_successful_pilot(tmp_path, catalog, monkeypatch):
    monkeypatch.setattr(run, "MAX_ROWS", 1)
    seed(catalog, [row("A"), row("B")])
    # setup_plan gives each agency one candidate; force the planner to return a split-file descriptor.
    original = run.groups.plan
    def split(*a, **kw):
        plan = original(*a, **kw)
        plan["groups"][1]["original_file"] = plan["groups"][0]["original_file"]
        return plan
    monkeypatch.setattr(run.groups, "plan", split)
    with pytest.raises(ValueError, match="whole-file bounds"):
        setup_plan(tmp_path, catalog)


def test_remaining_fill_requires_exact_writing_pilot(tmp_path, catalog):
    store, plan = setup_plan(tmp_path, catalog)
    args = arguments(tmp_path, plan, operation="fill")
    pilot = {"pilot": True, "status": "complete", "written": {"rows_changed": 0}, "plan_sha256": plan["sha256"],
             "runtime_ref": args.runtime_ref, "group": 0}
    path = tmp_path / "pilot.json"
    def pinned():
        path.write_text(json.dumps(pilot))
        store.objects["staging/pilot.json"] = path.read_bytes()
        args.config["pilot"] = {"key": "staging/pilot.json", **run.pin(path)}
        (args.workdir / "pilot.json").unlink(missing_ok=True)
    pinned()
    with pytest.raises(ValueError, match="successful writing pilot"):
        run.choose(args, store, "test")
    pilot["written"]["rows_changed"] = 1
    pinned()
    assert run.choose(args, store, "test") == [1, 2]
    pilot["plan_sha256"] = "0" * 64
    pinned()
    with pytest.raises(ValueError, match="exact plan/runtime"):
        run.choose(args, store, "test")


def test_resume_verifies_old_settings_before_capturing_new_guard(tmp_path, catalog, monkeypatch):
    store, plan = setup_plan(tmp_path, catalog)
    args = arguments(tmp_path, plan)
    monkeypatch.setattr(run.maintenance, "run", lambda *a: {})
    run.run_group(args, store, "test")
    run.owner.durable(args.workdir / "maintenance-state.json", {"generation": "prior"})
    run.recovery(args)
    args.config["recovery_sha256"] = run.digest(args.workdir / "recovery.json")
    args.operation = "resume"
    monkeypatch.setenv(run.iceberg.CATALOG_LOCK_ENV, "comments-catalog-write")
    monkeypatch.setattr(run, "verified_target_client", lambda: (store, "test"))
    calls = []
    def maintenance(operation, path):
        calls.append(operation)
        value = {"generation": "new"} if operation == "suspend" else run.load(path)
        run.owner.durable(path, {**value, "operation": operation})
    monkeypatch.setattr(run.maintenance, "run", maintenance)
    monkeypatch.setattr(run.runner, "supervise", lambda *a, **kw: run.run_group(args, store, "test"))
    run.guarded(args)
    assert calls == ["restore", "suspend", "verify", "restore"]
    assert run.load(args.workdir / "prior-maintenance-state.json")["generation"] == "prior"
    assert run.load(args.workdir / "maintenance-state.json")["generation"] == "new"
    assert run.load(args.workdir / "result.json")["written"]["new_rows_changed"] == 0
    assert (args.workdir / "published-result.json").exists()


def test_real_watchdog_stop_still_restores_maintenance(tmp_path, monkeypatch):
    import sys
    args = arguments(tmp_path, {"sha256": "b" * 64})
    monkeypatch.setenv(run.iceberg.CATALOG_LOCK_ENV, "comments-catalog-write")
    calls = []
    monkeypatch.setattr(run.maintenance, "run", lambda operation, path: calls.append(operation))
    original = run.runner.supervise
    def bounded(*a, **kw):
        return original(*a, command=[sys.executable, "-c", "import time; time.sleep(30)"],
                        limits={**run.LIMITS, "wall_seconds": .15, "rss_bytes": run.GiB})
    monkeypatch.setattr(run.runner, "supervise", bounded)
    with pytest.raises(ValueError, match="wall limit"):
        run.guarded(args)
    assert calls == ["suspend", "restore"]
    result = run.load(args.workdir / "execution.json")
    assert result["status"] == "failed" and result["remaining_processes"] == []


def test_always_cleanup_never_publishes_rejected_prior_recovery(tmp_path, monkeypatch):
    args = arguments(tmp_path, {"sha256": "b" * 64})
    monkeypatch.setenv("GITHUB_RUN_ID", "200")
    run.owner.durable(args.workdir / "authorized.json", {**run.ownership(args), "attempt": "199:1"})
    monkeypatch.setattr(run.maintenance, "run", lambda *a: pytest.fail("Unverified prior settings must not restore"))
    monkeypatch.setattr(run, "verified_target_client", lambda: pytest.fail("Unverified journals must not publish"))
    run.finalize(args)
    assert not (args.workdir / "recovery.json").exists()


def test_workflow_queues_writer_and_keeps_always_recovery():
    import yaml
    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/fill-comment-attachment-groups.yml").read_text())
    assert workflow["concurrency"]["group"] == "comments-catalog-write"
    assert workflow["concurrency"]["cancel-in-progress"] is False
    job = workflow["jobs"]["fill"]
    assert job["strategy"]["max-parallel"] == 1 and job["strategy"]["fail-fast"] is True
    steps = job["steps"]
    assert any(s.get("if", "").startswith("always()") and "--finalize" in s.get("run", "") for s in steps)
    assert steps[-1]["if"] == "always()" and "writer/fill/" in steps[-1]["with"]["path"]
    assert "runner.temp" not in str(job["env"])
    assert all("--no-sync" in s["run"] for s in steps if "uv run" in s.get("run", ""))
