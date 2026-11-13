"""Admission reads metadata only and the monitored publisher keeps its existing behavior."""

from pathlib import Path
from types import SimpleNamespace
import json

import pytest

from scripts import publish_comments_mirror_guarded as guard
from spicy_regs.pipelines import comments_mirror


class Heads:
    def __init__(self):
        self.calls = []
        self.objects = {"native/data.parquet": 200, "native/delete.parquet": 30, "comments.parquet": 300}

    def head_object(self, *, Bucket, Key):
        assert Bucket == "owned"
        self.calls.append(Key)
        return {"ContentLength": self.objects[Key], "ETag": '"' + Key + '"', "VersionId": "v1"}


def metadata(monkeypatch, *, moved=False, wrong_bucket=False):
    client = Heads()
    first = guard.iceberg.CatalogSnapshot("subject", 10, 3)
    snapshots = iter([first, guard.iceberg.CatalogSnapshot("subject", 11, 3) if moved else first])

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def execute(self, sql, args=None):
            assert sql.startswith("SET ") or ("iceberg_metadata(?)" in sql and "status <> 'DELETED'" in sql)
            return self

        def fetchall(self):
            bucket = "foreign" if wrong_bucket else "owned"
            return [(f"s3://{bucket}/native/data.parquet",), (f"s3://{bucket}/native/delete.parquet",),
                    (f"s3://{bucket}/native/data.parquet",)]

    monkeypatch.setattr(guard.iceberg, "_connect", Connection)
    monkeypatch.setattr(guard.iceberg, "_catalog_namespace", lambda _: "default_native")
    monkeypatch.setattr(guard.iceberg, "_qualified", lambda _: "catalog.default_native.comments")
    monkeypatch.setattr(guard.iceberg, "_read_snapshot", lambda *a, **kw: next(snapshots))
    return client, first


def test_admission_sizes_data_deletes_and_prior_without_payload_reads(tmp_path, monkeypatch):
    client, snapshot = metadata(monkeypatch)
    observed = guard.observe(client, "owned")
    assert observed["native_bytes"] == 230 and len(observed["native_objects"]) == 2
    assert set(client.calls) == set(client.objects)
    assert observed["snapshot"] == guard.asdict(snapshot)
    monkeypatch.setattr(guard.shutil, "disk_usage", lambda _: SimpleNamespace(free=100 * guard.GiB))
    admitted = guard.admission(tmp_path, observed)
    assert admitted["admitted"] and admitted["output_cap_bytes"] == 5 * 300 + 8 * guard.GiB
    assert admitted["required_free_bytes"] == admitted["output_cap_bytes"] + guard.FREE_FLOOR
    assert admitted["resources"]["memory"] == "3GB" and admitted["resources"]["threads"] == 1


@pytest.mark.parametrize("option,reason", [("moved", "Catalog moved"), ("wrong_bucket", "different storage target")])
def test_admission_refuses_moving_or_foreign_native_objects(monkeypatch, option, reason):
    client, _ = metadata(monkeypatch, **{option: True})
    with pytest.raises(ValueError, match=reason):
        guard.observe(client, "owned")
    assert "comments.parquet" not in client.calls


def test_insufficient_disk_retains_refusal_before_publisher(tmp_path, monkeypatch):
    client, _ = metadata(monkeypatch)
    monkeypatch.setattr(guard.r2, "verified_target_client", lambda: (client, "owned"))
    monkeypatch.setattr(guard.shutil, "disk_usage", lambda _: SimpleNamespace(free=guard.FREE_FLOOR))
    monkeypatch.setattr(comments_mirror, "publish_comments_mirror", lambda *a, **kw: pytest.fail("Export must not start"))
    args = SimpleNamespace(workdir=tmp_path, phase="admit", expected_source_sha="a" * 40)
    with pytest.raises(ValueError, match="Insufficient disk"):
        guard.worker(args)
    assert json.loads((tmp_path / "comments-admission.json").read_text())["admitted"] is False


def test_publisher_keeps_default_resources_and_rechecks_snapshot(tmp_path, monkeypatch):
    client, snapshot = metadata(monkeypatch)
    observed = guard.observe(client, "owned")
    monkeypatch.setattr(guard.shutil, "disk_usage", lambda _: SimpleNamespace(free=100 * guard.GiB))
    receipt = {**guard.admission(tmp_path, observed), "source_commit": "a" * 40}
    guard.owner.durable(tmp_path / "comments-admission.json", receipt)
    monkeypatch.setattr(guard.r2, "verified_target_client", lambda: (client, "owned"))
    monkeypatch.setattr(guard.iceberg, "_read_snapshot", lambda *a, **kw: snapshot)
    calls = []
    monkeypatch.setattr(comments_mirror, "publish_comments_mirror", lambda *a, **kw: calls.append((a, kw)))
    args = SimpleNamespace(workdir=tmp_path, phase="publish", expected_source_sha="a" * 40, skip_upload=True, force=False)
    guard.worker(args)
    assert calls == [((tmp_path,), {"resources": guard.ExportResources(), "skip_upload": True, "force": False})]
    monkeypatch.setattr(guard.iceberg, "_read_snapshot", lambda *a, **kw: guard.iceberg.CatalogSnapshot("subject", 11, 3))
    with pytest.raises(ValueError, match="snapshot/schema changed"):
        guard.worker(args)
    assert len(calls) == 1


def test_real_watchdog_retains_admission_failure_and_closes_child(tmp_path, monkeypatch):
    import sys
    monkeypatch.setenv(guard.iceberg.CATALOG_LOCK_ENV, "comments-catalog-write")
    args = SimpleNamespace(workdir=tmp_path, expected_source_sha="a" * 40, skip_upload=True, force=False)
    original = guard.runner.supervise
    def bounded(*a, **kw):
        assert kw["command"][-2:] == ["--phase", "admit"]
        return original(*a, command=[sys.executable, "-c", "import time; time.sleep(30)"],
                        limits={**guard.PRELIMITS, "wall_seconds": .15})
    monkeypatch.setattr(guard.runner, "supervise", bounded)
    with pytest.raises(ValueError, match="wall limit"):
        guard.run(args)
    execution = json.loads((tmp_path / "comments-admit-execution.json").read_text())
    assert execution["status"] == "failed" and execution["remaining_processes"] == []
    assert (tmp_path / "comments-admit-process-closure.json").exists()


def test_workflow_preserves_lock_runtime_and_always_evidence():
    import yaml
    root = Path(__file__).parents[1]
    wrapper = yaml.safe_load((root / ".github/workflows/publish-comments-mirror.yml").read_text())
    assert wrapper["concurrency"]["group"] == "comments-catalog-write"
    workflow = yaml.safe_load((root / ".github/workflows/_comments-mirror.yml").read_text())
    job = workflow["jobs"]["publish"]
    assert job["timeout-minutes"] == 180
    steps = job["steps"]
    runs = "\n".join(step.get("run", "") for step in steps)
    assert "uv sync --frozen --extra operational-runner" in runs
    assert 'publish_comments_mirror_guarded.py' in runs and '--expected-source-sha "$GITHUB_SHA"' in runs
    assert "uv run --frozen" not in runs
    evidence = next(step for step in steps if step.get("name") == "Retain publication receipts and resource measurements")
    assert evidence["if"] == "always()" and "comments-admit-*.json" in evidence["with"]["path"]
