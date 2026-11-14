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
    assert "comments-publish-*.json" in evidence["with"]["path"]
    refresh = yaml.safe_load((root / ".github/workflows/_regulations-refresh.yml").read_text())
    for job in (wrapper["jobs"]["refresh"], refresh["jobs"]["mirror"]):
        assert job["with"]["diagnostic_native_snapshot"] == "${{ inputs.diagnostic_native_snapshot }}"
    assert 'ARGS+=(--diagnostic-native-snapshot "$DIAGNOSTIC_NATIVE_SNAPSHOT")' in runs
    assert '"${ARGS[@]}"' in runs


def test_diagnostic_refuses_upload_and_wrong_subject_before_object_reads(tmp_path, monkeypatch):
    args = SimpleNamespace(skip_upload=False, diagnostic_native_snapshot={"table_uuid": "subject", "snapshot_id": 10, "schema_id": 3})
    with pytest.raises(ValueError, match="require --skip-upload"):
        guard.diagnostic_subject(args)
    client, _ = metadata(monkeypatch)
    wrong = {**args.diagnostic_native_snapshot, "snapshot_id": 11}
    with pytest.raises(ValueError, match="Diagnostic native snapshot differs"):
        guard.observe(client, "owned", evidence=tmp_path / "observed.json", expected_snapshot=wrong)
    assert not client.calls
    assert json.loads((tmp_path / "observed.json").read_text())["complete"] is False


def test_diagnostic_copy_retains_failure_phase_and_restores_original(tmp_path, monkeypatch):
    from scripts import read_comment_attachment_witnesses as witness
    from spicy_regs.sources import regulatory_catalog as native

    subject = {"table_uuid": "subject", "snapshot_id": 10, "schema_id": 3}
    args = SimpleNamespace(workdir=tmp_path, expected_source_sha="a" * 40, skip_upload=True,
                           diagnostic_native_snapshot=subject)
    monkeypatch.setattr(guard.iceberg, "_qualified", lambda _: "controlled_comments")
    connection, resources, parameters = object(), guard.ExportResources(), ["unchanged"]
    monkeypatch.setattr(witness, "runtime_identity", lambda con: {"same_connection": con is connection})
    pair = guard.iceberg.CatalogPairSnapshot(**subject, receipts=guard.iceberg.CatalogSnapshot("receipts", 51, 0))
    def same_transaction(con, record_type, *, namespace):
        assert con is connection and record_type is guard.COMMENT
        assert namespace == guard.iceberg._catalog_namespace(guard.COMMENT)
        return pair
    monkeypatch.setattr(guard.iceberg, "_read_pair_snapshot", same_transaction)
    calls = []

    def failing(con, sql, path, **kwargs):
        calls.append((con, sql, path, kwargs))
        saved = json.loads((tmp_path / "comments-publish-diagnostic.json").read_text())
        assert saved["phase"] == "native_copy" and saved["runtime"]["same_connection"]
        assert saved["copy_transaction_pair"] == guard.asdict(pair)
        raise RuntimeError("credential-like error must not enter structured diagnostic")

    monkeypatch.setattr(native, "_copy", failing)
    with pytest.raises(RuntimeError, match="credential-like"):
        with guard.copy_diagnostics(args, {"snapshot": subject}):
            native._copy(connection, "SELECT * FROM controlled_comments", tmp_path / "copy.parquet",
                         resources=resources, parameters=parameters, ordered=False)
    assert native._copy is failing
    assert calls == [(connection, "SELECT * FROM controlled_comments", tmp_path / "copy.parquet",
                      {"resources": resources, "parameters": parameters, "ordered": False})]
    text = (tmp_path / "comments-publish-diagnostic.json").read_text()
    saved = json.loads(text)
    assert saved["outcome"] == "failed" and saved["phase"] == "native_copy"
    assert saved["native_backtrace"] is False and saved["debugger"]["status"] == "unavailable"
    assert "credential-like" not in text and "unchanged" not in text


def test_diagnostic_uses_real_maintained_copy_and_preserves_nested_values(tmp_path, monkeypatch):
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    from spicy_regs.sources import regulatory_catalog as native

    subject = {"table_uuid": "subject", "snapshot_id": 10, "schema_id": 3}
    args = SimpleNamespace(workdir=tmp_path, expected_source_sha="a" * 40, skip_upload=True,
                           diagnostic_native_snapshot=subject)
    table = pa.Table.from_pylist([
        {"comment_id": "A", "attachments": None}, {"comment_id": "B", "attachments": []},
        {"comment_id": "C", "attachments": [{"title": "fourth", "formats": [{"size": -1, "format": "mp4"}]},
                                                {"title": "second", "formats": None}]}])
    monkeypatch.setattr(guard.iceberg, "_qualified", lambda _: "controlled_comments")
    pair = guard.iceberg.CatalogPairSnapshot(**subject, receipts=guard.iceberg.CatalogSnapshot("receipts", 52, 0))
    monkeypatch.setattr(guard.iceberg, "_read_pair_snapshot", lambda *_a, **_kw: pair)
    original = native._copy
    resources = guard.ExportResources()
    with duckdb.connect() as con:
        resources.configure(con, tmp_path / "spill")
        con.register("controlled_comments", table)
        with guard.copy_diagnostics(args, {"snapshot": subject}):
            native._copy(con, "SELECT * FROM controlled_comments", tmp_path / "copy.parquet", resources=resources)
    assert native._copy is original
    assert pq.read_table(tmp_path / "copy.parquet").to_pylist() == table.to_pylist()
    saved = json.loads((tmp_path / "comments-publish-diagnostic.json").read_text())
    assert saved["phase"] == "validated_no_upload" and saved["native_copy_calls"] == 1
    assert saved["runtime"]["engine_version"] and saved["resources"] == guard.asdict(resources)
    assert saved["copy_transaction_pair"]["receipts"]["snapshot_id"] == 52


@pytest.mark.parametrize("table_uuid,snapshot_id,schema_id", [("new-table", 10, 3), ("subject", 11, 3), ("subject", 10, 4)])
def test_copy_transaction_subject_drift_refuses_before_original_copy(tmp_path, monkeypatch, table_uuid, snapshot_id, schema_id):
    from scripts import read_comment_attachment_witnesses as witness
    from spicy_regs.sources import regulatory_catalog as native

    subject = {"table_uuid": "subject", "snapshot_id": 10, "schema_id": 3}
    args = SimpleNamespace(workdir=tmp_path, expected_source_sha="a" * 40, skip_upload=True,
                           diagnostic_native_snapshot=subject)
    monkeypatch.setattr(guard.iceberg, "_qualified", lambda _: "controlled_comments")
    pair = guard.iceberg.CatalogPairSnapshot(table_uuid, snapshot_id, schema_id,
                                           receipts=guard.iceberg.CatalogSnapshot("actual-receipts", 77, 2))
    monkeypatch.setattr(guard.iceberg, "_read_pair_snapshot", lambda *_a, **_kw: pair)
    monkeypatch.setattr(witness, "runtime_identity", lambda _: pytest.fail("Refuse before runtime inspection"))
    def original(*_a, **_kw):
        pytest.fail("Original COPY must not start")
    monkeypatch.setattr(native, "_copy", original)
    with pytest.raises(ValueError, match="COPY transaction subject differs"):
        with guard.copy_diagnostics(args, {"snapshot": subject}):
            native._copy(object(), "SELECT * FROM controlled_comments", tmp_path / "copy.parquet",
                         resources=guard.ExportResources())
    assert native._copy is original
    saved = json.loads((tmp_path / "comments-publish-diagnostic.json").read_text())
    assert saved["phase"] == "native_copy_snapshot" and saved["outcome"] == "failed"
    assert saved["copy_transaction_pair"] == guard.asdict(pair)
    assert not (tmp_path / "copy.parquet").exists()


def test_publisher_main_fatal_setup_does_not_change_importer():
    import os
    import subprocess
    import sys

    code = '''import faulthandler, json, resource, runpy, sys
sys.argv = [sys.argv[1], '--help']
before = resource.getrlimit(resource.RLIMIT_CORE)
fatal_before = faulthandler.is_enabled()
runpy.run_path(sys.argv[0], run_name='import_probe')
assert resource.getrlimit(resource.RLIMIT_CORE) == before
assert faulthandler.is_enabled() == fatal_before
try:
    runpy.run_path(sys.argv[0], run_name='__main__')
except SystemExit as error:
    assert error.code == 0
print(json.dumps({'fatal': faulthandler.is_enabled(), 'core': resource.getrlimit(resource.RLIMIT_CORE)}))
'''
    env = {k: v for k, v in os.environ.items() if k in {"PATH", "PYTHONPATH", "LANG", "TMPDIR"}}
    result = subprocess.run([sys.executable, "-B", "-c", code, guard.__file__], env=env,
                            text=True, capture_output=True, timeout=15, check=True)
    assert json.loads(result.stdout.splitlines()[-1]) == {"fatal": True, "core": [0, 0]}
