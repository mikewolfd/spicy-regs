"""The source-group executor resumes verified compact output and stops on resource or identity failures."""

import hashlib
from io import BytesIO
import json
from pathlib import Path
import sys
from types import SimpleNamespace

from botocore.exceptions import ClientError
from botocore.hooks import HierarchicalEmitter
import polars as pl
import pytest

from scripts import read_comment_attachment_group as run
from spicy_regs.pipelines import comment_source_groups


class Store:
    def __init__(self, objects):
        self.objects, self.gets = objects, []
        self.meta = SimpleNamespace(events=HierarchicalEmitter())

    def get_object(self, Bucket, Key):
        self.meta.events.emit("before-send.s3.GetObject")
        self.gets.append(Key)
        if Key not in self.objects:
            raise ClientError({"Error": {"Code": "NoSuchKey"}, "ResponseMetadata": {"HTTPStatusCode": 404}}, "GetObject")
        reply = {"Body": BytesIO(self.objects[Key]), "ContentLength": len(self.objects[Key]), "ETag": '"offline"'}
        self.meta.events.emit("after-call.s3.GetObject", parsed=reply)
        return reply

    def put_object(self, Bucket, Key, Body, **_):
        if Key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[Key] = Body
        return {"ETag": '"' + hashlib.md5(Body, usedforsecurity=False).hexdigest() + '"'}


def retained_group(tmp_path):
    fixture = Path(__file__).parent / "fixtures/comments_submitter_fields/CMS-2016-0123-0993.source.json"
    content = fixture.read_bytes()
    key = "raw-data/CMS/CMS-2016-0123/text-CMS-2016-0123/comments/CMS-2016-0123-0993.json"
    manifest, pins = tmp_path / "manifest.parquet", tmp_path / "pins.parquet"
    pl.DataFrame({"key": [key]}).write_parquet(manifest)
    sha256 = hashlib.sha256(content).hexdigest()
    pl.DataFrame({"key": [key], "etag": ['"offline"'], "size": [len(content)], "version_id": ["offline-v1"],
                  "raw_sha256": [sha256], "raw_custody": ["r2"]}).write_parquet(pins)
    folder = tmp_path / "groups"
    comment_source_groups.split(manifest, pins, folder, local_groups=1, runner_groups=1, max_spill="32MB")
    prefix = "staging/comment-attachment-groups/test"
    objects = {f"{prefix}/{path.relative_to(folder)}": path.read_bytes() for path in folder.rglob("*") if path.is_file()}
    original_key = "source-evidence/blobs/sha256/" + sha256
    objects[original_key] = content
    args = SimpleNamespace(groups_key=prefix + "/groups.json", groups_sha256=run.digest(folder / "groups.json"),
                           group="runner-000", runtime_ref="a" * 40, workdir=tmp_path / "work", workers=1)
    args.workdir.mkdir()
    return args, objects, original_key


def setup_clients(monkeypatch, objects):
    from spicy_docs.sources import mirrulations
    from spicy_regs.sources import r2

    clients = []

    def target():
        client = Store(objects)
        clients.append(client)
        return client, r2.VERIFIED_BUCKET

    source = SimpleNamespace(meta=SimpleNamespace(client=Store({})), Object=lambda *_: pytest.fail("Source GET forbidden"))
    monkeypatch.setattr(r2, "verified_target_client", target)
    monkeypatch.setattr(mirrulations, "s3_resource", lambda **_: source)
    return clients


def test_real_retained_group_executes_and_resumes_without_source_reads(tmp_path, monkeypatch):
    args, objects, original_key = retained_group(tmp_path)
    clients = setup_clients(monkeypatch, objects)
    run.execute(args)
    result = json.loads((args.workdir / "result.json").read_text())
    assert result["status"] == "complete" and result["keys"] == 1
    # The artifact's common root is WORKDIR; keep the actual read/plan/stage layout.
    import glob
    import yaml

    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/read-comment-attachments.yml").read_text())
    controls = next(step for step in workflow["jobs"]["read"]["steps"] if step.get("name") == "Retain compact aggregation controls")
    assert controls["if"] == "success()"
    selected = {str(Path(path).relative_to(args.workdir)) for pattern in controls["with"]["path"].splitlines()
                for path in glob.glob(pattern.replace("${{ env.WORKDIR }}", str(args.workdir)))}
    plan = next((args.workdir / "read/plan").glob("*.json"))
    assert selected == {"result.json", "stage/staging.json", "stage/reads.parquet.sha256", str(plan.relative_to(args.workdir))}
    assert not any("parts/" in path or path.endswith("reads.parquet") for path in selected)
    assert sum(client.gets.count(original_key) for client in clients) == 1
    assert json.loads((args.workdir / "source-usage.json").read_text())["attempts"] == 1
    run.execute(args)
    assert sum(client.gets.count(original_key) for client in clients) == 1
    assert pl.read_parquet(args.workdir / "stage/reads.parquet")["attachments_json"].to_list() == [None]
    identity = json.loads((args.workdir / "identity.json").read_text())
    identity["runtime_ref"] = "b" * 40
    (args.workdir / "identity.json").write_text(json.dumps(identity))
    with pytest.raises(ValueError, match="Resume identity"):
        run.execute(args)


def test_retained_original_corruption_refuses_and_keeps_checkpoint_evidence(tmp_path, monkeypatch):
    args, objects, key = retained_group(tmp_path)
    objects[key] = objects[key][:-1] + b"x"
    setup_clients(monkeypatch, objects)
    with pytest.raises(ValueError, match="deferred keys"):
        run.execute(args)
    assert list((args.workdir / "read/parts").glob("*/source-evidence/*.parquet"))
    assert not (args.workdir / "result.json").exists()
    assert json.loads((args.workdir / "source-usage.json").read_text())["attempts"] == 2


def test_group_identity_and_retained_stage_must_match(tmp_path, monkeypatch):
    args, objects, _ = retained_group(tmp_path)
    setup_clients(monkeypatch, objects)
    run.execute(args)
    path = args.workdir / "stage/staging.json"
    value = json.loads(path.read_text())
    value["plan_sha256"] = "0" * 64
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="stage does not belong"):
        run.execute(args)
    args.groups_sha256 = "f" * 64
    with pytest.raises(ValueError, match="Retained input differs"):
        run.execute(args)


def test_source_budget_counts_actual_attempts_and_bytes_across_resume(tmp_path):
    meter = run.SourceMeter(tmp_path, 1, maximum_bytes=3)
    meter.attempt()
    assert run.MeteredBody(BytesIO(b"ab"), meter).read(2) == b"ab"
    meter.close()
    meter = run.SourceMeter(tmp_path, 1, maximum_bytes=3)
    assert (meter.attempts, meter.bytes) == (1, 2)
    meter.attempt()
    with pytest.raises(run.SourceBudgetExceeded, match="attempt"):
        meter.attempt()
    with pytest.raises(run.SourceBudgetExceeded, match="byte"):
        run.MeteredBody(BytesIO(b"cd"), meter).read(2)
    assert meter.bytes == 3
    meter.close()


def test_sdk_retry_hooks_count_wire_attempts_before_another_request(tmp_path):
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from threading import Thread

    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            requests.append(self.path)
            error = len(requests) == 1
            body = b"<Error><Code>SlowDown</Code></Error>" if error else b"abc"
            self.send_response(503 if error else 200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args: object) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = Thread(target=server.serve_forever, kwargs={"poll_interval": .05}, daemon=True)
    thread.start()
    meter = run.SourceMeter(tmp_path, 1)
    client = boto3.client("s3", endpoint_url=f"http://127.0.0.1:{server.server_port}", region_name="us-east-1",
                          aws_access_key_id="offline", aws_secret_access_key="offline",
                          config=Config(signature_version=UNSIGNED, retries={"total_max_attempts": 3}, proxies={}))
    try:
        meter.watch(client)
        reply = client.get_object(Bucket="test", Key="original")
        try:
            assert reply["Body"].read() == b"abc"
        finally:
            reply["Body"].close()
        assert (len(requests), meter.attempts, meter.bytes) == (2, 2, 3)
        with pytest.raises(run.SourceBudgetExceeded):
            client.get_object(Bucket="test", Key="original")
        assert len(requests) == 2
    finally:
        client.close()
        meter.close()
        server.shutdown()
        server.server_close()
        thread.join(timeout=1)


def test_monitor_stops_real_child_tree_and_retains_measurements(tmp_path):
    args = SimpleNamespace(workdir=tmp_path / "work")
    code = "import subprocess,sys,time; subprocess.Popen([sys.executable,'-c','import time; time.sleep(30)']); time.sleep(30)"
    limits = {**run.LIMITS, "wall_seconds": .4, "rss_bytes": 2**30, "stop_free_bytes": 10 * 2**30}
    with pytest.raises(ValueError, match="wall limit"):
        run.supervise(args, command=[sys.executable, "-c", code], limits=limits)
    record = json.loads((args.workdir / "execution.json").read_text())
    assert record["status"] == "failed" and record["peaks"]["rss"] > 0
    assert not record["remaining_processes"]
    closure = json.loads((args.workdir / "process-closure.json").read_text())
    assert len(closure["known"]) >= 2
    assert not run.owner.members(closure["identity"], {})


def test_workflow_has_only_storage_credentials_and_always_keeps_compact_checkpoints():
    import yaml

    path = Path(__file__).parents[1] / ".github/workflows/read-comment-attachments.yml"
    text = path.read_text()
    workflow = yaml.safe_load(text)
    assert list(workflow[True]) == ["workflow_dispatch"]
    job = workflow["jobs"]["read"]
    assert job["env"]["WORKDIR"] == "/tmp/comment-attachment-group"
    assert job["strategy"]["max-parallel"] == 4 and job["timeout-minutes"] == 105
    assert "R2_CATALOG" not in text and "comments-catalog-write" not in text and "CLOUDFLARE" not in text
    assert job["steps"][-1]["if"] == "always()"
    assert job["steps"][1]["with"]["ref"] == "${{ inputs.runtime_ref }}"
