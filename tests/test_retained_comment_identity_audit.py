"""Retained membership custody, R2 backup custody, offline access limits and complete duplicate copies."""

import argparse
import hashlib
import io
import json
from pathlib import Path
import stat
from types import SimpleNamespace
import zipfile
import zlib

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import yaml

from scripts import dedupe_comments_catalog as run
from spicy_regs import operational_runner as owner

SUBJECT = {"table_uuid": "01a1bb8e-f307-7c62-bbe2-e57e4c6fbac9", "snapshot_id": 4394640905803876208, "schema_id": 1}


def package(tmp_path, rows=None, *, schema=None, altered=None):
    if rows is None:
        # Actual original IDs/versions, including all captured native-file copies.
        fixtures = Path(__file__).parent / "fixtures/comment_attachment_unknown_sizes"
        rows = []
        for name in ("HHS-OASH-2022-0006-0002", "FWS-R4-ES-2020-0109-0210"):
            raw = json.loads((fixtures / f"{name}.json").read_text())
            rows.extend(
                [
                    {"comment_id": name, "modify_date": raw["data"]["attributes"]["modifyDate"], "_file": file}
                    for file in ("native/a.parquet", "native/b.parquet")
                ]
            )
        rows += [
            {"comment_id": value, "modify_date": None, "_file": "native/null.parquet"}
            for value in (None, None, "", "", "unique")
        ]
    table = pa.Table.from_pylist(
        rows, schema=schema or pa.schema([(n, pa.string()) for n in ("comment_id", "modify_date", "_file")])
    )
    parquet = io.BytesIO()
    pq.write_table(table, parquet)
    bodies = {
        run.MEMBER: parquet.getvalue(),
        "execution.json": b'{"status":"failed"}',
        "execution.log": b"A staged comment identity appears more than once in the native catalog",
        "process-closure.json": b'{"state":"closing"}',
        "admission-plan.json": b'{"status":"admitted"}',
        "physical/membership-explain.json": json.dumps(
            {"snapshot": SUBJECT, "resources": {"memory": "4GB", "threads": 1, "max_spill": "3GB"}}
        ).encode(),
    }
    members = {
        name: {
            "bytes": len(body),
            "crc32": f"{zlib.crc32(body):08x}",
            "sha256": None if name == run.MEMBER else hashlib.sha256(body).hexdigest(),
        }
        for name, body in bodies.items()
    }
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, body in bodies.items():
            info = zipfile.ZipInfo(name)
            info.external_attr = (stat.S_IFREG | 0o600) << 16
            if altered == "symlink" and name == run.MEMBER:
                info.external_attr = (stat.S_IFLNK | 0o777) << 16
            archive.writestr(info, body)
        if altered == "unexpected":
            archive.writestr("../unexpected", b"refuse")
        if altered == "duplicate":
            with pytest.warns(UserWarning, match="Duplicate name"):
                archive.writestr("execution.json", bodies["execution.json"])
    raw = buffer.getvalue()
    request = {
        "format": "retained-comment-membership/1",
        "artifact": {
            "id": 1,
            "run_id": 2,
            "head_sha": "a" * 40,
            "name": "comment-attachment-fill-plan",
            "bytes": len(raw),
            "sha256": hashlib.sha256(raw).hexdigest(),
        },
        "snapshot": SUBJECT,
        "members": members,
    }
    (tmp_path / "request.json").write_text(json.dumps(request))
    return raw, request, rows


def extracted(tmp_path, **kwargs):
    raw, request, rows = package(tmp_path, **kwargs)
    (tmp_path / "artifact.zip").write_bytes(raw)
    request_pin = run.pin(tmp_path / "request.json")
    run.extract(tmp_path, request, request_pin)
    return request, request_pin, rows


def test_actual_original_id_versions_keep_every_native_copy_and_null_group(tmp_path, monkeypatch):
    request, request_pin, rows = extracted(tmp_path)
    monkeypatch.setattr(run.iceberg, "_connect", lambda: pytest.fail("Native catalog forbidden"))
    with run.without_credentials():
        run.query(tmp_path, request, request_pin, "b" * 40)
    report = json.loads((tmp_path / "audit.json").read_text())
    assert report["status"] == "complete" and report["semantic_verdict"] is None and report["input_unchanged"]
    assert report["counts"] == {
        "duplicate_ids": 4,
        "rows_in_duplicate_groups": 8,
        "excess_rows": 4,
        "membership_identity_groups_including_null": 5,
    }
    expected = [row for row in rows if row["comment_id"] != "unique"]
    assert sorted(json.dumps(row, sort_keys=True) for row in report["rows"]) == sorted(
        json.dumps(row, sort_keys=True) for row in expected
    )
    assert sum(row["comment_id"] is None for row in report["rows"]) == 2
    assert sum(row["comment_id"] == "" for row in report["rows"]) == 2
    assert "IS NOT DISTINCT FROM" in report["queries"]["duplicate_rows"]
    assert report["footer"]["rows"] == 9 and len(report["footer"]["columns"]) == 3


@pytest.mark.parametrize("altered", ["unexpected", "duplicate", "symlink"])
def test_archive_member_preflight_refuses_before_any_extraction(tmp_path, altered):
    raw, request, _ = package(tmp_path, altered=altered)
    (tmp_path / "artifact.zip").write_bytes(raw)
    with pytest.raises(ValueError, match="Archive contains|Unsafe or changed"):
        run.extract(tmp_path, request, run.pin(tmp_path / "request.json"))
    assert not (tmp_path / "inputs").exists()


def test_whole_zip_and_member_tampering_refuse(tmp_path):
    raw, request, _ = package(tmp_path)
    (tmp_path / "artifact.zip").write_bytes(raw + b"changed")
    with pytest.raises(ValueError, match="Whole artifact ZIP pin"):
        run.extract(tmp_path, request, run.pin(tmp_path / "request.json"))
    assert not (tmp_path / "inputs").exists()
    (tmp_path / "artifact.zip").write_bytes(raw)
    run.extract(tmp_path, request, run.pin(tmp_path / "request.json"))
    member = tmp_path / "inputs" / run.MEMBER
    member.chmod(0o644)
    member.write_bytes(member.read_bytes() + b"changed")
    with run.without_credentials(), pytest.raises(ValueError, match="Retained input changed"):
        run.query(tmp_path, request, run.pin(tmp_path / "request.json"), "b" * 40)


def test_acquisition_polls_metadata_and_redirect_without_forwarding_token(tmp_path, monkeypatch):
    raw, request, _ = package(tmp_path)
    expected, calls = request["artifact"], []
    artifact_polls = 0
    zip_polls = 0

    def respond(message):
        nonlocal artifact_polls, zip_polls
        calls.append(message)
        if message.url.host != "api.github.com":
            assert "Authorization" not in message.headers
            return httpx.Response(200, content=raw)
        assert message.headers["Authorization"] == "Bearer fixture-token"
        if message.url.path.endswith("/zip"):
            zip_polls += 1
            if zip_polls == 1:
                return httpx.Response(202)
            return httpx.Response(302, headers={"Location": "https://fixture.blob.core.windows.net/archive.zip"})
        if "/artifacts/" in message.url.path:
            artifact_polls += 1
            return httpx.Response(
                200,
                json={
                    "id": 1,
                    "name": expected["name"],
                    "size_in_bytes": len(raw),
                    "expired": artifact_polls == 1,
                    "digest": "sha256:" + expected["sha256"],
                    "workflow_run": {"id": 2, "head_sha": "a" * 40},
                },
            )
        return httpx.Response(
            200,
            json={
                "id": 2,
                "head_sha": "a" * 40,
                "status": "completed",
                "conclusion": "failure",
                "path": ".github/workflows/fill-comment-attachment-groups.yml",
            },
        )

    client = httpx.Client
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: client(transport=httpx.MockTransport(respond), **kwargs))
    monkeypatch.setattr(run.time, "sleep", lambda _: None)
    monkeypatch.setattr(run.sys, "platform", "linux")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("GITHUB_TOKEN", "fixture-token")
    run.acquire(tmp_path, request, run.pin(tmp_path / "request.json"))
    assert artifact_polls == 2 and zip_polls == 2 and len(calls) == 6
    custody = json.loads((tmp_path / "custody.json").read_text())
    assert custody["archive"]["sha256"] == expected["sha256"]
    assert custody["members"][run.MEMBER]["sha256"] == run.pin(tmp_path / "inputs" / run.MEMBER)["sha256"]
    assert not (tmp_path / "artifact.zip").stat().st_mode & 0o222


def test_backup_uploads_immutably_with_readback_and_rejects_catalog_creds(tmp_path, monkeypatch):
    from spicy_regs.sources import r2 as r2_mod

    request, request_pin, _ = extracted(tmp_path)
    stored: dict[str, bytes] = {}

    class FakeClient:
        def head_object(self, *, Bucket, Key, **_):
            if Key not in stored:
                from botocore.exceptions import ClientError

                raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")
            return {"ContentLength": len(stored[Key]), "ETag": f'"{hashlib.md5(stored[Key]).hexdigest()}"'}

        def put_object(self, *, Bucket, Key, Body, IfNoneMatch=None, **_):
            if IfNoneMatch == "*" and Key in stored:
                from botocore.exceptions import ClientError

                raise ClientError({"Error": {"Code": "PreconditionFailed", "Message": "Exists"}}, "PutObject")
            stored[Key] = Body
            return {"ETag": f'"{hashlib.md5(Body).hexdigest()}"'}

        def get_object(self, *, Bucket, Key):
            return {"Body": io.BytesIO(stored[Key])}

    monkeypatch.setattr(r2_mod, "verified_target_client", lambda: (FakeClient(), r2_mod.VERIFIED_BUCKET))
    monkeypatch.setattr(run.sys, "platform", "linux")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setenv("R2_CATALOG_TOKEN", "forbidden-in-backup")
    with pytest.raises(ValueError, match="GitHub or Iceberg catalog credentials"):
        run.backup(tmp_path, request, request_pin, "b" * 40)
    monkeypatch.delenv("R2_CATALOG_TOKEN")

    with run.without_non_storage_credentials():
        run.backup(tmp_path, request, request_pin, "b" * 40)
    receipt = json.loads((tmp_path / "backup.json").read_text())
    assert receipt["status"] == "complete"
    assert receipt["bucket"] == r2_mod.VERIFIED_BUCKET
    assert not receipt["archive_already_present"]
    assert stored[receipt["archive_key"]] == (tmp_path / "artifact.zip").read_bytes()
    assert json.loads(stored[receipt["custody_key"]])["archive"] == receipt["archive"]

    # Re-running against already-stored immutable objects reuses and verifies readback cleanly.
    with run.without_non_storage_credentials():
        run.backup(tmp_path, request, request_pin, "b" * 40)
    receipt2 = json.loads((tmp_path / "backup.json").read_text())
    assert receipt2["archive_already_present"] and receipt2["custody_already_present"]


@pytest.mark.parametrize("delta, admitted", [(-1, True), (0, False)])
def test_full_coexisting_storage_admission_strict_boundary(tmp_path, monkeypatch, delta, admitted):
    _, request, _ = package(tmp_path)
    proposed = request["artifact"]["bytes"] + sum(v["bytes"] for v in request["members"].values())
    held = 6 * run.GiB - 65536 - proposed - run.SPILL - run.WORKING - run.MAX_JSON + delta
    monkeypatch.setattr(owner, "sizes", lambda *a: {"logical": 0, "allocated": held})
    monkeypatch.setattr(run.shutil, "disk_usage", lambda p: SimpleNamespace(free=20 * run.GiB))
    if admitted:
        run.admission(tmp_path, request, "acquire")
    else:
        with pytest.raises(ValueError, match="storage admission refused"):
            run.admission(tmp_path, request, "acquire")
    record = json.loads((tmp_path / "acquire-admission.json").read_text())
    assert record["status"] == ("admitted" if admitted else "refused") and record["proposed_input_bytes"] == proposed


def test_free_floor_refuses_before_network(tmp_path, monkeypatch):
    _, request, _ = package(tmp_path)
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    monkeypatch.setattr(run.sys, "platform", "linux")
    monkeypatch.setattr(run.shutil, "disk_usage", lambda p: SimpleNamespace(free=4 * run.GiB))
    monkeypatch.setattr(httpx, "Client", lambda **kwargs: pytest.fail("Network before admission"))
    with pytest.raises(ValueError, match="storage admission refused"):
        run.acquire(tmp_path, request, run.pin(tmp_path / "request.json"))


@pytest.mark.parametrize("bound", ["ids", "copies", "bytes"])
def test_output_limits_retain_complete_counts_and_explicit_refusal(tmp_path, monkeypatch, bound):
    if bound == "ids":
        values = [str(i) for i in range(1001) for _ in range(2)]
    else:
        values = ["A"] * (10_001 if bound == "copies" else 2)
    rows = [
        {"comment_id": value, "modify_date": None, "_file": "x" * (100_000 if bound == "bytes" else 1)}
        for value in values
    ]
    request, request_pin, _ = extracted(tmp_path, rows=rows)
    if bound == "bytes":
        monkeypatch.setattr(run, "MAX_JSON", 32_768)
    with run.without_credentials(), pytest.raises(ValueError, match="bound refused|exceed report byte"):
        run.query(tmp_path, request, request_pin, "b" * 40)
    report = json.loads((tmp_path / "audit.json").read_text())
    assert report["status"] == "refused" and report["semantic_verdict"] is None and report["input_unchanged"]
    assert "rows" not in report and "duplicate_ids" not in report
    assert report["counts"]["rows_in_duplicate_groups"] == len(values)
    assert json.loads((tmp_path / "counts.json").read_text()) == report["counts"]


def test_only_admitted_local_file_is_readable_and_settings_are_locked(tmp_path):
    request, _, _ = extracted(tmp_path)
    assert request["snapshot"] == SUBJECT
    member = (tmp_path / "inputs" / run.MEMBER).resolve()
    denied = tmp_path / "unlisted.parquet"
    pq.write_table(pa.table({"value": [1]}), denied)
    with run._restricted_connection(member, tmp_path / "scratch/spill") as con:
        assert con.execute("SELECT count(*) FROM read_parquet(?)", [str(member)]).fetchone() == (9,)
        for sql, params in [
            ("SELECT * FROM read_parquet(?)", [str(denied)]),
            ("SELECT * FROM read_json_auto('https://example.invalid/denied')", []),
            ("SET enable_external_access=true", []),
            ("SET memory_limit='8GB'", []),
        ]:
            with pytest.raises(duckdb.Error):
                con.execute(sql, params)
        assert con.execute("SELECT current_setting('allow_persistent_secrets')").fetchone() == (False,)


def test_supervisor_strips_credentials_and_charges_acquisition_and_backup_elapsed(tmp_path, monkeypatch):
    from scripts import read_comment_attachment_group as supervisor

    request, _, _ = extracted(tmp_path)
    monkeypatch.setattr(run.subprocess, "check_output", lambda *a, **kw: "b" * 40)
    prior_acquire = {"status": "complete", "remaining_processes": [], "limits": run.LIMITS, "elapsed_seconds": 22.5}
    prior_backup = {
        "status": "complete",
        "remaining_processes": [],
        "limits": {**run.LIMITS, "wall_seconds": 877.5},
        "elapsed_seconds": 10.0,
    }
    (tmp_path / "acquire-execution.json").write_text(json.dumps(prior_acquire))
    (tmp_path / "backup-execution.json").write_text(json.dumps(prior_backup))
    (tmp_path / "backup.json").write_text(json.dumps({"status": "complete"}))
    for name in ("GITHUB_TOKEN", "GH_TOKEN", "R2_CATALOG_TOKEN", "AWS_SECRET_ACCESS_KEY", "ACTIONS_RUNTIME_TOKEN"):
        monkeypatch.setenv(name, "must-not-reach-child")

    def supervised(args, *, command, limits):
        assert not any(run._credential(name) for name in run.os.environ)
        assert limits["wall_seconds"] == 867.5 and limits["rss_bytes"] == 8 * run.GiB
        assert "--worker" in command and str(tmp_path / "request.json") in command
        run.query(tmp_path, request, run.pin(tmp_path / "request.json"), "b" * 40)
        (tmp_path / "execution.json").write_text(
            json.dumps({"status": "complete", "remaining_processes": [], "elapsed_seconds": 0.2})
        )

    monkeypatch.setattr(supervisor, "supervise", supervised)
    args = argparse.Namespace(
        workdir=tmp_path,
        retained_request=tmp_path / "request.json",
        runtime_ref="b" * 40,
        phase="query",
        worker=False,
    )
    assert run.retained(args) == 0
    complete = json.loads((tmp_path / "completion.json").read_text())
    assert complete["elapsed_seconds"] == 32.7 and complete["semantic_verdict"] is None
    assert complete["report"] == run.pin(tmp_path / "audit.json")
    assert complete["backup"] == run.pin(tmp_path / "backup.json")
    assert run.os.environ["GITHUB_TOKEN"] == "must-not-reach-child"
    with pytest.raises(ValueError, match="already attempted"):
        run.retained(args)


def test_multiphase_retained_audit_charges_cumulative_wall_seconds_and_rejects_tampering(tmp_path, monkeypatch):
    from scripts import read_comment_attachment_group as supervisor

    request, request_pin, _ = extracted(tmp_path)
    monkeypatch.setattr(run.subprocess, "check_output", lambda *a, **kw: "b" * 40)
    phase_elapsed = {"acquire": 12.345678, "backup": 7.654321, "query": 0.5}
    recorded_limits: dict[str, dict[str, float | int]] = {}

    def supervised(args, *, command, limits):
        phase = command[command.index("--phase") + 1]
        recorded_limits[phase] = dict(limits)
        if phase == "backup":
            (tmp_path / "backup.json").write_text(json.dumps({"status": "complete"}))
        elif phase == "query":
            run.query(tmp_path, request, request_pin, "b" * 40)
        (tmp_path / "execution.json").write_text(
            json.dumps(
                {
                    "status": "complete",
                    "remaining_processes": [],
                    "limits": dict(limits),
                    "elapsed_seconds": phase_elapsed[phase],
                }
            )
        )

    monkeypatch.setattr(supervisor, "supervise", supervised)
    for phase in ("acquire", "backup", "query"):
        args = argparse.Namespace(
            workdir=tmp_path,
            retained_request=tmp_path / "request.json",
            runtime_ref="b" * 40,
            phase=phase,
            worker=False,
        )
        assert run.retained(args) == 0

    assert recorded_limits["acquire"]["wall_seconds"] == 900
    assert recorded_limits["backup"]["wall_seconds"] == pytest.approx(900 - phase_elapsed["acquire"])
    assert recorded_limits["query"]["wall_seconds"] == pytest.approx(
        900 - phase_elapsed["acquire"] - phase_elapsed["backup"]
    )
    backup_exec = json.loads((tmp_path / "backup-execution.json").read_text())
    assert backup_exec["limits"]["wall_seconds"] == pytest.approx(900 - phase_elapsed["acquire"])
    complete = json.loads((tmp_path / "completion.json").read_text())
    assert complete["elapsed_seconds"] == pytest.approx(sum(phase_elapsed.values()))

    # Tampered backup-execution.json must still be rejected by _charge_prior.
    expected_backup_wall = 900 - phase_elapsed["acquire"]
    valid_backup = {
        "status": "complete",
        "remaining_processes": [],
        "limits": {**run.LIMITS, "wall_seconds": expected_backup_wall},
        "elapsed_seconds": phase_elapsed["backup"],
    }
    tampered_cases = [
        ("altered rss_bytes", {**valid_backup, "limits": {**valid_backup["limits"], "rss_bytes": 16 * run.GiB}}),
        ("altered output_bytes", {**valid_backup, "limits": {**valid_backup["limits"], "output_bytes": 12 * run.GiB}}),
        ("unreduced wall_seconds", {**valid_backup, "limits": {**valid_backup["limits"], "wall_seconds": 900}}),
        ("nudged wall_seconds", {**valid_backup, "limits": {**valid_backup["limits"], "wall_seconds": expected_backup_wall + 0.01}}),
        ("nonzero remaining_processes", {**valid_backup, "remaining_processes": [12345]}),
        ("negative elapsed_seconds", {**valid_backup, "elapsed_seconds": -0.1}),
        ("budget exhausted", {**valid_backup, "elapsed_seconds": expected_backup_wall}),
    ]
    for label, payload in tampered_cases:
        (tmp_path / "backup-execution.json").write_text(json.dumps(payload))
        limits: dict[str, float | int] = {"wall_seconds": expected_backup_wall, **{k: v for k, v in run.LIMITS.items() if k != "wall_seconds"}}
        with pytest.raises(ValueError, match="did not close|budget exhausted"):
            run._charge_prior(tmp_path, "backup-execution.json", limits, f"R2 backup ({label})")


def test_retained_workflow_cannot_enter_native_or_partition_jobs_or_upload_input():
    workflow = yaml.safe_load((Path(__file__).parents[1] / ".github/workflows/dedupe-comments-catalog.yml").read_text())
    jobs = workflow["jobs"]
    assert "retained_membership == ''" in jobs["audit"]["if"]
    assert "retained_membership == ''" in jobs["probe-partition"]["if"]
    job = jobs["retained-membership"]
    assert job["permissions"] == {"contents": "read", "actions": "read"}
    assert job["runs-on"] == "ubuntu-24.04" and "R2_CATALOG_" not in str(job)
    acquire = next(step for step in job["steps"] if "Acquire and verify" in step.get("name", ""))
    backup = next(step for step in job["steps"] if "Back up verified" in step.get("name", ""))
    query = next(step for step in job["steps"] if "Audit only admitted" in step.get("name", ""))
    assert set(acquire["env"]) == {"GITHUB_TOKEN", "RUNTIME_REF"}
    assert set(backup["env"]) == {
        "RUNTIME_REF",
        "R2_ACCESS_KEY_ID",
        "R2_SECRET_ACCESS_KEY",
        "R2_ENDPOINT",
        "R2_BUCKET_NAME",
    }
    assert set(query["env"]) == {"RUNTIME_REF"}
    paths = job["steps"][-1]["with"]["path"]
    assert ".parquet" not in paths and ".zip" not in paths
    assert workflow["concurrency"]["group"] == "comments-catalog-write"
