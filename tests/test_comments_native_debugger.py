"""Controlled protocol and real supervisor checks; these do not qualify an installed GDB."""

from types import SimpleNamespace
from typing import Any
import json
import os
import sys

import pytest

from scripts import comments_native_debugger as debugger
from scripts import publish_comments_mirror_guarded as guard
from scripts import read_comment_attachment_group as runner


SUBJECT = {"table_uuid": "subject", "snapshot_id": 12, "schema_id": 1}


@pytest.mark.parametrize("change", [dict(skip_upload=False), dict(comments_only=False), dict(subject=None),
                                     dict(subject={**SUBJECT, "snapshot_id": True}), dict(expected_sha="")])
def test_native_debugger_mode_refuses_without_exact_no_upload_inputs(change):
    args = dict(mode="gdb", expected_sha="a" * 64, skip_upload=True, comments_only=True, subject=SUBJECT)
    args.update(change)
    with pytest.raises(ValueError):
        debugger.validate_mode(**args)


def test_secret_free_environment_has_only_explicit_names(monkeypatch):
    monkeypatch.setenv("R2_SECRET_ACCESS_KEY", "private")
    monkeypatch.setenv("UNRECOGNIZED_PRIVATE_TOKEN", "private")
    monkeypatch.setenv("DEBUGINFOD_URLS", "https://must-not-fetch.invalid")
    env = debugger.safe_environment()
    assert "R2_SECRET_ACCESS_KEY" not in env and "UNRECOGNIZED_PRIVATE_TOKEN" not in env
    assert "HOME" not in env and env["DEBUGINFOD_URLS"] == ""
    assert env["PYTHONPATH"] == str(debugger.ROOT / "src") + os.pathsep + str(debugger.ROOT)


def test_missing_debugger_fails_before_cases_or_production_imports(tmp_path, monkeypatch):
    monkeypatch.setattr(debugger.shutil, "which", lambda _: None)
    monkeypatch.setattr(debugger, "launch_command", lambda *_a, **_kw: pytest.fail("No child permitted"))
    with pytest.raises(ValueError, match="unavailable"):
        debugger.qualify_worker(SimpleNamespace(workdir=tmp_path))
    assert not list(tmp_path.iterdir())


def test_launch_refuses_changed_control_before_exec(tmp_path, monkeypatch):
    binding = debugger.identity(sys.executable, "a" * 40, "1.1")
    command = debugger.launch_command(tmp_path, binding, [sys.executable, "-B", debugger.__file__, "fixture"],
                                     credential_free=True)
    sha = command[-1]
    (tmp_path / "request.json").write_text("{}")
    monkeypatch.setattr(debugger.os, "execve", lambda *_a: pytest.fail("Changed request must not execute"))
    with pytest.raises(ValueError, match="request differs"):
        debugger.launch(SimpleNamespace(workdir=tmp_path, request_sha256=sha))


@pytest.mark.parametrize("outcome,expected", [(0, 0), (7, 7), ("SIGABRT", 134), ("unexpected", 125)])
def test_fixed_gdb_program_preserves_exit_or_signal_and_never_prints_run_chatter(tmp_path, monkeypatch, outcome, expected):
    """Drive the exact embedded program's GDB interface; no installed debugger is claimed."""
    import types
    events = {"stop": [], "exited": []}
    calls = []
    class Quit(BaseException):
        pass
    class SignalEvent:
        stop_signal = "SIGABRT"
    class Thread:
        def __init__(self, number):
            self.global_num = number
        def switch(self):
            pass
    threads = [Thread(1), Thread(2)]
    def execute(command, **_kwargs):
        calls.append(command)
        if command == "run":
            if outcome == "SIGABRT":
                events["stop"][0](SignalEvent())
            elif isinstance(outcome, int):
                events["exited"][0](SimpleNamespace(exit_code=outcome))
            return debugger.CANARY
        if command.startswith("backtrace"):
            return "#0 0x123 in abort ()\n"
        if command == "info sharedlibrary":
            return "0x100 0x200 /installed/library.so\n"
        if command.startswith("quit "):
            raise Quit(int(command.split()[1]))
        return ""
    fake: Any = types.ModuleType("gdb")
    fake.VERSION = "controlled-interface-only"
    fake.events = SimpleNamespace(**{name: SimpleNamespace(connect=callbacks.append) for name, callbacks in events.items()})
    fake.SignalEvent = SignalEvent
    fake.parameter = lambda name: {"debuginfod enabled": "off", "print frame-arguments": "none",
                                   "print entry-values": "no", "print frame-info": "location-and-address"}.get(name, False)
    fake.execute = execute
    fake.selected_thread = lambda: threads[0]
    fake.selected_inferior = lambda: SimpleNamespace(threads=lambda: threads)
    fake.newest_frame = lambda: SimpleNamespace(older=lambda: None)
    monkeypatch.setitem(sys.modules, "gdb", fake)
    monkeypatch.setenv("COMMENTS_DEBUGGER_ROOT", str(tmp_path))
    monkeypatch.setattr(debugger.resource, "getrlimit", lambda _: (0, 0))
    with pytest.raises(Quit) as exited:
        exec(debugger.GDB_PROGRAM.removeprefix("python\n").removesuffix("end\n"), {})
    assert exited.value.args == (expected,)
    record = debugger.read(tmp_path / "debugger.json")
    assert record["wrapper_exit_code"] == expected
    assert debugger.CANARY not in (tmp_path / "debugger.json").read_text()
    if outcome == "SIGABRT":
        assert record["signal"] == "SIGABRT" and record["native_backtrace"]
        assert calls[-2:] == ["kill", "quit 134"]
        assert all("-frame-arguments none" in call and "-no-filters" in call for call in calls if call.startswith("backtrace"))
    else:
        assert "kill" not in calls and not (tmp_path / "native-backtrace.log").exists()


def test_real_supervisor_timeout_closes_leader_fixture_and_descendant(tmp_path, monkeypatch):
    import subprocess
    env = debugger.safe_environment()
    env["COMMENTS_DEBUGGER_CANARY"] = debugger.CANARY
    with monkeypatch.context() as scoped:
        for key in list(os.environ):
            scoped.delenv(key)
        for key, value in env.items():
            scoped.setenv(key, value)
        fixture = [sys.executable, "-B", debugger.__file__, "fixture", "--workdir", str(tmp_path),
                   "--case", "timeout", "--canary", debugger.CANARY]
        # One launcher stands in for GDB only to exercise unchanged tree accounting.
        command = [sys.executable, "-B", "-c", "import subprocess,sys; subprocess.run(sys.argv[1:])", *fixture]
        with pytest.raises(ValueError, match="wall limit"):
            runner.supervise(SimpleNamespace(workdir=tmp_path), command=command,
                             limits={**debugger.LIMITS, "wall_seconds": 10})
    result = debugger.check_case(tmp_path, "timeout")
    assert result["case"] == "timeout"
    closure = debugger.read(tmp_path / "process-closure.json")
    assert len(closure["known"]) >= 3
    assert debugger.read(tmp_path / "execution.json")["remaining_processes"] == []
    assert subprocess.run([sys.executable, "-c", "pass"], capture_output=True, timeout=5).returncode == 0


def test_case_refuses_unexpected_success_and_canary(tmp_path):
    (tmp_path / "process-closure.json").write_text("{}")
    execution = {"remaining_processes": [], "status": "complete", "limits": debugger.LIMITS}
    (tmp_path / "execution.json").write_text(json.dumps(execution))
    (tmp_path / "execution.log").write_text(debugger.CANARY)
    with pytest.raises(ValueError, match="private-value"):
        debugger.check_case(tmp_path, "abort")
    (tmp_path / "execution.log").write_text("")
    (tmp_path / "execution.json").write_text(json.dumps({**execution, "limits": {**debugger.LIMITS, "wall_seconds": 10}}))
    with pytest.raises(ValueError, match="Timeout did not refuse"):
        debugger.check_case(tmp_path, "timeout")


def test_guard_wraps_only_publish_and_charges_qualification(tmp_path, monkeypatch):
    monkeypatch.setenv(guard.iceberg.CATALOG_LOCK_ENV, "comments-catalog-write")
    binding = {"reviewed": True}
    monkeypatch.setattr(guard, "qualified_debugger", lambda _: {"binding": binding, "elapsed_seconds": 23})
    seen = []
    def supervised(args, *, command, limits):
        seen.append((command, limits))
        if command[-1] == "admit":
            guard.owner.durable(tmp_path / "comments-admission.json", {"tree_rss_cap_bytes": 8 * 2**30,
                                                                     "output_cap_bytes": 99})
    def wrapped(folder, actual, command, *, credential_free):
        assert actual is binding and not credential_free
        assert command[-2:] == ["--phase", "publish"] and "--skip-upload" in command
        assert "--diagnostic-native-snapshot" in command
        return ["qualified-wrapper"]
    monkeypatch.setattr(guard.runner, "supervise", supervised)
    monkeypatch.setattr(debugger, "launch_command", wrapped)
    args = SimpleNamespace(workdir=tmp_path, expected_source_sha="a" * 40, skip_upload=True, force=False,
                           diagnostic_native_snapshot=SUBJECT, debugger_qualification=tmp_path / "q.json",
                           expected_gdb_sha256="b" * 64)
    guard.run(args)
    assert seen[0][0][-2:] == ["--phase", "admit"] and seen[1][0] == ["qualified-wrapper"]
    assert guard.ACTIVE_SECONDS - 24 < seen[1][1]["wall_seconds"] <= guard.ACTIVE_SECONDS - 23
    assert seen[1][1]["rss_bytes"] == 8 * 2**30 and seen[1][1]["output_bytes"] == 99
    assert seen[1][1]["stop_free_bytes"] == guard.FREE_FLOOR


def test_qualification_consumer_refuses_stale_identity_changed_case_or_undercharge(tmp_path, monkeypatch):
    # Actual file hashes, with controlled completed-case results; no claim of a real GDB pass.
    binding = debugger.identity(sys.executable, "a" * 40, "12.1")
    cases = [{"case": name, "control": name + "-unchanged"} for name in debugger.CASES]
    monkeypatch.setattr(debugger, "check_case", lambda folder, case: {"case": case, "control": case + "-unchanged"})
    receipt = {"format": "comments-debugger-qualification/1", "qualified": True, "binding": binding, "cases": cases}
    path = tmp_path / "qualification.json"
    path.write_text(json.dumps(receipt))
    (tmp_path / "execution.json").write_text(json.dumps({"status": "complete", "remaining_processes": [],
                                                        "elapsed_seconds": 10,
                                                        "limits": {**debugger.LIMITS, "wall_seconds": debugger.TOTAL_ACTIVE}}))
    (tmp_path / "qualification-time.json").write_text('{"elapsed_seconds": 11}')
    expected = binding["gdb"]["sha256"]
    assert debugger.verified_qualification(path, "a" * 40, expected, "12.1")["elapsed_seconds"] == 11
    for source, sha, invocation in [("b" * 40, expected, "12.1"), ("a" * 40, "0" * 64, "12.1"),
                                    ("a" * 40, expected, "12.2")]:
        with pytest.raises(ValueError, match="identity differs"):
            debugger.verified_qualification(path, source, sha, invocation)
    receipt["cases"][0]["control"] = "changed"
    path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError, match="case evidence changed"):
        debugger.verified_qualification(path, "a" * 40, expected, "12.1")
    receipt["cases"][0]["control"] = "exit0-unchanged"
    path.write_text(json.dumps(receipt))
    (tmp_path / "qualification-time.json").write_text('{"elapsed_seconds": 9}')
    with pytest.raises(ValueError, match="total charge differs"):
        debugger.verified_qualification(path, "a" * 40, expected, "12.1")


def test_workflow_qualification_job_has_no_secrets_and_native_requalifies():
    import yaml
    workflow = yaml.safe_load((debugger.ROOT / ".github/workflows/_comments-mirror.yml").read_text())
    job = workflow["jobs"]["qualify-debugger"]
    text = json.dumps(job)
    assert "secrets." not in text and "R2_" not in text and "publish_comments_mirror_guarded" not in text
    assert job["steps"][0]["with"]["persist-credentials"] is False
    assert job["if"] == "${{ inputs.debugger_mode == 'qualify' }}"
    q_steps = job["steps"]
    q_install = next(step for step in q_steps if step.get("name") == "Install GDB for diagnostic mode")
    q_qualify = next(step for step in q_steps if step.get("name") == "Qualify installed debugger only")
    assert "secrets." not in json.dumps(q_install) and "R2_" not in json.dumps(q_install)
    assert q_install["run"].strip() == "sudo apt-get update && sudo apt-get install -y --no-install-recommends gdb"
    assert q_steps.index(q_install) + 1 == q_steps.index(q_qualify)
    publish = workflow["jobs"]["publish"]
    steps = publish["steps"]
    p_install = next(step for step in steps if step.get("name") == "Install GDB for diagnostic mode")
    assert "secrets." not in json.dumps(p_install) and "R2_" not in json.dumps(p_install)
    assert p_install["if"] == "${{ inputs.debugger_mode == 'gdb' }}"
    assert p_install["run"].strip() == "sudo apt-get update && sudo apt-get install -y --no-install-recommends gdb"
    qualify = next(step for step in steps if step.get("name") == "Repeat reviewed debugger qualification without credentials")
    assert steps.index(p_install) + 1 == steps.index(qualify)
    assert "secrets." not in json.dumps(qualify) and "R2_" not in json.dumps(qualify)
    assert qualify["if"] == "${{ inputs.debugger_mode == 'gdb' }}"
    native = next(step for step in steps if step.get("name") == "Publish comments mirror from the catalog")
    assert steps.index(qualify) < steps.index(native)
    assert '--expected-gdb-sha256 "$EXPECTED_GDB_SHA256"' in qualify["run"]
    assert '--debugger-qualification "$PWD/output/comments-publish-debugger/qualification.json"' in native["run"]
    refresh = yaml.safe_load((debugger.ROOT / ".github/workflows/_regulations-refresh.yml").read_text())
    assert all("debugger_mode == 'off'" in job["if"] for name, job in refresh["jobs"].items() if name != "mirror")


def test_imported_supervisor_and_owner_must_match_hashed_paths(tmp_path, monkeypatch):
    fake_file = tmp_path / "other_module.py"
    fake_file.write_text("# shadow")
    for module_name, label in (
        ("scripts.read_comment_attachment_group", "supervisor"),
        ("spicy_regs.operational_runner", "owner"),
        ("scripts.publish_comments_mirror_guarded", "publisher"),
    ):
        monkeypatch.setattr(sys.modules[module_name], "__file__", str(fake_file))
        with pytest.raises(ValueError, match=f"Imported {label} differs from hashed path"):
            debugger.identity(sys.executable, "a" * 40, "1.1")
        monkeypatch.undo()
    # Direct verified_module check in qualified_debugger refuses mismatched publisher too.
    monkeypatch.setattr(guard, "__file__", str(fake_file))
    with pytest.raises(ValueError, match="Imported publisher differs from hashed path"):
        guard.qualified_debugger(SimpleNamespace(debugger_qualification=tmp_path / "q.json",
                                                 expected_gdb_sha256="a" * 64,
                                                 expected_source_sha="a" * 40,
                                                 skip_upload=True,
                                                 diagnostic_native_snapshot=SUBJECT))


def test_ld_library_path_allows_only_base_prefix_lib_and_refuses_others(tmp_path, monkeypatch):
    base_lib = tmp_path / "lib"
    base_lib.mkdir()
    other_lib = tmp_path / "other"
    other_lib.mkdir()
    monkeypatch.setattr(debugger.sys, "base_prefix", str(tmp_path))
    monkeypatch.setenv("LD_LIBRARY_PATH", str(base_lib))
    assert debugger.safe_environment()["LD_LIBRARY_PATH"] == str(base_lib)

    binding = debugger.identity(sys.executable, "a" * 40, "1.1")
    command = debugger.launch_command(
        tmp_path, binding, [sys.executable, "-B", str(debugger.PUBLISHER_PATH), "--phase", "publish"],
        credential_free=False,
    )
    sha = command[-1]
    captured = {}
    monkeypatch.setattr(debugger.os, "execve", lambda exe, argv, env: captured.update(exe=exe, argv=argv, env=env))
    debugger.launch(SimpleNamespace(workdir=tmp_path, request_sha256=sha))
    assert captured["env"]["LD_LIBRARY_PATH"] == str(base_lib)

    monkeypatch.setenv("LD_LIBRARY_PATH", str(other_lib))
    assert "LD_LIBRARY_PATH" not in debugger.safe_environment()
    with pytest.raises(ValueError, match="Unqualified debugger loader"):
        debugger.launch(SimpleNamespace(workdir=tmp_path, request_sha256=sha))

    monkeypatch.setenv("LD_LIBRARY_PATH", str(base_lib))
    monkeypatch.setenv("LD_PRELOAD", "/tmp/evil.so")
    with pytest.raises(ValueError, match="Unqualified debugger loader"):
        debugger.launch(SimpleNamespace(workdir=tmp_path, request_sha256=sha))
