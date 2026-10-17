"""Tiny constructed fault probes; no corpus inputs or unrelated worker signals."""

import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time

import pytest

from spicy_regs import operational_runner as runner
from tests.operational_helpers import ENV, FIRST_PARTY, SCRIPT, finished, inventory, launch, observed_files, wait_for

WORKER = """
from pathlib import Path
import json, sys, time, subprocess
import fixture_helper
phase, candidate, run, count, flag, mode = sys.argv[1:]
candidate, run, count, flag = Path(candidate), Path(run), Path(count), Path(flag)
with count.open('a') as stream: stream.write(phase + '\\n')
if phase == 'build':
    candidate.mkdir()
    (candidate / 'value').write_text('unchanged original value')
    if mode == 'sleep': time.sleep(20)
    if mode == 'big':
        (candidate / 'big').write_bytes(b'x' * 2 * 1024 * 1024)
        time.sleep(1)
    if mode == 'drift': Path(fixture_helper.__file__).write_text('# rewritten after it was imported\\n')
    if mode == 'steer': (run / 'candidate.json').write_text(json.dumps({'path': str(run / 'scratch')}))
    if mode == 'rewrite-owner':
        record = json.loads((run / 'owner.json').read_text())
        record['started_monotonic'] += 100000
        (run / 'owner.json').write_text(json.dumps(record))
    if mode == 'forge-terminal':
        (run / 'audit-terminal.json').write_text(json.dumps({'state': 'passed', 'known_closure': True, 'known': []}))
    if mode == 'descendant':
        subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(20)'])
        time.sleep(.15)
else:
    assert (candidate / 'value').read_text() == 'unchanged original value'
    if flag.exists(): raise RuntimeError('injected complete audit failure')
    if mode == 'escape-link': (run / 'output' / 'escape').symlink_to('/etc/hosts')
    if mode == 'audit-sleep': time.sleep(20)
"""


@pytest.fixture(scope="module")
def runtime_pins(tmp_path_factory):
    """Interpreter plus every module the runner imports; independent of the test directory."""
    work = tmp_path_factory.mktemp("observe")
    files = inventory(observed_files("", [], work), [work, *FIRST_PARTY])["files"]
    return [*files, runner.file_pin(Path(sys.executable).resolve())]


@pytest.fixture
def selected(tmp_path, runtime_pins):
    worker, helper, source = tmp_path / "fixture_worker.py", tmp_path / "fixture_helper.py", tmp_path / "input.txt"
    worker.write_text(WORKER)
    helper.write_text("# pinned helper imported by the worker\n")
    source.write_text("selected input")
    base_paths = json.loads(
        subprocess.check_output(
            [
                sys.executable,
                "-I",
                "-B",
                "-c",
                "import json,sys; from pathlib import Path; print(json.dumps([str(Path(p).resolve()) for p in sys.path]))",
            ],
            text=True,
        )
    )
    count, flag = tmp_path / "counts", tmp_path / "fail-audit"
    request = {
        "version": 1,
        "run_root": str(tmp_path / "attempt1"),
        "python": sys.executable,
        "inputs": [runner.file_pin(source)],
        "code": [runner.file_pin(SCRIPT), runner.file_pin(worker), runner.file_pin(helper)],
        "runtime": runtime_pins,
        "activation": {
            "prefix": str(Path(sys.prefix).resolve()),
            "paths": [str(tmp_path)],
            "base_paths": base_paths,
            "origins": {"fixture_worker": str(worker)},
        },
        "offline_single_process": False,
        "limits": {
            "wall_seconds": 10,
            "interval_seconds": 0.02,
            "rss_bytes": 512 * 1024**2,
            "output_bytes": 8 * 1024**2,
            "start_free_bytes": 1,
            "stop_free_bytes": 1,
        },
        "commands": {
            phase: {
                "module": "fixture_worker",
                "args": [phase, "{candidate}", "{run}", str(count), str(flag), "normal"],
            }
            for phase in ("build", "audit")
        },
    }
    return request, count, flag


def following(request, tmp_path, root, name="next"):
    return {
        **request,
        "run_root": str(tmp_path / name),
        "continue_from": runner.file_pin(root / "build-completion.json"),
    }


def test_fresh_then_explicit_continuation_has_zero_candidate_calls(selected, tmp_path):
    request, count, flag = selected
    flag.touch()
    root = Path(request["run_root"])
    proc = launch(tmp_path, request)
    proc.communicate(timeout=20)
    failed = runner.read_json(root / "result.json")
    assert (
        failed["state"] == "failed"
        and failed["phases_launched"] == ["build", "audit"]
        and failed["reused_build"] is None
    )
    assert count.read_text().splitlines() == ["build", "audit"]
    original = runner.census(root)
    flag.unlink()
    request = following(request, tmp_path, root, "attempt2")
    fresh = Path(request["run_root"])
    result = finished(launch(tmp_path, request), fresh)
    assert result["state"] == "built-and-audited-awaiting-review"
    assert result["phases_launched"] == ["audit"] and result["reused_build"] == request["continue_from"]
    assert count.read_text().splitlines() == ["build", "audit", "audit"]
    assert runner.census(root) == original
    assert not (fresh / "candidate").exists() and not (fresh / "build-launch.json").exists()
    listing = runner.read_json(fresh / "output-files.json")
    assert listing["candidate"] == request["continue_from"] and not any(
        row["path"].startswith("candidate/") for row in listing["files"]
    )


@pytest.mark.parametrize("changed", ["input", "input-bytes", "code", "runtime", "build-command", "output"])
def test_continuation_drift_refuses_before_new_calls(selected, tmp_path, changed):
    request, count, _ = selected
    root = Path(request["run_root"])
    assert finished(launch(tmp_path, request), root)["state"] == "built-and-audited-awaiting-review"
    proposal = following(request, tmp_path, root)
    if changed == "output":
        (root / "candidate/value").write_text("tampered original value")
    elif changed == "input-bytes":
        Path(request["inputs"][0]["path"]).write_text("rewritten selected input")
    elif changed == "build-command":
        build = proposal["commands"]["build"]
        proposal["commands"] = {**proposal["commands"], "build": {**build, "args": [*build["args"][:-1], "sleep"]}}
    else:
        key = {"input": "inputs", "code": "code", "runtime": "runtime"}[changed]
        new = tmp_path / (changed + "-changed")
        new.write_bytes(b"changed binding")
        proposal[key] = [*proposal[key], runner.file_pin(new)]
    before = runner.census(root)
    launch(tmp_path, proposal).communicate(timeout=20)
    fresh = Path(proposal["run_root"])
    assert runner.read_json(fresh / "result.json")["state"] == "failed"
    assert count.read_text().splitlines() == ["build", "audit"]
    assert runner.census(root) == before
    assert not (fresh / "build-launch.json").exists() and not (fresh / "audit-launch.json").exists()


def test_report_failure_after_audit_then_continue_with_new_limits_and_audit_args(selected, tmp_path):
    request, count, _ = selected
    request["commands"]["audit"]["args"][-1] = "escape-link"
    root = Path(request["run_root"])
    launch(tmp_path, request).communicate(timeout=20)
    result = runner.read_json(root / "result.json")
    assert result["state"] == "failed" and "escapes the root" in result["error"]
    assert runner.read_json(root / "audit-terminal.json")["state"] == "passed"
    assert (root / "build-completion.json").exists()
    proposal = following(request, tmp_path, root)
    proposal["limits"] = {**proposal["limits"], "wall_seconds": 11}
    audit = proposal["commands"]["audit"]
    proposal["commands"] = {**proposal["commands"], "audit": {**audit, "args": [*audit["args"][:-1], "normal"]}}
    fresh = Path(proposal["run_root"])
    assert finished(launch(tmp_path, proposal), fresh)["state"] == "built-and-audited-awaiting-review"
    assert count.read_text().splitlines() == ["build", "audit", "audit"]
    assert not (fresh / "build-launch.json").exists()


def test_killed_supervisor_before_release_never_launches_payload(selected, tmp_path):
    request, count, _ = selected
    root = Path(request["run_root"])
    owner = launch(tmp_path, request, fault="build-release.json")
    wait_for(root / "build-child.json")
    owner.kill()
    owner.communicate(timeout=5)
    wait_for(root / "build-terminal.json")
    assert not count.exists()
    assert runner.read_json(root / "build-terminal.json")["state"] == "failed"
    assert not (root / "build-payload-release.json").exists()
    # Recovery is a distinct fresh process and never resumes build/audit.
    result = subprocess.check_output(
        [sys.executable, "-B", "-m", "spicy_regs.operational_runner", "reconcile", str(root)], env=ENV, text=True
    )
    assert json.loads(result)["qualification"] == "not-established"
    assert not count.exists()


def test_killed_owner_after_closed_build_can_reuse_only_owner_completion(selected, tmp_path):
    request, count, _ = selected
    root = Path(request["run_root"])
    owner = launch(tmp_path, request, fault="audit-release.json")
    wait_for(root / "build-completion.json")
    wait_for(root / "audit-child.json")
    owner.kill()
    owner.communicate(timeout=5)
    wait_for(root / "audit-terminal.json")
    proposal = following(request, tmp_path, root)
    assert (
        finished(launch(tmp_path, proposal), Path(proposal["run_root"]))["state"] == "built-and-audited-awaiting-review"
    )
    assert count.read_text().splitlines() == ["build", "audit"]


def test_killed_guardian_mid_audit_leaves_no_running_payload_and_can_be_continued(selected, tmp_path):
    request, count, _ = selected
    request["commands"]["audit"]["args"][-1] = "audit-sleep"
    request["limits"]["wall_seconds"] = 60
    root = Path(request["run_root"])
    owner = launch(tmp_path, request)
    wait_for(root / "audit-payload-release.json")
    until = time.monotonic() + 15
    while count.read_text().splitlines() != ["build", "audit"] and time.monotonic() < until:
        time.sleep(0.02)
    recorded = runner.read_json(root / "audit-child.json")
    guardian, payload = recorded["identity"], recorded["known"][0]
    assert runner.matching(guardian) and runner.matching(payload)
    os.kill(guardian["pid"], signal.SIGKILL)  # A descendant of this test's own owner subprocess.
    owner.communicate(timeout=30)
    # The owner closed what its lost guardian had recorded before it exited.
    result = runner.read_json(root / "result.json")
    assert result["state"] == "failed" and not (root / "audit-terminal.json").exists()
    cleanup = result["orphan_cleanup"]["audit"]
    assert payload in cleanup["terminated"] and cleanup["remaining"] == []
    assert runner.matching(payload) is None
    closed = runner.status(root)
    assert closed["closed"] is True and closed["phases"]["audit"] == "recorded-processes-and-group-gone"
    # Reconcile on an already closed run is a no-op record; continuation needs no reconcile.
    out = subprocess.run(
        [sys.executable, "-B", "-m", "spicy_regs.operational_runner", "reconcile", str(root)],
        env=ENV,
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    assert runner.read_json(root / "reconciliation.json")["state"] == "closed-incomplete-run"
    proposal = following(request, tmp_path, root)
    audit = proposal["commands"]["audit"]
    proposal["commands"] = {**proposal["commands"], "audit": {**audit, "args": [*audit["args"][:-1], "normal"]}}
    assert (
        finished(launch(tmp_path, proposal), Path(proposal["run_root"]))["state"] == "built-and-audited-awaiting-review"
    )
    assert count.read_text().splitlines() == ["build", "audit", "audit"]


def test_continuation_is_refused_while_a_recorded_process_lives_until_reconcile(selected, tmp_path, sleepers):
    request, count, _ = selected
    root = Path(request["run_root"])
    assert finished(launch(tmp_path, request), root)["state"] == "built-and-audited-awaiting-review"
    straggler = runner.birth(sleepers(start_new_session=True).pid)
    (root / "audit-terminal.json").unlink()
    child = runner.read_json(root / "audit-child.json")
    runner.durable(root / "audit-child.json", {**child, "known": [*child["known"], straggler]})
    assert runner.status(root)["closed"] is False
    proposal = following(request, tmp_path, root)
    launch(tmp_path, proposal).communicate(timeout=20)
    refused = runner.read_json(Path(proposal["run_root"]) / "result.json")
    assert refused["state"] == "failed" and "not closed" in refused["error"]
    assert count.read_text().splitlines() == ["build", "audit"]
    out = subprocess.run(
        [sys.executable, "-B", "-m", "spicy_regs.operational_runner", "reconcile", str(root)],
        env=ENV,
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stdout + out.stderr
    assert runner.matching(straggler) is None
    proposal = following(request, tmp_path, root, "next2")
    assert (
        finished(launch(tmp_path, proposal), Path(proposal["run_root"]))["state"] == "built-and-audited-awaiting-review"
    )


@pytest.mark.parametrize(
    "mode,phase,message",
    [
        ("steer", "build", "Runner record changed: candidate.json"),
        ("rewrite-owner", "build", "Runner record changed: owner.json"),
        ("forge-terminal", "build", "Pre-existing phase record: audit-terminal"),
    ],
)
def test_a_payload_cannot_steer_the_next_phase_or_forge_its_records(selected, tmp_path, mode, phase, message):
    request, count, _ = selected
    request["commands"][phase]["args"][-1] = mode
    root = Path(request["run_root"])
    launch(tmp_path, request).communicate(timeout=20)
    result = runner.read_json(root / "result.json")
    assert result["state"] == "failed" and message in result["error"]
    assert count.read_text().splitlines() == ["build"]  # The audit never ran against a substituted candidate.
    assert not (root / "audit-launch.json").exists() and not (root / "output-files.json").exists()


def test_a_forged_terminal_does_not_close_a_phase_whose_recorded_payload_lives(tmp_path, sleepers):
    identity = runner.birth(sleepers(start_new_session=True).pid)
    root = guarded_root(tmp_path, {"pid": 2**22 - 1, "created": 0})  # The guardian is gone.
    runner.durable(root / "build-child.json", {"identity": {"pid": 2**22 - 1, "created": 0}, "known": [identity]})
    runner.durable(
        root / "build-terminal.json",
        {"state": "passed", "known_closure": True, "known": [], "identity": {"pid": 1, "created": 1}},
    )
    held = runner.status(root)
    assert held["phases"]["build"] is None and held["closed"] is False and held["live_known"]
    result = runner.reconcile(root)
    assert result["state"] == "closed-incomplete-run" and runner.matching(identity) is None


def test_double_signal_to_a_guardian_still_leaves_its_terminal_record(selected, tmp_path):
    request, count, _ = selected
    root = Path(request["run_root"])
    owner = launch(tmp_path, request, fault="build-release.json")
    wait_for(root / "build-child.json")
    guardian = runner.read_json(root / "build-child.json")["identity"]
    assert runner.matching(guardian)
    os.kill(guardian["pid"], signal.SIGTERM)  # A descendant of this test's own owner subprocess.
    os.kill(guardian["pid"], signal.SIGTERM)
    wait_for(root / "build-terminal.json")
    terminal = runner.read_json(root / "build-terminal.json")
    assert terminal["state"] == "failed" and terminal["known_closure"] and not count.exists()
    owner.kill()
    owner.communicate(timeout=5)


def test_output_limit_stops_build_without_completion(selected, tmp_path):
    request, count, _ = selected
    request["commands"]["build"]["args"][-1] = "big"
    request["limits"]["output_bytes"] = 1024**2
    root = Path(request["run_root"])
    launch(tmp_path, request).communicate(timeout=20)
    terminal = runner.read_json(root / "build-terminal.json")
    result = runner.read_json(root / "result.json")
    assert terminal["state"] == "failed" and terminal["known_closure"]
    assert "Owned output size limit" in terminal.get("error", "") + result["error"]
    assert not (root / "build-completion.json").exists()
    assert count.read_text().splitlines() == ["build"]


def test_runtime_drift_during_phase_fails_closed(selected, tmp_path):
    request, count, _ = selected
    request["commands"]["build"]["args"][-1] = "drift"
    root = Path(request["run_root"])
    launch(tmp_path, request).communicate(timeout=20)
    terminal = runner.read_json(root / "build-terminal.json")
    assert terminal["state"] == "failed" and terminal["known_closure"]
    assert not (root / "build-completion.json").exists()
    assert runner.read_json(root / "result.json")["state"] == "failed"


def test_descendant_failure_is_closed(selected, tmp_path):
    request, _, _ = selected
    request["commands"]["build"]["args"][-1] = "descendant"
    root = Path(request["run_root"])
    proc = launch(tmp_path, request)
    proc.communicate(timeout=20)
    terminal = runner.read_json(root / "build-terminal.json")
    assert terminal["state"] == "failed"
    assert terminal["known_closure"] and not any(runner.matching(row) for row in terminal["known"])
    assert not (root / "build-completion.json").exists()


def test_payload_does_no_work_until_its_parent_released_it(selected, tmp_path):
    """A payload whose parent is not the recording guardian exits before activation or user code."""
    request, count, _ = selected
    root = Path(request["run_root"])
    root.mkdir()
    identity = runner.bind_root(root)
    (root / "scratch").mkdir()
    runner.durable(root / "request.json", request)
    runner.durable(root / "candidate.json", {"path": str(root / "candidate")})
    argv = [
        sys.executable,
        "-I",
        "-B",
        "-X",
        "pycache_prefix=" + str(root / "scratch/pycache"),
        str(SCRIPT),
        "_payload",
        str(root),
        "build",
        ":".join(map(str, identity)),
        "1",
    ]
    result = subprocess.run(argv, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0 and "Parent disappeared before release" in result.stderr
    assert not count.exists() and not (root / "candidate").exists()


def guarded_root(tmp_path, guardian, *, name="dead"):
    """A run whose guardian recorded `guardian` and whose owner is a dead identity."""
    root = tmp_path / name
    root.mkdir()
    runner.durable(
        root / "owner.json", {"identity": {"pid": 2**22 - 1, "created": 0}, "root_identity": runner.bind_root(root)}
    )
    runner.durable(root / "build-launch.json", {"phase": "build"})
    runner.durable(root / "build-release.json", guardian)
    runner.durable(root / "build-child.json", {"identity": guardian, "known": []})
    return root


@pytest.fixture
def sleepers():
    started = []

    def start(**options):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"], **options)
        started.append(process)
        return process

    yield start
    for process in started:
        process.terminate()
        process.wait(timeout=5)


def test_reused_pid_guardian_leading_a_group_stays_held_and_is_not_signalled(tmp_path, sleepers):
    leader = sleepers(start_new_session=True)
    stale = {**runner.birth(leader.pid), "created": 0}
    assert runner.terminate([stale]) == []
    root = guarded_root(tmp_path, stale)
    (root / "reconcile.lock").write_text("stale file is not a held kernel lock")
    before = runner.census(root)
    observation = runner.status(root)
    assert observation["live_known"] == [] and observation["phases"]["build"] is None and observation["closed"] is False
    assert runner.census(root) == before
    assert runner.reconcile(root)["state"] == "held"
    assert leader.poll() is None


def test_exact_live_guardian_is_terminated_by_reconcile(tmp_path, sleepers):
    leader = sleepers(start_new_session=True)
    identity = runner.birth(leader.pid)
    root = guarded_root(tmp_path, identity)
    assert runner.status(root)["live_known"] == [identity]
    result = runner.reconcile(root)
    assert (
        result["state"] == "closed-incomplete-run" and result["phases"]["build"] == "recorded-processes-and-group-gone"
    )
    assert runner.matching(identity) is None
    assert result["qualification"] == "not-established" and result["files"]


def test_reused_pid_of_a_non_leader_is_closed_and_not_signalled(tmp_path, sleepers):
    other = sleepers()
    root = guarded_root(tmp_path, {**runner.birth(other.pid), "created": 0})
    result = runner.reconcile(root)
    assert result["state"] == "closed-incomplete-run"
    assert other.poll() is None


def test_missing_launch_is_not_launched_and_missing_release_means_no_payload(tmp_path):
    root = tmp_path / "early"
    root.mkdir()
    runner.durable(
        root / "owner.json", {"identity": {"pid": 2**22 - 1, "created": 0}, "root_identity": runner.bind_root(root)}
    )
    assert runner.status(root)["phases"] == {"build": "not-launched", "audit": "not-launched"}
    runner.durable(root / "build-launch.json", {"phase": "build"})
    assert runner.status(root)["phases"]["build"] == "guardian-never-released"


def test_reconcile_refuses_a_concurrent_reconcile_and_records_an_untakeable_census(tmp_path):
    root = guarded_root(tmp_path, {"pid": 2**22 - 1, "created": 0})
    with (root / "reconcile.lock").open("a") as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with pytest.raises(ValueError, match="Another reconcile"):
            runner.reconcile(root)
    (root / "data").write_text("data")
    os.link(root / "data", root / "second-name")  # A closed census refuses hardlinks.
    result = runner.reconcile(root)
    assert result["state"] == "closed-incomplete-run" and "hardlink" in result["files_error"] and "files" not in result
    assert runner.read_json(root / "reconciliation.json")["files_error"] == result["files_error"]


def test_missing_operational_runner_extra_gives_a_clear_message():
    code = "import sys; sys.modules['psutil'] = None; import spicy_regs.operational_runner"
    result = subprocess.run([sys.executable, "-B", "-c", code], env=ENV, capture_output=True, text=True)
    assert result.returncode != 0 and "spicy-regs[operational-runner]" in result.stderr


def test_await_release_refuses_stale_identity_and_dead_parent(tmp_path):
    me = runner.birth()
    path = tmp_path / "release.json"
    with pytest.raises(ValueError, match="Parent disappeared"):
        runner.await_release(path, me, lambda: False, 0.01)
    runner.durable(path, {"pid": me["pid"], "created": 0})
    with pytest.raises(ValueError, match="Stale release"):
        runner.await_release(path, me, lambda: True, 0.01)
    runner.durable(path, me)
    runner.await_release(path, me, lambda: True, 0.01)
    with pytest.raises(ValueError, match="Stale release"):  # Released, but the parent died before it was read.
        runner.await_release(path, me, lambda: False, 0.01)


def test_request_grant_and_existing_root_refuse_without_calls(selected, tmp_path):
    from tests.operational_helpers import request_files

    request, count, _ = selected
    path, grant = request_files(tmp_path, request)
    value = runner.read_json(grant)
    value["expires_at"] = "2000-01-01T00:00:00+00:00"
    grant.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="Expired"):
        runner.execute(path, grant)
    assert not Path(request["run_root"]).exists() and not count.exists()
    _, grant = request_files(tmp_path, request)
    Path(request["run_root"]).mkdir()
    with pytest.raises(ValueError, match="Fresh run"):
        runner.execute(path, grant)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda request: request.update(policy={"different": True}),
        lambda request: request.update(offline_single_process="yes"),
        lambda request: request["activation"].update(extra=1),
        lambda request: request["inputs"][0].update(extra=1),
        lambda request: request.pop("limits"),
        lambda request: request.update(run_root=request["run_root"] + "/../again"),
    ],
)
def test_request_schema_refuses_unknown_fields_before_allocation(selected, tmp_path, mutation):
    from tests.operational_helpers import request_files

    request, count, _ = selected
    mutation(request)
    path, grant = request_files(tmp_path, request)
    with pytest.raises(ValueError):
        runner.execute(path, grant)
    assert not Path(request["run_root"]).exists() and not count.exists()


def test_sampling_refuses_an_unreadable_directory_instead_of_hiding_its_bytes(tmp_path):
    root = tmp_path / "run"
    (root / "candidate" / "hidden").mkdir(parents=True)
    (root / "candidate" / "hidden" / "big").write_bytes(b"x" * 4096)
    (root / "candidate" / "hidden").chmod(0)
    try:
        with pytest.raises(PermissionError):
            runner.sizes(root)
    finally:
        (root / "candidate" / "hidden").chmod(0o700)


def test_an_unreadable_process_group_member_holds_the_run(monkeypatch):
    def getpgid(pid):
        raise PermissionError(pid)

    monkeypatch.setattr(runner.os, "getpgid", getpgid)
    assert runner.group_live(2**22 - 1) is True


def test_cli_refusals_are_one_line_with_exit_code_two(selected, tmp_path):
    from tests.operational_helpers import grant

    request, count, _ = selected
    path = tmp_path / "request.json"
    path.write_text(json.dumps(request))
    expired = tmp_path / "expired.grant.json"
    expired.write_text(
        json.dumps(
            {
                "authorized": True,
                "request_sha256": runner.digest(path.read_bytes()),
                "expires_at": "2000-01-01T00:00:00+00:00",
            }
        )
    )
    for arguments in (["run", "--request", path, "--grant", expired], ["reconcile", tmp_path / "no-such-run"]):
        out = subprocess.run(
            [sys.executable, "-B", "-m", "spicy_regs.operational_runner", *map(str, arguments)],
            env=ENV,
            capture_output=True,
            text=True,
        )
        assert out.returncode == 2 and out.stderr.startswith("spicy-regs-run: ") and "Traceback" not in out.stderr
        assert len(out.stderr.strip().splitlines()) == 1
    assert (
        "Expired"
        in subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "spicy_regs.operational_runner",
                "run",
                "--request",
                str(path),
                "--grant",
                str(expired),
            ],
            env=ENV,
            capture_output=True,
            text=True,
        ).stderr
    )
    assert not Path(request["run_root"]).exists() and not count.exists() and grant


def test_budget_wall_limit_and_unbound_root(tmp_path):
    root = tmp_path / "run"
    root.mkdir()
    request = {
        "limits": {
            "wall_seconds": 0.1,
            "interval_seconds": 0.01,
            "rss_bytes": 10**12,
            "output_bytes": 1024**2,
            "stop_free_bytes": 1,
        }
    }
    with pytest.raises(ValueError, match="wall"):
        runner.Budget(root, request, time.monotonic() - 1).tick()


def test_sampling_tolerates_vanishing_entries_and_census_records_inroot_links(tmp_path, monkeypatch):
    root = tmp_path / "run"
    (root / "sub").mkdir(parents=True)
    (root / "sub/b").write_bytes(b"bytes")
    (root / "a").symlink_to("sub/b")
    real_walk = os.walk

    def vanishing(top, **options):
        for directory, dirs, names in real_walk(top, **options):
            yield directory, [*dirs, "gone-directory"], [*names, "gone-file"]

    with monkeypatch.context() as patch:
        patch.setattr(runner.os, "walk", vanishing)
        sampled = runner.sizes(root)
    assert sampled["logical"] >= len(b"bytes") + len("sub/b")
    files = runner.census(root)
    assert files[0] == {"path": "a", "link": "sub/b", "target": "sub/b"}
    assert [row["path"] for row in files] == ["a", "sub/b"] and files[1]["bytes"] == 5
    assert runner.census(root, skip=("sub",)) == [files[0]]


@pytest.mark.parametrize("case", ["outside", "dangling", "directory", "hardlink", "special"])
def test_census_refuses_unsafe_links_and_special_files(tmp_path, case):
    root = tmp_path / "run"
    root.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("outside")
    (root / "data").write_text("data")
    (root / "directory").mkdir()
    match = {
        "outside": "escapes",
        "dangling": "Dangling",
        "directory": "directory symlink",
        "hardlink": "hardlink",
        "special": "Regular file",
    }[case]
    if case == "outside":
        (root / "link").symlink_to(outside)
    elif case == "dangling":
        (root / "link").symlink_to(root / "missing")
    elif case == "directory":
        (root / "link").symlink_to(root / "directory")
    elif case == "hardlink":
        os.link(root / "data", root / "link")
    else:
        os.mkfifo(root / "link")
    with pytest.raises(ValueError, match=match):
        runner.census(root)


GUARD_PROBE = """
import json, os, posix, shutil, signal, subprocess, sys
from pathlib import Path
from spicy_regs import operational_runner as r
root, candidate, outside, phase = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3]), sys.argv[4]
os.chdir(root)  # The payload's working directory is its run root.
candidate_fd = os.open(candidate, os.O_RDONLY)
outside_fd = os.open(outside.parent, os.O_RDONLY)
(root / 'scratch' / 'made' / 'inner').mkdir(parents=True)
(root / 'scratch' / 'made' / 'inner' / 'file').write_text('temporary')
sleeper = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'], stdin=subprocess.DEVNULL,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
r.offline_guard(root, {'inputs': [], 'code': [], 'runtime': []}, readable=[candidate], phase=phase)
results = {'sleeper': sleeper.pid}
def attempt(name, action):
    try:
        action()
        results[name] = 'allowed'
    except ValueError as error:
        results[name] = str(error)
attempt('read candidate', lambda: (candidate / 'value').read_text())
attempt('write candidate', lambda: (candidate / 'value').write_text('changed'))
attempt('read outside', lambda: outside.read_text())
attempt('write output', lambda: (root / 'output' / 'ok').write_text('ok'))
attempt('rmtree scratch', lambda: shutil.rmtree(root / 'scratch' / 'made'))
attempt('open candidate by descriptor', lambda: os.close(os.open('value', os.O_RDONLY, dir_fd=candidate_fd)))
attempt('open outside by descriptor', lambda: os.close(os.open(outside.name, os.O_RDONLY, dir_fd=outside_fd)))
attempt('create outside by descriptor', lambda: os.close(os.open('made-outside', os.O_WRONLY | os.O_CREAT, dir_fd=outside_fd)))
attempt('create outside by posix.open', lambda: os.close(posix.open('made-posix', os.O_WRONLY | os.O_CREAT, dir_fd=outside_fd)))
attempt('unlink candidate by descriptor', lambda: os.unlink('value', dir_fd=candidate_fd))
attempt('write own candidate dir', lambda: (root / 'candidate' / 'file').write_text('x'))
attempt('write staging dir', lambda: ((root / '.stage').mkdir(), (root / '.stage' / 'file').write_text('x')))
attempt('write own imports', lambda: (root / (phase + '-imports.json')).write_text('{}'))
attempt('write other imports', lambda: (root / (('audit' if phase == 'build' else 'build') + '-imports.json')).write_text('{}'))
for name in ('owner.json', 'candidate.json', 'request.json', 'audit-terminal.json', 'build-child.json', 'result.json'):
    attempt('write ' + name, lambda name=name: (root / name).write_text('forged'))
attempt('remove owner.json', lambda: os.remove(root / 'owner.json'))
attempt('rename onto candidate.json', lambda: os.replace(root / 'scratch' / 'moved', root / 'candidate.json'))
attempt('create root file', lambda: (root / 'stray').write_text('x'))
attempt('signal self', lambda: os.kill(os.getpid(), 0))
attempt('signal another process', lambda: os.kill(sleeper.pid, signal.SIGTERM))
attempt('signal everything', lambda: os.kill(-1, 0))
attempt('signal a process group', lambda: os.killpg(os.getpgid(sleeper.pid), signal.SIGTERM))
print(json.dumps(results))
"""


@pytest.mark.parametrize("phase", ["build", "audit"])
def test_offline_guard_scopes_writes_and_reads_a_reused_candidate_by_descriptor(tmp_path, phase):
    root, candidate, outside = tmp_path / "run", tmp_path / "old/candidate", tmp_path / "outside.txt"
    for directory in ("scratch", "output", "candidate"):
        (root / directory).mkdir(parents=True)
    (root / "scratch" / "moved").write_text("payload file")
    for name in ("owner.json", "candidate.json", "request.json"):
        (root / name).write_text("runner record")
    candidate.mkdir(parents=True)
    (candidate / "value").write_text("verified candidate")
    outside.write_text("unpinned outside file")
    result = subprocess.run(
        [sys.executable, "-B", "-c", GUARD_PROBE, str(root), str(candidate), str(outside), phase],
        env=ENV,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    outcome = json.loads(result.stdout.splitlines()[-1])
    sleeper = outcome.pop("sleeper")
    try:
        os.kill(sleeper, 0)  # Still alive: the guarded payload's signals were refused.
    finally:
        os.kill(sleeper, signal.SIGTERM)  # The probe's own child, started for this test.
    allowed = {name for name, value in outcome.items() if value == "allowed"}
    assert allowed == {
        "read candidate",
        "write output",
        "rmtree scratch",
        "open candidate by descriptor",
        "write staging dir",
        "write own imports",
        "signal self",
        *(("write own candidate dir",) if phase == "build" else ()),
    }
    assert outcome["signal another process"] == outcome["signal everything"] == "Offline worker may signal only itself"
    assert outcome["signal a process group"] == "Offline worker may not signal a process group"
    assert outcome["write candidate"] == outcome["unlink candidate by descriptor"] == "Python write outside owned root"
    for name in ("create outside by descriptor", "create outside by posix.open"):
        assert outcome[name] == "Python write outside owned root"
    assert outcome["read outside"].startswith("Undeclared Python read")
    assert outcome["open outside by descriptor"].startswith("Undeclared Python read")
    for name in (
        "write other imports",
        "write owner.json",
        "write candidate.json",
        "write request.json",
        "write audit-terminal.json",
        "write build-child.json",
        "write result.json",
        "remove owner.json",
        "rename onto candidate.json",
        "create root file",
        *(("write own candidate dir",) if phase == "audit" else ()),
    ):
        assert outcome[name].startswith("Python write to a runner-owned path"), (name, outcome[name])
    assert (candidate / "value").read_text() == "verified candidate" and not (root / "scratch/made").exists()
    assert not (outside.parent / "made-outside").exists() and not (outside.parent / "made-posix").exists()
    assert (root / "owner.json").read_text() == "runner record" and not (root / "stray").exists()


KERNEL_PROBE = """
import json, sys
from pathlib import Path
from spicy_regs import operational_runner as r
root, pseudo, outside = map(Path, sys.argv[1:4])
r.KERNEL_ROOTS = (pseudo,)
r.offline_guard(root, {'inputs': [], 'code': [], 'runtime': []}, phase='build')
results = {}
def attempt(name, action):
    try:
        action()
        results[name] = 'allowed'
    except ValueError as error:
        results[name] = str(error)
attempt('read kernel state', lambda: (pseudo / 'stat').read_text())
attempt('write kernel state', lambda: (pseudo / 'stat').write_text('forged'))
attempt('read outside through a kernel link', lambda: (pseudo / 'escape').read_text())
attempt('read outside', lambda: outside.read_text())
print(json.dumps(results))
"""


@pytest.fixture
def pseudo(tmp_path, monkeypatch):
    """A directory registered as kernel state, so the /proc behavior is the same on every platform."""
    directory = tmp_path / "pseudo"
    directory.mkdir()
    (directory / "stat").write_text("ticks 1\n")
    monkeypatch.setattr(runner, "KERNEL_ROOTS", (directory,))
    return directory


def test_offline_guard_reads_kernel_state_but_never_writes_it_or_follows_it_out(tmp_path, pseudo):
    root, outside = tmp_path / "run", tmp_path / "outside.txt"
    root.mkdir()
    outside.write_text("unpinned outside file")
    (pseudo / "escape").symlink_to(outside)
    result = subprocess.run(
        [sys.executable, "-B", "-c", KERNEL_PROBE, str(root), str(pseudo), str(outside)],
        env=ENV,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    outcome = json.loads(result.stdout.splitlines()[-1])
    assert outcome["read kernel state"] == "allowed"
    assert outcome["write kernel state"] == "Python write outside owned root"
    assert outcome["read outside through a kernel link"].startswith("Undeclared Python read")
    assert outcome["read outside"].startswith("Undeclared Python read")
    assert (pseudo / "stat").read_text() == "ticks 1\n"


def test_a_kernel_interface_is_never_a_pin_so_a_changing_read_cannot_fail_admission(selected, tmp_path, pseudo):
    from tests.operational_helpers import request_files

    request, count, _ = selected
    stat, real = pseudo / "stat", Path(request["inputs"][0]["path"])
    # An observed inventory keeps the real file and drops the kernel one.
    assert [pin["path"] for pin in inventory([str(stat), str(real)], [])["files"]] == [str(real)]
    for kind in ("inputs", "code", "runtime"):
        path, grant = request_files(tmp_path, {**request, kind: [*request[kind], runner.file_pin(stat)]})
        with pytest.raises(ValueError, match=r"Kernel interface cannot be pinned \(" + kind):
            runner.execute(path, grant)
    assert not Path(request["run_root"]).exists() and not count.exists()


def test_kernel_interfaces_are_the_pseudo_filesystems_and_device_nodes_not_dev_data(tmp_path):
    assert runner.kernel_interface("/proc/stat") and runner.kernel_interface("/sys/devices/system/cpu/online")
    assert runner.kernel_interface("/dev/null")
    assert not runner.kernel_interface("/proc/../" + str(tmp_path / "data").lstrip("/"))
    assert not runner.kernel_interface(tmp_path / "data") and not runner.kernel_interface("/etc/hosts")
    if os.access("/dev/shm", os.W_OK):  # Linux tmpfs: regular bytes there are ordinary data and keep their pin.
        with tempfile.NamedTemporaryFile(dir="/dev/shm") as stream:
            assert not runner.kernel_interface(stream.name)


@pytest.mark.parametrize("route", ["tree", "group"])
def test_reuse_during_discovery_never_attributes_replacement(monkeypatch, route):
    """PID is reused between membership observation and attribution."""
    current = {"created": 10}

    class Process:
        def __init__(self, pid):
            self.pid = pid

        def create_time(self):
            return 1 if self.pid == 101 else 10

        def children(self, recursive):
            return [Process(202)] if route == "tree" else []

        def parents(self):
            current["created"] = 20
            return [Process(101)]

    def matching(identity):
        if identity["pid"] == 101 or identity["created"] == current["created"]:
            return Process(identity["pid"])
        return None

    def getpgid(pid):
        current["created"] = 20
        return 101

    monkeypatch.setattr(runner, "matching", matching)
    monkeypatch.setattr(runner.psutil, "process_iter", lambda _: [Process(202)] if route == "group" else [])
    monkeypatch.setattr(runner.os, "getpgid", getpgid)
    known = {}
    assert runner.members({"pid": 101, "created": 1}, known) == []
    assert known == {}


def test_cleanup_rediscovers_child_forked_during_term(tmp_path, monkeypatch):
    parent, child = {"pid": 101, "created": 1}, {"pid": 202, "created": 2}
    observations = iter([[parent], [child], []])
    signalled = []

    def members(leader, known):
        rows = next(observations)
        known.update({row["pid"]: row for row in rows})
        return rows

    monkeypatch.setattr(runner, "members", members)
    monkeypatch.setattr(runner, "terminate", lambda rows: signalled.append(rows))
    assert runner.close_members({"pid": 303, "created": 3}, {}, tmp_path / "child.json") == []
    assert signalled == [[parent], [child]]


def test_replaced_root_never_overwrites_replacement_records(tmp_path):
    root, old = tmp_path / "owned", tmp_path / "retained-old"
    root.mkdir()
    runner.bind_root(root)
    runner.durable(root / "result.json", {"owned": True})
    root.rename(old)
    root.mkdir()
    sentinel = b"Unrelated result must survive"
    (root / "result.json").write_bytes(sentinel)
    with pytest.raises(runner.OwnedRootChanged, match="replaced"):
        runner.durable(root / "result.json", {"bad": "overwrite"})
    assert (root / "result.json").read_bytes() == sentinel
    assert runner.read_json(old / "result.json") == {"owned": True}


def test_hidden_staging_directories_are_charged_as_data_and_record_temporaries_as_evidence(tmp_path):
    root = tmp_path / "owned"
    (root / ".uscode-editions-stage").mkdir(parents=True)
    (root / ".uscode-editions-stage" / "rows").write_bytes(b"x" * 100_000)
    (root / ".runner-record-result.json.abc").write_bytes(b"y" * 50_000)
    sampled = runner.sizes(root)
    assert sampled["data"] >= 100_000 and sampled["evidence"] >= 50_000
    assert sampled["evidence"] < 100_000 and sampled["scratch"] == 0


def test_accounting_charges_directory_allocations(tmp_path):
    root = tmp_path / "owned"
    root.mkdir()
    for name in ("scratch", "candidate", "output", "evidence"):
        (root / name).mkdir()
        for index in range(30):
            (root / name / str(index)).mkdir()
    expected = sum(path.stat().st_blocks * 512 for path in [root, *root.rglob("*")])
    actual = runner.sizes(root)
    assert actual["allocated"] == expected
    assert actual["scratch"] + actual["data"] + actual["evidence"] == expected
