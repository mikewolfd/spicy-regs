#!/usr/bin/env python3
"""Qualify an already installed GDB, then wrap only the guarded no-upload publisher."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import resource
import shutil
import subprocess
import sys
import time
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if __package__ is None:
    sys.path.insert(0, str(ROOT))

HELPER_PATH = Path(__file__).resolve()
SUPERVISOR_PATH = (ROOT / "scripts/read_comment_attachment_group.py").resolve()
OWNER_PATH = (ROOT / "src/spicy_regs/operational_runner.py").resolve()
PUBLISHER_PATH = (ROOT / "scripts/publish_comments_mirror_guarded.py").resolve()

# This module deliberately imports no catalog, storage, dotenv or publisher code.
LIMITS = {"wall_seconds": 60, "interval_seconds": .1, "rss_bytes": 2**30,
          "output_bytes": 32 * 2**20, "start_free_bytes": 4 * 2**30, "stop_free_bytes": 4 * 2**30}
TOTAL_ACTIVE = 260  # Existing supervisor has up to 30 seconds cleanup plus a 5-second wait; total stays below 300.
TEXT_LIMIT = 2**20
CASES = ("exit0", "exit7", "abort", "timeout")
CANARY = "synthetic-debugger-private-value-3ae670de"
SETTINGS = (
    "set auto-load off", "set debuginfod enabled off", "set startup-with-shell off",
    "set disable-randomization off", "set index-cache enabled off", "set pagination off",
    "set confirm off", "set print frame-arguments none", "set print entry-values no",
    "set print frame-info location-and-address", "set print thread-events off",
    "set print inferior-events off",
)

# A fixed GDB program, not a command/attach interface. Never evaluate inferior values.
# Capture GDB's run chatter (which can include argv) privately; inferior stdout/stderr
# must still reach the supervisor log, and the hosted qualification proves that fact.
GDB_PROGRAM = r'''python
import gdb, json, os, resource, sys
from pathlib import Path
root = Path(os.environ["COMMENTS_DEBUGGER_ROOT"])
state = {"format": "comments-native-debugger/1", "gdb_version": gdb.VERSION,
         "gdb_python": sys.version, "signal": None, "exit_code": None,
         "core_limit": list(resource.getrlimit(resource.RLIMIT_CORE)), "settings": {},
         "native_backtrace": False, "trace_truncated": False, "frame_limit_per_thread": 32}
def save():
    data = (json.dumps(state, sort_keys=True, indent=2) + "\n").encode()
    if len(data) > 65536:
        raise ValueError("Debugger record bound")
    with (root / "debugger.json").open("wb") as stream:
        stream.write(data); stream.flush(); os.fsync(stream.fileno())
def stopped(event):
    if isinstance(event, gdb.SignalEvent):
        state["signal"] = event.stop_signal
def exited(event):
    state["exit_code"] = getattr(event, "exit_code", None)
gdb.events.stop.connect(stopped)
gdb.events.exited.connect(exited)
code = 125
try:
    for name in ("auto-load python-scripts", "auto-load gdb-scripts", "auto-load local-gdbinit",
                 "debuginfod enabled", "startup-with-shell", "disable-randomization", "index-cache enabled",
                 "print frame-arguments", "print entry-values", "print frame-info"):
        state["settings"][name] = gdb.parameter(name)
    save()
    gdb.execute("run", from_tty=False, to_string=True)
    if state["signal"] is not None:
        current = gdb.selected_thread()
        threads = list(gdb.selected_inferior().threads())
        threads.sort(key=lambda thread: (thread != current, thread.global_num))
        state["threads_available"] = len(threads)
        state["threads_retained"] = min(len(threads), 32)
        state["trace_truncated"] = len(threads) > 32
        pieces = []
        for thread in threads[:32]:
            thread.switch()
            frame = gdb.newest_frame()
            for _ in range(32):
                if frame is None:
                    break
                frame = frame.older()
            state["trace_truncated"] |= frame is not None
            frames = gdb.execute("backtrace -no-filters -frame-arguments none -entry-values no -frame-info location-and-address 32",
                                 from_tty=False, to_string=True)
            pieces.append("Thread %d\n%s" % (thread.global_num, frames))
        pieces.append(gdb.execute("info sharedlibrary", from_tty=False, to_string=True))
        trace = "\n".join(pieces).encode()
        state["trace_truncated"] |= len(trace) > 1048576
        with (root / "native-backtrace.log").open("wb") as stream:
            stream.write(trace[:1048576]); stream.flush(); os.fsync(stream.fileno())
        state["native_backtrace"] = bool(current and pieces and "#0 " in pieces[0])
        code = {"SIGABRT": 134, "SIGSEGV": 139, "SIGBUS": 135, "SIGILL": 132, "SIGFPE": 136}.get(state["signal"], 125)
        state["wrapper_exit_code"] = code
        save()
        # Do not resume after a fatal stop. The outer supervisor also closes every member.
        gdb.execute("kill", from_tty=False, to_string=True)
    elif isinstance(state["exit_code"], int):
        code = state["exit_code"] if 0 <= state["exit_code"] <= 255 else 125
    state["wrapper_exit_code"] = code
    save()
except BaseException as error:
    state["error_type"] = type(error).__name__
    state["wrapper_exit_code"] = 125
    save()
    code = 125
gdb.execute("quit %d" % code)
end
'''


def require(value, message):
    if not value:
        raise ValueError(message)


def read(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(2**20 + 1)
    require(len(raw) <= 2**20, "Debugger control exceeds bound")
    return json.loads(raw)


def pin(path):
    path = Path(path).resolve(strict=True)
    require(path.is_file() and path.stat().st_size <= 128 * 2**20, "Debugger identity file refused")
    with path.open("rb") as stream:
        sha = hashlib.file_digest(stream, "sha256").hexdigest()
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": sha}


def verified_module(module, expected_path, label):
    actual = getattr(module, "__file__", None)
    require(
        actual is not None and Path(actual).resolve(strict=True) == expected_path.resolve(strict=True),
        f"Imported {label} differs from hashed path",
    )
    return module


def confirm_imported_modules():
    for name, expected_path, label in (
        ("scripts.comments_native_debugger", HELPER_PATH, "helper"),
        ("scripts.read_comment_attachment_group", SUPERVISOR_PATH, "supervisor"),
        ("spicy_regs.operational_runner", OWNER_PATH, "owner"),
        ("scripts.publish_comments_mirror_guarded", PUBLISHER_PATH, "publisher"),
    ):
        module = sys.modules.get(name)
        if module is not None:
            verified_module(module, expected_path, label)


def identity(gdb_path, source, invocation):
    confirm_imported_modules()
    return {"source_commit": source, "invocation": invocation, "gdb": pin(gdb_path),
            "python": pin(sys.executable), "python_version": sys.version, "platform": platform.platform(),
            "runner_image": {name: os.getenv(name) for name in ("ImageOS", "ImageVersion", "RUNNER_OS", "RUNNER_ARCH")},
            "helper": pin(HELPER_PATH), "supervisor": pin(SUPERVISOR_PATH),
            "owner": pin(OWNER_PATH), "publisher": pin(PUBLISHER_PATH)}


def allowed_ld_library_path(value):
    if not value:
        return False
    base_lib = Path(sys.base_prefix).resolve() / "lib"
    parts = [part for part in value.split(os.pathsep) if part]
    return len(parts) == 1 and base_lib.is_dir() and Path(parts[0]).resolve() == base_lib


def safe_environment():
    env = {key: os.environ[key] for key in ("PATH", "LANG", "LC_ALL", "TMPDIR", "ImageOS", "ImageVersion",
                                           "RUNNER_OS", "RUNNER_ARCH") if key in os.environ}
    if allowed_ld_library_path(os.environ.get("LD_LIBRARY_PATH", "")):
        env["LD_LIBRARY_PATH"] = os.environ["LD_LIBRARY_PATH"]
    return {**env, "PYTHONPATH": str(ROOT / "src") + os.pathsep + str(ROOT),
            "PYTHONDONTWRITEBYTECODE": "1", "DEBUGINFOD_URLS": ""}


def validate_mode(mode, expected_sha, *, skip_upload, comments_only, subject):
    require(mode in ("off", "qualify", "gdb"), "Unknown debugger mode")
    if mode == "off":
        require(not expected_sha, "Debugger pin requires debugger mode")
        return
    require(skip_upload and comments_only, "Debugger modes require skip_upload and comments_only")
    require(isinstance(subject, dict) and set(subject) == {"table_uuid", "snapshot_id", "schema_id"}
            and isinstance(subject["table_uuid"], str) and bool(subject["table_uuid"])
            and type(subject["snapshot_id"]) is int and subject["snapshot_id"] > 0
            and type(subject["schema_id"]) is int and subject["schema_id"] >= 0, "Exact diagnostic subject required")
    require((mode == "qualify" and not expected_sha) or
            (mode == "gdb" and re.fullmatch(r"[0-9a-f]{64}", expected_sha)), "Reviewed GDB SHA-256 required for native mode")


def check_source(source):
    require(re.fullmatch(r"[0-9a-f]{40}", source), "Exact source commit required")
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, timeout=5).strip()
    require(actual == source, "Debugger source differs")
    require(not subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=no"],
                                        cwd=ROOT, text=True, timeout=5).strip(), "Tracked debugger inputs modified")


def launch_command(folder, binding, command, *, credential_free):
    from spicy_regs import operational_runner as owner
    verified_module(owner, OWNER_PATH, "owner")
    folder.mkdir(parents=True, exist_ok=True)
    request = {"binding": binding, "command": command, "credential_free": credential_free}
    owner.durable(folder / "request.json", request)
    (folder / "commands.gdb").write_text(GDB_PROGRAM)
    return [sys.executable, "-B", str(Path(__file__).resolve()), "launch", "--workdir", str(folder),
            "--request-sha256", pin(folder / "request.json")["sha256"]]


def launch(args):
    folder = args.workdir.resolve()
    require(pin(folder / "request.json")["sha256"] == args.request_sha256, "Debugger request differs")
    request = read(folder / "request.json")
    binding = request["binding"]
    require(identity(binding["gdb"]["path"], binding["source_commit"], binding["invocation"]) == binding,
            "Debugger executable or implementation changed")
    require((folder / "commands.gdb").read_text() == GDB_PROGRAM, "Debugger command program differs")
    command = request["command"]
    require(isinstance(command, list) and all(isinstance(value, str) for value in command), "Debugger child command refused")
    require(command[:3] == [sys.executable, "-B", str(Path(__file__).resolve() if request["credential_free"]
                                                    else ROOT / "scripts/publish_comments_mirror_guarded.py")],
            "Debugger may run only its fixture or guarded publisher")
    env = safe_environment() if request["credential_free"] else dict(os.environ)
    if not request["credential_free"]:
        require(not any((name.startswith(("LD_", "DYLD_", "GDB", "GUILE_"))
                         and not (name == "LD_LIBRARY_PATH" and allowed_ld_library_path(env[name])))
                        or name in ("PYTHONHOME", "PYTHONSTARTUP", "PYTHONINSPECT") for name in env),
                "Unqualified debugger loader or initialization environment")
    env.update(COMMENTS_DEBUGGER_ROOT=str(folder), DEBUGINFOD_URLS="", PYTHONDONTWRITEBYTECODE="1")
    if request["credential_free"]:
        env["COMMENTS_DEBUGGER_CANARY"] = CANARY
        command = [*command, "--canary", CANARY]
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    gdb = binding["gdb"]["path"]
    options = [part for setting in SETTINGS for part in ("-iex", setting)]
    os.execve(gdb, [gdb, "--nx", "--batch", "--quiet", *options,
                   "--command", str(folder / "commands.gdb"), "--args", *command], env)


def check_case(folder, case):
    execution = read(folder / "execution.json")
    require(execution.get("remaining_processes") == [] and (folder / "process-closure.json").is_file(),
            "Debugger case lacks closed process proof")
    expected_limits = {**LIMITS, "wall_seconds": 10 if case == "timeout" else 60}
    require(execution["limits"] == expected_limits, "Debugger case resource limits differ")
    for path in (folder / "execution.log", folder / "debugger.json", folder / "native-backtrace.log"):
        if path.exists():
            raw = path.read_bytes()
            require(len(raw) <= TEXT_LIMIT and CANARY.encode() not in raw, "Debugger output bound or private-value refusal")
    if case == "timeout":
        require(execution["status"] == "failed" and "wall limit" in execution.get("error", ""), "Timeout did not refuse")
        require((folder / "fixture-ready.json").is_file(), "Timeout fixture did not start its descendant")
        closure = read(folder / "process-closure.json")
        descendant = read(folder / "fixture-ready.json")["pid"]
        require(len(closure["known"]) >= 3 and descendant in {member["pid"] for member in closure["known"]},
                "Timeout closure did not observe debugger, inferior and descendant")
    else:
        result = read(folder / "debugger.json")
        require(result["core_limit"] == [0, 0] and not result.get("error_type"), "Debugger configuration failed")
        settings = result["settings"]
        require(all(settings[name] is False for name in (
            "auto-load python-scripts", "auto-load gdb-scripts", "auto-load local-gdbinit", "startup-with-shell",
            "disable-randomization", "index-cache enabled")) and settings["debuginfod enabled"] == "off"
            and settings["print frame-arguments"] == "none" and settings["print entry-values"] == "no"
            and settings["print frame-info"] == "location-and-address", "Debugger effective settings differ")
        expected = {"exit0": 0, "exit7": 7, "abort": 134}[case]
        require(result["wrapper_exit_code"] == expected, "Debugger wrapper exit differs")
        require(execution["status"] == ("complete" if expected == 0 else "failed"), "Debugger masked child outcome")
        if expected:
            require(f"child exited {expected};" in execution.get("error", ""), "Supervisor child exit differs")
        if case == "abort":
            require(result["signal"] == "SIGABRT" and result["native_backtrace"] and result["threads_available"] >= 2
                    and not result["trace_truncated"], "Abort did not produce the required bounded native stack")
        else:
            require(result["exit_code"] == expected and result["signal"] is None, "Inferior exit differs")
        require(b"fixture-output-preserved" in (folder / "execution.log").read_bytes(), "Inferior output was lost")
    return {"case": case, "execution": pin(folder / "execution.json"), "closure": pin(folder / "process-closure.json"),
            "log": pin(folder / "execution.log"),
            "debugger": pin(folder / "debugger.json") if (folder / "debugger.json").exists() else None,
            "trace": pin(folder / "native-backtrace.log") if (folder / "native-backtrace.log").exists() else None}


def qualify_worker(args):
    from scripts import read_comment_attachment_group as runner
    from spicy_regs import operational_runner as owner
    verified_module(runner, SUPERVISOR_PATH, "supervisor")
    verified_module(owner, OWNER_PATH, "owner")
    gdb = shutil.which("gdb")
    require(gdb is not None and sys.platform == "linux", "Installed Linux GDB unavailable; no native diagnostic admitted")
    binding = identity(gdb, args.expected_source_sha, args.invocation)
    require(not args.expected_gdb_sha256 or binding["gdb"]["sha256"] == args.expected_gdb_sha256,
            "Installed GDB differs from reviewed binary")
    owner.durable(args.workdir / "binding.json", binding)
    cases = []
    for case in CASES:
        folder = args.workdir / case
        command = launch_command(folder, binding, [sys.executable, "-B", str(Path(__file__).resolve()), "fixture",
                                                   "--case", case, "--workdir", str(folder)],
                                 credential_free=True)
        try:
            runner.supervise(SimpleNamespace(workdir=folder), command=command,
                             limits={**LIMITS, "wall_seconds": 10 if case == "timeout" else 60})
        except ValueError:
            # Only exact expected nonzero/timeout outcomes below are accepted.
            pass
        cases.append(check_case(folder, case))
    owner.durable(args.workdir / "qualification.json", {"format": "comments-debugger-qualification/1", "binding": binding,
                                                       "gdb_version": read(args.workdir / "exit0/debugger.json")["gdb_version"],
                                                       "cases": cases, "qualified": True})


def qualify(args):
    started = time.monotonic()
    # Replace, rather than merely delete known credential names. The fixture cannot inherit any secret.
    safe = safe_environment()
    os.environ.clear()
    os.environ.update(safe)
    from scripts import read_comment_attachment_group as runner
    from spicy_regs import operational_runner as owner
    verified_module(runner, SUPERVISOR_PATH, "supervisor")
    verified_module(owner, OWNER_PATH, "owner")
    require(not args.workdir.exists(), "Fresh debugger qualification directory required")
    check_source(args.expected_source_sha)
    command = [sys.executable, "-B", str(Path(__file__).resolve()), "qualify-worker", "--workdir", str(args.workdir),
               "--expected-source-sha", args.expected_source_sha, "--invocation", args.invocation,
               "--expected-gdb-sha256", args.expected_gdb_sha256]
    runner.supervise(args, command=command, limits={**LIMITS, "wall_seconds": TOTAL_ACTIVE})
    elapsed = time.monotonic() - started
    owner.durable(args.workdir / "qualification-time.json", {"elapsed_seconds": elapsed})
    require(elapsed <= 300, "Debugger qualification total elapsed limit")


def verified_qualification(path, source, expected_sha, invocation):
    require(re.fullmatch(r"[0-9a-f]{64}", expected_sha), "Reviewed debugger SHA-256 required")
    receipt = read(path)
    require(receipt.get("format") == "comments-debugger-qualification/1" and receipt.get("qualified") is True,
            "Debugger qualification incomplete")
    binding = receipt["binding"]
    require(binding == identity(binding["gdb"]["path"], source, invocation) and binding["gdb"]["sha256"] == expected_sha,
            "Debugger qualification identity differs")
    require([case["case"] for case in receipt["cases"]] == list(CASES), "Debugger qualification cases differ")
    folder = Path(path).parent
    for case in receipt["cases"]:
        require(check_case(folder / case["case"], case["case"]) == case, "Debugger case evidence changed")
    execution = read(folder / "execution.json")
    require(execution["status"] == "complete" and execution.get("remaining_processes") == []
            and execution["elapsed_seconds"] <= 300 and execution["limits"] == {**LIMITS, "wall_seconds": TOTAL_ACTIVE},
            "Debugger qualification supervision differs")
    elapsed = read(folder / "qualification-time.json")["elapsed_seconds"]
    require(execution["elapsed_seconds"] <= elapsed <= 300, "Debugger qualification total charge differs")
    return {"binding": binding, "receipt": pin(path), "elapsed_seconds": elapsed}


def fixture(args):
    # Only synthetic values. Never import project code, storage, or a production native engine.
    import threading
    private_local = args.canary
    require(private_local == CANARY and os.getenv("COMMENTS_DEBUGGER_CANARY") == CANARY, "Synthetic canary absent")
    require(resource.getrlimit(resource.RLIMIT_CORE) == (0, 0), "Fixture cores enabled")
    print("fixture-output-preserved", flush=True)
    if args.case == "exit7":
        raise SystemExit(7)
    if args.case == "abort":
        ready = threading.Event()
        def waiting():
            ready.set()
            time.sleep(120)
        threading.Thread(target=waiting, daemon=True).start()
        require(ready.wait(5), "Synthetic second thread did not start")
        os.abort()
    if args.case == "timeout":
        child = subprocess.Popen([sys.executable, "-B", "-c", "import time; time.sleep(120)"])
        (args.workdir / "fixture-ready.json").write_text(json.dumps({"pid": child.pid}))
        time.sleep(120)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("check-mode", "qualify", "qualify-worker", "launch", "fixture"))
    parser.add_argument("--workdir", type=Path)
    parser.add_argument("--expected-source-sha", default="")
    parser.add_argument("--invocation", default="")
    parser.add_argument("--expected-gdb-sha256", default="")
    parser.add_argument("--request-sha256", default="")
    parser.add_argument("--case", choices=CASES)
    parser.add_argument("--canary", default="")
    args = parser.parse_args()
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    if args.action == "check-mode":
        validate_mode(os.getenv("DEBUGGER_MODE", "off"), os.getenv("EXPECTED_GDB_SHA256", ""),
                      skip_upload=os.getenv("SKIP_UPLOAD") == "true", comments_only=os.getenv("COMMENTS_ONLY") == "true",
                      subject=json.loads(os.getenv("DIAGNOSTIC_NATIVE_SNAPSHOT") or "null"))
    else:
        require(args.workdir is not None, "Debugger work directory required")
        if args.action in ("qualify", "qualify-worker"):
            require(re.fullmatch(r"[0-9]+\.[0-9]+", args.invocation), "Exact hosted invocation required")
        {"qualify": qualify, "qualify-worker": qualify_worker, "launch": launch, "fixture": fixture}[args.action](args)


if __name__ == "__main__":
    main()
