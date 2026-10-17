"""Fixed, explicit build/audit execution with retained process and byte evidence.

Sampled limits can overshoot. This is not an OS sandbox. Only observed process
trees and the live guardian's process group are covered; unknown detached
descendants are outside the assurance. No command automatically resumes a run.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import fcntl
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import posix
import runpy
import shutil
import signal
import stat
import subprocess
import sys
import time
import uuid

try:
    import psutil
except ImportError as error:
    raise ImportError(
        "spicy-regs-run needs the operational-runner extra: install spicy-regs[operational-runner]"
    ) from error

LIMITATIONS = (__doc__ or "").strip()
PHASES = ("build", "audit")
PHASE_RECORDS = ("launch", "child", "release", "payload-release", "terminal")
RUN_RECORDS = ("owner", "result", "reconciliation", "candidate", "build-completion", "reused-build")
REQUEST_KEYS = {
    "version",
    "run_root",
    "python",
    "inputs",
    "code",
    "runtime",
    "activation",
    "offline_single_process",
    "limits",
    "commands",
    "continue_from",
}
PIN_KEYS = {"path", "bytes", "sha256"}
# What produced the candidate. Limits and the audit command do not, so a resource
# failure in the audit can be continued with adjusted limits or a corrected audit.
BINDING_KEYS = ("version", "python", "inputs", "code", "runtime", "activation", "offline_single_process")
STOP_SECONDS = 30
HASH_READS = {}
ROOT_BINDINGS = {}
RAW_OPEN = os.open
# Live kernel state (Linux /proc/stat changes between reads), so no pin can describe it.
KERNEL_ROOTS = (Path("/proc"), Path("/sys"), Path("/dev"))


class OwnedRootChanged(ValueError):
    pass


def bind_root(root, expected=None):
    info = Path(root).lstat()
    actual = [info.st_dev, info.st_ino]
    require(stat.S_ISDIR(info.st_mode) and not Path(root).is_symlink(), "Owned directory required")
    if expected is not None and actual != list(expected):
        raise OwnedRootChanged("Owned root replaced")
    ROOT_BINDINGS[str(root)] = actual
    return actual


def check_bound_path(path):
    for root, expected in ROOT_BINDINGS.items():
        if Path(path).is_relative_to(root):
            info = Path(root).lstat()
            if not stat.S_ISDIR(info.st_mode) or [info.st_dev, info.st_ino] != expected:
                raise OwnedRootChanged("Owned root replaced")


def traversal_error(error):
    raise error


def vanished_or_error(error):
    """Sampling tolerates an entry deleted mid-walk; an unreadable directory must refuse."""
    if not isinstance(error, FileNotFoundError):
        raise error


def require(value, message):
    if not value:
        raise ValueError(message)


def kernel_interface(path):
    """Kernel-provided state (procfs, sysfs, device nodes): read freely, never pinned.

    The path is resolved here, so a link into a kernel directory counts as its target.
    A regular file under /dev (tmpfs such as /dev/shm) is ordinary data and keeps its pin.
    """
    path = Path(path).resolve()
    dev = Path("/dev").resolve()
    return any(path.is_relative_to(base.resolve()) for base in KERNEL_ROOTS) and not (
        path.is_relative_to(dev) and path.is_file()
    )


def digest(value):
    return "sha256:" + hashlib.sha256(value).hexdigest()


def read_json(path):
    with Path(path).open("rb") as stream:
        raw = stream.read(16 * 1024 * 1024 + 1)
    require(len(raw) <= 16 * 1024 * 1024, "Metadata byte bound")
    return json.loads(raw)


def durable(path, value):
    """One writer per record; fsync body and containing directory before return."""
    path = Path(path)
    check_bound_path(path)
    temporary = path.with_name(".runner-record-" + path.name + "." + uuid.uuid4().hex)
    fd = RAW_OPEN(path.parent, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        expected = ROOT_BINDINGS.get(str(path.parent))
        info = os.fstat(fd)
        if expected is not None and [info.st_dev, info.st_ino] != expected:
            raise OwnedRootChanged("Owned record directory replaced")
        body_fd = RAW_OPEN(temporary.name, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600, dir_fd=fd)
        with os.fdopen(body_fd, "wb") as stream:
            stream.write((json.dumps(value, sort_keys=True, indent=2) + "\n").encode())
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary.name, path.name, src_dir_fd=fd, dst_dir_fd=fd)
        os.fsync(fd)
    finally:
        os.close(fd)


def regular(path):
    path = Path(path)
    require(path.is_absolute(), "Absolute file path required")
    require(not any(p.is_symlink() for p in (path, *path.parents)), "Symlink refused: " + str(path))
    info = path.stat()
    require(stat.S_ISREG(info.st_mode), "Regular file required: " + str(path))
    return info


def file_pin(path, tick=lambda: None):
    path = Path(path)
    before = regular(path)
    sha = hashlib.sha256()
    with path.open("rb") as stream:
        while data := stream.read(1024 * 1024):
            tick()
            sha.update(data)
    after = regular(path)
    require(
        (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns, before.st_ctime_ns)
        == (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns, after.st_ctime_ns),
        "File changed while hashing: " + str(path),
    )
    previous = HASH_READS.setdefault(str(path), {"calls": 0, "bytes": 0})
    previous["calls"] += 1
    previous["bytes"] += after.st_size
    return {"path": str(path), "bytes": after.st_size, "sha256": "sha256:" + sha.hexdigest()}


def verify_pin(pin, tick=lambda: None):
    require(file_pin(Path(pin["path"]), tick) == pin, "File pin differs: " + str(pin["path"]))


def pinned_bytes(pin):
    """Read a pinned file once and decide on exactly those bytes."""
    raw = Path(pin["path"]).read_bytes()
    require(len(raw) == pin["bytes"] and digest(raw) == pin["sha256"], "File pin differs: " + pin["path"])
    return raw


def census(root, tick=lambda: None, skip=()):
    """Strict closed census, taken after every worker has exited.

    Regular single-link files are hashed. A file symlink that stays inside the
    root and resolves to a regular file is recorded as a link (court producers
    select their bundle that way). Dangling, escaping or directory links,
    hardlinks, special files and traversal errors refuse. `skip` names
    top-level directories left out of the listing.
    """
    root = Path(root)
    require(root.is_dir() and not root.is_symlink(), "Missing/unsafe output root")
    real_root = root.resolve()
    files = []
    for directory, dirs, names in os.walk(root, followlinks=False, onerror=traversal_error):
        tick()
        if Path(directory) == root:
            dirs[:] = [name for name in dirs if name not in skip]
        for name in dirs:
            require(not (Path(directory) / name).is_symlink(), "Output directory symlink")
        for name in sorted(names):
            path = Path(directory) / name
            relative = str(path.relative_to(root))
            if path.is_symlink():
                try:
                    target = path.resolve(strict=True)
                except (OSError, RuntimeError):
                    raise ValueError("Dangling output link: " + relative) from None
                require(
                    target.is_relative_to(real_root) and stat.S_ISREG(target.lstat().st_mode),
                    "Output link escapes the root or is not a file: " + relative,
                )
                files.append(
                    {"path": relative, "link": os.readlink(path), "target": str(target.relative_to(real_root))}
                )
                continue
            require(regular(path).st_nlink == 1, "Closed output hardlink")
            files.append({**file_pin(path, tick), "path": relative})
    return sorted(files, key=lambda item: item["path"])


def birth(pid=None):
    process = psutil.Process(os.getpid() if pid is None else pid)
    return {"pid": process.pid, "created": process.create_time()}


def matching(identity):
    if identity["pid"] <= 1:
        return None
    try:
        process = psutil.Process(identity["pid"])
        if process.create_time() == identity["created"] and process.status() != psutil.STATUS_ZOMBIE:
            return process
    except psutil.NoSuchProcess:
        pass
    return None


def members(leader, known):
    """Discover only while the original session leader is alive; retain births."""
    parent = matching(leader)
    if parent:
        for process in parent.children(recursive=True):
            try:
                identity = {"pid": process.pid, "created": process.create_time()}
                related = any(
                    ancestor.pid == leader["pid"] and ancestor.create_time() == leader["created"]
                    for ancestor in process.parents()
                )
                if related and matching(identity) and matching(leader):
                    known[process.pid] = identity
            except psutil.NoSuchProcess:
                pass
        # Reparented processes that kept the guardian's group remain attributable.
        for process in psutil.process_iter(["pid"]):
            # getpgid(0) means the caller's group, not the kernel's PID 0.
            if process.pid <= 1:
                continue
            try:
                identity = {"pid": process.pid, "created": process.create_time()}
                if (
                    process.pid != leader["pid"]
                    and os.getpgid(process.pid) == leader["pid"]
                    and matching(identity)
                    and matching(leader)
                ):
                    known[process.pid] = identity
            except (ProcessLookupError, psutil.NoSuchProcess):
                pass
    return [identity for identity in known.values() if matching(identity)]


def close_members(leader, known, record_path):
    """Bounded rediscovery closes descendants created during termination."""
    deadline = time.monotonic() + 4
    for _ in range(4):
        current = members(leader, known)
        try:
            durable(record_path, {"identity": leader, "known": list(known.values()), "state": "closing"})
        except OwnedRootChanged:
            pass  # Lost file ownership must not prevent exact process cleanup.
        if not current:
            return []
        terminate(current)
        if time.monotonic() >= deadline:
            break
    remaining = members(leader, known)
    try:
        durable(record_path, {"identity": leader, "known": list(known.values()), "state": "closing"})
    except OwnedRootChanged:
        pass
    return remaining


def terminate(identities):
    """psutil's signal methods recheck PID reuse immediately before signalling."""
    processes = []
    for identity in identities:
        process = matching(identity)
        if process:
            try:
                process.terminate()
                processes.append(process)
            except psutil.NoSuchProcess:
                pass
    _, alive = psutil.wait_procs(processes, timeout=1)
    for process in alive:
        try:
            process.kill()
        except psutil.NoSuchProcess:
            pass
    psutil.wait_procs(alive, timeout=1)
    return [identity for identity in identities if matching(identity)]


def sizes(root, deadline=None):
    """Tolerant sampling. Producers create and delete scratch while this walks,
    so a vanished entry is skipped and nothing is refused by type; links are
    charged as links and never followed. The strict check is the final census."""
    logical = allocated = 0
    parts = {"scratch": 0, "data": 0, "evidence": 0}

    def part(first):
        # Hidden root staging directories (a producer's candidate bytes in transit) are data.
        staging = first.startswith(".") and not first.startswith(".runner-record-")
        return (
            "scratch" if first == "scratch" else "data" if first in ("candidate", "output") or staging else "evidence"
        )

    for directory, dirs, names in os.walk(root, followlinks=False, onerror=vanished_or_error):
        require(deadline is None or time.monotonic() <= deadline, "Whole-run wall limit during accounting")
        try:
            info = os.lstat(directory)
        except FileNotFoundError:
            continue
        relative = Path(directory).relative_to(root).parts
        allocated += info.st_blocks * 512
        parts[part(relative[0] if relative else "")] += info.st_blocks * 512
        for name in [*names, *(name for name in dirs if os.path.islink(os.path.join(directory, name)))]:
            require(deadline is None or time.monotonic() <= deadline, "Whole-run wall limit during accounting")
            try:
                info = os.lstat(os.path.join(directory, name))
            except FileNotFoundError:
                continue
            logical += info.st_size
            allocated += info.st_blocks * 512
            parts[part(relative[0] if relative else name)] += max(info.st_size, info.st_blocks * 512)
    return {"logical": logical, "allocated": allocated, **parts}


class Budget:
    """Shared whole-run sampler for owner hashing and guardian execution."""

    def __init__(self, root, request, started, identities=lambda: []):
        self.root, self.limits, self.started = Path(root), request["limits"], started
        self.identities, self.last, self.peaks = identities, {}, {}
        self.sampled = 0.0
        info = self.root.stat()
        self.root_identity = (info.st_dev, info.st_ino)

    def tick(self, force=False):
        now = time.monotonic()
        info = self.root.stat()
        require(not self.root.is_symlink() and (info.st_dev, info.st_ino) == self.root_identity, "Owned root replaced")
        require(now - self.started <= self.limits["wall_seconds"], "Whole-run wall limit")
        if not force and now - self.sampled < self.limits["interval_seconds"]:
            return
        rss = 0
        for identity in {item["pid"]: item for item in [birth(), *self.identities()]}.values():
            process = matching(identity)
            if process:
                try:
                    rss += process.memory_info().rss
                except psutil.NoSuchProcess:
                    pass
        self.last = {
            **sizes(self.root, self.started + self.limits["wall_seconds"]),
            "rss": rss,
            "elapsed": now - self.started,
            "free": shutil.disk_usage(self.root).free,
        }
        self.sampled = now
        for key in ("logical", "allocated", "rss"):
            self.peaks[key] = max(self.peaks.get(key, 0), self.last[key])
        require(self.last["free"] >= self.limits["stop_free_bytes"], "Disk stop floor")
        require(rss <= self.limits["rss_bytes"], "Observed process RSS limit")
        require(
            max(self.last["logical"], self.last["allocated"]) <= self.limits["output_bytes"] - 65536,
            "Owned output size limit",
        )
        for part in ("scratch", "data", "evidence"):
            if part + "_bytes" in self.limits:
                require(self.last[part] <= self.limits[part + "_bytes"], "Owned " + part + " limit")


def check_pins(request, tick=lambda: None):
    seen = {}
    for kind in ("inputs", "code", "runtime"):
        for pin in request[kind]:
            if pin["path"] in seen:
                require(pin == seen[pin["path"]], "Conflicting selected pin")
            else:
                verify_pin(pin, tick)
                seen[pin["path"]] = pin


def binding(request):
    return {**{key: request[key] for key in BINDING_KEYS}, "build": request["commands"]["build"]}


def grant_check(raw, grant):
    require(
        type(grant) is dict and grant.get("authorized") is True and grant.get("request_sha256") == digest(raw),
        "Exact request grant required",
    )
    expiry = datetime.fromisoformat(grant["expires_at"])
    require(expiry.tzinfo is not None and datetime.now(timezone.utc) < expiry, "Expired grant")


def is_pin(pin):
    return (
        type(pin) is dict
        and set(pin) == PIN_KEYS
        and type(pin["path"]) is str
        and type(pin["bytes"]) is int
        and type(pin["sha256"]) is str
    )


def validate(request):
    require(type(request) is dict, "Runner request object required")
    unknown = set(request) - REQUEST_KEYS
    require(not unknown, "Unsupported request field(s): " + ", ".join(sorted(unknown)))
    missing = REQUEST_KEYS - {"continue_from"} - set(request)
    require(not missing, "Missing request field(s): " + ", ".join(sorted(missing)))
    require(request["version"] == 1, "Unsupported runner request")
    require(type(request["offline_single_process"]) is bool, "offline_single_process must be a boolean")
    activation = request["activation"]
    require(
        type(activation) is dict
        and set(activation) == {"prefix", "paths", "base_paths", "origins"}
        and type(activation["prefix"]) is str
        and type(activation["origins"]) is dict
        and all(
            type(activation[key]) is list and all(type(path) is str for path in activation[key])
            for key in ("paths", "base_paths")
        ),
        "Unsupported activation shape",
    )
    for kind in ("inputs", "code", "runtime"):
        require(
            type(request[kind]) is list and all(is_pin(pin) for pin in request[kind]),
            "Pin lists need exactly path, bytes and sha256: " + kind,
        )
        kernel = [pin["path"] for pin in request[kind] if kernel_interface(pin["path"])]
        require(not kernel, "Kernel interface cannot be pinned (" + kind + "): " + ", ".join(kernel[:1]))
    require(request.get("continue_from") is None or is_pin(request["continue_from"]), "Unsupported continue_from pin")
    require(type(request["run_root"]) is str, "Unsafe run root")
    root = Path(request["run_root"])
    require(
        root.is_absolute()
        and str(root) == os.path.normpath(str(root))
        and not any(p.is_symlink() for p in (root, *root.parents)),
        "Unsafe run root",
    )
    require(
        type(request["commands"]) is dict and set(request["commands"]) == set(PHASES),
        "Only build and complete audit commands supported",
    )
    limits = request["limits"]
    required = {"wall_seconds", "interval_seconds", "rss_bytes", "output_bytes", "start_free_bytes", "stop_free_bytes"}
    require(
        type(limits) is dict
        and required <= set(limits) <= required | {"scratch_bytes", "data_bytes", "evidence_bytes"},
        "Declared limits required",
    )
    require(
        all(type(value) in (int, float) and value > 0 for value in limits.values()), "Positive finite limits required"
    )
    require(all(value < float("inf") for value in limits.values()), "Finite limits required")
    require(
        limits["interval_seconds"] <= 1 and limits["start_free_bytes"] >= limits["stop_free_bytes"], "Limit ordering"
    )
    require(limits["output_bytes"] > 65536, "Terminal accounting reserve required")
    require(
        type(request["python"]) is str and Path(request["python"]).is_absolute(), "Absolute Python launcher required"
    )
    # Launcher may be a venv symlink; pin its resolved binary in runtime.
    require(str(Path(request["python"]).resolve()) in {p["path"] for p in request["runtime"]}, "Unpinned interpreter")
    require(str(Path(__file__).resolve()) in {p["path"] for p in request["code"]}, "Unpinned maintained runner")
    for phase in PHASES:
        command = request["commands"][phase]
        require(
            type(command) is dict and set(command) == {"module", "args"} and type(command["args"]) is list,
            "Python module command required",
        )
        require(
            type(command["module"]) is str and all(isinstance(arg, str) for arg in command["args"]),
            "String arguments required",
        )
    return root


def child_argv(request, root, *args):
    """One launcher form for guardian and payload.

    -B stops bytecode writes; the pycache prefix keeps Python from reading any
    existing __pycache__ file, so the pinned .py source is the code that runs.
    """
    return [
        request["python"],
        "-I",
        "-B",
        "-X",
        "pycache_prefix=" + str(root / "scratch" / "pycache"),
        str(Path(__file__).resolve()),
        *args,
    ]


def activate(request, root):
    """Activate before command imports, then check every loaded file against pins."""
    require(sys.flags.isolated and sys.dont_write_bytecode and not sys.flags.optimize, "Use isolated unoptimized -I -B")
    require(sys.pycache_prefix == str(root / "scratch" / "pycache"), "Run-owned bytecode prefix required")
    activation = request["activation"]
    require(str(Path(sys.executable).resolve()) == str(Path(request["python"]).resolve()), "Interpreter route drift")
    require(str(Path(sys.prefix).resolve()) == activation["prefix"], "Runtime prefix drift")
    require(
        not any(name == "spicy_regs" or name.startswith("spicy_regs.") for name in sys.modules), "Preloaded package"
    )
    require([str(Path(path).resolve()) for path in sys.path] == activation["base_paths"], "Initial import paths drift")
    sys.path[:0] = activation["paths"]
    # Python children of a payload that may spawn them keep the same code.
    os.environ["PYTHONPATH"] = os.pathsep.join(activation["paths"])
    for name, expected in activation["origins"].items():
        spec = importlib.util.find_spec(name)
        require(spec and spec.origin and str(Path(spec.origin).resolve()) == expected, "Import origin differs: " + name)


def loaded_files(request):
    allowed = {pin["path"]: pin for key in ("inputs", "code", "runtime") for pin in request[key]}
    found = {}
    for name, module in list(sys.modules.items()):
        path = getattr(module, "__file__", None)
        if not path or path.startswith("<"):
            continue
        path = str(Path(path).resolve())
        for package, expected in request["activation"]["origins"].items():
            if name == package or name.startswith(package + "."):
                origin = Path(expected).parent
                require(Path(path).is_relative_to(origin), "Mixed package origin: " + name)
                require(
                    all(Path(value).resolve().is_relative_to(origin) for value in getattr(module, "__path__", [])),
                    "Mixed package search path: " + name,
                )
        require(path in allowed, "Unpinned loaded module: " + name + " " + path)
        if path not in found:
            verify_pin(allowed[path])
            found[path] = allowed[path]
    return sorted(found.values(), key=lambda item: item["path"])


def offline_guard(root, request, readable=(), phase=None):
    """Retained H/D Python guards. Native I/O is outside these observations.

    Python signals may target only the payload itself, never another process or a group.
    Writes stay inside the payload's write scope: `scratch/`, `output/`, `candidate/`
    (build phase only), the phase's own `{phase}-imports.json`, and hidden
    producer staging directories at the root (for example `.uscode-editions-*`).
    The runner's own records are never writable. Reads may also come from pinned
    paths, directories, kernel interfaces (`/proc/stat`: live state no pin can
    describe) and `readable` roots (a reused, already verified candidate). A
    descriptor-relative open or mutation is resolved through its directory
    descriptor, so `shutil.rmtree` and `TemporaryDirectory` work. That judgment
    applies to calls through the patched `os.open`/`posix.open`; a reference
    captured before the guard was installed is not covered.
    """
    allowed = {pin["path"] for kind in ("inputs", "code", "runtime") for pin in request[kind]}
    readable = [Path(path).resolve() for path in readable]
    write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
    records = ".runner-record-"

    def descriptor_path(fd):
        if sys.platform == "darwin":
            return Path(os.fsdecode(fcntl.fcntl(fd, 50, bytes(1024)).split(b"\0", 1)[0])).resolve()
        return Path(os.readlink("/proc/self/fd/" + str(fd))).resolve()

    def location(value, dir_fd=None):
        if isinstance(value, int):
            return descriptor_path(value)
        path = Path(os.fsdecode(value))
        if dir_fd not in (None, -1) and not path.is_absolute():
            path = descriptor_path(dir_fd) / path
        return path.resolve()

    def inside(value, dir_fd=None):
        require(location(value, dir_fd).is_relative_to(root), "Python write outside owned root")

    def writable(value, dir_fd=None, exist_ok=False):
        path = location(value, dir_fd)
        require(path.is_relative_to(root), "Python write outside owned root")
        parts = path.relative_to(root).parts
        if exist_ok and not parts:
            return  # `mkdir(parents=True, exist_ok=True)` on the root itself creates nothing.
        own = (
            len(parts) == 1
            and phase is not None
            and (parts[0] == phase + "-imports.json" or parts[0].startswith(records + phase + "-imports.json."))
        )
        require(
            bool(parts)
            and (
                parts[0] in ("scratch", "output")
                or (parts[0] == "candidate" and phase == "build")
                or own
                or (parts[0].startswith(".") and not parts[0].startswith(records))
            ),
            "Python write to a runner-owned path: " + str(path.relative_to(root)),
        )

    def readable_path(path):
        require(
            path.is_relative_to(root)
            or any(path.is_relative_to(base) for base in readable)
            or str(path) in allowed
            or path.is_dir()
            or kernel_interface(path),
            "Undeclared Python read: " + str(path),
        )

    def audit(event, values):
        if event.startswith("socket.") or event in {
            "subprocess.Popen",
            "os.system",
            "os.posix_spawn",
            "os.posix_spawnp",
            "os.exec",
            "os.fork",
            "os.forkpty",
            "os.spawn",
        }:
            raise ValueError("Offline single-process worker forbids Python network/subprocess")
        if event == "open":
            value, mode, flags = values
            if (isinstance(mode, str) and any(c in mode for c in "wax+")) or flags & write_flags:
                writable(value)
            else:
                readable_path(location(value))
        elif event == "os.mkdir":
            writable(values[0], values[-1], exist_ok=True)
        elif event in {"os.remove", "os.rmdir", "os.chmod", "os.utime", "os.chown"}:
            writable(values[0], values[-1])
        elif event in {"os.truncate", "os.chflags", "os.setxattr", "os.removexattr"}:
            writable(values[0])
        elif event == "os.chdir":
            inside(values[0])
        elif event == "os.rename":
            writable(values[0], values[2])
            writable(values[1], values[3])
        elif event in {"os.link", "os.symlink"}:
            raise ValueError("Python links refused")
        elif event == "os.kill":
            require(values[0] == os.getpid(), "Offline worker may signal only itself")
        elif event == "os.killpg":
            raise ValueError("Offline worker may not signal a process group")

    original_open = os.open

    def checked_open(path, flags, mode=0o777, *, dir_fd=None):
        # CPython's open audit event omits dir_fd, so a relative name would be judged
        # against the working directory. Judge the descriptor-relative location here.
        if dir_fd is not None:
            where = location(path, dir_fd)
            if flags & write_flags:
                writable(where)
            else:
                readable_path(where)
        return original_open(path, flags, mode, dir_fd=dir_fd)

    setattr(os, "open", checked_open)
    setattr(posix, "open", checked_open)  # os.open is posix.open; patch both names.
    os.supports_dir_fd.add(checked_open)  # Callers feature-test descriptor-relative opens by identity.
    sys.addaudithook(audit)


def await_release(path, identity, parent_live, interval, tick=lambda: None):
    """Wait until the parent durably released exactly this process."""
    while not path.exists():
        require(parent_live(), "Parent disappeared before release")
        tick()
        time.sleep(interval)
    require(read_json(path) == identity and parent_live(), "Stale release")


def payload(root, phase, root_identity, guardian_pid):
    bind_root(root, root_identity)
    request = read_json(root / "request.json")
    # No activation, import or user code before the guardian recorded this process.
    await_release(
        root / (phase + "-payload-release.json"),
        birth(),
        lambda: os.getppid() == guardian_pid,
        request["limits"]["interval_seconds"],
    )
    activate(request, root)
    candidate = read_json(root / "candidate.json")["path"]
    if request["offline_single_process"]:
        offline_guard(root, request, readable=[] if Path(candidate).is_relative_to(root) else [candidate], phase=phase)
    before = loaded_files(request)
    command = request["commands"][phase]
    sys.argv = [
        command["module"],
        *[arg.replace("{run}", str(root)).replace("{candidate}", candidate) for arg in command["args"]],
    ]
    try:
        runpy.run_module(command["module"], run_name="__main__", alter_sys=True)
    finally:
        durable(root / (phase + "-imports.json"), {"before": before, "after": loaded_files(request)})


def guardian(root, phase, root_identity):
    """Durably identify before release; independently close work if owner dies."""
    bind_root(root, root_identity)
    owner = read_json(root / "owner.json")
    request = read_json(root / "request.json")
    interval = request["limits"]["interval_seconds"]
    identity = birth()
    require(os.getpgid(identity["pid"]) == identity["pid"], "Guardian must be a new session leader")
    path = root / (phase + "-child.json")
    durable(path, {"identity": identity, "known": [], "state": "waiting-for-owner"})
    known = {}
    budget = Budget(root, request, owner["started_monotonic"], lambda: [owner["identity"], *known.values()])
    proc = None
    result = {"state": "failed", "identity": identity, "known_closure": False}
    handlers = {}

    def interrupted(*_):
        # One-shot: a second signal must not interrupt cleanup or the terminal record.
        for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(number, signal.SIG_IGN)
        raise InterruptedError("Guardian interrupted")

    try:
        for number in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            handlers[number] = signal.signal(number, interrupted)
        await_release(
            root / (phase + "-release.json"), identity, lambda: matching(owner["identity"]), interval, budget.tick
        )
        with (root / (phase + ".log")).open("xb") as log:
            proc = subprocess.Popen(
                child_argv(
                    request, root, "_payload", str(root), phase, ":".join(map(str, root_identity)), str(identity["pid"])
                ),
                cwd=root,
                env={
                    "PATH": "/usr/bin:/bin",
                    "HOME": str(root),
                    "TMPDIR": str(root / "scratch"),
                    "GIT_OPTIONAL_LOCKS": "0",
                    "OMP_NUM_THREADS": "1",
                    "OPENBLAS_NUM_THREADS": "1",
                    "MKL_NUM_THREADS": "1",
                    "NUMEXPR_NUM_THREADS": "1",
                    "VECLIB_MAXIMUM_THREADS": "1",
                },
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            # The payload does no work until its birth is durable and it is released.
            known[proc.pid] = birth(proc.pid)
            recorded = dict(known)
            durable(path, {"identity": identity, "known": list(known.values()), "state": "payload-recorded"})
            durable(root / (phase + "-payload-release.json"), known[proc.pid])
            while True:
                members(identity, known)
                if known != recorded:
                    durable(path, {"identity": identity, "known": list(known.values()), "state": "running"})
                    recorded = dict(known)
                budget.tick(True)
                require(matching(owner["identity"]), "Owner disappeared during phase")
                if proc.poll() is not None:
                    require(not members(identity, known), "Remaining phase descendants")
                    require(proc.returncode == 0, "Phase failed: " + phase)
                    break
                time.sleep(interval)
        require((root / (phase + "-imports.json")).is_file(), "Missing payload import closure")
        result["state"] = "passed"
    except BaseException as error:
        result["error"] = type(error).__name__ + ": " + str(error)[:2000]
    finally:
        # The terminal record is always written; cleanup cannot be interrupted.
        for number in handlers:
            signal.signal(number, signal.SIG_IGN)
        remaining = list(known.values())
        try:
            # Discover and durably retain identities before any cleanup operation.
            remaining = close_members(identity, known, path)
            if proc:
                proc.wait(timeout=3)
        except BaseException as error:
            result["cleanup_error"] = type(error).__name__ + ": " + str(error)[:2000]
        remaining = [row for row in remaining if matching(row)]
        result.update(
            known=list(known.values()),
            remaining=remaining,
            known_closure=not remaining and "cleanup_error" not in result,
            last=budget.last,
            peaks=budget.peaks,
        )
        if "cleanup_error" in result:
            result["state"] = "failed"
        durable(root / (phase + "-terminal.json"), result)
    return 0 if result["state"] == "passed" else 1


def read_records(root, names):
    """Records by name: None when missing, {'unreadable': ...} when damaged."""
    records = {}
    for name in names:
        try:
            records[name] = read_json(Path(root) / (name + ".json"))
        except FileNotFoundError:
            records[name] = None
        except (ValueError, OSError) as error:
            records[name] = {"unreadable": str(error)}
    return records


def phase_record_names(phase):
    return [phase + "-" + kind for kind in PHASE_RECORDS]


def close_orphans(root, phase, notes):
    """The guardian is gone: close whatever it recorded, by exact birth match only.

    Nothing signals a group. Unrecorded live group members leave the run held.
    """
    records = read_records(root, phase_record_names(phase))
    if phase_closure(phase, records) is not None:
        return
    child = records.get(phase + "-child")
    recorded = [child["identity"], *child.get("known", [])] if isinstance(child, dict) and "identity" in child else []
    live = [row for row in recorded if matching(row)]
    remaining = terminate(live)
    notes[phase] = {
        "terminated": [row for row in live if row not in remaining],
        "remaining": remaining,
        "closure": phase_closure(phase, records),
    }


def run_phase(root, phase, request, budget, owner, candidate, notes):
    # Runner records the previous phase's payload could have written are re-verified
    # now, while no writer exists, and are never trusted from the payload side later.
    for kind in (*PHASE_RECORDS, "imports"):
        require(
            not (root / (phase + "-" + kind + ".json")).exists(), "Pre-existing phase record: " + phase + "-" + kind
        )
    require(read_json(root / "owner.json") == owner, "Runner record changed: owner.json")
    require(read_json(root / "candidate.json") == {"path": candidate}, "Runner record changed: candidate.json")
    for pin in owner["controls"]:
        verify_pin(pin, budget.tick)
    durable(root / (phase + "-launch.json"), {"phase": phase, "owner": birth()})
    with (root / (phase + "-guardian.log")).open("xb") as log:
        child = subprocess.Popen(
            child_argv(request, root, "_guardian", str(root), phase, ":".join(map(str, owner["root_identity"]))),
            start_new_session=True,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
    try:
        startup = root / (phase + "-child.json")
        while not startup.exists():
            require(child.poll() is None, "Guardian exited before startup record")
            budget.tick()
            time.sleep(request["limits"]["interval_seconds"])
        identity = read_json(startup)["identity"]
        require(identity == birth(child.pid), "Guardian startup identity differs")
        durable(root / (phase + "-release.json"), identity)
        while child.poll() is None:
            budget.tick()
            time.sleep(request["limits"]["interval_seconds"])
        terminal = read_json(root / (phase + "-terminal.json"))
        require(
            child.returncode == 0
            and terminal["state"] == "passed"
            and terminal["known_closure"]
            and terminal["identity"] == identity,
            "Phase did not close successfully: " + phase,
        )
        return terminal
    finally:
        # The guardian is our unreaped child, so its PID cannot have been reused.
        # Give its cleanup time to close the payload and write the terminal record.
        if child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=STOP_SECONDS)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()
        # A guardian lost without its terminal record must not leave its payload running.
        close_orphans(root, phase, notes)


def completion(root, request, budget, owner):
    """Only the owner seals a successful, closed build; payload markers are ignored."""
    check_pins(request, budget.tick)
    for pin in owner["controls"]:
        verify_pin(pin, budget.tick)
    candidate = root / "candidate"
    value = {
        "version": 1,
        "owner": owner,
        "binding": binding(request),
        "build_terminal": file_pin(root / "build-terminal.json", budget.tick),
        "candidate": str(candidate),
        "files": census(candidate, budget.tick),
    }
    require(read_json(root / "build-terminal.json")["known_closure"], "Build closure required")
    durable(root / "build-completion.json", value)
    return value


def reuse(pin, request, budget):
    verify_pin(pin, budget.tick)
    value = read_json(pin["path"])
    require(value["version"] == 1 and value["binding"] == binding(request), "Completion input/code/runtime drift")
    previous = Path(pin["path"]).parent
    require(Path(value["candidate"]) == previous / "candidate", "Candidate is not this attempt's own build")
    require(value["owner"] == read_json(previous / "owner.json"), "Completion owner differs")
    require(status(previous)["closed"], "Previous attempt is not closed; run spicy-regs-run reconcile")
    verify_pin(value["build_terminal"], budget.tick)
    terminal = read_json(value["build_terminal"]["path"])
    require(
        terminal["state"] == "passed"
        and terminal["known_closure"]
        and not matching(terminal["identity"])
        and not any(matching(item) for item in terminal["known"]),
        "Previous build not closed",
    )
    require(census(Path(value["candidate"]), budget.tick) == value["files"], "Closed build output drift")
    return value


def execute(request_path, grant_path):
    raw = Path(request_path).read_bytes()
    request, grant = json.loads(raw), read_json(grant_path)
    grant_check(raw, grant)  # Checked once, at admission.
    root = validate(request)
    require(not root.exists(), "Fresh run root required; existing attempts are never resumed")
    require(shutil.disk_usage(root.parent).free >= request["limits"]["start_free_bytes"], "Disk admission floor")
    root.mkdir(mode=0o700)
    root_identity = bind_root(root)
    durable(root / "request.json", request)
    durable(root / "grant.json", grant)
    owner = {
        "identity": birth(),
        "started": time.time(),
        "started_monotonic": time.monotonic(),
        "request_sha256": digest(raw),
        "root_identity": root_identity,
        "controls": [
            file_pin(Path(request_path).resolve()),
            file_pin(Path(grant_path).resolve()),
            file_pin(root / "request.json"),
            file_pin(root / "grant.json"),
        ],
    }
    durable(root / "owner.json", owner)
    (root / "scratch").mkdir()
    (root / "output").mkdir()
    budget = Budget(root, request, owner["started_monotonic"])
    notes: dict[str, object] = {}
    result: dict[str, object] = {
        "state": "failed",
        "run_root": str(root),
        "productionActions": [],
        "reused_build": request.get("continue_from"),
        "orphan_cleanup": notes,
    }
    try:
        check_pins(request, budget.tick)
        for pin in owner["controls"]:
            verify_pin(pin, budget.tick)
        if request.get("continue_from"):
            value = reuse(request["continue_from"], request, budget)
            candidate = value["candidate"]
            sealed = request["continue_from"]
            durable(root / "reused-build.json", sealed)
        else:
            candidate = str(root / "candidate")
        durable(root / "candidate.json", {"path": candidate})
        if not request.get("continue_from"):
            run_phase(root, "build", request, budget, owner, candidate, notes)
            value = completion(root, request, budget, owner)
            sealed = file_pin(root / "build-completion.json", budget.tick)
        run_phase(root, "audit", request, budget, owner, candidate, notes)
        require(census(Path(candidate), budget.tick) == value["files"], "Audit changed reusable candidate")
        check_pins(request, budget.tick)
        for pin in owner["controls"]:
            verify_pin(pin, budget.tick)
        # The candidate was hashed by the seal (or reuse) and by the check above;
        # it is listed here by its completion pin, not hashed again.
        durable(
            root / "output-files.json",
            {
                "files": census(root, budget.tick, skip=("candidate",)),
                "candidate": sealed,
                "excluded_final_records": ["output-files.json", "result.json"],
            },
        )
        budget.tick(True)
        result["state"] = "built-and-audited-awaiting-review"
    except BaseException as error:
        result["error"] = type(error).__name__ + ": " + str(error)[:2000]
    finally:
        result.update(
            last=budget.last,
            peaks=budget.peaks,
            phases_launched=[phase for phase in PHASES if (root / (phase + "-launch.json")).exists()],
            owner_hash_work={
                kind: {
                    "calls": sum(HASH_READS.get(pin["path"], {}).get("calls", 0) for pin in request[kind]),
                    "bytes": sum(HASH_READS.get(pin["path"], {}).get("bytes", 0) for pin in request[kind]),
                }
                for kind in ("inputs", "code", "runtime")
            },
            accounting="Root census names excluded final records; 65536 bytes reserved for final result; no self-hash claim",
            limitations=LIMITATIONS,
        )
        durable(root / "result.json", result)
    return result


def group_live(pgid):
    """Any non-zombie process still in this group. Observation only; never signal a group."""
    if pgid <= 1:
        return True
    for process in psutil.process_iter(["pid", "status"]):
        try:
            if process.info["status"] != psutil.STATUS_ZOMBIE and os.getpgid(process.pid) == pgid:
                return True
        except PermissionError:
            return True  # Cannot tell: held, not closed.
        except (ProcessLookupError, psutil.NoSuchProcess):
            pass
    return False


def phase_closure(phase, records):
    """Return why no process of this phase can still act, or None while that is unknown.

    POSIX keeps a process-group id reserved while any member lives, so an empty
    group for a dead recorded guardian means nothing from the phase survives.
    A reused id that now leads an unrelated group stays held: fail closed.
    """
    if records.get(phase + "-launch") is None:
        return "not-launched"
    terminal = records.get(phase + "-terminal")
    child = records.get(phase + "-child")
    # Only the guardian that has a durable child record can have written this terminal:
    # a record forged by an earlier phase's payload cannot know the future guardian's birth.
    if (
        isinstance(terminal, dict)
        and terminal.get("known_closure") is True
        and isinstance(child, dict)
        and "identity" in child
        and terminal.get("identity") == child["identity"]
    ):
        return "terminal-closed"
    identities = [child["identity"], *child.get("known", [])] if isinstance(child, dict) and "identity" in child else []
    if any(matching(row) for row in identities):
        return None
    if records.get(phase + "-release") is None:
        return "guardian-never-released"
    if not identities or group_live(child["identity"]["pid"]):
        return None
    return "recorded-processes-and-group-gone"


def status(root):
    """Read only. Missing or truncated records are uncertainty, never completion."""
    root = Path(root)
    records = read_records(root, (*RUN_RECORDS, *(name for phase in PHASES for name in phase_record_names(phase))))
    identities = []
    for phase in PHASES:
        row = records[phase + "-child"]
        if isinstance(row, dict) and "identity" in row:
            identities.extend([row["identity"], *row.get("known", [])])
    owner = records["owner"]
    owner_live = bool(matching(owner["identity"])) if isinstance(owner, dict) and "identity" in owner else None
    phases = {phase: phase_closure(phase, records) for phase in PHASES}
    return {
        "records": records,
        "live_known": [row for row in identities if matching(row)],
        "owner_live": owner_live,
        "phases": phases,
        "closed": owner_live is False and all(reason is not None for reason in phases.values()),
    }


def reconcile(root):
    """Explicit known-process cleanup. Never restart or infer missing success."""
    root = Path(root)
    owner = read_json(root / "owner.json")
    bind_root(root, owner["root_identity"])
    with (root / "reconcile.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise ValueError("Another reconcile holds this run") from None
        observation = status(root)
        require(observation["owner_live"] is False, "Owner is live or unresolved")
        terminate(observation["live_known"])
        observation = status(root)
        result: dict[str, object] = {
            "state": "closed-incomplete-run" if observation["closed"] else "held",
            "phases": observation["phases"],
            "live_known": observation["live_known"],
            "qualification": "not-established",
            "limitations": LIMITATIONS,
        }
        try:
            result["files"] = census(root)
        except (ValueError, OSError) as error:
            result["files_error"] = type(error).__name__ + ": " + str(error)[:2000]
        durable(root / "reconciliation.json", result)
        return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    start = commands.add_parser("run")
    start.add_argument("--request", required=True, type=Path)
    start.add_argument("--grant", required=True, type=Path)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("kind", choices=("h-annual", "d-cfr"))
    prepare.add_argument("--preparation", required=True, type=Path)
    prepare.add_argument("--runtime-inventory", required=True, type=Path)
    prepare.add_argument("--python", required=True)
    prepare.add_argument("--target", required=True, type=Path)
    prepare.add_argument("--run-root", required=True, type=Path)
    prepare.add_argument("--output", required=True, type=Path)
    for name in ("status", "reconcile"):
        commands.add_parser(name).add_argument("root", type=Path)
    for name in ("_guardian", "_payload"):
        internal = commands.add_parser(name)
        internal.add_argument("root", type=Path)
        internal.add_argument("phase", choices=PHASES)
        internal.add_argument("root_identity")
        if name == "_payload":
            internal.add_argument("guardian_pid", type=int)
    args = parser.parse_args(argv)
    if args.command == "_guardian":
        return guardian(args.root, args.phase, [int(value) for value in args.root_identity.split(":")])
    if args.command == "_payload":
        payload(args.root, args.phase, [int(value) for value in args.root_identity.split(":")], args.guardian_pid)
        return 0
    try:
        if args.command == "prepare":
            from spicy_regs.operational_prepare import prepare_request

            prepare_request(args)
            return 0
        result = execute(args.request, args.grant) if args.command == "run" else globals()[args.command](args.root)
    except (ValueError, OSError) as error:  # A refusal is an answer, not a crash: one line, exit 2.
        print("spicy-regs-run: " + str(error), file=sys.stderr)
        return 2
    print(json.dumps(result), flush=True)
    succeeded = {"run": "built-and-audited-awaiting-review", "reconcile": "closed-incomplete-run"}
    return 0 if args.command == "status" or result.get("state") == succeeded[args.command] else 1


if __name__ == "__main__":
    raise SystemExit(main())
