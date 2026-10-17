"""Shared constructors for the maintained-runner tests.

Every process here is a task-owned subprocess; nothing signals or inspects an
unrelated worker.
"""

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys
import time

from spicy_regs import operational_runner as runner

REPO = Path(__file__).resolve().parents[1]
SCRIPT = Path(runner.__file__).resolve()
# Pinned as code; everything else the phase opens (including checkout files the builder's recipe reads) is runtime.
FIRST_PARTY = (REPO / "src", REPO / "scripts")
ENV = {**os.environ, "PYTHONPATH": str(REPO / "src"), "PYTHONDONTWRITEBYTECODE": "1"}

# Same launcher flags as the runner's payload. The audit hook records every
# file the code opens, which is what an offline guard would later require pinned.
OBSERVE = """
import json, runpy, sys
from pathlib import Path
opened = set()
def hook(event, args):
    if event == 'open' and isinstance(args[0], (str, bytes)):
        opened.add(args[0] if isinstance(args[0], str) else args[0].decode(errors='surrogateescape'))
sys.addaudithook(hook)
runner, paths, code = sys.argv[1:4]
runpy.run_path(runner, run_name='inventory_only')
sys.path[:0] = json.loads(paths)
exec(code, {'__name__': 'observed'})
found = set()
for name in opened:
    try:
        if Path(name).is_file():
            found.add(str(Path(name).resolve()))
    except OSError:
        pass
for module in list(sys.modules.values()):
    file = getattr(module, '__file__', None)
    if file and not file.startswith('<'):
        found.add(str(Path(file).resolve()))
print(json.dumps(sorted(found)))
"""


def observed_files(code, paths, workdir):
    """Resolved regular files that `code` opened or imported under the payload's launcher flags."""
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-B",
            "-X",
            "pycache_prefix=" + str(workdir / "pyc"),
            "-c",
            OBSERVE,
            str(SCRIPT),
            json.dumps([str(path) for path in paths]),
            code,
        ],
        cwd=workdir,
        env=ENV,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout.splitlines()[-1])


def inventory(files, exclude):
    """A runtime inventory: every observed file not under an excluded root."""
    roots = [Path(path).resolve() for path in exclude]
    return {
        "files": [
            runner.file_pin(Path(path)) for path in files if not any(Path(path).is_relative_to(root) for root in roots)
        ]
    }


def grant(request_path, minutes=5):
    path = Path(request_path).with_suffix(".grant.json")
    path.write_text(
        json.dumps(
            {
                "authorized": True,
                "request_sha256": runner.digest(Path(request_path).read_bytes()),
                "expires_at": (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat(),
            }
        )
    )
    return path


def request_files(tmp_path, request):
    path = tmp_path / (Path(request["run_root"]).name + "-request.json")
    path.write_text(json.dumps(request))
    return path, grant(path)


def launch(tmp_path, request, *, fault=None):
    path, grant_path = request_files(tmp_path, request)
    if fault:
        code = """
import sys, time
from pathlib import Path
from spicy_regs import operational_runner as r
original = r.durable
def durable(path, value):
    if Path(path).name == sys.argv[3]: time.sleep(20)
    original(path, value)
r.durable = durable
r.execute(Path(sys.argv[1]), Path(sys.argv[2]))
"""
        return subprocess.Popen(
            [sys.executable, "-B", "-c", code, str(path), str(grant_path), fault],
            env=ENV,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
    return subprocess.Popen(
        [
            sys.executable,
            "-B",
            "-m",
            "spicy_regs.operational_runner",
            "run",
            "--request",
            str(path),
            "--grant",
            str(grant_path),
        ],
        env=ENV,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def finished(proc, root, timeout=20):
    out, err = proc.communicate(timeout=timeout)
    result = runner.read_json(root / "result.json")
    logs = "\n".join(p.read_text() for p in root.glob("*.log"))
    assert proc.returncode == 0, out + err + logs + json.dumps(result)
    return result


def wait_for(path, seconds=15):
    until = time.monotonic() + seconds
    while not path.exists() and time.monotonic() < until:
        time.sleep(0.02)
    assert path.exists(), str(path)


def cfr_capture(directory, make_body):
    """A complete 5-body/6-context CFR capture config with its custody chain, written under `directory`.

    Four gzip bodies (`make_body(i)`) and one identity 404 body. Six contexts
    observe bodies [0, 1, 2, 3, 3, 4]. Capture records alternate between the two
    retained field spellings, and the 404 record uses the `status` alias.
    """
    import gzip

    from spicy_regs import cfr_native_audit as audit

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)

    def pin(name, raw):
        path = directory / name
        path.write_bytes(raw)
        return {"path": str(path), "bytes": len(raw), "sha256": audit.digest(raw)}

    bodies, digests = {}, {}
    for index in range(5):
        raw = make_body(index) if index < 4 else b"<html>404</html>"
        compressed = index < 4
        stored = pin(f"body{index}", gzip.compress(raw, mtime=0) if compressed else raw)
        digests[str(index)] = audit.digest(raw)
        bodies[str(index)] = {
            "encoding": "gzip" if compressed else "identity",
            "decoded_sha256": digests[str(index)],
            "decoded_bytes": len(raw),
            "stored": stored,
            "aliases": [{"manifest": "fixture-manifest", "selector": f"/files/{index}"}],
        }
    contexts = []
    for number, key in enumerate(["0", "1", "2", "3", "3", "4"]):
        failure, spec = key == "4", bodies[key]
        url = f"https://example.test/annual/{number}"
        fields = {
            "content_type": "text/html" if failure else "application/xml",
            "observed_at": "fixture-time",
            "sha256": digests[key][len("sha256:") :] if number % 2 else digests[key],
        }
        if number % 2 == 0 and not failure:
            fields.update(requested_url=url, resolved_url=url, status_code=200, byte_size=spec["decoded_bytes"])
        else:
            fields.update(url=url, final_url=url, bytes=spec["decoded_bytes"])
            fields["status" if failure else "http_status"] = 404 if failure else 200
        contexts.append(
            {
                "id": str(number),
                "body": key,
                "kind": "failure" if failure else "annual",
                "year": 2023 if number == 4 else 2025,
                "expected_final_url": url,
                "capture": pin(f"record{number}", json.dumps(fields).encode()),
            }
        )
    objects = [
        {
            "stored": spec["stored"],
            "decoded_sha256": spec["decoded_sha256"],
            "decoded_bytes": spec["decoded_bytes"],
            "all_manifest_aliases": spec["aliases"],
        }
        for spec in bodies.values()
        if spec["encoding"] == "gzip"
    ]
    handoff = pin("handoff.json", json.dumps({"fixture": True}).encode())
    result = pin("result.json", json.dumps({"gzip_objects": objects}).encode())
    acceptance = pin(
        "acceptance.json",
        json.dumps(
            {"state": "accepted-exact-current-stored-decoded-original-custody", "handoff": handoff, "result": result}
        ).encode(),
    )
    return {
        "bodies": bodies,
        "contexts": contexts,
        "comparisons": [["0", "1"], ["3", "4"]],
        "custody_acceptance": [acceptance, handoff, result],
    }
