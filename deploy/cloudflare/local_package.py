"""Build one reviewed Regs wheel and check it in a private frozen offline runtime."""

import argparse
from datetime import UTC, datetime
import json
import os
from pathlib import Path
import subprocess
import time
import zipfile

from local_delivery import ROOT, save, sha, source
from qualify_reader import read_expected


def package(config, output):
    source(config["source"])
    check = config["check"]
    read_expected(Path(check["path"]), check["sha256"])
    python = Path(config["python"]).absolute()
    if not python.is_file():
        raise ValueError("Build interpreter is unavailable")
    uv = Path(config["uv"])
    if not uv.is_absolute() or not uv.is_file() or not os.access(uv, os.X_OK):
        raise ValueError("uv must name an absolute executable file")
    uv = uv.resolve()
    version = subprocess.run(
        [str(uv), "--version"], check=True, capture_output=True, text=True, timeout=10
    ).stdout.strip()
    if not version.startswith("uv "):
        raise ValueError("Selected executable did not identify itself as uv")
    watched = [
        python.resolve(), python.parent.parent / "pyvenv.cfg",
        *python.parent.parent.glob("lib/python*/site-packages"),
    ]

    def runtime_signature():
        return {
            str(path): [getattr(path.stat(), field) for field in ("st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns")]
            for path in watched if path.exists()
        }

    before = runtime_signature()
    output = output.absolute()
    output.mkdir()  # Refuse previous, partial or successful output; never replace it.
    private = output / "venv"
    temporary = output / "tmp"
    temporary.mkdir()
    env = {key: value for key, value in os.environ.items() if key not in {"PYTHONPATH", "PYTHONHOME"}}
    env.update(
        PYTHONDONTWRITEBYTECODE="1", UV_OFFLINE="1", UV_PYTHON_DOWNLOADS="never",
        TMPDIR=str(temporary), VIRTUAL_ENV=str(private), UV_PROJECT_ENVIRONMENT=str(private),
    )
    lock = sha(ROOT / "uv.lock")
    started = time.monotonic()
    commands = []

    def run(argv, name, cwd=ROOT):
        command = list(map(str, argv))
        begin = time.monotonic()
        with (output / f"{name}.log").open("xb") as log, (output / f"{name}.stderr.log").open("xb") as errors:
            result = subprocess.run(
                command, cwd=cwd, env=env, stdout=log, stderr=errors, timeout=180
            )
        commands.append({
            "command": command, "cwd": str(cwd), "seconds": time.monotonic() - begin,
            "exit_code": result.returncode,
        })
        if result.returncode:
            raise RuntimeError(f"{name} failed; see its retained logs")

    save(output / "inputs.json", config)
    save(output / "runtime-before.json", before)
    terminal = {
        "status": "fail", "started_at": datetime.now(UTC).isoformat(), "source": config["source"],
        "uv": {"executable": str(uv), "version": version},
    }
    try:
        run([uv, "build", "--wheel", "--offline", "--python", python, "--out-dir", output / "wheels", ROOT], "build")
        wheel, = (output / "wheels").glob("spicy_regs-*.whl")
        # Check the build artifact against the reviewed package, not a hand-maintained module list.
        package_root = ROOT / "src/spicy_regs"
        members = {
            str(path.relative_to(ROOT / "src")): path
            for path in package_root.rglob("*")
            if path.is_file() and path.suffix in {".py", ".json", ".xsd"}
        }
        with zipfile.ZipFile(wheel) as archive:
            actual = {
                name for name in archive.namelist()
                if name.startswith("spicy_regs/") and Path(name).suffix in {".py", ".json", ".xsd"}
            }
            if actual != set(members):
                raise ValueError("Wheel package membership differs")
            for name, path in members.items():
                if archive.read(name) != path.read_bytes():
                    raise ValueError(f"Wheel package member differs: {name}")
        run([uv, "sync", "--frozen", "--offline", "--no-dev", "--no-install-project", "--python", python], "sync")
        runtime = private / "bin/python"
        run([uv, "pip", "install", "--offline", "--no-deps", "--python", runtime, wheel], "install")
        run([
            runtime, "-I", "-B", Path(__file__).with_name("qualify_reader.py"),
            "--expected", check["path"], "--sha256", check["sha256"],
        ], "installed-check", cwd=output)
        qualification = json.loads((output / "installed-check.log").read_bytes())
        if qualification["status"] != "pass":
            raise ValueError("Installed qualification did not pass")
        source(config["source"])
        if sha(ROOT / "uv.lock") != lock:
            raise ValueError("Frozen lock changed")
        after = runtime_signature()
        save(output / "runtime-after.json", after)
        if before != after:
            raise ValueError("Build interpreter/runtime files changed")
        terminal.update(
            status="pass", wheel={"path": str(wheel), "sha256": sha(wheel), "bytes": wheel.stat().st_size},
            uv_lock_sha256=lock, installed_result_sha256=sha(output / "installed-check.log"),
            consumer=qualification["consumer"], package_members_checked=len(members),
            shared_runtime_signature_unchanged=True,
            output_logical_file_bytes=sum(
                item.stat().st_size for item in output.rglob("*") if item.is_file() and not item.is_symlink()
            ),
        )
    except BaseException as error:
        terminal["failure_type"] = type(error).__name__
        raise
    finally:
        terminal.update(finished_at=datetime.now(UTC).isoformat(), elapsed_seconds=time.monotonic() - started)
        save(output / "commands.json", commands)
        save(output / "terminal.json", terminal)
    return terminal


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inputs", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = package(json.loads(args.inputs.read_bytes()), args.output)
    print(json.dumps(result))


if __name__ == "__main__":
    main()
