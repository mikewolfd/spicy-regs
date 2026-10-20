"""Refuse mismatched package/check inputs before running build or pipeline work."""

import hashlib
import importlib
import json
from pathlib import Path
import subprocess
import zipfile

import pytest


@pytest.fixture
def helpers(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parents[1] / "deploy/cloudflare"))
    return importlib.import_module("local_package"), importlib.import_module("qualify_reader")


def test_source_mismatch_does_not_create_package_output(tmp_path, helpers):
    package, _ = helpers
    output = tmp_path / "package"
    with pytest.raises(ValueError, match="reviewed source"):
        package.package({"source": {"commit": "0" * 40, "tree": "0" * 40}}, output)
    assert not output.exists()


def test_expectation_digest_refuses_before_parsing(tmp_path, helpers):
    _, reader = helpers
    path = tmp_path / "expected.json"
    path.write_text("not JSON")
    with pytest.raises(ValueError, match="expectations changed"):
        reader.read_expected(path, "0" * 64)


def test_reader_check_requires_consumer_identity(tmp_path, helpers):
    _, reader = helpers
    path = tmp_path / "expected.json"
    path.write_text(json.dumps({
        "generation": {"directory": str(tmp_path), "logicalId": "id", "artifactDigest": "digest"},
        "queries": [{"sql": "SELECT 1", "columns": ["1"], "rows": [[1]]}],
    }))
    with pytest.raises(ValueError, match="consumer identity"):
        reader.read_expected(path, hashlib.sha256(path.read_bytes()).hexdigest())


def test_pipeline_check_refuses_existing_generation(tmp_path, helpers):
    _, reader = helpers
    path = tmp_path / "expected.json"
    path.write_text(json.dumps({
        "pipeline": {"module": "unused", "argv": []},
        "generation": {"directory": str(tmp_path)},
        "queries": [{"sql": "SELECT 1", "columns": ["1"], "rows": [[1]]}],
    }))
    with pytest.raises(ValueError, match="new generation directory"):
        reader.read_expected(path, hashlib.sha256(path.read_bytes()).hexdigest())


def test_explicit_uv_overrides_old_path_and_retains_commands(tmp_path, helpers, monkeypatch):
    package, _ = helpers
    root = tmp_path / "source"
    module = root / "src/spicy_regs/__init__.py"
    module.parent.mkdir(parents=True)
    module.write_text('"""Fixture package."""\n')
    (root / "uv.lock").write_text("fixture lock\n")
    interpreter = tmp_path / "python"
    interpreter.touch()
    expected = tmp_path / "expected.json"
    expected.write_text(json.dumps({
        "pipeline": {"module": "unused", "argv": []},
        "generation": {"directory": str(tmp_path / "generated")},
        "queries": [{"sql": "SELECT 1", "columns": ["1"], "rows": [[1]]}],
    }))
    output = tmp_path / "package"
    monkeypatch.setattr(package, "ROOT", root)
    monkeypatch.setattr(package, "source", lambda _value: None)
    old_bin = tmp_path / "old-bin"
    old_bin.mkdir()
    old_uv = old_bin / "uv"
    old_uv.write_text("#!/bin/sh\nprintf 'uv 0.6.10\\n'\n")
    old_uv.chmod(0o700)
    monkeypatch.setenv("PATH", str(old_bin))
    uv = tmp_path / "selected-uv"
    uv.write_text("#!/bin/sh\nprintf 'uv 0.11.21\\n'\n")
    uv.chmod(0o700)
    selected_uv = tmp_path / "uv-link"
    selected_uv.symlink_to(uv)
    run = subprocess.run

    def successful_command(argv, **kwargs):
        if argv[1] == "--version":
            return run(argv, **kwargs)
        if argv[1] == "build":
            wheels = output / "wheels"
            wheels.mkdir()
            with zipfile.ZipFile(wheels / "spicy_regs-0.0.0-py3-none-any.whl", "w") as archive:
                archive.write(module, "spicy_regs/__init__.py")
        elif argv[1] == "-I":
            kwargs["stdout"].write(b'{"status":"pass","consumer":{"fixture":true}}')
            kwargs["stderr"].write(b"Pipeline diagnostic\n")
        return subprocess.CompletedProcess(argv, 0)

    monkeypatch.setattr(package.subprocess, "run", successful_command)
    result = package.package({
        "source": {"commit": "fixture", "tree": "fixture"},
        "python": str(interpreter),
        "uv": str(selected_uv),
        "check": {"path": str(expected), "sha256": hashlib.sha256(expected.read_bytes()).hexdigest()},
    }, output)
    assert result["status"] == "pass"
    commands = json.loads((output / "commands.json").read_bytes())
    assert len(commands) == 4
    assert all(command["command"][0] == str(uv) for command in commands[:3])
    assert "--offline" in commands[0]["command"]
    assert {"--frozen", "--offline", "--no-install-project"} <= set(commands[1]["command"])
    assert {"--offline", "--no-deps"} <= set(commands[2]["command"])
    terminal = json.loads((output / "terminal.json").read_bytes())
    assert terminal["status"] == "pass"
    assert terminal["uv"] == {"executable": str(uv), "version": "uv 0.11.21"}
    assert (output / "installed-check.stderr.log").read_text() == "Pipeline diagnostic\n"


@pytest.mark.parametrize("kind", ["relative", "missing", "not_executable"])
def test_invalid_explicit_uv_refuses_before_build(tmp_path, helpers, monkeypatch, kind):
    package, _ = helpers
    monkeypatch.setattr(package, "source", lambda _value: None)
    monkeypatch.setattr(package, "read_expected", lambda *_args: None)
    interpreter = tmp_path / "python"
    interpreter.touch()
    uv = tmp_path / "uv"
    if kind == "not_executable":
        uv.touch(mode=0o600)
    output = tmp_path / "package"

    def unexpected_command(*_args, **_kwargs):
        pytest.fail("Invalid executable must refuse before any command")

    monkeypatch.setattr(package.subprocess, "run", unexpected_command)
    with pytest.raises(ValueError, match="absolute executable file"):
        package.package({
            "source": {},
            "python": str(interpreter),
            "uv": "uv" if kind == "relative" else str(uv),
            "check": {"path": str(tmp_path / "expected"), "sha256": "unused"},
        }, output)
    assert not output.exists()
