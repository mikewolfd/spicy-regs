"""The server image installs uv.lock's versions of every runtime package, and its build checks that it did.

Round 6 found the container running pydantic 2.13 while the lock and every test ran 2.12.5: the Dockerfile pinned
mcp and duckdb and left their dependencies to pip (corpora/mcp-chaos-2026-10-02/round6/phase3-review.md, item 1).
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).parents[1]
DOCKERFILE = ROOT / "deploy/cloudflare/Dockerfile"


def _lock_pins():
    spec = importlib.util.spec_from_file_location("lock_pins", ROOT / "deploy/cloudflare/lock_pins.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_the_image_installs_its_runtime_under_the_locks_constraints_and_checks_the_result():
    """Every pip install in the image is constrained by the lock or installs the project alone, and the build checks."""
    commands = " ".join(DOCKERFILE.read_text(encoding="utf-8").replace("\\\n", " ").splitlines())
    installs = re.findall(r"pip install ([^&;]*)", commands)
    assert installs, "the image installs nothing with pip"
    for arguments in installs:
        assert "-c " in arguments or arguments.split()[-1] == ".", arguments
        # A version or extra named here would bypass the lock: an extra's packages are not locked.
        assert not re.search(r"[=<>\[]", arguments.replace("-c ", "")), arguments
    assert "lock_pins.py check" in commands


def test_the_constraints_are_the_locked_versions_of_the_validation_packages():
    pins = dict(line.split("==") for line in _lock_pins().constraints(ROOT / "uv.lock").splitlines())
    assert (pins["pydantic"], pins["pydantic-core"], pins["mcp"], pins["duckdb"]) == ("2.12.5", "2.41.5", "2.2.0", "1.5.5")


def test_an_environment_installed_from_the_lock_passes_the_check():
    """This test environment is uv's frozen install of the same lock, so the image's check must accept it."""
    module = _lock_pins()
    assert module.mismatches(module.locked(ROOT / "uv.lock"), module.installed()) == []


def test_the_check_names_a_package_at_another_version_and_one_the_lock_does_not_hold():
    module = _lock_pins()
    installed = {"pydantic": "2.13.0", "uvloop": "0.21.0", "pip": "25.0", "mcp": "2.2.0"}
    assert module.mismatches(module.locked(ROOT / "uv.lock"), installed) == [
        "pydantic 2.13.0 is installed; uv.lock holds 2.12.5",
        "uvloop 0.21.0 is installed, and uv.lock does not hold it",
    ]
