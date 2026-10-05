"""uv.lock's versions for the server image: pip constraints from the lock, and a check of what is installed.

    python lock_pins.py constraints uv.lock > constraints.txt
    python lock_pins.py check uv.lock

The image installs a few root packages with the constraints, so every package pip resolves for them is the locked
version, then ``check`` refuses any installed package the lock does not hold or holds at another version (pip, the
installer the base image brings, excepted). It cannot see a package the server needs and the image never installed:
nothing is installed to differ. The Dockerfile's import step covers that. Standard library only, because it runs in
the image.
"""

from __future__ import annotations

import re
import sys
import tomllib
from importlib import metadata
from pathlib import Path

#: Installed by the base image to install everything else; never imported by the server.
INSTALLER = frozenset({"pip"})


def _name(raw: str) -> str:
    """A distribution name in the normalized form uv.lock writes (PEP 503)."""
    return re.sub(r"[-_.]+", "-", raw).lower()


def _packages(lock: Path) -> list[dict]:
    with lock.open("rb") as stream:
        return tomllib.load(stream)["package"]


def locked(lock: Path) -> dict[str, str]:
    """Every package the lock holds, the project itself included, with its version."""
    return {_name(package["name"]): package["version"] for package in _packages(lock)}


def constraints(lock: Path) -> str:
    """One ``name==version`` line per package the lock takes from a registry, for ``pip install -c``."""
    return "".join(f"{_name(package['name'])}=={package['version']}\n"
                   for package in _packages(lock) if "registry" in package["source"])


def installed() -> dict[str, str]:
    """Every distribution installed in this interpreter, with its version."""
    return {_name(distribution.metadata["Name"]): distribution.version for distribution in metadata.distributions()}


def mismatches(lock: dict[str, str], present: dict[str, str]) -> list[str]:
    """Each installed package the lock does not hold, or holds at another version, in plain words."""
    problems = []
    for name, version in sorted(present.items()):
        if name in INSTALLER:
            continue
        if name not in lock:
            problems.append(f"{name} {version} is installed, and uv.lock does not hold it")
        elif lock[name] != version:
            problems.append(f"{name} {version} is installed; uv.lock holds {lock[name]}")
    return problems


def main(argv: list[str]) -> int:
    command, lock = argv[1], Path(argv[2])
    if command == "constraints":
        sys.stdout.write(constraints(lock))
        return 0
    problems = mismatches(locked(lock), installed())
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
