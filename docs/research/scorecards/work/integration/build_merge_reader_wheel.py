"""Add scorecard readers to the newer consumer reader without dropping its fixes.

Reuses the reviewed scorecard overlay recipe, rebased onto the exact provider
commit used by the current consumer wheel. Both builds must be byte-identical.
This creates a local package candidate; consumer validation remains separate.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import sys

BASELINE = "a1b91ce28a2ac254d4f2301e5abb48a025dac198"
COMMON_BASE = "db313482663ae7f6eee04bbed9150766a70e2ce8"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", type=Path, required=True)
    parser.add_argument("--baseline-wheel", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    spec = importlib.util.spec_from_file_location("scorecard_recipe", Path(__file__).with_name("build_reader_wheel.py"))
    assert spec and spec.loader
    recipe = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(recipe)
    setattr(recipe, "BASELINE", BASELINE)
    uv_version = subprocess.check_output(["uv", "--version"], text=True).strip()
    if uv_version.split()[:2] != ["uv", recipe.BUILD_UV] or sys.version_info[:2] != (3, 12):
        raise ValueError("Use the qualified Python 3.12 and uv 0.11.21 build tools")
    repo, output = args.provider.resolve(), args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    files = recipe.overlay(repo)
    path = "src/spicy_docs/extraction/__init__.py"
    # Both branches add adjacent imports, so retain the exact current-consumer
    # derivation additions explicitly rather than choose one side of a conflict.
    base = subprocess.check_output(["git", "show", f"{COMMON_BASE}:{path}"], cwd=repo).decode()
    consumer = subprocess.check_output(["git", "show", f"{BASELINE}:{path}"], cwd=repo).decode()
    additions = (
        "from .derivation import BODY_TEXT_DERIVATION_VERSION, derivation_code_digest\n",
        '    "BODY_TEXT_DERIVATION_VERSION",\n',
        '    "derivation_code_digest",\n',
    )
    original = consumer
    for addition in additions:
        if consumer.count(addition) != 1:
            raise ValueError("Review changed current-consumer extraction exports")
        consumer = consumer.replace(addition, "", 1)
    if consumer != base:
        raise ValueError("Current consumer extraction exports have unreviewed changes")
    merged = files[path].decode()
    anchor = "from .body_text import BodyText, BodyTextError, RenditionCleanup, body_text, rendition_text\n"
    if merged.count(anchor) != 1 or merged.count("__all__ = [\n") != 1:
        raise ValueError("Review changed scorecard extraction exports")
    merged = merged.replace(anchor, anchor + additions[0], 1)
    merged = merged.replace("__all__ = [\n", "__all__ = [\n" + additions[1], 1)
    merged = merged.replace('    "body_text",\n', '    "body_text",\n' + additions[2], 1)
    if any(line not in merged for line in original.splitlines() if line.strip()):
        raise ValueError("A current-consumer extraction export was removed")
    files[path] = merged.encode()
    hashes = {name: recipe.sha(raw) for name, raw in sorted(files.items())}
    digest = recipe.sha(json.dumps(hashes, sort_keys=True, separators=(",", ":")).encode())
    version = "0.54.0+scorecards." + digest[:12]
    first = recipe.build(repo, output / "first", files, version)
    second = recipe.build(repo, output / "second", files, version)
    if first.read_bytes() != second.read_bytes():
        raise ValueError("Independent builds differ")
    changed = recipe.compare_packages(args.baseline_wheel, first, files)
    shutil.copy2(first, output / first.name)
    receipt = {
        "build_environment": {"uv": uv_version, "python": sys.version, "backend": "uv_build==" + recipe.BUILD_UV},
        "baseline_commit": BASELINE,
        "baseline_wheel_sha256": recipe.sha(args.baseline_wheel.read_bytes()),
        "version": version,
        "overlay_sha256": digest,
        "overlay_files": hashes,
        "shared_reconciliation": {
            "extraction_init": "Verified current-consumer derivation additions retained alongside new extraction exports.",
            "schema_registry": "Current consumer baseline registry plus scorecard tables; newer native tables retained.",
        },
        "wheel": first.name,
        "wheel_sha256": recipe.sha(first.read_bytes()),
        "wheel_bytes": first.stat().st_size,
        "repeat_build_identical": True,
        "changed_package_files": changed,
        "scope": "Current consumer reader retained outside the explicit scorecard/extraction overlay; no deployment.",
    }
    (output / "wheel.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(json.dumps({k: receipt[k] for k in ("version", "wheel", "wheel_sha256", "repeat_build_identical")}, indent=2))


if __name__ == "__main__":
    main()
