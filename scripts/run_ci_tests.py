"""Partition default pytest tests; leaf-test-only PRs execute just changed files."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

PARTITION_COUNT = 4


def leaf_test_selection(event: str, base: str, head: str) -> tuple[list[str] | None, str]:
    """Select only independent offline test modules; uncertainty keeps the full suite."""
    if event != "pull_request":
        return None, "not a pull request"
    if not base or not head:
        return None, "missing diff revision"
    try:
        result = subprocess.run(
            ["git", "diff", "--name-status", "--no-renames", "-z", f"{base}...{head}"],
            check=True, capture_output=True, text=True,
        )
        fields = result.stdout.rstrip("\0").split("\0")
        if not result.stdout or len(fields) % 2:
            return None, "empty or ambiguous diff"
        paths = []
        for status, path in zip(fields[::2], fields[1::2], strict=True):
            if status not in {"A", "M"} or not re.fullmatch(r"tests/test_[A-Za-z0-9_]+\.py", path):
                return None, "change outside added or modified leaf tests"
            file = Path(path)
            if file.is_symlink() or not file.is_file() or "integration" in file.read_text():
                return None, "live test or unsupported test file"
            # Test modules also serve as shared helpers. Check both revisions so
            # removing an import does not hide that the old module was shared.
            for revision in (base, head):
                references = subprocess.run(
                    ["git", "grep", "-l", "-F", "-e", file.stem, revision, "--", "*.py"],
                    capture_output=True, text=True,
                )
                if references.returncode not in {0, 1}:
                    return None, "cannot determine test references"
                if any(name.partition(":")[2] != path for name in references.stdout.splitlines()):
                    return None, "test module is referenced by another Python file"
            paths.append(path)
        return sorted(paths), "only independent offline test modules changed"
    except (OSError, UnicodeError, subprocess.CalledProcessError):
        return None, "cannot determine changed tests"


class Partition:
    def __init__(self, index: int, manifest_path: Path, changed_files=None, reason="full collection"):
        self.index = index
        self.manifest_path = manifest_path
        self.changed_files = changed_files
        self.reason = reason
        self.empty_leaf_partition = False

    def pytest_collection_modifyitems(self, config, items):
        import pytest

        original = list(items)
        collected_files = {item.nodeid.split("::", 1)[0] for item in original}
        if self.changed_files is not None and not set(self.changed_files) <= collected_files:
            self.changed_files = None
            self.reason = "changed file has no default tests; using full collection"
        selected, deselected = [], []
        for item in original:
            file = item.nodeid.split("::", 1)[0]
            partition = int.from_bytes(hashlib.sha256(file.encode()).digest(), "big") % PARTITION_COUNT
            affected = self.changed_files is None or file in self.changed_files
            (selected if affected and partition == self.index else deselected).append(item)
        manifest = {"partition": self.index,
                    "partitionCount": PARTITION_COUNT,
                    "selectionMode": "leaf-tests" if self.changed_files is not None else "full",
                    "selectionReason": self.reason,
                    "changedTestFiles": self.changed_files,
                    "collectedNodeIds": [item.nodeid for item in original],
                    "selectedNodeIds": [item.nodeid for item in selected]}
        encoded = (json.dumps(manifest, sort_keys=True) + "\n").encode()
        self.manifest_path.write_bytes(encoded)
        print("CI_PARTITION " + json.dumps({"partition": self.index,
              "partitionCount": PARTITION_COUNT,
              "collected": len(original), "selected": len(selected),
              "manifest": str(self.manifest_path), "sha256": hashlib.sha256(encoded).hexdigest()},
              sort_keys=True), flush=True)
        if not selected and self.changed_files is None:
            raise pytest.UsageError("CI partition has no selected tests")
        self.empty_leaf_partition = not selected
        items[:] = selected
        config.hook.pytest_deselected(items=deselected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("partition", type=int, choices=range(PARTITION_COUNT), nargs="?")
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--leaf-test-only", action="store_true",
                        help="Exit successfully only for an independent offline-test-only PR (no pytest import).")
    args, pytest_args = parser.parse_known_args()
    changed_files, reason = leaf_test_selection(
        os.environ.get("GITHUB_EVENT_NAME", ""), os.environ.get("CHANGE_BASE", ""), os.environ.get("CHANGE_HEAD", ""),
    )
    if args.leaf_test_only:
        print(reason, flush=True)
        return 0 if changed_files is not None else 1
    if args.partition is None:
        parser.error("partition is required")
    import pytest

    manifest = args.manifest or Path(f"ci-partition-{args.partition}.json")
    pytest.hookimpl(trylast=True)(Partition.pytest_collection_modifyitems)
    partition = Partition(args.partition, manifest, changed_files, reason)
    result = pytest.main(["--maxfail=1", *pytest_args], plugins=[partition])
    if result == pytest.ExitCode.NO_TESTS_COLLECTED and partition.empty_leaf_partition:
        return 0
    return result


if __name__ == "__main__":
    raise SystemExit(main())
