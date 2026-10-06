"""Run one disjoint file partition of the complete default pytest collection."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pytest


class Partition:
    def __init__(self, index: int, manifest_path: Path):
        self.index = index
        self.manifest_path = manifest_path

    @pytest.hookimpl(trylast=True)
    def pytest_collection_modifyitems(self, config, items):
        original = list(items)
        selected, deselected = [], []
        for item in original:
            file = item.nodeid.split("::", 1)[0]
            partition = int.from_bytes(hashlib.sha256(file.encode()).digest(), "big") % 2
            (selected if partition == self.index else deselected).append(item)
        manifest = {"partition": self.index,
                    "collectedNodeIds": [item.nodeid for item in original],
                    "selectedNodeIds": [item.nodeid for item in selected]}
        encoded = (json.dumps(manifest, sort_keys=True) + "\n").encode()
        self.manifest_path.write_bytes(encoded)
        print("CI_PARTITION " + json.dumps({"partition": self.index,
              "collected": len(original), "selected": len(selected),
              "manifest": str(self.manifest_path), "sha256": hashlib.sha256(encoded).hexdigest()},
              sort_keys=True), flush=True)
        if not selected:
            raise pytest.UsageError("CI partition has no selected tests")
        items[:] = selected
        config.hook.pytest_deselected(items=deselected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("partition", type=int, choices=(0, 1))
    parser.add_argument("--manifest", type=Path)
    args, pytest_args = parser.parse_known_args()
    manifest = args.manifest or Path(f"ci-partition-{args.partition}.json")
    return pytest.main(pytest_args, plugins=[Partition(args.partition, manifest)])


if __name__ == "__main__":
    raise SystemExit(main())
