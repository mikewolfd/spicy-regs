"""Run one disjoint file partition of the complete default pytest collection."""
from __future__ import annotations

import argparse
import hashlib
import json

import pytest


class Partition:
    def __init__(self, index: int):
        self.index = index

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
        print("CI_PARTITION " + json.dumps(manifest, sort_keys=True))
        if not selected:
            raise pytest.UsageError("CI partition has no selected tests")
        items[:] = selected
        config.hook.pytest_deselected(items=deselected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("partition", type=int, choices=(0, 1))
    args, pytest_args = parser.parse_known_args()
    return pytest.main(pytest_args, plugins=[Partition(args.partition)])


if __name__ == "__main__":
    raise SystemExit(main())
