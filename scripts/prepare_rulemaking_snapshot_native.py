#!/usr/bin/env python3
"""Prepare a fully pinned native rulemaking snapshot locally; never publish it."""
import argparse
from pathlib import Path

from spicy_regs.rulemaking_migration import prepare_snapshot


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('pointer', 'manifest', 'sources', 'destination'):
        parser.add_argument('--' + name, type=Path, required=True)
    for name in ('expected-pointer-sha256', 'expected-manifest-sha256', 'generation-id', 'asserted-at'):
        parser.add_argument('--' + name, required=True)
    args = parser.parse_args()
    print(prepare_snapshot(**vars(args)))


if __name__ == '__main__':
    main()
