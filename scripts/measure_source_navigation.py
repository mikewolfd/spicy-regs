#!/usr/bin/env python3
"""Measure named canonical routes against explicit pinned local main members.

This does not download data, read receipts, update baselines or publish metadata.
The members JSON maps exact public member paths to already-held local files.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.navigation_measurement_runner import run_measurements
from spicy_regs.navigation_measurements import MeasurementCache


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publication', type=Path, required=True)
    parser.add_argument('--members', type=Path, required=True)
    parser.add_argument('--extra-tables', type=Path)
    parser.add_argument('--cache', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--route', action='append', default=[], help='Canonical navigation ID or scalar Join.name; repeatable')
    parser.add_argument('--affected-table', action='append', default=[], help='Measure routes whose source or target is this table; repeatable')
    parser.add_argument('--all', action='store_true', help='Inventory and attempt every canonical table route, within explicit limits')
    parser.add_argument('--max-input-bytes', type=int, default=256 * 1024**2)
    parser.add_argument('--max-projected-bytes', type=int, default=32 * 1024**2)
    parser.add_argument('--max-route-input-bytes', type=int, default=256 * 1024**2,
                        help='Total selected member bytes per route, checked before reading files')
    args = parser.parse_args(argv)
    index, paths = json.loads(args.publication.read_text()), json.loads(args.members.read_text())
    extras = json.loads(args.extra_tables.read_text()) if args.extra_tables else {}
    cache = MeasurementCache(args.cache, max_input_bytes=args.max_input_bytes, max_projected_bytes=args.max_projected_bytes)
    try:
        result = run_measurements(index, paths, cache, routes=args.route,
                                  affected_tables=args.affected_table, all_routes=args.all,
                                  extra_tables=extras, max_route_input_bytes=args.max_route_input_bytes)
    except ValueError as error:
        parser.error(str(error))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    return 0 if not result['unavailable'] else 2


if __name__ == '__main__':
    raise SystemExit(main())
