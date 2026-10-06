#!/usr/bin/env python3
"""Measure named canonical routes against explicit pinned local main members.

This does not download data, read receipts, update baselines or publish metadata.
The members JSON maps exact public member paths to already-held local files.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from spicy_regs.explorer_metadata import read_json
from spicy_regs.navigation_measurements import MeasurementCache, scalar_navigation
from spicy_regs.subject_catalog import descriptors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--publication', type=Path, required=True)
    parser.add_argument('--members', type=Path, required=True)
    parser.add_argument('--extra-tables', type=Path)
    parser.add_argument('--cache', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--route', action='append', required=True, help='Canonical navigation ID or scalar Join.name; repeatable')
    parser.add_argument('--max-input-bytes', type=int, default=256 * 1024**2)
    parser.add_argument('--max-projected-bytes', type=int, default=32 * 1024**2)
    args = parser.parse_args()
    index, paths = json.loads(args.publication.read_text()), json.loads(args.members.read_text())
    extras = json.loads(args.extra_tables.read_text()) if args.extra_tables else {}
    registry = read_json('table_joins.json')
    specs = {s['id']: s for s in [*registry['navigation'], *[scalar_navigation(j) for j in registry['joins']]]}
    metadata = read_json('table_metadata.json')
    policies = descriptors()
    cache = MeasurementCache(args.cache, max_input_bytes=args.max_input_bytes, max_projected_bytes=args.max_projected_bytes)
    results, unavailable = [], []
    for identifier in args.route:
        if identifier not in specs:
            parser.error('Unknown canonical route: ' + identifier)
        spec = specs[identifier]
        policy = policies.get(spec['source'], {})
        identity = metadata.get(spec['source'], {}).get('identity_columns', []) or (
            policy.get('identity_fields', []) if policy.get('receipt_only') is False else [])
        for i, _target in enumerate(spec['targets']):
            try:
                results.append(cache.measure(index, paths, spec, i, source_identity=identity, extra_tables=extras))
            except (ValueError, OSError, RuntimeError) as error:
                unavailable.append({'route': {'id': identifier, 'targetIndex': i},
                                    'status': 'unavailable', 'reason': str(error)})
    args.output.write_text(json.dumps({'results': results, 'unavailable': unavailable, 'work': dict(cache.work)}, indent=2) + '\n')
    return 0 if not unavailable else 2


if __name__ == '__main__':
    raise SystemExit(main())
