"""Bounded local measurement runs over the maintained main navigation declarations.

The runner selects routes and records their state. MeasurementCache owns byte
admission, key semantics and incremental reuse; this is not a second cache.
"""
from __future__ import annotations

from collections import Counter
from copy import deepcopy

import duckdb
import pyarrow as pa

from spicy_regs.explorer_metadata import canonical_bytes, read_json
from spicy_regs.explorer_navigation import published_navigation
from spicy_regs.navigation_measurements import (
    MeasurementCache, digest, recipe_digest, scalar_navigation, selected_binding,
)
from spicy_regs.sources.publication import parse_index
from spicy_regs.subject_catalog import descriptors
from spicy_regs.table_joins import joins_record

FORMAT = 'main-navigation-census/1'
COUNTS = ('eligible', 'matched', 'missing', 'ambiguous', 'unsupportedReferences')


def canonical_routes():
    """Read declarations from their maintained code, not an older generated file."""
    record = joins_record()
    return [*record['navigation'], *[scalar_navigation(join) for join in record['joins']]]


def select_routes(specs, *, routes=(), affected_tables=(), all_routes=False):
    by_id = {spec['id']: spec for spec in specs}
    if len(by_id) != len(specs):
        raise ValueError('Canonical navigation route IDs must be unique')
    unknown = set(routes) - by_id.keys()
    if unknown:
        raise ValueError('Unknown canonical routes: ' + ', '.join(sorted(unknown)))
    tables = {spec['source'] for spec in specs} | {
        target['table'] for spec in specs for target in spec['targets']
        if not target['table'].startswith('@')}
    unknown = set(affected_tables) - tables
    if unknown:
        raise ValueError('No canonical routes involve tables: ' + ', '.join(sorted(unknown)))
    if all_routes and (routes or affected_tables):
        raise ValueError('--all cannot be combined with route or affected-table selection')
    if not all_routes and not routes and not affected_tables:
        raise ValueError('Select --all, --route or --affected-table explicitly')
    return {spec['id'] for spec in specs if all_routes or spec['id'] in routes
            or spec['source'] in affected_tables
            or any(target['table'] in affected_tables for target in spec['targets'])}


def _identity(source, metadata, policies):
    policy = policies.get(source, {})
    return metadata.get(source, {}).get('identity_columns', []) or (
        policy.get('identity_fields', []) if policy.get('receipt_only') is False else [])


def run_measurements(index, paths, cache: MeasurementCache, *, routes=(), affected_tables=(),
                     all_routes=False, extra_tables=None, max_route_input_bytes=256 * 1024**2,
                     specs=None, metadata=None, policies=None):
    """Return one census entry per canonical target, and unchanged publisher proofs.

    Unrequested targets stay unrequested, unavailable counts stay unknown, and
    external URL links are explicitly not applicable to table-match measurement.
    The per-route total limit is checked before opening any selected data file.
    Tests may inject tiny declarations; the command only uses canonical_routes.
    """
    if type(max_route_input_bytes) is not int or max_route_input_bytes < 1:
        raise ValueError('Route input-byte limit must be a positive integer')
    if not isinstance(paths, dict) or any(not isinstance(k, str) or not isinstance(v, str)
                                          for k, v in paths.items()):
        raise ValueError('Members must map selected public paths to local file paths')
    index = parse_index(canonical_bytes(index))
    extras = extra_tables or {}
    specs = deepcopy(canonical_routes() if specs is None else specs)
    metadata = read_json('table_metadata.json') if metadata is None else metadata
    policies = descriptors() if policies is None else policies
    selected = select_routes(specs, routes=routes, affected_tables=affected_tables, all_routes=all_routes)
    results, unavailable, census = [], [], []
    # Binding selection reads metadata only, and is shared by every touching route.
    bindings, binding_errors = {}, {}
    for table in {s['source'] for s in specs} | {
            t['table'] for s in specs for t in s['targets'] if not t['table'].startswith('@')}:
        try:
            bindings[table] = selected_binding(index, table, extras)
        except (ValueError, KeyError, TypeError) as error:
            binding_errors[table] = str(error)
    for spec in specs:
        identity = _identity(spec['source'], metadata, policies)
        resolved = published_navigation([spec], {name: b['schema'] for name, b in bindings.items()})[0]
        for i, target in enumerate(spec['targets']):
            source_binding, target_binding = bindings.get(spec['source']), bindings.get(target['table'])
            entry = {'route': {'id': spec['id'], 'targetIndex': i},
                     'source': spec['source'], 'target': target['table'],
                     'sourceIdentity': list(identity),
                     'binding': {'source': source_binding, 'target': target_binding,
                                 'recipe': recipe_digest(resolved, i, identity)},
                     'counts': dict.fromkeys(COUNTS), 'status': 'not_requested'}
            census.append(entry)
            if spec['id'] not in selected:
                continue
            if target['table'] == '@url':
                entry.update(status='not_applicable', reason='External offered URLs have no selected target table population')
                continue
            phase = 'selection'
            try:
                if not source_binding or not target_binding:
                    problems = {table: binding_errors.get(table, 'No selected main table')
                                for table in (spec['source'], target['table']) if table not in bindings}
                    entry['inputIssues'] = problems
                    raise ValueError('; '.join(f'{table}: {reason}' for table, reason in problems.items()))
                if not resolved['targets'][i]['sourceAvailable'] or not resolved['targets'][i]['available']:
                    raise ValueError('Required main source/target route fields are unavailable')
                phase = 'budget'
                members = {m['path']: m for b in (source_binding, target_binding) for m in b['members']}
                entry['selectedInputBytes'] = sum(m['byteSize'] for m in members.values())
                if entry['selectedInputBytes'] > max_route_input_bytes:
                    raise ValueError('Selected route exceeds total input-byte limit '
                                     f'({entry["selectedInputBytes"]} > {max_route_input_bytes})')
                missing = sorted(set(members) - paths.keys())
                if missing:
                    entry['missingMembers'] = missing
                    raise ValueError('Incomplete selected local member set: ' + ', '.join(missing))
                phase = 'measurement'
                before = cache.work.copy()
                proof = cache.measure(index, paths, spec, i, source_identity=identity, extra_tables=extras)
                entry.update(status='complete', counts={name: proof[name] for name in COUNTS},
                             proofDigest=digest(proof),
                             work=dict(cache.work - before))
                results.append(proof)
            except (ValueError, OSError, RuntimeError, duckdb.Error, pa.ArrowException) as error:
                entry.update(status='unavailable', phase=phase, reason=str(error), errorType=type(error).__name__)
                unavailable.append({key: entry[key] for key in
                                    ('route', 'status', 'phase', 'reason', 'errorType', 'binding')})
    return {'format': FORMAT, 'publicationDigest': digest(index),
            'selection': {'all': all_routes, 'routes': sorted(set(routes)),
                          'affectedTables': sorted(set(affected_tables)), 'selectedRoutes': sorted(selected)},
            'limits': {'routeInputBytes': max_route_input_bytes, 'memberInputBytes': cache.max_input_bytes,
                       'memberProjectedBytes': cache.max_projected_bytes},
            'results': results, 'unavailable': unavailable, 'census': census,
            'statusCounts': dict(Counter(entry['status'] for entry in census)), 'work': dict(cache.work)}
