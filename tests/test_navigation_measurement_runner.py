"""Route census selection, bounded admission and reuse with real tiny main files."""
from copy import deepcopy
import hashlib
import json

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import measure_source_navigation as cli
from spicy_regs.explorer_navigation import array, guard, key, part, route
from spicy_regs.navigation_measurement_runner import run_measurements, select_routes
from spicy_regs.navigation_measurements import MeasurementCache
from spicy_regs.native_types import described_schema
from spicy_regs.sources.publication import table_members


def fixture(tmp_path):
    tables = {}
    files = {}
    for name, rows in [('source', [{'id': 's1', 'congress': '119', 'refs': [{'id': 'a'}, {'id': 'missing'}]}]),
                       ('target', [{'id': 'a'}, {'id': 'a'}])]:
        value = pa.Table.from_pylist(rows)
        path = tmp_path / (name + '.parquet')
        pq.write_table(value, path)
        tables[path.name] = {'rows': len(rows), 'byteSize': path.stat().st_size,
                             'sha256': 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest(),
                             'columns': [list(c) for c in described_schema(value.schema)]}
        files[name] = path
    index = {'format': 'spicy-regs-publication', 'version': 2, 'families': {'fixture': {
        'prefix': 'generations/fixture/' + 'a' * 64, 'logicalId': 'urn:fixture',
        'artifactDigest': 'sha256:' + 'a' * 64, 'tables': tables}}}
    paths = {table_members(index, name + '.parquet')[0].path: str(path) for name, path in files.items()}
    spec = array('references', 'source', ('refs',),
                 (route('target', ('id',), (key(part('id')),)),), meaning='Explicit references')
    return index, paths, spec


def run(index, paths, cache, specs, **kwargs):
    return run_measurements(index, paths, cache, specs=specs,
                            metadata={'source': {'identity_columns': ['id']}}, policies={}, **kwargs)


def test_affected_tables_include_both_sides_without_selecting_unrelated_routes(tmp_path):
    _, _, spec = fixture(tmp_path)
    incoming = deepcopy(spec) | {'id': 'incoming', 'source': 'target', 'targets': [route('other', ('id',), (key(part('id')),))]}
    unrelated = deepcopy(spec) | {'id': 'unrelated', 'source': 'elsewhere', 'targets': [route('other', ('id',), (key(part('id')),))]}
    assert select_routes([spec, incoming, unrelated], affected_tables=['target']) == {'references', 'incoming'}
    with pytest.raises(ValueError, match='No canonical routes'):
        select_routes([spec], affected_tables=['typo'])
    with pytest.raises(ValueError, match='explicitly'):
        select_routes([spec])
    with pytest.raises(ValueError, match='cannot be combined'):
        select_routes([spec], routes=['references'], all_routes=True)


def test_census_keeps_complete_missing_unrequested_and_external_states_separate(tmp_path):
    index, paths, spec = fixture(tmp_path)
    spec['targets'].append(route('not_published', ('id',), (key(part('id')),)))
    external = array('documents', 'source', ('refs',), (route('@url', ('url',), (key(part('id')),)),), meaning='URLs')
    unrequested = deepcopy(spec) | {'id': 'unrequested'}
    result = run(index, paths, MeasurementCache(tmp_path / 'cache'), [spec, external, unrequested],
                 routes=['references', 'documents'])
    assert result['statusCounts'] == {'complete': 1, 'unavailable': 1, 'not_applicable': 1, 'not_requested': 2}
    assert result['census'][0]['counts']['ambiguous'] == 1
    assert result['census'][0]['counts']['missing'] == 1
    missing = result['unavailable'][0]
    assert missing['binding']['source']['pin'] == result['results'][0]['binding']['source']['pin']
    assert missing['binding']['target'] is None
    assert missing['phase'] == 'selection'
    assert all(n is None for n in result['census'][1]['counts'].values())


def test_unchanged_run_reuses_complete_proof_without_projection_or_occurrence_scans(tmp_path):
    index, paths, spec = fixture(tmp_path)
    first = run(index, paths, MeasurementCache(tmp_path / 'cache'), [spec], all_routes=True)
    reused = run(index, paths, MeasurementCache(tmp_path / 'cache'), [spec], all_routes=True)
    assert reused['results'] == first['results']
    assert reused['work']['result_hits'] == 1
    assert not set(reused['work']) & {'member_hashes', 'member_projections', 'occurrences_scans', 'route_measurements'}
    changed = deepcopy(spec)
    changed['targets'][0]['guards'] = [guard('congress', row=True, values=('118',))]
    guarded = run(index, paths, MeasurementCache(tmp_path / 'cache'), [changed], all_routes=True)
    assert guarded['results'][0]['eligible'] == 0
    assert guarded['work']['occurrences_scans'] == 1
    assert 'target_keys_scans' not in guarded['work']
    assert guarded['census'][0]['binding']['recipe'] != first['census'][0]['binding']['recipe']


def test_total_partition_budget_refuses_before_opening_data_or_using_historical_results(tmp_path, monkeypatch):
    index, paths, spec = fixture(tmp_path)
    cache = MeasurementCache(tmp_path / 'cache')
    run(index, paths, cache, [spec], all_routes=True)
    source = index['families']['fixture']['tables']['source.parquet']
    checksum = source.pop('sha256')
    source['partitionColumns'] = ['bucket']
    source['columns'].append(['bucket', 'VARCHAR'])
    source['members'] = [{'key': f'source/bucket={i}/part-000000.parquet', 'rows': source['rows'],
                          'byteSize': source['byteSize'], 'sha256': checksum,
                          'partition': {'bucket': str(i)}} for i in range(2)]
    source['rows'] *= 2
    source['byteSize'] *= 2
    monkeypatch.setattr(cache, 'measure', lambda *a, **k: pytest.fail('Over-budget route opened data'))
    result = run(index, {}, cache, [spec], all_routes=True, max_route_input_bytes=source['byteSize'] - 1)
    assert result['unavailable'][0]['phase'] == 'budget'
    assert 'total input-byte limit' in result['unavailable'][0]['reason']
    assert not result['results']
    assert all(n is None for n in result['census'][0]['counts'].values())


def test_missing_local_members_and_database_failures_do_not_hide_other_routes(tmp_path, monkeypatch):
    index, paths, spec = fixture(tmp_path)
    missing = run(index, {}, MeasurementCache(tmp_path / 'missing'), [spec], all_routes=True)
    assert missing['census'][0]['missingMembers'] == sorted(paths)
    assert missing['unavailable'][0]['phase'] == 'budget'
    cache = MeasurementCache(tmp_path / 'failed')
    monkeypatch.setattr(cache, 'measure', lambda *a, **k: (_ for _ in ()).throw(duckdb.OutOfMemoryException('fixture memory refusal')))
    failed = run(index, paths, cache, [spec], all_routes=True)
    assert failed['unavailable'][0]['phase'] == 'measurement'
    assert failed['unavailable'][0]['errorType'] == 'OutOfMemoryException'
    assert failed['census'][0]['binding']['target']['members']


def test_cli_uses_canonical_code_and_writes_partial_census_with_exit_two(tmp_path, monkeypatch):
    from spicy_regs import navigation_measurement_runner as runner
    index, paths, spec = fixture(tmp_path)
    monkeypatch.setattr(runner, 'canonical_routes', lambda: [spec])
    publication, members, output = (tmp_path / n for n in ('publication.json', 'members.json', 'run/output.json'))
    publication.write_text(json.dumps(index))
    members.write_text('{}')
    args = ['--publication', str(publication), '--members', str(members), '--cache', str(tmp_path / 'cache'),
            '--output', str(output), '--all']
    assert cli.main(args) == 2
    receipt = json.loads(output.read_text())
    assert len(receipt['census']) == 1 and not receipt['results']
    assert receipt['unavailable'][0]['binding']['source']['members']
    members.write_text(json.dumps(paths))
    assert cli.main(args) == 0
    assert json.loads(output.read_text())['census'][0]['status'] == 'complete'
