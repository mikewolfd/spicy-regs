"""Explicit retained identity proofs reuse counts only for freshly admitted exact inputs."""
from copy import deepcopy
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.navigation_measurements import MeasurementCache, selected_binding
from tests.test_navigation_measurements import selection
from tests.test_navigation_measurement_runner import run


def supplied_proof(tmp_path, binding, columns=('id',), *, name='proof.json'):
    pins = [{'url': 'https://example.test/' + member['path'],
             **{k: member[k] for k in ('sha256', 'rows', 'byteSize')}} for member in binding['members']]
    rows = binding['rows']
    record = {'source': binding['table'], 'identityColumns': list(columns), 'sourceRows': rows,
              'maintainedNullableIdentityFields': [],
              'sourcePin': pins, 'admissions': [{'member': pin} for pin in pins],
              'status': 'verified-full-main-complete-source-identity', 'missingIdentityColumns': [],
              'completeIdentityAdmittedRows': rows, 'completeIdentityNonNullRows': rows,
              'distinctCompleteSourceIdentities': rows, 'duplicateCompleteIdentityRows': 0,
              'sourceRowsWithIncompleteIdentity': 0, 'globalUniqueCompleteSourceIdentity': True,
              'globalUniqueNonNullCompleteSourceIdentity': True}
    path = tmp_path / name
    path.write_text(json.dumps({'sources': [record], 'resourceScope': 'Earlier full native identity grouping; independent limits.'}))
    return path, record


@pytest.mark.parametrize('split', [False, True])
def test_imported_population_reuses_existing_cache_shape_without_key_projection(tmp_path, monkeypatch, split):
    index, paths, _ = selection(tmp_path, split=split)
    binding = selected_binding(index, 'source')
    path, _ = supplied_proof(tmp_path, binding)
    cache = MeasurementCache(tmp_path / 'cache')
    supplied = cache.capture_population_proof(path)
    imported = cache.import_population(binding, paths, ('id',), supplied)
    assert (imported['rows'], imported['nonNullRows'], imported['distinctKeys']) == (4, 4, 4)
    assert imported['nullKeyRows'] == imported['duplicateKeys'] == 0
    assert imported['maximumRowsPerKey'] == 1
    provenance = imported['populationProof']
    assert provenance['authority'] == 'imported_existing_full_population'
    assert provenance['artifact']['sha256'] == 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest()
    assert provenance['physicalSchema'] == binding['schema']
    assert 'no new grouping execution' in provenance['execution']
    def refuse_scan(*args, **kwargs):
        pytest.fail('Imported exact population performed a new key projection or grouping')
    monkeypatch.setattr(cache, 'project', refuse_scan)
    assert cache.population(binding, cache.admit(binding, paths), ('id',)) == imported
    assert cache.work['population_imports'] == 1 and not cache.work['population_aggregations']
    assert not cache.work['member_projections']
    assert cache.import_population(binding, paths, ('id',), supplied) == imported
    assert cache.work['population_imports'] == 1


@pytest.mark.parametrize('damage', ['pins', 'partial_members', 'extra_member', 'keys', 'counts', 'nulls',
                                  'duplicates', 'partial_scope', 'missing_keys', 'admissions', 'boolean_counts',
                                  'duplicate_records', 'wrong_table', 'nullable_scope', 'unspecified_nullability'])
def test_partial_or_mismatched_proof_never_borrows_counts(tmp_path, damage):
    index, paths, _ = selection(tmp_path, split=True)
    binding = selected_binding(index, 'source')
    path, record = supplied_proof(tmp_path, binding)
    if damage == 'pins':
        record['sourcePin'][0]['sha256'] = 'sha256:' + 'b' * 64
    if damage == 'partial_members':
        record['sourcePin'].pop()
    if damage == 'extra_member':
        record['sourcePin'].append(deepcopy(record['sourcePin'][0]))
    if damage == 'keys':
        record['identityColumns'] = ['congress']
    if damage == 'counts':
        record['completeIdentityAdmittedRows'] -= 1
    if damage == 'nulls':
        record['sourceRowsWithIncompleteIdentity'] = 1
    if damage == 'duplicates':
        record['duplicateCompleteIdentityRows'] = 1
    if damage == 'partial_scope':
        record['status'] = 'verified-first-member-only'
    if damage == 'missing_keys':
        record['missingIdentityColumns'] = ['id']
    if damage == 'admissions':
        record['admissions'] = []
    if damage == 'boolean_counts':
        record['sourceRows'] = True
    if damage == 'wrong_table':
        record['source'] = 'other'
    if damage == 'nullable_scope':
        record['maintainedNullableIdentityFields'] = ['id']
    if damage == 'unspecified_nullability':
        record.pop('maintainedNullableIdentityFields')
    document = {'sources': [record, record] if damage == 'duplicate_records' else [record]}
    path.write_text(json.dumps(document))
    cache = MeasurementCache(tmp_path / 'cache')
    with pytest.raises(ValueError, match='Population proof'):
        cache.import_population(binding, paths, ('id',), cache.capture_population_proof(path))
    assert not list(cache.directory.glob('population-*.json'))


def test_fresh_admission_refuses_changed_member_schema_and_missing_part(tmp_path):
    index, paths, _ = selection(tmp_path, split=True)
    binding = selected_binding(index, 'source')
    path, _ = supplied_proof(tmp_path, binding)
    cache = MeasurementCache(tmp_path / 'cache')
    proof = cache.capture_population_proof(path)
    with pytest.raises(ValueError, match='Incomplete selected member'):
        cache.import_population(binding, {}, ('id',), proof)
    altered = deepcopy(binding)
    altered['schema'][0][1] = 'BIGINT'
    with pytest.raises(ValueError, match='schema differs'):
        cache.import_population(altered, paths, ('id',), proof)
    first = binding['members'][0]['path']
    pq.write_table(pa.table({'id': ['changed']}), paths[first])
    with pytest.raises(ValueError, match='byte size|SHA'):
        cache.import_population(binding, paths, ('id',), proof)


def test_changed_proof_invalidates_imported_population_and_derived_route_result(tmp_path):
    index, paths, spec = selection(tmp_path)
    binding = selected_binding(index, 'source')
    path, _ = supplied_proof(tmp_path, binding)
    cache = MeasurementCache(tmp_path / 'cache')
    captured = cache.capture_population_proof(path)
    cache.import_population(binding, paths, ('id',), captured)
    result = cache.measure(index, paths, spec, source_identity=('id',))
    assert result['sourceIdentity']['population']['populationProof']
    assert cache._read('result', result['binding']) == result
    path.write_text(path.read_text() + '\n')
    with pytest.raises(RuntimeError, match='changed'):
        cache.import_population(binding, paths, ('id',), captured)
    assert cache._read('result', result['binding']) is None
    entries = [json.loads(path.read_text()) for path in cache.directory.glob('population-*.json')]
    dependency = next(entry['dependency'] for entry in entries if entry['payload'].get('populationProof'))
    assert cache._read('population', dependency) is None


def test_changed_captured_records_do_not_borrow_file_authority(tmp_path):
    index, paths, _ = selection(tmp_path)
    binding = selected_binding(index, 'source')
    path, _ = supplied_proof(tmp_path, binding)
    cache = MeasurementCache(tmp_path / 'cache')
    captured = cache.capture_population_proof(path)
    captured['sources'][0]['status'] = 'edited'
    with pytest.raises(ValueError, match='Captured population proof records changed'):
        cache.import_population(binding, paths, ('id',), captured)


def test_runner_imports_only_explicit_exact_proofs_and_preserves_historical_authority(tmp_path):
    index, paths, spec = selection(tmp_path)
    path, _ = supplied_proof(tmp_path, selected_binding(index, 'source'))
    cache = MeasurementCache(tmp_path / 'cache')
    result = run(index, paths, cache, [spec], all_routes=True, population_proofs=[path])
    assert result['statusCounts'] == {'complete': 1}
    assert result['work']['population_imports'] == 1
    assert result['work']['population_aggregations'] == 1  # Target remains separately measured.
    assert result['results'][0]['sourceIdentity']['population']['populationProof']['artifact']['path'] == str(path)
    before = cache.work.copy()
    repeated = run(index, paths, cache, [spec], all_routes=True, population_proofs=[path])
    assert repeated['results'] == result['results']
    assert cache.work['population_imports'] == before['population_imports']
    assert cache.work['population_aggregations'] == before['population_aggregations']
    assert repeated['census'][0]['work']['result_hits'] == 1


def test_cli_requires_explicit_proof_path_and_keeps_provenance(tmp_path, monkeypatch):
    from scripts import measure_source_navigation as cli
    from spicy_regs import navigation_measurement_runner as runner
    index, paths, spec = selection(tmp_path)
    monkeypatch.setattr(runner, 'canonical_routes', lambda: [spec])
    path, _ = supplied_proof(tmp_path, selected_binding(index, 'source'))
    publication, members, output = (tmp_path / name for name in ('publication.json', 'members.json', 'output.json'))
    publication.write_text(json.dumps(index))
    members.write_text(json.dumps(paths))
    args = ['--publication', str(publication), '--members', str(members), '--cache', str(tmp_path / 'cache'),
            '--output', str(output), '--all', '--population-proof', str(path)]
    assert cli.main(args) == 0
    result = json.loads(output.read_text())
    # The CLI uses maintained table identities. This fixture has no maintained
    # source identity, so an explicitly named report cannot invent one.
    assert result['populationProofs'][0]['path'] == str(path)
    assert 'population_imports' not in result['work']
    assert not result['results'][0]['sourceIdentity']['columns']


def test_same_member_proof_does_not_lend_counts_to_other_keys_or_duplicate_sources(tmp_path):
    index, paths, spec = selection(tmp_path)
    path, _ = supplied_proof(tmp_path, selected_binding(index, 'source'), columns=('id', 'refs'))
    cache = MeasurementCache(tmp_path / 'cache')
    result = run(index, paths, cache, [spec], all_routes=True, population_proofs=[path])
    assert result['statusCounts'] == {'complete': 1}
    assert 'population_imports' not in result['work']
    second, _ = supplied_proof(tmp_path, selected_binding(index, 'source'), name='other.json')
    first, _ = supplied_proof(tmp_path, selected_binding(index, 'source'))
    duplicate = run(index, paths, cache, [spec], all_routes=True, population_proofs=[first, second])
    assert duplicate['unavailable'][0]['phase'] == 'population_import'
    assert 'Multiple supplied proofs' in duplicate['unavailable'][0]['reason']


def test_nullable_qualified_native_proof_does_not_qualify_strict_nonnull_population(tmp_path):
    index, paths, _ = selection(tmp_path)
    binding = selected_binding(index, 'source')
    path, record = supplied_proof(tmp_path, binding)
    # The retained FR report counts docket_source_ordinal as a permitted NULL
    # despite its globalUniqueNonNullCompleteSourceIdentity flag. This fixture
    # preserves that conflict between the declaration and claimed count scope.
    record['maintainedNullableIdentityFields'] = ['id']
    assert record['globalUniqueNonNullCompleteSourceIdentity'] is True
    path.write_text(json.dumps({'sources': [record]}))
    cache = MeasurementCache(tmp_path / 'cache')
    with pytest.raises(ValueError, match='permits nullable'):
        cache.import_population(binding, paths, ('id',), cache.capture_population_proof(path))
    assert not cache.work['population_imports']
    assert not list(cache.directory.glob('population-*.json'))


@pytest.mark.parametrize('status', ['reused-exact-small-complete-identity', 'partial_scope', 'failed'])
def test_runner_ineligible_report_does_not_invalidate_complete_native_measurement(tmp_path, status):
    index, paths, spec = selection(tmp_path)
    cache = MeasurementCache(tmp_path / 'cache')
    baseline = run(index, paths, cache, [spec], all_routes=True)
    path, record = supplied_proof(tmp_path, selected_binding(index, 'source'))
    record.update(status=status, distinctCompleteSourceIdentities=0)
    path.write_text(json.dumps({'sources': [record]}))
    repeated = run(index, paths, cache, [spec], all_routes=True, population_proofs=[path])
    assert repeated['results'] == baseline['results']
    assert repeated['statusCounts'] == {'complete': 1}
    assert repeated['census'][0]['work']['result_hits'] == 1
    assert not cache.work['population_imports']
    # With no held result, normal admission and measurement still run. The
    # report's ineligible zero distinct count never lends population authority.
    fresh = run(index, paths, MeasurementCache(tmp_path / 'fresh-cache'), [spec], all_routes=True, population_proofs=[path])
    assert {key: value for key, value in fresh['results'][0].items() if key != 'occurrences'} == {
        key: value for key, value in baseline['results'][0].items() if key != 'occurrences'}
    assert fresh['work']['population_aggregations'] == 2
    assert 'population_imports' not in fresh['work']


def test_runner_claimed_complete_nullable_report_refuses_even_with_completed_native_result(tmp_path):
    index, paths, spec = selection(tmp_path)
    cache = MeasurementCache(tmp_path / 'cache')
    assert run(index, paths, cache, [spec], all_routes=True)['statusCounts'] == {'complete': 1}
    path, record = supplied_proof(tmp_path, selected_binding(index, 'source'))
    record['maintainedNullableIdentityFields'] = ['id']
    path.write_text(json.dumps({'sources': [record]}))
    refused = run(index, paths, cache, [spec], all_routes=True, population_proofs=[path])
    assert refused['statusCounts'] == {'unavailable': 1}
    assert refused['unavailable'][0]['phase'] == 'population_import'
    assert 'nullable' in refused['unavailable'][0]['reason']
    assert not refused['results']
    assert all(value is None for value in refused['census'][0]['counts'].values())
