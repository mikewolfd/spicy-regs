"""Real tiny workflow checks and refusal controls for retained Court/FR replay."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import qualify_retained_population as runner
from spicy_regs import etl_bulk


def selected_plan(tmp_path, dataset, *, refused=False):
    if dataset == 'court_opinions':
        rows = [{'opinion_id': None if refused else '1', 'cluster_id': '2', 'author_str': 'café\x00\n',
                 'page_count': '0002', 'per_curiam': 't'},
                {'opinion_id': '3', 'cluster_id': None, 'author_str': '', 'page_count': None, 'per_curiam': None}]
    elif dataset == 'federal_register':
        # Original order has rin last; the actual processor emits canonical order.
        rows = [{'document_number': '2026-00001', 'publication_date': '2026-01-01', 'title': 'café\x1b\n',
                 'agencies_json': '[]', 'regulations_dot_gov_comments_count': '0007', 'rin': '007'},
                {'document_number': '2026-00002', 'publication_date': '2026-01-02', 'title': '', 'rin': None}]
    elif dataset == 'dockets':
        rows = [{'docket_id': 'FAA-2026-0001', 'title': 'café\x00\n', 'rin': '007'},
                {'docket_id': 'FAA-2026-0002', 'title': '', 'rin': None}]
    else:
        rows = [{'document_id': 'FAA-2026-0001-0001', 'title': 'café\x1b\n', 'withdrawn': 'False',
                 'attachment_records_json': json.dumps([{'id': 'a', 'type': 'attachments',
                    'attributes': {'title': 'source', 'agencyNote': None, 'authors': ['a'], 'docOrder': 1}}])},
                {'document_id': 'FAA-2026-0001-0002', 'title': '', 'withdrawn': None}]
    columns = list(dict.fromkeys(name for row in rows for name in row))
    schema = pa.schema([(name, pa.string()) for name in columns], metadata={b'original': b'line\n'})
    source = tmp_path / 'source.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source)
    plan = {'source': {'dataset': dataset, 'family': runner.DATASETS[dataset], 'localInput': str(source),
                       'sha256': 'sha256:' + hashlib.sha256(source.read_bytes()).hexdigest(),
                       'bytes': source.stat().st_size, 'rows': len(rows), 'columns': columns}}
    path = tmp_path / 'plan.json'
    path.write_text(json.dumps(plan))
    return path, plan, source


@pytest.mark.parametrize('dataset', runner.DATASETS)
def test_actual_complete_fixture_pipeline(tmp_path, dataset):
    path, _, source = selected_plan(tmp_path, dataset)
    output = tmp_path / 'workflow'
    result = subprocess.run([sys.executable, '-m', 'scripts.qualify_retained_population', str(path),
                             str(output), '--fixture'], cwd=Path(__file__).parents[1], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads((output / 'RESULT.json').read_text())['status'] == 'passed'
    phases = [item for line in (output / 'phases.jsonl').read_text().splitlines()
              if (item := json.loads(line))['status'] != 'running']
    assert all(item['status'] == 'passed' for item in phases)
    for name in ('separate-selected-prior-producer-and-full-admission', 'fresh-current-producer-and-full-admission'):
        admission = next(item for item in phases if item['phase'] == name)
        assert admission['sourceRows'] == admission['subjectRows'] == 2
        assert admission['actualBulkSettings']['threads'] == '4'
        assert all(route['fullyConsumed'] for route in admission['validationRoutes'])
        if dataset in runner.BASE_DATASETS:
            assert admission['baseAdapterCalls'] == admission['baseAdapterCompleted'] == 1
    if dataset in runner.BASE_DATASETS:
        oracle = next(item for item in phases if item['phase'] == 'full-original-row-producer-and-all-field-equivalence')
        assert oracle['subjectRows'] == 2 and oracle['receiptRows'] == 3
        restored = next(item for item in phases if item['phase'] == 'actual-full-processor-restore-original-and-canonical-conservation')
        assert restored['baseAdapterCompleted'] == 1 and restored['rowReaderEquivalentRows'] == 2
    # The history phase verifies every prior field and a distinct actual current file.
    history = next(item for item in phases if item['phase'] == 'full-unchanged-history-and-exact-prior-fields')
    assert history['selectedPrior']['sha256'] != history['current']['sha256']
    assert history['rows'] == 2 + (dataset != 'court_opinions')
    if dataset in runner.BASE_DATASETS:
        runner.compare_files(output / 'selected-prior' / 'etl_receipts.parquet', output / 'carried-receipts.parquet')
        handoff = json.loads((output / 'HANDOFF.json').read_text())
        assert handoff['resourceReleaseEvidenceRequired']
        assert handoff['receiptGenerationId'] == 'qualification-' + dataset
        assert dataset + '.parquet' in handoff['generationMembers']
        assert handoff['receiptPolicies'] == [runner.declared_policy(dataset).descriptor()]
    controls = json.loads((output / 'mcp-controls.json').read_text())
    assert controls['replies'][f'DELETE FROM "{dataset}" WHERE FALSE']['isError']
    assert pq.read_table(source).schema.metadata == {b'original': b'line\n'}


def test_refused_source_row_stops_full_claim_and_retains_evidence(tmp_path):
    path, _, _ = selected_plan(tmp_path, 'court_opinions', refused=True)
    output = tmp_path / 'refused'
    with pytest.raises(ValueError, match='refused/lost'):
        runner.qualify(path, output, fixture=True)
    assert not (output / 'RESULT.json').exists()
    assert (output / 'original.parquet').exists()
    assert (output / 'phases.jsonl').exists()


def test_wrong_pin_and_empty_selection_refuse_before_production(tmp_path):
    _, plan, _ = selected_plan(tmp_path, 'court_opinions')
    entry = dict(plan['source'], sha256='sha256:' + '0' * 64)
    with pytest.raises(ValueError):
        runner.capture_source(entry, tmp_path / 'partial.parquet')
    assert (tmp_path / 'partial.parquet').exists()
    with pytest.raises(ValueError, match='positive row count'):
        runner.capture_source(dict(plan['source'], rows=0), tmp_path / 'empty.parquet')
    assert not (tmp_path / 'empty.parquet').exists()


def test_source_changed_after_capture_refuses_final_claim(tmp_path):
    path, _, source = selected_plan(tmp_path, 'court_opinions')
    output = tmp_path / 'changed'
    code = '''
import sys
from pathlib import Path
from scripts import qualify_retained_population as runner
real = runner.produce
def produce(*args, **kwargs):
    result = real(*args, **kwargs)
    Path(sys.argv[3]).write_bytes(b'changed selected original after capture')
    return result
runner.produce = produce
runner.qualify(Path(sys.argv[1]), Path(sys.argv[2]), fixture=True)
'''
    result = subprocess.run([sys.executable, '-c', code, str(path), str(output), str(source)],
                            cwd=Path(__file__).parents[1], capture_output=True, text=True)
    assert result.returncode != 0
    assert 'differs from its admitted hash' in result.stderr
    assert not (output / 'RESULT.json').exists()
    phases = [json.loads(line) for line in (output / 'phases.jsonl').read_text().splitlines()]
    assert phases[-1]['phase'] == 'final-complete-artifact-stored-bytes-and-original-pins'
    assert phases[-1]['status'] == 'failed'
    assert pq.read_metadata(output / 'original.parquet').num_rows == 2
    assert source.read_bytes() == b'changed selected original after capture'


def test_route_observer_preserves_nonempty_ordinals_and_restores_on_error(tmp_path):
    original = etl_bulk._ordinal_file
    record = {}
    with duckdb.connect() as con:
        with pytest.raises(RuntimeError, match='probe'):
            with runner.observe_validation_routes(record):
                assert list(etl_bulk._ordinal_file(con, 'SELECT * FROM (VALUES (2),(5)) t(n)', [],
                                                   tmp_path / 'selected.parquet')) == [2, 5]
                raise RuntimeError('probe')
    assert etl_bulk._ordinal_file is original
    assert record['validationRoutes'] == [{'selection': 'selected.parquet', 'selectedRows': 2,
                                          'yieldedRows': 2, 'fullyConsumed': True}]


def test_base_capture_requires_actual_selection_and_unchanged_evidence(tmp_path):
    _, plan, source = selected_plan(tmp_path, 'dockets')
    with pytest.raises(ValueError, match='requires exact capture'):
        runner.capture_prerequisites(plan, required=True)
    entry = plan['source']
    values = {'plan': {'metadataPins': []}, 'review': {'verdict': 'fixture only'},
              'result': {'state': 'passed'}, 'qualification': {'state': 'passed', 'phases': [
                  {'key': 'source:dockets.parquet', 'path': str(source), 'bytes': entry['bytes'],
                   'rows': entry['rows'], 'sha256': entry['sha256'].removeprefix('sha256:')}]}}
    evidence = {}
    for name, value in values.items():
        path = tmp_path / (name + '.json')
        path.write_text(json.dumps(value))
        evidence[name] = {'path': str(path), 'bytes': path.stat().st_size,
                          'sha256': 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest()}
    plan['captureEvidence'] = evidence
    runner.capture_prerequisites(plan, required=True)
    with pytest.raises(ValueError, match='differs from the admitted capture'):
        runner.capture_prerequisites(dict(plan, source=dict(entry, localInput=str(tmp_path / 'substitution'))), required=True)
    Path(evidence['qualification']['path']).write_text('{}')
    with pytest.raises(ValueError, match='evidence differs'):
        runner.capture_prerequisites(plan, required=True)


def test_base_route_refuses_completed_row_fallback(monkeypatch):
    original = runner.regulations_bulk.write_held_dataset
    def unsupported(*args, **kwargs):
        raise etl_bulk.NotBulkEligible('fixture unsupported')
    monkeypatch.setattr(runner.regulations_bulk, 'write_held_dataset', unsupported)
    record = {}
    with pytest.raises(ValueError, match='did not complete exactly once'):
        with runner.observe_base_routes(record, 'write_held_dataset'):
            try:
                runner.regulations_bulk.write_held_dataset('dockets', Path('source'), Path('destination'),
                    generation_id='fixture', processor='fixture', witnesses=(),
                    include_source_witness=True, prior_receipts=())
            except etl_bulk.NotBulkEligible:
                pass
    assert record == {'baseAdapterCalls': 1, 'baseAdapterCompleted': 0}
    assert runner.regulations_bulk.write_held_dataset is unsupported
    monkeypatch.setattr(runner.regulations_bulk, 'write_held_dataset', original)
