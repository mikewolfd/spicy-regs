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
    else:
        # Original order has rin last; the actual processor emits canonical order.
        rows = [{'document_number': '2026-00001', 'publication_date': '2026-01-01', 'title': 'café\x1b\n',
                 'agencies_json': '[]', 'regulations_dot_gov_comments_count': '0007', 'rin': '007'},
                {'document_number': '2026-00002', 'publication_date': '2026-01-02', 'title': '', 'rin': None}]
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
    # The history phase verifies every prior field and a distinct actual current file.
    history = next(item for item in phases if item['phase'] == 'full-unchanged-history-and-exact-prior-fields')
    assert history['selectedPrior']['sha256'] != history['current']['sha256']
    assert history['selectedPrior']['sha256'] == history['carried']['sha256']
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
