"""Local export/refusal checks; the hosted Iceberg delete path needs its real run."""
import json

import duckdb
import pyarrow.parquet as pq
import pytest

from scripts import capture_comments_input as capture
from spicy_regs.sources import iceberg


@pytest.fixture
def expected():
    return {'tableUuid': 'original-table', 'snapshotId': 123, 'schemaId': 6,
            'schemas': [{'schema-id': 6, 'fields': [
                {'name': name, 'type': 'string', 'required': False, 'id': number}
                for number, name in enumerate(['comment_id', 'agency_code', 'duplicate_comments', 'text_content'], 1)
            ]}]}


def metadata(expected):
    return {'table-uuid': expected['tableUuid'], 'current-snapshot-id': expected['snapshotId'],
            'current-schema-id': expected['schemaId'], 'schemas': expected['schemas']}


@pytest.fixture
def logical_source(tmp_path, monkeypatch, expected):
    path = tmp_path / 'catalog.duckdb'
    with duckdb.connect(str(path)) as con:
        con.execute('CREATE TABLE logical_comments(comment_id VARCHAR, agency_code VARCHAR, '
                    'duplicate_comments VARCHAR, text_content VARCHAR)')
        con.execute("INSERT INTO logical_comments VALUES ('a','EPA','007','body'),('b',NULL,NULL,''),"
                    "('a','EPA','0',NULL),(NULL,'FAA',NULL,NULL)")
    query_calls = []

    def query(record, snapshot, *, namespace):
        query_calls.append((record.name, snapshot, namespace))
        return 'SELECT * FROM logical_comments'

    monkeypatch.setattr(iceberg, '_snapshot_query', query)
    monkeypatch.setattr(iceberg, '_table_metadata', lambda *a, **k: metadata(expected))
    connections = []

    def connect():
        con = duckdb.connect(str(path), read_only=True)
        connections.append(con)
        return con

    return connect, query_calls, connections


def test_capture_preserves_original_strings_nulls_and_complete_population(tmp_path, expected, logical_source):
    connect, query_calls, connections = logical_source
    output = tmp_path / 'capture'
    report = capture.capture(expected, output, namespace='default', connect=connect)
    assert report['status'] == 'captured'
    assert report['source']['rows'] == 4
    assert (report['nullIds'], report['distinctNonnullIds'], report['duplicateNonnullRows']) == (1, 2, 1)
    assert pq.read_table(output / 'comments.parquet')['duplicate_comments'].to_pylist() == ['007', None, '0', None]
    assert pq.read_table(output / 'agency-populations.parquet').to_pylist() == [
        {'agency_code': None, 'rows': 1}, {'agency_code': 'EPA', 'rows': 2}, {'agency_code': 'FAA', 'rows': 1}]
    assert query_calls == [('comments', iceberg.CatalogSnapshot('original-table', 123, 6), 'default')]
    assert len(connections) == 2 and connections[0] is not connections[1]
    for con in connections:
        with pytest.raises(duckdb.ConnectionException):
            con.execute('SELECT 1')
    assert (output / 'comments.parquet').stat().st_mode & 0o222 == 0


@pytest.mark.parametrize('which', ['before', 'after'])
def test_changed_snapshot_refuses_and_retains_attempt(tmp_path, expected, logical_source, monkeypatch, which):
    connect, query_calls, _ = logical_source
    calls = 0

    def changed(*args, **kwargs):
        nonlocal calls
        calls += 1
        value = metadata(expected)
        if which == 'before' or calls == 2:
            value['current-snapshot-id'] += 1
        return value

    monkeypatch.setattr(iceberg, '_table_metadata', changed)
    output = tmp_path / 'capture'
    with pytest.raises(RuntimeError, match='refused'):
        capture.capture(expected, output, namespace='default', connect=connect)
    result = json.loads((output / 'RESULT.json').read_text())
    assert result['status'] == 'refused'
    assert bool(query_calls) == (which == 'after')
    assert (output / 'comments.parquet').exists() == (which == 'after')


def test_connect_error_retains_only_sanitized_evidence(tmp_path, expected):
    secret = 'private-catalog-token-DO-NOT-RETAIN'

    def fail():
        raise RuntimeError('CREATE SECRET TOKEN ' + secret)

    output = tmp_path / 'capture'
    with pytest.raises(RuntimeError) as error:
        capture.capture(expected, output, namespace='default', connect=fail)
    assert secret not in str(error.value)
    assert secret not in (output / 'RESULT.json').read_text()
    assert error.value.__cause__ is None


def test_refuses_existing_output_without_overwriting(tmp_path, expected):
    output = tmp_path / 'capture'
    output.mkdir()
    held = output / 'RESULT.json'
    held.write_text('previous evidence')
    with pytest.raises(FileExistsError):
        capture.capture(expected, output, namespace='default', connect=lambda: pytest.fail('connected'))
    assert held.read_text() == 'previous evidence'


def test_production_snapshot_query_uses_exact_iceberg_version():
    from spicy_regs.schemas.regulations import RECORD_TYPES
    assert iceberg._snapshot_query(RECORD_TYPES['comments'], iceberg.CatalogSnapshot('original-table', 123, 6),
                                   namespace='default') == 'SELECT * FROM reg_catalog."default"."comments" AT (VERSION => 123)'
