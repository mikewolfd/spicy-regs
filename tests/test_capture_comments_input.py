"""Local export/refusal checks; the hosted Iceberg delete path needs its real run."""
import json

import duckdb
import pyarrow as pa
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


def test_lazy_sink_writes_each_batch_before_fetching_next(tmp_path, expected, monkeypatch):
    """The old COPY sink is red; this tests client ownership, not query memory."""
    schema = pa.schema([(field['name'], pa.string()) for field in capture.fields(expected)])
    events = []
    real_writer = pq.ParquetWriter
    rows = [dict(comment_id=str(n), agency_code=None, duplicate_comments='007', text_content='café\x00\n')
            for n in range(2001)]

    class Reader:
        def __init__(self):
            self.schema = schema

        def __enter__(self):
            return self

        def __exit__(self, *args):
            events.append('reader-close')

        def __iter__(self):
            for start in range(0, len(rows), 1000):
                events.append('fetch')
                yield pa.RecordBatch.from_pylist(rows[start:start + 1000], schema=schema)

    class Relation:
        def to_arrow_reader(self, batch_size):
            assert batch_size == 1000
            return Reader()

    class Connection:
        def __init__(self):
            self.inner = duckdb.connect()

        def execute(self, query, parameters=None):
            assert not query.startswith('COPY'), 'payload COPY is forbidden by lazy sink gate'
            return self.inner.execute(query, parameters) if parameters is not None else self.inner.execute(query)

        def sql(self, query):
            assert query == 'SELECT * FROM selected_original'
            return Relation()

        def close(self):
            self.inner.close()
            events.append('connection-close')

    class Writer:
        def __init__(self, *args, **kwargs):
            self.inner = real_writer(*args, **kwargs)

        def __enter__(self):
            return self

        def write_batch(self, batch, *, row_group_size):
            assert batch.num_rows <= row_group_size == 1000
            assert events[-1] == 'fetch'
            self.inner.write_batch(batch, row_group_size=row_group_size)
            events.append('write')

        def __exit__(self, *args):
            self.inner.close()
            events.append('writer-close')

    monkeypatch.setattr(iceberg, '_table_metadata', lambda *a, **k: metadata(expected))
    monkeypatch.setattr(iceberg, '_snapshot_query', lambda *a, **k: 'SELECT * FROM selected_original')
    monkeypatch.setattr(pq, 'ParquetWriter', Writer)
    output = tmp_path / 'lazy'
    report = capture.capture(expected, output, namespace='default', connect=Connection)
    assert report['status'] == 'captured'
    assert events[:6] == ['fetch', 'write', 'fetch', 'write', 'fetch', 'write']
    assert events.index('writer-close') < events.index('reader-close') < events.index('connection-close')
    assert pq.read_table(output / 'comments.parquet').to_pylist() == rows
    assert pq.read_schema(output / 'comments.parquet').equals(schema, check_metadata=True)


@pytest.mark.parametrize('outcome', ['empty', 'reader-failure', 'writer-failure', 'schema-mismatch', 'interruption'])
def test_real_reader_sink_empty_or_failure_retains_authority(tmp_path, expected, logical_source, monkeypatch, outcome):
    connect, _, _ = logical_source
    real = capture.write_original_batches
    real_writer = pq.ParquetWriter

    if outcome == 'empty':
        monkeypatch.setattr(iceberg, '_snapshot_query', lambda *a, **k: 'SELECT * FROM logical_comments WHERE FALSE')
    elif outcome == 'schema-mismatch':
        monkeypatch.setattr(iceberg, '_snapshot_query',
                            lambda *a, **k: 'SELECT agency_code,comment_id,duplicate_comments,text_content FROM logical_comments')
    elif outcome == 'reader-failure':
        real_connect = connect
        closed = []

        class FailedReader:
            def __init__(self, reader):
                self.inner = reader
                self.schema = reader.schema

            def __enter__(self):
                return self

            def __exit__(self, *args):
                self.inner.close()
                closed.append('reader')

            def __iter__(self):
                yield self.inner.read_next_batch()
                raise RuntimeError('do-not-retain-private-error-token')

        class Relation:
            def __init__(self, inner):
                self.inner = inner

            def to_arrow_reader(self, batch_size):
                return FailedReader(self.inner.to_arrow_reader(batch_size=batch_size))

        class Connection:
            def __init__(self):
                self.inner = real_connect()

            def execute(self, *args, **kwargs):
                return self.inner.execute(*args, **kwargs)

            def sql(self, query):
                return Relation(self.inner.sql(query))

            def close(self):
                self.inner.close()
                closed.append('connection')

        connect = Connection
    else:
        class FailedWriter:
            def __init__(self, *args, **kwargs):
                self.inner = real_writer(*args, **kwargs)

            def __enter__(self):
                return self

            def write_batch(self, batch, *, row_group_size):
                if outcome == 'writer-failure':
                    raise OSError('do-not-retain-private-error-token')
                self.inner.write_batch(batch, row_group_size=row_group_size)

            def __exit__(self, *args):
                self.inner.close()

        if outcome == 'writer-failure':
            monkeypatch.setattr(pq, 'ParquetWriter', FailedWriter)
        else:
            # Real reader and writer first complete; simulate an interruption before caller success.
            def interrupted(*args, **kwargs):
                real(*args, **kwargs)
                raise KeyboardInterrupt('do-not-retain-private-error-token')
            monkeypatch.setattr(capture, 'write_original_batches', interrupted)
    output = tmp_path / outcome
    if outcome == 'empty':
        report = capture.capture(expected, output, namespace='default', connect=connect)
        assert report['status'] == 'captured' and report['writer']['rows'] == 0
        assert pq.read_schema(output / 'comments.parquet').names == [field['name'] for field in capture.fields(expected)]
        assert pq.read_metadata(output / 'comments.parquet').num_rows == 0
    else:
        with pytest.raises(RuntimeError, match='refused') as error:
            capture.capture(expected, output, namespace='default', connect=connect)
        retained = (output / 'RESULT.json').read_text()
        report = json.loads(retained)
        assert report['status'] == 'refused'
        assert 'do-not-retain-private-error-token' not in retained + str(error.value)
        assert error.value.__cause__ is None
        if (output / 'comments.parquet').exists():
            assert (output / 'comments.parquet').stat().st_mode & 0o222
        if outcome == 'reader-failure':
            assert closed == ['reader', 'connection']
            assert report['failedPhase'] == 'logical-snapshot-batch-read'
            assert report['writer']['rows'] == 4
        if outcome == 'writer-failure':
            assert report['failedPhase'] == 'logical-snapshot-batch-write'
            assert report['writer']['rows'] == 0 and report['writer']['maxBatchRows'] == 4


def test_wrong_runtime_pin_refuses_before_selected_query(tmp_path, expected, logical_source):
    connect, query_calls, _ = logical_source
    with pytest.raises(RuntimeError, match='source-runtime-pins'):
        capture.capture(expected, tmp_path / 'wrong-runtime', namespace='default', connect=connect,
                        expected_runtime={'duckdbVersion': 'wrong'})
    assert not query_calls


def test_complete_legacy_schema_literals_and_batch_boundaries(tmp_path, monkeypatch):
    names = ('comment_id', 'docket_id', 'agency_code', 'first_name', 'last_name', 'organization', 'category',
             'title', 'comment', 'document_type', 'posted_date', 'modify_date', 'receive_date', 'attachments_json',
             'text_content', 'text_extraction_status', 'pdf_extraction_results_json', 'comment_on_document_id',
             'comment_on_object_id', 'original_document_id', 'comment_reference_values_json', 'subtype',
             'duplicate_comments')
    expected = {'tableUuid': 'full-original', 'snapshotId': 123, 'schemaId': 6,
                'schemas': [{'schema-id': 6, 'fields': [
                    {'name': name, 'type': 'string', 'required': False, 'id': number}
                    for number, name in enumerate(names, 1)]}]}
    schema = pa.schema([(name, pa.string()) for name in names])
    rows = [{name: None if ordinal % 3 == 0 else '' for name in names} for ordinal in range(2001)]
    for ordinal, row in enumerate(rows):
        row.update(comment_id=None if ordinal % 3 == 0 else 'same-id', agency_code='EPA', duplicate_comments='007',
                   text_content='café\x00\x1b\n' + 'w' * 4096,
                   attachments_json='[{"literal":"007","escaped":"\\n"}]')
    original = pa.Table.from_pylist(rows, schema=schema)
    database = tmp_path / 'full.duckdb'
    with duckdb.connect(str(database)) as con:
        con.register('fixture', original)
        con.execute('CREATE TABLE original_comments AS SELECT * FROM fixture')
    monkeypatch.setattr(iceberg, '_table_metadata', lambda *a, **k: metadata(expected))
    monkeypatch.setattr(iceberg, '_snapshot_query', lambda *a, **k: 'SELECT * FROM original_comments')
    output = tmp_path / 'complete'
    report = capture.capture(expected, output, namespace='default',
                             connect=lambda: duckdb.connect(str(database), read_only=True))
    assert pq.read_table(output / 'comments.parquet').equals(original, check_metadata=True)
    assert report['writer']['rows'] == 2001 and report['writer']['batches'] == 3
    assert report['writer']['maxBatchRows'] == 1000
    assert report['actualSourceSettings']['threads'] == '4'
    assert report['actualSourceSettings']['memory_limit'] == '3.7 GiB'
    assert report['actualSourceSettings']['max_temp_directory_size'] == '29.8 GiB'
    assert report['actualSourceSettings']['preserve_insertion_order'] == 'true'
    assert report['startRuntime'] == report['endRuntime']


def test_changed_end_runtime_refuses_complete_file(tmp_path, expected, logical_source, monkeypatch):
    connect, _, _ = logical_source
    calls = 0
    real = capture.runtime_identity

    def changed(con):
        nonlocal calls
        calls += 1
        result = real(con)
        if calls == 2:
            result['duckdbBinarySha256'] = 'changed'
        return result

    monkeypatch.setattr(capture, 'runtime_identity', changed)
    output = tmp_path / 'changed-runtime'
    with pytest.raises(RuntimeError, match='fresh-end-metadata'):
        capture.capture(expected, output, namespace='default', connect=connect)
    report = json.loads((output / 'RESULT.json').read_text())
    assert report['status'] == 'refused'
    assert pq.read_metadata(output / 'comments.parquet').num_rows == 4
    assert (output / 'comments.parquet').stat().st_mode & 0o222
