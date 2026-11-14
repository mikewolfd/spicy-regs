"""Read-only transport guards, without making a source-value semantic verdict."""
import io
import json
from dataclasses import asdict
import faulthandler
import os
import resource
import struct
import subprocess
import sys
from types import SimpleNamespace

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import read_comment_attachment_witnesses as witness


def example_parquet():
    out = io.BytesIO()
    pq.write_table(pa.table({'comment_id': ['a', 'b', 'y', 'z'], 'agency_code': ['A'] * 4,
                            'modify_date': ['v'] * 4, 'attachments': [None] * 4,
                            'unrelated': ['never project'] * 4}), out, row_group_size=2)
    return out.getvalue()


def test_footer_and_column_admission_use_actual_metadata():
    data = example_parquet()
    calls = []

    def read(start, end):
        calls.append((start, end))
        return data[start:end + 1]

    metadata = witness.footer(len(data), read)
    assert len(calls) == 2
    assert witness.candidate_groups(metadata, ['b']) == [0]
    assert witness.candidate_groups(metadata, ['middle']) == []
    compressed, uncompressed = witness.projected_bytes(metadata, [0])
    group = metadata.row_group(0)
    assert compressed == sum(group.column(i).total_compressed_size for i in range(4))
    assert uncompressed == sum(group.column(i).total_uncompressed_size for i in range(4))
    repeated = witness.footer(len(data), read)
    assert witness.physical_schema_text(metadata) == witness.physical_schema_text(repeated)
    assert '<pyarrow.' not in witness.physical_schema_text(metadata)
    assert witness.sha(witness.physical_schema_text(metadata).encode()) == witness.sha(
        witness.physical_schema_text(repeated).encode())


def test_oversized_footer_refuses_before_second_request():
    calls = []

    def read(start, end):
        calls.append((start, end))
        return struct.pack('<I', witness.MAX_FOOTER + 1) + b'PAR1'

    with pytest.raises(ValueError, match='Footer exceeds'):
        witness.footer(witness.MAX_FOOTER + 100, read)
    assert len(calls) == 1


def test_unknown_statistics_are_included():
    out = io.BytesIO()
    pq.write_table(pa.table({'comment_id': ['a']}), out, write_statistics=False)
    metadata = pq.ParquetFile(io.BytesIO(out.getvalue())).metadata
    assert witness.candidate_groups(metadata, ['z']) == [0]


def test_range_budget_refuses_before_get_and_checks_version():
    data = example_parquet()
    calls = []

    def reply(request):
        calls.append(request.method)
        return httpx.Response(200, headers={'ETag': '"a"', 'Content-Length': str(len(data))})

    with httpx.Client(transport=httpx.MockTransport(reply)) as client:
        stream = witness.PublicRanges(client, 'https://data.spicygov.ai/a', {'bytes': len(data), 'etag': '"a"'}, witness.ByteBudget(2))
        with pytest.raises(ValueError, match='budget exceeded'):
            stream._get(0, 2)
        assert calls == ['HEAD']
        with pytest.raises(ValueError, match='ETag differs'):
            witness.PublicRanges(client, 'https://data.spicygov.ai/a', {'bytes': len(data), 'etag': '"b"'}, witness.ByteBudget(20))


def test_ignored_range_is_refused_without_retry():
    calls = []

    def reply(request):
        calls.append(request.method)
        return httpx.Response(200, headers={'ETag': '"a"', 'Content-Length': '100'})

    with httpx.Client(transport=httpx.MockTransport(reply)) as client:
        stream = witness.PublicRanges(client, 'https://data.spicygov.ai/a', {'bytes': 100, 'etag': '"a"'}, witness.ByteBudget(20))
        with pytest.raises(ValueError, match='exact pinned byte range'):
            stream._get(0, 7)
    assert calls == ['HEAD', 'GET']


def test_actual_ranges_decode_selected_columns():
    data = example_parquet()

    def reply(request):
        headers = {'ETag': '"a"', 'Content-Length': str(len(data))}
        if request.method == 'HEAD':
            return httpx.Response(200, headers=headers)
        assert request.headers['if-match'] == '"a"'
        start, end = map(int, request.headers['range'].removeprefix('bytes=').split('-'))
        part = data[start:end + 1]
        return httpx.Response(206, content=part, headers={**headers, 'Content-Length': str(len(part)),
                              'Content-Range': f'bytes {start}-{end}/{len(data)}'})

    budget = witness.ByteBudget(100_000)
    with httpx.Client(transport=httpx.MockTransport(reply)) as client:
        with witness.PublicRanges(client, 'https://data.spicygov.ai/a', {'bytes': len(data), 'etag': '"a"'}, budget) as stream:
            metadata = witness.footer(len(data), stream._get)
            parquet = pq.ParquetFile(stream, metadata=metadata, pre_buffer=False, buffer_size=0)
            rows = parquet.read_row_group(0, columns=witness.COLUMNS).to_pylist()
    assert [r['comment_id'] for r in rows] == ['a', 'b']
    assert all(set(row) == set(witness.COLUMNS) for row in rows)
    assert budget.used == sum(event['bytes'] for event in budget.events)


@pytest.mark.parametrize('sql', ['DELETE FROM x', 'SELECT 1; DELETE FROM x', 'COPY (SELECT 1) TO \'x\''])
def test_mutating_or_multiple_sql_never_reaches_connection(sql):
    class NeverConnect:
        def execute(self, *_):
            pytest.fail('A refused SQL statement reached the connection')

    with pytest.raises(ValueError, match='one observation'):
        witness.read_sql(NeverConnect(), sql)


def test_single_describe_and_select_are_observations():
    with duckdb.connect() as con:
        assert witness.read_sql(con, 'SELECT ? AS answer', [7]) == [{'answer': 7}]
        assert witness.read_sql(con, 'DESCRIBE (SELECT 1 AS answer)')[0]['column_name'] == 'answer'


@pytest.mark.parametrize('value', [None, {}, {'table_uuid': 'u', 'snapshot_id': True, 'schema_id': 0}])
def test_snapshot_pin_refuses_missing_or_boolean_identity(value):
    with pytest.raises(ValueError):
        witness.subject_pin(value)


def test_native_snapshot_mismatch_precedes_file_reads(monkeypatch, tmp_path):
    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, sql):
            assert sql.startswith('SET ')

    monkeypatch.setattr(witness.iceberg, '_connect', Connection)
    monkeypatch.setattr(witness.iceberg, '_read_snapshot', lambda *_, **__: witness.iceberg.CatalogSnapshot('actual', 1, 3))
    monkeypatch.setattr(witness.r2, 'verified_target_client', lambda: (object(), 'spicy-regs'))
    monkeypatch.setattr(witness, 'runtime_identity', lambda _: {'fixture': True})
    args = SimpleNamespace(native_snapshot={'table_uuid': 'expected', 'snapshot_id': 1, 'schema_id': 3}, workdir=tmp_path)
    with pytest.raises(ValueError, match='snapshot differs'):
        witness.native(args, {'witnesses': []}, {})


def test_native_byte_refusal_precedes_payload_query(monkeypatch, tmp_path):
    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, sql):
            assert sql.startswith('SET ')

    class HeadsOnly:
        def head_object(self, **_):
            return {'ContentLength': witness.MAX_NATIVE_COLUMNS + 1, 'ETag': '"delete"'}

    def inventory(_con, sql, _parameters):
        assert sql.startswith('SELECT DISTINCT file_path,manifest_content,content FROM iceberg_metadata')
        assert 'snapshot_from_id=?' in sql and _parameters[-1] == 1
        return [{'file_path': 's3://spicy-regs/delete.parquet', 'manifest_content': 'DELETE', 'content': 'POSITION_DELETES'}]

    monkeypatch.setattr(witness.iceberg, '_connect', Connection)
    monkeypatch.setattr(witness.iceberg, '_read_snapshot', lambda *_, **__: witness.iceberg.CatalogSnapshot('actual', 1, 3))
    monkeypatch.setattr(witness.r2, 'verified_target_client', lambda: (HeadsOnly(), 'spicy-regs'))
    monkeypatch.setattr(witness, 'runtime_identity', lambda _: {'fixture': True})
    monkeypatch.setattr(witness, 'read_sql', inventory)
    args = SimpleNamespace(native_snapshot={'table_uuid': 'actual', 'snapshot_id': 1, 'schema_id': 3}, workdir=tmp_path)
    report = {}
    with pytest.raises(ValueError, match='exceed admission'):
        witness.native(args, {'witnesses': []}, report)
    assert report['admission']['projected_columns_and_deletes_bytes'] == witness.MAX_NATIVE_COLUMNS + 1
    assert 'rows' not in report


def test_binary_identity_is_bounded_and_checks_actual_bytes(tmp_path):
    path = tmp_path / 'engine.so'
    path.write_bytes(b'installed-binary-control')
    budget = witness.ByteBudget(100)
    assert witness.binary_identity(path, budget) == {
        'path': str(path), 'bytes': 24, 'sha256': witness.sha(path.read_bytes())}
    assert budget.used == 24
    with pytest.raises(ValueError, match='budget exceeded'):
        witness.binary_identity(path, witness.ByteBudget(1))
    with pytest.raises(ValueError, match='local installed path'):
        witness.binary_identity('https://example.invalid/engine.so', witness.ByteBudget(100))


def test_runtime_identity_uses_installed_engine_and_local_metadata(monkeypatch):
    monkeypatch.setenv('R2_CATALOG_TOKEN', 'credential-must-not-appear')
    with duckdb.connect() as con:
        result = witness.runtime_identity(con)
    assert result['engine_version'][0]['library_version'] == duckdb.__version__ or (
        result['engine_version'][0]['library_version'] == 'v' + duckdb.__version__)
    assert len(result['engine']['sha256']) == 64
    assert result['extensions']
    assert all('binary' in row for row in result['extensions'])
    assert result['local_binary_bytes_hashed'] >= result['engine']['bytes']
    assert result['python_fatal_stacks'] == faulthandler.is_enabled()
    assert result['core_dump_limit_bytes'] == resource.getrlimit(resource.RLIMIT_CORE)[0]
    assert result['native_backtrace'] is False
    assert 'credential-must-not-appear' not in json.dumps(result)


def test_script_main_enables_fatal_stacks_and_disables_core_in_its_child():
    # --help exits before credentials, catalog connections, or any source read.
    code = '''import faulthandler, json, resource, runpy, sys
sys.argv = [sys.argv[1], '--help']
before = resource.getrlimit(resource.RLIMIT_CORE)
fatal_before = faulthandler.is_enabled()
runpy.run_path(sys.argv[0], run_name='import_probe')
assert resource.getrlimit(resource.RLIMIT_CORE) == before
assert faulthandler.is_enabled() == fatal_before
try:
    runpy.run_path(sys.argv[0], run_name='__main__')
except SystemExit as error:
    assert error.code == 0
print(json.dumps({'fatal': faulthandler.is_enabled(), 'core': resource.getrlimit(resource.RLIMIT_CORE)}))
'''
    before = resource.getrlimit(resource.RLIMIT_CORE)
    env = {key: value for key, value in os.environ.items() if key in {'PATH', 'PYTHONPATH', 'LANG', 'TMPDIR'}}
    result = subprocess.run([sys.executable, '-B', '-c', code, witness.__file__],
                            capture_output=True, text=True, timeout=15, check=True, env=env)
    assert json.loads(result.stdout.splitlines()[-1]) == {'fatal': True, 'core': [0, 0]}
    assert resource.getrlimit(resource.RLIMIT_CORE) == before


def test_native_payload_phase_and_field_ids_survive_failure(monkeypatch, tmp_path):
    out = io.BytesIO()
    schema = pa.schema([pa.field('comment_id', pa.string(), metadata={b'PARQUET:field_id': b'1'}),
                        pa.field('agency_code', pa.string()), pa.field('modify_date', pa.string()),
                        pa.field('attachments', pa.list_(pa.struct([pa.field('title', pa.string())])))])
    pq.write_table(pa.Table.from_pylist([{'comment_id': 'A', 'agency_code': 'FWS', 'modify_date': 'v',
                                       'attachments': [{'title': 'retained'}]}], schema=schema), out)
    data = out.getvalue()
    requests = []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *_):
            pass

        def execute(self, sql):
            assert sql.startswith('SET ')

    class LocalObject:
        def head_object(self, **_):
            return {'ContentLength': len(data), 'ETag': '"file"'}

        def get_object(self, **kwargs):
            requests.append(kwargs)
            assert kwargs['IfMatch'] == '"file"'
            start, end = map(int, kwargs['Range'].removeprefix('bytes=').split('-'))
            return {'Body': io.BytesIO(data[start:end + 1]), 'ETag': '"file"',
                    'ContentRange': f'bytes {start}-{end}/{len(data)}'}

    def observe(_con, sql, _parameters=()):
        if sql.startswith('SELECT DISTINCT file_path'):
            return [{'file_path': 's3://spicy-regs/file.parquet', 'manifest_content': 'DATA', 'content': 'DATA'}]
        if sql.startswith('DESCRIBE'):
            return [{'column_name': 'attachments', 'column_type': 'STRUCT(title VARCHAR)[]'}]
        assert sql.startswith('SELECT comment_id,agency_code,modify_date,attachments')
        saved = json.loads((tmp_path / 'observation.json').read_text())
        assert saved['phase'] == 'native_payload'
        assert saved['query']['sql'] == sql
        assert saved['runtime'] == {'fixture': True}
        assert saved['subject_after_admission'] == {'table_uuid': 'actual', 'snapshot_id': 1, 'schema_id': 3}
        assert any('field_id=1 comment_id' in value for value in saved['physical_schemas'].values())
        assert len(requests) == 2  # Only the original footer tail and body; diagnostics add no GET.
        raise RuntimeError('controlled payload failure')

    monkeypatch.setattr(witness.iceberg, '_connect', Connection)
    monkeypatch.setattr(witness.iceberg, '_read_snapshot', lambda *_, **__: witness.iceberg.CatalogSnapshot('actual', 1, 3))
    monkeypatch.setattr(witness.iceberg, '_snapshot_query', lambda *_, **__: 'SELECT * FROM pinned_fixture')
    monkeypatch.setattr(witness.r2, 'verified_target_client', lambda: (LocalObject(), 'spicy-regs'))
    monkeypatch.setattr(witness, 'runtime_identity', lambda _: {'fixture': True})
    monkeypatch.setattr(witness, 'read_sql', observe)
    args = SimpleNamespace(native_snapshot={'table_uuid': 'actual', 'snapshot_id': 1, 'schema_id': 3}, workdir=tmp_path)
    with pytest.raises(RuntimeError, match='controlled payload failure'):
        witness.native(args, {'witnesses': [{'comment_id': 'A', 'agency_code': 'FWS', 'parent_modify_date': 'v'}]}, {})
    assert json.loads((tmp_path / 'observation.json').read_text())['phase'] == 'native_payload'


@pytest.mark.parametrize('conflicting_in_file_agency', [False, True])
def test_full_public_transport_with_real_publisher_partition_layout(monkeypatch, tmp_path, conflicting_in_file_agency):
    from spicy_regs.duckdb_settings import ExportResources
    from spicy_regs.pipelines import comments_mirror
    from spicy_regs.transforms.partition_comments import partition_comments

    version = '2026-01-01T00:00:00Z'
    controls = [{'comment_id': f'A-2026-0001-{i:04}', 'agency_code': 'A', 'docket_id': 'A-2026-0001',
                 'posted_date': version, 'modify_date': version,
                 'attachments': [{'attachment_id': f'controlled-{i}', 'title': 'transport control', 'formats': None}]}
                for i in range(1, 8)]
    pq.write_table(pa.Table.from_pylist(controls), tmp_path / 'comments.parquet')
    # Exercise the maintained publisher's physical layout, which omits agency_code.
    partition_comments(tmp_path, resources=ExportResources(memory='256MB', threads=1))
    key = 'comments/agency/agency_code=A/part-0.parquet'
    path = tmp_path / key
    physical = pq.ParquetFile(path).read()
    assert 'agency_code' not in physical.schema.names
    if conflicting_in_file_agency:
        pq.write_table(physical.append_column('agency_code', pa.array(['B'] * 7)), path)
    data = path.read_bytes()
    snapshot = witness.iceberg.CatalogPairSnapshot('subject', 10, 3, witness.iceberg.CatalogSnapshot('receipts', 8, 0))
    generation = {'artifactDigest': 'sha256:' + 'a' * 64}
    descriptor = {'bytes': len(data), 'etag': '"agency"', 'rows': 7, 'sha256': witness.sha(data)}
    receipt = {'source': asdict(snapshot), 'generation': generation,
               'files': {key: descriptor, 'comments.parquet': {'bytes': 1, 'etag': '"m"', 'rows': 7, 'sha256': 'b' * 64},
                         'comments_index.parquet': {'bytes': 1, 'etag': '"i"', 'rows': 1, 'sha256': 'c' * 64}}}
    receipt_bytes = json.dumps(receipt).encode()
    index_bytes = json.dumps({'families': {'comments': generation}}).encode()
    requests = []

    def reply(request):
        requests.append((request.method, request.url.path))
        assert request.method in {'HEAD', 'GET'}
        assert request.headers['user-agent'] == witness.UA
        if request.url.path == '/comments-publication.json':
            return httpx.Response(200, content=receipt_bytes)
        if request.url.path == '/publication.v2.json':
            return httpx.Response(200, content=index_bytes)
        assert request.url.path == '/' + key  # A base-URL HEAD is a consumer bug.
        headers: dict[str, str] = {'ETag': '"agency"', 'Content-Length': str(len(data))}
        if request.method == 'HEAD':
            return httpx.Response(200, headers=headers)
        start, end = map(int, request.headers['range'].removeprefix('bytes=').split('-'))
        assert request.headers['if-match'] == descriptor['etag']
        body = data[start:end + 1]
        return httpx.Response(206, content=body, headers={**headers, 'Content-Length': str(len(body)),
                              'Content-Range': f'bytes {start}-{end}/{len(data)}'})

    real_client = httpx.Client
    monkeypatch.setattr(witness.httpx, 'Client', lambda **kwargs: real_client(transport=httpx.MockTransport(reply), **kwargs))
    # Keep the real receipt validator but lower its production population floor for this controlled transport.
    monkeypatch.setattr(comments_mirror, 'MIN_EXPECTED_ROWS', 1)
    work = tmp_path / 'observations'
    work.mkdir()
    args = SimpleNamespace(publication_receipt_sha256=witness.sha(receipt_bytes), generation_digest=generation['artifactDigest'],
                           native_snapshot=asdict(snapshot.subject()), workdir=work)
    config = {'witnesses': [{'comment_id': row['comment_id'], 'agency_code': 'A', 'parent_modify_date': version} for row in controls]}
    report = {}
    if conflicting_in_file_agency:
        with pytest.raises(ValueError, match='In-file agency conflicts'):
            witness.public(args, config, report)
    else:
        witness.public(args, config, report)
        member = report['members'][0]
        assert len(member['rows']) == 7 and not member['other_versions']
        assert all('agency_code' not in row for row in member['rows'])  # Preserve physical payload.
        assert member['partition_values'] == {'agency_code': 'A'}
        assert member['agency_identity_source'] == 'verified publication member path'
        assert ('HEAD', '/' + key) == requests[-3]
        assert report['range_budget']['used'] > 0
