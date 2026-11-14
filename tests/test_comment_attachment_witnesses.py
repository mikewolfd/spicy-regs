"""Read-only transport guards, without making a source-value semantic verdict."""
import io
import json
from dataclasses import asdict
import struct
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
    monkeypatch.setattr(witness, 'read_sql', inventory)
    args = SimpleNamespace(native_snapshot={'table_uuid': 'actual', 'snapshot_id': 1, 'schema_id': 3}, workdir=tmp_path)
    report = {}
    with pytest.raises(ValueError, match='exceed admission'):
        witness.native(args, {'witnesses': []}, report)
    assert report['admission']['projected_columns_and_deletes_bytes'] == witness.MAX_NATIVE_COLUMNS + 1
    assert 'rows' not in report


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
