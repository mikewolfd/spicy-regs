"""The comments writer retains the exact catalog row attempts and subject order."""
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import comments_bulk, etl_bulk
from spicy_regs.etl_receipts import ReceiptContext, exact_json, validate_receipt_bundle
from spicy_regs.sources.regulatory_catalog import normalize_source_record
from spicy_regs.transforms.regulations_receipts import map_regulations_attempt, policy, write_records
from tests.duckdb_faults import FAILURES, assert_raised_as, fail_streams_after


def context(raw, ordinal):
    return ReceiptContext('g', f'row:{ordinal}', 'regulatory-catalog-native-v1', [{
        'source_id': 'source', 'source_uri': None, 'sha256': hashlib.sha256(exact_json(raw).encode()).hexdigest(),
        'locator': 'receipt.values.raw_source_record (canonical exact_json)', 'body_version': 'g'}])


def test_complete_comments_bundle_equals_catalog_row_writer(tmp_path, monkeypatch):
    rows = [
        {'comment_id': 'plain', 'comment': ' plain\u00a0text ', 'attachments_json': None, 'duplicate_comments': '007'},
        {'comment_id': 'html', 'comment': '<p>One&amp;#39;<br/>two</p>', 'attachments_json': '[]', 'duplicate_comments': '-3'},
        {'comment_id': 'nested', 'comment': 'é😀', 'attachments_json': '[{"title":"t","formats":[{"url":"u","format":"PDF","size":4}],"restrictReason":"r"}]'},
        {'comment_id': 'escaped', 'attachments_json': json.dumps([{'title': 'a\nb\t\x1bc\\d', 'formats': [{'url': 'u\nv\\w', 'format': 'P\tDF', 'size': 4}], 'restrictReason': '\x00'}])},
        {'comment_id': 'control', 'comment': '\x1btext', 'attachments_json': 'null'},
        {'comment_id': 'refused', 'attachments_json': '[{"unknown":"loss"}]'},
        {'comment_id': '\t', 'comment': None},
        {'comment_id': 'bad-integer', 'duplicate_comments': '1.1'},
        {'comment_id': 'empty-formats', 'attachments_json': '[null,{"formats":null,"attachment_id":"a"}]'},
    ]
    schema = pa.schema([(key, pa.string()) for key in sorted(set().union(*(row.keys() for row in rows)))])
    source = tmp_path / 'source.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source, row_group_size=4)
    rows = pq.read_table(source).to_pylist()
    def project(row):
        return normalize_source_record('comments', row)
    reference = write_records('comments', [(row, context(row, i)) for i, row in enumerate(rows)],
                              tmp_path / 'row', project=project)
    routed = []
    def attempt(raw, ordinal):
        routed.append(ordinal)
        return map_regulations_attempt('comments', raw, context(raw, ordinal), project=project)
    monkeypatch.setattr(comments_bulk, '_BATCH', 3)
    actual = tmp_path / 'subjects.parquet', tmp_path / 'receipts.parquet'
    count = comments_bulk.write_bundle(source, *actual, policy=policy('comments'), generation_id='g',
                                       source_label='source', row_attempt=attempt)
    assert count == len(routed)
    assert set(routed) >= {3, 4, 5, 6, 7}
    for expected, got in zip(reference, actual):
        assert pq.read_table(expected).equals(pq.read_table(got))
    validate_receipt_bundle({'comments': [actual[0]]}, [actual[1]], [policy('comments')], generation_id='g', bulk=False)
    etl_bulk.validate_bundle({'comments': [actual[0]]}, [actual[1]], [policy('comments')], generation_id='g')
    from spicy_regs.transforms.regulations_receipts import ReceiptInput, read_internal
    selected = ReceiptInput('comments', (actual[0],), actual[1], 'g')
    accepted = [raw for raw in rows if map_regulations_attempt('comments', raw, context(raw, 0), project=project)[0] is not None]
    assert list(read_internal(selected)) == [project(raw) for raw in accepted]
    restored = tmp_path/'restored.parquet'
    assert comments_bulk.restore_catalog(*actual, restored, generation_id='g') == len(accepted)
    from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS, TYPES
    restored_schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS['comments']])
    expected_restored = pa.Table.from_pylist([project(raw) for raw in accepted], schema=restored_schema)
    assert pq.read_table(restored).equals(expected_restored)


def test_comments_unknown_source_columns_refuse_bulk(tmp_path):
    source = tmp_path / 'unknown.parquet'
    pq.write_table(pa.table({'comment_id': ['c'], 'unclassified': ['value']}), source)
    with pytest.raises(etl_bulk.NotBulkEligible):
        comments_bulk.write_bundle(source, tmp_path/'subjects', tmp_path/'receipts', policy=policy('comments'),
                                   generation_id='g', source_label='source', row_attempt=lambda r, i: (None, {}))


@pytest.mark.parametrize('integer_count', [False, True])
@pytest.mark.parametrize('prefix', [0, 127, 255])
def test_large_rows_keep_exact_values_order_and_individual_output(tmp_path, monkeypatch, integer_count, prefix):
    rows = [
        {'comment_id': 'b', 'comment': 'x' * 63},  # Exactly 64 UTF-8 bytes.
        {'comment_id': 'u', 'comment': 'é' * 32},  # More bytes than characters.
        {'comment_id': 'sum', 'comment': 'x' * 31, 'text_content': 'y' * 31},
        {'comment_id': 'control', 'comment': '\x1bordinary'},
        {'comment_id': 'large', 'comment': '😀' * 100, 'duplicate_comments': 7 if integer_count else '007'},
        {'comment_id': 'refused', 'text_content': 'x' * 100, 'attachments_json': '{bad'},
        {'comment_id': 'last', 'comment': None},
    ]
    rows = [{'comment_id': f'p{i}'} for i in range(prefix)] + rows
    schema = pa.schema([(name, pa.int64() if name == 'duplicate_comments' and integer_count else pa.string())
                        for name in sorted(set().union(*(row.keys() for row in rows)))])
    source = tmp_path / 'source.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source, row_group_size=3)
    rows = pq.read_table(source).to_pylist()
    def project(row):
        return normalize_source_record('comments', row)
    reference = write_records('comments', [(row, context(row, i)) for i, row in enumerate(rows)],
                              tmp_path / 'reference', project=project)
    routed = []
    def attempt(raw, ordinal):
        assert raw == rows[ordinal]
        routed.append(ordinal)
        return map_regulations_attempt('comments', raw, context(raw, ordinal), project=project)
    segments = []
    real_segments = comments_bulk._segments
    def checked_segments(table):
        for part in real_segments(table):
            if any(part['_large'].to_pylist()):
                assert len(part) == 1
                segments.extend(part['_ordinal'].to_pylist())
            yield part
    monkeypatch.setattr(comments_bulk, '_BULK_ROW_BYTES', 64)
    monkeypatch.setattr(comments_bulk, '_segments', checked_segments)
    actual = tmp_path / 'subjects.parquet', tmp_path / 'receipts.parquet'
    assert comments_bulk.write_bundle(source, *actual, policy=policy('comments'), generation_id='g',
                                      source_label='source', row_attempt=attempt) == 5
    assert routed == [prefix + i for i in (1, 2, 3, 4, 5)]
    assert segments == [prefix + i for i in (1, 2, 4, 5)]
    for expected, got in zip(reference, actual):
        assert pq.read_table(expected).equals(pq.read_table(got))
    groups = pq.ParquetFile(actual[1]).metadata
    sizes = [groups.row_group(i).num_rows for i in range(groups.num_row_groups)]
    assert sizes.count(1) >= 2  # the oversized rows keep receipt row groups of their own
    with pq.ParquetFile(actual[1]) as receipts:
        refused_groups = [receipts.read_row_group(i, columns=['outcome']) for i in range(receipts.num_row_groups)
                          if 'refused' in receipts.read_row_group(i, columns=['outcome']).column(0).to_pylist()]
    assert len(refused_groups) == 1
    assert refused_groups[0].num_rows == 1  # fetching an ordinary receipt cannot load the large refused original
    validate_receipt_bundle({'comments': [actual[0]]}, [actual[1]], [policy('comments')], generation_id='g', bulk=False)


def test_restore_failure_keeps_existing_destination(tmp_path, monkeypatch):
    raw = {'comment_id': 'c', 'comment': '<p>literal</p>'}
    pair = write_records('comments', [(raw, context(raw, 0))], tmp_path / 'pair',
                         project=lambda row: normalize_source_record('comments', row))
    destination = tmp_path / 'existing.parquet'
    pq.write_table(pa.table({'sentinel': ['prior successful file']}), destination)
    before = destination.read_bytes()
    from spicy_regs.transforms import regulations_shape
    real = regulations_shape.shape_record
    def altered(dataset, row):
        shaped = real(dataset, row)
        shaped['comment_text'] = 'different from selected subject'
        return shaped
    monkeypatch.setattr(regulations_shape, 'shape_record', altered)
    with pytest.raises(etl_bulk.NotBulkEligible, match='processor input differs'):
        comments_bulk.restore_catalog(*pair, destination, generation_id='g')
    assert destination.read_bytes() == before
    assert not list(tmp_path.glob('comments-restore-*'))


@pytest.mark.parametrize('failure, invalid', FAILURES)
def test_duckdb_failure_after_partial_restore_fails_fast_and_keeps_existing_destination(
        tmp_path, monkeypatch, failure, invalid):
    rows = [{'comment_id': f'c{i}', 'comment': f'text {i}'} for i in range(5)]
    pair = write_records('comments', [(row, context(row, i)) for i, row in enumerate(rows)], tmp_path / 'pair',
                         project=lambda row: normalize_source_record('comments', row))
    destination = tmp_path / 'existing.parquet'
    destination.write_bytes(b'preserve')
    monkeypatch.setattr(comments_bulk, '_BATCH', 2)
    delivered = fail_streams_after(monkeypatch, failure)
    with pytest.raises(etl_bulk.BulkValidationError) as caught:
        comments_bulk.restore_catalog(*pair, destination, generation_id='g')
    assert delivered == [2]
    assert_raised_as(caught, failure, invalid)
    assert destination.read_bytes() == b'preserve'
    assert not list(tmp_path.glob('comments-restore-*'))


def test_mapper_output_the_schema_cannot_hold_goes_to_the_row_reader_not_a_failure(tmp_path, monkeypatch):
    raw = {'comment_id': 'c', 'comment': 'text'}
    pair = write_records('comments', [(raw, context(raw, 0))], tmp_path / 'pair',
                         project=lambda row: normalize_source_record('comments', row))
    from spicy_regs.transforms import regulations_shape
    real = regulations_shape.shape_record
    monkeypatch.setattr(regulations_shape, 'shape_record', lambda dataset, row: dict(real(dataset, row), comment_text=5))
    with pytest.raises(etl_bulk.NotBulkEligible, match='does not fit its schema'):
        comments_bulk.restore_catalog(*pair, tmp_path / 'out.parquet', generation_id='g')


def test_arrow_memory_failure_converting_a_batch_fails_fast(tmp_path, monkeypatch):
    raw = {'comment_id': 'c', 'comment': 'text'}
    pair = write_records('comments', [(raw, context(raw, 0))], tmp_path / 'pair',
                         project=lambda row: normalize_source_record('comments', row))
    failure = pa.ArrowMemoryError('injected')

    def exhausted(*args, **kwargs):
        raise failure

    from types import SimpleNamespace
    table_stub = SimpleNamespace(from_batches=pa.Table.from_batches, from_pylist=exhausted)
    monkeypatch.setattr(etl_bulk, 'pa', SimpleNamespace(**{**vars(pa), 'Table': table_stub}))
    with pytest.raises(etl_bulk.BulkValidationError) as caught:
        comments_bulk.restore_catalog(*pair, tmp_path / 'out.parquet', generation_id='g')
    assert_raised_as(caught, failure, False)


@pytest.mark.parametrize('batch_size', [1, 4])
def test_fresh_deep_attachment_uses_reference_order_and_error(tmp_path, monkeypatch, batch_size):
    rows = [
        {'comment_id': 'earlier-accepted', 'comment': '\x1bpreserve', 'attachments_json': '[]'},
        {'comment_id': 'earlier-refused', 'attachments_json': '{bad'},
        {'comment_id': 'deep', 'attachments_json': '[' * 20000 + ']' * 20000},
        {'comment_id': 'later', 'comment': '\x1blater', 'attachments_json': '[]'},
    ]
    source = tmp_path / 'source.parquet'
    schema = pa.schema([(name, pa.string()) for name in ('comment_id', 'comment', 'attachments_json')])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source)
    rows = pq.read_table(source).to_pylist()
    reference_order = []
    def reference_project(row):
        reference_order.append(row['comment_id'])
        return normalize_source_record('comments', row)
    with pytest.raises(RecursionError) as reference:
        write_records('comments', [(row, context(row, i)) for i, row in enumerate(rows)],
                      tmp_path / 'reference', project=reference_project)
    routed = []
    outcomes = []
    def attempt(raw, ordinal):
        routed.append((ordinal, raw['comment_id']))
        result = map_regulations_attempt('comments', raw, context(raw, ordinal),
                                         project=lambda row: normalize_source_record('comments', row))
        outcomes.append(result[1]['outcome'])
        return result
    monkeypatch.setattr(comments_bulk, '_BATCH', batch_size)
    monkeypatch.setattr(comments_bulk, '_BULK_ROW_BYTES', 64)
    with pytest.raises(RecursionError) as actual:
        comments_bulk.write_bundle(source, tmp_path / 'subjects.parquet', tmp_path / 'receipts.parquet',
                                   policy=policy('comments'), generation_id='g', source_label='source',
                                   row_attempt=attempt)
    assert str(actual.value) == str(reference.value)
    assert routed == list(enumerate(reference_order))
    assert reference_order == ['earlier-accepted', 'earlier-refused', 'deep']
    assert outcomes == ['accepted', 'refused']


def test_restore_catalog_uses_single_bounded_ordinal_admission_without_duckdb_payloads(tmp_path, monkeypatch):
    rows = [
        {'comment_id': f'c{i}', 'comment': f'<p>body {i} {"x" * 2048}</p>', 'duplicate_comments': str(i)}
        for i in range(7)
    ] + [{'comment_id': 'bad-attachment', 'attachments_json': '{bad'}]
    subjects, receipts = write_records(
        'comments', [(row, context(row, i)) for i, row in enumerate(rows)], tmp_path / 'pair',
        project=lambda row: normalize_source_record('comments', row))
    # Reverse the receipt file across small row groups so receipt ordinals differ from subject ordinals.
    receipt_table = pq.read_table(receipts)
    reversed_indices = pa.array(list(reversed(range(receipt_table.num_rows))), type=pa.int64())
    pq.write_table(receipt_table.take(reversed_indices), receipts, row_group_size=2)
    pq.write_table(pq.read_table(subjects), subjects, row_group_size=3)

    connections = []
    real_bulk_connection = etl_bulk.bulk_connection
    from contextlib import contextmanager

    @contextmanager
    def spy_connection(*args, **kwargs):
        connections.append(kwargs)
        with real_bulk_connection(*args, **kwargs) as (con, root):
            yield con, root
            columns = {row[0] for row in con.execute('DESCRIBE receipts').fetchall()}
            assert 'processing_json' not in columns

    validated_calls = []
    real_validated = etl_bulk._validated_bundle

    @contextmanager
    def spy_validated(*args, **kwargs):
        validated_calls.append(kwargs)
        with real_validated(*args, **kwargs) as con:
            yield con

    monkeypatch.setattr(etl_bulk, 'bulk_connection', spy_connection)
    monkeypatch.setattr(etl_bulk, '_validated_bundle', spy_validated)
    monkeypatch.setattr(comments_bulk, '_BATCH', 2)

    restored = tmp_path / 'restored.parquet'
    assert comments_bulk.restore_catalog(subjects, receipts, restored, generation_id='g') == 7
    assert connections == [{'threads': 1, 'memory_limit': '4GB'}]
    assert len(validated_calls) == 1
    assert validated_calls[0]['retain_processing'] is False
    assert validated_calls[0]['bounded_insert'] is True

    from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS, TYPES
    restored_schema = pa.schema([(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS['comments']])
    expected = pa.Table.from_pylist([normalize_source_record('comments', row) for row in rows[:7]], schema=restored_schema)
    assert pq.read_table(restored).equals(expected)


def test_restore_catalog_refuses_mutated_source_without_row_fallback(tmp_path, monkeypatch):
    rows = [{'comment_id': f'c{i}', 'comment': f'text {i}'} for i in range(4)]
    subjects, receipts = write_records(
        'comments', [(row, context(row, i)) for i, row in enumerate(rows)], tmp_path / 'pair',
        project=lambda row: normalize_source_record('comments', row))
    destination = tmp_path / 'existing.parquet'
    destination.write_bytes(b'keep')

    from spicy_regs import receipt_history
    real_take = receipt_history._take_prior_rows

    def mutate_then_take(*args, **kwargs):
        result = real_take(*args, **kwargs)
        receipts.write_bytes(receipts.read_bytes() + b' ')
        return result

    monkeypatch.setattr(receipt_history, '_take_prior_rows', mutate_then_take)
    with pytest.raises(ValueError, match='Ordinal processing source changed after selection') as caught:
        comments_bulk.restore_catalog(subjects, receipts, destination, generation_id='g')
    assert not isinstance(caught.value, etl_bulk.NotBulkEligible)
    assert destination.read_bytes() == b'keep'

