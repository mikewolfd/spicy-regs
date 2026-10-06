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
