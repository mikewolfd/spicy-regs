"""Regulatory SQL comparison accepts exactly what the established mapper comparison accepts."""
from contextlib import contextmanager
import datetime as dt

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import conversion_reads
from spicy_regs.conversion_reads import _FamilyReader, _open_family_reader
from spicy_regs.transforms.regulations_receipts import policy, write_held_dataset
from tests.test_regulatory_attributes_bulk import _table, DISPLAY


@contextmanager
def admitted(tmp_path, dataset, table):
    retained = tmp_path / 'original.parquet'
    pq.write_table(table, retained)
    subject, receipts = write_held_dataset(dataset, retained, tmp_path / 'native', generation_id='g', bulk=True)
    with _open_family_reader({dataset: [subject]}, [receipts], [policy(dataset)], generation_id='g', guard=lambda: None) as reader:
        yield reader, retained, subject


def verdicts(reader, dataset, original, monkeypatch):
    proof, real = [], _FamilyReader._bulk_regulatory_equal

    def spy(*args):
        proof.append(real(*args))
        return proof[-1]

    def run():
        try:
            reader.compare_original(dataset, original, source_schema=pq.read_schema(original))
        except ValueError as error:
            return str(error)

    with monkeypatch.context() as patch:
        patch.setattr(_FamilyReader, '_bulk_regulatory_equal', spy)
        bulk = run()
        patch.setattr(_FamilyReader, '_bulk_regulatory_equal', lambda *a: False)
        reference = run()
    return proof[0] if proof else None, bulk, reference


@pytest.mark.parametrize('dataset, rows', [
    ('comment_attributes', [{'comment_id': 'one', 'city': 'plain', 'page_count': 2,
                             'postmark_date': dt.datetime(2020, 2, 29, 12, 3, 4, 5, tzinfo=dt.timezone.utc),
                             'display_properties_json': DISPLAY}, {'comment_id': 'two', 'withdrawn': True}]),
    ('docket_attributes', [{'docket_id': 'one', 'keywords': ['repeat', None, 'repeat'], 'display_properties_json': DISPLAY},
                           {'docket_id': 'two', 'keywords': []}]),
    ('document_attributes', [{'document_id': 'one', 'authors': ['a', None], 'allow_late_comments': False,
                              'display_properties_json': DISPLAY}, {'document_id': 'two', 'authors': []}]),
    ('dockets', [{'docket_id': 'one', 'title': 'plain'}, {'docket_id': 'two', 'title': None}]),
    ('documents', [{'document_id': 'one', 'withdrawn': 'True', 'attachments_json': '[null]'},
                    {'document_id': 'two', 'withdrawn': 'false', 'attachments_json': '[]'}]),
])
def test_regulatory_bulk_proof_matches_all_supported_source_shapes(tmp_path, monkeypatch, dataset, rows):
    table = _table(dataset, rows).replace_schema_metadata({b'placement': b'original\x00\xff'})
    with admitted(tmp_path, dataset, table) as (reader, retained, _):
        assert verdicts(reader, dataset, retained, monkeypatch) == (True, None, None)


@pytest.mark.parametrize('change', ['value', 'null', 'order', 'repeat', 'extra', 'missing', 'metadata', 'field', 'type'])
def test_regulatory_changes_keep_the_row_comparators_refusal(tmp_path, monkeypatch, change):
    dataset = 'comment_attributes'
    table = _table(dataset, [{'comment_id': 'one', 'city': 'a'}, {'comment_id': 'two', 'city': 'b'}])
    with admitted(tmp_path, dataset, table) as (reader, retained, _):
        rows = table.to_pylist()
        if change == 'value':
            rows[0]['city'] = 'different'
        elif change == 'null':
            rows[0]['city'] = None
        elif change == 'order':
            rows.reverse()
        elif change == 'repeat':
            rows[1] = rows[0]
        elif change == 'extra':
            rows.append(rows[0])
        elif change == 'missing':
            rows.pop()
        changed = pa.Table.from_pylist(rows, schema=table.schema)
        if change == 'metadata':
            changed = changed.replace_schema_metadata({b'different': b'yes'})
        elif change == 'field':
            changed = changed.drop_columns(['city'])
        elif change == 'type':
            changed = changed.cast(pa.schema([f.with_type(pa.large_string()) if f.name == 'city' else f for f in table.schema]))
        pq.write_table(changed, retained)
        proof, bulk, reference = verdicts(reader, dataset, retained, monkeypatch)
        assert proof is not True and bulk == reference and reference is not None


def test_sparse_regulatory_exceptions_replay_only_their_rows_across_batches(tmp_path, monkeypatch):
    from spicy_regs import etl_bulk
    from spicy_regs.transforms import regulations_receipts
    dataset = 'comment_attributes'
    rows = [{'comment_id': str(i), 'city': 'plain'} for i in range(73)]
    for i in (7, 62):
        rows[i]['city'] = 'escape\x1b'
    with admitted(tmp_path, dataset, _table(dataset, rows)) as (reader, retained, _):
        monkeypatch.setattr(conversion_reads, '_GROUP_ROWS', 11)
        monkeypatch.setattr(etl_bulk, '_BATCH', 5)
        seen, process = [], regulations_receipts._processor_inputs

        def observe(name, selected):
            selected = list(selected)
            seen.extend(row['comment_id'] for row in selected)
            return process(name, selected)

        monkeypatch.setattr(regulations_receipts, '_processor_inputs', observe)
        reader.compare_original(dataset, retained, source_schema=pq.read_schema(retained))
        assert seen == ['7', '62']


def test_changed_subject_cannot_reuse_its_admitted_binding(tmp_path, monkeypatch):
    dataset = 'comment_attributes'
    with admitted(tmp_path, dataset, _table(dataset, [{'comment_id': 'one', 'city': 'plain'}])) as (reader, retained, subject):
        table = pq.read_table(subject)
        row = table.to_pylist()[0] | {'city': 'changed after admission'}
        pq.write_table(pa.Table.from_pylist([row], schema=table.schema), subject)
        proof, bulk, reference = verdicts(reader, dataset, retained, monkeypatch)
        assert proof is False and bulk == reference
        assert 'Missing, ambiguous or reused subject receipt' in reference


@pytest.mark.parametrize('change', ['subject', 'field'])
def test_valid_receipt_still_must_reproduce_subject_and_exact_source_fields(tmp_path, monkeypatch, change):
    from spicy_regs.etl_receipts import _digest, decode_exact_json, exact_json
    dataset = 'comment_attributes'
    original = tmp_path / 'original.parquet'
    pq.write_table(_table(dataset, [{'comment_id': 'one', 'city': 'before'}]), original)
    subject, receipts = write_held_dataset(dataset, original, tmp_path / 'native', generation_id='g', bulk=True)
    table = pq.read_table(receipts)
    rows = table.to_pylist()
    accepted = next(row for row in rows if row['outcome'] == 'accepted')
    processing = decode_exact_json(accepted['processing_json'])
    if change == 'subject':
        processing['raw_conversion_inputs']['city'] = 'after'
        pq.write_table(_table(dataset, [{'comment_id': 'one', 'city': 'after'}]), original)
    else:
        del processing['raw_conversion_inputs']['zip']  # Null and absent must remain distinct.
    accepted['processing_json'] = exact_json(processing)
    accepted['receipt_id'] = _digest({key: value for key, value in accepted.items() if key != 'receipt_id'})
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), receipts)
    with _open_family_reader({dataset: [subject]}, [receipts], [policy(dataset)], generation_id='g', guard=lambda: None) as reader:
        proof, bulk, reference = verdicts(reader, dataset, original, monkeypatch)
        assert proof is False and bulk == reference and reference is not None
        assert ('native subject' if change == 'subject' else 'field presence') in reference


def test_regulatory_comparison_reuses_one_reader_for_multiple_dataset_offsets(tmp_path):
    subjects, receipts, originals, policies = {}, [], {}, []
    for dataset, key in [('docket_attributes', 'docket_id'), ('comment_attributes', 'comment_id')]:
        original = tmp_path / f'{dataset}.parquet'
        pq.write_table(_table(dataset, [{key: 'one'}, {key: 'two'}]), original)
        subject, receipt = write_held_dataset(dataset, original, tmp_path / dataset, generation_id='g', bulk=True)
        subjects[dataset], originals[dataset] = [subject], original
        receipts.append(receipt)
        policies.append(policy(dataset))
    with _open_family_reader(subjects, receipts, policies, generation_id='g', guard=lambda: None) as reader:
        for dataset in ['comment_attributes', 'docket_attributes', 'comment_attributes']:
            assert reader._bulk_regulatory_equal(dataset, originals[dataset], pq.read_schema(originals[dataset]))


@pytest.mark.parametrize('invalid', [False, True])
def test_regulatory_proof_fault_does_not_retry_the_complete_row_comparison(tmp_path, monkeypatch, invalid):
    from spicy_regs import etl_bulk
    error = (etl_bulk.duckdb.InvalidInputException('injected') if invalid else
             etl_bulk.duckdb.OutOfMemoryException('injected'))
    dataset = 'comment_attributes'
    with admitted(tmp_path, dataset, _table(dataset, [{'comment_id': 'one'}])) as (reader, retained, _):
        connection = reader.connection

        class Fault:
            def __getattr__(self, name):
                return getattr(connection, name)

            def execute(self, query, *args):
                if 'btv_regulatory_subjects' in query:
                    raise error
                return connection.execute(query, *args)

        reader.connection = Fault()
        monkeypatch.setattr(reader, '_regulatory_original_rows', lambda *a, **k: pytest.fail('row retry after proof fault'))
        with pytest.raises(etl_bulk.BulkValidationError, match='no row fallback') as caught:
            reader.compare_original(dataset, retained, source_schema=pq.read_schema(retained))
        assert isinstance(caught.value, etl_bulk.BulkInputError) == invalid
