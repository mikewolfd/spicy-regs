"""Production reconstruction preserves page refusals without accepting their bodies."""
from contextlib import contextmanager
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk, native_conversion as conversion
from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.generations import build_generation
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, dataset_policy
from tests.test_fec_identity_receipts import WITNESS

DATASET = 'fec_research_source_pages'


def build_pages(tmp_path, rows):
    schema = pa.schema([('record_id', pa.string()), ('title', pa.string()),
                        ('text', pa.string()), ('content_status', pa.string())],
                       metadata={b'original': b'preserved'})
    original = tmp_path / 'original.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), original)
    bundle = tmp_path / 'bundle'
    with IdentityReceiptWriter(bundle, generation_id='pages', tables=[DATASET]) as writer:
        for row in rows:
            writer.emit(DATASET, row, input_witness=WITNESS)
    generation = tmp_path / 'generation'
    build_generation(generation, family='page-test', files=[bundle / (DATASET + '.parquet')],
                     expected_keys=[DATASET + '.parquet'], receipt_path=bundle / 'etl_receipts.parquet',
                     receipt_policies=[dataset_policy(DATASET)], receipt_generation_id='pages')
    return generation, original


def page(key, *, status='body_extracted', text='Body'):
    return dict(record_id=key, title=key, text=text, content_status=status)


@pytest.mark.parametrize('reference', [False, True])
def test_production_decoder_and_qualification_preserve_order_duplicates_and_failures(tmp_path, monkeypatch, reference):
    rows = [page('a'), page('failure', status='failed_page', text=None), page('b'),
            page('failure', status='failed_page', text=None), page('boundary', status='unsupported_body_boundaries', text=None)]
    generation, original = build_pages(tmp_path, rows)
    if reference:
        @contextmanager
        def not_bulk(*args, **kwargs):
            raise etl_bulk.NotBulkEligible('reference control')
            yield
        monkeypatch.setattr(etl_bulk, '_validated_bundle', not_bulk)
    with ConversionReadOperation() as operation:
        artifact, reader, _ = operation.admit_generation(generation)
        assert reader.compare_original(DATASET, original)['rows_only_in_retained'] == 0
        assert len(list(reader.read_subjects(DATASET))) == 2
        assert reader.preserved_source_page_refusals() == 3
        built = conversion._Built(generation, {DATASET: dataset_policy(DATASET).policy_version},
                                  reader.compare_original, lambda *_: 0, read_operation=operation)
        proof, problems, qualified = conversion._qualify_conversion('page-test', built,
            {'tables': {DATASET + '.parquet': {'rows': 5}}}, {DATASET + '.parquet': original},
            {'inputs': []}, False, [])
        assert problems == [] and qualified is not None
        assert proof['tables'][DATASET]['preserved_refused_rows'] == 3
        pq.write_table(pa.Table.from_pylist(list(reversed(rows)), schema=pq.read_schema(original)), original)
        with pytest.raises(ValueError, match='values, order or repetitions'):
            reader.compare_original(DATASET, original)


@pytest.mark.parametrize('status,text', [('unknown', None), ('body_extracted', None), ('failed_page', 'Error body')])
def test_refusal_exception_rejects_nonpermitted_status_and_text(tmp_path, status, text):
    # The writer enforces extracted pages as accepted; validate the narrow
    # refusal guard independently for that otherwise impossible outcome.
    generation, original = build_pages(tmp_path, [page('bad', status='failed_page', text=None)])
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(generation)
        with pytest.raises(ValueError, match='permitted body-free failure'):
            reader._source_page_refusal({'outcome': 'refused',
                'processing_fields': page('bad', status=status, text=text),
                'diagnostics': {'content_status': status}})


@pytest.mark.parametrize('damage', ['wrong-policy', 'wrong-dataset', 'wrong-outcome', 'wrong-diagnostic'])
def test_refusal_exception_requires_exact_dataset_policy_outcome_and_diagnostic(tmp_path, damage):
    generation, original = build_pages(tmp_path, [page('bad', status='failed_page', text=None)])
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(generation)
        receipt = next(reader.processing(DATASET))
        if damage == 'wrong-policy':
            reader.policies[DATASET] = replace(dataset_policy(DATASET), policy_version='unshipped/1')
        elif damage == 'wrong-dataset':
            reader.policies = {'fec_filings': dataset_policy('fec_filings')}
            assert reader.preserved_source_page_refusals() == 0
        elif damage == 'wrong-outcome':
            receipt['outcome'] = 'error'
        else:
            receipt['diagnostics']['content_status'] = 'body_extracted'
        with pytest.raises(ValueError, match='exact shipped page policy|permitted body-free failure'):
            reader._source_page_refusal(receipt)


def test_permitted_page_refusals_do_not_qualify_other_dataset_refusals(tmp_path):
    bundle = tmp_path / 'mixed-bundle'
    datasets = [DATASET, 'fec_filings']
    with IdentityReceiptWriter(bundle, generation_id='mixed', tables=datasets) as writer:
        writer.emit(DATASET, page('body'), input_witness=WITNESS)
        writer.emit(DATASET, page('failure', status='failed_page', text=None), input_witness=WITNESS)
        writer.emit('fec_filings', {'record_id': 'refused-filing'}, input_witness=WITNESS, outcome='refused')
    original = tmp_path / 'page-original.parquet'
    schema = pa.schema([('record_id', pa.string()), ('title', pa.string()), ('text', pa.string()), ('content_status', pa.string())])
    pq.write_table(pa.Table.from_pylist([page('body'), page('failure', status='failed_page', text=None)], schema=schema), original)
    generation = tmp_path / 'mixed-generation'
    build_generation(generation, family='mixed-page-test', files=[bundle / (name + '.parquet') for name in datasets],
        expected_keys=[name + '.parquet' for name in datasets], receipt_path=bundle / 'etl_receipts.parquet',
        receipt_policies=[dataset_policy(name) for name in datasets], receipt_generation_id='mixed')
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(generation)
        assert reader.outcome_counts() == {'accepted': 1, 'refused': 2}
        assert reader.preserved_source_page_refusals() == 1
        built = conversion._Built(generation, {name: dataset_policy(name).policy_version for name in datasets},
            reader.compare_original, lambda *_: 0, added_tables=frozenset({'fec_filings.parquet'}), read_operation=operation)
        _, problems, proof = conversion._qualify_conversion('mixed-page-test', built,
            {'tables': {DATASET + '.parquet': {'rows': 2}}}, {DATASET + '.parquet': original}, {'inputs': []}, False, [])
        assert proof is None
        assert any("receipts hold {'refused': 1}" in problem for problem in problems)
