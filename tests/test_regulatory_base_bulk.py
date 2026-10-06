"""Original regulatory base inputs keep the row producer's exact evidence."""
import json
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import exact_json, validate_receipt_bundle
from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal, policy, write_held_dataset


@pytest.mark.parametrize('dataset', ['dockets', 'documents'])
@pytest.mark.parametrize('own_witness', [True, False])
def test_held_base_bulk_matches_complete_row_pair_and_restore(tmp_path, monkeypatch, dataset, own_witness):
    identity = policy(dataset).identity_fields[0]
    rows = [{identity: f'{dataset}-{i}', 'title': value} for i, value in enumerate([
        'plain', '007', 'é😀', 'line\nNUL\x00escape\x1b', '', None,
    ])]
    if dataset == 'documents':
        for i, row in enumerate(rows):
            row.update(attachments_json=['null', '[]', '[null]',
                '[{"url":"u","format":"PDF","size":7}]', None, '[]'][i],
                attachment_records_json='[{"id":"a","type":"attachments","attributes":{"title":"t","fileFormats":[{"fileUrl":"u","format":"PDF","size":7}]}}]',
                additional_rins='["007",null,"007"]', withdrawn=['true', 'False', None, 'True', 'false', None][i],
                file_url='url', text_extraction_status='retained', pdf_extraction_results_json=' {"k":1} ')
    names = list(dict.fromkeys(name for row in rows for name in row))
    schema = pa.schema([(name, pa.string()) for name in names], metadata={b'placement': b'\x00\xff', b'clock': b'2026'})
    source = tmp_path/'original.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source, row_group_size=2)
    witness = [{'source_id': 'pinned', 'source_uri': None, 'sha256': 'a'*64, 'locator': 'source', 'body_version': 'v'}]
    reference = write_held_dataset(dataset, source, tmp_path/'row', bulk=False,
        generation_id='g', processor='kept-processor', witnesses=witness, include_source_witness=own_witness)
    from spicy_regs import regulations_bulk
    monkeypatch.setattr(regulations_bulk, '_BATCH', 2)
    routes = []
    original = regulations_bulk.write_held_dataset
    def observed(*args, **kwargs):
        routes.append('producer')
        return original(*args, **kwargs)
    monkeypatch.setattr(regulations_bulk, 'write_held_dataset', observed)
    from spicy_regs.transforms import regulations_receipts
    def no_whole_row_fallback(*args, **kwargs):
        raise AssertionError('Candidate must finish without whole-pair row fallback')
    monkeypatch.setattr(regulations_receipts, 'write_records', no_whole_row_fallback)
    actual = write_held_dataset(dataset, source, tmp_path/'bulk', bulk=True,
        generation_id='g', processor='kept-processor', witnesses=witness, include_source_witness=own_witness)
    assert routes == ['producer']
    for left, right in zip(reference, actual):
        assert pq.read_table(left).equals(pq.read_table(right), check_metadata=True)
    for bulk in [True, False]:
        validate_receipt_bundle({dataset: [actual[0]]}, [actual[1]], [policy(dataset)], generation_id='g', bulk=bulk)
    selected = ReceiptInput(dataset, (actual[0],), actual[1], 'g')
    row_restore = materialize_internal(selected, tmp_path/'row-restore.parquet', bulk=False)
    restore_route = regulations_bulk.materialize_internal
    def restored(*args, **kwargs):
        routes.append('restore')
        return restore_route(*args, **kwargs)
    monkeypatch.setattr(regulations_bulk, 'materialize_internal', restored)
    bulk_restore = materialize_internal(selected, tmp_path/'bulk-restore.parquet', bulk=True)
    assert routes == ['producer', 'restore']
    assert pq.read_table(row_restore).equals(pq.read_table(bulk_restore), check_metadata=True)
    assert pq.read_table(bulk_restore).select(names).equals(pq.read_table(source), check_metadata=True)


def test_document_refusals_equal_row_authority_and_are_not_lost(tmp_path, monkeypatch):
    rows = [
        {'document_id': 'good', 'attachments_json': '[]', 'withdrawn': 'false'},
        {'document_id': 'malformed', 'attachments_json': '[', 'withdrawn': 'true'},
        {'document_id': 'repeat-key', 'attachments_json': '[{"url":"a","url":"b"}]'},
        {'document_id': 'unknown', 'attachments_json': '[{"unknown":"loss"}]'},
        {'document_id': 'bad-bool', 'withdrawn': 'FALSE'},
        {'document_id': None, 'attachments_json': '[]'},
        {'document_id': 'last', 'attachments_json': 'null', 'withdrawn': None},
    ]
    source = tmp_path/'source.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(name, pa.string()) for name in ['document_id', 'attachments_json', 'withdrawn']])), source)
    from spicy_regs import regulations_bulk
    monkeypatch.setattr(regulations_bulk, '_BATCH', 3)
    expected = write_held_dataset('documents', source, tmp_path/'row', generation_id='g', bulk=False)
    actual = write_held_dataset('documents', source, tmp_path/'bulk', generation_id='g', bulk=True)
    for left, right in zip(expected, actual):
        assert pq.read_table(left).equals(pq.read_table(right))
    attempts = pq.read_table(actual[1]).to_pylist()
    assert [r['outcome'] for r in attempts].count('refused') == 5
    assert pq.read_table(actual[0])['document_id'].to_pylist() == ['good', 'last']


def test_late_processing_corruption_preserves_destination_and_refuses(tmp_path):
    source = tmp_path/'source.parquet'
    pq.write_table(pa.table({'docket_id': ['a', 'b'], 'title': ['first', 'last']}), source)
    subject, receipts = write_held_dataset('dockets', source, tmp_path/'native', generation_id='g', bulk=False)
    # Re-seal a valid receipt digest holding a changed processor input. Public
    # admission alone allows receipt-only evidence; reproduction must reject it.
    from spicy_regs.etl_receipts import decode_exact_json, _digest
    table = pq.read_table(receipts)
    rows = table.to_pylist()
    held = decode_exact_json(rows[1]['processing_json'])
    held['raw_conversion_inputs']['title'] = 'changed'
    rows[1]['processing_json'] = exact_json(held)
    rows[1]['receipt_id'] = _digest({k: v for k, v in rows[1].items() if k != 'receipt_id'})
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), receipts)
    selected = ReceiptInput('dockets', (subject,), receipts, 'g')
    destination = tmp_path/'existing.parquet'
    destination.write_bytes(b'preserve')
    with pytest.raises(ValueError, match='retained processor input differs'):
        materialize_internal(selected, destination, bulk=True)
    assert destination.read_bytes() == b'preserve'


def test_unknown_source_field_uses_reference_refusal(tmp_path):
    source = tmp_path/'source.parquet'
    pq.write_table(pa.table({'docket_id': ['a'], 'new_source_field': ['retain']}), source)
    expected = write_held_dataset('dockets', source, tmp_path/'row', generation_id='g', bulk=False)
    actual = write_held_dataset('dockets', source, tmp_path/'bulk', generation_id='g', bulk=True)
    for left, right in zip(expected, actual):
        assert pq.read_table(left).equals(pq.read_table(right))
    assert pq.read_table(actual[1])['outcome'].to_pylist() == ['observed', 'refused']


@pytest.mark.parametrize('bulk', [False, True])
def test_out_of_range_nested_integer_keeps_reference_whole_failure(tmp_path, bulk):
    source = tmp_path/'source.parquet'
    pq.write_table(pa.table({'document_id': ['a'], 'attachments_json': ['[{"size":9223372036854775808}]']}), source)
    with pytest.raises(OverflowError):
        write_held_dataset('documents', source, tmp_path/'native', generation_id='g', bulk=bulk)
    assert not (tmp_path/'native').exists()


@pytest.mark.parametrize('rows', [[], [{'document_id': None, 'title': 'keep refused'}]])
def test_empty_and_all_refused_keep_observed_metadata_and_exact_order(tmp_path, rows):
    source = tmp_path/'source.parquet'
    schema = pa.schema([('document_id', pa.string()), ('title', pa.string())], metadata={b'placement': b'\xff'})
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source)
    expected = write_held_dataset('documents', source, tmp_path/'row', generation_id='original', bulk=False)
    actual = write_held_dataset('documents', source, tmp_path/'bulk', generation_id='original', bulk=True)
    for left, right in zip(expected, actual):
        assert pq.read_table(left).equals(pq.read_table(right))
    assert pq.read_table(actual[1])['outcome'].to_pylist() == ['observed'] + ['refused']*len(rows)
    selected = ReceiptInput('documents', (actual[0],), actual[1], 'later-publisher')
    expected_restore = materialize_internal(selected, tmp_path/'row-restored', bulk=False)
    actual_restore = materialize_internal(selected, tmp_path/'bulk-restored', bulk=True)
    assert pq.read_table(expected_restore).equals(pq.read_table(actual_restore), check_metadata=True)


def test_shared_receipts_and_immutable_prior_generations_are_scoped(tmp_path, monkeypatch):
    from spicy_regs.etl_receipts import combine_receipts
    from spicy_regs import regulations_bulk
    dockets = tmp_path/'dockets.parquet'
    documents = tmp_path/'documents.parquet'
    pq.write_table(pa.table({'docket_id': ['a'], 'title': ['before']}), dockets)
    pq.write_table(pa.table({'document_id': ['d']}), documents)
    prior = write_held_dataset('dockets', dockets, tmp_path/'prior', generation_id='original', bulk=False)
    pq.write_table(pa.table({'docket_id': ['a'], 'title': ['after']}), dockets)
    expected = write_held_dataset('dockets', dockets, tmp_path/'row', generation_id='next', prior_receipts=[prior[1]], bulk=False)
    actual = write_held_dataset('dockets', dockets, tmp_path/'bulk', generation_id='next', prior_receipts=[prior[1]], bulk=True)
    for left, right in zip(expected, actual):
        assert pq.read_table(left).equals(pq.read_table(right))
    document_pair = write_held_dataset('documents', documents, tmp_path/'doc-pair', generation_id='separate', bulk=False)
    shared = combine_receipts([actual[1], document_pair[1]], tmp_path/'shared.parquet')
    selected = ReceiptInput('dockets', (actual[0],), shared, 'publisher-different-from-receipts')
    expected_restore = materialize_internal(selected, tmp_path/'row-restore', bulk=False)
    def no_row_restore(*args, **kwargs):
        raise AssertionError('Qualified shared receipts must use batch restore')
    from spicy_regs.transforms import regulations_receipts
    monkeypatch.setattr(regulations_receipts, '_qualified_rows', no_row_restore)
    actual_restore = materialize_internal(selected, tmp_path/'bulk-restore', bulk=True)
    assert pq.read_table(expected_restore).equals(pq.read_table(actual_restore), check_metadata=True)
    assert regulations_bulk.eligible('dockets', pq.read_schema(dockets))


def test_unchanged_callers_keep_row_authority_until_explicit_bulk_opt_in(tmp_path, monkeypatch):
    from spicy_regs import regulations_bulk
    source = tmp_path/'source.parquet'
    pq.write_table(pa.table({'docket_id': ['a'], 'title': ['retained']}), source)
    def candidate_not_selected(*args, **kwargs):
        raise AssertionError('Unchanged callers must not activate an unqualified candidate')
    monkeypatch.setattr(regulations_bulk, 'write_held_dataset', candidate_not_selected)
    monkeypatch.setattr(regulations_bulk, 'materialize_internal', candidate_not_selected)
    pair = write_held_dataset('dockets', source, tmp_path/'native', generation_id='g')
    selected = ReceiptInput('dockets', (pair[0],), pair[1], 'g')
    restored = materialize_internal(selected, tmp_path/'restored')
    assert pq.read_table(restored)['title'].to_pylist() == ['retained']


@pytest.mark.parametrize('bulk', [False, True])
def test_existing_native_attachment_policy_keeps_all_classified_source_attributes(tmp_path, bulk):
    # Same ten attributes already classified in the reviewed native converter
    # cohort (5892f5bf, FAA-2016-6907-0001 from original generation232d654a).
    attrs = {'title': 't', 'fileFormats': None, 'restrictReason': None, 'restrictReasonType': 'Restricted',
             'agencyNote': None, 'authors': ['source author'], 'docAbstract': None, 'docOrder': 1,
             'modifyDate': '2016-05-13T10:59:51Z', 'publication': None}
    records = [{'id': 'a', 'type': 'attachments', 'attributes': attrs}]
    literal = json.dumps(records, ensure_ascii=False, indent=2)
    source = tmp_path/'source.parquet'
    pq.write_table(pa.table({'document_id': ['supported', 'unknown'],
                            'attachment_records_json': [literal, json.dumps([{'id': 'u', 'type': 'attachments',
                                'attributes': {**attrs, 'unclassified': 'do not drop'}}])]}), source)
    subject, receipts = write_held_dataset('documents', source, tmp_path/'native', generation_id='g', bulk=bulk)
    assert pq.read_table(subject)['document_id'].to_pylist() == ['supported']
    from spicy_regs.etl_receipts import decode_exact_json
    rows = pq.read_table(receipts).to_pylist()
    assert [row['outcome'] for row in rows] == ['accepted', 'observed', 'refused']
    raw = decode_exact_json(rows[0]['processing_json'])['raw_conversion_inputs']
    assert raw['attachment_records_json'] == literal
    restored = materialize_internal(ReceiptInput('documents', (subject,), receipts, 'g'), tmp_path/'restored', bulk=bulk)
    assert pq.read_table(restored)['attachment_records_json'].to_pylist() == [literal]


@pytest.mark.parametrize('bulk', [False, True])
def test_api_witness_and_full_processing_survive_repeated_base_rewrites(tmp_path, bulk):
    from hashlib import sha256
    from spicy_regs.etl_receipts import ReceiptContext, resolve_receipt_witness
    from spicy_regs.transforms.regulations_receipts import write_records

    original = {'source_api': {'id': 'A', 'unmapped': 'keep original'}}
    witness = {'source_id': 'api', 'source_uri': None,
               'sha256': sha256(exact_json(original).encode()).hexdigest(),
               'locator': 'receipt.values.raw_source_record (canonical exact_json)', 'body_version': 'body-v1'}
    subject, receipts = write_records('dockets', [(original, ReceiptContext('api', 'initial', 'api', [witness]))],
                                      tmp_path / 'initial', project=lambda row: {'docket_id': row['source_api']['id']})
    original_receipt = receipts.read_bytes()
    for ordinal in range(2):
        restored = materialize_internal(ReceiptInput('dockets', (subject,), receipts, 'api'),
                                        tmp_path / f'input-{ordinal}.parquet', bulk=bulk)
        next_subject, next_receipts = write_held_dataset(
            'dockets', restored, tmp_path / f'next-{ordinal}', generation_id=f'next-{ordinal}',
            prior_receipts=[receipts], bulk=bulk,
        )
        restored.unlink()
        subject, receipts = next_subject, next_receipts
        [accepted] = [row for row in pq.read_table(receipts).to_pylist() if row['outcome'] == 'accepted']
        assert resolve_receipt_witness(accepted, witness) == exact_json(original).encode()
        for held in accepted['witnesses']:
            assert sha256(resolve_receipt_witness(accepted, held)).hexdigest() == held['sha256'].removeprefix('sha256:')
    assert (tmp_path / 'initial' / 'etl_receipts.parquet').read_bytes() == original_receipt


@pytest.mark.parametrize('bulk', [False, True])
def test_explicit_original_schema_preserves_absent_fields_and_refuses_null_substitution(tmp_path, bulk):
    source = tmp_path / 'source.parquet'
    schema = pa.schema([('title', pa.string()), ('document_id', pa.string())], metadata={b'keep': b'\xff'})
    pq.write_table(pa.Table.from_pylist([{'title': 'literal', 'document_id': 'D'}], schema=schema), source)
    subject, receipts = write_held_dataset('documents', source, tmp_path / 'native', generation_id='g', bulk=bulk)
    selected = ReceiptInput('documents', (subject,), receipts, 'g')
    restored = materialize_internal(selected, tmp_path / 'exact.parquet', bulk=bulk, source_schema=schema)
    assert pq.read_table(source).equals(pq.read_table(restored), check_metadata=True)
    canonical = pq.read_table(materialize_internal(selected, tmp_path / 'canonical.parquet', bulk=bulk))
    assert 'publisher_status' in canonical.column_names
    assert canonical['publisher_status'].to_pylist() == [None]
    with pytest.raises(ValueError, match='field presence differs'):
        materialize_internal(selected, tmp_path / 'invented.parquet', bulk=bulk,
                             source_schema=pa.schema(list(schema) + [pa.field('publisher_status', pa.string())]))
