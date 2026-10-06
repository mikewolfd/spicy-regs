"""Receipt scoping preserves cells and the row selector's refusal authority."""
from contextlib import contextmanager

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_receipts
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, exact_json, select_receipts
from spicy_regs.parquet_rows import write_rows
from .test_etl_bulk_validate import build


def reference(path, destination, dataset):
    return write_rows((row for row in etl_receipts._rows(path) if row['dataset'] == dataset), destination, RECEIPT_SCHEMA)


def observed(call):
    try:
        value = call()
        return pq.read_table(value).to_pylist(), None
    except Exception as error:
        return None, (type(error), str(error))


@pytest.mark.parametrize('dataset', ['things', 'other', 'absent', None])
def test_all_outcomes_literal_json_nulls_order_and_metadata_match(tmp_path, dataset):
    bundle = build(tmp_path, rows=8)
    rows = [row for group in bundle.receipts for row in group]
    rows[0]['diagnostic_json'] = 'malformed but scoping does not admit receipts'
    rows[1]['processing_json'] = '["str","original\\u001b literal"]'
    rows[-1]['dataset'] = None
    schema = RECEIPT_SCHEMA.with_metadata({b'input-only':b'preserved by the source, removed by selection'})
    source = tmp_path/'source.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source, row_group_size=3)
    expected = reference(source, tmp_path/'reference.parquet', dataset)
    actual = select_receipts(source, tmp_path/'batch.parquet', dataset=dataset)
    assert pq.read_table(actual).equals(pq.read_table(expected), check_metadata=True)
    assert exact_json(pq.read_table(actual).to_pylist()) == exact_json(pq.read_table(expected).to_pylist())


def test_exact_schema_uses_arrow_without_python_row_materialization(tmp_path, monkeypatch):
    bundle=build(tmp_path)
    _, paths, _=bundle.write(tmp_path/'bundle')
    expected=reference(paths[0],tmp_path/'reference.parquet','things')
    monkeypatch.setattr(etl_receipts,'_rows',lambda *args: pytest.fail('Exact shared schema must use bounded Arrow'))
    actual=select_receipts(paths[0],tmp_path/'batch.parquet',dataset='things')
    assert pq.read_table(actual).equals(pq.read_table(expected),check_metadata=True)


@pytest.mark.parametrize('selected', [True,False])
def test_extra_source_fields_stay_with_row_authority(tmp_path, selected):
    bundle=build(tmp_path,rows=1)
    row=bundle.receipts[0][0]
    row['dataset']='things' if selected else 'other'
    schema=RECEIPT_SCHEMA.append(pa.field('extra',pa.string()))
    source=tmp_path/'extra.parquet'
    pq.write_table(pa.Table.from_pylist([row|{'extra':'unclassified'}],schema=schema),source)
    expected=observed(lambda:reference(source,tmp_path/'reference.parquet','things'))
    actual=observed(lambda:select_receipts(source,tmp_path/'batch.parquet',dataset='things'))
    assert actual==expected


def test_excluded_invalid_utf8_is_decoded_and_refused_like_reference(tmp_path):
    bundle=build(tmp_path,rows=1)
    rows=[bundle.receipts[0][0]|{'dataset':'other'}]
    table=pa.Table.from_pylist(rows,schema=RECEIPT_SCHEMA)
    invalid=pa.Array.from_buffers(pa.string(),1,[None,pa.py_buffer(b'\x00\x00\x00\x00\x01\x00\x00\x00'),pa.py_buffer(b'\xff')])
    table=table.set_column(table.schema.get_field_index('processing_json'),'processing_json',invalid)
    source=tmp_path/'invalid.parquet'
    pq.write_table(table,source)
    expected=observed(lambda:reference(source,tmp_path/'reference.parquet','things'))
    assert expected[1] is not None
    assert observed(lambda:select_receipts(source,tmp_path/'batch.parquet',dataset='things'))==expected


def test_late_decode_failure_preserves_existing_destination(tmp_path,monkeypatch):
    bundle=build(tmp_path,rows=1)
    batch=pa.Table.from_pylist(bundle.receipts[0],schema=RECEIPT_SCHEMA).to_batches()[0]
    class Broken:
        schema_arrow=RECEIPT_SCHEMA
        def iter_batches(self,**kwargs):
            yield batch
            raise pa.ArrowInvalid('Retained late decoder failure')
    @contextmanager
    def broken(path):
        yield Broken()
    monkeypatch.setattr(etl_receipts,'_parquet',broken)
    destination=tmp_path/'unchanged.parquet'
    destination.write_bytes(b'previous complete selection')
    expected=observed(lambda:reference(tmp_path/'broken.parquet',tmp_path/'reference.parquet','things'))
    assert expected[1] is not None
    assert observed(lambda:select_receipts(tmp_path/'broken.parquet',destination,dataset='things'))==expected
    assert destination.read_bytes()==b'previous complete selection'


@pytest.mark.parametrize('field', ['processing_json','diagnostic_json'])
def test_malformed_selected_evidence_still_has_complete_admission_refusal(tmp_path,field):
    bundle=build(tmp_path,rows=2)
    row=next(row for row in bundle.receipts[0] if row['dataset']=='things')
    row[field]='malformed literal evidence'
    row['receipt_id']=etl_receipts._digest({key:value for key,value in row.items() if key!='receipt_id'})
    subjects,paths,policies=bundle.write(tmp_path/'bundle')
    errors=[]
    for arm in ('reference','batch'):
        target=tmp_path/f'{arm}.parquet'
        (reference if arm=='reference' else select_receipts)(paths[0],target,dataset='things')
        try:
            etl_receipts.validate_receipt_bundle({'things':subjects['things']},[target],[policies[0]],generation_id='g1')
        except Exception as error:
            errors.append((type(error),str(error)))
    assert len(errors)==2 and errors[0]==errors[1]


def test_missing_dataset_field_preserves_reference_error(tmp_path):
    source=tmp_path/'missing.parquet'
    pq.write_table(pa.table({'receipt_id':['literal']}),source)
    expected=observed(lambda:reference(source,tmp_path/'reference.parquet','things'))
    assert expected[1] is not None
    assert observed(lambda:select_receipts(source,tmp_path/'batch.parquet',dataset='things'))==expected


def test_later_excluded_invalid_utf8_preserves_previous_complete_output(tmp_path):
    bundle=build(tmp_path,rows=1)
    row=bundle.receipts[0][0]
    rows=[row|{'dataset':'things'} for _ in range(2000)]+[row|{'dataset':'other'}]
    table=pa.Table.from_pylist(rows,schema=RECEIPT_SCHEMA)
    offsets=pa.array(range(2002),type=pa.int32()).buffers()[1]
    invalid=pa.Array.from_buffers(pa.string(),2001,[None,offsets,pa.py_buffer(b'a'*2000+b'\xff')])
    table=table.set_column(table.schema.get_field_index('processing_json'),'processing_json',invalid)
    source=tmp_path/'late-invalid.parquet'
    pq.write_table(table,source,row_group_size=2000)
    expected=observed(lambda:reference(source,tmp_path/'reference.parquet','things'))
    assert expected[1] is not None
    destination=tmp_path/'existing.parquet'
    destination.write_bytes(b'previous complete selection')
    assert observed(lambda:select_receipts(source,destination,dataset='things'))==expected
    assert destination.read_bytes()==b'previous complete selection'


def test_callable_close_failure_preserves_reference_error_and_existing_destination(tmp_path):
    bundle = build(tmp_path, rows=2)
    _, paths, _ = bundle.write(tmp_path/'bundle')

    @contextmanager
    def source():
        with paths[0].open('rb') as stream:
            yield stream
        raise ValueError('retained source close failure')

    expected_path = tmp_path/'reference.parquet'
    actual_path = tmp_path/'actual.parquet'
    expected_path.write_bytes(b'previous complete selection')
    actual_path.write_bytes(b'previous complete selection')
    expected = observed(lambda: reference(source, expected_path, 'things'))
    assert expected[1] == (ValueError, 'retained source close failure')
    actual = observed(lambda: select_receipts(source, actual_path, dataset='things'))
    assert actual == expected
    assert expected_path.read_bytes() == actual_path.read_bytes() == b'previous complete selection'
    assert not any(path.is_dir() and path.name.startswith('tmp') for path in tmp_path.iterdir())
