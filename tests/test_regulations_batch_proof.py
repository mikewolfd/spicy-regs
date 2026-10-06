"""Batch reproduction keeps the row mapper's values, evidence and first refusal."""
from copy import deepcopy
from typing import Any

import pytest

from spicy_regs.etl_receipts import exact_json, read_with_receipts
from spicy_regs.transforms.regulations_receipts import _processor_input, _processor_inputs, policy
from spicy_regs.transforms.regulations_shape import IDENTITIES, SOURCE_COLUMNS, shape_record, shape_records
from .test_regulations_receipts import write


@pytest.mark.parametrize('dataset', list(SOURCE_COLUMNS))
def test_batch_exact_originals_agree_with_complete_row_reference(tmp_path, dataset):
    from datetime import date
    row: dict[str, Any] = {key: '1' for key in IDENTITIES[dataset] if key not in ('rule_target_id', 'lifecycle_event_id')}
    row.update({key: 1 for key in row if key in ('year', 'month', 'docket_source_ordinal')})
    if dataset == 'lifecycle_events':
        row.update(proceeding_id='P', document_id='D', stage='proposed', event_date=date(2026, 1, 1))
    if dataset == 'rule_targets':
        row.update(docket_id='D', source='docket_rin', rin='1000-AA00')
    selected = write(tmp_path, dataset, [row])
    rows = list(read_with_receipts(selected.subjects, [selected.receipts], policy(dataset), generation_id='g1', bulk=False))
    expected = [_processor_input(dataset, value) for value in rows]
    assert exact_json(_processor_inputs(dataset, rows)) == exact_json(expected)
    assert exact_json(shape_records(dataset, expected)) == exact_json([shape_record(dataset, value) for value in expected])


def test_batch_preserves_original_key_presence_nulls_and_repeated_nested_values(tmp_path):
    raw = [
        {'document_id':'a', 'additional_rins':'["007",null,"007"]'},
        {'document_id':'b', 'attachments_json':'[]', 'title':None},
        {'document_id':'c', 'attachments_json':'null', 'text_content':'\x00é\x1b'},
    ]
    selected = write(tmp_path, 'documents', raw)
    rows = list(read_with_receipts(selected.subjects, [selected.receipts], policy('documents'), generation_id='g1'))
    actual = _processor_inputs('documents', rows)
    assert exact_json(actual) == exact_json(raw)
    assert 'title' not in actual[0] and actual[1]['title'] is None


@pytest.mark.parametrize('fault', ['subject', 'processing', 'raw', 'structural'])
def test_batch_later_reproduction_mismatch_has_the_row_refusal(tmp_path, fault):
    selected = write(tmp_path, 'documents', [{'document_id':'a'}, {'document_id':'b', 'text_content':'held'}])
    rows = list(read_with_receipts(selected.subjects, [selected.receipts], policy('documents'), generation_id='g1'))
    rows = deepcopy(rows)
    if fault == 'subject':
        rows[-1]['raw_conversion_inputs']['title'] = 'changed'
    elif fault == 'processing':
        rows[-1]['text_content'] = 'changed'
    elif fault == 'raw':
        rows[-1]['raw_conversion_inputs'] = None
    else:
        rows[-1]['raw_conversion_inputs']['undeclared'] = 'retained but refused'
    with pytest.raises(Exception) as expected:
        [_processor_input('documents', row) for row in rows]
    with pytest.raises(type(expected.value)) as actual:
        _processor_inputs('documents', rows)
    assert str(actual.value) == str(expected.value)


def test_batch_earlier_arrow_overflow_precedes_later_structural_error():
    rows = [{'document_id':'a', 'attachments_json':'[{"size":18446744073709551616}]'},
            {'document_id':'b', 'undeclared':'later structure error'}]
    with pytest.raises(Exception) as expected:
        [shape_record('documents', row) for row in rows]
    with pytest.raises(type(expected.value)) as actual:
        shape_records('documents', rows)
    assert str(actual.value) == str(expected.value)


def test_batch_arrow_equality_cannot_merge_signed_zero(tmp_path):
    raw = {'agency_code':'A', 'baseline':-0.0, 'ratio':1.0}
    selected = write(tmp_path, 'discovery_signals', [raw])
    rows = list(read_with_receipts(selected.subjects, [selected.receipts], policy('discovery_signals'), generation_id='g1'))
    # Both are finite native floats and Arrow considers them equal, but the
    # exact receipt encoding distinguishes their signs.
    rows[0]['baseline'] = 0.0
    with pytest.raises(ValueError) as expected:
        _processor_input('discovery_signals', rows[0])
    with pytest.raises(ValueError) as actual:
        _processor_inputs('discovery_signals', rows)
    assert str(actual.value) == str(expected.value)


def test_batch_earlier_arrow_overflow_precedes_later_json_recursion():
    rows = [{'document_id':'a', 'attachments_json':'[{"size":18446744073709551616}]'},
            {'document_id':'b', 'attachments_json':'[' * 1500 + '0' + ']' * 1500}]
    with pytest.raises(Exception) as expected:
        [shape_record('documents', row) for row in rows]
    with pytest.raises(type(expected.value)) as actual:
        shape_records('documents', rows)
    assert str(actual.value) == str(expected.value)
