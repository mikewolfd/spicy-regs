"""Pinned main reads preserve resolver values; receipt-dependent aliases refuse."""
from copy import deepcopy
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.congress_receipts import CongressInput, policy, write_congress_dataset
from spicy_regs.generations import build_generation
from spicy_regs.scorecards.analysis_inputs import read_published_native_resolver_rows
from spicy_regs.scorecards.resolution import OFFICIAL_COLUMNS
from spicy_regs.sources import publication
from tests.generation_fakes import Store


def native_selection(tmp_path, name, records):
    raw = tmp_path / 'source.parquet'
    columns = tuple(dict.fromkeys((*OFFICIAL_COLUMNS[name], *(records[0] if records else ()))))
    pq.write_table(pa.Table.from_pylist(records, schema=pa.schema([(c, pa.string()) for c in columns])), raw)
    subject, receipts = write_congress_dataset(raw, tmp_path / 'native', dataset=name, generation_id='test-native')
    generation = tmp_path / 'generation'
    build_generation(generation, family='official', files=[subject], expected_keys=[name + '.parquet'],
                     receipt_path=receipts, receipt_policies=[policy(name)], receipt_generation_id='test-native')
    index = publication.publish_generation(generation, client=Store(), bucket='test', prior_index=publication.empty_index())
    return index, subject, receipts


@pytest.mark.parametrize('name,records', [
    ('congress_bills', [{'bill_id': '118-hr-1', 'subjects_json': '["health", "health"]', 'subject_count': '2'}]),
    ('amendments', [{'amendment_id': '118-hamdt-1', 'amended_bill_id': None}]),
    ('roll_call_votes', [{'vote_id': '118-h-1-1', 'bill_id': None, 'vote_date': '2024-02-29',
                          'yea': '42', 'documents_json': '[{"congress":118,"type":"bill","number":"1"}]'}]),
])
def test_main_values_equal_receipt_restoration(tmp_path, name, records):
    index, subject, receipts = native_selection(tmp_path, name, records)
    restored = CongressInput(subject, receipts, 'test-native').materialize(name, tmp_path / 'restored.parquet')
    original = pq.ParquetFile(restored).read(columns=list(OFFICIAL_COLUMNS[name])).to_pylist()
    assert read_published_native_resolver_rows(index, name, [subject]) == original


def test_changed_pin_and_incomplete_population_refuse(tmp_path):
    index, subject, _ = native_selection(tmp_path, 'congress_bills', [{'bill_id': '118-hr-1'}])
    bad = deepcopy(index)
    bad['families']['official']['tables']['congress_bills.parquet']['sha256'] = 'sha256:' + '0' * 64
    with pytest.raises(ValueError, match='immutable pin'):
        read_published_native_resolver_rows(bad, 'congress_bills', [subject])
    bad = deepcopy(index)
    bad['families']['official']['tables']['congress_bills.parquet']['rows'] += 1
    with pytest.raises(ValueError, match='complete selected population'):
        read_published_native_resolver_rows(bad, 'congress_bills', [subject])
    with pytest.raises(ValueError, match='every selected member'):
        read_published_native_resolver_rows(index, 'congress_bills', [])


def test_alias_omission_and_explicit_null_stay_receipt_checked(tmp_path):
    aliases = [{'last': 'Prior'}, {'first': None, 'last': 'Explicit'}]
    index, subject, receipts = native_selection(tmp_path, 'members', [{
        'bioguide_id': 'X000001', 'name_first': 'Alex', 'name_last': 'Example',
        'other_names_json': json.dumps(aliases),
    }])
    with pytest.raises(ValueError, match='not qualified for: members'):
        read_published_native_resolver_rows(index, 'members', [subject])
    restored = CongressInput(subject, receipts, 'test-native').materialize('members', tmp_path / 'restored.parquet')
    returned = json.loads(pq.ParquetFile(restored).read(columns=['other_names_json']).to_pylist()[0]['other_names_json'])
    assert returned == aliases
    assert 'first' not in returned[0] and returned[1]['first'] is None
