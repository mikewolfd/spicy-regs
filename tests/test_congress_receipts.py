"""Receipt linkage, producer replay and incremental state preservation."""

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.congress_receipts import (
    CongressBuild, CongressInput, policy, restore_processing_input, write_congress_dataset,
)
from spicy_regs.etl_receipts import validate_receipt_bundle
from spicy_regs.transforms.build_congress_index import INDEX_SPECS, build_index_table
from tests.test_congress_index import FixtureReader


def shaped(path, rows, metadata=None):
    columns = list(dict.fromkeys(k for row in rows for k in row))
    schema = pa.schema([(c, pa.string()) for c in columns], metadata=metadata)
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


@pytest.mark.parametrize('name', INDEX_SPECS)
def test_real_index_producer_admits_native_subjects_and_resumes_from_receipts(tmp_path, name):
    first = CongressBuild('first')
    reader = FixtureReader()
    subject = build_index_table(tmp_path, INDEX_SPECS[name], reader=reader, congresses=[119],
                                max_details=3, receipt_build=first)
    first.admit(tmp_path / 'generation', family=name.replace('_', '-'), subjects=[subject])
    assert 'url' not in pq.read_schema(subject).names
    if name == 'committee_meetings':
        assert pa.types.is_list(pq.read_schema(subject).field('witnesses').type)
    second = CongressBuild('second', inputs={name: CongressInput(subject, first.receipt_path, 'first')})
    next_reader = FixtureReader()
    again = build_index_table(tmp_path, INDEX_SPECS[name], reader=next_reader, congresses=[119],
                               max_details=3, receipt_build=second)
    before = {tuple(sorted((k, str(v)) for k,v in row.items())) for row in pq.read_table(subject).to_pylist()}
    after = {tuple(sorted((k, str(v)) for k,v in row.items())) for row in pq.read_table(again).to_pylist()}
    assert before == after
    assert not set(reader.details) & set(next_reader.details)


def test_missing_wrong_generation_and_changed_subject_cannot_resume(tmp_path):
    source = shaped(tmp_path / 'source.parquet', [{'bioguide_id': 'X', 'fec_ids_json': '[]', 'observed_at': 'then'}])
    subject, receipts = write_congress_dataset(source, tmp_path / 'bundle', dataset='members', generation_id='g')
    with pytest.raises(ValueError, match='missing selected generation receipts'):
        CongressInput(subject).materialize('members', tmp_path / 'missing.parquet')
    with pytest.raises(ValueError, match='selected generation'):
        restore_processing_input(subject, receipts, tmp_path / 'wrong.parquet', dataset='members', generation_id='other')
    table = pq.read_table(subject)
    changed = table.set_column(table.schema.get_field_index('name_first'), 'name_first', pa.array(['different']))
    pq.write_table(changed, tmp_path / 'changed.parquet')
    with pytest.raises(ValueError, match='Missing, ambiguous'):
        restore_processing_input(tmp_path / 'changed.parquet', receipts, tmp_path / 'out.parquet',
                                 dataset='members', generation_id='g')
    assert not (tmp_path / 'out.parquet').exists()


def test_processing_only_archive_and_footer_resume_exactly_without_subject(tmp_path):
    metadata = {b'spicy_regs.bill_family.completed_archive_scopes.v4': b'[[119,"hr"]]',
                b'spicy_regs.bill_family.status_refusals.v1': b'{"119-hr-4":{"reader":"p","refusal":"bad"}}'}
    source = shaped(tmp_path / 'source.parquet', [{'congress': '119', 'bill_type': 'hr', 'size': '50',
                                                'modified_at': '2026-01-01', 'observed_at': 'then'}], metadata)
    subject, receipts = write_congress_dataset(source, tmp_path / 'bundle', dataset='bill_family_archives', generation_id='g')
    assert subject is None
    assert not (tmp_path / 'bundle' / 'bill_family_archives.parquet').exists()
    restored = restore_processing_input(None, receipts, tmp_path / 'restored.parquet', dataset='bill_family_archives', generation_id='g')
    assert pq.read_schema(restored).equals(pq.read_schema(source), check_metadata=True)
    assert pq.read_table(restored).to_pylist() == pq.read_table(source).to_pylist()


def test_invalid_values_have_receipts_without_subjects_and_retry_as_unheld(tmp_path):
    source = shaped(tmp_path / 'source.parquet', [{'bill_id': '119-hr-1', 'subjects_json': '[malformed'}])
    subject, receipts = write_congress_dataset(source, tmp_path / 'bundle', dataset='bill_subjects', generation_id='g')
    assert pq.read_table(subject).num_rows == 0
    outcomes = pq.read_table(receipts)['outcome'].to_pylist()
    assert outcomes == ['observed', 'refused']
    assert subject is not None
    validate_receipt_bundle({'bill_subjects': [subject]}, [receipts], [policy('bill_subjects')], generation_id='g')
    restored = restore_processing_input(subject, receipts, tmp_path / 'retry.parquet', dataset='bill_subjects', generation_id='g')
    assert pq.read_table(restored).num_rows == 0


def test_exact_raw_list_spelling_and_duplicate_witnesses_are_preserved(tmp_path):
    raw = {'bioguide_id': 'X', 'fec_ids_json': '[ "A", null, "A" ]', 'other_names_json': '[{"last":"A","middle":null},{"last":"A"}]'}
    source = shaped(tmp_path / 'source.parquet', [raw])
    witness = {'source_id': 'retained-roster', 'sha256': 'a'*64, 'locator': None, 'body_version': None, 'source_uri': None}
    subject, receipts = write_congress_dataset(source, tmp_path / 'bundle', dataset='members', generation_id='g', witnesses=[witness, witness])
    assert pq.read_table(subject)['fec_ids'].to_pylist() == [['A', None, 'A']]
    accepted = [r for r in pq.read_table(receipts).to_pylist() if r['outcome'] == 'accepted'][0]
    assert accepted['witnesses'][1:] == [witness, witness]
    restored = restore_processing_input(subject, receipts, tmp_path / 'restored.parquet', dataset='members', generation_id='g')
    assert pq.read_table(restored).to_pylist() == [raw]


def test_unresolved_term_assignment_remains_receipt_only(tmp_path):
    raw = {'vote_id': '119-house-1-1', 'member_key': 'name:A', 'term_match': 'unresolved_member', 'term_index': None}
    source = shaped(tmp_path / 'source.parquet', [raw])
    subject, receipts = write_congress_dataset(source, tmp_path / 'bundle', dataset='member_vote_terms', generation_id='g')
    assert pq.read_table(subject).num_rows == 0
    restored = restore_processing_input(subject, receipts, tmp_path / 'restored.parquet', dataset='member_vote_terms', generation_id='g')
    assert pq.read_table(restored).to_pylist() == [raw]


def _next_inputs(paths, build):
    from spicy_regs.congress_subjects import INPUT_COLUMNS, RECEIPT_ONLY
    selected = {path.stem: CongressInput(path, build.receipt_path, build.generation_id)
                if path.stem in INPUT_COLUMNS else CongressInput(path, legacy_bootstrap=True)
                for path in paths}
    selected.update({name: CongressInput(None, build.receipt_path, build.generation_id)
                     for name in RECEIPT_ONLY})
    return selected


def test_bill_archive_footer_preserves_unchanged_zip_skip(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_bill_family import build_bill_family
    from tests.test_bill_family import StubBulkAcquirer, StubBodyAcquirer
    monkeypatch.setenv('BILL_FAMILY_CONGRESSES', '119')
    monkeypatch.setenv('BILL_FAMILY_BILL_TYPES', 'hr')
    monkeypatch.delenv('GEMINI_API_KEY', raising=False)
    monkeypatch.delenv('GOOGLE_API_KEY', raising=False)
    first = CongressBuild('first')
    paths = build_bill_family(tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=StubBodyAcquirer(), receipt_build=first)
    assert 'bill_family_archives' not in {p.stem for p in paths}
    second = CongressBuild('second', inputs=_next_inputs(paths, first))
    bulk, body = StubBulkAcquirer(), StubBodyAcquirer()
    build_bill_family(tmp_path, bulk_acquirer=bulk, body_acquirer=body, receipt_build=second)
    assert bulk.zip_downloads == []
    assert body.requested == []
    with pytest.raises(ValueError, match='sibling outputs'):
        first.admit(tmp_path / 'complete', family='bill-family', subjects=paths)


def test_receipt_only_backfills_resume_capped_walk(tmp_path, monkeypatch):
    from spicy_regs.transforms.build_bill_family import build_bill_family
    from tests.test_bill_family import _stub_source, _two_bills, _numbers
    monkeypatch.setenv('BILL_FAMILY_CONGRESSES', '92')
    monkeypatch.setenv('BILL_FAMILY_BILL_TYPES', 'hr')
    first_source = _stub_source(_two_bills())
    first = CongressBuild('first')
    paths = build_bill_family(tmp_path, list_source=first_source, max_version_fetches=2, receipt_build=first)
    assert _numbers(first_source.requested) == [2185]
    second = CongressBuild('second', inputs=_next_inputs(paths, first))
    second_source = _stub_source(_two_bills())
    paths = build_bill_family(tmp_path, list_source=second_source, receipt_build=second)
    assert _numbers(second_source.requested) == [2190]
    bills = next(p for p in paths if p.stem == 'congress_bills')
    assert pq.read_table(bills)['bill_id'].to_pylist() == ['92-hr-2185', '92-hr-2190']


def test_missing_subject_key_is_a_retained_refusal(tmp_path):
    source = shaped(tmp_path / 'source.parquet', [{'bioguide_id': None, 'name_first': 'Unknown'}])
    subject, receipts = write_congress_dataset(source, tmp_path / 'bundle', dataset='members', generation_id='g')
    assert pq.read_table(subject).num_rows == 0
    assert pq.read_table(receipts)['outcome'].to_pylist() == ['observed', 'refused']


def test_next_generation_keeps_prior_witnesses_when_subject_changes(tmp_path):
    first_raw = shaped(tmp_path / 'first.parquet', [{'bioguide_id': 'X', 'name_first': 'First'}])
    witness = {'source_id': 'native-capture', 'sha256': 'b' * 64, 'locator': 'entry:7',
               'body_version': None, 'source_uri': None}
    subject, receipts = write_congress_dataset(first_raw, tmp_path / 'first', dataset='members',
                                               generation_id='first', witnesses=[witness, witness])
    next_raw = shaped(tmp_path / 'next.parquet', [{'bioguide_id': 'X', 'name_first': 'Next'}])
    _, next_receipts = write_congress_dataset(next_raw, tmp_path / 'next', dataset='members',
        generation_id='next', prior=CongressInput(subject, receipts, 'first'))
    before = next(r for r in pq.read_table(receipts).to_pylist() if r['outcome'] == 'accepted')
    after = next(r for r in pq.read_table(next_receipts).to_pylist() if r['outcome'] == 'accepted')
    assert after['record_id'] == before['record_id']
    assert after['subject_version'] != before['subject_version']
    assert after['witnesses'][:-1] == before['witnesses']
    assert after['generation_id'] == 'next'


def test_members_source_journal_is_retained_in_shared_receipts(tmp_path):
    from spicy_regs.congress_receipts import ACQUISITION_POLICY
    from spicy_regs.etl_receipts import read_attempts, select_receipts
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.transforms.build_members import build_members
    from tests.test_source_evidence import legislators, roster, journal
    evidence = CaptureEvidence(tmp_path / 'capture', 'members')
    build = CongressBuild('members')
    with legislators(evidence, roster()) as acquirer:
        subjects = build_members(tmp_path, acquirer=acquirer, evidence=evidence, receipt_build=build)
    assert build.receipt_path is not None
    scoped = select_receipts(build.receipt_path, tmp_path / 'capture-receipts.parquet', dataset='congress_acquisition')
    events = list(read_attempts([scoped], ACQUISITION_POLICY, generation_id='members'))
    assert [e['processing_fields']['capture_event'] for e in events if 'capture_event' in e['processing_fields']] == journal(evidence)
    assert all('observed_at' not in pq.read_schema(path).names for path in subjects)
    build.admit(tmp_path / 'generation', family='members', subjects=subjects)


def test_failed_builder_retains_scrubbed_journal_and_cannot_admit(tmp_path):
    from spicy_regs.congress_receipts import ACQUISITION_POLICY
    from spicy_regs.etl_receipts import read_attempts
    from spicy_regs.source_evidence import CaptureEvidence
    from tests.test_source_evidence import KEY, journal
    evidence = CaptureEvidence(tmp_path / 'capture', 'failed')
    evidence.credential = KEY
    error = ValueError('bad page https://source.test/?api_key=' + KEY)
    def failed(output_dir, *, evidence):
        evidence.refusal(error, stage='listing')
        raise error
    build = CongressBuild('failed')
    with pytest.raises(ValueError):
        build.run(failed, tmp_path, evidence=evidence)
    assert build.receipt_path is not None
    attempts = list(read_attempts([build.receipt_path], ACQUISITION_POLICY, generation_id='failed'))
    assert attempts[0]['outcome'] == 'error'
    assert [a['processing_fields']['capture_event'] for a in attempts[1:]] == journal(evidence)
    assert KEY not in str(attempts)
    with pytest.raises(ValueError, match='No completed Congress build'):
        build.admit(tmp_path / 'must-not-publish', family='members', subjects=[])


def test_bill_subject_producer_keeps_occurrence_order_and_empty_unknown_distinction(tmp_path):
    from spicy_regs.transforms.enrich_bill_subjects import enrich_bill_subjects
    from tests.test_bill_subjects import _write_bills, _bill, _Folders, _StubFetcher
    source = tmp_path / 'bills.parquet'
    _write_bills(source, [_bill('119-hr-1', family=('Health', ['A', None, '', 'A'])),
                          _bill('119-hr-2', family=('Health', [])),
                          _bill('119-hr-3', family=('Health', None))])
    build = CongressBuild('subjects', inputs={'congress_bills': CongressInput(source, legacy_bootstrap=True)})
    result = enrich_bill_subjects(tmp_path, read_folder=_Folders(), fetcher=_StubFetcher(), receipt_build=build)
    rows = {r['bill_id']: r for r in pq.read_table(result).to_pylist()}
    assert rows['119-hr-1']['subjects'] == ['A', None, '', 'A']
    assert rows['119-hr-1']['subject_count'] == 4
    assert rows['119-hr-2']['subjects'] == []
    assert '119-hr-3' not in rows
