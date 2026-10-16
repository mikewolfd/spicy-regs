"""The replacement retains exact checks while reusing complete earlier evidence."""
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import fec_release_verification as verify
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter


def bound(path):
    return dict(path=str(path), signature=verify.signature(path), sha256=verify.digest(path.read_bytes()))


def test_implementation_binds_installed_code_and_policy():
    bindings = verify._implementation()
    assert 'fec_identity_context_fields.json' in bindings
    assert all(value.startswith('sha256:') for value in bindings.values())


@pytest.fixture
def assembly(tmp_path):
    checkpoint, generation, original_dir = (tmp_path / name for name in ('checkpoint', 'generation', 'originals'))
    for p in (checkpoint / 'closed', generation, original_dir):
        p.mkdir(parents=True)
    source_rows = {
        'fec_collections': [dict(collection_id=f'c{i}', record_count=i, relationship_count=0) for i in range(4)],
        'fec_relationships': [dict(source_family='fixture', source_sha256='sha256:'+'a'*64,
                                   source_locator_json=json.dumps({'row': i}), source_fields_json='{}') for i in range(4)],
        'fec_source_records': [dict(collection_id='c0', source_record_id='repeated', source_family='fixture',
                                    source_sha256='sha256:'+'a'*64, source_record_json=None if i%2 else '{"v":"é"}') for i in range(4)],
    }
    candidate = dict(guards={}, candidates={}, rows={t: 4 for t in verify.TABLES},
                     headerSHA256='sha256:'+'b'*64, totalReceiptRows=12, receiptInputs=[])
    context = dict(checkpoint=checkpoint, generation=generation, candidate=candidate,
                   header={'generation_id': 'fixture-generation'}, old={'tables': {}}, retained={})
    position, global_offset = 0, 0
    all_receipts = []
    for table, originals in source_rows.items():
        original = original_dir / (table+'.parquet')
        pq.write_table(pa.Table.from_pylist(originals), original, row_group_size=2)
        context['retained'][table+'.parquet'] = str(original)
        source_sha = verify.digest(original.read_bytes())
        context['old']['tables'][table+'.parquet'] = dict(rows=4, sha256=source_sha)
        subject_parts, receipt_parts = [], []
        for group in range(2):
            directory = checkpoint / 'attempts' / (f'{position:08d}-'+'c'*32) / 'closed'
            with IdentityReceiptWriter(directory, generation_id='fixture-generation', tables=[table],
                                       attempt_offset=global_offset) as writer:
                for local, row in enumerate(originals[group*2:group*2+2]):
                    writer.emit(table, row, input_witness=dict(source_id='fixture', sha256=source_sha),
                                observation_ordinal=group*2+local)
            files = {p.name: verify.digest(p.read_bytes()) for p in directory.iterdir()}
            for p in directory.iterdir():
                candidate['guards'][str(p)] = bound(p)
            entry = dict(checkpoint_sha256=candidate['headerSHA256'], directory=str(directory.relative_to(checkpoint)),
                         files=files, spec=dict(table=table, rows=2, member=0, row_group=group,
                                               ordinal_start=group*2, attempt_offset=global_offset, member_sha256=source_sha))
            control = checkpoint / 'closed' / f'{position:08d}.json'
            control.write_text(json.dumps(entry))
            candidate['guards'][str(control)] = bound(control)
            subject_parts.append(pq.read_table(directory / (table+'.parquet')))
            receipt_parts.append(pq.read_table(directory / 'etl_receipts.parquet'))
            if table != 'fec_source_records':
                candidate['receiptInputs'].append(str(directory / 'etl_receipts.parquet'))
            global_offset += 2
            position += 1
        pq.write_table(pa.concat_tables(subject_parts), generation / (table+'.parquet'), row_group_size=3)
        all_receipts.extend(receipt_parts)
        if table == 'fec_source_records':
            path = tmp_path / 'source-receipts.parquet'
            pq.write_table(pa.concat_tables(receipt_parts), path)
            candidate['candidates']['source_receipts'] = bound(path)
            candidate['receiptInputs'].append(str(path))
    pq.write_table(pa.concat_tables(all_receipts), generation / 'etl_receipts.parquet')
    return context


def test_complete_composition_and_exact_sample_restore(assembly, tmp_path):
    ranges = verify.verify_ranges(assembly)
    assert ranges['receiptRows'] == 12
    assert verify.verify_small_identities(assembly, tmp_path)['fec_relationships']['unique']
    samples = verify.restoration_smoke(assembly, ranges)
    assert all(s['rows'] == 4 and s['exactRestoration'] for s in samples.values())


def test_smoke_does_not_open_shared_receipt_footer(assembly, monkeypatch):
    ranges = verify.verify_ranges(assembly)
    original = pq.ParquetFile
    def selected(path, *args, **kwargs):
        assert Path(path) != assembly['generation'] / 'etl_receipts.parquet'
        return original(path, *args, **kwargs)
    monkeypatch.setattr(pq, 'ParquetFile', selected)
    verify.restoration_smoke(assembly, ranges)


@pytest.mark.parametrize('field,value', [('ordinal_start', 0), ('attempt_offset', 0), ('row_group', 0), ('rows', 1), ('member_sha256', 'foreign')])
def test_range_overlap_gap_and_foreign_source_refused(assembly, field, value):
    path = assembly['checkpoint'] / 'closed/00000001.json'
    entry = json.loads(path.read_text())
    entry['spec'][field] = value
    path.write_text(json.dumps(entry))
    assembly['candidate']['guards'][str(path)] = bound(path)
    with pytest.raises(ValueError, match='ranges|population'):
        verify.verify_ranges(assembly)


def test_missing_segment_refused(assembly):
    (assembly['checkpoint'] / 'closed/00000001.json').unlink()
    with pytest.raises(ValueError, match='sequence'):
        verify.verify_ranges(assembly)


def test_receipt_order_refused(assembly):
    assembly['candidate']['receiptInputs'].reverse()
    with pytest.raises(ValueError, match='population'):
        verify.verify_ranges(assembly)


def test_cross_segment_duplicate_key_refused(assembly, tmp_path):
    path = assembly['generation'] / 'fec_collections.parquet'
    table = pq.read_table(path)
    rows = table.to_pylist()
    rows[-1]['collection_id'] = rows[0]['collection_id']
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    with pytest.raises(ValueError, match='Cross-segment duplicate'):
        verify.verify_small_identities(assembly, tmp_path)
    assert not list(tmp_path.glob('identity-*'))


def test_changed_original_is_caught_by_restoration(assembly):
    path = Path(assembly['retained']['fec_source_records.parquet'])
    table = pq.read_table(path)
    rows = table.to_pylist()
    rows[-1]['source_record_json'] = '{"changed":true}'
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    with pytest.raises(ValueError, match='restoration differs'):
        verify.restoration_smoke(assembly, verify.verify_ranges(assembly))


def test_member_mutation_and_symlink_refused(tmp_path):
    path = tmp_path / 'input'
    path.write_bytes(b'first')
    item = bound(path)
    path.write_bytes(b'other')
    with pytest.raises(ValueError, match='changed'):
        verify.unchanged(item)
    alias = tmp_path / 'alias'
    alias.symlink_to(path)
    with pytest.raises(ValueError, match='symlink'):
        verify.signature(alias)


def test_control_digest_required(tmp_path):
    path = tmp_path / 'proof.json'
    path.write_text('{"valid":true}')
    with pytest.raises(ValueError, match='changed'):
        verify.load_bound(path, 'sha256:'+'0'*64)


def test_resume_reuses_completed_work_and_retries_only_failed_step(tmp_path):
    steps = verify.CompletedSteps(tmp_path, {'inputs': 'same', 'code': 'same'})
    assert steps.run('first', lambda: {'done': True}) == {'done': True}
    def fail():
        raise RuntimeError('interrupted')
    with pytest.raises(RuntimeError):
        steps.run('second', fail)
    assert not (tmp_path / 'second.json').exists()
    resumed = verify.CompletedSteps(tmp_path, {'inputs': 'same', 'code': 'same'})
    assert resumed.run('first', lambda: pytest.fail('completed step rerun')) == {'done': True}
    assert resumed.run('second', lambda: {'done': True}) == {'done': True}
    with pytest.raises(ValueError, match='different inputs or code'):
        verify.CompletedSteps(tmp_path, {'inputs': 'changed', 'code': 'same'}).run('first', fail)
