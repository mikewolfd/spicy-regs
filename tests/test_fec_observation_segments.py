"""Closed row-group checkpoints preserve complete ordered conversion evidence."""
import json
from uuid import UUID

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs import fec_native_conversion as native
from spicy_regs import native_conversion as conversion
from spicy_regs.fec_observation_segments import observation_segments, coalesced_tables
from spicy_regs.etl_receipts import RECEIPT_SCHEMA
from spicy_regs.generations import implementation_id
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
from spicy_regs.sources import publication
from tests.test_fec_native_conversion import old_family, fec_bucket
from tests.test_native_conversion import MAIN, WHEEL, STATE, BUCKET

PIN = {'fixture_source_commit': 'a' * 40, 'installed_sdk': '0.65.0'}


@pytest.fixture(name='bucket')
def bucket_fixture(monkeypatch):
    factory = getattr(fec_bucket, '__wrapped__', None)
    if not callable(factory):
        raise RuntimeError('Shared publication fixture has no callable factory')
    return factory(monkeypatch)


@pytest.fixture(name='inputs')
def fixture_inputs(tmp_path, monkeypatch, bucket):
    rows = [dict(collection_id='fixture', source_record_id=str(i % 2), source_family='fixture',
                 source_sha256='sha256:' + 'a' * 64, source_record_json='{"value":"é"}' if i % 2 else None)
            for i in range(6)]
    write = pq.write_table
    with monkeypatch.context() as patch:
        patch.setattr(pq, 'write_table', lambda *args, **kwargs: write(*args, **{**kwargs, 'row_group_size': 2}))
        snapshot, retained = old_family(tmp_path / 'source', monkeypatch, bucket, 'fec-observations',
            rows_by_table={'fec_source_records': rows,
                          'fec_collections': [dict(collection_id='fixture', record_count=6, relationship_count=1)],
                          'fec_relationships': [dict(source_family='fixture', source_sha256='sha256:' + 'a' * 64,
                                                     source_locator_json='{"array_index":0}', source_fields_json='{}')]})
    return snapshot, retained


def build(tmp_path, inputs, checkpoint, name, check=lambda: None, source_pin=PIN):
    snapshot, retained = inputs
    with ConversionReadOperation() as operation:
        return native.build_retained_family('fec-observations', snapshot['families']['fec-observations'],
            snapshot, retained, tmp_path / name, operation=operation, check_resources=check,
            observation_checkpoint=checkpoint, observation_source_pin=source_pin)


def interrupt_after_first_source_segment(tmp_path, inputs, checkpoint):
    def check():
        closed = sorted((checkpoint / 'closed').glob('*.json'))
        if any(json.loads(path.read_text())['spec']['table'] == 'fec_source_records' for path in closed):
            raise TimeoutError('fixture pause after closed source segment')
    with pytest.raises(TimeoutError, match='fixture pause'):
        build(tmp_path, inputs, checkpoint, 'interrupted', check)
    assert not (tmp_path / 'interrupted/build/generation/artifact.json').exists()


def contents(generation):
    return {name: pq.ParquetFile(generation / name).read().to_pylist() for name in
            ('fec_collections.parquet', 'fec_relationships.parquet', 'fec_source_records.parquet', 'etl_receipts.parquet')}


def test_interrupt_resume_matches_uninterrupted_and_does_not_rewrite_closed_segments(tmp_path, inputs, monkeypatch):
    checkpoint = tmp_path / 'checkpoint'
    interrupt_after_first_source_segment(tmp_path, inputs, checkpoint)
    committed = {}
    for record in (checkpoint / 'closed').glob('*.json'):
        entry = json.loads(record.read_text())
        for path in [record, *((checkpoint / entry['directory']).iterdir())]:
            committed[path] = (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns)
    # Model a process-killed attempt: files without a committed segment record
    # remain excluded, even if they look like output files.
    orphan = checkpoint / 'attempts' / ('00000003-' + 'b' * 32) / 'closed'
    orphan.mkdir(parents=True)
    (orphan / 'fec_source_records.parquet').write_bytes(b'uncommitted fixture')
    emitted = []
    prepare = IdentityReceiptWriter._prepare_emit
    def observed(self, table, row, **kwargs):
        if table == 'fec_source_records':
            emitted.append(kwargs['observation_ordinal'])
        return prepare(self, table, row, **kwargs)
    with monkeypatch.context() as patch:
        patch.setattr(IdentityReceiptWriter, '_prepare_emit', observed)
        resumed = build(tmp_path, inputs, checkpoint, 'resumed')
    assert emitted == [2, 3, 4, 5]  # The already closed input group is never remapped.
    for path, expected in committed.items():
        assert (path.read_bytes(), path.stat().st_ino, path.stat().st_mtime_ns) == expected
    generation_id = json.loads((checkpoint / 'checkpoint.json').read_text())['generation_id']
    monkeypatch.setattr(native, 'uuid4', lambda: UUID(hex=generation_id))
    uninterrupted = build(tmp_path, inputs, None, 'uninterrupted')
    assert contents(resumed) == contents(uninterrupted)
    assert [row['attempt_id'] for row in contents(resumed)['etl_receipts.parquet']] == [
        'fec_collections/1', 'fec_relationships/2', *[f'fec_source_records/{i}' for i in range(3, 9)]]
    assert (orphan / 'fec_source_records.parquet').read_bytes() == b'uncommitted fixture'


@pytest.mark.parametrize('change', ['input', 'segment', 'gap', 'overlap', 'source', 'policy', 'implementation'])
def test_resume_refuses_changed_inputs_segments_ranges_or_versions(tmp_path, inputs, monkeypatch, change):
    checkpoint = tmp_path / 'checkpoint'
    interrupt_after_first_source_segment(tmp_path, inputs, checkpoint)
    records = sorted((checkpoint / 'closed').glob('*.json'))
    source_pin = PIN
    if change == 'input':
        path = inputs[1]['fec_source_records.parquet']
        with pq.ParquetFile(path) as reader:
            schema = reader.schema_arrow
            rows = reader.read().to_pylist()
        rows[0]['source_record_json'] = 'changed fixture value'
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), path, row_group_size=2)
        message = 'retained input differs'
    elif change == 'segment':
        entry = json.loads(records[-1].read_text())
        with (checkpoint / entry['directory'] / 'fec_source_records.parquet').open('ab') as stream:
            stream.write(b'changed')
        message = 'segment bytes changed'
    elif change == 'gap':
        records[0].unlink()
        message = 'gap'
    elif change == 'overlap':
        entry = json.loads(records[-1].read_text())
        entry['spec']['ordinal_start'] += 1
        records[-1].write_text(json.dumps(entry))
        message = 'range, schema, ordinal'
    elif change == 'source':
        source_pin = {**PIN, 'fixture_source_commit': 'b' * 40}
        message = 'source, input, policy or version changed'
    elif change == 'policy':
        header = json.loads((checkpoint / 'checkpoint.json').read_text())
        header['identity']['policies'][0]['policy_version'] = 'changed'
        (checkpoint / 'checkpoint.json').write_text(json.dumps(header))
        message = 'source, input, policy or version changed'
    else:
        monkeypatch.setattr('spicy_regs.fec_observation_segments.implementation_id', lambda: 'changed')
        message = 'source, input, policy or version changed'
    with pytest.raises(ValueError, match=message):
        build(tmp_path, inputs, checkpoint, 'refused', source_pin=source_pin)
    assert not (tmp_path / 'refused/build/generation/artifact.json').exists()


def test_checkpoint_lock_and_old_partial_output_refuse(tmp_path, inputs):
    family = inputs[0]['families']['fec-observations']
    checkpoint = tmp_path / 'checkpoint'
    with observation_segments(checkpoint, captured_family=family, source_pin=PIN):
        with pytest.raises(ValueError, match='active owner'):
            with observation_segments(checkpoint, captured_family=family, source_pin=PIN):
                pytest.fail('Competing checkpoint owner admitted')
    old = tmp_path / 'old'
    old.mkdir()
    (old / 'partial.parquet').write_bytes(b'failed prior run')
    with pytest.raises(ValueError, match='not an observation checkpoint'):
        with observation_segments(old, captured_family=family, source_pin=PIN):
            pytest.fail('Prior open output was adopted')


def test_segment_path_escape_and_symlink_refuse(tmp_path, inputs):
    checkpoint = tmp_path / 'checkpoint'
    interrupt_after_first_source_segment(tmp_path, inputs, checkpoint)
    record = sorted((checkpoint / 'closed').glob('*.json'))[-1]
    entry = json.loads(record.read_text())
    entry['directory'] = '../outside'
    record.write_text(json.dumps(entry))
    with pytest.raises(ValueError, match='outside its owned attempt'):
        build(tmp_path, inputs, checkpoint, 'escape')
    alias = tmp_path / 'alias'
    alias.symlink_to(checkpoint, target_is_directory=True)
    with pytest.raises(ValueError, match='symlink'):
        build(tmp_path, inputs, alias, 'symlink')


def test_outer_conversion_resumes_checkpoint_with_new_work_and_complete_admission(tmp_path, inputs, monkeypatch):
    monkeypatch.setattr(conversion, '_historical_producer', lambda *_: (implementation_id(),
        {'version': WHEEL, 'wheels': [{'hash': STATE['spicy_docs_wheel']['locked_sha256']}]}))
    monkeypatch.setattr(conversion, '_checkpoint_ancestor', lambda *_: None)
    checkpoint = tmp_path / 'checkpoint'
    retained_map = {member.path: str(inputs[1][key]) for key in inputs[1]
                    for member in publication.table_members(inputs[0], key)}
    def check():
        if any(json.loads(p.read_text())['spec']['table'] == 'fec_source_records'
               for p in (checkpoint / 'closed').glob('*.json')):
            raise TimeoutError('fixture outer pause')
    with pytest.raises(TimeoutError, match='fixture outer pause'):
        conversion.convert('fec-observations', work=tmp_path / 'first', check_resources=check,
            allowed=['fec-observations'], expected_main=MAIN, expected_spicy_docs=WHEEL,
            state=lambda _: dict(STATE), observation_checkpoint=checkpoint, retained_members=retained_map)
    assert not (tmp_path / 'first/build/generation/artifact.json').exists()
    header_before = (checkpoint / 'checkpoint.json').read_bytes()
    advanced = {**STATE, 'main': 'e1' * 20}
    receipt = conversion.convert('fec-observations', work=tmp_path / 'second',
        allowed=['fec-observations'], expected_main=MAIN, expected_spicy_docs=WHEEL,
        state=lambda _: dict(advanced), observation_checkpoint=checkpoint, retained_members=retained_map)
    assert receipt['prepared'] and receipt['published'] is None
    assert receipt['tables']['fec_source_records']['retained_rows'] == 6
    assert (checkpoint / 'checkpoint.json').read_bytes() == header_before
    assert receipt['source']['main'] == advanced['main']
    assert receipt['producer']['commit'] == MAIN
    assert receipt['checkpoint_admission']['observed_remote_main'] == advanced['main']
    observations = [json.loads(p.read_text())['main'] for p in sorted((checkpoint / 'hosted-main-observations').glob('*.json'))]
    assert observations == [MAIN, advanced['main']]
    publisher = {**advanced, 'checkout': advanced['main']}
    sealed_bytes = (tmp_path / 'second' / conversion.RECEIPT).read_bytes()
    # Publishing still requires a clean checkout at actual current main.
    with pytest.raises(conversion.ConversionRefused, match='not main'):
        conversion.publish_prepared(tmp_path / 'second' / conversion.RECEIPT,
            allowed=['fec-observations'], expected_main=advanced['main'], expected_spicy_docs=WHEEL,
            expected_producer=MAIN, expect_bucket=BUCKET, state=lambda _: dict(advanced),
            expected_receipt_digest=receipt['prepared']['receiptDigest'])
    assert (tmp_path / 'second' / conversion.RECEIPT).read_bytes() == sealed_bytes
    published = conversion.publish_prepared(tmp_path / 'second' / conversion.RECEIPT,
        allowed=['fec-observations'], expected_main=advanced['main'], expected_spicy_docs=WHEEL,
        expected_producer=MAIN, expect_bucket=BUCKET, state=lambda _: dict(publisher),
        expected_receipt_digest=receipt['prepared']['receiptDigest'])
    assert published['published']
    assert published['source'] == advanced
    assert published['producer']['commit'] == MAIN


@pytest.mark.parametrize('family,publish', [('fec-query', False), ('fec-observations', True)])
def test_checkpoint_cannot_change_other_families_or_publish(tmp_path, family, publish):
    with pytest.raises(conversion.ConversionRefused, match='only to local fec-observations'):
        conversion.convert(family, allowed=[family], work=tmp_path / 'work', expected_main=MAIN,
                           expected_spicy_docs=WHEEL, publish=publish, observation_checkpoint=tmp_path / 'checkpoint')
    assert not (tmp_path / 'work').exists()


def test_checkpoint_refuses_download_mode_before_any_work(tmp_path):
    with pytest.raises(conversion.ConversionRefused, match='explicit retained member map; no downloads'):
        conversion.convert('fec-observations', allowed=['fec-observations'], work=tmp_path / 'work',
            expected_main=MAIN, expected_spicy_docs=WHEEL, observation_checkpoint=tmp_path / 'checkpoint')
    assert not (tmp_path / 'work').exists()


def test_small_read_batches_coalesce_to_practical_physical_row_groups(tmp_path):
    schema = pa.schema([('ordinal', pa.int64()), ('value', pa.string())])
    rows = [{'ordinal': i, 'value': None if i % 3 == 0 else 'é'} for i in range(20_000)]
    source = tmp_path / 'segment.parquet'
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), source)
    legacy, coalesced = tmp_path / 'legacy.parquet', tmp_path / 'coalesced.parquet'
    with pq.ParquetFile(source) as reader, pq.ParquetWriter(legacy, schema) as writer:
        for batch in reader.iter_batches(batch_size=32, use_threads=False):
            writer.write_batch(batch)
    with pq.ParquetFile(source) as reader, pq.ParquetWriter(coalesced, schema) as writer:
        for held in coalesced_tables(reader.iter_batches(batch_size=32, use_threads=False), schema,
                                     check_resources=lambda: None):
            writer.write_table(held, row_group_size=8192)
    with pq.ParquetFile(legacy) as reader:
        assert reader.metadata.num_row_groups == 625
    with pq.ParquetFile(coalesced) as reader:
        assert reader.metadata.num_row_groups == 3
        assert reader.read().to_pylist() == rows


def test_final_shared_receipts_use_coalescing_and_keep_exact_values(tmp_path):
    witness = dict(source_id='fixture', sha256='sha256:' + 'a' * 64, source_uri=None, locator=None)
    with IdentityReceiptWriter(tmp_path / 'segment', generation_id='fixture', tables=['fec_source_records']) as writer:
        writer.emit_batch('fec_source_records', ((dict(collection_id='fixture', source_record_id=str(i),
            source_sha256=witness['sha256'], source_record_json='{"value":"é"}'), dict(input_witness=witness))
            for i in range(2048)))
    source = tmp_path / 'segment/etl_receipts.parquet'
    expected = pq.ParquetFile(source).read().to_pylist()
    output = tmp_path / 'shared.parquet'
    # The actual owned consolidation consumes this derived receipt member.
    with pq.ParquetWriter(output, RECEIPT_SCHEMA) as writer:
        native._consume_receipt(source, writer, lambda: None, coalesce=True)
    with pq.ParquetFile(output) as reader:
        assert reader.metadata.num_row_groups == 1
        assert reader.read().to_pylist() == expected


def test_coalescing_byte_bound_keeps_complete_wide_values():
    schema = pa.schema([('value', pa.string())])
    rows = [{'value': 'é' * 1000}, {'value': None}, {'value': 'z' * 9000}, {'value': 'last'}]
    source = pa.RecordBatch.from_pylist(rows, schema=schema)
    groups = list(coalesced_tables([source], schema, check_resources=lambda: None, max_bytes=4096))
    assert [row for held in groups for row in held.to_pylist()] == rows
    assert all(held.nbytes <= 4096 or held.num_rows == 1 for held in groups)


def test_invalid_coalescing_bound_refuses_before_reading():
    with pytest.raises(ValueError, match='positive integer'):
        list(coalesced_tables([], pa.schema([]), check_resources=lambda: None, row_group_rows=0))


def test_hosted_history_requires_forward_real_git_commits(tmp_path, inputs, monkeypatch):
    import subprocess

    root = tmp_path / 'history'
    root.mkdir()
    def git(*args):
        return subprocess.run(['git', '-C', str(root), *args], check=True, capture_output=True,
                              text=True).stdout.strip()
    git('init', '-q')
    git('config', 'user.email', 'fixture@example.test')
    git('config', 'user.name', 'Fixture')
    (root / 'source').write_text('producer')
    git('add', 'source')
    git('commit', '-qm', 'producer')
    producer = git('rev-parse', 'HEAD')
    (root / 'source').write_text('ordinary advance')
    git('commit', '-qam', 'advance')
    advanced = git('rev-parse', 'HEAD')
    git('checkout', '-qb', 'diverged', producer)
    (root / 'source').write_text('other history')
    git('commit', '-qam', 'diverged')
    diverged = git('rev-parse', 'HEAD')
    ancestor = conversion._checkpoint_ancestor
    ancestor(producer, advanced, root=root)
    for before, after in [(advanced, producer), (advanced, diverged), (producer, 'f' * 40), (producer[:8], advanced)]:
        with pytest.raises(conversion.ConversionRefused, match='history|ancestry|rewound'):
            ancestor(before, after, root=root)
    monkeypatch.setattr(conversion, '_checkpoint_ancestor', lambda a, b: ancestor(a, b, root=root))
    checkpoint = tmp_path / 'journal'
    pin = {**PIN, 'checkout': producer}
    for head in (producer, advanced):
        with observation_segments(checkpoint, captured_family=inputs[0]['families']['fec-observations'],
                                  source_pin=pin, hosted_main=head):
            pass
    before = {p.relative_to(checkpoint): p.read_bytes() for p in checkpoint.rglob('*.json')}
    for head in (producer, diverged):
        with pytest.raises(conversion.ConversionRefused, match='rewound'):
            with observation_segments(checkpoint, captured_family=inputs[0]['families']['fec-observations'],
                                      source_pin=pin, hosted_main=head):
                pytest.fail('Rewound hosted head admitted')
        assert {p.relative_to(checkpoint): p.read_bytes() for p in checkpoint.rglob('*.json')} == before


@pytest.mark.parametrize('damage', ['checkout', 'dirty', 'untracked', 'wheel', 'code', 'lock', 'remote', 'observation'])
def test_frozen_producer_and_prepared_source_refuse_drift(monkeypatch, damage):
    import copy
    source = copy.deepcopy(STATE)
    source['main'] = 'e1' * 20
    locked = {'version': WHEEL, 'wheels': [{'hash': STATE['spicy_docs_wheel']['locked_sha256']}]}
    identity = implementation_id()
    monkeypatch.setattr(conversion, '_historical_producer', lambda *_: (identity, locked))
    monkeypatch.setattr(conversion, '_checkpoint_ancestor', lambda *_: None)
    producer = conversion._checkpoint_producer(source, MAIN, WHEEL)
    publisher = {**copy.deepcopy(source), 'checkout': source['main']}
    receipt = {'family': 'fec-observations', 'source': source, 'producer': producer,
               'checkpoint_admission': {'observed_remote_main': source['main'], 'remote': source['remote'],
                                        'producer_commit': MAIN}}
    assert conversion._receipt_producer_implementation(receipt, publisher, MAIN) == (identity, False)
    if damage == 'checkout':
        source['checkout'] = 'd1' * 20
    elif damage == 'dirty':
        source['uncommitted'] = [' M src/spicy_regs/native_conversion.py']
    elif damage == 'untracked':
        source['untracked'] = ['src/spicy_regs/new.py']
    elif damage == 'wheel':
        source['spicy_docs_wheel']['file_sha256'] = 'different'
    elif damage == 'code':
        identity += '-changed'
    elif damage == 'lock':
        locked['wheels'][0]['hash'] = 'sha256:' + 'f' * 64
    elif damage == 'remote':
        source['remote'] = 'https://example.test/unrelated.git'
    else:
        receipt['checkpoint_admission']['observed_remote_main'] = MAIN
    if damage not in ('remote', 'observation'):
        with pytest.raises(conversion.ConversionRefused):
            conversion._checkpoint_producer(source, MAIN, WHEEL)
    with pytest.raises(conversion.ConversionRefused):
        conversion._receipt_producer_implementation(receipt, publisher, MAIN)
