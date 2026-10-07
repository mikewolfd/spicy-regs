"""Actual admitted operation lifetime, exact reconstruction, and changed-source refusals."""
from contextlib import contextmanager
from dataclasses import replace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin

from spicy_regs import etl_bulk
from spicy_regs.congress_receipts import policy, write_congress_dataset
from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.generations import build_generation


def build(tmp_path, *, operation=None, canonical_root=None):
    original = tmp_path / 'members-original.parquet'
    schema = pa.schema([('bioguide_id', pa.string()), ('name_first', pa.string())], metadata={b'original': b'exact'})
    pq.write_table(pa.Table.from_pylist([{'bioguide_id': 'X', 'name_first': '  Thomas  '}], schema=schema), original)
    subject, receipts = write_congress_dataset(original, tmp_path / 'bundle', dataset='members', generation_id='g')
    assert subject is not None
    directory = tmp_path / 'generation'
    artifact = build_generation(directory, family='members', files=[subject], expected_keys=['members.parquet'],
                                receipt_path=receipts, receipt_policies=[policy('members')], receipt_generation_id='g',
                                read_operation=operation, canonical_root=canonical_root)
    if canonical_root is not None:
        directory = canonical_root / artifact.pin.artifact_digest.removeprefix('sha256:')
    return directory, artifact, original


@pytest.mark.parametrize('reference', [False, True])
def test_same_admission_exact_replay_and_streamed_original(tmp_path, monkeypatch, reference):
    directory, artifact, original = build(tmp_path)
    if reference:
        @contextmanager
        def not_bulk(*args, **kwargs):
            raise etl_bulk.NotBulkEligible('reference control')
            yield
        monkeypatch.setattr(etl_bulk, '_validated_bundle', not_bulk)
    with ConversionReadOperation() as operation:
        actual, reader, source = operation.admit_generation(directory, expected_pin=artifact.pin)
        assert actual.pin == artifact.pin
        assert operation.admit_generation(directory) == (actual, reader, source)
        assert reader.compare_original('members', original)['rows_only_in_retained'] == 0
        assert reader.outcome_counts()['accepted'] == 1
        assert list(reader.read_subjects('members'))[0]['name_first'] == '  Thomas  '
        assert operation.verified_source(directory) == (actual, source)
        with pytest.raises(ValueError, match='expected pin'):
            operation.admit_generation(directory, expected_pin=ArtifactPin(actual.pin.logical_id, 'sha256:' + 'f' * 64))
        operation.release_generation(directory)
        assert operation.verified_source(directory) == (actual, source)
        with pytest.raises(ValueError, match='closed'):
            reader.compare_original('members', original)
    with pytest.raises(ValueError, match='live conversion'):
        operation.verified_source(directory)


def test_canonical_adoption_uses_final_paths(tmp_path):
    canonical = tmp_path / 'generations'
    canonical.mkdir()
    with ConversionReadOperation() as operation:
        directory, artifact, original = build(tmp_path, operation=operation, canonical_root=canonical)
        assert not (tmp_path / 'generation').exists()
        actual, reader, _ = operation.admit_generation(directory)
        assert actual.pin == artifact.pin
        assert reader.compare_original('members', original)['rows_only_in_restored'] == 0


@pytest.mark.parametrize('mutation', ['replace', 'write', 'symlink'])
def test_verified_source_and_replay_refuse_later_member_change(tmp_path, mutation):
    directory, _, _ = build(tmp_path)
    with pytest.raises((ValueError, RuntimeError)), ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(directory)
        path = directory / 'etl_receipts.parquet'
        if mutation == 'write':
            with path.open('ab') as stream:
                stream.write(b'changed')
        else:
            changed = directory / 'changed'
            changed.write_bytes(path.read_bytes())
            if mutation == 'replace':
                changed.replace(path)
            else:
                path.unlink()
                path.symlink_to(changed)
        with pytest.raises((ValueError, RuntimeError)):
            operation.verified_source(directory)
        with pytest.raises((ValueError, RuntimeError)):
            list(reader.read_subjects('members'))


def test_concurrent_stream_and_closed_stream_refuse(tmp_path):
    directory, _, _ = build(tmp_path)
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(directory)
        stream = reader.read_subjects('members')
        next(stream)
        with pytest.raises(ValueError, match='active family stream'):
            list(reader.processing('members'))
        stream.close()
        assert list(reader.processing('members'))
    with pytest.raises(ValueError, match='closed'):
        list(reader.processing('members'))


def test_exact_descriptor_disagreement_refuses(tmp_path):
    original = tmp_path / 'original.parquet'
    pq.write_table(pa.table({'bioguide_id': ['X'], 'name_first': ['Thomas']}), original)
    subject, receipts = write_congress_dataset(original, tmp_path / 'bundle', dataset='members', generation_id='g')
    assert subject is not None
    changed = replace(policy('members'), receipt_fields=(*policy('members').receipt_fields, 'unknown'))
    with ConversionReadOperation() as operation, pytest.raises(ValueError):
        build_generation(tmp_path / 'generation', family='members', files=[subject], expected_keys=['members.parquet'],
                         receipt_path=receipts, receipt_policies=[changed], receipt_generation_id='g', read_operation=operation)


@pytest.mark.parametrize('release', [False, True])
def test_started_stream_refuses_after_owner_exit_or_release(tmp_path, release):
    directory, _, _ = build(tmp_path)
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(directory)
        stream = reader.processing('members')
        next(stream)
        if release:
            operation.release_generation(directory)
            with pytest.raises(ValueError, match='closed'):
                next(stream)
    if not release:
        with pytest.raises(ValueError, match='closed'):
            next(stream)


def test_owner_closes_platform_state_index(tmp_path):
    directory, _, _ = build(tmp_path)
    with ConversionReadOperation() as operation:
        artifact, _, _ = operation.admit_generation(directory)
        assert len(artifact.local_member_states) == 2
        operation.release_generation(directory)
        assert len(artifact.local_member_states) == 2
    with pytest.raises(RuntimeError, match='closed'):
        len(artifact.local_member_states)


def test_reference_cancelled_dataset_does_not_poison_next_dataset(tmp_path, monkeypatch):
    from spicy_regs.etl_receipts import combine_receipts
    _, _, _ = build(tmp_path)
    original = tmp_path / 'actions.parquet'
    pq.write_table(pa.table({'bill_id': ['119-hr-1'], 'action_index': ['0'], 'action_text': ['Repeated text']}), original)
    action, receipt = write_congress_dataset(original, tmp_path / 'actions', dataset='bill_actions', generation_id='g')
    assert action is not None
    combined = tmp_path / 'receipts.parquet'
    combine_receipts([tmp_path / 'bundle' / 'etl_receipts.parquet', receipt], combined)
    directory = tmp_path / 'multi'
    build_generation(directory, family='mixed', files=[tmp_path / 'bundle' / 'members.parquet', action],
                     expected_keys=['members.parquet', 'bill_actions.parquet'], receipt_path=combined,
                     receipt_policies=[policy('members'), policy('bill_actions')], receipt_generation_id='g')
    @contextmanager
    def not_bulk(*args, **kwargs):
        raise etl_bulk.NotBulkEligible('reference control')
        yield
    monkeypatch.setattr(etl_bulk, '_validated_bundle', not_bulk)
    with ConversionReadOperation() as operation:
        _, reader, _ = operation.admit_generation(directory)
        stream = reader.read_subjects('members')
        next(stream)
        stream.close()
        assert len(list(reader.read_subjects('bill_actions'))) == 1
        assert len(list(reader.read_subjects('members'))) == 1


def test_failed_admission_cleanup_closes_platform_index_even_if_reader_cleanup_refuses(tmp_path, monkeypatch):
    from spicy_regs import generations
    directory, _, _ = build(tmp_path)
    original = generations._verify_generation_source
    captured = []
    def verify_then_change(*args, **kwargs):
        artifact = original(*args, **kwargs)
        captured.append(artifact)
        (directory / 'unexpected').write_bytes(b'changed')
        return artifact
    monkeypatch.setattr(generations, '_verify_generation_source', verify_then_change)
    with ConversionReadOperation() as operation, pytest.raises(ValueError, match='changed after admission'):
        operation.admit_generation(directory)
    assert len(captured) == 1
    with pytest.raises(RuntimeError, match='closed'):
        len(captured[0].local_member_states)
