"""Actual Iceberg position-delete fixtures and exact original-field capture checks."""
import json
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip('pyiceberg')

from pyiceberg.io.pyarrow import PyArrowFileIO, schema_to_pyarrow
from pyiceberg.manifest import (DataFile, DataFileContent, FileFormat, ManifestContent,
                               ManifestEntry, ManifestEntryStatus, ManifestWriterV2, write_manifest_list)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import StaticTable
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.snapshots import Snapshot
from pyiceberg.table.sorting import SortOrder
from pyiceberg.typedef import Record
from pyiceberg.types import NestedField, StringType

from scripts import capture_comments_current as capture


class DeleteManifestWriter(ManifestWriterV2):
    """Test-only generation: PyIceberg's writer API currently writes data manifests."""
    def content(self):
        return ManifestContent.DELETES

    @property
    def _meta(self):
        return {**super()._meta, 'content': 'deletes'}


def make_fixture(root: Path, *, delete_type=DataFileContent.POSITION_DELETES, rows_count=12,
                 delete_sequence=2, evolved=False, empty_first=False):
    root.mkdir()
    io = PyArrowFileIO()
    names = ['comment_id', 'docket_id', 'agency_code', 'first_name', 'last_name', 'organization',
             'category', 'title', 'comment', 'document_type', 'posted_date', 'modify_date', 'receive_date',
             'attachments_json', 'text_content', 'text_extraction_status', 'pdf_extraction_results_json',
             'comment_on_document_id', 'comment_on_object_id', 'original_document_id',
             'comment_reference_values_json', 'subtype', 'duplicate_comments']
    schema = Schema(*(NestedField(i, name, StringType(), required=False) for i, name in enumerate(names, 1)))
    spec = PartitionSpec()
    arrow = schema_to_pyarrow(schema)
    odd = ['007', None, '', 'café\x00\r\n', '{"key":"\\n"}', 'x' * 8192]
    rows = [{name: odd[(i + j) % len(odd)] for j, name in enumerate(names)} for i in range(rows_count)]
    files = []
    for i, part in enumerate([rows[:rows_count // 2], rows[rows_count // 2:]]):
        file = root / f'data-{i}.parquet'
        if evolved and i == 1:
            part = [{name: value for name, value in row.items() if name in names[:-2]} for row in part]
            for row in rows[rows_count // 2:]:
                row[names[-1]] = row[names[-2]] = None
        pq.write_table(pa.Table.from_pylist(part, schema=pa.schema(list(arrow)[:-2]) if evolved and i == 1 else arrow),
                       file, row_group_size=2)
        data = DataFile.from_args(content=DataFileContent.DATA, file_path=file.resolve().as_uri(), file_format=FileFormat.PARQUET,
                                  partition=Record(), record_count=len(part), file_size_in_bytes=file.stat().st_size)
        data.spec_id = 0
        files.append(data)
    # Same-file positions, duplicated entry, and a second file: position 1 is deleted once.
    targets = [(files[0].file_path, 1), (files[0].file_path, 1), (files[0].file_path, 3),
               (files[1].file_path, 0)]
    if empty_first:
        targets = [(files[0].file_path, index) for index in range(rows_count // 2)] + [(files[1].file_path, 0)]
    path = root / 'delete.parquet'
    if delete_type == DataFileContent.POSITION_DELETES:
        delete_schema = pa.schema([pa.field('file_path', pa.string(), metadata={'PARQUET:field_id': '2147483546'}),
                                  pa.field('pos', pa.int64(), metadata={'PARQUET:field_id': '2147483545'})])
        pq.write_table(pa.Table.from_pylist([{'file_path': file, 'pos': pos} for file, pos in targets], schema=delete_schema), path)
    else:
        pq.write_table(pa.Table.from_pylist([rows[0]], schema=arrow), path)
    delete = DataFile.from_args(content=delete_type, file_path=path.resolve().as_uri(), file_format=FileFormat.PARQUET,
                               partition=Record(), record_count=len(targets) if delete_type == DataFileContent.POSITION_DELETES else 1, file_size_in_bytes=path.stat().st_size,
                               equality_ids=[1] if delete_type == DataFileContent.EQUALITY_DELETES else None)
    delete.spec_id = 0
    snapshots = []
    data_manifest = None
    for snapshot_id, sequence in [(11, 1), (22, 2)]:
        manifests = []
        if data_manifest is None:
            with ManifestWriterV2(spec, schema, io.new_output(str(root / 'data.avro')), 11, 'null') as writer:
                for file in files:
                    writer.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=11,
                               sequence_number=1, file_sequence_number=1, data_file=file))
            data_manifest = writer.to_manifest_file()
        manifests.append(data_manifest)
        if snapshot_id == 22:
            with DeleteManifestWriter(spec, schema, io.new_output(str(root / 'deletes.avro')), 22, 'null') as writer:
                writer.add(ManifestEntry.from_args(status=ManifestEntryStatus.ADDED, snapshot_id=22,
                           sequence_number=delete_sequence, file_sequence_number=2, data_file=delete))
            manifests.append(writer.to_manifest_file())
        manifest_list = root / f'list-{snapshot_id}.avro'
        with write_manifest_list(2, io.new_output(str(manifest_list)), snapshot_id, None if snapshot_id == 11 else 11,
                                 sequence, 'null') as writer:
            writer.add_manifests(manifests)
        snapshots.append(Snapshot.model_validate({'snapshot-id': snapshot_id, 'parent-snapshot-id': None if snapshot_id == 11 else 11,
                         'sequence-number': sequence, 'manifest-list': str(manifest_list), 'schema-id': schema.schema_id}))
    metadata = new_table_metadata(schema, spec, SortOrder(), str(root), {'format-version': '2'})
    metadata = metadata.model_copy(update={'snapshots': snapshots, 'current_snapshot_id': 22, 'last_sequence_number': 2})
    meta_path = root / 'v2.metadata.json'
    meta_path.write_text(metadata.model_dump_json(by_alias=True))
    table = StaticTable.from_metadata(str(meta_path))
    deleted = set(range(rows_count // 2)) | {rows_count // 2} if empty_first else {1, 3, rows_count // 2}
    surviving = [row for i, row in enumerate(rows) if delete_sequence < 1 or i not in deleted]
    return table, rows, surviving


def test_actual_position_deletes_preserve_all_values_duplicates_and_nulls(tmp_path):
    table, before, after = make_fixture(tmp_path / 'fixture')
    expected = capture.selection(table)
    schema = pa.schema([(field['name'], pa.string()) for field in capture.fields(expected)])
    for snapshot, rows in [(11, before), (22, after)]:
        batches = list(capture.original_batches(table, snapshot, schema))
        assert pa.Table.from_batches(batches).to_pylist() == rows
    result = capture.capture(expected, tmp_path / 'out', namespace='default',
                             expected_runtime=capture.reader_runtime(), loader=lambda _: table)
    assert result['status'] == 'captured'
    assert pq.read_table(tmp_path / 'out/comments.parquet').to_pylist() == after
    assert result['readerFields'] == result['readbackFields']
    inventory = json.loads((tmp_path / 'out/INVENTORY.json').read_text())
    assert len(inventory['dataFiles']) == 2
    assert inventory['applicablePositionDeleteFiles'][0]['rows'] == 4
    assert result['source']['rows'] == len(after)  # 4 delete records only remove 3 rows.


def test_actual_equality_deletes_refuse_before_output(tmp_path):
    table, _, _ = make_fixture(tmp_path / 'fixture', delete_type=DataFileContent.EQUALITY_DELETES)
    with pytest.raises(RuntimeError, match='selected-live-manifests'):
        capture.capture(capture.selection(table), tmp_path / 'out', namespace='default',
                        expected_runtime=capture.reader_runtime(), loader=lambda _: table)
    result = json.loads((tmp_path / 'out/RESULT.json').read_text())
    assert result['status'] == 'refused'
    assert not (tmp_path / 'out/comments.parquet').exists()


def test_runtime_refusal_does_not_load_source(tmp_path):
    expected = {'tableUuid': 'original', 'snapshotId': 1, 'schemaId': 0, 'schemas': [{'schema-id': 0, 'fields': [
        {'id': i, 'name': name, 'type': 'string', 'required': False}
        for i, name in enumerate(['comment_id', 'agency_code'], 1)]}]}
    with pytest.raises(RuntimeError, match='reader-runtime'):
        capture.capture(expected, tmp_path / 'out', namespace='default', expected_runtime={},
                        loader=lambda _: pytest.fail('loaded source'))


@pytest.mark.parametrize('change', ['tableUuid', 'snapshotId', 'schemaId'])
def test_fresh_end_change_refuses_preserving_partial_output(tmp_path, change):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    expected = capture.selection(table)
    calls = 0
    original = table.metadata
    def loader(_):
        nonlocal calls
        calls += 1
        if calls == 2:
            attr = {'tableUuid': 'table_uuid', 'snapshotId': 'current_snapshot_id', 'schemaId': 'current_schema_id'}[change]
            table.metadata = original.model_copy(update={attr: uuid4() if change == 'tableUuid' else 999})
        return table
    with pytest.raises(RuntimeError, match='fresh-end-metadata'):
        capture.capture(expected, tmp_path / 'out', namespace='default',
                        expected_runtime=capture.reader_runtime(), loader=loader)
    report = json.loads((tmp_path / 'out/RESULT.json').read_text())
    partial = Path(report['partialSource'])
    assert partial.exists()
    assert partial.stat().st_mode & 0o222
    assert not (tmp_path / 'out/comments.parquet').exists()


def test_loader_credentials_do_not_escape(tmp_path):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    secret = 'private-credential-never-retained'
    def loader(_):
        raise RuntimeError('remote URL ' + secret)
    with pytest.raises(RuntimeError) as exc:
        capture.capture(capture.selection(table), tmp_path / 'out', namespace='default',
                        expected_runtime=capture.reader_runtime(), loader=loader)
    assert secret not in str(exc.value)
    assert secret not in (tmp_path / 'out/RESULT.json').read_text()


def test_one_planned_task_lifetime_at_a_time(tmp_path, monkeypatch):
    """Regression: handing the complete task list to ArrowScan queues the full source."""
    import pyiceberg.io.pyarrow as module
    table, _, _ = make_fixture(tmp_path / 'fixture')
    expected = capture.selection(table)
    schema = pa.schema([(field['name'], pa.string()) for field in capture.fields(expected)])
    events = []
    active = set()
    class ObservedScan:
        def __init__(self, *args):
            pass
        def to_record_batches(self, tasks):
            assert len(tasks) == 1
            task = tasks[0]
            assert not active
            active.add(task.file.file_path)
            events.append('open')
            try:
                for _ in range(2):
                    events.append('pull')
                    yield pa.RecordBatch.from_pylist([{name: '007' for name in schema.names}], schema=schema)
            finally:
                events.append('close')
                active.remove(task.file.file_path)
    monkeypatch.setattr(module, 'ArrowScan', ObservedScan)
    reader = capture.original_batches(table, expected['snapshotId'], schema)
    assert events == []
    for batch in reader:
        events.append('write')
        assert batch.num_rows == 1
    assert events == ['open', 'pull', 'write', 'pull', 'write', 'close'] * 2
    assert not active


@pytest.mark.parametrize('failure', ['reader', 'writer'])
def test_incomplete_stream_refuses_and_preserves_evidence(tmp_path, monkeypatch, failure):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    if failure == 'reader':
        import pyiceberg.io.pyarrow as module
        original = module.ArrowScan.to_record_batches
        def refused(self, tasks):
            yield next(original(self, tasks))
            raise ValueError('private-reader-error')
        monkeypatch.setattr(module.ArrowScan, 'to_record_batches', refused)
    else:
        class FailedWriter:
            def __init__(self, *args, **kwargs):
                pass
            def __enter__(self):
                return self
            def __exit__(self, *args):
                pass
            def write_batch(self, *args, **kwargs):
                raise ValueError('private-writer-error')
        monkeypatch.setattr(capture.pq, 'ParquetWriter', FailedWriter)
    with pytest.raises(RuntimeError, match='logical-stream'):
        capture.capture(capture.selection(table), tmp_path / 'out', namespace='default',
                        expected_runtime=capture.reader_runtime(), loader=lambda _: table)
    report = json.loads((tmp_path / 'out/RESULT.json').read_text())
    assert report['status'] == 'refused'
    assert 'source' not in report
    assert f'private-{failure}-error' not in json.dumps(report)


@pytest.mark.parametrize('sequence', [0, 1, 2])
def test_actual_position_delete_sequence_applicability(tmp_path, sequence):
    table, _, after = make_fixture(tmp_path / 'fixture', delete_sequence=sequence)
    expected = capture.selection(table)
    schema = pa.schema([(field['name'], pa.string()) for field in capture.fields(expected)])
    actual = pa.Table.from_batches(list(capture.original_batches(table, 22, schema))).to_pylist()
    assert actual == after


def test_missing_evolved_nullable_fields_preserve_logical_nulls(tmp_path):
    table, _, after = make_fixture(tmp_path / 'fixture', evolved=True)
    expected = capture.selection(table)
    schema = pa.schema([(field['name'], pa.string()) for field in capture.fields(expected)])
    assert pa.Table.from_batches(list(capture.original_batches(table, 22, schema))).to_pylist() == after


def test_same_schema_id_changed_field_definition_refuses(tmp_path):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    expected = capture.selection(table)
    altered = Schema(NestedField(999, 'comment_id', StringType(), required=False), *table.schema().fields[1:])
    table.metadata = table.metadata.model_copy(update={'schemas': [altered]})
    with pytest.raises(RuntimeError, match='start-metadata'):
        capture.capture(expected, tmp_path / 'out', namespace='default',
                        expected_runtime=capture.reader_runtime(), loader=lambda _: table)
    assert not (tmp_path / 'out/comments.parquet').exists()


def test_checkpoint_bound_resumes_only_unfinished_task_and_preserves_all_fields(tmp_path, monkeypatch):
    table, _, after = make_fixture(tmp_path / 'fixture')
    expected = capture.selection(table)
    runtime = capture.reader_runtime()
    out = tmp_path / 'out'
    calls = []
    original = capture.task_batches
    def observed(table, scan, task, schema):
        calls.append(task.file.file_path)
        yield from original(table, scan, task, schema)
    monkeypatch.setattr(capture, 'task_batches', observed)
    first = capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                            loader=lambda _: table, max_tasks=1)
    assert first['status'] == 'checkpointed'
    assert first['completedTasks'] == first['remainingTasks'] == 1
    assert not (out / 'comments.parquet').exists()
    retained = (out / 'attempts' / first['attempt'] / 'RESULT.json').read_bytes()
    final = capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                            loader=lambda _: table, resume=True, max_tasks=1)
    assert final['status'] == 'captured'
    assert final['newTasks'] == final['reusedTasks'] == 1
    assert len(calls) == len(set(calls)) == 2
    assert pq.read_table(out / 'comments.parquet').to_pylist() == after
    assert final['readerFields'] == final['readbackFields']
    assert (out / 'attempts' / first['attempt'] / 'RESULT.json').read_bytes() == retained
    planning = json.loads((out / 'attempts' / first['attempt'] / 'PLANNING.json').read_text())
    assert planning['dataTasks'] == 2 and planning['sharedDeleteFiles'] == 1
    assert set(planning['deleteFileTaskUseCounts'].values()) == {2}


def test_interrupted_reader_keeps_completed_receipt_and_unadmitted_partial(tmp_path, monkeypatch):
    table, _, after = make_fixture(tmp_path / 'fixture')
    expected, runtime = capture.selection(table), capture.reader_runtime()
    out = tmp_path / 'out'
    original = capture.task_batches
    calls = []
    def interrupted(table, scan, task, schema):
        calls.append(task.file.file_path)
        if len(calls) == 2:
            raise ValueError('unretained-private-error')
        yield from original(table, scan, task, schema)
    monkeypatch.setattr(capture, 'task_batches', interrupted)
    with pytest.raises(RuntimeError, match='logical-stream'):
        capture.capture(expected, out, namespace='default', expected_runtime=runtime, loader=lambda _: table)
    assert (out / 'parts/000000.json').exists() and not (out / 'parts/000001.json').exists()
    partials = {p: p.read_bytes() for p in (out / 'parts').glob('*.pending')}
    assert partials
    second_task = calls[1]
    calls.clear()
    def observed(table, scan, task, schema):
        calls.append(task.file.file_path)
        yield from original(table, scan, task, schema)
    monkeypatch.setattr(capture, 'task_batches', observed)
    final = capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                            loader=lambda _: table, resume=True)
    assert calls == [second_task]
    assert final['status'] == 'captured'
    assert all(p.read_bytes() == raw for p, raw in partials.items())
    assert pq.read_table(out / 'comments.parquet').to_pylist() == after


def test_empty_task_is_closed_admitted_and_not_reread(tmp_path, monkeypatch):
    table, _, after = make_fixture(tmp_path / 'fixture', empty_first=True)
    expected, runtime = capture.selection(table), capture.reader_runtime()
    out = tmp_path / 'out'
    capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                    loader=lambda _: table, max_tasks=1)
    # Refuse at fresh end after both parts are durable; the first task is empty.
    calls = 0
    def loader(_):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise ValueError('end-refusal')
        return table
    with pytest.raises(RuntimeError, match='fresh-end-metadata'):
        capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                        loader=loader, resume=True)
    receipts = [json.loads(p.read_text()) for p in (out / 'parts').glob('*.json')]
    assert sorted(r['readerFields']['rows'] for r in receipts) == [0, len(after)]
    monkeypatch.setattr(capture, 'task_batches', lambda *args: pytest.fail('completed task reread'))
    final = capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                            loader=lambda _: table, resume=True)
    assert final['newTasks'] == 0 and final['reusedTasks'] == 2
    assert pq.read_table(out / 'comments.parquet').to_pylist() == after


@pytest.mark.parametrize('corruption', ['bytes', 'field_hash', 'binding', 'locator', 'missing'])
def test_corrupt_part_or_receipt_refuses_before_new_body_read(tmp_path, monkeypatch, corruption):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    expected, runtime = capture.selection(table), capture.reader_runtime()
    out = tmp_path / 'out'
    capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                    loader=lambda _: table, max_tasks=1)
    path = out / 'parts/000000.json'
    receipt = json.loads(path.read_text())
    part = out / 'parts' / receipt['part']
    if corruption == 'bytes':
        part.chmod(0o644)
        with part.open('ab') as stream:
            stream.write(b'changed')
    elif corruption == 'missing':
        part.unlink()
    elif corruption == 'field_hash':
        receipt['readerFields']['columns']['comment_id'] = 'wrong'
    elif corruption == 'binding':
        receipt['checkpointSha256'] = 'wrong'
    else:
        receipt['part'] = '../outside.parquet'
    if corruption not in ('bytes', 'missing'):
        path.write_text(json.dumps(receipt))
    monkeypatch.setattr(capture, 'task_batches', lambda *args: pytest.fail('new body read'))
    with pytest.raises(RuntimeError, match='checkpoint-readback'):
        capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                        loader=lambda _: table, resume=True)
    assert json.loads((out / 'RESULT.json').read_text())['status'] == 'refused'


@pytest.mark.parametrize('change', ['snapshot', 'field', 'runtime', 'code', 'task'])
def test_stale_checkpoint_pins_refuse_before_body_read(tmp_path, monkeypatch, change):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    expected, runtime = capture.selection(table), capture.reader_runtime()
    out = tmp_path / 'out'
    capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                    loader=lambda _: table, max_tasks=1)
    checkpoint_path = out / 'CHECKPOINT.json'
    checkpoint = json.loads(checkpoint_path.read_text())
    if change == 'task':
        checkpoint['tasks'][0]['deletes'][0]['record_count'] += 1
    elif change == 'code':
        checkpoint['binding']['captureCodeSha256'] = 'wrong'
    elif change == 'runtime':
        checkpoint['binding']['runtime'] = {}
    elif change == 'field':
        checkpoint['binding']['fields'][0]['id'] = 999
    else:
        checkpoint['binding']['identity']['snapshotId'] = 11
    checkpoint_path.write_text(json.dumps(checkpoint))
    monkeypatch.setattr(capture, 'task_batches', lambda *args: pytest.fail('body read'))
    with pytest.raises(RuntimeError, match='checkpoint-task-pins' if change == 'task' else 'checkpoint-binding'):
        capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                        loader=lambda _: table, resume=True)


@pytest.mark.parametrize('timing', ['resume', 'fresh-end'])
def test_name_mapping_drift_refuses_before_mixing_parts(tmp_path, monkeypatch, timing):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    expected, runtime = capture.selection(table), capture.reader_runtime()
    out = tmp_path / 'out'
    original = table.metadata
    def changed():
        table.metadata = original.model_copy(update={'properties': {
            'schema.name-mapping.default': '[{"field-id":999,"names":["comment_id"]}]'}})
    if timing == 'resume':
        capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                        loader=lambda _: table, max_tasks=1)
        changed()
        monkeypatch.setattr(capture, 'task_batches', lambda *args: pytest.fail('body read'))
        def loader(_):
            return table
    else:
        calls = 0
        def loader(_):
            nonlocal calls
            calls += 1
            if calls == 2:
                changed()
            return table
    with pytest.raises(RuntimeError, match='checkpoint-read-semantics' if timing == 'resume' else 'fresh-end-metadata'):
        capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                        loader=loader, resume=timing == 'resume')


def test_captured_output_resume_preserves_qualification_without_mutation(tmp_path):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    expected, runtime = capture.selection(table), capture.reader_runtime()
    out = tmp_path / 'out'
    capture.capture(expected, out, namespace='default', expected_runtime=runtime, loader=lambda _: table)
    before = {str(p.relative_to(out)): p.read_bytes() for p in out.rglob('*') if p.is_file()}
    with pytest.raises(ValueError, match='already qualified'):
        capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                        loader=lambda _: pytest.fail('loaded source'), resume=True)
    assert {str(p.relative_to(out)): p.read_bytes() for p in out.rglob('*') if p.is_file()} == before


def test_unqualified_monolith_has_no_admitted_task_boundaries(tmp_path):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    out = tmp_path / 'legacy'
    out.mkdir()
    partial = out / 'comments.parquet'
    partial.write_bytes(b'PAR1-unfinished-original')
    with pytest.raises(RuntimeError, match='checkpoint-binding'):
        capture.capture(capture.selection(table), out, namespace='default', expected_runtime=capture.reader_runtime(),
                        loader=lambda _: pytest.fail('loaded source'), resume=True)
    assert partial.read_bytes() == b'PAR1-unfinished-original'
    assert not list((out / 'parts').glob('*.json'))


def test_final_publication_interruption_resumes_from_parts_without_source_replay(tmp_path, monkeypatch):
    table, _, after = make_fixture(tmp_path / 'fixture')
    expected, runtime = capture.selection(table), capture.reader_runtime()
    out = tmp_path / 'out'
    original = capture.os.replace
    def interrupted(src, dst):
        if Path(dst) == out / 'agency-populations.parquet':
            raise ValueError('publication-interrupted')
        original(src, dst)
    monkeypatch.setattr(capture.os, 'replace', interrupted)
    with pytest.raises(RuntimeError):
        capture.capture(expected, out, namespace='default', expected_runtime=runtime, loader=lambda _: table)
    assert (out / 'comments.parquet').exists()
    retained_bytes = (out / 'comments.parquet').read_bytes()
    monkeypatch.setattr(capture.os, 'replace', original)
    monkeypatch.setattr(capture, 'task_batches', lambda *args: pytest.fail('source replay'))
    final = capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                            loader=lambda _: table, resume=True)
    assert final['status'] == 'captured' and final['newTasks'] == 0
    assert (out / 'attempts' / final['attempt'] / 'retained-comments.parquet').read_bytes() == retained_bytes
    assert pq.read_table(out / 'comments.parquet').to_pylist() == after


def test_interruption_before_receipt_preserves_orphan_and_rereads_only_unadmitted_task(tmp_path, monkeypatch):
    table, _, _ = make_fixture(tmp_path / 'fixture')
    expected, runtime = capture.selection(table), capture.reader_runtime()
    out = tmp_path / 'out'
    original = capture.atomic_json
    def interrupted(path, value):
        if path == out / 'parts/000000.json':
            raise ValueError('receipt-interrupted')
        original(path, value)
    monkeypatch.setattr(capture, 'atomic_json', interrupted)
    with pytest.raises(RuntimeError, match='logical-stream'):
        capture.capture(expected, out, namespace='default', expected_runtime=runtime, loader=lambda _: table)
    orphans = {p: p.read_bytes() for p in (out / 'parts').glob('*.parquet')}
    assert len(orphans) == 1 and not list((out / 'parts').glob('*.json'))
    monkeypatch.setattr(capture, 'atomic_json', original)
    result = capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                             loader=lambda _: table, resume=True, max_tasks=1)
    assert result['status'] == 'checkpointed' and result['reusedTasks'] == 0
    assert all(p.read_bytes() == raw for p, raw in orphans.items())


def test_changed_partition_definition_refuses_before_part_reuse(tmp_path, monkeypatch):
    from pyiceberg.partitioning import PartitionField
    from pyiceberg.transforms import IdentityTransform
    table, _, _ = make_fixture(tmp_path / 'fixture')
    expected, runtime = capture.selection(table), capture.reader_runtime()
    out = tmp_path / 'out'
    capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                    loader=lambda _: table, max_tasks=1)
    table.metadata = table.metadata.model_copy(update={'partition_specs': [PartitionSpec(
        PartitionField(source_id=3, field_id=1000, transform=IdentityTransform(), name='agency'), spec_id=0)]})
    monkeypatch.setattr(capture, 'task_batches', lambda *args: pytest.fail('body read'))
    with pytest.raises(RuntimeError, match='checkpoint-read-semantics'):
        capture.capture(expected, out, namespace='default', expected_runtime=runtime,
                        loader=lambda _: table, resume=True)
