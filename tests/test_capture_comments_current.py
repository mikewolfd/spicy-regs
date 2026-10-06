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


def make_fixture(root: Path, *, delete_type=DataFileContent.POSITION_DELETES, rows_count=12, delete_sequence=2, evolved=False):
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
    surviving = [row for i, row in enumerate(rows) if delete_sequence < 1 or i not in {1, 3, rows_count // 2}]
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
    assert (tmp_path / 'out/comments.parquet').exists()
    assert (tmp_path / 'out/comments.parquet').stat().st_mode & 0o222


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
