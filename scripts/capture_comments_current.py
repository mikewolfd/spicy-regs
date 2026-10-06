"""Capture selected original comments through maintained native Iceberg streaming.

Install the comments-reader extra. The caller supervises whole-process time,
RSS and disk; row-sized batches are not a byte or process-memory guarantee.
Only RESULT.json status=captured qualifies the complete local input.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import re
import tempfile
import time
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

def fields(expected: dict) -> list[dict]:
    schemas = [schema for schema in expected['schemas'] if schema['schema-id'] == expected['schemaId']]
    if len(schemas) != 1:
        raise ValueError('Expected schema must be unique')
    result = schemas[0]['fields']
    names = [field['name'] for field in result]
    if (not result or len(set(names)) != len(names)
            or not {'comment_id', 'agency_code'}.issubset(names)
            or any(field['type'] != 'string' or field['required'] for field in result)):
        raise ValueError('Expected complete nullable string source schema')
    return result

def configure(con, spill: Path) -> None:
    for key, value in [('threads', 4), ('memory_limit', '4GB'), ('max_temp_directory_size', '32GB'),
                       ('temp_directory', str(spill)), ('preserve_insertion_order', True)]:
        con.execute(f'SET {key} = ?', [value])

def digest(path: Path) -> str:
    with path.open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def reader_runtime() -> dict:
    """Pin the planner and native reader implementation, without credential properties."""
    result = {}
    for package in ('pyiceberg', 'pyarrow', 'duckdb'):
        dist = importlib.metadata.distribution(package)
        sha = hashlib.sha256()
        for file in sorted(dist.files or [], key=str):
            if str(file).endswith(('.py', '.so', '.dylib')):
                path = Path(str(dist.locate_file(file)))
                sha.update(str(file).encode())
                sha.update(bytes.fromhex(digest(path)))
        result[package] = {'version': dist.version, 'codeSha256': sha.hexdigest()}
    return result


def load_comments(namespace: str):
    """Read the REST table with its vended file credentials; never create a table."""
    from pyiceberg.catalog.rest import RestCatalog
    required = ('R2_CATALOG_URI', 'R2_CATALOG_WAREHOUSE', 'R2_CATALOG_TOKEN')
    if not all(os.getenv(key) for key in required):
        raise ValueError('Catalog environment is incomplete')
    catalog = RestCatalog('comments-capture', uri=os.environ[required[0]],
                          warehouse=os.environ[required[1]], token=os.environ[required[2]])
    return catalog.load_table((namespace, 'comments'))


def selection(table) -> dict:
    raw = table.metadata.model_dump(mode='json', by_alias=True)
    return {'tableUuid': raw['table-uuid'], 'snapshotId': raw['current-snapshot-id'],
            'schemaId': raw['current-schema-id'], 'schemas': raw['schemas']}


def check_selection(table, expected: dict) -> None:
    actual = selection(table)
    if (any(actual[key] != expected[key] for key in ('tableUuid', 'snapshotId', 'schemaId'))
            or fields(actual) != fields(expected)):
        raise ValueError('Selected source identity or original fields changed')
    snapshot = table.snapshot_by_id(expected['snapshotId'])
    if snapshot is None or snapshot.schema_id != expected['schemaId'] or table.format_version != 2:
        raise ValueError('Selected snapshot schema or format is unsupported')


def scan_inventory(table, snapshot_id: int) -> dict:
    """Inspect actual live tasks. Successful planning refuses equality deletes upstream."""
    from pyiceberg.manifest import DataFileContent, FileFormat
    tasks = list(table.scan(snapshot_id=snapshot_id).plan_files())
    deletes = {}
    files = []
    for task in tasks:
        if task.file.file_format != FileFormat.PARQUET:
            raise ValueError('Selected source contains unsupported file format')
        files.append({'path': task.file.file_path, 'rows': task.file.record_count,
                      'bytes': task.file.file_size_in_bytes, 'specId': task.file.spec_id})
        for delete in task.delete_files:
            if delete.content != DataFileContent.POSITION_DELETES or delete.file_format != FileFormat.PARQUET:
                raise ValueError('Selected source contains unsupported delete type or format')
            deletes[delete.file_path] = {'path': delete.file_path, 'rows': delete.record_count,
                                         'bytes': delete.file_size_in_bytes}
    return {'formatVersion': table.format_version, 'dataFiles': files,
            'applicablePositionDeleteFiles': sorted(deletes.values(), key=lambda x: x['path']),
            'equalityDeletes': 'absent in successfully planned selected live manifests',
            'populationMeaning': 'Physical data/delete counts are not logical row counts'}


def task_batches(table, scan, task, schema: pa.Schema):
    """Use the maintained reader for one complete task, retaining its close boundary."""
    from pyiceberg.io.pyarrow import ArrowScan
    reader = ArrowScan(table.metadata, table.io, scan.projection(), scan.row_filter,
                       scan.case_sensitive, scan.limit).to_record_batches([task])
    try:
        for batch in reader:
            if batch.schema.names != schema.names or any(
                    not (pa.types.is_string(field.type) or pa.types.is_large_string(field.type))
                    for field in batch.schema):
                raise ValueError('Maintained reader changed original fields or types')
            batch = batch.cast(schema, safe=True)
            for offset in range(0, batch.num_rows, 1000):
                yield batch.slice(offset, 1000)
    finally:
        close = getattr(reader, 'close', None)
        if close is not None:
            close()


def original_batches(table, snapshot_id: int, schema: pa.Schema):
    """Read one planned logical file at a time using public maintained delete handling.

    PyIceberg materializes each task's batches. Passing all tasks lets its executor
    accumulate the whole table. Passing one task bounds that lifetime to one data
    file plus its applicable deletes, not one batch. The caller's RSS watchdog
    must refuse a file that exceeds the admitted process limit.
    """
    scan = table.scan(snapshot_id=snapshot_id)
    for task in scan.plan_files():
        yield from task_batches(table, scan, task, schema)


class FieldDigests:
    """Length-framed UTF-8 values and null markers; each column retains stream order/repeats."""
    def __init__(self, schema):
        self.names = schema.names
        self.hashes = [hashlib.sha256() for _ in self.names]
        self.rows = 0

    def add(self, batch):
        for sha, column in zip(self.hashes, batch.columns, strict=True):
            # One client batch only; never materialize a complete column.
            for value in column.to_pylist():
                if value is None:
                    sha.update(b'\x00')
                else:
                    raw = value.encode('utf-8')
                    sha.update(b'\x01' + len(raw).to_bytes(8, 'big') + raw)
        self.rows += batch.num_rows

    def result(self):
        return {'rows': self.rows, 'columns': dict(zip(self.names, (sha.hexdigest() for sha in self.hashes), strict=True))}


def atomic_json(path: Path, value: dict) -> None:
    """Publish a closed JSON record atomically; interrupted temporary files remain."""
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, prefix=path.name + '.',
                                     suffix='.pending', delete=False) as temporary:
        json.dump(value, temporary, indent=2, allow_nan=False)
        temporary.write('\n')
        temporary.flush()
        os.fsync(temporary.fileno())
    os.replace(temporary.name, path)


def task_pin(task) -> dict:
    """Freeze public file descriptors and their complete planner-selected association."""
    from datetime import date, datetime, time as daytime
    from decimal import Decimal
    from enum import Enum
    from uuid import UUID
    from pyiceberg.typedef import Record

    def encode(value):
        if isinstance(value, Enum):
            return encode(value.value)
        if isinstance(value, bytes):
            return {'bytesHex': value.hex()}
        if isinstance(value, dict):
            return {str(key): encode(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, Record)):
            return [encode(item) for item in value]
        if isinstance(value, (date, datetime, daytime, Decimal, UUID)):
            return {'type': type(value).__name__, 'value': str(value)}
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        raise TypeError('Unsupported source descriptor value')

    def file_pin(file):
        names = ('content', 'file_path', 'file_format', 'partition', 'record_count',
                 'file_size_in_bytes', 'column_sizes', 'value_counts', 'null_value_counts',
                 'nan_value_counts', 'lower_bounds', 'upper_bounds', 'key_metadata',
                 'split_offsets', 'equality_ids', 'sort_order_id', 'spec_id')
        return {name: encode(getattr(file, name)) for name in names}

    return {'data': file_pin(task.file),
            'deletes': sorted((file_pin(file) for file in task.delete_files), key=lambda item: item['file_path']),
            'residual': str(task.residual)}


def pin_digest(value: dict) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':'),
                                    allow_nan=False).encode()).hexdigest()


def planning_report(tasks: list) -> dict:
    """Measure file associations from metadata; byte totals are not network observations."""
    uses = {}
    deletes = {}
    for task in tasks:
        for file in task.delete_files:
            uses[file.file_path] = uses.get(file.file_path, 0) + 1
            deletes[file.file_path] = file
    return {'dataTasks': len(tasks),
            'dataFileBytes': [task.file.file_size_in_bytes for task in tasks],
            'physicalDataRows': sum(task.file.record_count for task in tasks),
            'uniqueDeleteFiles': len(deletes), 'deleteFileTaskUseCounts': dict(sorted(uses.items())),
            'sharedDeleteFiles': sum(count > 1 for count in uses.values()),
            'declaredUniqueDeleteBytes': sum(file.file_size_in_bytes for file in deletes.values()),
            'declaredDeleteBytesAcrossTasks': sum(deletes[path].file_size_in_bytes * count
                                                for path, count in uses.items()),
            'meaning': 'Planned associations and compressed metadata sizes only; not logical rows or observed I/O'}


def read_semantics(table) -> dict:
    """Pin non-secret column-name mapping and partition definitions used by the reader."""
    mapping = table.metadata.name_mapping()
    return {'nameMapping': mapping.model_dump(mode='json', by_alias=True) if mapping is not None else None,
            'partitionSpecs': [spec.model_dump(mode='json', by_alias=True)
                               for _, spec in sorted(table.specs().items())],
            'formatVersion': table.format_version}


def part_values(path: Path, schema: pa.Schema) -> dict:
    """Read every local value, including a complete zero-row part."""
    values = FieldDigests(schema)
    with pq.ParquetFile(path) as footer:
        if not footer.schema_arrow.equals(schema, check_metadata=True):
            raise ValueError('Checkpoint part changed original schema')
        for batch in footer.iter_batches(batch_size=1000):
            values.add(batch)
        if footer.metadata.num_rows != values.rows:
            raise ValueError('Checkpoint part population differs')
    return values.result()


def verified_part(parts: Path, index: int, pin: dict, schema: pa.Schema,
                  checkpoint_sha: str) -> tuple[Path, dict] | None:
    receipt_path = parts / f'{index:06d}.json'
    if not receipt_path.exists():
        return None
    if receipt_path.is_symlink():
        raise ValueError('Checkpoint receipt is a symbolic link')
    receipt = json.loads(receipt_path.read_text())
    if (receipt['checkpointSha256'] != checkpoint_sha
            or receipt['taskPinSha256'] != pin_digest(pin) or not re.fullmatch(
            rf'{index:06d}-[0-9a-f]{{32}}\.parquet', receipt['part'])):
        raise ValueError('Checkpoint task or part locator differs')
    path = parts / receipt['part']
    if path.is_symlink() or path.stat().st_size != receipt['bytes'] or digest(path) != receipt['sha256']:
        raise ValueError('Checkpoint part bytes differ')
    if part_values(path, schema) != receipt['readerFields']:
        raise ValueError('Checkpoint part field readback differs')
    return path, receipt


def capture(expected: dict, output: Path, *, namespace: str, expected_runtime: dict,
            loader: Callable = load_comments, resume: bool = False, max_tasks: int | None = None) -> dict:
    """Checkpoint closed logical tasks; only complete final checks qualify the input."""
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', namespace):
        raise ValueError('Explicit simple namespace required')
    if max_tasks is not None and (type(max_tasks) is not int or max_tasks <= 0):
        raise ValueError('Positive new-task bound required')
    source_fields = fields(expected)
    schema = pa.schema([pa.field(field['name'], pa.string()) for field in source_fields])
    if resume:
        if not output.is_dir() or output.is_symlink():
            raise ValueError('Existing owned checkpoint directory required')
        result_path = output / 'RESULT.json'
        if result_path.exists() and json.loads(result_path.read_text()).get('status') == 'captured':
            raise ValueError('Final output is already qualified; preserve its evidence')
    else:
        output.mkdir(parents=True, exist_ok=False)
        atomic_json(output / 'EXPECTED.json', expected)
    attempts = output / 'attempts'
    parts = output / 'parts'
    if attempts.is_symlink() or parts.is_symlink():
        raise ValueError('Checkpoint directories cannot be symbolic links')
    attempts.mkdir(exist_ok=True)
    parts.mkdir(exist_ok=True)
    attempt = attempts / uuid4().hex
    attempt.mkdir()
    stage = 'reader-runtime'
    start = time.monotonic()
    report: dict = {'status': 'running', 'scope': 'original logical input only', 'namespace': namespace,
              'reader': 'PyIceberg public ArrowScan / one planned file at a time', 'productionActions': [],
              'attempt': attempt.name, 'resumed': resume, 'completedTasks': 0, 'newTasks': 0, 'reusedTasks': 0}
    def progress(phase: str, **detail):
        value = {'phase': phase, 'attempt': attempt.name, 'observedAtEpochSeconds': time.time(),
                 'elapsedSeconds': time.monotonic() - start,
                 'completedTasks': report['completedTasks'], **detail}
        atomic_json(output / 'PROGRESS.json', value)
        with (attempt / 'progress.jsonl').open('a') as log:
            log.write(json.dumps(value, allow_nan=False) + '\n')

    try:
        runtime = reader_runtime()
        report['startRuntime'] = runtime
        if runtime != expected_runtime:
            raise ValueError('Maintained reader differs from frozen runtime')
        stage = 'checkpoint-binding'
        binding = {'namespace': namespace, 'identity': {key: expected[key] for key in
                   ('tableUuid', 'snapshotId', 'schemaId')}, 'fields': source_fields,
                   'runtime': runtime, 'captureCodeSha256': digest(Path(__file__))}
        checkpoint_path = output / 'CHECKPOINT.json'
        checkpoint: dict | None = None
        if resume:
            if checkpoint_path.is_symlink():
                raise ValueError('Checkpoint manifest is a symbolic link')
            checkpoint = json.loads(checkpoint_path.read_text())
            if checkpoint['version'] != 1 or any(checkpoint['binding'].get(key) != value
                                                 for key, value in binding.items()):
                raise ValueError('Checkpoint source/schema/runtime binding differs')
            if json.loads((output / 'EXPECTED.json').read_text()) != expected:
                raise ValueError('Checkpoint expected metadata differs')
        stage = 'start-metadata'
        table = loader(namespace)
        check_selection(table, expected)
        binding['readSemantics'] = read_semantics(table)
        stage = 'checkpoint-read-semantics'
        if checkpoint is not None and checkpoint['binding'] != binding:
            raise ValueError('Checkpoint name mapping or partition definitions differ')
        report['startIdentity'] = {key: expected[key] for key in ('tableUuid', 'snapshotId', 'schemaId')}
        stage = 'selected-live-manifests'
        inventory = scan_inventory(table, expected['snapshotId'])
        atomic_json(attempt / 'INVENTORY.json', inventory)
        if not resume:
            atomic_json(output / 'INVENTORY.json', inventory)
        scan = table.scan(snapshot_id=expected['snapshotId'])
        tasks = list(scan.plan_files())
        atomic_json(attempt / 'PLANNING.json', planning_report(tasks))
        planned = {task.file.file_path: (task, task_pin(task)) for task in tasks}
        if len(planned) != len(tasks):
            raise ValueError('Duplicate planned data-file identity')
        if checkpoint is None:
            checkpoint = {'version': 1, 'binding': binding,
                          'tasks': [planned[task.file.file_path][1] for task in tasks]}
            atomic_json(checkpoint_path, checkpoint)
        else:
            stage = 'checkpoint-task-pins'
            frozen = checkpoint['tasks']
            if (len(frozen) != len(planned) or len({pin['data']['file_path'] for pin in frozen}) != len(frozen)
                    or any(pin['data']['file_path'] not in planned
                           or pin != planned[pin['data']['file_path']][1] for pin in frozen)):
                raise ValueError('Planned data/delete association differs from checkpoint')
            tasks = [planned[pin['data']['file_path']][0] for pin in frozen]
        checkpoint_sha = pin_digest(checkpoint)
        report['checkpointSha256'] = checkpoint_sha
        report['totalTasks'] = len(tasks)
        # An interrupted final publication is not a reusable receipt. Preserve it
        # and rebuild only from the independently verified complete task parts.
        for name in ('comments.parquet', 'agency-populations.parquet'):
            final = output / name
            if final.exists():
                os.replace(final, attempt / ('retained-' + name))
        complete: list[tuple[Path, dict]] = []
        for index, task in enumerate(tasks):
            pin = checkpoint['tasks'][index]
            stage = 'checkpoint-readback'
            reused = verified_part(parts, index, pin, schema, checkpoint_sha)
            if reused is not None:
                complete.append(reused)
                report['reusedTasks'] += 1
                report['completedTasks'] += 1
                progress('task-reused', taskIndex=index, rows=reused[1]['readerFields']['rows'])
                continue
            if max_tasks is not None and report['newTasks'] >= max_tasks:
                break
            stage = 'logical-stream'
            part = parts / f'{index:06d}-{uuid4().hex}.parquet'
            pending = part.with_suffix('.parquet.pending')
            task_start = time.monotonic()
            values = FieldDigests(schema)
            detail = {'taskIndex': index, 'dataFile': task.file.file_path,
                      'taskStartedEpochSeconds': time.time(),
                      'declaredDataRows': task.file.record_count,
                      'declaredDataBytes': task.file.file_size_in_bytes,
                      'applicableDeleteFiles': len(task.delete_files),
                      'declaredDeleteRows': sum(file.record_count for file in task.delete_files),
                      'declaredDeleteBytes': sum(file.file_size_in_bytes for file in task.delete_files),
                      'inputMeaning': 'Physical metadata sizes/counts; actual network reads and logical rows differ'}
            progress('task-reader', **detail, rows=0, outputBytes=0, taskSeconds=0)
            batches = task_batches(table, scan, task, schema)
            last_progress = task_start
            try:
                with pq.ParquetWriter(pending, schema, compression='zstd') as writer:
                    for batch in batches:
                        values.add(batch)
                        writer.write_batch(batch, row_group_size=1000)
                        if time.monotonic() - last_progress >= 5:
                            progress('task-write', **detail, rows=values.rows,
                                     outputBytes=pending.stat().st_size,
                                     taskSeconds=time.monotonic() - task_start)
                            last_progress = time.monotonic()
            finally:
                batches.close()
            with pending.open('rb') as closed:
                os.fsync(closed.fileno())
            progress('task-readback', **detail, rows=values.rows, outputBytes=pending.stat().st_size)
            if part_values(pending, schema) != values.result():
                raise ValueError('Task reader-to-part field fidelity differs')
            receipt = {'checkpointSha256': checkpoint_sha, 'taskPinSha256': pin_digest(pin),
                       'part': part.name, 'sha256': digest(pending), 'bytes': pending.stat().st_size,
                       'readerFields': values.result(), 'seconds': time.monotonic() - task_start}
            pending.chmod(0o444)
            os.replace(pending, part)
            atomic_json(parts / f'{index:06d}.json', receipt)
            complete.append((part, receipt))
            report['newTasks'] += 1
            report['completedTasks'] += 1
            progress('task-complete', **detail, rows=values.rows, outputBytes=receipt['bytes'],
                     taskSeconds=receipt['seconds'])
        report['memoryMeaning'] = 'One data file plus applicable deletes can materialize; whole-process watchdog bounds admitted RSS/time/disk'
        if len(complete) != len(tasks):
            stage = 'fresh-end-metadata'
            end_table = loader(namespace)
            check_selection(end_table, expected)
            if read_semantics(end_table) != binding['readSemantics']:
                raise ValueError('Source read semantics changed during checkpoint capture')
            if reader_runtime() != runtime:
                raise ValueError('Maintained reader changed during checkpoint capture')
            report.update(status='checkpointed', remainingTasks=len(tasks) - len(complete))
            progress('checkpointed', remainingTasks=report['remainingTasks'])
            return report
        stage = 'local-finalize'
        progress(stage)
        values = FieldDigests(schema)
        source = attempt / 'comments.parquet'
        report['partialSource'] = str(source)
        max_bytes = 0
        with pq.ParquetWriter(source, schema, compression='zstd') as writer:
            for part, receipt in complete:
                part_digest = FieldDigests(schema)
                with pq.ParquetFile(part) as footer:
                    if not footer.schema_arrow.equals(schema, check_metadata=True):
                        raise ValueError('Finalization part changed original schema')
                    for batch in footer.iter_batches(batch_size=1000):
                        part_digest.add(batch)
                        values.add(batch)
                        writer.write_batch(batch, row_group_size=1000)
                        max_bytes = max(max_bytes, batch.nbytes)
                if part_digest.result() != receipt['readerFields']:
                    raise ValueError('Finalization part differs from admitted reader fields')
        report['readerFields'] = values.result()
        report['maxBatchBytes'] = max_bytes
        stage = 'fresh-end-metadata'
        progress(stage)
        end_table = loader(namespace)
        check_selection(end_table, expected)
        if read_semantics(end_table) != binding['readSemantics']:
            raise ValueError('Source read semantics changed during capture')
        report['endIdentity'] = report['startIdentity']
        report['endRuntime'] = reader_runtime()
        if report['endRuntime'] != runtime:
            raise ValueError('Maintained reader changed during capture')
        stage = 'complete-field-readback'
        progress(stage)
        readback = FieldDigests(schema)
        with pq.ParquetFile(source) as footer:
            if not footer.schema_arrow.equals(schema, check_metadata=True):
                raise ValueError('Export changed original schema')
            for batch in footer.iter_batches(batch_size=1000):
                readback.add(batch)
            if footer.metadata.num_rows != values.rows or readback.result() != values.result():
                raise ValueError('Complete field fidelity readback differs')
        report['readbackFields'] = readback.result()
        stage = 'local-populations'
        progress(stage)
        spill = attempt / 'spill'
        spill.mkdir()
        with duckdb.connect() as local:
            configure(local, spill)
            local.from_parquet(str(source)).create_view('captured_comments')
            population = local.execute('SELECT count(*),count(comment_id),count(DISTINCT comment_id) '
                                       'FROM captured_comments').fetchone()
            if population is None or population[0] != values.rows:
                raise ValueError('Logical population differs')
            local.execute('COPY (SELECT agency_code,count(*) AS rows FROM captured_comments GROUP BY agency_code '
                          'ORDER BY agency_code NULLS FIRST) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)',
                          [str(attempt / 'agency-populations.parquet')])
        report.update(status='captured', source={'sha256': digest(source), 'bytes': source.stat().st_size,
                      'rows': values.rows, 'schema': str(schema)}, nonnullIds=population[1],
                      distinctNonnullIds=population[2], nullIds=population[0] - population[1],
                      duplicateNonnullRows=population[1] - population[2],
                      agencyPopulationsSha256=digest(attempt / 'agency-populations.parquet'))
        source.chmod(0o444)
        os.replace(source, output / 'comments.parquet')
        os.replace(attempt / 'agency-populations.parquet', output / 'agency-populations.parquet')
        report.pop('partialSource')
        progress('captured')
    except BaseException as error:
        # Remote errors can include tokens. Keep type/phase, never exception text.
        report.update(status='refused', failedPhase=stage, errorType=type(error).__name__)
        raise RuntimeError(f'Comments capture refused during {stage}; evidence retained') from None
    finally:
        report['elapsedSeconds'] = time.monotonic() - start
        atomic_json(attempt / 'RESULT.json', report)
        atomic_json(output / 'RESULT.json', report)
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('expected', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--expected-runtime', type=Path, required=True)
    parser.add_argument('--resume', action='store_true')
    parser.add_argument('--max-tasks', type=int, help='Bound newly read tasks; checkpointed output is unqualified')
    args = parser.parse_args()
    try:
        result = capture(json.loads(args.expected.read_text()), args.output, namespace=args.namespace,
                         expected_runtime=json.loads(args.expected_runtime.read_text()),
                         resume=args.resume, max_tasks=args.max_tasks)
    except Exception as error:
        print('REFUSED: ' + type(error).__name__)
        return 1
    print(result['status'].upper() + ': original logical comments input')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
