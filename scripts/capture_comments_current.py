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
import time

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


def original_batches(table, snapshot_id: int, schema: pa.Schema):
    """Read one planned logical file at a time using public maintained delete handling.

    PyIceberg materializes each task's batches. Passing all tasks lets its executor
    accumulate the whole table. Passing one task bounds that lifetime to one data
    file plus its applicable deletes, not one batch. The caller's RSS watchdog
    must refuse a file that exceeds the admitted process limit.
    """
    from pyiceberg.io.pyarrow import ArrowScan
    scan = table.scan(snapshot_id=snapshot_id)
    for task in scan.plan_files():
        reader = ArrowScan(table.metadata, table.io, scan.projection(), scan.row_filter,
                           scan.case_sensitive, scan.limit).to_record_batches([task])
        try:
            for batch in reader:
                if batch.schema.names != schema.names or any(
                        not (pa.types.is_string(field.type) or pa.types.is_large_string(field.type))
                        for field in batch.schema):
                    raise ValueError('Maintained reader changed original fields or types')
                # Safe offset conversion from large_string preserves strings/nulls.
                batch = batch.cast(schema, safe=True)
                for offset in range(0, batch.num_rows, 1000):
                    yield batch.slice(offset, 1000)
        finally:
            close = getattr(reader, "close", None)
            if close is not None:
                close()


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


def capture(expected: dict, output: Path, *, namespace: str, expected_runtime: dict,
            loader: Callable = load_comments) -> dict:
    """Retain partial files/refusals; require fresh end identity and complete field readback."""
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', namespace):
        raise ValueError('Explicit simple namespace required')
    source_fields = fields(expected)
    schema = pa.schema([pa.field(field['name'], pa.string()) for field in source_fields])
    output.mkdir(parents=True, exist_ok=False)
    (output / 'EXPECTED.json').write_text(json.dumps(expected, indent=2) + '\n')
    stage = 'reader-runtime'
    start = time.monotonic()
    report: dict = {'status': 'running', 'scope': 'original logical input only', 'namespace': namespace,
              'reader': 'PyIceberg public ArrowScan / one planned file at a time', 'productionActions': []}
    try:
        runtime = reader_runtime()
        report['startRuntime'] = runtime
        if runtime != expected_runtime:
            raise ValueError('Maintained reader differs from frozen runtime')
        stage = 'start-metadata'
        table = loader(namespace)
        check_selection(table, expected)
        report['startIdentity'] = {key: expected[key] for key in ('tableUuid', 'snapshotId', 'schemaId')}
        stage = 'selected-live-manifests'
        inventory = scan_inventory(table, expected['snapshotId'])
        (output / 'INVENTORY.json').write_text(json.dumps(inventory, indent=2) + '\n')
        stage = 'logical-stream'
        values = FieldDigests(schema)
        source = output / 'comments.parquet'
        max_bytes = 0
        batches = original_batches(table, expected['snapshotId'], schema)
        try:
            with pq.ParquetWriter(source, schema, compression='zstd') as writer:
                for batch in batches:
                    values.add(batch)
                    writer.write_batch(batch, row_group_size=1000)
                    max_bytes = max(max_bytes, batch.nbytes)
        finally:
            batches.close()
        report['readerFields'] = values.result()
        report['maxBatchBytes'] = max_bytes
        report['memoryMeaning'] = 'One data file plus applicable deletes can materialize; whole-process watchdog bounds admitted RSS/time/disk'
        stage = 'fresh-end-metadata'
        check_selection(loader(namespace), expected)
        report['endIdentity'] = report['startIdentity']
        report['endRuntime'] = reader_runtime()
        if report['endRuntime'] != runtime:
            raise ValueError('Maintained reader changed during capture')
        stage = 'complete-field-readback'
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
        spill = output / 'spill'
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
                          [str(output / 'agency-populations.parquet')])
        report.update(status='captured', source={'sha256': digest(source), 'bytes': source.stat().st_size,
                      'rows': values.rows, 'schema': str(schema)}, nonnullIds=population[1],
                      distinctNonnullIds=population[2], nullIds=population[0] - population[1],
                      duplicateNonnullRows=population[1] - population[2],
                      agencyPopulationsSha256=digest(output / 'agency-populations.parquet'))
        source.chmod(0o444)
    except BaseException as error:
        # Remote errors can include tokens. Keep type/phase, never exception text.
        report.update(status='refused', failedPhase=stage, errorType=type(error).__name__)
        raise RuntimeError(f'Comments capture refused during {stage}; evidence retained') from None
    finally:
        report['elapsedSeconds'] = time.monotonic() - start
        (output / 'RESULT.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('expected', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--expected-runtime', type=Path, required=True)
    args = parser.parse_args()
    try:
        capture(json.loads(args.expected.read_text()), args.output, namespace=args.namespace,
                expected_runtime=json.loads(args.expected_runtime.read_text()))
    except Exception as error:
        print('REFUSED: ' + type(error).__name__)
        return 1
    print('CAPTURED: original logical comments input')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
