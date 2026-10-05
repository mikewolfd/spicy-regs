"""Capture original logical comments from an explicitly pinned legacy snapshot.

Read only: retain the selected Iceberg time-travel query and frozen reader;
delete handling needs separate qualification. Never initialize or write catalog
tables. Preserve every original column without normalization.
The caller grants the heavy slot and measures the whole process and private disk.
An exported file is qualified only when RESULT.json reports captured.
"""
from __future__ import annotations

import argparse
from collections.abc import Callable
import hashlib
import json
from pathlib import Path
import re
import time

import duckdb
import _duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.schemas.regulations import RECORD_TYPES
from spicy_regs.sources import iceberg


def identity(metadata: dict) -> dict:
    selected = iceberg._current_snapshot(metadata)
    if not selected.table_uuid or selected.snapshot_id < 0 or selected.schema_id < 0:
        raise ValueError('Unusable legacy snapshot identity')
    return {'tableUuid': selected.table_uuid, 'snapshotId': selected.snapshot_id, 'schemaId': selected.schema_id}


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


def runtime_identity(con) -> dict:
    """Pin installed engine and loaded source extensions without retaining SQL secrets."""
    extensions = []
    for name, version, path in con.execute("SELECT extension_name,extension_version,install_path "
            "FROM duckdb_extensions() WHERE loaded AND extension_name IN ('avro','iceberg','httpfs') "
            "ORDER BY extension_name").fetchall():
        extensions.append({'name': name, 'version': version, 'path': path, 'sha256': digest(Path(path))})
    return {'duckdbVersion': duckdb.__version__, 'duckdbBinarySha256': digest(Path(_duckdb.__file__)),
            'extensions': extensions}


def actual_settings(con) -> dict:
    return dict(con.execute("SELECT name,value FROM duckdb_settings() WHERE name IN "
        "('threads','memory_limit','max_temp_directory_size','temp_directory','preserve_insertion_order')").fetchall())


def write_original_batches(con, query: str, source: Path, schema: pa.Schema, *,
                           observed: dict | None = None, progress: Callable = lambda value: None) -> dict:
    """Bound client batches and writer row groups; upstream query memory remains separate."""
    batch_rows = 1000
    observed = {} if observed is None else observed
    observed.update(batchRows=batch_rows, rows=0, batches=0, maxBatchRows=0, maxBatchBytes=0,
                    memoryMeaning='Client batches/writer row groups only; upstream memory separate')
    progress('logical-snapshot-reader-open')
    with con.sql(query).to_arrow_reader(batch_size=batch_rows) as reader:
        if not reader.schema.equals(schema, check_metadata=True):
            raise ValueError('Logical source reader schema differs from original fields')
        progress('logical-snapshot-writer-open')
        with pq.ParquetWriter(source, schema, compression='zstd') as writer:
            progress('logical-snapshot-batch-read')
            for batch in reader:
                progress('logical-snapshot-batch-schema')
                if batch.num_rows > batch_rows or not batch.schema.equals(schema, check_metadata=True):
                    raise ValueError('Logical source batch rows/schema differ from the declared reader')
                observed['maxBatchRows'] = max(observed['maxBatchRows'], batch.num_rows)
                observed['maxBatchBytes'] = max(observed['maxBatchBytes'], batch.nbytes)
                progress('logical-snapshot-batch-write')
                writer.write_batch(batch, row_group_size=batch_rows)
                observed['rows'] += batch.num_rows
                observed['batches'] += 1
                progress('logical-snapshot-batch-read')
            progress('logical-snapshot-writer-close')
        progress('logical-snapshot-reader-close')
    return observed


def capture(expected: dict, output: Path, *, namespace: str,
            connect: Callable = iceberg._connect, expected_runtime: dict | None = None) -> dict:
    """Fresh output retains failed attempts; fresh end connection checks identity."""
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', namespace):
        raise ValueError('Explicit simple legacy namespace required')
    expected_fields = fields(expected)
    schema = pa.schema([pa.field(field['name'], pa.string(), nullable=True) for field in expected_fields])
    selected = {key: expected[key] for key in ('tableUuid', 'snapshotId', 'schemaId')}
    if (not isinstance(selected['tableUuid'], str) or not selected['tableUuid']
            or type(selected['snapshotId']) is not int or selected['snapshotId'] < 0
            or type(selected['schemaId']) is not int or selected['schemaId'] < 0):
        raise ValueError('Explicit valid source identity required')
    output.mkdir(parents=True, exist_ok=False)
    spill = output / 'spill'
    spill.mkdir()
    start = time.monotonic()
    stage = 'connect'
    report: dict = {'status': 'running', 'expected': expected, 'namespace': namespace,
              'scope': 'original logical snapshot acquisition only; conversion and hosted writes unqualified'}
    (output / 'EXPECTED.json').write_text(json.dumps(expected, indent=2) + '\n')
    try:
        con = connect()
        try:
            configure(con, spill)
            stage = 'source-runtime-pins'
            report['startRuntime'] = runtime_identity(con)
            report['actualSourceSettings'] = actual_settings(con)
            if expected_runtime is not None and report['startRuntime'] != expected_runtime:
                raise ValueError('Source engine or extension differs from its frozen runtime pins')
            stage = 'start-metadata'
            metadata = iceberg._table_metadata(con, RECORD_TYPES['comments'], namespace=namespace)
            report['startIdentity'] = identity(metadata)
            if report['startIdentity'] != selected or fields({**expected, 'schemas': metadata['schemas']}) != expected_fields:
                raise ValueError('Source identity or schema changed before capture')
            snapshot = iceberg.CatalogSnapshot(selected['tableUuid'], selected['snapshotId'], selected['schemaId'])
            query = iceberg._snapshot_query(RECORD_TYPES['comments'], snapshot, namespace=namespace)
            report['query'] = query
            stage = 'logical-snapshot-export'
            source = output / 'comments.parquet'
            report['writer'] = {}

            def export_phase(value):
                nonlocal stage
                stage = value

            write_original_batches(con, query, source, schema, observed=report['writer'], progress=export_phase)
        finally:
            con.close()
        stage = 'fresh-end-metadata'
        end = connect()
        try:
            report['endRuntime'] = runtime_identity(end)
            if report['endRuntime'] != report['startRuntime']:
                raise ValueError('Source engine or extension changed during capture')
            metadata = iceberg._table_metadata(end, RECORD_TYPES['comments'], namespace=namespace)
            report['endIdentity'] = identity(metadata)
            if report['endIdentity'] != selected or fields({**expected, 'schemas': metadata['schemas']}) != expected_fields:
                raise ValueError('Source identity or schema changed during capture')
        finally:
            end.close()
        stage = 'local-source-verification'
        footer = pq.ParquetFile(source)
        if not footer.schema_arrow.equals(schema, check_metadata=True):
            raise ValueError('Exported original source schema differs')
        with duckdb.connect() as local:
            configure(local, spill)
            report['actualLocalSettings'] = actual_settings(local)
            local.from_parquet(str(source)).create_view('captured_comments')
            population = local.execute('SELECT count(*), count(comment_id), count(DISTINCT comment_id) '
                                       'FROM captured_comments').fetchone()
            if population is None:
                raise ValueError('Logical population query returned no row')
            if population[0] != footer.metadata.num_rows or population[0] != report['writer']['rows']:
                raise ValueError('Logical and footer populations differ')
            local.execute('COPY (SELECT agency_code, count(*) AS rows FROM captured_comments '
                          'GROUP BY agency_code ORDER BY agency_code NULLS FIRST) TO ? '
                          '(FORMAT PARQUET, COMPRESSION ZSTD)', [str(output / 'agency-populations.parquet')])
        report.update(status='captured', source={'sha256': digest(source), 'bytes': source.stat().st_size,
                      'rows': population[0], 'schema': str(schema)},
                      nonnullIds=population[1], distinctNonnullIds=population[2],
                      nullIds=population[0] - population[1], duplicateNonnullRows=population[1] - population[2],
                      agencyPopulationsSha256=digest(output / 'agency-populations.parquet'))
        source.chmod(0o444)
    except BaseException as error:
        # Catalog SQL errors may contain inlined credentials. Retain only type
        # and phase, never the exception message, traceback or connection text.
        report.update(status='refused', failedPhase=stage, errorType=type(error).__name__)
        raise RuntimeError(f'Comments capture refused during {stage}; private evidence retained') from None
    finally:
        report['elapsedSeconds'] = time.monotonic() - start
        (output / 'RESULT.json').write_text(json.dumps(report, indent=2) + '\n')
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('expected', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--expected-runtime', type=Path, required=True,
                        help='JSON containing the frozen engine and loaded extension identity')
    args = parser.parse_args()
    try:
        expected_runtime = json.loads(args.expected_runtime.read_text())
        if not isinstance(expected_runtime, dict):
            raise ValueError('Frozen runtime must be a JSON object')
        capture(json.loads(args.expected.read_text()), args.output, namespace=args.namespace,
                expected_runtime=expected_runtime)
    except Exception as error:
        print('REFUSED: ' + type(error).__name__)
        return 1
    print('CAPTURED: original logical comments input')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
