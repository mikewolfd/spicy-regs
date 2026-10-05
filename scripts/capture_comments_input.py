"""Capture original logical comments from an explicitly pinned legacy snapshot.

Read only: use Iceberg time travel (which applies deletes), never initialize or
write catalog tables. Preserve every original column without normalization.
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


def capture(expected: dict, output: Path, *, namespace: str,
            connect: Callable = iceberg._connect) -> dict:
    """Fresh output retains failed attempts; fresh end connection checks identity."""
    if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', namespace):
        raise ValueError('Explicit simple legacy namespace required')
    expected_fields = fields(expected)
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
            con.execute(f"COPY ({query}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 20000)",
                        [str(source)])
        finally:
            con.close()
        stage = 'fresh-end-metadata'
        end = connect()
        try:
            metadata = iceberg._table_metadata(end, RECORD_TYPES['comments'], namespace=namespace)
            report['endIdentity'] = identity(metadata)
            if report['endIdentity'] != selected or fields({**expected, 'schemas': metadata['schemas']}) != expected_fields:
                raise ValueError('Source identity or schema changed during capture')
        finally:
            end.close()
        stage = 'local-source-verification'
        footer = pq.ParquetFile(source)
        schema = pa.schema([pa.field(field['name'], pa.string(), nullable=True) for field in expected_fields])
        if footer.schema_arrow != schema:
            raise ValueError('Exported original source schema differs')
        with duckdb.connect() as local:
            configure(local, spill)
            local.from_parquet(str(source)).create_view('captured_comments')
            population = local.execute('SELECT count(*), count(comment_id), count(DISTINCT comment_id) '
                                       'FROM captured_comments').fetchone()
            if population is None:
                raise ValueError('Logical population query returned no row')
            if population[0] != footer.metadata.num_rows:
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
    args = parser.parse_args()
    try:
        capture(json.loads(args.expected.read_text()), args.output, namespace=args.namespace)
    except Exception as error:
        print('REFUSED: ' + type(error).__name__)
        return 1
    print('CAPTURED: original logical comments input')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
