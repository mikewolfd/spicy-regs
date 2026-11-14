#!/usr/bin/env python3
"""Retain actual values for seven pinned attachment witnesses; never judge semantics or write remotely."""
from __future__ import annotations

import argparse
from dataclasses import asdict
import hashlib
import io
import json
import os
from pathlib import Path
import re
import struct
import subprocess
import sys
from typing import Any, cast
from urllib.parse import urlsplit

if __package__ is None:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import httpx
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from scripts import read_comment_attachment_group as runner
from spicy_regs import operational_runner as owner
from spicy_regs.explorer_publications import _range, _version
from spicy_regs.pipelines.comments_mirror import receipt_files, receipt_source
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg, r2
from spicy_regs.sources.remote_parquet import _PublishedRanges

MiB = 2**20
COLUMNS = ['comment_id', 'agency_code', 'modify_date', 'attachments']
MAX_METADATA = 64 * MiB
MAX_FOOTER = 16 * MiB
MAX_NATIVE_COLUMNS = 1024 * MiB
MAX_PUBLIC_RANGES = 128 * MiB
MAX_GROUP_DECODED = 64 * MiB
LIMITS = {**runner.LIMITS, 'wall_seconds': 600, 'rss_bytes': 2 * 2**30,
          'output_bytes': 64 * MiB, 'start_free_bytes': 4 * 2**30, 'stop_free_bytes': 4 * 2**30}
UA = 'spicy-regs-attachment-witnesses/1.0'
require = runner.require


def sha(data):
    return hashlib.sha256(data).hexdigest()


def subject_pin(value):
    require(isinstance(value, dict) and set(value) == {'table_uuid', 'snapshot_id', 'schema_id'}, 'Exact subject pin required')
    require(isinstance(value['table_uuid'], str) and bool(value['table_uuid']), 'Table UUID required')
    require(all(type(value[k]) is int and value[k] >= 0 for k in ('snapshot_id', 'schema_id')), 'Integer snapshot/schema required')
    return value


class ByteBudget:
    def __init__(self, maximum):
        self.maximum, self.used, self.events = maximum, 0, []

    def reserve(self, size):
        require(type(size) is int and size > 0 and self.used + size <= self.maximum, 'Range byte budget exceeded before request')
        self.used += size

    def record(self, path, start, data, etag):
        self.events.append({'path': path, 'start': start, 'bytes': len(data), 'sha256': sha(data), 'etag': etag})


def footer(size, read):
    require(size >= 12, 'Parquet object too short')
    tail = read(size - 8, size - 1)
    count = struct.unpack('<I', tail[:4])[0]
    require(tail[4:] == b'PAR1' and 0 < count <= min(MAX_FOOTER, size - 12), 'Footer exceeds admitted bound or is invalid')
    body = read(size - 8 - count, size - 1)
    return pq.ParquetFile(io.BytesIO(b'PAR1' + body)).metadata


def projected_bytes(metadata, groups):
    compressed = uncompressed = 0
    for group in groups:
        row_group = metadata.row_group(group)
        for i in range(row_group.num_columns):
            column = row_group.column(i)
            if column.path_in_schema.split('.')[0] in COLUMNS:
                compressed += column.total_compressed_size
                uncompressed += column.total_uncompressed_size
    return compressed, uncompressed


def candidate_groups(metadata, ids):
    groups = []
    for group in range(metadata.num_row_groups):
        row_group = metadata.row_group(group)
        keys = [row_group.column(i) for i in range(row_group.num_columns)
                if row_group.column(i).path_in_schema == 'comment_id']
        require(len(keys) == 1, 'One comment_id leaf required')
        stats = keys[0].statistics
        if stats is None or not stats.has_min_max or any(stats.min <= value <= stats.max for value in ids):
            groups.append(group)
    return groups


def read_sql(con, sql, parameters=()):
    statements = duckdb.extract_statements(sql)
    require(len(statements) == 1 and statements[0].type == duckdb.StatementType.SELECT,
            'Only one observation SELECT/DESCRIBE is permitted')
    cursor = con.execute(sql, parameters)
    names = [column[0] for column in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def native(args, config, report):
    """Admit all projected data columns and complete position-delete objects before the native query."""
    client, bucket = r2.verified_target_client()
    budget = ByteBudget(MAX_METADATA)
    report['range_budget'] = budget.__dict__
    namespace = iceberg._catalog_namespace(COMMENT)
    with iceberg._connect() as con:
        con.execute("SET memory_limit='1GB'; SET threads=1; SET max_temp_directory_size='0B'; SET http_retries=0; SET http_timeout=20")
        observed = iceberg._read_snapshot(con, COMMENT, namespace=namespace)
        report['observed_subject'] = asdict(observed)
        require(asdict(observed) == args.native_snapshot, 'Selected native snapshot differs')
        inventory = read_sql(con, "SELECT DISTINCT file_path,manifest_content,content FROM iceberg_metadata(?, snapshot_from_id=?) WHERE status <> 'DELETED'", [iceberg._qualified(COMMENT), observed.snapshot_id])
        report['inventory_snapshot_id'] = observed.snapshot_id
        require(0 < len(inventory) <= 2048, 'Native file inventory outside the admitted bound')
        require(len({r['file_path'] for r in inventory}) == len(inventory), 'Native object has ambiguous manifest entries')
        projected, objects = 0, []
        for item in inventory:
            path = urlsplit(item['file_path'])
            require(path.scheme == 's3' and path.netloc == bucket and not path.query and not path.fragment, 'Unexpected native object target')
            key = path.path.lstrip('/')
            head = client.head_object(Bucket=bucket, Key=key)
            size, etag = head['ContentLength'], head.get('ETag')
            require(size > 0 and isinstance(etag, str) and etag, 'Native object lacks size/version')
            record = {**item, 'bytes': size, 'etag': etag}
            objects.append(record)
            if item['manifest_content'] == 'DATA':
                def read(start, end):
                    budget.reserve(end - start + 1)
                    reply = client.get_object(Bucket=bucket, Key=key, Range=f'bytes={start}-{end}', IfMatch=etag)
                    try:
                        require(reply.get('ETag') == etag and reply.get('ContentRange') == f'bytes {start}-{end}/{size}', 'Native range identity differs')
                        data = reply['Body'].read(end - start + 2)
                    finally:
                        reply['Body'].close()
                    require(len(data) == end - start + 1, 'Native range length differs')
                    budget.record(item['file_path'], start, data, etag)
                    return data
                metadata = footer(size, read)
                count, _ = projected_bytes(metadata, range(metadata.num_row_groups))
                record['projected_column_bytes'] = count
                projected += count
            else:
                # Equality deletes may require extra subject columns; do not under-admit them.
                require(item['manifest_content'] == 'DELETE' and item['content'] == 'POSITION_DELETES', 'Unqualified delete format')
                projected += size
            report['admission'] = {'objects': objects, 'projected_columns_and_deletes_bytes': projected,
                                   'maximum': MAX_NATIVE_COLUMNS, 'metadata_bytes': budget.used}
            owner.durable(args.workdir / 'observation.json', report)
            require(projected <= MAX_NATIVE_COLUMNS, 'Native projected columns/delete bytes exceed admission')
        with iceberg._connect() as admission_check:
            admitted_subject = asdict(iceberg._read_snapshot(admission_check, COMMENT, namespace=namespace))
        report['subject_after_admission'] = admitted_subject
        require(admitted_subject == args.native_snapshot, 'Native moved during admission')
        relation = iceberg._snapshot_query(COMMENT, observed, namespace=namespace)
        report['schema'] = read_sql(con, f'DESCRIBE ({relation})')
        predicate = ' OR '.join('(agency_code=? AND comment_id=? AND modify_date=?)' for _ in config['witnesses'])
        parameters = [v for w in config['witnesses'] for v in (w['agency_code'], w['comment_id'], w['parent_modify_date'])]
        sql = 'SELECT ' + ','.join(COLUMNS) + f' FROM ({relation}) WHERE {predicate} LIMIT 15'
        report['query'] = {'sql': sql, 'parameters': parameters}
        report['rows'] = read_sql(con, sql, parameters)
        require(len(report['rows']) < 15, 'Witness row bound reached; review multiplicity')
        for item in objects:
            key = urlsplit(item['file_path']).path.lstrip('/')
            after = client.head_object(Bucket=bucket, Key=key)
            require(after.get('ETag') == item['etag'] and after['ContentLength'] == item['bytes'], 'Native object changed during reading')
    # Fresh connection: do not rely on the prior connection's metadata cache.
    with iceberg._connect() as con:
        after = asdict(iceberg._read_snapshot(con, COMMENT, namespace=namespace))
    report['subject_after'] = after
    require(after == args.native_snapshot, 'Native moved during witness reading; retain historical observation')


class PublicRanges(_PublishedRanges):
    """Use the maintained seekable reader and strict range verifier, charging bytes before every GET."""
    def __init__(self, client, url, member, budget):
        super().__init__(client, url, byte_size=member['bytes'])
        require(self.etag == member['etag'], 'Public ETag differs from accepted publication')
        self.budget = budget

    def _get(self, start, end):
        self.budget.reserve(end - start + 1)
        data = _range(self.client, self.url, start, end, self.size, self.etag)
        self.budget.record(self.url, start, data, self.etag)
        return data


def public_json(client, name, maximum=2 * MiB):
    with client.stream('GET', r2.VERIFIED_PUBLIC_URL + '/' + name, headers={'Cache-Control': 'no-cache'}) as response:
        response.raise_for_status()
        parts, size = [], 0
        for data in response.iter_bytes():
            size += len(data)
            require(size <= maximum, 'Public control exceeds byte bound')
            parts.append(data)
    return b''.join(parts)


def public(args, config, report):
    require(re.fullmatch(r'[a-f0-9]{64}', args.publication_receipt_sha256), 'Exact public receipt SHA required')
    require(re.fullmatch(r'sha256:[a-f0-9]{64}', args.generation_digest), 'Exact generation digest required')
    budget = ByteBudget(MAX_PUBLIC_RANGES)
    report['range_budget'] = budget.__dict__
    with httpx.Client(timeout=20, follow_redirects=False, trust_env=False, headers={'User-Agent': UA}) as client:
        raw = public_json(client, 'comments-publication.json')
        require(sha(raw) == args.publication_receipt_sha256, 'Public receipt differs from accepted bytes')
        receipt = json.loads(raw)
        files, source = receipt_files(receipt), receipt_source(receipt)
        if files is None or source is None:
            raise ValueError('Public receipt has no qualified files/source')
        require(asdict(source.subject()) == args.native_snapshot, 'Publication belongs to a different native snapshot')
        index = json.loads(public_json(client, 'publication.v2.json'))
        require(receipt.get('generation', {}).get('artifactDigest') == args.generation_digest
                == index['families']['comments']['artifactDigest'], 'Selected comments generation differs')
        report['publication'] = {'receipt_sha256': sha(raw), 'generation': receipt['generation'], 'source': receipt['source']}
        report['members'] = []
        for agency in sorted({w['agency_code'] for w in config['witnesses']}):
            witnesses = [w for w in config['witnesses'] if w['agency_code'] == agency]
            key = f'comments/agency/agency_code={agency}/part-0.parquet'
            require(key in files, 'Required agency is absent from publication receipt')
            member = files[key]
            url = r2.VERIFIED_PUBLIC_URL + '/' + key
            record = {'key': key, 'descriptor': member, 'partition_values': {'agency_code': agency},
                      'agency_identity_source': 'verified publication member path', 'rows': [], 'other_versions': []}
            report['members'].append(record)
            with PublicRanges(client, url, member, budget) as stream:
                metadata = footer(member['bytes'], stream._get)
                require(metadata.num_rows == member['rows'], 'Public footer population differs')
                ids = [w['comment_id'] for w in witnesses]
                groups = candidate_groups(metadata, ids)
                compressed, decoded = projected_bytes(metadata, groups)
                require(budget.used + compressed <= MAX_PUBLIC_RANGES, 'Projected public chunks exceed remaining range budget')
                require(all(projected_bytes(metadata, [g])[1] <= MAX_GROUP_DECODED for g in groups), 'Public projected row group exceeds decode admission')
                record['admission'] = {'row_groups': groups, 'compressed_bytes': compressed, 'uncompressed_bytes': decoded}
                owner.durable(args.workdir / 'observation.json', report)
                parquet = pq.ParquetFile(stream, metadata=metadata, pre_buffer=False, buffer_size=0)
                record['schema'] = str(parquet.schema_arrow)
                columns = [name for name in COLUMNS if name in parquet.schema_arrow.names]
                require(all(name in columns for name in ('comment_id', 'modify_date', 'attachments')), 'Public witness columns are absent')
                record['physical_columns_read'] = columns
                expected = {(w['comment_id'], w['agency_code'], w['parent_modify_date']) for w in witnesses}
                for batch in parquet.iter_batches(batch_size=128, row_groups=groups, columns=columns, use_threads=False):
                    # PyArrow installs this maintained compute function dynamically; its stub omits the attribute.
                    chosen = batch.filter(cast(Any, pc).is_in(batch.column(batch.schema.get_field_index('comment_id')), value_set=pa.array(ids)))
                    for row in chosen.to_pylist():
                        require('agency_code' not in row or row['agency_code'] == agency, 'In-file agency conflicts with verified partition path')
                        identity = (row['comment_id'], agency, row['modify_date'])
                        if identity in expected:
                            record['rows'].append(row)
                        else:
                            record['other_versions'].append({k: row[k] for k in COLUMNS[:3] if k in row})
                    require(len(record['rows']) <= 2 * len(witnesses) and len(record['other_versions']) <= 14, 'Public witness row bound exceeded')
                require(_version(client, url, {'path': key, 'byteSize': member['bytes'], 'etag': member['etag']}) == stream.etag, 'Public version moved')
        require(sha(public_json(client, 'comments-publication.json')) == args.publication_receipt_sha256, 'Public receipt moved during reading')
        after_index = json.loads(public_json(client, 'publication.v2.json'))
        require(after_index['families']['comments']['artifactDigest'] == args.generation_digest, 'Selected comments generation moved during reading')


def worker(args, config):
    report = {'surface': args.surface, 'runtime_ref': args.runtime_ref, 'operational_source': config['operational_source'],
              'operational_files': config['operational_files'], 'expected_subject': args.native_snapshot,
              'witnesses': config['witnesses'], 'semantic_verdict': None}
    try:
        (native if args.surface == 'native' else public)(args, config, report)
        report['transport_completed'] = True
    except BaseException as error:
        # Avoid recording provider errors that could contain a credential-bearing attach statement.
        report['failure_type'] = type(error).__name__
        message = str(error)
        for name in ('R2_CATALOG_TOKEN', 'R2_ACCESS_KEY_ID', 'R2_SECRET_ACCESS_KEY'):
            if value := os.environ.get(name):
                message = message.replace(value, '[REDACTED]')
        report['failure_message'] = message[:2000]
        raise RuntimeError(report['failure_type'] + ': ' + report['failure_message']) from None
    finally:
        require(len(json.dumps(report).encode()) <= 8 * MiB, 'Witness evidence exceeds output bound')
        owner.durable(args.workdir / 'observation.json', report)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--runtime-ref', required=True)
    parser.add_argument('--surface', choices=['native', 'public'], required=True)
    parser.add_argument('--native-snapshot', required=True)
    parser.add_argument('--publication-receipt-sha256', default='')
    parser.add_argument('--generation-digest', default='')
    parser.add_argument('--workdir', type=Path, required=True)
    parser.add_argument('--worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    args.native_snapshot = subject_pin(json.loads(args.native_snapshot))
    require(re.fullmatch(r'[a-f0-9]{40}', args.runtime_ref), 'Exact runtime commit required')
    require(os.getenv(iceberg.CATALOG_LOCK_ENV) == 'comments-catalog-write', 'Shared comments queue required')
    repository = Path(__file__).resolve().parents[1]
    require(subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repository, text=True).strip() == args.runtime_ref, 'Checkout differs from reviewed runtime')
    require(not subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=no'], cwd=repository, text=True).strip(), 'Tracked runtime changed')
    config = json.loads(Path(__file__).with_name('comment_attachment_witnesses.json').read_text())
    require(len(config['witnesses']) == 7 and len({w['comment_id'] for w in config['witnesses']}) == 7, 'Pinned witness set differs')
    for path, digest in config['operational_files'].items():
        require(sha((repository / path).read_bytes()) == digest, 'Operational source file changed: ' + path)
    if args.worker:
        worker(args, config)
    else:
        require(not args.workdir.exists(), 'Fresh work directory required; preserve prior observations')
        runner.supervise(args, command=[sys.executable, '-B', str(Path(__file__).resolve()), *sys.argv[1:], '--worker'], limits=LIMITS)


if __name__ == '__main__':
    main()
