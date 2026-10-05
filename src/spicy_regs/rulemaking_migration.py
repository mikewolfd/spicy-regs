"""Prepare a pinned legacy rulemaking snapshot locally, preserving exact inputs.

This explicit import has no network, upload, or pointer replacement operation.
The normal scheduled pipeline continues to require native receipts.
"""
from __future__ import annotations

from hashlib import file_digest
import json
from pathlib import Path
import shutil

import pyarrow.parquet as pq

from spicy_regs.ontology.common import RunContext
from spicy_regs.pipelines.rulemaking_dataset import RulemakingDatasetPipeline
from spicy_regs.sources.publication import SNAPSHOT_FORMAT_VERSIONS, SNAPSHOT_ID
from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal


def digest(path: Path) -> str:
    with path.open('rb') as stream:
        return file_digest(stream, 'sha256').hexdigest()


def _exact_rows(original: Path, restored: Path) -> int:
    with pq.ParquetFile(original) as a, pq.ParquetFile(restored) as b:
        if not a.schema_arrow.equals(b.schema_arrow, check_metadata=True):
            raise ValueError(f'{original.name}: restored schema or metadata differs')
        rows = 0
        for left, right in zip(a.iter_batches(batch_size=2000), b.iter_batches(batch_size=2000), strict=True):
            if not left.equals(right):
                raise ValueError(f'{original.name}: restored row values or order differ')
            rows += left.num_rows
        return rows


def prepare_snapshot(*, pointer: Path, manifest: Path, sources: Path, destination: Path,
                     expected_pointer_sha256: str, expected_manifest_sha256: str,
                     generation_id: str, asserted_at: str) -> Path:
    """Admit only the named, hash-pinned complete legacy snapshot; never reset IDs."""
    if digest(pointer) != expected_pointer_sha256 or digest(manifest) != expected_manifest_sha256:
        raise ValueError('Pinned rulemaking pointer or manifest changed')
    old_pointer, old = json.loads(pointer.read_bytes()), json.loads(manifest.read_bytes())
    snapshot = old_pointer.get('snapshot_id', '')
    prefix = f'materialized/rulemaking/snapshots/{snapshot}/'
    if (not SNAPSHOT_ID.fullmatch(snapshot) or old_pointer.get('dataset') != 'rulemaking'
            or old_pointer.get('format_version') not in SNAPSHOT_FORMAT_VERSIONS
            or old_pointer.get('manifest_key') != prefix + 'manifest.json'
            or old.get('dataset') != 'rulemaking' or old.get('snapshot_id') != snapshot
            or old.get('format_version') != old_pointer['format_version'] or old.get('etlReceipts') is not None):
        raise ValueError('Expected one coherent legacy rulemaking snapshot')
    pipeline = RulemakingDatasetPipeline(skip_upload=True)
    if set(old.get('artifacts', {})) != set(pipeline.published_outputs):
        raise ValueError('Legacy rulemaking artifact membership differs from producer outputs')
    for name in pipeline.published_outputs:
        path, record = sources / name, old['artifacts'][name]
        if (record.get('remote_key') != prefix + name or record.get('visibility') != 'public'
                or path.stat().st_size != record.get('bytes') or digest(path) != record.get('sha256')
                or pq.ParquetFile(path).metadata.num_rows != record.get('rows')):
            raise ValueError(f'{name}: pinned legacy artifact differs')
    destination.mkdir(parents=True, exist_ok=False)
    retained = destination / 'retained'
    retained.mkdir()
    shutil.copyfile(pointer, retained / 'latest.json')
    shutil.copyfile(manifest, retained / 'manifest.json')
    if digest(retained / 'latest.json') != expected_pointer_sha256 or digest(retained / 'manifest.json') != expected_manifest_sha256:
        raise ValueError('Retained pointer or manifest changed during capture')
    for name in pipeline.published_outputs:
        shutil.copyfile(sources / name, retained / name)
        if digest(retained / name) != old['artifacts'][name]['sha256']:
            raise ValueError(f'{name}: artifact changed during capture')
        shutil.copyfile(retained / name, destination / name)
    context = RunContext.resolve(run_id=generation_id, asserted_at=asserted_at, prefix='rulemaking-migration')
    pipeline._classify_outputs(destination, context)
    restored = destination / 'restored'
    restored.mkdir()
    checks = {}
    for name in pipeline.published_outputs:
        output = materialize_internal(
            ReceiptInput(Path(name).stem, (destination / name,), destination / 'etl_receipts.parquet', generation_id),
            restored / name,
        )
        checks[name] = {'exact_rows_in_order': _exact_rows(retained / name, output)}
    inputs = {'legacy_snapshot': {'pointer_sha256': expected_pointer_sha256,
                                  'manifest_sha256': expected_manifest_sha256,
                                  'snapshot_id': snapshot, 'manifest': old},
              'previous_snapshot_id': snapshot}
    result, _, _ = pipeline._write_publication_files(destination, context=context, stages=(), input_snapshot=inputs)
    (destination / 'migration-checks.json').write_text(json.dumps(checks, indent=2) + '\n')
    return result
