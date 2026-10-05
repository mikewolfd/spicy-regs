"""Explicit pinned snapshot import preserves IDs and refuses changed inputs."""
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rulemaking_dataset import RulemakingDatasetPipeline
from spicy_regs.rulemaking_migration import digest, prepare_snapshot
from spicy_regs.transforms.regulations_shape import IDENTITIES, SOURCE_COLUMNS, TYPES
from spicy_regs.transforms.regulations_receipts import ReceiptInput, materialize_internal


@pytest.mark.parametrize('damage', [None, 'pointer', 'manifest', 'artifact', 'membership'])
def test_pinned_snapshot_migration_preserves_exact_rows_and_prior_ids(tmp_path, damage):
    sources = tmp_path / 'source'
    sources.mkdir()
    artifacts = {}
    for name in RulemakingDatasetPipeline.published_outputs:
        dataset = name.removesuffix('.parquet')
        rows = []
        if dataset == 'proceedings':
            row: dict[str, str | None] = {column: None for column, _ in SOURCE_COLUMNS[dataset]}
            for identity in IDENTITIES[dataset]:
                row[identity] = 'retained-proceeding-id'
            row['docket_ids_json'] = '["D"]'
            rows.append(row)
        path = sources / name
        pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(c, TYPES[t]) for c, t in SOURCE_COLUMNS[dataset]])), path)
        artifacts[name] = {'remote_key': 'materialized/rulemaking/snapshots/old/' + name,
                           'visibility': 'public', 'sha256': digest(path), 'bytes': path.stat().st_size,
                           'rows': len(rows)}
    pointer, manifest = tmp_path / 'pointer.json', tmp_path / 'manifest.json'
    pointer.write_text(json.dumps({'dataset': 'rulemaking', 'format_version': 2, 'snapshot_id': 'old',
                                   'manifest_key': 'materialized/rulemaking/snapshots/old/manifest.json'}))
    old = {'dataset': 'rulemaking', 'format_version': 2, 'snapshot_id': 'old', 'run_id': 'prior',
           'inputs': {'retained': 'source pins'}, 'artifacts': artifacts}
    if damage == 'membership':
        old['artifacts'].pop('comment_periods.parquet')
    manifest.write_text(json.dumps(old))
    pointer_sha, manifest_sha = digest(pointer), digest(manifest)
    if damage in ('pointer', 'manifest'):
        (pointer if damage == 'pointer' else manifest).write_text('{}')
    if damage == 'artifact':
        (sources / 'proceedings.parquet').write_bytes(b'changed')
    destination = tmp_path / 'native'
    kwargs: dict = dict(pointer=pointer, manifest=manifest, sources=sources, destination=destination,
                  expected_pointer_sha256=pointer_sha, expected_manifest_sha256=manifest_sha,
                  generation_id='migration', asserted_at='2026-10-05T00:00:00Z')
    if damage:
        with pytest.raises(ValueError):
            prepare_snapshot(**kwargs)
        assert not destination.exists()
        return
    result = prepare_snapshot(**kwargs)
    admitted = json.loads(result.read_text())
    assert admitted['inputs']['legacy_snapshot']['manifest'] == old
    assert admitted['artifacts']['etl_receipts.parquet']['visibility'] == 'internal'
    assert (destination / 'retained' / 'manifest.json').read_bytes() == manifest.read_bytes()
    restored = materialize_internal(ReceiptInput('proceedings', (destination / 'proceedings.parquet',),
                                               destination / 'etl_receipts.parquet', 'migration'), tmp_path / 'prior.parquet')
    assert pq.read_table(restored)['proceeding_id'].to_pylist() == ['retained-proceeding-id']
    with pytest.raises(FileExistsError):
        prepare_snapshot(**kwargs)
