"""Live scheduler publication uses native rows and selected shared receipts."""

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.regulatory_publication import (
    finish_checkpoints,
    finish_dataset,
    restore_checkpoint,
    restore_dataset,
)
from spicy_regs.schemas import DOCKET


def test_checkpoint_family_preserves_other_retry_set_and_retires_empty(tmp_path):
    failed = {
        "agency": "EPA",
        "record_type": "dockets",
        "key": "raw/EPA/D/docket/D.json",
        "status": "unreadable",
        "reason": "truncated",
        "attempted_at": "2026-10-03",
        "attempts": 2,
    }
    pending = {
        "agency_code": "EPA",
        "docket_id": "D",
        "comment_id": "C",
        "posted_date": None,
        "phase": "fetch",
        "reason": "timeout",
        "source_json": "{}",
        "rule_version": "v1",
        "attempted_at": "2026-10-03",
        "attempts": 1,
    }
    finish_checkpoints(tmp_path, {"failed_keys": [failed], "pending_comment_text": [pending]}, publish=False)
    assert restore_checkpoint(tmp_path, "failed_keys") == [failed]
    assert restore_checkpoint(tmp_path, "pending_comment_text") == [pending]
    finish_checkpoints(tmp_path, {"failed_keys": [], "pending_comment_text": [pending]}, publish=False)
    assert restore_checkpoint(tmp_path, "failed_keys") == []
    assert restore_checkpoint(tmp_path, "pending_comment_text") == [pending]
    assert not (tmp_path / "failed_keys.parquet").exists()
    assert not (tmp_path / "pending_comment_text.parquet").exists()


def test_native_dataset_roundtrip_and_missing_receipt_refusal(tmp_path):
    work = tmp_path / ".processing"
    work.mkdir()
    source = work / "dockets.parquet"
    row = {name: None for name in DOCKET.schema}
    row.update(docket_id="D", agency_code="EPA", title="Decision", rin="Not Assigned")
    pq.write_table(
        pa.Table.from_pylist([row], schema=pa.schema([(name, pa.string()) for name in DOCKET.schema])), source
    )
    finish_dataset(tmp_path, "dockets", source, publish=False)
    native = pq.read_table(tmp_path / "dockets.parquet")
    from spicy_regs.transforms.regulations_receipts import policy

    assert native.schema.equals(policy("dockets").subject_schema)
    assert restore_dataset(tmp_path, "dockets", work / "restored.parquet")
    assert pq.read_table(work / "restored.parquet").to_pylist() == [row]
    selected = json.loads((tmp_path / ".native-state/selection.json").read_text())["dockets"]
    Path(selected["receipts"]["path"]).unlink()
    with pytest.raises((ValueError, OSError)):
        restore_dataset(tmp_path, "dockets", work / "restored.parquet")


def _source(path, identities):
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(
        [{name: identity if name == 'docket_id' else None for name in DOCKET.schema} for identity in identities],
        schema=pa.schema([(name, pa.string()) for name in DOCKET.schema])), path)
    return path


def test_captured_remote_generation_overrides_stale_local_and_drives_commit(tmp_path, monkeypatch):
    from tests.regulatory_publication_fakes import install
    from spicy_regs.selected_generations import SelectedInputs
    from spicy_regs.sources import publication

    remote = install(monkeypatch)
    root = tmp_path / 'worker'
    finish_dataset(root, 'dockets', _source(root / 'input.parquet', ['old-local']), publish=False)
    publisher = tmp_path / 'publisher'
    publish_inputs = SelectedInputs(publisher, publisher / 'selected')
    finish_dataset(publisher, 'dockets', _source(publisher / 'input.parquet', ['new-remote']),
                   publish=True, inputs=publish_inputs)
    inputs = SelectedInputs(root, root / 'selected')
    restored = root / 'restored.parquet'
    assert restore_dataset(root, 'dockets', restored, inputs=inputs)
    assert pq.read_table(restored)['docket_id'].to_pylist() == ['new-remote']
    # An intervening valid publication changes the exact generation we read.
    finish_dataset(publisher, 'dockets', _source(publisher / 'input.parquet', ['intervening']),
                   publish=True, inputs=publish_inputs)
    before = remote.objects[publication.INDEX_V2_KEY]
    with pytest.raises((ValueError, RuntimeError), match='changed|concurrent|stale|Conflict'):
        finish_dataset(root, 'dockets', restored, publish=True, inputs=inputs)
    assert remote.objects[publication.INDEX_V2_KEY] == before
    local = SelectedInputs(root, root / 'local-read', public_url='').select('dockets')
    assert local is not None
    assert pq.read_table(local.subjects[0])['docket_id'].to_pylist() == ['old-local']


def test_checkpoint_read_uses_captured_remote_selection_over_local(tmp_path, monkeypatch):
    from tests.regulatory_publication_fakes import install
    from spicy_regs.selected_generations import SelectedInputs

    install(monkeypatch)
    root = tmp_path / 'worker'
    failed = {'agency': 'EPA', 'record_type': 'dockets', 'key': 'remote-key', 'status': 'unreadable',
              'reason': 'retry', 'attempted_at': '2026-10-03', 'attempts': 1}
    finish_checkpoints(root, {'failed_keys': [], 'pending_comment_text': []}, publish=False)
    publisher = tmp_path / 'publisher'
    inputs = SelectedInputs(publisher, publisher / 'selected')
    finish_checkpoints(publisher, {'failed_keys': [failed], 'pending_comment_text': []},
                       publish=True, inputs=inputs)
    selected = SelectedInputs(root, root / 'selected')
    assert restore_checkpoint(root, 'failed_keys', inputs=selected) == [failed]


def test_file_generation_retains_resolvable_inputs_after_source_removal(tmp_path):
    from hashlib import sha256
    from spicy_regs.etl_receipts import resolve_receipt_witness
    from spicy_regs.selected_generations import SelectedInputs

    source = _source(tmp_path / 'processing' / 'dockets.parquet', ['D'])
    finish_dataset(tmp_path, 'dockets', source, publish=False)
    source.unlink()
    selected = SelectedInputs(tmp_path, tmp_path / 'selected', public_url='').select('dockets')
    assert selected is not None
    for receipt in pq.read_table(selected.receipts).to_pylist():
        for witness in receipt['witnesses']:
            assert sha256(resolve_receipt_witness(receipt, witness)).hexdigest() == witness['sha256']
    restored = tmp_path / 'restored.parquet'
    assert restore_dataset(tmp_path, 'dockets', restored)
    assert pq.read_table(restored)['docket_id'].to_pylist() == ['D']


def test_file_rewrites_keep_original_api_evidence_in_immutable_prior_after_input_removal(tmp_path):
    from hashlib import sha256
    from spicy_regs.etl_receipts import ReceiptContext, exact_json, resolve_receipt_witness, decode_exact_json, _digest
    from spicy_regs.generations import build_generation, verify_generation
    from spicy_regs.selected_generations import SelectedInputs, remember_generation
    from spicy_regs.transforms.regulations_receipts import write_records, policy

    original = {'source_api': {'id': 'D', 'unmapped': 'evidence'}}
    witness = {'source_id': 'api', 'source_uri': None,
               'sha256': sha256(exact_json(original).encode()).hexdigest(),
               'locator': 'receipt.values.raw_source_record (canonical exact_json)', 'body_version': 'body-v1'}
    subject, receipts = write_records('dockets', [(original, ReceiptContext('initial', 'api', 'api-reader', [witness]))],
                                      tmp_path / 'initial', project=lambda row: {'docket_id': row['source_api']['id']})
    # Select the actual admitted immutable generation. Temporary writer inputs may be removed,
    # while the original selected member remains the authority for its own raw API evidence.
    initial = tmp_path / 'generations' / 'initial'
    artifact = build_generation(initial, family='dockets', files=[subject], expected_keys=[subject.name],
                                receipt_path=receipts, receipt_policies=[policy('dockets')],
                                receipt_generation_id='initial')
    remember_generation(tmp_path, initial, artifact)
    initial_receipt_bytes = (initial / 'etl_receipts.parquet').read_bytes()
    [original_receipt] = pq.read_table(initial / 'etl_receipts.parquet').to_pylist()
    rewritten = []
    for ordinal in range(2):
        source = tmp_path / f'processing-{ordinal}.parquet'
        assert restore_dataset(tmp_path, 'dockets', source)
        finish_dataset(tmp_path, 'dockets', source, publish=False)
        source.unlink()
        selected = SelectedInputs(tmp_path, tmp_path / f'checked-{ordinal}', public_url='').select('dockets')
        assert selected is not None
        [current] = [r for r in pq.read_table(selected.receipts).to_pylist() if r['outcome'] == 'accepted']
        rewritten.append(current)
    subject.unlink()
    receipts.unlink()
    first, second = rewritten
    assert first['subject_version'] == original_receipt['subject_version']
    assert first['processing_json'] != original_receipt['processing_json']
    assert decode_exact_json(first['diagnostic_json'])['prior_receipt'] == {
        'receipt_id': original_receipt['receipt_id'], 'generation_id': original_receipt['generation_id'],
        'processing_sha256': _digest(decode_exact_json(original_receipt['processing_json']))}
    assert second == first
    assert 'retained_processing' not in decode_exact_json(first['diagnostic_json'])
    assert witness not in first['witnesses']
    for current_witness in second['witnesses']:
        assert resolve_receipt_witness(second, current_witness)
    # Exact original source evidence resolves in the pinned original member after alias removal.
    assert verify_generation(initial, expected_pin=artifact.pin).pin == artifact.pin
    assert (initial / 'etl_receipts.parquet').read_bytes() == initial_receipt_bytes
    assert resolve_receipt_witness(original_receipt, witness) == exact_json(original).encode()
    with pytest.raises(ValueError, match='no retained payload'):
        resolve_receipt_witness(second, witness)
