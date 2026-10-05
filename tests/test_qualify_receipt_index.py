"""Private qualification uses actual producer bytes and preserves failed evidence."""
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from tests.test_fec_receipt_adapter import fixture


def invoke(tmp_path, *, wrong_pin=False):
    subject, receipts, index = fixture(tmp_path)
    pin = index['families']['fec-query']['etlReceipts']
    admitted = {name: pin[name] for name in ('sha256', 'byteSize', 'rows')}
    if wrong_pin:
        admitted['sha256'] = 'sha256:'+'0'*64
    pin_file = tmp_path/'pin.json'
    pin_file.write_text(json.dumps(admitted))
    output = tmp_path/'qualification'
    root = Path(__file__).resolve().parents[1]
    before = receipts.read_bytes()
    run = subprocess.run([sys.executable, str(root/'scripts/qualify_receipt_index.py'),
        str(receipts), str(output), '--pin', str(pin_file), '--dataset', 'fec_receipts',
        '--member', str(subject), '--force-small-fixture'], cwd=root,
        env={**os.environ, 'PYTHONPATH': str(root/'src')}, text=True, capture_output=True, timeout=90)
    assert receipts.read_bytes() == before
    return run, output


def test_actual_producer_local_upload_and_locked_http(tmp_path):
    run, output = invoke(tmp_path)
    assert run.returncode == 0, run.stdout+'\n'+run.stderr
    result = json.loads((output/'RESULT.json').read_text())
    assert result['status'] == 'passed' and result['scope'] == 'local-simulation'
    assert result['productionActions'] == []
    phases = [json.loads(line) for line in (output/'phases.jsonl').read_text().splitlines()]
    completed = [phase for phase in phases if phase['status'] == 'passed']
    assert len(completed) == 6
    serving = next(phase for phase in completed if phase['phase'].startswith('local-http'))
    assert serving['lookups'][-1]['returnedPerKey'] == [1, 0, 1]
    requests = [json.loads(line) for line in (output/'http-requests.jsonl').read_text().splitlines()]
    assert any(request['status'] == 206 and request['bodyBytes'] > 0 for request in requests)
    assert any(request['range'] is None and request['bodyBytes'] > 0 for request in requests)
    assert all(request['error'] is None for request in requests)
    assert {request['probe'] for request in requests} <= {'cold-admission', 'one-key',
        'up-to-ten-keys', 'up-to-hundred-keys', 'repeat-and-absent'}
    assert any(request['probe'] == 'cold-admission' for request in requests)
    assert any(request['probe'] == 'one-key' for request in requests)
    assert all(request['startedMonotonicNs'] <= request['finishedMonotonicNs'] for request in requests)
    # Exact byte equality is retained after upload and serving for each named member.
    for member in json.loads((output/'objects.json').read_text()):
        assert Path(member['source']).read_bytes() == (output/'objects'/member['route'].lstrip('/')).read_bytes()


def test_wrong_admitted_pin_refuses_and_retains_failed_phase(tmp_path):
    run, output = invoke(tmp_path, wrong_pin=True)
    assert run.returncode != 0 and 'Receipt differs from its admitted' in run.stderr
    phases = [json.loads(line) for line in (output/'phases.jsonl').read_text().splitlines()]
    assert phases[-1]['status'] == 'failed' and phases[-1]['phase'] == 'receipt-pin'
    assert not (output/'RESULT.json').exists()
    assert not (output/'objects').exists()


def test_explicit_private_artifact_binding_and_publication_descriptor(tmp_path):
    from scripts.qualify_receipt_index import bind_key_index_for_qualification
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.generations import build_generation
    from spicy_regs.receipt_key_index import KEY
    from spicy_regs.receipt_key_index_writer import build_key_index
    from spicy_regs.sources.publication import empty_index, publish_generation
    from tests.generation_fakes import Store
    subject, receipts, _ = fixture(tmp_path)
    before = receipts.read_bytes()
    generation = tmp_path/'private-generation'
    original = build_generation(generation, family='fec-query', files=[subject], expected_keys=[subject.name],
        receipt_path=receipts, receipt_policies=[installed_policies()['fec_receipts']],
        receipt_generation_id='qualification-publisher', read_snapshot=empty_index())
    sidecar = tmp_path/'measured-index.parquet'
    descriptor = build_key_index(receipts, sidecar, force=True)
    assert descriptor is not None
    admitted = bind_key_index_for_qualification(generation, sidecar, descriptor)
    assert admitted.root['spec'] == {**original.root['spec'], 'etlReceipts': {
        **original.root['spec']['etlReceipts'], 'keyIndex': descriptor}}
    for field in ('inputs', 'producer'):
        assert admitted.root[field] == original.root[field]
    assert (generation/'etl_receipts.parquet').read_bytes() == before == receipts.read_bytes()
    store = Store()
    publication = publish_generation(generation, client=store, bucket='private-test', prior_index=empty_index())
    selected = publication['families']['fec-query']
    assert selected['etlReceipts']['keyIndex'] == descriptor
    assert KEY not in selected['tables']
    with pytest.raises(ValueError, match='already declares'):
        bind_key_index_for_qualification(generation, sidecar, descriptor)
