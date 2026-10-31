"""Frozen receipt admission bounds payload work and preserves append recovery."""

import json
import os
from pathlib import Path

import pytest
from spicy_docs.storage import raw_archive as provider

from spicy_regs.source_catalog import maintenance, raw_archive
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.storage import connect, digest
from tests.source_catalog.test_input_refresh import file_rows
from tests.source_catalog import test_receipt_reuse

accepted = test_receipt_reuse.accepted


def freeze(args, name='receipts.json'):
    path = args['output'].parent / name
    result = maintenance.freeze_receipts(args['index'], path, expected_generation=args['expected_generation'],
                                         expected_index_sha256=digest(args['index']))
    return result, {'receipt_snapshot': path, 'receipt_snapshot_sha256': digest(path)}


def test_delta_opens_only_added_receipts_and_preserves_old_review_on_new_alias(accepted, monkeypatch):
    root, sha, args = accepted
    new = provider.CentralRawArchiveStore(root).record_receipt(
        files=[{'sha256': sha, 'bytes': 12, 'original_location': 'new alias'}], receipt={'run_id': 'new'})
    snapshot, options = freeze(args)
    assert snapshot['work']['inheritedReceiptReads'] == 3
    opened = []
    original = provider.CentralRawArchiveStore.open_member

    def track(self, key):
        opened.append(key)
        return original(self, key)

    monkeypatch.setattr(provider.CentralRawArchiveStore, 'open_member', track)
    inherited = {key for key in snapshot['receipts'] if key != new['object']}
    original_path_open = Path.open

    def forbid_inherited(path, *a, **kw):
        if path.is_relative_to(root) and str(path.relative_to(root)) in inherited:
            raise AssertionError('Inherited receipt body reopened')
        return original_path_open(path, *a, **kw)

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'open', forbid_inherited)
        result = maintenance.refresh_metadata(**args, **options)
    receipt_opens = [key for key in opened if key.startswith('acquisitions/')]
    assert receipt_opens == [new['object']]
    assert result['inputRefresh']['metadataExtraction']['new_receipt_payload_reads'] == 1
    assert result['inputRefresh']['metadataExtraction']['inherited_receipt_payloads_reused'] == 3
    fresh = root.parent / 'reference.sqlite'
    build(root, fresh, config=args['config'])
    assert file_rows(args['output']) == file_rows(fresh)
    with connect(args['output'], readonly=True, scope='all') as db:
        assert {row[0] for row in db.execute('SELECT status FROM files WHERE sha256=?', ('sha256:' + sha,))} == {'refused-source'}


def test_qualified_unchanged_receipts_need_no_payload_reads_on_next_process(accepted, monkeypatch):
    root, _sha, args = accepted
    _, options = freeze(args)
    first = maintenance.refresh_metadata(**args, **options)
    again = {**args, 'index': args['output'], 'output': root.parent / 'again.sqlite',
             'expected_generation': first['generation']}
    snapshot, options = freeze(again, 'again-receipts.json')
    assert snapshot['work']['inheritedReceiptReads'] == 0
    assert snapshot['work']['inheritedReceiptReused'] == 3
    import subprocess
    import sys

    program = '''
import json, sys
from pathlib import Path
from spicy_regs.source_catalog.maintenance import refresh_metadata
from spicy_docs.storage.raw_archive import CentralRawArchiveStore
args=json.loads(sys.argv[1])
root=Path(args['index']).parent/'raw'
original=Path.open
def guarded(path,*a,**kw):
    if path.is_relative_to(root/'acquisitions') or path.is_relative_to(root/'objects'):
        raise AssertionError('Inherited payload read')
    return original(path,*a,**kw)
Path.open=guarded
def forbidden(*a,**kw):raise AssertionError('Inherited provider payload read')
CentralRawArchiveStore.open_member=forbidden
for key in ('index','output','config','receipt_snapshot'):args[key]=Path(args[key])
print(json.dumps(refresh_metadata(**args)))
'''
    result = subprocess.run([sys.executable, '-c', program, json.dumps({**again, **options}, default=str)],
                            check=True, capture_output=True, text=True, env=os.environ)
    assert json.loads(result.stdout)['inputRefresh']['newReceipts'] == 0


def test_append_retains_valid_candidate_and_catch_up_reads_only_tail(accepted, monkeypatch):
    root, _sha, args = accepted
    _, options = freeze(args)
    original = raw_archive.RawImporter.import_receipts

    def append(self, *a, **kw):
        result = original(self, *a, **kw)
        provider.CentralRawArchiveStore(root).record_receipt(files=[], receipt={'run_id': 'later'})
        return result

    with monkeypatch.context() as patch:
        patch.setattr(raw_archive.RawImporter, 'import_receipts', append)
        result = maintenance.refresh_metadata(**args, **options)
    assert result['freshness']['status'] == 'pending-catch-up'
    assert len(result['freshness']['addedReceiptKeys']) == 1
    assert args['output'].is_file()
    advanced = {**args, 'index': args['output'], 'output': root.parent / 'caught-up.sqlite',
                'expected_generation': result['generation']}
    snapshot, options = freeze(advanced, 'tail.json')
    assert snapshot['work']['inheritedReceiptReads'] == 0
    caught_up = maintenance.refresh_metadata(**advanced, **options)
    assert caught_up['freshness']['status'] == 'current'
    assert caught_up['inputRefresh']['newReceipts'] == 1
    assert caught_up['inputRefresh']['metadataExtraction']['new_receipt_payload_reads'] == 1
    assert caught_up['ancestry'] == [args['expected_generation'], result['generation']]
    assert args['index'].is_file() and args['output'].is_file()


@pytest.mark.parametrize('mutation', ['receipt', 'missing', 'base', 'snapshot'])
def test_invalidated_frozen_input_refuses_before_clone(accepted, monkeypatch, mutation):
    root, _sha, args = accepted
    _, options = freeze(args)
    if mutation == 'receipt':
        path = next(root.glob('acquisitions/sha256/*/*'))
        path.write_bytes(path.read_bytes() + b' ')
    elif mutation == 'missing':
        next(root.glob('acquisitions/sha256/*/*')).unlink()
    elif mutation == 'base':
        with connect(args['index']) as db:
            db.execute("UPDATE collections SET title='mutated'")
    else:
        options['receipt_snapshot'].write_text('{}')
    monkeypatch.setattr(maintenance, '_clone_index', lambda *a: pytest.fail('Invalid inputs reached cloning'))
    with pytest.raises(ValueError):
        maintenance.refresh_metadata(**args, **options)
    assert not args['output'].exists()


def test_legacy_qualification_authenticates_receipt_and_base_bytes(accepted):
    root, _sha, args = accepted
    old = digest(args['index'])
    with connect(args['index']) as db:
        db.execute("UPDATE collections SET title='preexisting corruption'")
    with pytest.raises(ValueError, match='recorded identity'):
        maintenance.freeze_receipts(args['index'], root.parent / 'bad.json',
                                    expected_generation=args['expected_generation'], expected_index_sha256=old)


@pytest.mark.parametrize('key', ['../outside', 'acquisitions/sha256/aa/' + 'b' * 64, '/absolute'])
def test_provider_rejects_unsafe_selection_before_open(accepted, monkeypatch, key):
    root, _sha, _args = accepted
    monkeypatch.setattr(provider.CentralRawArchiveStore, 'open_member', lambda *a: pytest.fail('Opened unsafe key'))
    with pytest.raises(ValueError, match='selected acquisition receipt keys'):
        list(provider.iter_acquisition_receipts(root, receipt_keys=[key]))
    assert list(provider.iter_acquisition_receipts(root, receipt_keys=[])) == []


def _installed_refresh_export_select(accepted, monkeypatch, population):
    import shutil
    from spicy_regs.source_catalog import selection
    from spicy_regs.source_catalog.exporter import export

    root, _sha, args = accepted
    for number in range(population):
        provider.CentralRawArchiveStore(root).record_receipt(files=[], receipt={'run_id': 'inherited-' + str(number)})
    cache, public, staged = (root.parent / name for name in ('cache', 'public', 'staged'))
    cache.mkdir()
    report = build(root, cache / 'index.sqlite', config=args['config'])
    args = {**args, 'index': cache / 'index.sqlite', 'expected_generation': report['generation']}
    export(args['index'], public)
    pins = selection.raw_selection(root)
    selection.write(cache / 'index.sqlite.reconciliation.json', report)
    selection.write(cache / 'reconciliation.json', report)
    selection.write(cache / 'selection.json', {'generation': report['generation'], **pins})
    selection.write(cache / 'current.json', {'generation': report['generation'], **pins, 'index': 'index.sqlite',
                    'selection': 'selection.json', 'public_manifest': str(public / 'manifest.json'),
                    'public_manifest_sha256': digest(public / 'manifest.json').removeprefix('sha256:'),
                    'index_sha256': digest(args['index']).removeprefix('sha256:')})
    new = provider.CentralRawArchiveStore(root).record_receipt(files=[], receipt={'run_id': 'new'})
    snapshot, options = freeze(args)
    inherited = set(snapshot['receipts']) - {new['object']}
    opened = []
    original_open = Path.open
    original_provider_open = provider.CentralRawArchiveStore.open_member

    def guarded(path, *a, **kw):
        if path.is_relative_to(root) and str(path.relative_to(root)) in inherited:
            pytest.fail('Inherited receipt read after qualified admission')
        return original_open(path, *a, **kw)

    def observe(self, key):
        opened.append(key)
        return original_provider_open(self, key)

    with monkeypatch.context() as patch:
        patch.setattr(Path, 'open', guarded)
        patch.setattr(provider.CentralRawArchiveStore, 'open_member', observe)
        updated = maintenance.refresh_metadata(**args, **options)
        export(args['output'], staged)
        result = selection.select(args['output'], args['output'].with_suffix('.sqlite.reconciliation.json'),
                                  staged, cache, public, root, expected_generation=report['generation'],
                                  allow_inventory_change=True, **options)
    assert result['generation'] == updated['generation']
    assert opened == [new['object']]
    assert updated['inputRefresh']['metadataExtraction']['inherited_receipt_payloads_reused'] == population + 3
    assert selection.read(cache / 'current.json')['generation'] == selection.read(public / 'manifest.json')['generation']
    # This test owns its successful selection backup; originals remain untouched.
    shutil.rmtree(result['backup'])


@pytest.mark.parametrize('population', [0, 12])
def test_installed_refresh_export_select_payload_work_follows_delta(accepted, monkeypatch, population):
    _installed_refresh_export_select(accepted, monkeypatch, population)


@pytest.mark.parametrize('mutation', ['receipt', 'missing', 'root'])
def test_selection_rechecks_frozen_custody_after_canonical_export(accepted, monkeypatch, mutation):
    import shutil
    from spicy_regs.source_catalog import selection

    root, _sha, args = accepted
    previous = {}
    original_export = selection.export

    def change_after_export(*a, **kw):
        result = original_export(*a, **kw)
        cache = root.parent / 'cache'
        for path in (*cache.iterdir(), root.parent / 'public' / 'manifest.json'):
            if path.is_file():
                previous[path] = path.read_bytes()
        if mutation == 'root':
            retained = root.with_name(root.name + '-retained')
            root.rename(retained)
            shutil.copytree(retained, root)
        else:
            receipt = next(root.glob('acquisitions/sha256/*/*'))
            if mutation == 'missing':
                receipt.unlink()
            else:
                with open(receipt, 'ab') as stream:
                    stream.write(b' ')
        return result

    monkeypatch.setattr(selection, 'export', change_after_export)
    with pytest.raises(ValueError, match='custody changed|receipts disappeared'):
        _installed_refresh_export_select(accepted, monkeypatch, 0)
    assert previous
    assert all(path.read_bytes() == content for path, content in previous.items())
    assert args['output'].is_file()


def test_receipt_append_during_qualification_does_not_repeat_base_hash(accepted, monkeypatch):
    root, _sha, args = accepted
    hashed = []
    original = maintenance.digest

    def append(path):
        result = original(path)
        hashed.append(path)
        if path.is_relative_to(root / 'acquisitions'):
            provider.CentralRawArchiveStore(root).record_receipt(files=[], receipt={'run_id': 'appended-during-qualification'})
        return result

    with monkeypatch.context() as patch:
        patch.setattr(maintenance, 'digest', append)
        snapshot, options = freeze(args)
    assert hashed.count(args['index']) == 1
    assert len(snapshot['receipts']) == 3
    report = maintenance.refresh_metadata(**args, **options)
    assert report['freshness']['status'] == 'pending-catch-up'
    assert len(report['freshness']['addedReceiptKeys']) == 1


@pytest.mark.parametrize('fault', ['corrupt-receipt', 'unsafe-reference', 'missing-original'])
def test_invalid_new_reference_preserves_completed_base(accepted, fault):
    import hashlib

    root, _sha, args = accepted
    store = provider.CentralRawArchiveStore(root)
    body = b'new publisher original'
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob(sha, len(body), [body])
    receipt = store.record_receipt(files=[{'sha256': sha, 'bytes': len(body)}], receipt={'run_id': 'new'})
    path = root / receipt['object']
    if fault == 'corrupt-receipt':
        path.write_bytes(path.read_bytes() + b' ')
    elif fault == 'missing-original':
        (root / f'objects/sha256/{sha[:2]}/{sha}').unlink()
    else:
        value = json.loads(path.read_text())
        value['objects'][0]['object'] = value['files'][0]['object'] = '../outside'
        path.unlink()
        payload = json.dumps(value).encode()
        new_sha = hashlib.sha256(payload).hexdigest()
        path = root / f'acquisitions/sha256/{new_sha[:2]}/{new_sha}'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)
    before = args['index'].read_bytes()
    _, options = freeze(args)
    with pytest.raises((ValueError, FileNotFoundError)):
        maintenance.refresh_metadata(**args, **options)
    assert args['index'].read_bytes() == before
    assert not args['output'].exists()
