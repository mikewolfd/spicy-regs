"""Receipt additions and selected content refreshes preserve the inherited inventory."""
import hashlib
import gzip
import json

import pytest

from spicy_docs.storage import raw_archive as archive
from spicy_regs.source_catalog import raw_archive, maintenance
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.storage import connect
from tests.source_catalog.test_raw_archive import base_archive


def capture(root, body):
    store = archive.CentralRawArchiveStore(root)
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob(sha, len(body), [body])
    return store.record_receipt(files=[{'sha256': sha, 'bytes': len(body),
        'dataset_id': 'family:federal-register',
        'source_url': 'https://www.federalregister.gov/api/v1/documents.json'}], receipt={'status': 'captured'})


@pytest.fixture
def prepared(tmp_path, monkeypatch, request):
    root = tmp_path / 'raw'
    base_archive(root)
    capture(root, b'<html><title>Federal Register, Volume 91 Issue 188 (Wednesday, September 30, 2026)</title></html>')
    if getattr(request, 'param', 'receipt') == 'base':
        path, receipt = next(archive.iter_acquisition_receipts(root))
        snapshot = json.loads((root / 'snapshot.json').read_text())
        for name in ('objects', 'files'):
            index_path = root / (name + '.jsonl.gz')
            body = gzip.decompress(index_path.read_bytes())
            body += b''.join((json.dumps(row) + '\n').encode() for row in receipt[name])
            index_path.write_bytes(gzip.compress(body, mtime=0))
            snapshot['indexes'][index_path.name] = hashlib.sha256(index_path.read_bytes()).hexdigest()
        (root / 'snapshot.json').write_text(json.dumps(snapshot))
        path.unlink()
    config = tmp_path / 'registry.json'
    config.write_text('{}')
    index = tmp_path / 'base.sqlite'
    extract = raw_archive.extract_metadata
    with monkeypatch.context() as patch:
        patch.setattr(raw_archive, 'extract_metadata', lambda dataset, body:
                      {'outcome': 'unsupported'} if dataset == 'family:federal-register' else extract(dataset, body))
        report = build(root, index, config=config)
    return root, dict(index=index, output=tmp_path / 'candidate.sqlite', config=config,
                      expected_generation=report['generation'], include_acquisitions=True,
                      refresh_content=('family:federal-register',))


def file_rows(index):
    with connect(index, readonly=True, scope='all') as db:
        return [tuple(r) for r in db.execute('SELECT id,collection_id,status,data FROM files ORDER BY id')]


@pytest.mark.parametrize('prepared', ['base', 'receipt'], indirect=True)
def test_delta_matches_fresh_import_without_replaying_base(prepared, monkeypatch):
    root, args = prepared
    before = args['index'].read_bytes()
    capture(root, b'{"results":[{"document_number":"2026-21812","publication_date":"2026-10-01"}]}')
    with monkeypatch.context() as patch:
        def forbidden(*args, **kwargs):
            raise AssertionError('The unchanged base must not be replayed')
        patch.setattr(archive, 'iter_base_records', forbidden)
        result = maintenance.refresh_metadata(**args)
    assert args['index'].read_bytes() == before
    assert result['inputRefresh']['newReceipts'] == 1
    assert result['inputRefresh']['refreshedFileLocations'] == 1
    assert result['inputRefresh']['baseInventoryReplayed'] is False
    fresh = root.parent / 'fresh.sqlite'
    build(root, fresh, config=args['config'])
    assert file_rows(args['output']) == file_rows(fresh)
    with connect(args['output'], readonly=True, scope='all') as db, connect(fresh, readonly=True, scope='all') as expected:
        assert [tuple(r) for r in db.execute('SELECT * FROM raw_periods ORDER BY 1,2,3')] == [
            tuple(r) for r in expected.execute('SELECT * FROM raw_periods ORDER BY 1,2,3')]
    again = maintenance.refresh_metadata(**{**args, 'index': args['output'], 'output': root.parent / 'again.sqlite',
                                             'expected_generation': result['generation']})
    assert again['inputRefresh']['newReceipts'] == 0
    assert again['inputRefresh']['refreshedFileLocations'] == 0
    assert again['inputRefresh']['metadataExtraction'].get('verified_original_reads', 0) == 0
    assert file_rows(root.parent / 'again.sqlite') == file_rows(fresh)


def test_removed_receipt_requires_reconciliation(prepared):
    root, args = prepared
    next(root.glob('acquisitions/sha256/*/*')).unlink()
    with pytest.raises(ValueError, match='disappeared'):
        maintenance.refresh_metadata(**args)
    assert not args['output'].exists()


def test_changed_snapshot_is_not_an_append(prepared):
    root, args = prepared
    snapshot = root / 'snapshot.json'
    snapshot.write_text(snapshot.read_text() + '\n')
    with pytest.raises(ValueError, match='Raw inventory changed'):
        maintenance.refresh_metadata(**args)
    assert not args['output'].exists()


def test_late_unread_receipt_keeps_the_previous_selection(prepared, monkeypatch):
    root, args = prepared
    before = args['index'].read_bytes()
    original = raw_archive.RawImporter.import_receipts
    def interrupted(self, *a, **kw):
        result = original(self, *a, **kw)
        capture(root, b'{"results":[]}')
        return result
    monkeypatch.setattr(raw_archive.RawImporter, 'import_receipts', interrupted)
    with pytest.raises(ValueError, match='receipt inventory changed'):
        maintenance.refresh_metadata(**args)
    assert args['index'].read_bytes() == before
    assert not args['output'].exists()


def test_receipts_added_and_read_during_iteration_are_consistent(prepared, monkeypatch):
    root, args = prepared
    original = archive.iter_acquisition_receipts
    def included(*a, **kw):
        initial = list(original(*a, **kw))
        capture(root, b'{"results":[]}')
        yield from initial
        known = {p for p, _ in initial}
        yield from ((p, r) for p, r in original(*a, **kw) if p not in known)
    monkeypatch.setattr(archive, 'iter_acquisition_receipts', included)
    result = maintenance.refresh_metadata(**args)
    assert result['inputRefresh']['newReceipts'] == 1
