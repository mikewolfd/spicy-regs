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


def test_partial_replay_opens_only_named_original_and_preserves_family_pin(prepared, monkeypatch):
    root, args = prepared
    with connect(args['index'], readonly=True, scope='all') as db:
        target = db.execute("SELECT sha256 FROM files WHERE collection_id='family:federal-register'").fetchone()[0]
    listing = root.parent / 'affected.txt'
    listing.write_text(target + '\n')
    opened = []
    original = raw_archive.read_original
    def observe(root, sha, obj):
        opened.append(sha)
        return original(root, sha, obj)
    monkeypatch.setattr(raw_archive, 'read_original', observe)
    result = maintenance.refresh_metadata(**args, refresh_object_list=listing)
    assert opened == [target]
    partial = result['inputRefresh']['partialContentReaders']['family:federal-register']
    assert partial['objects'] == 1
    assert 'family:federal-register' not in result['inputRefresh']['contentReaders']
    opened.clear()
    again_args = {**args, 'index': args['output'], 'output': root.parent / 'again.sqlite',
                  'expected_generation': result['generation'], 'refresh_object_list': listing}
    again = maintenance.refresh_metadata(**again_args, reuse_unchanged=True)
    assert again['status'] == 'unchanged' and not again_args['output'].exists()
    assert not opened
    # An unscoped later refresh must still apply the reader to the full family.
    full = maintenance.refresh_metadata(**{**again_args, 'refresh_object_list': None})
    assert full['inputRefresh']['contentReaders']['family:federal-register'] == partial['reader']
    assert not full['inputRefresh']['partialContentReaders']
    assert opened == [target]


@pytest.mark.parametrize('value', ['', 'not-a-hash\n', 'sha256:' + 'f'*64 + '\n'])
def test_partial_replay_refuses_empty_invalid_or_unlocated_list(prepared, value):
    root, args = prepared
    listing = root.parent / 'affected.txt'
    listing.write_text(value)
    with pytest.raises(ValueError, match='Object list|Every replay object'):
        maintenance.refresh_metadata(**args, refresh_object_list=listing)
    assert not args['output'].exists()


def test_partial_replay_remaps_old_metadata_alias_and_exports_literal_period(tmp_path, monkeypatch):
    from spicy_regs.source_catalog.exporter import export
    from spicy_regs.source_catalog.raw_mapping import SourceMapper
    root = tmp_path / 'raw'
    base_archive(root)
    body = b'''<mods xmlns="http://www.loc.gov/mods/v3"><originInfo><dateIssued>1960-12-31</dateIssued>
      </originInfo><extension><collectionCode>FR</collectionCode><accessId>FR-1960-12-31</accessId></extension></mods>'''
    store = archive.CentralRawArchiveStore(root)
    sha = hashlib.sha256(body).hexdigest()
    store.put_blob(sha, len(body), [body])
    store.record_receipt(files=[{'sha256': sha, 'bytes': len(body),
        'dataset_id': 'family:federal-register', 'original_location': '/old/FR-1960-12-31_mods.xml'}],
        receipt={'status': 'captured'})
    config, index = tmp_path / 'registry.json', tmp_path / 'base.sqlite'
    config.write_text('{}')
    current_mapper = SourceMapper.map_source
    def old_mapper(self, row):
        mapped = current_mapper(self, row)
        if mapped.get('native_id') == 'FR-1960-12-31':
            mapped['native_id'] = 'FR-1960-12-31_mods'
        return mapped
    with monkeypatch.context() as patch:
        patch.setattr(SourceMapper, 'map_source', old_mapper)
        patch.setattr(raw_archive, 'extract_metadata', lambda *a: {'outcome': 'unsupported'})
        base = build(root, index, config=config)
    with connect(index, readonly=True, scope='all') as db:
        old = db.execute("SELECT id,data FROM files WHERE sha256=?", ('sha256:'+sha,)).fetchone()
    old_mapping = json.loads(old[1])['mapping']
    assert old_mapping['native_id'] == 'FR-1960-12-31_mods'
    listing = tmp_path / 'affected.txt'
    listing.write_text('sha256:'+sha+'\n')
    candidate = tmp_path / 'candidate.sqlite'
    maintenance.refresh_metadata(index, candidate, config=config, expected_generation=base['generation'],
        include_acquisitions=True, refresh_content=('family:federal-register',), refresh_object_list=listing)
    with connect(candidate, readonly=True, scope='all') as db:
        updated = db.execute('SELECT id,status,data FROM files WHERE sha256=?', ('sha256:'+sha,)).fetchone()
        periods = [tuple(p) for p in db.execute('SELECT kind,label FROM raw_periods WHERE file_id=?', (updated[0],))]
    mapping = json.loads(updated[2])['mapping']
    assert updated[0] == old[0] and updated[1] == 'held'
    assert mapping['native_id'] == 'FR-1960-12-31'
    assert not mapping.get('field_conflicts')
    assert mapping['receipt_evidence_ref'] == old_mapping['receipt_evidence_ref']
    assert ('publication-date','1960-12-31') in periods
    output = tmp_path / 'export'
    export(candidate, output)
    files = list(output.rglob('*.json'))
    assert any('1960-12-31' in path.read_text() and 'publication-date' in path.read_text() for path in files)



def test_partial_replay_requires_one_collection(prepared):
    root, args = prepared
    listing = root.parent / 'affected.txt'
    listing.write_text('sha256:' + 'f'*64 + '\n')
    with pytest.raises(ValueError, match='exactly one named collection'):
        maintenance.refresh_metadata(**{**args, 'refresh_content': ('family:federal-register', 'family:bill-family')},
                                     refresh_object_list=listing)
    assert not args['output'].exists()
