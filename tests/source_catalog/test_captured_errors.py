"""Exact error reviews refine usable data without changing retained bytes or transport."""

import gzip
import hashlib
import json

from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.raw_archive import classify_retained_objects, retained_classification_candidates
from spicy_regs.source_catalog.storage import connect

# Full retained original, not recompressed JSON: the SHA is the review identity.
SAM_SHA = 'sha256:39c09337378d41a18ad7a85f62a97d23751911bfc75f7395c97681f2aff3c0e2'
SAM_BODY = bytes.fromhex(
    '1f8b08000000000000ff6c8d3d0a02311085af32a416d9c2ca565cdb404e306487ece0902c931194c5bb3bb1b2b07cef7b3f7b'
    '58cdb664688f1ecee1344de1108c4dc8d5f5698ad9606621b8512545e356813b246311e00a515b51eadd6b0b19b278ef9b7790'
    'dde75a7e63478842d809f24af90e58d0a1a091c2387e6de3f7df38a936bdb465f039c5f0fe000000ffff0300048aa851ba000000'
)


def test_rebuild_separates_reviewed_gzip_error_valid_empty_and_unknown(tmp_path):
    root = tmp_path / 'raw'
    store = CentralRawArchiveStore(root)
    samples = {
        'sam-error': SAM_BODY,
        'valid-bill': b'{"bill":{"congress":119,"type":"HR","number":2}}',
        'empty': b'',
        'unknown': b'{"httpStatus":"400","title":"a data record, not an error"}',
    }
    files = []
    for name, body in samples.items():
        sha = hashlib.sha256(body).hexdigest()
        store.put_blob(sha, len(body), [body])
        files.append({'sha256': sha, 'bytes': len(body), 'name': name,
                      'original_location': '/historical/' + name + '.bin',
                      'receipt_context': {'family': 'sam-entities'} if name == 'sam-error' else {}})
        if name == 'sam-error':
            # Exact-byte aliases may have different or unknown transport facts.
            files.append(dict(files[-1], name='sam-alias', http_status=200))
    store.record_receipt(files=files, receipt={'status': 'captured'})
    before = {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}
    index = tmp_path / 'index.sqlite'
    build(root, index)
    with connect(index, scope='all') as db:
        rows = {r['name']: r for r in db.execute('SELECT * FROM main.files')}
        assert hashlib.sha256(SAM_BODY).hexdigest() == SAM_SHA[7:]
        assert json.loads(gzip.decompress(SAM_BODY))['errorCode'] == 'FSP'
        assert len(SAM_BODY) == 154
        for name in ('sam-error', 'sam-alias'):
            row = rows[name]
            assert row['status'] == 'refused-source'
            assert row['sha256'] == SAM_SHA and row['bytes'] == 154
            metadata = json.loads(row['data'])['mapping']['content_metadata']
            assert metadata['classification'] == 'source-error'
            assert metadata['role'] == 'failed-response'
            assert metadata['content_kind'] == 'captured-error-response'
            assert metadata['raw_sha256'] == SAM_SHA
            assert metadata['scope']['review_sha256'].startswith('sha256:')
            mapping = json.loads(row['data'])['mapping']
            assert mapping['http_status'] == (200 if name == 'sam-alias' else None)
            assert mapping['source_validation'] is None
            assert not mapping['periods']
        assert rows['valid-bill']['status'] == 'held'
        valid = json.loads(rows['valid-bill']['data'])['mapping']['content_metadata']
        assert valid['outcome'] == 'ok' and valid['primary_records'] == 1
        assert rows['empty']['status'] == 'unclassified-source'
        empty = json.loads(rows['empty']['data'])['mapping']['content_metadata']
        assert empty['classification'] == 'uncertain' and empty['outcome'] == 'uncertain'
        assert rows['unknown']['status'] == 'held'  # custody, not a usable-content claim
        unknown = json.loads(rows['unknown']['data'])['mapping']['content_metadata']
        assert unknown['outcome'] == 'unsupported'
        assert SAM_SHA in set(retained_classification_candidates(db))
        assert classify_retained_objects(db, root, [SAM_SHA])['reviewed_objects'] == 1
    assert before == {str(p.relative_to(root)): p.read_bytes() for p in root.rglob('*') if p.is_file()}


def test_rebuild_distinguishes_missing_base_original_from_unchecked_content(tmp_path):
    root = tmp_path / 'raw'
    root.mkdir()
    sha = hashlib.sha256(b'missing original').hexdigest()
    object_name = f'objects/sha256/{sha[:2]}/{sha}'
    obj = {'sha256': sha, 'bytes': 16, 'object': object_name, 'encoding': 'raw',
           'stored_sha256': sha, 'stored_bytes': 16}
    raw = {'sha256': sha, 'bytes': 16, 'object': object_name,
           'original_location': '/historical/missing.bin', 'receipt_status': 'provisional'}
    pins = {}
    for name, rows in {'objects': [obj], 'files': [raw], 'receipts': [],
                       'provisional-receipts': [], 'unavailable-receipts': []}.items():
        name += '.jsonl.gz'
        body = gzip.compress(b''.join((json.dumps(row) + '\n').encode() for row in rows), mtime=0)
        (root / name).write_bytes(body)
        pins[name] = hashlib.sha256(body).hexdigest()
    (root / 'snapshot.json').write_text(json.dumps({'format': 'spicygov-raw-files-1', 'indexes': pins}))
    index = tmp_path / 'index.sqlite'
    build(root, index)
    with connect(index, scope='all') as db:
        row = db.execute('SELECT status,sha256,local_path,data FROM main.files').fetchone()
        assert row['status'] == 'missing-bytes'
        assert row['sha256'] == 'sha256:' + sha
        assert row['local_path'] is None
        assert 'content_metadata' not in json.loads(row['data'])['mapping']
