from tests.source_catalog.file_helpers import read_file_page
"""Per-file origin exports preserve capture distinctions and public boundaries."""
import json

import pytest

from spicy_regs.source_catalog.exporter import export, file_provenance
from spicy_regs.source_catalog.storage import connect, schema

SHA = 'sha256:' + 'a' * 64
REF = {'sha256': SHA, 'entry': 3, 'manifest': '/private/manifest.jsonl'}


def test_legacy_hash_match_does_not_claim_original_capture():
    facts = {
        'receipt_status': 'existing',
        'receipt_matches': 'sha256 of identical raw bytes',
        'receipt_source_url': 'https://source.gov/record',
        'receipt_object': 'objects/sha256/aa/' + 'a' * 64,
        'mapping': {'acquired_at': '2026-09-14T22:05:04Z', 'receipt_evidence_ref': REF},
    }
    item = file_provenance(facts, status='held', local=True)
    assert item == {'basis': 'provisional', 'sourceUrls': ['https://source.gov/record'],
                    'availability': 'local', 'receipt': {'sha256': SHA, 'entry': 3},
                    'originalReceiptSha256': SHA}


def test_direct_receipt_uses_only_its_own_capture_facts_and_preserves_locations():
    facts = {
        'receipt_object': 'acquisitions/sha256/aa/' + 'a' * 64,
        'receipt_context': {'requested_url': 'https://source.gov/request', 'resolved_url': 'https://source.gov/result',
                            'original_receipt_sha256': SHA},
        'mapping': {'acquired_at': '2026-09-14T22:05:04Z', 'receipt_evidence_ref': REF},
    }
    item = file_provenance(facts, status='held', local=True)
    assert item['originalReceiptSha256'] == SHA
    assert item['basis'] == 'receipt-backed'
    assert item['capturedAt'] == '2026-09-14T22:05:04Z'
    assert item['sourceUrls'] == ['https://source.gov/request', 'https://source.gov/result']
    facts['mapping']['acquired_at'] = None  # conflicting receipt timestamps stay unknown
    assert 'capturedAt' not in file_provenance(facts, status='held', local=True)


@pytest.mark.parametrize(('status', 'local', 'expected'), [
    ('held', True, 'local'), ('missing-bytes', False, 'missing'),
    ('external-by-choice', False, 'external'), ('known-not-acquired', False, 'not-acquired'),
    ('receipt-only', False, 'unknown'),
])
def test_availability_is_independent_of_attribution(status, local, expected):
    item = file_provenance({}, status=status, local=local)
    assert item == {'basis': 'unknown', 'sourceUrls': [], 'availability': expected}


def test_inferred_origin_never_claims_a_capture_date_and_private_urls_are_withheld():
    item = file_provenance({
        'source_url': 'https://user:password@source.gov/private',
        'requested_url': 'https://source.gov/data?api_key=secret',
        'resolved_url': 'file:///Users/private/file',
        'original_location': 'https://source.gov/public',
        'mapping': {'acquired_at': '2026-01-01T00:00:00Z'},
    }, status='held', local=True)
    assert item['sourceUrls'] == ['https://source.gov/public']
    assert item['basis'] == 'inferred'
    assert 'capturedAt' not in item


def test_export_public_provenance_does_not_load_or_leak_receipt_bodies(tmp_path):
    index = tmp_path / 'index.sqlite'
    facts = {'encoded_bytes': 10, 'receipt_object': 'acquisitions/sha256/aa/' + 'a' * 64,
             'source_url': 'https://source.gov/data',
             'receipt_context': {'source_url': 'https://source.gov/data', 'password': 'never-export'},
             'mapping': {'acquired_at': '2026-09-14T22:05:04Z', 'receipt_evidence_ref': REF},
             'large_private_receipt': {'private_path': '/Users/private/raw', 'token': 'never-export'}}
    with connect(index) as db:
        schema(db)
        db.executemany('INSERT INTO metadata VALUES (?,?)', [('generation', json.dumps(SHA)), ('generatedAt', json.dumps('2026-10-28T00:00:00Z'))])
        db.execute('INSERT INTO collections VALUES (?,?,?)', ('test', 'Test', '{}'))
        db.execute('INSERT INTO objects VALUES (?,?)', (SHA, 10))
        db.execute('INSERT INTO files VALUES (?,?,?,?,?,?,?,?,?)', ('file:test', 'test', None, 'original.json', 'held', SHA, 10, '/private/raw', json.dumps(facts)))
        db.commit()
    root = tmp_path / 'public'
    manifest = export(index, root)
    detail = json.loads((root / manifest['collections'][0]['detailPath']).read_text())
    page = read_file_page(root / detail['files']['pages'][0]['path'])
    body = json.dumps(page)
    item = page['files'][0]
    assert item['provenance']['capturedAt'] == '2026-09-14T22:05:04Z'
    assert item['provenance']['receipt'] == {'sha256': SHA, 'entry': 3}
    assert item['provenance']['basis'] == 'receipt-backed'
    assert item['provenance']['availability'] == 'local'
    assert '/private' not in body and '/Users' not in body and 'never-export' not in body
