"""Exact XML identity and independent published dates stay separate from acquisition facts."""
import copy
import hashlib
import json

import pytest
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from spicy_regs.source_catalog import raw_archive, raw_content
from spicy_regs.source_catalog.exporter import export
from spicy_regs.source_catalog.file_inventory import validate
from spicy_regs.source_catalog.importer import build
from spicy_regs.source_catalog.maintenance import refresh_metadata
from spicy_regs.source_catalog.storage import connect
from tests.source_catalog.test_raw_archive import base_archive

BODY = (b'<PRESDOCU><PRORDER><DATE>July 8, 2025.</DATE><FRDOC>[FR Doc. 2025-13123 </FRDOC>'
        b'<FILED>Filed 7-10-25; 11:15 am]</FILED></PRORDER></PRESDOCU>')
SHA = 'sha256:' + hashlib.sha256(BODY).hexdigest()
KEY = 'generations/federal-register/' + 'a' * 64 + '/federal_register.parquet'


@pytest.fixture
def context(tmp_path, monkeypatch):
    projection = {'format': 'spicygov-publication-date-context-projection', 'version': 1,
                  'source': {'url': 'https://data.spicygov.ai/' + KEY, 'key': KEY, 'bytes': 1000, 'etag': 'test-etag'},
                  'columns': ['document_number', 'publication_date'],
                  'rows': [{'document_number': '2025-13123', 'publication_date': '2025-07-11'}]}
    projection_path = tmp_path / 'projection.json'
    projection_bytes = json.dumps(projection).encode()
    projection_path.write_bytes(projection_bytes)
    monkeypatch.setattr(raw_content, '_REGISTER_PROJECTION', projection_path)
    data = {'format': 'spicygov-publication-date-context', 'version': 1,
            'context': {'projectionSha256': 'sha256:' + hashlib.sha256(projection_bytes).hexdigest(),
                        'projectionBytes': len(projection_bytes),
                        'objectBytes': 1000, 'etag': 'test-etag', 'key': KEY,
                        'url': 'https://data.spicygov.ai/' + KEY},
            'scope': 'Independent published context; no acquisition URL or completeness claim.',
            'matches': [{'originalSha256': SHA, 'bytes': len(BODY), 'documentNumber': '2025-13123',
                         'publicationDate': '2025-07-11', 'matchingPublishedRows': 1,
                         'acquisitionUrlProven': False}]}
    path = tmp_path / 'date-context.json'
    monkeypatch.setattr(raw_content, '_REGISTER_CONTEXT', path)

    def save():
        path.write_text(json.dumps(data))
    save()
    return data, path, save


def test_independent_date_is_neither_signing_nor_filing_and_does_not_create_an_acquisition_url(context):
    result = raw_content.extract_metadata('family:federal-register', BODY)
    assert result['outcome'] == 'ok' and result['native_id'] == '2025-13123'
    assert [(p['label'], p['origin']) for p in result['periods']] == [('2025-07-11', 'publication-context')]
    assert result['evidence'][0]['locator'] == '/PRESDOCU//FRDOC'
    evidence = result['evidence'][1]
    assert evidence['context'] == context[0]['context']
    assert evidence['origin'] == 'publication-context' and evidence['acquisition_url_proven'] is False
    assert not any(key in result for key in ('source_url', 'requested_url', 'final_url', 'acquired_at'))
    assert raw_content.extract_metadata('family:bill-family', BODY)['outcome'] == 'unsupported'


@pytest.mark.parametrize('fault', ['duplicate', 'multiple-rows', 'conflicting-date', 'wrong-id',
                                  'size', 'date', 'pin', 'url', 'acquisition-claim', 'unobserved-date'])
def test_ambiguous_conflicting_or_invalid_context_never_supplies_a_date(context, fault):
    data, _, save = context
    match = data['matches'][0]
    if fault == 'duplicate':
        data['matches'].append(copy.deepcopy(match))
    elif fault == 'multiple-rows':
        match['matchingPublishedRows'] = 2
    elif fault == 'conflicting-date':
        data['matches'].append(dict(match, originalSha256='sha256:' + 'f' * 64, publicationDate='2025-07-12'))
    elif fault == 'wrong-id':
        match['documentNumber'] = '2025-99999'
    elif fault == 'size':
        match['bytes'] += 1
    elif fault == 'date':
        match['publicationDate'] = '2025-07-32'
    elif fault == 'unobserved-date':
        match['publicationDate'] = '2000-01-01'  # The XML validator alone accepts this date.
    elif fault == 'pin':
        data['context']['projectionSha256'] = None
    elif fault == 'url':
        data['context']['url'] = 'https://example.com/unrelated'
    else:
        match['acquisitionUrlProven'] = True
    save()
    result = raw_content.extract_metadata('family:federal-register', BODY)
    assert result['outcome'] == 'error' and result['periods'] == [] and result['native_id'] is None


def test_missing_or_byte_different_original_remains_unknown(context):
    data, _, save = context
    result = raw_content.extract_metadata('family:federal-register', BODY.replace(b'July 8', b'July 9'))
    assert result['outcome'] == 'unsupported' and result['periods'] == []
    data['matches'].clear()
    save()
    assert raw_content.extract_metadata('family:federal-register', BODY)['outcome'] == 'unsupported'


@pytest.mark.parametrize('change', ['repeated-row', 'conflicting-date', 'changed-projection'])
def test_actual_pinned_projection_must_supply_one_matching_row(context, change):
    data, _, save = context
    path = raw_content._REGISTER_PROJECTION
    projection = json.loads(path.read_text())
    projection['rows'].append(dict(projection['rows'][0], publication_date=(
        '2025-07-12' if change == 'conflicting-date' else '2025-07-11')))
    body = json.dumps(projection).encode()
    path.write_bytes(body)
    if change != 'changed-projection':
        data['context'].update(projectionSha256='sha256:' + hashlib.sha256(body).hexdigest(), projectionBytes=len(body))
        save()
    result = raw_content.extract_metadata('family:federal-register', BODY)
    assert result['outcome'] == 'error' and result['periods'] == []


def test_reader_identity_pins_context_and_maintained_xml_validator(context):
    from pathlib import Path
    from spicy_docs.sources.federal_register import body_sources, body_xml
    from spicy_docs.transport import source_acquirer
    dependencies = set(raw_content.reader_inputs('family:federal-register'))
    assert {context[1], raw_content._REGISTER_PROJECTION,
            *(Path(m.__file__) for m in (body_xml, body_sources, source_acquirer))} <= dependencies


def test_merge_preserves_context_origin_but_cannot_forge_receipt_origin(context):
    content = raw_content.extract_metadata('family:federal-register', BODY)
    merged = raw_archive.merge_content({'native_id': 'retained-native-id', 'source_url': None,
                                       'receipt_status': 'provisional', 'periods': []}, content, SHA)
    assert merged['native_id'] == 'retained-native-id' and merged['source_url'] is None
    assert merged['receipt_status'] == 'provisional'
    assert merged['periods'][0]['origin'] == merged['evidence'][1]['origin'] == 'publication-context'
    forged = copy.deepcopy(content)
    forged['evidence'][0]['origin'] = forged['periods'][0]['origin'] = 'receipt'
    merged = raw_archive.merge_content({'periods': []}, forged, SHA)
    assert merged['periods'][0]['origin'] == merged['evidence'][0]['origin'] == 'content'


def test_selective_refresh_preserves_original_and_receipt_then_exports_publication_period(tmp_path, context, monkeypatch):
    data, _, save = context
    root = tmp_path / 'raw'
    base_archive(root)
    store = CentralRawArchiveStore(root)
    store.put_blob(SHA, len(BODY), [BODY])
    store.record_receipt(files=[{'sha256': SHA, 'bytes': len(BODY), 'dataset_id': 'family:federal-register',
                                'original_location': '/retained/2025-13123.xml', 'receipt_status': 'provisional'}],
                         receipt={'status': 'retained', 'acquired_at': None})
    originals = {str(path.relative_to(root)): path.read_bytes() for path in root.rglob('*') if path.is_file()}
    matches = data['matches']
    data['matches'] = []
    save()
    config = tmp_path / 'registry.json'
    config.write_text('{}')
    base = tmp_path / 'base.sqlite'
    initial = build(root, base, config=config)
    with connect(base, readonly=True, scope='all') as db:
        before = json.loads(db.execute('SELECT data FROM files WHERE sha256=?', (SHA,)).fetchone()[0])
        assert not before['mapping']['periods'] and before['mapping']['native_id'] is None
    data['matches'] = matches
    save()
    listing = tmp_path / 'objects.txt'
    listing.write_text(SHA + '\n')
    opened = []
    read_original = raw_archive.read_original

    def observe(root, sha, obj):
        opened.append(sha)
        return read_original(root, sha, obj)

    monkeypatch.setattr(raw_archive, 'read_original', observe)
    candidate = tmp_path / 'candidate.sqlite'
    report = refresh_metadata(base, candidate, config=config, expected_generation=initial['generation'],
                              include_acquisitions=True, refresh_content=('family:federal-register',),
                              refresh_object_list=listing)
    assert opened == [SHA]
    assert report['inputRefresh']['partialContentReaders']['family:federal-register']['objects'] == 1
    with connect(candidate, readonly=True, scope='all') as db:
        after = json.loads(db.execute('SELECT data FROM files WHERE sha256=?', (SHA,)).fetchone()[0])
        assert [(r['kind'], r['label']) for r in db.execute('SELECT kind,label FROM raw_periods '
            'JOIN files ON files.id=raw_periods.file_id WHERE files.sha256=?', (SHA,))] == [('publication-date', '2025-07-11')]
    assert {k: v for k, v in after.items() if k != 'mapping'} == {k: v for k, v in before.items() if k != 'mapping'}
    assert after['receipt_status'] == 'provisional' and after['mapping']['acquired_at'] is None
    assert after['mapping']['periods'][0]['origin'] == 'publication-context'
    assert {str(path.relative_to(root)): path.read_bytes() for path in root.rglob('*') if path.is_file()} == originals
    output = tmp_path / 'public'
    manifest = export(candidate, output)
    collection = next(row for row in manifest['collections'] if row['id'] == 'family:federal-register')
    detail = json.loads((output / collection['detailPath']).read_text())
    page = detail['files']['pages'][0]
    rows = validate((output / page['path']).read_bytes(), generation=manifest['generation'],
                    dataset_id='family:federal-register', inventory_kind='active')
    assert rows[0]['period_label'] == '2025-07-11'
    assert rows[0]['period_basis'] == 'Observed in pinned publication context'
    assert rows[0]['captured_at'] is None and rows[0]['source_url'] is None
