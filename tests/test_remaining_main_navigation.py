"""Native namespaces, retained body qualification and independent memberships."""
from copy import deepcopy
from datetime import date, datetime

import pyarrow as pa
import pytest

from spicy_regs.explorer_navigation import declarations, part, published_navigation, target_keys, word
from spicy_regs.navigation_measurements import MeasurementCache
from spicy_regs.relationship_views.regulations_native import ARRAYS
from spicy_regs.sources.publication import table_members
from spicy_regs.table_joins import JOINS, RETIRED_PROCESSING_JOINS
from tests.test_navigation_measurements import member


def recipe(name):
    return next(s for s in declarations(RETIRED_PROCESSING_JOINS) if s['id'] == name)


def selected(tmp_path, tables):
    descriptors, local = {}, {}
    for name, rows, schema in tables:
        path, descriptor = member(tmp_path, name, rows, schema)
        descriptors[name + '.parquet'] = descriptor
        local[name] = path
    index = {'format': 'spicy-regs-publication', 'version': 2, 'families': {'fixture': {
        'prefix': 'generations/fixture/' + 'a'*64, 'logicalId': 'urn:fixture',
        'artifactDigest': 'sha256:' + 'a'*64, 'tables': descriptors}}}
    paths = {table_members(index, name + '.parquet')[0].path: str(path) for name, path in local.items()}
    return index, paths


def test_period_routes_reuse_native_memberships_and_return_all_rin_editions(tmp_path):
    native = {name: (source, field) for name, source, _, field, _, _ in ARRAYS}
    for name in ('comment_period_proceedings', 'comment_period_dockets', 'comment_period_rins'):
        s = recipe(name)
        assert (s['source'], s['fields'][0]) == native[name]
    s = recipe('comment_period_rins')
    rows = [{'comment_period_id': 'period1', 'rins': ['1234-AB12', '1234-AB12', None, '5678-CD34']}]
    index, paths = selected(tmp_path, [
        ('comment_periods', rows, pa.schema([('comment_period_id', pa.string()), ('rins', pa.list_(pa.string()))])),
        ('unified_agenda', [{'rin': '1234-AB12', 'agenda_edition': edition} for edition in ('202504', '202510')], None),
    ])
    proof = MeasurementCache(tmp_path/'cache').measure(index, paths, s, source_identity=('comment_period_id',))
    assert (proof['eligible'], proof['matched'], proof['missing'], proof['ambiguous']) == (3, 2, 1, 2)
    assert proof['repeatedReferences'] == 1
    assert proof['unsupportedReferences'] == 1
    assert proof['reverse']['matchedTargetRows'] == 2
    assert proof['reverse']['maximumReferencesPerTarget'] == 2
    assert proof['reverse']['maximumDistinctSourceRecordsPerTarget'] == 1


def test_period_evidence_uses_its_namespace_and_native_date_not_period_dates():
    reg, fr = recipe('comment_period_evidence')['targets']
    source = {'source': 'federal_register.comments_close_on', 'evidence_id': '00-111@2000-01-14',
              'document_number': '00-111', 'publication_date': '2000-01-14',
              'document_id': 'EPA-2026-0001-0001'}
    row = {'open_date': '2000-02-01', 'close_date': '2000-03-01'}
    assert target_keys(fr, source, row) == ['00-111', '2000-01-14']
    assert target_keys(reg, source, row) is None
    assert target_keys(fr, {**source, 'publication_date': '2000-01-18'}, row) is None
    assert target_keys(fr, {**source, 'evidence_id': '00-111'}, row) is None
    gov = {'source': 'documents.comment_end_date', 'document_id': 'EPA-2026-0001-0001',
           'evidence_id': 'EPA-2026-0001-0001'}
    assert target_keys(reg, gov, row) == ['EPA-2026-0001-0001']
    assert target_keys(reg, {**gov, 'evidence_id': 'other'}, row) is None
    assert target_keys(fr, gov, row) is None


def test_lifecycle_uses_dated_by_independent_of_stage_source():
    reg, fr, agenda = recipe('lifecycle_date_evidence')['targets']
    row = {'source': 'regulations_gov', 'dated_by': 'federal_register',
           'document_id': '00-111@2000-01-14', 'event_date': date(2026, 10, 1),
           'evidence_id': 'EPA-2026-0001-0001'}
    assert target_keys(fr, {}, row) == ['00-111', '2000-01-14']
    assert target_keys(reg, {}, row) is None
    assert target_keys(agenda, {}, {**row, 'dated_by': 'unified_agenda', 'document_id': 'agenda:x'}) == ['agenda:x']
    assert all(target_keys(t, {}, {**row, 'dated_by': None}) is None for t in (reg, fr, agenda))
    assert not any(j.child == 'lifecycle_events' and j.parent == 'documents' for j in JOINS)
    assert any(j.child == 'lifecycle_events' and j.parent == 'documents' for j in RETIRED_PROCESSING_JOINS)


@pytest.mark.parametrize('value', ['2024-02-29', '0001-01-01', '9999-12-31'])
def test_canonical_date_and_dated_fr_are_literal(value):
    assert word(part('', transform='canonical-date'), value, {}) == value
    assert word(part('', transform='fr-publication-date'), 'E6-1@' + value, {}) == value
    assert word(part('', transform='fr-document-number'), 'E6-1@' + value, {}) == 'E6-1'


@pytest.mark.parametrize('value', ['1900-02-29', '2023-02-29', '0000-01-01', '20240101',
                                  '2024-1-01', '2024-01-01T00:00:00Z', ' 2024-01-01',
                                  '2024-01-01\n', None, True, 20240101, date(2024, 1, 1), datetime(2024, 1, 1)])
def test_date_transform_refuses_noncanonical_source_shapes(value):
    assert word(part('', transform='canonical-date'), value, {}) is None
    packed = 'x@' + value if isinstance(value, str) else value
    assert word(part('', transform='fr-publication-date'), packed, {}) is None


def captured():
    return {'opinion_body_id': 'court-opinion-body:' + 'c'*64, 'opinion_id': '1', 'cluster_id': '2',
            'source_url': 'https://court.test/opinion.pdf', 'source_sha256': 'sha256:' + 'd'*64,
            'native_sha1': 'e'*40, 'actual_sha1': 'e'*40, 'sha1_matches': True,
            'parent_artifact_digest': 'sha256:' + 'a'*64, 'parent_member_sha256': 'b'*64,
            'parent_member_byte_size': 100}


def test_court_requires_native_parent_tuple_and_capture_witnesses_not_sha_alone():
    target = recipe('court_captured_opinion')['targets'][0]
    row = captured()
    assert target_keys(target, {}, row) == ['1', '2', 'e'*40, 'https://court.test/opinion.pdf']
    for field, bad in [('opinion_id', None), ('cluster_id', None), ('sha1_matches', False),
                       ('sha1_matches', 'true'), ('sha1_matches', 1), ('actual_sha1', 'f'*40),
                       ('source_sha256', None), ('parent_artifact_digest', None),
                       ('parent_member_sha256', None), ('parent_member_byte_size', 0)]:
        assert target_keys(target, {}, {**row, field: bad}) is None
    assert word(part('', transform='native-boolean'), False, {}) == 'false'
    assert word(part('', transform='native-boolean'), True, {}) == 'true'


def test_court_current_tuple_lookup_preserves_missing_and_reverse_outcomes(tmp_path):
    s = recipe('court_captured_opinion')
    source = captured()
    index, paths = selected(tmp_path, [
        ('court_opinion_pdf_extractions', [source, {**source, 'opinion_body_id': 'court-opinion-body:' + 'f'*64,
                                                   'source_url': 'https://court.test/old.pdf'}], None),
        ('court_opinions', [{'opinion_id': '1', 'cluster_id': '2', 'sha1': 'e'*40,
                             'download_url': 'https://court.test/opinion.pdf'},
                            {'opinion_id': '3', 'cluster_id': '2', 'sha1': 'e'*40,
                             'download_url': 'https://court.test/opinion.pdf'}], None),
    ])
    proof = MeasurementCache(tmp_path/'cache').measure(index, paths, s, source_identity=('opinion_body_id',))
    assert (proof['eligible'], proof['matched'], proof['missing'], proof['ambiguous']) == (2, 1, 1, 0)
    assert proof['reverse']['matchedTargetRows'] == 1
    assert proof['reverse']['unmatchedTargetRows'] == 1
    assert proof['reverse']['maximumDistinctSourceRecordsPerTarget'] == 1


def test_old_court_schema_cannot_borrow_parent_or_capture_guards_from_receipts():
    s = recipe('court_captured_opinion')
    s['receiptFields'] = list(captured())
    result = published_navigation([s], {
        'court_opinion_pdf_extractions': [('opinion_id', 'VARCHAR'), ('cluster_id', 'VARCHAR')],
        'court_opinions': [(c, 'VARCHAR') for c in s['targets'][0]['columns']],
    })[0]
    t = result['targets'][0]
    assert not t['sourceAvailable']
    assert not t['directions']['forward']['available']
    assert not t['directions']['reverse']['available']
    assert 'sha1_matches' in t['sourceUnavailableReason']


def test_scorecard_recorded_url_is_not_a_capture_target_and_unresolved_has_no_edge():
    s = recipe('scorecard_member_links_recorded_source')
    row = {'capture_id': 'opaque:capture', 'source_path': '/members/0', 'source_url': 'https://publisher.test/source'}
    assert s['targets'][0]['table'] == '@url'
    assert target_keys(s['targets'][0], {}, row) == ['https://publisher.test/source']
    assert target_keys(s['targets'][0], {}, {**row, 'capture_id': None}) is None
    resolved = recipe('resolved_scorecard_member_links_members')['targets'][0]
    assert target_keys(resolved, {}, {'bioguide_id': 'A000001', 'resolution_status': 'unresolved'}) is None
    assert target_keys(resolved, {}, {'bioguide_id': 'A000001', 'resolution_status': 'resolved'}) == ['A000001']
    snapshot = recipe('recorded_snapshot_scorecard_member_ratings_scorecard_snapshots')['targets'][0]
    assert snapshot['columns'] == ['snapshot_id', 'scorecard_id']
    assert target_keys(snapshot, {}, {'snapshot_id': 'snapshot:1', 'scorecard_id': 'edition:1'}) == ['snapshot:1', 'edition:1']
    assert target_keys(snapshot, {}, {'snapshot_id': 'snapshot:1'}) is None
    assert not any(j.child == 'scorecard_member_ratings' and j.parent == 'scorecard_snapshots' for j in JOINS)
    assert not any(t['table'].endswith('captures') for spec in declarations(RETIRED_PROCESSING_JOINS) for t in spec['targets'])


def test_snapshot_connection_requires_published_main_fields_and_matching_edition(tmp_path):
    s = recipe('recorded_snapshot_scorecard_member_ratings_scorecard_snapshots')
    old = published_navigation([s], {
        'scorecard_member_ratings': [('scorecard_id', 'VARCHAR')],
    })[0]['targets'][0]
    assert not old['sourceAvailable'] and not old['available']
    index, paths = selected(tmp_path, [
        ('scorecard_member_ratings', [{'scorecard_id': 's1', 'snapshot_id': 'snapshot:1'},
                                     {'scorecard_id': 's2', 'snapshot_id': 'snapshot:1'}], None),
        ('scorecard_snapshots', [{'scorecard_id': 's1', 'snapshot_id': 'snapshot:1'}], None),
    ])
    result = MeasurementCache(tmp_path/'cache').measure(index, paths, s)
    assert (result['eligible'], result['matched'], result['missing'], result['ambiguous']) == (2, 1, 1, 0)
    assert result['reverse']['matchedTargetRows'] == 1


def test_standalone_sources_do_not_invent_gao_report_or_senate_person_identity():
    assert not any(j.child == 'gao_decisions' and j.parent == 'gao_reports' for j in JOINS)
    assert not any(j.child.startswith('senate_expenditure') and j.parent in ('members', 'scorecard_members') for j in JOINS)
    assert not any(s['source'] in ('gao_decisions', 'senate_expenditures') for s in declarations(RETIRED_PROCESSING_JOINS))


def test_new_recipe_availability_does_not_claim_measurement_or_unique_actions():
    s = recipe('comment_period_proceedings')
    result = published_navigation([deepcopy(s)], {'comment_periods': [('proceeding_ids', 'VARCHAR[]')],
                                  'proceedings': [('proceeding_id', 'VARCHAR')]}, {'proceedings': ['proceeding_id']})[0]
    assert result['targets'][0]['completeKey']
    for direction in result['targets'][0]['directions'].values():
        assert direction['available']
        assert direction['measurement']['status'] == 'unknown'
        assert direction['lookup']['status'] == 'scan'
