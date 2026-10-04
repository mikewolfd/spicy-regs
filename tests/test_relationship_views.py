"""Array relationships preserve repeats and independently scoped source facts."""
import json

from typing import Any

import duckdb
import pytest

from spicy_regs.relationship_views import RELATIONSHIP_VIEWS, install_relationship_views
from spicy_regs.relationship_views.core import install_arrays


def rows(con, table):
    result = con.execute(f'SELECT * FROM "{table}"')
    columns = [c[0] for c in result.description]
    return [dict(zip(columns, row, strict=True)) for row in result.fetchall()]


def fixture(con, table, columns, values):
    con.execute(f'CREATE TABLE "{table}" (' + ','.join(f'"{c}" VARCHAR' for c in columns) + ')')
    con.executemany(f'INSERT INTO "{table}" VALUES (' + ','.join('?' for _ in columns) + ')', values)


def install(con, names, publication=None):
    specs = [s for s in RELATIONSHIP_VIEWS if s.name in names]
    return install_arrays(con, {s.source_table for s in specs}, specs, publication)


def test_duplicates_nulls_and_malformed_values():
    con: Any = duckdb.connect()
    fixture(con, 'fcc_filings', ['id_submission', 'proceeding_names_json'], [
        ('a', '["26-189","26-189",null,{},3,""]'), ('b', None), ('c', 'null'),
        ('d', '[]'), ('e', '{'), ('f', '{}'),
    ])
    install(con, ['fcc_filing_proceedings'], {'fcc_filings': {'artifact_digest': 'sha256:fixture'}})
    observed = sorted(rows(con, 'fcc_filing_proceedings_occurrences'), key=lambda r: r['source_ordinal'])
    assert [r['source_ordinal'] for r in observed] == list(range(6))
    assert [r['parsing_status'] for r in observed] == [
        'valid', 'valid', 'null_element', 'unsupported_element', 'unsupported_element', 'unsupported_element']
    assert len(rows(con, 'fcc_filing_proceedings_pairs')) == 1
    assert observed[0]['source_pointer'] == '/proceeding_names_json/0'
    assert json.loads(observed[0]['source_publication_json']) == {'artifact_digest': 'sha256:fixture'}
    states = {r['id_submission']: r['field_state'] for r in rows(con, 'fcc_filing_proceedings_field_states')}
    assert states == {'a': 'populated_array', 'b': 'sql_null', 'c': 'json_null', 'd': 'empty_array',
                      'e': 'malformed_json', 'f': 'unsupported_shape'}


def test_related_bill_details_complete_and_pairs_directional():
    con: Any = duckdb.connect()
    details = [{'type': 'Related bill', 'identifiedBy': 'CRS'}, {'type': 'Identical bill', 'identifiedBy': 'House'}]
    value = {'congress': '119', 'bill_type': 'HR', 'number': '1630', 'relationship_details': details}
    fixture(con, 'congress_bills', ['bill_id', 'related_bills_json'], [('119-hr-300', json.dumps([value, value]))])
    install(con, ['bill_related_bills'])
    occurrences = rows(con, 'bill_related_bills_occurrences')
    assert len(occurrences) == 2
    assert all(json.loads(r['relationship_details_json']) == details for r in occurrences)
    pairs = rows(con, 'bill_related_bills_pairs')
    assert [(r['bill_id'], r['target_key']) for r in pairs] == [('119-hr-300', '119-hr-1630')]
    assert pairs[0]['target_status'] == 'not_checked'


def test_vote_documents_independent_of_empty_amendment_blocks():
    con: Any = duckdb.connect()
    docs = [{'congress': 119, 'type': 'PN', 'number': f'55-{n}', 'name': f'PN55-{n}'} for n in range(1, 49)]
    amendments = [{'number': None, 'purpose': 'No Statement of Purpose on File.'} for _ in range(48)]
    fixture(con, 'roll_call_votes', ['vote_id', 'documents_json', 'amendments_json', 'question', 'source_url'],
            [('119-senate-1-522', json.dumps(docs), json.dumps(amendments), 'Motion', 'https://example.test/vote')])
    con.execute("ALTER TABLE roll_call_votes ADD COLUMN congress VARCHAR DEFAULT '119'")
    con.execute("ALTER TABLE roll_call_votes ADD COLUMN chamber VARCHAR DEFAULT 'senate'")
    install(con, ['vote_documents', 'vote_amendments'])
    observed = rows(con, 'vote_documents_occurrences')
    assert len(observed) == len(rows(con, 'vote_amendments_occurrences')) == 48
    assert len(rows(con, 'vote_documents_pairs')) == 48
    assert rows(con, 'vote_amendments_pairs') == []
    assert {r['target_kind'] for r in observed} == {'nomination'}
    assert {r['target_key'] for r in observed} == {f'119:PN55-{n}' for n in range(1, 49)}
    con.execute("UPDATE roll_call_votes SET amendments_json='[]'")
    assert len(rows(con, 'vote_documents_occurrences')) == 48


def test_no_default_congress_or_unqualified_treaty():
    con: Any = duckdb.connect()
    docs = [{'type': 'PN', 'number': '55-25'}, {'congress': 119, 'type': 'Treaty Doc.', 'number': '1'},
            {'congress': 119, 'type': 'S.Res.', 'number': '30'}]
    fixture(con, 'roll_call_votes', ['vote_id', 'documents_json', 'question', 'source_url'],
            [('vote', json.dumps(docs), 'Confirmation', None)])
    install(con, ['vote_documents'])
    observed = sorted(rows(con, 'vote_documents_occurrences'), key=lambda r: r['source_ordinal'])
    assert [r['target_key'] for r in observed] == [None, None, '119-sres-30']
    assert [r['target_status'] for r in observed] == ['unsupported', 'unsupported', 'not_checked']


def test_meeting_full_keys_independent_arrays():
    con: Any = duckdb.connect()
    fixture(con, 'committee_meetings', ['congress', 'chamber', 'event_id', 'meeting_status',
                                      'bill_ids_json', 'hearing_jackets_json'], [
        ('119', 'house', '119565', 'Canceled', '["119-hr-1653","119-hr-1653"]', '["63019","64431"]'),
        ('118', 'house', '119565', 'Scheduled', '["118-hr-1"]', '[]'),
    ])
    install(con, ['meeting_bills', 'meeting_hearing_jackets'])
    assert len(rows(con, 'meeting_bills_occurrences')) == 3
    assert len(rows(con, 'meeting_bills_pairs')) == 2
    jackets = rows(con, 'meeting_hearing_jackets_occurrences')
    assert {r['target_key'] for r in jackets} == {'119:house:63019', '119:house:64431'}
    assert all(r['meeting_status'] == 'Canceled' for r in jackets)


def test_fr_dated_identity_and_rins_do_not_zip():
    con: Any = duckdb.connect()
    fixture(con, 'federal_register', ['document_number', 'publication_date', 'regulation_id_numbers_json', 'docket_ids_json'], [
        ('94-190', '1994-01-01', '["3206-AO36","3206-AO80"]', '["SEC File No. SR-Amex-2003-102"]'),
        ('94-190', '1994-02-01', '["3206-AO36"]', '[]'),
    ])
    install(con, ['federal_register_rins', 'federal_register_dockets'])
    assert len(rows(con, 'federal_register_rins_pairs')) == 3
    dockets = rows(con, 'federal_register_dockets_occurrences')
    assert dockets[0]['target_key'] == 'SEC File No. SR-Amex-2003-102'
    assert dockets[0]['target_kind'] == 'docket_spelling'


def test_missing_dependencies_not_fake_empty_tables():
    con: Any = duckdb.connect()
    fixture(con, 'members', ['bioguide_id'], [('C000127',)])
    result = install_relationship_views(con, ['members'])
    assert result['member_fec_ids_occurrences']['status'] == 'unsupported'
    assert 'fec_ids_json' in result['member_fec_ids_occurrences']['reason']
    assert result['fcc_filing_proceedings_occurrences']['status'] == 'unavailable'
    with pytest.raises(duckdb.CatalogException):
        con.execute('SELECT * FROM member_fec_ids_occurrences')


def test_fec_presidential_id_and_source_context():
    con: Any = duckdb.connect()
    fixture(con, 'members', ['bioguide_id', 'fec_ids_json', 'observed_at', 'roster'], [
        ('C000127', '["S8WA00194","H2WA01054","P80001571",null,"bad"]', '2026-09-22', 'current'),
    ])
    install(con, ['member_fec_ids'])
    assert {r['target_key'] for r in rows(con, 'member_fec_ids_pairs')} == {'S8WA00194', 'H2WA01054', 'P80001571'}


def test_comment_reference_namespaces_and_legacy_states():
    con: Any = duckdb.connect()
    columns = ['comment_id', 'docket_id', 'comment_on_document_id', 'comment_on_object_id',
               'original_document_id', 'comment_reference_values_json']
    values = {'commentOnDocumentId': 'ODNI-2009-0004-0001', 'commentOn': '0900006480a18cfe',
              'originalDocumentId': 'ODNI_FRDOC_0001-0004'}
    fixture(con, 'comments', columns, [
        ('ODNI-2009-0004-0002', None, values['commentOnDocumentId'], values['commentOn'],
         values['originalDocumentId'], json.dumps(values)),
        ('absent', None, None, None, None, '{}'),
        ('null', None, None, None, None, '{"commentOnDocumentId":null}'),
        ('empty', None, None, None, None, '{"commentOnDocumentId":""}'),
        ('legacy', None, None, None, None, None),
        ('malformed', None, None, None, None, '{'),
    ])
    install_relationship_views(con, ['comments'])
    pairs = rows(con, 'comment_document_references_pairs')
    assert [(r['comment_id'], r['target_key']) for r in pairs] == [('ODNI-2009-0004-0002', 'ODNI-2009-0004-0001')]
    observed = rows(con, 'comment_native_references')
    originals = [r for r in observed if r['comment_id'] == 'ODNI-2009-0004-0002']
    assert len(originals) == 3 and all(r['docket_id'] is None for r in originals)
    assert {r['target_status'] for r in originals} == {'not_checked', 'unsupported'}
    states = {r['comment_id']: r['parsing_status'] for r in rows(con, 'comment_reference_field_states')
              if r['source_field'] == 'commentOnDocumentId'}
    assert states == {'ODNI-2009-0004-0002': 'valid', 'absent': 'absent', 'null': 'null',
                      'empty': 'empty_string', 'legacy': 'unread', 'malformed': 'malformed_json'}


def test_comment_reference_views_scan_comments_once():
    """Three reference fields come from one comments scan joined to the field list."""
    con: Any = duckdb.connect()
    fixture(con, 'comments', ['comment_id', 'docket_id', 'comment_on_document_id', 'comment_on_object_id',
                              'original_document_id', 'comment_reference_values_json'], [('c', None, None, None, None, '{}')])
    install_relationship_views(con, ['comments'])
    for view in ('comment_reference_field_states', 'comment_native_references', 'comment_document_references_pairs'):
        plan = con.execute(f'EXPLAIN SELECT count(*) FROM {view}').fetchall()[0][1]
        assert plan.count('SEQ_SCAN') + plan.count('TABLE_SCAN') == 1, plan


def test_installed_views_visible_to_request_cursor():
    con: Any = duckdb.connect()
    fixture(con, 'fcc_filings', ['id_submission', 'proceeding_names_json'], [('a', '["26-189"]')])
    install_relationship_views(con, ['fcc_filings'])
    cursor = con.cursor()
    try:
        assert cursor.execute('SELECT target_key FROM fcc_filing_proceedings_pairs').fetchall() == [('26-189',)]
    finally:
        cursor.close()


def test_all_registry_views_bind_without_reading_rows():
    con: Any = duckdb.connect()
    tables = {}
    for spec in RELATIONSHIP_VIEWS:
        tables.setdefault(spec.source_table, set()).update(spec.required_columns)
    for table, columns in tables.items():
        con.execute(f'CREATE TABLE "{table}" (' + ','.join(f'"{c}" VARCHAR' for c in sorted(columns)) + ')')
    results = install_arrays(con, tables, RELATIONSHIP_VIEWS)
    for spec in RELATIONSHIP_VIEWS:
        for name in spec.names:
            assert results[name]['status'] == 'available'
            assert con.cursor().execute(f'SELECT * FROM "{name}" LIMIT 0').fetchall() == []
