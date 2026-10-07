"""Admit one maintained ordinary view variant using schema binding, never rows."""
import json
from pathlib import Path

import duckdb
import pytest

from spicy_regs.relationship_views import install_relationship_views
from spicy_regs.relationship_views.fec_document_query import FEC_DOCUMENT_QUERY_VIEWS
from spicy_regs.relationship_views.sql_views import SQLView, install_sql_variants


@pytest.fixture
def connection():
    with duckdb.connect() as con:
        yield con


def test_legacy_lobbying_views_are_not_overwritten_by_incompatible_native_views(connection):
    connection.execute('CREATE TABLE lobbying_activities(filing_uuid VARCHAR, activity_index INTEGER, government_entities_json VARCHAR)')
    values = [{'id':'7','name':'Agency'}, {'id':'7','name':'Agency'}, None, 17]
    connection.execute('INSERT INTO lobbying_activities VALUES (?,?,?), (?,?,?), (?,?,?)',
                       ['f',2,json.dumps(values),'n',3,None,'b',4,'broken'])
    result = install_relationship_views(connection, ['lobbying_activities'])
    for name in ('lobbying_contacted_entities_occurrences','lobbying_contacted_entities_pairs'):
        assert result[name]['status'] == 'available'
        assert result[name]['metadata']['rule_version'] == 'held-array-v1'
    assert connection.execute('SELECT source_ordinal,target_key,parsing_status FROM lobbying_contacted_entities_occurrences ORDER BY source_ordinal').fetchall() == [
        (0,'7','valid'),(1,'7','valid'),(2,None,'null_element'),(3,None,'unsupported_element')]
    assert connection.execute('SELECT count(*) FROM lobbying_contacted_entities_pairs').fetchone() == (1,)
    assert connection.execute('SELECT field_state FROM lobbying_contacted_entities_field_states WHERE filing_uuid IN (\'n\',\'b\') ORDER BY filing_uuid').fetchall() == [('malformed_json',),('sql_null',)]


def test_native_lobbying_views_keep_source_identity_order_and_repeats(connection):
    connection.execute('CREATE TABLE lobbying_activities(filing_uuid VARCHAR, activity_index INTEGER, government_entities STRUCT(id VARCHAR, name VARCHAR)[])')
    connection.execute('INSERT INTO lobbying_activities VALUES (?,?,?)',
                       ['f',2,[{'id':'7','name':'Agency'},{'id':'7','name':'Agency'},None]])
    result = install_relationship_views(connection, ['lobbying_activities'])
    assert result['lobbying_contacted_entities_occurrences']['status'] == 'available'
    assert result['lobbying_contacted_entities_pairs']['status'] == 'available'
    assert connection.execute('SELECT filing_uuid,activity_index,source_ordinal,target_key FROM lobbying_contacted_entities_occurrences ORDER BY source_ordinal').fetchall() == [('f',2,0,'7'),('f',2,1,'7'),('f',2,2,None)]
    assert connection.execute('SELECT count(*) FROM lobbying_contacted_entities_pairs').fetchone() == (1,)


def test_legacy_fec_legal_variants_preserve_invalid_occurrences_and_direction(connection):
    spec = next(s for s in FEC_DOCUMENT_QUERY_VIEWS if s.name == 'fec_legal_citations')
    columns = spec.required['fec_legal_matters']
    connection.execute('CREATE TABLE fec_legal_matters(' + ','.join(c + ' VARCHAR' for c in columns) + ')')
    facts = {'ao_citations':[{'no':'2025-01'},None], 'aos_cited_by':[{'no':'2025-02'}],
             'subject':[{'text':'Repeated'},{'text':'Repeated'},17]}
    connection.execute('INSERT INTO fec_legal_matters(record_id,matter_id,native_facts_json) VALUES (?,?,?)', ['r','m',json.dumps(facts)])
    results = install_relationship_views(connection, ['fec_legal_matters'])
    for name in ('fec_legal_citations','fec_legal_subjects'):
        assert results[name]['status'] == 'available'
        assert results[name]['metadata']['schema_variant'] == 'fec-query-shape-v1'
    assert connection.execute('SELECT citation_kind,source_ordinal,advisory_opinion_number,parsing_status FROM fec_legal_citations ORDER BY citation_kind,source_ordinal').fetchall() == [
        ('ao_citations',0,'2025-01','reported'),('ao_citations',1,None,'json_null'),('aos_cited_by',0,'2025-02','reported')]
    assert connection.execute('SELECT subject,parsing_status FROM fec_legal_subjects ORDER BY source_pointer').fetchall() == [('Repeated','reported'),('Repeated','reported'),(None,'unsupported_shape')]


def native_legal(connection):
    metadata = json.loads((Path(__file__).parents[1] / 'src/spicy_regs/table_metadata.json').read_text())
    names = {'record_id','matter_id','ao_citations','aos_cited_by','regulatory_citations',
             'statutory_citations','citations','subject','subjects'}
    columns = [c for c in metadata['fec_legal_matters']['columns'] if c['column_name'] in names]
    connection.execute('CREATE TABLE fec_legal_matters(' + ','.join('"' + c['column_name'] + '" ' + c['column_type'] for c in columns) + ')')


def test_native_fec_legal_variants_keep_direction_and_subject_paths(connection):
    native_legal(connection)
    connection.execute('INSERT INTO fec_legal_matters(record_id,matter_id,ao_citations,aos_cited_by,subject,subjects) VALUES (?,?,?,?,?,?)',
                       ['r','m',[{'no':'2025-01','name':'A'},{'no':'2025-01','name':'A'},None],
                        [{'no':'2025-02','name':'B'}],
                        [{'path':[0], 'node':{'text':'Repeated','children':[1]}},
                         {'path':[0,0], 'node':{'text':'Repeated','children':[]}}],
                        [{'subject':'Flat','primary_subject_id':'2','secondary_subject_id':'3'}]])
    results = install_relationship_views(connection, ['fec_legal_matters'])
    for name in ('fec_legal_citations','fec_legal_subjects'):
        assert results[name]['status'] == 'available'
        assert results[name]['metadata']['schema_variant'] == 'fec-native-document-query/1'
    assert connection.execute('SELECT citation_kind,source_ordinal,advisory_opinion_number FROM fec_legal_citations ORDER BY citation_kind,source_ordinal').fetchall() == [
        ('ao_citations',0,'2025-01'),('ao_citations',1,'2025-01'),('ao_citations',2,None),('aos_cited_by',0,'2025-02')]
    assert connection.execute("SELECT ordinal_path,subject FROM fec_legal_subjects WHERE subject_kind='subject' ORDER BY ordinal_path").fetchall() == [([0],'Repeated'),([0,0],'Repeated')]


def test_nested_native_schema_mismatch_reports_unsupported_instead_of_crashing(connection):
    native_legal(connection)
    connection.execute('ALTER TABLE fec_legal_matters DROP COLUMN citations')
    connection.execute('ALTER TABLE fec_legal_matters ADD COLUMN citations STRUCT(wrong VARCHAR)')
    results = install_relationship_views(connection, ['fec_legal_matters'])
    assert results['fec_legal_citations']['status'] == 'unsupported'
    assert 'incompatible source types' in results['fec_legal_citations']['reason']
    assert results['fec_legal_subjects']['status'] == 'available'
    connection.execute('CREATE TABLE lobbying_activities(filing_uuid VARCHAR,activity_index INTEGER,government_entities INTEGER[])')
    results = install_relationship_views(connection, ['lobbying_activities'])
    assert results['lobbying_contacted_entities_occurrences']['status'] == 'unsupported'
    assert results['lobbying_contacted_entities_pairs']['status'] == 'unsupported'


def test_compatible_variant_probe_does_not_read_rows(connection):
    connection.execute("CREATE VIEW held AS SELECT error('must not evaluate')::INTEGER[] AS items")
    spec = SQLView('expanded', {'held':('items',)}, lambda p:'SELECT unnest(items) AS item FROM held', 'Held elements.', ('item',))
    assert install_sql_variants(connection, ['held'], [(spec,)])['expanded']['status'] == 'available'


def test_compatible_legacy_variant_wins_when_native_shape_cannot_bind(connection):
    connection.execute('CREATE TABLE held(typed INTEGER[],legacy VARCHAR)')
    native = SQLView('selected', {'held':('typed',)}, lambda p:'SELECT v.id FROM held, UNNEST(typed) e(v)', 'Native.', ('id',), 'native/1')
    legacy = SQLView('selected', {'held':('legacy',)}, lambda p:'SELECT legacy FROM held', 'Legacy.', ('legacy',), 'legacy/1')
    result = install_sql_variants(connection, ['held'], [(native,legacy)])
    assert result['selected']['status'] == 'available'
    assert result['selected']['metadata']['schema_variant'] == 'legacy/1'
    assert connection.execute('DESCRIBE selected').fetchall()[0][0] == 'legacy'
