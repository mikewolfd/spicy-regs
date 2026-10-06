"""Complete source keys, explicit refusals and shared SQL/browser recipe ownership."""
import json
import duckdb
import pytest
from spicy_regs.explorer_navigation import declarations, target_keys, array_sql_relationships, validate_navigation
from spicy_regs.table_joins import RETIRED_PROCESSING_JOINS


def recipe(name):
    return next(s for s in declarations(RETIRED_PROCESSING_JOINS) if s['id'] == name)


def test_partitioned_meeting_keys_are_indivisible_and_missing_part_is_unknown():
    n = recipe('meeting_nominations')['targets'][0]
    assert target_keys(n, {'congress':119,'number':14,'part':'2'}, {}) == ['119','PN14-2']
    assert target_keys(n, {'congress':119,'number':14,'part':'00'}, {}) == ['119','PN14']
    assert target_keys(n, {'congress':119,'number':14}, {}) is None
    t = recipe('meeting_treaties')['targets'][0]
    assert target_keys(t, {'congress':118,'number':2,'part':'00'}, {}) == ['118','2','']
    assert target_keys(t, {'congress':118,'number':2,'part':'3'}, {}) == ['118','2','3']


def test_nomination_hearing_uses_the_hearing_citation_congress_not_nomination_context():
    t=recipe('nomination_hearings')['targets'][0]
    assert target_keys(t,{'citation':'S.Hrg. 116-38','chamber':'Senate','jacketNumber':42444},{'congress':'119'}) == ['116','senate','42444']
    assert target_keys(t,{'chamber':'Senate','jacketNumber':42444},{'congress':'119'}) is None


def test_fcc_proceedings_require_name_and_native_id_and_document_urls_are_offers():
    t=recipe('fcc_filing_proceedings')['targets'][0]
    assert target_keys(t,{'name':'26-189','id_proceeding':'1784669453334'}, {}) == ['26-189','1784669453334']
    assert target_keys(t,{'name':'26-189','id_proceeding':None}, {}) is None
    d=recipe('fcc_filing_documents')['targets'][0]
    assert target_keys(d,{'src':'javascript:alert(1)'},{}) is None
    assert target_keys(d,{'src':'https://example.test/a.pdf'},{}) == ['https://example.test/a.pdf']


def test_vote_documents_and_amendments_do_not_borrow_unrelated_context():
    vote=recipe('vote_documents')
    assert target_keys(vote['targets'][1],{'type':'PN','congress':'118','number':'14-2'},{'congress':'119'}) == ['118','PN14-2']
    assert target_keys(vote['targets'][2],{'type':'Treaty Doc.','number':'118-2'},{'congress':'119'}) == ['118-2']
    t=recipe('vote_amendments')['targets'][0]
    assert target_keys(t,{'number':'S.Amdt. 14'},{'congress':'119','chamber':'senate','vote_id':'119-senate-1-1'}) == ['119-samdt-14']
    assert target_keys(t,{'number':'S.Amdt. 14'},{'congress':'118','chamber':'senate','vote_id':'119-senate-1-1'}) is None
    assert target_keys(t,{'number':'S.Amdt. 14'},{'congress':'119','chamber':'house','vote_id':'119-house-1-1'}) is None


def test_legal_navigation_reuses_resolver_candidate_identity_and_keeps_ambiguity():
    targets=recipe('native_legal_targets')['targets']
    t=next(t for t in targets if t['table']=='cfr_sections')
    element={'target_table_selected':'cfr_sections','target_status':'ambiguous','package_id':'CFR-2025-title2','granule_id':'g1'}
    assert target_keys(t,element,{}) == ['CFR-2025-title2','g1']
    assert target_keys(t,{**element,'target_status':'not_checked'}, {}) is None
    assert target_keys(t,{**element,'granule_id':None}, {}) is None


def test_new_mcp_sql_key_rules_compile_from_the_published_navigation_definition():
    s=next(s for s in array_sql_relationships() if s.name=='meeting_nominations')
    c=duckdb.connect()
    c.execute('CREATE TABLE source(congress VARCHAR, chamber VARCHAR, event_id VARCHAR, refs JSON)')
    c.execute('INSERT INTO source VALUES (?,?,?,?)',['119','senate','1',json.dumps([{'congress':118,'number':14,'part':'2'}])])
    assert c.execute(f'SELECT {s.target_expression} FROM source s,json_each(s.refs) e WHERE {s.valid_expression}').fetchall()==[('118:PN14-2',)]


def test_generation_rejects_incomplete_composite_keys():
    bad=recipe('meeting_nominations')
    bad['targets'][0]['keys'].pop()
    with pytest.raises(ValueError,match='every component'):
        validate_navigation([bad])
