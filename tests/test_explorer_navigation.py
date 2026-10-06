"""Complete source keys, explicit refusals and shared SQL/browser recipe ownership."""
import json
import duckdb
import pytest
from spicy_regs.explorer_navigation import declarations, target_keys, array_sql_relationships, validate_navigation, scalar
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


@pytest.mark.parametrize('part', ['07', '007', '7', 7])
def test_meeting_nomination_parts_use_the_publisher_citation_spelling(part):
    reference = {'congress':119, 'number':1272, 'part':part}
    assert target_keys(recipe('meeting_nominations')['targets'][0], reference,
                       {'congress':'118'}) == ['119','PN1272-7']
    assert reference['part'] == part  # Navigation leaves the recorded source untouched.


@pytest.mark.parametrize('part', [None, '', '00', 0])
def test_unpartitioned_treaty_reference_retains_its_own_congress(part):
    reference = {'congress':112, 'number':8}
    if part is not None:
        reference['part'] = part
    assert target_keys(recipe('meeting_treaties')['targets'][0], reference,
                       {'congress':'118'}) == ['112','8','']


@pytest.mark.parametrize('part', [True, False, 0.0, 7.0, {}, [], '-1', ' 07', '7 ', '7x'])
def test_meeting_partition_values_refuse_noninteger_and_malformed_shapes(part):
    for name in ('meeting_nominations', 'meeting_treaties'):
        assert target_keys(recipe(name)['targets'][0],
                           {'congress':119,'number':14,'part':part}, {}) is None


def test_meeting_partition_sql_matches_portable_keys_and_preserves_occurrence_order():
    references = [
        {'congress':119,'number':1272,'part':'07'},
        {'congress':119,'number':1272,'part':'07'},
        {'congress':119,'number':1272,'part':'10'},
        {'congress':112,'number':8},
        *({'congress':119,'number':14,'part':p} for p in
          ('00', '', 0, '0', None, False, True, 0.0, 7.0, {}, [], '-1', ' 07', '7x')),
    ]
    con = duckdb.connect()
    con.execute('CREATE TABLE source(congress VARCHAR, chamber VARCHAR, event_id VARCHAR, refs JSON)')
    con.execute('INSERT INTO source VALUES (?,?,?,?)', ['118','senate','334180',json.dumps(references)])
    for name in ('meeting_nominations', 'meeting_treaties'):
        target = recipe(name)['targets'][0]
        relationship = next(s for s in array_sql_relationships() if s.name == name)
        expected = [(str(i), ':'.join(keys)) for i, ref in enumerate(references)
                    if (keys := target_keys(target, ref, {'congress':'118'})) is not None]
        actual = con.execute(f'SELECT e.key,{relationship.target_expression} FROM source s,json_each(s.refs) e '
                             f'WHERE {relationship.valid_expression} ORDER BY CAST(e.key AS INTEGER)').fetchall()
        assert actual == expected
        expected_prefix = ([('0','119:PN1272-7'),('1','119:PN1272-7'),('2','119:PN1272-10')]
                           if name == 'meeting_nominations' else
                           [('0','119:1272:07'),('1','119:1272:07'),('2','119:1272:10')])
        assert actual[:3] == expected_prefix
    con.close()


@pytest.mark.parametrize('value', [-(2**63), 2**63 - 1, 2**63, 2**64 - 1])
def test_navigation_numeric_integers_cover_the_json_signed_and_unsigned_range(value):
    assert scalar(value) == str(value)


@pytest.mark.parametrize('value', [-(2**63) - 1, 2**64, 2**80])
def test_navigation_refuses_out_of_range_numeric_tokens_but_keeps_digit_strings(value):
    assert scalar(value) is None
    assert scalar(str(value)) == str(value)


@pytest.mark.parametrize('token', ['7.0', '7e0', '0.0', '0e0',
                                    '18446744073709551615', '18446744073709551616',
                                    '"18446744073709551616"', '-9223372036854775808',
                                    '-9223372036854775809'])
def test_raw_json_numeric_tokens_match_python_and_compiled_meeting_sql(token):
    con = duckdb.connect()
    con.execute('CREATE TABLE source(congress VARCHAR, chamber VARCHAR, event_id VARCHAR, refs JSON)')
    for field in ('congress', 'number', 'part'):
        fields = {'congress':'119', 'number':'14', 'part':'7'}
        fields[field] = token
        raw = '[{' + ','.join(json.dumps(k) + ':' + v for k, v in fields.items()) + '}]'
        element = json.loads(raw)[0]
        con.execute('DELETE FROM source')
        con.execute('INSERT INTO source VALUES (?,?,?,?)', ['118','senate','334180',raw])
        for name in ('meeting_nominations', 'meeting_treaties'):
            target = recipe(name)['targets'][0]
            relationship = next(s for s in array_sql_relationships() if s.name == name)
            keys = target_keys(target, element, {'congress':'118'})
            expected = [] if keys is None else [(':'.join(keys),)]
            assert con.execute(f'SELECT {relationship.target_expression} FROM source s,json_each(s.refs) e '
                               f'WHERE {relationship.valid_expression}').fetchall() == expected
    con.close()


def test_nomination_hearing_uses_the_hearing_citation_congress_not_nomination_context():
    t=recipe('nomination_hearings')['targets'][0]
    assert target_keys(t,{'citation':'S.Hrg. 116-38','chamber':'Senate','jacketNumber':42444},{'congress':'119'}) == ['116','senate','42444']
    assert target_keys(t,{'citation':'S.Hrg.119-136','chamber':'Senate','jacketNumber':61323},{'congress':'118'}) == ['119','senate','61323']
    assert target_keys(t,{'chamber':'Senate','jacketNumber':42444},{'congress':'119'}) is None


def test_nomination_hearing_sql_uses_stated_citation_and_jacket_for_both_spellings():
    s=next(s for s in array_sql_relationships() if s.name=='nomination_hearings')
    with duckdb.connect() as c:
        c.execute('CREATE TABLE source(congress VARCHAR, refs JSON)')
        refs=[{'citation':citation,'chamber':'Senate','jacketNumber':61323}
              for citation in ('S.Hrg.119-136','S.Hrg. 119-136','prefix S.Hrg.119-136','S.Hrg.119-136 suffix')]
        c.execute('INSERT INTO source VALUES (?,?)',['118',json.dumps(refs)])
        assert c.execute(f'SELECT {s.target_expression} FROM source s,json_each(s.refs) e WHERE {s.valid_expression}').fetchall()==[('119:senate:61323',),('119:senate:61323',)]


def test_fcc_proceedings_require_name_and_native_id_and_document_urls_are_offers():
    t=recipe('fcc_filing_proceedings')['targets'][0]
    assert target_keys(t,{'name':'26-189','id_proceeding':'1784669453334'}, {}) == ['26-189','1784669453334']
    assert target_keys(t,{'name':'26-189','id_proceeding':None}, {}) is None
    document_recipe=recipe('fcc_filing_documents')
    assert document_recipe['receiptFields'] == ['native_fields_json', 'pdf_extraction_results_json']
    assert document_recipe['elementPath'] == ['documents']
    d=document_recipe['targets'][0]
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


def test_invalid_equality_guard_is_refused_before_publication():
    bad = recipe('vote_amendments')
    bad['targets'][0]['guards'][2]['sameAs']['transform'] = 'guess'
    with pytest.raises(ValueError, match='Unknown navigation transform'):
        validate_navigation([bad])


def test_receipt_only_targets_use_a_published_legacy_table_when_its_key_exists():
    from spicy_regs.explorer_navigation import published_navigation
    spec = recipe('native_legal_read')
    schemas = {'native_legal_references': [('scope_id', 'VARCHAR')],
               'native_legal_reference_reads': [('scope_id', 'VARCHAR')]}
    target = published_navigation([spec], schemas)[0]['targets'][0]
    assert target['table'] == 'native_legal_reference_reads'
    assert target['available'] is True
    del schemas['native_legal_reference_reads']
    assert published_navigation([spec], schemas)[0]['targets'][0]['table'] == '@receipt:native_legal_reference_reads'


def test_fec_source_row_retains_collection_and_source_content_identity():
    target = recipe('fec_source_row_fec_filings')['targets'][0]
    row = {'collection_id':'filings-f13-selected','source_record_id':'1010420180036115817','source_sha256':'sha256:'+'a'*64}
    assert target_keys(target,row,row) == list(row.values())
    assert target_keys(target,row,{**row,'source_sha256':None}) is None
