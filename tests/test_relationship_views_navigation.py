"""Target lookups preserve source cardinality and typed complete keys."""

from decimal import Decimal
import json

from typing import Any

import duckdb

from spicy_regs.relationship_views import RELATIONSHIP_VIEWS, SQL_RELATIONSHIP_VIEWS, install_relationship_views, view_columns


def table(con, name, fields, records):
    con.execute(f'CREATE TABLE {name} (' + ','.join(f'{field} VARCHAR' for field in fields) + ')')
    if records:
        con.executemany(f'INSERT INTO {name} VALUES (' + ','.join('?' for _ in fields) + ')', records)


def test_all_navigation_definitions_bind_and_report_actual_schemas():
    con: Any = duckdb.connect()
    schema = {}
    for spec in RELATIONSHIP_VIEWS:
        schema.setdefault(spec.source_table,set()).update(spec.required_columns)
    for spec in SQL_RELATIONSHIP_VIEWS:
        for source, fields in spec.required.items():
            schema.setdefault(source,set()).update(fields)
    for name, fields in schema.items():
        table(con,name,sorted(fields),[])
    result = install_relationship_views(con,schema)
    for spec in SQL_RELATIONSHIP_VIEWS:
        assert result[spec.name]['status'] == 'available', result[spec.name]
        assert view_columns(con.execute(f'DESCRIBE {spec.name}').fetchall())
        assert con.cursor().execute(f'SELECT * FROM {spec.name} LIMIT 0').fetchall() == []


def test_entity_enrichment_cannot_multiply_award_money():
    con: Any = duckdb.connect()
    table(con,'sam_entities',['uei','entity_eft_indicator'],[
        ('CJLMN78UULH4',None),('CJLMN78UULH4','0001'),('bad',None),
    ])
    table(con,'usaspending_recipients',['recipient_id','uei','recipient_level','duns','total_award_amount'],[
        ('recipient-R','CJLMN78UULH4','R','123456789','123.45'),
        ('parent-P','CJLMN78UULH4','P',None,'200.55'),
        ('unknown-R','JE73CDQUAPA7','R',None,'10.00'),
        ('null-R',None,'R',None,'-1.25'),('malformed-R','bad','R',None,'0.01'),
    ])
    install_relationship_views(con,['sam_entities','usaspending_recipients'])
    assert con.execute('SELECT count(*) FROM recipient_sam_entities').fetchone()[0] == 5
    # Reconciliation checks preservation only; P/R totals need not be disjoint.
    before = con.execute('SELECT sum(total_award_amount::DECIMAL(38,2)) FROM usaspending_recipients').fetchone()[0]
    after = con.execute('SELECT sum(total_award_amount::DECIMAL(38,2)) FROM recipient_sam_entities').fetchone()[0]
    assert before == after == Decimal('332.76')
    assert con.execute("SELECT entity_status,registration_count FROM recipient_sam_entities WHERE recipient_id='recipient-R'").fetchone() == ('found',2)
    assert con.execute("SELECT entity_status FROM recipient_sam_entities WHERE recipient_id='malformed-R'").fetchone()[0] == 'unsupported'
    assert con.execute('SELECT count(*) FROM sam_uei_registrations WHERE entity_eft_indicator IS NULL').fetchone()[0] == 2


def test_court_endpoint_types_direction_and_ambiguity():
    con: Any = duckdb.connect()
    table(con,'court_citation_map',['citing_opinion_id','cited_opinion_id','dump_date'],[
        ('11','22','2026-01-01'),('11','33','2026-01-01'),('bad','22','2026-01-01'),
    ])
    table(con,'court_opinions',['opinion_id','cluster_id'],[('11','100'),('22','200'),('22','200')])
    table(con,'court_opinion_clusters',['cluster_id','cl_docket_id'],[('100','500'),('200','600')])
    install_relationship_views(con,['court_citation_map','court_opinions','court_opinion_clusters'])
    cases = con.execute('SELECT citing_opinion_id,cited_opinion_id,endpoint_role,target_kind,target_key,target_status '
                        'FROM court_opinion_citation_endpoints ORDER BY 1,2,3').fetchall()
    assert len(cases) == 6
    assert ('11','22','citing','court_opinion','11','found') in cases
    assert ('11','22','cited','court_opinion','22','ambiguous') in cases
    assert ('11','33','cited','court_opinion','33','missing') in cases
    assert ('bad','22','citing','court_opinion','bad','unsupported') in cases
    assert con.execute("SELECT target_kind FROM court_opinion_cluster_endpoints WHERE opinion_id='11'").fetchone()[0] == 'court_cluster'


def test_diff_endpoints_do_not_cross_provider_or_multiply():
    con: Any = duckdb.connect()
    table(con,'section_diff_items', ['bill_id','from_version_code','from_source','to_version_code','to_source',
                                     'seq','from_element_id','to_element_id','from_text_sha256','to_text_sha256'],[
        ('119-hr-1','ih','govinfo','ih','congress','0','s1','s1','old','new'),
        ('119-hr-1','ih','govinfo','ih','congress','1',None,'s2',None,'added'),
    ])
    table(con,'bill_sections',['bill_id','version_code','source','element_id','body_sha256'],[
        ('119-hr-1','ih','govinfo','s1','old'),('119-hr-1','ih','congress','s1','new'),
        ('119-hr-1','ih','congress','s2','wrong'),
    ])
    install_relationship_views(con,['section_diff_items','bill_sections'])
    assert con.execute('SELECT count(*) FROM section_diff_endpoints').fetchone()[0] == 4
    assert con.execute("SELECT target_status,text_digest_status FROM section_diff_endpoints WHERE seq='0'").fetchall() == [('found','matches'),('found','matches')]
    assert con.execute("SELECT target_status,text_digest_status FROM section_diff_endpoints WHERE seq='1' AND endpoint_side='from'").fetchone() == ('unsupported','unavailable')
    assert con.execute("SELECT target_status,text_digest_status FROM section_diff_endpoints WHERE seq='1' AND endpoint_side='to'").fetchone() == ('found','mismatch')
    con.execute("INSERT INTO bill_sections VALUES ('119-hr-1','ih','congress','s1','new')")
    assert con.execute('SELECT count(*) FROM section_diff_endpoints').fetchone()[0] == 4
    assert con.execute("SELECT target_status FROM section_diff_endpoints WHERE seq='0' AND endpoint_side='to'").fetchone()[0] == 'ambiguous'


def test_fec_companion_count_digest_and_explicit_empty_observation():
    con: Any = duckdb.connect()
    table(con,'fec_relationships',['source_locator_json','source_sha256','value_status','source_fields_json','relationship_type'],[
        (json.dumps({'collection_id':'a','source_record_id':'x'}),'sha-one','empty_list','{}','candidate'),
        (json.dumps({'collection_id':'b','source_record_id':'x'}),'sha-wrong','value','{}','candidate'),
        ('{}',None,'missing_field','{}','candidate'),
    ])
    table(con,'fec_source_records',['collection_id','source_record_id','source_sha256','source_url',
                                    'source_locator_json','metadata_json','source_record_json'],[
        ('a','x','sha-one','https://example.test/a','{}','{"candidate_ids":[]}','{}'),
        ('b','x','sha-two','https://example.test/b','{}','{}','{}'),
    ])
    install_relationship_views(con,['fec_relationships','fec_source_records'])
    result = con.execute('SELECT collection_id,value_status,target_status,recorded_digest_status,source_bytes_status '
                         'FROM fec_relationship_evidence ORDER BY collection_id NULLS LAST').fetchall()
    assert result == [('a','empty_list','found','matches','not_checked'),
                      ('b','value','found','mismatch','not_checked'),
                      (None,'missing_field','unsupported','not_checked','not_checked')]
    con.execute("INSERT INTO fec_source_records SELECT * FROM fec_source_records WHERE collection_id='a'")
    assert con.execute('SELECT count(*) FROM fec_relationship_evidence').fetchone()[0] == 3
    assert con.execute("SELECT target_status FROM fec_relationship_evidence WHERE collection_id='a'").fetchone()[0] == 'ambiguous'


def test_fec_collection_cycle_comes_only_from_the_publishers_bulk_directory():
    """FEC files each two-year cycle under bulk-downloads/<even year>/; nothing else names a cycle here."""
    def scope(*urls):
        return json.dumps({'captures': [{'requestUrl': url} for url in urls]})
    bulk = 'https://cg.example.test/bulk-downloads'
    con: Any = duckdb.connect()
    table(con,'fec_collections',['collection_id','source_family','profile','requested_scope_json'],[
        ('bulk-2026-oth-base-file','fec_intercommittee','positional',
         json.dumps({'capture': {'requestUrl': f'{bulk}/2026/oth26.zip'}})),
        ('bulk-pas224-zip','fec_intercommittee','positional',scope(f'{bulk}/2024/pas224.zip')),
        ('bulk-pas2_header_file-csv','fec_intercommittee','positional',scope(f'{bulk}/data_dictionaries/pas2.csv')),
        ('bulk-leadership2024-csv','fec_leadership','positional',scope(f'{bulk}/data.fec.gov/leadership2024.csv')),
        ('committee-census','fec_committees','committee',scope('https://api.open.fec.gov/v1/committees/?cycle=2024')),
        ('two-cycles','fec_candidates','bulk',scope(f'{bulk}/2024/cn24.zip',f'{bulk}/2026/cn26.zip')),
        ('no-scope','fec_candidates','candidate',None),
    ])
    install_relationship_views(con,['fec_collections'])
    rows = con.execute('SELECT collection_id,cycle,cycle_status FROM fec_collection_cycles ORDER BY collection_id').fetchall()
    assert rows == [('bulk-2026-oth-base-file','2026','bulk_directory'),
                    ('bulk-leadership2024-csv',None,'not_stated'),
                    ('bulk-pas224-zip','2024','bulk_directory'),
                    ('bulk-pas2_header_file-csv',None,'not_stated'),
                    ('committee-census',None,'not_stated'),
                    ('no-scope',None,'not_stated'),
                    ('two-cycles',None,'ambiguous')]


def test_offered_urls_and_native_topic_namespaces():
    con: Any = duckdb.connect()
    table(con,'documents',['document_id','attachments_json'],[
        ('doc','[{"url":"https://example.test/a","format":"PDF","size":123},{"url":"https://example.test/a","format":"HTML"}]')])
    table(con,'federal_register',['document_number','publication_date','agencies_json','topics_json'],[
        ('2026-17334','2026-08-25','[{"id":406,"name":"Office of Personnel Management","slug":"personnel-management-office"}]',
         '["Government employees","Government employees"]')])
    table(con,'lobbying_activities',['filing_uuid','activity_index','government_entities_json'],[('filing','0','[]')])
    install_relationship_views(con,['documents','federal_register','lobbying_activities'])
    assert con.execute('SELECT count(*) FROM document_artifacts_occurrences').fetchone()[0] == 2
    assert con.execute('SELECT count(*) FROM document_artifacts_pairs').fetchone()[0] == 1
    assert con.execute('SELECT DISTINCT acquisition_status,retained_digest FROM document_artifacts_occurrences').fetchall() == [('not_checked',None)]
    assert con.execute('SELECT source_namespace,target_key FROM federal_register_agencies_occurrences').fetchone() == ('federal_register_agency','406')
    assert con.execute('SELECT count(*) FROM lobbying_contacted_entities_pairs').fetchone()[0] == 0


def test_diff_version_endpoints_retain_engine_revision_and_candidates():
    con: Any = duckdb.connect()
    table(con,'section_diffs',['bill_id','from_version_code','from_source','to_version_code','to_source',
                               'engine_name','engine_version','engine_revision','computed_at'],[
        ('119-hr-1','ih','govinfo','ih','congress','engine','v1','abc123','2026-09-27'),
    ])
    table(con,'bill_versions',['bill_id','version_code','source','sha256','resolved_url','observed_at'],[
        ('119-hr-1','ih','govinfo','old','https://example.test/old','2026-09-26'),
        ('119-hr-1','ih','congress','new','https://example.test/new','2026-09-27'),
    ])
    install_relationship_views(con,['section_diffs','bill_versions'])
    result = con.execute('SELECT endpoint_side,engine_revision,target_status,candidate_versions_json '
                         'FROM section_diff_version_endpoints ORDER BY endpoint_side').fetchall()
    assert [(r[0],r[1],r[2]) for r in result] == [('from','abc123','found'),('to','abc123','found')]
    assert json.loads(result[0][3])[0]['sha256'] == 'old'
    assert json.loads(result[1][3])[0]['sha256'] == 'new'


def test_agenda_rin_keeps_all_editions_and_reports_duplicate_targets():
    con: Any = duckdb.connect()
    table(con,'regulatory_agenda_items',['agenda_item_id','rin'], [('item','3206-AO36'),('missing','0000-XXXX')])
    table(con,'unified_agenda',['rin','agenda_edition','url'],[
        ('3206-AO36','202504','https://example.test/202504'),
        ('3206-AO36','202510','https://example.test/202510'),
        ('3206-AO36','202510','https://example.test/202510'),
    ])
    install_relationship_views(con,['regulatory_agenda_items','unified_agenda'])
    assert con.execute('SELECT agenda_item_id,agenda_edition,target_count,target_status FROM agenda_item_editions '
                       'ORDER BY agenda_item_id,agenda_edition').fetchall() == [
                           ('item','202504',1,'found'),('item','202510',2,'ambiguous'),('missing',None,0,'missing')]


def test_identity_candidates_preserve_repeats_alternatives_and_pending_status():
    from spicy_regs.relationship_views.identity_candidates import FIELDS
    con: Any = duckdb.connect()
    record = ('Shared Name','SHARED NAME','SHARED NAME','organization_field','C00008896','SHARED NAME PAC',
              'prefix','medium','2','2000-01-01','2026-09-27')
    alternate = (*record[:4],'C00000001',*record[5:])
    table(con,'org_committee_links',FIELDS,[record,record,alternate])
    first_pin = {'org_committee_links': {'artifact_digest':'sha256:first'}}
    install_relationship_views(con,['org_committee_links'],first_pin)
    found = con.execute('SELECT candidate_id,observed_name,candidate_target_id,decision,acting_role '
                        'FROM org_identity_candidates ORDER BY candidate_target_id').fetchall()
    assert len(found) == 3
    assert len({r[0] for r in found}) == 2
    assert {r[1] for r in found} == {'Shared Name'}
    assert {(r[3],r[4]) for r in found} == {('pending','unknown')}
    old = {r[0] for r in found}
    install_relationship_views(con,['org_committee_links'],{'org_committee_links': {'artifact_digest':'sha256:second'}})
    assert old.isdisjoint({r[0] for r in con.execute('SELECT candidate_id FROM org_identity_candidates').fetchall()})
    install_relationship_views(con,['org_committee_links'])
    assert con.execute('SELECT DISTINCT candidate_id,candidate_id_status FROM org_identity_candidates').fetchall() == [(None,'unversioned_source')]


def test_party_intervals_preserve_boundary_gaps_overlap_and_missing_end():
    from spicy_regs.relationship_views.affiliations import AFFILIATION_FIELDS
    con: Any = duckdb.connect()
    table(con,'member_vote_terms',['vote_id','member_key','bioguide_id','term_index','vote_day'],[
        ('before','member','T000254','3','1964-09-15'),('boundary','member','T000254','3','1964-09-16'),
        ('gap','member','T000254','3','1967-01-03'),('undated','member','T000254','3',None),
    ])
    def affiliation(index,party,start,end,end_status='valid'):
        return ('T000254','capture','3',index,party,start,end,'valid',end_status,'Republican',
                '/terms/3/party_affiliations/'+index,'{}','2026-09-27')
    table(con,'member_party_affiliations',AFFILIATION_FIELDS,[
        affiliation('0','Democrat','1961-01-03','1964-09-16'),
        affiliation('1','Republican','1964-09-16','1967-01-03'),
        affiliation('2','Unknown','1967-01-03',None,'absent'),
    ])
    install_relationship_views(con,['member_vote_terms','member_party_affiliations'])
    result = con.execute('SELECT vote_id,dated_party,party_status,target_count FROM member_vote_party_affiliations '
                         'ORDER BY vote_id').fetchall()
    assert result == [('before','Democrat','source_interval',1),('boundary','Republican','source_interval',1),
                      ('gap',None,'unknown',0),('undated',None,'unknown',0)]
    candidate = json.loads(con.execute("SELECT affiliation_candidates_json FROM member_vote_party_affiliations WHERE vote_id='boundary'").fetchone()[0])[0]
    assert candidate['input_sha256'] == 'capture' and candidate['affiliation_index'] == '1'
    con.execute('INSERT INTO member_party_affiliations VALUES ('+','.join('?' for _ in AFFILIATION_FIELDS)+')',
                affiliation('3','Other','1964-09-01','1965-01-01'))
    assert con.execute('SELECT count(*) FROM member_vote_party_affiliations').fetchone()[0] == 4
    assert con.execute("SELECT dated_party,party_status,target_count FROM member_vote_party_affiliations WHERE vote_id='boundary'").fetchone() == (None,'ambiguous',2)


def test_communication_rins_retain_scoped_keys_spans_and_source_digest():
    con: Any = duckdb.connect()
    occurrence = {'rin':'1218-AC97','ordinal':0,'matched_text':'RIN 1218-AC97','span_start':20,'span_end':33,
                  'field_sha256':'field-sha','rule':'labeled-rin','rule_version':'v1'}
    table(con,'house_communications',['congress','communication_type','number','rin_occurrences_json',
                                     'source_route','record_package_id','record_granule_id','update_date','detail_read'],[
        ('114','ec','4329',json.dumps([occurrence,occurrence]),'congressional-record-granule',
         'CREC-2016-02-12','CREC-2016-02-12-pt1-PgH815-4','2016-02-12','true'),
        ('115','ec','4329','[]','house-communication',None,None,'2017-01-01','true'),
    ])
    install_relationship_views(con,['house_communications'])
    result = con.execute('SELECT congress,communication_type,number,source_ordinal,target_key,field_sha256,span_start '
                         'FROM house_communication_rins_occurrences ORDER BY source_ordinal').fetchall()
    assert result == [('114','ec','4329',0,'1218-AC97','field-sha','20'),
                      ('114','ec','4329',1,'1218-AC97','field-sha','20')]
    assert con.execute('SELECT count(*) FROM house_communication_rins_pairs').fetchone()[0] == 1


def test_fec_navigation_excludes_large_bodies_and_preserves_all_candidates():
    con: Any = duckdb.connect(config={'memory_limit': '64MB', 'threads': 1})
    table(con, 'fec_relationships', ['source_locator_json', 'source_sha256', 'source_fields_json', 'relationship_type'], [
        ('{"collection_id":"a","source_record_id":"x"}', 'sha-one', '{}', 'candidate'),
    ])
    # A raw body read would exceed the memory limit; navigation must prune it.
    con.execute("""CREATE VIEW fec_source_records AS SELECT 'a' AS collection_id, 'x' AS source_record_id,
        CASE WHEN i%2=0 THEN 'sha-one' END AS source_sha256,
        repeat('body-' || i::VARCHAR, 100000) AS source_record_json,
        'metadata' AS metadata_json, '{}' AS source_locator_json, 'https://example.test/' AS source_url
        FROM range(2000) AS rows(i)""")
    install_relationship_views(con, ['fec_relationships', 'fec_source_records'])
    plan = con.execute('EXPLAIN SELECT * FROM fec_relationship_evidence LIMIT 1').fetchone()[1]
    assert 'BLOCKWISE_NL_JOIN' not in plan and 'DELIM_JOIN' not in plan
    result = con.cursor().execute('SELECT * FROM fec_relationship_evidence LIMIT 1')
    row = dict(zip([c[0] for c in result.description], result.fetchone(), strict=True))
    assert row['target_count'] == 2000
    assert row['target_status'] == 'ambiguous'
    assert row['recorded_digest_status'] == 'not_checked'
    candidates = json.loads(row['companion_candidates_json'])
    assert len(candidates) == 2000
    assert all(set(c) == {'source_sha256'} for c in candidates)
    assert sum(c['source_sha256'] is None for c in candidates) == 1000
    assert len(row['companion_candidates_json']) < 100000
    assert con.execute('SELECT source_record_json FROM fec_source_records '
                       'WHERE collection_id=? AND source_record_id=? LIMIT 1', ['a', 'x']).fetchone()[0].startswith('body-')
