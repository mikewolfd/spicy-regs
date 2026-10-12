"""Complete FEC conversion and staged-parent controls with tiny original families."""
import base64
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import native_conversion as conversion
from spicy_regs.conversion_reads import ConversionReadOperation
from spicy_regs.fec_native_conversion import build_retained_family, staged_snapshot, family_tables
from spicy_regs.generations import build_generation
from spicy_regs.sources import publication
from spicy_regs.transforms.build_fec_observations import RECORD_SCHEMA, COLLECTION_SCHEMA, RELATIONSHIP_SCHEMA
from tests.test_native_conversion import bucket as _bucket, BASE, MAIN, WHEEL, STATE


@pytest.fixture(name="bucket")
def fec_bucket(monkeypatch):
    factory=getattr(_bucket,"__wrapped__",None)
    if not callable(factory):
        raise RuntimeError("The shared publication fixture has no callable factory")
    return factory(monkeypatch)


def old_family(tmp, monkeypatch, store, family, *, populated=False, rows_by_table=None, legacy_tables=()):
    from spicy_regs import etl_policy_registry
    if family == 'fec-observations':
        schemas = dict(zip(('fec_source_records', 'fec_collections', 'fec_relationships'),
                           (RECORD_SCHEMA, COLLECTION_SCHEMA, RELATIONSHIP_SCHEMA), strict=True))
    else:
        declared = json.loads((conversion.Path(conversion.__file__).resolve().parents[2] / 'data_dictionary/fec_typed_schemas.json').read_text())
        schemas = {name: pa.ipc.read_schema(pa.BufferReader(base64.b64decode(v['arrow_schema_ipc_base64'])))
                   for name, v in declared['tables'].items()}
    legacy=json.loads((conversion.Path(conversion.__file__).with_name('fec_retained_legacy_schemas.json')).read_text())
    for name in legacy_tables:
        schemas[name]=pa.ipc.read_schema(pa.BufferReader(base64.b64decode(legacy[name])))
    files = []
    for name, schema in schemas.items():
        path = tmp / 'original' / (name + '.parquet')
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = []
        if populated and name == 'fec_source_records':
            rows = [dict(collection_id='collection', source_record_id='record', source_sha256='sha256:' + 'a'*64,
                         source_family='fec-test', source_record_json='{"amount":"5"}')]*2
        rows = (rows_by_table or {}).get(name, rows)
        pq.write_table(pa.Table.from_pylist(rows, schema=schema.with_metadata({'retained': 'fixture'})), path)
        files.append(path)
    with monkeypatch.context() as patch:
        patch.setattr(etl_policy_registry, 'require_registered_receipts', lambda *_: None)
        build_generation(tmp / 'old-generation', family=family, files=files, expected_keys=[p.name for p in files])
        snapshot = publication.publish_generation(tmp / 'old-generation', client=store, bucket='spicy-regs',
                                                   prior_index=publication.current_index(BASE))
    return snapshot, {p.name: p for p in files}


def test_observations_converter_complete_family_restores_duplicate_rows_and_metadata(tmp_path, monkeypatch, bucket):
    snapshot, retained = old_family(tmp_path, monkeypatch, bucket, 'fec-observations', populated=True)
    receipt = conversion.convert('fec-observations', allowed=['fec-observations'], work=tmp_path / 'conversion',
        expected_main=MAIN, expected_spicy_docs=WHEEL, state=lambda _: dict(STATE))
    assert set(receipt['tables']) == {key.removesuffix('.parquet') for key in family_tables('fec-observations')}
    assert receipt['tables']['fec_source_records']['retained_rows'] == 2
    assert receipt['prepared'] and receipt['published'] is None
    assert snapshot == publication.current_index(BASE)


def test_incomplete_observations_membership_refused(tmp_path, monkeypatch, bucket):
    snapshot, retained = old_family(tmp_path, monkeypatch, bucket, 'fec-observations')
    old = snapshot['families']['fec-observations']
    with ConversionReadOperation() as operation:
        with pytest.raises(ValueError, match='retained table membership'):
            build_retained_family('fec-observations', old, snapshot, {next(iter(retained)): next(iter(retained.values()))},
                                  tmp_path / 'incomplete', operation=operation)
    wrong = {**old, 'tables': {next(iter(old['tables'])): next(iter(old['tables'].values()))}}
    with pytest.raises(ValueError, match='complete registered'):
        conversion._check_prepared_membership('fec-observations', wrong, {})


def test_query_requires_explicit_filing_inputs_and_exact_native_parent(tmp_path, monkeypatch, bucket):
    source_snapshot, source = old_family(tmp_path / 'source', monkeypatch, bucket, 'fec-observations')
    snapshot, retained = old_family(tmp_path / 'query', monkeypatch, bucket, 'fec-query')
    with ConversionReadOperation() as operation:
        native = build_retained_family('fec-observations', snapshot['families']['fec-observations'], snapshot,
                                      source, tmp_path / 'native-source', operation=operation)
        staged = staged_snapshot(snapshot, native, operation)
        assert staged['families']['fec-query'] == snapshot['families']['fec-query']
        assert staged['families']['fec-observations']['artifactDigest'] != snapshot['families']['fec-observations']['artifactDigest']
        assert 'publishedAt' not in staged['families']['fec-observations']
        from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
        from spicy_regs.transforms.fec_identity_receipts import dataset_policy
        target = tmp_path / 'empty-filings.parquet'
        pq.write_table(pa.Table.from_batches([], schema=dataset_policy('fec_filings').subject_schema), target)
        selection = FilingAssociationInputs(staged['families']['fec-observations']['artifactDigest'], None,
            [TypedInput(target, _sha(target), 0)], {}, filing_generation_scope='self')
        complete = build_retained_family('fec-query', snapshot['families']['fec-query'], snapshot, retained,
            tmp_path / 'complete-query', operation=operation, read_snapshot=staged, filing_associations=selection,
            observation_generation=native)
        artifact, reader, _ = operation.admit_generation(complete)
        assert set(artifact.root['spec']['tables']) == family_tables('fec-query')
        assert artifact.root['spec']['readSnapshot'] == staged
        assert set(artifact.root['spec']['parents']) == family_tables('fec-observations')
        for key, original in retained.items():
            assert reader.compare_original(key.removesuffix('.parquet'), original)['rows_only_in_restored'] == 0
        conversion._check_prepared_membership('fec-query', snapshot['families']['fec-query'], artifact.root['spec'])
        with pytest.raises(ValueError, match='admitted filing association'):
            build_retained_family('fec-query', snapshot['families']['fec-query'], snapshot, retained,
                tmp_path / 'native-query', operation=operation, read_snapshot=staged)
        with pytest.raises(ValueError, match='selected native observations'):
            build_retained_family('fec-query', snapshot['families']['fec-query'], snapshot, retained,
                tmp_path / 'legacy-query', operation=operation)


def test_source_occurrence_ordinals_allow_sample_gaps_and_refuse_resets(tmp_path):
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from tests.test_fec_identity_receipts import WITNESS
    with IdentityReceiptWriter(tmp_path / 'source', generation_id='sample', tables=['fec_source_records']) as writer:
        row = dict(collection_id='c', source_record_id='r', source_sha256=WITNESS['sha256'])
        writer.emit('fec_source_records', row, input_witness=WITNESS, observation_ordinal=32)
        writer.emit('fec_source_records', row, input_witness=WITNESS, observation_ordinal=4096)
        with pytest.raises(ValueError, match='must increase'):
            writer.emit('fec_source_records', row, input_witness=WITNESS, observation_ordinal=32)
    actual = pq.ParquetFile(tmp_path / 'source/fec_source_records.parquet').read().to_pylist()
    assert [r['observation_ordinal'] for r in actual] == [32, 4096]


def test_changed_fec_policies_preserve_exact_earlier_admission():
    from spicy_regs.subject_catalog import policies
    from spicy_regs.etl_receipts import receipt_policies
    from dataclasses import replace
    for name in ('fec_source_records', 'fec_receipts', 'fec_agency_mapping_dispositions'):
        current = policies()[name]
        versions = receipt_policies(current)
        assert [p.policy_version.rsplit('/', 1)[1] for p in versions] == ['3', '2', '1']
        changed = replace(current, subject_schema=current.subject_schema.append(pa.field('unadmitted', pa.string())))
        assert receipt_policies(changed) == (changed,)


def test_production_query_prepares_with_official_staged_parent_and_rejects_stale_publication(tmp_path, monkeypatch, bucket):
    from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    from tests.test_native_conversion import BUCKET
    old_family(tmp_path / 'source', monkeypatch, bucket, 'fec-observations')
    snapshot, retained = old_family(tmp_path / 'query', monkeypatch, bucket, 'fec-query')
    source_work = tmp_path / 'source-conversion'
    source = conversion.convert('fec-observations', allowed=['fec-observations'], work=source_work,
        expected_main=MAIN, expected_spicy_docs=WHEEL, state=lambda _: dict(STATE))
    target = tmp_path / 'filing-selection.parquet'
    pq.write_table(pa.Table.from_batches([], schema=dataset_policy('fec_filings').subject_schema), target)
    selection = FilingAssociationInputs(source['generation']['artifactDigest'], None,
        [TypedInput(target, _sha(target), 0)], {}, filing_generation_scope='self')
    work = tmp_path / 'query-conversion'
    done = conversion.convert('fec-query', allowed=['fec-query'], work=work, staged_observations=source_work / 'build/generation',
        filing_associations=selection, expected_main=MAIN, expected_spicy_docs=WHEEL, state=lambda _: dict(STATE))
    assert done['prepared'] and done['staged_observations']['preparation'] == source
    assert done['captured']['entry'] == snapshot['families']['fec-query']
    # Candidate staging is not pointer publication: query cannot publish until its exact parent is selected.
    with pytest.raises(conversion.ConversionRefused, match='parent|Parent'):
        conversion.publish_prepared(work / 'conversion.json', allowed=['fec-query'], expected_main=MAIN,
            expected_spicy_docs=WHEEL, expect_bucket=BUCKET, state=lambda _: dict(STATE))
    assert publication.current_index(BASE) == snapshot


def test_nonempty_complete_query_links_filing_source_and_retained_history(tmp_path, monkeypatch, bucket):
    from decimal import Decimal
    from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs, TypedInput, _sha
    from spicy_regs.transforms.fec_identity_context_fields import normalize_record
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    from tests.test_fec_filing_associations import financial, filing, SHA, NAMESPACE, EVIDENCE, pin
    from spicy_docs.sources.fec.committee_master import COMMITTEE_MASTER_FIELDS
    from spicy_docs.schemas.fec_committee_history import project_committee_master_row
    record=dict(collection_id='c',source_record_id='native-row',source_sha256=SHA,source_family='fixture',source_record_json='{}')
    observations,_=old_family(tmp_path/'source',monkeypatch,bucket,'fec-observations',rows_by_table={'fec_source_records':[record]})
    original_filing={**filing(), 'collection_id':'filings', 'source_record_id':'metadata', 'source_sha256':SHA,
                     'source_locator_json':'{"pointer":"/results/0"}'}
    native=dict.fromkeys(COMMITTEE_MASTER_FIELDS,'')
    native.update(CMTE_ID='C00000001',CMTE_NM='TEST COMMITTEE')
    business=project_committee_master_row({'fields':native,'cycle':2024})
    history={**business,'cycle':'2024','source_cycle':2024,'native_fields':native,'record_id':pin('history'),
             'mapping_version':'fec-retained-committee-history/1'}
    old_header=dict(record_id=pin('old-header'),policy_version='fec-retained-filing-association/1',
        target_table='fec_source_records',target_record_id='native-row',source_generation_pin=observations['families']['fec-observations']['artifactDigest'],
        collection_id='c',source_record_id='native-row',source_sha256=SHA,source_authority='official-fec',
        source_locator_json=financial()['source_locator_json'],association_basis='fixture-native-header',
        association_status='resolved_native_filing_key',referenced_filing_key=filing()['filing_key'],filing_key=filing()['filing_key'],
        selection_evidence_sha256=EVIDENCE,namespace_evidence_sha256=EVIDENCE,header_record_id='header-record',
        header_locator_json='{"pointer":"/header"}',filing_observation_count=1,filing_observation_ids=['metadata-1'])
    snapshot,retained=old_family(tmp_path/'query',monkeypatch,bucket,'fec-query',rows_by_table={
        'fec_filings':[original_filing], 'fec_filing_header_associations':[old_header], 'fec_receipts':[{**financial(),'amount':Decimal('5')}],
        'fec_agency_mapping_dispositions':[{'collection_id':'c','source_record_id':'native-row','mapping_status':'mapped'}],
        'fec_committee_master_observations':[history],
        'fec_record_evidence':[dict(target_table='fec_receipts',target_record_id=financial()['record_id'],
            target_generation_scope='self',witness_generation_scope='external',
            witness_generation_pin=observations['families']['fec-observations']['artifactDigest'],
            endpoint_kind='source_record',collection_id='c',source_record_id='native-row',witness_sha256=SHA,
            witness_locator_json=financial()['source_locator_json'])]},legacy_tables=['fec_committee_master_observations'])
    source_work=tmp_path/'prepared-source'
    source=conversion.convert('fec-observations',allowed=['fec-observations'],work=source_work,
        expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE))
    original=pq.ParquetFile(retained['fec_filings.parquet']).read().to_pylist()[0]
    mapped=normalize_record('fec_filings',original)
    target=tmp_path/'admitted-filings.parquet'
    pq.write_table(pa.Table.from_pylist([mapped],schema=dataset_policy('fec_filings').subject_schema),target)
    headers=tmp_path/'current-headers.parquet'
    old_schema=pq.read_schema(retained['fec_filing_header_associations.parquet'])
    from spicy_regs.transforms.fec_filing_associations import _base
    current_header={**old_header,'policy_version':'fec-retained-filing-association/2',
                    'selection_evidence_sha256':pin('current-header-selection')}
    current_header['record_id']=_base({**current_header,'record_id':'native-row'},'fec_source_records',
        current_header['source_generation_pin'],current_header['selection_evidence_sha256'],current_header['association_basis'])['record_id']
    pq.write_table(pa.Table.from_pylist([current_header],schema=old_schema),headers)
    selection=FilingAssociationInputs(observations['families']['fec-observations']['artifactDigest'],None,
        [TypedInput(target,_sha(target),1)],{NAMESPACE:EVIDENCE},[TypedInput(headers,_sha(headers),1)],filing_generation_scope='self')
    work=tmp_path/'prepared-query'
    result=conversion.convert('fec-query',allowed=['fec-query'],work=work,staged_observations=source_work/'build/generation',
        filing_associations=selection,expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE))
    generation=work/'build/generation'
    [money]=pq.ParquetFile(generation/'fec_receipts.parquet').read().to_pylist()
    assert money['amount']==Decimal('5') and money['filing_key']==filing()['filing_key']
    assert money['filing_association_target_generation_scope']=='self' and money['filing_association_target_generation_pin'] is None
    [header]=pq.ParquetFile(generation/'fec_filing_header_associations.parquet').read().to_pylist()
    assert header['filing_key']==filing()['filing_key'] and header['policy_version']=='fec-retained-filing-association/2'
    assert header['source_generation_pin']==source['generation']['artifactDigest']
    assert header['record_id']==_base({**current_header,'record_id':'native-row'},'fec_source_records',
        source['generation']['artifactDigest'],current_header['selection_evidence_sha256'],current_header['association_basis'])['record_id']
    [agency]=pq.ParquetFile(generation/'fec_agency_mapping_dispositions.parquet').read().to_pylist()
    assert agency['source_sha256']==SHA
    [witness]=pq.ParquetFile(generation/'fec_record_evidence.parquet').read().to_pylist()
    assert witness['witness_generation_pin']==source['generation']['artifactDigest']
    [committee]=pq.ParquetFile(generation/'fec_committee_master_observations.parquet').read().to_pylist()
    assert committee['name']=='TEST COMMITTEE' and committee['cycle']==2024
    assert result['tables']['fec_receipts']['retained_rows']==1
    with ConversionReadOperation() as operation:
        _,reader,_=operation.admit_generation(generation)
        for key,original in retained.items():
            reader.compare_original(key.removesuffix('.parquet'),original)


def test_resource_callback_reaches_fec_inner_table_loops(tmp_path,monkeypatch,bucket):
    snapshot,retained=old_family(tmp_path,monkeypatch,bucket,'fec-observations',populated=True)
    calls=[]
    def guard():
        calls.append(True)
        if len(calls)==4:
            raise TimeoutError('bounded fixture stop')
    with pytest.raises(TimeoutError,match='bounded fixture stop'):
        conversion.convert('fec-observations',allowed=['fec-observations'],work=tmp_path/'stopped',
            expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE),check_resources=guard)
    assert len(calls)==4 and publication.current_index(BASE)==snapshot
    assert not (tmp_path/'stopped/build/generation/artifact.json').exists()


def test_complete_fec_observations_resume_reuses_writer_without_rebuild(tmp_path,monkeypatch,bucket):
    from spicy_regs.generations import implementation_id
    from tests.test_native_conversion import PINNED
    old_family(tmp_path,monkeypatch,bucket,'fec-observations',populated=True)
    work=tmp_path/'work'
    original=conversion.convert('fec-observations',allowed=['fec-observations'],work=work,
        expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE))
    interrupted=dict(original)
    interrupted.pop('prepared')
    (work/'conversion.json').write_text(json.dumps(interrupted))
    identity=implementation_id()
    monkeypatch.setattr(conversion,'_historical_producer',lambda *_:(identity,{'version':WHEEL,'wheels':[{'hash':PINNED}]}))
    def rebuild(*args,**kwargs):
        pytest.fail('Resume must never call a FEC writer')
    monkeypatch.setattr(conversion,'_convert_fec',rebuild)
    resumed=conversion.resume_prepared(work/'build/generation',work=work,allowed=['fec-observations'],
        expected_main=MAIN,expected_spicy_docs=WHEEL,expected_producer=MAIN,state=lambda _:dict(STATE))
    assert resumed['prepared'] and resumed['tables']==original['tables'] and resumed['published'] is None


def test_legacy_source_page_refusal_retains_exact_original_evidence(tmp_path):
    from spicy_regs.fec_native_conversion import retained_identity_row
    from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter
    from spicy_regs.etl_receipts import _unpack
    from tests.test_fec_identity_receipts import WITNESS
    old=dict(record_id='held-page',title='Server error',text='Server error\nOriginal error text',source_url='https://www.fec.gov/error',
             headings_json='[{"tag":"h1","text":"Server error"}]',times_json='[]')
    row,overrides=retained_identity_row('fec_research_source_pages',old)
    assert row['content_status']=='failed_page' and row['text'] is None
    with IdentityReceiptWriter(tmp_path/'page',generation_id='page',tables=['fec_research_source_pages']) as writer:
        writer.emit('fec_research_source_pages',row,input_witness=WITNESS,source_input={'fec_retained_overrides':overrides})
    [receipt]=pq.ParquetFile(tmp_path/'page/etl_receipts.parquet').read().to_pylist()
    assert receipt['outcome']=='refused'
    held=_unpack(json.loads(receipt['processing_json']))
    reconstructed={name:held['source_input']['fec_retained_overrides'].get(name,held.get(name)) for name in old}
    assert reconstructed==old


def test_foreign_generation_pin_is_never_rebound():
    from spicy_regs.fec_native_conversion import ObservationRebinding,rebind_row
    proof=ObservationRebinding('sha256:'+'a'*64,'sha256:'+'b'*64)
    foreign={'witness_generation_pin':'sha256:'+'c'*64}
    assert rebind_row('fec_record_evidence',foreign,proof)==foreign


def test_adapters_refuse_unexplained_columns_and_third_schema():
    from spicy_regs.fec_native_conversion import retained_identity_row,validate_identity_schema
    with pytest.raises(ValueError,match='unexplained'):
        retained_identity_row('fec_filing_links',{'unreviewed_business_data':'value'})
    with pytest.raises(ValueError,match='neither supported current nor captured legacy'):
        validate_identity_schema('fec_filing_links',pa.schema([('record_id',pa.int64())]))


def test_noncanonical_partition_order_refuses_before_family_writes(tmp_path):
    from spicy_regs.fec_native_conversion import check_membership
    tables={key:{} for key in family_tables('fec-observations')}
    tables['fec_source_records.parquet']={'partitionColumns':['source_namespace'],'members':[
        {'key':'fec_source_records/source_namespace=fixture/part-000001.parquet'},
        {'key':'fec_source_records/source_namespace=fixture/part-000000.parquet'}]}
    with pytest.raises(ValueError,match='Noncanonical captured FEC member order'):
        check_membership('fec-observations',{'tables':tables})
    assert not (tmp_path/'build').exists()


def test_legacy_source_page_population_refuses_before_retaining_large_inputs(tmp_path,monkeypatch,bucket):
    from spicy_regs.fec_native_conversion import build_retained_family
    snapshot,retained=old_family(tmp_path,monkeypatch,bucket,'fec-query',
        rows_by_table={'fec_research_source_pages':[{'record_id':'page','title':'Server error','text':'Server error'}]},
        legacy_tables=['fec_research_source_pages'])
    def retain(*args,**kwargs):
        pytest.fail('Known unsupported source pages must refuse before any family member retention')
    monkeypatch.setattr(conversion,'_retain_table',retain)
    with pytest.raises(conversion.ConversionRefused,match='unsupported refused-page original reconstruction'):
        conversion.convert('fec-query',allowed=['fec-query'],work=tmp_path/'work',expected_main=MAIN,
            expected_spicy_docs=WHEEL,state=lambda _:dict(STATE))
    assert not (tmp_path/'work/build').exists() and not (tmp_path/'work/retained').exists()
    with ConversionReadOperation() as operation:
        with pytest.raises(ValueError,match='unsupported refused-page original reconstruction'):
            build_retained_family('fec-query',snapshot['families']['fec-query'],snapshot,retained,
                tmp_path/'direct',operation=operation)
    assert not (tmp_path/'direct').exists()


def test_already_current_header_selection_rejects_wrong_generation_bound_identity(tmp_path):
    from spicy_regs.fec_native_conversion import header_subjects
    from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs,TypedInput,_sha
    from tests.test_fec_filing_associations import pin,SHA
    row=dict(record_id=pin('old'),policy_version='fec-retained-filing-association/1',target_table='fec_source_records',
        target_record_id='r',source_generation_pin=pin('current-source'),collection_id='c',source_record_id='r',
        source_sha256=SHA,source_authority='official-fec',source_locator_json='{"collection_id":"c","source_record_id":"r"}',
        association_basis='native-api-original-url',selection_evidence_sha256=pin('selection'))
    original=tmp_path/'original.parquet'
    pq.write_table(pa.Table.from_pylist([row]),original)
    current=tmp_path/'current.parquet'
    pq.write_table(pa.Table.from_pylist([{**row,'record_id':pin('wrong-current-id'),'policy_version':'fec-retained-filing-association/2'}]),current)
    selection=FilingAssociationInputs(row['source_generation_pin'],None,(),{},[TypedInput(current,_sha(current),1)],
        filing_generation_scope='self')
    with pytest.raises(ValueError,match='maintained source-generation rule'):
        header_subjects(selection,{'fec_filing_header_associations.parquet':original},None,lambda:None)


def test_conversion_reuses_exact_local_originals_without_fetch(tmp_path,monkeypatch,bucket):
    snapshot,retained=old_family(tmp_path,monkeypatch,bucket,'fec-observations',populated=True)
    selected={member.path:str(retained[key]) for key in retained for member in publication.table_members(snapshot,key)}
    def fetch(*args,**kwargs):
        pytest.fail('Explicit complete local original selection must never download')
    monkeypatch.setattr(publication,'fetch_member',fetch)
    work=tmp_path/'reused'
    result=conversion.convert('fec-observations',allowed=['fec-observations'],work=work,retained_members=selected,
        expected_main=MAIN,expected_spicy_docs=WHEEL,state=lambda _:dict(STATE))
    assert result['prepared']
    for key,path in retained.items():
        assert path.stat().st_ino==(work/'retained'/key).stat().st_ino
