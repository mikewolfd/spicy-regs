"""Complete population admission and incremental invalidation, using tiny main files."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.explorer_navigation import array, route, key, part, guard
from spicy_regs.navigation_measurements import MeasurementCache
from spicy_regs.native_types import described_schema
from spicy_regs.sources.publication import table_members


def member(tmp_path, name, rows, schema=None):
    path = tmp_path / (name + '.parquet')
    table = pa.Table.from_pylist(rows, schema=schema)
    pq.write_table(table, path)
    return path, {'sha256':'sha256:'+hashlib.sha256(path.read_bytes()).hexdigest(),
                  'byteSize':path.stat().st_size, 'rows':len(rows),
                  'columns':[list(c) for c in described_schema(table.schema)]}


def selection(tmp_path, source_rows=None, *, split=False):
    source_rows = source_rows or [
        {'id':'s1','congress':'119','refs':[{'id':'a'},{'id':'a'},{'id':'missing'},None]},
        {'id':'s2','congress':'118','refs':[{'id':'b'}]},
        {'id':'s3','congress':'119','refs':[]},
        {'id':'s4','congress':'119','refs':None}]
    schema = pa.schema([('id',pa.string()),('congress',pa.string()),('refs',pa.list_(pa.struct([('id',pa.string())])))])
    src, descriptor = member(tmp_path,'source',source_rows,schema)
    tgt, target = member(tmp_path,'target',[{'id':'a'},{'id':'b'},{'id':'b'},{'id':None}])
    generation='a'*64
    index={'format':'spicy-regs-publication','version':2,'families':{'fixture':{
        'prefix':'generations/fixture/'+generation,'logicalId':'urn:fixture','artifactDigest':'sha256:'+generation,
        'tables':{'source.parquet':descriptor,'target.parquet':target}}}}
    paths={table_members(index,'source.parquet')[0].path:str(src),
           table_members(index,'target.parquet')[0].path:str(tgt)}
    if split:
        p0, d0=member(tmp_path,'first',source_rows[:2],schema)
        p1, d1=member(tmp_path,'second',source_rows[2:],schema)
        descriptors=[]
        for i,d in enumerate((d0,d1)):
            descriptors.append({'key':f'source/bucket={i}/part-000000.parquet',
                                **{k:d[k] for k in ('rows','byteSize','sha256')},'partition':{'bucket':str(i)}})
        # Published partition column belongs to every physical member too.
        for i,p in enumerate((p0,p1)):
            table=pq.read_table(p).append_column('bucket',pa.array([str(i)]*(d0,d1)[i]['rows']))
            pq.write_table(table,p)
            descriptors[i].update(sha256='sha256:'+hashlib.sha256(p.read_bytes()).hexdigest(),byteSize=p.stat().st_size)
        descriptor={'columns':[list(c) for c in described_schema(pq.ParquetFile(p0).schema_arrow)],
                    'rows':len(source_rows),'byteSize':sum(m['byteSize'] for m in descriptors),
                    'partitionColumns':['bucket'],'members':descriptors}
        index['families']['fixture']['tables']['source.parquet']=descriptor
        del paths[next(k for k in paths if k.endswith('/source.parquet'))]
        paths.update({m.path:str(p) for m,p in zip(table_members(index,'source.parquet'),(p0,p1),strict=True)})
    spec=array('references','source',('refs',),(route('target',('id',),(key(part('id')),)),),meaning='Explicit references.')
    return index,paths,spec


def test_cache_hit_skips_source_projection_recipe_scan_and_key_aggregation(tmp_path):
    index,paths,spec=selection(tmp_path)
    cache=MeasurementCache(tmp_path/'cache')
    result=cache.measure(index,paths,spec,source_identity=('id',))
    assert (result['eligible'],result['matched'],result['missing'],result['ambiguous']) == (4,3,1,1)
    assert result['repeatedReferences'] == 1
    assert result['unsupportedReferences'] == 1
    assert result['rawReferences'] == 5
    assert result['distinctMatchedSourceRecords'] == 2
    assert result['targetPopulation']['maximumRowsPerKey'] == 2
    assert result['fieldStates'] == {'populated_array':2,'empty_array':1,'sql_null':1}
    before={k:v for k,v in cache.work.items() if not k.endswith('_hits')}
    # A new process-equivalent cache reader reuses admitted member signatures too.
    reused=MeasurementCache(tmp_path/'cache')
    assert reused.measure(index,paths,spec,source_identity=('id',)) == result
    assert reused.work['result_hits'] == 1
    assert reused.work['member_hashes'] == reused.work['member_projections'] == reused.work['occurrences_scans'] == 0
    assert {k:v for k,v in cache.work.items() if not k.endswith('_hits')} == before


def test_changed_guard_invalidates_only_recipe_result_not_target_population(tmp_path):
    index,paths,spec=selection(tmp_path)
    cache=MeasurementCache(tmp_path/'cache')
    cache.measure(index,paths,spec,source_identity=('id',))
    before=cache.work.copy()
    changed=deepcopy(spec)
    changed['ruleVersion']='fixture-guard/2'
    changed['targets'][0]['guards']=[guard('congress',row=True,values=('119',))]
    result=cache.measure(index,paths,changed,source_identity=('id',))
    assert (result['eligible'],result['matched'],result['ambiguous']) == (3,2,0)
    assert cache.work['target_keys_scans'] == before['target_keys_scans']
    assert cache.work['population_aggregations'] == before['population_aggregations']
    assert cache.work['occurrences_scans'] == before['occurrences_scans']+1


def test_multipart_checks_every_member_and_incrementally_reuses_unchanged_member(tmp_path):
    index,paths,spec=selection(tmp_path,split=True)
    cache=MeasurementCache(tmp_path/'cache')
    first=cache.measure(index,paths,spec,source_identity=('id',))
    assert first['sourceRows'] == 4
    before=cache.work.copy()
    changed=deepcopy(index)
    descriptor=changed['families']['fixture']['tables']['source.parquet']
    last=descriptor['members'][1]
    member_key=table_members(index,'source.parquet')[1].path
    physical=Path(paths[member_key])
    table=pq.ParquetFile(physical).read().to_pylist()
    table[0]['refs']=[{'id':'a'}]
    pq.write_table(pa.Table.from_pylist(table,schema=pq.ParquetFile(physical).schema_arrow),physical)
    last.update(sha256='sha256:'+hashlib.sha256(physical.read_bytes()).hexdigest(),byteSize=physical.stat().st_size)
    descriptor['byteSize']=sum(m['byteSize'] for m in descriptor['members'])
    result=cache.measure(changed,paths,spec,source_identity=('id',))
    assert result['eligible'] == first['eligible']+1
    assert cache.work['member_hashes'] == before['member_hashes']+1
    assert cache.work['occurrences_scans'] == before['occurrences_scans']+1
    assert cache.work['target_keys_scans'] == before['target_keys_scans']


def test_partial_member_set_never_reuses_a_complete_result(tmp_path):
    index,paths,spec=selection(tmp_path,split=True)
    cache=MeasurementCache(tmp_path/'cache')
    cache.measure(index,paths,spec,source_identity=('id',))
    incomplete={k:v for k,v in paths.items() if 'bucket=1' not in k}
    with pytest.raises(ValueError,match='Incomplete selected member set'):
        cache.measure(index,incomplete,spec,source_identity=('id',))


@pytest.mark.parametrize('defect',['schema','rows','sum','digest'])
def test_later_member_admission_refuses_schema_footer_and_byte_mismatches(tmp_path,defect):
    index,paths,spec=selection(tmp_path,split=True)
    descriptor=index['families']['fixture']['tables']['source.parquet']
    last=descriptor['members'][-1]
    physical=Path(paths[table_members(index,'source.parquet')[-1].path])
    if defect=='schema':
        table=pq.ParquetFile(physical).read().drop(['refs'])
        pq.write_table(table,physical)
        last.update(sha256='sha256:'+hashlib.sha256(physical.read_bytes()).hexdigest(),byteSize=physical.stat().st_size)
        descriptor['byteSize']=sum(m['byteSize'] for m in descriptor['members'])
    elif defect=='rows':
        last['rows']+=1
        descriptor['rows']+=1
    elif defect=='sum':
        descriptor['rows']+=1
    else:
        last['sha256']='sha256:'+'0'*64
    with pytest.raises((ValueError,RuntimeError),match='schema|footer|counts|SHA|index'):
        MeasurementCache(tmp_path/'cache').measure(index,paths,spec,source_identity=('id',))


@pytest.mark.parametrize('identity',['absent','null','duplicate'])
def test_missing_or_duplicate_source_identity_prevents_distinct_record_claim(tmp_path,identity):
    rows=[{'id':'same','congress':'119','refs':[{'id':'a'}]},
          {'id':None if identity=='null' else 'same','congress':'119','refs':[{'id':'a'}]}]
    index,paths,spec=selection(tmp_path,rows)
    result=MeasurementCache(tmp_path/'cache').measure(index,paths,spec,
             source_identity=('missing',) if identity=='absent' else ('id',))
    assert result['matched'] == 2
    assert result['distinctMatchedSourceRecords'] is None
    assert result['reverse']['maximumDistinctSourceRecordsPerTarget'] is None
    assert not result['sourceIdentity']['qualified']


def test_changed_generation_reuses_member_work_but_rebinds_complete_result(tmp_path):
    index,paths,spec=selection(tmp_path)
    cache=MeasurementCache(tmp_path/'cache')
    old=cache.measure(index,paths,spec,source_identity=('id',))
    before=cache.work.copy()
    changed=deepcopy(index)
    family=changed['families']['fixture']
    family.update(artifactDigest='sha256:'+'b'*64,prefix='generations/fixture/'+'b'*64)
    newpaths={k.replace('/'+'a'*64+'/', '/'+'b'*64+'/'):v for k,v in paths.items()}
    result=cache.measure(changed,newpaths,spec,source_identity=('id',))
    assert result['binding'] != old['binding']
    assert cache.work['occurrences_scans'] == before['occurrences_scans']
    assert cache.work['target_keys_scans'] == before['target_keys_scans']
    assert cache.work['route_measurements'] == before['route_measurements']+1


def test_incomplete_or_corrupt_cache_entry_is_a_miss(tmp_path):
    index,paths,spec=selection(tmp_path)
    cache=MeasurementCache(tmp_path/'cache')
    result=cache.measure(index,paths,spec,source_identity=('id',))
    result_path=cache._path('result',result['binding'])
    payload=json.loads(result_path.read_text())
    payload['payload']['status']='partial'
    result_path.write_text(json.dumps(payload))
    before=cache.work.copy()
    repaired=cache.measure(index,paths,spec,source_identity=('id',))
    assert repaired['status']=='complete'
    assert cache.work['route_measurements'] == before['route_measurements']+1
    assert cache.work['occurrences_scans'] == before['occurrences_scans']


def test_cache_artifact_mutation_refuses_reuse_and_repairs_from_pinned_inputs(tmp_path):
    index,paths,spec=selection(tmp_path)
    cache=MeasurementCache(tmp_path/'cache')
    result=cache.measure(index,paths,spec,source_identity=('id',))
    Path(result['occurrences'][0]['path']).write_bytes(b'broken')
    before=cache.work.copy()
    repaired=cache.measure(index,paths,spec,source_identity=('id',))
    assert repaired['eligible']==result['eligible']
    assert cache.work['occurrences_scans']==before['occurrences_scans']+1


def test_bound_directions_attach_only_to_exact_complete_current_inputs_and_recipe(tmp_path):
    from spicy_regs.navigation_measurements import attached_directions
    index,paths,spec=selection(tmp_path)
    proof=MeasurementCache(tmp_path/'cache').measure(index,paths,spec,source_identity=('id',))
    directions=attached_directions(index,spec,0,proof)
    assert directions['forward']['measurement']['status']=='measured'
    assert directions['forward']['measurement']['targetUniqueness']=='many'
    assert directions['forward']['measurement']['targetUniquenessScope']=='full_nonnull_target_keys'
    assert directions['reverse']['measurement']['maximumReferencesPerTarget']==2
    assert directions['reverse']['measurement']['maximumPhysicalSourceRowsPerTarget']==1
    assert directions['reverse']['measurement']['maximumDistinctSourceRecordsPerTarget']==1
    partial=deepcopy(proof)
    partial['status']='partial'
    assert attached_directions(index,spec,0,partial) is None
    malformed=deepcopy(proof)
    malformed['matched']+=1
    assert attached_directions(index,spec,0,malformed) is None
    for path,value in [(('targetPopulation','scope'),'reference_only'),
                       (('targetPopulation','maximumRowsPerKey'),-1),
                       (('reverse','maximumPhysicalSourceRowsPerTarget'),-1),
                       (('fieldStates','empty_array'),100),
                       (('sourceIdentity','qualified'),'yes')]:
        malformed=deepcopy(proof)
        malformed[path[0]][path[1]]=value
        assert attached_directions(index,spec,0,malformed) is None
    changed=deepcopy(spec)
    changed['ruleVersion']='changed/2'
    assert attached_directions(index,changed,0,proof) is None
    drift=deepcopy(index)
    drift['families']['fixture']['artifactDigest']='sha256:'+'b'*64
    assert attached_directions(drift,spec,0,proof) is None


def test_metadata_does_not_promote_unbound_or_partial_measurements(tmp_path):
    from spicy_regs.explorer_metadata import build_bundle
    index,paths,spec=selection(tmp_path)
    proof=MeasurementCache(tmp_path/'cache').measure(index,paths,spec,source_identity=('id',))
    args: dict={'descriptions':{'source':{'identity_columns':['id']},'target':{'identity_columns':['id']}},
          'registry':{'sources':{}},'join_record':{'joins':[],'navigation':[spec]},'audit':{}}
    bundle=build_bundle(index,measurements=[proof],**args)
    assert bundle['navigation'][0]['targets'][0]['directions']['forward']['measurement']['status']=='measured'
    incomplete=deepcopy(proof)
    incomplete['status']='partial'
    unchanged=build_bundle(index,measurements=[incomplete],**args)
    assert unchanged['navigation'][0]['targets'][0]['directions']['forward']['measurement']['status']=='unknown'


def test_separate_publication_requires_exact_file_identity_and_retains_mutable_pin(tmp_path):
    from spicy_regs.navigation_measurements import attached_directions, digest
    index,paths,spec=selection(tmp_path)
    descriptor=index['families']['fixture']['tables'].pop('target.parquet')
    mainpath=next(k for k in paths if k.endswith('/target.parquet'))
    paths['target.parquet']=paths.pop(mainpath)
    file={'path':'target.parquet',**{k:descriptor[k] for k in ('sha256','rows','byteSize')},'etag':'"selected-version"'}
    identity={'kind':'comments','family':'comments','members':[file]}
    extras: dict={'target':{'family':'comments','descriptor':identity,'publicationSchema':descriptor['columns'],
                      'publicationIdentity':digest(identity)}}
    proof=MeasurementCache(tmp_path/'cache').measure(index,paths,spec,source_identity=('id',),extra_tables=extras)
    assert attached_directions(index,spec,0,proof,extras) is not None
    drift=deepcopy(extras)
    drift['target']['descriptor']['members'][0]['etag']='"new-version"'
    drift['target']['publicationIdentity']=digest(drift['target']['descriptor'])
    assert attached_directions(index,spec,0,proof,drift) is None


def test_scalar_measurement_uses_canonical_main_join_and_keeps_null_keys(tmp_path):
    from spicy_regs.navigation_measurements import scalar_navigation
    index,paths,_=selection(tmp_path)
    spec=scalar_navigation({'child':'source','child_columns':['id'],'parent':'target','parent_columns':['id'],'reason':'Declared key.'})
    result=MeasurementCache(tmp_path/'cache').measure(index,paths,spec,source_identity=('id',))
    assert result['rawReferences']==4
    assert result['eligible']==4
    assert result['matched']==0
    assert result['missing']==4
    assert result['targetPopulation']['nullKeyRows']==1


def test_native_scalar_keys_refuse_text_numeric_coercion(tmp_path):
    from spicy_regs.navigation_measurements import scalar_navigation
    index,paths,_=selection(tmp_path)
    path,descriptor=member(tmp_path,'numeric',[{'id':1}])
    index['families']['fixture']['tables']['target.parquet']=descriptor
    paths[table_members(index,'target.parquet')[0].path]=str(path)
    spec=scalar_navigation({'child':'source','child_columns':['id'],'parent':'target','parent_columns':['id']})
    with pytest.raises(ValueError,match='Native scalar key types'):
        MeasurementCache(tmp_path/'cache').measure(index,paths,spec,source_identity=('id',))


def test_conflicting_measurements_remain_unknown_without_hiding_other_metadata(tmp_path):
    from spicy_regs.explorer_metadata import build_bundle
    index,paths,spec=selection(tmp_path)
    proof=MeasurementCache(tmp_path/'cache').measure(index,paths,spec,source_identity=('id',))
    conflicting=deepcopy(proof)
    conflicting['matched']+=1
    bundle=build_bundle(index,descriptions={},registry={'sources':{}},audit={},
                       join_record={'joins':[],'navigation':[spec]},measurements=[proof,conflicting])
    assert bundle['navigation'][0]['targets'][0]['directions']['forward']['measurement']['status']=='unknown'
    assert set(bundle['tables'])=={'source','target'}


def test_candidate_key_occurrences_keep_each_identity_and_unresolved_candidate(tmp_path):
    index, paths, spec = selection(tmp_path)
    schema = pa.schema([('id', pa.string()), ('refs', pa.list_(pa.struct([
        ('target_status', pa.string()),
        ('candidate_keys', pa.list_(pa.struct([('id', pa.string())]))),
    ])))])
    rows = [{'id': 's1', 'refs': [
        {'target_status': 'found', 'candidate_keys': [{'id': 'a'}, {'id': 'a'}]},
        {'target_status': 'found', 'candidate_keys': [{'id': 'missing'}, None]},
        {'target_status': 'unsupported', 'candidate_keys': []}, None,
    ]}]
    source, descriptor = member(tmp_path, 'source', rows, schema)
    index['families']['fixture']['tables']['source.parquet'] = descriptor
    paths[table_members(index, 'source.parquet')[0].path] = str(source)
    spec['candidates'] = True
    spec['targets'][0]['guards'] = [guard('target_status', values=('found',))]
    cache = MeasurementCache(tmp_path / 'cache')
    result = cache.measure(index, paths, spec, source_identity=('id',))
    assert (result['rawReferences'], result['eligible'], result['matched'], result['missing']) == (6, 3, 2, 1)
    assert result['unsupportedReferences'] == 3
    assert result['repeatedReferences'] == 1
    assert result['distinctMatchedSourceRecords'] == 1
    occurrence_files = list((tmp_path / 'cache').rglob('*.parquet'))
    occurrences = [pq.read_table(path).to_pylist() for path in occurrence_files]
    held = next(rows for rows in occurrences if len(rows) == 6 and 'candidate_ordinal' in rows[0])
    assert [(r['candidate_ordinal'], r['target_key_ordinal']) for r in held] == [(0, 0), (0, 1), (1, 0), (1, 1), (2, None), (3, None)]


def test_native_date_keys_compare_calendar_values_without_text_coercion(tmp_path):
    from datetime import date
    from spicy_regs.navigation_measurements import scalar_navigation
    index, paths, _ = selection(tmp_path)
    for name, values in [('source', [date(2026, 1, 1), date(2026, 1, 2)]), ('target', [date(2026, 1, 1)])]:
        path, descriptor = member(tmp_path, name, [{'day': d} for d in values], pa.schema([('day', pa.date32())]))
        index['families']['fixture']['tables'][name + '.parquet'] = descriptor
        paths[table_members(index, name + '.parquet')[0].path] = str(path)
    spec = scalar_navigation({'child': 'source', 'child_columns': ['day'], 'parent': 'target', 'parent_columns': ['day']})
    cache = MeasurementCache(tmp_path / 'cache')
    measured = cache.measure(index, paths, spec, source_identity=('day',))
    assert (measured['eligible'], measured['matched'], measured['missing']) == (2, 1, 1)
    path, descriptor = member(tmp_path, 'target', [{'day': '2026-01-01'}], pa.schema([('day', pa.string())]))
    index['families']['fixture']['tables']['target.parquet'] = descriptor
    paths[table_members(index, 'target.parquet')[0].path] = str(path)
    with pytest.raises(ValueError, match='types are incompatible'):
        cache.measure(index, paths, spec, source_identity=('day',))
