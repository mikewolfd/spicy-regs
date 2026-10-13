"""Complete population admission and incremental invalidation, using tiny main files."""
from copy import deepcopy
import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.explorer_navigation import array, route, key, part, guard
from spicy_regs.navigation_measurements import MeasurementCache, attached_directions, qualify_exact_content
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


def carried(tmp_path, split=False):
    index, paths, spec = selection(tmp_path, split=split)
    proof = MeasurementCache(tmp_path / 'cache').measure(index, paths, spec, source_identity=('id',))
    current = deepcopy(index)
    family = current['families']['fixture']
    family.update(prefix='generations/fixture/' + 'b' * 64, artifactDigest='sha256:' + 'b' * 64)
    return current, spec, proof


def test_exact_content_reuse_preserves_historical_binding(tmp_path):
    current, spec, proof = carried(tmp_path)
    before = deepcopy(proof)
    assert attached_directions(current, spec, 0, proof) is None
    qualified = qualify_exact_content(current, spec, 0, proof)
    assert proof == before == qualified['historicalProof']
    directions = attached_directions(current, spec, 0, qualified)
    assert directions['forward']['measurement']['matched'] == proof['matched']
    assert directions['forward']['measurement']['qualification'] == 'exact_content_reuse'
    qualified['binding']['source']['pin']['sha256'] = 'sha256:' + 'c' * 64
    assert attached_directions(current, spec, 0, qualified) is None


@pytest.mark.parametrize('mutation', ['hash', 'size', 'rows', 'type', 'order', 'missing', 'duplicate', 'boolean'])
def test_content_change_or_incomplete_member_set_refuses(tmp_path, mutation):
    current, spec, proof = carried(tmp_path, split=True)
    descriptor = current['families']['fixture']['tables']['source.parquet']
    if mutation == 'hash':
        descriptor['members'][0]['sha256'] = 'sha256:' + 'c' * 64
    elif mutation == 'size':
        descriptor['members'][0]['byteSize'] += 1
        descriptor['byteSize'] += 1
    elif mutation == 'rows':
        descriptor['members'][0]['rows'] += 1
        descriptor['rows'] += 1
    elif mutation == 'type':
        descriptor['columns'][0][1] = 'BIGINT'
    elif mutation == 'order':
        descriptor['members'].reverse()
    elif mutation == 'missing':
        descriptor['members'].pop()
    elif mutation == 'duplicate':
        descriptor['members'][1] = deepcopy(descriptor['members'][0])
    else:
        descriptor['members'][0]['rows'] = True
    with pytest.raises((ValueError, KeyError, TypeError)):
        qualify_exact_content(current, spec, 0, proof)


@pytest.mark.parametrize('mutation', ['recipe', 'aggregation', 'counts', 'import', 'route', 'guard'])
def test_historical_rules_and_counts_remain_strict(tmp_path, mutation):
    current, spec, proof = carried(tmp_path)
    if mutation in {'recipe', 'aggregation'}:
        proof['binding'][mutation] = 'sha256:' + 'c' * 64
    elif mutation == 'counts':
        proof['matched'] += 1
    elif mutation == 'import':
        proof['targetPopulation']['populationProof'] = {'authority': 'bad', 'guardImplementation': 'bad'}
    elif mutation == 'route':
        proof['route']['targetIndex'] = 1
    else:
        spec['ruleVersion'] = 'new-rule'
    with pytest.raises(ValueError):
        qualify_exact_content(current, spec, 0, proof)


@pytest.fixture
def reviewed_history(tmp_path, monkeypatch):
    """Execute the exact old measure body on tiny inputs, not a relabelled result."""
    import importlib.util
    import inspect
    import sys
    from spicy_regs import navigation_measurements as measurements

    current = inspect.getsource(MeasurementCache.measure)
    new = """                                               target['table']: target['schema']},
                                               generation_pins=navigation.selected_generation_pins(index))[0]
        # The key scan and recipe digest must use the same bound guards that
        # selected metadata and admission use, including exact FEC pins.
        original = deepcopy(spec)"""
    old = """                                               target['table']: target['schema']})[0]
        original['field'] = spec['field']"""
    assert new in current
    previous = current.replace(new, old, 1)
    assert measurements.digest([measurements.VERSION, measurements.duckdb.__version__, previous]) == \
        measurements._REVIEWED_AGGREGATION[0]
    path = tmp_path / 'historical_measure_fixture.py'
    path.write_text('from spicy_regs.navigation_measurements import *\n\nclass HistoricalCache(MeasurementCache):\n'
                    + previous)
    loader = importlib.util.spec_from_file_location('historical_measure_fixture', path)
    assert loader is not None and loader.loader is not None
    module = importlib.util.module_from_spec(loader)
    monkeypatch.setitem(sys.modules, loader.name, module)
    loader.loader.exec_module(module)
    index, paths, spec = selection(tmp_path, split=True)
    proof = module.HistoricalCache(tmp_path / 'old-cache').measure(index, paths, spec, source_identity=('id',))
    assert proof['binding']['aggregation'] == measurements._REVIEWED_AGGREGATION[0]
    return index, spec, proof


def test_reviewed_aggregation_reuses_old_execution_without_mutating_or_relabelling_it(reviewed_history):
    from spicy_regs.navigation_measurements import qualify_compatible_aggregation, digest
    index, spec, proof = reviewed_history
    before = deepcopy(proof)
    assert attached_directions(index, spec, 0, proof) is None
    with pytest.raises(ValueError):
        qualify_exact_content(index, spec, 0, proof)  # The ordinary guard stays strict.
    qualified = qualify_compatible_aggregation(index, spec, 0, proof)
    assert proof == before == qualified['historicalProof']
    assert qualified['historicalDigest'] == digest(before)
    directions = attached_directions(index, spec, 0, qualified)
    assert directions['forward']['measurement']['matched'] == proof['matched']
    assert directions['reverse']['measurement']['matchedTargetRows'] == proof['reverse']['matchedTargetRows']
    assert directions['forward']['measurement']['qualification'] == 'reviewed_aggregation_reuse'


@pytest.mark.parametrize('mutation', ['hash', 'size', 'rows', 'type', 'order', 'missing', 'duplicate'])
def test_reviewed_aggregation_refuses_changed_ordered_content(reviewed_history, mutation):
    from spicy_regs.navigation_measurements import qualify_compatible_aggregation
    index, spec, proof = reviewed_history
    changed = deepcopy(index)
    descriptor = changed['families']['fixture']['tables']['source.parquet']
    if mutation == 'hash':
        descriptor['members'][0]['sha256'] = 'sha256:' + 'c' * 64
    elif mutation == 'size':
        descriptor['members'][0]['byteSize'] += 1
    elif mutation == 'rows':
        descriptor['members'][0]['rows'] += 1
        descriptor['rows'] += 1
    elif mutation == 'type':
        descriptor['columns'][0][1] = 'BIGINT'
    elif mutation == 'order':
        descriptor['members'].reverse()
    elif mutation == 'missing':
        descriptor['members'].pop()
    else:
        descriptor['members'][1] = deepcopy(descriptor['members'][0])
    with pytest.raises((ValueError, KeyError)):
        qualify_compatible_aggregation(changed, spec, 0, proof)


@pytest.mark.parametrize('mutation', ['hash', 'size', 'rows', 'type'])
def test_reviewed_aggregation_also_refuses_changed_target_content(reviewed_history, mutation):
    from spicy_regs.navigation_measurements import qualify_compatible_aggregation
    index, spec, proof = reviewed_history
    changed = deepcopy(index)
    descriptor = changed['families']['fixture']['tables']['target.parquet']
    if mutation == 'hash':
        descriptor['sha256'] = 'sha256:' + 'c' * 64
    elif mutation == 'size':
        descriptor['byteSize'] += 1
    elif mutation == 'rows':
        descriptor['rows'] += 1
    else:
        descriptor['columns'][0][1] = 'BIGINT'
    with pytest.raises(ValueError):
        qualify_compatible_aggregation(changed, spec, 0, proof)


@pytest.mark.parametrize('mutation', ['recipe', 'guard', 'identity', 'counts', 'import', 'route', 'aggregation'])
def test_reviewed_aggregation_keeps_recipe_count_import_and_route_checks(reviewed_history, mutation):
    from spicy_regs.navigation_measurements import qualify_compatible_aggregation
    index, spec, proof = deepcopy(reviewed_history)
    if mutation == 'recipe':
        spec['ruleVersion'] = 'unreviewed'
    elif mutation == 'guard':
        spec['targets'][0]['guards'] = [guard('congress', row=True, values=('119',))]
    elif mutation == 'identity':
        proof['sourceIdentity']['columns'] = ['congress']
    elif mutation == 'counts':
        proof['matched'] += 1
    elif mutation == 'import':
        proof['targetPopulation']['populationProof'] = {'authority': 'bad', 'guardImplementation': 'bad'}
    elif mutation == 'route':
        proof['route']['targetIndex'] = 1
    else:
        proof['binding']['aggregation'] = 'sha256:' + 'c' * 64
    with pytest.raises(ValueError):
        qualify_compatible_aggregation(index, spec, 0, proof)


@pytest.mark.parametrize('mutation', ['duckdb', 'pyarrow', 'key', 'measure'])
def test_reviewed_pair_fails_closed_after_runtime_key_or_measure_changes(reviewed_history, monkeypatch, mutation):
    from spicy_regs import navigation_measurements as measurements
    index, spec, proof = reviewed_history
    if mutation == 'duckdb':
        monkeypatch.setattr(measurements.duckdb, '__version__', 'future-runtime')
    elif mutation == 'pyarrow':
        monkeypatch.setattr(measurements.pa, '__version__', 'future-runtime')
    elif mutation == 'key':
        monkeypatch.setattr(measurements, 'implementation_digest', lambda: 'sha256:' + 'c' * 64)
    else:
        original = measurements.inspect.getsource
        monkeypatch.setattr(measurements.inspect, 'getsource',
                            lambda f: original(f) + ('\n# changed' if f is MeasurementCache.measure else ''))
    with pytest.raises(ValueError, match='implementation pair'):
        measurements.qualify_compatible_aggregation(index, spec, 0, proof)


@pytest.mark.parametrize('mutation', ['binding', 'historical', 'digest', 'implementation', 'compatibility'])
def test_attacher_recreates_the_complete_reviewed_wrapper(reviewed_history, mutation):
    from spicy_regs.navigation_measurements import qualify_compatible_aggregation
    index, spec, proof = reviewed_history
    qualified = qualify_compatible_aggregation(index, spec, 0, proof)
    if mutation == 'binding':
        qualified['binding']['aggregation'] = proof['binding']['aggregation']
    elif mutation == 'historical':
        qualified['historicalProof']['matched'] += 1
    elif mutation == 'digest':
        qualified['historicalDigest'] = 'sha256:' + 'c' * 64
    elif mutation == 'implementation':
        qualified['qualificationImplementation'] = 'sha256:' + 'c' * 64
    else:
        qualified['compatibility'] = 'unreviewed'
    assert attached_directions(index, spec, 0, qualified) is None


def test_existing_reviewed_wrapper_refuses_count_guard_implementation_drift(reviewed_history, monkeypatch):
    from spicy_regs import navigation_measurements as measurements
    index, spec, proof = reviewed_history
    qualified = measurements.qualify_compatible_aggregation(index, spec, 0, proof)
    original = measurements.count_value
    monkeypatch.setattr(measurements, 'count_value', lambda value: original(value))
    assert measurements.attached_directions(index, spec, 0, qualified) is None


@pytest.mark.parametrize('malformed', [None, [], 'not-a-measurement'])
def test_attacher_refuses_malformed_historical_objects_without_crashing(reviewed_history, malformed):
    from spicy_regs.navigation_measurements import qualify_compatible_aggregation
    index, spec, proof = reviewed_history
    qualified = qualify_compatible_aggregation(index, spec, 0, proof)
    qualified['historicalProof'] = malformed
    assert attached_directions(index, spec, 0, qualified) is None
    qualified['historicalProof'] = {**proof, 'binding': malformed}
    assert attached_directions(index, spec, 0, qualified) is None


@pytest.mark.parametrize('mutation', ['target', 'source', 'import_list', 'import_string', 'import_empty', 'import_null'])
def test_reviewed_reuse_cleanly_refuses_malformed_population_and_import_objects(reviewed_history, mutation):
    from spicy_regs.navigation_measurements import qualify_compatible_aggregation
    index, spec, proof = reviewed_history
    qualified = qualify_compatible_aggregation(index, spec, 0, proof)
    historical = qualified['historicalProof']
    if mutation == 'target':
        historical['targetPopulation'] = []
    elif mutation == 'source':
        historical['sourceIdentity']['population'] = []
    else:
        historical['targetPopulation']['populationProof'] = {
            'import_list': ['bad'], 'import_string': 'bad', 'import_empty': {}, 'import_null': None,
        }[mutation]
    assert attached_directions(index, spec, 0, qualified) is None
    with pytest.raises(ValueError, match='counts or imported evidence'):
        qualify_compatible_aggregation(index, spec, 0, historical)


def test_reviewed_reuse_preserves_valid_earlier_exact_content_wrapper(reviewed_history):
    import inspect
    from spicy_regs import navigation_measurements as measurements
    index, spec, historical = reviewed_history
    source = measurements.selected_binding(index, spec['source'])
    target = measurements.selected_binding(index, spec['targets'][0]['table'])
    carried = {'format': measurements.VERSION, 'status': 'complete', 'authority': 'exact_content_reuse',
               'binding': {**historical['binding'], 'source': source, 'target': target},
               'route': deepcopy(historical['route']), 'historicalProof': deepcopy(historical),
               'historicalDigest': measurements.digest(historical),
               'qualificationImplementation': measurements.digest([measurements.VERSION,
                   inspect.getsource(measurements._content_identity),
                   inspect.getsource(measurements.qualify_exact_content)])}
    before = deepcopy(carried)
    qualified = measurements.qualify_compatible_aggregation(index, spec, 0, carried)
    assert carried == before == qualified['historicalProof']
    assert measurements.attached_directions(index, spec, 0, qualified) is not None
    carried['historicalDigest'] = 'sha256:' + 'c' * 64
    with pytest.raises(ValueError, match='qualification changed'):
        measurements.qualify_compatible_aggregation(index, spec, 0, carried)
