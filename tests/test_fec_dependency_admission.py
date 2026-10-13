"""Small retained dependency admission is distinct from complete generation admission."""
from collections import Counter
from copy import deepcopy
from dataclasses import replace
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.fec_native_conversion import admit_filing_dependency_selection
from spicy_regs.transforms.assemble_fec_query import FilingAssociationInputs,TypedInput,_sha
from spicy_regs.transforms.fec_filing_associations import ASSOCIATION_SCHEMA,_base,associate_filing_numbers
from spicy_regs.transforms.fec_identity_receipts import dataset_policy
from spicy_regs.transforms.fec_query import RECEIPT_SCHEMA
from tests.test_fec_filing_associations import filing,headers,financial,GEN,EVIDENCE,NAMESPACE,pin


def selected(tmp_path, *, outcomes=None, filings=None, scope='self', target=None):
    filing_rows=filings if filings is not None else [filing()]
    native_schema=dataset_policy('fec_filings').subject_schema
    filing_rows=[{**dict.fromkeys(native_schema.names),**row} for row in filing_rows]
    header_rows=[]
    base=headers(filings=filing_rows)[0]
    for number,status in enumerate(outcomes or ['resolved_native_filing_key']):
        row={**deepcopy(base),'target_record_id':'synthetic-header-'+str(number),'association_status':status}
        if status!='resolved_native_filing_key':row.update(filing_key=None,filing_observation_count=0,filing_observation_ids=[])
        raw={**row,'record_id':row['target_record_id']}
        row['record_id']=_base(raw,row['target_table'],GEN,row['selection_evidence_sha256'],row['association_basis'])['record_id']
        header_rows.append(row)
    f=tmp_path/'filings.parquet';h=tmp_path/'headers.parquet'
    pq.write_table(pa.Table.from_pylist(filing_rows,schema=native_schema),f)
    pq.write_table(pa.Table.from_pylist(header_rows,schema=ASSOCIATION_SCHEMA),h,row_group_size=512)
    selection=FilingAssociationInputs(GEN,target,[TypedInput(f,_sha(f),len(filing_rows))],{NAMESPACE:EVIDENCE},[TypedInput(h,_sha(h),len(header_rows))],filing_generation_scope=scope)
    return selection,filing_rows,header_rows


def test_complete_selected_header_population_preserves_all_recorded_refusals(tmp_path):
    counts={'resolved_native_filing_key':65,'unresolved_no_native_url_witness':10,
            'unresolved_source_authority':597,'unresolved_archive_member_identity':685}
    outcomes=[name for name,count in counts.items() for _ in range(count)]
    selection,filings,original=selected(tmp_path,outcomes=outcomes)
    events=[]
    result=admit_filing_dependency_selection(selection,on_read=lambda stage,item,**values:events.append((stage,str(item.path),values)))
    assert result['filings']==filings and result['headers']==original
    assert result['proof']['header_statuses']==counts
    assert result['proof']['targetBinding']=='provisional-self'
    assert result['proof']['filing_generation_pin'] is None
    assert all(result['proof'][name] is False for name in ('fullParentAdmitted','fullTargetCorrespondence','generationAdmitted','namespaceDefinitionSemanticsAdmitted'))
    assert [event[0] for event in events]==['hash_pre','footer_open','decode','hash_post']*2
    for stage,path,values in events:
        if stage=='decode':
            meta=values['metadata']
            assert values['byte_size']==sum(meta.row_group(i).column(j).total_uncompressed_size for i in range(meta.num_row_groups) for j in range(meta.row_group(i).num_columns))
    assert next(values['metadata'].num_row_groups for stage,path,values in events if stage=='decode' and path.endswith('headers.parquet'))==3


def test_shared_prepare_reads_each_population_once_and_keeps_number_refusals(tmp_path,monkeypatch):
    selection,filings,_=selected(tmp_path);calls=[];native=pq.ParquetFile
    class Source:
        def __init__(self,path):self.inner=native(path);self.path=str(path)
        def __enter__(self):return self
        def __exit__(self,*args):self.inner.close()
        def __getattr__(self,name):return getattr(self.inner,name)
        def read(self,**kwargs):calls.append(self.path);return self.inner.read(**kwargs)
    monkeypatch.setattr(pq,'ParquetFile',Source)
    apply,proof=selection.prepare('fec_receipts',RECEIPT_SCHEMA,lambda:None)
    assert calls==[str(selection.filings[0].path),str(selection.header_associations[0].path)]
    decisions=associate_filing_numbers([financial(),financial(record_id='refused',source_authority='unknown')],table='fec_receipts',filings=filings,source_generation_pin=GEN,namespace_evidence={NAMESPACE:EVIDENCE})
    assert [row['association_status'] for row in decisions]==['resolved_native_filing_key','unresolved_source_authority']
    assert decisions[1]['filing_key'] is None and proof['filing_generation_scope']=='self'


@pytest.mark.parametrize('scope,target',[('self',pin('invented-target')),('external',None),('invented',None)])
def test_invalid_target_binding_refuses_before_any_input_read(tmp_path,scope,target):
    selection,_,_=selected(tmp_path,scope=scope,target=target)
    with pytest.raises(ValueError):
        admit_filing_dependency_selection(selection,on_read=lambda *a,**kw:pytest.fail('Invalid binding must precede input read'))


def test_declared_external_target_never_claims_generation_admission(tmp_path):
    selection,_,_=selected(tmp_path,scope='external',target=pin('declared-target'))
    result=admit_filing_dependency_selection(selection)
    assert result['proof']['targetBinding']=='declared-external-unadmitted'
    assert result['proof']['generationAdmitted'] is False and result['proof']['fullTargetCorrespondence'] is False


@pytest.mark.parametrize('change', ['identity','source','policy','witness_count','witness_ids','unresolved_key'])
def test_exact_current_header_identity_and_witness_refusals(tmp_path,change):
    selection,_,rows=selected(tmp_path)
    row=rows[0]
    updates={'identity':{'record_id':pin('foreign')},'source':{'source_generation_pin':pin('foreign')},
             'policy':{'policy_version':'old-policy'},'witness_count':{'filing_observation_count':2},
             'witness_ids':{'filing_observation_ids':['missing-target']},
             'unresolved_key':{'association_status':'unresolved_source_authority'}}[change]
    row.update(updates)
    path=selection.header_associations[0].path;pq.write_table(pa.Table.from_pylist(rows,schema=ASSOCIATION_SCHEMA),path)
    selection=replace(selection,header_associations=[TypedInput(path,_sha(path),1)])
    with pytest.raises(ValueError):admit_filing_dependency_selection(selection)


@pytest.mark.parametrize('kind',['filing','header'])
def test_complete_native_schema_refused_before_corresponding_decoded_read(tmp_path,kind):
    selection,_,_=selected(tmp_path)
    item=selection.filings[0] if kind=='filing' else selection.header_associations[0]
    pq.write_table(pa.table({'unexpected':['source']}),item.path)
    replacement=TypedInput(item.path,_sha(item.path),1)
    selection=replace(selection,**({'filings':[replacement]} if kind=='filing' else {'header_associations':[replacement]}))
    decodes=[]
    with pytest.raises(ValueError,match='exact native schema'):
        admit_filing_dependency_selection(selection,on_read=lambda stage,current,**values:decodes.append(str(current.path)) if stage=='decode' else None)
    assert str(item.path) not in decodes


def test_precharged_complete_decode_refusal_stops_before_read(tmp_path):
    selection,_,_=selected(tmp_path)
    def refuse(stage,item,**values):
        if stage=='decode':raise RuntimeError('Synthetic decoder budget stop')
    with pytest.raises(RuntimeError,match='Synthetic decoder budget stop'):
        admit_filing_dependency_selection(selection,on_read=refuse)


def test_digest_and_duplicate_selection_refuse(tmp_path):
    selection,_,_=selected(tmp_path)
    with pytest.raises(ValueError,match='selected bytes'):
        admit_filing_dependency_selection(replace(selection,filings=[replace(selection.filings[0],sha256=pin('wrong'))]))
    with pytest.raises(ValueError,match='selected twice'):
        admit_filing_dependency_selection(replace(selection,filings=list(selection.filings)*2))
