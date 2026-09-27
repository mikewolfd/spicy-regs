"""Replay retained FCC arrays and correction behavior without source acquisition."""
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.relationship_views.fcc_native import FCC_NATIVE_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.transforms.build_fcc_ecfs import (
    FILING_COLUMNS, _FILING_SCHEMA, _merge_incremental, _shape_filing,
)

FIXTURES = Path(__file__).parent / 'fixtures/fcc_native'


def retained(label):
    return json.loads((FIXTURES / (label+'.json')).read_bytes())['filing'][0]


def test_native_replay_preserves_roles_proceeding_ids_and_artifact_alternatives():
    rows = [_shape_filing(retained(label)) for label in ['positive','alternatives','empty']]
    con: Any = duckdb.connect()
    arrow = pa.Table.from_pylist(rows, schema=_FILING_SCHEMA)
    con.register('fcc_filings', arrow)
    con.execute("CREATE TABLE fcc_proceedings AS SELECT '17-108' AS name,'301759' AS id_proceeding")
    metadata = install_sql_views(con,['fcc_filings','fcc_proceedings'],FCC_NATIVE_VIEWS)
    assert all(v['status']=='available' for v in metadata.values())
    assert con.execute("SELECT count(*) FROM fcc_native_observations WHERE source_field='documents'").fetchone()[0]==5
    assert con.execute("SELECT participant_role,observed_name FROM fcc_native_observations "
                       "WHERE id_submission='04272972619149' AND participant_role IS NOT NULL ORDER BY participant_role").fetchall()==[
        ('authors','Wireline Competition Bureau'),('bureaus','Wireline Competition Bureau'),
        ('filers','Wireline Competition Bureau'),('lawfirms','FCC')]
    assert con.execute("SELECT native_proceeding_id,target_status FROM fcc_native_proceeding_links "
                       "WHERE id_submission='04272972619149'").fetchone()==('301759','found')
    con.execute("INSERT INTO fcc_proceedings VALUES ('17-108','301759')")
    assert con.execute("SELECT target_status FROM fcc_native_proceeding_links "
                       "WHERE id_submission='04272972619149'").fetchone()[0]=='ambiguous'
    assert con.execute("SELECT field_state FROM fcc_native_field_states "
                       "WHERE id_submission='104290235804100' AND source_field='documents'").fetchone()[0]=='empty_array'
    assert all(r['native_fields_sha256']=='sha256:'+hashlib.sha256(r['native_fields_json'].encode()).hexdigest() for r in rows)


def test_native_states_keep_missing_null_unsupported_and_repeated_elements():
    raw={'id_submission':'x','proceedings':[None,3,{'name':'17-108','id_proceeding':'301759'}]*2,
         'filers':None,'authors':42,'documents':[]}
    con: Any=duckdb.connect()
    arrow=pa.Table.from_pylist([_shape_filing(raw)],schema=_FILING_SCHEMA)
    con.register('fcc_filings',arrow)
    install_sql_views(con,['fcc_filings'],FCC_NATIVE_VIEWS)
    states=dict(con.execute('SELECT source_field,field_state FROM fcc_native_field_states').fetchall())
    assert states=={'proceedings':'populated_array','filers':'null','authors':'unsupported_shape',
                   'lawfirms':'absent','bureaus':'absent','documents':'empty_array'}
    assert con.execute('SELECT count(*) FROM fcc_native_observations').fetchone()[0]==6


def test_incremental_fcc_upgrades_legacy_schema_and_fresh_correction_replaces_arrays(tmp_path):
    prior=tmp_path/'prior.parquet'
    old=_shape_filing(retained('positive'))
    oldcols=[c for c in FILING_COLUMNS if not c.startswith('native_fields_')]
    untouched={**old,'id_submission':'untouched'}
    pq.write_table(pa.Table.from_pylist([old,untouched]).select(oldcols),prior)
    corrected=retained('positive')
    corrected['documents']=[]
    corrected['authors']=[]
    new=_shape_filing(corrected)
    out=_merge_incremental(tmp_path,output='fcc_filings.parquet',scratch_prefix='_test',columns=FILING_COLUMNS,
        schema=_FILING_SCHEMA,key='id_submission',order_by='date_received',rows=[new],prior_file=prior,have_prior=True)
    rows=pq.read_table(out).to_pylist()
    assert len(rows)==2
    by_id={row['id_submission']:row for row in rows}
    assert json.loads(by_id[new['id_submission']]['native_fields_json'])['documents']==[]
    assert by_id[new['id_submission']]['documents_json']=='[]'
    assert by_id['untouched']['native_fields_json'] is None
    assert by_id['untouched']['documents_json']==old['documents_json']


def test_refresh_preserves_pdf_diagnostics_only_for_unchanged_offers(tmp_path):
    old = _shape_filing(retained('positive'))
    old['pdf_extraction_results_json'] = '[{"url":"offered","source_sha256":"sha256:retained","status":"ok"}]'
    for changed in (False, True):
        prior = tmp_path / 'prior.parquet'
        pq.write_table(pa.Table.from_pylist([old], schema=_FILING_SCHEMA), prior)
        raw = retained('positive')
        if changed:
            raw['documents'] = []
        out = _merge_incremental(tmp_path, output='fcc_filings.parquet', scratch_prefix='_test',
            columns=FILING_COLUMNS, schema=_FILING_SCHEMA, key='id_submission', order_by='date_received',
            rows=[_shape_filing(raw)], prior_file=prior, have_prior=True)
        result = pq.read_table(out).to_pylist()[0]
        assert result['pdf_extraction_results_json'] == (None if changed else old['pdf_extraction_results_json'])
