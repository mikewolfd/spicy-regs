"""Replay retained FCC arrays and correction behavior without source acquisition."""
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS, map_subject, GovernmentShapeError

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
    arrow = pa.Table.from_pylist([map_subject("fcc_filings", row) for row in rows], schema=SUBJECT_SCHEMAS["fcc_filings"])
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
    assert con.execute("SELECT documents FROM fcc_filings WHERE id_submission='104290235804100'").fetchone()[0] == []
    assert "fcc_native_field_states" not in metadata
    assert all(r['native_fields_sha256']=='sha256:'+hashlib.sha256(r['native_fields_json'].encode()).hexdigest() for r in rows)


@pytest.mark.parametrize(
    "targets,expected_count,expected_status",
    [
        pytest.param([("17-108", "301759"), ("87-432", "301759")], 1, "found", id="reused-numeric-id"),
        pytest.param([("87-432", "301759")], 0, "missing", id="wrong-name"),
        pytest.param([("17-108", "999999")], 0, "missing", id="wrong-id-synthetic-control"),
        pytest.param(
            [("17-108", "301759"), ("87-432", "301759"), ("17-108", "301759")],
            2, "ambiguous", id="duplicate-complete-key",
        ),
        pytest.param([], 0, "missing", id="no-held-target"),
    ],
)
def test_native_proceeding_membership_requires_both_name_and_id(targets, expected_count, expected_status):
    """Keep a literal retained filing membership separate from target-ID reuse.

    positive.json names 17-108 / 301759. Independent published-data evidence in
    mcp-chaos-2026-10-02/audit-dev/q01-proceeding-identities.json also has 87-432 /
    301759. Neither key alone establishes membership. The wrong-ID and duplicate
    target populations are synthetic controls, not additional publisher claims.
    """
    source = retained('positive')
    subject = map_subject('fcc_filings', _shape_filing(source))
    with duckdb.connect() as con:
        con.register('fcc_filings', pa.Table.from_pylist([subject], schema=SUBJECT_SCHEMAS['fcc_filings']))
        con.execute("CREATE TABLE fcc_proceedings (name VARCHAR, id_proceeding VARCHAR)")
        if targets:
            con.executemany("INSERT INTO fcc_proceedings VALUES (?, ?)", targets)
        install_sql_views(con, ['fcc_filings', 'fcc_proceedings'], FCC_NATIVE_VIEWS)
        assert con.execute(
            "SELECT id_submission, source_ordinal, observed_name, native_proceeding_id, target_count, target_status "
            "FROM fcc_native_proceeding_links"
        ).fetchall() == [('04272972619149', 0, '17-108', '301759', expected_count, expected_status)]


def test_native_states_keep_null_and_repeated_elements_and_refuse_unsupported():
    raw = {'id_submission': 'x', 'proceedings': [None, {'name': '17-108', 'id_proceeding': '301759'}]*2,
           'filers': None, 'authors': [], 'documents': []}
    subject = map_subject('fcc_filings', _shape_filing(raw))
    con = duckdb.connect()
    con.register('fcc_filings', pa.Table.from_pylist([subject], schema=SUBJECT_SCHEMAS['fcc_filings']))
    install_sql_views(con, ['fcc_filings'], FCC_NATIVE_VIEWS)
    assert con.execute('SELECT source_ordinal, observed_name FROM fcc_native_observations ORDER BY source_ordinal').fetchall() == [
        (0, None), (1, '17-108'), (2, None), (3, '17-108')]
    assert subject['filers'] is None and subject['authors'] == [] and subject['lawfirms'] is None
    with pytest.raises(GovernmentShapeError):
        map_subject('fcc_filings', _shape_filing({**raw, 'authors': 42}))


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
