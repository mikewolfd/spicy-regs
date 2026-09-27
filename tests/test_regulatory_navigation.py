"""Docket candidates do not establish identity; date evidence uses its own namespace."""
import json
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.relationship_views.lifecycle_dates import LIFECYCLE_DATE_VIEWS
from spicy_regs.relationship_views.sql_views import install_sql_views
from spicy_regs.transforms.build_fr_docket_links import build_fr_docket_links


def test_lifecycle_uses_dated_by_not_stage_source():
    con: Any=duckdb.connect()
    con.execute("CREATE TABLE lifecycle_events AS SELECT 'p' AS proceeding_id,'94-190@1994-01-03' AS document_id,"
                "'regulations_gov' AS source,'federal_register' AS dated_by,'1994-01-03' AS event_date")
    con.execute("CREATE TABLE documents AS SELECT '94-190@1994-01-03' AS document_id")
    con.execute("CREATE TABLE federal_register AS SELECT '94-190' AS document_number,'1994-01-03' AS publication_date")
    con.execute("CREATE TABLE regulatory_agenda_items(agenda_item_id VARCHAR)")
    install_sql_views(con,['lifecycle_events','documents','federal_register','regulatory_agenda_items'],LIFECYCLE_DATE_VIEWS)
    assert con.execute('SELECT target_table,target_status FROM lifecycle_date_evidence').fetchone()==('federal_register','found')
    con.execute('DELETE FROM federal_register')
    assert con.execute('SELECT target_status FROM lifecycle_date_evidence').fetchone()[0]=='missing'


def test_docket_builder_preserves_raw_references_and_ordinal(tmp_path):
    fields=['document_number','title','abstract','document_type','subtype','publication_date','effective_on',
            'comments_close_on','signing_date','agency_slugs','docket_ids_json','regulation_id_numbers_json',
            'html_url','pdf_url','executive_order_number']
    row=dict.fromkeys(fields)
    row.update(document_number='2026-17334',publication_date='2026-08-25',
               docket_ids_json=json.dumps(['Docket ID OPM-2025-0001','File No. SR-Amex-2003-102',
                                          'Docket ID OPM-2025-0001']))
    pq.write_table(pa.Table.from_pylist([row],schema=pa.schema([(f,pa.string()) for f in fields])),
                   tmp_path/'federal_register.parquet')
    rows=pq.read_table(build_fr_docket_links(tmp_path)).to_pylist()
    assert len(rows)==3
    assert sorted(r['docket_source_ordinal'] for r in rows)==[0,1,2]
    assert next(json.loads(r['normalized_docket_candidates_json']) for r in rows if r['docket_source_ordinal']==1)==[]
    assert all(r['docket_normalization_rule'].startswith('spicy_docs.normalize_docket_references@sha256:') for r in rows)


def test_retained_fr_candidates_and_document_number_collision():
    from pathlib import Path
    from spicy_docs.interpretation.identifier_shapes import (
        normalize_docket_references, unpadded_federal_register_document_number,
    )
    page=json.loads((Path(__file__).parent/'fixtures/regulatory_rins/federal-register-response.json').read_bytes())
    record=next(row for row in page['results'] if row['document_number']=='2026-17334')
    references=record['docket_ids']
    assert len(references)==2
    assert [normalize_docket_references(value) for value in references]==[('OPM-2025-0004',),('OPM-2023-0027',)]
    assert unpadded_federal_register_document_number('94-0190')=='94-190'
    assert unpadded_federal_register_document_number('94-190')=='94-190'
