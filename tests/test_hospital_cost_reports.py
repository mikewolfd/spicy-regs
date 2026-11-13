"""Report revisions, native cells, explicit gaps and the ordinary query path."""
from decimal import Decimal
from pathlib import Path
import hashlib
import io
import json
from zipfile import ZipFile

import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.hospital_cost_reports import HospitalCostReportArchive
from spicy_regs import mcp_server
from spicy_regs.pipelines.hospital_cost_reports import build
from spicy_regs.sources import publication
from spicy_regs.transforms.build_hospital_cost_reports import build_hospital_cost_reports
from tests.generation_fakes import Store


def original(*, orphan=False, duplicate=False):
    out = io.BytesIO()
    with ZipFile(out, 'w') as z:
        z.writestr('hosp10_2010_RPT.csv',
            '1,2,010001,,1,05/01/2010,12/31/2010,01/01/2012,Y,N,1,00001,,01/01/2012,F,,,01/01/2012\n'
            '2,2,010001,,4,05/01/2010,12/31/2010,02/01/2012,N,N,1,00001,,02/01/2012,F,,,02/01/2012\n')
        numeric = '1,S300001,01400,00200,0\n1,C000001,20000,00300,100.25\n1,C000001,20000,00800,\n2,S300001,01400,00200,9\n'
        if orphan:
            numeric += '3,S300001,01400,00200,1\n'
        if duplicate:
            numeric += '1,S300001,01400,00200,1\n'
        z.writestr('hosp10_2010_NMRC.csv', numeric)
        z.writestr('hosp10_2010_alpha.csv', '1,S200001,00200,00100,"HOSPITAL, A"\n')
    return out.getvalue()


def reader(body):
    return HospitalCostReportArchive(io.BytesIO(body), sha256='sha256:'+hashlib.sha256(body).hexdigest(),
                                     byte_size=len(body), form='CMS-2552-10', year=2010)


def test_reports_revision_periods_and_three_missing_states(tmp_path):
    files, counts = build_hospital_cost_reports(reader(original()), tmp_path/'tables')
    assert counts == {'hospital_cost_reports': 2, 'hospital_cost_report_cells': 5, 'hospital_cost_report_measures': 8}
    reports = pq.read_table(files[0]).to_pylist()
    assert [r['report_status_raw'] for r in reports] == ['1','4']
    assert all(r['period_status'] == 'short' and r['ccn'] == '010001' for r in reports)
    assert json.loads(reports[0]['raw_report_json'])['npi'] == ''
    measures = {r['measure']:r for r in pq.read_table(files[2]).to_pylist() if r['report_id']=='1'}
    assert measures['hospital_beds']['value'] == 0
    assert measures['total_costs']['value'] == Decimal('100.25')
    assert measures['total_charges']['value_raw'] == '' and measures['total_charges']['value_status'] == 'blank'
    assert measures['net_patient_revenue']['value_raw'] is None
    assert measures['net_patient_revenue']['value_status'] == 'absent_cell'
    assert pq.read_table(files[1]).to_pylist()[-1]['value_raw'] == 'HOSPITAL, A'


@pytest.mark.parametrize('change', ['orphan', 'duplicate', 'measure'])
def test_ambiguous_changed_or_unrelated_input_refuses(tmp_path, change):
    with pytest.raises(ValueError):
        build_hospital_cost_reports(reader(original(orphan=change=='orphan', duplicate=change=='duplicate')),
                                    tmp_path/'tables', measures=('unsupported',) if change=='measure' else ('hospital_beds',))


def test_generation_publication_and_ordinary_query(tmp_path, monkeypatch):
    body = original()
    source=tmp_path/'original.zip'
    source.write_bytes(body)
    selection={'original':str(source),'sha256':'sha256:'+hashlib.sha256(body).hexdigest(),'byte_size':len(body),
               'form':'CMS-2552-10','year':2010,'source_url':'https://downloads.cms.gov/Files/hcris/HOSP10FY2010.zip',
               'measures':['hospital_beds','total_costs','total_charges','net_patient_revenue']}
    monkeypatch.setenv('SPICYGOV_RAW_ROOT',str(tmp_path/'raw'))
    result=build(selection,tmp_path/'build')
    index=publication.publish_generation(tmp_path/'build/generation',client=Store(),bucket='fixture',
        prior_index=publication.empty_index(),evidence_directories=(Path(result['evidence']),))
    assert index['families']['hospital-cost-reports']['artifactDigest']==result['pin']['artifactDigest']
    monkeypatch.setattr(mcp_server,'DATA_DIR',tmp_path/'build/generation')
    with mcp_server._build_connection() as con:
        rows=con.execute('SELECT ccn,report_id,value FROM hospital_cost_report_measures '
                         "WHERE measure='hospital_beds' ORDER BY report_id").fetchall()
    assert rows==[('010001','1',Decimal('0')),('010001','2',Decimal('9'))]


def test_maintained_local_cli_admits_hospital_and_requires_evidence(tmp_path, monkeypatch):
    from spicy_regs.local_publication import main

    body = original()
    source = tmp_path / 'original.zip'
    source.write_bytes(body)
    monkeypatch.setenv('SPICYGOV_RAW_ROOT', str(tmp_path / 'raw'))
    selection = {
        'original': str(source), 'sha256': 'sha256:' + hashlib.sha256(body).hexdigest(),
        'byte_size': len(body), 'form': 'CMS-2552-10', 'year': 2010,
        'source_url': 'https://downloads.cms.gov/Files/hcris/HOSP10FY2010.zip',
        'measures': ['hospital_beds', 'total_costs', 'total_charges', 'net_patient_revenue'],
    }
    result = build(selection, tmp_path / 'build')
    prior = tmp_path / 'private-empty-index.json'
    prior.write_text(json.dumps(publication.empty_index()))
    output = tmp_path / 'cli-receipt'
    args = [
        '--generation', str(tmp_path / 'build/generation'), '--family', 'hospital-cost-reports',
        '--dry-run', '--scratch-raw', '--live-index', str(prior), '--output', str(output),
    ]
    with pytest.raises(publication.PublicationError, match='source evidence it pins'):
        main(args)
    assert not output.exists()

    main([*args, '--evidence', result['evidence']])
    receipt = json.loads((output / 'dry-run.json').read_text())
    assert receipt['status'] == 'dry_run_accepted'
    assert receipt['artifactDigest'] == result['pin']['artifactDigest']
    assert receipt['tables']['hospital_cost_report_cells.parquet']['rows'] == 5
    assert receipt['tables']['hospital_cost_report_measures.parquet']['rows'] == 8
    assert receipt['other_family_changes'] == []
