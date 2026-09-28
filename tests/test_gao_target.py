"""Explicit target retries stay bounded and retained successes bypass acquisition."""
from typing import Any
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.gao.files import gao_report_index_locator
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs.transforms.build_gao_reports import _SCHEMA
from spicy_regs.transforms.build_gao_target import append_missing_target,target_index
from spicy_regs.citation_resolution import resolve_citations
import duckdb

PRODUCT='gao-17-317'
BODY=b'<title>High-Risk Series</title><a href="https://files.gao.gov/assets/gao-17-317.pdf">PDF</a><a href="https://www.gao.gov/products/gao-17-317">Product</a>'


def retained():
    url=gao_report_index_locator(PRODUCT)
    return CapturedBodyResponse(url,url,200,'text/html','2026-09-27T00:00:00Z',BODY)


def test_refusal_explicit_retry_and_retained_replay():
    calls=[]
    def refused(request):
        calls.append(request.url)
        return httpx.Response(403,stream=httpx.ByteStream(b'AccessDenied'))
    with pytest.raises(CredentialRefusedError):
        target_index(PRODUCT,transport=httpx.MockTransport(refused))
    assert len(calls)==1
    def success(request):
        calls.append(request.url)
        return httpx.Response(200,headers={'content-type':'text/html'},stream=httpx.ByteStream(BODY))
    index,capture=target_index(PRODUCT,transport=httpx.MockTransport(success))
    assert len(calls)==2 and index.product_id==PRODUCT
    again,_=target_index(PRODUCT,retained=capture,transport=httpx.MockTransport(refused))
    assert again==index and len(calls)==2


def test_bad_retained_identity_refuses_without_retry():
    bad=retained()
    bad=CapturedBodyResponse(bad.requested_url,bad.resolved_url,200,'text/html',bad.observed_at,b'<title>Other</title>')
    def forbidden(request):
        raise AssertionError('Retained input must not trigger acquisition')
    with pytest.raises(ValueError):
        target_index(PRODUCT,retained=bad,transport=httpx.MockTransport(forbidden))


def test_candidate_preserves_all_prior_cells_leaves_date_unknown_and_resolves(tmp_path):
    prior=tmp_path/'prior.parquet'
    out=tmp_path/'candidate.parquet'
    row=dict.fromkeys(_SCHEMA.names)
    row.update(report_id='gao-26-1',title='Existing',published_date='2026-01-02',topics_json='[]')
    pq.write_table(pa.Table.from_pylist([row],schema=_SCHEMA),prior)
    index,_=target_index(PRODUCT,retained=retained())
    report=append_missing_target(prior,out,index)
    assert report['prior_cells_equal'] and report['candidate_rows']==2
    added=pq.read_table(out).to_pylist()[1]
    assert added['published_date'] is None and added['topics_json'] is None
    c: Any=duckdb.connect()
    c.read_parquet(str(out)).create_view('gao_reports')
    result=resolve_citations(c,[{'cite_kind':'gao_product_id','target_key':'GAO-17-317'}],
                             {'gao_reports':{'artifact_digest':'synthetic-unit-test-pin'}})
    assert result['occurrences'][0]['target_status']=='found'
    with pytest.raises(ValueError,match='already exists'):
        append_missing_target(out,tmp_path/'overwrite.parquet',index)


def test_complete_generation_candidate_uses_prior_pin_without_publication(tmp_path,monkeypatch):
    from spicy_regs.generations import build_generation,verify_generation
    from spicy_regs.sources import publication
    from spicy_regs.transforms.build_gao_target import prepare_target_generation
    from tests.generation_fakes import Store
    import json
    prior=tmp_path/'gao_reports.parquet'
    row=dict.fromkeys(_SCHEMA.names)
    row.update(report_id='gao-26-1',title='Existing')
    pq.write_table(pa.Table.from_pylist([row],schema=_SCHEMA),prior)
    directory=tmp_path/'prior-generation'
    build_generation(directory,family='gao-reports',files=[prior],expected_keys=[prior.name])
    store=Store()
    snapshot=publication.publish_generation(directory,client=store,bucket='test',prior_index=publication.empty_index())
    raw=(directory/'artifact.json').read_bytes()
    monkeypatch.setattr(publication,'load_family_root',lambda *_:(raw,json.loads(raw)))
    report=prepare_target_generation(tmp_path/'target',product_id=PRODUCT,prior_file=prior,
        prior_index=snapshot,public_url='https://test.invalid',retained_index=retained())
    artifact=verify_generation(tmp_path/'target/generation')
    assert report['status']=='verified_candidate_not_published'
    assert artifact.root['spec']['parents']['gao_reports.parquet']['artifactDigest']==snapshot['families']['gao-reports']['artifactDigest']
    assert report['candidate_rows']==2


def test_retained_product_page_qualifies_exact_heading_and_explicit_day():
    from pathlib import Path
    from spicy_docs.sources.gao.product_metadata import product_page_metadata
    raw=(Path(__file__).parent/'fixtures/gao_target/product-page.zip').read_bytes()
    meta=product_page_metadata(raw,PRODUCT)
    assert meta.title=='High-Risk Series: Progress on Many High-Risk Areas, While Substantial Efforts Needed on Others'
    assert meta.published_date=='2017-02-15'
    with pytest.raises(ValueError,match='different product'):
        product_page_metadata(raw,'gao-17-999')


def test_month_only_heading_block_does_not_infer_day():
    from pathlib import Path
    from zipfile import ZipFile
    from spicy_docs.sources.gao.native import iter_gao_product_pages
    from spicy_docs.sources.zyte import ZyteHttpResponse
    from spicy_docs.sources.gao.product_metadata import product_page_metadata
    with ZipFile(Path(__file__).parent/'fixtures/gao_target/product-page.zip') as archive:
        body=archive.read('product.html').replace(b'Published: Feb 15, 2017.',b'Published: February 2017.')
    url='https://www.gao.gov/products/'+PRODUCT
    pages=list(iter_gao_product_pages(lambda _:ZyteHttpResponse(url,url,200,'text/html',body),
                                     query_scope={'productIds':[PRODUCT]}))
    assert product_page_metadata(pages[0].response_bytes,PRODUCT).published_date is None
