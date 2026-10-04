"""Court receipts bind native rows, retries and internal processing reads."""
import hashlib
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.court_receipts import (
    POLICIES, build_court_generation, local_receipt_selection, read_court_rows, write_court_rows,
)
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.etl_receipts import exact_json, validate_receipt_bundle
from spicy_regs.generations import verify_generation
from spicy_regs.transforms.build_court_opinion_clusters import held_export
from spicy_regs.transforms.build_court_pdf_extractions import RAW_SCHEMA, merge_extractions

WITNESS = {'source_id': 'retained-source', 'source_uri': None,
           'sha256': 'sha256:' + 'a' * 64, 'locator': None, 'body_version': None}


def write(tmp_path, dataset, rows, generation='chosen-build'):
    return write_court_rows(dataset, rows, tmp_path, witnesses=[WITNESS, WITNESS], generation_id=generation)


def test_lists_and_processing_evidence_roundtrip_and_invalid_row_is_receipt_only(tmp_path):
    good = {'cl_docket_id': '1', 'parties_json': '["Agency",null,"","Agency"]',
            'attorneys_json': None, 'firms_json': '[]', 'absolute_url': 'https://example.test/docket/1'}
    bad = {'cl_docket_id': '2', 'parties_json': '[invalid'}
    path = write(tmp_path, 'court_dockets', [good, bad])
    assert pq.read_table(path).to_pylist()[0]['parties'] == ['Agency', None, '', 'Agency']
    assert 'absolute_url' not in pq.read_schema(path).names
    receipt_path, generation = local_receipt_selection(path)
    receipts = pq.read_table(receipt_path).to_pylist()
    assert [r['outcome'] for r in receipts] == ['accepted', 'refused']
    assert receipts[0]['witnesses'][0] == receipts[0]['witnesses'][1]
    assert receipts[0]['witnesses'] == [WITNESS, WITNESS]
    assert receipts[1]['processing_json'] == exact_json({'raw_source_record': bad})
    restored, = read_court_rows(path, dataset='court_dockets')
    assert restored['parties_json'] == good['parties_json']
    assert restored['absolute_url'] == good['absolute_url']
    validate_receipt_bundle({'court_dockets': [path]}, [receipt_path], [POLICIES['court_dockets']], generation_id=generation)


def test_partial_stream_and_duplicate_identity_preserve_the_complete_prior_pair(tmp_path):
    path = write(tmp_path, 'court_citation_map', [{'citing_opinion_id': '1', 'cited_opinion_id': '2', 'depth': '3'}])
    previous = path.resolve()
    previous_bytes = path.read_bytes()
    def interrupted():
        yield {'citing_opinion_id': '1', 'cited_opinion_id': '2', 'depth': '4'}
        raise RuntimeError('source stopped midway')
    with pytest.raises(RuntimeError, match='midway'):
        write(tmp_path, 'court_citation_map', interrupted())
    assert path.resolve() == previous and path.read_bytes() == previous_bytes
    failure_files = list((tmp_path / '.court-etl').glob('failed-*/etl_receipts.parquet'))
    assert len(failure_files) == 1
    assert pq.read_table(failure_files[0]).to_pylist()[0]['outcome'] == 'error'
    with pytest.raises(ValueError, match='Duplicate|ambiguous'):
        write(tmp_path, 'court_citation_map', [{'citing_opinion_id': '1', 'cited_opinion_id': '2'}] * 2)
    assert path.resolve() == previous


def test_cluster_incremental_watermark_uses_selected_receipts(tmp_path):
    path = write(tmp_path, 'court_opinion_clusters', [
        {'cluster_id': '10', 'date_created': '2026-06-30T08:00:00Z', 'ingest_source': 'bulk'},
        {'cluster_id': '20', 'date_created': '2026-07-01T08:00:00Z', 'ingest_source': 'search'},
    ])
    assert 'ingest_source' not in pq.read_schema(path).names
    assert held_export(path) == (10, '2026-06-30')
    receipt, _ = local_receipt_selection(path)
    with pytest.raises(ValueError, match='generation'):
        held_export(path, receipt_path=receipt, generation_id='wrong-build')
    copied = tmp_path / 'unbound.parquet'
    copied.write_bytes(path.read_bytes())
    with pytest.raises(ValueError, match='no selected receipt'):
        list(read_court_rows(copied, dataset='court_opinion_clusters'))


def _pdf(body=b'pdf', text='Retained opinion', status='ok'):
    return {'opinion_id': '1', 'cluster_id': '2', 'source_sha256': 'sha256:' + hashlib.sha256(body).hexdigest(),
            'sha1_matches': 'true', 'text_content': text,
            'pdf_extraction_results_json': json.dumps([{'status': status}])}


def test_missing_body_has_only_receipt_and_failed_retry_preserves_good_text(tmp_path):
    good = _pdf()
    missing = _pdf(text=None, status='empty')
    merged = merge_extractions(pa.Table.from_pylist([good], schema=RAW_SCHEMA),
                              pa.Table.from_pylist([missing], schema=RAW_SCHEMA))
    path = write(tmp_path, 'court_opinion_pdf_extractions', merged.to_pylist())
    subject, = pq.read_table(path).to_pylist()
    assert subject['text_content'] == 'Retained opinion'
    receipts = pq.read_table(local_receipt_selection(path)[0]).to_pylist()
    assert [r['outcome'] for r in receipts] == ['accepted', 'refused']
    failed_path = write(tmp_path / 'no-prior', 'court_opinion_pdf_extractions', [missing])
    assert pq.ParquetFile(failed_path).metadata.num_rows == 0


def test_distinct_pdf_bodies_keep_distinct_business_keys_and_valid_receipt_generation(tmp_path):
    path = write(tmp_path, 'court_opinion_pdf_extractions', [_pdf(b'a'), _pdf(b'b')])
    rows = pq.read_table(path).to_pylist()
    assert len({r['opinion_body_id'] for r in rows}) == 2
    assert pq.read_schema(path) == SUBJECT_SCHEMAS['court_opinion_pdf_extractions']
    artifact = build_court_generation(tmp_path / 'generation', family='court-opinion-pdf-extractions', files=[path])
    verified = verify_generation(tmp_path / 'generation', expected_pin=artifact.pin)
    assert verified.pin == artifact.pin
    assert artifact.root['spec']['etlReceipts']['generationId'] == 'chosen-build'


def test_legacy_processing_inputs_require_explicit_import(tmp_path):
    path = tmp_path / 'old.parquet'
    pq.write_table(pa.Table.from_pylist([{'cluster_id': '1', 'ingest_source': 'bulk'}]), path)
    with pytest.raises(ValueError, match='explicit migration'):
        list(read_court_rows(path, dataset='court_opinion_clusters'))
    assert list(read_court_rows(path, dataset='court_opinion_clusters', allow_legacy=True))[0]['ingest_source'] == 'bulk'


def test_installed_court_policies_refuse_generation_without_receipts(tmp_path):
    from spicy_regs.etl_policy_registry import installed_policies
    from spicy_regs.generations import build_generation

    installed = installed_policies()
    for name, policy in POLICIES.items():
        assert installed[name].descriptor() == policy.descriptor()
    path = write(tmp_path, 'court_opinion_clusters', [{'cluster_id': '1'}])
    with pytest.raises(ValueError, match='require ETL receipts'):
        build_generation(tmp_path / 'bypass', family='court-opinion-clusters',
                         files=[path], expected_keys=[path.name])
