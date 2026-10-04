import hashlib
import json
from types import SimpleNamespace

import pytest

from spicy_regs.transforms.build_court_pdf_extractions import shape_captured_opinion
from spicy_regs.transforms.pdf_text import PdfTextResult, PdfTextStatus


def case():
    body = b'%PDF-retained-test'
    capture = SimpleNamespace(body=body, url='https://court.example/opinion.pdf',
                              resolved_url='https://court.example/opinion.pdf', observed_at='2026-09-27T00:00:00Z')
    opinion = dict(opinion_id='1', cluster_id='2', download_url=capture.url,
                   sha1=hashlib.sha1(body).hexdigest())
    return opinion, capture


def test_matching_fingerprint_preserves_typed_ids_and_derived_diagnostics():
    opinion, capture = case()
    row = shape_captured_opinion(opinion, capture, parent={'artifactDigest':'pin'},
        extract=lambda _: PdfTextResult(PdfTextStatus.OK, 'retained words', 2))
    assert (row['opinion_id'], row['cluster_id'], row['text_content']) == ('1', '2', 'retained words')
    assert row['native_sha1'] == row['actual_sha1']
    assert json.loads(row['pdf_extraction_results_json'])[0]['page_count'] == 2


def test_changed_bytes_never_supply_text_or_run_extractor():
    opinion, capture = case()
    capture.body += b'changed'
    def reject(_):
        raise AssertionError('Must not extract a different native body')
    row = shape_captured_opinion(opinion, capture, parent={}, extract=reject)
    assert row['sha1_matches'] == 'false' and row['text_content'] is None
    assert json.loads(row['pdf_extraction_results_json'])[0]['status'] == 'native_digest_mismatch'


def test_wrong_offered_url_refuses():
    opinion, capture = case()
    capture.url += '?different'
    with pytest.raises(ValueError, match='literal offered'):
        shape_captured_opinion(opinion, capture, parent={})


def test_repeated_cohort_refuses_before_writing(tmp_path):
    from spicy_regs.transforms.build_court_pdf_extractions import prepare_captured_opinions
    opinion, capture = case()
    output = tmp_path / 'candidate'
    with pytest.raises(ValueError, match='Repeated opinion'):
        prepare_captured_opinions(output, [(opinion,capture)]*2,
            read_snapshot={'families':{'court-opinions':{}}}, public_url='https://unused.example')
    assert not output.exists()


def test_preserving_merge_idempotence_body_versions_and_duplicate_refusal():
    import pyarrow as pa
    from spicy_regs.transforms.build_court_pdf_extractions import RAW_SCHEMA as SCHEMA, merge_extractions
    opinion, capture = case()
    row = shape_captured_opinion(opinion, capture, parent={},
        extract=lambda _: PdfTextResult(PdfTextStatus.OK, 'old', 1))
    prior = pa.Table.from_pylist([row], schema=SCHEMA)
    assert merge_extractions(prior, prior).equals(prior)
    corrected = pa.Table.from_pylist([{**row, 'text_content':'corrected'}], schema=SCHEMA)
    assert merge_extractions(prior, corrected).to_pylist()[0]['text_content'] == 'corrected'
    new_body = pa.Table.from_pylist([{**row, 'source_sha256':'sha256:different'}], schema=SCHEMA)
    assert len(merge_extractions(prior, new_body)) == 2
    with pytest.raises(ValueError, match='duplicate'):
        merge_extractions(prior, pa.concat_tables([prior,prior]))
    assert prior.to_pylist()[0]['text_content'] == 'old'
