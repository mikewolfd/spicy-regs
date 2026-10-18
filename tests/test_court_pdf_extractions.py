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


def test_pdf_failure_survives_unrelated_refresh_until_fresh_matching_success(tmp_path, monkeypatch):
    import shutil
    from typing import Any
    from unittest.mock import Mock

    import pyarrow.parquet as pq

    from spicy_regs.court_receipts import local_receipt_selection
    from spicy_regs.sources import publication, r2
    from spicy_regs.transforms import build_court_pdf_extractions as module

    # Exercise the actual preparation, merge, mapping and receipt carry. Replace
    # only capture/parent qualification, transport and generation packaging.
    evidence = Mock()
    evidence.inputs.return_value = []
    monkeypatch.setattr(module, 'CaptureEvidence', lambda *_: evidence)
    monkeypatch.setattr(module, 'qualify_parent_opinions', lambda *_: None)
    monkeypatch.setattr(module, 'build_court_generation', lambda *a, **k: SimpleNamespace(
        pin=SimpleNamespace(as_dict=lambda: {})))
    monkeypatch.setattr(module, 'verify_generation', lambda *a, **k: None)
    original_shape = module.shape_captured_opinion
    extraction = PdfTextResult(PdfTextStatus.OK, 'held text', 1)
    monkeypatch.setattr(module, 'shape_captured_opinion', lambda opinion, capture, *, parent: original_shape(
        opinion, capture, parent=parent, extract=lambda _: extraction))
    locations = {}
    monkeypatch.setattr(r2, 'get_r2_client', lambda: SimpleNamespace(
        download_file=lambda bucket, key, target: shutil.copyfile(locations[key], target)))
    monkeypatch.setattr(publication, 'fetch_member', lambda base, member, target:
                        bool(shutil.copyfile(locations[member.path], target)))
    snapshot: dict[str, Any] = {'families': {'court-opinions': {'artifactDigest': 'sha256:' + 'a' * 64,
        'tables': {'court_opinions.parquet': {'sha256': 'sha256:' + 'b' * 64, 'byteSize': 1}}}}}
    opinion, capture = case()
    capture.media_type = 'application/pdf'

    def run(name, fresh_opinion):
        output = tmp_path / name
        module.prepare_captured_opinions(output, [(fresh_opinion, capture)],
                                        read_snapshot=snapshot, public_url='https://fixture.invalid')
        subject = (output / module.KEY).resolve()
        receipt, generation = local_receipt_selection(subject)
        prefix = 'generations/' + module.FAMILY + '/' + name
        locations[prefix + '/' + module.KEY] = subject
        locations[prefix + '/etl_receipts.parquet'] = receipt

        def descriptor(path):
            return {'sha256': 'sha256:' + hashlib.sha256(path.read_bytes()).hexdigest(),
                    'byteSize': path.stat().st_size, 'rows': pq.ParquetFile(path).metadata.num_rows}

        snapshot['families'][module.FAMILY] = {
            'artifactDigest': 'sha256:' + hashlib.sha256(name.encode()).hexdigest(), 'prefix': prefix,
            'tables': {module.KEY: descriptor(subject)},
            'etlReceipts': {**descriptor(receipt), 'key': 'etl_receipts.parquet',
                            'datasets': ['court_opinion_pdf_extractions'], 'generationId': generation},
        }
        return subject, receipt, pq.read_table(receipt).to_pylist()

    first, first_receipt, accepted = run('first', opinion)
    original = {p: p.read_bytes() for p in (first, first_receipt)}
    extraction = PdfTextResult(PdfTextStatus.ERROR, '', 0, error='failed newer extraction')
    second, _, second_receipts = run('failed', opinion)
    refused = [r for r in second_receipts if r['outcome'] == 'refused']
    assert len(refused) == 1  # The fresh admission file must not append the failure a second time.
    assert pq.read_table(second).equals(pq.read_table(first))
    assert [r for r in second_receipts if r['outcome'] == 'accepted'] == accepted
    extraction = PdfTextResult(PdfTextStatus.OK, 'held text', 1)
    third, _, third_receipts = run('unrelated', {**opinion, 'opinion_id': '3'})
    assert pq.ParquetFile(third).metadata.num_rows == 2
    assert [r for r in third_receipts if r['outcome'] == 'refused'] == refused
    fourth, _, fourth_receipts = run('resolved', opinion)
    assert pq.ParquetFile(fourth).metadata.num_rows == 2
    assert all(r['outcome'] == 'accepted' for r in fourth_receipts)
    assert all(p.read_bytes() == contents for p, contents in original.items())
