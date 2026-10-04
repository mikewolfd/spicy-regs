"""Build bounded derived opinion text from captured, literally offered PDF URLs."""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from importlib.metadata import version
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.generations import verify_generation
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.court_receipts import (
    _pdf_failure, build_court_generation, read_court_rows, write_court_rows,
)
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.pdf_text import PdfTextResult, extract_pdf_text

INPUT_COLUMNS = (
    'opinion_id', 'cluster_id', 'source_url', 'resolved_url', 'source_sha256',
    'native_sha1', 'actual_sha1', 'sha1_matches', 'text_content',
    'pdf_extraction_results_json', 'observed_at', 'extractor', 'extractor_version',
    'parent_opinion_publication_json',
)
RAW_SCHEMA = pa.schema([(column, pa.string()) for column in INPUT_COLUMNS])
SCHEMA = SUBJECT_SCHEMAS['court_opinion_pdf_extractions']
COLUMNS = tuple(SCHEMA.names)
FAMILY = 'court-opinion-pdf-extractions'
KEY = 'court_opinion_pdf_extractions.parquet'


def shape_captured_opinion(
    opinion: Mapping[str, Any], capture: Any, *, parent: Mapping[str, Any],
    extract: Callable[[bytes], PdfTextResult] = extract_pdf_text,
) -> dict:
    """A mismatched native digest records refusal and never supplies opinion text."""
    identity = str(opinion.get('opinion_id', ''))
    cluster = str(opinion.get('cluster_id', ''))
    if not identity.isdecimal() or int(identity) <= 0 or not cluster.isdecimal() or int(cluster) <= 0:
        raise ValueError('Expected positive native opinion and cluster IDs')
    if capture.url != opinion.get('download_url'):
        raise ValueError('Capture is not the literal offered opinion URL')
    native = opinion.get('sha1')
    if not isinstance(native, str) or len(native) != 40 or any(c not in '0123456789abcdef' for c in native):
        raise ValueError('Native opinion SHA-1 is absent or unsupported')
    actual = hashlib.sha1(capture.body).hexdigest()
    digest = 'sha256:' + hashlib.sha256(capture.body).hexdigest()
    matches = actual == native
    result = extract(capture.body) if matches else None
    diagnostic = {
        'url': capture.url, 'source_sha256': digest,
        'status': result.status.value if result else 'native_digest_mismatch',
        'page_count': result.page_count if result else None,
        'error': result.error if result else 'Captured bytes differ from held native SHA-1',
    }
    return dict(zip(INPUT_COLUMNS, (
        identity, cluster, capture.url, capture.resolved_url, digest, native, actual,
        str(matches).lower(), result.text if result and result.ok else None,
        json.dumps([diagnostic]), capture.observed_at, 'spicy_regs.transforms.pdf_text/pypdf',
        version('pypdf'), json.dumps(dict(parent), sort_keys=True),
    ), strict=True))



def merge_extractions(prior: pa.Table, fresh: pa.Table) -> pa.Table:
    """Keep distinct captured bodies; replace only the same opinion/body extraction."""
    rows = {}
    failures = []
    for table in (prior, fresh):
        seen = set()
        for row in table.to_pylist():
            key = (row['opinion_id'], row['source_sha256'])
            if any(value is None for value in key) or key in seen:
                raise ValueError('Missing or duplicate opinion/body identity in one input')
            seen.add(key)
            if _pdf_failure(row):
                failures.append(row)
            else:
                rows[key] = row
    return pa.Table.from_pylist([rows[key] for key in sorted(rows)] + failures, schema=RAW_SCHEMA)


def prepare_captured_opinions(
    output: Path, items: Sequence[tuple[Mapping[str, Any], Any]], *,
    read_snapshot: dict, public_url: str, max_records: int = 10,
    max_capture_bytes: int = 16 * 1024 * 1024,
) -> dict:
    """Seal an explicit bounded cohort using existing generation/evidence APIs.

    New body digests remain separate versions. A repeated opinion/body pair
    replaces only its extraction, retaining all other captured versions and opinions.
    The caller acquires through the supported provider and passes retained captures.
    """
    if not items or len(items) > max_records:
        raise ValueError('Opinion extraction cohort exceeds its explicit record bound')
    parent = read_snapshot['families']['court-opinions']
    ids = [str(item[0].get('opinion_id')) for item in items]
    if len(set(ids)) != len(ids):
        raise ValueError('Repeated opinion IDs require explicit version selection')
    if any(len(capture.body) > max_capture_bytes for _, capture in items):
        raise ValueError('Opinion capture exceeds its explicit byte bound')
    output.mkdir(parents=True, exist_ok=False)
    evidence = CaptureEvidence(output, FAMILY)
    evidence.inherit(read_snapshot, public_url=public_url)
    rows = []
    for opinion, capture in items:
        row = shape_captured_opinion(opinion, capture, parent=parent)
        evidence.retain_bytes(capture.body, stage='opinion-pdf-retained-input',
                              requested_url=capture.url, resolved_url=capture.resolved_url,
                              observed_at=capture.observed_at, media_type=capture.media_type)
        evidence.event('opinion-pdf-capture', opinion=dict(opinion),
                       requested_url=capture.url, resolved_url=capture.resolved_url,
                       observed_at=capture.observed_at, media_type=capture.media_type,
                       sha256=row['source_sha256'], byte_size=len(capture.body),
                       parent_opinion_generation=parent, extraction=row)
        rows.append(row)
    path = output / KEY
    table = pa.Table.from_pylist(rows, schema=RAW_SCHEMA)
    prior_member = None
    receipt_path = None
    if FAMILY in read_snapshot['families']:
        from spicy_regs.sources import publication, r2
        prior_member = publication.single_member(read_snapshot, KEY)
        if prior_member.byte_size is None or prior_member.byte_size > 256 * 1024 * 1024:
            raise ValueError('Prior extraction table exceeds its explicit byte bound')
        prior_path = output / 'prior.parquet'
        r2.get_r2_client().download_file(os.getenv('R2_BUCKET', 'spicy-regs'), prior_member.path, str(prior_path))
        with prior_path.open('rb') as stream:
            digest = 'sha256:' + hashlib.file_digest(stream, 'sha256').hexdigest()
        if prior_path.stat().st_size != prior_member.byte_size or digest != prior_member.sha256:
            raise ValueError('Prior extraction bytes differ from selected publication')
        if pq.ParquetFile(prior_path).metadata.num_rows > 10000:
            raise ValueError('Prior extraction table exceeds row bound')
        receipt_path = generation_id = None
        owner = read_snapshot['families'][FAMILY]
        if 'etlReceipts' in owner:
            member, = publication.receipt_members(read_snapshot, dataset='court_opinion_pdf_extractions')
            receipt_path = output / 'prior-etl-receipts.parquet'
            if not publication.fetch_member(public_url, member, receipt_path):
                raise ValueError('Prior extraction receipts are missing')
            generation_id = owner['etlReceipts']['generationId']
        prior_rows = read_court_rows(prior_path, dataset='court_opinion_pdf_extractions',
            receipt_path=receipt_path, generation_id=generation_id)
        prior_table = pa.Table.from_pylist(list(prior_rows), schema=RAW_SCHEMA)
        table = merge_extractions(prior_table, table)
        evidence.event('preserving-extraction-merge', prior_sha256=digest, prior_rows=len(prior_table),
                       fresh_rows=len(rows), output_rows=len(table),
                       identity=['opinion_id', 'source_sha256'],
                       meaning='Different bodies retain separate versions; same opinion/body replaces extraction only.')
    witnesses = [{'source_id': str(opinion['opinion_id']), 'source_uri': capture.url,
                  'sha256': 'sha256:' + hashlib.sha256(capture.body).hexdigest(), 'locator': None,
                  'body_version': None} for opinion, capture in items]
    if prior_member is not None:
        witnesses.append({'source_id': KEY, 'source_uri': prior_member.path,
                          'sha256': prior_member.sha256, 'locator': None,
                          'body_version': read_snapshot['families'][FAMILY]['artifactDigest']})
    path = write_court_rows('court_opinion_pdf_extractions', table.to_pylist(), output, witnesses=witnesses, prior_receipts=receipt_path)
    artifact = build_court_generation(output / 'generation', family=FAMILY, files=[path],
                                read_snapshot=read_snapshot, inputs=evidence.inputs(),
                                parents={'court_opinions.parquet': {
                                    'sha256': parent['tables']['court_opinions.parquet']['sha256'],
                                    'byteSize': parent['tables']['court_opinions.parquet']['byteSize'],
                                    'family': 'court-opinions', 'artifactDigest': parent['artifactDigest'],
                                }})
    verify_generation(output / 'generation', expected_pin=artifact.pin)
    evidence.finish()
    return {'candidate_pin': artifact.pin.as_dict(), 'generation_directory': str(output / 'generation'),
            'evidence_directory': str(evidence.artifact_dir), 'rows': pq.ParquetFile(path).metadata.num_rows,
            'parent_opinion_generation': parent, 'status': 'verified_local_candidate_not_published'}
