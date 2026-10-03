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

from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.pdf_text import PdfTextResult, extract_pdf_text

COLUMNS = (
    'opinion_id', 'cluster_id', 'source_url', 'resolved_url', 'source_sha256',
    'native_sha1', 'actual_sha1', 'sha1_matches', 'text_content',
    'pdf_extraction_results_json', 'observed_at', 'extractor', 'extractor_version',
    'parent_opinion_publication_json',
)
SCHEMA = pa.schema([(column, pa.string()) for column in COLUMNS])
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
    return dict(zip(COLUMNS, (
        identity, cluster, capture.url, capture.resolved_url, digest, native, actual,
        str(matches).lower(), result.text if result and result.ok else None,
        json.dumps([diagnostic]), capture.observed_at, 'spicy_regs.transforms.pdf_text/pypdf',
        version('pypdf'), json.dumps(dict(parent), sort_keys=True),
    ), strict=True))



def merge_extractions(prior: pa.Table, fresh: pa.Table) -> pa.Table:
    """Keep distinct captured bodies; replace only the same opinion/body extraction."""
    rows = {}
    for table in (prior, fresh):
        seen = set()
        for row in table.to_pylist():
            key = (row['opinion_id'], row['source_sha256'])
            if any(value is None for value in key) or key in seen:
                raise ValueError('Missing or duplicate opinion/body identity in one input')
            seen.add(key)
            rows[key] = row
    return pa.Table.from_pylist([rows[key] for key in sorted(rows)], schema=SCHEMA)


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
    table = pa.Table.from_pylist(rows, schema=SCHEMA)
    prior_member = None
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
        prior_table = pq.read_table(prior_path)
        if len(prior_table) > 10000 or prior_table.schema.names != list(COLUMNS):
            raise ValueError('Prior extraction table exceeds row bound or has unsupported schema')
        table = merge_extractions(prior_table, table)
        evidence.event('preserving-extraction-merge', prior_sha256=digest, prior_rows=len(prior_table),
                       fresh_rows=len(rows), output_rows=len(table),
                       identity=['opinion_id', 'source_sha256'],
                       meaning='Different bodies retain separate versions; same opinion/body replaces extraction only.')
    pq.write_table(table, path)
    artifact = build_generation(output / 'generation', family=FAMILY, files=[path],
                                expected_keys=[KEY], read_snapshot=read_snapshot, inputs=evidence.inputs(),
                                parents={'court_opinions.parquet': {
                                    'sha256': parent['tables']['court_opinions.parquet']['sha256'],
                                    'byteSize': parent['tables']['court_opinions.parquet']['byteSize'],
                                    'family': 'court-opinions', 'artifactDigest': parent['artifactDigest'],
                                }})
    verify_generation(output / 'generation', expected_pin=artifact.pin)
    evidence.finish()
    return {'candidate_pin': artifact.pin.as_dict(), 'generation_directory': str(output / 'generation'),
            'evidence_directory': str(evidence.artifact_dir), 'rows': len(table),
            'parent_opinion_generation': parent, 'status': 'verified_local_candidate_not_published'}
