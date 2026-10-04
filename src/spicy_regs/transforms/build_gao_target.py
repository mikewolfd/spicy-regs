"""Explicit GAO target repair from one qualified index, without RSS discovery.

The current report table stores metadata only. PDF bytes and offered locators
belong in generation evidence until a separate body-reference schema is adopted.
"""
from __future__ import annotations

import json
import hashlib
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.build_gao_reports import COLUMNS, SOURCE_REPAIR
from spicy_regs.transforms.government_receipts import internal_prior, receipt_builder, generation_receipt_args
from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS
from spicy_regs.native_types import described_schema

if TYPE_CHECKING:
    import httpx
    from spicy_docs.transport.captured import CapturedBodyResponse
    from spicy_regs.source_evidence import CaptureEvidence

MAX_INDEX_BYTES = 2 * 1024 * 1024



def target_index(product_id: str, *, retained: CapturedBodyResponse | None = None,
                 transport: httpx.BaseTransport | None = None, evidence: CaptureEvidence | None = None):
    """Replay retained input first; otherwise one bounded explicit request, with no implicit retry.

    A caller may retry a refused selection explicitly. Successful retained input
    bypasses transport completely; a damaged retained capture refuses rather than
    silently reacquiring. The provider controls what refusal evidence may be retained.
    """
    from spicy_docs.sources.gao.files import (
        GaoReportFileAcquirer, GaoReportFileBudget, GaoReportFileSourceError,
        gao_report_index_locator, parse_gao_report_index,
    )
    if evidence:
        evidence.event('selection', stage='gao-explicit-target', product_id=product_id,
                       max_requests=0 if retained else 1, max_bytes=MAX_INDEX_BYTES, timeout_seconds=35)
    try:
        if retained is not None:
            locator = gao_report_index_locator(product_id)
            if (retained.status_code != 200 or retained.requested_url != locator or retained.resolved_url != locator
                    or (retained.content_type or '').split(';')[0].strip().lower() != 'text/html'):
                raise GaoReportFileSourceError('Retained GAO index capture differs from the selected source route')
            index = parse_gao_report_index(retained.body, product_id=product_id, max_bytes=MAX_INDEX_BYTES)
            capture = retained
        else:
            budget = GaoReportFileBudget(max_requests=1,max_index_bytes=MAX_INDEX_BYTES,
                max_pdf_bytes=16*1024*1024,timeout_seconds=35,min_request_interval_seconds=1)
            with GaoReportFileAcquirer(budget=budget,transport=transport) as reader:
                index,capture = reader.capture_report_index(product_id)
        if not index.title:
            raise GaoReportFileSourceError('Selected GAO index does not state a title')
        if evidence:
            evidence.capture(capture,stage='gao-explicit-target-index')
        return index,capture
    except Exception as error:
        if evidence:
            evidence.refusal(error,stage='gao-explicit-target-index')
        raise


@receipt_builder(output_argument="output", dataset="gao_reports")
def append_missing_target(prior: Path, output: Path, index: Any) -> dict:
    """Preserve every prior cell, adding one source-identified report with unknown fields null."""
    prior = internal_prior("gao_reports", prior)
    table = pq.read_table(prior)
    if not set(COLUMNS) <= set(table.column_names):
        raise ValueError('Prior GAO table lacks required metadata columns')
    if index.product_id in table['report_id'].to_pylist():
        raise ValueError('Target already exists; this missing-target path never overwrites it')
    if not index.title:
        raise ValueError('A source-stated GAO title is required')
    row = dict.fromkeys(table.column_names)
    row.update(report_id=index.product_id,title=index.title,url=index.product_url,
               published_date=getattr(index,'published_date',None))
    if 'source' in row:
        row['source']=SOURCE_REPAIR
    # An online report index supplies no day; a qualified product page may.
    # Neither supplies a report type, abstract or structured tags here.
    # Unknown remains NULL, rather than an invented date or known-empty list.
    addition = pa.Table.from_pylist([row],schema=table.schema)
    combined = pa.concat_tables([table,addition])
    output.parent.mkdir(parents=True,exist_ok=True)
    staged = output.with_suffix('.partial.parquet')
    pq.write_table(combined,staged,compression='zstd')
    actual = pq.read_table(staged)
    if not actual.slice(0,len(table)).equals(table,check_metadata=True):
        raise ValueError('GAO candidate changed prior cells')
    staged.replace(output)
    return {'prior_rows':len(table),'candidate_rows':len(actual),'prior_cells_equal':True,
            'added_report_id':index.product_id,'published_date':row['published_date'],'table_body_storage':'unsupported'}


@receipt_builder
def _target_tables(output_dir: Path, *, prior_file: Path, index, siblings: tuple[Path, ...],
                   prior_receipts: Path | None, prior_generation_id: str | None, report: dict, evidence) -> tuple[Path, ...]:
    """Rebuild the full selected family while retaining sibling processing evidence."""
    import shutil
    output_dir.mkdir(parents=True, exist_ok=True)
    prior = internal_prior("gao_reports", prior_file, receipt_path=prior_receipts, generation_id=prior_generation_id)
    output = output_dir / "gao_reports.parquet"
    report.update(append_missing_target(prior, output, index))
    paths = [output]
    for sibling in siblings:
        literal = internal_prior(sibling.stem, sibling, receipt_path=prior_receipts, generation_id=prior_generation_id)
        target = output_dir / sibling.name
        shutil.copyfile(literal, target)
        paths.append(target)
    return tuple(paths)


def prepare_target_generation(output_dir: Path, *, product_id: str, prior_file: Path,
                              prior_index: dict, public_url: str, prior_decisions_file: Path | None = None,
                              retained_index: CapturedBodyResponse | None = None,
                              retained_pdf: CapturedBodyResponse | None = None,
                              retained_product_page: bytes | None = None,
                              transport: httpx.BaseTransport | None = None) -> dict:
    """Build and verify a whole-family local candidate; never publish it."""
    from spicy_regs.generations import build_generation, verify_generation
    from spicy_regs.source_evidence import CaptureEvidence
    from spicy_regs.sources import publication
    from spicy_docs.sources.gao.files import validate_gao_report_pdf
    from spicy_docs.sources.gao.product_metadata import product_page_metadata

    key='gao_reports.parquet'
    owner=publication.table_owner(prior_index,key)
    if owner is None or owner[0]!='gao-reports' or key not in owner[1]['tables'] or not set(owner[1]['tables']) <= {key, 'gao_decisions.parquet'}:
        raise ValueError('Expected complete managed gao-reports family')
    identity=publication.file_identity(prior_file)
    declared=publication.table_descriptor(prior_index,key)
    if declared is None or (identity['sha256']!=declared['sha256'] or identity['bytes']!=declared['byteSize']):
        raise ValueError('Retained GAO prior differs from the captured publication')
    evidence=CaptureEvidence(output_dir,'gao-reports')
    try:
        evidence.inherit(prior_index,public_url=public_url)
        if retained_product_page is not None:
            if retained_index is not None:
                raise ValueError('Select one retained metadata source')
            # SpicyDocs 0.50.1 also refuses a page without exactly one Full Report link. The label stays /1: it names
            # the heading and publication-block read, whose values are unchanged on every page still read, and all
            # 47 product pages retained on 2026-08-22 link one Full Report (spicy-docs decisions, 0.50.1).
            index=product_page_metadata(retained_product_page,product_id)
            digest='sha256:'+hashlib.sha256(retained_product_page).hexdigest()
            evidence.retain_bytes(retained_product_page,stage="gao-retained-product-page",
                product_id=product_id,source_url=index.product_url,origin_requests=0)
            evidence.event('retained-product-page-replay',product_id=product_id,sha256=digest,
                byte_size=len(retained_product_page),source_url=index.product_url,origin_requests=0,
                heading=index.title,published_date=index.published_date,
                rule='gao-qualified-page-heading-publication-block/1')
        else:
            index,_=target_index(product_id,retained=retained_index,transport=transport,evidence=evidence)
        if retained_pdf is not None:
            if retained_pdf.byte_size>32*1024*1024 or retained_pdf.status_code!=200:
                raise ValueError('Retained PDF exceeds bounds or is not a successful capture')
            validate_gao_report_pdf(retained_pdf,product_id=product_id)
            evidence.capture(retained_pdf,stage='gao-explicit-target-retained-pdf')
        selected_dir = output_dir / 'selected-prior'
        selected_dir.mkdir(parents=True, exist_ok=True)
        siblings = []
        parents = {key: {'sha256':identity['sha256'], 'byteSize':identity['bytes'],
                         'family':'gao-reports', 'artifactDigest':owner[1]['artifactDigest']}}
        for sibling_key in set(owner[1]['tables']) - {key}:
            sibling = prior_decisions_file or selected_dir / sibling_key
            if prior_decisions_file is None:
                members = publication.table_members(prior_index, sibling_key)
                if len(members) != 1 or not publication.fetch_member(public_url, members[0], sibling):
                    raise ValueError('Missing pinned GAO sibling')
            sibling_identity = publication.file_identity(sibling)
            declared_sibling = publication.table_descriptor(prior_index, sibling_key)
            if declared_sibling is None:
                raise ValueError('Selected GAO sibling has no declared identity')
            if (sibling_identity['sha256'] != declared_sibling['sha256'] or
                    sibling_identity['bytes'] != declared_sibling['byteSize']):
                raise ValueError('Retained GAO sibling differs from the captured publication')
            # Use the declared table name even when the caller provided an alias.
            selected = selected_dir / sibling_key
            if sibling != selected:
                import shutil
                shutil.copyfile(sibling, selected)
            siblings.append(selected)
            parents[sibling_key] = {'sha256':sibling_identity['sha256'], 'byteSize':sibling_identity['bytes'],
                                   'family':'gao-reports', 'artifactDigest':owner[1]['artifactDigest']}
        prior_receipts = None
        prior_generation_id = None
        if owner[1].get('etlReceipts'):
            members = publication.receipt_members(prior_index, dataset='gao_reports')
            prior_receipts = selected_dir / 'etl_receipts.parquet'
            if len(members) != 1 or not publication.fetch_member(public_url, members[0], prior_receipts):
                raise ValueError('Missing pinned GAO receipts')
            prior_generation_id = owner[1]['etlReceipts']['generationId']
        report = {}
        outputs = _target_tables(output_dir/'candidate', prior_file=prior_file, index=index, siblings=tuple(siblings),
            prior_receipts=prior_receipts, prior_generation_id=prior_generation_id, report=report, evidence=evidence)
        evidence.event('explicit-target-projection',**report,offered_pdf_url=index.pdf_url,
            limits='Metadata table only; no text/body reference column, no exact publication day inferred. '
                   'PDF validation checks file route and format; content identity is a separate qualification.')
        generation=output_dir/'generation'
        artifact=build_generation(generation,family='gao-reports',files=outputs,expected_keys=tuple(owner[1]['tables']),
            read_snapshot=prior_index,inputs=evidence.inputs(),
            schemas={path.stem: described_schema(SUBJECT_SCHEMAS[path.stem]) for path in outputs},
            **generation_receipt_args(outputs), parents=parents)
        verify_generation(generation,expected_pin=artifact.pin)
        report.update(status='verified_candidate_not_published',candidate_pin=artifact.pin.as_dict(),
                      generation_directory=str(generation),evidence_directory=str(evidence.artifact_dir))
        (output_dir/'candidate-receipt.json').write_text(json.dumps(report,indent=2))
        evidence.finish()
        return report
    except BaseException as error:
        evidence.finish(error)
        raise
