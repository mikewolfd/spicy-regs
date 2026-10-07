"""Court builders use the shared receipt writer and exact internal receipt reads."""
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.court_subjects import (
    IDENTITIES, RECEIPT_FIELDS, SUBJECT_SCHEMAS, normalize_court_row,
)
from spicy_regs.etl_receipts import (
    DatasetPolicy, RECEIPT_SCHEMA, ReceiptContext, ReceiptLineage, combine_receipts, failure_receipt,
    read_with_receipts, select_receipts, split_record, write_dataset,
)

POLICIES = {
    name: DatasetPolicy(name, schema, IDENTITIES[name], RECEIPT_FIELDS[name], policy_version='courts/2' if name in {'court_opinions', 'court_opinion_pdf_extractions'} else 'courts/1')
    for name, schema in SUBJECT_SCHEMAS.items()
}


def file_witness(path: Path, *, source_id: str | None = None) -> dict:
    """Pin retained bytes, not a mutable filename or inferred edition."""
    with path.open('rb') as stream:
        digest = 'sha256:' + hashlib.file_digest(stream, 'sha256').hexdigest()
    return {'source_id': source_id or path.name, 'source_uri': str(path.resolve()),
            'sha256': digest, 'locator': None, 'body_version': None}


def parquet_rows(path: Path) -> Iterable[dict]:
    with pq.ParquetFile(path) as parquet:
        for batch in parquet.iter_batches(batch_size=2000):
            yield from batch.to_pylist()


def record_source_failure(dataset: str, output_dir: Path, *, witnesses: Sequence[Mapping], error: Exception) -> Path:
    """Retain a failed build attempt without selecting its empty subject file."""
    context = ReceiptContext(uuid4().hex, 'source-read', 'spicy_regs/courts/1', witnesses,
                             {'error_type': type(error).__name__, 'error': str(error)})
    directory = output_dir / '.court-etl' / ('failed-' + uuid4().hex)
    failure = failure_receipt(POLICIES[dataset], context, outcome='error', raw_fields={})
    _, receipt = write_dataset((), directory, POLICIES[dataset], failures=[failure])
    return receipt


def _pdf_failure(row: Mapping) -> str | None:
    if row.get('sha1_matches') not in ('true', True):
        return 'native_digest_mismatch'
    diagnostics = json.loads(row.get('pdf_extraction_results_json') or '[]')
    if not isinstance(diagnostics, list) or len(diagnostics) != 1:
        return 'missing_extraction_diagnostic'
    status = diagnostics[0].get('status')
    return None if status == 'ok' and isinstance(row.get('text_content'), str) else status or 'missing_body'


def write_court_rows(
    dataset: str, rows: Iterable[Mapping], output_dir: Path, *, witnesses: Sequence[Mapping],
    generation_id: str | None = None, processor: str = 'spicy_regs/courts/1',
    diagnostics: Mapping | None = None, refused_receipts: Path | None = None,
    attempt_prefix: str = "row", prior_receipts: Path | None = None,
) -> Path:
    """Expose a complete subject/receipt pair only after validating both.

    The public local filename points into the immutable shared-writer bundle.
    Replacing that one link keeps a previous subject and its receipts together
    on a partial source read, failed conversion, or interrupted retry.
    """
    policy = POLICIES[dataset]
    generation_id = generation_id or uuid4().hex
    output_dir.mkdir(parents=True, exist_ok=True)
    bundle = output_dir / '.court-etl' / uuid4().hex
    bundle.parent.mkdir(exist_ok=True)
    with ReceiptLineage([] if prior_receipts is None else [prior_receipts], dataset=dataset) as lineage, \
            TemporaryDirectory(prefix='.court-failures-', dir=output_dir) as temporary:
        failures_path = Path(temporary) / 'etl_receipts.parquet'
        with pq.ParquetWriter(failures_path, RECEIPT_SCHEMA, compression='zstd') as writer:
            failures = []

            def records():
                for ordinal, raw in enumerate(rows):
                    context = ReceiptContext(generation_id, f'{dataset}:{attempt_prefix}:{ordinal}', processor,
                        [dict(w) for w in witnesses],
                        diagnostics or {})
                    try:
                        mapped = normalize_court_row(dataset, raw)
                        reason = _pdf_failure(mapped) if dataset == 'court_opinion_pdf_extractions' else None
                        if reason:
                            raise ValueError(f'Opinion body unavailable: {reason}')
                        # Validate before inheriting evidence or replacing a subject.
                        split_record(policy, mapped, context)
                        context = lineage.inherit(context, policy, mapped)
                    except (ValueError, TypeError, KeyError, pa.ArrowException) as error:
                        failed = replace(context, diagnostics={**context.diagnostics,
                            'error_type': type(error).__name__, 'error': str(error)})
                        failures.append(failure_receipt(policy, failed, outcome='refused',
                                                        raw_fields={'raw_source_record': dict(raw)}))
                        if len(failures) >= 2000:
                            writer.write_table(pa.Table.from_pylist(failures, schema=RECEIPT_SCHEMA))
                            failures.clear()
                    else:
                        yield mapped, context
                if failures:
                    writer.write_table(pa.Table.from_pylist(failures, schema=RECEIPT_SCHEMA))
                    failures.clear()
                if refused_receipts is not None:
                    for receipt in parquet_rows(refused_receipts):
                        if receipt['dataset'] != dataset or receipt['generation_id'] != generation_id:
                            raise ValueError('Rejected updates differ from the selected court build')
                        if receipt['outcome'] == 'refused':
                            writer.write_table(pa.Table.from_pylist([receipt], schema=RECEIPT_SCHEMA))
                writer.close()

            try:
                subject, _ = write_dataset(records(), bundle, policy, failures=parquet_rows(failures_path))
            except Exception as error:
                try:
                    record_source_failure(dataset, output_dir, witnesses=witnesses, error=error)
                except Exception as receipt_error:
                    error.add_note(f'Failed to retain source failure receipt: {receipt_error}')
                raise
    assert subject is not None
    (bundle / 'court-build.json').write_text(json.dumps({'generation_id': generation_id, 'dataset': dataset}) + '\n')
    public = output_dir / (dataset + '.parquet')
    pointer = output_dir / ('.' + dataset + '.' + uuid4().hex + '.partial')
    try:
        pointer.symlink_to(os.path.relpath(subject, output_dir))
        pointer.replace(public)
    finally:
        pointer.unlink(missing_ok=True)
    # Generation admission accepts regular immutable files, never mutable links.
    return public.resolve()


def finish_court_output(dataset: str, staged: Path, output_dir: Path, *, witnesses=None,
                        generation_id: str | None = None, diagnostics: Mapping | None = None,
                        refused_receipts: Path | None = None, prior_receipts: Path | None = None) -> Path:
    """Split a completed private mapper output; keep its bytes as retained evidence."""
    return write_court_rows(dataset, parquet_rows(staged), output_dir,
                            witnesses=witnesses or [file_witness(staged)], generation_id=generation_id,
                            diagnostics=diagnostics, refused_receipts=refused_receipts, prior_receipts=prior_receipts)


def admit_court_updates(dataset: str, source: Path, directory: Path, *, schema: pa.Schema,
                        generation_id: str) -> tuple[Path, Path]:
    """Validate updates before merging identities; return accepted inputs and all refusal receipts."""
    subject = write_court_rows(dataset, parquet_rows(source), directory,
        witnesses=[file_witness(source)], generation_id=generation_id, attempt_prefix='incoming')
    receipts, _ = local_receipt_selection(subject)
    accepted = restore_processing_input(subject, directory / 'accepted-inputs.parquet',
        dataset=dataset, schema=schema)
    return accepted, receipts


def local_receipt_selection(path: Path) -> tuple[Path, str]:
    bundle = path.resolve().parent
    descriptor = bundle / 'court-build.json'
    if not descriptor.exists() or not (bundle / 'etl_receipts.parquet').is_file():
        raise ValueError(f'Court subject has no selected receipt bundle: {path}')
    values = json.loads(descriptor.read_text())
    if values['dataset'] != path.resolve().stem:
        raise ValueError('Court receipt bundle names another dataset')
    return bundle / 'etl_receipts.parquet', values['generation_id']


def read_court_rows(path: Path, *, dataset: str, receipt_path: Path | None = None,
                    generation_id: str | None = None) -> Iterable[dict]:
    """Internal processing read; a migrated row never falls back to missing evidence."""
    from spicy_regs.etl_receipts import selected_subject_policy
    try:
        selected_policy = selected_subject_policy(POLICIES[dataset], [path])
    except ValueError as error:
        raise ValueError('Court prior must use the native subject schema and selected receipts') from error
    if receipt_path is None:
        receipt_path, generation_id = local_receipt_selection(path)
    if generation_id is None:
        raise ValueError('Court internal read requires the selected receipt generation')
    with TemporaryDirectory(prefix='court-read-') as temporary:
        selected = select_receipts(receipt_path, Path(temporary) / 'etl_receipts.parquet', dataset=dataset)
        yield from read_with_receipts([path], [selected], selected_policy, generation_id=generation_id)


def processing_rows(rows, *, dataset: str, schema: pa.Schema):
    """Decode the maintained court mapper input from admitted exact matching rows."""
    names = tuple(schema.names)
    string_fields = tuple(field.name for field in schema if pa.types.is_string(field.type))

    def restored():
        for row in rows:
            row = dict(row)
            row.update(row.get('conversion_inputs') or {})
            if dataset == 'court_dockets':
                for name in ('parties', 'attorneys', 'firms'):
                    if name + '_json' not in row:
                        row[name + '_json'] = json.dumps(row.get(name)) if row.get(name) is not None else None
            if dataset == 'court_opinion_clusters':
                value = row.get('attorneys')
                row['attorneys'] = [value] if isinstance(value, str) else value
            for name in string_fields:
                value = row.get(name)
                if value is not None and not isinstance(value, str):
                    row[name] = json.dumps(value) if isinstance(value, (dict, list)) else str(value)
            yield {name: row.get(name) for name in names}
    yield from restored()



def restore_processing_input(path: Path, destination: Path, *, dataset: str, schema: pa.Schema,
                             receipt_path: Path | None = None, generation_id: str | None = None) -> Path:
    """Reconstruct the mapper's private merge columns, preserving exact raw literals."""
    from spicy_regs.transforms.parquet_rows import write_rows

    return write_rows(processing_rows(
        read_court_rows(path, dataset=dataset, receipt_path=receipt_path, generation_id=generation_id),
        dataset=dataset, schema=schema), destination, schema)

def prior_receipt_selection(path: Path, *, dataset: str) -> tuple[Path | None, str | None]:
    """Select receipts from a local bundle or the same pinned publication as the prior."""
    if path.is_symlink() or (path.parent / 'court-build.json').exists():
        return local_receipt_selection(path)
    from spicy_regs.sources import publication
    base = os.getenv('R2_PUBLIC_URL')
    if not base:
        return None, None
    index = publication.current_index(base)
    owner = publication.table_owner(index, dataset + '.parquet')
    if owner is None or 'etlReceipts' not in owner[1]:
        return None, None
    member, = publication.receipt_members(index, dataset=dataset)
    destination = path.parent / ('.' + dataset + '.selected-receipts-' + uuid4().hex + '.parquet')
    if not publication.fetch_member(base, member, destination, dataset + ' receipts'):
        raise ValueError('Selected court receipts are missing')
    return destination, owner[1]['etlReceipts']['generationId']


def generation_options(paths: Sequence[Path]) -> dict:
    """Arguments for shared build_generation; receipts are never ordinary subject outputs."""
    selections = [local_receipt_selection(path) for path in paths]
    ids = {generation for _, generation in selections}
    if len(ids) != 1:
        raise ValueError('Court build outputs do not share one receipt generation')
    directory = paths[0].parent / '.court-etl' / uuid4().hex
    directory.mkdir(parents=True)
    receipt = combine_receipts([path for path, _ in selections], directory / 'etl_receipts.parquet')
    return {'receipt_path': receipt, 'receipt_policies': [POLICIES[path.stem] for path in paths],
            'receipt_generation_id': ids.pop()}


def build_court_generation(directory: Path, *, family: str, files: Sequence[Path], **kwargs):
    """Admit court subjects and receipts together, including standalone local builds."""
    from spicy_regs.generations import build_generation
    from spicy_regs.native_types import described_schema
    schemas = {path.stem: described_schema(SUBJECT_SCHEMAS[path.stem]) for path in files}
    return build_generation(directory, family=family, files=files,
                            expected_keys=[path.name for path in files], schemas=schemas,
                            **generation_options(files), **kwargs)
