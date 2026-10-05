"""Measure selected legacy Court/FR output conversion and private serving.

This replays the hash-pinned published population. It does not qualify the
CourtListener CSV acquisition or Federal Register REST acquisition/merge.
An enclosing owned supervisor supplies the complete wall/resource bound.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
from itertools import zip_longest
import json
from pathlib import Path
import time
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.parquet as pq
from rulespec_artifacts import LocalMemberSource, admit_artifact

from scripts.qualification_store import DiskStore
from scripts.qualify_congress_bulk import compare_files
from scripts.qualify_receipt_index import assert_pin, bind_key_index_for_qualification, phase, serve
from scripts.qualify_votes_workflow import capture_code_pins, child, mcp_controls, verify_code_pins
from spicy_regs import court_receipts, etl_bulk
from spicy_regs.etl_receipts import carry_receipt_history, validate_receipt_bundle
from spicy_regs.generations import build_generation
from spicy_regs.native_types import described_schema
from spicy_regs.receipt_key_index import KEY
from spicy_regs.sources import publication
from spicy_regs.transforms import regulations_receipts as regulations


DATASETS = {'court_opinions': 'court-opinions', 'federal_register': 'federal-register'}


def declared_policy(dataset):
    return court_receipts.POLICIES[dataset] if dataset == 'court_opinions' else regulations.policy(dataset)


@contextmanager
def observe_validation_routes(record):
    """Record actual full-admission reference routes without changing their inputs."""
    original_ordinals, original_validate = etl_bulk._ordinal_file, etl_bulk.validate_bundle
    routes = []
    record['validationRoutes'] = routes

    def ordinals(con, query, parameters, path):
        observed = {'selection': path.name, 'selectedRows': None, 'yieldedRows': 0, 'fullyConsumed': False}
        routes.append(observed)
        record['actualBulkSettings'] = dict(con.execute("SELECT name,value FROM duckdb_settings() WHERE name IN "
            "('threads','memory_limit','max_temp_directory_size','temp_directory')").fetchall())
        try:
            for ordinal in original_ordinals(con, query, parameters, path):
                if observed['selectedRows'] is None:
                    observed['selectedRows'] = pq.read_metadata(path).num_rows
                observed['yieldedRows'] += 1
                yield ordinal
            observed['selectedRows'] = pq.read_metadata(path).num_rows
            observed['fullyConsumed'] = True
        finally:
            if observed['selectedRows'] is None and path.exists():
                observed['selectedRows'] = pq.read_metadata(path).num_rows

    def validate(*args, **kwargs):
        try:
            return original_validate(*args, **kwargs)
        except etl_bulk.NotBulkEligible:
            record['fullDatasetRowFallback'] = True
            raise

    setattr(etl_bulk, '_ordinal_file', ordinals)
    setattr(etl_bulk, 'validate_bundle', validate)
    try:
        yield
    finally:
        setattr(etl_bulk, '_ordinal_file', original_ordinals)
        setattr(etl_bulk, 'validate_bundle', original_validate)


def capture_source(entry, destination):
    """Keep partial bytes on failure; pin a complete source before any producer."""
    if type(entry['rows']) is not int or entry['rows'] <= 0:
        raise ValueError('Selected full population must declare a positive row count')
    selected = entry.get('localInput')
    source = Path(selected).open('rb') if selected else urlopen(entry['url'], timeout=30)
    with source, destination.open('xb') as sink:
        total = 0
        while block := source.read(1024 * 1024):
            total += len(block)
            sink.write(block)
            if total > entry['bytes']:
                raise ValueError('Selected source exceeds its declared byte size')
    pin = {name: entry['bytes' if name == 'byteSize' else name] for name in ('sha256', 'byteSize', 'rows')}
    assert_pin(destination, pin)
    schema = pq.read_schema(destination)
    if schema.names != entry['columns'] or any(not pa.types.is_string(f.type) for f in schema):
        raise ValueError('Original legacy source columns/order/string types differ')
    destination.chmod(0o444)
    return pin, schema


def produce(dataset, source, directory, generation):
    if dataset == 'court_opinions':
        subject = court_receipts.write_court_rows(dataset, court_receipts.parquet_rows(source), directory,
            witnesses=[court_receipts.file_witness(source)], generation_id=generation,
            attempt_prefix='qualification')
        receipt, selected_generation = court_receipts.local_receipt_selection(subject)
        if selected_generation != generation:
            raise ValueError('Court receipt selection differs from the declared generation')
        return subject, receipt
    return regulations.write_held_dataset(dataset, source, directory, generation_id=generation)


def populations(dataset, source, subject, receipt):
    with etl_bulk.bulk_connection() as (con, _):
        counts = dict(con.execute('SELECT outcome,count(*) FROM read_parquet(?) GROUP BY outcome',
                                 [str(receipt)]).fetchall())
    rows = pq.read_metadata(source).num_rows
    if counts.get('accepted', 0) != rows or pq.read_metadata(subject).num_rows != rows:
        raise ValueError('Selected population contains refused/lost source rows; full qualification stops')
    expected = {'accepted': rows}
    if dataset == 'federal_register':
        expected['observed'] = 1
    if counts != expected:
        raise ValueError('Receipt outcomes differ from the complete selected source population')
    return {'sourceRows': rows, 'subjectRows': rows, 'receiptOutcomes': counts}


def complete_sample(dataset, source, destination, *, bound=2000):
    """Scan all source shapes; retain first eight per shape plus periodic rows."""
    fields = (('per_curiam', 'page_count', 'author_str') if dataset == 'court_opinions'
              else ('document_type', 'agencies_json', 'title'))
    selected, seen = [], {}
    with pq.ParquetFile(source) as parquet:
        ordinal = 0
        for batch in parquet.iter_batches(batch_size=2000):
            for row in batch.to_pylist():
                text = row.get(fields[2])
                shape = (row.get(fields[0]), row.get(fields[1]) is None,
                         text is None, bool(text and not text.isascii()),
                         bool(text and any(value in text for value in ('\x00', '\x1b', '\n'))))
                seen[shape] = seen.get(shape, 0) + 1
                if seen[shape] <= 8 or ordinal % 100003 == 0:
                    if len(selected) == bound:
                        raise ValueError('Complete stratified reference sample exceeds its declared bound')
                    selected.append(row)
                ordinal += 1
        pq.write_table(pa.Table.from_pylist(selected, schema=parquet.schema_arrow), destination)
    return {'sampleRows': len(selected), 'sampleBound': bound, 'sourceRowsScanned': ordinal,
            'strata': [fields[0], fields[1] + ' null', fields[2] + ' null/Unicode/control'],
            'referenceScope': 'Complete sample bundle through both public validator modes; actual producer is row-based'}


def restore(dataset, source, subject, receipt, destination, generation):
    if dataset == 'court_opinions':
        court_receipts.restore_processing_input(subject, destination, dataset=dataset, schema=pq.read_schema(source),
                                               receipt_path=receipt, generation_id=generation)
        return compare_files(source, destination)
    regulations.materialize_internal(regulations.ReceiptInput(dataset, (subject,), receipt, generation), destination)
    # The real processor emits canonical column order, including rin before the
    # final Regulations.gov fields. Check that separately from original order.
    source_schema = pq.read_schema(source)
    canonical = pa.schema([(name, regulations.TYPES[kind]) for name, kind in regulations.SOURCE_COLUMNS[dataset]],
                          metadata=source_schema.metadata)
    if any(name not in canonical.names for name in source_schema.names):
        raise ValueError('Unclassified original columns cannot be reconstructed')
    with pq.ParquetFile(source) as original, pq.ParquetFile(destination) as actual:
        if not actual.schema_arrow.equals(canonical, check_metadata=True):
            raise ValueError('Canonical processor schema/metadata differs')
        rows = 0
        for left, right in zip_longest(original.iter_batches(batch_size=2000), actual.iter_batches(batch_size=2000)):
            if left is None or right is None:
                raise ValueError('Canonical restoration changed the complete source count')
            expected = pa.Table.from_batches([left])
            for field in canonical:
                if field.name not in expected.column_names:
                    expected = expected.append_column(field.name, pa.nulls(left.num_rows, type=field.type))
            expected = expected.select(canonical.names).cast(canonical)
            held = pa.Table.from_batches([right])
            if not expected.equals(held, check_metadata=True):
                raise ValueError(f'Canonical source values/order differ at row {rows}')
            if not held.select(source_schema.names).cast(source_schema).equals(pa.Table.from_batches([left]),
                                                                              check_metadata=True):
                raise ValueError('Original source field order/literals cannot be reconstructed')
            rows += left.num_rows
    return rows


def qualify(plan_path, output, *, fixture=False):
    started = time.monotonic()
    output.mkdir(parents=True, exist_ok=False)
    raw_plan = plan_path.read_bytes()
    (output / 'PLAN.json').write_bytes(raw_plan)
    plan, log = json.loads(raw_plan), output / 'phases.jsonl'
    entry = plan['source']
    dataset, family = entry['dataset'], entry['family']
    if DATASETS.get(dataset) != family:
        raise ValueError('Explicit Court/FR dataset and complete single-table family required')
    with phase(log, 'frozen-code-and-dependency-pins') as record:
        frozen = capture_code_pins()
        if not fixture and (frozen['dirty'] or frozen != plan['codePins']):
            raise ValueError('Full qualification requires the exact clean reviewed code/dependencies')
        record.update(frozen, fixture=fixture, planSha256=hashlib.sha256(raw_plan).hexdigest())
    source = output / 'original.parquet'
    with phase(log, 'complete-original-acquisition-and-admission') as record:
        pin, schema = capture_source(entry, source)
        record.update(pin, originalSchema=str(schema), originalMetadata=str(schema.metadata),
                      acquisitionScope='Published legacy mapper output; upstream CSV/REST acquisition unqualified')
    declared, generation = declared_policy(dataset), 'qualification-' + family
    with phase(log, 'separate-selected-prior-producer-and-full-admission') as record, observe_validation_routes(record):
        prior_subject, prior = produce(dataset, source, output / 'selected-prior', generation + '-prior')
        validate_receipt_bundle({dataset: [prior_subject]}, [prior], [declared], generation_id=generation + '-prior')
        record.update(populations(dataset, source, prior_subject, prior))
    with phase(log, 'fresh-current-producer-and-full-admission') as record, observe_validation_routes(record):
        subject, fresh = produce(dataset, source, output / 'current', generation)
        validate_receipt_bundle({dataset: [subject]}, [fresh], [declared], generation_id=generation)
        record.update(populations(dataset, source, subject, fresh))
    with phase(log, 'complete-stratified-public-row-validator-oracle') as record:
        sample = output / 'reference-source.parquet'
        record.update(complete_sample(dataset, source, sample))
        sample_subject, sample_receipt = produce(dataset, sample, output / 'reference-bundle', generation + '-reference')
        populations(dataset, sample, sample_subject, sample_receipt)
        for bulk in (True, False):
            validate_receipt_bundle({dataset: [sample_subject]}, [sample_receipt], [declared],
                                    generation_id=generation + '-reference', bulk=bulk)
    with phase(log, 'full-unchanged-history-and-exact-prior-fields') as record:
        carried = carry_receipt_history(fresh, [prior], output / 'carried-receipts.parquet')
        record.update(rows=compare_files(prior, carried), current=publication.file_identity(fresh),
                      selectedPrior=publication.file_identity(prior), carried=publication.file_identity(carried),
                      configuredHistoryThreads=4, configuredHistoryMemory='1GB', configuredHistorySpill='32GB')
        if record['current']['sha256'] == record['selectedPrior']['sha256']:
            raise ValueError('History qualification requires distinct separately produced current/prior receipts')
        validate_receipt_bundle({dataset: [subject]}, [carried], [declared], generation_id=generation)
    with phase(log, 'actual-full-processor-restore-original-and-canonical-conservation') as record:
        record['rows'] = restore(dataset, source, subject, carried, output / 'restored.parquet', generation)
    directory = output / 'generation'
    with phase(log, 'complete-native-generation-admission'):
        build_generation(directory, family=family, files=[subject], expected_keys=[dataset + '.parquet'],
                         schemas={dataset: described_schema(declared.subject_schema)}, receipt_path=carried,
                         receipt_policies=[declared], receipt_generation_id=generation)
    with phase(log, 'full-key-index-private-copy-and-locked-range-serving'):
        identity = publication.file_identity(carried)
        receipt_pin = output / 'receipt-pin.json'
        receipt_pin.write_text(json.dumps({'sha256': identity['sha256'], 'byteSize': identity['bytes'],
                                          'rows': pq.read_metadata(carried).num_rows}) + '\n')
        arguments = [str(carried), str(output / 'index'), '--pin', str(receipt_pin), '--dataset', dataset,
                     '--member', str(subject)]
        if fixture:
            arguments.append('--force-small-fixture')
        child('qualify_receipt_index.py', arguments)
    with phase(log, 'explicit-full-index-artifact-admission'):
        descriptor = json.loads((output / 'index' / 'descriptor.json').read_text())
        artifact = bind_key_index_for_qualification(directory, output / 'index' / KEY, descriptor)
    store = DiskStore(output / 'private-store', bucket='retained-qualification')
    with phase(log, 'actual-private-publication-upload-readback'):
        index = publication.publish_generation(directory, client=store, bucket=store.bucket,
                                               prior_index=publication.empty_index())
        (output / 'private-publication.json').write_text(json.dumps(index, indent=2) + '\n')
        (output / 'private-store-calls.json').write_text(json.dumps(store.calls, indent=2) + '\n')
    with phase(log, 'actual-production-mcp-http-and-dataset-correct-controls') as record:
        marker = {'probe': 'actual-mcp-startup-and-controls'}
        with serve({'/' + key: path for key, path in store.objects.items()},
                   output / 'mcp-requests.jsonl', marker) as base:
            controls = mcp_controls(base, output, {dataset: subject}, policies={dataset: declared})
        (output / 'mcp-controls.json').write_text(json.dumps(controls, indent=2) + '\n')
        record.update(settings=controls['settings'], transport=controls['transport'], schemeOverride=controls['schemeOverride'])
    with phase(log, 'final-complete-artifact-stored-bytes-and-original-pins'):
        admit_artifact(LocalMemberSource(directory), expected_pin=artifact.pin)
        admit_artifact(publication._S3Members(store, store.bucket, index['families'][family]['prefix']),
                       expected_pin=artifact.pin)
        assert_pin(source, pin)
        if entry.get('localInput'):
            assert_pin(Path(entry['localInput']), pin)
    verify_code_pins(log, frozen)
    result = {'status': 'passed', 'seconds': time.monotonic() - started, 'fixture': fixture,
              'scope': 'Selected published legacy population replay/private DiskStore/actual loopback MCP',
              'unqualified': ['upstream CSV/REST acquisition/merge', 'hosted publication/MCP'], 'productionActions': []}
    (output / 'RESULT.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('plan', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--fixture', action='store_true')
    args = parser.parse_args()
    print(json.dumps(qualify(args.plan, args.output, fixture=args.fixture)), flush=True)


if __name__ == '__main__':
    main()
