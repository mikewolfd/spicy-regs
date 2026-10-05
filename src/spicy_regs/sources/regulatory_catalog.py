"""Native regulatory catalog rows and their receipts commit in one transaction.

Native subjects and shared receipts are the only catalog authority. Processing
reads return the exact retained source observations after validating their
current native subject joins; they never reconstruct an older storage format.
"""
from __future__ import annotations

from contextlib import contextmanager
from hashlib import sha256
import shutil
from pathlib import Path
from tempfile import TemporaryDirectory, NamedTemporaryFile
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, rebind_receipt, read_with_receipts, subject_identity
from spicy_regs.native_types import described_schema
from spicy_regs.transforms.parquet_rows import write_rows
from spicy_regs.transforms.regulations_receipts import policy, write_records
from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS, TYPES

DATASETS = frozenset({'dockets', 'documents', 'comments'})


class CatalogConversionRefused(ValueError):
    """Conversion failed; the separately staged attempts survive transaction rollback."""
    def __init__(self, receipt_path):
        import shutil
        with NamedTemporaryFile(prefix='catalog-refusal-', suffix='.parquet', delete=False) as retained:
            self.receipt_path = Path(retained.name)
        shutil.copyfile(receipt_path, self.receipt_path)
        super().__init__('Catalog conversion refused source rows; source state was not changed')


def retain_refusal(con, error):
    """After rollback, persist only failed attempts; no subject becomes accepted."""
    while error is not None and not isinstance(error, CatalogConversionRefused):
        error = error.__cause__
    if error is None:
        return
    from . import iceberg
    try:
        con.execute(f'CREATE SCHEMA IF NOT EXISTS {iceberg._CATALOG_ALIAS}."{namespace()}"')
        with _transaction(con):
            con.execute(f'CREATE TABLE IF NOT EXISTS {receipts_table()} ({_ddl(RECEIPT_SCHEMA)})')
            con.execute(f"INSERT INTO {receipts_table()} SELECT * FROM read_parquet(?) WHERE outcome<>'accepted'",
                        [str(error.receipt_path)])
    except BaseException as persistence_error:
        error.add_note(f'Refusal receipts remain recoverable at {error.receipt_path}')
        raise error from persistence_error
    else:
        error.receipt_path.unlink(missing_ok=True)


def rejected_attempts(con, source, dataset, work, generation, *, reason):
    """Keep unselected exact source observations as processing evidence."""
    from spicy_regs.etl_receipts import ReceiptContext, failure_receipt, exact_json
    path = work / ('rejected-source-' + uuid4().hex + '.parquet')
    _copy(con, f'SELECT * FROM {source}', path)
    def records():
        for i, row in enumerate(_rows(path)):
            witness = {'source_id': source, 'source_uri': None,
                       'sha256': sha256(exact_json(row).encode()).hexdigest(),
                       'locator': 'receipt.values.raw_source_record (canonical exact_json)',
                       'body_version': generation}
            context = ReceiptContext(generation, f'unselected-row:{i}', 'regulatory-catalog-native-v1',
                                     [witness], {'reason': reason})
            yield failure_receipt(policy(dataset), context, outcome='rejected', raw_fields={'raw_source_record': row})
    receipts = work / ('rejected-receipts-' + uuid4().hex + '.parquet')
    write_rows(records(), receipts, RECEIPT_SCHEMA)
    con.execute(f'INSERT INTO {receipts_table()} SELECT * FROM read_parquet(?)', [str(receipts)])


def supports(record_type):
    return record_type.name in DATASETS


def namespace():
    from . import iceberg
    return iceberg._namespace() + '_native'


def qualified(record_type):
    from . import iceberg
    return f'{iceberg._CATALOG_ALIAS}."{namespace()}"."{record_type.name}"'


def receipts_table():
    from . import iceberg
    return f'{iceberg._CATALOG_ALIAS}."{namespace()}"."etl_receipts"'


def _exists(con, name):
    # A failed SELECT on an absent Iceberg namespace caches that negative lookup
    # for the connection. Inspect metadata instead so a preview can precede DDL.
    import re
    parts = [part[1:-1].replace('""', '"') if part.startswith('"') else part
             for part in re.findall(r'"(?:[^"]|"")*"|[^.]+', name)]
    if len(parts) != 3:
        raise ValueError('Catalog existence lookup requires a qualified table name')
    return con.execute('SELECT 1 FROM information_schema.tables WHERE table_catalog=? '
                       'AND table_schema=? AND table_name=? LIMIT 1', parts).fetchone() is not None


def _ddl(schema):
    return ', '.join('"' + name.replace('"', '""') + '" ' + dtype for name, dtype in described_schema(schema))


def _copy(con, sql, path):
    from .iceberg import _sql_str
    con.execute(f"COPY ({sql}) TO '{_sql_str(str(path))}' (FORMAT PARQUET, COMPRESSION ZSTD)")


def _rows(path):
    for batch in pq.ParquetFile(path).iter_batches(batch_size=2000):
        yield from batch.to_pylist()


def _validate_source_columns(con, source, dataset):
    """Require declared mapper input columns before retaining a source observation."""
    types = {r[0]: r[1] for r in con.execute(f'DESCRIBE SELECT * FROM {source}').fetchall()}
    held = set(types)
    wrong = [name for name, dtype in SOURCE_COLUMNS[dataset]
             if dtype == 'VARCHAR' and name in held and types[name] != 'VARCHAR']
    if wrong:
        raise ValueError(f'{dataset}: source string columns have incompatible types: {wrong}')
    unknown = held - {c for c, _ in SOURCE_COLUMNS[dataset]}
    if unknown:
        raise ValueError(f'{dataset}: unclassified source columns {sorted(unknown)}')
    return ', '.join(f'CAST("{name}" AS {dtype}) AS "{name}"' if name in held
                     else f'CAST(NULL AS {dtype}) AS "{name}"' for name, dtype in SOURCE_COLUMNS[dataset])


def normalize_source_record(dataset, row):
    from datetime import date, datetime
    shaped = {}
    identity = row.get(policy(dataset).identity_fields[0])
    if not isinstance(identity, str) or not identity.strip():
        raise ValueError(f'{dataset}: source identity must be nonblank text')
    for name, dtype in SOURCE_COLUMNS[dataset]:
        value = row.get(name)
        if isinstance(value, str) and dtype in {'INTEGER', 'BIGINT'}:
            import re
            if not re.fullmatch(r'[+-]?[0-9]+', value):
                raise ValueError(f'{dataset}.{name}: invalid integer')
            value = int(value)
        elif isinstance(value, str) and dtype == 'BOOLEAN':
            if value.lower() not in {'true', 'false'}:
                raise ValueError(f'{dataset}.{name}: invalid boolean')
            value = value.lower() == 'true'
        elif isinstance(value, str) and dtype == 'DATE':
            value = date.fromisoformat(value)
        elif isinstance(value, str) and dtype.startswith('TIMESTAMP'):
            value = datetime.fromisoformat(value.replace('Z', '+00:00'))
        elif isinstance(value, str) and dtype == 'DOUBLE':
            value = float(value)
        shaped[name] = value
    return shaped


def _stage(con, source, dataset, work, generation, *, prior_receipts=None, bulk=True):
    from spicy_regs.etl_receipts import ReceiptContext
    source_path = work / 'source.parquet'
    _validate_source_columns(con, source, dataset)  # Refuse unclassified columns before conversion.
    source_query = f'SELECT * FROM {source}'
    _copy(con, source_query, source_path)
    def observations():
        from spicy_regs.etl_receipts import exact_json
        for i, row in enumerate(_rows(source_path)):
            # The exact source record is retained in this receipt; its canonical bytes
            # remain reconstructable after temporary staging files are removed.
            witness = {'source_id': source, 'source_uri': None,
                       'sha256': sha256(exact_json(row).encode()).hexdigest(),
                       'locator': 'receipt.values.raw_source_record (canonical exact_json)',
                       'body_version': generation}
            context = ReceiptContext(generation, f'row:{i}', 'regulatory-catalog-native-v1', [witness])
            yield row, context
    records = observations()
    from spicy_regs import comments_bulk, etl_bulk
    from spicy_regs.etl_receipts import carry_receipt_history, validate_receipt_bundle
    from spicy_regs.transforms.regulations_receipts import map_regulations_attempt
    if bulk and dataset == 'comments' and comments_bulk.eligible(pq.read_schema(source_path)):
        mapped = work / 'mapped'
        mapped.mkdir()
        subject, receipts = mapped / 'subjects.parquet', mapped / 'etl_receipts.parquet'
        def attempt(row, ordinal):
            from spicy_regs.etl_receipts import exact_json
            witness = {'source_id': source, 'source_uri': None,
                       'sha256': sha256(exact_json(row).encode()).hexdigest(),
                       'locator': 'receipt.values.raw_source_record (canonical exact_json)', 'body_version': generation}
            context = ReceiptContext(generation, f'row:{ordinal}', 'regulatory-catalog-native-v1', [witness])
            return map_regulations_attempt(dataset, row, context, project=lambda raw: normalize_source_record(dataset, raw))
        try:
            comments_bulk.write_bundle(source_path, subject, receipts, policy=policy(dataset), generation_id=generation,
                                       source_label=source, row_attempt=attempt)
        except (etl_bulk.duckdb.Error, pa.ArrowInvalid):
            # A separate private row destination preserves reference decoding and
            # first-error behavior if SQL cannot read or represent the source.
            subject, receipts = write_records(dataset, records, work / 'mapped-row',
                                             project=lambda row: normalize_source_record(dataset, row))
    else:
        subject, receipts = write_records(dataset, records, work / 'mapped',
                                         project=lambda row: normalize_source_record(dataset, row))
    if prior_receipts is not None:
        prior_path = work / 'prior-receipts.parquet'
        _copy(con, f'SELECT unnest(receipt) FROM {prior_receipts}', prior_path)
        carried = carry_receipt_history(receipts, [prior_path], work / 'carried-receipts.parquet')
        carried.replace(receipts)
    try:
        etl_bulk.validate_bundle({dataset: [subject]}, [receipts], [policy(dataset)], generation_id=generation)
    except etl_bulk.NotBulkEligible:
        validate_receipt_bundle({dataset: [subject]}, [receipts], [policy(dataset)], generation_id=generation)
    if pq.ParquetFile(subject).metadata.num_rows != pq.ParquetFile(source_path).metadata.num_rows:
        raise CatalogConversionRefused(receipts)
    return subject, receipts


@contextmanager
def _transaction(con, *, retain_failed=False):
    con.execute('BEGIN')
    try:
        yield
        con.execute('COMMIT')
    except BaseException as error:
        try:
            con.execute('ROLLBACK')
        except Exception:
            pass
        if retain_failed:
            retain_refusal(con, error)
        raise


def initialized(con, dataset):
    """Selection requires one checked shared receipt for completed initialization."""
    if not _exists(con, receipts_table()):
        return False
    rows = con.execute(f"SELECT * FROM {receipts_table()} WHERE dataset=? "
                       "AND processor='regulatory-catalog-initialization-v1'",
                       [dataset]).to_arrow_table().to_pylist()
    if not rows:
        return False
    if len(rows) != 1:
        raise ValueError('Native catalog has ambiguous initialization receipts')
    row = rows[0]
    from spicy_regs.etl_receipts import exact_json, ReceiptContext
    ReceiptContext(row['generation_id'], row['attempt_id'], row['processor'], row['witnesses'])
    rebind_receipt(row, generation_id=row['generation_id'])  # Verify the exact receipt digest.
    if (row['outcome'] != 'observed' or row['policy_version'] != policy(dataset).policy_version
            or row['record_id'] is not None or row['subject_version'] is not None
            or row['processing_json'] != exact_json({'input_metadata': {'catalog_initialized': True}})):
        raise ValueError('Native catalog initialization receipt differs from field policy')
    return True


def require_initialized(con, record_type):
    """Refuse an absent or merely prepared catalog; physical existence is not selection."""
    if not supports(record_type):
        raise ValueError(f'Unsupported regulatory catalog dataset: {record_type.name}')
    target = qualified(record_type)
    if not _exists(con, target) or not initialized(con, record_type.name):
        raise ValueError('Native catalog has no qualified initialization receipt')
    observed = [(r[0], r[1]) for r in con.execute(f'DESCRIBE {target}').fetchall()]
    if observed != described_schema(policy(record_type.name).subject_schema):
        raise ValueError('Native catalog schema differs from declared policy')


def ensure_native(con, record_type):
    """Initialize an empty native dataset; never read or migrate another namespace.

    Storage preparation and initialization are separate transactions because the
    Iceberg engine does not retain manifests from a multi-table CREATE+data commit.
    Readers require the checked initialization receipt, including after interruption.
    """
    from . import iceberg
    from spicy_regs.etl_receipts import ReceiptContext, observation_receipt
    if not supports(record_type):
        raise ValueError(f'Unsupported regulatory catalog dataset: {record_type.name}')
    dataset = record_type.name
    declared = policy(dataset)
    target = qualified(record_type)
    con.execute(f'CREATE SCHEMA IF NOT EXISTS {iceberg._CATALOG_ALIAS}."{namespace()}"')
    if _exists(con, target) and initialized(con, dataset):
        require_initialized(con, record_type)
        return
    with _transaction(con):
        con.execute(f'CREATE TABLE IF NOT EXISTS {receipts_table()} ({_ddl(RECEIPT_SCHEMA)})')
        con.execute(f'CREATE TABLE IF NOT EXISTS {target} ({_ddl(declared.subject_schema)})')
    with TemporaryDirectory(prefix='catalog-initialize-') as temporary, _transaction(con):
        if initialized(con, dataset):
            return
        if con.execute(f'SELECT count(*) FROM {target}').fetchone()[0]:
            raise ValueError('Uninitialized native catalog contains unselected rows')
        generation = 'initialize-' + uuid4().hex
        context = ReceiptContext(generation, 'initialize', 'regulatory-catalog-initialization-v1',
            [{'source_id': dataset, 'source_uri': None, 'sha256': None,
              'locator': 'empty native catalog initialization', 'body_version': generation}])
        marker = observation_receipt(declared, context, processing_fields={
            'input_metadata': {'catalog_initialized': True}})
        marker_path = Path(temporary) / 'initialization.parquet'
        write_rows([marker], marker_path, RECEIPT_SCHEMA)
        con.execute(f'INSERT INTO {receipts_table()} SELECT * FROM read_parquet(?)', [str(marker_path)])
        require_initialized(con, record_type)


def processing_table(con, record_type, *, where: str | None = None, in_transaction=False):
    """Return exact retained mapper observations after checking native subject joins.

    The receipt owns the original processing input. Native business columns are
    not converted back into strings or JSON to manufacture an older table shape.
    """
    from . import iceberg
    from contextlib import nullcontext
    dataset = record_type.name
    target = qualified(record_type)
    temporary_name = '_processing_' + dataset + '_' + uuid4().hex
    predicate = f' WHERE {where}' if where else ''
    with TemporaryDirectory(prefix='catalog-read-') as temporary:
        work = Path(temporary)
        with (nullcontext() if in_transaction else _transaction(con)):
            generation = 'read-' + uuid4().hex
            require_initialized(con, record_type)
            subject = work / 'subjects.parquet'
            _copy(con, f'SELECT * FROM {target}{predicate}', subject)
            ids = work / 'identities.parquet'
            write_rows(({'record_id': subject_identity(policy(dataset), row)[0]} for row in _rows(subject)),
                       ids, pa.schema([('record_id', pa.string())]))
            raw_receipts = work / 'selected-receipts.parquet'
            # Full reads also detect orphan receipts (for example an unpaired DELETE).
            # Scoped reads join only selected identities and avoid restoring unrelated agencies.
            selected_ids = (f" SEMI JOIN read_parquet('{iceberg._sql_str(str(ids))}') i USING (record_id)"
                            if where else '')
            _copy(con, f"SELECT r.* FROM {receipts_table()} r{selected_ids} "
                       f"WHERE r.dataset='{dataset}' AND r.outcome='accepted'", raw_receipts)
            receipts = work / 'receipts.parquet'
            shutil.copyfile(raw_receipts, receipts)
            restored = work / 'processing.parquet'
            def source_rows():
                for row in read_with_receipts([subject], [receipts], policy(dataset), generation_id=generation):
                    from spicy_regs.transforms.regulations_receipts import _processor_input
                    yield _processor_input(dataset, row)
            rows = source_rows()
            write_rows(rows, restored, pa.schema([(n, TYPES[t]) for n, t in SOURCE_COLUMNS[dataset]]))
            con.execute(f'CREATE TEMP TABLE "{temporary_name}" AS SELECT * FROM read_parquet(?)', [str(restored)])
    return '"' + temporary_name + '"'


def replace_native(con, record_type, source, *, expected_prior=None, scope=None, expected_snapshot=None, in_transaction=False, delete_scope=False, rejected_source=None):
    """Replace selected rows and current receipts in one multi-table catalog commit."""
    from . import iceberg
    from contextlib import nullcontext
    if not in_transaction:
        ensure_native(con, record_type)
    key = record_type.dedup_key
    key_sql = '"' + key.replace('"', '""') + '"'
    scope_sql = iceberg._scope_predicate(record_type, scope)
    count, distinct, missing, outside = con.execute(f'''SELECT count(*), count(DISTINCT {key_sql}),
        count(*) FILTER (WHERE {key_sql} IS NULL OR trim({key_sql})=''),
        count(*) FILTER (WHERE ({scope_sql}) IS NOT TRUE) FROM {source}''').fetchone()
    if missing or count != distinct or outside:
        raise ValueError('Catalog replacements require distinct, nonblank identities within scope')
    if delete_scope and not scope:
        raise ValueError('Replacing a whole scope requires an explicit scope')
    if not count and not delete_scope and rejected_source is None:
        return
    with TemporaryDirectory(prefix='catalog-replace-') as temporary:
        work = Path(temporary)
        generation = 'write-' + uuid4().hex
        with (nullcontext() if in_transaction else _transaction(con, retain_failed=True)):
            if expected_snapshot is not None and iceberg._read_snapshot(con, record_type) != expected_snapshot:
                raise RuntimeError('Catalog snapshot changed after preparation; rerun the operation')
            prior_where = scope_sql if delete_scope else f'{key_sql} IN (SELECT {key_sql} FROM {source}) AND {scope_sql}'
            prior = processing_table(con, record_type, where=prior_where, in_transaction=True)
            if expected_prior is not None:
                expected_columns = _validate_source_columns(con, expected_prior, record_type.name)
                expected_prior = f'(SELECT {expected_columns} FROM {expected_prior} WHERE {prior_where})'
                delta = con.execute(f'''SELECT count(*) FROM ((SELECT * FROM {prior} EXCEPT ALL SELECT * FROM {expected_prior})
                    UNION ALL (SELECT * FROM {expected_prior} EXCEPT ALL SELECT * FROM {prior}))''').fetchone()[0]
                if delta:
                    raise RuntimeError('Catalog changed after preparation; rerun the operation')
            if scope and con.execute(f'SELECT 1 FROM {qualified(record_type)} WHERE {key_sql} IN '
                                     f'(SELECT {key_sql} FROM {source}) AND ({scope_sql}) IS NOT TRUE LIMIT 1').fetchone():
                raise ValueError('Catalog replacement identity belongs to another scope')
            identities = work / 'prior-identities.parquet'
            reader = con.execute(f'SELECT {key_sql} FROM {prior}').to_arrow_reader(2000)
            write_rows(({'record_id': subject_identity(policy(record_type.name), row)[0], key: row[key]}
                        for batch in reader for row in batch.to_pylist()), identities,
                       pa.schema([('record_id', pa.string()), (key, pa.string())]))
            prior_receipts = '_prior_receipts_' + uuid4().hex
            con.execute(f'CREATE TEMP TABLE {prior_receipts} AS SELECT i."{key}", r AS receipt '
                        f'FROM {receipts_table()} r JOIN read_parquet(?) i USING (record_id) '
                        "WHERE r.dataset=? AND r.outcome='accepted'", [str(identities), record_type.name])
            subject, receipts = _stage(con, source, record_type.name, work, generation,
                                      prior_receipts=prior_receipts)
            native = '_native_replacement_' + uuid4().hex
            pending = '_receipt_replacement_' + uuid4().hex
            con.execute(f'CREATE TEMP TABLE {native} AS SELECT * FROM read_parquet(?)', [str(subject)])
            con.execute(f'CREATE TEMP TABLE {pending} AS SELECT * FROM read_parquet(?)', [str(receipts)])
            if delete_scope:
                removed = work / 'removed-identities.parquet'
                from spicy_regs.transforms.regulations_shape import shape_record
                write_rows(({'record_id': subject_identity(policy(record_type.name), shape_record(record_type.name, row))[0]}
                            for batch in con.execute(f'SELECT * FROM {prior} WHERE {key_sql} NOT IN '
                                                   f'(SELECT {key_sql} FROM {source})').to_arrow_reader(2000)
                            for row in batch.to_pylist()),
                           removed, pa.schema([('record_id', pa.string())]))
                from spicy_regs.etl_receipts import retire_receipt
                retired = work / 'retired-receipts.parquet'
                prior_rows = con.execute(f'SELECT receipt FROM {prior_receipts} WHERE "{key}" NOT IN '
                                         f'(SELECT {key_sql} FROM {source})').to_arrow_reader(2000)
                write_rows((retire_receipt(row['receipt'], generation_id=generation,
                                          reason='explicit scope replacement')
                            for batch in prior_rows for row in batch.to_pylist()), retired, RECEIPT_SCHEMA)
                con.execute(f'INSERT INTO {receipts_table()} SELECT * FROM read_parquet(?)', [str(retired)])
                con.execute(f'DELETE FROM {receipts_table()} WHERE dataset=? AND outcome=\'accepted\' AND record_id IN '
                            '(SELECT record_id FROM read_parquet(?))', [record_type.name, str(removed)])
                con.execute(f'DELETE FROM {qualified(record_type)} WHERE {scope_sql} AND {key_sql} NOT IN '
                            f'(SELECT {key_sql} FROM {source})')
            columns = policy(record_type.name).subject_schema.names
            assignments = ', '.join(f'"{c}"=s."{c}"' for c in columns)
            names = ', '.join(f'"{c}"' for c in columns)
            values = ', '.join(f's."{c}"' for c in columns)
            changed = con.execute(f'''MERGE INTO {qualified(record_type)} t USING {native} s ON t.{key_sql}=s.{key_sql}
                WHEN MATCHED THEN UPDATE SET {assignments}
                WHEN NOT MATCHED THEN INSERT ({names}) VALUES ({values})''').fetchone()[0]
            if changed != count:
                raise RuntimeError('Catalog replacement changed an unexpected number of rows')
            con.execute(f'''DELETE FROM {receipts_table()} WHERE dataset=? AND outcome='accepted'
                AND record_id IN (SELECT record_id FROM {pending})''', [record_type.name])
            con.execute(f'INSERT INTO {receipts_table()} SELECT * FROM {pending}')
            if rejected_source is not None:
                rejected_attempts(con, rejected_source, record_type.name, work, generation,
                                  reason='not selected: duplicate or not newer than current subject')
            verified = processing_table(con, record_type, where=f'{key_sql} IN (SELECT {key_sql} FROM {source})',
                                        in_transaction=True)
            source_columns = _validate_source_columns(con, source, record_type.name)
            comparison = f'(SELECT {source_columns} FROM {source})'
            delta = con.execute(f'''SELECT count(*) FROM ((SELECT * FROM {verified} EXCEPT ALL SELECT * FROM {comparison})
                UNION ALL (SELECT * FROM {comparison} EXCEPT ALL SELECT * FROM {verified}))''').fetchone()[0]
            if delta:
                raise RuntimeError('Catalog replacement changed source values')
            for temporary_table in (prior, verified, prior_receipts, native, pending):
                con.execute(f'DROP TABLE {temporary_table}')


def export_pair(con, record_type, output_dir, *, generation_id, snapshot=None):
    """Export and qualify one native catalog read, preserving its source witnesses."""
    import json
    from dataclasses import asdict
    from spicy_regs.transforms.regulations_receipts import ReceiptInput
    from . import iceberg
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset = record_type.name
    subjects = output_dir / f'{dataset}.parquet'
    receipts = output_dir / 'etl_receipts.parquet'
    with TemporaryDirectory(prefix='catalog-export-') as temporary, _transaction(con):
        if not initialized(con, dataset):
            raise ValueError('Native catalog has no qualified initialization receipt')
        if snapshot is not None and iceberg._read_pair_snapshot(con, record_type) != snapshot:
            raise RuntimeError('Catalog snapshot changed before paired export; retry')
        _copy(con, f'SELECT * FROM {qualified(record_type)}', subjects)
        raw_receipts = Path(temporary) / 'receipts.parquet'
        # Explicit scope deletion retains an internal audit receipt. It is no
        # longer a selected attempt, so omit only that exact retirement marker.
        # Other observed/rejected/refused receipts remain part of the selection.
        _copy(con, f"""SELECT * FROM {receipts_table()} r WHERE dataset='{dataset}' AND NOT (
            outcome='observed' AND record_id IS NOT NULL AND subject_version IS NULL
            AND ends_with(attempt_id, ':retired') AND EXISTS (
                SELECT 1 FROM json_each(r.diagnostic_json, '$[1]') d
                WHERE json_extract_string(d.value, '$[0]')='retired_reason'
                  AND json_extract_string(d.value, '$[1][0]')='str'
                  AND json_extract_string(d.value, '$[1][1]')='explicit scope replacement'))""", raw_receipts)
        shutil.copyfile(raw_receipts, receipts)
        for _ in read_with_receipts([subjects], [receipts], policy(dataset), generation_id=generation_id):
            pass
    metadata = {'generation_id': generation_id, 'dataset': dataset}
    if snapshot is not None:
        metadata['snapshot'] = asdict(snapshot)
    (output_dir / 'generation.json').write_text(json.dumps(metadata, sort_keys=True) + '\n')
    return ReceiptInput(dataset, (subjects,), receipts, generation_id)
