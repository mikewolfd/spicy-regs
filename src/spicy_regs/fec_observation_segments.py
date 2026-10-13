"""Closed local preparation segments; only complete-family admission can publish."""
from contextlib import contextmanager
import base64
import json
import os
from pathlib import Path
import re
from uuid import uuid4

import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_SCHEMA
from spicy_regs.generations import implementation_id
from spicy_regs.local_data import file_signature
from spicy_regs.transforms.assemble_fec_query import _sha
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, dataset_policy

FORMAT = 'fec-observation-segments/1'


def _same_json(left, right):
    return json.dumps(left, sort_keys=True, allow_nan=False) == json.dumps(right, sort_keys=True, allow_nan=False)


def _directory(path):
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError('Observation checkpoint contains a symlink directory')
    missing = [parent for parent in (path, *path.parents) if not parent.exists()]
    path.mkdir(parents=True, exist_ok=True)
    if not path.is_dir():
        raise ValueError('Observation checkpoint directory required')
    for created in reversed(missing):
        _sync_directory(created)
        _sync_directory(created.parent)


def _existing_directory(path):
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError('Observation checkpoint contains a symlink directory')
    if not path.is_dir():
        raise ValueError('Observation segment directory missing')


def _file(path):
    if path.is_symlink() or not path.is_file() or path.stat().st_nlink != 1:
        raise ValueError('Observation checkpoint requires single-link regular files')


def _sync_directory(path):
    fd = os.open(path, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _write_new(path, value):
    """The exclusive checkpoint lock protects this atomic, fsynced new record."""
    if path.exists() or path.is_symlink():
        raise ValueError('A committed observation checkpoint cannot be overwritten')
    temporary = path.parent / ('.pending-' + uuid4().hex)
    with temporary.open('x') as stream:
        json.dump(value, stream, sort_keys=True)
        stream.write('\n')
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    _sync_directory(path.parent)


@contextmanager
def observation_segments(path, *, captured_family, source_pin):
    """Serialize all preparation and readback; a dead owner releases its lock."""
    import fcntl
    path = Path(path).absolute()
    _directory(path)
    header = path / 'checkpoint.json'
    if header.is_symlink():
        raise ValueError('Observation checkpoint header cannot be a symlink')
    if not header.exists() and any(p.name != 'lock' and not p.name.startswith('.pending-') for p in path.iterdir()):
        raise ValueError('Existing partial output is not an observation checkpoint')
    lock = path / 'lock'
    fd = os.open(lock, os.O_CREAT | os.O_RDWR | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        _file(lock)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError('Observation checkpoint already has an active owner') from error
        identity = dict(format=FORMAT, captured_family=captured_family, source_pin=source_pin,
                        implementation=implementation_id(),
                        policies=[dataset_policy(name).descriptor() for name in
                                  sorted(key.removesuffix('.parquet') for key in captured_family['tables'])])
        if not header.exists():
            if any(p.name != 'lock' and not p.name.startswith('.pending-') for p in path.iterdir()):
                raise ValueError('Existing partial output is not an observation checkpoint')
            _write_new(header, dict(identity=identity, generation_id=uuid4().hex))
        _file(header)
        value = json.loads(header.read_text())
        if (not isinstance(value, dict) or set(value) != {'identity', 'generation_id'}
                or not _same_json(value['identity'], identity) or not isinstance(value['generation_id'], str)
                or not re.fullmatch('[0-9a-f]{32}', value['generation_id'])):
            raise ValueError('Observation checkpoint source, input, policy or version changed')
        for name in ('closed', 'attempts'):
            _directory(path / name)
        _sync_directory(path)
        yield ObservationSegments(path, value['generation_id'], _sha(header))
    finally:
        os.close(fd)


class ObservationSegments:
    def __init__(self, root, generation_id, header_sha):
        self.root, self.generation_id, self.header_sha = root, generation_id, header_sha
        self.position = self.attempt_offset = 0
        self.closed = sorted(p for p in (root / 'closed').iterdir() if not p.name.startswith('.pending-'))
        if [p.name for p in self.closed] != [f'{i:08d}.json' for i in range(len(self.closed))]:
            raise ValueError('Observation checkpoint has a gap or unexpected closed segment')
        for path in self.closed:
            _file(path)

    def _verify_closed(self, entry, spec, check_resources):
        if not isinstance(entry, dict) or set(entry) != {'spec', 'directory', 'files', 'checkpoint_sha256'}:
            raise ValueError('Observation segment manifest shape changed')
        if not _same_json(entry['spec'], spec) or entry['checkpoint_sha256'] != self.header_sha:
            raise ValueError('Observation segment range, schema, ordinal or source binding changed')
        if not isinstance(entry['directory'], str) or not re.fullmatch('attempts/[0-9]{8}-[0-9a-f]{32}/closed', entry['directory']):
            raise ValueError('Observation segment directory is outside its owned attempt')
        directory = self.root / entry['directory']
        _existing_directory(directory)
        schemas = {spec['table'] + '.parquet': dataset_policy(spec['table']).subject_schema,
                   'etl_receipts.parquet': RECEIPT_SCHEMA}
        expected_names = set(schemas) | {'identity-context.json'}
        if set(entry['files']) != expected_names or {p.name for p in directory.iterdir()} != expected_names:
            raise ValueError('Observation segment file membership changed')
        for name in sorted(expected_names):
            check_resources()
            path = directory / name
            _file(path)
            if _sha(path) != entry['files'][name]:
                raise ValueError('Observation segment bytes changed')
            if name in schemas:
                with pq.ParquetFile(path) as reader:
                    if (reader.metadata.num_rows != spec['rows'] or
                            not reader.schema_arrow.equals(schemas[name], check_metadata=False)):
                        raise ValueError('Observation segment schema or membership changed')
        context = json.loads((directory / 'identity-context.json').read_text())
        if not _same_json(context, dict(generation_id=self.generation_id,
                           policies=[dataset_policy(spec['table']).descriptor()],
                           subject_rows={spec['table']: spec['rows']}, receipt_rows=spec['rows'])):
            raise ValueError('Observation segment writer evidence changed')
        return directory

    def prepare_table(self, table, selected, witnesses, schema, output, *, prepare_row, check_resources):
        """Rehash admitted inputs, reuse closed ranges, and decode only new groups."""
        from spicy_regs.fec_native_conversion import _bounded_record_batches
        policy = dataset_policy(table)
        ordinal, directories = 0, []
        for member_index, (item, witness) in enumerate(zip(selected, witnesses, strict=True)):
            before = file_signature(item.path)
            check_resources()
            if _sha(item.path) != item.sha256 or file_signature(item.path) != before:
                raise ValueError('Observation segment input bytes changed')
            with pq.ParquetFile(item.path) as source:
                if source.metadata.num_rows != item.rows or not source.schema_arrow.equals(schema, check_metadata=True):
                    raise ValueError('Observation segment input schema or membership changed')
                for group in range(source.metadata.num_row_groups):
                    check_resources()
                    count = source.metadata.row_group(group).num_rows
                    spec = dict(table=table, member=member_index, member_sha256=item.sha256,
                                witness=witness, row_group=group, rows=count, ordinal_start=ordinal,
                                attempt_offset=self.attempt_offset, schema=base64.b64encode(schema.serialize()).decode())
                    if self.position < len(self.closed):
                        entry = json.loads(self.closed[self.position].read_text())
                        directory = self._verify_closed(entry, spec, check_resources)
                    else:
                        attempt = self.root / 'attempts' / (f'{self.position:08d}-' + uuid4().hex)
                        _directory(attempt)
                        directory = attempt / 'closed'
                        def records():
                            local = 0
                            for batch in source.iter_batches(batch_size=32, row_groups=[group], use_threads=False):
                                check_resources()
                                for part in _bounded_record_batches(batch):
                                    check_resources()
                                    for original in part.to_pylist():
                                        row, source_input = prepare_row(original)
                                        yield row, dict(input_witness=witness, source_input=source_input,
                                                        observation_ordinal=ordinal + local)
                                        local += 1
                            if local != count:
                                raise ValueError('Observation segment changed row-group membership')
                        with IdentityReceiptWriter(directory, generation_id=self.generation_id,
                                tables=[table], attempt_offset=self.attempt_offset) as writer:
                            if table == 'fec_source_records':
                                writer.emit_batch(table, records(), check_resources=check_resources)
                            else:
                                for row, kwargs in records():
                                    writer.emit(table, row, **kwargs)
                        if file_signature(item.path) != before:
                            raise ValueError('Observation input changed during segment conversion')
                        files = {}
                        for path in sorted(directory.iterdir()):
                            check_resources()
                            _file(path)
                            with path.open('rb') as stream:
                                os.fsync(stream.fileno())
                            files[path.name] = _sha(path)
                        _sync_directory(directory)
                        _sync_directory(attempt)
                        entry = dict(spec=spec, directory=directory.relative_to(self.root).as_posix(),
                                     files=files, checkpoint_sha256=self.header_sha)
                        directory = self._verify_closed(entry, spec, check_resources)
                        record = self.root / 'closed' / f'{self.position:08d}.json'
                        _write_new(record, entry)
                        self.closed.append(record)
                    if file_signature(item.path) != before:
                        raise ValueError('Observation input changed during segment readback')
                    directories.append((directory, entry['files']))
                    self.position += 1
                    ordinal += count
                    self.attempt_offset += count
            check_resources()
            if file_signature(item.path) != before or _sha(item.path) != item.sha256:
                raise ValueError('Observation input changed before table completion')
        output.mkdir(parents=True, exist_ok=False)
        for name, expected_schema in ((table + '.parquet', policy.subject_schema), ('etl_receipts.parquet', RECEIPT_SCHEMA)):
            with pq.ParquetWriter(output / name, expected_schema, compression='zstd') as writer:
                for directory, hashes in directories:
                    check_resources()
                    path = directory / name
                    if _sha(path) != hashes[name]:
                        raise ValueError('Observation segment changed before consolidation')
                    with pq.ParquetFile(path) as source:
                        for batch in source.iter_batches(batch_size=32, use_threads=False):
                            check_resources()
                            writer.write_batch(batch)
                    if _sha(path) != hashes[name]:
                        raise ValueError('Observation segment changed during consolidation')
        return output / (table + '.parquet'), output / 'etl_receipts.parquet'

    def finish(self):
        if self.position != len(self.closed):
            raise ValueError('Observation checkpoint has overlapping or extra closed segments')
