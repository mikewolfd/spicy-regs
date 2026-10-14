"""Closed local preparation segments; only complete-family admission can publish."""
from contextlib import contextmanager
from contextvars import ContextVar
import base64
import json
import os
from pathlib import Path
import re
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.etl_receipts import RECEIPT_SCHEMA
from spicy_regs.generations import implementation_id
from spicy_regs.local_data import file_signature
from spicy_regs.transforms.assemble_fec_query import _sha
from spicy_regs.transforms.fec_identity_receipts import IdentityReceiptWriter, dataset_policy

FORMAT = 'fec-observation-segments/1'
_ORIGIN = ContextVar('fec_checkpoint_origin', default=None)


def coalesced_tables(batches, schema, *, check_resources, row_group_rows=8192, max_bytes=8 * 1024 * 1024):
    """Coalesce small Arrow views without decoding values or holding wide groups."""
    from spicy_regs.fec_native_conversion import _bounded_record_batches
    if type(row_group_rows) is not int or row_group_rows < 1 or type(max_bytes) is not int or max_bytes < 1:
        raise ValueError('Coalescing requires positive integer row and byte bounds')
    buffers, rows, size = [], 0, 0
    for batch in batches:
        check_resources()
        for part in _bounded_record_batches(batch, max_bytes=max_bytes):
            offset = 0
            while offset < part.num_rows:
                count = min(part.num_rows - offset, row_group_rows - rows)
                view = part.slice(offset, count)
                if buffers and size + view.nbytes > max_bytes:
                    yield pa.Table.from_batches(buffers, schema=schema)
                    buffers, rows, size = [], 0, 0
                    continue
                buffers.append(view)
                rows += count
                size += view.nbytes
                offset += count
                if rows == row_group_rows or size >= max_bytes:
                    yield pa.Table.from_batches(buffers, schema=schema)
                    buffers, rows, size = [], 0, 0
    if buffers:
        yield pa.Table.from_batches(buffers, schema=schema)


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


def _check_resource_refactor(previous, current):
    """Prove that the resource split preserves the complete original kernel."""
    import ast
    old, new = ast.parse(previous), ast.parse(current)
    additions = {'_check_resource_refactor', 'verify_checkpoint_origin', '_checkpoint_implementation',
                 '_checkpoint_source_pin', 'checkpoint_executor'}
    new.body = [node for node in new.body if not isinstance(node, ast.FunctionDef) or node.name not in additions]
    extra = ast.parse("from contextvars import ContextVar\n_ORIGIN = ContextVar('fec_checkpoint_origin', default=None)").body
    for expected in extra:
        matches = [node for node in new.body if ast.dump(node) == ast.dump(expected)]
        if len(matches) != 1:
            raise ValueError('Checkpoint executor scope changed')
        new.body.remove(matches[0])
    cls = next(node for node in new.body if isinstance(node, ast.ClassDef) and node.name == 'ObservationSegments')
    prepared = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == 'prepare_segments')
    assembled = next(node for node in cls.body if isinstance(node, ast.FunctionDef) and node.name == 'prepare_table')
    expected = ast.parse("""directories = self.prepare_segments(table, selected, witnesses, schema,
        prepare_row=prepare_row, check_resources=check_resources)
policy = dataset_policy(table)
""").body
    if (ast.dump(ast.Module(body=assembled.body[1:3], type_ignores=[])) != ast.dump(ast.Module(body=expected, type_ignores=[]))
            or ast.dump(prepared.body[-1]) != ast.dump(ast.parse('return directories').body[0])):
        raise ValueError('Checkpoint executor has no exact preparation split')
    # Its new API must expose exactly the old arguments except assembly output.
    definition = ast.parse('def prepare_segments(self, table, selected, witnesses, schema, *, prepare_row, check_resources): pass').body[0]
    assert isinstance(definition, ast.FunctionDef)
    args = definition.args
    if ast.dump(prepared.args) != ast.dump(args):
        raise ValueError('Checkpoint executor preparation arguments changed')
    assembled.body = prepared.body[:-1] + assembled.body[3:]
    cls.body.remove(prepared)
    context = next(node for node in new.body if isinstance(node, ast.FunctionDef) and node.name == 'observation_segments')
    if context.args.kwonlyargs[-1].arg != 'origin' or ast.dump(context.args.kw_defaults[-1]) != ast.dump(ast.Constant(None)):
        raise ValueError('Checkpoint executor origin argument changed')
    context.args.kwonlyargs.pop()
    context.args.kw_defaults.pop()
    extra = ast.parse("""origin = origin or _ORIGIN.get()
source_pin = _checkpoint_source_pin(path, captured_family, source_pin, origin)
""").body
    for expected in extra:
        containers = [value for node in ast.walk(context) for _, value in ast.iter_fields(node) if isinstance(value, list)]
        matches = [(container, node) for container in containers for node in container
                   if isinstance(node, ast.AST) and ast.dump(node) == ast.dump(expected)]
        if len(matches) != 1:
            raise ValueError('Checkpoint executor source scope changed')
        matches[0][0].remove(matches[0][1])
    replacements = 0
    for node in ast.walk(context):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == '_checkpoint_implementation':
            expected = ast.parse('_checkpoint_implementation(path, captured_family, source_pin, origin)', mode='eval').body
            if ast.dump(node) != ast.dump(expected):
                raise ValueError('Checkpoint executor origin gate changed')
            node.func.id = 'implementation_id'
            node.args = []
            replacements += 1
    if replacements != 1 or ast.dump(old) != ast.dump(new):
        raise ValueError('Checkpoint executor changes the frozen segment kernel')


def verify_checkpoint_origin(path, captured_family, source_pin, origin):
    """Admit one explicit immutable checkpoint against a distinct clean executor."""
    import subprocess
    from spicy_regs.native_conversion import source_state, _checkpoint_producer, _historical_producer
    if (not isinstance(origin, dict) or set(origin) != {'commit', 'header_sha256', 'executor_commit', 'remote'}
            or any(not isinstance(origin[name], str) or not re.fullmatch('[0-9a-f]{40}', origin[name])
                   for name in ('commit', 'executor_commit'))
            or not re.fullmatch('sha256:[0-9a-f]{64}', origin['header_sha256'])):
        raise ValueError('Checkpoint origin requires exact producer, executor and header pins')
    header = Path(path) / 'checkpoint.json'
    _file(header)
    if _sha(header) != origin['header_sha256']:
        raise ValueError('Checkpoint original header bytes changed')
    value = json.loads(header.read_text())
    identity = value['identity']
    if (identity['captured_family'] != captured_family
            or identity['source_pin']['checkout'] != origin['commit']):
        raise ValueError('Checkpoint original producer or selected inputs changed')
    now = source_state(origin['remote'])
    original_source = identity['source_pin']
    if source_pin not in (original_source, {k: v for k, v in now.items() if k != 'main'}):
        raise ValueError('Checkpoint source pin names neither admitted role')
    source_pin = original_source
    executor = _checkpoint_producer(now, origin['executor_commit'], source_pin['pinned_spicy_docs'])
    if (source_pin['uncommitted'] or source_pin['untracked'] or source_pin['remote'] != now['remote']
            or source_pin['spicy_docs'] != now['spicy_docs']
            or source_pin['spicy_docs_wheel'] != now['spicy_docs_wheel']):
        raise ValueError('Checkpoint original runtime or repository differs from executor')
    root = Path(__file__).resolve().parents[2]
    original_identity, locked = _historical_producer(root, origin['commit'], now['checkout'])
    if identity['implementation'] != original_identity:
        raise ValueError('Checkpoint original implementation differs from retained producer')
    def git(*args):
        return subprocess.run(['git', '-C', str(root), *args], check=True, capture_output=True,
                              env={**os.environ, 'GIT_OPTIONAL_LOCKS': '0'}).stdout.decode()
    changed = git('diff', '--name-only', origin['commit'], now['checkout'], '--', 'src', 'data_dictionary', 'uv.lock').splitlines()
    resource = 'src/spicy_regs/fec_observation_segments.py'
    if changed != [resource]:
        raise ValueError('Checkpoint transformation, writer, schema, policy or runtime changed')
    _check_resource_refactor(git('show', origin['commit'] + ':' + resource), Path(__file__).read_text())
    return {'originalProducer': {'commit': origin['commit'], 'implementationId': original_identity,
                                'lockedSpicyDocs': locked},
            'executor': executor, 'executorSource': now, 'originalHeaderSHA256': origin['header_sha256'],
            'originalSourcePin': original_source,
            'compatibility': 'exact-resource-split-with-frozen-transformations/1'}


@contextmanager
def checkpoint_executor(origin):
    """Scope an explicitly pinned original checkpoint to one distinct executor."""
    token = _ORIGIN.set(origin)
    try:
        yield
    finally:
        _ORIGIN.reset(token)


def _checkpoint_source_pin(path, captured_family, source_pin, origin):
    if origin is None:
        return source_pin
    return verify_checkpoint_origin(path, captured_family, source_pin, origin)['originalSourcePin']


def _checkpoint_implementation(path, captured_family, source_pin, origin):
    if origin is None:
        return implementation_id()
    return verify_checkpoint_origin(path, captured_family, source_pin, origin)['originalProducer']['implementationId']


@contextmanager
def observation_segments(path, *, captured_family, source_pin, hosted_main=None, origin=None):
    """Serialize all preparation and readback; a dead owner releases its lock."""
    import fcntl
    origin = origin or _ORIGIN.get()
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
        source_pin = _checkpoint_source_pin(path, captured_family, source_pin, origin)
        identity = dict(format=FORMAT, captured_family=captured_family, source_pin=source_pin,
                        implementation=_checkpoint_implementation(path, captured_family, source_pin, origin),
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
        if hosted_main is not None:
            from spicy_regs.native_conversion import _checkpoint_ancestor
            history = path / 'hosted-main-observations'
            _directory(history)
            records = sorted(p for p in history.iterdir() if not p.name.startswith('.pending-'))
            if [p.name for p in records] != [f'{i:08d}.json' for i in range(len(records))]:
                raise ValueError('Checkpoint hosted-main observation history has a gap')
            previous = source_pin['checkout']
            header_sha = _sha(header)
            for record in records:
                _file(record)
                observed = json.loads(record.read_text())
                if (not isinstance(observed, dict) or set(observed) != {'checkpoint_sha256', 'main'}
                        or observed['checkpoint_sha256'] != header_sha):
                    raise ValueError('Checkpoint hosted-main observation binding changed')
                _checkpoint_ancestor(previous, observed['main'])
                previous = observed['main']
            _checkpoint_ancestor(previous, hosted_main)
            _write_new(history / f'{len(records):08d}.json', dict(checkpoint_sha256=header_sha, main=hosted_main))
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

    def prepare_segments(self, table, selected, witnesses, schema, *, prepare_row, check_resources):
        """Rehash admitted inputs, reuse closed ranges, and decode only new groups."""
        from spicy_regs.fec_native_conversion import _bounded_record_batches
        policy = dataset_policy(table)  # noqa: F841 - retain the original lookup in the verified prefix
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
        return directories

    def prepare_table(self, table, selected, witnesses, schema, output, *, prepare_row, check_resources):
        """Prepare verified segments, then consolidate a complete table once."""
        directories = self.prepare_segments(table, selected, witnesses, schema,
            prepare_row=prepare_row, check_resources=check_resources)
        policy = dataset_policy(table)
        output.mkdir(parents=True, exist_ok=False)
        for name, expected_schema in ((table + '.parquet', policy.subject_schema), ('etl_receipts.parquet', RECEIPT_SCHEMA)):
            def batches():
                for directory, hashes in directories:
                    check_resources()
                    path = directory / name
                    if _sha(path) != hashes[name]:
                        raise ValueError('Observation segment changed before consolidation')
                    with pq.ParquetFile(path) as source:
                        yield from source.iter_batches(batch_size=32, use_threads=False)
                    if _sha(path) != hashes[name]:
                        raise ValueError('Observation segment changed during consolidation')
            with pq.ParquetWriter(output / name, expected_schema, compression='zstd') as writer:
                for held in coalesced_tables(batches(), expected_schema, check_resources=check_resources):
                    check_resources()
                    writer.write_table(held, row_group_size=8192)
        return output / (table + '.parquet'), output / 'etl_receipts.parquet'

    def finish(self):
        if self.position != len(self.closed):
            raise ValueError('Observation checkpoint has overlapping or extra closed segments')
