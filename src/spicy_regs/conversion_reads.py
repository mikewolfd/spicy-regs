"""A bounded admitted family reader owned by one explicit conversion operation.

Ordinary table readers keep their independent, narrow admission. This lifetime
reuses the same maintained admission and matching replay for complete conversions.
"""
from contextlib import ExitStack, closing, contextmanager
import json
from itertools import islice
from pathlib import Path
import sqlite3
from tempfile import TemporaryDirectory
from time import perf_counter

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import (
    _bundle_policies, _index_processing, _joined_subjects, _load_receipts, _unpack,
)
from spicy_regs.runtime_bounds import checkpoint


class _FamilyReader:
    def __init__(self, connection, subjects, policies, guard, *, row_reference):
        self.connection, self.subjects, self.policies = connection, subjects, policies
        self.guard, self.row_reference = guard, row_reference
        self.active, self.reading = True, False
        self.admission_seconds = 0.0

    def check(self):
        checkpoint()
        self._require_active()
        self.guard()

    def _require_active(self):
        if not self.active:
            raise ValueError("Conversion family reader is closed")

    def _live_rows(self, rows):
        iterator = iter(rows)
        try:
            while True:
                self._require_active()
                try:
                    row = next(iterator)
                except StopIteration:
                    return
                self._require_active()
                yield row
        finally:
            close = getattr(iterator, 'close', None)
            if close is not None:
                try:
                    close()
                except (etl_bulk.duckdb.Error, sqlite3.Error):
                    if self.active:
                        raise

    @contextmanager
    def _stream(self):
        self.check()
        if self.reading:
            raise ValueError("Finish the active family stream before another read")
        self.reading = True
        try:
            yield
            self.check()
        finally:
            self.reading = False

    def read_subjects(self, dataset):
        """Exercise the shared exact matcher using this operation's admitted state."""
        policy, paths = self.policies[dataset], self.subjects[dataset]
        with self._stream():
            if self.row_reference:
                self.connection.execute("UPDATE receipts SET used=0 WHERE dataset=?", [dataset])
                try:
                    for _, subject, processing in self._live_rows(_joined_subjects(
                            self.connection, {dataset: paths}, {dataset: policy})):
                        yield subject | processing
                finally:
                    if self.active:
                        # Admission already proved every dataset. A cancelled
                        # replay must not leave unrelated reads with unused flags.
                        self.connection.execute("UPDATE receipts SET used=1 WHERE dataset=?", [dataset])
            else:
                first = self.connection.execute(
                    "SELECT coalesce(min(n),0) FROM subjects WHERE dataset=?", [dataset]).fetchone()[0]
                yield from self._live_rows(etl_bulk._replay_admitted_subjects(self.connection, paths, policy, start=first))

    def processing(self, dataset, *, outcomes=None):
        """Visit admitted original processing fields in receipt order without rereading Parquet."""
        if dataset not in self.policies:
            raise ValueError("Dataset is outside the admitted conversion family")
        with self._stream():
            if self.row_reference:
                rows = self.connection.execute(
                    "SELECT outcome,processing,diagnostics FROM receipts WHERE dataset=? ORDER BY rowid", [dataset])
                for outcome, processing, diagnostics in self._live_rows(rows):
                    checkpoint()
                    if outcomes is None or outcome in outcomes:
                        yield {"outcome": outcome, "processing_fields": _index_processing(processing),
                               "diagnostics": _index_processing(diagnostics)}
            else:
                query = "SELECT outcome,processing_json,diagnostic_json FROM receipts WHERE dataset=? ORDER BY n"
                for batch in self._live_rows(self.connection.execute(query, [dataset]).to_arrow_reader(etl_bulk._BATCH)):
                    checkpoint()
                    for outcome, processing, diagnostics in zip(*(batch.column(i).to_pylist() for i in range(3)), strict=True):
                        self._require_active()
                        if outcomes is None or outcome in outcomes:
                            yield {"outcome": outcome, "processing_fields": _unpack(json.loads(processing)),
                                   "diagnostics": _unpack(json.loads(diagnostics))}

    def outcome_counts(self):
        """Count actual admitted receipt rows without decoding the source log again."""
        with self._stream():
            return {outcome: count for outcome, count in self.connection.execute(
                "SELECT outcome,count(*) FROM receipts GROUP BY outcome").fetchall()}

    def compare_original(self, dataset, original, *, source_schema=None):
        """Prove exact source members, Arrow schema/metadata, values, order and repetitions."""
        import pyarrow as pa
        import pyarrow.parquet as pq
        from spicy_regs.congress_subjects import INPUT_COLUMNS as congress
        from spicy_regs.legislative_documents import field_registry
        from spicy_regs.local_data import file_signature
        from spicy_regs.native_types import extra_field_checker

        self.check()
        original = Path(original)
        if original.is_symlink():
            raise ValueError("Retained original must not be a symlink")
        members = ({p.relative_to(original).as_posix(): p for p in sorted(original.rglob('*.parquet'))}
                   if original.is_dir() else {original.name: original})
        before = {name: file_signature(path) for name, path in members.items()}

        def compare(path, schema, rows):
            with pq.ParquetFile(path) as parquet:
                if not schema.equals(parquet.schema_arrow, check_metadata=True):
                    raise ValueError("Retained original schema or metadata changed")
                iterator = iter(rows)
                check = extra_field_checker(schema)
                for expected in parquet.iter_batches(batch_size=2000):
                    checkpoint()
                    batch = list(islice(iterator, expected.num_rows))
                    for row in batch:
                        check(row)
                    actual = pa.RecordBatch.from_pylist(batch, schema=schema)
                    if not expected.equals(actual):
                        raise ValueError("Restored processing values, order or repetitions differ from original")
                if next(iterator, None) is not None:
                    raise ValueError("Extra restored processing rows")

        if dataset in field_registry():
            from spicy_regs.legislative_receipts import FILE_STATES, retained_source_rows
            policy = self.policies[dataset]
            states = [r['processing_fields']['file_state'] for r in self.processing(
                FILE_STATES, outcomes=frozenset({'observed'}))
                if r['processing_fields']['file_state']['dataset'] == dataset]
            if not states or len({s['relative_path'] for s in states}) != len(states):
                raise ValueError("Missing or duplicate retained source file state")
            if states[0]['relative_path'] is None:
                if len(states) != 1 or not states[0]['partitioned'] or states[0]['rows'] != 0 or members or not original.is_dir():
                    raise ValueError("Invalid empty partitioned source state")
            elif any(s['partitioned'] != original.is_dir() for s in states):
                raise ValueError("Retained table physical membership changed")
            named = {s['relative_path'] if s['partitioned'] else original.name for s in states
                     if s['relative_path'] is not None}
            if named != set(members):
                raise ValueError("Retained table member set changed")
            records = ((r['processing_fields'] for r in self.processing(dataset, outcomes=frozenset({'observed'})))
                       if policy.receipt_only else self.read_subjects(dataset))
            rows = iter(retained_source_rows(dataset, policy, records))
            for state in states:
                if state['relative_path'] is None:
                    continue
                relative = Path(state['relative_path'])
                if relative.is_absolute() or '..' in relative.parts:
                    raise ValueError("Invalid retained member path")
                name = state['relative_path'] if state['partitioned'] else original.name
                path = members[name]
                if pq.read_metadata(path).num_rows != state['rows']:
                    raise ValueError("Retained source member row count changed")
                schema = pa.ipc.read_schema(pa.BufferReader(state['schema']))
                compare(path, schema, islice(rows, state['rows']))
            if next(rows, None) is not None:
                raise ValueError("Extra prior receipt inputs without retained source rows")
        else:
            if original.is_dir():
                raise ValueError("This source requires a single retained Parquet member")
            schema = pq.read_schema(original)
            if dataset in congress:
                from spicy_regs.congress_receipts import retained_source_rows
                # Exercise exact subject matching once before the retained source stream.
                for _ in self.read_subjects(dataset):
                    pass
                rows = retained_source_rows(self.processing(dataset), schema=schema)
            elif dataset.startswith('court_'):
                from spicy_regs.court_receipts import processing_rows
                schema = schema.remove_metadata()
                rows = processing_rows(self.read_subjects(dataset), dataset=dataset, schema=schema)
            else:
                from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS
                if dataset in SOURCE_COLUMNS:
                    rows = self._regulatory_original_rows(dataset, schema, source_schema=source_schema)
                else:
                    from spicy_regs.transforms.fec_identity_context_fields import REGISTRY
                    if dataset not in REGISTRY:
                        raise ValueError("No maintained original decoder for admitted dataset")
                    def fec_rows():
                        for row in self.read_subjects(dataset):
                            originals = row['conversion_inputs']
                            yield {name: originals.get(name, row.get(name)) for name in schema.names}
                    rows = fec_rows()
            compare(original, schema, rows)
        if {name: file_signature(path) for name, path in members.items()} != before:
            raise ValueError("Retained original changed during comparison")
        if original.is_dir() and set(p.relative_to(original).as_posix() for p in original.rglob('*.parquet')) != set(members):
            raise ValueError("Retained original member set changed during comparison")
        self.check()
        checked = _equal_original_report()
        if original.is_dir():
            checked.update(members={name: _equal_original_report() | {'schema_changed': False} for name in members},
                           members_only_in_restored=[], members_only_in_retained=[], schema_changes=[])
        return checked

    def _regulatory_original_rows(self, dataset, schema, *, source_schema=None):
        import pyarrow as pa
        from spicy_regs.transforms.regulations_receipts import _processor_inputs, processing_schema
        from spicy_regs.transforms.regulations_shape import SOURCE_COLUMNS, TYPES

        known = dict(SOURCE_COLUMNS[dataset])
        if len(set(schema.names)) != len(schema.names) or any(
                f.name not in known or f.type != TYPES[known[f.name]] for f in schema):
            raise ValueError("Exact regulatory source schema differs from declared input fields")
        expected_metadata = schema.metadata or {}
        restored_schema = processing_schema(dataset, source_schema=source_schema, metadata=expected_metadata)
        if not restored_schema.equals(schema, check_metadata=True):
            raise ValueError("Maintained regulatory restored schema differs from retained original")
        rows = iter(self.read_subjects(dataset))
        count = 0
        while batch := list(islice(rows, 2000)):
            for row in batch:
                if (pa.schema([], metadata=row.get('input_metadata', {})).metadata or {}) != expected_metadata:
                    raise ValueError("Regulatory input metadata differs from retained original")
            for raw in _processor_inputs(dataset, batch):
                if set(raw) != set(schema.names):
                    raise ValueError("Exact regulatory source field presence differs from retained original")
                count += 1
                yield raw
        if not count:
            held = None
            for attempt in self.processing(dataset, outcomes=frozenset({'observed'})):
                if attempt['diagnostics'].get('kind') == 'input_file_metadata':
                    metadata = pa.schema([], metadata=attempt['processing_fields']['input_metadata']).metadata or {}
                    if held is not None and held != metadata:
                        raise ValueError("Empty input metadata differs across selected receipts")
                    held = metadata
            if (held or {}) != expected_metadata:
                raise ValueError("Empty regulatory input metadata differs from retained original")


def _equal_original_report():
    """Ordered exact equality implies no bag, column, type or metadata differences."""
    return {'metadata_changes': [], 'columns_only_in_restored': [], 'columns_only_in_retained': [],
            'type_changes': {}, 'rows_only_in_restored': 0, 'rows_only_in_retained': 0}


@contextmanager
def _open_family_reader(subjects, receipts, policies, *, generation_id, guard):
    """Admit all supplied family inputs once, retain bounded processing, then close on every exit.

    The owning conversion supplies an immutable-source change guard. It also
    checks complete descriptor/membership agreement before reaching this helper.
    No saved passed flag or validation bypass can create this reader.
    """
    registered = _bundle_policies(subjects, receipts, policies)
    guard()
    start = perf_counter()
    with ExitStack() as stack:
        try:
            connection = stack.enter_context(etl_bulk._validated_bundle(
                subjects, receipts, policies, generation_id=generation_id, retain_processing=True))
            reference = False
        except etl_bulk.NotBulkEligible:
            temporary = Path(stack.enter_context(TemporaryDirectory(prefix="conversion-reference-")))
            connection = stack.enter_context(closing(sqlite3.connect(str(temporary / "joins.db"))))
            # The reference index has the same finite disk ceiling as bulk spill.
            page_size = connection.execute("PRAGMA page_size").fetchone()[0]
            connection.execute(f"PRAGMA max_page_count={32 * 1024**3 // page_size}")
            _load_receipts(connection, receipts, registered, generation_id, retain_diagnostics=True)
            for _ in _joined_subjects(connection, subjects, registered):
                pass
            reference = True
        guard()
        reader = _FamilyReader(connection, subjects, registered, guard, row_reference=reference)
        reader.admission_seconds = perf_counter() - start
        try:
            yield reader
            reader.check()
        finally:
            reader.active = False


class ConversionReadOperation:
    """Keep actual admitted generation state until this explicit operation closes.

    Build, full original comparisons and same-process publication share this
    lifetime. A restart or anonymous remote source creates a new operation.
    """
    def __init__(self):
        self._stack = ExitStack()
        self._generations = {}
        self._guards = {}
        self._reader_stacks = {}
        self.active = False

    def __enter__(self):
        self.active = True
        return self

    def __exit__(self, *error):
        try:
            return self._stack.__exit__(*error)
        finally:
            self.active = False

    def admit_generation(self, directory, *, expected_pin=None):
        """Hash every member once and retain the same product receipt admission."""
        from rulespec_artifacts import ArtifactPin, LocalFileState, LocalMemberSource
        from spicy_regs.generations import _verify_generation_source
        from spicy_regs.local_data import file_signature

        if not self.active:
            raise ValueError("Conversion read operation is closed")
        if isinstance(expected_pin, dict):
            if set(expected_pin) != {'logicalId', 'artifactDigest'}:
                raise ValueError("Invalid expected conversion artifact pin")
            expected_pin = ArtifactPin(expected_pin['logicalId'], expected_pin['artifactDigest'])
        directory = Path(directory).absolute()
        held = self._generations.get(directory)
        if held is not None:
            artifact, reader, source = held
            reader.check()
            if expected_pin is not None and artifact.pin != expected_pin:
                raise ValueError("Conversion generation differs from its expected pin")
            return held
        source = LocalMemberSource(directory)
        keys = tuple(source.keys())
        signatures = {key: file_signature(directory / key) for key in keys}

        def guard():
            if not self.active:
                raise ValueError("Conversion read operation is closed")
            if set(source.keys()) != set(keys) or any(
                    file_signature(directory / key) != signature for key, signature in signatures.items()):
                raise ValueError("Conversion generation member changed after admission")

        def footer(key):
            # Complete receipt/subject admission decodes every data page below.
            # Repeating that decode merely to count rows is unnecessary here.
            import duckdb
            import pyarrow.parquet as pq
            with source.open(key) as stream:
                parquet = pq.ParquetFile(stream)
                rows = parquet.metadata.num_rows
            with duckdb.connect() as connection:
                columns = connection.execute(
                    "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false)",
                    [str(directory / key)]).fetchall()
            return {"columns": [[name, kind] for name, kind, *_ in columns], "rows": rows}

        readers = []
        reader_stack = ExitStack()
        self._stack.enter_context(reader_stack)

        def admit(subjects, receipts, policies, *, generation_id):
            from spicy_regs.etl_receipts import receipt_policies
            from spicy_regs.subject_catalog import policies as installed_policies

            installed = installed_policies()
            for policy in policies:
                current = installed.get(policy.dataset)
                if current is None or not any(
                        policy.descriptor() == candidate.descriptor() for candidate in receipt_policies(current)):
                    raise ValueError("Conversion generation policy descriptor is not an exact shipped policy")
            reader = reader_stack.enter_context(_open_family_reader(
                subjects, receipts, policies, generation_id=generation_id, guard=guard))
            readers.append(reader)

        artifact = None
        try:
            artifact = _verify_generation_source(source, footer, expected_pin=expected_pin, admit_receipts=admit)
            if len(readers) != 1:
                raise ValueError("Conversion generation requires one complete receipt admission")
            guard()
            # Consume the platform's actual byte-pass receipts, not just a saved
            # success flag or a freshly observed file signature.
            if artifact.local_member_states is None or any(
                    LocalFileState.from_stat((directory / key).lstat()) != state
                    for key, state in artifact.local_member_states.items()):
                raise ValueError("Conversion generation differs from its verified local member states")
        except BaseException:
            try:
                reader_stack.close()
            finally:
                if artifact is not None and artifact.local_member_states is not None:
                    artifact.local_member_states.close()
            raise
        self._stack.callback(artifact.local_member_states.close)
        held = (artifact, readers[0], source)
        readers[0].family = artifact.root['spec']['family']
        self._generations[directory] = held
        self._guards[directory] = guard
        self._reader_stacks[directory] = reader_stack
        return held

    def verified_source(self, directory):
        """Return an actual byte-admitted source only while its original state remains current."""
        directory = Path(directory).absolute()
        if not self.active or directory not in self._generations:
            raise ValueError("Generation has no admission in this live conversion operation")
        self._guards[directory]()
        artifact, _, source = self._generations[directory]
        return artifact, source

    def release_generation(self, directory):
        """Release bounded replay state before admitting another source; retain guarded byte proof."""
        directory = Path(directory).absolute()
        self.verified_source(directory)
        self._reader_stacks[directory].close()

    def admit_public_generation(self, base, index, family, directory):
        """Fetch each actual credentialless remote member once, then independently admit those bytes."""
        from rulespec_artifacts import ArtifactPin, parse_admitted_json, sha256_digest, validate_object_key
        from spicy_regs.sources import publication

        if not self.active:
            raise ValueError("Conversion read operation is closed")
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=False)
        entry = index['families'][family]
        raw, root = publication.load_family_root(base, entry)
        (directory / 'artifact.json').write_bytes(raw)
        keys = set()
        for manifest in root['memberManifests']:
            key = validate_object_key(manifest['objectKey'], path='manifest.objectKey')
            raw = publication._bounded_get(f"{base.rstrip('/')}/{entry['prefix']}/{key}",
                                          allow_missing=False, limit=publication.EVIDENCE_CONTROL_LIMIT)
            if raw is None or sha256_digest(raw) != manifest['sha256']:
                raise ValueError("Public generation manifest differs from its pin")
            target = directory / key
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(raw)
            for member in parse_admitted_json(raw)['members']:
                member_key = validate_object_key(member['objectKey'], path='member.objectKey')
                if member_key in keys:
                    raise ValueError("Public generation repeats a member")
                keys.add(member_key)
                target = directory / member_key
                target.parent.mkdir(parents=True, exist_ok=True)
                descriptor = publication.Member(entry['prefix'] + '/' + member_key, member['sha256'],
                                                member['byteSize'], member.get('recordCount'))
                if not publication.fetch_member(base, descriptor, target):
                    raise ValueError("Public generation member is absent")
        return self.admit_generation(directory, expected_pin=ArtifactPin(entry['logicalId'], entry['artifactDigest']))
