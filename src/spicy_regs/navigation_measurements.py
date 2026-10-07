"""Incremental, pinned main-field measurements using canonical navigation recipes.

Member projections and occurrences are reusable independently. Aggregate results
bind every selected member, schema, generation and rule. This reads explicit
local main files only; receipt tables and acquisition are outside this adapter.
"""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
import hashlib
import inspect
import json
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory
from urllib.parse import urlsplit
from uuid import uuid4

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import explorer_navigation as navigation
from spicy_regs.explorer_metadata import canonical_bytes
from spicy_regs.local_data import assert_local_members_unchanged, file_signature
from spicy_regs.native_types import described_schema
from spicy_regs.relationship_views.core import literal, quoted
from spicy_regs.sources.publication import parse_index, table_descriptor, table_members, table_pin

VERSION = 'main-navigation-measurements/1'


def digest(value):
    return 'sha256:' + hashlib.sha256(canonical_bytes(value)).hexdigest()


def count_value(value):
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def implementation_digest():
    # Declaration changes invalidate their own recipe, not every population.
    functions = (navigation.scalar, navigation.at, navigation.word, navigation.target_keys,
                 field_elements, identity_key, MeasurementCache._key_member)
    return digest([VERSION, duckdb.__version__, pa.__version__, *[inspect.getsource(f) for f in functions]])


def recipe_digest(spec, target_index, source_identity):
    return digest({key: spec.get(key) for key in ('id', 'source', 'field', 'fields', 'mode', 'candidates', 'ruleVersion', 'equality')} |
                  {'target': {k: spec['targets'][target_index][k] for k in ('table', 'columns', 'keys', 'guards')}, 'sourceIdentity': list(source_identity),
                   'implementation': implementation_digest()})


def selected_binding(index, table, extra_tables=None):
    descriptor = table_descriptor(index, table + '.parquet')
    if descriptor is None:
        extra = (extra_tables or {}).get(table, {})
        descriptor = extra.get('descriptor')
        if (not descriptor or extra.get('publicationIdentity') != digest(descriptor)
                or not extra.get('publicationSchema') or not descriptor.get('members')):
            raise ValueError('Unpinned main table cannot be measured: ' + table)
        return {'table': table, 'pin': {'publicationIdentity': extra['publicationIdentity'],
                                      'descriptor': descriptor},
                'schema': extra['publicationSchema'], 'rows': sum(m['rows'] for m in descriptor['members']),
                'members': descriptor['members']}
    return {'table': table, 'pin': table_pin(index, table + '.parquet'),
            'schema': descriptor['columns'], 'rows': descriptor['rows'],
            'members': [{'path': m.path, 'sha256': m.sha256, 'byteSize': m.byte_size, 'rows': m.rows}
                        for m in table_members(index, table + '.parquet')]}


def identity_key(row, columns):
    if not columns or any(row.get(c) is None for c in columns):
        return None
    values = [navigation.scalar(row.get(c)) for c in columns]
    return json.dumps(values, separators=(',', ':')) if None not in values else None


def field_elements(spec, row):
    """Retained audit extraction, sharing all key semantics with target_keys."""
    if spec['mode'] == 'row':
        return 'row', [(0, row)]
    field = spec.get('field') or next((f for f in spec['fields'] if f in row), None)
    if field is None:
        return 'unpublished', []
    value = row[field]
    if value is None:
        return 'sql_null', []
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return 'malformed_json', []
        if value is None:
            return 'json_null', []
    if not isinstance(value, list):
        return 'unsupported_shape', []
    # Keep original ordinal, including refused candidate shapes.
    return ('populated_array' if value else 'empty_array'), list(enumerate(value))


class MeasurementCache:
    """Local derived cache; completed entries replace temporary outputs atomically."""
    def __init__(self, directory, *, max_input_bytes=256 * 1024**2, max_projected_bytes=32 * 1024**2):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.max_input_bytes = max_input_bytes
        self.max_projected_bytes = max_projected_bytes
        self.work = Counter()

    @contextmanager
    def _connection(self, *, preserve_order=False):
        with TemporaryDirectory(prefix='measurement-spill-', dir=self.directory) as spill:
            with duckdb.connect(config={'threads': 1, 'memory_limit': '256MiB',
                                        'preserve_insertion_order': preserve_order,
                                        'temp_directory': spill, 'max_temp_directory_size': '1GiB'}) as con:
                yield con

    def _path(self, kind, dependency):
        return self.directory / (kind + '-' + digest(dependency).removeprefix('sha256:') + '.json')

    def _read(self, kind, dependency):
        path = self._path(kind, dependency)
        try:
            entry = json.loads(path.read_text())
            payload = entry['payload']
            if (payload.get('status') != 'complete' or entry['dependency'] != dependency
                    or entry['sha256'] != digest(payload)):
                return None
            artifacts = ([payload['artifact']] if payload.get('artifact') else []) + payload.get('occurrences', [])
            populations = [payload]
            if kind == 'result':
                populations += [payload.get('sourceIdentity', {}).get('population') or {},
                                payload.get('targetPopulation', {})]
            imported = [p['populationProof'] for p in populations if p.get('populationProof')]
            if imported:
                guard = digest([VERSION, inspect.getsource(self.import_population)])
                if any(proof.get('authority') != 'imported_existing_full_population'
                       or proof.get('guardImplementation') != guard for proof in imported):
                    return None
            artifacts += [proof['artifact'] for proof in imported]
            for artifact in artifacts:
                if file_signature(Path(artifact['path'])) != artifact['signature']:
                    return None
            self.work[kind + '_hits'] += 1
            return payload
        except (OSError, ValueError, KeyError, TypeError, RuntimeError):
            return None

    def _write(self, kind, dependency, payload):
        payload = {'status': 'complete', **payload}
        entry = {'dependency': dependency, 'payload': payload, 'sha256': digest(payload)}
        with NamedTemporaryFile(dir=self.directory, suffix='.partial.json', mode='w', delete=False) as stream:
            json.dump(entry, stream, sort_keys=True)
            temporary = Path(stream.name)
        temporary.replace(self._path(kind, dependency))
        return payload

    def _artifact(self, path):
        before = file_signature(path)
        with path.open('rb') as stream:
            checksum = 'sha256:' + hashlib.file_digest(stream, 'sha256').hexdigest()
        assert_local_members_unchanged({str(path): before})
        return {'path': str(path), 'sha256': checksum, 'signature': before}

    def admit(self, binding, paths):
        """Every pinned member must pass byte, footer, schema and summed-row checks."""
        admitted, total = [], 0
        for member in binding['members']:
            if member['path'] not in paths:
                raise ValueError('Incomplete selected member set: ' + member['path'])
            path = Path(paths[member['path']]).absolute()
            signature = file_signature(path)
            if path.stat().st_size != member['byteSize']:
                raise ValueError('Member byte size differs from publication')
            dependency = {'path': str(path), 'signature': signature, 'member': member, 'schema': binding['schema']}
            held = self._read('admission', dependency)
            if held is None:
                if member['byteSize'] > self.max_input_bytes:
                    raise ValueError('Unadmitted member exceeds explicit input-byte limit')
                with path.open('rb') as stream:
                    checksum = 'sha256:' + hashlib.file_digest(stream, 'sha256').hexdigest()
                if checksum != member['sha256']:
                    raise ValueError('Member SHA differs from publication')
                self.work['member_hashes'] += 1
            with pq.ParquetFile(path) as parquet:
                columns = [list(pair) for pair in described_schema(parquet.schema_arrow)]
                if parquet.metadata.num_rows != member['rows'] or columns != binding['schema']:
                    raise ValueError('Member footer count or schema differs from publication')
            assert_local_members_unchanged({str(path): signature})
            admitted.append({'member': member, 'path': str(path), 'signature': signature})
            total += member['rows']
            if held is None:
                self._write('admission', dependency, {'complete': True})
        if total != binding['rows']:
            raise ValueError('Summed selected member rows differ from publication')
        return admitted

    def project(self, binding, admitted, columns):
        columns = sorted(set(columns))
        if not columns or not set(columns) <= set(dict(binding['schema'])):
            raise ValueError('Required main projection fields are not published')
        outputs = []
        for item in admitted:
            # Content and schema own projection identity. A carried member in a
            # new generation can reuse its projection but not an aggregate proof.
            dependency = {'version': VERSION, 'member': {k: item['member'][k] for k in ('sha256', 'rows', 'byteSize')},
                          'schema': binding['schema'], 'columns': columns}
            held = self._read('projection', dependency)
            if held is None:
                with pq.ParquetFile(item['path']) as parquet:
                    compressed = sum(parquet.metadata.row_group(g).column(c).total_compressed_size
                                     for g in range(parquet.num_row_groups)
                                     for c in range(parquet.metadata.row_group(g).num_columns)
                                     if parquet.metadata.row_group(g).column(c).path_in_schema.split('.')[0] in columns)
                if compressed > self.max_projected_bytes:
                    raise ValueError('Projection exceeds explicit compressed-byte limit')
                output = self._path('projection', dependency).with_suffix('.parquet')
                temporary = output.with_suffix('.' + uuid4().hex + '.partial.parquet')
                with self._connection(preserve_order=True) as con:
                    names = ','.join(quoted(c) for c in columns)
                    con.execute(f'COPY (SELECT {names} FROM read_parquet($input_file, hive_partitioning=false)) TO $output_file (FORMAT PARQUET)',
                                {'input_file': item['path'], 'output_file': str(temporary)})
                assert_local_members_unchanged({item['path']: item['signature']})
                temporary.replace(output)
                held = self._write('projection', dependency, {'artifact': self._artifact(output),
                                                              'rows': item['member']['rows'], 'columns': columns})
                self.work['member_projections'] += 1
            outputs.append(held)
        return outputs

    def capture_population_proof(self, path):
        """Capture one explicitly supplied earlier full-native-identity report."""
        path = Path(path).absolute()
        before = file_signature(path)
        if path.stat().st_size > 8 * 1024**2:
            raise ValueError('Population proof exceeds the explicit 8MiB report limit')
        with path.open('rb') as stream:
            raw = stream.read()
        assert_local_members_unchanged({str(path): before})
        document = json.loads(raw)
        if not isinstance(document, dict) or not isinstance(document.get('sources'), list):
            raise ValueError('Population proof requires explicit source identity records')
        artifact = {'path': str(path), 'sha256': 'sha256:' + hashlib.sha256(raw).hexdigest(), 'signature': before}
        return {'artifact': artifact, 'sources': document['sources'],
                'recordSetDigest': digest(document['sources']), 'resourceScope': document.get('resourceScope')}

    def import_population(self, binding, paths, columns, proof):
        """Reuse earlier counts only after current exact member admission.

        This accepts only complete non-null unique native key populations. It
        does not claim that the earlier grouping ran under this cache's limits.
        """
        admitted = self.admit(binding, paths)
        artifact = proof['artifact']
        assert_local_members_unchanged({artifact['path']: artifact['signature']})
        if digest(proof['sources']) != proof['recordSetDigest']:
            raise ValueError('Captured population proof records changed')
        candidates = [record for record in proof['sources'] if isinstance(record, dict)
                      and record.get('source') == binding['table']]
        if len(candidates) != 1:
            raise ValueError('Population proof requires one exact source table record')
        record, = candidates
        keys = record.get('identityColumns')
        if (not isinstance(keys, list) or not keys or any(not isinstance(k, str) for k in keys)
                or len(set(keys)) != len(keys) or sorted(keys) != sorted(columns)
                or not set(keys) <= set(dict(binding['schema']))):
            raise ValueError('Population proof differs from the exact requested native key columns')
        # The older inventory permits declared nullable native key fields.
        # Its non-null count excludes those fields, so it cannot qualify this
        # cache's stricter all-key-fields-present population.
        if record.get('maintainedNullableIdentityFields') != []:
            raise ValueError('Population proof permits nullable or unspecified native identity fields')
        if (record.get('status') != 'verified-full-main-complete-source-identity'
                or record.get('missingIdentityColumns') != []
                or record.get('globalUniqueCompleteSourceIdentity') is not True
                or record.get('globalUniqueNonNullCompleteSourceIdentity') is not True):
            raise ValueError('Population proof is not a completed full non-null unique native identity population')
        fields = ('sourceRows', 'completeIdentityAdmittedRows', 'completeIdentityNonNullRows',
                  'distinctCompleteSourceIdentities', 'duplicateCompleteIdentityRows', 'sourceRowsWithIncompleteIdentity')
        if (any(not count_value(record.get(field)) for field in fields)
                or any(record[field] != binding['rows'] for field in fields[:4])
                or any(record[field] for field in fields[4:])):
            raise ValueError('Population proof counts do not establish the complete selected identity population')

        def member_pin(member):
            if not isinstance(member, dict):
                raise ValueError('Population proof has an invalid member pin')
            path = member.get('path')
            if path is None:
                if not isinstance(member.get('url'), str):
                    raise ValueError('Population proof requires exact selected member locators')
                url = urlsplit(member['url'])
                if not url.netloc or url.scheme not in ('https', 'http') or url.query or url.fragment:
                    raise ValueError('Population proof requires exact selected member locators')
                path = url.path.lstrip('/')
            if not isinstance(path, str) or not path:
                raise ValueError('Population proof requires exact selected member locators')
            return {'path': path, 'sha256': member.get('sha256'),
                    'byteSize': member.get('byteSize'), 'rows': member.get('rows')}

        selected = sorted(canonical_bytes(member_pin(member)) for member in binding['members'])
        previous = record.get('sourcePin')
        admissions = record.get('admissions')
        if (not isinstance(previous, list) or not isinstance(admissions, list)
                or not previous or len(previous) != len(admissions)
                or selected != sorted(canonical_bytes(member_pin(m)) for m in previous)
                or selected != sorted(canonical_bytes(member_pin(m.get('member'))) for m in admissions
                                      if isinstance(m, dict))):
            raise ValueError('Population proof differs from the complete selected member set')
        # Exact member hashes seal the earlier physical schema; fresh admit
        # checks that every current footer matches this selected schema.
        assert_local_members_unchanged({item['path']: item['signature'] for item in admitted})
        dependency = {'binding': binding, 'columns': sorted(columns),
                      'implementation': digest([VERSION, duckdb.__version__, inspect.getsource(self.population)])}
        held = self._read('population', dependency)
        if held is not None:
            return held
        self.work['population_imports'] += 1
        return self._write('population', dependency, {
            'rows': binding['rows'], 'nonNullRows': binding['rows'], 'nullKeyRows': 0,
            'distinctKeys': binding['rows'], 'duplicateKeys': 0,
            'maximumRowsPerKey': 1 if binding['rows'] else 0, 'scope': 'full_selected_inputs',
            'populationProof': {'authority': 'imported_existing_full_population', 'artifact': artifact,
                                'guardImplementation': digest([VERSION, inspect.getsource(self.import_population)]),
                                'sourceRecordDigest': digest(record), 'identityColumns': keys,
                                'physicalSchema': binding['schema'], 'resourceScope': proof['resourceScope'],
                                'execution': 'Earlier native identity grouping reused; no new grouping execution claimed.'}})

    def population(self, binding, admitted, columns):
        """Cache full selected non-null key populations independently of routes."""
        dependency = {'binding': binding, 'columns': sorted(columns),
                      'implementation': digest([VERSION, duckdb.__version__, inspect.getsource(self.population)])}
        held = self._read('population', dependency)
        if held is not None:
            return held
        projections = self.project(binding, admitted, columns)
        paths = [p['artifact']['path'] for p in projections]
        names = ','.join(quoted(c) for c in columns)
        nonnull = ' AND '.join(quoted(c) + ' IS NOT NULL' for c in columns)
        with self._connection() as con:
            counts = con.execute(f'''WITH rows AS NOT MATERIALIZED (SELECT {names} FROM read_parquet(?, hive_partitioning=false)),
              groups AS (SELECT {names},count(*) n FROM rows WHERE {nonnull} GROUP BY ALL)
              SELECT (SELECT count(*) FROM rows),(SELECT count(*) FROM rows WHERE {nonnull}),count(*),
              count(*) FILTER(WHERE n>1),coalesce(max(n),0) FROM groups''', [paths]).fetchone()
        if counts is None:
            raise ValueError('Population query returned no result')
        if counts[0] != binding['rows']:
            raise ValueError('Projected population is incomplete')
        self.work['population_aggregations'] += 1
        return self._write('population', dependency, {'rows': counts[0], 'nonNullRows': counts[1],
                           'nullKeyRows': counts[0] - counts[1], 'distinctKeys': counts[2],
                           'duplicateKeys': counts[3], 'maximumRowsPerKey': counts[4], 'scope': 'full_selected_inputs'})

    def _key_member(self, projection, columns, spec=None, target_index=0, source_identity=()):
        """Cache canonical guarded occurrences per member, keeping physical order."""
        dependency = {'projection': projection['artifact']['sha256'], 'columns': list(columns),
                      'recipe': recipe_digest(spec, target_index, source_identity) if spec else None,
                      'implementation': implementation_digest()}
        held = self._read('occurrences' if spec else 'target_keys', dependency)
        if held is not None:
            return held
        kind = 'occurrences' if spec else 'target_keys'
        schema = pa.schema([('key_json', pa.string()), ('source_identity', pa.string()),
                            ('row_position', pa.int64()), ('source_ordinal', pa.int64()),
                            ('candidate_ordinal', pa.int64()), ('target_key_ordinal', pa.int64())])
        output = self._path(kind, dependency).with_suffix('.parquet')
        temporary = output.with_suffix('.' + uuid4().hex + '.partial.parquet')
        states, raw, refused, repeats, position, target_null, target_unsupported, unusable_identity = Counter(), 0, 0, 0, 0, 0, 0, 0
        with pq.ParquetFile(projection['artifact']['path']) as parquet, pq.ParquetWriter(temporary, schema) as writer:
            for batch in parquet.iter_batches(batch_size=4096):
                output_rows = []
                for row in batch.to_pylist():
                    if spec is None:
                        value = identity_key(row, columns)
                        if any(row.get(c) is None for c in columns):
                            target_null += 1
                        elif value is None:
                            target_unsupported += 1
                        if value is not None:
                            output_rows.append({'key_json': value, 'source_identity': None,
                                                'row_position': position, 'source_ordinal': 0})
                    else:
                        unusable_identity += identity_key(row, source_identity) is None
                        state, elements = field_elements(spec, row)
                        states[state] += 1
                        seen = set()
                        for ordinal, element in elements:
                            expanded = [(None, element)]
                            if spec.get('candidates'):
                                keys = element.get('candidate_keys') if isinstance(element, dict) else None
                                expanded = [(i, {**element, **candidate} if isinstance(candidate, dict) else None)
                                            for i, candidate in enumerate(keys)] if isinstance(keys, list) and keys else [(None, None)]
                            for key_ordinal, candidate in expanded:
                                raw += 1
                                values = navigation.target_keys(spec['targets'][target_index], candidate, row)
                                value = json.dumps(values, separators=(',', ':')) if values is not None else None
                                if value is None:
                                    refused += 1
                                else:
                                    repeats += value in seen
                                    seen.add(value)
                                output_rows.append({'key_json': value, 'source_identity': identity_key(row, source_identity),
                                                    'row_position': position, 'source_ordinal': ordinal,
                                                    'candidate_ordinal': ordinal if spec.get('candidates') else None,
                                                    'target_key_ordinal': key_ordinal})
                    position += 1
                writer.write_table(pa.Table.from_pylist(output_rows, schema=schema))
        if position != projection['rows']:
            raise ValueError('Incomplete projected member read')
        assert_local_members_unchanged({projection['artifact']['path']: projection['artifact']['signature']})
        temporary.replace(output)
        self.work[kind + '_scans'] += 1
        return self._write(kind, dependency, {'artifact': self._artifact(output), 'sourceRows': position,
                           'rawReferences': raw, 'unsupportedReferences': refused, 'repeatedReferences': repeats,
                           'fieldStates': dict(states), 'nullTargetKeys': target_null,
                           'unsupportedTargetKeys': target_unsupported, 'unusableSourceIdentityRows': unusable_identity})

    def measure(self, index, paths, spec, target_index=0, *, source_identity=(), extra_tables=None):
        """Measure one canonical route; caches never qualify missing/partial inputs."""
        index = parse_index(canonical_bytes(index))
        source = selected_binding(index, spec['source'], extra_tables)
        target = selected_binding(index, spec['targets'][target_index]['table'], extra_tables)
        original = deepcopy(spec)
        # Ignore previous availability metadata: resolve canonical requirements
        # against the actual selected schemas in this operation.
        spec = navigation.published_navigation([original], {source['table']: source['schema'],
                                               target['table']: target['schema']})[0]
        original['field'] = spec['field']
        route = spec['targets'][target_index]
        if not route['sourceAvailable'] or not route['available']:
            raise ValueError('Required main source/target route fields are unavailable')
        if spec.get('equality') == 'native_scalar':
            integer_types = {'tinyint', 'smallint', 'integer', 'bigint', 'utinyint', 'usmallint', 'uinteger', 'ubigint'}
            for recipe, column in zip(route['keys'], route['columns'], strict=True):
                source_column = recipe['parts'][0]['path'][0]
                left = duckdb.sqltype(dict(source['schema'])[source_column]).id
                right = duckdb.sqltype(dict(target['schema'])[column]).id
                if not (left == right and left in {'varchar', 'date'} or left in integer_types and right in integer_types):
                    raise ValueError('Native scalar key types are incompatible or unsupported')
        dependency = {'source': source, 'target': target,
                      'recipe': recipe_digest(original, target_index, source_identity),
                      'aggregation': digest([VERSION, duckdb.__version__, inspect.getsource(self.measure)])}
        source_admitted, target_admitted = self.admit(source, paths), self.admit(target, paths)
        signatures = {entry['path']: entry['signature'] for entry in [*source_admitted, *target_admitted]}
        held = self._read('result', dependency)
        if held is not None and held.get('status') == 'complete':
            assert_local_members_unchanged(signatures)
            return held
        main = [r['path'].split('.')[0] for r in route['requiredMainFields']]
        identity_available = bool(source_identity) and set(source_identity) <= set(dict(source['schema']))
        source_columns = list(dict.fromkeys([*main, *(source_identity if identity_available else ())]))
        source_projected = self.project(source, source_admitted, source_columns)
        target_projected = self.project(target, target_admitted, route['columns'])
        source_artifacts = [self._key_member(p, (), original, target_index,
                            source_identity if identity_available else ()) for p in source_projected]
        target_artifacts = [self._key_member(p, route['columns']) for p in target_projected]
        identity_population = self.population(source, source_admitted, source_identity) if identity_available else None
        target_population = self.population(target, target_admitted, route['columns'])
        cache_signatures = {p['artifact']['path']: p['artifact']['signature']
                            for p in [*source_projected, *target_projected, *source_artifacts, *target_artifacts]}
        source_files, target_files = [p['artifact']['path'] for p in source_artifacts], [p['artifact']['path'] for p in target_artifacts]
        identity_qualified = bool(identity_population and identity_population['scope'] == 'full_selected_inputs'
                                  and identity_population['rows'] == source['rows'] == identity_population['nonNullRows']
                                  and identity_population['distinctKeys'] == source['rows']
                                  and not identity_population['nullKeyRows'] and not identity_population['duplicateKeys']
                                  and not sum(p['unusableSourceIdentityRows'] for p in source_artifacts))
        # The earlier type gate admits only exact string/date/integer equality.
        # These untransformed row keys preserve one occurrence per physical row;
        # native uniqueness therefore also proves canonical key uniqueness.
        native_columns = [recipe['parts'][0]['path'][0] for recipe in route['keys']] if spec.get('equality') == 'native_scalar' else []
        native_rows = bool(native_columns and spec['mode'] == 'row' and not spec.get('candidates') and all(
            recipe == navigation.key(navigation.part(column, row=True), pattern=r'[\s\S]*')
            for recipe, column in zip(route['keys'], native_columns, strict=True)))
        safe_identity_types = {'varchar', 'date', 'tinyint', 'smallint', 'integer', 'bigint',
                               'utinyint', 'usmallint', 'uinteger', 'ubigint'}
        unique_rows = bool(spec['mode'] == 'row' and not spec.get('candidates') and identity_qualified
                         and all(duckdb.sqltype(dict(source['schema'])[column]).id in safe_identity_types
                                 for column in source_identity)
                         and all(p['rawReferences'] == p['sourceRows'] and not p['repeatedReferences'] for p in source_artifacts))
        # A guarded or transformed flat row still emits one target tuple or
        # refusal per qualified source record; it cannot infer key uniqueness.
        unique_source_keys = bool(native_rows and unique_rows and sorted(native_columns) == sorted(source_identity))
        unique_target_keys = bool(native_rows and target_population['scope'] == 'full_selected_inputs'
                                  and target_population['rows'] == target['rows'] == target_population['nonNullRows']
                                  and target_population['distinctKeys'] == target['rows']
                                  and not target_population['nullKeyRows'] and not target_population['duplicateKeys']
                                  and not sum(p['unsupportedTargetKeys'] for p in target_artifacts))
        with self._connection() as con:
            union = ' UNION ALL '.join(
                f"SELECT {literal(item['member']['path'])} AS physical_member,* FROM read_parquet({literal(path)}, hive_partitioning=false)"
                for item, path in zip(source_admitted, source_files, strict=True))
            con.execute('CREATE TEMP VIEW refs AS ' + union)
            con.read_parquet(target_files, hive_partitioning=False).create_view('targets')
            con.execute('CREATE TEMP VIEW target_keys AS SELECT key_json,1::BIGINT n FROM targets' if unique_target_keys else
                        'CREATE TEMP TABLE target_keys AS SELECT key_json,count(*) n FROM targets GROUP BY ALL')
            reference_counts = con.execute('''SELECT count(*) FILTER(WHERE r.key_json IS NOT NULL),
                count(*) FILTER(WHERE t.n IS NOT NULL), count(*) FILTER(WHERE r.key_json IS NOT NULL AND t.n IS NULL),
                count(*) FILTER(WHERE t.n>1)
                FROM refs r LEFT JOIN target_keys t USING(key_json)''').fetchone()
            if unique_rows and reference_counts is not None:
                identity_counts = (reference_counts[1], reference_counts[0], reference_counts[2], reference_counts[3])
                self.work['unique_row_aggregations'] += 1
            else:
                # GROUP BY can spill; multiple filtered COUNT(DISTINCT) aggregates
                # retain independent hash sets and exceed the same memory budget.
                identity_counts = con.execute('''WITH source_flags AS (
                    SELECT r.source_identity, bool_or(t.n IS NOT NULL) has_match,
                        bool_or(r.key_json IS NOT NULL) has_eligible,
                        bool_or(r.key_json IS NOT NULL AND t.n IS NULL) has_missing,
                        bool_or(coalesce(t.n>1,false)) has_ambiguous
                    FROM refs r LEFT JOIN target_keys t USING(key_json)
                    WHERE r.source_identity IS NOT NULL GROUP BY r.source_identity)
                    SELECT count(*) FILTER(WHERE has_match),count(*) FILTER(WHERE has_eligible),
                        count(*) FILTER(WHERE has_missing),count(*) FILTER(WHERE has_ambiguous) FROM source_flags''').fetchone()
            counts = reference_counts + identity_counts if reference_counts is not None and identity_counts is not None else None
            if unique_rows:
                # A unique source native key occurs at most once; a broader FK
                # still groups references, but does not regroup source identities.
                sources_sql = ('SELECT key_json,1::BIGINT n FROM refs WHERE key_json IS NOT NULL' if unique_source_keys else
                               'SELECT key_json,count(*) n FROM refs WHERE key_json IS NOT NULL GROUP BY key_json')
                reverse = con.execute('''WITH sources AS (''' + sources_sql + ''')
                    SELECT coalesce(sum(t.n) FILTER(WHERE s.n IS NOT NULL),0),
                    coalesce(sum(t.n) FILTER(WHERE s.n IS NULL),0),coalesce(max(s.n),0),
                    coalesce(max(s.n),0),coalesce(max(s.n),0)
                    FROM target_keys t LEFT JOIN sources s USING(key_json)''').fetchone()
            else:
                reverse = con.execute('''WITH source_identities AS (
                    SELECT key_json,source_identity FROM refs
                    WHERE key_json IS NOT NULL AND source_identity IS NOT NULL GROUP BY ALL),
                    identity_counts AS (SELECT key_json,count(*) source_keys FROM source_identities GROUP BY key_json),
                    physical_rows AS (SELECT key_json,physical_member,row_position FROM refs
                        WHERE key_json IS NOT NULL GROUP BY ALL),
                    row_counts AS (SELECT key_json,count(*) source_rows FROM physical_rows GROUP BY key_json),
                    sources AS (SELECT key_json,count(*) n FROM refs WHERE key_json IS NOT NULL GROUP BY key_json)
                    SELECT coalesce(sum(t.n) FILTER(WHERE s.n IS NOT NULL),0),
                    coalesce(sum(t.n) FILTER(WHERE s.n IS NULL),0),coalesce(max(s.n),0),
                    coalesce(max(i.source_keys),0),coalesce(max(r.source_rows),0)
                    FROM target_keys t LEFT JOIN sources s USING(key_json)
                    LEFT JOIN identity_counts i USING(key_json) LEFT JOIN row_counts r USING(key_json)''').fetchone()
        if counts is None or reverse is None:
            raise ValueError('Relationship query returned no result')
        result = {'format': VERSION, 'status': 'complete', 'binding': dependency,
                  'route': {'id': original['id'], 'targetIndex': target_index},
                  'equality': 'canonical_navigation_words',
                  'aggregationMethods': {'sourceRecords': ('unique_native_row' if native_rows else 'qualified_flat_row') if unique_rows else 'grouped_occurrences',
                                         'targetKeys': 'unique_native_keys' if unique_target_keys else 'grouped_keys',
                                         'reverseSourceKeys': 'unique_native_keys' if unique_source_keys else 'grouped_keys'},
                  'sourceRows': source['rows'], 'targetRows': target['rows'],
                  'fieldStates': dict(sum((Counter(p['fieldStates']) for p in source_artifacts), Counter())),
                  'rawReferences': sum(p['rawReferences'] for p in source_artifacts),
                  'unsupportedReferences': sum(p['unsupportedReferences'] for p in source_artifacts),
                  'repeatedReferences': sum(p['repeatedReferences'] for p in source_artifacts),
                  'eligible': counts[0], 'matched': counts[1], 'missing': counts[2], 'ambiguous': counts[3],
                  'distinctMatchedSourceRecords': counts[4] if identity_qualified else None,
                  'distinctEligibleSourceRecords': counts[5] if identity_qualified else None,
                  'distinctMissingSourceRecords': counts[6] if identity_qualified else None,
                  'distinctAmbiguousSourceRecords': counts[7] if identity_qualified else None,
                  'sourceIdentity': {'columns': list(source_identity), 'qualified': identity_qualified,
                                     'population': identity_population},
                  'targetPopulation': target_population,
                  'unsupportedTargetKeys': sum(p['unsupportedTargetKeys'] for p in target_artifacts),
                  'reverse': {'matchedTargetRows': reverse[0], 'unmatchedTargetRows': reverse[1],
                              'maximumReferencesPerTarget': reverse[2],
                              'maximumPhysicalSourceRowsPerTarget': reverse[4],
                              'maximumDistinctSourceRecordsPerTarget': reverse[3] if identity_qualified else None},
                  'occurrences': [{'member': item['member'], **proof['artifact']} for item, proof in zip(source_admitted, source_artifacts, strict=True)]}
        if result['eligible'] != result['matched'] + result['missing'] or result['rawReferences'] != result['eligible'] + result['unsupportedReferences']:
            raise ValueError('Measurement counts do not reconcile')
        assert_local_members_unchanged(signatures)
        assert_local_members_unchanged(cache_signatures)
        self.work['route_measurements'] += 1
        return self._write('result', dependency, result)


def scalar_navigation(join):
    """Compile a canonical scalar declaration into the existing recipe format."""
    name = f"{join['child']}.{'+'.join(join['child_columns'])} -> {join['parent']}.{'+'.join(join['parent_columns'])}"
    spec = navigation.array(name, join['child'], (), (
        navigation.route(join['parent'], tuple(join['parent_columns']), tuple(
            navigation.key(navigation.part(c, row=True), pattern=r'[\s\S]*')
            for c in join['child_columns'])),), meaning=join.get('reason', ''), mode='row')
    spec['equality'] = 'native_scalar'
    return spec


def attached_directions(index, spec, target_index, proof, extra_tables=None):
    """Only a fully reconciled result with exact current bindings can qualify metadata."""
    try:
        if proof.get('format') != VERSION or proof.get('status') != 'complete':
            return None
        # Raw retained result files must meet the same imported-population
        # admission rules as cache hits before qualifying public measurements.
        for population in (proof.get('sourceIdentity', {}).get('population') or {}, proof.get('targetPopulation', {})):
            imported = population.get('populationProof')
            if imported and (imported.get('authority') != 'imported_existing_full_population' or
                             imported.get('guardImplementation') != digest([VERSION, inspect.getsource(MeasurementCache.import_population)])):
                return None
        source = selected_binding(index, spec['source'], extra_tables)
        target = selected_binding(index, spec['targets'][target_index]['table'], extra_tables)
        selected = navigation.published_navigation([spec], {source['table']: source['schema'], target['table']: target['schema']})[0]
        columns = proof['sourceIdentity']['columns']
        expected = {'source': source, 'target': target, 'recipe': recipe_digest(selected, target_index, columns),
                    'aggregation': digest([VERSION, duckdb.__version__, inspect.getsource(MeasurementCache.measure)])}
        if (proof['binding'] != expected or proof['route'] != {'id': spec['id'], 'targetIndex': target_index}
                or proof['equality'] != 'canonical_navigation_words'
                or not selected['targets'][target_index]['directions']['forward']['available']):
            return None
        counts = [proof[k] for k in ('sourceRows', 'targetRows', 'rawReferences', 'unsupportedReferences',
                  'repeatedReferences', 'eligible', 'matched', 'missing', 'ambiguous', 'unsupportedTargetKeys')]
        if any(not count_value(n) for n in counts):
            return None
        if (proof['sourceRows'] != source['rows'] or proof['targetRows'] != target['rows']
                or proof['matched'] + proof['missing'] != proof['eligible']
                or proof['rawReferences'] != proof['eligible'] + proof['unsupportedReferences']
                or proof['ambiguous'] > proof['matched'] or proof['repeatedReferences'] > proof['eligible']):
            return None
        states = proof['fieldStates']
        if not isinstance(states, dict) or any(not count_value(n) for n in states.values()) or sum(states.values()) != source['rows']:
            return None
        target_population = proof['targetPopulation']
        population_counts = ('rows', 'nonNullRows', 'nullKeyRows', 'distinctKeys', 'duplicateKeys', 'maximumRowsPerKey')
        if any(not count_value(target_population[k]) for k in population_counts):
            return None
        reverse = proof['reverse']
        if any(not count_value(reverse[k]) for k in
               ('matchedTargetRows', 'unmatchedTargetRows', 'maximumReferencesPerTarget', 'maximumPhysicalSourceRowsPerTarget')):
            return None
        if (target_population['scope'] != 'full_selected_inputs' or target_population['rows'] != target['rows'] or
                target_population['nonNullRows'] + target_population['nullKeyRows'] != target['rows'] or
                target_population['distinctKeys'] > target_population['nonNullRows'] or
                target_population['duplicateKeys'] > target_population['distinctKeys'] or
                target_population['maximumRowsPerKey'] > target_population['nonNullRows'] or
                proof['reverse']['matchedTargetRows'] + proof['reverse']['unmatchedTargetRows'] +
                target_population['nullKeyRows'] + proof['unsupportedTargetKeys'] != target['rows']):
            return None
        source_identity = proof['sourceIdentity']
        population = source_identity['population']
        if not isinstance(source_identity['qualified'], bool):
            return None
        if source_identity['qualified'] and (not population or population['rows'] != source['rows']
                or population['nullKeyRows'] or population['duplicateKeys']
                or population['scope'] != 'full_selected_inputs' or population['distinctKeys'] != source['rows']
                or population['nonNullRows'] != source['rows'] or not columns
                or not set(columns) <= set(dict(source['schema']))):
            return None
        distinct_counts = [proof[k] for k in ('distinctMatchedSourceRecords', 'distinctEligibleSourceRecords',
                                            'distinctMissingSourceRecords', 'distinctAmbiguousSourceRecords')]
        distinct_counts.append(reverse['maximumDistinctSourceRecordsPerTarget'])
        if source_identity['qualified'] and any(not count_value(n) or n > source['rows'] for n in distinct_counts):
            return None
        if not source_identity['qualified'] and any(n is not None for n in distinct_counts):
            return None
        directions = deepcopy(selected['targets'][target_index]['directions'])
        maximum = target_population['maximumRowsPerKey']
        uniqueness = ('unknown' if proof['unsupportedTargetKeys'] else 'empty' if not target_population['nonNullRows']
                      else 'one' if maximum == 1 else 'many')
        directions['forward']['measurement'] = {
            'status': 'measured', 'scope': 'full_selected_inputs', 'equality': proof['equality'],
            'eligible': proof['eligible'], 'matched': proof['matched'], 'missing': proof['missing'],
            'ambiguous': proof['ambiguous'], 'unsupported': proof['unsupportedReferences'],
            'repeatedReferences': proof['repeatedReferences'], 'targetUniqueness': uniqueness,
            'targetUniquenessScope': 'full_nonnull_target_keys',
            'maximumTargetRowsPerKey': maximum, 'proofDigest': digest(proof)}
        directions['reverse']['measurement'] = {
            'status': 'measured', 'scope': 'all_selected_targets_against_eligible_source_references',
            **proof['reverse'], 'distinctSourceIdentityQualified': proof['sourceIdentity']['qualified'],
            'proofDigest': digest(proof)}
        return directions
    except (KeyError, TypeError, ValueError):
        return None
