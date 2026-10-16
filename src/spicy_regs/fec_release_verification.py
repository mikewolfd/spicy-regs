"""Compose completed FEC segment/assembly proofs without rebuilding a wide join.

Every closed segment passed receipt admission before commitment. Accepted ordered
comparisons bind the assembled subjects and shared receipts to those segments.
This verifier checks composition, remaining global keys, and restoration samples.
It never describes sampled restoration as a full restored-original comparison.
"""
from __future__ import annotations

from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import stat
import tempfile
from time import perf_counter

TABLES = ('fec_collections', 'fec_relationships', 'fec_source_records')
FORMAT = 'fec-composed-release-verification/1'


def signature(path):
    path = Path(path).absolute()
    if path.resolve() != path:
        raise ValueError(f'Evidence path contains a symlink: {path}')
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode):
        raise ValueError(f'Evidence is not a regular file: {path}')
    return dict(device=s.st_dev, inode=s.st_ino, size=s.st_size,
                mtime_ns=s.st_mtime_ns, ctime_ns=s.st_ctime_ns)


def unchanged(item):
    if signature(item['path']) != item['signature']:
        raise ValueError(f"Previously verified file changed: {item['path']}")


def digest(raw):
    return 'sha256:' + hashlib.sha256(raw).hexdigest()


def load_bound(path, expected):
    path = Path(path)
    before = signature(path)
    if before['size'] > 64 * 1024**2:
        raise ValueError('Verification control exceeds 64 MiB')
    raw = path.read_bytes()
    if digest(raw) != expected or signature(path) != before:
        raise ValueError(f'Verification control changed: {path}')
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError('Verification control must be an object')
    return value


def _json_digest(value):
    return digest(json.dumps(value, sort_keys=True, separators=(',', ':')).encode())


def _save(path, value):
    with tempfile.NamedTemporaryFile(mode='w', dir=path.parent, prefix='.pending-', delete=False) as stream:
        temporary = Path(stream.name)
        try:
            json.dump(value, stream, sort_keys=True, indent=2)
            stream.write('\n')
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _lock(path, *, shared=False):
    with path.open('a+') as stream:
        fcntl.flock(stream, (fcntl.LOCK_SH if shared else fcntl.LOCK_EX) | fcntl.LOCK_NB)
        yield


class CompletedSteps:
    """Atomic completion records; interrupted work never becomes a success."""
    def __init__(self, directory, binding):
        self.directory = directory
        self.binding = _json_digest(binding)
        self.results = {}

    def run(self, name, operation):
        path = self.directory / (name + '.json')
        if path.exists():
            value = json.loads(path.read_text())
            if (value.get('binding') != self.binding or value.get('step') != name
                    or value.get('resultDigest') != _json_digest(value.get('result'))):
                raise ValueError('Saved verification step belongs to different inputs or code')
        else:
            started = perf_counter()
            result = operation()
            value = dict(binding=self.binding, step=name, result=result, resultDigest=_json_digest(result),
                         seconds=perf_counter()-started)
            _save(path, value)
        self.results[name] = value
        return value['result']


def audit_assembly(admission_path, admission_digest, retained_path, retained_digest, checkpoint):
    """Read control files and file metadata, never rehash corpus payloads."""
    from spicy_regs import native_conversion as native
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy

    admission = load_bound(admission_path, admission_digest)
    if admission.get('format') != 'fec-retained-generation-completion/1':
        raise ValueError('Unsupported assembly evidence')
    checkpoint = Path(checkpoint).absolute()
    prior = Path(admission['priorWork'])
    generation = prior / 'build/generation'
    candidate_item = admission['candidateProof']
    candidate = load_bound(candidate_item['path'], candidate_item['sha256'])
    header = load_bound(checkpoint / 'checkpoint.json', candidate['headerSHA256'])
    if candidate.get('format') != 'fec-frozen-candidate-reuse/1':
        raise ValueError('Unsupported ordered assembly proof')
    if set(candidate['candidates']) != {*TABLES, 'source_receipts'} or set(candidate['rows']) != set(TABLES):
        raise ValueError('Incomplete observation assembly')
    for item in (candidate_item, *admission['files'].values(), *candidate['guards'].values(),
                 *candidate['candidates'].values()):
        unchanged(item)
    physical = {str(p.relative_to(prior)) for folder in ('build/generation', 'retained', 'build/source-evidence')
                for p in (prior / folder).rglob('*') if p.is_file() or p.is_symlink()}
    if physical != set(admission['files']):
        raise ValueError('Assembly member set changed')
    for relative, item in admission['files'].items():
        if Path(relative).is_absolute() or '..' in Path(relative).parts or item['path'] != str(prior / relative):
            raise ValueError('Assembly member escaped its original directory')
    controls = {}
    for name in ('capturedIndex', 'priorState', 'sharedProof'):
        item = admission[name]
        unchanged(item)
        controls[name] = load_bound(item['path'], item['sha256'])
    shared = controls['sharedProof']
    if (shared.get('format') != 'fec-shared-receipt-reuse/1' or shared.get('orderedCopy') is not True
            or shared.get('adoption') != 'copy-protected'
            or shared['candidateProofDigest'] != candidate_item['sha256']
            or shared['rows'] != candidate['totalReceiptRows']
            or shared['file'].get('orderedValuesCompared') is not True):
        raise ValueError('Shared receipts lack their completed ordered comparison')
    unchanged(shared['file'])
    root = load_bound(generation / 'artifact.json', admission['files']['build/generation/artifact.json']['sha256'])
    members = load_bound(generation / 'members.json', admission['files']['build/generation/members.json']['sha256'])
    if {key: root[key] for key in ('logicalId', 'artifactDigest')} != admission['generationPin']:
        raise ValueError('Generation pin changed')
    spec = root['spec']
    old = controls['capturedIndex']['families']['fec-observations']
    if (spec['family'] != 'fec-observations' or spec['publicationStatus'] != 'complete-family'
            or spec['readSnapshot'] != controls['capturedIndex']
            or spec['etlReceipts']['generationId'] != header['generation_id']
            or header['identity']['captured_family'] != old
            or set(spec['tables']) != {name + '.parquet' for name in TABLES}):
        raise ValueError('Generation or captured source differs from the proven assembly')
    policies = [dataset_policy(table).descriptor() for table in TABLES]
    if spec['etlReceipts']['policies'] != policies or header['identity']['policies'] != policies:
        raise ValueError('Assembly policies differ from the shipped verifier')
    listed = {m['objectKey']: m for m in members['members']}
    if len(listed) != len(members['members']) or set(listed) != {'etl_receipts.parquet', *(t+'.parquet' for t in TABLES)}:
        raise ValueError('Generation member declaration differs')
    for table in TABLES:
        item, member = candidate['candidates'][table], listed[table+'.parquet']
        if (item.get('orderedValuesCompared') is not True or item['rows'] != candidate['rows'][table]
                or (member['sha256'], member['byteSize'], member['recordCount']) !=
                (item['sha256'], item['signature']['size'], item['rows'])
                or spec['tables'][table+'.parquet']['rows'] != item['rows']):
            raise ValueError('Published subject bytes differ from their ordered proof')
    source_receipts = candidate['candidates']['source_receipts']
    if source_receipts.get('orderedValuesCompared') is not True or source_receipts['rows'] != candidate['rows'][TABLES[-1]]:
        raise ValueError('Source receipts lack their ordered assembly proof')
    member = listed['etl_receipts.parquet']
    if (member['sha256'], member['byteSize'], member['recordCount']) != (
            shared['file']['sha256'], shared['file']['signature']['size'], shared['rows']):
        raise ValueError('Generation receipts differ from their ordered proof')
    if spec['etlReceipts']['rows'] != shared['rows']:
        raise ValueError('Receipt row declaration differs')
    retained_admission = load_bound(retained_path, retained_digest)
    if retained_admission.get('format') != 'fec-verified-original-reuse/1':
        raise ValueError('Unsupported retained-original proof')
    retained_controls = {}
    for name in ('state', 'proof', 'finished'):
        item = retained_admission[name]
        unchanged(item)
        retained_controls[name] = load_bound(item['path'], item['sha256'])
    retained = retained_controls['proof']
    if (retained.get('format') != 'fec-retained-inputs-admission/1'
            or retained['admissionDigest'] != admission_digest
            or retained['stateDigest'] != retained_admission['state']['sha256']):
        raise ValueError('Retained-original proof belongs to another assembly')
    expected_retained = {t+'.parquet': str(prior / 'retained' / (t+'.parquet')) for t in TABLES}
    if retained['retained'] != expected_retained or native._original_states(
            {k: Path(v) for k, v in expected_retained.items()}) != retained['originalPins']:
        raise ValueError('Verified original files changed')
    evidence = tuple(sorted(p.parent for p in (prior / 'build/source-evidence').glob('*/artifact/artifact.json')))
    native._prepared_evidence(root, 'fec-observations', evidence)
    prior_input = dict(role='prior-generation', logicalId=old['logicalId'], artifactDigest=old['artifactDigest'])
    if root['inputs'].count(prior_input) != 1 or len(root['inputs']) != 2:
        raise ValueError('Original generation lineage differs')
    return dict(admission=admission, candidate=candidate, header=header, root=root, old=old,
                generation=generation, checkpoint=checkpoint, retained=expected_retained,
                shared=shared, evidence=evidence, members=members, old_state=controls['priorState'])


def verify_ranges(context):
    """Prove complete disjoint source and receipt ranges from accepted controls."""
    candidate, checkpoint = context['candidate'], context['checkpoint']
    closed = sorted((checkpoint / 'closed').iterdir())
    if [p.name for p in closed] != [f'{i:08d}.json' for i in range(len(closed))]:
        raise ValueError('Closed segment sequence has gaps or extra files')
    rows, groups = dict.fromkeys(TABLES, 0), dict.fromkeys(TABLES, 0)
    receipts, offset, previous_table = [], 0, 0
    samples = {name: [] for name in TABLES}
    for path in closed:
        guard = candidate['guards'].get(str(path))
        if guard is None:
            raise ValueError('Segment was not covered by the accepted candidate proof')
        entry = load_bound(path, guard['sha256'])
        spec, table = entry['spec'], entry['spec']['table']
        if table not in TABLES or TABLES.index(table) < previous_table:
            raise ValueError('Segment table order differs')
        previous_table = TABLES.index(table)
        count = spec['rows']
        if (type(count) is not int or count <= 0 or spec['member'] != 0
                or spec['ordinal_start'] != rows[table] or spec['row_group'] != groups[table]
                or spec['attempt_offset'] != offset
                or entry['checkpoint_sha256'] != candidate['headerSHA256']
                or spec['member_sha256'] != context['old']['tables'][table+'.parquet']['sha256']):
            raise ValueError('Segment ranges overlap, omit rows, or name a foreign source')
        if not re.fullmatch(r'attempts/[0-9]{8}-[0-9a-f]{32}/closed', entry['directory']):
            raise ValueError('Segment escaped checkpoint')
        directory = checkpoint / entry['directory']
        if set(entry['files']) != {table+'.parquet', 'etl_receipts.parquet', 'identity-context.json'}:
            raise ValueError('Segment member set differs')
        for name, expected in entry['files'].items():
            item = candidate['guards'].get(str(directory / name))
            if item is None or item['sha256'] != expected:
                raise ValueError('Segment member was not covered by ordered proof')
        if table != 'fec_source_records':
            receipts.append(str(directory / 'etl_receipts.parquet'))
        samples[table].append(dict(directory=str(directory), ordinal=rows[table], rows=count,
                                   receipt_offset=offset, row_group=spec['row_group']))
        rows[table] += count
        groups[table] += 1
        offset += count
    receipts.append(candidate['candidates']['source_receipts']['path'])
    if (rows != candidate['rows'] or offset != candidate['totalReceiptRows']
            or receipts != candidate['receiptInputs']
            or any(rows[t] != context['old']['tables'][t+'.parquet']['rows'] for t in TABLES)):
        raise ValueError('Segments do not cover the complete retained population')
    picked = {table: [items[i] for i in sorted({0, len(items)//2, len(items)-1})]
              for table, items in samples.items() if items}
    return dict(rows=rows, segments=groups, receiptRows=offset, sampleSegments=picked,
                sourceIdentity='disjoint observation_ordinal ranges in validated segments',
                receiptIdentity='disjoint attempt_id ranges included in each validated receipt digest')


def verify_small_identities(context, work):
    """Check only two natural-key columns; no wide receipt database or join."""
    import pyarrow.parquet as pq
    checked = {}
    with tempfile.TemporaryDirectory(prefix='identity-', dir=work) as temporary:
        connection = sqlite3.connect(str(Path(temporary) / 'keys.sqlite'))
        try:
            connection.execute('PRAGMA cache_size=-8192')
            connection.execute('PRAGMA journal_mode=OFF')
            connection.execute('CREATE TABLE keys (value TEXT PRIMARY KEY) WITHOUT ROWID')
            for table, field in (('fec_collections', 'collection_id'), ('fec_relationships', 'record_id')):
                count = 0
                with pq.ParquetFile(context['generation'] / (table+'.parquet')) as source:
                    for batch in source.iter_batches(columns=[field], batch_size=8192, use_threads=False):
                        values = batch.column(0).to_pylist()
                        if any(not isinstance(v, str) or not v for v in values):
                            raise ValueError(f'Invalid natural identity: {table}')
                        try:
                            connection.executemany('INSERT INTO keys VALUES (?)', ((v,) for v in values))
                        except sqlite3.IntegrityError as error:
                            raise ValueError(f'Cross-segment duplicate identity: {table}') from error
                        count += len(values)
                if count != context['candidate']['rows'][table]:
                    raise ValueError('Identity check row count differs')
                checked[table] = dict(rows=count, column=field, unique=True)
                connection.execute('DELETE FROM keys')
            connection.commit()
        finally:
            connection.close()
    return checked


def _take_rows(path, positions):
    """Read selected row groups in small batches, preserving requested order."""
    import pyarrow.parquet as pq
    wanted, result = sorted(set(positions)), {}
    with pq.ParquetFile(path) as source:
        schema, start = source.schema_arrow, 0
        for group in range(source.metadata.num_row_groups):
            end = start + source.metadata.row_group(group).num_rows
            selected = [n for n in wanted if start <= n < end]
            if selected:
                at = start
                for batch in source.iter_batches(row_groups=[group], batch_size=32, use_threads=False):
                    for n in selected:
                        if at <= n < at + batch.num_rows:
                            result[n] = batch.slice(n-at, 1).to_pylist()[0]
                    at += batch.num_rows
                    if all(n in result for n in selected):
                        break
            start = end
            if len(result) == len(wanted):
                break
    if set(result) != set(wanted):
        raise ValueError('Smoke read omitted a requested row')
    return schema, [result[n] for n in positions]


def restoration_smoke(context, ranges):
    import pyarrow as pa
    from spicy_regs.etl_receipts import subject_identity, validate_receipt_row, _unpack
    from spicy_regs.transforms.fec_identity_receipts import dataset_policy
    result = {}
    for table, segments in ranges['sampleSegments'].items():
        policy = dataset_policy(table)
        positions = sorted({s['ordinal']+n for s in segments for n in (0, s['rows']-1)})
        schema, originals = _take_rows(context['retained'][table+'.parquet'], positions)
        subject_schema, subjects = _take_rows(context['generation'] / (table+'.parquet'), positions)
        if not subject_schema.equals(policy.subject_schema, check_metadata=False):
            raise ValueError('Smoke subject schema differs')
        table_offset = sum(ranges['rows'][t] for t in TABLES[:TABLES.index(table)])
        # Ordered-copy evidence already binds these rows to the shared member.
        # Reading its very large footer again would add cost without new proof.
        sampled_receipts = {}
        for segment in segments:
            local = sorted({0, segment['rows']-1})
            _, rows = _take_rows(Path(segment['directory']) / 'etl_receipts.parquet', local)
            sampled_receipts.update({segment['ordinal']+n: row for n, row in zip(local, rows, strict=True)})
        receipts = [sampled_receipts[n] for n in positions]
        for n, original, subject, receipt in zip(positions, originals, subjects, receipts, strict=True):
            validate_receipt_row(receipt, {table: policy})
            key, version, identity = subject_identity(policy, subject)
            if (receipt['generation_id'] != context['header']['generation_id'] or receipt['outcome'] != 'accepted'
                    or (receipt['record_id'], receipt['subject_version'], receipt['identity_json']) != (key, version, identity)
                    or receipt['attempt_id'] != f'{table}/{table_offset+n+1}'):
                raise ValueError('Smoke subject/receipt identity or ordering differs')
            combined = subject | _unpack(json.loads(receipt['processing_json']))
            moved = combined['conversion_inputs']
            overrides = (combined.get('source_input') or {}).get('fec_retained_overrides', {})
            restored = {name: overrides.get(name, moved.get(name, combined.get(name))) for name in schema.names}
            if not pa.Table.from_pylist([restored], schema=schema).equals(pa.Table.from_pylist([original], schema=schema)):
                raise ValueError(f'Sampled original restoration differs: {table} row {n}')
        result[table] = dict(positions=positions, rows=len(positions), exactRestoration=True)
    return result


def _implementation():
    module = Path(__file__).parent
    return {name: digest((module / name).read_bytes()) for name in (
        'fec_release_verification.py', 'etl_receipts.py', 'native_conversion.py', 'source_evidence.py',
        'transforms/fec_identity_receipts.py', 'transforms/fec_identity_context_fields.py',
        'fec_identity_context_fields.json')}


@dataclass(frozen=True)
class VerifiedFecRelease:
    root: dict
    pin: object
    members: tuple
    proof: dict
    _bindings: tuple

    def check(self):
        for item in self._bindings:
            unchanged(item)


def verify_publication_basis(directory, basis, *, expected_pin=None):
    """Consume a pinned completed proof, retaining its explicit claim limits."""
    from rulespec_artifacts import ArtifactPin, MemberDescriptor
    if not isinstance(basis, dict) or set(basis) != {'path', 'sha256'}:
        raise ValueError('Publication requires the exact verification result path and digest')
    report = load_bound(basis['path'], basis['sha256'])
    if report.get('format') != FORMAT or report.get('qualifiedForRelease') is not True:
        raise ValueError('Unsupported or incomplete FEC release verification')
    binding, evidence = report['binding'], report['evidence']
    if binding['format'] != FORMAT or binding['implementation'] != _implementation():
        raise ValueError('Release verification implementation changed')
    context = audit_assembly(evidence['admission'], binding['admission'], evidence['retainedAdmission'],
                             binding['retained'], evidence['checkpoint'])
    if (Path(directory).absolute() != context['generation'] or report['generationDirectory'] != str(context['generation'])
            or report['generationPin'] != context['admission']['generationPin']
            or binding['generation'] != report['generationPin']):
        raise ValueError('Release verification names another generation')
    checks = report['checks']
    if (checks.get('fullRestoredOriginalComparison') is not False
            or checks.get('orderedAssembly') != 'reused complete comparisons'
            or checks.get('segmentReceiptValidation') != 'reused complete writer validation'
            or checks.get('lineage') != 'verified'):
        raise ValueError('Release verification misstates its coverage')
    steps = CompletedSteps(Path(basis['path']).parent, binding)
    for name, key in (('ranges', 'ranges'), ('identities', 'naturalIdentities'), ('restoration-smoke', 'restorationSamples')):
        def missing():
            raise ValueError('Release verification has no completed step')
        if steps.run(name, missing) != checks[key]:
            raise ValueError('Release verification differs from its completed checks')
    pin = ArtifactPin(report['generationPin']['logicalId'], report['generationPin']['artifactDigest'])
    if isinstance(expected_pin, dict):
        expected_pin = ArtifactPin(expected_pin['logicalId'], expected_pin['artifactDigest'])
    if expected_pin is not None and pin != expected_pin:
        raise ValueError('Release verification differs from expected generation')
    members = tuple(MemberDescriptor(m['objectKey'], m['role'], m['mediaType'], m['byteSize'], m['sha256'],
                                     m.get('recordCount')) for m in context['members']['members'])
    controls = tuple(context['admission'][name] for name in ('candidateProof', 'sharedProof', 'capturedIndex', 'priorState'))
    result = VerifiedFecRelease(context['root'], pin, members, report,
                               (*context['admission']['files'].values(), *controls,
                                dict(path=basis['path'], signature=signature(basis['path']))))
    result.check()
    return result


def verify_release(*, admission, admission_digest, retained_admission, retained_digest, checkpoint, output):
    output = Path(output).absolute()
    output.mkdir(parents=True, exist_ok=True)
    with ExitStack() as locks:
        locks.enter_context(_lock(output / 'lock'))
        locks.enter_context(_lock(Path(checkpoint) / 'lock', shared=True))
        locks.enter_context(_lock(Path(checkpoint).parent.parent / 'operation.lock', shared=True))
        started = perf_counter()
        context = audit_assembly(admission, admission_digest, retained_admission, retained_digest, checkpoint)
        binding = dict(format=FORMAT, admission=admission_digest, retained=retained_digest,
                       implementation=_implementation(), generation=context['admission']['generationPin'])
        steps = CompletedSteps(output, binding)
        ranges = steps.run('ranges', lambda: verify_ranges(context))
        identities = steps.run('identities', lambda: verify_small_identities(context, output))
        smoke = steps.run('restoration-smoke', lambda: restoration_smoke(context, ranges))
        for item in context['admission']['files'].values():
            unchanged(item)
        result = dict(format=FORMAT, verifiedAt=datetime.now(timezone.utc).isoformat(), binding=binding,
                      generationDirectory=str(context['generation']), generationPin=binding['generation'],
                      priorPin={k: context['old'][k] for k in ('logicalId', 'artifactDigest')},
                      evidence=dict(admission=str(admission), retainedAdmission=str(retained_admission),
                                    checkpoint=str(Path(checkpoint).absolute()),
                                    candidate=context['admission']['candidateProof']['sha256'],
                                    sharedReceipts=context['admission']['sharedProof']['sha256']),
                      checks=dict(orderedAssembly='reused complete comparisons',
                                  segmentReceiptValidation='reused complete writer validation', ranges=ranges,
                                  naturalIdentities=identities, restorationSamples=smoke,
                                  fullRestoredOriginalComparison=False, lineage='verified'),
                      releaseBasis='validated segments + ordered assembly + disjoint identities + lineage + sampled restoration',
                      seconds=perf_counter()-started, qualifiedForRelease=True, published=False)
        _save(output / 'verification.json', result)
        return result
