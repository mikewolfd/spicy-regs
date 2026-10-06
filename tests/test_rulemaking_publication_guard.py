"""Current snapshot publication must not replace a changed prior."""
import hashlib
from io import BytesIO
import json

from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.ontology.common import RunContext
from spicy_regs.pipelines.rulemaking_dataset import RulemakingDatasetPipeline
from spicy_regs.sources import publication, r2
from spicy_regs.transforms.regulations_shape import IDENTITIES, SOURCE_COLUMNS, TYPES


def raw(value):
    return json.dumps(value, sort_keys=True).encode()


class Store:
    def __init__(self):
        self.objects = {}
        self.conditions = []
        self.race_at_commit = False
        self.lose_commit_response = False

    def get_object(self, *, Bucket, Key):
        if Key not in self.objects:
            raise ClientError({'Error': {'Code': 'NoSuchKey'}}, 'GetObject')
        body = self.objects[Key]
        return {'Body': BytesIO(body), 'ETag': hashlib.sha256(body).hexdigest()}

    def put_object(self, *, Bucket, Key, Body, **kwargs):
        self.conditions.append(kwargs)
        if self.race_at_commit:
            self.objects[Key] = b'other writer'
        current = self.objects.get(Key)
        etag = hashlib.sha256(current).hexdigest() if current is not None else None
        if ('IfMatch' in kwargs and kwargs['IfMatch'] != etag
                or kwargs.get('IfNoneMatch') == '*' and current is not None):
            raise ClientError({'Error': {'Code': 'PreconditionFailed'}}, 'PutObject')
        self.objects[Key] = Body
        if self.lose_commit_response:
            raise TimeoutError('response lost after pointer saved')


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    pipeline = RulemakingDatasetPipeline(output_dir=tmp_path, skip_upload=False)
    for key in pipeline.published_outputs:
        dataset = key.removesuffix('.parquet')
        rows = []
        if dataset == 'proceedings':
            row = {name: None for name, _ in SOURCE_COLUMNS[dataset]}
            for name in IDENTITIES[dataset]:
                row[name] = 'P'
            rows.append(row)
        pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema(
            [(name, TYPES[kind]) for name, kind in SOURCE_COLUMNS[dataset]])), tmp_path / key)
    context = RunContext.resolve(run_id='new', asserted_at='2026-10-05T00:00:00Z', prefix='test')
    pipeline._classify_outputs(tmp_path, context)
    manifest, pointer, artifacts = pipeline._write_publication_files(
        tmp_path, context=context, stages=(), input_snapshot={'previous_snapshot_id': 'old'})
    old_pointer = raw({'dataset': 'rulemaking', 'format_version': 2, 'snapshot_id': 'old',
                       'manifest_key': 'materialized/rulemaking/snapshots/old/manifest.json',
                       'extra': {'literal': [None, 'kept']}})
    old_manifest = raw({'dataset': 'rulemaking', 'format_version': 2, 'snapshot_id': 'old',
                        'artifacts': {'prior': {'sha256': 'retained'}}, 'extra': [None, 'kept']})
    (tmp_path / '_rulemaking_latest.json').write_bytes(old_pointer)
    (tmp_path / '_rulemaking_previous_manifest.json').write_bytes(old_manifest)
    store = Store()
    store.objects[publication.SNAPSHOT_POINTER] = old_pointer
    store.objects['materialized/rulemaking/snapshots/old/manifest.json'] = old_manifest
    uploads = []
    after_upload = []

    def upload(path, *, remote_key):
        uploads.append(remote_key)
        store.objects[remote_key] = path.read_bytes()
        if after_upload:
            after_upload.pop(0)()

    monkeypatch.setenv('R2_BUCKET_NAME', 'test-only')
    monkeypatch.delenv('R2_PUBLIC_URL', raising=False)
    monkeypatch.setattr(r2, 'get_r2_client', lambda: store)
    monkeypatch.setattr(r2, 'upload_file', upload)
    return pipeline, manifest, pointer, artifacts, store, uploads, after_upload


def publish(prepared):
    pipeline, manifest, pointer, artifacts, *_ = prepared
    pipeline._publish(manifest_path=manifest, pointer_path=pointer, artifact_paths=artifacts)


def test_current_snapshot_publishes_validated_artifacts_then_conditional_pointer(prepared):
    pipeline, manifest, pointer, artifacts, store, uploads, _ = prepared
    prior = store.objects[publication.SNAPSHOT_POINTER]
    publish(prepared)
    assert uploads == [json.loads(manifest.read_text())['artifacts'][name]['remote_key'] for name in artifacts] + [
        json.loads(pointer.read_text())['manifest_key']]
    assert store.objects[publication.SNAPSHOT_POINTER] == pointer.read_bytes()
    assert store.conditions[0]['IfMatch'] == hashlib.sha256(prior).hexdigest()
    assert len(store.conditions) == 1
    assert not hasattr(pipeline, '_rulemaking_pointer_guard')


@pytest.mark.parametrize('changed', ['pointer', 'manifest'])
def test_full_prior_change_refuses_before_any_upload(prepared, changed):
    *_, store, uploads, _ = prepared
    key = publication.SNAPSHOT_POINTER if changed == 'pointer' else 'materialized/rulemaking/snapshots/old/manifest.json'
    value = json.loads(store.objects[key])
    value['extra'] = 'different despite same snapshot id'
    store.objects[key] = raw(value)
    with pytest.raises(ValueError, match='changed before publication'):
        publish(prepared)
    assert uploads == []
    assert store.conditions == []
    assert store.objects[key] == raw(value)


@pytest.mark.parametrize('changed', ['pointer', 'manifest'])
def test_prior_changed_during_uploads_preserves_other_writer(prepared, changed):
    *_, store, uploads, after_upload = prepared
    key = publication.SNAPSHOT_POINTER if changed == 'pointer' else 'materialized/rulemaking/snapshots/old/manifest.json'
    after_upload.append(lambda: store.objects.__setitem__(key, b'changed during upload'))
    with pytest.raises(ValueError, match='changed during publication'):
        publish(prepared)
    assert uploads
    assert store.conditions == []
    assert store.objects[key] == b'changed during upload'


def test_pointer_race_at_commit_is_conditionally_refused(prepared):
    *_, store, _, _ = prepared
    store.race_at_commit = True
    with pytest.raises(ValueError, match='conditional publication'):
        publish(prepared)
    assert store.objects[publication.SNAPSHOT_POINTER] == b'other writer'
    assert len(store.conditions) == 1


def test_lost_pointer_response_is_not_retried_or_rolled_back(prepared):
    _, _, pointer, _, store, _, _ = prepared
    store.lose_commit_response = True
    with pytest.raises(TimeoutError, match='response lost'):
        publish(prepared)
    assert store.objects[publication.SNAPSHOT_POINTER] == pointer.read_bytes()
    assert len(store.conditions) == 1


def test_changed_candidate_pointer_refuses_before_upload(prepared):
    _, _, pointer, _, store, uploads, _ = prepared
    value = json.loads(pointer.read_text())
    value['snapshot_id'] = 'different'
    pointer.write_bytes(raw(value))
    with pytest.raises(ValueError, match='candidate pointer and manifest differ'):
        publish(prepared)
    assert uploads == []
    assert store.conditions == []


def test_current_native_admission_still_rejects_changed_artifact(prepared):
    _, _, _, artifacts, store, uploads, _ = prepared
    artifacts['proceedings.parquet'].write_bytes(b'not its admitted native subject')
    with pytest.raises(pa.ArrowInvalid):
        publish(prepared)
    assert uploads == []
    assert store.conditions == []


def test_missing_captured_prior_needs_explicit_bootstrap(prepared):
    pipeline, manifest, _, _, store, uploads, _ = prepared
    (manifest.parent / '_rulemaking_latest.json').unlink()
    with pytest.raises(ValueError, match='requires its captured full prior'):
        publish(prepared)
    assert uploads == []
    del store.objects[publication.SNAPSHOT_POINTER]
    pipeline.allow_bootstrap = True
    publish(prepared)
    assert store.conditions[0]['IfNoneMatch'] == '*'
