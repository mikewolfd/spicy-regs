"""Actual publisher admission and private store refusal checks."""
import base64
import hashlib
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from scripts.qualification_store import DiskStore
from spicy_regs.generations import build_generation
from spicy_regs.sources import publication


def test_actual_publisher_uploads_and_readmits_streamed_multipart_members(tmp_path, monkeypatch):
    source = tmp_path / 'a.parquet'
    pq.write_table(pa.table({'id': ['one', 'two']}), source)
    generation = tmp_path / 'generation'
    artifact = build_generation(generation, family='private-test', files=[source], expected_keys=[source.name])
    store = DiskStore(tmp_path / 'store', bucket='private')
    monkeypatch.setattr(publication, 'PART_BYTES', 31)
    selected = publication.publish_generation(generation, client=store, bucket='private',
                                               prior_index=publication.empty_index())
    family = selected['families']['private-test']
    assert family['artifactDigest'] == artifact.pin.artifact_digest
    assert store.objects[family['prefix'] + '/a.parquet'].read_bytes() == source.read_bytes()
    assert any(call['method'] == 'upload_part' for call in store.calls)
    assert store.uploads == {}
    assert publication.parse_index(store.objects[publication.INDEX_V2_KEY].read_bytes()) == selected
    assert publication.parse_index(store.objects[publication.INDEX_KEY].read_bytes()) == publication.derive_v1(selected)


def test_conditional_digest_and_bucket_refusals_preserve_private_bytes(tmp_path):
    store = DiskStore(tmp_path / 'store', bucket='private')

    class Bounded(BytesIO):
        def read(self, size=-1):
            assert 0 < size <= 1024 * 1024
            return super().read(size)

    store.put_object(Bucket='private', Key='pointer', Body=Bounded(b'first'), IfNoneMatch='*')
    etag = store.head_object(Bucket='private', Key='pointer')['ETag']
    for kwargs in ({'IfNoneMatch': '*'}, {'IfMatch': 'wrong'}, {'ContentMD5': 'wrong'}):
        with pytest.raises(ClientError):
            store.put_object(Bucket='private', Key='pointer', Body=b'second', **kwargs)
        assert store.objects['pointer'].read_bytes() == b'first'
    store.put_object(Bucket='private', Key='pointer', Body=b'second', IfMatch=etag)
    assert store.objects['pointer'].read_bytes() == b'second'
    for bucket, key in [('other', 'pointer'), ('private', '../escape'), ('private', '/escape')]:
        with pytest.raises(ValueError):
            store.head_object(Bucket=bucket, Key=key)


def test_incomplete_multipart_refuses_before_exposing_bytes(tmp_path):
    store = DiskStore(tmp_path / 'store', bucket='private')
    identity = store.create_multipart_upload(Bucket='private', Key='data')['UploadId']
    raw = b'part'
    digest = base64.b64encode(hashlib.md5(raw, usedforsecurity=False).digest()).decode()
    part = store.upload_part(Bucket='private', Key='data', UploadId=identity, PartNumber=1,
                             Body=raw, ContentMD5=digest)
    with pytest.raises(ValueError):
        store.complete_multipart_upload(Bucket='private', Key='data', UploadId=identity,
                                        MultipartUpload={'Parts': []}, IfNoneMatch='*')
    assert 'data' not in store.objects
    store.complete_multipart_upload(Bucket='private', Key='data', UploadId=identity,
                                    MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': part['ETag']}]},
                                    IfNoneMatch='*')
    with store.get_object(Bucket='private', Key='data')['Body'] as body:
        assert body.read() == raw
