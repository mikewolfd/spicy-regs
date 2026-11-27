"""Actual publisher admission and private store refusal checks."""
import base64
import hashlib
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from botocore.exceptions import ClientError

from spicy_regs.sources.disk_store import DiskStore, LocalObjectSource
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
    # The uploader sends Content-MD5 alone, so storage holds no SHA-256 for a member or the pointer.
    for key in (family['prefix'] + '/a.parquet', publication.INDEX_V2_KEY):
        assert 'ChecksumSHA256' not in store.head_object(Bucket='private', Key=key, ChecksumMode='ENABLED')


def test_the_store_refuses_what_r2_refuses(tmp_path):
    """Run 41223379525 and the 2026-11-01 probe: the store must not accept a checksum shape storage refuses."""
    store = DiskStore(tmp_path / 'store', bucket='private')
    md5 = base64.b64encode(hashlib.md5(b'data', usedforsecurity=False).digest()).decode()
    sha256 = base64.b64encode(hashlib.sha256(b'data').digest()).decode()
    with pytest.raises(ClientError, match='InvalidRequest'):
        store.put_object(Bucket='private', Key='data', Body=b'data', ContentMD5=md5, ChecksumSHA256=sha256)
    upload = store.create_multipart_upload(Bucket='private', Key='data')['UploadId']
    with pytest.raises(ClientError, match='NotImplemented'):
        store.upload_part(Bucket='private', Key='data', UploadId=upload, PartNumber=1, Body=b'data',
                          ContentMD5=md5, ChecksumSHA256=sha256)
    store.abort_multipart_upload(Bucket='private', Key='data', UploadId=upload)
    assert store.objects == {}


def test_a_wrong_sha256_checksum_refuses_before_writing_private_state(tmp_path):
    store = DiskStore(tmp_path / 'store', bucket='private')
    wrong = base64.b64encode(hashlib.sha256(b'other').digest()).decode()
    with pytest.raises(ClientError, match='BadDigest'):
        store.put_object(Bucket='private', Key='data', Body=b'data', ChecksumSHA256=wrong)
    assert store.objects == {} and store.checksums == {} and store.uploads == {}
    right = base64.b64encode(hashlib.sha256(b'data').digest()).decode()
    store.put_object(Bucket='private', Key='data', Body=b'data', ChecksumSHA256=right)
    assert store.head_object(Bucket='private', Key='data', ChecksumMode='ENABLED')['ChecksumSHA256'] == right


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


def test_pointer_condition_is_rechecked_after_streaming_a_competing_write(tmp_path):
    store = DiskStore(tmp_path / 'store', bucket='private')
    store.put_object(Bucket='private', Key='pointer', Body=b'old')
    etag = store.head_object(Bucket='private', Key='pointer')['ETag']

    class Competing(BytesIO):
        first = True

        def read(self, size=-1):
            if self.first:
                self.first = False
                store.put_object(Bucket='private', Key='pointer', Body=b'competing', IfMatch=etag)
            return super().read(size)

    with pytest.raises(ClientError, match='PreconditionFailed'):
        store.put_object(Bucket='private', Key='pointer', Body=Competing(b'stale'), IfMatch=etag)
    assert store.objects['pointer'].read_bytes() == b'competing'
    assert not list(store.root.glob('put-*'))


@pytest.mark.parametrize('method', ['put_object', 'create_multipart_upload'])
def test_unsupported_options_refuse_before_writing_private_state(tmp_path, method):
    store = DiskStore(tmp_path / 'store', bucket='private')
    arguments: dict[str, object] = {'Bucket': 'private', 'Key': 'data', 'ImaginaryOption': True}
    if method == 'put_object':
        arguments['Body'] = b'data'
    with pytest.raises(ValueError, match='Unsupported private store options'):
        getattr(store, method)(**arguments)
    assert store.objects == {} and store.uploads == {}
    assert not list(store.root.iterdir())


def test_a_copy_keeps_the_bytes_and_etag_and_never_overwrites(tmp_path):
    store = DiskStore(tmp_path / 'store', bucket='private')
    store.put_object(Bucket='private', Key='old/a.parquet', Body=b'member')
    store.copy_object(Bucket='private', Key='new/a.parquet', CopySource={'Bucket': 'private', 'Key': 'old/a.parquet'})
    assert store.objects['new/a.parquet'].read_bytes() == b'member'
    assert store.head_object(Bucket='private', Key='new/a.parquet')['ETag'] == store.etags['old/a.parquet']
    for source, code in (('old/a.parquet', 'PreconditionFailed'), ('old/absent.parquet', 'NoSuchKey')):
        with pytest.raises(ClientError) as raised:
            store.copy_object(Bucket='private', Key='new/a.parquet', CopySource={'Bucket': 'private', 'Key': source})
        assert raised.value.response['Error']['Code'] == code


def test_local_source_multipart_verifies_etag_and_immutability_with_zero_disk_bytes(tmp_path):
    """Zero-copy local-source verification preserves composite ETags, read-backs, copies and every refusal."""
    payload = b'part-one!' * 10 + b'part-two?' * 10 + b'part-end.' * 5
    source_file = tmp_path / 'blob.bin'
    source_file.write_bytes(payload)
    local_source = LocalObjectSource(size=len(payload), open_stream=lambda: source_file.open('rb'), path=source_file)
    store = DiskStore(tmp_path / 'store', bucket='private', local_sources={'blob': local_source})

    upload_id = store.create_multipart_upload(Bucket='private', Key='blob')['UploadId']
    parts_raw = [payload[:90], payload[90:180], payload[180:]]
    first_md5 = base64.b64encode(hashlib.md5(parts_raw[0], usedforsecurity=False).digest()).decode()
    first_sha = base64.b64encode(hashlib.sha256(parts_raw[0]).digest()).decode()

    with pytest.raises(ClientError, match='NotImplemented'):
        store.upload_part(Bucket='private', Key='blob', UploadId=upload_id, PartNumber=1,
                          Body=parts_raw[0], ContentMD5=first_md5, ChecksumSHA256=first_sha)
    with pytest.raises(ClientError, match='BadDigest'):
        store.upload_part(Bucket='private', Key='blob', UploadId=upload_id, PartNumber=1,
                          Body=parts_raw[0], ContentMD5=base64.b64encode(b'0' * 16).decode())
    with pytest.raises(ValueError, match='in order'):
        store.upload_part(Bucket='private', Key='blob', UploadId=upload_id, PartNumber=2,
                          Body=parts_raw[1], ContentMD5=first_md5)

    uploaded = []
    part_digests = []
    for number, chunk in enumerate(parts_raw, start=1):
        digest = hashlib.md5(chunk, usedforsecurity=False)
        part_digests.append(digest.digest())
        md5_b64 = base64.b64encode(digest.digest()).decode()
        reply = store.upload_part(Bucket='private', Key='blob', UploadId=upload_id, PartNumber=number,
                                  Body=chunk, ContentMD5=md5_b64)
        uploaded.append({'PartNumber': number, 'ETag': reply['ETag']})

    assert store.disk_bytes_written == 0
    assert list(store.root.rglob('*')) == []

    with pytest.raises(ValueError, match='differs from uploaded parts'):
        store.complete_multipart_upload(Bucket='private', Key='blob', UploadId=upload_id,
                                        MultipartUpload={'Parts': uploaded[:-1]}, IfNoneMatch='*')
    with pytest.raises(ClientError, match='NoSuchKey'):
        store.head_object(Bucket='private', Key='blob')

    done = store.complete_multipart_upload(Bucket='private', Key='blob', UploadId=upload_id,
                                           MultipartUpload={'Parts': uploaded}, IfNoneMatch='*')
    expected_etag = f'"{hashlib.md5(b"".join(part_digests), usedforsecurity=False).hexdigest()}-3"'
    assert done['ETag'] == expected_etag
    assert store.disk_bytes_written == 0
    assert list(store.root.rglob('*')) == []
    assert store.uploads == {}
    assert store.head_object(Bucket='private', Key='blob') == {'ContentLength': len(payload), 'ETag': expected_etag}
    with store.get_object(Bucket='private', Key='blob')['Body'] as body:
        assert body.read() == payload

    # Server-side copy shares the verified reference with zero disk writes and preserves ETag and immutability.
    store.copy_object(Bucket='private', Key='copy/blob', CopySource={'Bucket': 'private', 'Key': 'blob'})
    assert store.disk_bytes_written == 0
    assert store.head_object(Bucket='private', Key='copy/blob') == {'ContentLength': len(payload), 'ETag': expected_etag}
    with store.get_object(Bucket='private', Key='copy/blob')['Body'] as body:
        assert body.read() == payload
    with pytest.raises(ClientError, match='PreconditionFailed'):
        store.copy_object(Bucket='private', Key='copy/blob', CopySource={'Bucket': 'private', 'Key': 'blob'})
    with pytest.raises(ClientError, match='PreconditionFailed'):
        store.put_object(Bucket='private', Key='blob', Body=payload, IfNoneMatch='*')

    listed = [entry for page in store.get_paginator('list_objects_v2').paginate(Bucket='private', Prefix='copy/')
              for entry in page['Contents']]
    assert listed == [{'Key': 'copy/blob', 'Size': len(payload)}]


def test_local_source_spills_divergent_bytes_and_detects_post_upload_source_mutation(tmp_path):
    """Divergent or truncated uploads materialize actual uploaded bytes; post-upload file mutation raises."""
    original = b'A' * 64 + b'B' * 64
    source_file = tmp_path / 'source.bin'
    source_file.write_bytes(original)
    local_source = LocalObjectSource(size=len(original), open_stream=lambda: source_file.open('rb'), path=source_file)
    store = DiskStore(
        tmp_path / 'store',
        bucket='private',
        local_sources={'mutated': local_source, 'truncated': local_source, 'verified': local_source},
    )

    # 1. Multipart upload whose second part differs from source_file (with valid ContentMD5 for the mutated bytes).
    mutated_part2 = b'C' * 64
    uid = store.create_multipart_upload(Bucket='private', Key='mutated')['UploadId']
    parts = []
    for num, chunk in enumerate((b'A' * 64, mutated_part2), start=1):
        md5 = base64.b64encode(hashlib.md5(chunk, usedforsecurity=False).digest()).decode()
        parts.append({'PartNumber': num, 'ETag': store.upload_part(
            Bucket='private', Key='mutated', UploadId=uid, PartNumber=num, Body=chunk, ContentMD5=md5)['ETag']})
    store.complete_multipart_upload(Bucket='private', Key='mutated', UploadId=uid,
                                    MultipartUpload={'Parts': parts}, IfNoneMatch='*')
    with store.get_object(Bucket='private', Key='mutated')['Body'] as body:
        assert body.read() == b'A' * 64 + mutated_part2

    # 2. Multipart upload that is a strict prefix of source_file materializes only the uploaded prefix.
    uid2 = store.create_multipart_upload(Bucket='private', Key='truncated')['UploadId']
    p1 = b'A' * 64
    md5_1 = base64.b64encode(hashlib.md5(p1, usedforsecurity=False).digest()).decode()
    etag1 = store.upload_part(Bucket='private', Key='truncated', UploadId=uid2, PartNumber=1,
                              Body=p1, ContentMD5=md5_1)['ETag']
    store.complete_multipart_upload(Bucket='private', Key='truncated', UploadId=uid2,
                                    MultipartUpload={'Parts': [{'PartNumber': 1, 'ETag': etag1}]}, IfNoneMatch='*')
    with store.get_object(Bucket='private', Key='truncated')['Body'] as body:
        assert body.read() == p1

    # 3. Clean zero-copy put_object detects if source_file is later mutated on disk before read-back.
    full_md5 = base64.b64encode(hashlib.md5(original, usedforsecurity=False).digest()).decode()
    store.put_object(Bucket='private', Key='verified', Body=original, ContentMD5=full_md5, IfNoneMatch='*')
    source_file.write_bytes(b'X' * len(original))
    with pytest.raises(OSError, match='changed before read-back'):
        store.get_object(Bucket='private', Key='verified')
