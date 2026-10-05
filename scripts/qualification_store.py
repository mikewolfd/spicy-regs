"""Private disk-backed test client for the existing publication API.

This models the small S3 operation set used by a fresh local rehearsal. It has
no credentials or network transport. Unsupported calls raise instead of being
approximated. HTTP serving belongs to the qualification script's range server.
"""
from __future__ import annotations

import base64
import hashlib
from pathlib import Path
import shutil
from uuid import uuid4

from botocore.exceptions import ClientError


def refused(code):
    return ClientError({'Error': {'Code': code}}, 'private-qualification')


class DiskStore:
    def __init__(self, root: Path, *, bucket: str):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=False)
        self.bucket = bucket
        self.objects: dict[str, Path] = {}
        self.etags: dict[str, str] = {}
        self.uploads: dict[str, dict] = {}
        self.calls: list[dict] = []

    def path(self, bucket, key):
        if (bucket != self.bucket or not key or key.startswith('/') or
                any(part in ('', '.', '..') for part in key.split('/'))):
            raise ValueError('Private store requires its exact bucket and a relative object key')
        return self.root / 'objects' / key

    def head_object(self, *, Bucket, Key):
        self.path(Bucket, Key)
        if Key not in self.objects:
            raise refused('NoSuchKey')
        return {'ContentLength': self.objects[Key].stat().st_size, 'ETag': self.etags[Key]}

    def get_object(self, *, Bucket, Key):
        result = self.head_object(Bucket=Bucket, Key=Key)
        self.calls.append({'method': 'get_object', 'key': Key})
        return {**result, 'Body': self.objects[Key].open('rb')}

    def _condition(self, key, if_match, if_none_match):
        if if_none_match not in (None, '*'):
            raise ValueError('Unsupported private store condition')
        if (if_none_match == '*' and key in self.objects) or (
                if_match is not None and self.etags.get(key) != if_match):
            raise refused('PreconditionFailed')

    def put_object(self, *, Bucket, Key, Body, IfMatch=None, IfNoneMatch=None, ContentMD5=None,
                   ContentLength=None, **kwargs):
        target = self.path(Bucket, Key)
        self._condition(Key, IfMatch, IfNoneMatch)
        temporary = self.root / ('put-' + uuid4().hex)
        digest, size = hashlib.md5(usedforsecurity=False), 0
        with temporary.open('xb') as sink:
            chunks = iter(lambda: Body.read(1024 * 1024), b'') if hasattr(Body, 'read') else (Body,)
            for chunk in chunks:
                sink.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        if ((ContentMD5 is not None and base64.b64encode(digest.digest()).decode() != ContentMD5) or
                (ContentLength is not None and ContentLength != size)):
            temporary.unlink()
            raise refused('BadDigest')
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary.replace(target)
        self.objects[Key], self.etags[Key] = target, '"' + digest.hexdigest() + '"'
        self.calls.append({'method': 'put_object', 'key': Key, 'bytes': size})
        return {'ETag': self.etags[Key]}

    def get_paginator(self, name):
        if name != 'list_objects_v2':
            raise ValueError('Unsupported private store paginator')
        return self

    def paginate(self, *, Bucket, Prefix):
        self.path(Bucket, Prefix.rstrip('/'))
        entries = [{'Key': key, 'Size': path.stat().st_size}
                   for key, path in sorted(self.objects.items()) if key.startswith(Prefix)]
        for offset in range(0, len(entries), 1000):
            yield {'Contents': entries[offset:offset + 1000]}

    def create_multipart_upload(self, *, Bucket, Key, **kwargs):
        self.path(Bucket, Key)
        identity = uuid4().hex
        directory = self.root / 'multipart' / identity
        directory.mkdir(parents=True)
        self.uploads[identity] = {'key': Key, 'directory': directory, 'parts': []}
        return {'UploadId': identity}

    def _upload(self, bucket, key, identity):
        self.path(bucket, key)
        upload = self.uploads[identity]
        if upload['key'] != key:
            raise ValueError('Private multipart identity belongs to a different object')
        return upload

    def upload_part(self, *, Bucket, Key, UploadId, PartNumber, Body, ContentMD5):
        upload = self._upload(Bucket, Key, UploadId)
        if PartNumber != len(upload['parts']) + 1:
            raise ValueError('Private multipart parts must arrive in order')
        digest = hashlib.md5(Body, usedforsecurity=False)
        if base64.b64encode(digest.digest()).decode() != ContentMD5:
            raise refused('BadDigest')
        path = upload['directory'] / str(PartNumber)
        path.write_bytes(Body)
        etag = '"' + digest.hexdigest() + '"'
        upload['parts'].append({'PartNumber': PartNumber, 'ETag': etag, 'path': path, 'digest': digest.digest()})
        self.calls.append({'method': 'upload_part', 'key': Key, 'bytes': len(Body)})
        return {'ETag': etag}

    def complete_multipart_upload(self, *, Bucket, Key, UploadId, MultipartUpload, IfNoneMatch):
        upload = self._upload(Bucket, Key, UploadId)
        wanted = [{name: part[name] for name in ('PartNumber', 'ETag')} for part in upload['parts']]
        if not wanted or MultipartUpload != {'Parts': wanted}:
            raise ValueError('Private multipart completion differs from uploaded parts')
        self._condition(Key, None, IfNoneMatch)
        target = self.path(Bucket, Key)
        temporary = upload['directory'] / 'complete'
        with temporary.open('xb') as sink:
            for part in upload['parts']:
                with part['path'].open('rb') as source:
                    shutil.copyfileobj(source, sink, 1024 * 1024)
        digest = hashlib.md5(b''.join(part['digest'] for part in upload['parts']), usedforsecurity=False)
        etag = f'"{digest.hexdigest()}-{len(wanted)}"'
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary.replace(target)
        self.objects[Key], self.etags[Key] = target, etag
        self.abort_multipart_upload(Bucket=Bucket, Key=Key, UploadId=UploadId)
        return {'ETag': etag}

    def abort_multipart_upload(self, *, Bucket, Key, UploadId):
        upload = self._upload(Bucket, Key, UploadId)
        shutil.rmtree(upload['directory'])
        del self.uploads[UploadId]
