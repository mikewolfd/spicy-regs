"""Private disk-backed test client for the existing publication API.

This models the small S3 operation set used by a fresh local rehearsal. It has
no credentials or network transport. Unsupported calls raise instead of being
approximated. HTTP serving belongs to the qualification script's range server.
Checksums follow R2: a SHA-256 is checked on a whole put and reported by HEAD with
``ChecksumMode='ENABLED'``, refused beside Content-MD5, and not implemented on a part.
"""
from __future__ import annotations

import base64
from collections.abc import Callable, Mapping
import ctypes
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import shutil
import sys
from threading import RLock
from typing import BinaryIO, ContextManager
from uuid import uuid4

from botocore.exceptions import ClientError


def refused(code):
    return ClientError({'Error': {'Code': code}}, 'private-qualification')


def _file_state(path: Path) -> tuple[int, int, int, int, int]:
    info = path.stat()
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns


def _stream_matches_bytes(stream: BinaryIO, payload: bytes, chunk_size: int = 1024 * 1024) -> bool:
    """Compare ``payload`` against the next ``len(payload)`` bytes of ``stream`` without copying ``payload``."""
    view = memoryview(payload)
    offset, total = 0, len(payload)
    while offset < total:
        piece = stream.read(min(chunk_size, total - offset))
        if not piece or view[offset : offset + len(piece)] != piece:
            return False
        offset += len(piece)
    return True


def _copy_exact(source: BinaryIO, sink: BinaryIO, count: int, chunk_size: int = 1024 * 1024) -> None:
    remaining = count
    while remaining > 0:
        piece = source.read(min(chunk_size, remaining))
        if not piece:
            raise OSError('Local source ended before expected byte count')
        sink.write(piece)
        remaining -= len(piece)


def _clone_or_copy(source: Path, target: Path) -> bool:
    """Copy ``source`` to ``target``, preferring APFS copy-on-write; return True when bytes were copied."""
    if sys.platform == 'darwin':
        library = ctypes.CDLL(None, use_errno=True)
        if library.clonefile(os.fsencode(source), os.fsencode(target), 0) == 0:
            return False
    shutil.copyfile(source, target)
    return True


@dataclass(frozen=True)
class LocalObjectSource:
    """Read-only local source for zero-duplication dry-run verification in ``DiskStore``.

    ``open_stream`` returns a context manager yielding a binary stream of the exact local bytes
    (a regular file or a decoded central-raw archive member). ``DiskStore`` never writes to or
    hard-links the underlying local file: it verifies every byte received through S3 APIs against
    ``open_stream`` (alongside Content-MD5, ChecksumSHA256, ContentLength and preconditions) and
    serves read-backs from the verified read-only source without duplicating payload bytes on disk.
    """

    size: int
    open_stream: Callable[[], ContextManager[BinaryIO]]
    path: Path | None = None


@dataclass(frozen=True)
class _VerifiedReference:
    source: LocalObjectSource
    size: int
    sha256: str
    file_state: tuple[int, int, int, int, int] | None


class _ContextStream:
    """Expose a context-managed binary stream as a closeable S3 ``Body``."""

    def __init__(
        self,
        manager: ContextManager[BinaryIO],
        *,
        expected_path: Path | None = None,
        expected_state: tuple[int, int, int, int, int] | None = None,
    ):
        self._manager = manager
        self._expected_path = expected_path
        self._expected_state = expected_state
        if expected_path is not None and expected_state is not None and _file_state(expected_path) != expected_state:
            raise OSError(f'Verified local source changed before read-back: {expected_path}')
        self._stream = manager.__enter__()
        self.closed = False

    def read(self, size: int = -1) -> bytes:
        return self._stream.read(size)

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self._manager.__exit__(None, None, None)
            if (
                self._expected_path is not None
                and self._expected_state is not None
                and _file_state(self._expected_path) != self._expected_state
            ):
                raise OSError(f'Verified local source changed during read-back: {self._expected_path}')

    def __enter__(self) -> _ContextStream:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class DiskStore:
    def __init__(
        self,
        root: Path,
        *,
        bucket: str,
        local_sources: Mapping[str, LocalObjectSource] | None = None,
    ):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=False)
        self.bucket = bucket
        self.objects: dict[str, Path] = {}
        self._references: dict[str, _VerifiedReference] = {}
        self._local_sources: dict[str, LocalObjectSource] = dict(local_sources or {})
        self.etags: dict[str, str] = {}
        self.checksums: dict[str, str] = {}
        self.uploads: dict[str, dict] = {}
        self.calls: list[dict] = []
        self.disk_bytes_written: int = 0
        self.lock = RLock()

    def _exists(self, key: str) -> bool:
        return key in self.objects or key in self._references

    def _size(self, key: str) -> int:
        if key in self.objects:
            return self.objects[key].stat().st_size
        return self._references[key].size

    def _all_keys(self) -> list[str]:
        return sorted(self.objects.keys() | self._references.keys())

    @staticmethod
    def _close_upload_stream(upload: dict) -> None:
        manager = upload.pop('expected_cm', None)
        upload.pop('expected_stream', None)
        if manager is not None:
            manager.__exit__(None, None, None)

    @staticmethod
    def options(options):
        if set(options) - {'ContentType', 'CacheControl'}:
            raise ValueError('Unsupported private store options')

    def path(self, bucket, key):
        if (bucket != self.bucket or not key or key.startswith('/') or
                any(part in ('', '.', '..') for part in key.split('/'))):
            raise ValueError('Private store requires its exact bucket and a relative object key')
        return self.root / 'objects' / key

    def head_object(self, *, Bucket, Key, ChecksumMode=None):
        self.path(Bucket, Key)
        if ChecksumMode not in (None, 'ENABLED'):
            raise ValueError('Unsupported private store checksum mode')
        with self.lock:
            if not self._exists(Key):
                raise refused('NoSuchKey')
            head = {'ContentLength': self._size(Key), 'ETag': self.etags[Key]}
            if ChecksumMode == 'ENABLED' and Key in self.checksums:
                head['ChecksumSHA256'] = self.checksums[Key]
            return head

    def get_object(self, *, Bucket, Key):
        with self.lock:
            result = self.head_object(Bucket=Bucket, Key=Key)
            self.calls.append({'method': 'get_object', 'key': Key})
            if Key in self.objects:
                body: BinaryIO | _ContextStream = self.objects[Key].open('rb')
            else:
                reference = self._references[Key]
                body = _ContextStream(
                    reference.source.open_stream(),
                    expected_path=reference.source.path,
                    expected_state=reference.file_state,
                )
            return {**result, 'Body': body}

    def _condition(self, key, if_match, if_none_match):
        if if_none_match not in (None, '*'):
            raise ValueError('Unsupported private store condition')
        if (if_none_match == '*' and self._exists(key)) or (
                if_match is not None and self.etags.get(key) != if_match):
            raise refused('PreconditionFailed')

    @staticmethod
    def _one_checksum(md5, sha256):
        # R2: "You can only specify one non-default checksum at a time."
        if md5 is not None and sha256 is not None:
            raise refused('InvalidRequest')

    def put_object(self, *, Bucket, Key, Body, IfMatch=None, IfNoneMatch=None, ContentMD5=None,
                   ContentLength=None, ChecksumSHA256=None, **kwargs):
        target = self.path(Bucket, Key)
        self.options(kwargs)
        self._one_checksum(ContentMD5, ChecksumSHA256)
        with self.lock:
            self._condition(Key, IfMatch, IfNoneMatch)
        source = self._local_sources.get(Key)
        if source is not None:
            return self._put_with_local_source(
                target,
                Key,
                Body,
                source,
                IfMatch=IfMatch,
                IfNoneMatch=IfNoneMatch,
                ContentMD5=ContentMD5,
                ContentLength=ContentLength,
                ChecksumSHA256=ChecksumSHA256,
            )
        temporary = self.root / ('put-' + uuid4().hex)
        digest, sha256, size = hashlib.md5(usedforsecurity=False), hashlib.sha256(), 0
        with temporary.open('xb') as sink:
            chunks = iter(lambda: Body.read(1024 * 1024), b'') if hasattr(Body, 'read') else (Body,)
            for chunk in chunks:
                sink.write(chunk)
                digest.update(chunk)
                sha256.update(chunk)
                size += len(chunk)
        with self.lock:
            self.disk_bytes_written += size
        if ((ContentMD5 is not None and base64.b64encode(digest.digest()).decode() != ContentMD5) or
                (ChecksumSHA256 is not None and base64.b64encode(sha256.digest()).decode() != ChecksumSHA256) or
                (ContentLength is not None and ContentLength != size)):
            temporary.unlink()
            raise refused('BadDigest')
        try:
            with self.lock:
                # A competing writer may have committed while Body was streamed.
                self._condition(Key, IfMatch, IfNoneMatch)
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary.replace(target)
                self._references.pop(Key, None)
                self.objects[Key], self.etags[Key] = target, '"' + digest.hexdigest() + '"'
                self.checksums.pop(Key, None)
                if ChecksumSHA256 is not None:
                    self.checksums[Key] = ChecksumSHA256
                self.calls.append({'method': 'put_object', 'key': Key, 'bytes': size})
                return {'ETag': self.etags[Key]}
        finally:
            temporary.unlink(missing_ok=True)

    def _put_with_local_source(
        self,
        target: Path,
        key: str,
        body,
        source: LocalObjectSource,
        *,
        IfMatch=None,
        IfNoneMatch=None,
        ContentMD5=None,
        ContentLength=None,
        ChecksumSHA256=None,
    ):
        before_state = _file_state(source.path) if source.path is not None else None
        digest, sha256, size = hashlib.md5(usedforsecurity=False), hashlib.sha256(), 0
        matched = True
        temporary: Path | None = None
        sink: BinaryIO | None = None
        written_bytes = 0
        chunks = iter(lambda: body.read(1024 * 1024), b'') if hasattr(body, 'read') else (body,)
        try:
            with source.open_stream() as expected:
                for chunk in chunks:
                    if matched:
                        if not _stream_matches_bytes(expected, chunk):
                            matched = False
                            temporary = self.root / ('put-' + uuid4().hex)
                            sink = temporary.open('xb')
                            if size > 0:
                                with source.open_stream() as replay:
                                    _copy_exact(replay, sink, size)
                                written_bytes += size
                            sink.write(chunk)
                            written_bytes += len(chunk)
                    else:
                        assert sink is not None
                        sink.write(chunk)
                        written_bytes += len(chunk)
                    digest.update(chunk)
                    sha256.update(chunk)
                    size += len(chunk)
                if matched and expected.read(1):
                    matched = False
                    temporary = self.root / ('put-' + uuid4().hex)
                    sink = temporary.open('xb')
                    if size > 0:
                        with source.open_stream() as replay:
                            _copy_exact(replay, sink, size)
                        written_bytes += size
            if sink is not None:
                sink.close()
                sink = None
            if written_bytes:
                with self.lock:
                    self.disk_bytes_written += written_bytes
            if matched and source.path is not None and _file_state(source.path) != before_state:
                raise OSError(f'Verified local source changed during upload: {source.path}')
            if ((ContentMD5 is not None and base64.b64encode(digest.digest()).decode() != ContentMD5) or
                    (ChecksumSHA256 is not None and base64.b64encode(sha256.digest()).decode() != ChecksumSHA256) or
                    (ContentLength is not None and ContentLength != size)):
                raise refused('BadDigest')
            with self.lock:
                self._condition(key, IfMatch, IfNoneMatch)
                if matched:
                    self.objects.pop(key, None)
                    self._references[key] = _VerifiedReference(
                        source=source, size=size, sha256=sha256.hexdigest(), file_state=before_state
                    )
                else:
                    assert temporary is not None
                    self._references.pop(key, None)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    temporary.replace(target)
                    self.objects[key] = target
                self.etags[key] = '"' + digest.hexdigest() + '"'
                self.checksums.pop(key, None)
                if ChecksumSHA256 is not None:
                    self.checksums[key] = ChecksumSHA256
                self.calls.append({'method': 'put_object', 'key': key, 'bytes': size})
                return {'ETag': self.etags[key]}
        finally:
            if sink is not None:
                sink.close()
            if temporary is not None:
                temporary.unlink(missing_ok=True)

    def copy_object(self, *, Bucket, Key, CopySource):
        """A server-side copy as R2 makes one: the source's bytes and ETag, no stored SHA-256, never an overwrite."""
        target, source = self.path(Bucket, Key), CopySource['Key']
        self.path(CopySource['Bucket'], source)
        with self.lock:
            if not self._exists(source):
                raise refused('NoSuchKey')
            if self._exists(Key):
                raise refused('PreconditionFailed')
            if source in self._references:
                reference = self._references[source]
                self._references[Key] = reference
                self.etags[Key] = self.etags[source]
                self.checksums.pop(Key, None)
                self.calls.append({'method': 'copy_object', 'key': Key, 'bytes': reference.size})
                return {}
            target.parent.mkdir(parents=True, exist_ok=True)
            copied = _clone_or_copy(self.objects[source], target)
            size = target.stat().st_size
            if copied:
                self.disk_bytes_written += size
            self.objects[Key], self.etags[Key] = target, self.etags[source]
            self.checksums.pop(Key, None)
            self.calls.append({'method': 'copy_object', 'key': Key, 'bytes': size})
            return {}

    def get_paginator(self, name):
        if name != 'list_objects_v2':
            raise ValueError('Unsupported private store paginator')
        return self

    def paginate(self, *, Bucket, Prefix):
        self.path(Bucket, Prefix.rstrip('/'))
        with self.lock:
            entries = [{'Key': key, 'Size': self._size(key)}
                       for key in self._all_keys() if key.startswith(Prefix)]
        for offset in range(0, len(entries), 1000):
            yield {'Contents': entries[offset:offset + 1000]}

    def create_multipart_upload(self, *, Bucket, Key, **kwargs):
        self.path(Bucket, Key)
        self.options(kwargs)
        identity = uuid4().hex
        source = self._local_sources.get(Key)
        if source is not None:
            before_state = _file_state(source.path) if source.path is not None else None
            expected_cm = source.open_stream()
            expected_stream = expected_cm.__enter__()
            upload = {
                'key': Key,
                'directory': None,
                'parts': [],
                'source': source,
                'source_state': before_state,
                'expected_cm': expected_cm,
                'expected_stream': expected_stream,
                'matched': True,
                'size': 0,
                'sha256': hashlib.sha256(),
            }
        else:
            directory = self.root / 'multipart' / identity
            directory.mkdir(parents=True)
            upload = {
                'key': Key,
                'directory': directory,
                'parts': [],
                'source': None,
                'source_state': None,
                'matched': False,
                'size': 0,
                'sha256': hashlib.sha256(),
            }
        with self.lock:
            self.uploads[identity] = upload
        return {'UploadId': identity}

    def _upload(self, bucket, key, identity):
        self.path(bucket, key)
        with self.lock:
            upload = self.uploads[identity]
        if upload['key'] != key:
            raise ValueError('Private multipart identity belongs to a different object')
        return upload

    def _spill_upload_to_disk(self, upload: dict, identity: str, part_number: int, body: bytes) -> Path:
        self._close_upload_stream(upload)
        upload['matched'] = False
        directory = self.root / 'multipart' / identity
        directory.mkdir(parents=True)
        upload['directory'] = directory
        replayed = 0
        if upload['parts']:
            with upload['source'].open_stream() as replay:
                for prior in upload['parts']:
                    prior_path = directory / str(prior['PartNumber'])
                    with prior_path.open('xb') as sink:
                        _copy_exact(replay, sink, prior['size'])
                    prior['path'] = prior_path
                    replayed += prior['size']
        path = directory / str(part_number)
        path.write_bytes(body)
        with self.lock:
            self.disk_bytes_written += replayed + len(body)
        return path

    def upload_part(self, *, Bucket, Key, UploadId, PartNumber, Body, ContentMD5, ChecksumSHA256=None):
        if ChecksumSHA256 is not None:
            raise refused('NotImplemented')  # R2 on UploadPart, probe 2026-11-01
        upload = self._upload(Bucket, Key, UploadId)
        if PartNumber != len(upload['parts']) + 1:
            raise ValueError('Private multipart parts must arrive in order')
        digest = hashlib.md5(Body, usedforsecurity=False)
        if base64.b64encode(digest.digest()).decode() != ContentMD5:
            raise refused('BadDigest')
        path: Path | None
        if upload['matched']:
            if _stream_matches_bytes(upload['expected_stream'], Body):
                path = None
            else:
                path = self._spill_upload_to_disk(upload, UploadId, PartNumber, Body)
        else:
            path = upload['directory'] / str(PartNumber)
            path.write_bytes(Body)
            with self.lock:
                self.disk_bytes_written += len(Body)
        etag = '"' + digest.hexdigest() + '"'
        upload['sha256'].update(Body)
        upload['size'] += len(Body)
        upload['parts'].append({
            'PartNumber': PartNumber,
            'ETag': etag,
            'path': path,
            'digest': digest.digest(),
            'size': len(Body),
        })
        with self.lock:
            self.calls.append({'method': 'upload_part', 'key': Key, 'bytes': len(Body)})
        return {'ETag': etag}

    def complete_multipart_upload(self, *, Bucket, Key, UploadId, MultipartUpload, IfNoneMatch):
        upload = self._upload(Bucket, Key, UploadId)
        wanted = [{'PartNumber': part['PartNumber'], 'ETag': part['ETag']} for part in upload['parts']]
        if not wanted or MultipartUpload != {'Parts': wanted}:
            raise ValueError('Private multipart completion differs from uploaded parts')
        with self.lock:
            self._condition(Key, None, IfNoneMatch)
        digest = hashlib.md5(b''.join(part['digest'] for part in upload['parts']), usedforsecurity=False)
        etag = f'"{digest.hexdigest()}-{len(wanted)}"'
        target = self.path(Bucket, Key)
        if upload['matched']:
            trailing = upload['expected_stream'].read(1)
            self._close_upload_stream(upload)
            source = upload['source']
            if not trailing:
                if source.path is not None and _file_state(source.path) != upload['source_state']:
                    raise OSError(f'Verified local source changed during upload: {source.path}')
                with self.lock:
                    self._condition(Key, None, IfNoneMatch)
                    self.objects.pop(Key, None)
                    self._references[Key] = _VerifiedReference(
                        source=source,
                        size=upload['size'],
                        sha256=upload['sha256'].hexdigest(),
                        file_state=upload['source_state'],
                    )
                    self.etags[Key] = etag
                    self.checksums.pop(Key, None)
                    self.abort_multipart_upload(Bucket=Bucket, Key=Key, UploadId=UploadId)
                    return {'ETag': etag}
            # Uploaded bytes were a strict prefix of the local source: materialize those exact bytes.
            directory = self.root / 'multipart' / UploadId
            directory.mkdir(parents=True)
            upload['directory'] = directory
            temporary = directory / 'complete'
            with source.open_stream() as replay, temporary.open('xb') as sink:
                _copy_exact(replay, sink, upload['size'])
            with self.lock:
                self.disk_bytes_written += upload['size']
        else:
            temporary = upload['directory'] / 'complete'
            reassembled = 0
            with temporary.open('xb') as sink:
                for part in upload['parts']:
                    with part['path'].open('rb') as source:
                        shutil.copyfileobj(source, sink, 1024 * 1024)
                    reassembled += part['size']
            with self.lock:
                self.disk_bytes_written += reassembled
        try:
            with self.lock:
                self._condition(Key, None, IfNoneMatch)
                target.parent.mkdir(parents=True, exist_ok=True)
                temporary.replace(target)
                self._references.pop(Key, None)
                self.objects[Key], self.etags[Key] = target, etag
                self.checksums.pop(Key, None)
                self.abort_multipart_upload(Bucket=Bucket, Key=Key, UploadId=UploadId)
                return {'ETag': etag}
        finally:
            temporary.unlink(missing_ok=True)

    def abort_multipart_upload(self, *, Bucket, Key, UploadId):
        upload = self._upload(Bucket, Key, UploadId)
        self._close_upload_stream(upload)
        if upload['directory'] is not None:
            shutil.rmtree(upload['directory'])
        with self.lock:
            del self.uploads[UploadId]
