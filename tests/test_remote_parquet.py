"""Real smart_open/PyArrow streaming against an in-memory conditional S3 client."""

from dataclasses import FrozenInstanceError
import hashlib
from io import BytesIO
import re
import os
from typing import Any, cast

from botocore.exceptions import ClientError
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from smart_open import s3

from spicy_regs.sources import remote_parquet as remote


class Store:
    """In-memory S3 stand-in: conditional range gets and puts, multipart upload with aborts, and failure switches."""

    def __init__(self):
        self.objects = {}
        self.uploads = {}
        self.gets = []
        self.bodies = []
        self.aborts = []
        self.completed = []
        self.part_sizes = []
        self.fail_complete = False
        self.fail_part = False
        self.bad_head = None

    @staticmethod
    def etag(body):
        return '"' + hashlib.sha256(body).hexdigest() + '"'

    def get_object(self, *, Bucket, Key, Range, IfMatch):
        self.gets.append((Key, Range, IfMatch))
        body = self.objects[Key]
        if IfMatch != self.etag(body):
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "GetObject")
        match = re.fullmatch(r"bytes=(\d*)-(\d*)", Range)
        assert match
        start = int(match[1]) if match[1] else max(0, len(body) - int(match[2]))
        stop = min(int(match[2]) if match[2] and match[1] else len(body) - 1, len(body) - 1)
        if start >= len(body):
            raise ClientError(
                cast(Any, {"Error": {"Code": "InvalidRange", "ActualObjectSize": str(len(body))}}), "GetObject"
            )
        selected = body[start : stop + 1]
        stream = BytesIO(selected)
        self.bodies.append(stream)
        return {
            "Body": stream,
            "ETag": self.etag(body),
            "ContentLength": len(selected),
            "ContentRange": f"bytes {start}-{stop}/{len(body)}",
            "ResponseMetadata": {"HTTPStatusCode": 206, "RetryAttempts": 0},
        }

    def create_multipart_upload(self, *, Bucket, Key):
        self.uploads[Key] = []
        return {"UploadId": Key}

    def upload_part(self, *, Bucket, Key, UploadId, PartNumber, Body):
        if self.fail_part:
            raise RuntimeError("part interrupted")
        assert PartNumber == len(self.uploads[Key]) + 1
        data = Body.read()
        self.part_sizes.append(len(data))
        self.uploads[Key].append(data)
        return {"ETag": str(PartNumber)}

    def complete_multipart_upload(self, *, Bucket, Key, UploadId, MultipartUpload, IfNoneMatch):
        assert IfNoneMatch == "*"
        if self.fail_complete:
            raise RuntimeError("completion interrupted")
        self.put_object(Bucket=Bucket, Key=Key, Body=b"".join(self.uploads[Key]), IfNoneMatch=IfNoneMatch)
        self.completed.append(Key)
        del self.uploads[Key]
        return {"ETag": self.etag(self.objects[Key])}

    def put_object(self, *, Bucket, Key, Body, IfNoneMatch):
        assert IfNoneMatch == "*"
        if Key in self.objects:
            raise ClientError({"Error": {"Code": "PreconditionFailed"}}, "PutObject")
        self.objects[Key] = Body
        return {"ETag": self.etag(Body)}

    def abort_multipart_upload(self, *, Bucket, Key, UploadId):
        self.aborts.append(Key)
        self.uploads.pop(Key, None)

    def head_object(self, *, Bucket, Key):
        actual = {"ContentLength": len(self.objects[Key]), "ETag": self.etag(self.objects[Key])}
        return actual | (self.bad_head or {})


SCHEMA = pa.schema([("id", pa.int64()), ("text", pa.string())])


def write(store, batches, **kwargs):
    """Write ``batches`` through ``write_remote_parquet`` to the fixed staging key."""
    return remote.write_remote_parquet(
        client=store,
        bucket="fork",
        key="staging/body.parquet",
        schema=SCHEMA,
        batches=batches,
        max_bytes=kwargs.pop("max_bytes", 1024 * 1024),
        **kwargs,
    )


def test_parquet_roundtrip_preserves_literals_nulls_and_footer_digest():
    store = Store()
    table = pa.Table.from_pylist(
        [{"id": 1, "text": None}, {"id": 2, "text": ""}, {"id": 3, "text": "<p>Literal &amp; markup\nΔ</p>"}],
        schema=SCHEMA,
    )
    result = write(store, [table.slice(0, 1), table.slice(1).to_batches()[0]])
    body = store.objects[result.key]
    assert body[:4] == body[-4:] == b"PAR1"
    assert result.sha256 == "sha256:" + hashlib.sha256(body).hexdigest()
    assert result.byte_size == len(body) and result.rows == 3 and result.etag == store.etag(body)
    assert pq.read_table(BytesIO(body)).equals(table, check_metadata=True)
    with pytest.raises(FrozenInstanceError):
        setattr(result, "rows", 99)
    assert not store.uploads and not store.aborts


def test_empty_iterator_writes_valid_zero_row_parquet():
    store = Store()
    result = write(store, iter(()))
    restored = pq.read_table(BytesIO(store.objects[result.key]))
    assert result.rows == 0 and restored.num_rows == 0 and restored.schema.equals(SCHEMA)
    assert result.byte_size > 0


def test_reader_defers_gets_and_pins_every_ranged_seek():
    store = Store()
    body = bytes(range(256)) * (10 * 1024 * 1024 // 256)
    store.objects["source"] = body
    with remote.open_pinned_s3(client=store, bucket="fork", key="source", etag=store.etag(body)) as reader:
        assert not store.gets and reader.seekable()
        assert reader.read(16) == body[:16]
        reader.seek(9 * 1024 * 1024)
        assert reader.read(16) == body[9 * 1024 * 1024 : 9 * 1024 * 1024 + 16]
        reader.seek(0)
        assert reader.read(16) == body[:16]
    assert all(body.closed for body in store.bodies) and len(store.gets) >= 3
    for _, selected, etag in store.gets:
        start, stop = map(int, selected.removeprefix("bytes=").split("-"))
        assert stop - start + 1 <= 8 * 1024 * 1024 and etag == store.etag(body)


def test_reader_refuses_source_change_on_later_range():
    store = Store()
    store.objects["source"] = b"a" * (10 * 1024 * 1024)
    etag = store.etag(store.objects["source"])
    with remote.open_pinned_s3(client=store, bucket="fork", key="source", etag=etag) as reader:
        assert reader.read(1) == b"a"
        store.objects["source"] = b"b" * (10 * 1024 * 1024)
        with pytest.raises(OSError):
            reader.seek(9 * 1024 * 1024)
            reader.read(1)
    assert all(row[2] == etag for row in store.gets)


@pytest.mark.parametrize("failure", ["body", "part", "complete", "schema", "metadata", "type", "bound"])
def test_failed_upload_aborts_without_creating_object(failure):
    store = Store()
    table = pa.Table.from_pylist([{"id": 1, "text": "ok"}], schema=SCHEMA)

    def batches():
        yield table
        if failure == "body":
            raise KeyboardInterrupt("producer interrupted")
        if failure == "schema":
            yield pa.table({"wrong": [1]})
        if failure == "metadata":
            yield table.replace_schema_metadata({b"changed": b"yes"})
        if failure == "type":
            yield "not Arrow"

    store.fail_part = failure == "part"
    store.fail_complete = failure == "complete"
    expected: type[BaseException] = {
        "body": KeyboardInterrupt,
        "part": RuntimeError,
        "complete": RuntimeError,
        "schema": ValueError,
        "metadata": ValueError,
        "type": TypeError,
        "bound": ValueError,
    }[failure]
    with pytest.raises(expected):
        write(store, batches(), max_bytes=4 if failure == "bound" else 1024 * 1024)
    assert store.objects == {} and store.uploads == {} and store.aborts == ["staging/body.parquet"]


def test_existing_key_refuses_completion_and_preserves_original():
    store = Store()
    store.objects["staging/body.parquet"] = b"prior original"
    with pytest.raises(ClientError):
        write(store, [])
    assert store.objects == {"staging/body.parquet": b"prior original"}
    assert store.uploads == {} and store.aborts == ["staging/body.parquet"]


@pytest.mark.parametrize("head", [{"ContentLength": 1}, {"ETag": None}, {"ETag": ""}])
def test_head_without_matching_size_and_nonempty_etag_refuses_receipt(head):
    store = Store()
    store.bad_head = head
    with pytest.raises(ValueError, match="size or ETag"):
        write(store, [])
    assert store.completed == ["staging/body.parquet"]  # unadmitted staging bytes are not deleted


def test_stream_configuration_includes_both_conditional_completion_paths(monkeypatch):
    original = s3.open
    observed = []

    def open_stream(*args, **kwargs):
        observed.append(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(s3, "open", open_stream)
    write(Store(), [])
    assert observed[0]["min_part_size"] == 64 * 1024 * 1024
    assert observed[0]["client_kwargs"] == {
        "S3.Client.complete_multipart_upload": {"IfNoneMatch": "*"},
        "S3.Client.put_object": {"IfNoneMatch": "*"},
    }


def test_sink_rejects_oversized_chunk_before_forwarding_it():
    target = BytesIO()
    sink = remote._HashingSink(target, 4)
    assert sink.write(b"abc") == 3
    with pytest.raises(ValueError, match="max_bytes"):
        sink.write(b"de")
    assert target.getvalue() == b"abc" and sink.byte_size == 3
    assert sink.digest.hexdigest() == hashlib.sha256(b"abc").hexdigest()


@pytest.mark.parametrize("bound", [0, -1, True, remote.UPLOAD_PART_BYTES * 10_000 + 1])
def test_invalid_service_byte_bound_refuses_before_upload_creation(bound):
    store = Store()
    with pytest.raises(ValueError, match="max_bytes"):
        write(store, [], max_bytes=bound)
    assert not store.uploads and not store.aborts and not store.objects


def test_reader_context_closes_partial_http_body_on_interruption():
    store = Store()
    store.objects["source"] = b"a" * (2 * 1024 * 1024)
    with pytest.raises(KeyboardInterrupt):
        with remote.open_pinned_s3(
            client=store, bucket="fork", key="source", etag=store.etag(store.objects["source"])
        ) as reader:
            assert reader.read(1) == b"a"
            raise KeyboardInterrupt
    assert len(store.bodies) == 1 and store.bodies[0].closed


def test_multiple_parts_stream_and_reread_through_pinned_ranges(monkeypatch):
    # Smaller valid service parts exercise rollover without a large fixture.
    monkeypatch.setattr(remote, "UPLOAD_PART_BYTES", 5 * 1024 * 1024)
    store = Store()
    schema = pa.schema([("blob", pa.binary())])
    values = [os.urandom(6 * 1024 * 1024), os.urandom(6 * 1024 * 1024)]
    batches = [pa.table({"blob": [value]}, schema=schema) for value in values]
    result = remote.write_remote_parquet(
        client=store,
        bucket="fork",
        key="staging/large.parquet",
        schema=schema,
        batches=iter(batches),
        max_bytes=20 * 1024 * 1024,
    )
    assert len(store.part_sizes) == 3 and max(store.part_sizes) <= 5 * 1024 * 1024
    with remote.open_pinned_s3(client=store, bucket="fork", key=result.key, etag=result.etag) as stream:
        restored = pq.read_table(stream)
    assert restored.column("blob").to_pylist() == values and result.rows == 2
    assert all(body.closed for body in store.bodies)


@pytest.mark.parametrize("etag", ["", "  ", None])
def test_missing_pin_refuses_before_first_source_request(etag):
    store = Store()
    with pytest.raises(ValueError, match="ETag"):
        with remote.open_pinned_s3(client=store, bucket="fork", key="source", etag=etag):
            pytest.fail("unpinned source admitted")
    assert not store.gets


def test_interruption_after_uploaded_part_aborts_all_remote_parts(monkeypatch):
    monkeypatch.setattr(remote, "UPLOAD_PART_BYTES", 5 * 1024 * 1024)
    store = Store()
    schema = pa.schema([("blob", pa.binary())])

    def interrupted():
        yield pa.table({"blob": [os.urandom(6 * 1024 * 1024)]}, schema=schema)
        assert store.part_sizes  # a failure after remote progress, not only an in-memory buffer
        raise KeyboardInterrupt("stop after first uploaded part")

    with pytest.raises(KeyboardInterrupt):
        remote.write_remote_parquet(
            client=store,
            bucket="fork",
            key="staging/interrupted.parquet",
            schema=schema,
            batches=interrupted(),
            max_bytes=20 * 1024 * 1024,
        )
    assert store.objects == {} and store.uploads == {} and store.aborts == ["staging/interrupted.parquet"]
