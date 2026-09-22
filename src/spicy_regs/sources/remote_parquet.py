"""Bounded Parquet streams for caller-owned, unpublished S3 staging keys.

The digest describes the bytes produced, including the Parquet footer. A head
response confirms size and supplies an ETag; it does not independently verify
that digest. Generation admission must reread the pinned object before use.
"""

from __future__ import annotations

from collections.abc import Buffer, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import io
from typing import Any, cast

import pyarrow as pa
import pyarrow.parquet as pq

READ_BUFFER_BYTES = 1024 * 1024
READ_RANGE_BYTES = 8 * 1024 * 1024
UPLOAD_PART_BYTES = 64 * 1024 * 1024


@dataclass(frozen=True)
class StoredParquet:
    """Produced bytes and the stored object's pin, pending independent admission."""

    key: str
    byte_size: int
    sha256: str
    etag: str
    rows: int


class _ClosingReadClient:
    """Close the last HTTP body even though smart_open 8.0.1 Reader.close is a no-op."""

    def __init__(self, client: Any):
        self.client = client
        self.body: Any = None

    def get_object(self, **kwargs: Any) -> Any:
        self.close()
        response = self.client.get_object(**kwargs)
        self.body = response["Body"]
        return response

    def close(self) -> None:
        if self.body is not None:
            self.body.close()
            self.body = None


@contextmanager
def open_pinned_s3(*, client: Any, bucket: str, key: str, etag: str) -> Iterator[io.BufferedIOBase]:
    """Read seekable bounded ranges; every remote GET must match the same ETag."""
    from smart_open import s3

    if not isinstance(etag, str) or not etag.strip():
        raise ValueError("A nonempty ETag is required for pinned S3 reads")
    tracked = _ClosingReadClient(client)
    try:
        with s3.open(
            bucket,
            key,
            "rb",
            client=tracked,
            client_kwargs={"S3.Client.get_object": {"IfMatch": etag}},
            defer_seek=True,
            buffer_size=READ_BUFFER_BYTES,
            range_chunk_size=READ_RANGE_BYTES,
        ) as stream:
            yield stream
    finally:
        tracked.close()


class _HashingSink(io.RawIOBase):
    """Count and hash each accepted write without buffering a whole output file."""

    def __init__(self, stream: io.BufferedIOBase, max_bytes: int):
        self.stream = stream
        self.max_bytes = max_bytes
        self.byte_size = 0
        self.digest = hashlib.sha256()

    def writable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.byte_size

    def write(self, data: Buffer) -> int:
        if self.closed:
            raise ValueError("Parquet byte sink is closed")
        view = memoryview(data).cast("B")
        if self.byte_size + len(view) > self.max_bytes:
            raise ValueError("Parquet output exceeds max_bytes")
        written = self.stream.write(view)
        if written != len(view):
            raise OSError("Incomplete Parquet stream write")
        self.digest.update(view)
        self.byte_size += written
        return written


def write_remote_parquet(
    *,
    client: Any,
    bucket: str,
    key: str,
    schema: pa.Schema,
    batches: Iterable[pa.Table | pa.RecordBatch],
    max_bytes: int,
) -> StoredParquet:
    """Write a new staging object; abort multipart state on body or close failure.

    The caller chooses a unique staging key. Conditional completion preserves
    an already-existing object. This helper never changes a publication index.
    Memory is bounded by the supplied Arrow batch and a 64 MiB upload buffer;
    max_bytes limits serialized bytes, including headers and the final footer.
    """
    from smart_open import s3

    if not isinstance(schema, pa.Schema):
        raise TypeError("schema must be an Arrow schema")
    if type(max_bytes) is not int or not 0 < max_bytes <= UPLOAD_PART_BYTES * 10_000:
        raise ValueError("max_bytes must be positive and fit within 10,000 upload parts")
    stream = cast(
        s3.MultipartWriter,
        s3.open(
            bucket,
            key,
            "wb",
            client=client,
            multipart_upload=True,
            min_part_size=UPLOAD_PART_BYTES,
            client_kwargs={
                "S3.Client.complete_multipart_upload": {"IfNoneMatch": "*"},
                "S3.Client.put_object": {"IfNoneMatch": "*"},
            },
        ),
    )
    sink = _HashingSink(stream, max_bytes)
    rows = 0
    try:
        with pq.ParquetWriter(sink, schema, compression="zstd") as writer:
            for batch in batches:
                if not isinstance(batch, (pa.Table, pa.RecordBatch)):
                    raise TypeError("Each Parquet batch must be an Arrow table or record batch")
                if not batch.schema.equals(schema, check_metadata=True):
                    raise ValueError("Parquet batch schema differs from the declared schema")
                if isinstance(batch, pa.RecordBatch):
                    writer.write_batch(batch)
                else:
                    writer.write_table(batch)
                rows += batch.num_rows
        # smart_open's __exit__ does not terminate when its own close raises.
        # Keep explicit completion inside the same abort guard as Arrow writes.
        stream.close()
    except BaseException as error:
        try:
            stream.terminate()
        except BaseException as abort_error:
            error.add_note(f"Multipart abort also failed: {type(abort_error).__name__}: {abort_error}")
        raise
    finally:
        sink.close()
    head = client.head_object(Bucket=bucket, Key=key)
    etag = head.get("ETag")
    if head.get("ContentLength") != sink.byte_size or not isinstance(etag, str) or not etag.strip():
        raise ValueError("Stored Parquet size or ETag differs from the completed upload")
    return StoredParquet(key, sink.byte_size, "sha256:" + sink.digest.hexdigest(), etag, rows)
