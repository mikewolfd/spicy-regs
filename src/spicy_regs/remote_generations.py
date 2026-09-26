"""Stage and publish ordinary generations without full local table copies.

Parquet members stay in an unreferenced remote staging prefix. Small artifact
metadata stays local. The existing verifier and publisher still own admission,
family membership, shrink checks and the conditional publication pointer.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb
import pyarrow.parquet as pq

from spicy_regs.generations import verify_generation_source, _write_generation_metadata
from spicy_regs.sources.publication import (
    PART_BYTES,
    PublicationError,
    _create_multipart,
    _publish_verified_generation,
    _put_immutable,
    _S3Members,
)

if TYPE_CHECKING:
    from spicy_regs.sources.remote_parquet import StoredParquet


class _RemoteGenerationSource:
    """Member source over staged remote Parquet plus local artifact metadata.

    Refuses an unnamed staging prefix, a member key outside it, duplicate names,
    an empty member set, or a member without a source etag.
    """

    def __init__(self, directory: Path, *, client, bucket: str, staging_prefix: str,
                 members: Sequence[StoredParquet]):
        from rulespec_artifacts import LocalMemberSource

        if not staging_prefix or staging_prefix != staging_prefix.strip("/"):
            raise ValueError("A remote generation needs an explicit staging prefix")
        self.directory = directory
        self.client, self.bucket, self.prefix = client, bucket, staging_prefix
        self.local = LocalMemberSource(directory)
        self.remote = _S3Members(client, bucket, staging_prefix)
        self.members = {}
        for member in members:
            key = Path(member.key).name
            if (member.key != staging_prefix + "/" + key or not key.endswith(".parquet")
                    or key in self.members or not member.etag):
                raise ValueError("Staged table names or source pins are invalid")
            self.members[key] = member
        if not self.members:
            raise ValueError("A remote generation cannot omit all tables")

    def keys(self):
        """List all local and staged keys; a local/remote collision is refused so undeclared staging fails admission."""
        local = set(self.local.keys())
        remote = set(self.remote.keys())
        if local & remote:
            raise ValueError("Remote staging collides with local artifact metadata")
        # Listing every key makes undeclared staging objects an admission error.
        yield from sorted(local | remote)

    @contextmanager
    def open(self, key):
        """Stream one member: pinned remote bytes for staged tables, else local artifact metadata."""
        from spicy_regs.sources.remote_parquet import open_pinned_s3

        if key not in self.members:
            with self.local.open(key) as stream:
                yield stream
            return
        member = self.members[key]
        with open_pinned_s3(client=self.client, bucket=self.bucket, key=member.key, etag=member.etag) as stream:
            yield stream

    def table_info(self, key):
        """Observed columns and row count for a member; decoding all pages catches a body/footer mismatch."""
        with self.open(key) as stream, pq.ParquetFile(stream) as parquet:
            with duckdb.connect() as con:
                con.register("generation_schema", parquet.schema_arrow.empty_table())
                columns = con.execute("DESCRIBE SELECT * FROM generation_schema").fetchall()
            rows = sum(batch.num_rows for batch in parquet.iter_batches(batch_size=64, use_threads=False))
            if rows != parquet.metadata.num_rows:
                raise ValueError(f"Parquet body/footer row mismatch: {key}")
        return {"columns": [[row[0], row[1]] for row in columns], "rows": rows}


def _verify_remote(source, *, expected_pin=None):
    """Admit the artifact, then require staged receipts to equal independently verified bytes."""
    from rulespec_artifacts import iter_member_descriptors

    artifact = verify_generation_source(source, source.table_info, expected_pin=expected_pin)
    if set(artifact.root["spec"]["tables"]) != set(source.members):
        raise ValueError("Staged receipts differ from the declared generation")
    for descriptor in iter_member_descriptors(artifact, source):
        staged = source.members[descriptor.object_key]
        if (descriptor.sha256 != staged.sha256 or descriptor.byte_size != staged.byte_size
                or descriptor.record_count != staged.rows):
            raise ValueError("Staged receipt differs from independently verified bytes")
    return artifact


def prepare_remote_generation(
    directory: Path, *, family: str, client, bucket: str, staging_prefix: str,
    members: Sequence[StoredParquet], expected_keys: Sequence[str],
    schemas: Mapping[str, list[tuple[str, str]]], read_snapshot: Mapping | None = None,
    carried_forward: Mapping[str, str] | None = None, publication_status: str = "complete-family", inputs=(),
):
    """Create and fully verify the standard artifact using pinned remote tables.

    Writer receipts supply expected sizes/hashes/counts, not proof. Rulespec
    independently hashes every remote byte, and every Parquet page is decoded
    through conditional reads before this function succeeds.
    """
    from rulespec_artifacts import MemberDescriptor

    directory.mkdir(parents=True, exist_ok=False)
    source = _RemoteGenerationSource(directory, client=client, bucket=bucket,
                                     staging_prefix=staging_prefix, members=members)
    if len(set(expected_keys)) != len(expected_keys) or set(expected_keys) != set(source.members):
        raise ValueError("Staged outputs differ from the declared complete family")
    tables = {}
    descriptors = []
    for key, member in sorted(source.members.items()):
        columns = schemas[Path(key).stem]
        tables[key] = {"columns": [list(column) for column in columns], "rows": member.rows}
        descriptors.append(MemberDescriptor(
            object_key=key, role="table", media_type="application/vnd.apache.parquet",
            byte_size=member.byte_size, sha256=member.sha256, record_count=member.rows,
        ))
    _write_generation_metadata(
        directory, family=family, tables=tables, members=descriptors, read_snapshot=read_snapshot,
        carried_forward=carried_forward, publication_status=publication_status, inputs=inputs,
        extra_packages=("pyarrow", "duckdb", "smart-open", "wrapt"),
    )
    return _verify_remote(source)


def verify_remote_generation(directory: Path, *, client, bucket: str, staging_prefix: str,
                             members: Sequence[StoredParquet], expected_pin=None):
    """Re-admit exact staging bytes and decode all columns without local copies."""
    source = _RemoteGenerationSource(directory, client=client, bucket=bucket,
                                     staging_prefix=staging_prefix, members=members)
    return _verify_remote(source, expected_pin=expected_pin)


def _copy_immutable(client, bucket: str, target: str, member: StoredParquet) -> None:
    """Promote pinned bytes with multipart copy; final admission checks SHA-256.

    R2 documents copy-source conditionals as unimplemented for UploadPartCopy,
    so ``CopySourceIfMatch`` guards only on S3; admission is the guard that
    holds on both.
    """
    if not 0 < member.byte_size <= PART_BYTES * 10_000:
        raise PublicationError("Remote member exceeds bounded multipart size")

    def copy_parts(upload_id: str) -> list[dict]:
        parts = []
        for start in range(0, member.byte_size, PART_BYTES):
            result = client.upload_part_copy(
                Bucket=bucket, Key=target, UploadId=upload_id, PartNumber=len(parts) + 1,
                CopySource={"Bucket": bucket, "Key": member.key}, CopySourceIfMatch=member.etag,
                CopySourceRange=f"bytes={start}-{min(start + PART_BYTES, member.byte_size) - 1}",
            )
            parts.append({"PartNumber": len(parts) + 1, "ETag": result["CopyPartResult"]["ETag"]})
        return parts

    _create_multipart(client, bucket, target, copy_parts)


def publish_remote_generation(
    directory: Path, *, client, bucket: str, staging_prefix: str, members: Sequence[StoredParquet],
    prior_index: Mapping, evidence_directories: tuple[Path, ...] = (),
) -> dict:
    """Use the ordinary publication gates, promoting tables without local copies."""
    source = _RemoteGenerationSource(directory, client=client, bucket=bucket,
                                     staging_prefix=staging_prefix, members=members)
    artifact = _verify_remote(source)

    def upload(prefix, key):
        target = prefix + "/" + key
        if key in source.members:
            _copy_immutable(client, bucket, target, source.members[key])
        else:
            _put_immutable(client, bucket, target, directory / key)

    return _publish_verified_generation(
        artifact, source, upload_member=upload, client=client, bucket=bucket, prior_index=prior_index,
        evidence_directories=evidence_directories,
    )
