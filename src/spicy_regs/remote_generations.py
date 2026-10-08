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


def _member_map(staging_prefix, members):
    if not staging_prefix or staging_prefix != staging_prefix.strip("/"):
        raise ValueError("A remote generation needs an explicit staging prefix")
    result = {}
    for member in members:
        key = Path(member.key).name
        if (member.key != staging_prefix + "/" + key or not key.endswith(".parquet")
                or key in result or not member.etag):
            raise ValueError("Staged table names or source pins are invalid")
        result[key] = member
    if not result:
        raise ValueError("A remote generation cannot omit all tables")
    return result


class _RemoteGenerationSource:
    """Member source over staged remote Parquet plus local artifact metadata.

    Refuses an unnamed staging prefix, a member key outside it, duplicate names,
    an empty member set, or a member without a source etag.
    """

    def __init__(self, directory: Path, *, client, bucket: str, staging_prefix: str,
                 members: Sequence[StoredParquet]):
        from rulespec_artifacts import LocalMemberSource

        self.directory = directory
        self.client, self.bucket, self.prefix = client, bucket, staging_prefix
        self.local = LocalMemberSource(directory)
        self.remote = _S3Members(client, bucket, staging_prefix)
        self.members = _member_map(staging_prefix, members)
        self.observed_tables = {}

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
        observed = {"columns": [[row[0], row[1]] for row in columns], "rows": rows}
        self.observed_tables[key] = observed
        return observed


class _PublicGenerationSource(_RemoteGenerationSource):
    """Same verifier over declared public members, with no local table downloads.

    Public manifests declare membership, as in the existing public conversion
    reader. Public listing is unavailable; staged membership was checked before
    publication. Every declared body is independently hashed and decoded here.
    """

    def __init__(self, directory, base_url, prefix, members):
        from rulespec_artifacts import LocalMemberSource
        self.local = LocalMemberSource(directory)
        self.base_url = base_url.rstrip("/")
        self.members = _member_map(prefix, members)
        self.observed_tables = {}

    def keys(self):
        local = set(self.local.keys())
        if local & set(self.members):
            raise ValueError("Public members collide with local artifact metadata")
        yield from sorted(local | set(self.members))

    @contextmanager
    def open(self, key):
        from spicy_regs.sources.remote_parquet import open_pinned_http
        if key in self.members:
            member = self.members[key]
            with open_pinned_http(self.base_url + "/" + member.key, etag=member.etag) as stream:
                yield stream
        else:
            with self.local.open(key) as stream:
                yield stream


def verify_public_generation(base_url, index, family, directory, *, bounded_receipts=False, observations=None):
    """Fully admit actual anonymous generation bytes while retaining only metadata locally."""
    from rulespec_artifacts import ArtifactPin, parse_admitted_json, sha256_digest, validate_object_key
    from spicy_regs.sources import publication, r2
    from spicy_regs.sources.remote_parquet import StoredParquet

    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    entry = index["families"][family]
    raw, root = publication.load_family_root(base_url, entry)
    (directory / "artifact.json").write_bytes(raw)
    members = []
    for manifest in root["memberManifests"]:
        key = validate_object_key(manifest["objectKey"], path="manifest.objectKey")
        raw = publication._bounded_get(base_url.rstrip("/") + "/" + entry["prefix"] + "/" + key,
                                       allow_missing=False, limit=publication.EVIDENCE_CONTROL_LIMIT)
        if raw is None or sha256_digest(raw) != manifest["sha256"]:
            raise ValueError("Public generation manifest differs from its pin")
        target = directory / key
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(raw)
        for member in parse_admitted_json(raw)["members"]:
            name = validate_object_key(member["objectKey"], path="member.objectKey")
            remote_key = entry["prefix"] + "/" + name
            version = r2.public_object_version(base_url.rstrip("/") + "/" + remote_key)
            if version is None or version["bytes"] != member["byteSize"]:
                raise ValueError("Public member is missing or differs in size")
            members.append(StoredParquet(remote_key, member["byteSize"], member["sha256"],
                                         version["etag"], member["recordCount"]))
    source = _PublicGenerationSource(directory, base_url, entry["prefix"], members)
    outcomes = {} if observations is not None else None
    artifact = _verify_remote(source, expected_pin=ArtifactPin(entry["logicalId"], entry["artifactDigest"]),
                              bounded_receipts=bounded_receipts, outcomes=outcomes)
    if observations is not None:
        observations.update(tables=source.observed_tables, outcomes=outcomes)
    return artifact


def _verify_remote(source, *, expected_pin=None, bounded_receipts=False, outcomes=None):
    """Admit the artifact, then require staged receipts to equal independently verified bytes."""
    from rulespec_artifacts import iter_member_descriptors

    if outcomes is not None and not bounded_receipts:
        raise ValueError("Outcome evidence requires the bounded complete receipt admission")
    if not bounded_receipts:
        artifact = verify_generation_source(source, source.table_info, expected_pin=expected_pin)
    else:
        from spicy_regs.generations import _verify_generation_source
        from spicy_regs.etl_bulk import _validated_bundle
        def admit_receipts(subjects, receipts, policies, *, generation_id):
            with _validated_bundle(subjects, receipts, policies, generation_id=generation_id,
                                   threads=1, memory_limit="8GB", bounded_insert=True) as con:
                if outcomes is not None:
                    from spicy_regs.etl_receipts import OUTCOMES
                    # Reuse the already admitted receipt table; never reread remote bodies for counts.
                    counts = {name: 0 for name in sorted(OUTCOMES)}
                    counts.update(con.execute("SELECT outcome, count(*) FROM receipts GROUP BY outcome").fetchall())
                    outcomes.update(counts)
        artifact = _verify_generation_source(source, source.table_info, expected_pin=expected_pin,
                                             admit_receipts=admit_receipts)
    expected = set(artifact.root["spec"]["tables"])
    if "etlReceipts" in artifact.root["spec"]:
        expected.add("etl_receipts.parquet")
    if expected != set(source.members):
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
    receipt_policies: Sequence | None = None, receipt_generation_id: str | None = None,
    parents: Mapping[str, Mapping] | None = None,
    bounded_receipts=False,
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
    receipt_spec = None
    if "etl_receipts.parquet" in tables:
        if not receipt_policies or not receipt_generation_id:
            raise ValueError("Remote receipts need policies and a generation identity")
        receipt_spec = {"key": "etl_receipts.parquet", "generationId": receipt_generation_id,
                        "policies": [p.descriptor() for p in receipt_policies],
                        **tables.pop("etl_receipts.parquet")}
    elif receipt_policies is not None or receipt_generation_id is not None:
        raise ValueError("Remote receipt admission cannot omit receipts")
    from spicy_regs.etl_policy_registry import require_registered_receipts
    require_registered_receipts(tables, receipt_spec)
    _write_generation_metadata(
        directory, family=family, tables=tables, members=descriptors, read_snapshot=read_snapshot,
        carried_forward=carried_forward, publication_status=publication_status, inputs=inputs,
        extra_packages=("pyarrow", "duckdb", "smart-open", "wrapt"), etl_receipts=receipt_spec,
        parents=parents,
    )
    return _verify_remote(source, bounded_receipts=bounded_receipts)


def verify_remote_generation(directory: Path, *, client, bucket: str, staging_prefix: str,
                             members: Sequence[StoredParquet], expected_pin=None, bounded_receipts=False):
    """Re-admit exact staging bytes and decode all columns without local copies."""
    source = _RemoteGenerationSource(directory, client=client, bucket=bucket,
                                     staging_prefix=staging_prefix, members=members)
    return _verify_remote(source, expected_pin=expected_pin, bounded_receipts=bounded_receipts)


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
    added_tables: frozenset[str] = frozenset(), receipt_only_tables: frozenset[str] = frozenset(),
    bounded_receipts=False,
    expected_pin=None, before_pointer=None,
) -> dict:
    """Use the ordinary publication gates, promoting tables without local copies."""
    source = _RemoteGenerationSource(directory, client=client, bucket=bucket,
                                     staging_prefix=staging_prefix, members=members)
    artifact = _verify_remote(source, expected_pin=expected_pin, bounded_receipts=bounded_receipts)

    def upload(prefix, key):
        target = prefix + "/" + key
        if key in source.members:
            _copy_immutable(client, bucket, target, source.members[key])
        else:
            _put_immutable(client, bucket, target, directory / key)

    return _publish_verified_generation(
        artifact, source, upload_member=upload, client=client, bucket=bucket, prior_index=prior_index,
        evidence_directories=evidence_directories, added_tables=added_tables, receipt_only_tables=receipt_only_tables,
        before_pointer=before_pointer,
    )
