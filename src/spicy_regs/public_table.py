"""Immutable public Parquet views derived from admitted source-native releases.

Rulespec owns the artifact root, member manifests, digests, and admission.
SpicyRegs owns only the faithful flat source view.  DuckDB reads the declared
Parquet members directly, and an injected PyIceberg table may adopt the same
files as one standard Iceberg snapshot.
"""

from __future__ import annotations

import re
import sqlite3
import tempfile
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable
from urllib.parse import quote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
from rulespec_artifacts import (
    ROOT_OBJECT_KEY,
    ArtifactInput,
    ArtifactPin,
    LocalMemberSource,
    MemberDescriptor,
    MemberManifestReference,
    MemberSource,
    Producer,
    Supersedes,
    VerifiedArtifact,
    admit_artifact,
    build_artifact_root,
    canonical_json_bytes,
    describe_member,
    iter_member_descriptors,
    parse_canonical_json,
)

from spicy_regs.public_table_profiles import PublicTableProfile
from spicy_regs.publication import (
    ImmutablePublicationError,
    publish_directory_once,
    write_bytes_once,
)

KIND = "spicyregs-public-table"
FORMAT_VERSION = "1.0"
VERIFIER_ID = "urn:spicy-regs:public-table-verifier"
VERIFIER_VERSION = "1.0"
INPUT_ROLE = "source-native-release"
MEMBER_ROLE = "public-table-data"
MANIFEST_KEY = "manifests/public-table.json"
PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"
PARQUET_FORMAT_VERSION = "2.6"
PARQUET_COMPRESSION = "zstd"
DEFAULT_MAX_ROWS_PER_MEMBER = 100_000
DEFAULT_MAX_ROWS_PER_BATCH = 2_000
DEFAULT_MAX_BATCH_BYTES = 16 * 1024 * 1024

_SPEC_FIELDS = frozenset(
    {
        "columns",
        "maxRowsPerMember",
        "parquetCompression",
        "parquetFormatVersion",
        "partitionColumns",
        "primaryKey",
        "projectionId",
        "projectionVersion",
        "schemaId",
        "sortColumns",
        "sourceStateDigest",
        "sourceStateScope",
        "sourceSystemId",
        "tableName",
    }
)
_PARTITION_VALUE = re.compile(r"^[A-Za-z0-9._-]+$")
_PART_FILE = re.compile(r"^part-([0-9]{6})\.parquet$")


class PublicTableError(ValueError):
    """A public table cannot be built, admitted, or published safely."""


@runtime_checkable
class SourceNativeTableInput(Protocol):
    """The admitted source-native facts needed by the public projection."""

    @property
    def pin(self) -> ArtifactPin: ...

    source_state_scope: str
    source_system_id: str
    source_state_digest: str

    def iter_records(self) -> Iterator[Mapping[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class PublicTableBuild:
    """Product identity and bounded physical choices for one generation."""

    producer: Producer
    max_rows_per_member: int = DEFAULT_MAX_ROWS_PER_MEMBER
    max_rows_per_batch: int = DEFAULT_MAX_ROWS_PER_BATCH
    max_batch_bytes: int = DEFAULT_MAX_BATCH_BYTES
    supersedes: Supersedes | None = None

    def __post_init__(self) -> None:
        if self.producer.product != "spicy-regs":
            raise PublicTableError("public-table producer product must be spicy-regs")
        if (
            self.producer.verifier_id != VERIFIER_ID
            or self.producer.verifier_version != VERIFIER_VERSION
        ):
            raise PublicTableError("public-table producer names an unsupported verifier")
        if min(
            self.max_rows_per_member,
            self.max_rows_per_batch,
            self.max_batch_bytes,
        ) < 1:
            raise PublicTableError("public-table physical bounds must be positive")


@dataclass(frozen=True, slots=True)
class PublishedPublicTable:
    root: Path
    artifact: VerifiedArtifact


class PublicTableArtifactLocation:
    """Bind admission and member reads to one exact artifact location.

    A local location admits and reads the same directory.  A remote location
    admits an exact distribution through its injected ``MemberSource`` and
    resolves data below bases whose final path is the admitted Rulespec
    artifact digest, written as ``sha256/<hex>``.
    """

    def __init__(
        self,
        *,
        source: MemberSource,
        expected_pin: ArtifactPin,
        local_root: Path | None = None,
        duckdb_base_uri: str | None = None,
        iceberg_base_uri: str | None = None,
    ) -> None:
        local = Path(local_root).absolute() if local_root is not None else None
        if (local is None) == (duckdb_base_uri is None):
            raise PublicTableError(
                "public-table location must be exactly one local or remote artifact"
            )
        self.source = source
        self.expected_pin = expected_pin
        self._local_root = local
        self._duckdb_base_uri = (
            _content_addressed_base(
                duckdb_base_uri,
                expected_pin=expected_pin,
                purpose="DuckDB",
                allowed_schemes=frozenset({"https"}),
                allow_loopback_http=True,
            )
            if duckdb_base_uri is not None
            else None
        )
        self._iceberg_base_uri = (
            _content_addressed_base(
                iceberg_base_uri,
                expected_pin=expected_pin,
                purpose="Iceberg",
                allowed_schemes=frozenset({"s3", "gs", "abfs", "abfss", "https"}),
                allow_loopback_http=False,
            )
            if iceberg_base_uri is not None
            else None
        )

    @classmethod
    def local(
        cls,
        root: Path,
        *,
        expected_pin: ArtifactPin,
    ) -> PublicTableArtifactLocation:
        selected = Path(root).absolute()
        return cls(
            source=LocalMemberSource(selected),
            expected_pin=expected_pin,
            local_root=selected,
        )

    @classmethod
    def content_addressed_remote(
        cls,
        source: MemberSource,
        *,
        expected_pin: ArtifactPin,
        duckdb_base_uri: str,
        iceberg_base_uri: str | None = None,
    ) -> PublicTableArtifactLocation:
        return cls(
            source=source,
            expected_pin=expected_pin,
            duckdb_base_uri=duckdb_base_uri,
            iceberg_base_uri=iceberg_base_uri,
        )

    def duckdb_member(self, object_key: str) -> str:
        if self._local_root is not None:
            return str(self._local_member(object_key))
        assert self._duckdb_base_uri is not None
        return f"{self._duckdb_base_uri}/{quote(object_key, safe='/=._-')}"

    def iceberg_member(self, object_key: str) -> str:
        if self._local_root is not None:
            return str(self._local_member(object_key))
        if self._iceberg_base_uri is None:
            raise PublicTableError(
                "remote public-table location has no content-addressed Iceberg base"
            )
        return f"{self._iceberg_base_uri}/{quote(object_key, safe='/=._-')}"

    def _local_member(self, object_key: str) -> Path:
        assert self._local_root is not None
        path = (self._local_root / object_key).absolute()
        if not path.is_relative_to(self._local_root):
            raise PublicTableError("public-table member escapes its local artifact")
        return path


def _content_addressed_base(
    value: str,
    *,
    expected_pin: ArtifactPin,
    purpose: str,
    allowed_schemes: frozenset[str],
    allow_loopback_http: bool,
) -> str:
    selected = value.rstrip("/")
    parsed = urlparse(selected)
    scheme_allowed = parsed.scheme in allowed_schemes
    if (
        parsed.scheme == "http"
        and allow_loopback_http
        and parsed.hostname in {"127.0.0.1", "::1", "localhost"}
    ):
        scheme_allowed = True
    digest = expected_pin.artifact_digest
    digest_hex = digest.removeprefix("sha256:")
    path_parts = parsed.path.rstrip("/").split("/")
    if (
        not scheme_allowed
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or re.fullmatch(r"[0-9a-f]{64}", digest_hex) is None
        or path_parts[-2:] != ["sha256", digest_hex]
    ):
        raise PublicTableError(
            f"{purpose} base must be a clean content-addressed artifact URI"
        )
    return selected


def _arrow_schema(profile: PublicTableProfile) -> pa.Schema:
    return pa.schema((name, pa.string()) for name in profile.columns)


def _spec(
    source: SourceNativeTableInput,
    *,
    profile: PublicTableProfile,
    max_rows_per_member: int,
) -> dict[str, Any]:
    return {
        "columns": list(profile.columns),
        "maxRowsPerMember": max_rows_per_member,
        "parquetCompression": PARQUET_COMPRESSION,
        "parquetFormatVersion": PARQUET_FORMAT_VERSION,
        "partitionColumns": list(profile.partition_columns),
        "primaryKey": profile.primary_key,
        "projectionId": profile.projection_id,
        "projectionVersion": profile.projection_version,
        "schemaId": profile.schema_id,
        "sortColumns": list(profile.sort_columns),
        "sourceStateDigest": source.source_state_digest,
        "sourceStateScope": source.source_state_scope,
        "sourceSystemId": source.source_system_id,
        "tableName": profile.table_name,
    }


def _validate_profile(profile: PublicTableProfile) -> None:
    columns = profile.columns
    if not columns or len(columns) != len(set(columns)):
        raise PublicTableError("public-table columns must be nonempty and distinct")
    if profile.primary_key not in columns:
        raise PublicTableError("public-table primary key is absent from its columns")
    if any(name not in columns for name in (*profile.partition_columns, *profile.sort_columns)):
        raise PublicTableError("public-table partition or sort column is absent")
    if len(profile.partition_columns) != len(set(profile.partition_columns)):
        raise PublicTableError("public-table partition columns repeat")
    if not profile.sort_columns or profile.primary_key not in profile.sort_columns:
        raise PublicTableError("public-table total order must include its primary key")


def _member_position(
    object_key: str,
    *,
    profile: PublicTableProfile,
) -> tuple[tuple[str, ...], int]:
    parts = object_key.split("/")
    expected_length = 2 + len(profile.partition_columns)
    empty_partition_member = bool(profile.partition_columns) and len(parts) == 2
    if (
        parts[0] != "data"
        or (len(parts) != expected_length and not empty_partition_member)
    ):
        raise PublicTableError(f"invalid public-table member key: {object_key}")
    values: list[str] = []
    partition_components = () if empty_partition_member else parts[1:-1]
    for column, component in zip(
        profile.partition_columns if not empty_partition_member else (),
        partition_components,
        strict=True,
    ):
        prefix = f"{column}="
        value = component.removeprefix(prefix)
        if (
            not component.startswith(prefix)
            or not value
            or _PARTITION_VALUE.fullmatch(value) is None
        ):
            raise PublicTableError(f"invalid public-table partition key: {object_key}")
        values.append(value)
    match = _PART_FILE.fullmatch(parts[-1])
    if match is None:
        raise PublicTableError(f"invalid public-table member filename: {object_key}")
    return tuple(values), int(match.group(1))


def _member_key(
    partition: tuple[str, ...],
    part: int,
    *,
    profile: PublicTableProfile,
) -> str:
    if len(partition) not in {0, len(profile.partition_columns)}:
        raise PublicTableError("public-table partition arity differs")
    if not partition and profile.partition_columns:
        # A valid empty release still needs one self-describing Parquet member.
        # No row can contradict the absent Hive value.
        return f"data/part-{part:06d}.parquet"
    components = ["data"]
    for column, value in zip(profile.partition_columns, partition, strict=True):
        if _PARTITION_VALUE.fullmatch(value) is None:
            raise PublicTableError(
                f"public-table partition value for {column} is not portable: {value!r}"
            )
        components.append(f"{column}={value}")
    components.append(f"part-{part:06d}.parquet")
    return "/".join(components)


def _create_row_index(path: Path, profile: PublicTableProfile) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    partition_fields = ", ".join(
        f"partition_{index} TEXT NOT NULL"
        for index, _ in enumerate(profile.partition_columns)
    )
    sort_fields = ", ".join(
        f"sort_{index} TEXT" for index, _ in enumerate(profile.sort_columns)
    )
    fields = ["row_id TEXT PRIMARY KEY"]
    if partition_fields:
        fields.append(partition_fields)
    if sort_fields:
        fields.append(sort_fields)
    fields.append("payload BLOB NOT NULL")
    connection.execute(f"CREATE TABLE rows ({', '.join(fields)})")
    return connection


def _index_rows(
    connection: sqlite3.Connection,
    source: SourceNativeTableInput,
    *,
    profile: PublicTableProfile,
) -> int:
    columns = [
        "row_id",
        *(f"partition_{index}" for index, _ in enumerate(profile.partition_columns)),
        *(f"sort_{index}" for index, _ in enumerate(profile.sort_columns)),
        "payload",
    ]
    placeholders = ", ".join("?" for _ in columns)
    query = f"INSERT INTO rows ({', '.join(columns)}) VALUES ({placeholders})"
    count = 0
    for source_row in source.iter_records():
        row = profile.project(source_row)
        identity = row[profile.primary_key]
        assert identity is not None
        partition = tuple(row[name] for name in profile.partition_columns)
        if any(value is None for value in partition):
            raise PublicTableError("public-table partition value is null")
        sort = tuple(row[name] for name in profile.sort_columns)
        payload = canonical_json_bytes(row)
        try:
            connection.execute(
                query,
                (
                    identity,
                    *partition,
                    *sort,
                    payload,
                ),
            )
        except sqlite3.IntegrityError as error:
            raise PublicTableError(
                f"public table repeats primary key {identity!r}"
            ) from error
        count += 1
        if count % 10_000 == 0:
            connection.commit()
    connection.commit()
    return count


def _partitions(
    connection: sqlite3.Connection,
    *,
    profile: PublicTableProfile,
) -> Iterator[tuple[str, ...]]:
    if not profile.partition_columns:
        yield ()
        return
    columns = ", ".join(
        f"partition_{index}" for index, _ in enumerate(profile.partition_columns)
    )
    for row in connection.execute(
        f"SELECT DISTINCT {columns} FROM rows ORDER BY {columns}"
    ):
        yield tuple(str(value) for value in row)


def _partition_rows(
    connection: sqlite3.Connection,
    partition: tuple[str, ...],
    *,
    profile: PublicTableProfile,
) -> sqlite3.Cursor:
    where = " AND ".join(
        f"partition_{index} = ?" for index, _ in enumerate(partition)
    ) or "1 = 1"
    order_terms: list[str] = []
    for index, _ in enumerate(profile.sort_columns):
        order_terms.extend((f"sort_{index} IS NULL", f"sort_{index}"))
    return connection.execute(
        f"SELECT payload FROM rows WHERE {where} ORDER BY {', '.join(order_terms)}",
        partition,
    )


def _decode_row(payload: bytes) -> dict[str, str | None]:
    value = parse_canonical_json(payload, path="public-table-row")
    if not isinstance(value, Mapping):
        raise PublicTableError("indexed public-table row is not an object")
    return {str(name): item for name, item in value.items()}  # type: ignore[misc]


def _write_empty_member(staging: Path, *, profile: PublicTableProfile) -> tuple[str, int]:
    key = _member_key((), 0, profile=profile)
    path = staging / key
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        _arrow_schema(profile).empty_table(),
        path,
        compression=PARQUET_COMPRESSION,
        version=PARQUET_FORMAT_VERSION,
    )
    return key, 0


def _write_partition(
    staging: Path,
    connection: sqlite3.Connection,
    partition: tuple[str, ...],
    *,
    profile: PublicTableProfile,
    build: PublicTableBuild,
) -> list[tuple[str, int]]:
    cursor = _partition_rows(connection, partition, profile=profile)
    schema = _arrow_schema(profile)
    completed: list[tuple[str, int]] = []
    writer: pq.ParquetWriter | None = None
    key = ""
    part = 0
    member_rows = 0
    batch: list[dict[str, str | None]] = []
    batch_bytes = 0

    def open_writer() -> None:
        nonlocal writer, key
        key = _member_key(partition, part, profile=profile)
        path = staging / key
        path.parent.mkdir(parents=True, exist_ok=True)
        writer = pq.ParquetWriter(
            path,
            schema,
            compression=PARQUET_COMPRESSION,
            version=PARQUET_FORMAT_VERSION,
        )

    def flush_batch() -> None:
        nonlocal batch, batch_bytes
        if not batch:
            return
        assert writer is not None
        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
        batch = []
        batch_bytes = 0

    def close_writer() -> None:
        nonlocal writer, member_rows
        if writer is None:
            return
        flush_batch()
        writer.close()
        completed.append((key, member_rows))
        writer = None
        member_rows = 0

    for (raw_payload,) in cursor:
        payload = bytes(raw_payload)
        if len(payload) > build.max_batch_bytes:
            raise PublicTableError(
                "one public-table row exceeds the configured batch-byte bound"
            )
        if writer is None:
            open_writer()
        if member_rows == build.max_rows_per_member:
            close_writer()
            part += 1
            open_writer()
        if batch and (
            len(batch) == build.max_rows_per_batch
            or batch_bytes + len(payload) > build.max_batch_bytes
        ):
            flush_batch()
        batch.append(_decode_row(payload))
        batch_bytes += len(payload)
        member_rows += 1
    close_writer()
    return completed


def _write_members(
    staging: Path,
    connection: sqlite3.Connection,
    *,
    profile: PublicTableProfile,
    build: PublicTableBuild,
) -> list[tuple[str, int]]:
    members: list[tuple[str, int]] = []
    for partition in _partitions(connection, profile=profile):
        members.extend(
            _write_partition(
                staging,
                connection,
                partition,
                profile=profile,
                build=build,
            )
        )
    if not members:
        members.append(_write_empty_member(staging, profile=profile))
    return members


def _validate_root(
    artifact: VerifiedArtifact,
    source: MemberSource,
    *,
    profile: PublicTableProfile,
) -> tuple[Mapping[str, Any], tuple[MemberDescriptor, ...]]:
    root = artifact.root
    if root.get("kind") != KIND:
        raise PublicTableError("artifact is not a SpicyRegs public table")
    producer = root.get("producer")
    if not isinstance(producer, Mapping) or (
        producer.get("product") != "spicy-regs"
        or producer.get("verifierId") != VERIFIER_ID
        or producer.get("verifierVersion") != VERIFIER_VERSION
    ):
        raise PublicTableError("public-table producer identity differs")
    inputs = root.get("inputs")
    if (
        not isinstance(inputs, list)
        or len(inputs) != 1
        or not isinstance(inputs[0], Mapping)
        or inputs[0].get("role") != INPUT_ROLE
    ):
        raise PublicTableError("public table must pin exactly one source-native release")
    spec = root.get("spec")
    if not isinstance(spec, Mapping) or set(spec) != _SPEC_FIELDS:
        raise PublicTableError("public-table specification fields differ")
    expected_profile_values = {
        "columns": list(profile.columns),
        "parquetCompression": PARQUET_COMPRESSION,
        "parquetFormatVersion": PARQUET_FORMAT_VERSION,
        "partitionColumns": list(profile.partition_columns),
        "primaryKey": profile.primary_key,
        "projectionId": profile.projection_id,
        "projectionVersion": profile.projection_version,
        "schemaId": profile.schema_id,
        "sortColumns": list(profile.sort_columns),
        "sourceSystemId": profile.source_system_id,
        "tableName": profile.table_name,
    }
    if any(spec.get(name) != value for name, value in expected_profile_values.items()):
        raise PublicTableError("public-table specification differs from its injected profile")
    row_limit = spec.get("maxRowsPerMember")
    if isinstance(row_limit, bool) or not isinstance(row_limit, int) or row_limit < 1:
        raise PublicTableError("public-table member row limit is invalid")
    if spec.get("sourceStateScope") not in {"complete-snapshot", "observed-crawl"}:
        raise PublicTableError("public-table source-state scope is invalid")
    state_digest = spec.get("sourceStateDigest")
    if (
        not isinstance(state_digest, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", state_digest) is None
    ):
        raise PublicTableError("public-table source-state digest is invalid")
    manifests = root.get("memberManifests")
    if (
        not isinstance(manifests, list)
        or len(manifests) != 1
        or not isinstance(manifests[0], Mapping)
        or manifests[0].get("scopeKind") != "global"
        or manifests[0].get("scopeId") != profile.table_name
        or manifests[0].get("objectKey") != MANIFEST_KEY
    ):
        raise PublicTableError("public table must carry one global member manifest")
    members = tuple(iter_member_descriptors(artifact, source))
    if not members:
        raise PublicTableError("public table has no Parquet member")
    positions: list[tuple[tuple[str, ...], int]] = []
    total_rows = 0
    for member in members:
        if (
            member.object_key is None
            or member.role != MEMBER_ROLE
            or member.media_type != PARQUET_MEDIA_TYPE
            or member.schema_id != profile.schema_id
            or member.record_count is None
            or member.record_count > row_limit
        ):
            raise PublicTableError("public-table member descriptor differs")
        position = _member_position(member.object_key, profile=profile)
        if profile.partition_columns and not position[0] and member.record_count != 0:
            raise PublicTableError(
                "partitioned public table has rows outside a Hive partition"
            )
        positions.append(position)
        total_rows += member.record_count
    if positions != sorted(positions) or len(positions) != len(set(positions)):
        raise PublicTableError("public-table members are not sorted and distinct")
    by_partition: dict[tuple[str, ...], list[int]] = {}
    for partition, part in positions:
        by_partition.setdefault(partition, []).append(part)
    if any(parts != list(range(len(parts))) for parts in by_partition.values()):
        raise PublicTableError("public-table member sequence is incomplete")
    if total_rows == 0:
        if len(members) != 1 or members[0].record_count != 0:
            raise PublicTableError("empty public table must carry one empty member")
    elif any(member.record_count == 0 for member in members):
        raise PublicTableError("nonempty public table carries an empty member")
    return spec, members


def verify_public_table_admission(
    artifact: VerifiedArtifact,
    source: MemberSource,
    *,
    profile: PublicTableProfile,
) -> None:
    """Check the cheap product shape after Rulespec structural admission."""

    _validate_profile(profile)
    _validate_root(artifact, source, profile=profile)


def _sort_value(row: Mapping[str, Any], columns: Sequence[str]) -> tuple[tuple[int, str], ...]:
    return tuple((1, "") if row[name] is None else (0, str(row[name])) for name in columns)


def verify_public_table_release(
    artifact: VerifiedArtifact,
    source: MemberSource,
    *,
    profile: PublicTableProfile,
) -> None:
    """Run the bounded producer gate over every public Parquet row."""

    spec, members = _validate_root(artifact, source, profile=profile)
    expected_schema = _arrow_schema(profile)
    seen_by_partition: dict[tuple[str, ...], tuple[tuple[int, str], ...]] = {}
    with tempfile.TemporaryDirectory(prefix="public-table-verify-") as directory:
        identities = sqlite3.connect(Path(directory) / "identities.sqlite3")
        identities.execute("CREATE TABLE ids (value TEXT PRIMARY KEY)")
        try:
            for member in members:
                assert member.object_key is not None and member.record_count is not None
                partition, _ = _member_position(member.object_key, profile=profile)
                with source.open(member.object_key) as stream:
                    parquet = pq.ParquetFile(stream)
                    if parquet.schema_arrow != expected_schema:
                        raise PublicTableError("public-table Parquet schema differs")
                    if parquet.metadata.num_rows != member.record_count:
                        raise PublicTableError("public-table Parquet row count differs")
                    previous = seen_by_partition.get(partition)
                    for batch in parquet.iter_batches(batch_size=2_000):
                        for row in pa.Table.from_batches([batch]).to_pylist():
                            identity = row[profile.primary_key]
                            if not isinstance(identity, str) or not identity:
                                raise PublicTableError("public-table primary key is empty")
                            try:
                                identities.execute("INSERT INTO ids VALUES (?)", (identity,))
                            except sqlite3.IntegrityError as error:
                                raise PublicTableError(
                                    f"public table repeats primary key {identity!r}"
                                ) from error
                            actual_partition = tuple(str(row[name]) for name in profile.partition_columns)
                            if actual_partition != partition:
                                raise PublicTableError(
                                    "public-table row differs from its Hive partition"
                                )
                            order = _sort_value(row, profile.sort_columns)
                            if previous is not None and order <= previous:
                                raise PublicTableError(
                                    "public-table rows are not in their declared total order"
                                )
                            previous = order
                    if previous is not None:
                        seen_by_partition[partition] = previous
            identities.commit()
            observed = int(identities.execute("SELECT count(*) FROM ids").fetchone()[0])
            if observed != artifact.root["counts"]["totalRecordCount"]:
                raise PublicTableError("public-table root row accounting differs")
            if any(
                member.record_count is not None
                and member.record_count > spec["maxRowsPerMember"]
                for member in members
            ):
                raise PublicTableError("public-table member exceeds its row bound")
        finally:
            identities.close()


class PublicTablePublisher:
    """Project one admitted source release into one immutable public table."""

    def __init__(self, profile: PublicTableProfile) -> None:
        _validate_profile(profile)
        self._profile = profile

    def publish(
        self,
        source: SourceNativeTableInput,
        *,
        build: PublicTableBuild,
        destination: Path,
    ) -> PublishedPublicTable:
        destination = Path(destination).absolute()
        if destination.exists() or destination.is_symlink():
            raise ImmutablePublicationError(
                f"refusing to replace immutable directory: {destination}"
            )
        if source.source_system_id != self._profile.source_system_id:
            raise PublicTableError("source-native release does not match the public-table profile")
        destination.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(
            prefix=f".{destination.name}.build-",
            dir=destination.parent,
        ) as directory:
            workspace = Path(directory)
            staging = workspace / "generation"
            staging.mkdir()
            connection = _create_row_index(workspace / "rows.sqlite3", self._profile)
            try:
                indexed = _index_rows(connection, source, profile=self._profile)
                member_rows = _write_members(
                    staging,
                    connection,
                    profile=self._profile,
                    build=build,
                )
            finally:
                connection.close()
            if sum(count for _, count in member_rows) != indexed:
                raise PublicTableError("public-table member accounting differs")
            local = LocalMemberSource(staging)
            members = [
                describe_member(
                    local,
                    object_key=key,
                    role=MEMBER_ROLE,
                    media_type=PARQUET_MEDIA_TYPE,
                    record_count=count,
                    schema_id=self._profile.schema_id,
                )
                for key, count in member_rows
            ]
            manifest, manifest_bytes = MemberManifestReference.for_members(
                scope_kind="global",
                scope_id=self._profile.table_name,
                object_key=MANIFEST_KEY,
                members=members,
            )
            write_bytes_once(staging / MANIFEST_KEY, manifest_bytes)
            root = build_artifact_root(
                kind=KIND,
                spec=_spec(
                    source,
                    profile=self._profile,
                    max_rows_per_member=build.max_rows_per_member,
                ),
                producer=build.producer,
                inputs=(
                    ArtifactInput(
                        role=INPUT_ROLE,
                        logical_id=source.pin.logical_id,
                        artifact_digest=source.pin.artifact_digest,
                    ),
                ),
                manifests=(manifest,),
                supersedes=build.supersedes,
            )
            write_bytes_once(staging / ROOT_OBJECT_KEY, canonical_json_bytes(root))
            artifact = admit_artifact(
                LocalMemberSource(staging),
                semantic_verifier=lambda artifact, source: verify_public_table_release(
                    artifact,
                    source,
                    profile=self._profile,
                ),
                scratch_directory=workspace / "verify",
            )
            publish_directory_once(staging, destination)
        return PublishedPublicTable(destination, artifact)


class PublicTableReader:
    """Admit one public table and expose its exact Parquet members."""

    def __init__(
        self,
        location: PublicTableArtifactLocation,
        *,
        profile: PublicTableProfile,
        accepted_verifier_implementation_ids: frozenset[str],
    ) -> None:
        if not accepted_verifier_implementation_ids:
            raise PublicTableError("at least one public-table verifier must be accepted")
        self._profile = profile
        self._location = location
        source = location.source
        self._artifact = admit_artifact(
            source,
            expected_pin=location.expected_pin,
            semantic_verifier=lambda artifact, source: verify_public_table_admission(
                artifact,
                source,
                profile=profile,
            ),
        )
        producer = self._artifact.root["producer"]
        if producer["verifierImplementationId"] not in accepted_verifier_implementation_ids:
            raise PublicTableError("public-table verifier implementation is not accepted")
        spec, members = _validate_root(self._artifact, source, profile=profile)
        self._members = tuple(
            sorted(
                members,
                key=lambda member: _member_position(
                    str(member.object_key), profile=profile
                ),
            )
        )
        source_input = self._artifact.root["inputs"][0]
        self.source_pin = ArtifactPin(
            logical_id=str(source_input["logicalId"]),
            artifact_digest=str(source_input["artifactDigest"]),
        )
        self.source_state_digest = str(spec["sourceStateDigest"])
        self.source_state_scope = str(spec["sourceStateScope"])

    @property
    def pin(self) -> ArtifactPin:
        return self._artifact.pin

    @property
    def table_name(self) -> str:
        return self._profile.table_name

    @property
    def columns(self) -> tuple[str, ...]:
        return self._profile.columns

    @property
    def object_keys(self) -> tuple[str, ...]:
        return tuple(str(member.object_key) for member in self._members)

    @property
    def duckdb_member_locations(self) -> tuple[str, ...]:
        return tuple(
            self._location.duckdb_member(object_key) for object_key in self.object_keys
        )

    @property
    def iceberg_member_locations(self) -> tuple[str, ...]:
        return tuple(
            self._location.iceberg_member(object_key) for object_key in self.object_keys
        )

    def duckdb_relation(
        self,
        connection: Any,
    ) -> Any:
        """Return DuckDB's native Parquet relation over exact admitted members."""

        locations = list(self.duckdb_member_locations)
        if len(locations) != len(set(locations)) or any(not value for value in locations):
            raise PublicTableError("public-table member locations are empty or repeated")
        return connection.from_parquet(
            locations,
            hive_partitioning=bool(self._profile.partition_columns),
            union_by_name=False,
        )

@runtime_checkable
class IcebergTable(Protocol):
    """The two standard PyIceberg table operations used by the sink."""

    def current_snapshot(self) -> object | None: ...

    def add_files(
        self,
        file_paths: list[str],
        *,
        check_duplicate_files: bool = True,
    ) -> None: ...


class IcebergPublicTableSink:
    """Adopt exact Parquet members as the first snapshot of an injected table."""

    def __init__(self, table: IcebergTable) -> None:
        self._table = table

    def publish(
        self,
        public_table: PublicTableReader,
    ) -> object:
        if self._table.current_snapshot() is not None:
            raise PublicTableError(
                "Iceberg target must be a new empty table for this immutable generation"
            )
        locations = list(public_table.iceberg_member_locations)
        if len(locations) != len(set(locations)) or any(not value for value in locations):
            raise PublicTableError("Iceberg member locations are empty or repeated")
        self._table.add_files(locations, check_duplicate_files=True)
        snapshot = self._table.current_snapshot()
        if snapshot is None:
            raise PublicTableError("Iceberg did not commit a snapshot")
        return snapshot


__all__ = [
    "FORMAT_VERSION",
    "IcebergPublicTableSink",
    "IcebergTable",
    "KIND",
    "PublicTableArtifactLocation",
    "PublicTableBuild",
    "PublicTableError",
    "PublicTablePublisher",
    "PublicTableReader",
    "PublishedPublicTable",
    "SourceNativeTableInput",
    "VERIFIER_ID",
    "VERIFIER_VERSION",
    "verify_public_table_admission",
    "verify_public_table_release",
]
