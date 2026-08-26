"""Source-native-backed public Parquet, DuckDB, and Iceberg behavior."""

from __future__ import annotations

import ast
import json
import threading
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import duckdb
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import (
    ArtifactPin,
    ArtifactVerificationError,
    LocalMemberSource,
    Producer,
)

from spicy_regs.public_table import (
    IcebergPublicTableSink,
    PublicTableArtifactLocation,
    PublicTableBuild,
    PublicTableError,
    PublicTablePublisher,
    PublicTableReader,
    VERIFIER_ID as PUBLIC_VERIFIER_ID,
    VERIFIER_VERSION as PUBLIC_VERIFIER_VERSION,
)
from spicy_regs.public_table_profiles import (
    FEDERAL_REGISTER_PUBLIC_TABLE,
    REGULATIONS_GOV_COMMENT_PUBLIC_TABLE,
    REGULATIONS_GOV_DOCKET_PUBLIC_TABLE,
    REGULATIONS_GOV_DOCUMENT_PUBLIC_TABLE,
    PublicTableProfile,
)
from spicy_regs.publication import ImmutablePublicationError
from spicy_regs.regulations_gov_source_native import (
    COMMENT_COLLECTION,
    iter_regulations_gov_comment_pages,
)
from spicy_regs.schemas.federal_register import FEDERAL_REGISTER_COLUMNS
from spicy_regs.schemas.regulations import COMMENT, DOCKET, DOCUMENT
from spicy_regs.source_native import (
    SourceNativeReleaseBuild,
    SourceNativeReleasePublisher,
    SourceNativeReleaseReader,
)
from spicy_regs.source_native import VERIFIER_ID as SOURCE_VERIFIER_ID
from spicy_regs.source_native import VERIFIER_VERSION as SOURCE_VERIFIER_VERSION
from spicy_regs.source_native_profiles import REGULATIONS_GOV_COMMENT_PROFILE

_IMPLEMENTATION_ID = "git+https://example.test/spicy-regs@" + "a" * 40
_SOURCE_PRODUCER = Producer(
    product="spicy-regs",
    implementation_id=_IMPLEMENTATION_ID,
    verifier_id=SOURCE_VERIFIER_ID,
    verifier_version=SOURCE_VERIFIER_VERSION,
    verifier_implementation_id=_IMPLEMENTATION_ID,
)
_PUBLIC_PRODUCER = Producer(
    product="spicy-regs",
    implementation_id=_IMPLEMENTATION_ID,
    verifier_id=PUBLIC_VERIFIER_ID,
    verifier_version=PUBLIC_VERIFIER_VERSION,
    verifier_implementation_id=_IMPLEMENTATION_ID,
)


def _comment(
    identity: str,
    *,
    docket: str,
    posted: str,
    modified: str | None,
    body: str,
) -> dict[str, Any]:
    return {
        "data": {
            "attributes": {
                "agencyId": "EPA",
                "category": "Public Comment",
                "comment": body,
                "docketId": docket,
                "documentType": "Public Submission",
                "modifyDate": modified,
                "organization": "Example Org",
                "postedDate": posted,
                "receiveDate": posted,
                "title": f"Title {identity}",
            },
            "id": identity,
            "type": COMMENT_COLLECTION,
        },
        "included": [
            {
                "attributes": {
                    "fileFormats": [
                        {
                            "fileUrl": f"https://downloads.regulations.gov/{identity}/attachment.pdf",
                            "format": "pdf",
                            "size": 42,
                        }
                    ],
                    "modifyDate": modified,
                    "title": "Attachment",
                },
                "id": f"{identity}-attachment",
                "type": "attachments",
            }
        ],
    }


@dataclass(frozen=True, slots=True)
class _Object:
    key: str
    etag: str
    version_id: str | None
    content: bytes


class _ObjectReader:
    def __init__(self, objects: list[_Object]) -> None:
        self._objects = objects

    def iter_source_objects(self, *, max_bytes: int) -> Iterator[_Object]:
        assert max_bytes == 16 * 1024 * 1024
        yield from self._objects


def _object(record: Mapping[str, Any], *, observation: str) -> _Object:
    identity = str(record["data"]["id"])  # type: ignore[index]
    return _Object(
        key=(
            f"raw-data/EPA/EPA-2026-0001/text-{observation}/comments/"
            f"{identity}.json"
        ),
        etag=f'"{observation}-etag"',
        version_id=f"{observation}-version",
        content=json.dumps(record, indent=2).encode(),
    )


def _source_release(tmp_path: Path) -> SourceNativeReleaseReader:
    posted = "2026-08-24T04:00:00Z"
    first_old = _comment(
        "EPA-2026-0001-0002",
        docket="EPA-2026-0001",
        posted=posted,
        modified="2026-08-24T05:00:00Z",
        body="old observation",
    )
    first_new = _comment(
        "EPA-2026-0001-0002",
        docket="EPA-2026-0001",
        posted=posted,
        modified="2026-08-25T05:00:00Z",
        body="newest observation",
    )
    second = _comment(
        "EPA-2026-0001-0001",
        docket="EPA-2026-0000",
        posted="2026-08-24T03:00:00Z",
        modified=None,
        body="single null-version observation",
    )
    objects = sorted(
        [
            _object(first_old, observation="1-old"),
            _object(first_new, observation="2-new"),
            _object(second, observation="3-null"),
        ],
        key=lambda item: item.key,
    )
    scope = {
        "agencies": ["EPA"],
        "postedFrom": "2026-08-24",
        "postedThrough": "2026-08-24",
    }
    published = SourceNativeReleasePublisher(REGULATIONS_GOV_COMMENT_PROFILE).publish(
        iter_regulations_gov_comment_pages(
            lambda agency: _ObjectReader(objects) if agency == "EPA" else pytest.fail(agency),
            query_scope=scope,
        ),
        build=SourceNativeReleaseBuild(
            query_scope=scope,
            producer=_SOURCE_PRODUCER,
            started_at="2026-08-25T00:00:00Z",
            completed_at="2026-08-25T00:00:01Z",
        ),
        destination=tmp_path / "source",
    )
    return SourceNativeReleaseReader(
        LocalMemberSource(published.root),
        profile=REGULATIONS_GOV_COMMENT_PROFILE,
        expected_pin=published.artifact.pin,
        accepted_verifier_implementation_ids=frozenset({_IMPLEMENTATION_ID}),
    )


def _public_reader(
    root: Path,
    profile: PublicTableProfile,
    pin: ArtifactPin,
) -> PublicTableReader:
    return PublicTableReader(
        PublicTableArtifactLocation.local(root, expected_pin=pin),
        profile=profile,
        accepted_verifier_implementation_ids=frozenset({_IMPLEMENTATION_ID}),
    )


def test_comment_public_table_preserves_schema_newest_value_and_hive_pruning(
    tmp_path: Path,
) -> None:
    source = _source_release(tmp_path)
    destination = tmp_path / "public"
    published = PublicTablePublisher(REGULATIONS_GOV_COMMENT_PUBLIC_TABLE).publish(
        source,
        build=PublicTableBuild(
            _PUBLIC_PRODUCER,
            max_rows_per_member=1,
            max_rows_per_batch=1,
        ),
        destination=destination,
    )
    reader = _public_reader(
        published.root,
        REGULATIONS_GOV_COMMENT_PUBLIC_TABLE,
        published.artifact.pin,
    )

    assert reader.source_pin == source.pin
    assert reader.columns == tuple(COMMENT.schema)
    assert reader.object_keys == (
        "data/agency_code=EPA/part-000000.parquet",
        "data/agency_code=EPA/part-000001.parquet",
    )
    for key in reader.object_keys:
        parquet = pq.ParquetFile(destination / key)
        assert parquet.schema_arrow.names == list(COMMENT.schema)
        assert parquet.metadata.num_rows == 1

    relation = reader.duckdb_relation(duckdb.connect())
    rows = relation.order("docket_id, posted_date, comment_id").fetchdf()
    assert rows["agency_code"].tolist() == ["EPA", "EPA"]
    assert rows["comment_id"].tolist() == [
        "EPA-2026-0001-0001",
        "EPA-2026-0001-0002",
    ]
    assert rows["comment"].tolist() == [
        "single null-version observation",
        "newest observation",
    ]
    assert "attachment.pdf" in rows["attachments_json"].tolist()[0]


@dataclass(slots=True)
class _SourceStub:
    profile: PublicTableProfile
    rows: list[Mapping[str, Any]]
    pin: ArtifactPin = ArtifactPin(
        "urn:spicy:artifact:spicyregs-source-native-release:" + "b" * 64,
        "sha256:" + "c" * 64,
    )
    source_state_scope: str = "complete-snapshot"
    source_state_digest: str = "sha256:" + "d" * 64

    @property
    def source_system_id(self) -> str:
        return self.profile.source_system_id

    def iter_records(self) -> Iterator[Mapping[str, Any]]:
        yield from self.rows


def _source_row(
    profile: PublicTableProfile,
    identity: str,
    record: Mapping[str, Any],
) -> Mapping[str, Any]:
    return {
        "record": record,
        "schemaName": profile.source_schema_name,
        "sourceRecordId": identity,
    }


@pytest.mark.parametrize(
    ("profile", "record", "expected"),
    [
        (
            FEDERAL_REGISTER_PUBLIC_TABLE,
            {
                "agencies": [
                    {
                        "name": "Environmental Protection Agency",
                        "slug": "environmental-protection-agency",
                    }
                ],
                "document_number": "2026-00001",
                "docket_ids": ["EPA-HQ-OAR-2026-0001"],
                "html_url": "https://www.federalregister.gov/d/2026-00001",
                "publication_date": "2026-08-24",
                "regulation_id_numbers": ["2060-AV12"],
                "title": "Federal Register title",
                "topics": ["Air pollution control"],
                "type": "Rule",
            },
            {
                "primary": "document_number",
                "columns": FEDERAL_REGISTER_COLUMNS,
                "agency_slugs": "environmental-protection-agency",
                "docket_ids_json": '["EPA-HQ-OAR-2026-0001"]',
            },
        ),
        (
            REGULATIONS_GOV_DOCUMENT_PUBLIC_TABLE,
            {
                "data": {
                    "attributes": {
                        "agencyId": "EPA",
                        "docketId": "EPA-2026-0001",
                        "fileFormats": [
                            {
                                "fileUrl": "https://example.test/document.pdf",
                                "format": "pdf",
                                "size": 12,
                            }
                        ],
                        "postedDate": "2026-08-24T00:00:00Z",
                        "withdrawn": False,
                    },
                    "id": "EPA-2026-0001-0001",
                    "type": "documents",
                }
            },
            {
                "primary": "document_id",
                "columns": tuple(DOCUMENT.schema),
                "file_url": "https://example.test/document.pdf",
                "withdrawn": "false",
            },
        ),
        (
            REGULATIONS_GOV_DOCKET_PUBLIC_TABLE,
            {
                "data": {
                    "attributes": {
                        "agencyId": "EPA",
                        "dkAbstract": "Exact docket abstract",
                        "modifyDate": "2026-08-24T00:00:00Z",
                        "title": "Docket title",
                    },
                    "id": "EPA-2026-0001",
                    "type": "dockets",
                }
            },
            {
                "primary": "docket_id",
                "columns": tuple(DOCKET.schema),
                "abstract": "Exact docket abstract",
            },
        ),
    ],
)
def test_source_public_tables_preserve_proven_columns(
    tmp_path: Path,
    profile: PublicTableProfile,
    record: Mapping[str, Any],
    expected: Mapping[str, Any],
) -> None:
    identity = (
        str(record["document_number"])
        if profile is FEDERAL_REGISTER_PUBLIC_TABLE
        else str(record["data"]["id"])  # type: ignore[index]
    )
    source = _SourceStub(profile, [_source_row(profile, identity, record)])
    destination = tmp_path / profile.table_name
    published = PublicTablePublisher(profile).publish(
        source,
        build=PublicTableBuild(_PUBLIC_PRODUCER),
        destination=destination,
    )
    reader = _public_reader(destination, profile, published.artifact.pin)
    row = reader.duckdb_relation(duckdb.connect()).fetchdf().iloc[0]

    assert reader.columns == expected["columns"]
    assert row[expected["primary"]] == identity
    for name, value in expected.items():
        if name not in {"primary", "columns"}:
            assert row[name] == value


def test_public_table_refuses_duplicate_source_identity(tmp_path: Path) -> None:
    profile = REGULATIONS_GOV_DOCKET_PUBLIC_TABLE
    identity = "EPA-2026-0001"
    record = {
        "data": {
            "attributes": {"agencyId": "EPA", "modifyDate": "2026-08-24T00:00:00Z"},
            "id": identity,
            "type": "dockets",
        }
    }
    repeated = _source_row(profile, identity, record)
    source = _SourceStub(profile, [repeated, repeated])

    with pytest.raises(PublicTableError, match="repeats primary key"):
        PublicTablePublisher(profile).publish(
            source,
            build=PublicTableBuild(_PUBLIC_PRODUCER),
            destination=tmp_path / "duplicate",
        )
    assert not (tmp_path / "duplicate").exists()


def test_public_table_refuses_a_row_larger_than_its_batch_bound(tmp_path: Path) -> None:
    profile = REGULATIONS_GOV_DOCKET_PUBLIC_TABLE
    identity = "EPA-2026-0001"
    source = _SourceStub(
        profile,
        [
            _source_row(
                profile,
                identity,
                {
                    "data": {
                        "attributes": {
                            "agencyId": "EPA",
                            "dkAbstract": "larger than the deliberate test bound",
                        },
                        "id": identity,
                        "type": "dockets",
                    }
                },
            )
        ],
    )

    with pytest.raises(PublicTableError, match="batch-byte bound"):
        PublicTablePublisher(profile).publish(
            source,
            build=PublicTableBuild(_PUBLIC_PRODUCER, max_batch_bytes=8),
            destination=tmp_path / "oversize",
        )
    assert not (tmp_path / "oversize").exists()


def test_public_table_is_immutable_and_tamper_fails_before_read(tmp_path: Path) -> None:
    source = _source_release(tmp_path)
    destination = tmp_path / "public"
    publisher = PublicTablePublisher(REGULATIONS_GOV_COMMENT_PUBLIC_TABLE)
    published = publisher.publish(
        source,
        build=PublicTableBuild(_PUBLIC_PRODUCER),
        destination=destination,
    )

    with pytest.raises(ImmutablePublicationError, match="refusing to replace"):
        publisher.publish(
            source,
            build=PublicTableBuild(_PUBLIC_PRODUCER),
            destination=destination,
        )

    member = destination / "data/agency_code=EPA/part-000000.parquet"
    member.write_bytes(member.read_bytes() + b"changed")
    with pytest.raises(ArtifactVerificationError, match="invalid.member-digest"):
        _public_reader(
            destination,
            REGULATIONS_GOV_COMMENT_PUBLIC_TABLE,
            published.artifact.pin,
        )


class _IcebergTable:
    def __init__(self, *, existing: object | None = None) -> None:
        self.snapshot = existing
        self.calls: list[tuple[list[str], bool]] = []

    def current_snapshot(self) -> object | None:
        return self.snapshot

    def add_files(
        self,
        file_paths: list[str],
        *,
        check_duplicate_files: bool = True,
    ) -> None:
        self.calls.append((file_paths, check_duplicate_files))
        self.snapshot = {"snapshot-id": 123}


def test_iceberg_sink_adopts_exact_members_in_one_standard_snapshot(tmp_path: Path) -> None:
    source = _source_release(tmp_path)
    destination = tmp_path / "public"
    published = PublicTablePublisher(REGULATIONS_GOV_COMMENT_PUBLIC_TABLE).publish(
        source,
        build=PublicTableBuild(_PUBLIC_PRODUCER, max_rows_per_member=1),
        destination=destination,
    )
    reader = _public_reader(
        destination,
        REGULATIONS_GOV_COMMENT_PUBLIC_TABLE,
        published.artifact.pin,
    )
    table = _IcebergTable()

    snapshot = IcebergPublicTableSink(table).publish(reader)

    assert snapshot == {"snapshot-id": 123}
    assert table.calls == [
        ([str(destination / key) for key in reader.object_keys], True)
    ]

    with pytest.raises(PublicTableError, match="new empty table"):
        IcebergPublicTableSink(table).publish(reader)


def test_remote_location_refuses_a_different_artifact_address(tmp_path: Path) -> None:
    profile = REGULATIONS_GOV_DOCKET_PUBLIC_TABLE
    identity = "EPA-2026-0001"

    def source(abstract: str) -> _SourceStub:
        return _SourceStub(
            profile,
            [
                _source_row(
                    profile,
                    identity,
                    {
                        "data": {
                            "attributes": {
                                "agencyId": "EPA",
                                "dkAbstract": abstract,
                            },
                            "id": identity,
                            "type": "dockets",
                        }
                    },
                )
            ],
        )

    first = PublicTablePublisher(profile).publish(
        source("first artifact"),
        build=PublicTableBuild(_PUBLIC_PRODUCER),
        destination=tmp_path / "first",
    )
    second = PublicTablePublisher(profile).publish(
        source("different artifact"),
        build=PublicTableBuild(_PUBLIC_PRODUCER),
        destination=tmp_path / "second",
    )
    second_digest = second.artifact.pin.artifact_digest.removeprefix("sha256:")

    with pytest.raises(PublicTableError, match="content-addressed artifact URI"):
        PublicTableArtifactLocation.content_addressed_remote(
            LocalMemberSource(first.root),
            expected_pin=first.artifact.pin,
            duckdb_base_uri=(
                f"https://data.example.test/artifacts/sha256/{second_digest}"
            ),
        )


def test_duckdb_reads_admitted_members_over_anonymous_http_ranges(tmp_path: Path) -> None:
    source = _source_release(tmp_path)
    destination = tmp_path / "public"
    published = PublicTablePublisher(REGULATIONS_GOV_COMMENT_PUBLIC_TABLE).publish(
        source,
        build=PublicTableBuild(_PUBLIC_PRODUCER, max_rows_per_member=1),
        destination=destination,
    )
    ranges: list[str] = []
    digest = published.artifact.pin.artifact_digest.removeprefix("sha256:")
    content_prefix = f"artifacts/sha256/{digest}/"

    class _Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            del format
            del args

        def _path(self) -> Path:
            requested = unquote(self.path).lstrip("/")
            if not requested.startswith(content_prefix):
                raise FileNotFoundError
            selected = (destination / requested.removeprefix(content_prefix)).resolve()
            if not selected.is_relative_to(destination.resolve()):
                raise FileNotFoundError
            return selected

        def do_HEAD(self) -> None:  # noqa: N802 - HTTP handler API
            path = self._path()
            self.send_response(200)
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(path.stat().st_size))
            self.end_headers()

        def do_GET(self) -> None:  # noqa: N802 - HTTP handler API
            payload = self._path().read_bytes()
            start = 0
            end = len(payload) - 1
            requested = self.headers.get("Range")
            if requested:
                ranges.append(requested)
                unit, value = requested.split("=", 1)
                assert unit == "bytes" and "," not in value
                first, last = value.split("-", 1)
                if not first:
                    start = max(0, len(payload) - int(last))
                else:
                    start = int(first)
                    if last:
                        end = min(end, int(last))
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(payload)}")
            else:
                self.send_response(200)
            selected = payload[start : end + 1]
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(selected)))
            self.end_headers()
            self.wfile.write(selected)

    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host = server.server_address[0]
        port = server.server_address[1]
        connection = duckdb.connect()
        connection.execute("LOAD httpfs")
        location = PublicTableArtifactLocation.content_addressed_remote(
            LocalMemberSource(destination),
            expected_pin=published.artifact.pin,
            duckdb_base_uri=(
                f"http://{host}:{port}/artifacts/sha256/{digest}"
            ),
        )
        reader = PublicTableReader(
            location,
            profile=REGULATIONS_GOV_COMMENT_PUBLIC_TABLE,
            accepted_verifier_implementation_ids=frozenset({_IMPLEMENTATION_ID}),
        )
        relation = reader.duckdb_relation(connection)
        rows = relation.filter("docket_id = 'EPA-2026-0001'").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "EPA-2026-0001-0002"
        assert ranges
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def test_public_table_target_has_no_legacy_publication_imports() -> None:
    root = Path(__file__).resolve().parents[1] / "src/spicy_regs"
    forbidden = {
        "spicy_regs.cli",
        "spicy_regs.mcp_server",
        "spicy_regs.pipelines",
        "spicy_regs.published",
        "spicy_regs.sources.iceberg",
        "spicy_regs.sources.r2",
    }
    imported: set[str] = set()
    for name in ("public_table.py", "public_table_profiles.py"):
        tree = ast.parse((root / name).read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
    assert not any(
        actual == denied or actual.startswith(f"{denied}.")
        for actual in imported
        for denied in forbidden
    )

    public_module = ast.parse((root / "public_table.py").read_text(encoding="utf-8"))
    public_arguments = {
        argument.arg
        for node in ast.walk(public_module)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        for argument in (*node.args.args, *node.args.kwonlyargs)
    }
    assert "locate_member" not in public_arguments
