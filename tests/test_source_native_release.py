"""Focused Federal Register source-native publication and reader checks."""

from __future__ import annotations

import hashlib
import json
import tomllib
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest
from rulespec_artifacts import (
    ArtifactVerificationError,
    LocalMemberSource,
    MemberSourceError,
    Producer,
    admit_artifact,
)

from spicy_regs.publication import ImmutablePublicationError
from spicy_regs.federal_register_source_native import (
    DOCUMENT_FIELDS,
    FederalRegisterPage,
    FederalRegisterSourceError,
    federal_register_documents_url,
    iter_federal_register_pages,
    parse_page_response,
)
from spicy_regs.source_native import (
    SourceNativeReleaseBuild,
    SourceNativeReleaseError,
    SourceNativeReleasePublisher,
    SourceNativeReleaseReader,
    installed_release_schema_bundle,
    release_schema_bundle,
)
from spicy_regs.source_native_store import LocalSourceNativeBlobStore
from spicy_regs.source_native_profiles import FEDERAL_REGISTER_PROFILE

IMPLEMENTATION_ID = "git+https://example.test/spicy-regs@" + "a" * 40
PRODUCER = Producer(
    product="spicy-regs",
    implementation_id=IMPLEMENTATION_ID,
    verifier_id="urn:spicy-regs:source-native-release-verifier",
    verifier_version="1.0",
    verifier_implementation_id=IMPLEMENTATION_ID,
)
QUERY_SCOPE = {"publishedFrom": "2026-08-25", "publishedThrough": "2026-08-25"}


def _completed_at() -> datetime:
    return datetime(2026, 8, 25, 0, 0, 1, tzinfo=UTC)


class _PassAcquisitionCheck:
    def add_window(
        self,
        response: Mapping[str, Any],
        *,
        page_window: object | None,
        records_included: bool,
        response_bytes: bytes,
    ) -> None:
        del response, page_window, records_included, response_bytes

    def finish(self, *, query_scope: Mapping[str, Any]) -> None:
        del query_scope


def _accept_all_records(
    response: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> bool:
    del response, query_scope, page_window
    return True


def _accept_record_scope(
    record: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> None:
    del record, query_scope, page_window


def _request_window(request_key: str) -> object:
    return request_key


def _publication_version(record: Mapping[str, Any]) -> str | None:
    return str(record["publication_date"])


def _document(number: str = "2026-00001", **changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "agencies": [],
        "body_html_url": None,
        "document_number": number,
        "html_url": f"https://www.federalregister.gov/d/{number}",
        "pdf_url": None,
        "publication_date": "2026-08-25",
        "regulation_id_numbers": ["not-a-rin"],
        "title": "A source-native rule",
        "topics": [],
        "type": "Rule",
    }
    value.update(changes)
    return value


def _response(
    *documents: dict[str, object],
    next_page_url: str | None = None,
    count: int | None = None,
    total_pages: int | None = None,
) -> bytes:
    return json.dumps(
        {
            "count": len(documents) if count is None else count,
            "next_page_url": next_page_url,
            "results": list(documents),
            "total_pages": (1 if next_page_url is None else 2) if total_pages is None else total_pages,
        },
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _page(
    traversal: int,
    page: int,
    response: bytes,
    *,
    cursor: str | None = None,
) -> FederalRegisterPage:
    return FederalRegisterPage(
        traversal_index=traversal,
        page_index=page,
        request_key=cursor or federal_register_documents_url(QUERY_SCOPE),
        source_cursor=cursor,
        response_bytes=response,
        window_index=0,
        window_page_index=page,
    )


def _build(*, query_scope: dict[str, str] | None = None) -> SourceNativeReleaseBuild:
    return SourceNativeReleaseBuild(
        query_scope=query_scope or QUERY_SCOPE,
        producer=PRODUCER,
        started_at="2026-08-25T00:00:00Z",
    )


def _stable_pages(*documents: dict[str, object]) -> list[FederalRegisterPage]:
    response = _response(*documents)
    return [_page(0, 0, response), _page(1, 0, response)]


def _stable_paged_pages(
    *documents: dict[str, object],
) -> list[FederalRegisterPage]:
    pages: list[FederalRegisterPage] = []
    for traversal in range(2):
        cursor: str | None = None
        for page_index, document in enumerate(documents):
            next_cursor = (
                None
                if page_index == len(documents) - 1
                else (
                    "https://www.federalregister.gov/api/v1/documents"
                    f"?format=json&page={page_index + 2}"
                )
            )
            pages.append(
                _page(
                    traversal,
                    page_index,
                    _response(
                        document,
                        next_page_url=next_cursor,
                        count=len(documents),
                        total_pages=len(documents),
                    ),
                    cursor=cursor,
                )
            )
            cursor = next_cursor
    return pages


def _publish(tmp_path: Path, pages: list[FederalRegisterPage]):
    return SourceNativeReleasePublisher(
        FEDERAL_REGISTER_PROFILE,
        blob_store=LocalSourceNativeBlobStore(tmp_path / "blobs"),
        clock=_completed_at,
    ).publish(
        pages,
        build=_build(),
        destination=tmp_path / "release",
    )


def _reader(root: Path, pin):
    return SourceNativeReleaseReader(
        LocalMemberSource(root),
        blob_source=LocalSourceNativeBlobStore(root.parent / "blobs"),
        profile=FEDERAL_REGISTER_PROFILE,
        expected_pin=pin,
        accepted_verifier_implementation_ids=frozenset({IMPLEMENTATION_ID}),
    )


def _payload_rows(root: Path, partition_kind: str) -> list[dict[str, Any]]:
    receipt = json.loads((root / "receipts/publication.json").read_bytes())
    store = LocalSourceNativeBlobStore(root.parent / "blobs")
    rows: list[dict[str, Any]] = []
    for partition in receipt["payloadPartitions"]:
        if partition["partitionKind"] != partition_kind:
            continue
        with store.open(partition["blobRef"]) as stream:
            rows.extend(json.loads(line) for line in stream)
    return rows


def _partition_map(root: Path) -> dict[tuple[str, str], dict[str, object]]:
    receipt = json.loads((root / "receipts/publication.json").read_bytes())
    return {
        (value["partitionKind"], value["partitionId"]): value
        for value in receipt["payloadPartitions"]
    }


class _CountingBlobSource:
    def __init__(self, store: LocalSourceNativeBlobStore) -> None:
        self._store = store
        self.active = 0
        self.maximum = 0

    @contextmanager
    def open(self, blob_ref: str):
        self.active += 1
        self.maximum = max(self.maximum, self.active)
        try:
            with self._store.open(blob_ref) as stream:
                yield stream
        finally:
            self.active -= 1


def test_stable_release_preserves_source_value_and_streams(tmp_path: Path) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))
    reader = _reader(published.root, published.artifact.pin)

    records = list(reader.iter_records())
    assert reader.pin == published.artifact.pin
    assert reader.source_state_scope == "observed-crawl"
    assert reader.source_system_id == "https://www.federalregister.gov/api/v1"
    assert reader.source_system_version == "v1"
    assert reader.source_state_digest.startswith("sha256:")
    assert reader.source_native_schema_set_digest.startswith("sha256:")
    receipt = json.loads((published.root / "receipts/publication.json").read_bytes())
    assert receipt["startedAt"] == "2026-08-25T00:00:00Z"
    assert receipt["completedAt"] == "2026-08-25T00:00:01Z"
    assert records[0]["record"]["agencies"] == []
    assert records[0]["record"]["topics"] == []
    assert records[0]["record"]["regulation_id_numbers"] == ["not-a-rin"]
    assert records[0]["fieldDiagnostics"] == [
        {
            "code": "malformed-rin",
            "field": "regulation_id_numbers",
            "value": "not-a-rin",
        }
    ]

    renditions = list(reader.iter_renditions())
    assert [(row["sourceField"], row["locator"], row["mediaType"]) for row in renditions] == [
        ("body_html_url", None, "text/html"),
        ("html_url", "https://www.federalregister.gov/d/2026-00001", "text/html"),
        ("pdf_url", None, "application/pdf"),
    ]


def test_identical_evidence_pages_keep_distinct_page_inventories(
    tmp_path: Path,
) -> None:
    response = _response(_document())
    pages = [
        FederalRegisterPage(
            traversal_index=traversal,
            page_index=window,
            request_key=f"https://example.test/window/{window}",
            source_cursor=None,
            response_bytes=response,
            window_index=window,
            window_page_index=0,
        )
        for traversal in range(2)
        for window in range(2)
    ]
    profile = replace(
        FEDERAL_REGISTER_PROFILE,
        acquisition_check=_PassAcquisitionCheck,
        observation_version=_publication_version,
        page_window=_request_window,
        records_included=_accept_all_records,
        validate_record_scope=_accept_record_scope,
    )
    published = SourceNativeReleasePublisher(
        profile,
        blob_store=LocalSourceNativeBlobStore(tmp_path / "blobs"),
        clock=_completed_at,
    ).publish(
        pages,
        build=_build(),
        destination=tmp_path / "release",
    )
    page_rows = sorted(
        _payload_rows(published.root, "acquisition-pages"),
        key=lambda row: (row["traversalIndex"], row["pageIndex"]),
    )

    assert len({row["evidenceBlobRef"] for row in page_rows}) == 1
    assert [len(row["discoveredRecords"]) for row in page_rows] == [1, 1, 1, 1]
    reader = SourceNativeReleaseReader(
        LocalMemberSource(published.root),
        blob_source=LocalSourceNativeBlobStore(tmp_path / "blobs"),
        profile=profile,
        expected_pin=published.artifact.pin,
        accepted_verifier_implementation_ids=frozenset({IMPLEMENTATION_ID}),
    )
    assert len(list(reader.iter_records())) == 1


def test_source_native_record_preserves_predecessor_source_facts(tmp_path: Path) -> None:
    document = _document(
        agencies=[
            {
                "name": "Environmental Protection Agency",
                "raw_name": "ENVIRONMENTAL PROTECTION AGENCY",
                "slug": "environmental-protection-agency",
            }
        ],
        body_html_url="https://www.federalregister.gov/documents/full_text/html/2026-00001.html",
        docket_ids=["EPA-HQ-OAR-2026-0001"],
        pdf_url="https://www.govinfo.gov/content/pkg/FR-2026-08-25/pdf/2026-00001.pdf",
        regulation_id_numbers=["2060-AV12"],
        title="Native Federal Register title",
        topics=["Air pollution control"],
        type="Notice",
    )

    published = _publish(tmp_path, _stable_pages(document))
    reader = _reader(published.root, published.artifact.pin)

    assert list(reader.iter_records())[0]["record"] == document
    assert [(row["sourceField"], row["locator"]) for row in reader.iter_renditions()] == [
        ("body_html_url", document["body_html_url"]),
        ("html_url", document["html_url"]),
        ("pdf_url", document["pdf_url"]),
    ]


@pytest.mark.parametrize("publication_date", [None, "", "not-a-date", "2026-08-25T00:00:00Z"])
def test_source_issued_publication_date_is_required(tmp_path: Path, publication_date: object) -> None:
    with pytest.raises(FederalRegisterSourceError, match="publication_date"):
        _publish(tmp_path, _stable_pages(_document(publication_date=publication_date)))


def test_acquisition_exception_cannot_publish_partial_release(tmp_path: Path) -> None:
    destination = tmp_path / "interrupted"

    def interrupted_pages() -> Iterator[FederalRegisterPage]:
        yield _page(0, 0, _response(_document()))
        raise RuntimeError("source stopped")

    with pytest.raises(RuntimeError, match="source stopped"):
        SourceNativeReleasePublisher(
            FEDERAL_REGISTER_PROFILE,
            blob_store=LocalSourceNativeBlobStore(tmp_path / "blobs"),
            clock=_completed_at,
        ).publish(
            interrupted_pages(),
            build=_build(),
            destination=destination,
        )

    assert not destination.exists()
    orphan_refs = {path.name for path in (tmp_path / "blobs" / "sha256").iterdir()}
    assert len(orphan_refs) == 1

    recovered = _publish(tmp_path, _stable_pages(_document()))
    receipt = json.loads((recovered.root / "receipts/publication.json").read_bytes())
    assert receipt["byteMeasurements"]["payloadBytesReused"] > 0
    assert orphan_refs <= {
        path.name for path in (tmp_path / "blobs" / "sha256").iterdir()
    }


def test_successor_reuses_unchanged_buckets_and_writes_only_new_payloads(
    tmp_path: Path,
) -> None:
    documents = [_document(f"2026-{number:05d}") for number in range(1, 13)]
    store = LocalSourceNativeBlobStore(tmp_path / "blobs")

    initial = SourceNativeReleasePublisher(
        FEDERAL_REGISTER_PROFILE,
        blob_store=store,
        clock=_completed_at,
    ).publish(
        _stable_paged_pages(*documents),
        build=_build(),
        destination=tmp_path / "initial",
    )
    initial_files = {
        path.name: path.stat().st_size for path in (tmp_path / "blobs" / "sha256").iterdir()
    }

    changed_index = 5
    changed_documents = [dict(value) for value in documents]
    changed_documents[changed_index]["title"] = "One changed source row"
    successor = SourceNativeReleasePublisher(
        FEDERAL_REGISTER_PROFILE,
        blob_store=store,
        clock=_completed_at,
    ).publish(
        _stable_paged_pages(*changed_documents),
        build=_build(),
        destination=tmp_path / "successor",
    )

    initial_partitions = _partition_map(initial.root)
    successor_partitions = _partition_map(successor.root)
    changed_record_id = str(documents[changed_index]["document_number"])
    changed_bucket = int.from_bytes(
        hashlib.sha256(changed_record_id.encode()).digest(), "big"
    ) % 64
    expected_changed = {
        ("records", f"{changed_bucket:02d}"),
        ("acquisition-records", f"{changed_bucket:02d}"),
        *{
            (
                "acquisition-pages",
                f"{int.from_bytes(hashlib.sha256(f'{traversal}:{changed_index}'.encode()).digest(), 'big') % 64:02d}",
            )
            for traversal in range(2)
        },
    }
    observed_changed = {
        key
        for key in initial_partitions
        if initial_partitions[key]["blobRef"] != successor_partitions[key]["blobRef"]
    }
    assert observed_changed == expected_changed
    assert all(
        initial_partitions[key]["blobRef"] == successor_partitions[key]["blobRef"]
        for key in initial_partitions
        if key not in expected_changed
    )

    successor_receipt = json.loads(
        (successor.root / "receipts/publication.json").read_bytes()
    )
    current_files = {
        path.name: path.stat().st_size for path in (tmp_path / "blobs" / "sha256").iterdir()
    }
    new_files = set(current_files) - set(initial_files)
    assert successor_receipt["byteMeasurements"]["payloadBytesWritten"] == sum(
        current_files[name] for name in new_files
    )
    assert successor_receipt["byteMeasurements"]["payloadBytesReused"] > 0

    rebuilt = SourceNativeReleasePublisher(
        FEDERAL_REGISTER_PROFILE,
        blob_store=store,
        clock=_completed_at,
    ).publish(
        _stable_paged_pages(*documents),
        build=_build(),
        destination=tmp_path / "rebuilt",
    )
    rebuilt_receipt = json.loads((rebuilt.root / "receipts/publication.json").read_bytes())
    assert _partition_map(rebuilt.root) == initial_partitions
    assert rebuilt.artifact.pin.logical_id == initial.artifact.pin.logical_id
    assert rebuilt.artifact.pin.artifact_digest != initial.artifact.pin.artifact_digest
    assert rebuilt_receipt["byteMeasurements"]["payloadBytesWritten"] == 0
    assert (
        rebuilt_receipt["byteMeasurements"]["payloadBytesReused"]
        == rebuilt_receipt["byteMeasurements"]["payloadBytesRead"]
    )


def test_blob_store_refuses_corrupt_existing_content(tmp_path: Path) -> None:
    payload = b"digest-addressed source bytes"
    blob_ref = "sha256:" + hashlib.sha256(payload).hexdigest()
    store = LocalSourceNativeBlobStore(tmp_path / "blobs")

    write = store.put_blob(blob_ref, len(payload), (payload,))
    assert write.reused is False
    assert write.bytes_written == len(payload)
    path = tmp_path / "blobs" / "sha256" / blob_ref[7:]
    path.write_bytes(b"x" * len(payload))

    with pytest.raises(ImmutablePublicationError, match="content identity"):
        store.put_blob(blob_ref, len(payload), (payload,))


def test_read_only_blob_store_open_does_not_create_missing_layout(
    tmp_path: Path,
) -> None:
    root = tmp_path / "blobs"
    root.mkdir()

    with pytest.raises(ValueError, match="layout is missing"):
        LocalSourceNativeBlobStore(root, create=False)

    assert list(root.iterdir()) == []


def test_blob_store_reader_refuses_a_replaced_root(tmp_path: Path) -> None:
    root = tmp_path / "blobs"
    store = LocalSourceNativeBlobStore(root)
    payload = b"pinned source-native bytes"
    blob_ref = "sha256:" + hashlib.sha256(payload).hexdigest()
    store.put_blob(blob_ref, len(payload), (payload,))
    retained = tmp_path / "retained-blobs"
    root.rename(retained)
    LocalSourceNativeBlobStore(root)

    with pytest.raises(MemberSourceError, match="artifact root changed"):
        with store.open(blob_ref):
            pytest.fail("a replaced root must fail before yielding bytes")

    assert (retained / "sha256" / blob_ref[7:]).read_bytes() == payload


@pytest.mark.parametrize("child_name", ["sha256", ".pending"])
def test_blob_store_refuses_internal_symlink_layout(
    tmp_path: Path,
    child_name: str,
) -> None:
    root = tmp_path / "blobs"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    sentinel = outside / "sentinel"
    sentinel.write_text("unchanged")
    (root / child_name).symlink_to(outside, target_is_directory=True)

    with pytest.raises(ValueError, match="non-symlink directory"):
        LocalSourceNativeBlobStore(root)

    assert list(outside.iterdir()) == [sentinel]
    assert sentinel.read_text() == "unchanged"


@pytest.mark.parametrize("child_name", ["sha256", ".pending"])
def test_blob_store_refuses_replaced_internal_directory(
    tmp_path: Path,
    child_name: str,
) -> None:
    root = tmp_path / "blobs"
    outside = tmp_path / "outside"
    outside.mkdir()
    store = LocalSourceNativeBlobStore(root)
    original = root / f"{child_name}.original"
    (root / child_name).rename(original)
    (root / child_name).symlink_to(outside, target_is_directory=True)
    payload = b"must remain inside the admitted store"
    blob_ref = "sha256:" + hashlib.sha256(payload).hexdigest()

    with pytest.raises(ValueError, match="non-symlink directory"):
        store.put_blob(blob_ref, len(payload), (payload,))

    assert list(outside.iterdir()) == []
    assert list(original.iterdir()) == []


def test_external_payloads_require_injected_blob_source(tmp_path: Path) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))

    with pytest.raises(ArtifactVerificationError, match="injected BlobSource"):
        admit_artifact(LocalMemberSource(published.root))


def test_reader_holds_at_most_the_fixed_bucket_count_of_streams(tmp_path: Path) -> None:
    documents = [_document(f"2026-{number:05d}") for number in range(1, 257)]
    published = _publish(tmp_path, _stable_pages(*documents))
    counting = _CountingBlobSource(LocalSourceNativeBlobStore(tmp_path / "blobs"))
    reader = SourceNativeReleaseReader(
        LocalMemberSource(published.root),
        blob_source=counting,
        profile=FEDERAL_REGISTER_PROFILE,
        expected_pin=published.artifact.pin,
        accepted_verifier_implementation_ids=frozenset({IMPLEMENTATION_ID}),
    )

    assert len(list(reader.iter_records())) == len(documents)
    assert 1 < counting.maximum <= 64
    assert counting.active == 0


def test_concurrent_publishers_never_replace_the_winner(tmp_path: Path) -> None:
    barrier = Barrier(2)
    destination = tmp_path / "release"

    def publish():
        def synchronized_pages() -> Iterator[FederalRegisterPage]:
            barrier.wait()
            yield from _stable_pages(_document())

        return SourceNativeReleasePublisher(
            FEDERAL_REGISTER_PROFILE,
            blob_store=LocalSourceNativeBlobStore(tmp_path / "blobs"),
            clock=_completed_at,
        ).publish(
            synchronized_pages(),
            build=_build(),
            destination=destination,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish) for _ in range(2)]
    successes = [future.result() for future in futures if future.exception() is None]
    failures = [future.exception() for future in futures if future.exception() is not None]

    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], (FileExistsError, ImmutablePublicationError))
    assert _reader(destination, successes[0].artifact.pin).pin == successes[0].artifact.pin


def test_stale_publication_lock_file_does_not_poison_retry(tmp_path: Path) -> None:
    destination = tmp_path / "release"
    (tmp_path / ".release.publish.lock").write_bytes(b"abandoned")

    published = SourceNativeReleasePublisher(
        FEDERAL_REGISTER_PROFILE,
        blob_store=LocalSourceNativeBlobStore(tmp_path / "blobs"),
        clock=_completed_at,
    ).publish(
        _stable_pages(_document()),
        build=_build(),
        destination=destination,
    )

    assert _reader(destination, published.artifact.pin).pin == published.artifact.pin


def test_observed_crawl_refuses_one_unreconciled_traversal(tmp_path: Path) -> None:
    with pytest.raises(SourceNativeReleaseError, match="two stable consecutive traversals"):
        _publish(tmp_path, [_page(0, 0, _response(_document()))])


def test_complete_snapshot_refuses_different_reconciliation_passes(tmp_path: Path) -> None:
    pages = [
        _page(0, 0, _response(_document("2026-00001"))),
        _page(1, 0, _response(_document("2026-00002"))),
    ]

    with pytest.raises(SourceNativeReleaseError, match="stable consecutive traversals"):
        _publish(tmp_path, pages)


def test_stable_federal_reconciliation_exposes_observed_crawl_scope(tmp_path: Path) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))

    assert _reader(published.root, published.artifact.pin).source_state_scope == "observed-crawl"


def test_unclassified_source_field_fails_publication(tmp_path: Path) -> None:
    drifted = _document(new_upstream_field="unclassified")

    with pytest.raises(FederalRegisterSourceError, match="unclassified.*document fields"):
        _publish(tmp_path, _stable_pages(drifted))


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"title": 7}, "title must be text or null"),
        ({"volume": True}, "volume must be an integer or null"),
        ({"docket_ids": [7]}, "docket_ids must be a text array or null"),
        ({"topics": [7]}, "topics must be a text array or null"),
    ],
)
def test_source_field_type_drift_fails_publication(
    tmp_path: Path,
    changes: dict[str, object],
    message: str,
) -> None:
    document = _document()
    document.update(changes)
    with pytest.raises(FederalRegisterSourceError, match=message):
        _publish(tmp_path, _stable_pages(document))


def test_missing_or_forked_page_chain_fails(tmp_path: Path) -> None:
    next_url = "https://www.federalregister.gov/api/v1/documents?format=json&page=2&cursor=stable"
    first = _page(0, 0, _response(_document(), next_page_url=next_url))
    second = _page(0, 1, _response(_document("2026-00002")), cursor="wrong")

    with pytest.raises(SourceNativeReleaseError, match="missing or forked"):
        _publish(tmp_path, [first, second])


def test_publisher_refuses_a_cyclic_source_cursor(tmp_path: Path) -> None:
    initial = federal_register_documents_url(QUERY_SCOPE)
    second = "https://www.federalregister.gov/api/v1/documents?format=json&page=2"
    pages = [
        FederalRegisterPage(
            0,
            0,
            initial,
            None,
            _response(_document("2026-00001"), next_page_url=second, count=3, total_pages=3),
        ),
        FederalRegisterPage(
            0,
            1,
            second,
            second,
            _response(_document("2026-00002"), next_page_url=initial, count=3, total_pages=3),
            window_page_index=1,
        ),
    ]

    with pytest.raises(FederalRegisterSourceError, match="cyclic page cursor"):
        _publish(tmp_path, pages)


def test_changed_member_fails_before_reader_yields(tmp_path: Path) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))
    receipt = json.loads((published.root / "receipts/publication.json").read_bytes())
    record_partition = next(
        value
        for value in receipt["payloadPartitions"]
        if value["partitionKind"] == "records"
    )
    records_path = tmp_path / "blobs" / "sha256" / record_partition["blobRef"][7:]
    records_path.write_bytes(records_path.read_bytes().replace(b"source-native rule", b"changed source rule"))

    with pytest.raises(ArtifactVerificationError, match="invalid.member-digest"):
        _reader(published.root, published.artifact.pin)


@pytest.mark.parametrize("mutation", ["same-size", "changed-size"])
def test_blob_mutation_after_reader_admission_fails_before_a_row_is_returned(
    tmp_path: Path,
    mutation: str,
) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))
    reader = _reader(published.root, published.artifact.pin)
    receipt = json.loads((published.root / "receipts/publication.json").read_bytes())
    record_partition = next(
        value
        for value in receipt["payloadPartitions"]
        if value["partitionKind"] == "records"
    )
    records_path = tmp_path / "blobs" / "sha256" / record_partition["blobRef"][7:]
    payload = records_path.read_bytes()
    records_path.write_bytes(
        bytes([payload[0] ^ 1]) + payload[1:]
        if mutation == "same-size"
        else payload + b"\n"
    )

    with pytest.raises(ArtifactVerificationError, match="invalid.member-digest"):
        next(reader.iter_records())


def test_records_and_renditions_stream_across_fixed_identity_buckets(
    tmp_path: Path,
) -> None:
    documents = [_document(f"2026-{number:05d}") for number in range(3, 0, -1)]
    published = _publish(tmp_path, _stable_pages(*documents))
    reader = _reader(published.root, published.artifact.pin)

    assert [row["sourceRecordId"] for row in reader.iter_records()] == [
        "2026-00001",
        "2026-00002",
        "2026-00003",
    ]
    assert len(list(reader.iter_renditions())) == 9
    receipt = json.loads((published.root / "receipts/publication.json").read_bytes())
    assert receipt["partitionPolicy"] == {
        "algorithm": "sha256-utf8-modulo",
        "bucketCount": 64,
        "identityEncoding": "utf-8",
    }
    assert sorted(path.name for path in (published.root / "records").iterdir()) == [
        "scopes.jsonl"
    ]
    assert {
        value["partitionKind"] for value in receipt["payloadPartitions"]
    } >= {"records", "renditions"}


def test_reader_requires_explicit_verifier_allowlist(tmp_path: Path) -> None:
    published = _publish(tmp_path, _stable_pages(_document()))

    with pytest.raises(SourceNativeReleaseError, match="at least one verifier"):
        SourceNativeReleaseReader(
            LocalMemberSource(published.root),
            blob_source=LocalSourceNativeBlobStore(tmp_path / "blobs"),
            profile=FEDERAL_REGISTER_PROFILE,
            expected_pin=published.artifact.pin,
            accepted_verifier_implementation_ids=frozenset(),
        )
    with pytest.raises(SourceNativeReleaseError, match="not accepted"):
        SourceNativeReleaseReader(
            LocalMemberSource(published.root),
            blob_source=LocalSourceNativeBlobStore(tmp_path / "blobs"),
            profile=FEDERAL_REGISTER_PROFILE,
            expected_pin=published.artifact.pin,
            accepted_verifier_implementation_ids=frozenset({"git+https://example.test/other@" + "b" * 40}),
        )


def test_new_path_uses_only_shared_artifact_implementation() -> None:
    source_native = Path(__file__).parents[1] / "src/spicy_regs/source_native.py"
    profile = Path(__file__).parents[1] / "src/spicy_regs/federal_register_source_native.py"
    text = source_native.read_text() + profile.read_text()

    assert "rulespec_conformance" not in text
    assert "build_artifact_root" in text
    assert "docspec" not in text.lower()
    assert "refspec" not in text.lower()


def test_base_package_keeps_legacy_platform_dependencies_out_of_source_native_path() -> None:
    project_root = Path(__file__).parents[1]
    configuration = tomllib.loads(
        (project_root / "pyproject.toml").read_text(encoding="utf-8")
    )
    project = configuration["project"]

    assert project["version"] == "0.1.7"
    dependencies = project["dependencies"]
    assert "rulespec-artifacts==1.0.9" in dependencies
    assert configuration["tool"]["uv"]["sources"]["rulespec-artifacts"] == {
        "path": "vendor/rulespec_artifacts-1.0.9-py3-none-any.whl"
    }
    rulespec_wheel = project_root / "vendor/rulespec_artifacts-1.0.9-py3-none-any.whl"
    assert hashlib.sha256(rulespec_wheel.read_bytes()).hexdigest() == (
        "67cb33bf63c11bc6812ad0e8f0a8b73e89501fa6d4242acf75a7cc6612f5d6c6"
    )
    assert not any(
        dependency.startswith(("refspec", "rdflib", "rulespec-conformance"))
        for dependency in dependencies
    )
    assert "build-source-catalog" not in project["scripts"]


def test_installed_schema_bundle_is_the_exact_generated_bundle() -> None:
    assert installed_release_schema_bundle() == release_schema_bundle()


def test_injected_page_fetcher_builds_the_closed_query_and_reconciles() -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-13"}
    initial = federal_register_documents_url(scope, per_page=1000)
    next_url = "https://www.federalregister.gov/api/v1/documents?format=json&page=2&cursor=stable"
    responses = {
        initial: _response(
            _document("2026-00002"),
            next_page_url=next_url,
            count=2,
            total_pages=2,
        ),
        next_url: _response(
            _document("2026-00001"),
            count=2,
            total_pages=2,
        ),
    }
    requests: list[str] = []

    def fetch(url: str) -> bytes:
        requests.append(url)
        return responses[url]

    pages = list(iter_federal_register_pages(fetch, query_scope=scope))

    assert requests == [initial, next_url, initial, next_url]
    assert [(page.traversal_index, page.page_index, page.source_cursor) for page in pages] == [
        (0, 0, None),
        (0, 1, next_url),
        (1, 0, None),
        (1, 1, next_url),
    ]
    query = parse_qs(urlparse(initial).query)
    assert query["conditions[publication_date][gte]"] == ["2026-04-13"]
    assert query["conditions[publication_date][lte]"] == ["2026-04-13"]
    assert query["fields[]"] == sorted(DOCUMENT_FIELDS)


def test_injected_page_fetcher_refuses_incomplete_or_cyclic_inventory() -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-13"}
    initial = federal_register_documents_url(scope)

    with pytest.raises(FederalRegisterSourceError, match="declared and observed record counts"):
        list(
            iter_federal_register_pages(
                lambda _url: _response(_document(), count=2, total_pages=1),
                query_scope=scope,
            )
        )

    with pytest.raises(FederalRegisterSourceError, match="cyclic page cursor"):
        list(
            iter_federal_register_pages(
                lambda _url: _response(
                    _document(),
                    next_page_url=initial,
                    count=2,
                    total_pages=2,
                ),
                query_scope=scope,
                traversals=1,
            )
        )


def test_capped_interval_splits_into_exact_ordered_leaf_evidence(tmp_path: Path) -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-14"}
    requests: list[tuple[str, str]] = []

    def fetch(url: str) -> bytes:
        query = parse_qs(urlparse(url).query)
        window = (
            query["conditions[publication_date][gte]"][0],
            query["conditions[publication_date][lte]"][0],
        )
        requests.append(window)
        if window[0] != window[1]:
            return _response(
                _document("2026-cap-probe", publication_date=window[0]),
                count=10_000,
                total_pages=10,
            )
        number = "2026-00013" if window[0].endswith("13") else "2026-00014"
        return _response(_document(number, publication_date=window[0]))

    destination = tmp_path / "split-release"
    published = SourceNativeReleasePublisher(
        FEDERAL_REGISTER_PROFILE,
        blob_store=LocalSourceNativeBlobStore(tmp_path / "blobs"),
        clock=_completed_at,
    ).publish(
        iter_federal_register_pages(fetch, query_scope=scope),
        build=_build(query_scope=scope),
        destination=destination,
    )
    reader = _reader(published.root, published.artifact.pin)

    assert [row["sourceRecordId"] for row in reader.iter_records()] == ["2026-00013", "2026-00014"]
    assert requests == [
        ("2026-04-13", "2026-04-14"),
        ("2026-04-13", "2026-04-13"),
        ("2026-04-14", "2026-04-14"),
    ] * 2
    pages = sorted(
        _payload_rows(destination, "acquisition-pages"),
        key=lambda row: (row["traversalIndex"], row["pageIndex"]),
    )
    assert [
        (
            row["traversalIndex"],
            row["windowIndex"],
            row["windowPageIndex"],
            row["recordsIncluded"],
        )
        for row in pages
    ] == [
        (0, 0, 0, False),
        (0, 1, 0, True),
        (0, 2, 0, True),
        (1, 0, 0, False),
        (1, 1, 0, True),
        (1, 2, 0, True),
    ]
    evidence_refs = {row["evidenceBlobRef"] for row in pages}
    assert len(evidence_refs) == 3
    assert not (destination / "evidence").exists()
    store = LocalSourceNativeBlobStore(tmp_path / "blobs")
    evidence_counts = set()
    for blob_ref in evidence_refs:
        with store.open(blob_ref) as stream:
            evidence_counts.add(parse_page_response(stream.read())["count"])
    assert evidence_counts == {1, 10_000}


def test_capped_single_day_refuses_ambiguous_source_state() -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-13"}
    pages = iter_federal_register_pages(
        lambda _url: _response(
            _document("2026-cap", publication_date="2026-04-13"),
            count=10_000,
            total_pages=10,
        ),
        query_scope=scope,
    )

    probe = next(pages)
    assert parse_page_response(probe.response_bytes)["count"] == 10_000
    with pytest.raises(FederalRegisterSourceError, match="result cap is ambiguous.*2026-04-13"):
        next(pages)


@pytest.mark.parametrize(
    "days",
    [
        ["2026-04-13"],
        ["2026-04-14", "2026-04-13"],
    ],
)
def test_publisher_refuses_missing_or_reordered_date_windows(tmp_path: Path, days: list[str]) -> None:
    scope = {"publishedFrom": "2026-04-13", "publishedThrough": "2026-04-14"}
    pages = [
        FederalRegisterPage(
            traversal_index=0,
            page_index=index,
            request_key=federal_register_documents_url(
                {"publishedFrom": day, "publishedThrough": day}
            ),
            source_cursor=None,
            response_bytes=_response(
                _document(f"2026-{day[-2:]}", publication_date=day)
            ),
            window_index=index,
            window_page_index=0,
        )
        for index, day in enumerate(days)
    ]

    with pytest.raises(FederalRegisterSourceError, match="split windows"):
        SourceNativeReleasePublisher(
            FEDERAL_REGISTER_PROFILE,
            blob_store=LocalSourceNativeBlobStore(tmp_path / "blobs"),
            clock=_completed_at,
        ).publish(
            pages,
            build=_build(query_scope=scope),
            destination=tmp_path / "invalid-windows",
        )


def test_publisher_independently_refuses_a_false_source_count(tmp_path: Path) -> None:
    response = _response(_document(), count=2, total_pages=1)

    with pytest.raises(FederalRegisterSourceError, match="declared and observed record counts"):
        _publish(tmp_path, [_page(0, 0, response)])
