"""Regulations.gov source-native facts and exact acquisition evidence."""

from __future__ import annotations

import json
from collections.abc import Iterator
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from rulespec_artifacts import LocalMemberSource, Producer

from spicy_regs.regulations_gov_source_native import (
    DOCUMENT_COLLECTION,
    DOCUMENT_SOURCE_SYSTEM_ID,
    DOCKET_COLLECTION,
    DOCKET_SOURCE_SYSTEM_ID,
    RegulationsGovPage,
    RegulationsGovSourceError,
    classify_document,
    classify_docket,
    document_rendition_rows,
    iter_regulations_gov_document_pages,
    iter_regulations_gov_docket_pages,
    parse_document_page_response,
    parse_mirrulations_request,
    regulations_gov_document_query_scope,
    regulations_gov_docket_query_scope,
)
from spicy_regs.source_native import (
    SourceNativeReleaseBuild,
    SourceNativeReleaseError,
    SourceNativeReleasePublisher,
    SourceNativeReleaseReader,
)
from spicy_regs.source_native_profiles import (
    REGULATIONS_GOV_DOCUMENT_PROFILE,
    REGULATIONS_GOV_DOCKET_PROFILE,
)

_IMPLEMENTATION_ID = "git+https://example.test/spicy-regs@" + "a" * 40
_PRODUCER = Producer(
    product="spicy-regs",
    implementation_id=_IMPLEMENTATION_ID,
    verifier_id="urn:spicy-regs:source-native-release-verifier",
    verifier_version="1.0",
    verifier_implementation_id=_IMPLEMENTATION_ID,
)


def _document(identity: str = "EPA-2026-0001-0001", **attributes: object) -> dict[str, Any]:
    values: dict[str, Any] = {
        "additionalRins": ["2060-AV12", "source-value-that-is-not-a-rin"],
        "agencyId": "EPA",
        "commentEndDate": None,
        "docketId": "EPA-2026-0001",
        "documentType": "Notice",
        "fileFormats": [
            {
                "fileUrl": f"https://downloads.regulations.gov/{identity}/content.pdf",
                "format": "pdf",
                "size": 123,
            },
            {
                "fileUrl": f"https://downloads.regulations.gov/{identity}/notice.xml",
                "format": "xml",
                "size": None,
            },
        ],
        "frDocNum": "2026-10001",
        "modifyDate": "2026-08-25T01:02:03Z",
        "postedDate": "2026-08-24T04:00:00Z",
        "reasonWithdrawn": "Issued in error",
        "subtype": None,
        "title": "Exact source title",
        "topics": ["Air quality", {"id": "source-topic", "label": "Source topic"}],
        "withdrawn": True,
    }
    values.update(attributes)
    return {
        "data": {
            "id": identity,
            "type": DOCUMENT_COLLECTION,
            "attributes": values,
            "links": {"self": f"https://api.regulations.gov/v4/documents/{identity}"},
        },
        "included": [
            {
                "id": f"{identity}-attachment-1",
                "type": "attachments",
                "attributes": {
                    "description": None,
                    "fileFormats": [
                        {
                            "fileUrl": (
                                f"https://downloads.regulations.gov/{identity}/attachment.docx"
                            ),
                            "format": "docx",
                            "size": "99",
                        }
                    ],
                    "title": "Supporting attachment",
                },
            }
        ],
        "meta": {"hasMore": False, "totalElements": 1},
    }


def _docket(identity: str = "EPA-2026-0001", **attributes: object) -> dict[str, Any]:
    values: dict[str, Any] = {
        "agencyId": "EPA",
        "category": None,
        "displayProperties": [
            {"name": "abstract", "label": "Description", "tooltip": None}
        ],
        "dkAbstract": "Exact docket abstract",
        "docketType": "Rulemaking",
        "keywords": ["air", "emissions"],
        "modifyDate": "2026-08-24T05:00:00Z",
        "rin": None,
        "title": "Exact docket title",
    }
    values.update(attributes)
    return {
        "data": {
            "id": identity,
            "type": DOCKET_COLLECTION,
            "attributes": values,
            "links": {"self": f"https://api.regulations.gov/v4/dockets/{identity}"},
        }
    }


def _bytes(value: object, *, indent: int | None = None) -> bytes:
    return json.dumps(value, indent=indent, separators=None if indent else (",", ":")).encode()


@dataclass(frozen=True, slots=True)
class _Object:
    key: str
    etag: str
    version_id: str | None
    content: bytes


class _Reader:
    def __init__(self, objects: list[_Object], calls: list[tuple[str, int]] | None = None) -> None:
        self.objects = objects
        self.calls = calls

    def iter_source_objects(self, *, max_bytes: int) -> Iterator[_Object]:
        if self.calls is not None:
            self.calls.append(("iter", max_bytes))
        yield from self.objects


def _document_object(
    identity: str = "EPA-2026-0001-0001",
    *,
    value: dict[str, Any] | None = None,
    etag: str = '"document-etag"',
) -> _Object:
    return _Object(
        key=(
            "raw-data/EPA/EPA-2026-0001/text-1/documents/"
            f"{identity}.json"
        ),
        etag=etag,
        version_id="document-version-1",
        content=_bytes(value or _document(identity), indent=2),
    )


def _docket_object(
    identity: str = "EPA-2026-0001",
    *,
    value: dict[str, Any] | None = None,
) -> _Object:
    return _Object(
        key=f"raw-data/EPA/{identity}/text-1/docket/{identity}.json",
        etag='"docket-etag"',
        version_id=None,
        content=_bytes(value or _docket(identity), indent=2),
    )


def _document_scope() -> dict[str, object]:
    return {
        "agencies": ["EPA"],
        "publishedFrom": "2026-08-24",
        "publishedThrough": "2026-08-24",
    }


def _docket_scope() -> dict[str, object]:
    return {
        "agencies": ["EPA"],
        "modifiedFrom": "2026-08-24",
        "modifiedThrough": "2026-08-24",
    }


def _build(profile, query_scope: dict[str, object]) -> SourceNativeReleaseBuild:
    return SourceNativeReleaseBuild(
        query_scope=query_scope,
        producer=_PRODUCER,
        started_at="2026-08-25T00:00:00Z",
        completed_at="2026-08-25T00:00:01Z",
    )


def _reader(root: Path, pin, profile) -> SourceNativeReleaseReader:
    return SourceNativeReleaseReader(
        LocalMemberSource(root),
        profile=profile,
        expected_pin=pin,
        accepted_verifier_implementation_ids=frozenset({_IMPLEMENTATION_ID}),
    )


def test_document_record_preserves_source_facts_and_join_keys_without_prejoining() -> None:
    raw = _document()

    assert classify_document(raw) == raw
    attributes = raw["data"]["attributes"]
    assert attributes["docketId"] == "EPA-2026-0001"
    assert attributes["frDocNum"] == "2026-10001"
    assert attributes["topics"] == [
        "Air quality",
        {"id": "source-topic", "label": "Source topic"},
    ]
    assert attributes["withdrawn"] is True
    assert attributes["reasonWithdrawn"] == "Issued in error"
    assert "docket" not in raw and "federalRegister" not in raw
    assert raw["meta"]["hasMore"] is False


def test_document_renditions_preserve_all_file_format_evidence() -> None:
    rows = document_rendition_rows(classify_document(_document()))

    assert [row["sourceField"] for row in rows] == [
        "data.attributes.fileFormats[0]",
        "data.attributes.fileFormats[1]",
        "included[0].attributes.fileFormats[0]",
    ]
    assert [row["mediaType"] for row in rows] == [
        "application/pdf",
        "application/xml",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ]
    assert [row["expectedByteSize"] for row in rows] == [123, None, 99]
    assert all(row["expectedSha256"] is None for row in rows)


def test_document_pages_capture_exact_listing_metadata_and_object_bytes_once() -> None:
    calls: list[tuple[str, int]] = []
    source_object = _document_object()
    pages = list(
        iter_regulations_gov_document_pages(
            lambda agency: _Reader(
                [source_object] if agency == "EPA" else pytest.fail("wrong agency"),
                calls,
            ),
            query_scope=_document_scope(),
        )
    )

    assert calls == [("iter", 16 * 1024 * 1024)]
    assert len(pages) == 2
    enumeration, exact_object = pages
    assert exact_object.response_bytes == source_object.content
    manifest = json.loads(enumeration.response_bytes)
    assert manifest["objects"] == [
        {
            "byteSize": len(source_object.content),
            "etag": source_object.etag,
            "key": source_object.key,
            "versionId": source_object.version_id,
        }
    ]
    object_window = parse_mirrulations_request(exact_object.request_key)
    assert object_window.key == source_object.key
    assert object_window.etag == source_object.etag
    assert object_window.version_id == source_object.version_id
    assert parse_document_page_response(exact_object.response_bytes)["results"] == [
        _document()
    ]


def test_document_release_uses_one_source_enumeration_and_replays_exact_bytes(
    tmp_path: Path,
) -> None:
    source_object = _document_object()
    release = tmp_path / "documents"
    published = SourceNativeReleasePublisher(REGULATIONS_GOV_DOCUMENT_PROFILE).publish(
        iter_regulations_gov_document_pages(
            lambda _agency: _Reader([source_object]),
            query_scope=_document_scope(),
        ),
        build=_build(REGULATIONS_GOV_DOCUMENT_PROFILE, _document_scope()),
        destination=release,
    )
    reader = _reader(
        release,
        published.artifact.pin,
        REGULATIONS_GOV_DOCUMENT_PROFILE,
    )

    assert reader.source_system_id == DOCUMENT_SOURCE_SYSTEM_ID
    assert reader.source_state_scope == "complete-snapshot"
    assert list(reader.iter_records())[0]["record"] == _document()
    assert len(list(reader.iter_renditions())) == 3
    receipt = json.loads((release / "receipts/publication.json").read_bytes())
    assert receipt["reconciliationPassCount"] == 1
    assert len(list(release.glob("evidence/traversal-000/page-*.json"))) == 2

    with pytest.raises(SourceNativeReleaseError, match="unsupported Regulations.gov dockets"):
        _reader(release, published.artifact.pin, REGULATIONS_GOV_DOCKET_PROFILE)


def test_docket_release_is_separate_and_preserves_docket_source_facts(tmp_path: Path) -> None:
    raw = _docket()
    source_object = _docket_object(value=raw)
    release = tmp_path / "dockets"
    published = SourceNativeReleasePublisher(REGULATIONS_GOV_DOCKET_PROFILE).publish(
        iter_regulations_gov_docket_pages(
            lambda _agency: _Reader([source_object]),
            query_scope=_docket_scope(),
        ),
        build=_build(REGULATIONS_GOV_DOCKET_PROFILE, _docket_scope()),
        destination=release,
    )
    reader = _reader(release, published.artifact.pin, REGULATIONS_GOV_DOCKET_PROFILE)

    assert classify_docket(raw) == raw
    assert reader.source_system_id == DOCKET_SOURCE_SYSTEM_ID
    assert list(reader.iter_records())[0]["record"] == raw
    assert list(reader.iter_renditions()) == []


def test_out_of_scope_objects_remain_evidence_without_becoming_records(tmp_path: Path) -> None:
    in_scope = _document_object()
    out_record = _document(
        "EPA-2026-0001-0002",
        postedDate="2026-08-23T23:59:59Z",
    )
    out_of_scope = _document_object(
        "EPA-2026-0001-0002",
        value=out_record,
        etag='"older-etag"',
    )
    release = tmp_path / "bounded"
    published = SourceNativeReleasePublisher(REGULATIONS_GOV_DOCUMENT_PROFILE).publish(
        iter_regulations_gov_document_pages(
            lambda _agency: _Reader([out_of_scope, in_scope]),
            query_scope=_document_scope(),
        ),
        build=_build(REGULATIONS_GOV_DOCUMENT_PROFILE, _document_scope()),
        destination=release,
    )
    reader = _reader(release, published.artifact.pin, REGULATIONS_GOV_DOCUMENT_PROFILE)
    pages = [json.loads(line) for line in (release / "acquisition/pages.jsonl").read_text().splitlines()]

    assert [row["sourceRecordId"] for row in reader.iter_records()] == [
        "EPA-2026-0001-0001"
    ]
    assert [row["recordsIncluded"] for row in pages] == [False, True, False]
    assert len(list(release.glob("evidence/traversal-000/page-*.json"))) == 3


def test_missing_or_changed_enumerated_object_refuses_complete_snapshot(tmp_path: Path) -> None:
    pages = list(
        iter_regulations_gov_document_pages(
            lambda _agency: _Reader([_document_object()]),
            query_scope=_document_scope(),
        )
    )
    with pytest.raises(RegulationsGovSourceError, match="missing object bytes"):
        SourceNativeReleasePublisher(REGULATIONS_GOV_DOCUMENT_PROFILE).publish(
            pages[:1],
            build=_build(REGULATIONS_GOV_DOCUMENT_PROFILE, _document_scope()),
            destination=tmp_path / "missing",
        )

    changed_request = pages[1].request_key.replace("document-etag", "changed-etag")
    changed_page = RegulationsGovPage(
        traversal_index=0,
        page_index=1,
        window_index=1,
        window_page_index=0,
        request_key=changed_request,
        source_cursor=None,
        response_bytes=pages[1].response_bytes,
    )
    with pytest.raises(RegulationsGovSourceError, match="pinned enumeration"):
        SourceNativeReleasePublisher(REGULATIONS_GOV_DOCUMENT_PROFILE).publish(
            [pages[0], changed_page],
            build=_build(REGULATIONS_GOV_DOCUMENT_PROFILE, _document_scope()),
            destination=tmp_path / "changed",
        )


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update({"newTopLevel": None}), "record fields"),
        (
            lambda value: value["data"]["attributes"].update({"newAttribute": None}),
            "attributes fields",
        ),
        (
            lambda value: value["data"]["attributes"]["fileFormats"][0].update(
                {"newFormatFact": None}
            ),
            "fileFormats.*fields",
        ),
        (
            lambda value: value["included"][0]["attributes"].update(
                {"newAttachmentFact": None}
            ),
            "attachment attributes fields",
        ),
    ],
)
def test_document_schema_drift_fails_closed(mutate, message: str) -> None:
    raw = deepcopy(_document())
    mutate(raw)
    with pytest.raises(RegulationsGovSourceError, match=message):
        classify_document(raw)


def test_strict_ascii_ids_and_keys_make_declared_order_unambiguous() -> None:
    with pytest.raises(RegulationsGovSourceError, match="strict ASCII"):
        classify_document(_document("EPA-2026-0001-000é"))
    invalid = _Object(
        key="raw-data/EPA/EPA-2026/text-1/documents/é.json",
        etag='"etag"',
        version_id=None,
        content=_bytes(_document()),
    )
    with pytest.raises(RegulationsGovSourceError, match="object key"):
        list(
            iter_regulations_gov_document_pages(
                lambda _agency: _Reader([invalid]),
                query_scope=_document_scope(),
            )
        )


@pytest.mark.parametrize(
    ("scope", "validator", "message"),
    [
        (
            {
                "agencies": ["EPA", "EPA"],
                "publishedFrom": "2026-08-24",
                "publishedThrough": "2026-08-24",
            },
            regulations_gov_document_query_scope,
            "sorted, and distinct",
        ),
        (
            {
                "agencies": ["EPA"],
                "modifiedFrom": "2026-08-25",
                "modifiedThrough": "2026-08-24",
            },
            regulations_gov_docket_query_scope,
            "reversed",
        ),
        (
            {
                "agencies": ["EPA"],
                "publishedFrom": "2024-01-01",
                "publishedThrough": "2026-08-24",
            },
            regulations_gov_document_query_scope,
            "date bound",
        ),
    ],
)
def test_query_scopes_are_closed_ascii_and_bounded(scope, validator, message: str) -> None:
    with pytest.raises(RegulationsGovSourceError, match=message):
        validator(scope)
