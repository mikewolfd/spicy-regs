"""Exact Mirrulations evidence for separate Regulations.gov source releases.

Documents and dockets remain independent source collections.  This module
captures the complete S3 listing metadata and each ETag-pinned object's exact
bytes; downstream products may join the two releases by their preserved source
keys.
"""

from __future__ import annotations

import json
import re
from io import BytesIO
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Final, Literal, Protocol, cast
from urllib.parse import parse_qs, urlencode, urlparse
from zipfile import ZIP_DEFLATED, BadZipFile, ZipFile, ZipInfo

from rulespec_artifacts import (
    FramedSection,
    canonical_json_bytes,
    framed_section_digest,
    schema_bundle_digest,
)

DOCUMENT_COLLECTION: Final = "documents"
DOCKET_COLLECTION: Final = "dockets"
COMMENT_COLLECTION: Final = "comments"
SOURCE_SYSTEM_VERSION: Final = "regulations.gov-v4-mirrulations-raw-data"
DOCUMENT_SOURCE_SYSTEM_ID: Final = (
    "urn:spicy-regs:source:regulations-gov-mirrulations:documents"
)
DOCKET_SOURCE_SYSTEM_ID: Final = (
    "urn:spicy-regs:source:regulations-gov-mirrulations:dockets"
)
COMMENT_SOURCE_SYSTEM_ID: Final = (
    "urn:spicy-regs:source:regulations-gov-mirrulations:comments"
)
DOCUMENT_SCOPE_ID: Final = "regulations-gov-documents"
DOCKET_SCOPE_ID: Final = "regulations-gov-dockets"
COMMENT_SCOPE_ID: Final = "regulations-gov-comments"
DOCUMENT_SCHEMA_NAME: Final = "regulations-gov-document-raw"
DOCKET_SCHEMA_NAME: Final = "regulations-gov-docket-raw"
COMMENT_SCHEMA_NAME: Final = "regulations-gov-comment-raw"
SCHEMA_VERSION: Final = "1.0"
DOCUMENT_SCHEMA_PATH: Final = "sources/regulations-gov-document-raw-1.0.schema.json"
DOCKET_SCHEMA_PATH: Final = "sources/regulations-gov-docket-raw-1.0.schema.json"
COMMENT_SCHEMA_PATH: Final = "sources/regulations-gov-comment-raw-1.0.schema.json"
DOCUMENT_SOURCE_SCHEMA_KEY: Final = (
    "schemas/regulations-gov-document-raw-1.0.schema.json"
)
DOCKET_SOURCE_SCHEMA_KEY: Final = "schemas/regulations-gov-docket-raw-1.0.schema.json"
COMMENT_SOURCE_SCHEMA_KEY: Final = "schemas/regulations-gov-comment-raw-1.0.schema.json"
DOCUMENT_RECORD_STEM: Final = "regulations-gov-document"
DOCKET_RECORD_STEM: Final = "regulations-gov-docket"
COMMENT_RECORD_STEM: Final = "regulations-gov-comment"
DOCUMENT_ACQUISITION_POLICY_ID: Final = (
    "urn:spicy-regs:acquisition:mirrulations-document-source-enumeration"
)
DOCKET_ACQUISITION_POLICY_ID: Final = (
    "urn:spicy-regs:acquisition:mirrulations-docket-source-enumeration"
)
COMMENT_ACQUISITION_POLICY_ID: Final = (
    "urn:spicy-regs:acquisition:mirrulations-comment-source-enumeration"
)
ACQUISITION_POLICY_VERSION: Final = "1.0"
MAX_TRAVERSALS: Final = 1
MAX_EVIDENCE_PACK_OBJECTS: Final = 1_000
MAX_EVIDENCE_PACK_RAW_BYTES: Final = 16 * 1024 * 1024
MAX_OBJECT_BYTES: Final = 16 * 1024 * 1024
MAX_QUERY_DAYS: Final = 366
EVIDENCE_PACK_TYPE: Final = "mirrulations-evidence-pack-v1"
EVIDENCE_PACK_MEDIA_TYPE: Final = "application/zip"

_ASCII_ID: Final = re.compile(r"^[A-Za-z0-9._-]+$")
_ASCII_KEY: Final = re.compile(r"^[\x21-\x7e]+$")
_DISPLAY_PROPERTY_FIELDS: Final = frozenset({"label", "name", "tooltip"})
_FILE_FORMAT_FIELDS: Final = frozenset({"fileUrl", "format", "size"})
_LINK_FIELDS: Final = frozenset({"related", "self"})
_RELATIONSHIP_FIELDS: Final = frozenset({"attachments"})
_RELATIONSHIP_VALUE_FIELDS: Final = frozenset({"data", "links"})
_LINKAGE_FIELDS: Final = frozenset({"id", "type"})
_ATTACHMENT_ATTRIBUTE_FIELDS: Final = frozenset(
    {
        "agencyNote",
        "authors",
        "description",
        "docAbstract",
        "docOrder",
        "fileFormats",
        "modifyDate",
        "publication",
        "restrictReason",
        "restrictReasonType",
        "title",
    }
)
_INCLUDED_FIELDS: Final = frozenset(
    {"attributes", "id", "links", "relationships", "type"}
)
_META_FIELDS: Final = frozenset(
    {
        "hasMore",
        "hasNextPage",
        "numberOfElements",
        "pageNumber",
        "pageSize",
        "totalElements",
        "totalPages",
    }
)
DOCUMENT_ATTRIBUTE_FIELDS: Final = frozenset(
    {
        "additionalRins",
        "address1",
        "address2",
        "agencyId",
        "allowLateComments",
        "authorDate",
        "authors",
        "category",
        "cfrPart",
        "city",
        "comment",
        "commentEndDate",
        "commentStartDate",
        "country",
        "displayProperties",
        "docAbstract",
        "docketId",
        "documentType",
        "effectiveDate",
        "email",
        "exhibitLocation",
        "exhibitType",
        "fax",
        "field1",
        "field2",
        "fileFormats",
        "firstName",
        "frDocNum",
        "frVolNum",
        "govAgency",
        "govAgencyType",
        "implementationDate",
        "lastName",
        "legacyId",
        "media",
        "modifyDate",
        "objectId",
        "ombApproval",
        "openForComment",
        "organization",
        "originalDocumentId",
        "pageCount",
        "paperLength",
        "paperWidth",
        "phone",
        "postedDate",
        "postmarkDate",
        "reasonWithdrawn",
        "receiveDate",
        "regWriterInstruction",
        "restrictReason",
        "restrictReasonType",
        "sourceCitation",
        "startEndPage",
        "stateProvinceRegion",
        "subject",
        "submitterRep",
        "submitterRepAddress",
        "submitterRepCityState",
        "subtype",
        "title",
        "topics",
        "trackingNbr",
        "withdrawn",
        "withinCommentPeriod",
        "zip",
    }
)
DOCKET_ATTRIBUTE_FIELDS: Final = frozenset(
    {
        "agencyId",
        "category",
        "displayProperties",
        "dkAbstract",
        "docketType",
        "effectiveDate",
        "field1",
        "field2",
        "generic",
        "keywords",
        "legacyId",
        "modifyDate",
        "objectId",
        "organization",
        "petitionNbr",
        "program",
        "rin",
        "shortTitle",
        "subType",
        "subType2",
        "title",
    }
)
COMMENT_ATTRIBUTE_FIELDS: Final = frozenset(
    {
        "address1",
        "address2",
        "agencyId",
        "category",
        "city",
        "comment",
        "commentOn",
        "commentOnDocumentId",
        "country",
        "displayProperties",
        "docAbstract",
        "docketId",
        "documentType",
        "duplicateComments",
        "email",
        "fax",
        "field1",
        "field2",
        "fileFormats",
        "firstName",
        "govAgency",
        "govAgencyType",
        "lastName",
        "legacyId",
        "modifyDate",
        "objectId",
        "openForComment",
        "organization",
        "originalDocumentId",
        "pageCount",
        "phone",
        "postedDate",
        "postmarkDate",
        "reasonWithdrawn",
        "receiveDate",
        "restrictReason",
        "restrictReasonType",
        "stateProvinceRegion",
        "submitterRep",
        "submitterRepAddress",
        "submitterRepCityState",
        "subtype",
        "title",
        "trackingNbr",
        "withdrawn",
        "zip",
    }
)
_DOCUMENT_BOOLEAN_FIELDS: Final = frozenset(
    {"allowLateComments", "openForComment", "withdrawn", "withinCommentPeriod"}
)
_DOCUMENT_INTEGER_FIELDS: Final = frozenset({"pageCount", "paperLength", "paperWidth"})
_DOCUMENT_TEXT_ARRAY_FIELDS: Final = frozenset({"additionalRins", "authors", "cfrPart"})
_TOPIC_FIELDS: Final = frozenset({"id", "label", "name", "slug"})
_COMMENT_BOOLEAN_FIELDS: Final = frozenset({"openForComment", "withdrawn"})
_COMMENT_INTEGER_FIELDS: Final = frozenset({"duplicateComments"})
_COMMENT_INTEGER_OR_TEXT_FIELDS: Final = frozenset({"pageCount"})


class RegulationsGovSourceError(ValueError):
    """Mirrulations evidence cannot prove the requested source release."""


class MirrulationsObject(Protocol):
    @property
    def key(self) -> str: ...

    @property
    def etag(self) -> str: ...

    @property
    def version_id(self) -> str | None: ...

    @property
    def content(self) -> bytes: ...


class MirrulationsObjectReader(Protocol):
    def iter_source_objects(
        self,
        *,
        max_bytes: int,
    ) -> Iterator[MirrulationsObject]: ...


RegulationsGovRead = Callable[[str], MirrulationsObjectReader]


@dataclass(frozen=True, slots=True)
class RegulationsGovPage:
    """One bounded ZIP pack of exact Mirrulations object bytes."""

    traversal_index: int
    page_index: int
    window_index: int
    window_page_index: int
    request_key: str
    source_cursor: str | None
    response_bytes: bytes

    @property
    def evidence_media_type(self) -> str:
        return EVIDENCE_PACK_MEDIA_TYPE

    def __post_init__(self) -> None:
        if min(
            self.traversal_index,
            self.page_index,
            self.window_index,
            self.window_page_index,
        ) < 0:
            raise RegulationsGovSourceError("Mirrulations page indexes must be non-negative")
        if self.traversal_index != 0:
            raise RegulationsGovSourceError("Mirrulations enumeration has one traversal")
        if self.window_index != self.page_index or self.window_page_index != 0:
            raise RegulationsGovSourceError("Mirrulations evidence pages are explicit windows")
        if self.source_cursor is not None:
            raise RegulationsGovSourceError(
                "Mirrulations source enumeration does not use caller-authored cursors"
            )
        if not self.request_key or not self.response_bytes:
            raise RegulationsGovSourceError("Mirrulations evidence must be nonempty")


@dataclass(frozen=True, slots=True)
class MirrulationsWindow:
    kind: Literal["pack"]
    collection: str
    agency: str
    pack_index: int
    terminal: bool


def _date_scope(
    value: Mapping[str, Any],
    *,
    from_field: str,
    through_field: str,
) -> dict[str, Any]:
    if set(value) != {"agencies", from_field, through_field}:
        raise RegulationsGovSourceError("Regulations.gov query scope fields differ")
    agencies = value.get("agencies")
    if (
        not isinstance(agencies, list)
        or not agencies
        or any(
            not isinstance(agency, str)
            or _ASCII_ID.fullmatch(agency) is None
            for agency in agencies
        )
        or agencies != sorted(set(agencies))
    ):
        raise RegulationsGovSourceError(
            "Regulations.gov agencies must be nonempty, ASCII, sorted, and distinct"
        )
    try:
        start = date.fromisoformat(str(value[from_field]))
        end = date.fromisoformat(str(value[through_field]))
    except ValueError as error:
        raise RegulationsGovSourceError("Regulations.gov query dates are invalid") from error
    if end < start:
        raise RegulationsGovSourceError("Regulations.gov query scope is reversed")
    if (end - start).days >= MAX_QUERY_DAYS:
        raise RegulationsGovSourceError(
            f"Regulations.gov query scope exceeds the {MAX_QUERY_DAYS}-day date bound"
        )
    return {
        "agencies": list(agencies),
        from_field: start.isoformat(),
        through_field: end.isoformat(),
    }


def regulations_gov_document_query_scope(value: Mapping[str, Any]) -> dict[str, Any]:
    return _date_scope(
        value,
        from_field="publishedFrom",
        through_field="publishedThrough",
    )


def regulations_gov_docket_query_scope(value: Mapping[str, Any]) -> dict[str, Any]:
    return _date_scope(
        value,
        from_field="modifiedFrom",
        through_field="modifiedThrough",
    )


def regulations_gov_comment_query_scope(value: Mapping[str, Any]) -> dict[str, Any]:
    return _date_scope(
        value,
        from_field="postedFrom",
        through_field="postedThrough",
    )


def _closed_mapping(
    value: object,
    *,
    allowed: frozenset[str],
    label: str,
    required: frozenset[str] = frozenset(),
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RegulationsGovSourceError(f"{label} must be an object")
    unknown = set(value) - allowed
    missing = required - set(value)
    if unknown:
        raise RegulationsGovSourceError(f"unclassified {label} fields: {sorted(unknown)}")
    if missing:
        raise RegulationsGovSourceError(f"{label} lacks fields: {sorted(missing)}")
    return cast(Mapping[str, Any], value)


def _text_or_null(value: object, label: str) -> None:
    if value is not None and not isinstance(value, str):
        raise RegulationsGovSourceError(f"{label} must be text or null")


def _validate_links(value: object, label: str) -> None:
    if value is None:
        return
    links = _closed_mapping(value, allowed=_LINK_FIELDS, label=label)
    for name, locator in links.items():
        _text_or_null(locator, f"{label}.{name}")


def _validate_file_formats(value: object, label: str) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise RegulationsGovSourceError(f"{label} must be an array or null")
    for index, item in enumerate(value):
        row = _closed_mapping(
            item,
            allowed=_FILE_FORMAT_FIELDS,
            label=f"{label}[{index}]",
            required=frozenset({"fileUrl"}),
        )
        locator = row.get("fileUrl")
        if not isinstance(locator, str) or not locator:
            raise RegulationsGovSourceError(f"{label}[{index}].fileUrl is invalid")
        _text_or_null(row.get("format"), f"{label}[{index}].format")
        size = row.get("size")
        if size is not None and (
            isinstance(size, bool)
            or not isinstance(size, (int, str))
            or isinstance(size, int)
            and size < 0
            or isinstance(size, str)
            and not size.isdigit()
        ):
            raise RegulationsGovSourceError(f"{label}[{index}].size is invalid")


def _validate_display_properties(value: object, label: str) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise RegulationsGovSourceError(f"{label} must be an array or null")
    for index, item in enumerate(value):
        row = _closed_mapping(
            item,
            allowed=_DISPLAY_PROPERTY_FIELDS,
            label=f"{label}[{index}]",
        )
        for name, field_value in row.items():
            _text_or_null(field_value, f"{label}[{index}].{name}")


def _validate_relationships(value: object, label: str) -> None:
    if value is None:
        return
    relationships = _closed_mapping(
        value,
        allowed=_RELATIONSHIP_FIELDS,
        label=label,
    )
    for name, relationship in relationships.items():
        row = _closed_mapping(
            relationship,
            allowed=_RELATIONSHIP_VALUE_FIELDS,
            label=f"{label}.{name}",
        )
        _validate_links(row.get("links"), f"{label}.{name}.links")
        if "data" in row:
            data = row["data"]
            values = data if isinstance(data, list) else [data]
            for index, linkage in enumerate(values):
                if linkage is None:
                    continue
                item = _closed_mapping(
                    linkage,
                    allowed=_LINKAGE_FIELDS,
                    label=f"{label}.{name}.data[{index}]",
                    required=_LINKAGE_FIELDS,
                )
                for field_name, field_value in item.items():
                    if not isinstance(field_value, str) or not field_value:
                        raise RegulationsGovSourceError(
                            f"{label}.{name}.data[{index}].{field_name} is invalid"
                        )


def _validate_included(value: object) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise RegulationsGovSourceError("Regulations.gov included must be an array")
    for index, item in enumerate(value):
        row = _closed_mapping(
            item,
            allowed=_INCLUDED_FIELDS,
            label=f"Regulations.gov included[{index}]",
            required=frozenset({"attributes", "id", "type"}),
        )
        identity = row.get("id")
        if not isinstance(identity, str) or _ASCII_ID.fullmatch(identity) is None:
            raise RegulationsGovSourceError("Regulations.gov attachment id is not strict ASCII")
        if row.get("type") != "attachments":
            raise RegulationsGovSourceError("Regulations.gov included type is unsupported")
        attributes = _closed_mapping(
            row.get("attributes"),
            allowed=_ATTACHMENT_ATTRIBUTE_FIELDS,
            label="Regulations.gov attachment attributes",
        )
        for name in _ATTACHMENT_ATTRIBUTE_FIELDS - {
            "authors",
            "docOrder",
            "fileFormats",
        }:
            _text_or_null(attributes.get(name), f"attachment.{name}")
        authors = attributes.get("authors")
        if authors is not None and (
            not isinstance(authors, list)
            or any(not isinstance(author, str) for author in authors)
        ):
            raise RegulationsGovSourceError(
                "Regulations.gov attachment authors must be a text array or null"
            )
        doc_order = attributes.get("docOrder")
        if doc_order is not None and (
            isinstance(doc_order, bool) or not isinstance(doc_order, int)
        ):
            raise RegulationsGovSourceError(
                "Regulations.gov attachment docOrder must be an integer or null"
            )
        _validate_file_formats(attributes.get("fileFormats"), "attachment.fileFormats")
        _validate_links(row.get("links"), "attachment.links")
        _validate_relationships(row.get("relationships"), "attachment.relationships")


def _source_data(value: object, *, collection: str) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    top = _closed_mapping(
        value,
        allowed=frozenset({"data", "included", "meta"}),
        label=f"Regulations.gov {collection} record",
        required=frozenset({"data"}),
    )
    data = _closed_mapping(
        top["data"],
        allowed=frozenset({"attributes", "id", "links", "relationships", "type"}),
        label=f"Regulations.gov {collection} data",
        required=frozenset({"attributes", "id", "type"}),
    )
    identity = data.get("id")
    if not isinstance(identity, str) or _ASCII_ID.fullmatch(identity) is None:
        raise RegulationsGovSourceError(
            f"Regulations.gov {collection} id must use strict ASCII"
        )
    if data.get("type") != collection:
        raise RegulationsGovSourceError(f"Regulations.gov {collection} type differs")
    _validate_links(data.get("links"), f"Regulations.gov {collection} links")
    _validate_relationships(
        data.get("relationships"),
        f"Regulations.gov {collection} relationships",
    )
    _validate_included(top.get("included"))
    if "meta" in top:
        meta = _closed_mapping(
            top["meta"],
            allowed=_META_FIELDS,
            label="Regulations.gov meta",
        )
        canonical_json_bytes(meta)
    return top, data


def _validate_document_attributes(attributes: Mapping[str, Any]) -> None:
    unknown = set(attributes) - DOCUMENT_ATTRIBUTE_FIELDS
    if unknown:
        raise RegulationsGovSourceError(
            f"unclassified Regulations.gov document attribute fields: {sorted(unknown)}"
        )
    for name in _DOCUMENT_BOOLEAN_FIELDS:
        value = attributes.get(name)
        if value is not None and not isinstance(value, bool):
            raise RegulationsGovSourceError(f"document attribute {name} must be boolean or null")
    for name in _DOCUMENT_INTEGER_FIELDS:
        value = attributes.get(name)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
            raise RegulationsGovSourceError(f"document attribute {name} must be integer or null")
    for name in _DOCUMENT_TEXT_ARRAY_FIELDS:
        value = attributes.get(name)
        if value is not None and (
            not isinstance(value, list)
            or any(not isinstance(item, str) for item in value)
        ):
            raise RegulationsGovSourceError(f"document attribute {name} must be a text array or null")
    for name in DOCUMENT_ATTRIBUTE_FIELDS - (
        _DOCUMENT_BOOLEAN_FIELDS
        | _DOCUMENT_INTEGER_FIELDS
        | _DOCUMENT_TEXT_ARRAY_FIELDS
        | {"displayProperties", "fileFormats", "topics"}
    ):
        _text_or_null(attributes.get(name), f"document attribute {name}")
    _validate_display_properties(attributes.get("displayProperties"), "document.displayProperties")
    _validate_file_formats(attributes.get("fileFormats"), "document.fileFormats")
    topics = attributes.get("topics")
    if topics is not None:
        if not isinstance(topics, list):
            raise RegulationsGovSourceError("document topics must be an array or null")
        for index, topic in enumerate(topics):
            if isinstance(topic, str):
                continue
            row = _closed_mapping(
                topic,
                allowed=_TOPIC_FIELDS,
                label=f"document topics[{index}]",
            )
            for name, item in row.items():
                _text_or_null(item, f"document topics[{index}].{name}")


def _validate_docket_attributes(attributes: Mapping[str, Any]) -> None:
    unknown = set(attributes) - DOCKET_ATTRIBUTE_FIELDS
    if unknown:
        raise RegulationsGovSourceError(
            f"unclassified Regulations.gov docket attribute fields: {sorted(unknown)}"
        )
    for name in DOCKET_ATTRIBUTE_FIELDS - {"displayProperties", "keywords"}:
        _text_or_null(attributes.get(name), f"docket attribute {name}")
    _validate_display_properties(attributes.get("displayProperties"), "docket.displayProperties")
    keywords = attributes.get("keywords")
    if keywords is not None and (
        not isinstance(keywords, list)
        or any(not isinstance(item, str) for item in keywords)
    ):
        raise RegulationsGovSourceError("docket keywords must be a text array or null")


def _validate_comment_attributes(attributes: Mapping[str, Any]) -> None:
    unknown = set(attributes) - COMMENT_ATTRIBUTE_FIELDS
    if unknown:
        raise RegulationsGovSourceError(
            f"unclassified Regulations.gov comment attribute fields: {sorted(unknown)}"
        )
    for name in _COMMENT_BOOLEAN_FIELDS:
        value = attributes.get(name)
        if value is not None and not isinstance(value, bool):
            raise RegulationsGovSourceError(
                f"comment attribute {name} must be boolean or null"
            )
    for name in _COMMENT_INTEGER_FIELDS:
        value = attributes.get(name)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, int)
        ):
            raise RegulationsGovSourceError(
                f"comment attribute {name} must be integer or null"
            )
    for name in _COMMENT_INTEGER_OR_TEXT_FIELDS:
        value = attributes.get(name)
        if value is not None and (
            isinstance(value, bool)
            or not isinstance(value, (int, str))
            or isinstance(value, int)
            and value < 0
            or isinstance(value, str)
            and not value.isdigit()
        ):
            raise RegulationsGovSourceError(
                f"comment attribute {name} must be a non-negative integer, digits, or null"
            )
    for name in COMMENT_ATTRIBUTE_FIELDS - (
        _COMMENT_BOOLEAN_FIELDS
        | _COMMENT_INTEGER_FIELDS
        | _COMMENT_INTEGER_OR_TEXT_FIELDS
        | {"displayProperties", "fileFormats"}
    ):
        _text_or_null(attributes.get(name), f"comment attribute {name}")
    _validate_display_properties(
        attributes.get("displayProperties"),
        "comment.displayProperties",
    )
    _validate_file_formats(attributes.get("fileFormats"), "comment.fileFormats")


def classify_document(value: object) -> dict[str, Any]:
    top, data = _source_data(value, collection=DOCUMENT_COLLECTION)
    attributes = _closed_mapping(
        data["attributes"],
        allowed=DOCUMENT_ATTRIBUTE_FIELDS,
        label="Regulations.gov document attributes",
        required=frozenset({"agencyId", "postedDate"}),
    )
    _validate_document_attributes(attributes)
    _record_date(attributes.get("postedDate"), "document postedDate")
    canonical_json_bytes(top)
    return dict(top)


def classify_docket(value: object) -> dict[str, Any]:
    top, data = _source_data(value, collection=DOCKET_COLLECTION)
    attributes = _closed_mapping(
        data["attributes"],
        allowed=DOCKET_ATTRIBUTE_FIELDS,
        label="Regulations.gov docket attributes",
        required=frozenset({"agencyId", "modifyDate"}),
    )
    _validate_docket_attributes(attributes)
    _record_date(attributes.get("modifyDate"), "docket modifyDate")
    canonical_json_bytes(top)
    return dict(top)


def classify_comment(value: object) -> dict[str, Any]:
    top, data = _source_data(value, collection=COMMENT_COLLECTION)
    attributes = _closed_mapping(
        data["attributes"],
        allowed=COMMENT_ATTRIBUTE_FIELDS,
        label="Regulations.gov comment attributes",
        required=frozenset({"agencyId", "postedDate"}),
    )
    _validate_comment_attributes(attributes)
    _record_date(attributes.get("postedDate"), "comment postedDate")
    comment_observation_version(dict(top))
    canonical_json_bytes(top)
    return dict(top)


def _data_attributes(record: Mapping[str, Any]) -> Mapping[str, Any]:
    return cast(Mapping[str, Any], cast(Mapping[str, Any], record["data"])["attributes"])


def source_record_id(record: Mapping[str, Any]) -> str:
    return str(cast(Mapping[str, Any], record["data"])["id"])


def _record_date(value: object, label: str) -> date:
    if not isinstance(value, str) or len(value) < 10:
        raise RegulationsGovSourceError(f"Regulations.gov {label} is invalid")
    try:
        parsed = date.fromisoformat(value[:10])
    except ValueError as error:
        raise RegulationsGovSourceError(f"Regulations.gov {label} is invalid") from error
    if parsed.isoformat() != value[:10]:
        raise RegulationsGovSourceError(f"Regulations.gov {label} is not canonical")
    return parsed


def comment_source_issued_version(record: Mapping[str, Any]) -> str | None:
    """Return the exact source value; null is a valid public observation."""

    value = _data_attributes(record).get("modifyDate")
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise RegulationsGovSourceError(
            "Regulations.gov comment modifyDate must be nonempty text or null"
        )
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise RegulationsGovSourceError(
            "Regulations.gov comment modifyDate is invalid"
        ) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RegulationsGovSourceError(
            "Regulations.gov comment modifyDate lacks a UTC offset"
        )
    return value


def comment_observation_version(record: Mapping[str, Any]) -> str | None:
    """Normalize the exact source instant only for deterministic comparison."""

    value = comment_source_issued_version(record)
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.astimezone(UTC).isoformat(timespec="microseconds").replace(
        "+00:00",
        "Z",
    )


def source_issued_version(record: Mapping[str, Any], *, collection: str) -> str:
    attributes = _data_attributes(record)
    fields = ("modifyDate", "postedDate") if collection == DOCUMENT_COLLECTION else ("modifyDate",)
    for name in fields:
        value = attributes.get(name)
        if isinstance(value, str) and value:
            return value
    raise RegulationsGovSourceError(f"Regulations.gov {collection} lacks a source-issued version")


def _source_record(
    record: Mapping[str, Any],
    *,
    schema_digest: str,
    schema_name: str,
    scope_id: str,
) -> dict[str, Any]:
    return {
        "fieldDiagnostics": [],
        "record": dict(record),
        "schemaDigest": schema_digest,
        "schemaName": schema_name,
        "schemaVersion": SCHEMA_VERSION,
        "scopeId": scope_id,
        "sourceRecordId": source_record_id(record),
    }


def document_source_record(
    record: Mapping[str, Any],
    *,
    schema_digest: str,
) -> dict[str, Any]:
    return _source_record(
        record,
        schema_digest=schema_digest,
        schema_name=DOCUMENT_SCHEMA_NAME,
        scope_id=DOCUMENT_SCOPE_ID,
    )


def docket_source_record(
    record: Mapping[str, Any],
    *,
    schema_digest: str,
) -> dict[str, Any]:
    return _source_record(
        record,
        schema_digest=schema_digest,
        schema_name=DOCKET_SCHEMA_NAME,
        scope_id=DOCKET_SCOPE_ID,
    )


def comment_source_record(
    record: Mapping[str, Any],
    *,
    schema_digest: str,
) -> dict[str, Any]:
    return _source_record(
        record,
        schema_digest=schema_digest,
        schema_name=COMMENT_SCHEMA_NAME,
        scope_id=COMMENT_SCOPE_ID,
    )


def _media_type(value: object, locator: str) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if "/" in normalized:
            return normalized
        aliases = {
            "doc": "application/msword",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "htm": "text/html",
            "html": "text/html",
            "pdf": "application/pdf",
            "txt": "text/plain",
            "xml": "application/xml",
        }
        if normalized in aliases:
            return aliases[normalized]
    suffix = locator.rsplit("?", 1)[0].rsplit(".", 1)[-1].lower()
    if suffix and suffix != locator.lower():
        return _media_type(suffix, "")
    return "application/octet-stream"


def _rendition(
    *,
    source_record_id_value: str,
    source_field: str,
    rendition_id: str,
    value: Mapping[str, Any],
) -> dict[str, Any]:
    locator = value.get("fileUrl")
    if not isinstance(locator, str) or not locator:
        raise RegulationsGovSourceError(f"Regulations.gov {source_field} lacks fileUrl")
    size = value.get("size")
    expected_size = int(size) if isinstance(size, str) and size.isdigit() else size
    if expected_size is not None and (
        isinstance(expected_size, bool)
        or not isinstance(expected_size, int)
        or expected_size < 0
    ):
        raise RegulationsGovSourceError(f"Regulations.gov {source_field} size is invalid")
    return {
        "expectedByteSize": expected_size,
        "expectedSha256": None,
        "locator": locator,
        "mediaType": _media_type(value.get("format"), locator),
        "renditionId": rendition_id,
        "sourceField": source_field,
        "sourceRecordId": source_record_id_value,
    }


def _rendition_rows(
    record: Mapping[str, Any],
    *,
    direct_prefix: str,
) -> tuple[dict[str, Any], ...]:
    identity = source_record_id(record)
    rows: list[dict[str, Any]] = []
    formats = _data_attributes(record).get("fileFormats") or []
    for index, value in enumerate(cast(Sequence[Mapping[str, Any]], formats)):
        rows.append(
            _rendition(
                source_record_id_value=identity,
                source_field=f"data.attributes.fileFormats[{index}]",
                rendition_id=f"{direct_prefix}-{index:04d}",
                value=value,
            )
        )
    for included_index, included in enumerate(
        cast(Sequence[Mapping[str, Any]], record.get("included") or [])
    ):
        attributes = cast(Mapping[str, Any], included["attributes"])
        for format_index, value in enumerate(
            cast(Sequence[Mapping[str, Any]], attributes.get("fileFormats") or [])
        ):
            rows.append(
                _rendition(
                    source_record_id_value=identity,
                    source_field=(
                        f"included[{included_index}].attributes.fileFormats[{format_index}]"
                    ),
                    rendition_id=f"attachment-{included_index:04d}-{format_index:04d}",
                    value=value,
                )
            )
    return tuple(rows)


def document_rendition_rows(record: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    return _rendition_rows(record, direct_prefix="document")


def comment_rendition_rows(record: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    return _rendition_rows(record, direct_prefix="comment")


def docket_rendition_rows(record: Mapping[str, Any]) -> tuple[()]:
    del record
    return ()


def _decode_json(raw: bytes) -> object:
    def duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise RegulationsGovSourceError(f"Regulations.gov JSON repeats field {key!r}")
            result[key] = value
        return result

    def unsupported_float(value: str) -> None:
        raise RegulationsGovSourceError(
            f"Regulations.gov JSON contains unsupported float {value!r}"
        )

    try:
        return json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=duplicate_keys,
            parse_float=unsupported_float,
            parse_constant=unsupported_float,
        )
    except RegulationsGovSourceError:
        raise
    except (UnicodeError, json.JSONDecodeError) as error:
        raise RegulationsGovSourceError(f"invalid Regulations.gov JSON: {error}") from error


def _enumeration_entry(value: object) -> dict[str, Any]:
    row = _closed_mapping(
        value,
        allowed=frozenset({"byteSize", "etag", "key", "versionId"}),
        label="Mirrulations enumeration entry",
        required=frozenset({"byteSize", "etag", "key", "versionId"}),
    )
    key = row.get("key")
    etag = row.get("etag")
    version_id = row.get("versionId")
    byte_size = row.get("byteSize")
    if not isinstance(key, str) or _ASCII_KEY.fullmatch(key) is None:
        raise RegulationsGovSourceError("Mirrulations enumeration key is not strict ASCII")
    if not isinstance(etag, str) or not etag:
        raise RegulationsGovSourceError("Mirrulations enumeration ETag is invalid")
    if version_id is not None and (not isinstance(version_id, str) or not version_id):
        raise RegulationsGovSourceError("Mirrulations enumeration versionId is invalid")
    if isinstance(byte_size, bool) or not isinstance(byte_size, int) or byte_size < 1:
        raise RegulationsGovSourceError("Mirrulations enumeration byteSize is invalid")
    return {
        "byteSize": byte_size,
        "etag": etag,
        "key": key,
        "versionId": version_id,
    }


@dataclass(frozen=True, slots=True)
class _PackedObject:
    key: str
    etag: str
    version_id: str | None
    content: bytes
    included: bool


def _zip_entry(name: str) -> ZipInfo:
    entry = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    entry.compress_type = ZIP_DEFLATED
    entry.external_attr = 0o100644 << 16
    return entry


def _pack_bytes(
    objects: Sequence[_PackedObject],
    *,
    collection: str,
    agency: str,
    pack_index: int,
    terminal: bool,
) -> bytes:
    if len(objects) > MAX_EVIDENCE_PACK_OBJECTS:
        raise RegulationsGovSourceError("Mirrulations evidence pack has too many objects")
    if sum(len(item.content) for item in objects) > MAX_EVIDENCE_PACK_RAW_BYTES:
        raise RegulationsGovSourceError("Mirrulations evidence pack exceeds its raw-byte bound")
    manifest_objects = [
        {
            "byteSize": len(item.content),
            "entry": f"objects/{index:06d}.json",
            "etag": item.etag,
            "included": item.included,
            "key": item.key,
            "versionId": item.version_id,
        }
        for index, item in enumerate(objects)
    ]
    manifest = canonical_json_bytes(
        {
            "agency": agency,
            "collection": collection,
            "evidenceType": EVIDENCE_PACK_TYPE,
            "objects": manifest_objects,
            "packIndex": pack_index,
            "terminal": terminal,
        }
    )
    output = BytesIO()
    with ZipFile(output, "w", compression=ZIP_DEFLATED, compresslevel=6) as archive:
        archive.writestr(_zip_entry("manifest.json"), manifest)
        for item, descriptor in zip(objects, manifest_objects, strict=True):
            archive.writestr(_zip_entry(str(descriptor["entry"])), item.content)
    return output.getvalue()


def _parse_page_response(
    raw: bytes,
    *,
    collection: str,
    classifier: Callable[[object], Mapping[str, Any]],
) -> Mapping[str, Any]:
    try:
        archive = ZipFile(BytesIO(raw), "r")
    except BadZipFile as error:
        raise RegulationsGovSourceError("Mirrulations evidence pack is not a ZIP file") from error
    with archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        if not names or names[0] != "manifest.json" or len(names) != len(set(names)):
            raise RegulationsGovSourceError("Mirrulations evidence pack membership differs")
        manifest_info = infos[0]
        if manifest_info.file_size > MAX_OBJECT_BYTES:
            raise RegulationsGovSourceError("Mirrulations evidence manifest exceeds its bound")
        manifest = _closed_mapping(
            _decode_json(archive.read(manifest_info)),
            allowed=frozenset(
                {"agency", "collection", "evidenceType", "objects", "packIndex", "terminal"}
            ),
            required=frozenset(
                {"agency", "collection", "evidenceType", "objects", "packIndex", "terminal"}
            ),
            label="Mirrulations evidence-pack manifest",
        )
        agency = manifest.get("agency")
        pack_index = manifest.get("packIndex")
        terminal = manifest.get("terminal")
        if (
            manifest.get("collection") != collection
            or manifest.get("evidenceType") != EVIDENCE_PACK_TYPE
            or not isinstance(agency, str)
            or _ASCII_ID.fullmatch(agency) is None
            or isinstance(pack_index, bool)
            or not isinstance(pack_index, int)
            or pack_index < 0
            or not isinstance(terminal, bool)
        ):
            raise RegulationsGovSourceError("Mirrulations evidence-pack identity differs")
        values = manifest.get("objects")
        if not isinstance(values, list) or len(values) > MAX_EVIDENCE_PACK_OBJECTS:
            raise RegulationsGovSourceError("Mirrulations evidence-pack objects differ")
        expected_names = ["manifest.json"]
        entries: list[dict[str, Any]] = []
        packed_records: list[dict[str, Any]] = []
        raw_byte_count = 0
        for index, value in enumerate(values):
            row = _closed_mapping(
                value,
                allowed=frozenset(
                    {"byteSize", "entry", "etag", "included", "key", "versionId"}
                ),
                required=frozenset(
                    {"byteSize", "entry", "etag", "included", "key", "versionId"}
                ),
                label="Mirrulations evidence-pack object",
            )
            entry = row.get("entry")
            included = row.get("included")
            expected_entry = f"objects/{index:06d}.json"
            if entry != expected_entry or not isinstance(included, bool):
                raise RegulationsGovSourceError("Mirrulations evidence-pack object fields differ")
            metadata = _enumeration_entry(
                {
                    "byteSize": row.get("byteSize"),
                    "etag": row.get("etag"),
                    "key": row.get("key"),
                    "versionId": row.get("versionId"),
                }
            )
            expected_names.append(expected_entry)
            info = infos[index + 1] if index + 1 < len(infos) else None
            if info is None or info.filename != expected_entry or info.file_size != metadata["byteSize"]:
                raise RegulationsGovSourceError("Mirrulations evidence-pack object size differs")
            raw_byte_count += info.file_size
            if raw_byte_count > MAX_EVIDENCE_PACK_RAW_BYTES:
                raise RegulationsGovSourceError("Mirrulations evidence pack exceeds its raw-byte bound")
            content = archive.read(info)
            if len(content) != metadata["byteSize"]:
                raise RegulationsGovSourceError("Mirrulations evidence-pack object is truncated")
            record = dict(classifier(_decode_json(content)))
            if _data_attributes(record).get("agencyId") != agency:
                raise RegulationsGovSourceError(
                    "Mirrulations evidence-pack record agency differs"
                )
            entries.append({"agency": agency, **metadata})
            packed_records.append({"included": included, "record": record})
        if names != expected_names:
            raise RegulationsGovSourceError("Mirrulations evidence pack has extra or reordered members")
    results = [item["record"] for item in packed_records if item["included"]]
    return {
        "_agency": agency,
        "_collection": collection,
        "_evidenceType": EVIDENCE_PACK_TYPE,
        "_objects": entries,
        "_packIndex": pack_index,
        "_packedRecords": packed_records,
        "_terminal": terminal,
        "count": len(results),
        "next_page_url": None,
        "results": results,
        "total_pages": 1,
    }


def parse_document_page_response(raw: bytes) -> Mapping[str, Any]:
    return _parse_page_response(raw, collection=DOCUMENT_COLLECTION, classifier=classify_document)


def parse_docket_page_response(raw: bytes) -> Mapping[str, Any]:
    return _parse_page_response(raw, collection=DOCKET_COLLECTION, classifier=classify_docket)


def parse_comment_page_response(raw: bytes) -> Mapping[str, Any]:
    return _parse_page_response(raw, collection=COMMENT_COLLECTION, classifier=classify_comment)


def regulations_gov_next_page_url(
    response: Mapping[str, Any],
    *,
    seen_urls: set[str],
) -> None:
    del seen_urls
    if response.get("next_page_url") is not None:
        raise RegulationsGovSourceError("Mirrulations exact-object evidence cannot paginate")
    return None


@dataclass(slots=True)
class RegulationsGovTraversalCheck:
    observed_pages: int = 0

    def add(self, response: Mapping[str, Any], *, page_index: int) -> None:
        results = response.get("results")
        if (
            page_index != 0
            or not isinstance(results, list)
            or not 0 <= len(results) <= MAX_EVIDENCE_PACK_OBJECTS
            or response.get("count") != len(results)
        ):
            raise RegulationsGovSourceError("Mirrulations evidence-pack inventory differs")
        self.observed_pages += 1

    def finish(self) -> None:
        if self.observed_pages != 1:
            raise RegulationsGovSourceError("Mirrulations object window is incomplete")


def _pack_request(
    *,
    collection: str,
    agency: str,
    pack_index: int,
    terminal: bool,
) -> str:
    query = urlencode(
        {
            "agency": agency,
            "packIndex": str(pack_index),
            "terminal": "true" if terminal else "false",
        }
    )
    return f"mirrulations://regulations-gov/{collection}/pack?{query}"


def parse_mirrulations_request(value: str) -> MirrulationsWindow:
    parsed = urlparse(value)
    path = parsed.path.strip("/").split("/")
    if (
        parsed.scheme != "mirrulations"
        or parsed.netloc != "regulations-gov"
        or len(path) != 2
        or path[0]
        not in {COMMENT_COLLECTION, DOCUMENT_COLLECTION, DOCKET_COLLECTION}
        or path[1] != "pack"
        or parsed.fragment
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise RegulationsGovSourceError("Mirrulations request key is invalid")
    collection, _ = path
    parameters = parse_qs(parsed.query, keep_blank_values=True)
    agency_values = parameters.get("agency", [])
    if len(agency_values) != 1 or _ASCII_ID.fullmatch(agency_values[0]) is None:
        raise RegulationsGovSourceError("Mirrulations request agency is invalid")
    agency = agency_values[0]
    if set(parameters) != {"agency", "packIndex", "terminal"} or any(
        len(values) != 1 for values in parameters.values()
    ):
        raise RegulationsGovSourceError("Mirrulations pack request fields differ")
    pack_index_text = parameters["packIndex"][0]
    terminal_text = parameters["terminal"][0]
    if (
        not pack_index_text.isdigit()
        or terminal_text not in {"false", "true"}
    ):
        raise RegulationsGovSourceError("Mirrulations pack request values are invalid")
    pack_index = int(pack_index_text)
    terminal = terminal_text == "true"
    if _pack_request(
        collection=collection,
        agency=agency,
        pack_index=pack_index,
        terminal=terminal,
    ) != value:
        raise RegulationsGovSourceError("Mirrulations pack request is not canonical")
    return MirrulationsWindow(
        kind="pack",
        collection=collection,
        agency=agency,
        pack_index=pack_index,
        terminal=terminal,
    )


@dataclass(slots=True)
class MirrulationsAcquisitionCheck:
    """Validate one complete, bounded, strictly ordered source enumeration."""

    collection: str
    observed_agencies: list[str] = field(default_factory=list)
    observed_objects: int = 0
    current_agency: str | None = None
    next_pack_index: int = 0
    current_terminal: bool = True
    previous_key: str | None = None

    def add_window(
        self,
        response: Mapping[str, Any],
        *,
        page_window: object | None,
        records_included: bool,
        response_bytes: bytes,
    ) -> None:
        if not isinstance(page_window, MirrulationsWindow) or page_window.collection != self.collection:
            raise RegulationsGovSourceError("Mirrulations evidence request collection differs")
        del response_bytes
        if (
            response.get("_evidenceType") != EVIDENCE_PACK_TYPE
            or response.get("_collection") != self.collection
            or response.get("_agency") != page_window.agency
            or response.get("_packIndex") != page_window.pack_index
            or response.get("_terminal") is not page_window.terminal
            or records_included is not bool(response.get("results"))
        ):
            raise RegulationsGovSourceError("Mirrulations pack request and bytes differ")
        agency = page_window.agency
        if agency != self.current_agency:
            if self.current_agency is not None and not self.current_terminal:
                raise RegulationsGovSourceError("Mirrulations agency enumeration lacks a terminal pack")
            if self.observed_agencies and agency <= self.observed_agencies[-1]:
                raise RegulationsGovSourceError("Mirrulations enumeration agencies are not ASCII-sorted")
            if page_window.pack_index != 0:
                raise RegulationsGovSourceError("Mirrulations agency enumeration does not start at pack zero")
            self.observed_agencies.append(agency)
            self.current_agency = agency
            self.next_pack_index = 0
        elif self.current_terminal:
            raise RegulationsGovSourceError("Mirrulations agency enumeration continues after its terminal pack")
        if page_window.pack_index != self.next_pack_index:
            raise RegulationsGovSourceError("Mirrulations evidence packs are missing or reordered")
        entries = response.get("_objects")
        if not isinstance(entries, list):
            raise RegulationsGovSourceError("Mirrulations pack object inventory differs")
        for entry in entries:
            if not isinstance(entry, Mapping):
                raise RegulationsGovSourceError("Mirrulations pack object inventory differs")
            key = entry.get("key")
            if (
                not isinstance(key, str)
                or not key.startswith(f"raw-data/{agency}/")
                or self.previous_key is not None
                and key <= self.previous_key
            ):
                raise RegulationsGovSourceError(
                    "Mirrulations object keys are not globally sorted and distinct"
                )
            self.previous_key = key
            self.observed_objects += 1
        self.next_pack_index += 1
        self.current_terminal = page_window.terminal

    def finish(self, *, query_scope: Mapping[str, Any]) -> None:
        agencies = query_scope.get("agencies")
        if self.observed_agencies != agencies:
            raise RegulationsGovSourceError("Mirrulations enumeration does not cover exact agencies")
        if not self.current_terminal:
            raise RegulationsGovSourceError("Mirrulations enumeration is missing a terminal pack")


def _records_included(
    response: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
    collection: str,
) -> bool:
    if not isinstance(page_window, MirrulationsWindow) or page_window.collection != collection:
        raise RegulationsGovSourceError("Mirrulations page lacks a validated request")
    if (
        response.get("_evidenceType") != EVIDENCE_PACK_TYPE
        or response.get("_agency") != page_window.agency
        or response.get("_packIndex") != page_window.pack_index
        or response.get("_terminal") is not page_window.terminal
    ):
        raise RegulationsGovSourceError("Mirrulations pack request differs from its evidence")
    packed_records = response.get("_packedRecords")
    results = response.get("results")
    if not isinstance(packed_records, list) or not isinstance(results, list):
        raise RegulationsGovSourceError("Mirrulations evidence pack has no record inventory")
    expected_results: list[Mapping[str, Any]] = []
    for item in packed_records:
        if not isinstance(item, Mapping) or not isinstance(item.get("included"), bool):
            raise RegulationsGovSourceError("Mirrulations evidence-pack disposition differs")
        record = item.get("record")
        if not isinstance(record, Mapping):
            raise RegulationsGovSourceError("Mirrulations evidence pack has an invalid record")
        included = _record_in_scope(
            record,
            query_scope=query_scope,
            agency=page_window.agency,
            collection=collection,
        )
        if item["included"] is not included:
            raise RegulationsGovSourceError(
                "Mirrulations evidence-pack disposition differs from the query scope"
            )
        if included:
            expected_results.append(record)
    if results != expected_results:
        raise RegulationsGovSourceError("Mirrulations evidence-pack result inventory differs")
    return bool(expected_results)


def _record_in_scope(
    record: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    agency: str,
    collection: str,
) -> bool:
    attributes = _data_attributes(record)
    record_agency = attributes.get("agencyId")
    if record_agency != agency:
        raise RegulationsGovSourceError("Mirrulations record agency differs from its object path")
    if record_agency not in query_scope["agencies"]:
        return False
    if collection == DOCUMENT_COLLECTION:
        value = _record_date(attributes.get("postedDate"), "document postedDate")
        start = date.fromisoformat(str(query_scope["publishedFrom"]))
        end = date.fromisoformat(str(query_scope["publishedThrough"]))
    elif collection == DOCKET_COLLECTION:
        value = _record_date(attributes.get("modifyDate"), "docket modifyDate")
        start = date.fromisoformat(str(query_scope["modifiedFrom"]))
        end = date.fromisoformat(str(query_scope["modifiedThrough"]))
    else:
        value = _record_date(attributes.get("postedDate"), "comment postedDate")
        start = date.fromisoformat(str(query_scope["postedFrom"]))
        end = date.fromisoformat(str(query_scope["postedThrough"]))
    return start <= value <= end


def document_records_included(
    response: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> bool:
    return _records_included(
        response,
        query_scope=query_scope,
        page_window=page_window,
        collection=DOCUMENT_COLLECTION,
    )


def docket_records_included(
    response: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> bool:
    return _records_included(
        response,
        query_scope=query_scope,
        page_window=page_window,
        collection=DOCKET_COLLECTION,
    )


def comment_records_included(
    response: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> bool:
    return _records_included(
        response,
        query_scope=query_scope,
        page_window=page_window,
        collection=COMMENT_COLLECTION,
    )


def _validate_record_scope(
    record: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
    collection: str,
) -> None:
    if not isinstance(page_window, MirrulationsWindow) or page_window.collection != collection:
        raise RegulationsGovSourceError("Mirrulations page lacks a validated request")
    if not _record_in_scope(
        record,
        query_scope=query_scope,
        agency=page_window.agency,
        collection=collection,
    ):
        raise RegulationsGovSourceError("Regulations.gov record falls outside its query scope")


def validate_document_record_scope(
    record: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> None:
    _validate_record_scope(
        record,
        query_scope=query_scope,
        page_window=page_window,
        collection=DOCUMENT_COLLECTION,
    )


def validate_docket_record_scope(
    record: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> None:
    _validate_record_scope(
        record,
        query_scope=query_scope,
        page_window=page_window,
        collection=DOCKET_COLLECTION,
    )


def validate_comment_record_scope(
    record: Mapping[str, Any],
    *,
    query_scope: Mapping[str, Any],
    page_window: object | None,
) -> None:
    _validate_record_scope(
        record,
        query_scope=query_scope,
        page_window=page_window,
        collection=COMMENT_COLLECTION,
    )


def _acquisition_policy(
    query_scope: Mapping[str, Any],
    *,
    validator: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    collection: str,
) -> dict[str, Any]:
    return {
        "collection": collection,
        "dateSelection": "after-full-agency-object-acquisition",
        "evidence": "bounded-zip-packs-of-listed-metadata-and-exact-object-bytes",
        "initialQueryScope": dict(validator(query_scope)),
        "maxObjectsPerEvidencePack": MAX_EVIDENCE_PACK_OBJECTS,
        "maxQueryDays": MAX_QUERY_DAYS,
        "maxRawBytesPerEvidencePack": MAX_EVIDENCE_PACK_RAW_BYTES,
        "maxObjectBytes": MAX_OBJECT_BYTES,
        "maxTraversals": MAX_TRAVERSALS,
        "strategy": "complete-mirrulations-source-enumeration",
    }


def document_acquisition_policy(query_scope: Mapping[str, Any]) -> dict[str, Any]:
    return _acquisition_policy(
        query_scope,
        validator=regulations_gov_document_query_scope,
        collection=DOCUMENT_COLLECTION,
    )


def docket_acquisition_policy(query_scope: Mapping[str, Any]) -> dict[str, Any]:
    return _acquisition_policy(
        query_scope,
        validator=regulations_gov_docket_query_scope,
        collection=DOCKET_COLLECTION,
    )


def comment_acquisition_policy(query_scope: Mapping[str, Any]) -> dict[str, Any]:
    policy = _acquisition_policy(
        query_scope,
        validator=regulations_gov_comment_query_scope,
        collection=COMMENT_COLLECTION,
    )
    policy["observationSelection"] = {
        "groupBy": "/data/id",
        "orderBy": "/data/attributes/modifyDate DESC NULLS LAST",
        "tieDisposition": "refuse-repeated-normalized-instant",
    }
    return policy


def _iter_pages(
    read: RegulationsGovRead,
    *,
    query_scope: Mapping[str, Any],
    collection: str,
    scope_validator: Callable[[Mapping[str, Any]], Mapping[str, Any]],
    classifier: Callable[[object], Mapping[str, Any]],
    scratch_directory: Path | None,
) -> Iterator[RegulationsGovPage]:
    scope = scope_validator(query_scope)
    path_collection = {
        COMMENT_COLLECTION: COMMENT_COLLECTION,
        DOCUMENT_COLLECTION: DOCUMENT_COLLECTION,
        DOCKET_COLLECTION: "docket",
    }[collection]
    del scratch_directory
    page_index = 0
    previous_global_key: str | None = None
    for agency in cast(Sequence[str], scope["agencies"]):
        pack: list[_PackedObject] = []
        pack_raw_bytes = 0
        pack_index = 0
        reader = read(agency)
        for source_object in reader.iter_source_objects(max_bytes=MAX_OBJECT_BYTES):
            key = source_object.key
            etag = source_object.etag
            version_id = source_object.version_id
            content = source_object.content
            if (
                not isinstance(key, str)
                or _ASCII_KEY.fullmatch(key) is None
                or not key.startswith(f"raw-data/{agency}/")
                or f"/{path_collection}/" not in key
                or previous_global_key is not None
                and key <= previous_global_key
            ):
                raise RegulationsGovSourceError(
                    "Mirrulations object keys must be globally sorted, distinct, and match agency/collection"
                )
            if not isinstance(etag, str) or not etag:
                raise RegulationsGovSourceError(f"Mirrulations object {key} lacks a source ETag")
            if version_id is not None and (
                not isinstance(version_id, str) or not version_id
            ):
                raise RegulationsGovSourceError(f"Mirrulations object {key} version is invalid")
            if not isinstance(content, bytes) or not content or len(content) > MAX_OBJECT_BYTES:
                raise RegulationsGovSourceError(f"Mirrulations object {key} bytes are invalid")
            record = classifier(_decode_json(content))
            included = _record_in_scope(
                record,
                query_scope=scope,
                agency=agency,
                collection=collection,
            )
            if pack and (
                len(pack) == MAX_EVIDENCE_PACK_OBJECTS
                or pack_raw_bytes + len(content) > MAX_EVIDENCE_PACK_RAW_BYTES
            ):
                request_key = _pack_request(
                    collection=collection,
                    agency=agency,
                    pack_index=pack_index,
                    terminal=False,
                )
                yield RegulationsGovPage(
                    traversal_index=0,
                    page_index=page_index,
                    window_index=page_index,
                    window_page_index=0,
                    request_key=request_key,
                    source_cursor=None,
                    response_bytes=_pack_bytes(
                        pack,
                        collection=collection,
                        agency=agency,
                        pack_index=pack_index,
                        terminal=False,
                    ),
                )
                page_index += 1
                pack_index += 1
                pack = []
                pack_raw_bytes = 0
            pack.append(
                _PackedObject(
                    key=key,
                    etag=etag,
                    version_id=version_id,
                    content=content,
                    included=included,
                )
            )
            pack_raw_bytes += len(content)
            previous_global_key = key
        request_key = _pack_request(
            collection=collection,
            agency=agency,
            pack_index=pack_index,
            terminal=True,
        )
        yield RegulationsGovPage(
            traversal_index=0,
            page_index=page_index,
            window_index=page_index,
            window_page_index=0,
            request_key=request_key,
            source_cursor=None,
            response_bytes=_pack_bytes(
                pack,
                collection=collection,
                agency=agency,
                pack_index=pack_index,
                terminal=True,
            ),
        )
        page_index += 1


def iter_regulations_gov_document_pages(
    read: RegulationsGovRead,
    *,
    query_scope: Mapping[str, Any],
    scratch_directory: Path | None = None,
) -> Iterator[RegulationsGovPage]:
    return _iter_pages(
        read,
        query_scope=query_scope,
        collection=DOCUMENT_COLLECTION,
        scope_validator=regulations_gov_document_query_scope,
        classifier=classify_document,
        scratch_directory=scratch_directory,
    )


def iter_regulations_gov_docket_pages(
    read: RegulationsGovRead,
    *,
    query_scope: Mapping[str, Any],
    scratch_directory: Path | None = None,
) -> Iterator[RegulationsGovPage]:
    return _iter_pages(
        read,
        query_scope=query_scope,
        collection=DOCKET_COLLECTION,
        scope_validator=regulations_gov_docket_query_scope,
        classifier=classify_docket,
        scratch_directory=scratch_directory,
    )


def iter_regulations_gov_comment_pages(
    read: RegulationsGovRead,
    *,
    query_scope: Mapping[str, Any],
    scratch_directory: Path | None = None,
) -> Iterator[RegulationsGovPage]:
    return _iter_pages(
        read,
        query_scope=query_scope,
        collection=COMMENT_COLLECTION,
        scope_validator=regulations_gov_comment_query_scope,
        classifier=classify_comment,
        scratch_directory=scratch_directory,
    )


_NULLABLE_TEXT_SCHEMA: Final = {"type": ["string", "null"]}
_NULLABLE_BOOLEAN_SCHEMA: Final = {"type": ["boolean", "null"]}
_NULLABLE_INTEGER_SCHEMA: Final = {"type": ["integer", "null"]}
_TEXT_ARRAY_SCHEMA: Final = {
    "items": {"type": "string"},
    "type": ["array", "null"],
}
_DISPLAY_PROPERTIES_SCHEMA: Final = {
    "items": {
        "additionalProperties": False,
        "properties": {
            name: _NULLABLE_TEXT_SCHEMA for name in sorted(_DISPLAY_PROPERTY_FIELDS)
        },
        "type": "object",
    },
    "type": ["array", "null"],
}
_FILE_FORMATS_SCHEMA: Final = {
    "items": {
        "additionalProperties": False,
        "properties": {
            "fileUrl": {"minLength": 1, "type": "string"},
            "format": _NULLABLE_TEXT_SCHEMA,
            "size": {"type": ["integer", "string", "null"]},
        },
        "required": ["fileUrl"],
        "type": "object",
    },
    "type": ["array", "null"],
}
_LINKS_SCHEMA: Final = {
    "additionalProperties": False,
    "properties": {name: _NULLABLE_TEXT_SCHEMA for name in sorted(_LINK_FIELDS)},
    "type": ["object", "null"],
}
_RELATIONSHIPS_SCHEMA: Final = {
    "additionalProperties": False,
    "properties": {
        "attachments": {
            "additionalProperties": False,
            "properties": {
                "data": {
                    "oneOf": [
                        {"type": "null"},
                        {
                            "additionalProperties": False,
                            "properties": {
                                "id": {"minLength": 1, "type": "string"},
                                "type": {"minLength": 1, "type": "string"},
                            },
                            "required": ["id", "type"],
                            "type": "object",
                        },
                        {
                            "items": {
                                "additionalProperties": False,
                                "properties": {
                                    "id": {"minLength": 1, "type": "string"},
                                    "type": {"minLength": 1, "type": "string"},
                                },
                                "required": ["id", "type"],
                                "type": "object",
                            },
                            "type": "array",
                        },
                    ]
                },
                "links": _LINKS_SCHEMA,
            },
            "type": ["object", "null"],
        }
    },
    "type": ["object", "null"],
}
_ATTACHMENT_ATTRIBUTES_SCHEMA: dict[str, Any] = {
    name: _NULLABLE_TEXT_SCHEMA
    for name in sorted(
        _ATTACHMENT_ATTRIBUTE_FIELDS - {"authors", "docOrder", "fileFormats"}
    )
}
_ATTACHMENT_ATTRIBUTES_SCHEMA.update(
    {
        "authors": _TEXT_ARRAY_SCHEMA,
        "docOrder": _NULLABLE_INTEGER_SCHEMA,
        "fileFormats": _FILE_FORMATS_SCHEMA,
    }
)
_INCLUDED_SCHEMA: Final = {
    "items": {
        "additionalProperties": False,
        "properties": {
            "attributes": {
                "additionalProperties": False,
                "properties": _ATTACHMENT_ATTRIBUTES_SCHEMA,
                "type": "object",
            },
            "id": {"minLength": 1, "type": "string"},
            "links": _LINKS_SCHEMA,
            "relationships": _RELATIONSHIPS_SCHEMA,
            "type": {"const": "attachments"},
        },
        "required": ["attributes", "id", "type"],
        "type": "object",
    },
    "type": ["array", "null"],
}
_META_SCHEMA: Final = {
    "additionalProperties": False,
    "properties": {
        "hasMore": {"type": ["boolean", "null"]},
        "hasNextPage": {"type": ["boolean", "null"]},
        "numberOfElements": _NULLABLE_INTEGER_SCHEMA,
        "pageNumber": _NULLABLE_INTEGER_SCHEMA,
        "pageSize": _NULLABLE_INTEGER_SCHEMA,
        "totalElements": _NULLABLE_INTEGER_SCHEMA,
        "totalPages": _NULLABLE_INTEGER_SCHEMA,
    },
    "type": ["object", "null"],
}


def _raw_schema(
    *,
    schema_id: str,
    collection: str,
    attributes: Mapping[str, Any],
    required_attributes: Sequence[str],
) -> dict[str, Any]:
    return {
        "$id": schema_id,
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "additionalProperties": False,
        "properties": {
            "data": {
                "additionalProperties": False,
                "properties": {
                    "attributes": {
                        "additionalProperties": False,
                        "properties": dict(attributes),
                        "required": list(required_attributes),
                        "type": "object",
                    },
                    "id": {"minLength": 1, "type": "string"},
                    "links": _LINKS_SCHEMA,
                    "relationships": _RELATIONSHIPS_SCHEMA,
                    "type": {"const": collection},
                },
                "required": ["attributes", "id", "type"],
                "type": "object",
            },
            "included": _INCLUDED_SCHEMA,
            "meta": _META_SCHEMA,
        },
        "required": ["data"],
        "type": "object",
        "x-spicy-record-order": [
            {
                "fieldPath": "/data/id",
                "nullOrder": "forbidden",
                "tupleComparison": "utf16-code-unit",
                "valueType": "string",
            }
        ],
    }


_DOCUMENT_ATTRIBUTES_SCHEMA: dict[str, Any] = {
    name: _NULLABLE_TEXT_SCHEMA for name in sorted(DOCUMENT_ATTRIBUTE_FIELDS)
}
_DOCUMENT_ATTRIBUTES_SCHEMA.update(
    {
        name: _NULLABLE_BOOLEAN_SCHEMA for name in _DOCUMENT_BOOLEAN_FIELDS
    }
)
_DOCUMENT_ATTRIBUTES_SCHEMA.update(
    {name: _NULLABLE_INTEGER_SCHEMA for name in _DOCUMENT_INTEGER_FIELDS}
)
_DOCUMENT_ATTRIBUTES_SCHEMA.update(
    {name: _TEXT_ARRAY_SCHEMA for name in _DOCUMENT_TEXT_ARRAY_FIELDS}
)
_DOCUMENT_ATTRIBUTES_SCHEMA.update(
    {
        "displayProperties": _DISPLAY_PROPERTIES_SCHEMA,
        "fileFormats": _FILE_FORMATS_SCHEMA,
        "topics": {
            "items": {
                "oneOf": [
                    {"type": "string"},
                    {
                        "additionalProperties": False,
                        "properties": {
                            name: _NULLABLE_TEXT_SCHEMA for name in sorted(_TOPIC_FIELDS)
                        },
                        "type": "object",
                    },
                ]
            },
            "type": ["array", "null"],
        },
    }
)
_DOCKET_ATTRIBUTES_SCHEMA: dict[str, Any] = {
    name: _NULLABLE_TEXT_SCHEMA for name in sorted(DOCKET_ATTRIBUTE_FIELDS)
}
_DOCKET_ATTRIBUTES_SCHEMA.update(
    {
        "displayProperties": _DISPLAY_PROPERTIES_SCHEMA,
        "keywords": _TEXT_ARRAY_SCHEMA,
    }
)
_COMMENT_ATTRIBUTES_SCHEMA: dict[str, Any] = {
    name: _NULLABLE_TEXT_SCHEMA for name in sorted(COMMENT_ATTRIBUTE_FIELDS)
}
_COMMENT_ATTRIBUTES_SCHEMA.update(
    {name: _NULLABLE_BOOLEAN_SCHEMA for name in _COMMENT_BOOLEAN_FIELDS}
)
_COMMENT_ATTRIBUTES_SCHEMA.update(
    {name: _NULLABLE_INTEGER_SCHEMA for name in _COMMENT_INTEGER_FIELDS}
)
_COMMENT_ATTRIBUTES_SCHEMA.update(
    {
        name: {"type": ["integer", "string", "null"]}
        for name in _COMMENT_INTEGER_OR_TEXT_FIELDS
    }
)
_COMMENT_ATTRIBUTES_SCHEMA.update(
    {
        "displayProperties": _DISPLAY_PROPERTIES_SCHEMA,
        "fileFormats": _FILE_FORMATS_SCHEMA,
    }
)

REGULATIONS_GOV_DOCUMENT_SCHEMA: Final = _raw_schema(
    schema_id="urn:spicy-regs:schema:regulations-gov-document-raw:1.0",
    collection=DOCUMENT_COLLECTION,
    attributes=_DOCUMENT_ATTRIBUTES_SCHEMA,
    required_attributes=("agencyId", "postedDate"),
)
REGULATIONS_GOV_DOCKET_SCHEMA: Final = _raw_schema(
    schema_id="urn:spicy-regs:schema:regulations-gov-docket-raw:1.0",
    collection=DOCKET_COLLECTION,
    attributes=_DOCKET_ATTRIBUTES_SCHEMA,
    required_attributes=("agencyId", "modifyDate"),
)
REGULATIONS_GOV_COMMENT_SCHEMA: Final = _raw_schema(
    schema_id="urn:spicy-regs:schema:regulations-gov-comment-raw:1.0",
    collection=COMMENT_COLLECTION,
    attributes=_COMMENT_ATTRIBUTES_SCHEMA,
    required_attributes=("agencyId", "postedDate"),
)


def document_source_schema_digest() -> str:
    return schema_bundle_digest({DOCUMENT_SCHEMA_PATH: REGULATIONS_GOV_DOCUMENT_SCHEMA})


def docket_source_schema_digest() -> str:
    return schema_bundle_digest({DOCKET_SCHEMA_PATH: REGULATIONS_GOV_DOCKET_SCHEMA})


def comment_source_schema_digest() -> str:
    return schema_bundle_digest({COMMENT_SCHEMA_PATH: REGULATIONS_GOV_COMMENT_SCHEMA})


def document_source_schema_declaration() -> dict[str, str]:
    return {
        "schemaDigest": document_source_schema_digest(),
        "schemaName": DOCUMENT_SCHEMA_NAME,
        "schemaVersion": SCHEMA_VERSION,
    }


def docket_source_schema_declaration() -> dict[str, str]:
    return {
        "schemaDigest": docket_source_schema_digest(),
        "schemaName": DOCKET_SCHEMA_NAME,
        "schemaVersion": SCHEMA_VERSION,
    }


def comment_source_schema_declaration() -> dict[str, str]:
    return {
        "schemaDigest": comment_source_schema_digest(),
        "schemaName": COMMENT_SCHEMA_NAME,
        "schemaVersion": SCHEMA_VERSION,
    }


def document_source_record_digest(record: Mapping[str, Any]) -> str:
    return framed_section_digest(
        "spicyregs-regulations-gov-document-record/1",
        (FramedSection("record", 1, (dict(record),)),),
    )


def docket_source_record_digest(record: Mapping[str, Any]) -> str:
    return framed_section_digest(
        "spicyregs-regulations-gov-docket-record/1",
        (FramedSection("record", 1, (dict(record),)),),
    )


def comment_source_record_digest(record: Mapping[str, Any]) -> str:
    return framed_section_digest(
        "spicyregs-regulations-gov-comment-record/1",
        (FramedSection("record", 1, (dict(record),)),),
    )


__all__ = [
    "ACQUISITION_POLICY_VERSION",
    "COMMENT_ACQUISITION_POLICY_ID",
    "COMMENT_ATTRIBUTE_FIELDS",
    "COMMENT_COLLECTION",
    "COMMENT_RECORD_STEM",
    "COMMENT_SCOPE_ID",
    "COMMENT_SOURCE_SCHEMA_KEY",
    "COMMENT_SOURCE_SYSTEM_ID",
    "DOCUMENT_ACQUISITION_POLICY_ID",
    "DOCUMENT_ATTRIBUTE_FIELDS",
    "DOCUMENT_COLLECTION",
    "DOCUMENT_RECORD_STEM",
    "DOCUMENT_SCOPE_ID",
    "DOCUMENT_SOURCE_SCHEMA_KEY",
    "DOCUMENT_SOURCE_SYSTEM_ID",
    "DOCKET_ACQUISITION_POLICY_ID",
    "DOCKET_ATTRIBUTE_FIELDS",
    "DOCKET_COLLECTION",
    "DOCKET_RECORD_STEM",
    "DOCKET_SCOPE_ID",
    "DOCKET_SOURCE_SCHEMA_KEY",
    "DOCKET_SOURCE_SYSTEM_ID",
    "MAX_OBJECT_BYTES",
    "MAX_TRAVERSALS",
    "MirrulationsAcquisitionCheck",
    "RegulationsGovPage",
    "RegulationsGovRead",
    "RegulationsGovSourceError",
    "RegulationsGovTraversalCheck",
    "REGULATIONS_GOV_DOCUMENT_SCHEMA",
    "REGULATIONS_GOV_DOCKET_SCHEMA",
    "REGULATIONS_GOV_COMMENT_SCHEMA",
    "SOURCE_SYSTEM_VERSION",
    "classify_document",
    "classify_docket",
    "classify_comment",
    "comment_acquisition_policy",
    "comment_observation_version",
    "comment_records_included",
    "comment_rendition_rows",
    "comment_source_issued_version",
    "comment_source_record",
    "comment_source_record_digest",
    "comment_source_schema_declaration",
    "comment_source_schema_digest",
    "docket_acquisition_policy",
    "docket_records_included",
    "docket_rendition_rows",
    "docket_source_record",
    "docket_source_record_digest",
    "docket_source_schema_declaration",
    "docket_source_schema_digest",
    "document_acquisition_policy",
    "document_records_included",
    "document_rendition_rows",
    "document_source_record",
    "document_source_record_digest",
    "document_source_schema_declaration",
    "document_source_schema_digest",
    "iter_regulations_gov_document_pages",
    "iter_regulations_gov_docket_pages",
    "iter_regulations_gov_comment_pages",
    "parse_comment_page_response",
    "parse_document_page_response",
    "parse_docket_page_response",
    "parse_mirrulations_request",
    "regulations_gov_document_query_scope",
    "regulations_gov_docket_query_scope",
    "regulations_gov_comment_query_scope",
    "regulations_gov_next_page_url",
    "source_issued_version",
    "source_record_id",
    "validate_document_record_scope",
    "validate_docket_record_scope",
    "validate_comment_record_scope",
]
