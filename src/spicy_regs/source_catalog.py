"""Map regulatory sources into one Rulespec-backed source-catalog artifact."""

from __future__ import annotations

import hashlib
import json
import re
import shutil
import sqlite3
import tempfile
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import jsonschema
from rulespec_conformance.platform_artifact import (
    ROOT_OBJECT_KEY,
    SOURCE_CATALOG_ITEM_SCHEMA_ID,
    LocalMemberSource,
    MemberManifestReference,
    MemberSource,
    SourceCatalogSpec,
    VerifiedArtifact,
    admit_artifact,
    build_artifact_root,
    canonical_json_bytes,
    describe_member,
    iter_member_descriptors,
    parse_canonical_json,
    source_catalog_item_schema_bytes,
)

from spicy_regs.publication import publish_directory_once, write_bytes_once, write_chunks_once
from spicy_regs.schemas.base import RecordType
from spicy_regs.sources.base import Reader
from spicy_regs.transforms.build_federal_register import shape_federal_register_document

SOURCE_ITEMS_OBJECT_KEY = "records/source-items.jsonl"
SOURCE_SCHEMA_OBJECT_KEY = "schemas/source-catalog-item-v1.schema.json"
SOURCE_ITEMS_MANIFEST_KEY = "manifests/source-items.json"
SOURCE_ITEMS_ROLE = "source-items"
SOURCE_SCHEMA_ROLE = "schema"
SOURCE_ITEMS_MEDIA_TYPE = "application/x-ndjson"
SOURCE_SCHEMA_MEDIA_TYPE = "application/schema+json"
MAX_SOURCE_ITEM_BYTES = 4 * 1024 * 1024
MAX_SOURCE_SCHEMA_BYTES = 512 * 1024

_SOURCE_SCHEMA_BYTES = source_catalog_item_schema_bytes()
_SOURCE_SCHEMA = json.loads(_SOURCE_SCHEMA_BYTES)
SOURCE_ITEMS_SCHEMA_ID = SOURCE_CATALOG_ITEM_SCHEMA_ID
jsonschema.Draft202012Validator.check_schema(_SOURCE_SCHEMA)
_SOURCE_ITEM_VALIDATOR = jsonschema.Draft202012Validator(_SOURCE_SCHEMA)
_RIN = re.compile(r"^[0-9]{4}-[A-Z][A-Z0-9]{3}$")


class SourceCatalogError(ValueError):
    """A source stream or catalog member is not publishable."""


class SourceCatalogIncompleteError(SourceCatalogError):
    """A reader reported source keys it could not consume completely."""


@dataclass(frozen=True, slots=True)
class SourceCatalogItem:
    """One product-owned source record ready for schema validation and sealing."""

    value: Mapping[str, Any]

    @property
    def source_item_id(self) -> str:
        value = self.value.get("sourceItemId")
        return value if isinstance(value, str) else ""

    @property
    def selected(self) -> bool:
        selection = self.value.get("selection")
        return isinstance(selection, Mapping) and selection.get("disposition") == "selected"

    def as_dict(self) -> dict[str, Any]:
        value = dict(self.value)
        try:
            _SOURCE_ITEM_VALIDATOR.validate(value)
        except jsonschema.ValidationError as error:
            path = "/".join(str(part) for part in error.absolute_path) or "$"
            raise SourceCatalogError(f"source item schema failure at {path}: {error.message}") from error
        selection = value["selection"]
        if selection["disposition"] == "selected" and not value["candidateRenditions"]:
            raise SourceCatalogError("selected source item must offer at least one candidate rendition")
        if selection["disposition"] != "selected" and not {
            "reasonCode",
            "reason",
        } <= selection.keys():
            raise SourceCatalogError("non-selected source item must explain its disposition")
        return value


@dataclass(frozen=True, slots=True)
class SourceCatalogBuild:
    """SpicyRegs source and selection inputs supplied before set digests exist."""

    catalog_id: str
    source_system_id: str
    source_system_version: str
    selection_policy_id: str
    selection_policy_version: str
    selection_policy_digest: str


@dataclass(frozen=True, slots=True)
class PublishedSourceCatalog:
    root: Path
    artifact: VerifiedArtifact


def _iso_date(value: object) -> str | None:
    if not isinstance(value, str) or len(value) < 10:
        return None
    candidate = value[:10]
    try:
        date.fromisoformat(candidate)
    except ValueError:
        return None
    return candidate


def _list(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def _strings(value: object) -> list[str]:
    return sorted({item for item in _list(value) if isinstance(item, str) and item})


def _media_type(value: object, locator: str) -> str:
    if isinstance(value, str):
        normalized = value.strip().lower()
        if "/" in normalized:
            return normalized
        aliases = {
            "doc": "application/msword",
            "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "html": "text/html",
            "htm": "text/html",
            "pdf": "application/pdf",
            "txt": "text/plain",
            "xml": "application/xml",
        }
        if normalized in aliases:
            return aliases[normalized]
    suffix = locator.rsplit("?", 1)[0].rsplit(".", 1)[-1].lower()
    return _media_type(suffix, "") if suffix and suffix != locator.lower() else "application/octet-stream"


def _rendition(locator: object, media_type: object = None, size: object = None) -> dict[str, Any] | None:
    if not isinstance(locator, str) or not locator.startswith(("http://", "https://")):
        return None
    byte_size: int | None = None
    if isinstance(size, int) and not isinstance(size, bool) and size >= 0:
        byte_size = size
    elif isinstance(size, str) and size.isdigit():
        byte_size = int(size)
    digest = hashlib.sha256(locator.encode("utf-8")).hexdigest()
    return {
        "expectedByteSize": byte_size,
        "expectedSha256": None,
        "locator": locator,
        "mediaType": _media_type(media_type, locator),
        "renditionId": f"urn:spicy-regs:rendition:{digest}",
    }


def _dedupe_renditions(values: Iterable[dict[str, Any] | None]) -> list[dict[str, Any]]:
    by_locator = {value["locator"]: value for value in values if value is not None}
    return [by_locator[locator] for locator in sorted(by_locator)]


def _topics(values: object, *, scheme: str) -> list[dict[str, str]]:
    topics: dict[tuple[str, str], dict[str, str]] = {}
    for value in _list(values):
        if isinstance(value, str):
            label = value
            identity = hashlib.sha256(value.encode("utf-8")).hexdigest()
        elif isinstance(value, Mapping):
            label_value = value.get("name") or value.get("label")
            if not isinstance(label_value, str) or not label_value:
                continue
            label = label_value
            raw_identity = value.get("slug") or value.get("id")
            identity = (
                raw_identity
                if isinstance(raw_identity, str) and raw_identity
                else hashlib.sha256(label.encode("utf-8")).hexdigest()
            )
        else:
            continue
        topic = {
            "label": label,
            "observedTopicId": identity,
            "observedTopicScheme": scheme,
        }
        topics[(identity, label)] = topic
    return [topics[key] for key in sorted(topics)]


def _selection(
    normalized: Mapping[str, Any] | None,
    renditions: Sequence[Mapping[str, Any]],
    *,
    withdrawn: bool = False,
    withdrawn_reason: object = None,
) -> dict[str, str]:
    if withdrawn:
        reason = (
            withdrawn_reason
            if isinstance(withdrawn_reason, str) and withdrawn_reason
            else "Source marks the item withdrawn."
        )
        return {
            "disposition": "excluded",
            "reason": reason,
            "reasonCode": "selection.source-withdrawn",
        }
    if normalized is None or not renditions:
        return {
            "disposition": "excluded",
            "reason": "Required metadata or a retrievable rendition is absent.",
            "reasonCode": "selection.missing-required-data",
        }
    return {"disposition": "selected"}


def _normalized_regulations(record: Mapping[str, Any]) -> dict[str, Any] | None:
    title = record.get("title")
    agency_id = record.get("agency_code")
    document_type = record.get("document_type")
    publication_date = _iso_date(record.get("posted_date"))
    document_id = record.get("document_id")
    if not all(
        isinstance(value, str) and value
        for value in (title, agency_id, document_type, document_id)
    ) or publication_date is None:
        return None
    rins = _strings(record.get("additional_rins"))
    docket_id = record.get("docket_id")
    return {
        "agencies": [{"agencyId": agency_id, "agencyName": agency_id}],
        "commentCloseDate": _iso_date(record.get("comment_end_date")),
        "docketIds": [docket_id] if isinstance(docket_id, str) and docket_id else [],
        "documentType": document_type,
        "language": "en",
        "lastUpdatedDate": _iso_date(record.get("modify_date")),
        "publicationDate": publication_date,
        "regulationIdentifierNumbers": [rin for rin in rins if _RIN.fullmatch(rin)],
        "sourceUrl": f"https://www.regulations.gov/document/{document_id}",
        "title": title,
    }


def _normalized_federal(raw: Mapping[str, Any], record: Mapping[str, Any]) -> dict[str, Any] | None:
    title = record.get("title")
    document_type = record.get("document_type")
    publication_date = _iso_date(record.get("publication_date"))
    source_url = record.get("html_url")
    agencies: list[dict[str, str]] = []
    for agency in _list(raw.get("agencies")):
        if not isinstance(agency, Mapping):
            continue
        name = agency.get("name") or agency.get("raw_name")
        identity = agency.get("slug") or name
        if isinstance(name, str) and name and isinstance(identity, str) and identity:
            agencies.append({"agencyId": identity, "agencyName": name})
    agencies.sort(key=lambda agency: (agency["agencyId"], agency["agencyName"]))
    if not (
        isinstance(title, str)
        and title
        and isinstance(document_type, str)
        and document_type
        and publication_date is not None
        and isinstance(source_url, str)
        and source_url.startswith(("http://", "https://"))
        and agencies
    ):
        return None
    return {
        "agencies": agencies,
        "commentCloseDate": _iso_date(record.get("comments_close_on")),
        "docketIds": _strings(record.get("docket_ids_json")),
        "documentType": document_type,
        "language": "en",
        "lastUpdatedDate": None,
        "publicationDate": publication_date,
        "regulationIdentifierNumbers": [
            rin
            for rin in _strings(record.get("regulation_id_numbers_json"))
            if _RIN.fullmatch(rin)
        ],
        "sourceUrl": source_url,
        "title": title,
    }


def _reader_failures(reader: Reader) -> None:
    transient = tuple(str(value) for value in getattr(reader, "failed_keys", ()))
    parsing = tuple(str(value) for value in getattr(reader, "parse_failed_keys", ()))
    if transient or parsing:
        detail = ", ".join((*transient[:3], *parsing[:3]))
        raise SourceCatalogIncompleteError(
            f"source reader reported {len(transient)} transport and {len(parsing)} parse failures: {detail}"
        )


def regulations_gov_items(reader: Reader, record_type: RecordType) -> Iterator[SourceCatalogItem]:
    """Map an existing Mirrulations document reader without changing its I/O."""

    if record_type.name != "documents":
        raise SourceCatalogError("the source-catalog publisher accepts regulations.gov documents")
    for raw in reader.iter_records():
        record = record_type.extract(raw)
        identity = record.get(record_type.dedup_key)
        version = record.get("modify_date") or record.get("posted_date")
        if not isinstance(identity, str) or not identity or not isinstance(version, str) or not version:
            raise SourceCatalogError("regulations.gov item lacks document identity or source-issued version")
        attributes = raw.get("data", {}).get("attributes", {})
        renditions = _dedupe_renditions(
            _rendition(value.get("url"), value.get("format"), value.get("size"))
            for value in _list(record.get("attachments_json"))
            if isinstance(value, Mapping)
        )
        normalized = _normalized_regulations(record)
        withdrawn = record.get("withdrawn") in {True, "true", "True", "1", 1}
        yield SourceCatalogItem(
            {
                "candidateRenditions": renditions,
                "documentId": identity,
                "normalizedMetadata": normalized,
                "selection": _selection(
                    normalized,
                    renditions,
                    withdrawn=withdrawn,
                    withdrawn_reason=record.get("reason_withdrawn"),
                ),
                "sourceIssuedVersion": version,
                "sourceItemId": identity,
                "sourceNativeMetadata": dict(raw),
                "sourceObservations": [],
                "sourceObservedTopics": _topics(
                    attributes.get("topics") if isinstance(attributes, Mapping) else [],
                    scheme="regulations.gov",
                ),
            }
        )
    _reader_failures(reader)


def federal_register_items(reader: Reader) -> Iterator[SourceCatalogItem]:
    """Map the Federal Register reader through its existing published shape."""

    for raw in reader.iter_records():
        record = shape_federal_register_document(raw)
        identity = record.get("document_number")
        version = record.get("publication_date")
        if not isinstance(identity, str) or not identity or not isinstance(version, str) or not version:
            raise SourceCatalogError("Federal Register item lacks document_number or publication_date")
        renditions = _dedupe_renditions(
            (
                _rendition(record.get("body_html_url"), "text/html"),
                _rendition(record.get("html_url"), "text/html"),
                _rendition(record.get("pdf_url"), "application/pdf"),
            )
        )
        normalized = _normalized_federal(raw, record)
        yield SourceCatalogItem(
            {
                "candidateRenditions": renditions,
                "documentId": identity,
                "normalizedMetadata": normalized,
                "selection": _selection(normalized, renditions),
                "sourceIssuedVersion": version,
                "sourceItemId": identity,
                "sourceNativeMetadata": dict(raw),
                "sourceObservations": [],
                "sourceObservedTopics": _topics(
                    raw.get("topics"),
                    scheme="federalregister.gov",
                ),
            }
        )
    _reader_failures(reader)


class _SetDigest:
    def __init__(self) -> None:
        self._digest = hashlib.sha256()
        self._digest.update(b"[")
        self._first = True

    def add(self, value: str) -> None:
        if not self._first:
            self._digest.update(b",")
        self._digest.update(canonical_json_bytes(value))
        self._first = False

    def finish(self) -> str:
        self._digest.update(b"]")
        return "sha256:" + self._digest.hexdigest()


def _set_digest(rows: Iterable[tuple[str]]) -> str:
    digest = _SetDigest()
    for (value,) in rows:
        digest.add(value)
    return digest.finish()


def _source_item_chunks(connection: sqlite3.Connection) -> Iterator[bytes]:
    for (payload,) in connection.execute("SELECT payload FROM items ORDER BY source_item_id"):
        yield bytes(payload)
        yield b"\n"


def _read_bounded(source: MemberSource, object_key: str, byte_limit: int) -> bytes:
    with source.open(object_key) as stream:
        value = stream.read(byte_limit + 1)
    if len(value) > byte_limit:
        raise SourceCatalogError(f"{object_key} exceeds its {byte_limit}-byte product limit")
    return value


def verify_source_catalog_semantics(artifact: VerifiedArtifact, source: MemberSource) -> None:
    """Stream the SpicyRegs source members after common structural admission."""

    members = {member.object_key: member for member in iter_member_descriptors(artifact, source)}
    if set(members) != {SOURCE_ITEMS_OBJECT_KEY, SOURCE_SCHEMA_OBJECT_KEY}:
        raise SourceCatalogError("source catalog must contain one source-items member and its schema")
    items_member = members[SOURCE_ITEMS_OBJECT_KEY]
    schema_member = members[SOURCE_SCHEMA_OBJECT_KEY]
    if (
        items_member.role != SOURCE_ITEMS_ROLE
        or items_member.media_type != SOURCE_ITEMS_MEDIA_TYPE
        or items_member.schema_id != SOURCE_ITEMS_SCHEMA_ID
        or items_member.record_count is None
    ):
        raise SourceCatalogError("source-items descriptor differs from the SpicyRegs semantic view")
    if (
        schema_member.role != SOURCE_SCHEMA_ROLE
        or schema_member.media_type != SOURCE_SCHEMA_MEDIA_TYPE
        or schema_member.schema_id != SOURCE_ITEMS_SCHEMA_ID
        or schema_member.record_count is not None
    ):
        raise SourceCatalogError("source schema descriptor differs from the SpicyRegs semantic view")
    if _read_bounded(source, SOURCE_SCHEMA_OBJECT_KEY, MAX_SOURCE_SCHEMA_BYTES) != _SOURCE_SCHEMA_BYTES:
        raise SourceCatalogError("source catalog carries a different source-item schema")

    requested = _SetDigest()
    selected = _SetDigest()
    previous: str | None = None
    record_count = 0
    with source.open(SOURCE_ITEMS_OBJECT_KEY) as stream:
        while raw := stream.readline(MAX_SOURCE_ITEM_BYTES + 2):
            if len(raw) > MAX_SOURCE_ITEM_BYTES + 1:
                raise SourceCatalogError("source item exceeds the product row limit")
            if not raw.endswith(b"\n"):
                raise SourceCatalogError("source-items member must end every record with a newline")
            value = parse_canonical_json(
                raw[:-1],
                path=SOURCE_ITEMS_OBJECT_KEY,
                code="invalid.schema",
            )
            if not isinstance(value, Mapping):
                raise SourceCatalogError("source item must be a JSON object")
            item = SourceCatalogItem(value)
            item.as_dict()
            identity = item.source_item_id
            if previous is not None and identity <= previous:
                raise SourceCatalogError("source item identities must be strictly increasing")
            previous = identity
            requested.add(identity)
            if item.selected:
                selected.add(identity)
            record_count += 1
    if record_count != items_member.record_count:
        raise SourceCatalogError("source-items record count differs from its descriptor")
    spec = artifact.root["spec"]
    if requested.finish() != spec["requestedUniverseSetDigest"]:
        raise SourceCatalogError("requested source set digest differs")
    if selected.finish() != spec["selectedSourceSetDigest"]:
        raise SourceCatalogError("selected source set digest differs")


class SourceCatalogPublisher:
    """Seal any mapped source stream through one bounded publication path."""

    def publish(
        self,
        items: Iterable[SourceCatalogItem],
        *,
        build: SourceCatalogBuild,
        destination: Path,
    ) -> PublishedSourceCatalog:
        destination = Path(destination)
        if destination.exists() or destination.is_symlink():
            raise FileExistsError(f"refusing to replace immutable catalog: {destination}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        staging = Path(
            tempfile.mkdtemp(
                prefix=f".{destination.name}.",
                suffix=".staging",
                dir=destination.parent,
            )
        )
        scratch = Path(tempfile.mkdtemp(prefix="source-catalog-index-", dir=destination.parent))
        try:
            connection = sqlite3.connect(scratch / "items.sqlite3")
            try:
                connection.execute(
                    "CREATE TABLE items ("
                    "source_item_id TEXT PRIMARY KEY, selected INTEGER NOT NULL, payload BLOB NOT NULL) "
                    "WITHOUT ROWID"
                )
                for item in items:
                    payload = canonical_json_bytes(item.as_dict())
                    if len(payload) > MAX_SOURCE_ITEM_BYTES:
                        raise SourceCatalogError("source item exceeds the product row limit")
                    try:
                        connection.execute(
                            "INSERT INTO items VALUES (?, ?, ?)",
                            (item.source_item_id, int(item.selected), payload),
                        )
                    except sqlite3.IntegrityError as error:
                        raise SourceCatalogError(
                            f"source catalog repeats item identity {item.source_item_id!r}"
                        ) from error
                connection.commit()
                requested_digest = _set_digest(
                    connection.execute("SELECT source_item_id FROM items ORDER BY source_item_id")
                )
                selected_digest = _set_digest(
                    connection.execute(
                        "SELECT source_item_id FROM items WHERE selected = 1 ORDER BY source_item_id"
                    )
                )
                record_count = int(connection.execute("SELECT count(*) FROM items").fetchone()[0])
                write_chunks_once(staging / SOURCE_ITEMS_OBJECT_KEY, _source_item_chunks(connection))
            finally:
                connection.close()

            write_bytes_once(staging / SOURCE_SCHEMA_OBJECT_KEY, _SOURCE_SCHEMA_BYTES)
            source = LocalMemberSource(staging)
            members = (
                describe_member(
                    source,
                    object_key=SOURCE_ITEMS_OBJECT_KEY,
                    role=SOURCE_ITEMS_ROLE,
                    media_type=SOURCE_ITEMS_MEDIA_TYPE,
                    record_count=record_count,
                    schema_id=SOURCE_ITEMS_SCHEMA_ID,
                ),
                describe_member(
                    source,
                    object_key=SOURCE_SCHEMA_OBJECT_KEY,
                    role=SOURCE_SCHEMA_ROLE,
                    media_type=SOURCE_SCHEMA_MEDIA_TYPE,
                    schema_id=SOURCE_ITEMS_SCHEMA_ID,
                ),
            )
            manifest, manifest_bytes = MemberManifestReference.for_members(
                scope_kind="global",
                scope_id="source-items",
                object_key=SOURCE_ITEMS_MANIFEST_KEY,
                members=members,
            )
            write_bytes_once(staging / SOURCE_ITEMS_MANIFEST_KEY, manifest_bytes)
            root = build_artifact_root(
                spec=SourceCatalogSpec(
                    catalog_id=build.catalog_id,
                    source_system_id=build.source_system_id,
                    source_system_version=build.source_system_version,
                    selection_policy_id=build.selection_policy_id,
                    selection_policy_version=build.selection_policy_version,
                    selection_policy_digest=build.selection_policy_digest,
                    requested_universe_set_digest=requested_digest,
                    selected_source_set_digest=selected_digest,
                ),
                inputs=(),
                manifests=(manifest,),
                accounted_input_count=record_count,
            )
            write_bytes_once(staging / ROOT_OBJECT_KEY, canonical_json_bytes(root))
            artifact = admit_artifact(
                LocalMemberSource(staging),
                semantic_verifier=verify_source_catalog_semantics,
                scratch_directory=scratch / "verify",
            )
            publish_directory_once(staging, destination)
            return PublishedSourceCatalog(destination, artifact)
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
            shutil.rmtree(staging, ignore_errors=True)


__all__ = [
    "PublishedSourceCatalog",
    "SourceCatalogBuild",
    "SourceCatalogError",
    "SourceCatalogIncompleteError",
    "SourceCatalogItem",
    "SourceCatalogPublisher",
    "federal_register_items",
    "regulations_gov_items",
    "verify_source_catalog_semantics",
]
