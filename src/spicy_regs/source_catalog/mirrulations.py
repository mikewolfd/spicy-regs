"""Turn the Mirrulations draw and verified cache into discovery records.

``corpora.mirrulations_document_corpus`` already freezes exact S3 object
identities (``draw``) and captures the JSON/HTML pairs behind them into one
verified cache (``fetch``, ``validate``).  This adapter reads that pair —
nothing else — and reports what the source states.  It decides no membership:
the universe specification does that.

What the source states, and what it does not
--------------------------------------------
Eight of the ten normalized MVP fields come straight off the Regulations.gov
record through ``schemas.regulations.DOCUMENT``: title, document type,
publication and update dates, docket identifier, Regulation Identifier Numbers,
comment closing date, and source URL.  ``agencies[].agencyId`` comes from the
record's agency code.

Two do not exist in the source at all:

* ``language`` — no Regulations.gov document record states one.
* ``agencies[].agencyName`` — the record states a code and never a name.

Both are declared by the universe's
:class:`~spicy_regs.source_catalog.universe.NormalizationPolicy`, so they ride
inside ``policySha256`` where a consumer can see that they were chosen rather
than observed.  Nothing here fills either with a placeholder: an item whose
agency code the universe does not name takes a non-selected disposition saying
so.

Topics are likewise absent.  A Regulations.gov document record carries no topic
vocabulary, so ``sourceObservedTopics`` is empty for every item this adapter
produces.  The reader below still maps an ``attributes.topics`` array if the
source ever serves one, and the boundary check that a source-observed topic is
never a RefSpec concept runs either way.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote

from spicy_regs.corpora.mirrulations_document_corpus import (
    MirrulationsCorpusError,
    draw_documents,
    read_draw,
)
from spicy_regs.document_release_v3 import DocumentReleaseV3Error, canonical_json_bytes
from spicy_regs.schemas import DOCUMENT
from spicy_regs.source_catalog.discovery import (
    CandidateRendition,
    DiscoveredItem,
    NormalizedDraft,
    Observation,
    ObservedTopic,
    SourceOutcome,
)
from spicy_regs.source_catalog.universe import DATE_RE, RIN_RE, SourceCatalogError

SOURCE_ITEM_ID_PREFIX = "regulations.gov/"
MIRROR_URL_TEMPLATE = "https://{bucket}.s3.amazonaws.com/{key}"
MIRROR_RENDITION_ID = "mirrulations-mirror"
SOURCE_RENDITION_ID = "source-declared"
OBSERVED_TOPIC_SCHEME = "regulations.gov/topics"


def _date_part(value: object) -> str | None:
    """The calendar date of a source-stated instant, or ``None``.

    Regulations.gov states UTC instants; the normalized field set wants dates.
    Taking the leading ten characters keeps the source's own day boundary
    rather than re-zoning it into a different one.
    """

    if not isinstance(value, str) or len(value) < 10:
        return None
    head = value[:10]
    return head if DATE_RE.fullmatch(head) else None


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _attachments(flattened: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    try:
        parsed = json.loads(str(flattened.get("attachments_json") or "[]"))
    except json.JSONDecodeError:
        return []
    return [item for item in parsed if isinstance(item, Mapping)] if isinstance(parsed, list) else []


def _regulation_identifier_numbers(flattened: Mapping[str, Any]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Split the source's RIN list into conforming and non-conforming values."""

    try:
        parsed = json.loads(str(flattened.get("additional_rins") or "[]"))
    except json.JSONDecodeError:
        return (), ()
    values = [item for item in parsed if isinstance(item, str) and item] if isinstance(parsed, list) else []
    conforming = tuple(sorted({value for value in values if RIN_RE.fullmatch(value)}))
    other = tuple(sorted({value for value in values if not RIN_RE.fullmatch(value)}))
    return conforming, other


def _observed_topics(attributes: Mapping[str, Any]) -> tuple[ObservedTopic, ...]:
    topics = attributes.get("topics")
    if not isinstance(topics, list):
        return ()
    return tuple(
        ObservedTopic(observed_topic_id=value, observed_topic_scheme=OBSERVED_TOPIC_SCHEME, label=value)
        for value in sorted({item for item in topics if isinstance(item, str) and item})
    )


def _observations(
    entry: Mapping[str, Any], flattened: Mapping[str, Any], attributes: Mapping[str, Any], other_rins: Sequence[str]
) -> tuple[Observation, ...]:
    """A bounded, deterministic set of source-stated facts the ten fields miss."""

    pairs: list[tuple[str, str]] = []
    for key, value in (
        ("commentStartDate", flattened.get("comment_start_date")),
        ("frDocNum", flattened.get("fr_doc_num")),
        ("reasonWithdrawn", flattened.get("reason_withdrawn")),
        ("subtype", attributes.get("subtype")),
    ):
        text = _text(value)
        if text is not None:
            pairs.append((key, text))
    metadata_object = entry.get("metadata_object")
    rendition_object = entry.get("rendition_object")
    if isinstance(metadata_object, Mapping):
        pairs.append(("mirrorMetadataObjectKey", str(metadata_object.get("key"))))
    if isinstance(rendition_object, Mapping):
        pairs.append(("mirrorRenditionObjectKey", str(rendition_object.get("key"))))
    pairs.append(("mirrorJsonRevision", str(entry.get("json_revision", 0))))
    pairs.extend(("unnormalizedRegulationIdentifierNumber", value) for value in other_rins)
    return tuple(Observation(observation_key=key, observation_value=value) for key, value in sorted(pairs))


def _mirror_locator(bucket: str, key: str, *, template: str) -> str:
    return template.format(bucket=bucket, key=quote(key, safe="/"))


def _renditions(
    entry: Mapping[str, Any],
    receipt: Mapping[str, Any],
    flattened: Mapping[str, Any],
    *,
    bucket: str,
    mirror_url_template: str,
) -> tuple[CandidateRendition, ...]:
    rendition = receipt.get("rendition")
    rendition_object = entry.get("rendition_object")
    if not isinstance(rendition, Mapping) or not isinstance(rendition_object, Mapping):
        return ()
    media_type = str(rendition.get("media_type") or "text/html")
    digest = rendition.get("sha256")
    renditions = [
        CandidateRendition(
            rendition_id=MIRROR_RENDITION_ID,
            media_type=media_type,
            locator=_mirror_locator(bucket, str(rendition_object.get("key")), template=mirror_url_template),
            # The exact bytes SpicyRegs already verified at this locator, so the
            # digest is known before capture rather than guessed at it.
            expected_sha256=f"sha256:{digest}" if isinstance(digest, str) and digest else None,
            expected_byte_size=rendition_object.get("size"),
        )
    ]
    declared_url = _text((receipt.get("document") or {}).get("rendition_url"))
    if declared_url is not None:
        declared_size = None
        for attachment in _attachments(flattened):
            if attachment.get("url") == declared_url and isinstance(attachment.get("size"), int):
                declared_size = attachment["size"]
                break
        renditions.append(
            CandidateRendition(
                rendition_id=SOURCE_RENDITION_ID,
                media_type=media_type,
                locator=declared_url,
                # No digest: the source declares a size for this URL, never a
                # hash, and the mirror's digest describes the mirror's bytes.
                expected_sha256=None,
                expected_byte_size=declared_size,
            )
        )
    return tuple(renditions)


def _frozen_object_metadata(entry: Mapping[str, Any]) -> dict[str, Any]:
    """What the mirror itself states about an item whose pair never captured."""

    return {
        "documentId": entry.get("document_id"),
        "jsonRevision": entry.get("json_revision"),
        "metadataObject": dict(entry.get("metadata_object") or {}),
        "mirrorDirectory": entry.get("mirror_directory"),
        "renditionObject": dict(entry.get("rendition_object") or {}),
    }


def _source_issued_version(flattened: Mapping[str, Any], entry: Mapping[str, Any]) -> str:
    """The version label the SOURCE issued, never a capture digest.

    ``modifyDate`` is the source's own statement of which revision this is.
    When the record omits it the posted date serves, and when it omits that too
    the mirror's ``lastModified`` for the metadata object does — still a
    source-stated fact, just stated by the acquisition source.
    """

    for candidate in (flattened.get("modify_date"), flattened.get("posted_date")):
        text = _text(candidate)
        if text is not None:
            return text
    metadata_object = entry.get("metadata_object")
    if isinstance(metadata_object, Mapping):
        text = _text(metadata_object.get("last_modified"))
        if text is not None:
            return text
    raise SourceCatalogError(f"{entry.get('document_id')}: the source states no version at all")


def _canonicalizable(value: Any) -> bool:
    try:
        canonical_json_bytes(value)
    except DocumentReleaseV3Error:
        return False
    return True


def discover_items(
    manifest: Mapping[str, Any],
    *,
    cache_dir: Path | str,
    mirror_url_template: str = MIRROR_URL_TEMPLATE,
) -> tuple[DiscoveredItem, ...]:
    """Report one discovery record per drawn document, in draw order."""

    cache = Path(cache_dir)
    source = manifest.get("source")
    bucket = source.get("bucket") if isinstance(source, Mapping) else None
    if not isinstance(bucket, str) or not bucket:
        raise SourceCatalogError("the draw states no source bucket")

    items: list[DiscoveredItem] = []
    for entry in draw_documents(manifest):
        document_id = str(entry["document_id"])
        source_item_id = f"{SOURCE_ITEM_ID_PREFIX}{document_id}"
        metadata_object = entry.get("metadata_object")
        location_key = str(metadata_object.get("key")) if isinstance(metadata_object, Mapping) else None
        frozen = _frozen_object_metadata(entry)

        receipt_path = cache / "receipts" / f"{document_id}.json"
        try:
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            items.append(
                DiscoveredItem(
                    source_item_id=source_item_id,
                    document_id=document_id,
                    source_issued_version=_source_issued_version({}, entry),
                    source_native_metadata=frozen,
                    outcome=SourceOutcome.UNAVAILABLE,
                    outcome_reason_code="source.pair-not-captured",
                    outcome_reason=(
                        "The verified cache holds no receipt for this drawn document, so its exact "
                        "metadata and rendition bytes are not in hand."
                    ),
                    location_key=location_key,
                )
            )
            continue

        payload, failure = _source_payload(cache, receipt)
        if payload is None:
            items.append(
                DiscoveredItem(
                    source_item_id=source_item_id,
                    document_id=document_id,
                    source_issued_version=_source_issued_version({}, entry),
                    source_native_metadata=frozen,
                    outcome=SourceOutcome.FAILED,
                    outcome_reason_code="source.metadata-unparsable",
                    outcome_reason=str(failure),
                    location_key=location_key,
                )
            )
            continue

        flattened = DOCUMENT.extract(json.loads(json.dumps(payload)))
        data = payload.get("data")
        attributes = data.get("attributes") if isinstance(data, Mapping) else None
        attributes = attributes if isinstance(attributes, Mapping) else {}
        native = dict(data) if isinstance(data, Mapping) and _canonicalizable(data) else frozen
        conforming_rins, other_rins = _regulation_identifier_numbers(flattened)
        links = data.get("links") if isinstance(data, Mapping) else None
        source_url = _text(links.get("self")) if isinstance(links, Mapping) else None
        agency_code = _text(flattened.get("agency_code"))
        docket_id = _text(flattened.get("docket_id"))

        draft = NormalizedDraft(
            title=_text(flattened.get("title")),
            agency_ids=(agency_code,) if agency_code else (),
            document_type=_text(flattened.get("document_type")),
            publication_date=_date_part(flattened.get("posted_date")),
            last_updated_date=_date_part(flattened.get("modify_date")),
            docket_ids=(docket_id,) if docket_id else (),
            regulation_identifier_numbers=conforming_rins,
            comment_close_date=_date_part(flattened.get("comment_end_date")),
            source_url=source_url,
        )
        withdrawn = bool(flattened.get("withdrawn"))
        items.append(
            DiscoveredItem(
                source_item_id=source_item_id,
                document_id=document_id,
                source_issued_version=_source_issued_version(flattened, entry),
                source_native_metadata=native,
                normalized=draft,
                observed_topics=_observed_topics(attributes),
                observations=_observations(entry, flattened, attributes, other_rins),
                # A withdrawn document is the source's own settlement, so it
                # carries no rendition to offer for capture.
                renditions=()
                if withdrawn
                else _renditions(entry, receipt, flattened, bucket=bucket, mirror_url_template=mirror_url_template),
                outcome=SourceOutcome.DELETED if withdrawn else SourceOutcome.AVAILABLE,
                outcome_reason_code="source.withdrawn-after-publication" if withdrawn else None,
                outcome_reason=(
                    "The source marks this document withdrawn"
                    + (f": {flattened['reason_withdrawn']}" if _text(flattened.get("reason_withdrawn")) else ".")
                )
                if withdrawn
                else None,
                location_key=location_key,
            )
        )
    return tuple(items)


def _source_payload(cache: Path, receipt: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str | None]:
    metadata = receipt.get("metadata")
    relative = metadata.get("cache_file") if isinstance(metadata, Mapping) else None
    if not isinstance(relative, str) or not relative:
        return None, "The cache receipt names no metadata file."
    pure = Path(relative)
    if pure.is_absolute() or ".." in pure.parts:
        return None, "The cache receipt names a metadata path outside the cache."
    try:
        payload = json.loads((cache / pure).read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        return None, f"The cached source metadata could not be read as JSON: {error}"
    if not isinstance(payload, dict) or not isinstance(payload.get("data"), Mapping):
        return None, "The cached source metadata is not a Regulations.gov document record."
    return payload, None


def load_discovery(
    draw_path: Path | str,
    cache_dir: Path | str,
    *,
    mirror_url_template: str = MIRROR_URL_TEMPLATE,
) -> tuple[DiscoveredItem, ...]:
    """Read one frozen draw plus its verified cache and report discovery."""

    try:
        manifest = read_draw(Path(draw_path))
    except MirrulationsCorpusError as error:
        raise SourceCatalogError(f"the draw is unusable: {error}") from error
    return discover_items(manifest, cache_dir=cache_dir, mirror_url_template=mirror_url_template)


def source_item_ids(items: Iterable[DiscoveredItem]) -> list[str]:
    return sorted(item.source_item_id for item in items)


__all__ = [
    "MIRROR_RENDITION_ID",
    "MIRROR_URL_TEMPLATE",
    "OBSERVED_TOPIC_SCHEME",
    "SOURCE_ITEM_ID_PREFIX",
    "SOURCE_RENDITION_ID",
    "discover_items",
    "load_discovery",
    "source_item_ids",
]
