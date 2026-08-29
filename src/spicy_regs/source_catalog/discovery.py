"""What a discovery adapter hands the producer, and nothing more.

A discovery adapter enumerates one source system and reports facts.  It never
decides membership: the universe specification does that, in
``release.select_source_items``.  So a :class:`DiscoveredItem` carries the
source's own identity, its exact native metadata, the nine normalized fields a
source can state, its observed topics and other observations, and the
renditions it offers — plus, when the source itself already settled the matter,
one terminal :class:`SourceOutcome` (a withdrawn document, an uncaptured pair,
an unparsable record).

The tenth normalized field, ``language``, and the ``agencyName`` half of the
agency pair are declared by the universe, not observed here.  See
:class:`~spicy_regs.source_catalog.universe.NormalizationPolicy`.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any

from spicy_regs.source_catalog.universe import (
    DATE_RE,
    HTTP_URL_RE,
    MAX_JSON_SAFE_INTEGER,
    MEDIA_TYPE_RE,
    REASON_CODE_RE,
    RIN_RE,
    SourceCatalogError,
)


class SourceOutcome(StrEnum):
    """What the SOURCE already settled about an item before any policy ran.

    ``AVAILABLE`` is the only outcome that reaches the universe policy.  The
    other three map straight onto the schema's non-selected dispositions, which
    is why they carry the source's name and not a policy verdict.
    """

    AVAILABLE = "available"
    DELETED = "deleted"
    UNAVAILABLE = "unavailable"
    FAILED = "failed"


def _text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise SourceCatalogError(f"{field_name} must be a non-empty string, got {value!r}")
    return value


def _optional_date(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or DATE_RE.fullmatch(value) is None:
        raise SourceCatalogError(f"{field_name} must be an ISO-8601 date or null, got {value!r}")
    return value


@dataclass(frozen=True)
class ObservedTopic:
    """One topic the SOURCE observed.  Never a RefSpec concept."""

    observed_topic_id: str
    observed_topic_scheme: str
    label: str

    def __post_init__(self) -> None:
        _text(self.observed_topic_id, field_name="observedTopicId")
        _text(self.observed_topic_scheme, field_name="observedTopicScheme")
        _text(self.label, field_name="label")

    def as_record(self) -> dict[str, str]:
        return {
            "label": self.label,
            "observedTopicId": self.observed_topic_id,
            "observedTopicScheme": self.observed_topic_scheme,
        }


@dataclass(frozen=True)
class Observation:
    """One source-stated key/value the normalized field set does not cover."""

    observation_key: str
    observation_value: str

    def __post_init__(self) -> None:
        _text(self.observation_key, field_name="observationKey")
        if not isinstance(self.observation_value, str):
            raise SourceCatalogError(f"observationValue must be a string, got {self.observation_value!r}")

    def as_record(self) -> dict[str, str]:
        return {"observationKey": self.observation_key, "observationValue": self.observation_value}


@dataclass(frozen=True)
class CandidateRendition:
    """One rendition the source offers for capture.

    ``expected_sha256`` is present only when the producer already knows the
    exact bytes that locator serves — a verified local capture of them, for
    instance.  It is never a guess, and it is never a later capture digest
    written back into the catalog.
    """

    rendition_id: str
    media_type: str
    locator: str
    expected_sha256: str | None = None
    expected_byte_size: int | None = None

    def __post_init__(self) -> None:
        _text(self.rendition_id, field_name="renditionId")
        if MEDIA_TYPE_RE.fullmatch(self.media_type) is None:
            raise SourceCatalogError(f"renditionId {self.rendition_id!r} has an invalid mediaType")
        if not isinstance(self.locator, str) or HTTP_URL_RE.fullmatch(self.locator) is None:
            raise SourceCatalogError(f"renditionId {self.rendition_id!r} needs an http(s) locator")
        if self.expected_sha256 is not None:
            digest = self.expected_sha256
            if not isinstance(digest, str) or not digest.startswith("sha256:") or len(digest) != 71:
                raise SourceCatalogError(f"renditionId {self.rendition_id!r} has an invalid expectedSha256")
            if any(character not in "0123456789abcdef" for character in digest.removeprefix("sha256:")):
                raise SourceCatalogError(f"renditionId {self.rendition_id!r} has a non-hex expectedSha256")
        if self.expected_byte_size is not None:
            size = self.expected_byte_size
            if isinstance(size, bool) or not isinstance(size, int) or not 0 <= size <= MAX_JSON_SAFE_INTEGER:
                raise SourceCatalogError(f"renditionId {self.rendition_id!r} has an invalid expectedByteSize")

    def as_record(self) -> dict[str, Any]:
        return {
            "expectedByteSize": self.expected_byte_size,
            "expectedSha256": self.expected_sha256,
            "locator": self.locator,
            "mediaType": self.media_type,
            "renditionId": self.rendition_id,
        }


@dataclass(frozen=True)
class NormalizedDraft:
    """The nine normalized MVP fields a source can state, exactly as stated.

    Every field is optional here and nullable at the wire only where the schema
    says so.  A draft missing a field the schema requires does not become a
    selected item: the producer gives it a non-selected disposition naming the
    field.  Nothing is defaulted, and no placeholder is written.
    """

    title: str | None = None
    agency_ids: tuple[str, ...] = ()
    document_type: str | None = None
    publication_date: str | None = None
    last_updated_date: str | None = None
    docket_ids: tuple[str, ...] = ()
    regulation_identifier_numbers: tuple[str, ...] = ()
    comment_close_date: str | None = None
    source_url: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "agency_ids", tuple(dict.fromkeys(self.agency_ids)))
        object.__setattr__(self, "docket_ids", tuple(dict.fromkeys(self.docket_ids)))
        object.__setattr__(
            self,
            "regulation_identifier_numbers",
            tuple(sorted(dict.fromkeys(self.regulation_identifier_numbers))),
        )
        _optional_date(self.publication_date, field_name="publicationDate")
        _optional_date(self.last_updated_date, field_name="lastUpdatedDate")
        _optional_date(self.comment_close_date, field_name="commentCloseDate")
        for rin in self.regulation_identifier_numbers:
            if RIN_RE.fullmatch(rin) is None:
                raise SourceCatalogError(f"regulationIdentifierNumbers holds a non-RIN value {rin!r}")

    def missing_required_field(self, *, source_url: str | None = None) -> str | None:
        """Name the first schema-required field the source did not state.

        ``source_url`` overrides the drafted one when the universe declares a
        source-URL form for a source whose records omit it; passing ``None``
        keeps the drafted value, so a caller that declares nothing sees the
        source's own answer.
        """

        if not isinstance(self.title, str) or not self.title:
            return "title"
        if not isinstance(self.document_type, str) or not self.document_type:
            return "documentType"
        if not isinstance(self.publication_date, str):
            return "publicationDate"
        resolved = source_url if source_url is not None else self.source_url
        if not isinstance(resolved, str) or HTTP_URL_RE.fullmatch(resolved) is None:
            return "sourceUrl"
        return None


@dataclass(frozen=True)
class DiscoveredItem:
    """One member of the requested universe as the adapter found it."""

    source_item_id: str
    document_id: str
    source_issued_version: str
    source_native_metadata: Mapping[str, Any]
    normalized: NormalizedDraft | None = None
    observed_topics: tuple[ObservedTopic, ...] = ()
    observations: tuple[Observation, ...] = ()
    renditions: tuple[CandidateRendition, ...] = ()
    outcome: SourceOutcome = SourceOutcome.AVAILABLE
    outcome_reason_code: str | None = None
    outcome_reason: str | None = None
    # Where the item sits in the source system's own address space — an object
    # key, a bucket path, an API collection.  The universe's location prefixes
    # are matched against it; nothing else reads it.
    location_key: str | None = None

    def __post_init__(self) -> None:
        _text(self.source_item_id, field_name="sourceItemId")
        _text(self.document_id, field_name="documentId")
        _text(self.source_issued_version, field_name="sourceIssuedVersion")
        if not isinstance(self.source_native_metadata, Mapping):
            raise SourceCatalogError(
                f"{self.source_item_id}: sourceNativeMetadata must be a JSON object, "
                f"got {type(self.source_native_metadata).__name__}"
            )
        if self.outcome is not SourceOutcome.AVAILABLE:
            if not self.outcome_reason_code or not self.outcome_reason:
                raise SourceCatalogError(
                    f"{self.source_item_id}: outcome {self.outcome} requires a reason code and a reason"
                )
        if self.outcome_reason_code is not None and REASON_CODE_RE.fullmatch(self.outcome_reason_code) is None:
            raise SourceCatalogError(
                f"{self.source_item_id}: reason code {self.outcome_reason_code!r} is not machine-legible"
            )
        seen: set[str] = set()
        for rendition in self.renditions:
            if rendition.rendition_id in seen:
                raise SourceCatalogError(f"{self.source_item_id}: duplicate renditionId {rendition.rendition_id!r}")
            seen.add(rendition.rendition_id)


def require_unique_source_item_ids(items: Iterable[DiscoveredItem]) -> tuple[DiscoveredItem, ...]:
    """Refuse a discovery set that names one item twice."""

    ordered = tuple(items)
    seen: set[str] = set()
    for item in ordered:
        if item.source_item_id in seen:
            raise SourceCatalogError(f"duplicate sourceItemId in discovery input: {item.source_item_id!r}")
        seen.add(item.source_item_id)
    return ordered


__all__ = [
    "CandidateRendition",
    "DiscoveredItem",
    "NormalizedDraft",
    "Observation",
    "ObservedTopic",
    "SourceOutcome",
    "require_unique_source_item_ids",
]
