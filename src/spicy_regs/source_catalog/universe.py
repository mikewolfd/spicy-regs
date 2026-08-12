"""The requested universe ``U`` as configuration, and its selection policy.

A ``SourceCatalogRelease`` root records the policy that decided membership of
the selected set ``S`` within the requested universe ``U`` as exactly
``{policyId, policyVersion, policySha256}`` — an identity, a version, and a
digest over the exact policy bytes.  The release does not carry the bytes.

:class:`UniverseSpec` is those bytes.  It is a value loaded from configuration,
so the corpus universe is named by whoever writes the configuration and never
by this module.  :meth:`UniverseSpec.policy_document` is its canonical form and
:meth:`UniverseSpec.policy_sha256` the digest the release quotes, so two
spellings of the same scope produce one digest and any change of scope,
window, language, or agency crosswalk produces another.

Canonical JSON is the repository's single integer-only RFC 8785 profile from
``spicy_regs.document_release_v3``; this module adds no thirty-fourth
implementation (`PLAN.md` §7).
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

from spicy_regs.document_release_v3 import DocumentReleaseV3Error, canonical_json_bytes, sha256_bytes


class SourceCatalogError(RuntimeError):
    """The universe specification, discovery input, or bundle failed closed."""


# Mirrors of the pinned schema's own lexical rules.  They are restated (not
# imported) because a specification is refused before a bundle exists to
# validate, and a caller deserves the refusal at the field it names.  The
# pinned schemas remain the authority: `verify` re-checks every produced row
# against them, so a drift between these expressions and the schema surfaces
# as a refused bundle rather than as a published one.
ABSOLUTE_ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.-]*:[^\s]+$")
DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
LANGUAGE_RE = re.compile(r"^[A-Za-z]{2,3}(-[A-Za-z0-9]{2,8})*$")
REASON_CODE_RE = re.compile(r"^[a-z][a-z0-9]*(\.[a-z][a-z0-9-]*)+$")
RIN_RE = re.compile(r"^[0-9]{4}-[A-Z][A-Z0-9]{3}$")
HTTP_URL_RE = re.compile(r"^https?://[^\s]+$")
MEDIA_TYPE_RE = re.compile(r"^[a-z]+/[A-Za-z0-9.+-]+$")
INSTANT_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$")

MAX_JSON_SAFE_INTEGER = (1 << 53) - 1

#: The single placeholder a declared ``sourceUrlTemplate`` may carry.
SOURCE_URL_PLACEHOLDER = "{documentId}"


def canonical_digest(value: Any) -> str:
    """Return the lowercase SHA-256 over one value's canonical JSON bytes."""

    try:
        return sha256_bytes(canonical_json_bytes(value))
    except DocumentReleaseV3Error as error:  # pragma: no cover - defensive
        raise SourceCatalogError(f"value is not canonicalizable: {error}") from error


def _require_absolute_id(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or ABSOLUTE_ID_RE.fullmatch(value) is None:
        raise SourceCatalogError(f"{field_name} must be an absolute identifier, got {value!r}")
    return value


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise SourceCatalogError(f"{field_name} must be a non-empty string, got {value!r}")
    return value


def _require_date(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or DATE_RE.fullmatch(value) is None:
        raise SourceCatalogError(f"{field_name} must be an ISO-8601 date, got {value!r}")
    return value


def _sorted_unique(values: Iterable[object], *, field_name: str) -> tuple[str, ...]:
    seen: list[str] = []
    for value in values:
        seen.append(_require_text(value, field_name=field_name))
    return tuple(sorted(set(seen)))


@dataclass(frozen=True)
class PublicationWindow:
    """An inclusive publication-date window; either end may stay open."""

    start: str | None = None
    end: str | None = None

    def __post_init__(self) -> None:
        if self.start is not None:
            _require_date(self.start, field_name="publicationWindow.from")
        if self.end is not None:
            _require_date(self.end, field_name="publicationWindow.to")
        if self.start is not None and self.end is not None and self.start > self.end:
            raise SourceCatalogError("publicationWindow.from must not follow publicationWindow.to")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> PublicationWindow:
        if value is None:
            return cls()
        if not isinstance(value, Mapping):
            raise SourceCatalogError("publicationWindow must be an object")
        unexpected = sorted(set(value) - {"from", "to"})
        if unexpected:
            raise SourceCatalogError(f"publicationWindow has unknown keys: {unexpected}")
        return cls(start=value.get("from"), end=value.get("to"))

    def canonical(self) -> dict[str, Any]:
        return {"from": self.start, "to": self.end}

    def admits(self, publication_date: str) -> bool:
        if self.start is not None and publication_date < self.start:
            return False
        return not (self.end is not None and publication_date > self.end)


@dataclass(frozen=True)
class UniverseScope:
    """The predicates that decide which discovered items belong to ``U``.

    Every facet is optional and empty means "unconstrained".  Facets compose by
    conjunction: an item is in scope when it satisfies all of them.
    """

    location_prefixes: tuple[str, ...] = ()
    agency_ids: tuple[str, ...] = ()
    docket_ids: tuple[str, ...] = ()
    document_types: tuple[str, ...] = ()
    publication_window: PublicationWindow = field(default_factory=PublicationWindow)
    max_items: int | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "location_prefixes", _sorted_unique(self.location_prefixes, field_name="scope.locationPrefixes")
        )
        object.__setattr__(self, "agency_ids", _sorted_unique(self.agency_ids, field_name="scope.agencyIds"))
        object.__setattr__(self, "docket_ids", _sorted_unique(self.docket_ids, field_name="scope.docketIds"))
        object.__setattr__(
            self, "document_types", _sorted_unique(self.document_types, field_name="scope.documentTypes")
        )
        if self.max_items is not None:
            if isinstance(self.max_items, bool) or not isinstance(self.max_items, int):
                raise SourceCatalogError("scope.maxItems must be an integer")
            if not 0 < self.max_items <= MAX_JSON_SAFE_INTEGER:
                raise SourceCatalogError("scope.maxItems must be a positive JSON-safe integer")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> UniverseScope:
        if value is None:
            return cls()
        if not isinstance(value, Mapping):
            raise SourceCatalogError("scope must be an object")
        known = {
            "locationPrefixes",
            "agencyIds",
            "docketIds",
            "documentTypes",
            "publicationWindow",
            "maxItems",
        }
        unexpected = sorted(set(value) - known)
        if unexpected:
            raise SourceCatalogError(f"scope has unknown keys: {unexpected}")
        return cls(
            location_prefixes=tuple(value.get("locationPrefixes") or ()),
            agency_ids=tuple(value.get("agencyIds") or ()),
            docket_ids=tuple(value.get("docketIds") or ()),
            document_types=tuple(value.get("documentTypes") or ()),
            publication_window=PublicationWindow.from_mapping(value.get("publicationWindow")),
            max_items=value.get("maxItems"),
        )

    def canonical(self) -> dict[str, Any]:
        """Total canonical form: every facet appears, unset ones as empty."""

        return {
            "agencyIds": list(self.agency_ids),
            "docketIds": list(self.docket_ids),
            "documentTypes": list(self.document_types),
            "locationPrefixes": list(self.location_prefixes),
            "maxItems": self.max_items,
            "publicationWindow": self.publication_window.canonical(),
        }

    def admits(self, draft: Any, *, location_key: str | None) -> tuple[str, str] | None:
        """Return ``None`` when the item is in scope, else its exclusion reason.

        ``draft`` is a :class:`~spicy_regs.source_catalog.discovery.NormalizedDraft`;
        it is typed loosely here so the scope model stays free of a discovery
        import cycle.
        """

        if self.location_prefixes:
            if not isinstance(location_key, str) or not any(
                location_key.startswith(prefix) for prefix in self.location_prefixes
            ):
                return (
                    "policy.location-out-of-scope",
                    f"The universe covers {list(self.location_prefixes)}; this item is at {location_key!r}.",
                )
        if self.document_types:
            if draft.document_type not in self.document_types:
                return (
                    "policy.document-type-out-of-scope",
                    f"The universe admits {list(self.document_types)}; this item is a {draft.document_type!r}.",
                )
        if self.agency_ids:
            if not set(draft.agency_ids) & set(self.agency_ids):
                return (
                    "policy.agency-out-of-scope",
                    f"The universe covers {list(self.agency_ids)}; this item names {list(draft.agency_ids)}.",
                )
        if self.docket_ids:
            if not set(draft.docket_ids) & set(self.docket_ids):
                return (
                    "policy.docket-out-of-scope",
                    f"The universe covers {list(self.docket_ids)}; this item names {list(draft.docket_ids)}.",
                )
        window = self.publication_window
        if window.start is not None or window.end is not None:
            publication_date = draft.publication_date
            if not isinstance(publication_date, str):
                # A window cannot admit or refuse an item whose publication date
                # the source never stated in a usable form.  Saying so is not
                # the same as saying the item fell outside the window, so it
                # gets its own reason code rather than borrowing that one.
                return (
                    "policy.publication-date-unusable",
                    f"The universe covers {window.start!r}..{window.end!r}; the source states no "
                    "publication date this policy can read, so the item can be placed neither inside "
                    "the window nor outside it.",
                )
            if not window.admits(publication_date):
                return (
                    "policy.publication-window-out-of-scope",
                    f"The universe covers {window.start!r}..{window.end!r}; "
                    f"this item was published {publication_date!r}.",
                )
        return None


# The sampler admits exactly the mechanics it implements.  A specification that
# named a partition, stratum, hash, or allocation this module cannot execute
# would put a rule inside `policySha256` that no run ever applied, so each is a
# closed vocabulary of one and a spec naming anything else is refused at load.
SAMPLE_PARTITION_KEYS: tuple[str, ...] = ("documentType",)
SAMPLE_STRATUM_KEYS: tuple[tuple[str, ...], ...] = (("agencyId", "publicationYear"),)
SAMPLE_ORDER_HASHES: tuple[str, ...] = ("md5(documentId:seed)",)
SAMPLE_ALLOCATIONS: tuple[str, ...] = ("sqrt-proportional",)
SAMPLE_UNKNOWN_STRATUM_PART = "unknown"


#: The roles a declared source may play.  ``metadata`` supplies the normalized
#: field set; ``rendition`` supplies locators to capture.  One source may do
#: both, and a universe may declare several of either.
SOURCE_ROLES: tuple[str, ...] = ("metadata", "rendition")


@dataclass(frozen=True)
class PinnedSource:
    """One discovery source, named and pinned to the exact bytes it served.

    A universe that reads more than one source has to say which ones, at which
    versions, or its ``policySha256`` would describe a selection nobody could
    reproduce.  The wire schema carries a single ``sourceSystem`` object and is
    owned by Rulespec, so the release states a composite identity there and the
    per-source pins ride inside the policy document, where this list is what
    the composite version digests.
    """

    source_system_id: str
    source_system_version: str
    role: str

    def __post_init__(self) -> None:
        _require_absolute_id(self.source_system_id, field_name="sourceSystems[].sourceSystemId")
        _require_text(self.source_system_version, field_name="sourceSystems[].sourceSystemVersion")
        if self.role not in SOURCE_ROLES:
            raise SourceCatalogError(f"sourceSystems[].role must be one of {list(SOURCE_ROLES)}, got {self.role!r}")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> PinnedSource:
        if not isinstance(value, Mapping):
            raise SourceCatalogError("each entry of sourceSystems must be an object")
        unexpected = sorted(set(value) - {"sourceSystemId", "sourceSystemVersion", "role"})
        if unexpected:
            raise SourceCatalogError(f"sourceSystems entry has unknown keys: {unexpected}")
        for required in ("sourceSystemId", "sourceSystemVersion", "role"):
            if required not in value:
                raise SourceCatalogError(f"sourceSystems[].{required} is required")
        return cls(
            source_system_id=value["sourceSystemId"],
            source_system_version=value["sourceSystemVersion"],
            role=value["role"],
        )

    def canonical(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "sourceSystemId": self.source_system_id,
            "sourceSystemVersion": self.source_system_version,
        }


def composite_source_version(sources: Sequence[PinnedSource]) -> str:
    """The one version string that names a whole set of pinned sources."""

    return f"sha256:{canonical_digest([source.canonical() for source in sources])}"


@dataclass(frozen=True)
class SampleCandidate:
    """One in-scope item, reduced to the facts the sampler reads."""

    source_item_id: str
    document_id: str
    document_type: str
    agency_ids: tuple[str, ...] = ()
    publication_date: str | None = None


@dataclass(frozen=True)
class SamplePolicy:
    """A deterministic stratified draw over the items the scope admits.

    The draw is the selection policy, not a convenience: an item the scope
    admits and this policy does not draw is ``excluded`` with
    ``policy.sample-not-drawn``, so the frame it was drawn from stays visible
    in the release rather than being silently narrowed.

    The mechanics are the ones proven against the published corpus:

    * partition the frame by document type and cap each partition;
    * stratify each partition by agency and publication year;
    * order within a stratum by ``md5(documentId:seed)``, then by document id;
    * order the partition by ``rank / sqrt(stratumSize)``, which lets a small
      stratum contribute early rows without letting a large one crowd it out;
    * take the first ``perPartitionLimit`` rows.

    Every input is a source-stated fact or a declared constant, so two runs
    over the same frame draw the same set and a consumer can re-derive it.
    """

    seed: str
    per_partition_limit: int
    partition_by: str = SAMPLE_PARTITION_KEYS[0]
    stratify_by: tuple[str, ...] = SAMPLE_STRATUM_KEYS[0]
    order_hash: str = SAMPLE_ORDER_HASHES[0]
    allocation: str = SAMPLE_ALLOCATIONS[0]

    def __post_init__(self) -> None:
        _require_text(self.seed, field_name="sample.seed")
        limit = self.per_partition_limit
        if isinstance(limit, bool) or not isinstance(limit, int):
            raise SourceCatalogError("sample.perPartitionLimit must be an integer")
        if not 0 < limit <= MAX_JSON_SAFE_INTEGER:
            raise SourceCatalogError("sample.perPartitionLimit must be a positive JSON-safe integer")
        object.__setattr__(self, "stratify_by", tuple(self.stratify_by))
        if self.partition_by not in SAMPLE_PARTITION_KEYS:
            raise SourceCatalogError(f"sample.partitionBy must be one of {list(SAMPLE_PARTITION_KEYS)}")
        if self.stratify_by not in SAMPLE_STRATUM_KEYS:
            raise SourceCatalogError(f"sample.stratifyBy must be one of {[list(k) for k in SAMPLE_STRATUM_KEYS]}")
        if self.order_hash not in SAMPLE_ORDER_HASHES:
            raise SourceCatalogError(f"sample.orderHash must be one of {list(SAMPLE_ORDER_HASHES)}")
        if self.allocation not in SAMPLE_ALLOCATIONS:
            raise SourceCatalogError(f"sample.allocation must be one of {list(SAMPLE_ALLOCATIONS)}")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> SamplePolicy | None:
        if value is None:
            return None
        if not isinstance(value, Mapping):
            raise SourceCatalogError("sample must be an object")
        known = {"seed", "perPartitionLimit", "partitionBy", "stratifyBy", "orderHash", "allocation"}
        unexpected = sorted(set(value) - known)
        if unexpected:
            raise SourceCatalogError(f"sample has unknown keys: {unexpected}")
        for required in ("seed", "perPartitionLimit"):
            if required not in value:
                raise SourceCatalogError(f"sample.{required} is required")
        return cls(
            seed=value["seed"],
            per_partition_limit=value["perPartitionLimit"],
            partition_by=value.get("partitionBy", SAMPLE_PARTITION_KEYS[0]),
            stratify_by=tuple(value.get("stratifyBy") or SAMPLE_STRATUM_KEYS[0]),
            order_hash=value.get("orderHash", SAMPLE_ORDER_HASHES[0]),
            allocation=value.get("allocation", SAMPLE_ALLOCATIONS[0]),
        )

    def canonical(self) -> dict[str, Any]:
        return {
            "allocation": self.allocation,
            "orderHash": self.order_hash,
            "partitionBy": self.partition_by,
            "perPartitionLimit": self.per_partition_limit,
            "seed": self.seed,
            "stratifyBy": list(self.stratify_by),
        }

    def order_key(self, document_id: str) -> str:
        """``md5(documentId:seed)`` — the declared per-item ordering hash."""

        return hashlib.md5(f"{document_id}:{self.seed}".encode(), usedforsecurity=False).hexdigest()

    def stratum(self, candidate: SampleCandidate) -> str:
        """``agencyId|publicationYear``, with either part ``unknown`` when unstated."""

        agency = "+".join(sorted(candidate.agency_ids)) or SAMPLE_UNKNOWN_STRATUM_PART
        date = candidate.publication_date
        year = date[:4] if isinstance(date, str) and len(date) >= 4 else SAMPLE_UNKNOWN_STRATUM_PART
        return f"{agency}|{year}"

    def draw(self, candidates: Iterable[SampleCandidate]) -> frozenset[str]:
        """Return the ``sourceItemId`` set this policy draws from one frame."""

        partitions: dict[str, list[SampleCandidate]] = {}
        for candidate in candidates:
            partitions.setdefault(candidate.document_type, []).append(candidate)

        drawn: set[str] = set()
        for members in partitions.values():
            strata: dict[str, list[SampleCandidate]] = {}
            order_keys = {candidate.document_id: self.order_key(candidate.document_id) for candidate in members}
            for candidate in members:
                strata.setdefault(self.stratum(candidate), []).append(candidate)
            scored: list[tuple[float, str, str, str]] = []
            for rows in strata.values():
                rows.sort(key=lambda candidate: (order_keys[candidate.document_id], candidate.document_id))
                spread = math.sqrt(len(rows))
                for rank, candidate in enumerate(rows, start=1):
                    scored.append(
                        (
                            rank / spread,
                            order_keys[candidate.document_id],
                            candidate.document_id,
                            candidate.source_item_id,
                        )
                    )
            scored.sort()
            drawn.update(entry[3] for entry in scored[: self.per_partition_limit])
        return frozenset(drawn)


@dataclass(frozen=True)
class NormalizationPolicy:
    """The declared facts the normalized MVP field set needs and no source states.

    Two of the ten normalized fields cannot be read off a Regulations.gov or
    Mirrulations record:

    ``language``
        The schema requires a BCP 47 tag and admits no null.  The source states
        no language at all, so the universe declares the one its corpus is in.
        Declaring it here puts it inside ``policySha256``, where a consumer can
        see it was chosen rather than observed.

    ``agencies[].agencyName``
        The source states an agency *code* and no name, while the schema
        requires both.  ``agencyNames`` is the declared crosswalk.  An agency
        code with no declared name is not guessed and not filled with its own
        code: the item takes a non-selected disposition naming the gap.

    ``sourceUrl`` is the third, and only for a source whose records omit it.
    The schema requires an http(s) URL and admits no null.  A source system
    that publishes a per-item address but does not restate it inside each
    record can declare the address form once, here, as
    ``sourceUrlTemplate`` — one ``{documentId}`` placeholder, filled with the
    percent-encoded source-stated identifier.  A record that *does* state its
    own URL keeps it; the template never overwrites an observed value, and a
    universe that declares none leaves the gap where it is.
    """

    language: str
    agency_names: Mapping[str, str] = field(default_factory=dict)
    source_url_template: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.language, str) or LANGUAGE_RE.fullmatch(self.language) is None:
            raise SourceCatalogError(f"normalization.language must be a BCP 47 tag, got {self.language!r}")
        names = dict(self.agency_names)
        for key, value in names.items():
            _require_text(key, field_name="normalization.agencyNames key")
            _require_text(value, field_name=f"normalization.agencyNames[{key}]")
        object.__setattr__(self, "agency_names", dict(sorted(names.items())))
        template = self.source_url_template
        if template is not None:
            _require_text(template, field_name="normalization.sourceUrlTemplate")
            if template.count(SOURCE_URL_PLACEHOLDER) != 1:
                raise SourceCatalogError(
                    f"normalization.sourceUrlTemplate must hold exactly one {SOURCE_URL_PLACEHOLDER} placeholder"
                )
            if HTTP_URL_RE.fullmatch(template) is None:
                raise SourceCatalogError("normalization.sourceUrlTemplate must be an http(s) URL")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> NormalizationPolicy:
        if not isinstance(value, Mapping):
            raise SourceCatalogError("normalization must be an object declaring at least a language")
        unexpected = sorted(set(value) - {"language", "agencyNames", "sourceUrlTemplate"})
        if unexpected:
            raise SourceCatalogError(f"normalization has unknown keys: {unexpected}")
        if "language" not in value:
            raise SourceCatalogError(
                "normalization.language is required: the schema admits no null language and no source states one"
            )
        return cls(
            language=value["language"],
            agency_names=value.get("agencyNames") or {},
            source_url_template=value.get("sourceUrlTemplate"),
        )

    def canonical(self) -> dict[str, Any]:
        return {
            "agencyNames": dict(self.agency_names),
            "language": self.language,
            "sourceUrlTemplate": self.source_url_template,
        }

    def source_url(self, stated: str | None, document_id: str) -> str | None:
        """The item's source URL: the one the record states, else the declared form."""

        if isinstance(stated, str) and HTTP_URL_RE.fullmatch(stated) is not None:
            return stated
        if self.source_url_template is None:
            return stated
        return self.source_url_template.replace(SOURCE_URL_PLACEHOLDER, quote(document_id, safe=""))

    def agencies(self, agency_ids: Iterable[str]) -> tuple[list[dict[str, str]] | None, tuple[str, str] | None]:
        """Resolve agency codes to the schema's ``{agencyId, agencyName}`` pairs."""

        resolved: list[dict[str, str]] = []
        for agency_id in sorted(set(agency_ids)):
            name = self.agency_names.get(agency_id)
            if name is None:
                return None, (
                    "policy.agency-name-undeclared",
                    f"The source states agency code {agency_id!r} and no agency name, and the universe "
                    "declares no name for it.",
                )
            resolved.append({"agencyId": agency_id, "agencyName": name})
        if not resolved:
            return None, (
                "source.normalized-field-missing",
                "The source states no agency; the normalized field set requires at least one.",
            )
        return resolved, None


@dataclass(frozen=True)
class UniverseSpec:
    """One named requested universe and the policy that selects inside it."""

    universe_id: str
    catalog_id: str
    policy_id: str
    policy_version: str
    source_system_id: str
    source_system_version: str
    # No default: a universe that declares no language cannot produce a
    # selected item, and the schema will not accept a null one.
    normalization: NormalizationPolicy
    scope: UniverseScope = field(default_factory=UniverseScope)
    # Absent means the scope alone decides ``S``: every item the scope admits
    # and the source can supply is selected.
    sample: SamplePolicy | None = None
    # Empty means the universe reads exactly the one source named above.  When
    # several are declared, ``source_system_version`` must be the digest over
    # this list, so the composite the wire carries is checkable rather than
    # asserted.
    sources: tuple[PinnedSource, ...] = ()
    # Rendition families in preference order.  An item takes the highest family
    # that has anything to offer, and the lower ones are not carried: the
    # release states the rendition a capture should take, not a menu.
    rendition_preference: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "sources", tuple(self.sources))
        object.__setattr__(self, "rendition_preference", tuple(self.rendition_preference))
        if self.sources:
            derived = composite_source_version(self.sources)
            if self.source_system_version != derived:
                raise SourceCatalogError(
                    f"sourceSystem.sourceSystemVersion is {self.source_system_version!r}, but the "
                    f"{len(self.sources)} declared sourceSystems derive {derived}"
                )
        for family in self.rendition_preference:
            _require_text(family, field_name="renditionPreference[]")
        if len(set(self.rendition_preference)) != len(self.rendition_preference):
            raise SourceCatalogError("renditionPreference names one family twice")
        self._validate_identifiers()

    def _validate_identifiers(self) -> None:
        _require_absolute_id(self.universe_id, field_name="universeId")
        _require_absolute_id(self.catalog_id, field_name="catalogId")
        _require_absolute_id(self.policy_id, field_name="selectionPolicy.policyId")
        _require_text(self.policy_version, field_name="selectionPolicy.policyVersion")
        _require_absolute_id(self.source_system_id, field_name="sourceSystem.sourceSystemId")
        _require_text(self.source_system_version, field_name="sourceSystem.sourceSystemVersion")

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> UniverseSpec:
        if not isinstance(value, Mapping):
            raise SourceCatalogError("a universe specification must be a JSON object")
        known = {
            "universeId",
            "catalogId",
            "selectionPolicy",
            "sourceSystem",
            "sourceSystems",
            "renditionPreference",
            "scope",
            "sample",
            "normalization",
        }
        unexpected = sorted(set(value) - known)
        if unexpected:
            raise SourceCatalogError(f"universe specification has unknown keys: {unexpected}")
        policy = value.get("selectionPolicy")
        if not isinstance(policy, Mapping):
            raise SourceCatalogError("selectionPolicy must be an object with policyId and policyVersion")
        unexpected = sorted(set(policy) - {"policyId", "policyVersion"})
        if unexpected:
            raise SourceCatalogError(f"selectionPolicy has unknown keys: {unexpected}")
        system = value.get("sourceSystem")
        if not isinstance(system, Mapping):
            raise SourceCatalogError("sourceSystem must be an object with sourceSystemId and sourceSystemVersion")
        unexpected = sorted(set(system) - {"sourceSystemId", "sourceSystemVersion"})
        if unexpected:
            raise SourceCatalogError(f"sourceSystem has unknown keys: {unexpected}")
        for required, holder, label in (
            ("universeId", value, "universeId"),
            ("catalogId", value, "catalogId"),
            ("policyId", policy, "selectionPolicy.policyId"),
            ("policyVersion", policy, "selectionPolicy.policyVersion"),
            ("sourceSystemId", system, "sourceSystem.sourceSystemId"),
            ("sourceSystemVersion", system, "sourceSystem.sourceSystemVersion"),
        ):
            if required not in holder:
                raise SourceCatalogError(f"{label} is required")
        return cls(
            universe_id=value["universeId"],
            catalog_id=value["catalogId"],
            policy_id=policy["policyId"],
            policy_version=policy["policyVersion"],
            source_system_id=system["sourceSystemId"],
            source_system_version=system["sourceSystemVersion"],
            scope=UniverseScope.from_mapping(value.get("scope")),
            sample=SamplePolicy.from_mapping(value.get("sample")),
            sources=tuple(PinnedSource.from_mapping(entry) for entry in value.get("sourceSystems") or ()),
            rendition_preference=tuple(value.get("renditionPreference") or ()),
            normalization=NormalizationPolicy.from_mapping(value.get("normalization")),
        )

    def policy_document(self) -> dict[str, Any]:
        """The exact policy bytes ``policySha256`` digests.

        ``policyId`` and ``policyVersion`` are deliberately outside it: they
        name and version the policy, and the digest states what the policy
        *is*.  A renamed policy with identical rules keeps its digest, which is
        what lets a consumer notice that only the label moved.
        """

        document: dict[str, Any] = {
            "normalization": self.normalization.canonical(),
            "sample": self.sample.canonical() if self.sample is not None else None,
            "scope": self.scope.canonical(),
            "sourceSystem": self.source_system_record(),
            "universeId": self.universe_id,
        }
        # Added only when declared.  A universe that reads one source and ranks
        # nothing keeps the document it always had, so its digest — and the
        # release identity resting on it — does not move under this feature.
        if self.sources:
            document["sourceSystems"] = [source.canonical() for source in self.sources]
        if self.rendition_preference:
            document["renditionPreference"] = list(self.rendition_preference)
        return document

    def policy_document_bytes(self) -> bytes:
        return canonical_json_bytes(self.policy_document())

    def policy_sha256(self) -> str:
        return canonical_digest(self.policy_document())

    def selection_policy_record(self) -> dict[str, Any]:
        """The ``{policyId, policyVersion, policySha256}`` the release root carries."""

        return {
            "policyId": self.policy_id,
            "policySha256": self.policy_sha256(),
            "policyVersion": self.policy_version,
        }

    def source_system_record(self) -> dict[str, Any]:
        return {
            "sourceSystemId": self.source_system_id,
            "sourceSystemVersion": self.source_system_version,
        }


def load_universe_spec(path: Path | str) -> UniverseSpec:
    """Read one universe specification from a JSON configuration file."""

    path = Path(path)
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SourceCatalogError(f"universe specification is unreadable: {path} ({error})") from error
    return UniverseSpec.from_mapping(value)


__all__ = [
    "MAX_JSON_SAFE_INTEGER",
    "SAMPLE_ALLOCATIONS",
    "SAMPLE_ORDER_HASHES",
    "SAMPLE_PARTITION_KEYS",
    "SAMPLE_STRATUM_KEYS",
    "SOURCE_URL_PLACEHOLDER",
    "NormalizationPolicy",
    "PinnedSource",
    "PublicationWindow",
    "SampleCandidate",
    "SamplePolicy",
    "SourceCatalogError",
    "UniverseScope",
    "UniverseSpec",
    "canonical_digest",
    "composite_source_version",
    "load_universe_spec",
]
