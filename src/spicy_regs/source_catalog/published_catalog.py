"""Turn the published SpicyRegs documents catalog into discovery records.

``data.spicy-regs.dev/documents.parquet`` is one row per Regulations.gov
document, already flattened by ``schemas.regulations.DOCUMENT``.  This adapter
reads that table and reports what it states.  It decides no membership: the
universe specification does that.

What the table states, and what it does not
-------------------------------------------
Seven of the ten normalized MVP fields come straight off a row: title, document
type, the posted and modify dates, the docket identifier, the Regulation
Identifier Numbers, and the comment closing date.  ``agencies[].agencyId`` is
the row's agency code.

Three do not exist in the table at all:

* ``language`` — no row states one.
* ``agencies[].agencyName`` — a row states a code and never a name.
* ``sourceUrl`` — the table carries no per-item address.  Regulations.gov does
  publish one, deterministically formed from the document identifier the table
  *does* state, so the universe declares that form as
  ``normalization.sourceUrlTemplate`` and it rides inside ``policySha256``.

All three are declared by the universe, never guessed here.

Dates are read strictly.  A posted, modify, or comment-closing value that is
not a ``YYYY-MM-DDTHH:MM:SSZ`` instant over a real calendar date is not
coerced, not zeroed, and not dropped: the normalized field stays unstated and
the raw string is carried as a source observation, so the policy can refuse the
item by name and a reader can still see what the source actually said.

The body text column is not read; it is a derived capture rather than
source-native metadata, and the catalog states it separately.

Where a rendition comes from
----------------------------
The table states a ``file_url`` for 2.5% of its rows, so a universe that wants
renditions has to look further than the row.  Three families are available and
the universe ranks them; an item takes the *highest family that has anything*
and carries only that one, so ``candidateRenditions`` names the rendition to
capture rather than a menu:

``mirrulations-mirror``
    Objects from a sealed mirror index, each with a verified
    ``expectedSha256`` — the index was built by fetching those exact bytes
    under their listed ETag and digesting them.  This is the only family whose
    digest is known before capture, which is why the owner ranks it first.

``source-file-url``
    The row's own ``file_url`` and attachment URLs, with the sizes the catalog
    declares and no digest, because the catalog states sizes and never hashes.

``federal-register``
    The FR ``pdf_url`` and ``html_url`` for a row that states its own
    ``frDocNum``.  Address only: no digest, no size.  The join is on the
    document's own number; nothing here reaches an FR document through a
    shared docket, which would attach another document's rendition to this one.

Passing no index and no FR table leaves the catalog's own locators, so the
adapter still works against the published table alone.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import quote

from spicy_regs.source_catalog.discovery import (
    CandidateRendition,
    DiscoveredItem,
    NormalizedDraft,
    Observation,
    SourceOutcome,
)
from spicy_regs.source_catalog.mirrulations import MIRROR_RENDITION_ID, MIRROR_URL_TEMPLATE
from spicy_regs.source_catalog.universe import INSTANT_RE, RIN_RE, SourceCatalogError

#: The mirror the verified index was built from.  Named here so the locator
#: this adapter writes and the one ``source_catalog.mirrulations`` writes for
#: the same object are the same string.
MIRROR_BUCKET = "mirrulations"

SOURCE_ITEM_ID_PREFIX = "regulations.gov/"
FILE_RENDITION_ID = "source-file-url"
ATTACHMENT_RENDITION_ID_PREFIX = "source-attachment-"
FEDERAL_REGISTER_RENDITION_ID = "federal-register"
DEFAULT_MEDIA_TYPE = "application/octet-stream"

#: ``{documentId: [{key, sha256, size}, ...]}`` — one sealed mirror index, as
#: ``tools/build_mirrulations_mirror_index.py`` writes it.
MirrorIndex = Mapping[str, Sequence[Mapping[str, Any]]]

#: Rendition families, best first.  A universe overrides this by declaring
#: ``renditionPreference``; it is spelled here so a caller with no universe
#: still gets the order the owner chose rather than an accident of dict order.
DEFAULT_RENDITION_PREFERENCE: tuple[str, ...] = (
    MIRROR_RENDITION_ID,
    FILE_RENDITION_ID,
    FEDERAL_REGISTER_RENDITION_ID,
)

#: The columns this adapter reads.  ``text_content`` is deliberately absent.
CATALOG_COLUMNS: tuple[str, ...] = (
    "document_id",
    "docket_id",
    "agency_code",
    "title",
    "document_type",
    "posted_date",
    "modify_date",
    "comment_start_date",
    "comment_end_date",
    "file_url",
    "attachments_json",
    "fr_doc_num",
    "withdrawn",
    "reason_withdrawn",
    "additional_rins",
    "text_extraction_status",
)

#: Extension to media type.  A locator whose extension is not named here gets
#: ``application/octet-stream``: the source declares a format and never a media
#: type, so an unrecognized one is reported as unknown rather than invented.
MEDIA_TYPES: Mapping[str, str] = {
    "pdf": "application/pdf",
    "htm": "text/html",
    "html": "text/html",
    "txt": "text/plain",
    "rtf": "application/rtf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "xls": "application/vnd.ms-excel",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
    "xml": "application/xml",
    "json": "application/json",
}

#: The observation keys this adapter may emit, so a reader can enumerate them.
OBSERVATION_KEYS: tuple[str, ...] = (
    "commentStartDate",
    "frDocNum",
    "reasonWithdrawn",
    "textExtractionStatus",
    "unparsableCommentEndDate",
    "unparsableModifyDate",
    "unparsablePostedDate",
    "unnormalizedRegulationIdentifierNumber",
)


def _text(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _strict_date(value: object) -> tuple[str | None, str | None]:
    """Split a source-stated instant into ``(calendar date, unreadable raw)``.

    Exactly one half is ever set.  A value the source never stated yields
    neither: nothing was said, so there is nothing to report as unreadable.
    """

    text = _text(value)
    if text is None:
        return None, None
    if INSTANT_RE.fullmatch(text) is None:
        return None, text
    head = text[:10]
    try:
        date.fromisoformat(head)
    except ValueError:
        return None, text
    return head, None


def _media_type(locator: str, declared_format: object = None) -> str:
    for candidate in (_text(declared_format), locator.rsplit(".", 1)[-1] if "." in locator else None):
        if candidate is not None:
            media_type = MEDIA_TYPES.get(candidate.lower())
            if media_type is not None:
                return media_type
    return DEFAULT_MEDIA_TYPE


def _json_list(value: object) -> list[Any]:
    text = _text(value)
    if text is None:
        return []
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _regulation_identifier_numbers(row: Mapping[str, Any]) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Split the row's RIN list into conforming and non-conforming values."""

    values = [item for item in _json_list(row.get("additional_rins")) if isinstance(item, str) and item]
    conforming = tuple(sorted({value for value in values if RIN_RE.fullmatch(value)}))
    other = tuple(sorted({value for value in values if not RIN_RE.fullmatch(value)}))
    return conforming, other


def _attachments(row: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    return [item for item in _json_list(row.get("attachments_json")) if isinstance(item, Mapping)]


def _attachment_urls(row: Mapping[str, Any]) -> set[str]:
    return {url for attachment in _attachments(row) if (url := _text(attachment.get("url"))) is not None}


def _mirror_renditions(document_id: str, mirror_index: MirrorIndex | None) -> tuple[CandidateRendition, ...]:
    """The verified mirror objects held for one document, best family first.

    Every one carries an ``expectedSha256`` because the index was built by
    fetching those exact bytes under their listed ETag and digesting them —
    the same discipline ``source_catalog.mirrulations`` applies to a draw, and
    the reason the mirror outranks a locator whose bytes nobody has read.
    """

    if mirror_index is None:
        return ()
    entries = mirror_index.get(document_id) or ()
    renditions: list[CandidateRendition] = []
    for position, entry in enumerate(sorted(entries, key=lambda item: str(item["key"]))):
        key = str(entry["key"])
        suffix = f"-{position}" if position else ""
        renditions.append(
            CandidateRendition(
                rendition_id=f"{MIRROR_RENDITION_ID}{suffix}",
                media_type=_media_type(key),
                locator=MIRROR_URL_TEMPLATE.format(bucket=MIRROR_BUCKET, key=quote(key, safe="/")),
                expected_sha256=str(entry["sha256"]),
                expected_byte_size=int(entry["size"]),
            )
        )
    return tuple(renditions)


def _federal_register_renditions(
    row: Mapping[str, Any], federal_register: Mapping[str, Mapping[str, Any]] | None
) -> tuple[CandidateRendition, ...]:
    """The Federal Register locators for a document that states its FR number.

    No digest and no size: the FR table states addresses, not bytes.  The join
    is on the document's own ``frDocNum``; nothing here reaches an FR document
    through a shared docket, which would attach another document's rendition
    to this one.
    """

    if federal_register is None:
        return ()
    number = _text(row.get("fr_doc_num"))
    if number is None:
        return ()
    record = federal_register.get(number)
    if record is None:
        return ()
    renditions: list[CandidateRendition] = []
    for rendition_id, field_name in (
        (f"{FEDERAL_REGISTER_RENDITION_ID}-pdf", "pdf_url"),
        (f"{FEDERAL_REGISTER_RENDITION_ID}-html", "html_url"),
    ):
        locator = _text(record.get(field_name))
        if locator is None:
            continue
        renditions.append(
            CandidateRendition(
                rendition_id=rendition_id,
                media_type=_media_type(locator) if field_name == "pdf_url" else "text/html",
                locator=locator,
                expected_sha256=None,
                expected_byte_size=None,
            )
        )
    return tuple(renditions)


def _renditions(row: Mapping[str, Any]) -> tuple[CandidateRendition, ...]:
    """Every distinct locator the row offers, with the sizes it declares.

    ``expectedSha256`` is ``null`` on all of them.  The catalog declares a byte
    size for an attachment and never a digest, and a digest this producer has
    not verified against those exact bytes would be a guess.
    """

    declared_sizes: dict[str, int] = {}
    declared_formats: dict[str, str] = {}
    for attachment in _attachments(row):
        url = _text(attachment.get("url"))
        if url is None:
            continue
        size = attachment.get("size")
        if isinstance(size, int) and not isinstance(size, bool) and size >= 0:
            declared_sizes.setdefault(url, size)
        attachment_format = _text(attachment.get("format"))
        if attachment_format is not None:
            declared_formats.setdefault(url, attachment_format)

    renditions: list[CandidateRendition] = []
    claimed: set[str] = set()
    file_url = _text(row.get("file_url"))
    if file_url is not None:
        claimed.add(file_url)
        renditions.append(
            CandidateRendition(
                rendition_id=FILE_RENDITION_ID,
                media_type=_media_type(file_url, declared_formats.get(file_url)),
                locator=file_url,
                expected_sha256=None,
                expected_byte_size=declared_sizes.get(file_url),
            )
        )
    # Sorted by locator and numbered densely, so the same row always yields the
    # same rendition identities however the source ordered its attachments.
    attachments = 0
    for url in sorted(_attachment_urls(row)):
        if url in claimed:
            continue
        claimed.add(url)
        renditions.append(
            CandidateRendition(
                rendition_id=f"{ATTACHMENT_RENDITION_ID_PREFIX}{attachments}",
                media_type=_media_type(url, declared_formats.get(url)),
                locator=url,
                expected_sha256=None,
                expected_byte_size=declared_sizes.get(url),
            )
        )
        attachments += 1
    return tuple(renditions)


def _observations(
    row: Mapping[str, Any], unreadable: Mapping[str, str | None], other_rins: Sequence[str]
) -> tuple[Observation, ...]:
    """A bounded, deterministic set of row-stated facts the ten fields miss."""

    pairs: list[tuple[str, str]] = []
    for key, value in (
        ("commentStartDate", row.get("comment_start_date")),
        ("frDocNum", row.get("fr_doc_num")),
        ("reasonWithdrawn", row.get("reason_withdrawn")),
        ("textExtractionStatus", row.get("text_extraction_status")),
    ):
        text = _text(value)
        if text is not None:
            pairs.append((key, text))
    for key, raw in unreadable.items():
        if raw is not None:
            pairs.append((key, raw))
    pairs.extend(("unnormalizedRegulationIdentifierNumber", value) for value in other_rins)
    return tuple(Observation(observation_key=key, observation_value=value) for key, value in sorted(pairs))


def _source_native_metadata(row: Mapping[str, Any]) -> dict[str, Any]:
    """The row's stated columns, verbatim.  An unstated column is not carried."""

    return {column: row[column] for column in CATALOG_COLUMNS if row.get(column) is not None}


def _source_issued_version(row: Mapping[str, Any], document_id: str) -> str:
    """The version label the SOURCE issued, never a capture digest."""

    for candidate in (row.get("modify_date"), row.get("posted_date")):
        text = _text(candidate)
        if text is not None:
            return text
    raise SourceCatalogError(f"{document_id}: the published catalog states neither a modify nor a posted date")


def resolve_renditions(
    row: Mapping[str, Any],
    document_id: str,
    *,
    mirror_index: MirrorIndex | None = None,
    federal_register: Mapping[str, Mapping[str, Any]] | None = None,
    preference: Sequence[str] = DEFAULT_RENDITION_PREFERENCE,
) -> tuple[CandidateRendition, ...]:
    """Take the highest-ranked family that has anything, and only that one.

    The declared order is a preference, not a merge: an item whose bytes the
    mirror already holds does not also carry a locator nobody has read, so a
    consumer reading ``candidateRenditions`` sees the rendition to capture
    rather than a menu it has to re-rank. Every family the universe does not
    name is skipped entirely, so a source can be switched off by declaration.
    """

    families: dict[str, tuple[CandidateRendition, ...]] = {
        MIRROR_RENDITION_ID: _mirror_renditions(document_id, mirror_index),
        FILE_RENDITION_ID: _renditions(row),
        FEDERAL_REGISTER_RENDITION_ID: _federal_register_renditions(row, federal_register),
    }
    for family in preference:
        offered = families.get(family)
        if offered:
            return offered
    return ()


def discovered_item(
    row: Mapping[str, Any],
    *,
    mirror_index: MirrorIndex | None = None,
    federal_register: Mapping[str, Mapping[str, Any]] | None = None,
    preference: Sequence[str] = DEFAULT_RENDITION_PREFERENCE,
) -> DiscoveredItem:
    """Report one discovery record for one published-catalog row."""

    document_id = _text(row.get("document_id"))
    if document_id is None:
        raise SourceCatalogError("the published catalog holds a row with no document identifier")

    posted, unreadable_posted = _strict_date(row.get("posted_date"))
    modified, unreadable_modified = _strict_date(row.get("modify_date"))
    closed, unreadable_closed = _strict_date(row.get("comment_end_date"))
    conforming_rins, other_rins = _regulation_identifier_numbers(row)
    agency_code = _text(row.get("agency_code"))
    docket_id = _text(row.get("docket_id"))

    draft = NormalizedDraft(
        title=_text(row.get("title")),
        agency_ids=(agency_code,) if agency_code else (),
        document_type=_text(row.get("document_type")),
        publication_date=posted,
        last_updated_date=modified,
        docket_ids=(docket_id,) if docket_id else (),
        regulation_identifier_numbers=conforming_rins,
        comment_close_date=closed,
        # The table states no per-item address; the universe declares its form.
        source_url=None,
    )
    withdrawn = _text(row.get("withdrawn")) == "true"
    return DiscoveredItem(
        source_item_id=f"{SOURCE_ITEM_ID_PREFIX}{document_id}",
        document_id=document_id,
        source_issued_version=_source_issued_version(row, document_id),
        source_native_metadata=_source_native_metadata(row),
        normalized=draft,
        # A Regulations.gov document record carries no topic vocabulary.
        observed_topics=(),
        observations=_observations(
            row,
            {
                "unparsableCommentEndDate": unreadable_closed,
                "unparsableModifyDate": unreadable_modified,
                "unparsablePostedDate": unreadable_posted,
            },
            other_rins,
        ),
        # A withdrawn document is the source's own settlement, so it offers
        # nothing to capture.
        renditions=()
        if withdrawn
        else resolve_renditions(
            row,
            document_id,
            mirror_index=mirror_index,
            federal_register=federal_register,
            preference=preference,
        ),
        outcome=SourceOutcome.DELETED if withdrawn else SourceOutcome.AVAILABLE,
        outcome_reason_code="source.withdrawn-after-publication" if withdrawn else None,
        outcome_reason=(
            "The source marks this document withdrawn"
            + (f": {row['reason_withdrawn']}" if _text(row.get("reason_withdrawn")) else ".")
        )
        if withdrawn
        else None,
    )


def discover_published_catalog(
    catalog_path: Path | str,
    *,
    batch_size: int = 50_000,
    mirror_index: MirrorIndex | None = None,
    federal_register: Mapping[str, Mapping[str, Any]] | None = None,
    preference: Sequence[str] = DEFAULT_RENDITION_PREFERENCE,
) -> Iterator[DiscoveredItem]:
    """Stream one discovery record per row of the published catalog.

    Rows arrive in Parquet order and in batches, so the whole table never has
    to be held as Arrow and as Python objects at the same time.  Membership is
    decided downstream; this yields every row the table holds.
    """

    import pyarrow.parquet as pq

    path = Path(catalog_path)
    try:
        parquet = pq.ParquetFile(path)
    except (OSError, ValueError) as error:
        raise SourceCatalogError(f"the published catalog is unreadable: {path} ({error})") from error

    present = set(parquet.schema_arrow.names)
    missing = [column for column in CATALOG_COLUMNS if column not in present]
    if missing:
        raise SourceCatalogError(f"the published catalog is missing columns this adapter reads: {missing}")

    for batch in parquet.iter_batches(batch_size=batch_size, columns=list(CATALOG_COLUMNS)):
        for row in batch.to_pylist():
            yield discovered_item(
                row,
                mirror_index=mirror_index,
                federal_register=federal_register,
                preference=preference,
            )


__all__ = [
    "ATTACHMENT_RENDITION_ID_PREFIX",
    "CATALOG_COLUMNS",
    "DEFAULT_RENDITION_PREFERENCE",
    "FEDERAL_REGISTER_RENDITION_ID",
    "MIRROR_BUCKET",
    "MirrorIndex",
    "FILE_RENDITION_ID",
    "MEDIA_TYPES",
    "OBSERVATION_KEYS",
    "SOURCE_ITEM_ID_PREFIX",
    "discover_published_catalog",
    "discovered_item",
    "resolve_renditions",
]
