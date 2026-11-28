"""Reports, letters, hearing cover links and their own acquisition checkpoints.

MODS cover links and the CBO letter rule use bytes already fetched. Agenda
acquisition is deferred: this pass has no verified meeting-to-jacket join or
House repository locator. Pending reads precede discovery under the same cap;
completed empty covers are checkpointed without inventing a link row. A report
is one row per published part and dated package revision: every part is read
or none, and that revision's part rows are replaced as a set. The current
checkpoint stays keyed by package.
"""

from __future__ import annotations

import hashlib
import json
import os
from collections import Counter
from collections.abc import Callable, Collection, Iterator, Mapping
from dataclasses import asdict, dataclass, replace
from datetime import UTC, date, datetime, timedelta
from functools import cache, partial
from importlib.metadata import version
from pathlib import Path
from typing import Any, Protocol

import duckdb
import httpx
from loguru import logger
from spicy_docs.extraction.body_text import BodyText, body_text
from spicy_docs.extraction.derivation import BODY_TEXT_DERIVATION_VERSION, DERIVATION_READS_AS
from spicy_docs.extraction.model import PageContent, PageResult, TextBlock
from spicy_docs.interpretation.cbo_estimates import read_cbo_estimate, recital_bill_id
from spicy_docs.interpretation.hearing_bill_links import cover_links
from spicy_docs.schemas.hearing_bill_link_tables import shape_hearing_bill_link
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.committee_report_tables import (
    shape_committee_report,
    shape_committee_report_cbo_letter,
    shape_hearing_transcript,
    shape_report_section,
)
from spicy_docs.schemas.tables import natural_key
from spicy_docs.sources.agency_reports.report_blocks import parse_agency_blocks
from spicy_docs.sources.govinfo.bodies import (
    GovInfoBodySourceError,
    GovInfoPartsRefusedError,
    ModsBill,
    parse_package_id,
    publisher_body_status,
)
from spicy_docs.sources.govinfo.body_acquisition import (
    GovInfoBodyAcquirer,
    GovInfoBodyBudget,
    GovInfoFormatNotOfferedError,
    GovInfoPartsOverBudgetError,
)
from spicy_docs.reading.refusals import RefusedResponse
from spicy_docs.reading.paged_json import PagedJsonSourceError, PagedJsonUnavailableError
from spicy_docs.schemas.tables import text
from spicy_docs.sources.congress.listing import LIST_ROUTES, CongressListingReader
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, collection_url
from spicy_docs.storage.blobs import iter_verified_blob
from spicy_docs.storage.raw_archive import CentralRawArchiveStore, default_raw_archive_root
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.govinfo_revisions import latest_read, revision_instant
from spicy_regs.sources import r2
from spicy_regs.sources.hearing_detail import hearing_detail_facts, hearing_detail_urls
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key, listing_reader
from spicy_regs.phase_log import annotate
from spicy_regs.transforms.table_merge import (
    merge_contract_table,
    merge_table,
    published_table,
    set_column,
    write_in_place,
)
from spicy_regs.transforms.committee_report_reads import (
    DETAIL_AWAITED,
    DETAIL_GRACE_DAYS,
    DETAIL_REFUSED,
    DETAIL_UNAVAILABLE,
    READ_COLUMNS,
    READS_TABLE,
    REFUSED_FINAL,
    RULE_VERSIONS,
    SIZE_BASIS,
    awaited,
    prior_reads,
    refusal_rule,
    settled,
)
from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError
from spicy_regs.sources.retained import RetainedGovInfoBodyAcquirer, RetainedGovInfoDiscoveryReader


class PackageDiscoverySource(Protocol):
    """What this transform needs of a GovInfo discovery reader."""

    def packages(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


class PackageBodySource(Protocol):
    """What this transform needs of a GovInfo package-body acquirer.

    ``acquire_parts`` reads a report, every part its record states or none;
    ``acquire`` reads a hearing, whose record states no parts. The rendition
    order is the acquirer's sealed default
    (``sources.govinfo.bodies.BODY_PREFERENCE`` — XML, HTML, text, then PDF);
    ``prefer`` is passed only as ``("pdf",)``, to read the PDF a part offers
    in place of the publisher's placeholder text.
    """

    def acquire(self, package_id: str, *, max_bytes: int | None = ..., prefer: tuple[str, ...] = ...) -> Any: ...

    def acquire_parts(self, package_id: str, *, max_bytes: int | None = ..., prefer: tuple[str, ...] = ...) -> tuple[Any, ...]: ...


class HearingDetailSource(Protocol):
    """What this transform needs of a Congress.gov listing reader: the ``hearing-detail`` route."""

    def records(self, route: Any, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


DISCOVERY_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Per package: summary and MODS, then one body per part (``2 + P``), paced,
#: with every retry drawn from the same count. Eight gives a two-part report,
#: the most any of the 145 retained CRPT packages states (spicy-docs
#: ``docs/tables.md``), four requests of retry headroom, and reads up to six
#: parts; a record stating seven or more is refused before any body request
#: (``GovInfoPartsOverBudgetError``), every run, until this grows.
BODY_BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=24 * 1024 * 1024,  # MAX_EVIDENCE_BYTES
    max_metadata_bytes=4 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=0.34,
)
#: The bounds a final refusal (:func:`_refusal_is_final`) is recorded under; raising either reads it again.
REFUSAL_BOUNDS = f"body={BODY_BUDGET.max_body_bytes};requests={BODY_BUDGET.max_requests}"

#: Cold-start window only: with no prior table there is no watermark, and
#: thirty days back covers a daily cron with a wide margin for an outage.
DEFAULT_WINDOW_DAYS = 30

#: Re-ask this far back from the stored watermark, so a package modified after
#: the previous run's cutoff is still seen.
OVERLAP_HOURS = 24

#: ~3 requests each (two keyed) at ~3/s. A 244-report reader refresh costs
#: ~732 requests (~4.1 minutes paced), and fits under the CRPT cap. Raising
#: only CRPT adds ~200 routine keyed requests at the cap; leave CHRG and its
#: additional Congress.gov detail requests on their existing budget.
MAX_PACKAGES_PER_RUN = 300
MAX_HEARING_PACKAGES_PER_RUN = 200

MAX_PAGES = 40


def _since(prior_file: Path | None = None, window_days: int = DEFAULT_WINDOW_DAYS,
           *, evidence: CaptureEvidence | None = None, now: datetime | None = None) -> str:
    """Last-modified floor from the selected prior's successful discovery, never imported rows.

    ``prior_file`` is retained for callers; its historical contents cannot establish
    a live cursor. Without qualified discovery state the declared cold window applies.
    """
    raw = os.environ.get("COMMITTEE_REPORTS_SINCE", "").strip()
    if raw:
        return raw if raw.endswith("Z") else f"{raw}T00:00:00Z"
    state = (evidence.inherited_event("committee-discovery") or {}) if evidence is not None else {}
    cutoff = datetime.fromisoformat(state["cursor"]) if state.get("cursor") else None
    start = (cutoff - timedelta(hours=OVERLAP_HOURS)) if cutoff else (now or datetime.now(UTC)) - timedelta(days=window_days)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ")


def _package_ids(reader: PackageDiscoverySource, collection: str, since: str,
                 evidence: CaptureEvidence | None = None, *, until: str | None = None) -> dict[str, str | None]:
    """Enumerate within the page cap; use the package grammar before spending body requests."""
    found = {}
    unsupported = 0
    for page in reader.packages(collection_url(collection, since, until), max_pages=MAX_PAGES):
        if evidence:
            evidence.capture(page.capture, stage=collection + ":listing")
        for record in page.records:
            package_id = record.get("packageId")
            try:
                identity = parse_package_id(package_id)
            except GovInfoBodySourceError:
                unsupported += 1
                continue
            if identity.collection == collection:
                found[str(package_id)] = text(record.get("lastModified"))
    logger.info("{}: {} supported packages in window, {} unsupported IDs", collection, len(found), unsupported)
    return found


#: The ``publisher_body_status`` a read acts on: the text rendition is the
#: publisher's own notice that the text is only in the PDF.
PLACEHOLDER = "publisher_placeholder"


@dataclass(frozen=True, slots=True)
class ReadBody:
    """One body with extracted text and optional newly computed page details."""

    body: Any
    text: str
    completeness: str
    text_sha256: str
    derivation: str
    page_count: int | None
    extracted: BodyText | None = None


def _read(body: Any) -> ReadBody:
    """The text derivation for one fetched body's own rendition, with its status and digest."""
    # No extractor argument: ``body_text``'s default (``DocumentExtractor(NativeText())``,
    # PyMuPDF) is the pipeline ``gpo_normalize`` was derived on — the GPO
    # gutter-numbered layout only comes through on PyMuPDF's own line
    # adjacency, not pypdf's (measured
    # ``docs/research/gpo-normalizer-vs-upstream-2026-09-19.md`` in spicy-docs).
    derived = body_text(body)
    return ReadBody(
        body, derived.text, publisher_body_status(derived.text, rendition=derived.rendition),
        "sha256:" + hashlib.sha256(derived.text.encode("utf-8")).hexdigest(),
        derived.derivation, None if derived.pages is None else len(derived.pages), derived,
    )


def _part_key(body: Any, package_id: str) -> str:
    """The part a body is: its report part's id, or the package id for a hearing, whose record states none."""
    return package_id if body.part is None else body.part.part_id


def _retain(evidence: CaptureEvidence, bodies: Collection[Any], stage: str) -> None:
    """Retain one acquisition's summary and MODS once, then each body; every part repeats the first two."""
    first = next(iter(bodies))
    for capture in (first.summary_capture, first.mods_capture, *(body.body_capture for body in bodies)):
        evidence.capture(capture, stage=stage)


@cache
def _extraction_versions() -> dict[str, str]:
    """What a PDF's text is re-derived with from its retained bytes; read only once a PDF was extracted."""
    return {name: version(name) for name in ("spicy-docs", "pymupdf")}


def _prior_hearing_rows(path: Path | None, packages: Collection[str]) -> dict[tuple[str, str], dict]:
    """Read only selected prior hearings once, including their retained text and join witness."""
    if path is None or not packages:
        return {}
    with duckdb.connect(config={"threads": 1, "memory_limit": "512MiB"}) as db:
        available = {row[0] for row in db.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall()}
        columns = [name for name in ("package_id", "last_modified", "event_id", "body_text", "body_completeness",
                                     "text_sha256", "text_derivation", "page_count", "sha256", "format", "hearing_detail_json", "mods_event_ids_json")
                   if name in available]
        cursor = db.execute("SELECT " + ", ".join(columns) + " FROM read_parquet(?) WHERE package_id IN (SELECT unnest(?))",
                            [str(path), sorted(packages)])
        rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
    lookup = {}
    for row in rows:
        key = (row["package_id"], row["last_modified"])
        if key in lookup:
            raise ValueError("Prior hearing rows repeat a dated key")
        lookup[key] = row
    return lookup


def _qualified_prior_event(row: Mapping[str, Any], body: Any) -> bool:
    event = row.get("event_id")
    if event is None:
        return False
    if set(body.mods.event_ids) == {event}:
        return True
    detail = json.loads(row.get("hearing_detail_json") or "null")
    return (isinstance(detail, dict) and detail.get("relationship") == "mods-corroborated"
            and detail.get("event_id") == event
            and set(json.loads(row.get("mods_event_ids_json") or "[]")) == {event})


def _reuse_hearing_text(body: Any, prior: Mapping[tuple[str, str], dict], prior_read: Mapping[str, Any]) -> ReadBody | None:
    tokens: dict[str, str] = {}
    for part in (prior_read.get("rule_version") or "").split(";"):
        if "=" in part:
            key, value = part.split("=", 1)
            tokens[key] = value
    old_version = tokens.get("text")
    current = BODY_TEXT_DERIVATION_VERSION
    while current != old_version and current in DERIVATION_READS_AS:
        earlier, renditions = DERIVATION_READS_AS[current]
        if body.format not in renditions:
            break
        current = earlier
    if current != old_version or (body.format == "pdf" and tokens.get("pymupdf") != version("pymupdf")):
        return None
    stamp = prior_read.get("last_modified")
    if not isinstance(stamp, str):
        return None
    row = prior.get((body.identity.package_id, stamp))
    if (row is None or row.get("body_text") is None or row.get("sha256") != body.body_capture.sha256
            or row.get("format") != body.format):
        return None
    text = row["body_text"]
    if "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest() != row["text_sha256"]:
        raise ValueError("Retained hearing text differs from its digest")
    pages = row.get("page_count")
    return ReadBody(body, text, row["body_completeness"], row["text_sha256"], row["text_derivation"],
                    None if pages is None else int(pages))


def _read_bodies(acquirer: PackageBodySource, package_id: str, collection: str,
                 evidence: CaptureEvidence | None = None, *,
                 reuse: Callable[[Any], ReadBody | None] | None = None) -> list[ReadBody]:
    """Each fetched body of one package with the one text derivation for its rendition.

    A report is every part its record states, read by ``acquire_parts``: one
    summary and one MODS, then one body per part, all or nothing, so a
    package's part rows are never half replaced. A hearing's record states no
    parts, so it is one ``acquire``. Takes the narrow Protocol rather than the
    concrete acquirer, the way the bill family's ``_version_captures`` does:
    what this transform depends on is the facts ``body_text`` reads off a
    fetched body, and the rendition it was fetched in is the body's own, never
    this caller's guess.

    A part whose text is the publisher's placeholder is read again from the
    PDF its record offers, under that PDF's own digest; the placeholder's
    capture is kept beside it. A part offering no PDF keeps its placeholder,
    marked, since nothing more is published, and so does one whose PDF every
    later run would be refused in the same way (:func:`_refusal_is_final`),
    so the package is not re-read forever; a transient failure refuses it. The journal records digests and
    counts, never text: the bytes are retained, and a PDF's text is re-derived
    from them with the recorded versions.
    """
    bodies = acquirer.acquire_parts(package_id) if collection == "CRPT" else (acquirer.acquire(package_id),)
    if evidence:
        _retain(evidence, bodies, package_id + ":used")
    stamps = {body.summary.last_modified for body in bodies}
    if len(stamps) != 1:
        raise ValueError(f"{package_id}: report parts disagree on publisher last_modified")
    revision_instant(next(iter(stamps)))
    def read_or_reuse(body: Any) -> ReadBody:
        reused = reuse(body) if reuse is not None else None
        if reused is not None:
            if evidence:
                evidence.event("body-text-reused", package_id=package_id,
                               source_sha256=body.body_capture.sha256, text_sha256=reused.text_sha256)
            return reused
        return _read(body)

    reads = [read_or_reuse(body) for body in bodies]
    keys = [_part_key(read.body, package_id) for read in reads]
    placeholders = {key: read for key, read in zip(keys, reads, strict=True) if read.completeness == PLACEHOLDER}
    offered = [key for key, read in placeholders.items()
               if read.body.format != "pdf" and "pdf" in read.body.offered_formats]
    if evidence:
        for key, read in placeholders.items():
            evidence.event("body-placeholder", package_id=package_id, part_id=key,
                           source_sha256=read.body.body_capture.sha256, text_sha256=read.text_sha256,
                           text_chars=len(read.text), offered_formats=list(read.body.offered_formats))
    if offered:
        # A report's parts are read all or none, so every part is asked for as
        # PDF and only the placeholder parts take theirs. A failure refuses the
        # package, which stays pending until a read succeeds.
        try:
            pdfs = (acquirer.acquire_parts(package_id, prefer=("pdf",)) if collection == "CRPT"
                    else (acquirer.acquire(package_id, prefer=("pdf",)),))
        except GovInfoBodySourceError as error:
            if not _refusal_is_final(error):
                raise
            logger.warning("{}: offered PDF refused for good; placeholder kept for {}: {}", package_id, offered, error)
            if evidence is not None:
                evidence.refusal(error, stage=package_id + ":pdf-fallback")
            return reads
        by_part = {_part_key(body, package_id): body for body in pdfs}
        if missing := [key for key in offered if key not in by_part]:
            raise ValueError(f"PDF acquisition did not return placeholder parts {missing}")
        if evidence:
            _retain(evidence, [by_part[key] for key in offered], package_id + ":pdf-fallback")
        reads = [read_or_reuse(by_part[key]) if key in offered else prior for key, prior in zip(keys, reads, strict=True)]
        if any(item.body.summary.last_modified not in stamps for item in reads):
            raise ValueError(f"{package_id}: publisher revision changed during PDF fallback")
    if evidence:
        for key, read in zip(keys, reads, strict=True):
            if read.extracted is not None and read.extracted.pages is not None:
                replaced = placeholders[key].body.body_capture.sha256 if key in offered else None
                evidence.event("body-text", package_id=package_id, part_id=key,
                               source_sha256=read.body.body_capture.sha256, replaces_sha256=replaced,
                               text_sha256=read.text_sha256, text_chars=len(read.text),
                               page_count=read.page_count, derivation=read.derivation,
                               extraction_versions=_extraction_versions(), cleanup=asdict(read.extracted.record),
                               body_completeness=read.completeness)
    return reads


def _refusal_is_final(error: Exception) -> bool:
    """Whether a later run would get the same refusal: the publisher's record and this run's bounds decide it.

    A rendition a part does not offer, a report whose parts exceed the request
    budget, a record stating its parts in a shape the reader refuses
    (CRPT-119hrpt853 states two parts as part 1), and a body past the byte bound
    (the transport's ``response-byte-limit`` refusal) repeat until the publisher
    changes the package, which moves its ``last_modified``, the reader changes,
    which moves the rule, or the bound grows past it (:func:`_refusal_basis`).
    Anything else may be transient.
    """
    if isinstance(error, (GovInfoFormatNotOfferedError, GovInfoPartsOverBudgetError, GovInfoPartsRefusedError)):
        return True
    refused = getattr(error, "refused_response", None)
    return isinstance(refused, RefusedResponse) and refused.unavailable_reason == "response-byte-limit"


def _refusal_basis(error: Exception) -> str:
    """What a final refusal rests on: the size its response stated, else the bounds that refused it.

    A body past the byte bound states its ``Content-Length`` (the 1946 parts'
    312,487,940 and 373,504,330 bytes), so it is final on that size against the
    bound, not on the bound's value; a record stating too many parts or parts the
    reader refuses, no rendition taken, or an unstated length rests on :data:`REFUSAL_BOUNDS`.
    """
    refused = getattr(error, "refused_response", None)
    if isinstance(refused, RefusedResponse) and refused.stated_byte_size is not None:
        return f"{SIZE_BASIS}{refused.stated_byte_size}"
    return REFUSAL_BOUNDS


def _settled(row: dict, collection: str, modified: str | None = None) -> bool:
    return settled(row, collection, modified, bounds=REFUSAL_BOUNDS, max_body_bytes=BODY_BUDGET.max_body_bytes)


def _read_this_run(row: dict, collection: str) -> bool:
    """A package this run read: complete, or a hearing whose body read even if its optional Congress.gov detail
    is awaited, unavailable, or transiently refused."""
    return row.get("outcome") in {"complete", DETAIL_REFUSED}


def _section_input(derived: BodyText) -> str | tuple[PageResult, ...]:
    """What ``parse_agency_blocks`` reads: the pages, where the rendition states them, so a section cites its pages.

    ``BodyText.text`` is its pages joined by one ``\\n``, the join
    ``parse_agency_blocks`` makes, so a section's offsets are the same either
    way. Page ``N`` is the PDF's ``N``-th page, as extraction numbers it.
    """
    if derived.pages is None:
        return derived.text
    return tuple(PageResult({"page": number}, PageContent((TextBlock(page),), ()))
                 for number, page in enumerate(derived.pages, start=1))


#: The one MODS ``context`` that states a package is *about* a bill, as
#: opposed to mentioning it (``OTHER``, ``COVER``, ``BODY``, or none). The
#: rule itself is ``PackageModsIdentity.primary_bill``'s; this names what the
#: mention count leaves out.
PRIMARY_BILL_CONTEXT = "PRIMARY"


def _bill_key(package_id: str, bill: ModsBill) -> str | None:
    """The bill's natural key, or None when the publisher's type is outside the vocabulary.

    ``normalized_bill_type`` is spicy-docs' own lower-casing of the MODS
    ``type``, checked against ``BILL_TYPES``; a ``<bill>`` spelled in a type
    that vocabulary lacks is logged and not linked, rather than published as a
    key with a hole in it.
    """
    if bill.normalized_bill_type is None:
        logger.warning("{}: MODS bill type {!r} is not a supported bill type; not linked", package_id, bill.bill_type)
        return None
    # ``int`` on purpose: the wheel keeps the publisher's digits verbatim
    # (``isdecimal()`` is guaranteed), while ``congress_bills.bill_id`` is
    # spelled from an int, so a zero-padded MODS number must collapse to the
    # key it can join on.
    return natural_key(bill.congress, bill.normalized_bill_type, int(bill.number))


#: What one hearing-detail request can refuse with, short of a credential
#: refusal, which propagates: the reader's own refusals (``404`` included), a
#: transport failure after its retries, and a record not shaped as expected.
_HEARING_REFUSALS = (PagedJsonSourceError, httpx.HTTPError, ConnectionError, ValueError, TypeError, KeyError)


def _hearing_detail(hearings: HearingDetailSource, package: Any,
                    evidence: CaptureEvidence | None = None) -> tuple[str | None, str, dict]:
    """Keep a detail observation separate; qualify a dated meeting only with its MODS witness."""
    identity = package.identity
    stage = identity.package_id + ":hearing-detail"
    try:
        urls = hearing_detail_urls(identity)
        for index, url in enumerate(urls):
            try:
                page = next(iter(hearings.records(LIST_ROUTES["hearing-detail"], url, max_pages=1)))
                break
            except PagedJsonUnavailableError:
                if index == len(urls) - 1:
                    raise
        if evidence:
            evidence.capture(page.capture, stage=stage)
        detail = hearing_detail_facts(identity, page.records, page.capture)
    except CredentialRefusedError:
        raise
    except _HEARING_REFUSALS as error:
        if evidence:
            evidence.refusal(error, stage=stage)
        outcome = "unavailable" if isinstance(error, PagedJsonUnavailableError) else "refused"
        logger.warning("CHRG: {} hearing detail {}: {}", identity.package_id, outcome,
                       scrub_credential(str(error), evidence.credential if evidence else ""))
        return None, outcome, {"state": outcome, "relationship": "unproved"}

    event_id = detail["event_id"]
    native_ids = set(package.mods.event_ids)
    relation = "not-stated"
    if event_id is not None:
        relation = ("mods-corroborated" if native_ids == {event_id}
                    else "conflicting-or-multiple-mods-ids" if native_ids else "detail-only")
    detail["relationship"] = relation
    if evidence:
        evidence.event("hearing-detail-observation", **detail)
    return (event_id if relation == "mods-corroborated" else None,
            "meeting" if event_id is not None else "no_meeting", detail)


#: Which detail answer a checkpoint keeps when a package's bodies disagree (higher wins).
_DETAIL_RANK = {
    refusal_rule("CHRG", DETAIL_UNAVAILABLE): 0,
    refusal_rule("CHRG", DETAIL_AWAITED): 1,
    RULE_VERSIONS["CHRG"]: 2,
}


def _today() -> date:
    """The run's UTC date; the one clock the hearing-detail grace window reads."""
    return datetime.now(UTC).date()


def _unavailable_rule(package: Any, collection: str) -> str:
    """The checkpoint rule for a hearing whose detail Congress.gov does not have: awaited, then settled.

    Age runs from the package's GovInfo ``dateIssued`` (the summary's), else
    the date of its ``last_modified``. Within :data:`DETAIL_GRACE_DAYS` it is
    asked again every run but is not pending; past it, it is settled.
    """
    stamp = package.summary.date_issued or package.summary.last_modified or ""
    try:
        issued = date.fromisoformat(stamp[:10])
    except ValueError:
        issued = None
    within = issued is not None and (_today() - issued).days <= DETAIL_GRACE_DAYS
    return refusal_rule(collection, DETAIL_AWAITED if within else DETAIL_UNAVAILABLE)


def _retry_order(row: dict, modified: str | None) -> tuple[bool, str]:
    """Where a package goes in the cap: never attempted at this stamp first, then retries, oldest attempt first.

    A fixed order let the same first 200 unsettled hearings take the cap every
    night (run 42558022791). A package new to the listing, changed since its
    checkpoint, or deferred by the cap last run (its checkpoint still
    ``pending``) has not been attempted at this stamp; a refusal has, at its
    ``observed_at``, and goes behind every older one. Ties keep their order.
    """
    retry = row.get("outcome") not in (None, "pending") and row.get("last_modified") == modified
    return retry, (row.get("observed_at") or "") if retry else ""


def _refuse_unsafe_reread(reread: Collection[str], checkpointed: set[str], reads: dict[str, dict]) -> None:
    """Refuse, before any source request, a named re-read that would publish state no read established.

    An id without a prior checkpoint (a typo, or no prior at all) would publish
    a refused checkpoint that every scheduled run retries; a prior row without
    one would publish the pending checkpoint :func:`prior_reads` invents for it,
    with no clock, since only discovery reads it.
    """
    if unknown := sorted(set(reread) - checkpointed):
        raise ValueError(f"reread names packages with no prior checkpoint: {unknown}")
    if unchecked := sorted(set(reads) - checkpointed):
        raise ValueError(f"reread needs every prior row checkpointed; run discovery first: {unchecked}")


def _link_committee_codes(links: Path, transcripts: Path) -> int:
    """Give each link its hearing's first committee code as ``hearing_transcripts`` states it; how many moved.

    spicy-docs' link rule publishes the MODS ``authorityId`` as the record spells it (CHRG-119hhrg61487 states
    ``HSAS00``), while its transcript shaper lower-cases each code the way ``committees.system_code`` keys it, root
    first and then each granule. Taking the shaper's first code keeps one statement of a hearing's committee and
    no second copy of its fold, on every merged row, so a held link needs no re-read. A link whose hearing row
    predates ``committee_system_codes_json`` (NULL) keeps its own code. O(links + hearings).
    """
    import pyarrow.parquet as pq

    stated = pq.read_table(transcripts, columns=["package_id", "last_modified", "committee_system_codes_json"]).to_pylist()
    first = {(row["package_id"], row["last_modified"]): next(iter(json.loads(row["committee_system_codes_json"])), None)
             for row in stated if row["committee_system_codes_json"] is not None}
    table = pq.read_table(links)
    held = table["committee_system_code"].to_pylist()
    codes = [first.get((package_id, modified), code) for package_id, modified, code in zip(
        table["package_id"].to_pylist(), table["last_modified"].to_pylist(), held)]
    moved = sum(new != old for new, old in zip(codes, held))
    if moved:
        write_in_place(set_column(table, "committee_system_code", codes), links)
    return moved


_RETAINED_METADATA_LIMIT = 8 * 1024 * 1024
_RETAINED_MAX_FILE_BYTES = 16 * 1024 * 1024
_RETAINED_MAX_TOTAL_BYTES = 64 * 1024 * 1024
_RETAINED_MAX_REVISIONS = 50
_HEX_CHARS = frozenset("0123456789abcdef")


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _bounded_bytes(path: Path, limit: int) -> bytes:
    """Read a resident local file within ``limit``, refusing cloud-dataless or mutating inputs."""
    stat = path.stat()
    if getattr(stat, "st_flags", 0) & 0x40000000:
        raise ValueError(f"Cloud-dataless evidence is outside retained replay: {path.name}")
    if stat.st_size > limit:
        raise ValueError(f"Retained evidence exceeds its admitted byte bound: {path.name}")
    payload = path.read_bytes()
    if len(payload) != stat.st_size:
        raise ValueError(f"Retained evidence changed while reading: {path.name}")
    return payload


def _captured_response(event: Mapping[str, Any], payload: bytes) -> CapturedBodyResponse:
    """Build a verified ``CapturedBodyResponse`` from a retained capture event and its checked bytes."""
    for field in ("requested_url", "resolved_url", "observed_at"):
        if not isinstance(event.get(field), str) or not event[field]:
            raise ValueError(f"Retained capture event lacks {field}")
    status = event.get("status_code")
    if type(status) is not int or status != 200:
        raise ValueError("Retained capture event must record a complete HTTP 200 response")
    return CapturedBodyResponse(
        requested_url=event["requested_url"],
        resolved_url=event["resolved_url"],
        status_code=status,
        content_type=event.get("content_type"),
        observed_at=event["observed_at"],
        body=payload,
        content_encoding=str(event.get("content_encoding") or "identity"),
        method=str(event.get("method") or "GET"),
    )


def _retained_http_response(capture: CapturedBodyResponse) -> httpx.Response:
    """Replay one verified retained capture through httpx while preserving its original capture clock."""
    return httpx.Response(
        capture.status_code,
        headers={"Content-Type": capture.content_type or ""},
        stream=httpx.ByteStream(capture.body),
        extensions={"spicy_docs_retained_observed_at": capture.observed_at},
    )


@dataclass(frozen=True, slots=True)
class RetainedHearingRevision:
    """One verified historical hearing revision bound to preserved wrapper events and original bytes."""

    package_id: str
    last_modified: str
    source_evidence_artifact: str
    summary: CapturedBodyResponse
    mods: CapturedBodyResponse
    body: CapturedBodyResponse
    detail: CapturedBodyResponse
    wrapper_lines: tuple[int, ...]
    wrapper_generation: str | None


@dataclass(frozen=True, slots=True)
class RetainedHearingBundle:
    """An audited, cryptographically verified offline cohort of withdrawn GovInfo hearing revisions."""

    pins_sha256: str
    wrapper_sha256: str
    wrapper_bytes: int
    revisions: tuple[RetainedHearingRevision, ...]
    unique_originals: int
    decoded_bytes: int

    @property
    def packages(self) -> tuple[str, ...]:
        return tuple(sorted({revision.package_id for revision in self.revisions}))


def load_retained_hearing_bundle(
    pins_path: Path,
    *,
    bundle_root: Path | None = None,
    raw_archive_root: Path | None = None,
) -> RetainedHearingBundle:
    """Load and cryptographically verify an audited retained-hearing pin selection before any merge.

    Accepts either a ``selected-capture-pins.json`` file or a directory containing it. Every pinned metadata file
    (including ``evidence-events.jsonl``), wrapper event binding, summary coordinate, and original blob SHA-256 and
    byte size is checked up front; any mismatch refuses before writing or modifying tables. Original bytes resolve
    first from ``<bundle_root>/originals/<sha256>`` (matching R2 ``archive/committee-reports-govinfo-withdrawn-*``)
    and fall back to ``CentralRawArchiveStore`` when present.
    """
    pins_path = Path(pins_path)
    if pins_path.is_dir():
        pins_file = pins_path / "selected-capture-pins.json"
        effective_root = Path(bundle_root) if bundle_root is not None else pins_path
    else:
        pins_file = pins_path
        effective_root = Path(bundle_root) if bundle_root is not None else None
    if not pins_file.is_file():
        raise ValueError(f"Retained bundle selection file is missing: {pins_file.name}")

    pins_payload = _bounded_bytes(pins_file, _RETAINED_METADATA_LIMIT)
    pins_sha256 = _sha256_bytes(pins_payload)
    selection = json.loads(pins_payload)
    if not isinstance(selection, dict):
        raise ValueError("Retained bundle selection must be a JSON object")

    metadata = selection.get("retained_bundle_metadata")
    selected = selection.get("selected")
    if not isinstance(metadata, dict) or "evidence-events.jsonl" not in metadata:
        raise ValueError("Retained bundle selection lacks retained_bundle_metadata['evidence-events.jsonl']")
    if not isinstance(selected, list) or not selected:
        raise ValueError("Retained bundle selection must name at least one revision coordinate")
    if len(selected) > _RETAINED_MAX_REVISIONS:
        raise ValueError(f"Retained bundle exceeds the {_RETAINED_MAX_REVISIONS}-revision bound")

    wrapper_pin = metadata["evidence-events.jsonl"]
    if (
        not isinstance(wrapper_pin, dict)
        or not isinstance(wrapper_pin.get("sha256"), str)
        or type(wrapper_pin.get("bytes")) is not int
    ):
        raise ValueError("Retained bundle evidence-events.jsonl pin lacks sha256 or byte size")

    if effective_root is not None:
        wrapper_path = effective_root / "evidence-events.jsonl"
        bundle_dir = effective_root
    elif (pins_file.parent / "evidence-events.jsonl").is_file():
        wrapper_path = pins_file.parent / "evidence-events.jsonl"
        bundle_dir = pins_file.parent
    elif isinstance(wrapper_pin.get("path"), str):
        wrapper_path = Path(wrapper_pin["path"])
        bundle_dir = wrapper_path.parent
    else:
        raise ValueError("Retained bundle cannot locate evidence-events.jsonl")

    wrapper_payload: bytes | None = None
    for name, meta in metadata.items():
        if not isinstance(name, str) or Path(name).name != name or not isinstance(meta, dict):
            raise ValueError("Retained bundle metadata entry is invalid")
        expected_sha = meta.get("sha256")
        expected_bytes = meta.get("bytes")
        if not isinstance(expected_sha, str) or type(expected_bytes) is not int or expected_bytes < 0:
            raise ValueError(f"Retained bundle metadata pin is incomplete: {name}")
        candidate = bundle_dir / name
        if effective_root is None and not candidate.is_file() and isinstance(meta.get("path"), str):
            candidate = Path(meta["path"])
        if not candidate.is_file():
            raise ValueError(f"Retained bundle metadata file is missing: {name}")
        payload = _bounded_bytes(candidate, _RETAINED_METADATA_LIMIT)
        if len(payload) != expected_bytes or _sha256_bytes(payload) != expected_sha:
            raise ValueError(f"Preserved bundle file differs from its selected SHA-256 or size: {name}")
        if name == "evidence-events.jsonl":
            wrapper_payload = payload
    assert wrapper_payload is not None
    wrappers = [json.loads(line) for line in wrapper_payload.splitlines()]

    archive_store: CentralRawArchiveStore | None = None
    archive_checked = False
    blob_cache: dict[str, bytes] = {}
    decoded_total = 0

    def read_original(digest: Any, byte_size: Any) -> bytes:
        nonlocal archive_store, archive_checked, decoded_total
        if not isinstance(digest, str) or not digest.startswith("sha256:"):
            raise ValueError("Retained original lacks a sha256 digest")
        hex_digest = digest.removeprefix("sha256:")
        if len(hex_digest) != 64 or not set(hex_digest) <= _HEX_CHARS:
            raise ValueError("Retained original digest is not a 64-hex sha256")
        if type(byte_size) is not int or not 0 <= byte_size <= _RETAINED_MAX_FILE_BYTES:
            raise ValueError("Retained original byte size is outside its admitted bound")
        if digest in blob_cache:
            cached = blob_cache[digest]
            if len(cached) != byte_size:
                raise ValueError("Retained original byte size conflicts with earlier binding")
            return cached
        original_file = bundle_dir / "originals" / hex_digest
        if original_file.is_file():
            payload = _bounded_bytes(original_file, _RETAINED_MAX_FILE_BYTES)
        else:
            if not archive_checked:
                archive_checked = True
                candidate_root = (
                    Path(raw_archive_root) if raw_archive_root is not None else default_raw_archive_root()
                )
                if candidate_root.is_dir():
                    archive_store = CentralRawArchiveStore(candidate_root, create=False)
            if archive_store is None:
                raise ValueError(f"Retained original blob is missing for {digest}")
            payload = b"".join(iter_verified_blob(archive_store, digest, byte_size))
        if len(payload) != byte_size or _sha256_bytes(payload) != digest:
            raise ValueError("Retained original differs from its pinned SHA-256 or byte size")
        decoded_total += len(payload)
        if decoded_total > _RETAINED_MAX_TOTAL_BYTES:
            raise ValueError("Retained bundle originals exceed the admitted total byte bound")
        blob_cache[digest] = payload
        return payload

    def require_capture_event(event: Any) -> Mapping[str, Any]:
        if (
            not isinstance(event, dict)
            or event.get("event") != "capture"
            or event.get("response_complete") is not True
            or event.get("body_retained") is not True
            or event.get("status_code") != 200
        ):
            raise ValueError("Retained event does not pin a complete retained HTTP 200 capture")
        return event

    revisions: list[RetainedHearingRevision] = []
    seen_coords: set[tuple[str, str]] = set()
    for row in selected:
        if not isinstance(row, dict):
            raise ValueError("Retained bundle coordinate entry must be an object")
        package_id = row.get("package_id")
        last_modified = row.get("last_modified")
        evidence_id = row.get("source_evidence_artifact")
        if not isinstance(package_id, str) or not isinstance(last_modified, str) or not isinstance(evidence_id, str):
            raise ValueError("Retained bundle coordinate lacks package_id, last_modified, or source_evidence_artifact")
        identity = parse_package_id(package_id)
        if identity.collection != "CHRG":
            raise ValueError(f"Retained hearing bundle supports CHRG packages only, got {package_id}")
        revision_instant(last_modified)
        coord = (package_id, last_modified)
        if coord in seen_coords:
            raise ValueError(f"Retained bundle repeats dated coordinate {package_id}@{last_modified}")
        seen_coords.add(coord)

        source_captures = row.get("original_source_captures")
        if not isinstance(source_captures, list) or len(source_captures) != 3:
            raise ValueError(f"Retained coordinate {package_id}@{last_modified} must bind summary, mods, and body")

        captures: dict[str, CapturedBodyResponse] = {}
        matched_lines: list[int] = []
        wrapper_generation: str | None = None
        for raw_event in source_captures:
            event = require_capture_event(raw_event)
            matches = [
                (line_no, wrapper)
                for line_no, wrapper in enumerate(wrappers, 1)
                if isinstance(wrapper, dict)
                and wrapper.get("evidence") == evidence_id
                and isinstance(wrapper.get("event"), dict)
                and all(event.get(key) == value for key, value in wrapper["event"].items())
            ]
            if len(matches) != 1:
                raise ValueError("Selected original lacks exactly one matching genuine wrapper event")
            line_no, wrapper = matches[0]
            if package_id not in str(event.get("requested_url", "")):
                raise ValueError(f"Selected original URL differs from package {package_id}")
            payload = read_original(event.get("sha256"), event.get("byte_size"))
            capture = _captured_response(event, payload)
            tail = capture.requested_url.rsplit("/", 1)[-1]
            role = tail if tail in {"summary", "mods"} else "body"
            if role in captures:
                raise ValueError(f"Retained coordinate {package_id}@{last_modified} duplicates {role} capture")
            captures[role] = capture
            matched_lines.append(line_no)
            if wrapper_generation is None and isinstance(wrapper.get("generation"), str):
                wrapper_generation = wrapper["generation"]

        if set(captures) != {"summary", "mods", "body"}:
            raise ValueError(f"Retained coordinate {package_id}@{last_modified} lacks summary, mods, or body")
        summary_doc = json.loads(captures["summary"].body)
        if summary_doc.get("packageId") != package_id or summary_doc.get("lastModified") != last_modified:
            raise ValueError(f"Retained summary payload disagrees with selected coordinate {package_id}@{last_modified}")

        ref = row.get("detail_original_event_reference")
        if not isinstance(ref, dict):
            raise ValueError(f"Retained coordinate {package_id}@{last_modified} lacks detail_original_event_reference")
        source_file = ref.get("source_file")
        if (
            not isinstance(source_file, dict)
            or source_file.get("sha256") != wrapper_pin["sha256"]
            or source_file.get("bytes") != wrapper_pin["bytes"]
        ):
            raise ValueError("Detail wrapper source pin differs from retained evidence-events.jsonl")
        line_no = ref.get("wrapper_line")
        if type(line_no) is not int or not 1 <= line_no <= len(wrappers):
            raise ValueError("Selected detail wrapper_line is outside retained evidence-events.jsonl")
        detail_wrapper = wrappers[line_no - 1]
        if (
            detail_wrapper != ref.get("wrapper")
            or not isinstance(detail_wrapper, dict)
            or detail_wrapper.get("evidence") != evidence_id
        ):
            raise ValueError("Selected detail wrapper line differs from its retained event")
        detail_event = require_capture_event(detail_wrapper.get("event"))
        detail_url = str(detail_event.get("requested_url", ""))
        if package_id not in detail_url and str(identity.number) not in detail_url:
            raise ValueError(f"Selected detail URL differs from package {package_id}")
        detail_payload = read_original(detail_event.get("sha256"), detail_event.get("byte_size"))
        detail_capture = _captured_response(detail_event, detail_payload)
        matched_lines.append(line_no)
        if wrapper_generation is None and isinstance(detail_wrapper.get("generation"), str):
            wrapper_generation = detail_wrapper["generation"]

        revisions.append(
            RetainedHearingRevision(
                package_id=package_id,
                last_modified=last_modified,
                source_evidence_artifact=evidence_id,
                summary=captures["summary"],
                mods=captures["mods"],
                body=captures["body"],
                detail=detail_capture,
                wrapper_lines=tuple(matched_lines),
                wrapper_generation=wrapper_generation,
            )
        )

    revisions.sort(key=lambda item: (item.package_id, revision_instant(item.last_modified), item.last_modified))
    return RetainedHearingBundle(
        pins_sha256=pins_sha256,
        wrapper_sha256=wrapper_pin["sha256"],
        wrapper_bytes=wrapper_pin["bytes"],
        revisions=tuple(revisions),
        unique_originals=len(blob_cache),
        decoded_bytes=decoded_total,
    )


class _RetainedHearingDetails:
    """Offline Congress.gov hearing-detail reader backed strictly by one verified retained capture."""

    def __init__(self, capture: CapturedBodyResponse, candidate_urls: Collection[str] = ()):
        self._capture = capture
        self._candidate_urls = frozenset(candidate_urls)

    def records(self, route: Any, url: str, *, max_pages: int = 1) -> Iterator[Any]:
        if url != self._capture.requested_url:
            if url in self._candidate_urls:
                raise PagedJsonUnavailableError(
                    CapturedBodyResponse(
                        requested_url=url,
                        resolved_url=url,
                        status_code=404,
                        content_type="application/json",
                        observed_at=self._capture.observed_at,
                        body=b"",
                    )
                )
            raise ValueError(f"Unselected offline hearing-detail route: {url}")
        budget = replace(DISCOVERY_BUDGET, min_request_interval_seconds=0.0)
        with CongressListingReader(
            budget=budget,
            api_key="offline-retained-bundle",
            transport=httpx.MockTransport(lambda _request: _retained_http_response(self._capture)),
        ) as reader:
            yield from reader.records(route, url, max_pages=max_pages)


def build_committee_reports(
    output_dir: Path,
    *,
    reader: PackageDiscoverySource | None = None,
    acquirer: PackageBodySource | None = None,
    hearings: HearingDetailSource | None = None,
    max_packages: int | None = None,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence: CaptureEvidence | None = None,
    reread: Collection[str] = (),
    coverage: dict | None = None,
    retained_bundle: Path | RetainedHearingBundle | None = None,
    retained_bundle_root: Path | None = None,
    retained_raw_archive: Path | None = None,
) -> tuple[Path, ...]:
    """Build five contract tables and the acquisition checkpoint, with one owner.

    ``reread`` names CRPT or CHRG packages to read again whatever their checkpoints
    say, and nothing else: discovery is skipped and the inherited
    ``committee-discovery`` cursor is preserved unchanged, so every other row and
    checkpoint carries forward from the prior. It refuses (see
    :func:`_refuse_unsafe_reread`) rather than publish a state only a discovery
    run can settle.

    ``retained_bundle`` instead merges an audited, cryptographically verified
    offline cohort of withdrawn GovInfo hearing revisions (e.g.
    ``selected-capture-pins.json``) onto the prior generation without making any
    network request or moving the discovery cursor.
    """
    if reread and retained_bundle is not None:
        raise ValueError("reread and retained_bundle cannot be combined")
    if outside := sorted(key for key in reread if not key.startswith(("CRPT-", "CHRG-"))):
        raise ValueError(f"reread names packages outside CRPT and CHRG: {outside}")

    retained: RetainedHearingBundle | None = None
    if retained_bundle is not None:
        retained = (
            retained_bundle
            if isinstance(retained_bundle, RetainedHearingBundle)
            else load_retained_hearing_bundle(
                retained_bundle,
                bundle_root=retained_bundle_root,
                raw_archive_root=retained_raw_archive,
            )
        )
    elif reader is None or acquirer is None or hearings is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Committee reports need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = reader or (RetainedGovInfoDiscoveryReader(budget=DISCOVERY_BUDGET, api_key=api_key, evidence=evidence)
                            if evidence else GovInfoDiscoveryReader(budget=DISCOVERY_BUDGET, api_key=api_key))
        if evidence:
            evidence.credential = api_key
        acquirer = acquirer or (RetainedGovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key, evidence=evidence)
                               if evidence else GovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key))
        hearings = hearings or listing_reader(api_key, evidence=evidence)

    prior_files = {
        name: published_table(output_dir, name, download_prior) for name in ("committee_reports", "hearing_transcripts")
    }
    checkpoint = published_table(output_dir, READS_TABLE, download_prior)
    reads = prior_reads(checkpoint, prior_files)
    prior_hearings = prior_files["hearing_transcripts"]
    previous_reads = dict(reads)
    if reread:
        _refuse_unsafe_reread(reread, set(prior_reads(checkpoint, {})), reads)
    discovered_at = datetime.now(UTC)
    since = _since(evidence=evidence, now=discovered_at)
    raw_until = os.environ.get("COMMITTEE_REPORTS_UNTIL", "").strip()
    until = (raw_until if raw_until.endswith("Z") else f"{raw_until}T00:00:00Z") if raw_until else None
    mode_desc = (
        f"merging {len(retained.revisions)} retained hearing revisions across {list(retained.packages)}"
        if retained is not None
        else f"re-reading only {sorted(reread)}"
        if reread
        else f"modified since {since}"
    )
    caps = {"CRPT": MAX_PACKAGES_PER_RUN, "CHRG": MAX_HEARING_PACKAGES_PER_RUN}
    if max_packages is not None:
        caps = dict.fromkeys(caps, max_packages)
    logger.info("Committee reports: {}; package caps {}; agenda cap 0", mode_desc, caps)
    observed_at = datetime.now(UTC).isoformat()
    if evidence:
        evidence.event(
            "selection",
            since=None if (reread or retained is not None) else since,
            reread=sorted(reread),
            retained_bundle_pins_sha256=None if retained is None else retained.pins_sha256,
            max_packages_per_collection=caps,
            max_pages=MAX_PAGES,
            agenda_cap=0,
            checkpoint_observed_at=observed_at,
        )
    link_rows: list[dict] = []
    evaluated_hearings: set[tuple[str, str]] = set()
    protected_hearings: set[str] = set()
    evaluated_reports: set[tuple[str, str]] = set()
    report_rows: list[dict] = []
    letter_rows: list[dict] = []
    section_rows: list[dict] = []
    hearing_rows: list[dict] = []
    renditions: Counter[str] = Counter()
    completeness: Counter[str] = Counter()
    mentions: Counter[str] = Counter()
    meetings: Counter[str] = Counter()
    refused = unchanged = linked = deferred = listed_count = 0
    attempted: dict[str, list[str]] = {}

    if retained is not None:
        if evidence:
            evidence.event(
                "retained-bundle-merge",
                pins_sha256=retained.pins_sha256,
                wrapper_sha256=retained.wrapper_sha256,
                wrapper_bytes=retained.wrapper_bytes,
                selected_packages=list(retained.packages),
                selected_revisions=len(retained.revisions),
                unique_originals=retained.unique_originals,
                decoded_bytes=retained.decoded_bytes,
                new_acquisition=False,
                bindings=[
                    {
                        "package_id": rev.package_id,
                        "last_modified": rev.last_modified,
                        "wrapper_evidence": rev.source_evidence_artifact,
                        "wrapper_generation": rev.wrapper_generation,
                        "wrapper_lines": list(rev.wrapper_lines),
                        "summary_sha256": rev.summary.sha256,
                        "mods_sha256": rev.mods.sha256,
                        "body_sha256": rev.body.sha256,
                        "detail_sha256": rev.detail.sha256,
                    }
                    for rev in retained.revisions
                ],
            )
        attempted["CHRG"] = list(retained.packages)
        prior_hearing_rows = _prior_hearing_rows(prior_hearings, retained.packages)
        in_run_text: dict[tuple[str, str], ReadBody] = {}
        offline_budget = replace(BODY_BUDGET, min_request_interval_seconds=0.0)
        for rev in retained.revisions:
            package_id = rev.package_id
            originals = {cap.requested_url: cap for cap in (rev.summary, rev.mods, rev.body)}

            def replay_govinfo(request: httpx.Request, *, _originals: Mapping[str, CapturedBodyResponse] = originals) -> httpx.Response:
                url = str(request.url)
                if url not in _originals:
                    raise ValueError(f"Unselected offline route: {url}")
                return _retained_http_response(_originals[url])

            prior_read = previous_reads.get(package_id, {})

            def reuse_retained(
                body: Any,
                *,
                _prior_read: Mapping[str, Any] = prior_read,
            ) -> ReadBody | None:
                cached = in_run_text.get((body.format, body.body_capture.sha256))
                if cached is not None:
                    return ReadBody(
                        body,
                        cached.text,
                        cached.completeness,
                        cached.text_sha256,
                        cached.derivation,
                        cached.page_count,
                    )
                return _reuse_hearing_text(body, prior_hearing_rows, _prior_read)

            with GovInfoBodyAcquirer(
                budget=offline_budget,
                api_key="offline-retained-bundle",
                transport=httpx.MockTransport(replay_govinfo),
            ) as offline_acquirer:
                bodies = _read_bodies(offline_acquirer, package_id, "CHRG", evidence, reuse=reuse_retained)
            modified = bodies[0].body.summary.last_modified
            if modified != rev.last_modified:
                raise ValueError(
                    f"{package_id}: retained summary last_modified {modified!r} differs from pinned {rev.last_modified!r}"
                )
            for reading in bodies:
                body = reading.body
                in_run_text.setdefault((body.format, body.body_capture.sha256), reading)
                status = reading.completeness
                renditions[body.format] += 1
                completeness[status] += 1
                stated = body.part if body.part is not None else body.mods
                primary = stated.primary_bill
                bill = None if primary is None else _bill_key(package_id, primary)
                linked += bill is not None
                mentions.update(entry.context for entry in stated.bills if entry.context != PRIMARY_BILL_CONTEXT)
                common = {
                    "page_count": reading.page_count,
                    "text_sha256": reading.text_sha256,
                    "body_completeness": status,
                    "text_derivation": reading.derivation,
                }
                event_id, outcome, detail = _hearing_detail(
                    _RetainedHearingDetails(rev.detail, hearing_detail_urls(body.identity)), body, evidence
                )
                meetings[outcome] += 1
                if outcome not in {"meeting", "no_meeting"}:
                    raise RuntimeError(
                        f"retained bundle detail did not complete for {package_id}@{modified}: {outcome}"
                    )
                held_row = prior_hearing_rows.get((package_id, modified))
                if event_id is None and held_row is not None and _qualified_prior_event(held_row, body):
                    protected_hearings.add(package_id)
                    continue
                hearing_rows.append(
                    shape_hearing_transcript(
                        body,
                        event_id=event_id,
                        hearing_detail=detail,
                        body_text=reading.text if status != PLACEHOLDER else None,
                        **common,
                    )
                )
                link_rows.extend(
                    shape_hearing_bill_link(link, last_modified=modified)
                    for link in cover_links(body.mods, event_id=event_id)
                )
                evaluated_hearings.add((package_id, modified))
            reads[package_id] = {
                "package_id": package_id,
                "last_modified": modified,
                "outcome": "complete",
                "rule_version": RULE_VERSIONS["CHRG"],
                "observed_at": observed_at,
            }
        if evidence:
            for package_id in retained.packages:
                evidence.event("package-outcome", **latest_read(previous_reads.get(package_id, {}), reads[package_id]))
    else:
        assert reader is not None and acquirer is not None and hearings is not None
        for collection in ("CRPT", "CHRG"):
            cap = caps[collection]
            if reread:
                listed = {}
                pending = {key: reads.get(key, {}).get("last_modified") for key in reread
                           if key.startswith(collection + "-")}
            else:
                try:
                    listed = _package_ids(reader, collection, since, evidence, until=until)
                except Exception as error:
                    if evidence:
                        evidence.refusal(error, stage=collection + ":listing")
                    raise
                pending = {key: row.get("last_modified") for key, row in reads.items()
                           if key.startswith(collection + "-") and not _settled(row, collection)}
                pending.update({key: modified for key, modified in listed.items()
                                if not _settled(reads.get(key, {}), collection, modified)})
                pending = dict(sorted(pending.items(), key=lambda item: _retry_order(reads.get(item[0], {}), item[1])))
            listed_count += len(listed)
            deferred += max(0, len(pending) - cap)
            unchanged += len(listed) - sum(key in pending for key in listed)
            queue = list(pending)
            selected, held_back = queue[:cap], queue[cap:]
            if evidence:
                evidence.event("package-selection", collection=collection, listed=listed,
                               selected=selected, deferred=held_back,
                               unchanged=[key for key in listed if key not in pending],
                               refused_final=sorted(key for key, row in reads.items() if key.startswith(collection + "-")
                                                    and row.get("outcome") == REFUSED_FINAL and key not in pending))
            for package_id, modified in pending.items():
                reads[package_id] = {"package_id": package_id, "last_modified": modified,
                                     "outcome": "pending", "rule_version": RULE_VERSIONS[collection],
                                     "observed_at": observed_at}
            logger.info("{}: {} pending; {} deferred by package cap", collection, len(pending), len(held_back))
            attempted[collection] = selected
            prior_hearing_rows = _prior_hearing_rows(prior_hearings, attempted[collection]) if collection == "CHRG" else {}
            for package_id in attempted[collection]:
                state = reads[package_id]
                try:
                    prior_read = previous_reads.get(package_id, {})
                    reuse = (partial(_reuse_hearing_text, prior=prior_hearing_rows, prior_read=prior_read)
                             if collection == "CHRG" else None)
                    bodies = _read_bodies(acquirer, package_id, collection, evidence, reuse=reuse)
                except (CredentialRefusedError, SourceEvidenceError):
                    raise
                except Exception as error:  # noqa: BLE001 — retained for the next run
                    if evidence:
                        evidence.refusal(error, stage=package_id)
                    refused += 1
                    if _refusal_is_final(error):
                        # Not asked again until its stamp, the rule or the bounds move.
                        state.update(outcome=REFUSED_FINAL, rule_version=refusal_rule(collection, _refusal_basis(error)))
                    else:
                        state["outcome"] = "refused"
                    logger.warning("{}: {} {}: {}", collection, package_id, state["outcome"].replace("_", " "),
                                   scrub_credential(str(error), evidence.credential if evidence else ""))
                    if evidence:
                        evidence.event("package-outcome", **state)
                    continue
                modified = bodies[0].body.summary.last_modified
                # A placeholder whose part offers no PDF is complete: nothing more
                # is published, its row says so, and a new stamp or rule re-reads it.
                state.update(last_modified=modified, outcome="complete")
                for reading in bodies:
                    body = reading.body
                    status = reading.completeness
                    renditions[body.format] += 1
                    completeness[status] += 1
                    # A report part states its own bills; a multi-part package's
                    # root states none (CRPT-119hrpt455, -119hrpt494). A hearing
                    # has no part, so its bills are the package's.
                    stated = body.part if body.part is not None else body.mods
                    primary = stated.primary_bill
                    bill = None if primary is None else _bill_key(package_id, primary)
                    linked += bill is not None
                    mentions.update(entry.context for entry in stated.bills if entry.context != PRIMARY_BILL_CONTEXT)
                    common = {"page_count": reading.page_count,
                              "text_sha256": reading.text_sha256, "body_completeness": status,
                              "text_derivation": reading.derivation}
                    if collection == "CHRG":
                        event_id, outcome, detail = _hearing_detail(hearings, body, evidence)
                        meetings[outcome] += 1
                        # A package's bodies may answer differently: a retried refusal
                        # outranks a detail still awaited, which outranks a settled one.
                        rule = (RULE_VERSIONS[collection] if outcome == "refused"
                                else _unavailable_rule(body, collection) if outcome == "unavailable" else None)
                        held = _DETAIL_RANK[state["rule_version"]] if state["outcome"] == DETAIL_REFUSED else -1
                        if rule is not None and _DETAIL_RANK[rule] > held:
                            state.update(outcome=DETAIL_REFUSED, rule_version=rule)
                        held_row = prior_hearing_rows.get((package_id, modified))
                        if (event_id is None and held_row is not None
                                and (_qualified_prior_event(held_row, body) or outcome in {"refused", "unavailable"})):
                            # The current observation remains in source evidence; do not replace its dated parent/links.
                            if state["outcome"] == "complete":
                                protected_hearings.add(package_id)
                            continue
                        hearing_rows.append(shape_hearing_transcript(
                            body, event_id=event_id, hearing_detail=detail,
                            body_text=reading.text if status != PLACEHOLDER else None, **common
                        ))
                        link_rows.extend(shape_hearing_bill_link(link, last_modified=modified)
                                         for link in cover_links(body.mods, event_id=event_id))
                        evaluated_hearings.add((package_id, modified))
                    else:
                        estimate = read_cbo_estimate(reading.text)
                        report_rows.append(shape_committee_report(
                            body, bill_id=bill, estimate=estimate,
                            recital_bill_id=recital_bill_id(estimate, body.identity.congress), **common))
                        letter_rows.extend(shape_committee_report_cbo_letter(
                            letter, package_id=package_id, part_id=body.part.part_id, seq=seq, last_modified=modified
                        ) for seq, letter in enumerate(estimate.letters))
                        assert reading.extracted is not None
                        section_rows.extend(shape_report_section(
                            block, package_id=package_id, part_id=body.part.part_id, seq=seq, last_modified=modified
                        ) for seq, block in enumerate(parse_agency_blocks(_section_input(reading.extracted))))
                if collection == "CRPT":
                    evaluated_reports.add((package_id, modified))
                if evidence:
                    evidence.event("package-outcome", **state)

    # The one state that fails the run (through the rollup's deferred failure, after publishing): a collection
    # that attempted packages this run and read none of them, the signature of a reader or source refusing
    # everything. Decided from this run's rows, before any prior checkpoint is merged back in. A final refusal
    # (size or parts bound) is left out: it is settled and needs no one; a hearing whose body read and only
    # its optional Congress.gov detail refused counts as read because its transcript published and ``detail_refused``
    # retries next run.
    nothing_read = []
    for collection, keys in attempted.items():
        considered = [reads[key] for key in keys if reads[key]["outcome"] != REFUSED_FINAL]
        if considered and not any(_read_this_run(row, collection) for row in considered):
            nothing_read.append(collection)
    completed = (
        {key for key, _modified in evaluated_hearings}
        | protected_hearings
        | {key for key, _modified in evaluated_reports}
    )
    if unfinished := sorted(key for key in reread if key not in completed or reads[key]["outcome"] != "complete"):
        # A refused named read would publish a checkpoint every later run
        # retries, and a refused hearing detail would drop the prior event_id.
        raise RuntimeError(f"reread did not complete, so nothing is merged: {unfinished}")
    logger.info(
        "Committee reports: {:,} reports, {:,} CBO letters, {:,} sections, {:,} hearings, {:,} already held, {:,} refused",
        len(report_rows),
        len(letter_rows),
        len(section_rows),
        len(hearing_rows),
        unchanged,
        refused,
    )
    logger.info(
        "Committee reports: {:,} report parts and hearings name a PRIMARY bill; other mentions by MODS context — {}",
        linked,
        dict(mentions),
    )
    if meetings:
        # The three outcomes of the hearing-detail read, kept apart: a NULL
        # event_id is either a detail that names no meeting or one not read.
        logger.info("Committee reports: hearing details by outcome — {}", dict(meetings))
    if renditions:
        # The run's tally of what each row states in format, text_derivation
        # and body_completeness: a placeholder kept for want of a PDF shows here.
        logger.info("Committee reports: renditions read — {}; body completeness — {}",
                    dict(renditions), dict(completeness))
    logger.info("Committee reports: {} cover links; agenda deferred (no verified meeting-to-jacket join)", len(link_rows))
    # Replace only the successfully read package revision. A removed part or
    # child stays in its earlier revision; failed and unread scopes stay whole.
    # Legacy report rows predate part_id and represent the package's one part.
    # Keep that established backfill alongside the publisher revision key.
    replaced = {"committee_reports": evaluated_reports, "report_sections": evaluated_reports,
                "committee_report_cbo_letters": evaluated_reports,
                "hearing_bill_links": evaluated_hearings, "hearing_transcripts": evaluated_hearings}
    paths = {name: merge_contract_table(output_dir, name, rows, download_prior=download_prior,
                                        replace_parents=(("package_id", "last_modified"), replaced[name]),
                                        backfill_prior={"part_id": "package_id"} if name in
                                        ("committee_reports", "report_sections", "committee_report_cbo_letters") else None,
                                        prior_present=(prior_files[name] is not None) if name in prior_files else None)
             for name, rows in (("committee_reports", report_rows), ("report_sections", section_rows),
                                ("hearing_transcripts", hearing_rows), ("hearing_bill_links", link_rows),
                                ("committee_report_cbo_letters", letter_rows))}
    relinked = _link_committee_codes(paths["hearing_bill_links"], paths["hearing_transcripts"])
    logger.info("Committee reports: {:,} hearing links take their hearing's committee code", relinked)
    reads = {key: latest_read(previous_reads.get(key, {}), row) for key, row in reads.items()}
    read_path = merge_table(output_dir, name=READS_TABLE, columns=READ_COLUMNS,
                                         identity=("package_id",), version_column="observed_at", rows=reads.values(),
                                         remote_key=f"{READS_TABLE}.parquet", download_prior=download_prior,
                                         prior_present=checkpoint is not None)
    waiting = sorted(key for key, row in reads.items() if awaited(row, key.split("-", 1)[0]))
    if waiting:
        annotate("warning", "Committee reports: hearing details awaited",
                 f"{len(waiting)} hearing(s) not yet on Congress.gov, asked again until {DETAIL_GRACE_DAYS} days "
                 f"after GovInfo issued them: {', '.join(waiting[:20])}{' ...' if len(waiting) > 20 else ''}")
    # Pending work is retried by the next run without anyone acting, so it warns rather than fails the run:
    # previously unrelated SpicyDocs builds re-read all CRPT checkpoints, more than one night's cap,
    # and one Congress.gov 500 left the run red for nights it had published (runs 42558022791,
    # 42751350570, 42954778184). A package that never reads stays named here and in acquisition-coverage.
    pending = [key for key, row in reads.items()
               if not _settled(row, key.split("-", 1)[0]) and not awaited(row, key.split("-", 1)[0])]
    retrying = sorted(key for key in pending if reads[key]["outcome"] != "pending")
    waiting_cap = sorted(key for key in pending if reads[key]["outcome"] == "pending")
    if pending:
        named = set([*retrying, *waiting_cap][:20])
        pending_caps = {caps[key.split("-", 1)[0]] for key in waiting_cap or retrying}
        cap_label = (f"{next(iter(pending_caps))}-package cap" if len(pending_caps) == 1
                     else f"CRPT {caps['CRPT']}/CHRG {caps['CHRG']} package caps")

        def ids(keys: list[str]) -> str:
            shown = [key for key in keys if key in named]
            return (", ".join(shown) + (" ..." if len(shown) < len(keys) else "")) if shown else (
                "ids not listed" if keys else "none")

        annotate("warning", "Committee reports: acquisition pending",
                 f"{len(pending)} package(s) pending, retried next run: {len(retrying)} refused ({ids(retrying)}), "
                 f"{len(waiting_cap)} deferred by the {cap_label} ({ids(waiting_cap)})")
    discovery_state = "retained_bundle" if retained is not None else "not_requested" if reread else "complete"
    status = {"discovery": discovery_state, "since": None if (reread or retained is not None) else since,
              "until": until, "listed": listed_count, "unchanged": unchanged, "refused": refused, "deferred": deferred,
              "pending": len(pending), "nothing_read": nothing_read,
              "awaiting_detail": len(waiting),
              "table_generation": "prepared", "atomic_snapshot": False, "whole_history_complete": False}
    if coverage is not None:
        coverage.update(status)
    if evidence is not None:
        evidence.event("acquisition-coverage", **status)
        state = evidence.inherited_event("committee-discovery") or {}
        explicit = bool(os.environ.get("COMMITTEE_REPORTS_SINCE", "").strip()) or until is not None
        evidence.event("committee-discovery",
                       cursor=discovered_at.isoformat() if not reread and retained is None and not explicit else state.get("cursor"),
                       since=status["since"],
                       mode="retained-bundle" if retained is not None else "reread" if reread else "explicit-window" if explicit else "refresh")
    return (*paths.values(), read_path)
