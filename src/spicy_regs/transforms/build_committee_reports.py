"""Reports, letters, hearing cover links and their own acquisition checkpoints.

MODS cover links and the CBO letter rule use bytes already fetched. Agenda
acquisition is deferred: this pass has no verified meeting-to-jacket join or
House repository locator. Pending reads precede discovery under the same cap;
completed empty covers are checkpointed without inventing a link row.
"""

from __future__ import annotations

import hashlib
import os
from collections import Counter
from collections.abc import Callable, Collection, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

import httpx
from loguru import logger
from spicy_docs.extraction.body_text import BodyText, body_text
from spicy_docs.interpretation.cbo_estimates import read_cbo_estimate, recital_bill_id
from spicy_docs.interpretation.hearing_bill_links import cover_links
from spicy_docs.schemas.hearing_bill_link_tables import shape_hearing_bill_link
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.committee_report_tables import (
    shape_committee_report,
    shape_hearing_transcript,
    shape_report_section,
)
from spicy_docs.schemas.tables import natural_key
from spicy_docs.sources.agency_reports.report_blocks import parse_agency_blocks
from spicy_docs.sources.govinfo.bodies import ModsBill, GovInfoBodySourceError, parse_package_id
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.schemas.tables import text
from spicy_docs.sources.congress.listing import LIST_ROUTES, list_route_url
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, collection_url
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key, listing_reader
from spicy_regs.transforms.table_merge import merge_contract_table, merge_table, published_table
from spicy_regs.transforms.committee_report_reads import READS_TABLE, READ_COLUMNS, RULE_VERSIONS, complete, prior_reads
from spicy_regs.source_evidence import CaptureEvidence, SourceEvidenceError
from spicy_regs.sources.retained import RetainedGovInfoBodyAcquirer, RetainedGovInfoDiscoveryReader


class PackageDiscoverySource(Protocol):
    """What this transform needs of a GovInfo discovery reader."""

    def packages(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


class PackageBodySource(Protocol):
    """What this transform needs of a GovInfo package-body acquirer.

    No ``prefer``: the rendition order is the acquirer's sealed default
    (``sources.govinfo.bodies.BODY_PREFERENCE`` — XML, HTML, text, then PDF),
    so this transform neither passes one nor needs a seam that accepts one.
    """

    def acquire(self, package_id: str, *, max_bytes: int | None = ...) -> Any: ...


class HearingDetailSource(Protocol):
    """What this transform needs of a Congress.gov listing reader: the ``hearing-detail`` route."""

    def records(self, route: Any, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


DISCOVERY_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Three requests per package (summary, MODS, body), paced.
BODY_BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=24 * 1024 * 1024,  # MAX_EVIDENCE_BYTES
    max_metadata_bytes=4 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=0.34,
)

#: Cold-start window only: with no prior table there is no watermark, and
#: thirty days back covers a daily cron with a wide margin for an outage.
DEFAULT_WINDOW_DAYS = 30

#: Re-ask this far back from the stored watermark, so a package modified after
#: the previous run's cutoff is still seen.
OVERLAP_HOURS = 24

#: ~3 requests each at ~3/s: 200 packages is ~3.5 minutes per collection.
MAX_PACKAGES_PER_RUN = 200

MAX_PAGES = 40


def _prior_watermark(prior_file: Path) -> datetime | None:
    """The largest ``last_modified`` already published, or None."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(last_modified) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return datetime.fromisoformat(str(row[0]).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        logger.warning("Committee reports: prior last_modified {!r} is not an instant — using the cold window", row[0])
        return None


def _since(prior_file: Path | None = None, window_days: int = DEFAULT_WINDOW_DAYS) -> str:
    """The last-modified floor this run asks GovInfo for."""
    raw = os.environ.get("COMMITTEE_REPORTS_SINCE", "").strip()
    if raw:
        return raw if raw.endswith("Z") else f"{raw}T00:00:00Z"
    watermark = _prior_watermark(prior_file) if prior_file is not None else None
    if watermark is not None:
        start = watermark - timedelta(hours=OVERLAP_HOURS)
    else:
        start = datetime.now(UTC) - timedelta(days=window_days)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ")


def _package_ids(reader: PackageDiscoverySource, collection: str, since: str,
                 evidence: CaptureEvidence | None = None) -> dict[str, str | None]:
    """Enumerate within the page cap; use the package grammar before spending body requests."""
    found = {}
    unsupported = 0
    for page in reader.packages(collection_url(collection, since), max_pages=MAX_PAGES):
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


def _read_body(acquirer: PackageBodySource, package_id: str,
               evidence: CaptureEvidence | None = None) -> tuple[Any, BodyText]:
    """One package's fetched body and the one text derivation for its rendition.

    Takes the narrow Protocol rather than the concrete acquirer, the way the
    bill family's ``_version_captures`` does: what this transform depends on is
    the three facts ``body_text`` reads off a fetched body, and the rendition
    it was fetched in is the body's own, never this caller's guess.
    """
    package = acquirer.acquire(package_id)
    if evidence:
        for capture in package.captures:
            evidence.capture(capture, stage=package_id + ":used")
    # No extractor argument: ``body_text``'s default (``DocumentExtractor(NativeText())``,
    # PyMuPDF) is the pipeline ``gpo_normalize`` was derived on — the GPO
    # gutter-numbered layout only comes through on PyMuPDF's own line
    # adjacency, not pypdf's (measured
    # ``docs/research/gpo-normalizer-vs-upstream-2026-09-19.md`` in spicy-docs).
    return package, body_text(package)


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


#: A CHRG package id spells its chamber in the first letter of its document
#: type (``hhrg``, ``shrg``, ``jhrg``); the hearing detail route wants the word.
_CHAMBER_OF_DOCUMENT_TYPE = {"h": "house", "s": "senate", "j": "joint"}

#: What one hearing-detail request can refuse with, short of a credential
#: refusal, which propagates: the reader's own refusals (``404`` included), a
#: transport failure after its retries, and a record not shaped as expected.
_HEARING_REFUSALS = (PagedJsonSourceError, httpx.HTTPError, ConnectionError, ValueError, TypeError, KeyError)


def _event_id(hearings: HearingDetailSource, package: Any,
              evidence: CaptureEvidence | None = None) -> tuple[str | None, str]:
    """``(event_id, outcome)`` for one CHRG package from its Congress.gov hearing detail.

    ``outcome`` is ``meeting``, ``no_meeting`` or ``refused``, so the run log
    can count the three apart. The detail must name the jacket and Congress
    the package id spells, or it is refused rather than read.
    """
    identity = package.identity
    chamber = _CHAMBER_OF_DOCUMENT_TYPE.get(str(identity.document_type)[:1])
    route = LIST_ROUTES["hearing-detail"]
    try:
        if chamber is None:
            raise ValueError(f"document type {identity.document_type!r} names no chamber")
        url = list_route_url(route, congress=identity.congress, chamber=chamber, number=int(identity.number), limit=1)
        page = next(iter(hearings.records(route, url, max_pages=1)))
        if evidence:
            evidence.capture(page.capture, stage=identity.package_id + ":hearing-detail")
        if len(page.records) != 1:
            raise PagedJsonSourceError(f"hearing-detail answered {len(page.records)} records, not one")
        record = page.records[0]
        if str(record["jacketNumber"]) != str(int(identity.number)) or str(record["congress"]) != str(
            identity.congress
        ):
            raise PagedJsonSourceError("hearing-detail identity differs from the requested jacket")
    except CredentialRefusedError:
        raise
    except _HEARING_REFUSALS as error:
        if evidence:
            evidence.refusal(error, stage=identity.package_id + ":hearing-detail")
        logger.warning("CHRG: {} hearing detail refused: {}", identity.package_id,
                       scrub_credential(str(error), evidence.credential if evidence else ""))
        return None, "refused"
    meeting = record.get("associatedMeeting")
    event_id = text(meeting.get("eventId")) if isinstance(meeting, dict) else None
    return event_id, "meeting" if event_id is not None else "no_meeting"


def build_committee_reports(
    output_dir: Path,
    *,
    reader: PackageDiscoverySource | None = None,
    acquirer: PackageBodySource | None = None,
    hearings: HearingDetailSource | None = None,
    max_packages: int = MAX_PACKAGES_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence: CaptureEvidence | None = None,
    reread: Collection[str] = (),
) -> tuple[Path, ...]:
    """Build four contract tables and the acquisition checkpoint, with one owner.

    ``reread`` names packages to read again whatever their checkpoints say, and
    nothing else: discovery is skipped, so every other row and checkpoint is
    carried from the prior unchanged. It re-establishes the named packages'
    capture evidence without moving the rest of the family.
    """
    if outside := sorted(key for key in reread if not key.startswith(("CRPT-", "CHRG-"))):
        raise ValueError(f"reread names packages outside CRPT and CHRG: {outside}")
    if reader is None or acquirer is None or hearings is None:
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
    since = _since(prior_files["committee_reports"])
    logger.info("Committee reports: {}; cap {} per collection; agenda cap 0",
                f"re-reading only {sorted(reread)}" if reread else f"modified since {since}", max_packages)
    observed_at = datetime.now(UTC).isoformat()
    if evidence:
        evidence.event("selection", since=None if reread else since, reread=sorted(reread),
                       max_packages_per_collection=max_packages, max_pages=MAX_PAGES,
                       agenda_cap=0, checkpoint_observed_at=observed_at)
    link_rows: list[dict] = []
    evaluated_hearings: set[str] = set()
    evaluated_reports: set[str] = set()
    report_rows: list[dict] = []
    section_rows: list[dict] = []
    hearing_rows: list[dict] = []
    renditions: Counter[str] = Counter()
    mentions: Counter[str] = Counter()
    meetings: Counter[str] = Counter()
    refused = unchanged = linked = 0

    for collection in ("CRPT", "CHRG"):
        if reread:
            listed = {}
            pending = {key: reads.get(key, {}).get("last_modified") for key in reread
                       if key.startswith(collection + "-")}
        else:
            try:
                listed = _package_ids(reader, collection, since, evidence)
            except Exception as error:
                if evidence:
                    evidence.refusal(error, stage=collection + ":listing")
                raise
            pending = {key: row.get("last_modified") for key, row in reads.items()
                       if key.startswith(collection + "-") and not complete(row, collection)}
            pending.update({key: modified for key, modified in listed.items()
                            if not complete(reads.get(key, {}), collection, modified)})
        unchanged += len(listed) - sum(key in pending for key in listed)
        if evidence:
            evidence.event("package-selection", collection=collection, listed=listed,
                           selected=list(pending)[:max_packages], deferred=list(pending)[max_packages:],
                           unchanged=[key for key in listed if key not in pending])
        for package_id, modified in pending.items():
            reads[package_id] = {"package_id": package_id, "last_modified": modified,
                                 "outcome": "pending", "rule_version": RULE_VERSIONS[collection],
                                 "observed_at": observed_at}
        logger.info("{}: {} pending; {} deferred by package cap", collection, len(pending), max(0, len(pending) - max_packages))
        for package_id in list(pending)[:max_packages]:
            state = reads[package_id]
            try:
                package, derived = _read_body(acquirer, package_id, evidence)
            except (CredentialRefusedError, SourceEvidenceError):
                raise
            except Exception as error:  # noqa: BLE001 — retained for the next run
                if evidence:
                    evidence.refusal(error, stage=package_id)
                refused += 1
                state["outcome"] = "refused"
                logger.warning("{}: {} refused: {}", collection, package_id,
                               scrub_credential(str(error), evidence.credential if evidence else ""))
                continue
            renditions[derived.rendition] += 1
            primary = package.mods.primary_bill
            bill = None if primary is None else _bill_key(package_id, primary)
            linked += bill is not None
            mentions.update(entry.context for entry in package.mods.bills if entry.context != PRIMARY_BILL_CONTEXT)
            common = {"page_count": None if derived.pages is None else len(derived.pages),
                      "text_sha256": "sha256:" + hashlib.sha256(derived.text.encode("utf-8")).hexdigest()}
            state.update(last_modified=package.summary.last_modified, outcome="complete")
            if collection == "CHRG":
                event_id, outcome = _event_id(hearings, package, evidence)
                meetings[outcome] += 1
                if outcome == "refused":
                    state["outcome"] = "detail_refused"
                hearing_rows.append(shape_hearing_transcript(package, event_id=event_id, **common))
                link_rows.extend(shape_hearing_bill_link(link) for link in cover_links(package.mods, event_id=event_id))
                evaluated_hearings.add(package_id)
            else:
                estimate = read_cbo_estimate(derived.text)
                report_rows.append(shape_committee_report(
                    package, bill_id=bill, estimate=estimate,
                    recital_bill_id=recital_bill_id(estimate, package.identity.congress), **common))
                section_rows.extend(shape_report_section(
                    block, package_id=package_id, seq=seq, last_modified=package.summary.last_modified
                ) for seq, block in enumerate(parse_agency_blocks(derived.text)))
                evaluated_reports.add(package_id)
            if evidence:
                evidence.event("package-outcome", **state)

    logger.info(
        "Committee reports: {:,} reports, {:,} sections, {:,} hearings, {:,} already held, {:,} refused",
        len(report_rows),
        len(section_rows),
        len(hearing_rows),
        unchanged,
        refused,
    )
    logger.info(
        "Committee reports: {:,} packages name a PRIMARY bill; other mentions by MODS context — {}",
        linked,
        dict(mentions),
    )
    if meetings:
        # The three outcomes of the hearing-detail read, kept apart: a NULL
        # event_id is either a detail that names no meeting or one not read.
        logger.info("Committee reports: hearing details by outcome — {}", dict(meetings))
    if renditions:
        # Which rendition each package was actually read in. Neither contract
        # has a column for the derivation name, so this is where it is stated.
        logger.info("Committee reports: renditions read — {}", dict(renditions))
    logger.info("Committee reports: {} cover links; agenda deferred (no verified meeting-to-jacket join)", len(link_rows))
    paths = tuple(merge_contract_table(output_dir, name, rows, download_prior=download_prior,
                                     replace_parents=("package_id", evaluated_hearings) if name == "hearing_bill_links"
                                     else ("package_id", evaluated_reports) if name == "report_sections" else None,
                                     prior_present=(prior_files[name] is not None) if name in prior_files else None)
                  for name, rows in (("committee_reports", report_rows), ("report_sections", section_rows),
                                     ("hearing_transcripts", hearing_rows), ("hearing_bill_links", link_rows)))
    return (*paths, merge_table(output_dir, name=READS_TABLE, columns=READ_COLUMNS,
                               identity=("package_id",), version_column="observed_at", rows=reads.values(),
                               remote_key=f"{READS_TABLE}.parquet", download_prior=download_prior,
                               prior_present=checkpoint is not None))
