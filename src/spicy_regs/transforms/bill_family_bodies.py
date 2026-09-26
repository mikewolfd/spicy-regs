"""The bill family's body pass: each printing's text, sections and comparisons, apart from its status.

A printing is pending by its own state -- listed in ``bill_versions`` with no
processed body -- not because its bill's BILLSTATUS moved, so this pass reads
the printings the status pass listed (this run's and every earlier run's) and
never rebuilds a bill to reach one.

From the 113th Congress on, bodies come from GovInfo's BILLS bulk folders
(``spicy_docs.sources.congress.bulk_bills``), one ``(congress, bill_type)``
group at a time: both sessions' listings are read, and a folder's zip is
downloaded only when its listing names a printing this run needs -- a pending
printing, or a held neighbour whose document a new comparison needs -- and then
read once for all of them. A printing the zip refused, or the engine could not
parse, is remembered with the zip entry it was read at, so an unchanged zip is
not downloaded again for it. Before the 113th, and for a printing no listing
names, the per-package route (``PackageBodySource``) remains, under the run's
``max_version_fetches`` cap, newest Congress first.

Every bill with work is built by the provider's ``build_bill_printings``: the
newly read printings emit their rows, held neighbours take part in order and
comparison as context, and the rest are placeholders. So ``printing_order``,
``seq`` identity, numbered reprints and the repeated-printing refusal are the
provider's, applied exactly as the status pass applies them.

What is written scales with what was read: each bill built emits the rows of
the printings it read and of the comparisons they complete, and a listed
placeholder a body now stands for is retired, so a printing has one
``bill_versions`` row. The merge still rewrites each table whole, since the
publication holds one member per table.

Cost, with B bills with work, P pending printings, Q pending comparisons and F
BILLS folders naming a needed printing: planning is O(rows of the B bills)
over the prior index, never over every bill; the pass makes two listing
requests per ``(congress, bill_type)`` group with work and one zip download per
folder in F, decompresses and parses only the members it keeps (P plus the
held sides of Q), and diffs only the consecutive pairs that are new or missing.
Before, the status pass reached a body only by rebuilding its whole bill, at
three keyed requests a printing (summary, MODS, body) under a cap of 600 a run.
"""

from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Callable, Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from loguru import logger
from spicy_docs.extraction.body_text import body_text
from spicy_docs.interpretation.bill_family import (
    BillFamilyTables,
    BillSummarizer,
    BillVersionCapture,
    DiffSummarizer,
    EngineStamp,
    SectionClassifier,
    build_bill_printings,
)
from spicy_docs.sources.congress.bill_acquisition import BillSourceUnavailableError
from spicy_docs.sources.congress.bill_status import BillIdentity, BillTextFormat, BillTextVersion
from spicy_docs.sources.congress.bill_tree import engine_available, parse_bill_tree
from spicy_docs.sources.congress.bill_versions import (
    DEFAULT_FORMAT_PREFERENCE,
    choose_format,
    consecutive_pairs,
    format_name,
    printing_order,
)
from spicy_docs.sources.congress.bulk_bills import BULK_BILLS_FLOOR, BULK_BILLS_SESSIONS
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyBudget
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.source_evidence import SourceEvidenceError

#: Three requests per printing (summary, MODS, body), paced at ~3/s.
BODY_BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=24 * 1024 * 1024,  # MAX_EVIDENCE_BYTES
    max_metadata_bytes=4 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=0.34,
)

#: ~3 requests each at ~3/s: 600 printings is ~10 minutes. The per-package
#: route's cap, shared with the pre-BILLSTATUS backfill's detail requests.
MAX_VERSION_FETCHES = 600

#: How many printings one run reads from the BILLS zips, counted by the
#: ``(congress, bill_type)`` group: a group is started only while the count is
#: under this, and then finished, so no zip is read for half its printings.
#: Measured on the 119th H.R. (9,220 printings read from two zips): the body
#: pass added 0.69 GB to the run's peak, about 75 KB a printing, and 163,003
#: section rows. At this bound the 133,919 printings the 113th-119th lacked on
#: 2026-09-26 take 8 runs of at most 21,402 printings each, newest Congress
#: first; a steady-state day is a few hundred.
MAX_BULK_PRINTINGS = 15_000

#: The ``source`` a printing carries once its body is read, and the one it
#: carries while only listed. A bulk member is the per-package XML byte for
#: byte, so both routes publish ``govinfo``.
ACQUIRED_SOURCE = "govinfo"
LISTED_SOURCE = "congress"

#: The printings a BILLS zip refused or the engine could not parse, by zip link,
#: with the zip entry they were read at. Parquet metadata on the archives table,
#: like the status scopes' completion list: an unchanged zip is not downloaded
#: again for a printing it already failed, and a moved one is.
TEXT_REFUSALS_KEY = "spicy_regs.bill_family.text_archive_refusals.v1"


class PackageBodySource(Protocol):
    """What this pass needs of a GovInfo package-body acquirer.

    No ``prefer``: the rendition order is the acquirer's sealed default now
    (``sources.govinfo.bodies.BODY_PREFERENCE``), so this transform neither
    passes one nor needs a seam that accepts one.
    """

    def acquire(self, package_id: str, *, max_bytes: int | None = ...) -> Any: ...


class BulkBillsSource(Protocol):
    """What this pass needs of a BILLS folder acquirer; the real one is ``BulkBillsAcquirer``."""

    def list_folder(self, congress: int, session: int, bill_type: str) -> Any: ...

    def acquire(self, listing: Any, *, keep: Callable[[str], bool] | None = ...) -> Any: ...


def body_kind(content_type: str | None, format_name: str | None) -> str | None:
    """Classify a fetched body as ``pdf``, ``xml``, or the source's own format name."""
    media = (content_type or "").split(";", 1)[0].strip().lower()
    if media == "application/pdf":
        return "pdf"
    if media in {"application/xml", "text/xml"} or media.endswith("+xml"):
        return "xml"
    return format_name


def processed_capture(entry: BillVersionCapture) -> bool:
    """Whether a captured body carries the derived artifact its kind needs (XML tree or PDF cleanup)."""
    if entry.body is None:
        return False
    kind = body_kind(entry.body.content_type, entry.format_name)
    if kind == "xml":
        return entry.document is not None
    if kind == "pdf":
        return entry.cleanup is not None
    return True


@dataclass(frozen=True, slots=True)
class Printing:
    """One listed printing of a bill, as ``bill_versions`` states it, and how far it has got.

    ``held`` is a published processed body; ``xml`` is that body as XML with
    every section row published, which is what lets it be a side of a
    comparison; ``sha256`` is that body's digest, which a re-read for
    comparison must match.
    """

    code: str
    version: BillTextVersion
    package_id: str | None
    held: bool
    xml: bool
    sha256: str | None

    @property
    def chosen(self) -> BillTextFormat | None:
        return choose_format(self.version.formats, prefer=DEFAULT_FORMAT_PREFERENCE)


@dataclass(slots=True)
class BillWork:
    """One bill's printings in the provider's order, and what this pass has to do for it."""

    bill_id: str
    identity: BillIdentity
    printings: list[Printing]
    #: Codes to read a body for: listed, addressable and not held.
    pending: set[str]
    #: Consecutive pairs with no complete published comparison whose sides are, or may become, XML.
    open_pairs: list[tuple[str, str]]
    by_code: dict[str, Printing] = field(init=False)

    def __post_init__(self) -> None:
        self.by_code = {printing.code: printing for printing in self.printings}

    def needed(self, attempted: Collection[str]) -> set[str]:
        """The documents this run needs: the printings it will attempt, and both sides of each live pair.

        A pair is live when each side is held as XML or among ``attempted``;
        a pair with a side that will not be read this run is left for the run
        that reads it, so no held neighbour is downloaded for nothing.
        """
        available = {printing.code for printing in self.printings if printing.xml} | set(attempted)
        needed = set(attempted)
        for older, newer in self.open_pairs:
            if older in available and newer in available:
                needed.update((older, newer))
        return needed


@dataclass(slots=True)
class Document:
    """A body read this run, and what reading it produced.

    A bulk member is parsed when its bill is built, not when its zip is read
    (``link`` names that zip), so a folder's parsed trees are never all in
    memory at once: on 119 H.R. (9,190 printings) holding them added about
    2.1 GB to the run's peak.
    """

    body: Any
    document: Any = None
    cleanup: Any = None
    chosen: BillTextFormat | None = None
    link: str | None = None


@dataclass(slots=True)
class BodyOutcome:
    families: list[BillFamilyTables] = field(default_factory=list)
    #: ``(bill_id, version_code, source)`` of listed placeholders a processed body now stands for.
    superseded: set[tuple[str, str, str]] = field(default_factory=set)
    #: Published comparisons no established neighbour pair accounts for.
    retired_pairs: set[tuple[str, ...]] = field(default_factory=set)
    refusals: dict[str, dict[str, Any]] = field(default_factory=dict)
    listings: int = 0
    zips: int = 0
    zips_not_needed: int = 0
    bulk_read: int = 0
    fetched: int = 0
    captured: int = 0


def bill_identity(bill_id: str) -> BillIdentity:
    """``119-hr-983`` back to its identity; the provider's own ``bill_id`` spelling."""
    congress, bill_type, number = bill_id.split("-")
    return BillIdentity(int(congress), bill_type, int(number))


def _version(row: Mapping[str, Any]) -> BillTextVersion:
    """The publisher's facts a ``bill_versions`` row states, as the version record they were shaped from."""
    formats = json.loads(row["offered_formats_json"]) if row.get("offered_formats_json") else []
    return BillTextVersion(
        type=row["label"],
        date=row["version_date"],
        formats=tuple(BillTextFormat(item["url"], item.get("type"), item.get("package_id")) for item in formats),
        package_id=row["package_id"],
    )


def plan_work(
    rows: Iterable[Mapping[str, Any]],
    *,
    held: Callable[[str], Collection[str]],
    xml: Callable[[str], Collection[str]],
    complete_pairs: Collection[tuple[str, str, str]],
    published_pairs: Mapping[str, Collection[tuple[str, str, str, str]]],
    shadowed: Collection[tuple[str, str]] = (),
) -> tuple[list[BillWork], set[tuple[str, ...]], set[tuple[str, str, str]]]:
    """Every bill with body or comparison work, the stale comparisons to retire, and the redundant placeholders.

    ``rows`` are the ``bill_versions`` rows of the candidate bills, prior and
    fresh; a listed row's facts win over a body row's, since the status pass
    refreshes them. A pending printing is one with a package id and no
    processed body. A comparison is needed where the provider will pair two
    printings that are, or will be, XML and none is published complete; both
    sides' documents are then needed. One pass over the rows and one over each
    bill's printings: O(rows).
    """
    by_bill: dict[str, dict[str, Mapping[str, Any]]] = defaultdict(dict)
    digests: dict[tuple[str, str], str] = {}
    for row in rows:
        bill, code = row["bill_id"], row["version_code"]
        if row["source"] == ACQUIRED_SOURCE and row.get("sha256"):
            digests[(bill, code)] = row["sha256"]
        if row["source"] not in (LISTED_SOURCE, ACQUIRED_SOURCE):
            continue
        if code not in by_bill[bill] or row["source"] == LISTED_SOURCE:
            by_bill[bill][code] = row
    work: list[BillWork] = []
    retired: set[tuple[str, ...]] = set()
    redundant: set[tuple[str, str, str]] = set()
    for bill, facts in by_bill.items():
        held_codes, xml_codes = set(held(bill)), set(xml(bill))
        listed = [
            Printing(
                code=code,
                version=_version(row),
                package_id=row["package_id"],
                held=code in held_codes,
                xml=code in xml_codes,
                sha256=digests.get((bill, code)),
            )
            for code, row in facts.items()
        ]
        order = printing_order([(printing.code, printing.version.date) for printing in listed])
        printings = [listed[index] for index in order]
        codes = [printing.code for printing in printings]
        established = {
            (codes[older], codes[newer])
            for older, newer in consecutive_pairs([(printing.code, printing.version.date) for printing in printings])
        }
        retired.update((bill, *pair) for pair in published_pairs.get(bill, ()) if (pair[0], pair[2]) not in established)
        redundant.update((bill, code, LISTED_SOURCE) for code in held_codes if (bill, code) in shadowed)
        pending = {printing.code for printing in printings if not printing.held and printing.package_id}
        can_be_xml = xml_codes | pending
        open_pairs = [
            (older, newer)
            for older, newer in zip(codes, codes[1:], strict=False)
            if (older, newer) in established
            and older in can_be_xml
            and newer in can_be_xml
            and (bill, older, newer) not in complete_pairs
        ]
        if pending or open_pairs:
            work.append(BillWork(bill, bill_identity(bill), printings, pending, open_pairs))
    return work, retired, redundant


def _groups(work: Sequence[BillWork]) -> list[tuple[tuple[int, str], list[BillWork]]]:
    """Bills by ``(congress, bill_type)``, newest Congress first: the order both routes spend their bounds in."""
    groups: dict[tuple[int, str], list[BillWork]] = defaultdict(list)
    for bill in work:
        groups[(bill.identity.congress, bill.identity.bill_type)].append(bill)
    return sorted(groups.items(), key=lambda item: (-item[0][0], item[0][1]))


def _parse(body: bytes, code: str, label: str) -> Any:
    """The section tree of an XML body, or ``None`` with the refusal logged."""
    if not engine_available():
        return None
    try:
        return parse_bill_tree(body, version=code)
    except Exception as error:  # noqa: BLE001 — an unparsed printing is a NULL tree
        logger.warning("Bill family: {} tree refused: {}", label, scrub_credential(str(error), ""))
        return None


class BodyPass:
    """One run's body work: bulk folders first, the per-package route for the rest, one build per bill."""

    def __init__(
        self,
        *,
        bills_source: BulkBillsSource | None,
        body_source: PackageBodySource | None,
        remaining: list[int],
        engine: EngineStamp,
        classify: SectionClassifier | None,
        summarize_diff: DiffSummarizer | None,
        refusals: Mapping[str, Mapping[str, Any]],
        summarize: BillSummarizer | None = None,
        bill_rows: Mapping[str, Mapping[str, Any]] | None = None,
        max_bulk_printings: int | None = None,
    ) -> None:
        self.bills_source = bills_source
        self.body_source = body_source
        self.remaining = remaining
        self.engine = engine
        self.classify = classify
        self.summarize_diff = summarize_diff
        #: A printing's plain-language summary reads its bill's title, stage and
        #: money-bill kind, from the bill's ``congress_bills`` row.
        self.summarize = summarize
        self.bill_rows = bill_rows or {}
        self.prior_refusals = refusals
        self.max_bulk_printings = MAX_BULK_PRINTINGS if max_bulk_printings is None else max_bulk_printings
        self.outcome = BodyOutcome(refusals={link: dict(entry) for link, entry in refusals.items()})
        #: Each BILLS zip this group read: its entry, and the pending printings it refused.
        self._read_zips: dict[str, tuple[Any, set[str]]] = {}

    # -- bulk ---------------------------------------------------------------
    def _listings(self, source: BulkBillsSource, congress: int, bill_type: str) -> tuple[list[Any], bool]:
        """Both sessions' listings, and whether each was answered: a 404 is a folder not yet published.

        A listing that was refused any other way leaves the group's pending
        printings unanswered, so none of them is sent to the per-package route
        as one the bulk collection does not hold.
        """
        listings = []
        answered = True
        for session in BULK_BILLS_SESSIONS:
            try:
                acquisition = source.list_folder(congress, session, bill_type)
            except (CredentialRefusedError, SourceEvidenceError):
                raise
            except BillSourceUnavailableError:
                logger.info("Bill family: BILLS {} {} session {} is not published", congress, bill_type, session)
                continue
            except Exception as error:  # noqa: BLE001 — a missing folder leaves its printings pending
                logger.warning(
                    "Bill family: BILLS {} {} session {} listing refused: {}",
                    congress,
                    bill_type,
                    session,
                    scrub_credential(str(error), ""),
                )
                answered = False
                continue
            self.outcome.listings += 1
            listings.append(acquisition.listing)
        return listings, answered

    def _refused_before(self, listing: Any) -> set[str]:
        """The packages this zip refused when last read, if the listing shows it has not moved since."""
        entry = listing.zip_entry
        prior = self.prior_refusals.get(entry.link)
        if prior is None or (prior.get("modified_at"), prior.get("size")) != (
            entry.modified_at.isoformat(),
            entry.size,
        ):
            return set()
        return set(prior.get("packages", ()))

    def _read_zips_for(
        self,
        source: BulkBillsSource,
        listings: Sequence[Any],
        wanted: Mapping[str, Sequence[tuple[BillWork, str]]],
        documents: dict[tuple[str, str], Document],
    ) -> None:
        """Download each listed zip that names a wanted package, once, and keep only those members."""
        for listing in listings:
            keep = {package for package in wanted if package in listing.members}
            if not keep:
                self.outcome.zips_not_needed += 1
                continue
            try:
                acquisition = source.acquire(listing, keep=keep.__contains__)
            except (CredentialRefusedError, SourceEvidenceError):
                raise
            except Exception as error:  # noqa: BLE001 — one folder's zip is not the run's
                logger.warning(
                    "Bill family: BILLS zip {} refused: {}", listing.zip_entry.link, scrub_credential(str(error), "")
                )
                continue
            self.outcome.zips += 1
            # Still refused: what this unchanged zip refused before and this run did not ask for again.
            refused = self._refused_before(listing) - keep
            for member in acquisition.archive.members:
                if member.body is None and member.package_id in wanted:
                    logger.warning("Bill family: BILLS member {} refused: {}", member.name, member.refusal)
                for bill, code in wanted.get(member.package_id or "", ()):
                    if member.body is None:
                        if code in bill.pending:
                            refused.add(member.package_id)
                        continue
                    self.outcome.bulk_read += 1
                    chosen = next(
                        (item for item in bill.by_code[code].version.formats if format_name(item) == "xml"), None
                    )
                    documents[(bill.bill_id, code)] = Document(member.body, chosen=chosen, link=listing.zip_entry.link)
            self._read_zips[listing.zip_entry.link] = (listing.zip_entry, refused)

    # -- per package -----------------------------------------------------------
    def _fetch(self, bill: BillWork, code: str, documents: dict[tuple[str, str], Document]) -> None:
        """One printing through the per-package route, under the run's cap, as the status pass used to."""
        printing = bill.by_code[code]
        chosen = printing.chosen
        if self.body_source is None or chosen is None or not printing.package_id or self.remaining[0] <= 0:
            return
        self.remaining[0] -= 1
        self.outcome.fetched += 1
        try:
            package = self.body_source.acquire(printing.package_id)
        except (CredentialRefusedError, SourceEvidenceError):
            raise
        except Exception as error:  # noqa: BLE001 — one printing's refusal is not the bill's
            logger.warning(
                "Bill family: {} {} body refused: {}", printing.package_id, code, scrub_credential(str(error), "")
            )
            return
        body = package.body_capture
        document = cleanup = None
        if package.format == "xml":
            document = _parse(body.body, code, printing.package_id)
        elif package.format == "pdf":
            # Keyed on the rendition actually fetched; only the PDF branch of
            # ``body_text`` yields the ``GpoCleanupRecord`` the cleanup_* columns
            # describe, through its default PyMuPDF reader (measured
            # ``docs/research/gpo-normalizer-vs-upstream-2026-09-19.md`` in spicy-docs).
            try:
                cleanup = body_text(package).record
            except Exception as error:  # noqa: BLE001 — an unextracted PDF is a NULL cleanup
                logger.warning(
                    "Bill family: {} PDF text refused: {}", printing.package_id, scrub_credential(str(error), "")
                )
        documents[(bill.bill_id, code)] = Document(body, document, cleanup, chosen)

    # -- build ---------------------------------------------------------------
    def _parsed(self, bill: BillWork, code: str, read: Document | None) -> Document | None:
        """A bulk member's tree, parsed now; a member the engine refuses is not read, and is remembered."""
        if read is None or read.link is None or read.document is not None:
            return read
        read.document = _parse(read.body.body, code, bill.by_code[code].package_id or code)
        if read.document is not None:
            return read
        if code in bill.pending:
            self._read_zips[read.link][1].add(bill.by_code[code].package_id or "")
        return None

    def _remember_refusals(self) -> None:
        """Each zip read this run: what it refused, at the entry it was read at, or nothing."""
        for link, (entry, refused) in self._read_zips.items():
            if refused:
                self.outcome.refusals[link] = {
                    "modified_at": entry.modified_at.isoformat(),
                    "size": entry.size,
                    "packages": sorted(refused),
                }
            else:
                self.outcome.refusals.pop(link, None)
        self._read_zips = {}

    def _build(self, bill: BillWork, documents: Mapping[tuple[str, str], Document]) -> None:
        versions: list[BillVersionCapture] = []
        context: set[tuple[str, str]] = set()
        for printing in bill.printings:
            read = self._parsed(bill, printing.code, documents.get((bill.bill_id, printing.code)))
            # A held printing read again for a comparison is context only while
            # its bytes are the ones published; bytes the source has since
            # changed are captured anew, so its rows and the comparison agree.
            context_only = printing.held and read is not None and read.body.sha256 == printing.sha256
            if printing.held and read is not None and not context_only:
                logger.warning(
                    "Bill family: {} {} now reads {}, not the published {} — its rows are replaced",
                    bill.bill_id,
                    printing.code,
                    read.body.sha256,
                    printing.sha256,
                )
            if read is None:
                versions.append(
                    BillVersionCapture(
                        version=printing.version,
                        version_code=printing.code,
                        source=LISTED_SOURCE,
                        package_id=printing.package_id,
                        chosen_format=printing.chosen,
                    )
                )
                context.add((printing.code, LISTED_SOURCE))
                continue
            entry = BillVersionCapture(
                version=printing.version,
                version_code=printing.code,
                source=ACQUIRED_SOURCE,
                package_id=printing.package_id,
                chosen_format=read.chosen,
                body=read.body,
                document=read.document,
                cleanup=read.cleanup,
            )
            versions.append(entry)
            if context_only:
                context.add((printing.code, ACQUIRED_SOURCE))
            elif processed_capture(entry):
                self.outcome.captured += 1
                self.outcome.superseded.add((bill.bill_id, printing.code, LISTED_SOURCE))
        tables = build_bill_printings(
            bill.identity,
            versions,
            engine=self.engine,
            context=context,
            classify=self.classify,
            summarize=self.summarize,
            bill=self.bill_rows.get(bill.bill_id),
            summarize_diff=self.summarize_diff,
        )
        self.outcome.families.append(tables)

    def run(self, work: Sequence[BillWork]) -> BodyOutcome:
        """Each ``(congress, bill_type)`` group in turn: new bodies first, then what their comparisons need.

        The per-package route reads the pending printings bulk does not hold
        before anything is downloaded for them, so a held neighbour is fetched
        only for a printing that was actually read; then each BILLS zip naming
        a needed printing is read once; then the per-package route reads the
        held neighbours bulk does not hold; then each bill is built and its
        documents released.
        """
        bulk_budget = self.max_bulk_printings
        for (congress, bill_type), bills in _groups(work):
            source = self.bills_source if congress >= BULK_BILLS_FLOOR else None
            listings: list[Any] = []
            answered = True
            if source is not None:
                if bulk_budget <= 0:
                    continue
                bulk_budget -= sum(len(bill.pending) for bill in bills)
                listings, answered = self._listings(source, congress, bill_type)
            folder_of = {package: listing for listing in listings for package in listing.members}
            # What the bulk collection does not hold, and so the per-package route may:
            # everything before the 113th; after it, what an answered listing does not name.
            unlisted = {
                (bill.bill_id, printing.code)
                for bill in bills
                for printing in bill.printings
                if answered and (printing.package_id or "") not in folder_of
            }
            documents: dict[tuple[str, str], Document] = {}
            ordered = sorted(bills, key=lambda bill: bill.identity.number)
            for bill in ordered:
                for printing in bill.printings:
                    if printing.code in bill.pending and (bill.bill_id, printing.code) in unlisted:
                        self._fetch(bill, printing.code, documents)
            wanted: dict[str, list[tuple[BillWork, str]]] = defaultdict(list)
            neighbours: list[tuple[BillWork, str]] = []
            for bill in ordered:
                attempted = set()
                for code in bill.pending:
                    listing = folder_of.get(bill.by_code[code].package_id or "")
                    if (bill.bill_id, code) in documents or (
                        listing is not None and bill.by_code[code].package_id not in self._refused_before(listing)
                    ):
                        attempted.add(code)
                needed = bill.needed(attempted)
                for printing in bill.printings:
                    key = (bill.bill_id, printing.code)
                    if printing.code not in needed or key in documents:
                        continue
                    if (printing.package_id or "") in folder_of:
                        wanted[printing.package_id or ""].append((bill, printing.code))
                    elif key in unlisted:
                        neighbours.append((bill, printing.code))
            if source is not None:
                self._read_zips_for(source, listings, wanted, documents)
            for bill, code in neighbours:
                self._fetch(bill, code, documents)
            read: dict[str, list[tuple[str, str]]] = defaultdict(list)
            for key in documents:
                read[key[0]].append(key)
            for bill in bills:
                if bill.bill_id in read:
                    self._build(bill, documents)
                    for key in read[bill.bill_id]:
                        del documents[key]
            self._remember_refusals()
        return self.outcome


def refusal_state(raw: bytes | None) -> dict[str, dict[str, Any]]:
    """The retained text-archive refusals, or none when the metadata is absent or unreadable."""
    if not raw:
        return {}
    try:
        value = json.loads(raw)
    except ValueError:
        return {}
    return (
        {str(link): dict(entry) for link, entry in value.items() if isinstance(entry, Mapping)}
        if isinstance(value, Mapping)
        else {}
    )


def refusal_metadata(refusals: Mapping[str, Mapping[str, Any]]) -> str:
    return json.dumps({link: refusals[link] for link in sorted(refusals)}, sort_keys=True)


__all__ = [
    "ACQUIRED_SOURCE",
    "BODY_BUDGET",
    "LISTED_SOURCE",
    "MAX_BULK_PRINTINGS",
    "MAX_VERSION_FETCHES",
    "TEXT_REFUSALS_KEY",
    "BillWork",
    "BodyOutcome",
    "BodyPass",
    "BulkBillsSource",
    "PackageBodySource",
    "Printing",
    "bill_identity",
    "body_kind",
    "plan_work",
    "processed_capture",
    "refusal_metadata",
    "refusal_state",
]
