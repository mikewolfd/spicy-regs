"""Build the bill family through spicy-docs, retaining this rollup's own resume state.

BILLSTATUS supplies the CBO index and explicit per-bill outcomes. Optional
body capture and model processing keep their separate declared caps.
"""

from __future__ import annotations

import functools
import json
import httpx
import os
from collections import Counter
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol, Self

from loguru import logger
from spicy_docs.extraction.body_text import body_text
from spicy_docs.interpretation.bill_family import (
    BillFamilyCapture,
    BillFamilyTables,
    BillVersionCapture,
    BillSummarizer,
    DiffSummarizer,
    EngineStamp,
    SectionClassifier,
    installed_engine_stamp,
)
from spicy_docs.interpretation.bill_family import build_bill_family as build_family
from spicy_docs.interpretation.bill_summaries import summarize_bill, summarize_diff
from spicy_docs.interpretation.gemini_call import DEFAULT_MODEL, model_call
from spicy_docs.interpretation.section_classification import classify_sections
from spicy_docs.interpretation.vote_matching import (
    VoteMatchError,
    VoteReference,
    read_vote_key,
    recorded_vote_references,
)
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.tables import bill_id as bill_key, text
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.schemas.activity_events import activity_events, snapshot_from_rows
from spicy_docs.sources.congress.bill_status import (
    BillAction,
    BillIdentity,
    BillLaw,
    BillSponsor,
    BillSourceError,
    BillStatus,
    bill_package_id_from_url,
)
from spicy_docs.sources.congress.bill_tree import engine_available, parse_bill_tree
from spicy_docs.sources.congress.bill_versions import (
    DEFAULT_FORMAT_PREFERENCE,
    VERSION_CODES,
    VersionCodeError,
    bill_version_package_id,
    choose_format,
    version_slug,
)
from spicy_docs.sources.congress.bulk_status import (
    BulkListingEntry,
    BulkStatusAcquirer,
    bulk_status_locator,
)
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import (
    API_KEY_ENV_VARS,
    _MAX_PAGES,
    _resolve_api_key,
    bill_detail,
    listing_reader,
)
from spicy_regs.transforms.congress_scope import (
    BULK_STATUS_FLOOR,
    bill_types_from_env,
    bulk_status_budget,
    congresses_from_env,
)
from spicy_regs.transforms.model_call import resolve_gemini_key
from spicy_regs.transforms.table_merge import merge_contract_table, merge_table, published_table

#: The DeltaTrack commit the vendored wheel was built from. ``installed_engine_stamp``
#: reads a revision out of ``direct_url.json``, which only a git install writes;
#: this repository vendors wheels on purpose, so the stamp it returns has an
#: empty revision. The commit is a fact of the vendoring, recorded in
#: ``vendor/README.md`` beside the wheel's SHA-256, and is supplied here rather
#: than left blank — ``section_diffs.engine_revision`` is the column that says
#: which engine produced a diff.
DELTATRACK_REVISION = "c636448ba08d55bba7cb8c884aad0f5ac1ccf2f6"

#: Three requests per printing (summary, MODS, body), paced at ~3/s.
BODY_BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=24 * 1024 * 1024,  # MAX_EVIDENCE_BYTES
    max_metadata_bytes=4 * 1024 * 1024,
    timeout_seconds=120.0,
    min_request_interval_seconds=0.34,
)

#: ~3 requests each at ~3/s: 600 printings is ~10 minutes.
MAX_VERSION_FETCHES = 600

#: How many distinct refusal reasons a run's log names before it stops and says
#: how many it did not. Distinct reasons, not refusals: a prompt every bill
#: refuses is one reason and one line. The cap is only against a reason that
#: embeds a publisher string and so varies per row.
REFUSAL_REASONS_LOGGED = 20

#: The per-folder listing entries this rollup retains between runs, so the next
#: run can prove a BILLSTATUS zip has not moved without downloading it. It is
#: processing state, not a contract table: nothing in ``spicy_docs.schemas``
#: shapes it, and the dictionary documents it as this rollup's own.
ARCHIVES_TABLE = "bill_family_archives"

#: The four fields ``BulkStatusAcquirer.acquire`` actually compares (``name``
#: and ``link`` to prove the entry describes *this* folder's zip, ``modified_at``
#: and ``size`` to prove it has not moved), the publisher's own stamp string the
#: instant was read from, the folder the entry belongs to, and when it was seen.
ARCHIVE_COLUMNS: tuple[str, ...] = (
    "name",
    "link",
    "formatted_last_modified_time",
    "modified_at",
    "size",
    "congress",
    "bill_type",
    "observed_at",
)

#: One entry per folder, and the folder is what a run looks it up by.
ARCHIVE_IDENTITY: tuple[str, ...] = ("congress", "bill_type")

# Qualifies status, bodies and applicable XML pairs, not optional model work.
# A source listing stamp alone never proves that this work completed.
ARCHIVE_COMPLETION_KEY = "spicy_regs.bill_family.completed_archive_scopes.v1"

#: The roll calls each bill's own actions record, one row per ``recordedVotes``
#: entry. Not a contract table — nothing in ``spicy_docs.schemas`` shapes it —
#: but a published output all the same, because the votes rollup reads it.
VOTE_REFERENCES_TABLE = "bill_vote_references"

#: The six sealed fields of a recorded vote (``vote_matching.RECORDED_VOTE_FIELDS``)
#: as the reference carries them, the action they sat on, the seventh field
#: the guide documents and the publisher may resume sending, and the capture.
VOTE_REFERENCE_COLUMNS: tuple[str, ...] = (
    "bill_id",
    "chamber",
    "congress",
    "session",
    "roll_number",
    "action_index",
    "url",
    "date",
    "full_action_name",
    "observed_at",
)

#: One row per recorded vote per action. The publisher attaches the same roll
#: call to each floor action it settled (measured: a passage vote sits on both
#: "On passage" and "Motion to reconsider"), so the action is part of the key.
#: ``url``, ``date`` and ``full_action_name`` are values, not key parts:
#: ``full_action_name`` is NULL on every entry measured, and a NULL key part
#: would have ``merge_table`` drop the row rather than publish it.
VOTE_REFERENCE_IDENTITY: tuple[str, ...] = ("bill_id", "chamber", "congress", "session", "roll_number", "action_index")

#: The oldest Congress the ``bill`` list route reaches (measured:
#: ``bill/{congress}?format=json&limit=1`` answers a declared count for every
#: Congress from the 82nd on — the A11 receipt's ``declared-counts.json``).
#: Naming an older one refuses the run rather than walking into an empty
#: success that would read as absence.
LIST_ROUTE_FLOOR = 82

#: The backfill's own retained state, beside ``bill_family_archives``. One row
#: per bill the backfill has attempted: filled (``refusal`` NULL, the
#: ``congress_bills`` row is published) or refused (``refusal`` names the
#: error class). That is the CRS summaries resume pattern this repository's
#: AGENTS.md points at — resume skips only a success and retries every
#: failure — and, per ``(congress, bill_type)`` walked, the route's declared
#: total against what was walked.
BACKFILLS_TABLE = "bill_family_backfills"
BACKFILL_WALKS_TABLE = "bill_family_backfill_walks"

#: The stamp a state row keeps is the list record's own
#: ``updateDateIncludingText`` exactly as that walk saw it — compared as a
#: string, because both walks read the same route shape, and an old Congress's
#: list records truncate the instant to a date while its detail records do not.
#: ``refusal`` is the error's class name only, never its message: a class name
#: cannot carry a credential or a publisher's text, and the run log has the
#: scrubbed message.
BACKFILL_COLUMNS: tuple[str, ...] = (
    "congress",
    "bill_type",
    "number",
    "list_update_date_including_text",
    "refusal",
    "observed_at",
)

#: One row per attempted bill; the bill is what a walk looks it up by.
BACKFILL_IDENTITY: tuple[str, ...] = ("congress", "bill_type", "number")

#: One row per ``(congress, bill_type)`` walked — the same unit as
#: ``bill_family_archives``, because ``bill/{congress}/{type}`` declares its
#: own count, so the scope's ``BILL_FAMILY_BILL_TYPES`` narrows the walk the
#: way it narrows the zips and each unit settles on its own declared total.
#: Rewritten each walk (fresh ``observed_at`` wins the merge); a settled unit
#: is not walked and keeps its prior row.
BACKFILL_WALK_COLUMNS: tuple[str, ...] = (
    "congress",
    "bill_type",
    "declared_count",
    "records_walked",
    "pages_walked",
    "list_completed",
    "unwalkable_count",
    "repeated_count",
    "backfilled_count",
    "observed_at",
)

BACKFILL_WALK_IDENTITY: tuple[str, ...] = ("congress", "bill_type")

#: The ``congress_bills`` columns a backfilled row NULLs after ``shape_bill``
#: because the detail record cannot substantiate them. The counts are stated
#: by the record only as sub-route ``count`` values — the items are the
#: sub-route's own requests, which the backfill does not walk — and a
#: published zero where the publisher declared a positive count would be a
#: false statement, not a smaller one (``cosponsor_count`` is the exception:
#: the record does state ``cosponsors.count``, and ``_backfill_bill_row``
#: publishes that). ``stage`` is the shaper's default rung ("introduced")
#: reached because the status carried no actions to classify, which is not a
#: finding about the bill — the A11 sample row "Became Public Law" and would
#: have said introduced. ``signed_date_rule`` would state
#: ``public_law_without_became_law_action``, a rule whose count the stage
#: module documents as the measure of whether a fallback is needed; on the
#: detail route no action was examined, so the rule did not run on the bill.
#: ``schema_version`` has no detail counterpart at all, so its NULL is also the
#: provenance marker: a row for a Congress below the bulk floor (where no
#: BILLSTATUS exists) with these columns NULL came from the API detail route,
#: and the dictionary prose pins that reading beside the column.
BACKFILL_UNSUBSTANTIATED: tuple[str, ...] = (
    "schema_version",
    "action_count",
    "committee_count",
    "version_count",
    "subject_count",
    "subjects_json",
    "related_bill_count",
    "related_bills_json",
    "stage",
    "signed_date_rule",
)

#: The three tables ``public_activity_events`` compares between runs.
SNAPSHOT_TABLES = ("congress_bills", "bill_versions", "bill_summaries")

#: ``snapshot_from_rows`` names its arguments after the contracts, except
#: for bills, which it calls ``bills`` rather than ``congress_bills``.
_SNAPSHOT_ARG = {
    "congress_bills": "bills",
    "bill_versions": "bill_versions",
    "bill_summaries": "bill_summaries",
}

#: The ``source`` value a printing carries once its body has been captured.
#: A held printing also needs captured bytes and applicable parse evidence.
ACQUIRED_SOURCE = "govinfo"

#: Provider-owned shapes, including the CBO publication index.
FAMILY_TABLES: tuple[tuple[str, str], ...] = (
    ("congress_bills", "bills"),
    ("bill_actions", "bill_actions"),
    ("bill_committees", "bill_committees"),
    ("bill_publisher_summaries", "bill_publisher_summaries"),
    ("bill_versions", "bill_versions"),
    ("bill_sections", "bill_sections"),
    ("section_diffs", "section_diffs"),
    ("section_diff_items", "section_diff_items"),
    ("financial_changes", "financial_changes"),
    ("section_classifications", "section_classifications"),
    ("bill_summaries", "bill_summaries"),
    ("diff_summaries", "diff_summaries"),
    ("cbo_cost_estimates", "cbo_cost_estimates"),
)


class BulkStatusSource(Protocol):
    """What this transform needs of a BILLSTATUS archive acquirer.

    Structural on purpose: the real ``BulkStatusAcquirer`` satisfies it, and so
    does the fixture-backed stub in ``tests/test_bill_family.py``. Naming the
    concrete class here would make the hermetic test unrepresentable.
    """

    def acquire(self, congress: int, bill_type: str, *, unchanged_since: BulkListingEntry | None = ...) -> Any: ...

    def list_archives(self, congress: int, bill_type: str) -> Any: ...


class PackageBodySource(Protocol):
    """What this transform needs of a GovInfo package-body acquirer.

    No ``prefer``: the rendition order is the acquirer's sealed default now
    (``sources.govinfo.bodies.BODY_PREFERENCE``), so this transform neither
    passes one nor needs a seam that accepts one.
    """

    def acquire(self, package_id: str, *, max_bytes: int | None = ...) -> Any: ...


class ListBackfillSource(Protocol):
    """What the pre-BILLSTATUS backfill needs of the ``bill`` route.

    Structural on purpose: :class:`CongressListBackfill` satisfies it with
    spicy-docs' reader over the seam ``sources.congress_bills`` owns, and the
    hermetic test stubs it with retained pages. ``pages`` yields the reader's
    page objects (``records``, ``declared_count``, a capture carrying
    ``observed_at``) for one ``(congress, bill_type)`` to the walk's terminal
    page or refusal; ``detail`` returns one bill's detail mapping beside the
    instant it was captured at.
    """

    def __enter__(self) -> Self: ...

    def __exit__(self, *_error: object) -> None: ...

    def pages(self, congress: int, bill_type: str) -> Iterator[Any]: ...

    def detail(self, identity: BillIdentity) -> tuple[Mapping[str, Any], str]: ...


class CongressListBackfill:
    """The production :class:`ListBackfillSource`: one reader instance for the run.

    ``listing_reader`` and ``bill_detail`` are the only constructor and the
    only single-request primitive this repository has for spicy-docs'
    ``CongressListingReader`` on the bill route (see
    ``sources.congress_bills``); this class composes them into the two calls
    the backfill loop makes, so the backfill is not a second reader of the
    route. The reader's budget is per request (see ``listing_reader``); the
    run's bound is the transform's shared ``max_version_fetches`` cap, which
    charges every page and every detail.
    """

    def __init__(self, api_key: str, transport: httpx.BaseTransport | None = None) -> None:
        self._reader = listing_reader(api_key, transport)

    def __enter__(self) -> Self:
        self._reader.__enter__()
        return self

    def __exit__(self, *_error: object) -> None:
        self._reader.__exit__(*_error)

    def pages(self, congress: int, bill_type: str) -> Iterator[Any]:
        from spicy_docs.sources.congress.listing import bill_list_url

        return self._reader.bills(bill_list_url(congress=congress, bill_type=bill_type), max_pages=_MAX_PAGES)

    def detail(self, identity: BillIdentity) -> tuple[Mapping[str, Any], str]:
        record, capture = bill_detail(self._reader, identity)
        return record, str(capture.observed_at)


def engine_stamp() -> EngineStamp:
    """The installed engine's stamp, with the vendored commit filled in."""
    stamp = installed_engine_stamp()
    return stamp if stamp.revision else EngineStamp(stamp.name, stamp.version, DELTATRACK_REVISION)


def _needed_printings(codes: Sequence[str], held: Collection[str]) -> set[int]:
    """Which printings this run must fetch: the unheld ones and their neighbours.

    A printing already captured needs no second fetch — its row, sections and
    digests are published. But a diff is computed over *consecutive* pairs, so
    a new printing's predecessor has to be in hand for that pair to exist.
    Fetching the unheld printings plus their immediate neighbours is the
    smallest set that leaves no diff uncomputed, and on a bill whose printings
    are all held it is empty, which is the common case and the whole saving.
    """
    needed: set[int] = set()
    for index, code in enumerate(codes):
        if code in held:
            continue
        needed.update({index - 1, index, index + 1})
    return {index for index in needed if 0 <= index < len(codes)}


def _version_captures(
    status: Any,
    acquirer: PackageBodySource | None,
    budget: list[int],
    held: Collection[str] = (),
    retry_codes: Collection[str] = (),
) -> list[BillVersionCapture]:
    """One :class:`BillVersionCapture` per printing this run has anything to say about.

    A printing whose body is not fetched still gets a capture when the run is
    seeing it for the first time — the publisher's own facts about it (type,
    date, offered formats) are real and belong in ``bill_versions``, and the
    NULL body columns say what is missing.

    A fully completed bill emits no new captures. During partial work, held
    printings still appear as metadata-only ``congress`` placeholders when they
    cannot be refetched. This keeps source order intact and prevents a diff
    across a missing middle printing; it never overwrites a retained complete
    ``govinfo`` body row with failed or absent processing facts.

    **PDF is now in the preference, last.** Both halves of the choice take the
    sealed order from spicy-docs rather than a local one: ``choose_format``
    reads Congress.gov's stated links in ``DEFAULT_FORMAT_PREFERENCE``, and the
    acquirer fetches the GovInfo rendition in ``bodies.BODY_PREFERENCE``, which
    is the same order spelled in that module's own names. Bills before the
    113th Congress offer no XML at all (measured:
    ``spicy-docs/docs/research/pdf-only-corpus-2026-09-19.md``), so under the
    previous local ``("xml", "txt")`` they yielded no body at all. They now
    yield one, through :func:`body_text`, whose PDF branch is the GPO
    normalizer — and that run's ``GpoCleanupRecord`` is what fills the version
    row's ``cleanup_*`` columns. A PDF printing still has no section tree
    (``parse_bill_tree`` reads XML), so it has no ``bill_sections`` rows and a
    pair it belongs to is refused by name through the existing
    ``FamilyRefusal`` path rather than diffed against nothing.
    """
    captures: list[BillVersionCapture] = []
    printings = _ordered_printings(status)
    codes = [version_slug(version.type) for version in printings]
    needed = _needed_printings(codes, held) | {index for index, code in enumerate(codes) if code in retry_codes}

    if not needed:
        return []

    # A one-body budget must reach an uncaptured body before refetching a held
    # neighbour. The provider orders returned captures before computing diffs.
    for index in sorted(range(len(printings)), key=lambda index: (codes[index] in held, index)):
        version = printings[index]
        version_code = codes[index]
        chosen = choose_format(version.formats, prefer=DEFAULT_FORMAT_PREFERENCE)
        package_id = version.package_id
        if package_id is None and chosen is not None:
            package_id = bill_package_id_from_url(status.identity, chosen.url)
        if package_id is None:
            try:
                package_id = bill_version_package_id(status.identity, version_code)
            except VersionCodeError as error:
                # A printing whose slug the sealed vocabulary knows but whose
                # GovInfo suffix it does not: `Private Law` is the measured case
                # (`private-law` resolves as a slug, and `govinfo_suffix` refuses
                # it, because GovInfo publishes private laws as PLAW `pvtl` and
                # not as a BILLS printing). Derived here rather than inside the
                # acquire below, this refusal used to abort the whole run --
                # seventeen tables lost to one printing of one bill, on a
                # cold-start walk of the 119th (receipt
                # `d1-measured-run-2026-09-19/`, first attempt).
                #
                # The wheel already answers the same refusal twice for the same
                # reason -- `BillVersionCapture.version_code_is_reprint_ambiguous`
                # says so in as many words ("rather than left to abort a whole
                # bill over one unrecognised printing") and `_sorted_versions`
                # tolerates the type. This is the third place, and it answers it
                # the way this function already handles a printing it cannot
                # fetch: the row still carries the publisher's own facts about
                # the printing, and the columns that need an address are NULL.
                # `package_id` is `str | None` on the capture for exactly this.
                logger.warning(
                    "Bill family: {} {} names no GovInfo package: {}",
                    bill_key(status.identity),
                    version_code,
                    scrub_credential(str(error), ""),
                )

        body = None
        document = None
        cleanup = None
        source = "congress"
        # ``package_id`` is what the acquirer is addressed by, so an unaddressable
        # printing is not fetched and does not spend the run's budget; it still
        # gets its row below from the publisher's own facts.
        if index in needed and acquirer is not None and chosen is not None and package_id is not None and budget[0] > 0:
            budget[0] -= 1
            try:
                package = acquirer.acquire(package_id)
            except CredentialRefusedError:
                raise
            except Exception as error:  # noqa: BLE001 — one printing's refusal is not the bill's
                logger.warning(
                    "Bill family: {} {} body refused: {}",
                    package_id,
                    version_code,
                    scrub_credential(str(error), ""),
                )
            else:
                body = package.body_capture
                source = "govinfo"
                if package.format == "xml" and engine_available():
                    try:
                        document = parse_bill_tree(body.body, version=version_code)
                    except Exception as error:  # noqa: BLE001 — an unparsed printing is a NULL tree
                        logger.warning("Bill family: {} tree refused: {}", package_id, scrub_credential(str(error), ""))
                elif package.format == "pdf":
                    # Keyed on the rendition actually fetched, not on the link
                    # chosen: the acquirer picks from what the package MODS
                    # offers, and only the PDF branch of ``body_text`` yields
                    # the ``GpoCleanupRecord`` the cleanup_* columns describe.
                    # No extractor argument: ``body_text``'s default
                    # (``DocumentExtractor(NativeText())``, PyMuPDF) is the
                    # pipeline ``gpo_normalize`` was derived on — pypdf glues
                    # the GPO gutter number onto its content line, so the
                    # normalizer never detects the layout through it (measured
                    # ``docs/research/gpo-normalizer-vs-upstream-2026-09-19.md``
                    # in spicy-docs).
                    try:
                        cleanup = body_text(package).record
                    except Exception as error:  # noqa: BLE001 — an unextracted PDF is a NULL cleanup
                        logger.warning(
                            "Bill family: {} PDF text refused: {}", package_id, scrub_credential(str(error), "")
                        )

        entry = BillVersionCapture(
            version=version,
            version_code=version_code,
            source=source,
            package_id=package_id,
            chosen_format=chosen,
            body=body,
            document=document,
            cleanup=cleanup,
        )
        # Keep metadata placeholders between fetched printings: omitting a held
        # middle printing would make the provider diff nonconsecutive versions.
        # The congress source preserves listed facts without replacing an older
        # complete govinfo row when its neighbour refresh failed or was capped.
        if version_code in held and not _processed_capture(entry):
            entry = replace(entry, source="congress", body=None, document=None, cleanup=None)
        captures.append(entry)
    return captures


def _ordered_printings(status: Any) -> list[Any]:
    """Use publisher dates and the owner's public vocabulary declaration order."""
    order = {entry.slug: index for index, entry in enumerate(VERSION_CODES)}
    return sorted(
        (version for version in status.text_versions if version.type),
        key=lambda version: (getattr(version, "date", None) or "", order.get(version_slug(version.type), len(order))),
    )


def _body_kind(content_type: str | None, format_name: str | None) -> str | None:
    """Classify a fetched body as ``pdf``, ``xml``, or the source's own format name."""
    media = (content_type or "").split(";", 1)[0].strip().lower()
    if media == "application/pdf":
        return "pdf"
    if media in {"application/xml", "text/xml"} or media.endswith("+xml"):
        return "xml"
    return format_name


def _processed_capture(entry: BillVersionCapture) -> bool:
    """Whether a captured body carries the derived artifact its kind needs (XML tree or PDF cleanup)."""
    if entry.body is None:
        return False
    kind = _body_kind(entry.body.content_type, entry.format_name)
    if kind == "xml":
        return entry.document is not None
    if kind == "pdf":
        return entry.cleanup is not None
    return True


@dataclass(frozen=True, slots=True)
class PriorIndex:
    """Published status stamps, processed bodies and completed XML pairs."""

    bill_text_dates: Mapping[str, str | None]
    printings: Collection[str]
    xml_printings: Collection[str] = field(default_factory=frozenset)
    pairs: Collection[tuple[str, str, str]] = field(default_factory=frozenset)
    pending_bills: Collection[str] = field(default_factory=frozenset)

    @property
    def is_cold(self) -> bool:
        """True when the prior tables held no bills and no processed printings."""
        return not self.bill_text_dates and not self.printings

    def held_codes(self, bill_id: str) -> set[str]:
        """The version codes this bill has a published processed body for."""
        prefix = f"{bill_id}\x1f"
        return {key[len(prefix) :] for key in self.printings if key.startswith(prefix)}

    def xml_codes(self, bill_id: str) -> set[str]:
        """The version codes this bill has a complete published XML body and section rows for."""
        prefix = f"{bill_id}\x1f"
        return {key[len(prefix) :] for key in self.xml_printings if key.startswith(prefix)}

    def pending_pairs(
        self, status: Any, xml_codes: Collection[str] | None = None, completed: Collection[tuple[str, str, str]] = ()
    ) -> set[tuple[str, str]]:
        """The consecutive XML printing pairs of this bill that have no published diff yet."""
        identifier = bill_key(status.identity)
        xml = self.xml_codes(identifier) if xml_codes is None else xml_codes
        codes = [version_slug(version.type) for version in _ordered_printings(status)]
        return {
            (older, newer)
            for older, newer in zip(codes, codes[1:])
            if older in xml
            and newer in xml
            and (identifier, older, newer) not in self.pairs
            and (identifier, older, newer) not in completed
        }


def _download_prior(output_dir: Path, download_prior: Callable[[str, Path], bool]) -> dict[str, Path | None]:
    """Fetch each prior table this run reads before merging, to the path the merge reuses in place."""
    return {
        name: published_table(output_dir, name, download_prior)
        for name in (
            *SNAPSHOT_TABLES,
            "bill_sections",
            "section_diffs",
            "section_diff_items",
            "cbo_cost_estimates",
            ARCHIVES_TABLE,
            BACKFILLS_TABLE,
            BACKFILL_WALKS_TABLE,
        )
    }


def _prior_index(paths: Mapping[str, Path | None]) -> PriorIndex:
    """Read the skip lookups out of the prior tables without materialising them."""
    import duckdb

    text_dates: dict[str, str | None] = {}
    version_counts: dict[str, int | None] = {}
    bills_path = paths.get("congress_bills")
    wants_versions = bills_path is not None and _has_columns(bills_path, ("bill_id", "version_count"))
    wants_text = (
        bills_path is not None
        and paths.get("cbo_cost_estimates") is not None
        and _has_columns(bills_path, ("bill_id", "update_date_including_text", "cbo_cost_estimates_outcome"))
    )
    if wants_versions and wants_text:
        # One scan serves both lookups where the prior used two: the text-dates
        # subset is split out of the same rows instead of re-reading the file.
        rows = duckdb.sql(
            f"SELECT bill_id, version_count, update_date_including_text, cbo_cost_estimates_outcome "
            f"FROM read_parquet('{bills_path}')"
        ).fetchall()
        version_counts = {bill: _int_or_none(count) for bill, count, _text, _outcome in rows}
        text_dates = {bill: text for bill, _count, text, outcome in rows if outcome is not None}
    elif wants_versions:
        version_counts = {
            bill: _int_or_none(count)
            for bill, count in duckdb.sql(
                f"SELECT bill_id, version_count FROM read_parquet('{bills_path}')"
            ).fetchall()
        }
    elif wants_text:
        text_dates = dict(
            duckdb.sql(
                f"SELECT bill_id, update_date_including_text FROM read_parquet('{bills_path}') "
                "WHERE cbo_cost_estimates_outcome IS NOT NULL"
            ).fetchall()
        )

    printings: set[str] = set()
    listed: dict[str, set[str]] = {}
    dates: dict[tuple[str, str], str] = {}
    xml_printings: set[str] = set()
    section_counts = {}
    sections_path = paths.get("bill_sections")
    if sections_path is not None and not _has_columns(sections_path, ("bill_id", "version_code", "source")):
        sections_path = None
    if sections_path is not None:
        section_counts = {
            (bill, code): count
            for bill, code, count in duckdb.sql(
                f"SELECT bill_id, version_code, count(*) FROM read_parquet('{sections_path}') "
                "WHERE source = 'govinfo' GROUP BY bill_id, version_code"
            ).fetchall()
        }
    versions_path = paths.get("bill_versions")
    needed = (
        "bill_id",
        "version_code",
        "source",
        "sha256",
        "byte_size",
        "format_name",
        "content_type",
        "section_count",
        "cleanup_json",
        "version_date",
    )
    versions_qualified = versions_path is not None and _has_columns(versions_path, needed)
    if versions_qualified:
        for row in (
            duckdb.sql(f"SELECT {', '.join(needed)} FROM read_parquet('{versions_path}')").to_arrow_table().to_pylist()
        ):
            if row["source"] in {"congress", ACQUIRED_SOURCE}:
                listed.setdefault(row["bill_id"], set()).add(row["version_code"])
                dates[(row["bill_id"], row["version_code"])] = row["version_date"] or ""
            if row["source"] != ACQUIRED_SOURCE or not row["sha256"] or not row["byte_size"]:
                continue
            key = f"{row['bill_id']}\x1f{row['version_code']}"
            kind = _body_kind(row["content_type"], row["format_name"])
            if kind == "xml":
                count = row["section_count"]
                if sections_path is None or count is None or not count.isdigit():
                    continue
                if int(count) != section_counts.get((row["bill_id"], row["version_code"]), 0):
                    continue
                xml_printings.add(key)
            elif kind == "pdf" and row["cleanup_json"] is None:
                continue
            elif kind is None:
                continue
            printings.add(key)
    pairs: set[tuple[str, str, str]] = set()
    diffs_path = paths.get("section_diffs")
    items_path = paths.get("section_diff_items")
    pair_identity = TABLE_CONTRACTS["section_diffs"].identity
    if (
        diffs_path is not None
        and items_path is not None
        and _has_columns(diffs_path, (*pair_identity, "item_count"))
        and _has_columns(items_path, (*pair_identity, "seq"))
    ):
        columns = ", ".join(pair_identity)
        child_counts = {
            tuple(row[:-1]): row[-1]
            for row in duckdb.sql(
                f"SELECT {columns}, count(*) FROM read_parquet('{items_path}') GROUP BY {columns}"
            ).fetchall()
        }
        for row in duckdb.sql(f"SELECT {columns}, item_count FROM read_parquet('{diffs_path}')").fetchall():
            key, count = tuple(row[:-1]), _int_or_none(row[-1])
            if (
                key[2] == ACQUIRED_SOURCE
                and key[4] == ACQUIRED_SOURCE
                and count is not None
                and count >= 0
                and count == child_counts.get(key, 0)
            ):
                pairs.add((key[0], key[1], key[3]))
    logger.info("Bill family: prior holds {:,} bills and {:,} processed printings", len(text_dates), len(printings))
    pending_bills = {
        bill for bill, codes in listed.items() if any(f"{bill}\x1f{code}" not in printings for code in codes)
    }
    # The surviving version rows cannot establish which source rows were lost.
    # Source counts are independent of acquisition paths: congress/govinfo
    # duplicates count once, and uploaded/PDF-twin rows never fill a source gap.
    pending_bills.update(
        bill
        for bill in text_dates
        if not versions_qualified
        or version_counts.get(bill) != len(listed.get(bill, ()))
    )
    order = {entry.slug: position for position, entry in enumerate(VERSION_CODES)}
    for bill, codes in listed.items():
        ordered = sorted(codes, key=lambda code: (dates[(bill, code)], order.get(code, len(order))))
        if any(
            f"{bill}\x1f{older}" in xml_printings
            and f"{bill}\x1f{newer}" in xml_printings
            and (bill, older, newer) not in pairs
            for older, newer in zip(ordered, ordered[1:])
        ):
            pending_bills.add(bill)
    return PriorIndex(text_dates, printings, xml_printings, pairs, pending_bills)


def _complete_child_scopes(
    parent: str,
    parents: Sequence[Mapping[str, Any]],
    children: Sequence[Mapping[str, Any]],
    count_column: str,
) -> set[tuple[str, ...]]:
    """Successfully shaped child scopes, including explicitly empty results."""
    identity = TABLE_CONTRACTS[parent].identity
    counts = Counter(tuple(row[column] for column in identity) for row in children)
    complete = set()
    for row in parents:
        key = tuple(row[column] for column in identity)
        count = _int_or_none(row[count_column])
        if all(isinstance(value, str) for value in key) and count is not None and count >= 0 and count == counts[key]:
            complete.add(key)
    return complete


def _has_columns(path: Path, needed: Sequence[str]) -> bool:
    """Whether a prior table predates the columns a lookup needs."""
    import duckdb

    present = {str(row[0]) for row in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
    missing = [column for column in needed if column not in present]
    if missing:
        logger.info("Bill family: prior {} lacks {} — treating as cold for that lookup", path.name, missing)
    return not missing


def _archive_row(entry: BulkListingEntry, *, congress: int, bill_type: str, observed_at: str | None) -> dict:
    """One ``bill_family_archives`` row: the folder's own zip entry as this run saw it."""
    return {
        "name": text(entry.name),
        "link": text(entry.link),
        "formatted_last_modified_time": text(entry.formatted_last_modified_time),
        "modified_at": text(entry.modified_at.isoformat()),
        "size": text(entry.size),
        "congress": text(congress),
        "bill_type": text(bill_type),
        "observed_at": text(observed_at),
    }


def _vote_reference_row(reference: VoteReference, *, full_action_name: str | None, observed_at: str | None) -> dict:
    """One ``bill_vote_references`` row from one reference the bill's own actions carry."""
    return {
        "bill_id": bill_key(reference.bill),
        "chamber": text(reference.vote.chamber),
        "congress": text(reference.vote.congress),
        "session": text(reference.vote.session),
        "roll_number": text(reference.vote.roll_number),
        "action_index": text(reference.action_index),
        "url": text(reference.url),
        "date": text(reference.date),
        "full_action_name": text(full_action_name),
        "observed_at": text(observed_at),
    }


def _full_action_name(action: Any, reference: VoteReference) -> str | None:
    """The seventh, optional field of the entry ``reference`` was read from.

    ``VoteReference`` carries the six sealed fields and not this one, so the
    entry is found again on its action by the same key the reference states.
    """
    for entry in getattr(action, "recorded_votes", ()):
        try:
            if read_vote_key(entry) == reference.vote:
                return getattr(entry, "full_action_name", None)
        except VoteMatchError:  # an entry the reader refused has no name to give
            continue
    return None


def vote_reference_rows(status: Any, *, observed_at: str | None) -> tuple[list[dict], int]:
    """Every recorded vote on one bill's actions as rows, and how many entries were refused.

    A refused entry — one missing any of the six sealed fields — costs that
    entry, not the bill; the count is what the run log reports.
    """
    references = recorded_vote_references(status.identity, status.actions)
    rows = [
        _vote_reference_row(
            reference,
            full_action_name=_full_action_name(status.actions[reference.action_index], reference)
            if reference.action_index is not None
            else None,
            observed_at=observed_at,
        )
        for reference in references.references
    ]
    return rows, len(references.refusals)


def _comparison_entry(row: Mapping[str, Any]) -> BulkListingEntry | None:
    """Rebuild, from a retained row, the entry ``acquire`` compares this folder's listing against.

    ``acquire`` reads four of :class:`BulkListingEntry`'s eleven fields:
    ``name`` and ``link``, which must name this folder's own zip or the skip is
    refused outright rather than risk comparing a different file, and
    ``modified_at`` and ``size``, which must match for the zip to be skipped.
    Those four are what this table retains. The other six the publisher states
    — display label, just-file-name, folder flag, mime type, file extension and
    formatted size — are not retained and are left empty here rather than
    guessed at: this is a comparison value, not a republication of the
    listing. ``modified_at`` round-trips through ISO 8601 rather than being
    re-parsed out of ``formatted_last_modified_time``, so the publisher's own
    ``DD-Mon-YYYY HH:MM`` rule stays spicy-docs' and is never restated here;
    the stamp string travels beside it as the evidence the instant came from.

    A row missing any of the four is no comparison value at all, and returning
    ``None`` means the next ``acquire`` downloads the zip the way a cold run
    does.
    """
    stated = [row.get(column) for column in ("name", "link", "modified_at", "size")]
    if any(value is None for value in stated):
        return None
    name, link, modified_at, size = stated
    try:
        instant = datetime.fromisoformat(str(modified_at))
        byte_size = int(str(size))
    except ValueError:
        logger.warning("Bill family: retained archive row for {} is unreadable — its zip will be downloaded", link)
        return None
    return BulkListingEntry(
        name=str(name),
        display_label="",
        just_file_name="",
        link=str(link),
        folder=False,
        formatted_last_modified_time=str(row.get("formatted_last_modified_time") or ""),
        modified_at=instant,
        mime_type=None,
        file_extension=None,
        formatted_size=None,
        size=byte_size,
    )


def _held_archives(path: Path | None) -> dict[tuple[str, str], BulkListingEntry]:
    """``(congress, bill_type)`` -> the zip entry the last run retained for that folder."""
    if path is None or not _has_columns(path, ARCHIVE_COLUMNS):
        return {}
    import duckdb
    import pyarrow.parquet as pq

    raw = (pq.read_schema(path).metadata or {}).get(ARCHIVE_COMPLETION_KEY.encode())
    try:
        completed = set(tuple(scope) for scope in json.loads(raw)) if raw else set()
    except (ValueError, TypeError):
        completed = set()
    columns = ", ".join(ARCHIVE_COLUMNS)
    held: dict[tuple[str, str], BulkListingEntry] = {}
    for row in duckdb.sql(f"SELECT {columns} FROM read_parquet('{path}')").to_arrow_table().to_pylist():
        entry = _comparison_entry(row)
        scope = (str(row["congress"]), str(row["bill_type"]))
        if entry is not None and row["congress"] and row["bill_type"] and scope in completed:
            held[scope] = entry
    logger.info("Bill family: {} folder listing entries retained from the last run", len(held))
    return held


def _comparable_entry(held: BulkListingEntry | None, congress: int, bill_type: str) -> BulkListingEntry | None:
    """The retained entry, but only while it still names this folder's own zip.

    ``acquire`` refuses an entry whose ``name`` or ``link`` differs from the
    folder's zip entry — it describes a different file, so comparing its stamp
    would risk a false skip — and it refuses it *after* reading the listing.
    Those two fields are not listing facts, though: ``bulk_status_locator``
    spells the zip's address, and ``read_bulk_listing`` proves every entry
    against that same locator before returning one. So a stale row is
    recognised here for no requests at all, and the folder falls back to a cold
    download that pays one listing read — the one ``_retained_entry`` makes to
    replace the row — instead of that plus the refused call's own.

    Checked rather than caught for the same reason: the refusal carries no
    listing to reuse, and retrying around it would ask the publisher twice for
    something this run can already answer.
    """
    if held is None:
        return None
    locator = bulk_status_locator(congress, bill_type)
    # The comparison ``acquire`` makes, against the address it would compare to.
    if (held.name, held.link) == (locator.rsplit("/", 1)[-1], locator):
        return held
    logger.warning(
        "Bill family: {} {} retained entry names {} rather than this folder's own zip — downloading it",
        congress,
        bill_type,
        held.name,
    )
    return None


def _acquire_archive(acquirer: BulkStatusSource, congress: int, bill_type: str, held: BulkListingEntry | None) -> Any:
    """One folder's archive, skipping the zip when the retained entry still describes it."""
    entry = _comparable_entry(held, congress, bill_type)
    if entry is None:
        return acquirer.acquire(congress, bill_type)
    return acquirer.acquire(congress, bill_type, unchanged_since=entry)


def _retained_entry(acquirer: BulkStatusSource, acquisition: Any, congress: int, bill_type: str) -> dict | None:
    """The row to retain for this folder, so the next run can skip its zip.

    ``acquire`` reads the folder listing only when it was given something to
    compare against, so a folder seen for the first time comes back with no
    ``listing_entry`` and the listing is asked for here — one small request,
    once per folder, and without it the next run would have nothing to prove
    the zip unchanged with and would download it again.
    """
    entry, capture = acquisition.listing_entry, acquisition.listing_capture
    if entry is None or capture is None:
        try:
            listing = acquirer.list_archives(congress, bill_type)
        except Exception as error:  # noqa: BLE001 — no entry means no skip next run, not a failed run
            logger.warning(
                "Bill family: {} {} listing refused, so the next run cannot skip its zip: {}",
                congress,
                bill_type,
                scrub_credential(str(error), ""),
            )
            return None
        entry, capture = listing.listing.zip_entry, listing.capture
    return _archive_row(entry, congress=congress, bill_type=bill_type, observed_at=capture.observed_at)


def _prior_snapshot(paths: Mapping[str, Path | None], bill_ids: Collection[str]) -> Any:
    """The prior rows for *this run's* bills only, as an event snapshot.

    Filtered in DuckDB rather than in Python: activity events only ever compare
    bills this run rebuilt, so pulling the whole published table into a list to
    compare a few thousand of them is work with no answer attached to it.
    """
    import duckdb

    rows: dict[str, list[dict]] = {name: [] for name in SNAPSHOT_TABLES}
    if not bill_ids:
        return snapshot_from_rows(**{_SNAPSHOT_ARG[name]: rows[name] for name in SNAPSHOT_TABLES})

    wanted = duckdb.sql("SELECT UNNEST(?::VARCHAR[]) AS bill_id", params=[sorted(bill_ids)])  # noqa: F841 — referenced by name in the queries below
    for name in SNAPSHOT_TABLES:
        path = paths.get(name)
        if path is None:
            continue
        columns = TABLE_CONTRACTS[name].columns
        present = {str(r[0]) for r in duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{path}')").fetchall()}
        select = ", ".join(f"t.{c}" if c in present else f"CAST(NULL AS VARCHAR) AS {c}" for c in columns)
        rows[name] = (
            duckdb.sql(f"SELECT {select} FROM read_parquet('{path}') t SEMI JOIN wanted w ON t.bill_id = w.bill_id")
            .to_arrow_table()
            .to_pylist()
        )
        logger.info("Bill family: prior {} contributes {:,} rows for this run's bills", name, len(rows[name]))
    return snapshot_from_rows(**{_SNAPSHOT_ARG[name]: rows[name] for name in SNAPSHOT_TABLES})


# --------------------------------------------------------------------------- #
# The pre-BILLSTATUS backfill: the 82nd-107th from the API ``bill`` route.
# --------------------------------------------------------------------------- #

#: A state key: ``(congress, bill_type, number)`` as the state table spells them.
type BackfillKey = tuple[str, str, str]


def _detail_str(value: Any) -> str | None:
    """A detail field as text: ``None`` stays ``None``, a number spells as the publisher wrote it.

    The reader parses JSON numbers as decimals, so ``str`` keeps ``2185`` from
    ever becoming ``2185.0`` on the way into a VARCHAR column.
    """
    return None if value is None else str(value)


def backfill_status(identity: BillIdentity, record: Mapping[str, Any]) -> BillStatus:
    """One bill's status from its detail record: what the route states, nothing else.

    ``latestAction`` (date and text only — the coded fields live in the
    actions sub-route, not the record), ``laws`` and ``sponsors`` arrive in
    the record itself; the actions, committees, titles, subjects, summaries
    and text versions arrive only as sub-route counts, so those status fields
    come back empty rather than invented, and the shaper's derived zeros and
    defaults are NULLed downstream (:data:`BACKFILL_UNSUBSTANTIATED`).
    ``schema_version`` names the BILLSTATUS schema and the detail route states
    none, so it is empty here and NULL in the published row.
    """
    latest = record.get("latestAction")
    latest_action = None
    if isinstance(latest, Mapping) and (latest.get("actionDate") is not None or latest.get("text") is not None):
        latest_action = BillAction(
            text=_detail_str(latest.get("text")),
            action_date=_detail_str(latest.get("actionDate")),
            action_time=_detail_str(latest.get("actionTime")),
            action_code=None,
            action_type=None,
            source_system_code=None,
            source_system_name=None,
        )
    title = _detail_str(record.get("title"))
    if not title:
        raise ValueError(f"{identity.congress}/{identity.bill_type}/{identity.number} detail record states no title")
    policy_area = record.get("policyArea")
    return BillStatus(
        identity=identity,
        schema_version="",
        title=title,
        origin_chamber=_detail_str(record.get("originChamber")),
        introduced_date=_detail_str(record.get("introducedDate")),
        update_date=_detail_str(record.get("updateDate")),
        update_date_including_text=_detail_str(record.get("updateDateIncludingText")),
        legislation_url=_detail_str(record.get("legislationUrl")),
        latest_action=latest_action,
        policy_area=_detail_str(policy_area.get("name")) if isinstance(policy_area, Mapping) else None,
        subjects=(),
        summaries=(),
        actions=(),
        sponsors=tuple(
            BillSponsor(_detail_str(entry.get("bioguideId")), _detail_str(entry.get("fullName")))
            for entry in record.get("sponsors") or ()
            if isinstance(entry, Mapping)
        ),
        text_versions=(),
        laws=tuple(
            BillLaw(_detail_str(entry.get("number")), _detail_str(entry.get("type")))
            for entry in record.get("laws") or ()
            if isinstance(entry, Mapping)
        ),
    )


def _backfill_identity(congress: int, bill_type: str, record: Mapping[str, Any]) -> BillIdentity | None:
    """The identity a list record names, or ``None`` when it names nothing this walk can address.

    A record whose type is not the one walked, or whose number is not a
    positive integer, cannot address a detail request from this walk at all;
    it is counted as unwalkable on the walk row rather than retried, since no
    identity exists to retry.
    """
    try:
        if str(record["type"]).lower() != bill_type:
            return None
        return BillIdentity(congress=congress, bill_type=bill_type, number=int(str(record["number"])))
    except (KeyError, TypeError, ValueError, BillSourceError):
        return None


def _sub_route_count(record: Mapping[str, Any], name: str) -> str | None:
    """A sub-route's stated ``count``, or ``None`` when the record states no such sub-route."""
    value = record.get(name)
    count = value.get("count") if isinstance(value, Mapping) else None
    return _detail_str(count) if isinstance(count, int) and not isinstance(count, bool) else None


def _backfill_bill_row(row: Any, record: Mapping[str, Any]) -> Any:
    """NULL what this row cannot substantiate, then prove it still satisfies the contract.

    :data:`BACKFILL_UNSUBSTANTIATED` names the columns. ``cosponsor_count`` is
    the one count the record *does* state — ``cosponsors`` is its own
    sub-route with a ``count`` — so it is published from there; never from
    the shaper's ``len(sponsors) - 1``, which on the detail route is the
    sponsor list and has nothing to do with cosponsors (measured: the A11
    receipt's 107/hr/3162 has one sponsor and ``cosponsors.count`` 1).
    """
    adjusted: dict[str, Any] = dict(row)
    for column in BACKFILL_UNSUBSTANTIATED:
        adjusted[column] = None
    adjusted["cosponsor_count"] = _sub_route_count(record, "cosponsors")
    # The shaper labels any stated url as BILLSTATUS's; this row's came from the detail record.
    adjusted["url_source"] = "congress_api" if adjusted.get("url") else None
    return TABLE_CONTRACTS["congress_bills"].checked(adjusted)


@dataclass(slots=True)
class BackfillState:
    """What the state table says about every bill the backfill has attempted, kept live through a run.

    ``filled`` maps a key to the list stamp it was filled under — a matching
    stamp costs no request; ``refused`` maps a key to the stamp its record
    carried when the attempt failed, and is what the next run retries first.
    A key is in one or the other, never both.
    """

    filled: dict[BackfillKey, str] = field(default_factory=dict)
    refused: dict[BackfillKey, str] = field(default_factory=dict)

    def count(self, congress: str, bill_type: str) -> tuple[int, int]:
        """``(filled, refused)`` for one walk unit."""
        prefix = (congress, bill_type)
        return (
            sum(1 for key in self.filled if key[:2] == prefix),
            sum(1 for key in self.refused if key[:2] == prefix),
        )


def _held_backfills(path: Path | None) -> BackfillState:
    """The state table as the last run left it."""
    state = BackfillState()
    if path is None or not _has_columns(path, BACKFILL_COLUMNS):
        return state
    import duckdb

    columns = ", ".join(BACKFILL_COLUMNS)
    for row in duckdb.sql(f"SELECT {columns} FROM read_parquet('{path}')").to_arrow_table().to_pylist():
        key = (str(row["congress"]), str(row["bill_type"]), str(row["number"]))
        stamp = str(row["list_update_date_including_text"] or "")
        if row["refusal"] is None:
            state.filled[key] = stamp
        else:
            state.refused[key] = stamp
    logger.info(
        "Bill family backfill: {:,} filled and {:,} refused bills retained from the last run",
        len(state.filled),
        len(state.refused),
    )
    return state


def _prior_walks(path: Path | None) -> dict[tuple[str, str], dict[str, Any]]:
    """Per walk unit, the record the last walk left: declared total, completion, unwalkable records."""
    if path is None or not _has_columns(path, BACKFILL_WALK_COLUMNS):
        return {}
    import duckdb

    columns = ", ".join(BACKFILL_WALK_COLUMNS)
    return {
        (str(row["congress"]), str(row["bill_type"])): row
        for row in duckdb.sql(f"SELECT {columns} FROM read_parquet('{path}')").to_arrow_table().to_pylist()
    }


def _int_or_none(value: Any) -> int | None:
    """Parse a value's text as an int, or None when it does not spell one."""
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _settled(walk: Mapping[str, Any] | None, state: BackfillState, congress: str, bill_type: str) -> bool:
    """Whether a walk unit needs no list requests: walked complete, and every entry accounted for.

    Accounted for means filled, refused (and so retried directly, without a
    walk), unwalkable, or a repeat of an entry already counted — the
    publisher's ``updateDate desc`` order repeats a bill across a page
    boundary when its stamp ties (measured on the 92nd: ``hr 4634`` twice,
    identical, in a walk whose declared and observed counts agreed at 767),
    so the declared total counts entries, not bills, and so does this. A unit
    that never reached its terminal page, or whose declared total is unknown,
    is walked again — pages charged to the cap — because only a walk can find
    the entries it did not reach.
    """
    if walk is None or str(walk.get("list_completed")) != "true":
        return False
    declared = _int_or_none(walk.get("declared_count"))
    if declared is None:
        return False
    filled, refused = state.count(congress, bill_type)
    accounted = (_int_or_none(walk.get("unwalkable_count")) or 0) + (_int_or_none(walk.get("repeated_count")) or 0)
    return filled + refused + accounted >= declared


@dataclass(frozen=True, slots=True)
class _BackfillOutcome:
    """What one backfill pass over the named pre-floor Congresses produced."""

    families: list[BillFamilyTables]
    touched: set[str]
    state_rows: list[dict[str, Any]]
    walk_rows: list[dict[str, Any]]
    refused: int


class _CapReached(Exception):
    """Internal: the run's shared fetch budget ran out mid-backfill."""


def _state_row(key: BackfillKey, stamp: str, refusal: str | None, observed_at: str) -> dict[str, Any]:
    congress, bill_type, number = key
    return {
        "congress": text(congress),
        "bill_type": text(bill_type),
        "number": text(number),
        "list_update_date_including_text": text(stamp),
        "refusal": text(refusal),
        "observed_at": text(observed_at),
    }


class _Backfill:
    """One run's backfill pass: the retry of retained refusals, then the walks, under one cap.

    A class rather than a closure so the three things every attempt touches —
    the live state, the shared budget and the rows to publish — are named
    once. ``attempt`` is the single place a detail request is made and its
    outcome recorded, for a retried refusal and a walked record alike.
    """

    def __init__(
        self,
        source: ListBackfillSource,
        state: BackfillState,
        remaining: list[int],
        *,
        engine: EngineStamp,
        classify: SectionClassifier | None,
        summarize: BillSummarizer | None,
        summarize_diff: DiffSummarizer | None,
    ) -> None:
        self.source = source
        self.state = state
        self.remaining = remaining
        self.build = functools.partial(
            build_family, engine=engine, classify=classify, summarize=summarize, summarize_diff=summarize_diff
        )
        self.families: list[BillFamilyTables] = []
        self.touched: set[str] = set()
        self.state_rows: list[dict[str, Any]] = []
        self.walk_rows: list[dict[str, Any]] = []
        self.refused = 0
        #: Every identity this run has requested, whatever the outcome: a bill
        #: is attempted at most once a run, so a refusal retried at the start
        #: is not asked for again when its unit's walk reaches it.
        self.attempted: set[BackfillKey] = set()

    def charge(self) -> None:
        """One request against the run's cap; the cap reached ends the pass where it stands."""
        if self.remaining[0] <= 0:
            raise _CapReached
        self.remaining[0] -= 1

    def attempt(self, identity: BillIdentity, stamp: str) -> bool:
        """One detail request for one bill: a row on success, a refused state row otherwise.

        A ``401``/``403`` propagates and aborts the run. A refusal by the
        reader, a transport failure after its retries (``ConnectionError`` is
        what spicy-docs raises then) or an ``httpx`` error is that one bill's
        gap: recorded as refused, retried first next run, never a row with
        invented values.
        """
        key: BackfillKey = (str(identity.congress), identity.bill_type, str(identity.number))
        self.charge()
        self.attempted.add(key)
        try:
            detail, detail_at = self.source.detail(identity)
            status = backfill_status(identity, detail)
            built = self.build(BillFamilyCapture(status=status, versions=(), observed_at=detail_at))
            if not built.bills:
                raise ValueError("the shaper produced no bills row")
            row = _backfill_bill_row(built.bills[0], detail)
        except CredentialRefusedError:
            raise
        except (PagedJsonSourceError, httpx.HTTPError, ConnectionError, ValueError, TypeError) as error:
            self.refused += 1
            logger.warning(
                "Bill family backfill: {} {} {} refused: {}", key[0], key[1], key[2], scrub_credential(str(error), "")
            )
            self.state.filled.pop(key, None)
            self.state.refused[key] = stamp
            self.state_rows.append(_state_row(key, stamp, type(error).__name__, _detected_at()))
            return False
        self.families.append(replace(built, bills=(row,)))
        self.touched.add(bill_key(status.identity))
        self.state.refused.pop(key, None)
        self.state.filled[key] = stamp
        self.state_rows.append(_state_row(key, stamp, None, detail_at))
        return True

    def retry_refusals(self, congresses: Sequence[int], bill_types: Sequence[str]) -> None:
        """Every retained refusal in scope, newest Congress first, one request each and no walk."""
        in_scope = {str(congress) for congress in congresses}
        for key in sorted(self.state.refused, key=lambda key: (-int(key[0]), key[1], int(key[2]))):
            if key[0] not in in_scope or key[1] not in bill_types:
                continue
            identity = BillIdentity(congress=int(key[0]), bill_type=key[1], number=int(key[2]))
            self.attempt(identity, self.state.refused[key])

    def walk(self, congress: int, bill_type: str) -> bool:
        """One walk unit's pages, each charged, each record filled unless its retained stamp matches.

        Returns whether the walk reached the route's terminal page. A walk
        that refuses (a changed declared count, a repeated continuation)
        propagates and fails the run loudly: a walk asked for and not
        received must never read as a completed one — which is also why the
        walk row states the route's declared total beside what this run
        actually walked.
        """
        declared: int | None = None
        walked = pages = unwalkable = repeated = 0
        completed = False
        observed_at = ""
        seen: set[BackfillKey] = set()
        pages_iter = iter(self.source.pages(congress, bill_type))
        try:
            while True:
                self.charge()
                page = next(pages_iter, None)
                if page is None:
                    self.remaining[0] += 1  # the terminal page was already paid for
                    completed = True
                    break
                pages += 1
                observed_at = str(getattr(page.capture, "observed_at", "") or "")
                if declared is None and page.declared_count is not None:
                    declared = int(page.declared_count)
                for record in page.records:
                    walked += 1
                    identity = _backfill_identity(congress, bill_type, record)
                    if identity is None:
                        unwalkable += 1
                        logger.warning(
                            "Bill family backfill: {} {} list record names no walkable bill", congress, bill_type
                        )
                        continue
                    stamp = _detail_str(record.get("updateDateIncludingText")) or ""
                    key: BackfillKey = (str(congress), bill_type, str(identity.number))
                    if key in seen:
                        repeated += 1
                        continue
                    seen.add(key)
                    if self.state.filled.get(key) == stamp or key in self.attempted:
                        continue
                    self.attempt(identity, stamp)
        finally:
            filled, _ = self.state.count(str(congress), bill_type)
            self.walk_rows.append(
                {
                    "congress": text(congress),
                    "bill_type": text(bill_type),
                    "declared_count": text(declared),
                    "records_walked": text(walked),
                    "pages_walked": text(pages),
                    "list_completed": text(completed),
                    "unwalkable_count": text(unwalkable),
                    "repeated_count": text(repeated),
                    "backfilled_count": text(filled),
                    "observed_at": text(observed_at),
                }
            )
            logger.info(
                "Bill family backfill: {} {} — declared {}, walked {:,} on {} page(s), {:,} filled in all",
                congress,
                bill_type,
                "?" if declared is None else f"{declared:,}",
                walked,
                pages,
                filled,
            )
        return completed


def _run_backfill(
    list_source: ListBackfillSource,
    congresses: Sequence[int],
    bill_types: Sequence[str],
    *,
    state: BackfillState,
    prior_walks: Mapping[tuple[str, str], Mapping[str, Any]],
    remaining: list[int],
    engine: EngineStamp,
    classify: SectionClassifier | None,
    summarize: BillSummarizer | None,
    summarize_diff: DiffSummarizer | None,
) -> _BackfillOutcome:
    """Retry last run's refusals, then walk every unsettled ``(congress, bill_type)``, newest first.

    The budget is the rollup's own per-run cap (``remaining``, shared with the
    bulk printings) and every request — a retried detail, a list page, a
    walked detail — is charged to it. When it runs out the pass stops where
    it stands: the walk row records how far that unit got, and the next run
    resumes on the retained state. Resume is the state table: a retained
    refusal is retried directly, without a walk; a bill whose state row still
    matches its list stamp costs no request; a unit walked complete with
    every record filled, refused or unwalkable is not walked again (a refused
    bill keeps being retried, one request a run, so a permanently gapped unit
    costs one request per gap rather than a page walk). Only an unsettled unit
    pays for its pages again, because only a walk can reach the records it
    did not.
    """
    pass_ = _Backfill(
        list_source,
        state,
        remaining,
        engine=engine,
        classify=classify,
        summarize=summarize,
        summarize_diff=summarize_diff,
    )
    with list_source:
        try:
            pass_.retry_refusals(congresses, bill_types)
            for congress in congresses:
                for bill_type in bill_types:
                    if _settled(prior_walks.get((str(congress), bill_type)), state, str(congress), bill_type):
                        logger.info(
                            "Bill family backfill: {} {} walked complete and every record accounted for — no list requests",
                            congress,
                            bill_type,
                        )
                        continue
                    pass_.walk(congress, bill_type)
        except _CapReached:
            logger.warning(
                "Bill family backfill: the per-run cap was reached — the next run resumes on the retained state: "
                "refusals retried first, filled bills skipped by their stamp, unsettled units walked again"
            )
    return _BackfillOutcome(
        families=pass_.families,
        touched=pass_.touched,
        state_rows=pass_.state_rows,
        walk_rows=pass_.walk_rows,
        refused=pass_.refused,
    )


def build_bill_family(
    output_dir: Path,
    *,
    bulk_acquirer: BulkStatusSource | None = None,
    body_acquirer: PackageBodySource | None = None,
    list_source: ListBackfillSource | None = None,
    max_version_fetches: int = MAX_VERSION_FETCHES,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> tuple[Path, ...]:
    """Build all eighteen bill-family outputs; returns one path per table."""
    if isinstance(max_version_fetches, bool) or not isinstance(max_version_fetches, int) or max_version_fetches < 0:
        raise ValueError("max_version_fetches must be a nonnegative integer; zero disables acquisition")
    congresses = congresses_from_env()
    bill_types = bill_types_from_env()
    logger.info("Bill family: Congresses {}, bill types {}", congresses, bill_types)

    # One scope input, two routes: BILLSTATUS bulk serves nothing below its
    # floor and the ``bill`` list route nothing below its own, so the named
    # Congresses split on the publisher's fact, and the backfill walks newest
    # first. A name neither route reaches refuses the run rather than walking
    # an empty success that would read as absence.
    bulk_congresses = [congress for congress in congresses if congress >= BULK_STATUS_FLOOR]
    backfill_congresses = sorted((congress for congress in congresses if congress < BULK_STATUS_FLOOR), reverse=True)
    unreachable = [congress for congress in backfill_congresses if congress < LIST_ROUTE_FLOOR]
    if unreachable:
        raise ValueError(
            f"BILL_FAMILY_CONGRESSES names {unreachable}: the bill list route reaches no older Congress than "
            f"the {LIST_ROUTE_FLOOR}nd (LIST_ROUTE_FLOOR; measured: the A11 receipt's declared-counts.json). "
            "Nothing below it can be walked, and an empty walk would read as absence."
        )

    api_key = _resolve_api_key()
    bulk_acquirer = bulk_acquirer or BulkStatusAcquirer(budget=bulk_status_budget())
    if body_acquirer is None and max_version_fetches > 0:
        if api_key:
            body_acquirer = GovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key)
        else:
            logger.warning(
                "Bill family: no api.data.gov key ({}) — publishing status-derived tables only",
                ", ".join(API_KEY_ENV_VARS),
            )

    # The model seams, wired only when a key is present. Nothing wraps them:
    # since spicy-docs 0.22.0 `build_bill_family` runs all three inside its own
    # guard and files a `FamilyRefusal` naming the reader's message, while a
    # credential refusal and a transport failure still abort. A wrapper here
    # could only re-file one of those as the other, which is what the 0.21.3
    # one had to do.
    gemini_key = resolve_gemini_key()
    classify = summarize = summarize_diff_call = None
    if gemini_key:
        from spicy_docs.extraction.gemini import GeminiClient

        model = os.environ.get("GEMINI_MODEL", DEFAULT_MODEL)
        call = model_call(GeminiClient(api_key=gemini_key))
        classify = functools.partial(classify_sections, call=call, model=model)
        summarize = functools.partial(summarize_bill, call=call, model=model)
        summarize_diff_call = functools.partial(summarize_diff, call=call, model=model)
        logger.info("Bill family: model seams wired ({})", model)
    else:
        logger.info("Bill family: no Gemini key — section_classifications, bill_summaries, diff_summaries stay empty")

    stamp = engine_stamp()
    logger.info("Bill family: engine {} {} @ {}", stamp.name, stamp.version, stamp.revision or "(unknown)")

    # 1. What the last run already published, so this one can skip it.
    prior_paths = _download_prior(output_dir, download_prior)
    index = _prior_index(prior_paths)
    held_archives = _held_archives(prior_paths.get(ARCHIVES_TABLE))
    # An unchanged archive still needs a read for legacy status rows or pending bodies/pairs.
    bills_prior = prior_paths.get("congress_bills")
    if bills_prior is not None:
        # A narrow projection replaces materialising every prior row into Python
        # dicts: only the three columns the prune decides on are read.
        if _has_columns(bills_prior, ("bill_id", "congress", "bill_type")):
            import duckdb

            rows = duckdb.sql(
                f"SELECT bill_id, congress, bill_type FROM read_parquet('{bills_prior}')"
            ).fetchall()
            for bill_id, congress, bill_type in rows:
                if bill_id not in index.bill_text_dates or bill_id in index.pending_bills:
                    held_archives.pop((congress, bill_type), None)
        else:
            import pyarrow.parquet as pq

            for row in pq.read_table(bills_prior).to_pylist():
                if row["bill_id"] not in index.bill_text_dates or row["bill_id"] in index.pending_bills:
                    held_archives.pop((row.get("congress"), row.get("bill_type")), None)

    if prior_paths.get("bill_versions") is None or bills_prior is None:
        held_archives.clear()
    completed_archives = set(held_archives)
    visited_archives: set[tuple[str, str]] = set()

    # 2. Acquire and build, one bill at a time, skipping only completed work.
    remaining = [max_version_fetches]
    families: list[BillFamilyTables] = []
    touched: set[str] = set()
    archive_rows: list[dict] = []
    vote_rows: list[dict] = []
    bills = unchanged = skipped = archives_skipped = votes_refused = 0
    for congress in bulk_congresses:
        for bill_type in bill_types:
            scope = (str(congress), bill_type)
            visited_archives.add(scope)
            acquisition = _acquire_archive(
                bulk_acquirer, congress, bill_type, held_archives.get((str(congress), bill_type))
            )
            retained = _retained_entry(bulk_acquirer, acquisition, congress, bill_type)
            if acquisition.skipped_unchanged:
                if retained is not None:
                    archive_rows.append(retained)
                # Only qualified completed scopes enter this skip. The source
                # listing then proves their BILLSTATUS archive has not moved.
                archives_skipped += 1
                logger.info(
                    "Bill family: {} {} — zip unchanged since the last run, not downloaded", congress, bill_type
                )
                continue
            archive = acquisition.archive
            observed_at = acquisition.capture.observed_at
            logger.info(
                "Bill family: {} {} — {:,} parsed, {:,} refused by the reader",
                congress,
                bill_type,
                archive.parsed_count,
                archive.refused_count,
            )
            skipped += archive.refused_count
            archive_complete = archive.refused_count == 0
            completed_archives.discard(scope)
            for member in archive.members:
                if member.status is None:
                    continue
                identifier = bill_key(member.status.identity)
                # A matching publisher stamp skips only when all listed bodies
                # and applicable XML pairs are already complete. Optional model
                # retry policy remains separate from body completeness.
                published = index.bill_text_dates.get(identifier)
                held = index.held_codes(identifier)
                codes = {version_slug(version.type) for version in _ordered_printings(member.status)}
                pending_pairs = index.pending_pairs(member.status)
                if (
                    published is not None
                    and published == member.status.update_date_including_text
                    and codes <= held
                    and not pending_pairs
                    and identifier not in index.pending_bills
                ):
                    unchanged += 1
                    continue
                capture = BillFamilyCapture(
                    status=member.status,
                    versions=tuple(
                        _version_captures(
                            member.status,
                            body_acquirer,
                            remaining,
                            held=held,
                            retry_codes={code for pair in pending_pairs for code in pair},
                        )
                    ),
                    observed_at=observed_at,
                )
                tables = build_family(
                    capture,
                    engine=stamp,
                    classify=classify,
                    summarize=summarize,
                    summarize_diff=summarize_diff_call,
                )
                processed = {entry.version_code for entry in capture.versions if _processed_capture(entry)}
                xml_codes = index.xml_codes(identifier) | {
                    entry.version_code for entry in capture.versions if entry.document is not None
                }
                completed_pairs = {
                    (bill, older, newer)
                    for bill, older, older_source, newer, newer_source in _complete_child_scopes(
                        "section_diffs", tables.section_diffs, tables.section_diff_items, "item_count"
                    )
                    if older_source == ACQUIRED_SOURCE and newer_source == ACQUIRED_SOURCE
                }
                if (
                    not tables.bills
                    or not codes <= (held | processed)
                    or index.pending_pairs(member.status, xml_codes, completed_pairs)
                ):
                    archive_complete = False
                families.append(tables)
                rows, refused = vote_reference_rows(member.status, observed_at=observed_at)
                vote_rows.extend(rows)
                votes_refused += refused
                touched.add(identifier)
                bills += 1
            if archive_complete and retained is not None:
                archive_rows.append(retained)
                completed_archives.add(scope)

    # 2b. The pre-BILLSTATUS backfill: same scope input, the route the
    # publisher serves below the bulk floor, the same per-run cap.
    backfill_state: list[dict[str, Any]] = []
    backfill_walk_state: list[dict[str, Any]] = []
    if backfill_congresses:
        source = list_source
        if source is None and api_key:
            source = CongressListBackfill(api_key)
        if source is not None:
            outcome = _run_backfill(
                source,
                backfill_congresses,
                bill_types,
                state=_held_backfills(prior_paths.get(BACKFILLS_TABLE)),
                prior_walks=_prior_walks(prior_paths.get(BACKFILL_WALKS_TABLE)),
                remaining=remaining,
                engine=stamp,
                classify=classify,
                summarize=summarize,
                summarize_diff=summarize_diff_call,
            )
            families.extend(outcome.families)
            touched.update(outcome.touched)
            backfill_state = outcome.state_rows
            backfill_walk_state = outcome.walk_rows
            if outcome.refused:
                logger.warning(
                    "Bill family backfill: {:,} detail attempts refused (retried first next run)", outcome.refused
                )
        else:
            logger.warning(
                "Bill family backfill: Congresses {} named below the bulk floor but no api.data.gov key ({}) — "
                "publishing no backfill rows",
                backfill_congresses,
                ", ".join(API_KEY_ENV_VARS),
            )

    folded = BillFamilyTables.concat(families)
    # One pass over every refusal both passes produced — `concat` carries them,
    # so the backfill's families are counted here too, which the per-bill count
    # this replaces never reached. Keyed by the reason as well as the table: a
    # `FamilyRefusal` carries the reason so someone can act on it, and a count
    # alone throws that away. The C1 defect read as "the model tables are
    # short" until the message said which keys the answer had left out.
    # Distinct reasons rather than one line per refusal keeps it bounded: a
    # systematically refused prompt is one line, not one per bill.
    refusals: Counter[str] = Counter(refusal.table for refusal in folded.refusals)
    reasons: Counter[tuple[str, str]] = Counter(
        (refusal.table, scrub_credential(refusal.reason, gemini_key or "")) for refusal in folded.refusals
    )
    logger.info(
        "Bill family: {:,} bills rebuilt, {:,} unchanged and skipped, {:,} status fetches (printings and backfill "
        "details) of {:,} allowed",
        bills,
        unchanged,
        max_version_fetches - remaining[0],
        max_version_fetches,
    )
    if remaining[0] == 0:
        logger.warning(
            "Bill family: the per-run cap was reached — the next run resumes on what this one did not reach: "
            "incomplete bulk bodies and XML pairs remain retryable; backfilled bills use their retained state"
        )
    if archives_skipped:
        logger.info(
            "Bill family: {:,} of {:,} folder zips proved unchanged and were not downloaded",
            archives_skipped,
            len(bulk_congresses) * len(bill_types),
        )
    if skipped:
        logger.warning("Bill family: {:,} archive entries the reader refused", skipped)
    if refusals:
        logger.warning("Bill family: refusals by table — {}", dict(refusals))
        for (table, reason), count in reasons.most_common(REFUSAL_REASONS_LOGGED):
            logger.warning("Bill family: {} refused {:,}x — {}", table, count, reason)
        if len(reasons) > REFUSAL_REASONS_LOGGED:
            logger.warning(
                "Bill family: {:,} further distinct refusal reasons not listed", len(reasons) - REFUSAL_REASONS_LOGGED
            )
    logger.info("Bill family: {:,} recorded-vote references on this run's bills", len(vote_rows))
    if votes_refused:
        logger.warning("Bill family: {:,} recordedVotes entries refused for a missing sealed field", votes_refused)

    # 3. The thirteenth table: what changed since the last published run,
    # compared over this run's bills only — the rest cannot have changed.
    prior = _prior_snapshot(prior_paths, touched)
    current = snapshot_from_rows(
        bills=folded.bills,
        bill_versions=folded.bill_versions,
        bill_summaries=folded.bill_summaries,
    )
    events = activity_events(prior, current, detected_at=_detected_at())
    logger.info("Bill family: {:,} activity events", len(events))

    # 4. Publish. Every table goes through the one merge helper. Each prior was
    # either downloaded above or found absent, so the merge is told rather than
    # left to retry a download that already failed.
    section_scopes = _complete_child_scopes("bill_versions", folded.bill_versions, folded.bill_sections, "section_count")
    item_scopes = _complete_child_scopes("section_diffs", folded.section_diffs, folded.section_diff_items, "item_count")

    def publish(contract: str, rows: Any) -> Path:
        return merge_contract_table(
            output_dir,
            contract,
            rows,
            download_prior=download_prior,
            prior_present=(prior_paths.get(contract) is not None) if contract in prior_paths else None,
            replace_parents=(
                "bill_id",
                {
                    identifier
                    for row in folded.bills
                    if (identifier := row["bill_id"]) is not None
                    and row.get("cbo_cost_estimates_outcome")
                    in {
                        "populated",
                        "requested-empty:absent",
                        "requested-empty:present-and-empty",
                    }
                },
            )
            if contract == "cbo_cost_estimates"
            else (TABLE_CONTRACTS["bill_versions"].identity, section_scopes)
            if contract == "bill_sections"
            else (TABLE_CONTRACTS["section_diffs"].identity, item_scopes)
            if contract == "section_diff_items"
            else None,
        )

    paths = [publish(contract, getattr(folded, attr)) for contract, attr in FAMILY_TABLES]
    paths.append(publish("public_activity_events", events))
    # The last two outputs are this repository's own tables, not contracts, so
    # they go through the same merge helper one level down.
    paths.append(
        merge_table(
            output_dir,
            name=ARCHIVES_TABLE,
            columns=ARCHIVE_COLUMNS,
            identity=ARCHIVE_IDENTITY,
            version_column="observed_at",
            rows=archive_rows,
            remote_key=f"{ARCHIVES_TABLE}.parquet",
            download_prior=download_prior,
            prior_present=prior_paths.get(ARCHIVES_TABLE) is not None,
            replace_parents=(ARCHIVE_IDENTITY, visited_archives),
            parquet_metadata={ARCHIVE_COMPLETION_KEY: json.dumps(sorted(completed_archives))},
        )
    )
    paths.append(
        merge_table(
            output_dir,
            name=VOTE_REFERENCES_TABLE,
            columns=VOTE_REFERENCE_COLUMNS,
            identity=VOTE_REFERENCE_IDENTITY,
            version_column="observed_at",
            rows=vote_rows,
            remote_key=f"{VOTE_REFERENCES_TABLE}.parquet",
            download_prior=download_prior,
        )
    )
    # The backfill's own retained state, beside the archives table: what was
    # filled under which list stamp, and, per Congress walked, the route's
    # declared total against what was reached. Both merge like the archives —
    # this run's rows win on a repeated identity, and an empty run republishes
    # the prior state untouched.
    paths.append(
        merge_table(
            output_dir,
            name=BACKFILLS_TABLE,
            columns=BACKFILL_COLUMNS,
            identity=BACKFILL_IDENTITY,
            version_column="observed_at",
            rows=backfill_state,
            remote_key=f"{BACKFILLS_TABLE}.parquet",
            download_prior=download_prior,
            prior_present=prior_paths.get(BACKFILLS_TABLE) is not None,
        )
    )
    paths.append(
        merge_table(
            output_dir,
            name=BACKFILL_WALKS_TABLE,
            columns=BACKFILL_WALK_COLUMNS,
            identity=BACKFILL_WALK_IDENTITY,
            version_column="observed_at",
            rows=backfill_walk_state,
            remote_key=f"{BACKFILL_WALKS_TABLE}.parquet",
            download_prior=download_prior,
            prior_present=prior_paths.get(BACKFILL_WALKS_TABLE) is not None,
        )
    )
    return tuple(paths)


def _detected_at() -> str:
    """The run instant every event this run detects is stamped with."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
