"""Transform: the bill family — thirteen tables from one acquisition pass.

``spicy_docs.interpretation.bill_family.build_bill_family`` does the composing:
given one bill's BILLSTATUS and its captured printings, it returns twelve row
tuples (bills, actions, committees, publisher summaries, versions, sections,
the three diff tables, the two model tables, diff summaries) plus the refusals.
This transform acquires the input, folds the per-bill results, derives the
thirteenth table (``public_activity_events``) by comparing the run against the
previously published one, and merges each through the one shared helper. It
re-derives no rule: every column is shaped in spicy-docs.

A fourteenth output, ``bill_family_archives``, is not a table of the family: it
is this rollup's own processing state, one row per BILLSTATUS folder holding
the listing entry the next run compares against (see ``ARCHIVES_TABLE``).

**Acquisition, and why it is bounded.** Bills come from the BILLSTATUS bulk
archive — one keyless zip per ``(congress, bill_type)``, so the whole scope
costs eight requests, and a folder whose zip has not moved since the last run
costs one small listing request instead of up to 32 MB of it. Printings are the
expensive half: each is a GovInfo package (summary, MODS, body — three keyed
requests), and a Congress has tens of thousands. A rollup publishes nothing
until it finishes, so an unbounded first pass would time out and persist
nothing, exactly the failure ``build_congress_bills`` documents.
``MAX_VERSION_FETCHES`` bounds the printings; the status-derived tables still
cover every bill in scope, and the coverage statement says which is which.

**The model seams.** ``classify`` and ``summarize`` are wired only when a
Gemini key is in the environment; without one they are ``None`` and the three
model tables come back empty, which is what a keyless CI run does. Nothing
about a bill changes when they are off.

**Refusals are counted.** A printing with no parsed document, a pair that
cannot be diffed, a row whose identity has a null part — ``build_bill_family``
returns each as a ``FamilyRefusal``, and this logs them by table. They are
never dropped silently and never published as a row with invented values.
"""

from __future__ import annotations

import functools
import os
from collections import Counter
from collections.abc import Callable, Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from loguru import logger
from spicy_docs.extraction.body_text import body_text
from spicy_docs.interpretation.bill_family import (
    BillFamilyCapture,
    BillFamilyTables,
    BillVersionCapture,
    EngineStamp,
    installed_engine_stamp,
)
from spicy_docs.interpretation.bill_family import build_bill_family as build_family
from spicy_docs.interpretation.bill_summaries import summarize_bill, summarize_diff
from spicy_docs.interpretation.section_classification import classify_sections
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.schemas.tables import bill_id as bill_key, text
from spicy_docs.schemas.activity_events import activity_events, snapshot_from_rows
from spicy_docs.sources.congress.bill_status import bill_package_id_from_url
from spicy_docs.sources.congress.bill_tree import engine_available, parse_bill_tree
from spicy_docs.sources.congress.bill_versions import (
    DEFAULT_FORMAT_PREFERENCE,
    bill_version_package_id,
    choose_format,
    version_slug,
)
from spicy_docs.sources.congress.bulk_status import BulkListingEntry, BulkStatusAcquirer, BulkStatusBudget
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.congress_scope import bill_types_from_env, congresses_from_env
from spicy_regs.transforms.model_call import DEFAULT_MODEL, model_call, resolve_gemini_key
from spicy_regs.transforms.table_merge import merge_contract_table, merge_table, prior_scratch_path

#: The DeltaTrack commit the vendored wheel was built from. ``installed_engine_stamp``
#: reads a revision out of ``direct_url.json``, which only a git install writes;
#: this repository vendors wheels on purpose, so the stamp it returns has an
#: empty revision. The commit is a fact of the vendoring, recorded in
#: ``vendor/README.md`` beside the wheel's SHA-256, and is supplied here rather
#: than left blank — ``section_diffs.engine_revision`` is the column that says
#: which engine produced a diff.
DELTATRACK_REVISION = "c636448ba08d55bba7cb8c884aad0f5ac1ccf2f6"

#: One zip per (congress, bill_type); the archive bounds live on the budget.
BULK_BUDGET = BulkStatusBudget(
    max_requests=4,
    max_bytes=256 * 1024 * 1024,
    timeout_seconds=300.0,
    min_request_interval_seconds=1.0,
)

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
#: A prior row with this source is what 'already held' means.
ACQUIRED_SOURCE = "govinfo"

#: The twelve ``BillFamilyTables`` row tuples, by the contract each fills.
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
) -> list[BillVersionCapture]:
    """One :class:`BillVersionCapture` per printing this run has anything to say about.

    A printing whose body is not fetched still gets a capture when the run is
    seeing it for the first time — the publisher's own facts about it (type,
    date, offered formats) are real and belong in ``bill_versions``, and the
    NULL body columns say what is missing.

    A printing that is **already published and not needed for a diff** is left
    out entirely rather than re-emitted with NULLs. Re-emitting it would
    overwrite a captured row's digest, byte size and URLs with nothing, which
    is the same defect the two writers of ``congress_bills`` had; leaving it
    out lets the merge keep the published row untouched.

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
    printings = [version for version in status.text_versions if version.type]
    codes = [version_slug(version.type) for version in printings]
    needed = _needed_printings(codes, held)

    for index, version in enumerate(printings):
        version_code = codes[index]
        if index not in needed and version_code in held:
            continue
        chosen = choose_format(version.formats, prefer=DEFAULT_FORMAT_PREFERENCE)
        package_id = version.package_id
        if package_id is None and chosen is not None:
            package_id = bill_package_id_from_url(status.identity, chosen.url)
        if package_id is None:
            package_id = bill_version_package_id(status.identity, version_code)

        body = None
        document = None
        cleanup = None
        source = "congress"
        if acquirer is not None and chosen is not None and budget[0] > 0:
            budget[0] -= 1
            try:
                package = acquirer.acquire(package_id)
            except Exception as error:  # noqa: BLE001 — one printing's refusal is not the bill's
                logger.warning("Bill family: {} {} body refused: {}", package_id, version_code, error)
            else:
                body = package.body_capture
                source = "govinfo"
                if package.format == "xml" and engine_available():
                    try:
                        document = parse_bill_tree(body.body, version=version_code)
                    except Exception as error:  # noqa: BLE001 — an unparsed printing is a NULL tree
                        logger.warning("Bill family: {} tree refused: {}", package_id, error)
                elif package.format == "pdf":
                    # Keyed on the rendition actually fetched, not on the link
                    # chosen: the acquirer picks from what the package MODS
                    # offers, and only the PDF branch of ``body_text`` yields
                    # the ``GpoCleanupRecord`` the cleanup_* columns describe.
                    try:
                        cleanup = body_text(package).record
                    except Exception as error:  # noqa: BLE001 — an unextracted PDF is a NULL cleanup
                        logger.warning("Bill family: {} PDF text refused: {}", package_id, error)

        captures.append(
            BillVersionCapture(
                version=version,
                version_code=version_code,
                source=source,
                package_id=package_id,
                chosen_format=chosen,
                body=body,
                document=document,
                cleanup=cleanup,
            )
        )
    return captures


@dataclass(frozen=True, slots=True)
class PriorIndex:
    """The two lookups that let a run skip work it has already published.

    Deliberately narrow: two columns of ``congress_bills`` and three of
    ``bill_versions``, read as tuples. The published tables run to hundreds of
    thousands of rows, and materialising them to decide what to skip would cost
    more than the skipping saves.
    """

    #: ``bill_id`` -> the publisher's ``update_date_including_text`` as published.
    bill_text_dates: Mapping[str, str | None]
    #: ``{bill_id}\x1f{version_code}\x1f{source}`` for every printing already published.
    printings: Collection[str]

    @property
    def is_cold(self) -> bool:
        return not self.bill_text_dates and not self.printings

    def held_codes(self, bill_id: str) -> set[str]:
        """The version codes already captured for one bill, from the acquiring source."""
        prefix = f"{bill_id}\x1f"
        return {
            key[len(prefix) :].rsplit("\x1f", 1)[0]
            for key in self.printings
            if key.startswith(prefix) and key.endswith(f"\x1f{ACQUIRED_SOURCE}")
        }


def _download_prior(output_dir: Path, download_prior: Callable[[str, Path], bool]) -> dict[str, Path | None]:
    """Fetch each prior table this run reads before merging, to the path the merge reuses in place."""
    paths: dict[str, Path | None] = {}
    for name in (*SNAPSHOT_TABLES, ARCHIVES_TABLE):
        path = prior_scratch_path(output_dir, name)
        paths[name] = path if (path.exists() or download_prior(f"{name}.parquet", path)) else None
    return paths


def _prior_index(paths: Mapping[str, Path | None]) -> PriorIndex:
    """Read the skip lookups out of the prior tables without materialising them."""
    import duckdb

    text_dates: dict[str, str | None] = {}
    bills_path = paths.get("congress_bills")
    if bills_path is not None and _has_columns(bills_path, ("bill_id", "update_date_including_text")):
        text_dates = dict(
            duckdb.sql(f"SELECT bill_id, update_date_including_text FROM read_parquet('{bills_path}')").fetchall()
        )

    printings: set[str] = set()
    versions_path = paths.get("bill_versions")
    if versions_path is not None and _has_columns(versions_path, ("bill_id", "version_code", "source")):
        printings = {
            f"{bill_id}\x1f{version_code}\x1f{source}"
            for bill_id, version_code, source in duckdb.sql(
                f"SELECT bill_id, version_code, source FROM read_parquet('{versions_path}')"
            ).fetchall()
        }

    logger.info(
        "Bill family: prior holds {:,} bills and {:,} printings",
        len(text_dates),
        len(printings),
    )
    return PriorIndex(bill_text_dates=text_dates, printings=printings)


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

    columns = ", ".join(ARCHIVE_COLUMNS)
    held: dict[tuple[str, str], BulkListingEntry] = {}
    for row in duckdb.sql(f"SELECT {columns} FROM read_parquet('{path}')").to_arrow_table().to_pylist():
        entry = _comparison_entry(row)
        if entry is not None and row["congress"] and row["bill_type"]:
            held[(str(row["congress"]), str(row["bill_type"]))] = entry
    logger.info("Bill family: {} folder listing entries retained from the last run", len(held))
    return held


def _acquire_archive(acquirer: BulkStatusSource, congress: int, bill_type: str, held: BulkListingEntry | None) -> Any:
    """One folder's archive, skipping the zip when the retained entry still describes it.

    A retained entry whose ``name`` or ``link`` no longer matches the folder's
    own zip is refused upstream on purpose — it describes a different file, so
    comparing its stamp would risk a false skip. That is a stale cache, not a
    reason to abandon the folder, so the zip is asked for the way a cold run
    asks for it rather than letting one bad row wedge the rollup until someone
    deletes the table.
    """
    if held is None:
        return acquirer.acquire(congress, bill_type)
    try:
        return acquirer.acquire(congress, bill_type, unchanged_since=held)
    except Exception as error:  # noqa: BLE001 — a stale retained entry is not a failed run
        logger.warning(
            "Bill family: {} {} retained listing entry refused ({}) — downloading the zip", congress, bill_type, error
        )
        return acquirer.acquire(congress, bill_type)


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
                error,
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


def build_bill_family(
    output_dir: Path,
    *,
    bulk_acquirer: BulkStatusSource | None = None,
    body_acquirer: PackageBodySource | None = None,
    max_version_fetches: int = MAX_VERSION_FETCHES,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> tuple[Path, ...]:
    """Build all fourteen bill-family outputs; returns one path per table."""
    congresses = congresses_from_env()
    bill_types = bill_types_from_env()
    logger.info("Bill family: Congresses {}, bill types {}", congresses, bill_types)

    bulk_acquirer = bulk_acquirer or BulkStatusAcquirer(budget=BULK_BUDGET)
    if body_acquirer is None:
        api_key = _resolve_api_key()
        if api_key:
            body_acquirer = GovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key)
        else:
            logger.warning(
                "Bill family: no api.data.gov key ({}) — publishing status-derived tables only",
                ", ".join(API_KEY_ENV_VARS),
            )

    # The model seams, wired only when a key is present.
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

    # 2. Acquire and build, one bill at a time, skipping the unchanged.
    remaining = [max_version_fetches]
    families: list[BillFamilyTables] = []
    refusals: Counter[str] = Counter()
    touched: set[str] = set()
    archive_rows: list[dict] = []
    bills = unchanged = skipped = archives_skipped = 0
    for congress in congresses:
        for bill_type in bill_types:
            acquisition = _acquire_archive(
                bulk_acquirer, congress, bill_type, held_archives.get((str(congress), bill_type))
            )
            retained = _retained_entry(bulk_acquirer, acquisition, congress, bill_type)
            if retained is not None:
                archive_rows.append(retained)
            if acquisition.skipped_unchanged:
                # The folder's own listing proves the zip has not moved since
                # the entry retained last run, so its bills cannot have
                # changed either: every one of them would have matched its
                # published `updateDateIncludingText` below and been skipped.
                # This buys that outcome for one small listing request instead
                # of up to 32 MB of zip.
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
            for member in archive.members:
                if member.status is None:
                    continue
                identifier = bill_key(member.status.identity)
                # The publisher's own "has anything about this bill, including
                # its text, changed" stamp. Equal means nothing to do: no
                # version requests, no model calls, and the published rows
                # stand. This is what makes a steady-state run cheap.
                published = index.bill_text_dates.get(identifier)
                if published is not None and published == member.status.update_date_including_text:
                    unchanged += 1
                    continue
                capture = BillFamilyCapture(
                    status=member.status,
                    versions=tuple(
                        _version_captures(
                            member.status,
                            body_acquirer,
                            remaining,
                            held=index.held_codes(identifier),
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
                families.append(tables)
                for refusal in tables.refusals:
                    refusals[refusal.table] += 1
                touched.add(identifier)
                bills += 1

    folded = BillFamilyTables.concat(families)
    logger.info(
        "Bill family: {:,} bills rebuilt, {:,} unchanged and skipped, {:,} printings fetched of {:,} allowed",
        bills,
        unchanged,
        max_version_fetches - remaining[0],
        max_version_fetches,
    )
    if remaining[0] == 0:
        logger.warning(
            "Bill family: the per-run printing cap was reached — the next run resumes on the "
            "printings this one did not reach, because already-published printings are skipped"
        )
    if archives_skipped:
        logger.info(
            "Bill family: {:,} of {:,} folder zips proved unchanged and were not downloaded",
            archives_skipped,
            len(congresses) * len(bill_types),
        )
    if skipped:
        logger.warning("Bill family: {:,} archive entries the reader refused", skipped)
    if refusals:
        logger.warning("Bill family: refusals by table — {}", dict(refusals))

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
    def publish(contract: str, rows: Any) -> Path:
        return merge_contract_table(
            output_dir,
            contract,
            rows,
            download_prior=download_prior,
            prior_present=(prior_paths.get(contract) is not None) if contract in SNAPSHOT_TABLES else None,
        )

    paths = [publish(contract, getattr(folded, attr)) for contract, attr in FAMILY_TABLES]
    paths.append(publish("public_activity_events", events))
    # The fourteenth output is this rollup's own processing state, not a
    # contract table, so it goes through the same merge helper one level down.
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
        )
    )
    return tuple(paths)


def _detected_at() -> str:
    """The run instant every event this run detects is stamped with."""
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
