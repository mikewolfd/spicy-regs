"""Transform: build ``senate_expenditures.parquet`` — the Secretary of the Senate's ruled tables, its own rollup.

Its acquisition is unlike the other PDF families': *granule* bodies read with
PyMuPDF table detection **on** (26.5–84.6 ms a page) over volumes of 1,259 to
3,018 pages published twice a year, so reads are capped at
:data:`MAX_PAGES_PER_FILE` and ``pages_capped`` is true on every row — every
count over this table is a floor. The semiannual reports (2 U.S.C. 104a) are
carried as ``GPO-CDOC-{congress}sdoc{number}`` ``CDOC`` packages; the
``published`` window is filtered by the report's own title and then by the
package-id grammar, and the issue-date floor (``SENATE_EXPENDITURES_SINCE``,
default :data:`DEFAULT_ISSUE_FLOOR`) is a scope, not a coverage claim.

The publisher's own ``packages/{id}/granules`` route states which files exist
and each is fetched through ``acquire_granule`` so the granule summary proves
it belongs to the package asked for; the **Full Report is skipped** because it
duplicates Part I's pages byte-identically under a second ``file_name``, and
``file_name`` is in the row identity. Part granules are processed in
deterministic part order (``-1``, ``-2``, ...) per package so that opening
``appropriation_summary`` pages repeated verbatim in later parts (same
``package_id`` and page ``text_sha256``) are skipped rather than double-counted,
while non-identical summary pages and all B-pages (``organization_detail`` /
``payee_detail``) are preserved. senate.gov's own index page is keyless
and links the same PDFs, but states no package identity, granule list or
last-modified, so it is deliberately not the route here. No citation extractor
runs over these volumes (the MODS yield is zero at full depth) — only the ruled
grid is published, one row per ruled row (header and total rows included) in
spicy-docs' own row shape. Successful reads, including reads yielding no
tables, are checkpointed in the Parquet metadata along with the publisher's
modification values, rule/extractor versions and page limit: a changed rule,
source or limit triggers a fresh read that replaces all prior rows for that
file, while a refusal or incomplete page stream preserves the prior rows and
checkpoint. This is completion of a bounded prefix, never of the whole report.

Needs an api.data.gov key for the ``published``, granule-list, granule-summary
and granule-MODS routes (bodies are keyless); a ``401``/``403`` aborts the run.
"""

from __future__ import annotations

import hashlib
import os
import re
from collections import Counter, defaultdict
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import httpx
from loguru import logger
from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonSourceError
from spicy_docs.schemas.senate_expenditure_tables import (
    SENATE_EXPENDITURE_RULE_VERSION,
    grid_kind,
    page_context,
    shape_senate_expenditure_rows,
)
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget
from spicy_docs.sources.govinfo.bodies import parse_package_id
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, package_granules_url, published_url
from spicy_docs.sources.govinfo.senate_expenditure_reports import is_senate_expenditure_report
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints
from spicy_regs.transforms.table_merge import merge_contract_table, published_table

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

NAME = "senate_expenditures"
#: Bump when this adapter changes how it reads or interprets a page stream.
READ_POLICY_VERSION = "002"

#: Grid kind deduplicated across parts of the same package when a later part
#: repeats an earlier part's opening summary page verbatim.
APPROPRIATION_SUMMARY_GRID = "appropriation_summary"

_TRAILING_PART = re.compile(r".*-(\d+)\Z")

#: The collection walked. Which of its rows are the report is spicy-docs' rule (``is_senate_expenditure_report``,
#: round 5 X5): a ``GPO-CDOC-`` id, the collection and prefix differing on purpose (the reprints live inside ``CDOC``
#: under a ``GPO-CDOC-`` id, and ``GPO-CDOC`` is its own entry in the package-id grammar so that ``GPO-J6-REPORT``,
#: which states the same ``collectionCode``, is still refused by name), and the report's own title, which every one
#: of the five matched packages states in full. Selective on real data: 291 CDOC rows walked, 5 matched (2026-09-20).
COLLECTION = "CDOC"

#: Granules that are a file of the report rather than metadata.
CONTENT_GRANULE = "CONTENT"

#: The issue-date floor a cold run walks from — a scope, not a coverage claim.
#: 2024-01-01 reaches the 118th Congress's two reports and all three of the
#: 119th's. ``SENATE_EXPENDITURES_SINCE`` moves it.
DEFAULT_ISSUE_FLOOR = "2024-01-01"

#: Pages read per granule file, with table detection on. spicy-docs' own
#: measurement read 1–80 of two volumes; at 26.5–84.6 ms a page that is two to
#: seven seconds a file. Every published count is a floor on the volume and
#: ``pages_capped`` says so on every row.
MAX_PAGES_PER_FILE = 80

#: Packages fetched per run. Each costs one keyed granule-list request plus two
#: keyed requests per granule file (granule summary and granule MODS; the body
#: is keyless), so four packages is at most 28 keyed — a small fraction of the
#: headroom left under the shared 4,000-per-hour GovInfo ceiling by the 3,680
#: worst hour D1 measured, and this rollup's cron shares an hour with nothing.
#: The publisher adds two packages a year, so this is a catch-up bound.
MAX_PACKAGES_PER_RUN = 4

MAX_PAGES = 20

DISCOVERY_BUDGET = PagedJsonBudget(
    max_requests=500,
    max_page_bytes=8 * 1024 * 1024,
    timeout_seconds=60.0,
    min_request_interval_seconds=0.2,
)

#: Three requests per granule (summary, MODS, body), paced. ``max_body_bytes``
#: is 16 MiB against measured bodies of 4.3 to 11.7 MB, and the long timeout is
#: for the body: these are five-to-twelve-megabyte PDFs.
BODY_BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=16 * 1024 * 1024,
    max_metadata_bytes=8 * 1024 * 1024,
    timeout_seconds=300.0,
    min_request_interval_seconds=0.34,
)

_REFUSALS = (
    PagedJsonSourceError,
    httpx.HTTPError,
    ConnectionError,
    ValueError,
    OSError,
)


class PackageDiscoverySource(Protocol):
    """What this transform needs of a GovInfo discovery reader: the two list routes."""

    def packages(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...

    def granules(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


class GranuleBodySource(Protocol):
    """What this transform needs of a GovInfo body acquirer: one granule's PDF."""

    def acquire_granule(
        self, package_id: str, granule_id: str, *, prefer: Sequence[str] = ..., max_bytes: int | None = ...
    ) -> Any: ...


class PageSource(Protocol):
    """What this transform needs of a PDF extractor: a stream of pages with their tables.

    The parameter is named ``source`` because ``DocumentExtractor.extract``
    names it that and it is positional-or-keyword there, so a Protocol that
    renamed it would not describe the object actually passed.
    """

    def extract(self, source: bytes, *, media_type: str) -> Iterator[Any]: ...


def _issue_floor() -> str:
    raw = os.environ.get("SENATE_EXPENDITURES_SINCE", "").strip()
    return raw or DEFAULT_ISSUE_FLOOR


def _extractor() -> PageSource:
    """PyMuPDF with table detection on — the pipeline the contract was measured against.

    Imported here rather than at module scope so a base install without the
    ``pdf`` extra can still import this module (the rollup registry does).
    """
    from spicy_docs.extraction.api import DocumentExtractor, NativeText

    return DocumentExtractor(NativeText(), tables=True)


def _processing_version() -> dict[str, str | None]:
    """Changes to shaping, extraction, or this adapter invalidate successful reads."""
    dependencies: dict[str, str | None] = {}
    for package in ("spicy-docs", "PyMuPDF"):
        try:
            dependencies[package] = version(package)
        except PackageNotFoundError:
            # An injected extractor may run without the optional PDF extra.
            dependencies[package] = None
    return {"reader": READ_POLICY_VERSION, "shaper": SENATE_EXPENDITURE_RULE_VERSION, **dependencies}


def _complete(
    checkpoint: dict,
    *,
    modified: str | None,
    granule_modified: str | None,
    processing_version: dict[str, str | None],
) -> bool:
    """A complete read of the requested prefix, never a claim to the whole volume."""
    return (
        (modified is not None or granule_modified is not None)
        and checkpoint.get("last_modified") == modified
        and checkpoint.get("granule_last_modified") == granule_modified
        and _current_processing(checkpoint, processing_version)
    )


def _current_processing(checkpoint: dict, processing_version: dict[str, str | None]) -> bool:
    """Whether the retained prefix used this rule and limit, independent of source freshness."""
    pages_read = checkpoint.get("pages_read")
    page_count = checkpoint.get("page_count")
    return (
        checkpoint.get("processing_version") == processing_version
        and checkpoint.get("page_limit") == MAX_PAGES_PER_FILE
        and type(pages_read) is int
        and type(page_count) is int
        and page_count > 0
        and pages_read == min(page_count, MAX_PAGES_PER_FILE)
    )


def _granule_sort_key(granule_id: str, package_id: str) -> tuple[int, int, str]:
    """Deterministic part order per package: numeric parts ``-1``, ``-2``, ..., ``-10``, then lexical."""
    stem = granule_id.removesuffix(".pdf")
    prefix = f"{package_id}-"
    if stem.startswith(prefix):
        suffix = stem[len(prefix) :]
        if suffix.isascii() and suffix.isdigit():
            return (0, int(suffix), granule_id)
    match = _TRAILING_PART.fullmatch(stem)
    if match is not None:
        return (1, int(match.group(1)), granule_id)
    return (2, 0, granule_id)


def _page_text_sha256(page_text: str) -> str:
    """Hex SHA-256 of extracted page text, used to detect verbatim Part I/II summary repeats."""
    return hashlib.sha256(page_text.encode("utf-8")).hexdigest()


def _read_prior_index(
    path: Path | None,
) -> tuple[set[tuple[str, str]], dict[tuple[str, str], set[str]]]:
    """Held ``(package_id, file_name)`` pairs and their emitted ``appropriation_summary`` text digests."""
    if path is None:
        return set(), {}
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(path)
    available = set(parquet.schema_arrow.names)
    if "package_id" not in available or "file_name" not in available:
        return set(), {}
    columns = [col for col in ("package_id", "file_name", "grid_kind", "text_sha256") if col in available]
    files: set[tuple[str, str]] = set()
    summary_hashes: dict[tuple[str, str], set[str]] = defaultdict(set)
    for batch in parquet.iter_batches(columns=columns):
        for row in batch.to_pylist():
            package_id = row.get("package_id")
            file_name = row.get("file_name")
            if not isinstance(package_id, str) or not isinstance(file_name, str):
                continue
            key = (package_id, file_name)
            files.add(key)
            if row.get("grid_kind") == APPROPRIATION_SUMMARY_GRID:
                digest = row.get("text_sha256")
                if isinstance(digest, str) and digest:
                    summary_hashes[key].add(digest.removeprefix("sha256:"))
    return files, dict(summary_hashes)


def _packages_with_duplicate_summary_hashes(
    prior_summary_hashes: Mapping[tuple[str, str], set[str]],
) -> set[str]:
    """Packages whose held files in ``prior`` still share an ``appropriation_summary`` page digest."""
    by_package: dict[str, list[set[str]]] = defaultdict(list)
    for (package_id, _file_name), hashes in prior_summary_hashes.items():
        if hashes:
            by_package[package_id].append(hashes)
    duplicated: set[str] = set()
    for package_id, hash_sets in by_package.items():
        seen: set[str] = set()
        for hashes in hash_sets:
            if hashes & seen:
                duplicated.add(package_id)
                break
            seen.update(hashes)
    return duplicated


def _listed(reader: PackageDiscoverySource, since: str) -> list[tuple[str, str | None]]:
    """Every Secretary-of-the-Senate package id the CDOC window states, newest issue first.

    The whole window every run, for the reason ``build_print_citations``
    states: a package refused this run must be enumerated again next run, and
    a last-modified watermark would advance past it.
    """
    accepted: list[tuple[str, str | None]] = []
    walked = 0
    url = published_url(since, datetime.now(UTC).strftime("%Y-%m-%d"), collections=[COLLECTION], page_size=1000)
    for page in reader.packages(url, max_pages=MAX_PAGES):
        for record in page.records:
            walked += 1
            package_id = str(record.get("packageId") or "")
            if not is_senate_expenditure_report(package_id, str(record.get("title") or "")):
                continue
            try:
                parse_package_id(package_id)
            except ValueError as error:
                # A ``GPO-`` id the reprint grammar does not accept, e.g. a
                # ``GPO-CDOC-...hdoc...``. Refused by name, never guessed at.
                logger.warning("{}: {} is not a GPO-CDOC reprint id: {}", COLLECTION, package_id, error)
                continue
            accepted.append((package_id, str(record.get("lastModified") or "") or None))
    logger.info("{}: {:,} rows walked since {}, {:,} Secretary reports", COLLECTION, walked, since, len(accepted))
    return accepted


def _granule_ids(reader: PackageDiscoverySource, package_id: str) -> list[tuple[str, str | None]]:
    """The report's own files in deterministic part order — the Full Report excluded.

    The Full Report duplicates Part I's pages under a second ``file_name``
    (measured byte-identical over the first 60 pages), and ``file_name`` is in
    this table's identity, so reading both would publish every ruled row twice.
    A granule whose id is the bare package id is that duplicate. Returning part
    granules in deterministic ``-1``, ``-2``, ... order ensures Part I owns
    shared opening ``appropriation_summary`` pages before later parts are
    deduplicated against them.
    """
    ids: list[tuple[str, str | None]] = []
    for page in reader.granules(package_granules_url(package_id, page_size=100), max_pages=MAX_PAGES):
        for record in page.records:
            granule_id = str(record.get("granuleId") or "")
            if not granule_id or granule_id == package_id:
                continue
            if str(record.get("granuleClass") or CONTENT_GRANULE) != CONTENT_GRANULE:
                continue
            ids.append((granule_id, str(record.get("lastModified") or "") or None))
    ids.sort(key=lambda item: _granule_sort_key(item[0], package_id))
    return ids


def _read_pages(body: Any, extractor: PageSource) -> tuple[list[Any], int]:
    """The first ``MAX_PAGES_PER_FILE`` pages of one granule PDF, and the file's own page count.

    The extractor yields lazily and the generator is closed the moment the cap
    is reached, so a 1,335-page volume costs eighty pages of table detection
    and not 1,335. ``page_count`` is the PDF's own — the granule summary states
    no extent, unlike a package summary — and it is read off the first page's
    metadata rather than re-derived.
    """
    pages: list[Any] = []
    page_count = 0
    stream = extractor.extract(body.body_capture.body, media_type=body.body.media_type)
    try:
        for result in stream:
            current_count = int(result.metadata["page_count"])
            if (
                current_count <= 0
                or (page_count and current_count != page_count)
                or int(result.metadata["page"]) != len(pages) + 1
                or len(pages) >= current_count
            ):
                raise ValueError("PDF extraction did not return consecutive pages with a consistent extent")
            page_count = current_count
            pages.append(result)
            if len(pages) >= MAX_PAGES_PER_FILE:
                break
    finally:
        # Closing is what releases the document, and the cap means this always
        # returns early. ``Iterator`` states no ``close``, so it is asked for
        # rather than assumed -- a caller may hand over a plain iterator.
        close = getattr(stream, "close", None)
        if close is not None:
            close()
    if not page_count or len(pages) != min(page_count, MAX_PAGES_PER_FILE):
        raise ValueError("PDF extraction ended before the requested page range was complete")
    return pages, page_count


def _file_rows(
    pages: Sequence[Any],
    *,
    package_id: str,
    file_name: str,
    page_count: int,
    body_rendition: str,
    seen_summary_page_hashes: set[str] | None = None,
) -> tuple[list[dict], int, set[str], int]:
    """Every ruled row of the pages read, table count, emitted summary page hashes, and skipped duplicate summary pages.

    One ``page_context`` per page, not per table: the page text is scanned and
    hashed once and handed to each table on it, which is what the shaper's
    ``context`` argument exists for. When ``seen_summary_page_hashes`` contains
    the page's SHA-256 text digest from an earlier part of the same package,
    tables whose ``grid_kind`` is ``appropriation_summary`` are skipped while
    non-identical summary pages and all B-pages (``organization_detail``,
    ``payee_detail``) are always emitted.
    """
    rows: list[dict] = []
    tables = 0
    emitted_summary_hashes: set[str] = set()
    duplicate_summary_pages = 0
    for result in pages:
        if not result.tables:
            continue
        context = page_context(result.text)
        page_hash = _page_text_sha256(result.text)
        page_emitted_summary = False
        page_skipped_duplicate = False
        for ordinal, table in enumerate(result.tables):
            table_cells = getattr(table, "cells", None)
            kind = grid_kind(table) if table_cells else None
            if (
                kind == APPROPRIATION_SUMMARY_GRID
                and seen_summary_page_hashes is not None
                and page_hash in seen_summary_page_hashes
            ):
                page_skipped_duplicate = True
                continue
            tables += 1
            shaped = shape_senate_expenditure_rows(
                table,
                result.text,
                package_id=package_id,
                file_name=file_name,
                table_ordinal=ordinal,
                page_count=page_count,
                pages_read=len(pages),
                body_rendition=body_rendition,
                body_derivation="pdf-extraction-lines",
                context=context,
            )
            if kind == APPROPRIATION_SUMMARY_GRID and shaped:
                page_emitted_summary = True
            rows.extend(shaped)
        if page_skipped_duplicate:
            duplicate_summary_pages += 1
        if page_emitted_summary:
            emitted_summary_hashes.add(page_hash)
    return rows, tables, emitted_summary_hashes, duplicate_summary_pages


def build_senate_expenditures(
    output_dir: Path,
    *,
    reader: PackageDiscoverySource | None = None,
    acquirer: GranuleBodySource | None = None,
    extractor: PageSource | None = None,
    max_packages: int = MAX_PACKAGES_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
    evidence: CaptureEvidence | None = None,
    coverage: dict[str, Any] | None = None,
) -> Path:
    """Build ``senate_expenditures.parquet`` from the report's ruled tables."""
    if reader is None or acquirer is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(
                f"Senate expenditures need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})"
            )
        if evidence is not None:
            evidence.credential = api_key
        reader = reader or GovInfoDiscoveryReader(
            budget=DISCOVERY_BUDGET,
            api_key=api_key,
            transport=None
            if evidence is None
            else evidence.transport(stage="senate-expenditures-discovery", max_bytes=DISCOVERY_BUDGET.max_page_bytes),
        )
        acquirer = acquirer or GovInfoBodyAcquirer(
            budget=BODY_BUDGET,
            api_key=api_key,
            transport=None
            if evidence is None
            else evidence.transport(
                stage="senate-expenditures-granule",
                max_bytes=max(BODY_BUDGET.max_body_bytes, BODY_BUDGET.max_metadata_bytes),
            ),
        )
    extractor = extractor or _extractor()

    prior = published_table(output_dir, NAME, download_prior)
    prior_files, prior_summary_hashes = _read_prior_index(prior)
    checkpoints = {
        (row["package_id"], row["file_name"]): row
        for row in read_checkpoints(prior, NAME)
        if isinstance(row.get("package_id"), str) and isinstance(row.get("file_name"), str)
    }
    processing_version = _processing_version()
    since = _issue_floor()

    rows: list[dict] = []
    completed_files: set[tuple[str, str]] = set()
    refusals: Counter[str] = Counter()
    grids: Counter[str] = Counter()
    duplicate_summary_pages_by_file: Counter[str] = Counter()
    read_files = tables = pages = skipped = duplicate_summary_pages = 0
    packages = held_packages = 0

    listed = _listed(reader, since)
    discovered = {package_id for package_id, _ in listed}
    stale_packages = sorted(
        {
            package_id
            for package_id, file_name in prior_files | checkpoints.keys()
            if package_id not in discovered
            and not _current_processing(checkpoints.get((package_id, file_name), {}), processing_version)
        }
        | (_packages_with_duplicate_summary_hashes(prior_summary_hashes) - discovered)
    )
    if stale_packages:
        logger.info(
            "Senate expenditures: {:,} retained packages need correction outside discovery", len(stale_packages)
        )
    # These packages have no freshly observed package modification date. Ask
    # their granule list again; never substitute an old date as current proof.
    for package_id, modified in [*listed, *((package_id, None) for package_id in stale_packages)]:
        if packages >= max_packages:
            logger.warning(
                "Senate expenditures: per-run cap of {:,} packages reached — the rest of the window is"
                " retried next run",
                max_packages,
            )
            break
        try:
            granule_ids = _granule_ids(reader, package_id)
        except CredentialRefusedError:
            raise
        except _REFUSALS as error:
            if evidence is not None:
                evidence.refusal(error, stage="senate-expenditures-discovery")
            reason = type(error).__name__
            refusals[reason] += 1
            logger.warning("{}: granule list refused ({}): {}", package_id, reason, scrub_credential(str(error), ""))
            continue
        if not granule_ids:
            # A package stating no content granule is not an error and not a
            # zero: it is one package that published nothing this route can
            # address, and it is enumerated again next run.
            logger.warning("{}: states no content granule", package_id)
            continue
        # Existing rows alone prove neither a complete prefix nor that they
        # used the current rule. The checkpoint also represents empty success.
        prior_seen: set[str] = set()
        unheld: list[tuple[str, str | None]] = []
        for gid, granule_modified in granule_ids:
            file_name = f"{gid}.pdf"
            checkpoint = checkpoints.get((package_id, file_name), {})
            # Outside discovery this queue repairs processing only. A missing
            # sibling may keep the package pending, but must not cause an
            # already-corrected file to consume a read and a cap slot again.
            current = (
                _complete(
                    checkpoint,
                    modified=modified,
                    granule_modified=granule_modified,
                    processing_version=processing_version,
                )
                if package_id in discovered
                else _current_processing(checkpoint, processing_version)
            )
            file_prior_hashes = prior_summary_hashes.get((package_id, file_name), set())
            if current and not (file_prior_hashes & prior_seen):
                prior_seen.update(file_prior_hashes)
            else:
                unheld.append((gid, granule_modified))
        skipped += len(granule_ids) - len(unheld)
        if not unheld:
            # **A package with nothing left to read costs its granule list and
            # not a cap slot.** Counting it against the cap is what the first
            # version did, and with more matched packages than the cap it
            # stalls permanently: every run re-lists the same held packages,
            # reaches the cap on them and breaks before the one package it has
            # never read, which is therefore never read. Measured on the
            # 2026-09-20 receipt, where five packages matched, the cap was
            # four, and `GPO-CDOC-118sdoc11` could not be reached by any number
            # of runs.
            held_packages += 1
            continue
        read_before = read_files
        unheld_ids = {gid for gid, _ in unheld}
        seen_summary_page_hashes: set[str] = set()
        for granule_id, granule_modified in granule_ids:
            file_name = f"{granule_id}.pdf"
            file_prior_hashes = prior_summary_hashes.get((package_id, file_name), set())
            if granule_id not in unheld_ids:
                if not (file_prior_hashes & seen_summary_page_hashes):
                    seen_summary_page_hashes.update(file_prior_hashes)
                    continue
                # An earlier part read in this run emitted summary pages that this
                # held later part also holds in prior; re-read it to drop duplicates.
                skipped -= 1
            try:
                body = acquirer.acquire_granule(package_id, granule_id, prefer=("pdf",))
                read_pages, page_count = _read_pages(body, extractor)
                file_rows, found, emitted_summary_hashes, file_dup_summary_pages = _file_rows(
                    read_pages,
                    package_id=package_id,
                    file_name=file_name,
                    page_count=page_count,
                    body_rendition=body.format,
                    seen_summary_page_hashes=seen_summary_page_hashes,
                )
                read = len(read_pages)
            except CredentialRefusedError:
                raise
            except _REFUSALS as error:
                if evidence is not None:
                    evidence.refusal(error, stage="senate-expenditures-granule")
                reason = type(error).__name__
                refusals[reason] += 1
                logger.warning(
                    "{}: {} refused ({}): {}", package_id, granule_id, reason, scrub_credential(str(error), "")
                )
                # Prior rows for a refused file remain in the merged output, so
                # retain any summary hashes they already hold for later parts.
                seen_summary_page_hashes.update(file_prior_hashes)
                continue
            # A successful evaluation replaces this file's entire prior
            # result, even when a corrected rule now yields zero rows. A
            # refused or truncated read reaches neither replacement nor the
            # checkpoint, preserving the prior successful result for retry.
            completed_files.add((package_id, file_name))
            rows.extend(file_rows)
            seen_summary_page_hashes.update(emitted_summary_hashes)
            checkpoints[(package_id, file_name)] = {
                "package_id": package_id,
                "file_name": file_name,
                "last_modified": modified,
                "granule_last_modified": granule_modified,
                "processing_version": processing_version,
                "page_limit": MAX_PAGES_PER_FILE,
                "pages_read": read,
                "page_count": page_count,
                "duplicate_summary_pages_skipped": file_dup_summary_pages,
            }
            read_files += 1
            tables += found
            pages += read
            duplicate_summary_pages += file_dup_summary_pages
            if file_dup_summary_pages:
                duplicate_summary_pages_by_file[file_name] += file_dup_summary_pages
            grids.update(row["grid_kind"] or "unrecognised" for row in file_rows)
            logger.info(
                "{}: {} — {:,} pages of {} read, {:,} tables, {:,} ruled rows{}",
                package_id,
                file_name,
                read,
                page_count or "an unstated extent",
                found,
                len(file_rows),
                f", {file_dup_summary_pages:,} duplicate summary pages skipped" if file_dup_summary_pages else "",
            )
        if read_files > read_before:
            # A package whose every file refused spends its requests without a
            # cap slot, the same way `print-citations` does not charge a
            # refused package: a permanent refusal must not lock the rest of
            # the window out. The window itself is the bound — five packages
            # over three Congresses — not this counter.
            packages += 1

    logger.info(
        "Senate expenditures: {:,} ruled rows from {:,} files over {:,} packages, {:,} pages, {:,} tables;"
        " {:,} duplicate summary pages skipped, {:,} files already held in {:,} fully-held packages, {:,} refused",
        len(rows),
        read_files,
        packages,
        pages,
        tables,
        duplicate_summary_pages,
        skipped,
        held_packages,
        sum(refusals.values()),
    )
    if refusals:
        logger.info("Senate expenditures: refusals by reason — {}", dict(refusals))
    if grids:
        # Which grid each row came from. The contract has a column for it per
        # row but none for the run's distribution, and an "unrecognised" count
        # above zero is the signal that the print changed shape.
        logger.info("Senate expenditures: rows by grid — {}", dict(grids))

    status: dict[str, Any] = {
        "refused": sum(refusals.values()),
        "refusals": dict(refusals),
        "read_files": read_files,
        "skipped": skipped,
        "packages": packages,
        "duplicate_summary_pages_skipped": duplicate_summary_pages,
    }
    if duplicate_summary_pages_by_file:
        status["duplicate_summary_pages_by_file"] = dict(duplicate_summary_pages_by_file)
    if coverage is not None:
        coverage.update(status)
    if evidence is not None:
        evidence.event("acquisition-coverage", **status)

    metadata = checkpoint_metadata(prior, NAME, (checkpoints[key] for key in sorted(checkpoints)))
    return merge_contract_table(
        output_dir,
        NAME,
        rows,
        download_prior=download_prior,
        prior_present=prior is not None,
        replace_parents=(("package_id", "file_name"), completed_files),
        parquet_metadata=metadata,
    )
