"""Transform: build ``senate_expenditures`` — the Secretary of the Senate's ruled tables.

One table, its own rollup, because its acquisition pass is nothing like the
other PDF families'. ``build_print_citations`` reads a package body once with
table detection **off** at 9.8 ms a page; this reads *granule* bodies with
PyMuPDF's ``find_tables()`` **on**, which costs 26.5 to 84.6 ms a page
(spicy-docs ``docs/research/pdf-yield-mods-recheck-2026-09-20.md``), over
volumes of 1,259 to 3,018 pages published twice a year. Grouping the two would
put a per-page cap the citation families do not need onto families that are
measurably worse for having one.

**What the publisher actually publishes.** The Secretary of the Senate reports
semiannually under 2 U.S.C. 104a, each report covering October 1 – March 31 or
April 1 – September 30. GovInfo carries them as ``CDOC`` packages under the
``GPO-`` prefixed reprint id — ``GPO-CDOC-{congress}sdoc{number}``, never the
bare ``CDOC-119sdoc3`` — and each package publishes the report as several PDFs:
a Full Report and one file per Part.

**Enumeration, and its floor.** The ``published`` route scoped to ``CDOC``,
filtered by the report's own title and then by the package-id grammar. Measured
live 2026-09-20 over issue dates 2024-01-01..2026-09-20: **291 CDOC rows
walked, 5 matched** — ``GPO-CDOC-119sdoc3``, ``119sdoc5``, ``119sdoc6``,
``118sdoc11`` and ``118sdoc13`` — so the title rule is selective on real data
and the floor this repository has *measured* is the 118th Congress. The floor
is a scope and not a coverage claim: GovInfo's CDOC collection reaches 1817 and
nobody has walked an earlier window. ``SENATE_EXPENDITURES_SINCE`` moves it.

senate.gov's own index page (``legislative/common/generic/report_secsen.htm``)
links the same PDFs and is keyless, and is deliberately **not** the route
here: it states two fields — an href and its link text — where GovInfo states
the package identity, the granule list and a last-modified, which is what lets
a run skip a file it has already read.

**The files come from the granules route, never derived.** ``-1.pdf`` and
``-2.pdf`` look derivable from the package id and are not derived: the
publisher's own ``packages/{id}/granules`` route states which files exist, one
keyed request per package, and each is then fetched through
``acquire_granule`` so the granule summary proves the file belongs to the
package asked for. Measured 2026-09-20: ``GPO-CDOC-119sdoc3`` and
``GPO-CDOC-119sdoc6`` each state exactly two ``CONTENT`` granules, Part I and
Part II.

The **Full Report is skipped**, and skipping it is the point of reading
granules rather than the package body: ``GPO-CDOC-119sdoc3.pdf`` and
``GPO-CDOC-119sdoc3-1.pdf`` are different files with different digests whose
first 60 pages extract to byte-identical text. Reading both would publish the
same ruled rows twice under two ``file_name`` values, and ``file_name`` is in
the identity precisely because the publisher does this.

**Pages are capped here, unlike the citation families, and the cap is the
measurement's own.** spicy-docs read pages 1–80 of two volumes and found 139
tables and 673 ruled rows in them; the ``C-`` compensation and ``D-``
mail-allocation sections its contents page names sit past page 2,000 and are
**not** reached by any cap this rollup could afford. ``pages_capped`` is
``true`` on every row for that reason, and every count over this table is a
floor on the volume. Raising ``MAX_PAGES_PER_FILE`` is a wall-time decision,
not a correctness one.

**What this table is not.** No citation extractor runs over these volumes. The
MODS yield is zero at full page depth across all eight volumes spicy-docs
sampled — no public law, no U.S. Code section, no bill that survived
inspection — so extracting citations here would recreate what the index
already has, which is the owner's first rule. What only the print holds is the
ruled grid, and that is what is published.

**The row shape is spicy-docs'.** ``shape_senate_expenditure_rows`` returns one
row per ruled row of one table, including its header and total rows, and
``page_context`` reads the office, funding year, appropriation title and
printed page off the page text once per page. ``extraction_rule_version`` is
the shaper's own constant. Nothing about a column, an identity or a
classification is restated here.

**Corrections replace the file's result.** Successful reads are checkpointed
in this Parquet artifact's metadata, including reads yielding no tables. The
checkpoint covers the publisher's modification values, shaper and extractor
versions, and requested page limit. An older rule, changed source, or changed
limit triggers a fresh read; a complete read replaces all prior rows for that
file. A refusal or incomplete page stream preserves the previous rows and
checkpoint. This is completion of a bounded prefix, not of the entire report.

Needs an api.data.gov key: GovInfo's ``published``, granule-list, granule
summary and granule MODS routes are keyed. The bodies are not. A ``401``/``403``
aborts the run rather than being counted as a bad row.
"""

from __future__ import annotations

import os
import re
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any, Protocol

import httpx
from loguru import logger
from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonSourceError
from spicy_docs.schemas.senate_expenditure_tables import (
    SENATE_EXPENDITURE_RULE_VERSION,
    page_context,
    shape_senate_expenditure_rows,
)
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget
from spicy_docs.sources.govinfo.bodies import parse_package_id
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, package_granules_url, published_url
from spicy_docs.transport.credentials import CredentialRefusedError, scrub_credential

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.read_checkpoints import checkpoint_metadata, read_checkpoints
from spicy_regs.transforms.table_merge import merge_contract_table, published_table

NAME = "senate_expenditures"
#: Bump when this adapter changes how it reads or interprets a page stream.
READ_POLICY_VERSION = "001"

#: The collection walked, and the id prefix a row must carry to be one of
#: these reports. The two differ on purpose — the reprints live inside the
#: ``CDOC`` collection under a ``GPO-CDOC-`` id, and ``GPO-CDOC`` is its own
#: entry in spicy-docs' package-id grammar so that ``GPO-J6-REPORT``, which
#: states the same ``collectionCode``, is still refused by name.
COLLECTION = "CDOC"
PACKAGE_PREFIX = "GPO-CDOC-"

#: The report's own title, which every one of the five matched packages states
#: in full. Selective on real data: 291 CDOC rows walked, 5 matched
#: (2026-09-20). Pinned in ``tests/test_senate_expenditures.py``.
REPORT_TITLE = re.compile(r"(?i)\breport of the secretary of the senate\b")

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
    TypeError,
    KeyError,
    OSError,
)


class PackageDiscoverySource(Protocol):
    """What this transform needs of a GovInfo discovery reader: the two list routes."""

    def packages(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...

    def granules(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


class GranuleBodySource(Protocol):
    """What this transform needs of a GovInfo body acquirer: one granule's PDF."""

    def acquire_granule(self, package_id: str, granule_id: str, *, max_bytes: int | None = ...) -> Any: ...


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


def _prior_files(path: Path | None) -> set[tuple[str, str]]:
    """Old output identities are correction work even if discovery no longer reaches them."""
    if path is None:
        return set()
    import pyarrow.parquet as pq

    return {
        (row["package_id"], row["file_name"])
        for batch in pq.ParquetFile(path).iter_batches(columns=["package_id", "file_name"])
        for row in batch.to_pylist()
        if isinstance(row["package_id"], str) and isinstance(row["file_name"], str)
    }


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
            if not package_id.startswith(PACKAGE_PREFIX) or REPORT_TITLE.search(str(record.get("title") or "")) is None:
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
    """The report's own files, as the publisher states them — the Full Report excluded.

    The Full Report duplicates Part I's pages under a second ``file_name``
    (measured byte-identical over the first 60 pages), and ``file_name`` is in
    this table's identity, so reading both would publish every ruled row twice.
    A granule whose id is the bare package id is that duplicate.
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
) -> tuple[list[dict], int]:
    """Every ruled row of the pages read, and how many tables they came from.

    One ``page_context`` per page, not per table: the page text is scanned and
    hashed once and handed to each table on it, which is what the shaper's
    ``context`` argument exists for.
    """
    rows: list[dict] = []
    tables = 0
    for result in pages:
        if not result.tables:
            continue
        context = page_context(result.text)
        for ordinal, table in enumerate(result.tables):
            tables += 1
            rows.extend(
                shape_senate_expenditure_rows(
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
            )
    return rows, tables


def build_senate_expenditures(
    output_dir: Path,
    *,
    reader: PackageDiscoverySource | None = None,
    acquirer: GranuleBodySource | None = None,
    extractor: PageSource | None = None,
    max_packages: int = MAX_PACKAGES_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> Path:
    """Build ``senate_expenditures.parquet`` from the report's ruled tables."""
    if reader is None or acquirer is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(
                f"Senate expenditures need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})"
            )
        reader = reader or GovInfoDiscoveryReader(budget=DISCOVERY_BUDGET, api_key=api_key)
        acquirer = acquirer or GovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key)
    extractor = extractor or _extractor()

    prior = published_table(output_dir, NAME, download_prior)
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
    read_files = tables = pages = skipped = 0
    packages = held_packages = 0

    listed = _listed(reader, since)
    discovered = {package_id for package_id, _ in listed}
    stale_packages = sorted(
        {
            package_id
            for package_id, file_name in _prior_files(prior) | checkpoints.keys()
            if package_id not in discovered
            and not _current_processing(checkpoints.get((package_id, file_name), {}), processing_version)
        }
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
        unheld: list[tuple[str, str | None]] = []
        for gid, granule_modified in granule_ids:
            checkpoint = checkpoints.get((package_id, f"{gid}.pdf"), {})
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
            if not current:
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
        for granule_id, granule_modified in unheld:
            file_name = f"{granule_id}.pdf"
            try:
                body = acquirer.acquire_granule(package_id, granule_id)
                read_pages, page_count = _read_pages(body, extractor)
                file_rows, found = _file_rows(
                    read_pages,
                    package_id=package_id,
                    file_name=file_name,
                    page_count=page_count,
                    body_rendition=body.format,
                )
                read = len(read_pages)
            except CredentialRefusedError:
                raise
            except _REFUSALS as error:
                reason = type(error).__name__
                refusals[reason] += 1
                logger.warning(
                    "{}: {} refused ({}): {}", package_id, granule_id, reason, scrub_credential(str(error), "")
                )
                continue
            # A successful evaluation replaces this file's entire prior
            # result, even when a corrected rule now yields zero rows. A
            # refused or truncated read reaches neither replacement nor the
            # checkpoint, preserving the prior successful result for retry.
            completed_files.add((package_id, file_name))
            rows.extend(file_rows)
            checkpoints[(package_id, file_name)] = {
                "package_id": package_id,
                "file_name": file_name,
                "last_modified": modified,
                "granule_last_modified": granule_modified,
                "processing_version": processing_version,
                "page_limit": MAX_PAGES_PER_FILE,
                "pages_read": read,
                "page_count": page_count,
            }
            read_files += 1
            tables += found
            pages += read
            grids.update(row["grid_kind"] or "unrecognised" for row in file_rows)
            logger.info(
                "{}: {} — {:,} pages of {} read, {:,} tables, {:,} ruled rows",
                package_id,
                file_name,
                read,
                page_count or "an unstated extent",
                found,
                len(file_rows),
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
        " {:,} files already held in {:,} fully-held packages, {:,} refused",
        len(rows),
        read_files,
        packages,
        pages,
        tables,
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
