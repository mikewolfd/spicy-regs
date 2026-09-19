"""Transform: build ``committee_reports``, ``report_sections``, ``hearing_transcripts``.

Three tables from two GovInfo collections, in one pass because they share an
acquirer and a pacing budget:

* **CRPT** (committee reports) fills ``committee_reports``, one row per
  captured package, and ``report_sections`` — the per-agency blocks
  ``spicy_docs.sources.agency_reports.report_blocks`` parses out of the report
  text, each carrying the header pattern that fired as its provenance.
* **CHRG** (hearing transcripts) fills ``hearing_transcripts``.

Report text comes from ``extraction.body_text``, which owns one derivation per
rendition: the markup reader for ``htm`` and ``xml``, the shared cleanup for
``txt``, and extraction plus ``gpo_normalize`` for ``pdf``. Measured
2026-09-19 (spicy-docs ``docs/sources/govinfo-bodies.md``), CRPT and CHRG offer
only ``htm`` and ``pdf``, the ``htm`` *is* GPO's text inside a ``<pre>``
wrapper, and no GovInfo body of any rendition carries a ``[[Page N]]`` marker
or a form feed. So a committee report is read as HTML and states no page
boundaries at all — ``page_count`` is NULL for it, and only the PDF branch,
which an extractor paginates, fills one.

The derivation name ``body_text`` returns earns no column: it is a pure
function of the rendition through ``extraction.body_text.RENDITION_DERIVATIONS``,
a static four-entry table, so a column for it would restate ``format`` in a
second vocabulary and could only ever disagree with it by being stale. The
renditions actually read are logged instead. ``format`` is the column that says
which rendition was read, and the shape function fills it from the fetched body
itself. The PDF cleanup record is processing provenance and is not
published here — ``bill_versions`` is the table that carries it, for the
printings where it changes the text people read.

**``bill_id`` is read from the package's own MODS**, which the acquirer already
fetches to prove the package's identity, so the linkage costs no request. A
GovInfo MODS names the bills a package relates to as
``<extension><bill congress type number context/>``, and ``context`` is the
publisher's own statement of how: ``PRIMARY`` is the measure a report
accompanies, ``OTHER``, ``COVER`` and ``BODY`` are measures it mentions.
Only ``PRIMARY`` fills the column. Measured live 2026-09-19 on the first
twelve reports the Congress.gov ``committee-report/119`` route lists, the
MODS ``PRIMARY`` bill agreed with the route's own ``associatedBill[0]`` on 12
of 12 — the same edge the legislative data map resolved 17 of 17 — at one
keyed detail request per report that this design does not make. Over 52
hearings from the ``hearing/119`` route, no CHRG MODS carried a ``PRIMARY``
bill (18 carried ``BODY`` or ``COVER`` mentions, up to 25 on one hearing), and
of the 6 whose detail named a committee meeting none of those meetings'
``relatedItems.bills`` named a bill, so ``hearing_transcripts.bill_id`` is NULL
in practice and the ``hearing -> meeting -> bill`` chain, at two keyed
requests per hearing, is not walked. A mention is never promoted to a
linkage: publishing H.R. 1 as the bill of a hearing that cites it would be a
guess dressed as a fact. The mentions are counted in the run log; the column
that would carry them, ``associated_bills_json`` with each bill's context, is
not in the contract and is stated as a request rather than added here.

**Incremental.** The window starts at the prior published table's max
``last_modified`` minus a short overlap, so a steady-state run asks GovInfo for
the packages changed since the last run rather than a fixed thirty days of
them. The thirty-day default is the cold-start window only — with no prior
table there is no watermark to start from. ``COMMITTEE_REPORTS_SINCE`` overrides
both. Enumerating by last-modified rather than by issue date is deliberate: a
revision of an older report is a change this table should pick up.

A package already published with the same ``last_modified`` is skipped without
being re-fetched, which is where the three-requests-per-package cost goes.

Needs an api.data.gov key: GovInfo's summary and MODS routes are keyed.
"""

from __future__ import annotations

import hashlib
import os
from collections import Counter
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from loguru import logger
from spicy_docs.extraction.body_text import BodyText, body_text
from spicy_docs.interpretation.vote_matching import VoteMatchError, bill_type_of
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.committee_report_tables import (
    shape_committee_report,
    shape_hearing_transcript,
    shape_report_section,
)
from spicy_docs.schemas.tables import bill_id as bill_key
from spicy_docs.sources.agency_reports.report_blocks import parse_agency_blocks
from spicy_docs.sources.congress.bill_status import BillIdentity, BillSourceError
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, collection_url
from spicy_docs.sources.govinfo.mods import GovInfoModsError, parse_govinfo_mods

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.table_merge import merge_contract_table, published_table


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


def _held_packages(prior_file: Path) -> dict[str, str | None]:
    """``package_id`` -> published ``last_modified``, for skipping unchanged packages."""
    if not prior_file.exists():
        return {}
    import duckdb

    return dict(duckdb.sql(f"SELECT package_id, last_modified FROM read_parquet('{prior_file}')").fetchall())


def _package_ids(reader: PackageDiscoverySource, collection: str, since: str, limit: int) -> list[str]:
    """Package ids in one collection modified since ``since``, newest first, bounded."""
    ids: list[str] = []
    truncated = False
    for page in reader.packages(collection_url(collection, since), max_pages=MAX_PAGES):
        for record in page.records:
            package_id = record.get("packageId")
            if package_id:
                ids.append(str(package_id))
        if len(ids) >= limit:
            # The walk stopped early, so the window was not fully enumerated —
            # that is the fact worth warning about, not the arithmetic below.
            truncated = True
            logger.warning(
                "{}: per-run cap of {:,} packages reached — the window was not fully walked;"
                " the next run resumes from the published watermark",
                collection,
                limit,
            )
            break
    if not truncated:
        logger.info("{}: {:,} packages in window", collection, len(ids))
    return ids[:limit]


def _read_body(acquirer: PackageBodySource, package_id: str) -> tuple[Any, BodyText]:
    """One package's fetched body and the one text derivation for its rendition.

    Takes the narrow Protocol rather than the concrete acquirer, the way the
    bill family's ``_version_captures`` does: what this transform depends on is
    the three facts ``body_text`` reads off a fetched body, and the rendition
    it was fetched in is the body's own, never this caller's guess.
    """
    package = acquirer.acquire(package_id)
    # No extractor argument: ``body_text``'s default (``DocumentExtractor(NativeText())``,
    # PyMuPDF) is the pipeline ``gpo_normalize`` was derived on — the GPO
    # gutter-numbered layout only comes through on PyMuPDF's own line
    # adjacency, not pypdf's (measured
    # ``docs/research/gpo-normalizer-vs-upstream-2026-09-19.md`` in spicy-docs).
    return package, body_text(package)


#: The one MODS ``context`` that states a package is *about* a bill, as
#: opposed to mentioning it (``OTHER``, ``COVER``, ``BODY``).
PRIMARY_BILL_CONTEXT = "PRIMARY"


def _mods_bills(package: Any) -> tuple[tuple[str, str], ...]:
    """``(context, bill_id)`` for every ``<bill>`` the package's MODS extension states, in publisher order.

    The MODS bytes are the ones the acquirer already proved the package by;
    ``parse_govinfo_mods`` maps them without narrowing, and ``fields`` walks
    ``extension/bill`` by expanded name. A bill the publisher spells in a type
    the vocabulary lacks, or with a number that is not one, is skipped with a
    warning rather than published as a key with a hole in it.
    """
    try:
        record = parse_govinfo_mods(package.mods_capture.body).package
    except GovInfoModsError as error:
        logger.warning("{}: MODS unreadable, no bill linkage: {}", package.identity.package_id, error)
        return ()
    stated: list[tuple[str, str]] = []
    for element in record.fields("extension", "bill"):
        context = element.attribute("context") or ""
        try:
            identity = BillIdentity(
                congress=int(element.attribute("congress") or ""),
                bill_type=bill_type_of(element.attribute("type")),
                number=int(element.attribute("number") or ""),
            )
        except (ValueError, VoteMatchError, BillSourceError) as error:
            logger.warning("{}: MODS bill entry skipped: {}", package.identity.package_id, error)
            continue
        stated.append((context, bill_key(identity)))
    return tuple(stated)


def _primary_bill(bills: Sequence[tuple[str, str]]) -> str | None:
    """The one bill the publisher marks ``PRIMARY``, or NULL; a mention is not a linkage."""
    return next((bill for context, bill in bills if context == PRIMARY_BILL_CONTEXT), None)


def build_committee_reports(
    output_dir: Path,
    *,
    reader: PackageDiscoverySource | None = None,
    acquirer: PackageBodySource | None = None,
    max_packages: int = MAX_PACKAGES_PER_RUN,
    download_prior: Callable[[str, Path], bool] = r2.download,
) -> tuple[Path, Path, Path]:
    """Build the two committee-report tables and the hearing-transcript table."""
    if reader is None or acquirer is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Committee reports need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = reader or GovInfoDiscoveryReader(budget=DISCOVERY_BUDGET, api_key=api_key)
        acquirer = acquirer or GovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key)

    prior_files = {
        name: published_table(output_dir, name, download_prior) for name in ("committee_reports", "hearing_transcripts")
    }
    have_prior = {name: path is not None for name, path in prior_files.items()}
    # The watermark comes from the reports table: both collections are walked on
    # the same window, and it is the one with the longer history.
    since = _since(prior_files["committee_reports"])
    held = {name: ({} if path is None else _held_packages(path)) for name, path in prior_files.items()}
    logger.info("Committee reports: packages modified since {}", since)

    report_rows: list[dict] = []
    section_rows: list[dict] = []
    hearing_rows: list[dict] = []
    renditions: Counter[str] = Counter()
    mentions: Counter[str] = Counter()
    refused = unchanged = linked = 0

    for collection, table, rows, shape in (
        ("CRPT", "committee_reports", report_rows, shape_committee_report),
        ("CHRG", "hearing_transcripts", hearing_rows, shape_hearing_transcript),
    ):
        package_ids = _package_ids(reader, collection, since, max_packages)
        already = held[table]
        for package_id in package_ids:
            if package_id in already:
                # Published already, and the window is a last-modified window —
                # so a package that reappears unchanged costs nothing.
                unchanged += 1
                continue
            try:
                package, derived = _read_body(acquirer, package_id)
            except Exception as error:  # noqa: BLE001 — a package refusal is counted, not fatal
                refused += 1
                logger.warning("{}: {} refused: {}", collection, package_id, error)
                continue
            renditions[derived.rendition] += 1
            bills = _mods_bills(package)
            bill = _primary_bill(bills)
            if bill is not None:
                linked += 1
            mentions.update(context for context, _ in bills if context != PRIMARY_BILL_CONTEXT)
            rows.append(
                shape(
                    package,
                    bill_id=bill,
                    # NULL rather than 1: a rendition that states no page
                    # boundary has no page count, and calling the whole body
                    # one page would be a measurement nothing made.
                    page_count=None if derived.pages is None else len(derived.pages),
                    text_sha256="sha256:" + hashlib.sha256(derived.text.encode("utf-8")).hexdigest(),
                )
            )
            if collection == "CRPT":
                for seq, block in enumerate(parse_agency_blocks(derived.text)):
                    section_rows.append(
                        shape_report_section(
                            block,
                            package_id=package_id,
                            seq=seq,
                            last_modified=report_rows[-1].get("last_modified"),
                        )
                    )

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
    if renditions:
        # Which rendition each package was actually read in. Neither contract
        # has a column for the derivation name, so this is where it is stated.
        logger.info("Committee reports: renditions read — {}", dict(renditions))
    return (
        merge_contract_table(
            output_dir, "committee_reports", report_rows, prior_present=have_prior["committee_reports"]
        ),
        merge_contract_table(output_dir, "report_sections", section_rows),
        merge_contract_table(
            output_dir, "hearing_transcripts", hearing_rows, prior_present=have_prior["hearing_transcripts"]
        ),
    )
