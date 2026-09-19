"""Transform: build ``committee_reports``, ``report_sections``, ``hearing_transcripts``.

Three tables from two GovInfo collections, in one pass because they share an
acquirer and a pacing budget:

* **CRPT** (committee reports) fills ``committee_reports``, one row per
  captured package, and ``report_sections`` — the per-agency blocks
  ``spicy_docs.sources.agency_reports.report_blocks`` parses out of the report
  text, each carrying the header pattern that fired as its provenance.
* **CHRG** (hearing transcripts) fills ``hearing_transcripts``.

Report text goes through ``extraction.gpo_normalize`` before block parsing:
GPO text carries line numbers, VerDate footers and small-caps artifacts that
otherwise land inside a block's body. The cleanup record is processing
provenance and is not published here — ``bill_versions`` is the table that
carries it, for the printings where it changes the text people read.

``bill_id`` is a preserved NULL on both package tables. A package-keyed report
is fillable today; joining it to the bill it reports on is not, and the
contract says so rather than guessing from a title.

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
from collections.abc import Callable, Iterator, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Protocol

from loguru import logger
from spicy_docs.extraction.gpo_normalize import normalize_gpo_pages
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.schemas.committee_report_tables import (
    shape_committee_report,
    shape_hearing_transcript,
    shape_report_section,
)
from spicy_docs.sources.agency_reports.report_blocks import parse_agency_blocks
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyAcquirer, GovInfoBodyBudget
from spicy_docs.sources.govinfo.discovery import GovInfoDiscoveryReader, collection_url

from spicy_regs.sources import r2
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.table_merge import merge_contract_table, prior_scratch_path


class PackageDiscoverySource(Protocol):
    """What this transform needs of a GovInfo discovery reader."""

    def packages(self, url: str, *, max_pages: int = ...) -> Iterator[Any]: ...


class PackageBodySource(Protocol):
    """What this transform needs of a GovInfo package-body acquirer."""

    def acquire(self, package_id: str, *, prefer: Sequence[str] = ..., max_bytes: int | None = ...) -> Any: ...


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

#: GPO text separates pages with a form feed.
PAGE_BREAK = "\f"


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


def _body_text(package: Any) -> tuple[str, tuple[str, ...]]:
    """Decode a captured package body and normalize its GPO pages."""
    raw = package.body_capture.body.decode("utf-8", errors="replace")
    pages = tuple(raw.split(PAGE_BREAK))
    normalized, _cleanup = normalize_gpo_pages(pages)
    return "\n".join(normalized), normalized


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
        "committee_reports": prior_scratch_path(output_dir, "committee_reports"),
        "hearing_transcripts": prior_scratch_path(output_dir, "hearing_transcripts"),
    }
    have_prior = {
        name: (path.exists() or download_prior(f"{name}.parquet", path)) for name, path in prior_files.items()
    }
    # The watermark comes from the reports table: both collections are walked on
    # the same window, and it is the one with the longer history.
    since = _since(prior_files["committee_reports"] if have_prior["committee_reports"] else None)
    held = {name: (_held_packages(path) if have_prior[name] else {}) for name, path in prior_files.items()}
    logger.info("Committee reports: packages modified since {}", since)

    report_rows: list[dict] = []
    section_rows: list[dict] = []
    hearing_rows: list[dict] = []
    refused = unchanged = 0

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
                package = acquirer.acquire(package_id, prefer=("txt", "htm", "xml"))
            except Exception as error:  # noqa: BLE001 — a package refusal is counted, not fatal
                refused += 1
                logger.warning("{}: {} refused: {}", collection, package_id, error)
                continue
            text, pages = _body_text(package)
            rows.append(
                shape(
                    package,
                    page_count=len(pages),
                    text_sha256="sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest(),
                )
            )
            if collection == "CRPT":
                for seq, block in enumerate(parse_agency_blocks(text)):
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
    return (
        merge_contract_table(
            output_dir, "committee_reports", report_rows, prior_present=have_prior["committee_reports"]
        ),
        merge_contract_table(output_dir, "report_sections", section_rows),
        merge_contract_table(
            output_dir, "hearing_transcripts", hearing_rows, prior_present=have_prior["hearing_transcripts"]
        ),
    )
