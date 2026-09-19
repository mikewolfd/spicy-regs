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

Needs an api.data.gov key: GovInfo's summary and MODS routes are keyed.
"""

from __future__ import annotations

import hashlib
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

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

from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key
from spicy_regs.transforms.table_merge import merge_contract_table

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

#: Packages are enumerated by last-modified, so a run picks up a revision of an
#: older report as well as a new one. Thirty days back covers a daily cron with
#: a wide margin for an outage.
DEFAULT_WINDOW_DAYS = 30

#: ~3 requests each at ~3/s: 200 packages is ~3.5 minutes per collection.
MAX_PACKAGES_PER_RUN = 200

MAX_PAGES = 40

#: GPO text separates pages with a form feed.
PAGE_BREAK = "\f"


def _since(window_days: int = DEFAULT_WINDOW_DAYS) -> str:
    raw = os.environ.get("COMMITTEE_REPORTS_SINCE", "").strip()
    if raw:
        return raw if raw.endswith("Z") else f"{raw}T00:00:00Z"
    start = datetime.now(UTC) - timedelta(days=window_days)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ")


def _package_ids(reader: GovInfoDiscoveryReader, collection: str, since: str, limit: int) -> list[str]:
    """Package ids in one collection modified since ``since``, newest first, bounded."""
    ids: list[str] = []
    for page in reader.packages(collection_url(collection, since), max_pages=MAX_PAGES):
        for record in page.records:
            package_id = record.get("packageId")
            if package_id:
                ids.append(str(package_id))
        if len(ids) >= limit:
            break
    if len(ids) > limit:
        logger.warning("{}: {:,} packages in window, taking the first {:,}", collection, len(ids), limit)
    return ids[:limit]


def _body_text(package) -> tuple[str, tuple[str, ...]]:
    """Decode a captured package body and normalize its GPO pages."""
    raw = package.body_capture.body.decode("utf-8", errors="replace")
    pages = tuple(raw.split(PAGE_BREAK))
    normalized, _cleanup = normalize_gpo_pages(pages)
    return "\n".join(normalized), normalized


def build_committee_reports(
    output_dir: Path,
    *,
    reader: GovInfoDiscoveryReader | None = None,
    acquirer: GovInfoBodyAcquirer | None = None,
    max_packages: int = MAX_PACKAGES_PER_RUN,
) -> tuple[Path, Path, Path]:
    """Build the two committee-report tables and the hearing-transcript table."""
    if reader is None or acquirer is None:
        api_key = _resolve_api_key()
        if not api_key:
            raise RuntimeError(f"Committee reports need an api.data.gov key (set one of {', '.join(API_KEY_ENV_VARS)})")
        reader = reader or GovInfoDiscoveryReader(budget=DISCOVERY_BUDGET, api_key=api_key)
        acquirer = acquirer or GovInfoBodyAcquirer(budget=BODY_BUDGET, api_key=api_key)

    since = _since()
    logger.info("Committee reports: packages modified since {}", since)

    report_rows: list[dict] = []
    section_rows: list[dict] = []
    hearing_rows: list[dict] = []
    refused = 0

    for collection, rows, shape in (
        ("CRPT", report_rows, shape_committee_report),
        ("CHRG", hearing_rows, shape_hearing_transcript),
    ):
        package_ids = _package_ids(reader, collection, since, max_packages)
        logger.info("{}: {:,} packages in window", collection, len(package_ids))
        for package_id in package_ids:
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
        "Committee reports: {:,} reports, {:,} sections, {:,} hearings, {:,} refused",
        len(report_rows),
        len(section_rows),
        len(hearing_rows),
        refused,
    )
    return (
        merge_contract_table(output_dir, "committee_reports", report_rows),
        merge_contract_table(output_dir, "report_sections", section_rows),
        merge_contract_table(output_dir, "hearing_transcripts", hearing_rows),
    )
