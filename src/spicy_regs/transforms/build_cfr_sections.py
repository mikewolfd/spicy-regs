"""Transform: build ``cfr_sections.parquet`` from the GovInfo CFR API and annual volume XML.

All-VARCHAR section metadata, one row per CFR granule (a section-level unit
within a title's annual edition) keyed on ``granule_id``. ``cfr_ref`` is the
join key back to Federal Register ``cfr_references_json``; ``title``, ``part``
and ``section`` describe structure. SECTION METADATA ONLY — the full
regulatory text is deliberately out of scope (see ``sources/cfr_sections.py``).

Incremental by design: best-effort prior from R2, fetch bounded by
``since_year``, dedup on ``granule_id`` preferring the fresh row, so a short
run never trips the R2 catastrophic-shrink guard. With no prior table the
output covers only the requested year window, not a full historical backfill.

Neither a granule id nor a printed section number establishes the enclosing
part, so section granules are placed from their package's annual volume XML:
one download and one streaming SpicyDocs scan per package, then a lookup per
granule token among the scan's canonical sections, O(volume bytes + granules).
``part`` is the innermost PART heading. Title 43 numbers sections by subpart:
§ 1601.0-1 sits in Part 1600, so ``part`` is 1600 (structure) while only
``cfr_ref`` (``43-1601.0-1``, the printed citation) joins the Federal Register
side. Title 41's compound parts keep their hyphen (``50-201``) and Title 14
Part 241 prints ``19-8.1``. ``cfr_ref`` is NULL for ranges, publisher typos,
unprefixed numbers and parenthesized citations (SpicyDocs'
``split_annual_cfr_section``). A volume SpicyDocs' identity validator refuses
is still scanned and the refusal logged; an empty scan places nothing; a volume
that cannot be downloaded or scanned keeps that package's prior rows.

Placement changes only the rows the scan holds. Every other granule keeps its
identifier-derived values: TOC, NODE and appendix part tokens (so Title 41's
TOC and NODE parts stay cut at the first hyphen, ``part50-201`` gives ``50``),
and for a section's appendix or TOC (``sec746-10-app1``) the token's leading
number (746), as published before cac7615. A plain section token the scan does
not hold keeps an unknown part.

A package is re-placed only when it is new to the prior table or any of its
granules is new or carries a different ``last_modified``; otherwise its prior
rows stand and no volume is downloaded. That is safe only once the prior table
was itself built by this placement: generations published before it
(``8fb97150`` and earlier) hold identifier-derived parts that an unchanged
package would keep. Publish the full rebuild (the A8 candidate) before the
scheduled run resumes.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.sources.cfr_sections import CfrSectionsError, CfrSectionsReader
from spicy_regs.transforms.table_merge import merge_local_prior

if TYPE_CHECKING:
    from spicy_docs.sources.cfr.acquisition import CfrAcquirer
    from spicy_docs.sources.cfr.models import AnnualCfrSelection

OUTPUT = "cfr_sections.parquet"

#: Byte cap for one annual volume's download and scan. The largest retained
#: 2025 volume (40 CFR vol 20) is 12,576,481 bytes; SpicyDocs caps any CFR
#: capture at 256 MiB (spicy-docs docs/sources/cfr.md).
MAX_VOLUME_BYTES = 64 * 1024 * 1024
_VOLUME_REQUESTS = 3  # per volume, retries of a 429/5xx/transport failure included
_VOLUME_TIMEOUT_SECONDS = 60.0
_VOLUME_REQUEST_INTERVAL_SECONDS = 1.0

# The published schema: all VARCHAR, in a fixed order. ``granule_id`` is the
# primary/dedup key.
COLUMNS = (
    "granule_id",
    "package_id",
    "cfr_ref",
    "title",
    "part",
    "section",
    "heading",
    "structure_level",
    "edition_year",
    "last_modified",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (title/part come as ints.)"""
    if value is None:
        return None
    return str(value)


def _cfr_ref(title: object, part: object, section: object) -> str | None:
    """Compose a compact CFR citation like ``40-60.1`` from title/part/section."""
    if title is None or part is None:
        return None
    if section is None:
        return f"{title}-{part}"
    return f"{title}-{part}.{section}"


# ID-grammar parsers. GovInfo CFR IDs look like:
#   package: CFR-2024-title48-vol5
#   granule: CFR-2024-title48-vol5-chap7-appA / …-part700 / …-sec60-1
# A section token alone does not establish its enclosing part.
_EDITION_RE = re.compile(r"CFR-(\d{4})")
_TITLE_RE = re.compile(r"title(\d+)")
# Letter suffixes are part of the publisher's identifier: Part 1203a and
# Part 1203b must not collapse into Part 1203, including their TOC/child rows.
_PART_RE = re.compile(r"part(\d+[A-Za-z]*)")
_SECTION_RE = re.compile(r"sec([\w.-]+)")
# A section's appendix or TOC (``746-10-app1``, ``1002-31-toc-id1699``) is not a
# section the volume scan holds; it keeps the part its token's leading number
# spells and the rest as its section, as every generation before cac7615 did.
_ATTACHED_RE = re.compile(r"^(\d+[A-Za-z]*)-(.+-(?:app|toc).*)$")
_VOLUME_RE = re.compile(r"CFR-(\d{4})-title(\d+)-vol(\d+)")
# A section granule's token, less GovInfo's duplicate suffix: ``sec849-504-id915``
# is the same printed section as ``sec849-504``.
_SECTION_TOKEN_RE = re.compile(r"-sec(.+?)(?:-id\d+)?$")


def _first(pattern: re.Pattern[str], text: str | None) -> str | None:
    """Return the first capture group of ``pattern`` in ``text``, else None."""
    if not text:
        return None
    match = pattern.search(text)
    return match.group(1) if match else None


def _shape(granule: dict) -> dict:
    """Map one raw GovInfo CFR granule onto the published all-VARCHAR shape, from its identifiers.

    Section granules are placed afterwards by ``place_sections``. Missing
    optional facts remain NULL; a missing source identity refuses.
    """
    granule_id = granule.get("granuleId")
    if not isinstance(granule_id, str) or not granule_id.strip():
        raise CfrSectionsError("CFR row requires a nonempty granuleId")
    package_id = _s(granule.get("_package_id") or granule.get("packageId"))

    # CFR title number + edition year: prefer the package id (always well-formed),
    # fall back to the granule id, then dateIssued's leading year for the edition.
    id_for_meta = package_id or granule_id
    edition_year = _first(_EDITION_RE, id_for_meta)
    if edition_year is None:
        date_issued = _s(granule.get("dateIssued"))
        edition_year = date_issued[:4] if date_issued else None
    title_num = _first(_TITLE_RE, id_for_meta)

    # Part / section tokens from the granule id (both nullable — see module docstring).
    part = _first(_PART_RE, granule_id)
    section = _first(_SECTION_RE, granule_id)
    attached = _ATTACHED_RE.match(section) if part is None and section is not None else None
    if attached:
        part, section = attached.group(1), attached.group(2)

    return {
        "granule_id": granule_id,
        "package_id": package_id,
        "cfr_ref": _cfr_ref(title_num, part, section),
        "title": title_num,
        "part": part,
        "section": section,
        # The granule list-level ``title`` field is the heading text.
        "heading": _s(granule.get("title")),
        "structure_level": _s(granule.get("granuleClass")),
        "edition_year": edition_year,
        "last_modified": _s(granule.get("lastModified") or granule.get("_package_last_modified")),
        "url": f"https://www.govinfo.gov/app/details/{package_id}/{granule_id}" if package_id and granule_id else None,
    }


def annual_volume(package_id: str | None) -> AnnualCfrSelection | None:
    """The annual volume a package id names; ``None`` for other CFR packages (``GPO-CFR-INDEX-2025``)."""
    from spicy_docs.sources.cfr.models import AnnualCfrSelection

    match = _VOLUME_RE.fullmatch(package_id or "")
    return AnnualCfrSelection(int(match[1]), int(match[2]), int(match[3])) if match else None


def place_sections(rows: Iterable[dict], xml: bytes, volume: AnnualCfrSelection) -> list[dict]:
    """Place each section granule of one package under its PART heading in the package's volume XML.

    The one placement both the scheduled build and an offline rebuild call.
    SpicyDocs' identity validator refuses two real volumes (CFR-2025-title34-vol4
    repeats TITLENUM for the combined Title 34/35 volume; CFR-2025-title40-vol9
    prints no SECTION), so its refusal is logged, never a reason to skip the
    scan. The scan itself refuses a non-volume root and unsafe or oversized XML.
    A granule is looked up by token among the ``canonical`` sections only;
    nested, wrapped and revised copies are current text too. Rows the scan does
    not hold keep their identifier-derived values.
    """
    from spicy_docs.sources.cfr.annual import (
        annual_cfr_xml_locator,
        scan_annual_cfr_sections,
        split_annual_cfr_section,
        validate_annual_cfr_xml,
    )
    from spicy_docs.sources.cfr.models import CfrSourceError

    locator = annual_cfr_xml_locator(volume)
    try:
        validate_annual_cfr_xml(xml, identity=volume, final_url=locator, max_bytes=MAX_VOLUME_BYTES)
    except CfrSourceError as refusal:
        logger.warning("CFR: {} fails the annual volume validator ({}); placing from its scan", locator, refusal)
    sections = {s.granule: s for s in scan_annual_cfr_sections(xml, max_bytes=MAX_VOLUME_BYTES) if s.canonical}

    placed = []
    for row in rows:
        token = _first(_SECTION_TOKEN_RE, row["granule_id"])
        section = sections.get(token) if token else None
        if section is None:
            placed.append(row)
            continue
        number = split_annual_cfr_section(section.number, section.part, section.subpart)
        joins = number.citation is not None and number.citation_joins and row["title"] is not None
        cfr_ref = f"{row['title']}-{number.citation}" if joins else None
        placed.append({**row, "part": section.part, "section": number.section, "cfr_ref": cfr_ref})
    return placed


def _placed_package(acquirer: CfrAcquirer, package_id: str | None, rows: list[dict]) -> list[dict] | None:
    """The package's rows with section granules placed; ``None`` when its volume cannot be read.

    One download per package that has section granules. A failure leaves the
    prior table's rows for the package in place (the merge keeps them).
    """
    volume = annual_volume(package_id)
    if volume is None or not any(_SECTION_TOKEN_RE.search(row["granule_id"]) for row in rows):
        return rows
    from spicy_docs.sources.cfr.acquisition import CfrSourceUnavailableError
    from spicy_docs.sources.cfr.annual import annual_cfr_xml_locator
    from spicy_docs.sources.cfr.models import CfrSourceError
    from spicy_docs.transport.credentials import CredentialRefusedError

    try:
        xml, _capture = acquirer.capture_validated(
            annual_cfr_xml_locator(volume),
            media_types=("application/xml", "text/xml"),
            parse=lambda response, _limit: response.body,
            max_bytes=MAX_VOLUME_BYTES,
            unavailable=CfrSourceUnavailableError,
            context={"operation": "annual-cfr-volume", "package": package_id},
        )
        return place_sections(rows, xml, volume)
    except (CfrSourceError, CredentialRefusedError, httpx.HTTPError, ConnectionError) as error:
        logger.warning("CFR: {} volume could not be placed ({}); keeping its prior rows", package_id, error)
        return None


def _prior_modified(prior_file: Path | None) -> dict[str, str | None]:
    """Each prior granule's ``last_modified``, by ``granule_id``; empty without a prior table."""
    if prior_file is None:
        return {}
    table = pq.read_table(prior_file, columns=["granule_id", "last_modified"])
    return dict(zip(table["granule_id"].to_pylist(), table["last_modified"].to_pylist(), strict=True))


def _unchanged(rows: list[dict], prior: dict[str, str | None]) -> bool:
    """Every granule is already in the prior table with the same ``last_modified``."""
    return all(row["granule_id"] in prior and prior[row["granule_id"]] == row["last_modified"] for row in rows)


def _volume_acquirer() -> CfrAcquirer:
    """A paced, bounded GovInfo client for annual volume XML."""
    from spicy_docs.sources.cfr.acquisition import CfrAcquirer, CfrAcquisitionBudget

    budget = CfrAcquisitionBudget(
        max_requests=_VOLUME_REQUESTS,
        max_bytes=MAX_VOLUME_BYTES,
        timeout_seconds=_VOLUME_TIMEOUT_SECONDS,
        min_request_interval_seconds=_VOLUME_REQUEST_INTERVAL_SECONDS,
    )
    return CfrAcquirer(budget=budget)


def build_cfr_sections(output_dir: Path, *, since_year: int | None = None, acquirer: CfrAcquirer | None = None) -> Path:
    """Build ``cfr_sections.parquet`` (incremental merge with the prior table).

    ``acquirer`` replaces the default volume client (for hermetic replay).
    """
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_cfr_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("CFR: merging against prior table {}", prior_file)
    else:
        logger.info("CFR: no prior table found — output covers the selected year window")

    # 2. Complete the selected traversal before downloading any volume or writing output.
    reader = CfrSectionsReader(since_year=since_year)
    packages: dict[str | None, list[dict]] = {}
    for granule in reader.iter_records():
        row = _shape(granule)
        packages.setdefault(row["package_id"], []).append(row)

    # 3. Re-place only changed packages (see module docstring); an unchanged package
    # or a failed volume keeps that package's prior rows.
    prior = _prior_modified(prior_file if have_prior else None)
    rows: list[dict] = []
    unchanged = failed = 0
    with acquirer or _volume_acquirer() as volumes:
        for package_id, shaped in packages.items():
            if _unchanged(shaped, prior):
                unchanged += 1
                continue
            placed = _placed_package(volumes, package_id, shaped)
            if placed is None:
                failed += 1
            else:
                rows.extend(placed)
    new_file = output_dir / "_cfr_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info(
        "CFR: {:,} granules re-placed this run; prior rows kept for {} unchanged and {} failed packages",
        len(rows),
        unchanged,
        failed,
    )

    # 4. Merge prior + new, dedup on granule_id preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    merge_local_prior(
        con,
        columns=COLUMNS,
        identity="granule_id",
        order_by="edition_year DESC, granule_id",
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=out_file,
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("CFR sections: {:,} rows", total)
    return out_file
