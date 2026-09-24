"""Transform: build ``sam_entities.parquet`` from the SAM.gov Entity API (v4).

Produces a 19-column all-VARCHAR schema keyed on ``(uei, entity_eft_indicator)`` --
one row per registration, since an entity registers once per EFT indicator -- the
federal entity registry anchoring organization/entity resolution across the corpus.

**Incremental and bounded.** Best-effort download the prior table; fetch a
bounded window of active registrations (``max_records``); dedup the union on
the registration key preferring the fresh row. A first run seeds the table where later runs
refresh and extend it, and a full backfill is never triggered implicitly —
raise ``max_records`` (and widen the year range) deliberately for a wider pull.

**Coverage mechanism (``mode``).** The default ``"extract"`` fetches via SAM's
bulk async extract (``format=json``), one request per ``registrationDate``
year, each returning up to 1M records — full coverage of the ~765K active
registry is reachable within the 1,000 req/day key budget (≈20 year-window
requests) rather than the ~76,000 paginated requests a 10-record synchronous
page would need. The ``"partition"`` fallback walks the paginated endpoint with
adaptive date-window subdivision (see :mod:`spicy_docs.sources.sam`).
Both are bounded by ``max_records`` and the ``[since_year, until_year]`` range,
so a scheduled run advances coverage and the merge accretes it across runs.

Every column comes from the ``/entities`` payload: no per-entity detail
fetches.
"""

from __future__ import annotations

import time
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.transforms.table_merge import merge_local_prior

# Env vars checked in order for the api.data.gov key. SAM.gov needs a key that is
# specifically associated with a SAM.gov account holding the Entity API role, so
# the SAM-dedicated var is preferred first; a generic api.data.gov key (which
# works against regulations.gov / Congress.gov) is only a fallback and returns a
# bare 404 here if it isn't SAM-authorized.
SAM_API_KEY_ENV_VARS = (
    "SAM_API_KEY",
    "API_GOV",  # the shared api.data.gov key, under the name RefSpec/.env uses
    "DATA_GOV_API_KEY",
    "REGULATIONS_GOV_API_KEY",
)

# Earliest plausible registrationDate year to window over for a full extract.
MIN_REGISTRATION_YEAR = 2000

# Monotonic seconds, from the run's first extract trigger, that all of its extracts
# may take: each year's reader waits only what is left, and a year with none left
# is refused before its trigger. The 2026 registration-year extract (147,256 rows,
# 71.5 MB gzip) was still generating 21 minutes after its trigger and ready at the
# next poll, 46 minutes after it (spicy-docs sam_extract.py; receipts in
# sam-initial-load-2026-09-23/), past spicy-docs' 25-minute default. An hour clears
# 46 minutes and leaves 15 of rollup-sam-entities.yml's 75 for setup, download,
# merge and upload. A scheduled run fetches one rotating year; a multi-year
# dispatch shares the hour, so it refuses rather than being cancelled by the job.
EXTRACT_MAX_WAIT = 60 * 60.0

# The paged walk's per-request budget: the same retry margin the old local
# reader carried, with the paged reader's own page/byte bounds.
PAGED_BUDGET = None  # built lazily beside the reader that needs it


def _resolve_sam_api_key() -> str:
    """The first api.data.gov key set in :data:`SAM_API_KEY_ENV_VARS`; a missing key refuses."""
    import os

    for var in SAM_API_KEY_ENV_VARS:
        value = os.environ.get(var)
        if value:
            return value
    from spicy_docs.sources.sam_extract import SamExtractError

    raise SamExtractError("SAM entities require a SAM-authorized API key; set SAM_API_KEY")


def _iter_sam_entities(
    *,
    mode: str,
    registration_status: str,
    since_year: int | None,
    until_year: int | None,
    year_windows: bool,
    max_records: int | None,
):
    """Yield raw SAM entity dicts through the owner's two coverage mechanisms.

    ``extract`` drives the spicy-docs bulk extract (one request per
    ``registrationDate`` year); ``partition`` drives the owner's adaptive
    windowed walk under the publisher's reach bound. The env-key chain and
    the year-range policy stay here; the acquisition mechanics are the
    owner's.
    """
    from collections.abc import Iterator
    from datetime import date

    from spicy_docs.reading.paged_json import PagedJsonBudget
    from typing import cast

    from spicy_docs.sources.sam import RegistrationStatus, SamEntitiesReader, windowed_entities
    from spicy_docs.sources.sam_extract import SamBulkExtract, SamExtractError

    if mode not in ("extract", "partition"):
        raise ValueError(f"mode must be 'extract' or 'partition', got {mode!r}")
    for name, value in (("since_year", since_year), ("until_year", until_year)):
        if value is not None and (type(value) is not int or not 1 <= value <= 9999):
            raise ValueError(f"{name} must be a valid calendar year")
    if (since_year or MIN_REGISTRATION_YEAR) > (until_year or date.today().year):
        raise ValueError("since_year must not exceed until_year")
    if max_records is not None and (type(max_records) is not int or max_records <= 0):
        raise ValueError("max_records must be a positive integer or None")

    api_key = _resolve_sam_api_key()
    emitted = 0

    def budget_left() -> bool:
        return max_records is None or emitted < max_records

    def emit(records: Iterator[dict]):
        nonlocal emitted
        for record in records:
            if not budget_left():
                return
            emitted += 1
            yield record

    if mode == "extract":
        years: list[int | None] = [None] if not year_windows else list(
            range(
                since_year if since_year is not None else MIN_REGISTRATION_YEAR,
                (until_year if until_year is not None else date.today().year) + 1,
            )
        )
        deadline = time.monotonic() + EXTRACT_MAX_WAIT
        for year in years:
            if not budget_left():
                return
            left = deadline - time.monotonic()
            if left <= 0:
                raise SamExtractError(
                    f"SAM extract: the run's {EXTRACT_MAX_WAIT:,.0f} s wait is spent before year {year}; "
                    "nothing is published"
                )
            extractor = SamBulkExtract(
                api_key=api_key, registration_status=registration_status, year=year, max_wait=left
            )
            yield from emit(extractor.records())
        return

    # partition: the owner's adaptive windowed walk under the reach bound.
    budget = PagedJsonBudget(max_requests=5, max_page_bytes=8 * 1024 * 1024, timeout_seconds=120.0, min_request_interval_seconds=0.2)
    reader = SamEntitiesReader(budget=budget, api_key=api_key)
    since = date(since_year or MIN_REGISTRATION_YEAR, 1, 1)
    until = date(until_year or date.today().year, 12, 31)
    yield from emit(
        windowed_entities(
            reader,
            registered_from=since,
            registered_to=until,
            registration_status=cast(RegistrationStatus, registration_status),
            max_records=max_records,
        )
    )

OUTPUT = "sam_entities.parquet"

# Bounded default window pulled per run (a scheduled ingest must never attempt a
# full ~hundreds-of-thousands-row backfill). Raise deliberately for a wider pull.
DEFAULT_MAX_RECORDS = 5_000

# The published schema: 19 columns, all VARCHAR, in a fixed order. ``(uei,
# entity_eft_indicator)`` is the registration key: one entity registers once per EFT
# indicator (187 of 147,038 UEIs in the 2026 registration-year extract). Nested paths (source field) are noted where non-obvious.
COLUMNS = (
    "uei",  # entityRegistration.ueiSAM
    "entity_eft_indicator",  # entityRegistration.entityEFTIndicator; with uei, the registration key
    "cage_code",  # entityRegistration.cageCode
    "legal_business_name",  # entityRegistration.legalBusinessName
    "dba_name",  # entityRegistration.dbaName
    "entity_structure_desc",  # coreData.generalInformation.entityStructureDesc
    "entity_type_desc",  # coreData.generalInformation.entityTypeDesc
    "profit_structure_desc",  # coreData.generalInformation.profitStructureDesc
    "state",  # coreData.physicalAddress.stateOrProvinceCode
    "city",  # coreData.physicalAddress.city
    "zip_code",  # coreData.physicalAddress.zipCode
    "congressional_district",  # coreData.congressionalDistrict
    "primary_naics",  # assertions.goodsAndServices.primaryNaics
    "registration_status",  # entityRegistration.registrationStatus
    "registration_date",  # entityRegistration.registrationDate
    "registration_expiration_date",  # entityRegistration.registrationExpirationDate
    "exclusion_status_flag",  # entityRegistration.exclusionStatusFlag
    "purpose_of_registration_desc",  # entityRegistration.purposeOfRegistrationDesc
    "entity_url",  # coreData.entityInformation.entityURL
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (some fields come as ints.)"""
    if value is None:
        return None
    return str(value)


def _shape(doc: dict) -> dict:
    """Map one raw SAM.gov entity onto the published column shape.

    Missing nested objects degrade to null rather than raising, so a sparse
    registration (e.g. one with no ``coreData``) still produces a row keyed by
    its UEI.
    """
    from spicy_docs.sources.sam_extract import validate_entity

    validate_entity(doc)
    reg = doc["entityRegistration"]
    core = doc.get("coreData") or {}
    entity_info = core.get("entityInformation") or {}
    address = core.get("physicalAddress") or {}
    general = core.get("generalInformation") or {}
    goods = (doc.get("assertions") or {}).get("goodsAndServices") or {}
    return {
        "uei": reg.get("ueiSAM"),
        "entity_eft_indicator": reg.get("entityEFTIndicator"),
        "cage_code": reg.get("cageCode"),
        "legal_business_name": reg.get("legalBusinessName"),
        "dba_name": reg.get("dbaName"),
        "entity_structure_desc": general.get("entityStructureDesc"),
        "entity_type_desc": general.get("entityTypeDesc"),
        "profit_structure_desc": general.get("profitStructureDesc"),
        "state": address.get("stateOrProvinceCode"),
        "city": address.get("city"),
        "zip_code": address.get("zipCode"),
        "congressional_district": _s(core.get("congressionalDistrict")),
        "primary_naics": _s(goods.get("primaryNaics")),
        "registration_status": reg.get("registrationStatus"),
        "registration_date": reg.get("registrationDate"),
        "registration_expiration_date": reg.get("registrationExpirationDate"),
        "exclusion_status_flag": reg.get("exclusionStatusFlag"),
        "purpose_of_registration_desc": reg.get("purposeOfRegistrationDesc"),
        "entity_url": entity_info.get("entityURL"),
    }


def build_sam_entities(
    output_dir: Path,
    *,
    max_records: int | None = DEFAULT_MAX_RECORDS,
    registration_status: str = "A",
    mode: str = "extract",
    since_year: int | None = None,
    until_year: int | None = None,
    year_windows: bool = True,
) -> Path:
    """Build ``sam_entities.parquet`` (incremental merge with the prior table).

    ``mode`` selects the coverage mechanism (``"extract"`` bulk async extract, the
    default, or ``"partition"`` paginated adaptive walk); ``since_year``/``until_year``
    bound the ``registrationDate`` window range walked this run. See the module
    docstring and :mod:`spicy_docs.sources.sam_extract` for the full-coverage story.
    """
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_sam_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means a fresh seed).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("SAM entities: merging against prior table {}", prior_file)
    else:
        logger.info("SAM entities: no prior table found — seeding a new table")

    # 2. Fetch + shape into a "new rows" parquet.
    rows = [
        _shape(doc)
        for doc in _iter_sam_entities(
            mode=mode,
            registration_status=registration_status,
            since_year=since_year,
            until_year=until_year,
            year_windows=year_windows,
            max_records=max_records,
        )
    ]
    new_file = output_dir / "_sam_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("SAM entities: fetched {:,} entities this run", len(rows))

    # 3. Merge prior + new, dedup on the registration key preferring the new row.
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
        identity=("uei", "entity_eft_indicator"),
        order_by="legal_business_name, uei, entity_eft_indicator",
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=out_file,
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("SAM entities: {:,} rows", total)
    return out_file
