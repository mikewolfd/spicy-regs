"""Transform: build ``usaspending_recipients.parquet`` from USASpending.gov.

Produces an all-VARCHAR schema keyed on ``recipient_id`` — the
federal-award **recipient** reference dimension that sits alongside the
regulations.gov corpus and complements the SAM entity registry and FEC
committee table for resolving organization commenters by their federal
funding. The same UEI can appear at multiple ``recipient_level`` values (parent
``P`` / child ``C`` / standalone ``R``), so ``recipient_id``, unique per level,
is the primary/dedup key; consumers filter by level and join on ``uei``.

Scope is deliberately bounded to the **top-N recipients by trailing-12-month federal
award amount**: the endpoint reports ~18M recipients across all history, so a
full walk is infeasible and the largest-funded organizations are both the most
resolution-useful and naturally bounded. There is no watermark — recipients
are a reference dimension, not a time series — so each run fetches the current
top-N and merges it with the prior table, dedup on ``recipient_id`` preferring
the fresh row, which keeps the row count monotonic and clear of the R2
catastrophic-shrink guard.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

OUTPUT = "usaspending_recipients.parquet"

# The provider serves at most 100 rows per request; the default selection walks
# the top 100 pages (at most 10,000 rows). max_pages remains an explicit
# operational scope control over the top-N ranking, not the whole population.
PER_PAGE = 100
DEFAULT_MAX_PAGES = 100
_MAX_REQUESTS_PER_PAGE = 5

# The published schema, all VARCHAR, in a fixed order (``data_dictionary`` declares
# the same list). ``recipient_id`` is the primary / dedup key.
COLUMNS = (
    "recipient_id",
    "uei",
    "duns",
    "name",
    "recipient_level",
    "total_award_amount",
    "observed_at",
    "source_capture_sha256",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (``amount`` comes as a float.)"""
    if value is None:
        return None
    return str(value)


def _shape(doc: dict) -> dict:
    """Map one raw USASpending recipient onto the published column shape."""
    return {
        "recipient_id": doc.get("id"),
        "uei": doc.get("uei"),
        "duns": doc.get("duns"),
        "name": doc.get("name"),
        "recipient_level": doc.get("recipient_level"),
        "total_award_amount": _s(doc.get("amount")),
        "observed_at": doc.get("_source_observed_at"),
        "source_capture_sha256": doc.get("_source_capture_sha256"),
    }


def _iter_recipient_rows(
    *,
    per_page: int = PER_PAGE,
    max_pages: int | None = None,
    transport: httpx.BaseTransport | None = None,
    evidence: CaptureEvidence | None = None,
    include_observation: bool = False,
) -> Iterator[dict]:
    """Yield the selected top-page recipient ranking through SpicyDocs' evidenced reader.

    SpicyDocs owns the POST page-number walk, page parsing, declared counts,
    continuations and exact page evidence. The walk stops after ``max_pages``
    pages rather than refusing at the owner's page bound: a configured page cap
    completes this selected scope, not the entire recipient population. A page
    whose metadata omits or contradicts its continuation, a continuation that
    skips a page or follows an empty page, and a missing or repeated recipient
    ``id`` refuse the selection.
    """
    from spicy_docs.reading.paged_json import PagedJsonBudget, PagedJsonSourceError
    from spicy_docs.sources.usaspending import UsaspendingRecipientsReader, recipients_request

    pages = DEFAULT_MAX_PAGES if max_pages is None else max_pages
    url, body = recipients_request(limit=per_page)
    budget = PagedJsonBudget(
        max_requests=_MAX_REQUESTS_PER_PAGE,
        max_page_bytes=16 * 1024 * 1024,
        timeout_seconds=60,
        min_request_interval_seconds=0,
    )
    seen: set[str] = set()
    if evidence is not None:
        # The owner requests identity encoding and reads raw bytes, so each
        # page's ``capture.sha256`` names the blob this tee retains; the tee
        # alone also keeps retries and pages the owner refuses.
        evidence.event("selection", stage="usaspending", url=url, request_body=body, max_pages=pages)
        transport = evidence.transport(transport, stage="usaspending-response", max_bytes=budget.max_page_bytes)
    with UsaspendingRecipientsReader(budget=budget, transport=transport) as reader:
        for index, page in enumerate(reader.recipients(body, max_pages=pages)):
            metadata = json.loads(page.capture.body).get("page_metadata")
            if not isinstance(metadata, dict) or type(metadata.get("hasNext")) is not bool or "next" not in metadata:
                raise PagedJsonSourceError("USAspending page omitted its continuation metadata")
            if metadata["hasNext"] != (page.next_url is not None):
                raise PagedJsonSourceError("USAspending hasNext and next disagree")
            if page.next_url is not None:
                if (
                    not page.records
                    or page.next_body is None
                    or page.request_body is None
                    or page.next_body["page"] != page.request_body["page"] + 1
                ):
                    raise PagedJsonSourceError("USAspending continuation skips a page or follows an empty page")
            if evidence is not None:
                # The tee journals when the request started; a row states the reader's response-complete
                # time, so the journal carries that too, keyed by the same digest.
                evidence.event("page-read", stage="usaspending", sha256=page.capture.sha256,
                               observed_at=page.capture.observed_at, records=len(page.records))
            for record in page.records:
                identity = record.get("id")
                if not isinstance(identity, str) or not identity.strip() or identity in seen:
                    raise PagedJsonSourceError("USAspending page contains a missing or repeated recipient identity")
                seen.add(identity)
                observed = dict(record)
                if include_observation:
                    observed["_source_observed_at"] = page.capture.observed_at
                    observed["_source_capture_sha256"] = page.capture.sha256
                yield observed
            if index + 1 == pages and page.next_url is not None:
                return  # the requested top-page selection is complete even when more pages exist


def build_usaspending_recipients(
    output_dir: Path, *, max_pages: int | None = None, evidence: CaptureEvidence | None = None,
) -> Path:
    """Build ``usaspending_recipients.parquet`` (top-N merged with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_usaspending_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means a clean build).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("USASpending recipients: merging against prior table {}", prior_file)
    else:
        logger.info("USASpending recipients: no prior table found — clean build")

    # 2. Fetch + shape into a "new rows" parquet.
    rows = [_shape(doc) for doc in _iter_recipient_rows(
        max_pages=max_pages, include_observation=True, evidence=evidence,
    )]
    new_file = output_dir / "_usaspending_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("USASpending recipients: fetched {:,} recipients this run", len(rows))

    # 3. Merge prior + new, dedup on recipient_id preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    cols = ", ".join(COLUMNS)
    if have_prior:
        prior_columns = set(pq.read_schema(prior_file).names)
        # Only the additive observation fields may be absent in a legacy file.
        # Never stamp carried records with this run's time or capture digest.
        prior_cols = ", ".join(
            f"CAST(NULL AS VARCHAR) AS {col}" if col not in prior_columns and col in
            ("observed_at", "source_capture_sha256") else col for col in COLUMNS
        )
        union = (
            f"SELECT {prior_cols}, 0 AS _src FROM read_parquet('{prior_file}') "
            f"UNION ALL BY NAME "
            f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"
        )
    else:
        union = f"SELECT {cols}, 1 AS _src FROM read_parquet('{new_file}')"

    con.execute(
        f"""
        COPY (
            SELECT {cols} FROM (
                SELECT {cols}, ROW_NUMBER() OVER (
                    PARTITION BY recipient_id ORDER BY _src DESC
                ) AS _rn
                FROM ({union})
                WHERE recipient_id IS NOT NULL
            )
            WHERE _rn = 1
            ORDER BY TRY_CAST(total_award_amount AS DOUBLE) DESC NULLS LAST, recipient_id
        ) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
        """
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("USASpending recipients: {:,} rows", total)
    return out_file
