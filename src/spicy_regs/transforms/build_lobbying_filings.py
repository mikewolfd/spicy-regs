"""Transform: build ``lobbying_filings.parquet`` from the Senate LDA REST API.

Produces a pinned, all-VARCHAR schema keyed by ``filing_uuid``. Nested and
array-valued fields (lobbying activities, the government entities lobbied) are
serialized as JSON strings so the published table stays flat and portable.

Incremental by design (mirrors ``build_federal_register``): a full re-fetch of
the multi-million-row LDA archive every run would be wasteful *and* would trip
the R2 catastrophic-shrink guard on any short run. Instead we:

1. Best-effort download the prior ``lobbying_filings.parquet`` from R2.
2. Fetch only filings posted since its max ``dt_posted`` (minus a short overlap
   to catch late-posted / amended filings).
3. Dedup the union on ``filing_uuid``, preferring the freshly fetched row.

With no prior table (first run) step 2 becomes a full backfill. The reader is
functional keyless, so this runs in CI with or without ``LDA_API_KEY``.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import date, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.transforms.table_merge import merge_local_prior

if TYPE_CHECKING:
    from spicy_docs.sources.lda import LdaFilingsReader

    from spicy_regs.source_evidence import CaptureEvidence

OUTPUT = "lobbying_filings.parquet"

# Re-scan this many days before the last stored dt_posted on each run, so
# filings posted/amended after the watermark are picked up.
OVERLAP_DAYS = 7
MAX_WINDOW_DAYS = 30

#: Requests per page: one attempt plus the six retries the deleted local
#: reader made per request; keyless requests are rate-limited more
#: aggressively, so 429s are expected and retried within this per-page budget
#: (spicy-docs' retry policy).
MAX_REQUESTS_PER_PAGE = 7

#: Pacing between page requests. Keyless, lda.gov served 15 pages back to back and then
#: answered "Request was throttled. Expected available in 44 seconds." until the per-page
#: retries ran out (2026-09-23), so an unpaced keyless walk cannot finish; 15 a minute is
#: what it measurably allows. A key raises the limit.
KEYLESS_INTERVAL_SECONDS = 4.0
KEYED_INTERVAL_SECONDS = 0.5

#: The whole archive at page size 25 is ~79,082 pages (spicy-docs' SR07
#: measurement, 2026-09-21); this bound preserves a cold-start full backfill
#: with headroom while the walk still refuses past it rather than ending
#: silently.
MAX_PAGES = 100_000


class LobbyingFilingsError(ValueError):
    """The requested LDA scope could not be read completely."""


def _bounded_until(since: date | None, until: date | None, *, today: date | None = None) -> date | None:
    """Return a deterministic end date no more than one catch-up window ahead."""
    if since is None:
        return until
    if until is not None and until < since:
        raise ValueError(f"LDA until date {until} precedes since date {since}")
    return min(until or today or date.today(), since + timedelta(days=MAX_WINDOW_DAYS))


def _filings_url(*, since: date | None, until: date | None, filing_year: int | None) -> str:
    """The one filings query: a year or a posted-date window, oldest first.

    Pagination requires a data filter (page/ordering do not count), so a cold
    start with no filter holds at today's existing upper-bound meaning without
    guessing the first archive year or dropping historical filings — the same
    filtered-request semantics the deleted local reader carried.
    """
    from spicy_docs.sources.lda import filings_url

    if filing_year is None and since is None and until is None:
        until = date.today()
    return filings_url(
        filing_year=filing_year,
        posted_after=since.isoformat() if since is not None else None,
        posted_before=until.isoformat() if until is not None else None,
        ordering="dt_posted",
    )


def _iter_filings(reader: LdaFilingsReader, url: str, *, max_records: int | None) -> Iterator[dict]:
    """Yield raw filing dicts from spicy-docs' page walk, bound by ``max_records``.

    Every row must carry a nonempty ``filing_uuid`` — the merge identity — so a
    malformed row refuses instead of landing a NULL key in the published table.
    The owner's walk already refuses a changed declared count, an observed
    total disagreeing with it, or a page bound reached before the terminal
    page; a ``max_records`` stop is an explicit prefix of that walk.
    """
    yielded = 0
    for page in reader.filings(url, max_pages=MAX_PAGES):
        for record in page.records:
            filing = dict(record)
            identity = filing.get("filing_uuid")
            if not isinstance(identity, str) or not identity.strip():
                raise LobbyingFilingsError("LDA filings page has a row without a nonempty filing_uuid")
            yield filing
            yielded += 1
            if max_records is not None and yielded >= max_records:
                return


# The published schema: all VARCHAR, keyed by filing_uuid. Array/nested fields
# are JSON strings.
COLUMNS = (
    "filing_uuid",
    "filing_type",
    "filing_year",
    "filing_period",
    "dt_posted",
    "registrant_name",
    "registrant_id",
    "client_name",
    "client_id",
    "income",
    "expenses",
    "lobbying_activities_json",
    "government_entities_json",
    "url",
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL. (ids come as ints.)"""
    if value is None:
        return None
    return str(value)


def _activities(filing: dict) -> list[dict]:
    """Project lobbying_activities to their issue codes + descriptions."""
    out: list[dict] = []
    for act in filing.get("lobbying_activities") or []:
        if not isinstance(act, dict):
            continue
        out.append(
            {
                "general_issue_code": act.get("general_issue_code"),
                "general_issue_code_display": act.get("general_issue_code_display"),
                "description": act.get("description"),
            }
        )
    return out


def _government_entities(filing: dict) -> list[dict]:
    """Collect the distinct government entities (agencies/chambers) lobbied.

    They are nested under each lobbying activity; flatten + dedup on entity id
    (falling back to name) so a filing that lobbied the same chamber across
    several issues lists it once.
    """
    seen: set[object] = set()
    out: list[dict] = []
    for act in filing.get("lobbying_activities") or []:
        if not isinstance(act, dict):
            continue
        for ent in act.get("government_entities") or []:
            if not isinstance(ent, dict):
                continue
            key = ent.get("id") if ent.get("id") is not None else ent.get("name")
            if key in seen:
                continue
            seen.add(key)
            out.append({"id": ent.get("id"), "name": ent.get("name")})
    return out


def _shape(filing: dict) -> dict:
    """Map one raw LDA filing onto the published column shape."""
    registrant = filing.get("registrant") or {}
    client = filing.get("client") or {}
    return {
        "filing_uuid": filing.get("filing_uuid"),
        "filing_type": filing.get("filing_type"),
        "filing_year": _s(filing.get("filing_year")),
        "filing_period": filing.get("filing_period"),
        "dt_posted": filing.get("dt_posted"),
        "registrant_name": registrant.get("name") if isinstance(registrant, dict) else None,
        "registrant_id": _s(registrant.get("id")) if isinstance(registrant, dict) else None,
        "client_name": client.get("name") if isinstance(client, dict) else None,
        "client_id": _s(client.get("id") or client.get("client_id")) if isinstance(client, dict) else None,
        "income": filing.get("income"),
        "expenses": filing.get("expenses"),
        "lobbying_activities_json": json.dumps(_activities(filing)),
        "government_entities_json": json.dumps(_government_entities(filing)),
        "url": filing.get("filing_document_url"),
    }


def _prior_max_dt_posted(prior_file: Path) -> date | None:
    """Largest ``dt_posted`` date in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(dt_posted) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def build_lobbying_filings(
    output_dir: Path,
    *,
    evidence: CaptureEvidence | None = None,
    since: date | None = None,
    until: date | None = None,
    filing_year: int | None = None,
    max_records: int | None = None,
) -> Path:
    """Build ``lobbying_filings.parquet`` (incremental merge with the prior table)."""
    import duckdb

    out_file = output_dir / OUTPUT
    prior_file = output_dir / "_lda_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        logger.info("LDA: merging against prior table {}", prior_file)
    else:
        logger.info("LDA: no prior table found — full backfill")

    # 2. Decide the fetch window start.
    if since is None:
        prior_max = _prior_max_dt_posted(prior_file) if have_prior else None
        since = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else None
    # A stale watermark must not create an ever-growing request that repeatedly
    # hits the 30-minute CI timeout. Walk toward today in bounded windows; the
    # overlap makes each successive merge resilient to late/amended filings.
    until = _bounded_until(since, until)
    logger.info("LDA: fetching filings posted {} through {}", since or "the beginning", until or "today")

    # 3. Fetch + shape into a "new rows" parquet. spicy-docs' LDA reader owns
    # the requests, retries and walk refusals; an optional LDA_API_KEY raises
    # the keyless rate limit.
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.lda import LdaFilingsReader

    url = _filings_url(since=since, until=until, filing_year=filing_year)
    api_key = os.environ.get("LDA_API_KEY", "").strip() or None
    budget = PagedJsonBudget(
        max_requests=MAX_REQUESTS_PER_PAGE,
        max_page_bytes=16 * 1024 * 1024,
        timeout_seconds=60.0,
        min_request_interval_seconds=KEYED_INTERVAL_SECONDS if api_key else KEYLESS_INTERVAL_SECONDS,
    )
    transport = None
    if evidence is not None:
        evidence.credential = api_key or ""
        evidence.event("selection", stage="lobbying", url=url, max_records=max_records)
        transport = evidence.transport(stage="lobbying-response", max_bytes=budget.max_page_bytes)
    with LdaFilingsReader(budget=budget, api_key=api_key, transport=transport) as reader:
        rows = [_shape(f) for f in _iter_filings(reader, url, max_records=max_records)]
    new_file = output_dir / "_lda_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("LDA: fetched {:,} filings this run", len(rows))

    # 4. Merge prior + new, dedup on filing_uuid preferring the new row.
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
        identity="filing_uuid",
        order_by="dt_posted DESC, filing_uuid",
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=out_file,
    )
    con.close()

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("Lobbying filings: {:,} rows", total)
    return out_file
