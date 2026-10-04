"""Transform: build ``lobbying_filings.parquet`` from the Senate LDA REST API.

Produces native domain tables and separate ETL receipts. ``lobbying_filings`` is keyed by
``filing_uuid``, with ordered native activity and entity arrays.
``lobbying_activities`` holds one row per activity a filing reports, keyed
``(filing_uuid, activity_index)``. ``lobbying_activity_lobbyists`` holds one row
per lobbyist an activity names, keyed ``(filing_uuid, activity_index,
lobbyist_index)`` (owner decision 47). An index is the position in the filing's
own list: LDA never edits a filing, since an amendment is a new filing, so the
positions are stable. The lobbyists were dropped before, so history is read
once, with the shape that keeps them.

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

from spicy_regs.transforms.parquet_rows import str_or_none
from spicy_regs.sources import r2
from spicy_regs.transforms.government_receipts import internal_prior, receipt_builder
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


# Literal processing columns used by the source mapper and incremental merge.
# The receipt writer converts the final output to the native subject schema.
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

ACTIVITIES_OUTPUT = "lobbying_activities.parquet"
ACTIVITY_COLUMNS = (
    "filing_uuid",
    "activity_index",
    "general_issue_code",
    "general_issue_code_display",
    "description",
    "foreign_entity_issues",
    "government_entities_json",
)
LOBBYISTS_OUTPUT = "lobbying_activity_lobbyists.parquet"
LOBBYIST_COLUMNS = (
    "filing_uuid",
    "activity_index",
    "lobbyist_index",
    "lobbyist_id",
    "prefix",
    "first_name",
    "nickname",
    "middle_name",
    "last_name",
    "suffix",
    "covered_position",
    "new",
)



def _source_list(value: object, field: str) -> list | None:
    """Keep null, empty and positional entries; refuse unsupported source shapes."""
    if value is None:
        return None
    if not isinstance(value, list) or any(item is not None and not isinstance(item, dict) for item in value):
        raise LobbyingFilingsError(f"{field} must be an array of objects or nulls")
    return value


def _entities(activity: dict) -> list | None:
    values = _source_list(activity.get("government_entities"), "government_entities")
    return None if values is None else [None if value is None else {"id": value.get("id"), "name": value.get("name")} for value in values]


def _activities(filing: dict) -> list | None:
    """Keep activity descriptions at their source positions, including nulls."""
    values = _source_list(filing.get("lobbying_activities"), "lobbying_activities")
    return None if values is None else [None if act is None else {
        "general_issue_code": act.get("general_issue_code"),
        "general_issue_code_display": act.get("general_issue_code_display"),
        "description": act.get("description"),
    } for act in values]


def _government_entities(filing: dict) -> list | None:
    """Known entity occurrences in activity order; child rows retain per-activity null lists."""
    activities = _source_list(filing.get("lobbying_activities"), "lobbying_activities")
    if activities is None:
        return None
    return [entity for act in activities if act is not None for entity in (_entities(act) or [])]


def _shape(filing: dict) -> dict:
    """Map one raw LDA filing onto the published column shape."""
    registrant = filing.get("registrant") or {}
    client = filing.get("client") or {}
    return {
        "filing_uuid": filing.get("filing_uuid"),
        "filing_type": filing.get("filing_type"),
        "filing_year": str_or_none(filing.get("filing_year")),
        "filing_period": filing.get("filing_period"),
        "dt_posted": filing.get("dt_posted"),
        "registrant_name": registrant.get("name") if isinstance(registrant, dict) else None,
        "registrant_id": str_or_none(registrant.get("id")) if isinstance(registrant, dict) else None,
        "client_name": client.get("name") if isinstance(client, dict) else None,
        "client_id": str_or_none(client.get("id") or client.get("client_id")) if isinstance(client, dict) else None,
        "income": filing.get("income"),
        "expenses": filing.get("expenses"),
        "lobbying_activities_json": json.dumps(_activities(filing)),
        "government_entities_json": json.dumps(_government_entities(filing)),
        "url": filing.get("filing_document_url"),
    }


def _activity_rows(filing: dict) -> list[dict]:
    """One row per activity the filing reports, at its position in the filing's own list."""
    rows = []
    for index, act in enumerate(_source_list(filing.get("lobbying_activities"), "lobbying_activities") or []):
        act = act or {}
        entities = _entities(act)
        rows.append({
            "filing_uuid": filing.get("filing_uuid"),
            "activity_index": str(index),
            "general_issue_code": act.get("general_issue_code"),
            "general_issue_code_display": act.get("general_issue_code_display"),
            "description": act.get("description"),
            "foreign_entity_issues": act.get("foreign_entity_issues"),
            "government_entities_json": json.dumps(entities),
        })
    return rows


def _lobbyist_rows(filing: dict) -> list[dict]:
    """One row per lobbyist an activity names, at its positions in the filing's lists."""
    rows = []
    for index, act in enumerate(_source_list(filing.get("lobbying_activities"), "lobbying_activities") or []):
        act = act or {}
        for position, entry in enumerate(_source_list(act.get("lobbyists"), "lobbyists") or []):
            entry = entry or {}
            person = entry.get("lobbyist")
            if not isinstance(person, dict):
                person = {}
            rows.append({
                "filing_uuid": filing.get("filing_uuid"),
                "activity_index": str(index),
                "lobbyist_index": str(position),
                "lobbyist_id": str_or_none(person.get("id")),
                "prefix": person.get("prefix"),
                "first_name": person.get("first_name"),
                "nickname": person.get("nickname"),
                "middle_name": person.get("middle_name"),
                "last_name": person.get("last_name"),
                "suffix": person.get("suffix"),
                "covered_position": entry.get("covered_position"),
                "new": str_or_none(entry.get("new")),
            })
    return rows


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


@receipt_builder
def build_lobbying_filings(
    output_dir: Path,
    *,
    evidence: CaptureEvidence | None = None,
    since: date | None = None,
    until: date | None = None,
    filing_year: int | None = None,
    max_records: int | None = None,
) -> tuple[Path, Path, Path]:
    """Build the filings, activities and lobbyists tables, each merged with its prior table."""
    import duckdb

    prior_file = output_dir / "_lda_prior.parquet"

    # 1. Pull the prior table (best effort — absence just means full backfill).
    have_prior = prior_file.exists() or r2.download(OUTPUT, prior_file)
    if have_prior:
        prior_file = internal_prior("lobbying_filings", prior_file)
    if have_prior:
        logger.info("LDA: merging against prior table {}", prior_file)
    else:
        logger.info("LDA: no prior table found — full backfill")

    # 2. Decide the fetch window start. A filing year is read whole: the
    # posted-date watermark would narrow a history year to its last days.
    if since is None and filing_year is None:
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
        filings = list(_iter_filings(reader, url, max_records=max_records))
    fresh = {
        OUTPUT: (COLUMNS, [_shape(f) for f in filings]),
        ACTIVITIES_OUTPUT: (ACTIVITY_COLUMNS, [row for f in filings for row in _activity_rows(f)]),
        LOBBYISTS_OUTPUT: (LOBBYIST_COLUMNS, [row for f in filings for row in _lobbyist_rows(f)]),
    }
    logger.info("LDA: fetched {:,} filings ({:,} activities, {:,} lobbyist rows) this run", len(filings),
                len(fresh[ACTIVITIES_OUTPUT][1]), len(fresh[LOBBYISTS_OUTPUT][1]))

    # 4. Merge each table's prior + new, dedup on its identity preferring the new row.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    # The prior grows with every backfilled year and the dedup window reads all of it: at 4GB the 2013 run
    # (36356691399, 36362663551) ran out of memory in merge_local_prior. Hosted runners have 16 GB; leave room
    # for Python and the OS, and let DuckDB spill the rest to temp_directory.
    con.execute("SET memory_limit='12GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")
    shapes: dict[str, tuple[str | tuple[str, ...], str]] = {
        OUTPUT: ("filing_uuid", "dt_posted DESC, filing_uuid"),
        ACTIVITIES_OUTPUT: (("filing_uuid", "activity_index"), "filing_uuid, CAST(activity_index AS INTEGER)"),
        LOBBYISTS_OUTPUT: (("filing_uuid", "activity_index", "lobbyist_index"),
                           "filing_uuid, CAST(activity_index AS INTEGER), CAST(lobbyist_index AS INTEGER)"),
    }
    priors: dict[str, tuple[Path, bool]] = {OUTPUT: (prior_file, have_prior)}
    outputs = []
    try:
        for key, (columns, rows) in fresh.items():
            identity, order_by = shapes[key]
            if key in priors:
                table_prior, table_has_prior = priors[key]
            else:
                table_prior = output_dir / f"_{key.removesuffix('.parquet')}_prior.parquet"
                table_has_prior = table_prior.exists() or r2.download(key, table_prior)
                if table_has_prior:
                    table_prior = internal_prior(key.removesuffix(".parquet"), table_prior)
            schema = pa.schema([(c, pa.string()) for c in columns])
            new_file = output_dir / f"_{key.removesuffix('.parquet')}_new.parquet"
            pq.write_table(pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table(), new_file,
                           compression="zstd")
            out = output_dir / key
            merge_local_prior(con, columns=columns, identity=identity, order_by=order_by,
                              prior_file=table_prior if table_has_prior else None, new_file=new_file, out_file=out)
            # Housekeeping: drop scratch files so they aren't mistaken for outputs.
            for scratch in (table_prior, new_file):
                scratch.unlink(missing_ok=True)
            outputs.append(out)
            logger.info("{}: {:,} rows", key, pq.ParquetFile(out).metadata.num_rows)
    finally:
        con.close()
    return outputs[0], outputs[1], outputs[2]
