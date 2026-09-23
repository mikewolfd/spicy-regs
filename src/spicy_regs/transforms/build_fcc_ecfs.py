"""Transforms: build ``fcc_proceedings.parquet`` and ``fcc_filings.parquet``.

The FCC layer of the dataset: proceedings are the FCC's docket equivalent and
filings are its comment equivalent, both ingested from the ECFS public API
through spicy-docs' evidenced page reader
(see :mod:`spicy_docs.sources.fcc_ecfs`). Column conventions match the other
external-source tables — all VARCHAR, array fields serialized as JSON strings.

Proceedings are walked whole every run (see :func:`build_fcc_proceedings`).
Filings are incremental, following the federal_register pattern:

1. Best-effort download the prior parquet from R2.
2. Fetch only filings received since the prior table's max date (minus a short
   overlap to catch late-disseminated / corrected records).
3. Dedup the union on ``id_submission``, preferring the freshly fetched row.

ECFS holds tens of millions of filings, so a filings first run is bounded to
the last :data:`FILINGS_FIRST_RUN_DAYS` days unless an explicit ``since``
(``FCC_SINCE``) is passed. Deeper backfills are expected to run scoped to
specific proceedings (``FCC_PROCEEDINGS``) and/or in date slices.

Every window is pooled over whole walks (spicy-docs ``pool_walks``) until one
walk is clean or the pool holds exactly the count ECFS aggregates for it
(:data:`_COUNTED_BY`); the 2026-09-22 scheduled filings run met an identity
repeated within one window.
"""

from __future__ import annotations

import json
import os
from collections.abc import Hashable, Iterator, Mapping
from datetime import UTC, date, datetime, timedelta
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.sources import r2
from spicy_regs.transforms.table_merge import merge_local_prior

if TYPE_CHECKING:
    import httpx

    from spicy_docs.sources.fcc_ecfs import FccEcfsReader

PROCEEDINGS_OUTPUT = "fcc_proceedings.parquet"
FILINGS_OUTPUT = "fcc_filings.parquet"

# Re-scan this many days before the last stored date on each run, so records
# disseminated or corrected after their nominal date are picked up.
OVERLAP_DAYS = 7

# A filings run with no prior table and no explicit since covers this many
# trailing days instead of walking the (enormous) full archive.
FILINGS_FIRST_RUN_DAYS = 30

PROCEEDING_COLUMNS = (
    "name",
    "id_proceeding",
    "description",
    "bureau_code",
    "bureau_name",
    "rulemaking_or_docket",
    "filing_status",
    "date_created",
    "date_closed",
    "comment_start_date",
    "comment_end_date",
    "reply_comment_start_date",
    "reply_comment_end_date",
    "filed_by",
)
_PROCEEDING_SCHEMA = pa.schema([(c, pa.string()) for c in PROCEEDING_COLUMNS])

FILING_COLUMNS = (
    "id_submission",
    "proceeding_names_json",
    "submission_type",
    "express_comment",
    "date_received",
    "date_submission",
    "date_disseminated",
    "filing_status",
    "viewing_status",
    "exparte_or_late_filed",
    "filers_json",
    "authors_json",
    "lawfirms_json",
    "bureaus_json",
    "text_data",
    "total_page_count",
    "documents_json",
    "filing_url",
)
_FILING_SCHEMA = pa.schema([(c, pa.string()) for c in FILING_COLUMNS])


def _s(value: object) -> str | None:
    """Coerce a scalar to str, preserving NULL (ids/counts/flags come as ints)."""
    if value is None:
        return None
    return str(value)


def _names(entries: list | None) -> list[str]:
    """Extract the ``name`` of each dict in an API list field, dropping empties."""
    out: list[str] = []
    for entry in entries or []:
        if isinstance(entry, dict):
            name = entry.get("name")
            if isinstance(name, str) and name:
                out.append(name)
    return out


def _dict_field(raw: dict, key: str) -> dict:
    """A dict-valued API field, or {} when absent/mistyped."""
    value = raw.get(key)
    return value if isinstance(value, dict) else {}


def _shape_proceeding(raw: dict) -> dict:
    """Map one raw ECFS proceeding onto the published column shape."""
    # The proceedings endpoint nests the bureau as {code, name}; filings embed
    # proceedings with flat bureau_code / bureau_name. Accept both.
    bureau = _dict_field(raw, "bureau")
    return {
        "name": raw.get("name"),
        "id_proceeding": _s(raw.get("id_proceeding")),
        "description": raw.get("description_display") or raw.get("description"),
        "bureau_code": bureau.get("code") or raw.get("bureau_code"),
        "bureau_name": bureau.get("name") or raw.get("bureau_name"),
        "rulemaking_or_docket": raw.get("flag_rulemaking_or_docket"),
        "filing_status": raw.get("filingStatus"),
        "date_created": raw.get("date_proceeding_created"),
        "date_closed": raw.get("date_closed"),
        "comment_start_date": raw.get("comment_start_date"),
        "comment_end_date": raw.get("comment_end_date"),
        "reply_comment_start_date": raw.get("comment_reply_start_date"),
        "reply_comment_end_date": raw.get("comment_reply_end_date"),
        "filed_by": raw.get("filed_by"),
    }


def _shape_filing(raw: dict) -> dict:
    """Map one raw ECFS filing onto the published column shape."""
    id_submission = _s(raw.get("id_submission"))
    submissiontype = _dict_field(raw, "submissiontype")
    filingstatus = _dict_field(raw, "filingstatus")
    viewingstatus = _dict_field(raw, "viewingstatus")
    raw_documents = raw.get("documents")
    documents = raw_documents if isinstance(raw_documents, list) else []
    return {
        "id_submission": id_submission,
        "proceeding_names_json": json.dumps(_names(raw.get("proceedings"))),
        "submission_type": submissiontype.get("description"),
        "express_comment": _s(raw.get("express_comment")),
        "date_received": raw.get("date_received"),
        "date_submission": raw.get("date_submission"),
        "date_disseminated": raw.get("date_disseminated"),
        "filing_status": filingstatus.get("description"),
        "viewing_status": viewingstatus.get("description"),
        "exparte_or_late_filed": raw.get("exparte_or_late_filed"),
        "filers_json": json.dumps(_names(raw.get("filers"))),
        "authors_json": json.dumps(_names(raw.get("authors"))),
        "lawfirms_json": json.dumps(_names(raw.get("lawfirms"))),
        "bureaus_json": json.dumps(_names(raw.get("bureaus"))),
        "text_data": raw.get("text_data"),
        "total_page_count": _s(raw.get("total_page_count")),
        "documents_json": json.dumps(
            [
                {"filename": d.get("filename"), "src": d.get("src")}
                for d in documents
                if isinstance(d, dict) and (d.get("filename") or d.get("src"))
            ]
        ),
        "filing_url": f"https://www.fcc.gov/ecfs/filing/{id_submission}" if id_submission else None,
    }


# -- source fetch ---------------------------------------------------------------
# spicy-docs owns the ECFS transport, offset walk, credential header and
# inclusive-day URL bounds (:mod:`spicy_docs.sources.fcc_ecfs`). The walk below
# is this host's window policy on top of it: spans whose reach would pass the
# publisher's 10,000-record result ceiling bisect on their dates, and a single
# day that still hits the ceiling refuses, because two sorted slices cannot
# prove coverage when source timestamps tie. Source-confirmed empty windows are
# valid.

ECFS_EPOCH = date(1990, 1, 1)
API_KEY_ENV_VARS = ("API_GOV", "DATA_GOV_API_KEY", "FCC_API_KEY", "REGULATIONS_GOV_API_KEY")
PER_PAGE = 250
MAX_RESULT_WINDOW = 10_000
_MAX_REQUESTS_PER_PAGE = 5

#: ECFS states no total, but every response, empty ones included, carries Elasticsearch term
#: aggregations over the whole query. One field's buckets plus ``sum_other_doc_count`` count the
#: records carrying that field; a record without it is counted as observed. Measured 2026-09-23:
#: filings' ``express_comment`` summed to all 5,137 filings received 2026-08-23..09-22, and
#: proceedings' ``bureau_name`` to every proceeding walked but the one with no bureau.
_COUNTED_BY = {"proceedings": "bureau_name", "filings": "express_comment"}


class FccEcfsError(ValueError):
    """The selected date window cannot be completely traversed."""


def _resolve_api_key() -> str | None:
    for name in API_KEY_ENV_VARS:
        value = os.environ.get(name, "").strip()
        if value:
            return value
    return None


def _fetch_fcc(
    endpoint: str,
    *,
    since: date | None = None,
    until: date | None = None,
    api_key: str | None = None,
    per_page: int = PER_PAGE,
    proceedings: tuple[str, ...] = (),
    transport: httpx.BaseTransport | None = None,
) -> Iterator[dict]:
    """Yield raw ECFS records for ``endpoint`` ("proceedings" or "filings").

    ``proceedings`` scopes a filings selection to named proceedings, one full
    window walk each; a filing legitimately shared by two selected proceedings
    is yielded for each, and table merging owns identity deduplication.
    """
    if isinstance(per_page, bool) or not isinstance(per_page, int) or per_page < 1:
        raise ValueError("per_page must be a positive integer")
    since = since or ECFS_EPOCH
    until = until or date.today()
    api_key = api_key if api_key is not None else _resolve_api_key()
    per_page = min(per_page, PER_PAGE)
    if not api_key:
        raise FccEcfsError("ECFS requires an api.data.gov key")
    if since > until:
        raise FccEcfsError("ECFS selection start must not follow its end")
    try:
        from spicy_docs.reading.paged_json import PagedJsonBudget
        from spicy_docs.sources.fcc_ecfs import FccEcfsReader
    except ModuleNotFoundError as error:
        if error.name == "spicy_docs":
            raise RuntimeError(
                "FCC ECFS requires spicy-regs[source-readers]. Run `uv sync --frozen` in a SpicyRegs checkout."
            ) from None
        raise
    if endpoint not in _COUNTED_BY:
        raise ValueError(f"unknown ECFS endpoint {endpoint!r}")
    record_key = "proceeding" if endpoint == "proceedings" else "filing"
    budget = PagedJsonBudget(
        max_requests=_MAX_REQUESTS_PER_PAGE,
        max_page_bytes=16 * 1024 * 1024,
        timeout_seconds=60,
        min_request_interval_seconds=0,
    )
    with FccEcfsReader(budget=budget, api_key=api_key, transport=transport) as reader:
        for name in proceedings or (None,):
            extra = {"proceedings.name": name} if name else {}
            yield from _fetch_window(
                reader,
                endpoint=endpoint,
                record_key=record_key,
                gte=since,
                lte=until,
                per_page=per_page,
                extra=extra,
            )


def _fetch_window(
    reader: FccEcfsReader,
    *,
    endpoint: str,
    record_key: str,
    gte: date,
    lte: date,
    per_page: int,
    extra: dict[str, str],
) -> Iterator[dict]:
    """Yield one window, bisecting a span past the result ceiling; a single over-ceiling day refuses.

    A window within the ceiling is pooled over whole walks until one is clean or the pool
    holds exactly the window's aggregate count (:data:`_COUNTED_BY`, spicy-docs ``pool_walks``).
    Each walk takes the next of :func:`_walk_page_sizes`, so its page boundaries fall on other
    records; a window is within the ceiling only if a walk at the smallest of them can exhaust
    it, so a later, smaller walk never meets the ceiling the first walk did not.
    """
    from spicy_docs.reading.paged_json import WalkPass, pool_walks

    sizes = _walk_page_sizes(per_page)

    def walk(size: int) -> tuple[list[dict], bool, int]:
        return _page_window(
            reader, endpoint=endpoint, record_key=record_key, gte=gte, lte=lte, per_page=size, extra=extra
        )

    records, exhausted, counted = walk(sizes[0])
    if not exhausted or len(records) >= MAX_RESULT_WINDOW // sizes[-1] * sizes[-1]:
        if gte == lte:
            raise FccEcfsError(f"ECFS {endpoint} reaches the result ceiling on {gte}; narrow the selection")
        mid = gte + (lte - gte) // 2
        for start, end in ((gte, mid), (mid + timedelta(days=1), lte)):
            yield from _fetch_window(
                reader,
                endpoint=endpoint,
                record_key=record_key,
                gte=start,
                lte=end,
                per_page=per_page,
                extra=extra,
            )
        return

    def walk_pass(index: int) -> WalkPass:
        if index == 0:
            return WalkPass(tuple(records), counted)
        again, exhausted, count = walk(sizes[index % len(sizes)])
        if not exhausted:
            raise FccEcfsError(f"ECFS {endpoint} {gte}..{lte} grew past the result ceiling between passes")
        return WalkPass(tuple(again), count)

    label = f"ECFS {endpoint} {gte}..{lte}"
    pooled = pool_walks(walk_pass, key=partial(_identity, endpoint), label=label)
    logger.info(
        "{}: {:,} records of {:,} counted, in {} walk(s)", label, len(pooled.records), pooled.declared, pooled.passes
    )
    yield from (dict(record) for record in pooled.records)


def _walk_page_sizes(per_page: int) -> tuple[int, ...]:
    """The page sizes successive walks of one window cycle through: 250 gives 250, 237 and 223.

    A walk that repeats one record and skips another does so at its page boundaries, so a
    second walk with the same boundaries tends to skip the same record. This is the rule
    spicy-docs' Congress reader applies (``CongressListingReader.pooled``), restated because
    spicy-docs does not export it; B7 moves ECFS pooling there, where the two can share it.
    """
    return tuple(dict.fromkeys((per_page, per_page - per_page // 19, per_page - per_page // 9)))


#: Proceeding fields that summarize filing activity. They change between two walks of one window
#: while the document does not, so a key covering them would never settle a pool. Measured
#: 2026-09-23 on 21,691 documents: ``total_filing_count`` and ``recent_filing_count`` on 16,850,
#: ``last_30_days`` on 2,034, and ``total``, ``recent_filings`` and ``days`` (the span
#: ``recent_filings`` counts over) on 1,515; 14-58 carried ``total`` 10,777 beside
#: ``total_filing_count`` 4,642.
_PROCEEDING_ACTIVITY = frozenset(
    {"days", "last_30_days", "recent_filing_count", "recent_filings", "total", "total_filing_count"}
)


def _identity(endpoint: str, record: Mapping[str, Any]) -> Hashable:
    """The record's identity within one window.

    A filing is its ``id_submission``; spicy-docs refuses a missing or blank one. A proceeding
    has no such key: ECFS holds more than one document for some dockets and reuses one
    ``id_proceeding`` across documents (measured 2026-09-23: 553 of 21,138 ids, two of them a
    single docket's two documents, 24-89 and 25-12). So a proceeding is its document without
    the filing-activity counters (:data:`_PROCEEDING_ACTIVITY`), which move as filings arrive
    while the document does not; :func:`build_fcc_proceedings` chooses among a docket's
    documents. An edit to any other field between two walks reads as another document, so the
    pool then overfills and only a clean walk settles that window. Within one walk, a document
    edited mid-walk and served in both versions reads as two documents, so it can fill the slot
    of one the walk skipped and the walk still looks clean; excluding the activity fields
    narrows that to edits of the document itself.
    """
    if endpoint == "proceedings":
        document = {field: value for field, value in record.items() if field not in _PROCEEDING_ACTIVITY}
        return json.dumps(document, sort_keys=True, default=str)
    return record.get("id_submission")


def _carries_counted_field(endpoint: str, record: dict) -> bool:
    """Whether ``record`` carries the field its endpoint's window count is aggregated over."""
    if endpoint == "proceedings":
        return bool(_dict_field(record, "bureau").get("name"))
    return record.get("express_comment") is not None


def _window_count(endpoint: str, page, records: list[dict]) -> int:
    """The window's record count: its aggregate over :data:`_COUNTED_BY` plus the records without that field."""
    field = _COUNTED_BY[endpoint]
    try:
        aggregation = json.loads(page.capture.body)["aggregations"][field]
        counted = sum(bucket["doc_count"] for bucket in aggregation["buckets"]) + aggregation["sum_other_doc_count"]
    except (KeyError, TypeError, ValueError):
        raise FccEcfsError(f"ECFS {endpoint} answered no {field} aggregation to count its window by") from None
    return counted + sum(1 for record in records if not _carries_counted_field(endpoint, record))


def _page_window(
    reader: FccEcfsReader,
    *,
    endpoint: str,
    record_key: str,
    gte: date,
    lte: date,
    per_page: int,
    extra: dict[str, str],
) -> tuple[list[dict], bool, int]:
    """Page one window through the owner's offset walk; return its records, whether it exhausted, and its count."""
    from spicy_docs.reading.paged_json import with_query
    from spicy_docs.sources.fcc_ecfs import filings_url, proceedings_url

    if endpoint == "proceedings":
        url = proceedings_url(
            created_from=gte.isoformat(), created_to=lte.isoformat(), limit=per_page, descending=False
        )
    else:
        url = filings_url(received_from=gte.isoformat(), received_to=lte.isoformat(), limit=per_page, descending=False)
    for key, value in extra.items():
        url = with_query(url, key, value)
    records: list[dict] = []
    while True:
        page = reader.page(url, records_key=record_key)
        records.extend(dict(record) for record in page.records)
        if page.next_url is None:
            return records, True, _window_count(endpoint, page, records)
        if len(records) + per_page > MAX_RESULT_WINDOW:
            return records, False, 0
        url = page.next_url


def _prior_max_date(prior_file: Path, column: str) -> date | None:
    """Largest ``column`` value in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max({column}) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def _merge_incremental(
    output_dir: Path,
    *,
    output: str,
    scratch_prefix: str,
    columns: tuple[str, ...],
    schema: pa.Schema,
    key: str,
    order_by: str,
    rows: list[dict],
    prior_file: Path,
    have_prior: bool,
) -> Path:
    """Union prior + freshly fetched rows, dedup on ``key`` preferring fresh."""
    import duckdb

    out_file = output_dir / output
    new_file = output_dir / f"{scratch_prefix}_new.parquet"
    table = pa.Table.from_pylist(rows, schema=schema) if rows else schema.empty_table()
    pq.write_table(table, new_file, compression="zstd")

    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")

    merge_local_prior(
        con,
        columns=columns,
        identity=key,
        order_by=f"{order_by} DESC, {key}",
        prior_file=prior_file if have_prior else None,
        new_file=new_file,
        out_file=out_file,
    )
    con.close()

    for scratch in (prior_file, new_file):
        scratch.unlink(missing_ok=True)
    return out_file


def _instant(value: object) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value)) if value else None
    except ValueError:
        return None


def _populated(value: object) -> int:
    """How many leaf values a document fills."""
    if isinstance(value, dict):
        return sum(_populated(v) for v in value.values())
    if isinstance(value, list):
        return sum(_populated(v) for v in value)
    return int(value not in (None, ""))


def _docket_documents_in_preference(documents: list[dict]) -> list[dict]:
    """One docket name's ECFS documents, the one to publish first.

    ECFS re-created 13-84, 15-91 and 15-94 on 2026-09-21 as second, sparser documents beside
    the originals, and holds two documents for 21-62, 24-89 and 25-12 (measured 2026-09-23).
    The published row is the original docket (earliest created), then the one ECFS edited
    last, then the fuller document, then the lowest ``id_proceeding``, which picks the original
    or the edited document in every measured case.
    """
    never = datetime.max.replace(tzinfo=UTC)
    ordered = sorted(documents, key=lambda d: str(d.get("id_proceeding")))
    ordered.sort(key=_populated, reverse=True)
    ordered.sort(key=lambda d: _instant(d.get("date_last_modified")) or datetime.min.replace(tzinfo=UTC), reverse=True)
    ordered.sort(key=lambda d: _instant(d.get("date_proceeding_created")) or never)
    return ordered


def build_fcc_proceedings(output_dir: Path) -> Path:
    """Build ``fcc_proceedings.parquet`` from a whole walk of ECFS proceedings, one row per docket name.

    A whole walk (21,691 documents in three windows, measured 2026-09-23) rather than an
    increment: windows on creation date never refresh a proceeding's later closing or status,
    and a window holding only a docket's re-created document would publish it over the
    original. An empty whole walk refuses; a document with no docket name cannot be cited or
    joined and is left out, by ``id_proceeding``, at WARNING.
    """
    out_file = output_dir / PROCEEDINGS_OUTPUT
    logger.info("FCC proceedings: walking every proceeding created since {}", ECFS_EPOCH)
    documents = list(_fetch_fcc("proceedings"))
    if not documents:
        raise FccEcfsError("ECFS answered no proceedings at all; an empty whole walk is not an empty table")
    by_name: dict[str, list[dict]] = {}
    nameless = []
    for document in documents:
        name = document.get("name")
        if isinstance(name, str) and name.strip():
            by_name.setdefault(name, []).append(document)
        else:
            nameless.append(str(document.get("id_proceeding")))
    if nameless:
        logger.warning("FCC proceedings: left out {} documents with no docket name: {}", len(nameless), nameless)
    repeated = sorted(name for name, docs in by_name.items() if len(docs) > 1)
    if repeated:
        logger.info("FCC proceedings: {} docket names hold more than one document: {}", len(repeated), repeated)
    rows = [_shape_proceeding(_docket_documents_in_preference(docs)[0]) for docs in by_name.values()]
    rows.sort(key=lambda row: row["name"])
    rows.sort(key=lambda row: row["date_created"] or "", reverse=True)

    staged = output_dir / f".{PROCEEDINGS_OUTPUT}.partial"
    pq.write_table(
        pa.Table.from_pylist(rows, schema=_PROCEEDING_SCHEMA), staged, compression="zstd", row_group_size=50_000
    )
    staged.replace(out_file)
    logger.info("FCC proceedings: {:,} rows from {:,} documents", len(rows), len(documents))
    return out_file


def build_fcc_filings(
    output_dir: Path,
    *,
    since: date | None = None,
    proceedings: tuple[str, ...] = (),
) -> Path:
    """Build ``fcc_filings.parquet`` (incremental merge with the prior table).

    ``proceedings`` scopes the fetch to specific proceeding names — used for
    targeted backfills of big dockets without walking all of ECFS.
    """
    prior_file = output_dir / "_fcc_filings_prior.parquet"

    have_prior = prior_file.exists() or r2.download(FILINGS_OUTPUT, prior_file)
    if since is None:
        prior_max = _prior_max_date(prior_file, "date_received") if have_prior else None
        if prior_max:
            since = prior_max - timedelta(days=OVERLAP_DAYS)
        else:
            since = date.today() - timedelta(days=FILINGS_FIRST_RUN_DAYS)
            logger.warning(
                "FCC filings: no prior table — bounding the first run to the last {} days "
                "(since {}). Pass FCC_SINCE / FCC_PROCEEDINGS for a deeper, scoped backfill.",
                FILINGS_FIRST_RUN_DAYS,
                since,
            )
    logger.info("FCC filings: fetching filings received since {} (proceedings={})", since, proceedings or "all")

    rows = [_shape_filing(f) for f in _fetch_fcc("filings", since=since, proceedings=proceedings)]
    logger.info("FCC filings: fetched {:,} filings this run", len(rows))

    out = _merge_incremental(
        output_dir,
        output=FILINGS_OUTPUT,
        scratch_prefix="_fcc_filings",
        columns=FILING_COLUMNS,
        schema=_FILING_SCHEMA,
        key="id_submission",
        order_by="date_received",
        rows=rows,
        prior_file=prior_file,
        have_prior=have_prior,
    )
    total = pq.ParquetFile(out).metadata.num_rows
    logger.info("FCC filings: {:,} rows", total)
    return out
