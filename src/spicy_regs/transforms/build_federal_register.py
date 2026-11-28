"""Transform: build ``federal_register.parquet`` from the FR REST API.

Produces SpicyDocs' public projection (``FEDERAL_REGISTER_COLUMNS``, all
VARCHAR, through ``project_federal_register_document``) plus the derived
``rin``: one owner for the column list and the value mapping, not a local
copy (consolidation plan B2). ``topics_json`` is the publisher's ``topics``;
a row published before that column existed reads NULL until it is fetched
again, and a full re-read (``since`` at the epoch) refills every row.

Incremental by design, in two steps (ss-vse3). ``build_federal_register``
reads the documents published since the prior table's newest
``publication_date`` minus ``OVERLAP_DAYS`` (the FR epoch with no prior) and
keeps one row per ``(document_number, publication_date)``: a document read
twice is one observation, and conflicting observations within one run refuse
publication rather than choose a row. ``merge_federal_register`` maps them
(``regulations_shape``) into the published table the captured index pins: a
fresh row replaces the row of its dated key, so corrections republished under
the same number on another date stay distinct rows, and every other row is
carried as published. A carried row keeps the mapping of the run that last read
it, except ``rin``, which the merge derives on every row; any other mapping
change reaches old rows only through a full re-read (``FR_SINCE=1994-01-01``).
A document the mapping refuses fails the run: there is no receipt to refuse it
into.

``rin`` is the one column not fetched: the first Regulation Identifier Number
in ``regulation_id_numbers_json``, retained as a compatibility projection — an
empty array and NULL both give NULL — while a consumer wanting every RIN of a
multi-RIN document unnests the array (see docs/regulatory-rins.md).
``modify_date`` is not exposed by the REST API, so fresh rows carry NULL for it
while carried rows keep whatever the prior table had.

``source_sha256`` is the row's pointer to its evidence: the SHA-256 of the
retained API page it was last read from, spelled as the run's journal spells
it (``sha256:<hex>``; the bytes are ``source-evidence/blobs/sha256/<hex>``).
A row no run has read since the column was added holds NULL.
"""

from __future__ import annotations

import hashlib
import math
import time
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.transforms.regulations_shape import ROW_GROUP_ROWS
from spicy_regs.transforms.table_merge import published_prior

OUTPUT = "federal_register.parquet"

#: The column naming the retained API page a row was last read from; appended after the contract's columns.
POINTER = "source_sha256"

#: Number of projected rows buffered per Parquet batch while streaming observations to disk,
#: keeping peak RSS bounded well below hosted runner limits on multi-year backfills.
STREAM_BATCH_ROWS = 10_000


# The oldest documents the API serves. A backfill with no prior table starts here.
FR_EPOCH = date(1994, 1, 1)

# Re-scan this many days before the last stored publication_date on each run, so documents added or corrected after
# their nominal publication date are picked up, and so regulations_dot_gov_comments_count is the Register's latest
# relay rather than its publication-week one: the Register re-checks regulations.gov until about the comment period's
# close. Measured 2026-10-03 (round6/impl-W/m2/overlap_days.out): since 2016 a period closes a median 32 days after
# publication (90th percentile 62, 99th about 91; 487 of 96,442 later than 120); of the March 2024 documents the
# Register relayed a count for, 64 of 325 were last checked within 7 days of publication and 269 within 120. About
# 12 more keyless requests a day.
OVERLAP_DAYS = 120


def build_federal_register_layer1(manifest: Path, archive_root: Path, output_dir: Path, *, max_records: int = 10000) -> Path:
    """Build a bounded typed source slice offline; never reads or publishes R2.

    The manifest selects retained pages by URL and digest. The maintained
    SpicyDocs traversal validates their field request, dates and inventory.
    This explicit candidate route is separate from the scheduled public table.
    """
    from spicy_regs.transforms.federal_register_layer1 import build

    return build(manifest, archive_root, output_dir, max_records=max_records)


def layer1_main() -> None:
    """Installed offline entry point for a retained Federal Register slice."""
    import argparse

    parser = argparse.ArgumentParser(description="Build a typed Federal Register slice from retained originals only")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--archive-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--max-records", type=int, default=10000)
    args = parser.parse_args()
    build_federal_register_layer1(args.manifest, args.archive_root, args.output_dir, max_records=args.max_records)


def published_columns() -> tuple[str, ...]:
    """The built width: SpicyDocs' ``federal_register`` contract (the projection plus the derived ``rin``), then
    this builder's page pointer.

    A function, not a constant, because base installs import this module
    without the source-readers extra that carries SpicyDocs.
    """
    from spicy_docs.schemas.federal_register import FEDERAL_REGISTER

    return (*FEDERAL_REGISTER.columns, POINTER)

#: ``rin`` as a projection of the array, in each shape the table takes: the night's text rows and the published
#: table's native list, where the merge derives it for every row. ``[]`` and NULL both give NULL: a join key is
#: present or it is not.
_RIN_SQL = "json_extract_string(regulation_id_numbers_json, '$[0]')"
_PUBLISHED_RIN_SQL = "regulation_id_numbers[1]"


def _prior_max_publication_date(prior_file: Path) -> date | None:
    """Largest ``publication_date`` in the prior table, or None if empty/absent."""
    if not prior_file.exists():
        return None
    import duckdb

    row = duckdb.sql(f"SELECT max(publication_date) FROM read_parquet('{prior_file}')").fetchone()
    if not row or row[0] is None:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


@contextmanager
def _federal_register_evidence(output_dir: Path, evidence):
    """Standalone acquisition owns its journal; scheduled builds use their run's owner."""
    from spicy_regs.source_evidence import CaptureEvidence

    owned = evidence is None
    evidence = evidence if evidence is not None else CaptureEvidence(output_dir, "federal-register")
    try:
        yield evidence
    except BaseException as error:
        if owned:
            evidence.finish(error)
        raise
    else:
        if owned:
            evidence.finish()


def _coerce_iso_date(value: date | str | None, *, name: str) -> date | None:
    """Accept ``date`` or strict ``YYYY-MM-DD`` text (or ``None``); reject ``datetime`` and non-ISO spellings."""
    if value is None:
        return None
    if type(value) is date:
        return value
    if isinstance(value, str):
        raw = value.strip()
        try:
            parsed = date.fromisoformat(raw)
        except ValueError as exc:
            raise ValueError(f"{name} must be YYYY-MM-DD, got {value!r}") from exc
        if parsed.isoformat() != raw:
            raise ValueError(f"{name} must be YYYY-MM-DD, got {value!r}")
        return parsed
    raise ValueError(f"{name} must be a date or YYYY-MM-DD string, got {value!r}")


def _validate_date_window(since: date, until: date | None) -> None:
    """Refuse an inverted ``[since, until]`` window before any network or disk work."""
    if until is not None and since > until:
        raise ValueError(
            f"Federal Register date window is reversed: since ({since.isoformat()}) > until ({until.isoformat()})"
        )


def _validate_request_interval(request_interval_seconds: float) -> float:
    """Validate optional inter-request pacing in seconds (non-negative finite float)."""
    if isinstance(request_interval_seconds, bool) or not isinstance(request_interval_seconds, (int, float)):
        raise ValueError(
            f"request_interval_seconds must be a non-negative finite number, got {request_interval_seconds!r}"
        )
    value = float(request_interval_seconds)
    if not math.isfinite(value) or value < 0:
        raise ValueError(
            f"request_interval_seconds must be a non-negative finite number, got {request_interval_seconds!r}"
        )
    return value


def _validate_batch_size(batch_size: int) -> int:
    """Validate the streaming Parquet batch size."""
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size!r}")
    return batch_size


def _live_federal_register_documents(
    since: date | str,
    *,
    until: date | str | None = None,
    request_interval_seconds: float = 0.0,
    output_dir: Path,
    evidence=None,
    sleep: Callable[[float], None] | None = None,
    monotonic: Callable[[], float] | None = None,
) -> Iterator[tuple[str, dict]]:
    """Yield ``(page SHA-256, raw FR document)`` for documents published in ``[since, until or today]`` through SpicyDocs.

    The owner acquires exact API pages with its cap-safe date-window traversal:
    a window whose ``count`` reaches the 10,000-result cap is split, and a
    single day that still reads capped refuses the run rather than publish a
    hole. Pages the owner declares included carry the raw API results unchanged;
    capped probe pages carry none (their halves do), so those are skipped here.

    One traversal: the merge below deduplicates repeated observations and
    refuses conflicting ones itself, so the owner's two-traversal
    reconciliation would double the request budget for a contract this
    transform already checks. When ``request_interval_seconds > 0``, consecutive
    API requests are paced politely without sleeping before the first request.
    """
    since_date = _coerce_iso_date(since, name="since")
    assert since_date is not None
    until_date = _coerce_iso_date(until, name="until")
    interval = _validate_request_interval(request_interval_seconds)
    published_through = until_date if until_date is not None else date.today()
    _validate_date_window(since_date, published_through)
    sleep_fn = time.sleep if sleep is None else sleep
    monotonic_fn = time.monotonic if monotonic is None else monotonic

    try:
        from spicy_docs.sources.federal_register import native as federal_register
        from spicy_docs.transport.acquisition import federal_register_fetcher
    except ModuleNotFoundError as error:
        if error.name == "spicy_docs":
            raise RuntimeError(
                "The Federal Register acquisition requires spicy-regs[source-readers]. "
                "Run `uv sync --frozen` in a SpicyRegs checkout."
            ) from None
        raise
    scope = {
        "publishedFrom": since_date.isoformat(),
        "publishedThrough": published_through.isoformat(),
    }
    with _federal_register_evidence(output_dir, evidence) as evidence, federal_register_fetcher(None) as fetch:
        last_request_at: float | None = None

        def retained_fetch(url):
            nonlocal last_request_at
            if interval > 0 and last_request_at is not None:
                remaining = interval - (monotonic_fn() - last_request_at)
                if remaining > 0:
                    sleep_fn(remaining)
            try:
                body = fetch(url)
            finally:
                last_request_at = monotonic_fn()
            # The fetcher exposes only request URL and bytes. Preserve the exact
            # response before parsing; final URL and HTTP capture time stay unknown.
            evidence.retain_bytes(body, raw_source=True, stage="api-page", source_url=url, requested_url=url,
                                  covered_period={"requested_from": scope["publishedFrom"],
                                                  "requested_through": scope["publishedThrough"]})
            return body

        window = None
        pages = []
        for page in federal_register.iter_federal_register_pages(retained_fetch, query_scope=scope, traversals=1):
            response = federal_register.parse_page_response(page.response_bytes)
            digest = "sha256:" + hashlib.sha256(page.response_bytes).hexdigest()
            evidence.declare_used_inputs(
                [{"location": page.request_key, "sha256": digest,
                  "bytes": len(page.response_bytes), "asset_kind": "json-body"}],
                scope_id=f"{evidence.family}:successful-source-files", complete=False,
            )
            # A later page's key is the source's next_page_url, which is not a window request and does not parse as one.
            if page.window_page_index == 0:
                window = federal_register.federal_register_request_window(page.request_key)
            included = federal_register.federal_register_records_included(response, query_scope=scope,
                                                                          page_window=window)
            # Each page by the digest its rows point at (source_sha256); a capped probe's records are its halves'.
            pages.append({"sha256": digest, "records": len(response["results"]) if included else 0})
            if not included:
                continue
            for document in response["results"]:
                yield digest, document
        evidence.event("federal-register-pages", requested_from=scope["publishedFrom"],
                       requested_through=scope["publishedThrough"], pages=pages)


def build_federal_register(
    output_dir: Path,
    *,
    since: date | str = FR_EPOCH,
    until: date | str | None = None,
    request_interval_seconds: float = 0.0,
    batch_size: int = STREAM_BATCH_ROWS,
    documents: Callable[[date], Iterable[dict]] | None = None,
    evidence=None,
) -> Path:
    """This run's documents published in ``[since, until or today]``, projected, checked, one per dated key.

    ``documents`` is the raw API records published since a date; the default is SpicyDocs' Federal Register
    acquisition, and a hermetic test passes its own. Only the default names a retained page, so only its rows carry
    ``source_sha256``. Incoming rows stream into ``_fr_new.parquet`` in ``batch_size``-row Parquet batches so peak
    RAM stays bounded even across multi-year backfill windows. The prior table is merged in ``merge_federal_register``,
    at the published columns.
    """
    import duckdb
    from spicy_docs.schemas.federal_register import FEDERAL_REGISTER_COLUMNS, project_federal_register_document

    since_date = _coerce_iso_date(since, name="since")
    assert since_date is not None
    until_date = _coerce_iso_date(until, name="until")
    _validate_date_window(since_date, until_date)
    interval = _validate_request_interval(request_interval_seconds)
    chunk_rows = _validate_batch_size(batch_size)

    out_file = output_dir / OUTPUT
    schema = pa.schema([(c, pa.string()) for c in (*FEDERAL_REGISTER_COLUMNS, POINTER)])
    if until_date is not None:
        logger.info("FR: fetching documents published from {} through {}", since_date, until_date)
    else:
        logger.info("FR: fetching documents published since {}", since_date)
    if documents is not None:
        projected_rows: Iterator[dict] = (project_federal_register_document(doc) for doc in documents(since_date))
    else:
        projected_rows = (
            project_federal_register_document(doc) | {POINTER: page}
            for page, doc in _live_federal_register_documents(
                since_date,
                until=until_date,
                request_interval_seconds=interval,
                output_dir=output_dir,
                evidence=evidence,
            )
        )
    new_file = output_dir / "_fr_new.parquet"
    fetched = 0
    batch: list[dict] = []
    try:
        with pq.ParquetWriter(new_file, schema, compression="zstd") as writer:
            for row in projected_rows:
                batch.append(row)
                fetched += 1
                if len(batch) >= chunk_rows:
                    writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                    batch.clear()
            if batch:
                writer.write_table(pa.Table.from_pylist(batch, schema=schema))
                batch.clear()
            elif fetched == 0:
                writer.write_table(schema.empty_table())
    except BaseException:
        new_file.unlink(missing_ok=True)
        raise
    logger.info("FR: fetched {:,} documents this run", fetched)

    # The SpicyDocs public-table primary key (projection 1.1). The columns and their literal values stay unchanged;
    # this is not an IRI normalization or an assertion that two dated printings are one matter.
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{spill_dir}'")
    # The pointer rides with each row but is no part of the record: one document read from two pages is one
    # observation, so the conflict check below compares the projection alone.
    cols = ", ".join(FEDERAL_REGISTER_COLUMNS)
    merged_file = output_dir / "_fr_merged.parquet"
    try:
        con.execute(f"CREATE TEMP TABLE _fresh AS SELECT {cols}, {POINTER}, file_row_number AS _row "
                    f"FROM read_parquet('{new_file}', file_row_number=true)")
        invalid = con.execute(
            """SELECT document_number, publication_date FROM _fresh
                WHERE document_number IS NULL OR trim(document_number) = ''
                   OR publication_date IS NULL
                   OR try_cast(publication_date AS DATE) IS NULL
                   OR cast(try_cast(publication_date AS DATE) AS VARCHAR) <> publication_date
                LIMIT 1"""
        ).fetchone()
        if invalid:
            raise ValueError(f"Federal Register record lacks a complete canonical identity: {invalid!r}")
        conflict = con.execute(
            f"""SELECT document_number, publication_date FROM _fresh
                GROUP BY document_number, publication_date
                HAVING count(DISTINCT ({cols})) > 1 LIMIT 1"""
        ).fetchone()
        if conflict:
            raise ValueError(f"Federal Register conflicting observations of the same number/date: {conflict!r}")
        # Written in the contract's published order; SpicyDocs 0.66.6 lists ``rin`` before its newest fields.
        published = published_columns()
        if sorted(published) != sorted((*FEDERAL_REGISTER_COLUMNS, "rin", POINTER)):
            raise ValueError("Federal Register contract differs from its projection plus the derived rin")
        selected = ", ".join(f"{_RIN_SQL} AS rin" if column == "rin" else column for column in published)
        # One row per dated key, the first page's: a document on two pages is one observation (checked above).
        con.execute(
            f"""
        COPY (
            SELECT {selected} FROM _fresh
            QUALIFY row_number() OVER (PARTITION BY document_number, publication_date ORDER BY _row) = 1
            ORDER BY publication_date DESC, document_number
        ) TO '{merged_file}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 50000);
            """
        )
        merged_file.replace(out_file)
    except BaseException:
        merged_file.unlink(missing_ok=True)
        raise
    finally:
        con.close()
        new_file.unlink(missing_ok=True)
    logger.info("Federal Register: {:,} documents observed", pq.ParquetFile(out_file).metadata.num_rows)
    return out_file


def merge_federal_register(
    output_dir: Path,
    *,
    since: date | str | None = None,
    until: date | str | None = None,
    request_interval_seconds: float = 0.0,
    batch_size: int = STREAM_BATCH_ROWS,
    documents: Callable[[date], Iterable[dict]] | None = None,
    download_prior: Callable[[str, Path], bool] = published_prior,
    evidence=None,
) -> Path:
    """The published table: this run's observations, mapped once, over the pinned prior public table (ss-vse3).

    ``build_federal_register`` reads and checks the run's documents; they are mapped to the published columns
    here (``regulations_shape``) and replace the prior table's rows of the same ``(document_number,
    publication_date)``. Every other prior row (including rows outside ``[since, until]``) is carried as published,
    never read back through a receipt or rewritten one row at a time: on 2026-11-09 that restore and rewrite were
    1,542 of the job's 1,764 s for 233 changed rows. ``rin`` is derived again on every row. Order is the table's:
    date descending, then number; row groups stay at ``ROW_GROUP_ROWS``.

    A prior without one of the columns that left receipts (``PROMOTED_LAST``) is refused: carried, its rows would
    publish NULL for values only its receipts held. A prior that differs otherwise (a column the contract added or
    dropped since, a type it changed) is read back to the text shape and mapped as the night's rows are, so a column
    it lacks reads NULL until a re-read fills it. The merge is checked by count against the prior as published, before
    any re-map: one row per key and every prior key kept. The counts, and whether a re-map ran and which columns it
    added, dropped or retyped, go to the run's journal beside the pages it read.
    """
    import duckdb

    from spicy_regs.transforms.regulations_shape import (
        IDENTITIES, PROMOTED_LAST, mapped_table, published_as_source, subject_schema,
    )

    since_date = _coerce_iso_date(since, name="since")
    until_date = _coerce_iso_date(until, name="until")
    if since_date is not None:
        _validate_date_window(since_date, until_date)
    interval = _validate_request_interval(request_interval_seconds)
    chunk_rows = _validate_batch_size(batch_size)

    schema = subject_schema("federal_register")
    key = ", ".join(IDENTITIES["federal_register"])
    prior, out_file = output_dir / "_fr_published.parquet", output_dir / OUTPUT
    scratch = [prior, output_dir / "_fr_prior_text.parquet", output_dir / "_fr_prior.parquet",
               output_dir / "_fr_observed.parquet", output_dir / "_fr_fresh.parquet"]
    existed_prior = prior.exists()
    downloaded_prior = False if existed_prior else bool(download_prior(OUTPUT, prior))
    have_prior = existed_prior or downloaded_prior
    if not have_prior and out_file.exists():
        # Validate implicit since <= until before moving a local output file onto the prior scratch path.
        if since_date is None and until_date is not None:
            local_max = _prior_max_publication_date(out_file)
            _validate_date_window((local_max - timedelta(days=OVERLAP_DAYS)) if local_max else FR_EPOCH, until_date)
        # A local run beside the table it last built: that table is its prior.
        out_file.replace(prior)
        have_prior = True
    if since_date is None:
        prior_max = _prior_max_publication_date(prior) if have_prior else None
        since_date = (prior_max - timedelta(days=OVERLAP_DAYS)) if prior_max else FR_EPOCH
    try:
        _validate_date_window(since_date, until_date)
    except BaseException:
        if downloaded_prior:
            prior.unlink(missing_ok=True)
        raise
    # Counted as published, before any re-map, so a re-map that loses or repeats a row fails the check below.
    prior_rows = pq.ParquetFile(prior).metadata.num_rows if have_prior else 0
    remap: dict[str, list[str]] = {}
    if have_prior and not (held := pq.read_schema(prior)).equals(schema, check_metadata=False):
        if missing := [name for name in PROMOTED_LAST["federal_register"] if name not in held.names]:
            raise ValueError(
                f"The published federal_register lacks {missing}, which a generation before 2026-11-10 held only in "
                "its receipts: carried, its rows would publish NULL for them. Point the family back at a generation "
                "that has them (publication.restore_family; every one since 2026-11-10 does) before merging at the "
                "public table")
        # Journaled, so a column the contract renames leaves a trace: one added, one dropped.
        remap = {"columns_added": [name for name in schema.names if name not in held.names],
                 "columns_dropped": [name for name in held.names if name not in schema.names],
                 "columns_retyped": [field.name for field in schema if field.name in held.names
                                     and not held.field(field.name).type.equals(field.type)]}
        logger.info("FR: the prior differs from the published columns ({}); mapping its {:,} rows again", remap,
                    prior_rows)
        prior = mapped_table("federal_register", published_as_source("federal_register", prior, scratch[1]),
                             scratch[2])
    # The night's observations alone (no prior to merge), moved off the table's name before the merge writes it.
    observed = build_federal_register(
        output_dir,
        since=since_date,
        until=until_date,
        request_interval_seconds=interval,
        batch_size=chunk_rows,
        documents=documents,
        evidence=evidence,
    ).replace(scratch[3])
    fresh = mapped_table("federal_register", observed, scratch[4])
    read_rows = pq.ParquetFile(fresh).metadata.num_rows
    names = ", ".join(f"{_PUBLISHED_RIN_SQL} AS rin" if name == "rin" else f'"{name}"' for name in schema.names)
    merged = output_dir / "_fr_merged.parquet"
    spill_dir = output_dir / ".duckdb_tmp"
    spill_dir.mkdir(exist_ok=True)
    with duckdb.connect() as con:
        con.execute("SET memory_limit='4GB'")
        con.execute("SET threads=2")
        con.execute(f"SET temp_directory='{spill_dir}'")
        con.execute(f"CREATE VIEW fresh AS SELECT * FROM read_parquet('{fresh}')")
        carried = (f" UNION ALL BY NAME SELECT * FROM read_parquet('{prior}') ANTI JOIN fresh USING ({key})"
                   if have_prior else "")
        rows = con.execute(f"SELECT {names} FROM (SELECT * FROM fresh{carried}) "
                           "ORDER BY publication_date DESC, document_number")
        with pq.ParquetWriter(merged, schema, compression="zstd") as writer:
            for batch in rows.to_arrow_reader(50_000):
                writer.write_table(pa.Table.from_batches([batch]).cast(schema), row_group_size=ROW_GROUP_ROWS)
        [(total, keys)] = con.execute(f"SELECT count(*), count(DISTINCT ({key})) FROM read_parquet('{merged}')"
                                      ).fetchall()
        [(added,)] = (con.execute(f"SELECT count(*) FROM fresh ANTI JOIN read_parquet('{prior}') USING ({key})")
                      .fetchall() if have_prior else [(read_rows,)])
    counts = {"prior_rows": prior_rows, "read_rows": read_rows, "new_rows": added, "rows": total}
    if total != keys or total != prior_rows + added:
        raise ValueError(f"Federal Register merge lost or repeated a record: {counts}, {keys} keys")
    merged.replace(out_file)
    for path in scratch:
        path.unlink(missing_ok=True)
    if evidence is not None:
        evidence.event("federal-register-merge", **counts, remapped=bool(remap), **remap)
    logger.info("Federal Register: {:,} rows ({:,} read, {:,} new)", total, read_rows, added)
    return out_file

