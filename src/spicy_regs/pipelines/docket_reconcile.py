"""Reconcile recently active dockets with Regulations.gov, flagging postings the publisher removed or moved.

The Mirrulations mirror only adds captures, so a document the publisher removes stays held and unflagged, and its
comments can appear twice once the publisher re-posts them under the document's new id. FNA-2026-0301-0004, the
Utah notice filed to the Idaho docket, answered 404 on 2026-10-03 while the mirror kept it and its four comments
beside their re-posts in FNA-2026-0313 (receipt ``mcp-chaos-2026-10-02/round6/audit-xiomara.md``).

Each run lists the documents of every docket with a held document posted or modified in the last ``WINDOW_DAYS``,
through the keyed API reader ``fill-docket-gaps`` uses, and compares the listing with what the documents working copy
holds. A held document the listing omits is read once by id: 404 or 410 records it ``removed``, a served one
``listed``. Every answer is kept in ``docket_reconcile_outcomes.parquet``, one row per document, which the documents
family joins as ``publisher_status`` and ``removed_observed_at`` (:func:`with_publisher_status`). Nothing is deleted.

- A run makes at most ``max_requests`` keyed requests and stops before the next; the dockets it did not reach lead
  the next day's run. A docket is listed at most once a UTC day, the longest-unreconciled first.
- A listing the publisher cuts at its 40-page cap never implies removal: only the documents it names are recorded.
- A document read by id is not read again for ``RETRY_AFTER``; the listing's silence meanwhile leaves its answer.
- A transport failure, a 429 or a refusal answers nothing: the docket in flight records nothing, the dockets done
  before it are kept, and the step fails so the run shows it.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any

import duckdb
import polars as pl
from cyclopts import App, Parameter
from dotenv import load_dotenv
from loguru import logger

from spicy_regs.pipelines.keyed_regulations import keyed_reader, load_outcomes, save_outcomes
from spicy_regs.schemas.regulations import DOCUMENT_FAMILY_COLUMNS
from spicy_regs.sources import r2

OUTCOMES = "docket_reconcile_outcomes.parquet"
OUTCOME_SCHEMA = {"document_id": pl.Utf8, "docket_id": pl.Utf8, "publisher_status": pl.Utf8,
                  "observed_at": pl.Utf8, "removed_observed_at": pl.Utf8, "http_status": pl.Int64,
                  "body_sha256": pl.Utf8}
WINDOW_DAYS = 7
RETRY_AFTER = timedelta(days=30)
#: The owner's daily budget, about 1,100 keyed requests (decision 2026-10-03). The 1,091 dockets active in the 7 days
#: to 2026-10-03 hold 71,693 documents and need at least 1,308 listing pages, so a run reaches about five in six and
#: the rest lead the next (receipt round6/impl-C1/first_run_scope.out).
DEFAULT_MAX_REQUESTS = 1_100
#: The key allows 1,000 requests an hour and other readers share it; one request every 5 s is 720 an hour.
REQUEST_INTERVAL_SECONDS = 5.0


class _CapReached(Exception):
    """The run's request budget ran out before the docket in flight finished."""


@dataclass
class _Budget:
    limit: int
    used: int = 0

    def take(self) -> None:
        if self.used >= self.limit:
            raise _CapReached
        self.used += 1


def active_dockets(documents: Path, since: date, only: Sequence[str] = ()) -> dict[str, set[str]]:
    """Each docket with a held document posted or modified on or after ``since``, with every document it holds.

    ``only`` names the dockets to take instead, however long ago they were touched.
    """
    touched, values = (
        ("list_contains(?, docket_id)", [list(only)])
        if only
        else ("try_cast(left(posted_date, 10) AS DATE) >= ? OR try_cast(left(modify_date, 10) AS DATE) >= ?",
              [since, since])
    )
    rows = duckdb.connect().execute(
        f"""
        WITH held AS (SELECT document_id, docket_id, posted_date, modify_date FROM read_parquet(?)
                      WHERE docket_id IS NOT NULL AND document_id IS NOT NULL)
        SELECT docket_id, list(document_id) FROM held
        WHERE docket_id IN (SELECT docket_id FROM held WHERE {touched})
        GROUP BY docket_id
        """,
        [str(documents), *values],
    ).fetchall()
    return {docket: set(ids) for docket, ids in rows}


def due(active: Mapping[str, set[str]], outcomes: pl.DataFrame, now: datetime) -> list[str]:
    """Active dockets not reconciled today (UTC): never-reconciled ones first, then the longest-unreconciled."""
    last = dict(outcomes.group_by("docket_id").agg(pl.col("observed_at").max()).iter_rows())
    today = now.date().isoformat()
    return sorted((d for d in active if last.get(d, "") < today), key=lambda d: (last.get(d, ""), d))


def _listing(reader, docket_id: str, budget: _Budget) -> tuple[set[str], bool]:
    """The documents the docket's listing names, and whether the publisher's page cap cut the listing short."""
    from spicy_docs.sources.regulations_gov.api import MAX_PAGE_SIZE, document_list_url

    pages = reader.documents(document_list_url(docket_id=docket_id, page_size=MAX_PAGE_SIZE))
    listed: set[str] = set()
    while True:
        budget.take()
        page = next(pages)
        listed.update(page.document_ids)
        if page.total_elements > page.reachable_elements:
            return listed, True
        if not page.has_next_page:
            return listed, False


def reconcile_docket(
    reader, docket_id: str, held: set[str], prior: Mapping[str, Mapping[str, Any]], budget: _Budget, now: datetime
) -> tuple[list[dict], bool]:
    """This docket's new answers, one per held document it settles, and whether its listing was cut."""
    from spicy_docs.sources.regulations_gov.api import RegulationsGovApiUnavailableError

    observed_at = now.isoformat()
    listed, cut = _listing(reader, docket_id, budget)
    rows = []
    for document_id in sorted(held):
        base = {"document_id": document_id, "docket_id": docket_id, "observed_at": observed_at}
        if document_id in listed:
            rows.append({**base, "publisher_status": "listed"})
            continue
        before = prior.get(document_id) or {}
        read_recently = before.get("http_status") is not None and (
            now - datetime.fromisoformat(before["observed_at"]) < RETRY_AFTER
        )
        if cut or read_recently:
            continue
        budget.take()
        try:
            detail = reader.document(document_id)
        except RegulationsGovApiUnavailableError as error:
            removed_at = before.get("removed_observed_at") if before.get("publisher_status") == "removed" else None
            rows.append({**base, "publisher_status": "removed", "removed_observed_at": removed_at or observed_at,
                         "http_status": error.capture.status_code,
                         "body_sha256": hashlib.sha256(error.capture.body).hexdigest()})
        else:
            rows.append({**base, "publisher_status": "listed", "http_status": detail.capture.status_code,
                         "body_sha256": hashlib.sha256(detail.capture.body).hexdigest()})
    return rows, cut


def reconcile(
    output_dir: Path,
    reader,
    *,
    max_requests: int = DEFAULT_MAX_REQUESTS,
    window_days: int = WINDOW_DAYS,
    dockets: Sequence[str] = (),
    skip_upload: bool = True,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> dict[str, int]:
    """List each due docket, read each held document its listing omits, and record every answer.

    ``dockets`` reconciles those held dockets instead of the active ones, the same rules applying.
    """
    from spicy_docs.reading.paged_json import PagedJsonSourceError

    output_dir.mkdir(parents=True, exist_ok=True)
    documents = output_dir / "documents.parquet"
    if not documents.exists() and not r2.download_working_copy(documents.name, documents):
        raise RuntimeError("docket reconcile: working copy documents.parquet is missing")
    outcome_file, prior = load_outcomes(output_dir, OUTCOMES, OUTCOME_SCHEMA)
    started = now()
    held = active_dockets(documents, (started - timedelta(days=window_days)).date(), dockets)
    queue = due(held, prior, started)
    prior_rows = {row["document_id"]: row for row in prior.iter_rows(named=True)}
    budget = _Budget(max_requests)
    rows: list[dict] = []
    counts = dict.fromkeys(("reconciled", "cut", "unanswered"), 0)
    try:
        for docket_id in queue:
            try:
                answers, cut = reconcile_docket(reader, docket_id, held[docket_id], prior_rows, budget, started)
            except _CapReached:
                break
            except PagedJsonSourceError as error:
                logger.warning("docket reconcile: {} answered nothing usable ({})", docket_id, error)
                counts["unanswered"] += 1
                continue
            rows.extend(answers)
            counts["reconciled"] += 1
            counts["cut"] += cut
    finally:
        save_outcomes(outcome_file, prior, rows, key="document_id", skip_upload=skip_upload)
        counts |= {
            "active": len(held), "due": len(queue), "requests": budget.used,
            "left": len(queue) - counts["reconciled"] - counts["unanswered"],
            "listed": sum(row["publisher_status"] == "listed" for row in rows),
            "removed": sum(row["publisher_status"] == "removed" for row in rows),
        }
        logger.info("docket reconcile: {}", counts)
    if counts["unanswered"]:
        raise RuntimeError(f"docket reconcile: {counts['unanswered']} docket(s) answered nothing; asked again next run")
    return counts


def with_publisher_status(documents: Path, outcomes: Path | None) -> None:
    """Add ``publisher_status`` and ``removed_observed_at`` to the documents file in place, NULL where never read.

    Each row group is rewritten as it was with the two columns appended, so the row groups the family's admission
    bound measures keep their rows.
    """
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    columns = ["document_id", *DOCUMENT_FAMILY_COLUMNS]
    read = outcomes is not None and outcomes.exists()
    answers = pq.read_table(outcomes, columns=columns) if read else pa.table({c: pa.array([], pa.string()) for c in columns})
    answers = {column: answers[column].cast(pa.string()).combine_chunks() for column in columns}
    source = pq.ParquetFile(documents)
    schema = source.schema_arrow
    for column in DOCUMENT_FAMILY_COLUMNS:
        if column not in schema.names:
            schema = schema.append(pa.field(column, pa.string()))
    groups = source.metadata.num_row_groups
    codec = source.metadata.row_group(0).column(0).compression if groups else "ZSTD"
    lookup = pc.SetLookupOptions(value_set=answers["document_id"])
    staged = documents.with_name(f".{documents.name}.status")
    with pq.ParquetWriter(staged, schema, compression="none" if codec == "UNCOMPRESSED" else codec) as writer:
        for index in range(groups):
            group = source.read_row_group(index)
            found = pc.call_function("index_in", [group["document_id"].cast(pa.string())], lookup)
            for column in DOCUMENT_FAMILY_COLUMNS:
                values = pc.take(answers[column], found)
                if column in group.column_names:
                    # No new observation leaves the prior publisher statement intact.
                    values = pc.call_function("if_else", [found.is_null(), group[column].cast(pa.string()), values])
                    group = group.set_column(group.schema.get_field_index(column), column, values)
                else:
                    group = group.append_column(column, values)
            writer.write_table(group, row_group_size=max(group.num_rows, 1))
    staged.replace(documents)


app = App(name="reconcile-dockets", help="Flag held documents Regulations.gov no longer serves.")


@app.default
def main(
    output_dir: Annotated[Path, Parameter(help="Working directory")] = Path("output"),
    max_requests: Annotated[int, Parameter(help="Most keyed requests this run makes")] = DEFAULT_MAX_REQUESTS,
    window_days: Annotated[int, Parameter(help="List dockets with a document posted or modified this recently")] = (
        WINDOW_DAYS
    ),
    docket: Annotated[tuple[str, ...], Parameter(help="Reconcile this held docket instead (repeatable)")] = (),
    skip_upload: Annotated[bool, Parameter(help="Ask and report only; no upload")] = True,
) -> None:
    load_dotenv()
    # One attempt per request: a 429 means the shared hourly budget is spent, and a retry would spend more of it.
    with keyed_reader(max_requests=1, min_request_interval_seconds=REQUEST_INTERVAL_SECONDS) as reader:
        reconcile(output_dir, reader, max_requests=max_requests, window_days=window_days, dockets=docket,
                  skip_upload=skip_upload)


if __name__ == "__main__":
    app()
