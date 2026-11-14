"""Transform: build ``crs_reports.parquet`` from the Congress.gov REST API.

Produces a native subject table and shared receipts keyed on ``report_id`` and
``update_date``, retaining each captured revision of the report's list metadata.

Incremental discovery uses the prior selected generation's successful crawl
cutoff with overlap, never a maximum timestamp among imported rows. Explicit
reconciliation enumerates the native inventory without a date window. Dedup on
``(report_id, update_date)`` preferring the fresh copy of a repeated revision.
With no prior table it is a full list backfill. Explicit ``detail_limit`` adds a
bounded enrichment pass for selected revisions; ordinary refreshes keep
qualified detail and its witnesses at an unchanged native coordinate.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from contextlib import nullcontext
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from spicy_regs.source_evidence import CaptureEvidence

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
from loguru import logger

from spicy_regs.crs_report_detail import (
    DETAIL_COLUMNS,
    LIST_SOURCE_FIELDS,
    check_detail_replacement,
    json_text,
    merge_list_observation,
    qualified_detail,
)
from spicy_regs.sources import r2
from spicy_regs.sources.crs_reports import CrsReportDetailReader, CrsReportsReader
from spicy_regs.transforms.government_receipts import internal_prior, receipt_builder
from spicy_regs.transforms.government_source_shapes import GovernmentShapeError
from spicy_regs.transforms.parquet_rows import str_or_none

OUTPUT = "crs_reports.parquet"

# Reopen this many days before the selected generation's discovery cutoff.
OVERLAP_DAYS = 3

# Literal processing schema, converted to native subjects and retained in receipts.
# The report id and source update date identify a retained revision.
COLUMNS = (
    "report_id",
    "title",
    "report_type",
    "status",
    "published_date",
    "update_date",
    "version",
    "url",
    *DETAIL_COLUMNS,
)
_SCHEMA = pa.schema([(c, pa.string()) for c in COLUMNS])


class CrsOperationLimitError(RuntimeError):
    """A declared operation bound was reached before replacing the prior."""


class _CrsRequests:
    """Charge all list/detail attempts, including retries and redirects."""

    def __init__(self, maximum: int, list_bytes: int):
        self.maximum, self.list_bytes = maximum, list_bytes
        self.requests = self.received_list_bytes = 0

    def transport(self, inner: httpx.BaseTransport | None, *, listing: bool = False) -> httpx.BaseTransport:
        owner = self

        class Stream(httpx.SyncByteStream):
            def __init__(self, stream):
                self.inner = stream

            def __iter__(self):
                for chunk in self.inner:
                    owner.received_list_bytes += len(chunk)
                    if owner.received_list_bytes > owner.list_bytes:
                        raise CrsOperationLimitError("CRS list response bytes exceed the declared bound; prior preserved")
                    yield chunk

            def close(self):
                self.inner.close()

        class Charged(httpx.BaseTransport):
            def __init__(self):
                self.inner = inner

            def handle_request(self, request):
                if owner.requests >= owner.maximum:
                    raise CrsOperationLimitError("CRS origin requests exceed the declared bound; prior preserved")
                owner.requests += 1
                if self.inner is None:
                    self.inner = httpx.HTTPTransport()
                response = self.inner.handle_request(request)
                if listing:
                    response.stream = Stream(response.stream)
                return response

            def close(self):
                if self.inner is not None:
                    self.inner.close()

        return Charged()


def _admit_literal_prior(path: Path, *, max_bytes: int, max_rows: int) -> None:
    """Legacy processing inputs have no separate receipt member to restore."""
    if path.stat().st_size > max_bytes:
        raise ValueError("CRS prior bytes exceed the declared bound")
    metadata = pq.read_metadata(path)
    if metadata.num_rows > max_rows:
        raise ValueError("CRS prior rows exceed the declared bound")
    decoded = sum(metadata.row_group(i).column(j).total_uncompressed_size
                  for i in range(metadata.num_row_groups) for j in range(metadata.num_columns))
    if decoded > max_bytes:
        raise ValueError("CRS decoded prior bytes exceed the declared bound")



def _shape(doc: dict) -> dict:
    """Map the native list fields without giving unrequested detail false values."""
    row = {column: doc.get(field) for column, field in LIST_SOURCE_FIELDS.items()}
    row["version"] = str_or_none(row["version"])
    return {**dict.fromkeys(DETAIL_COLUMNS), "detail_state": "not_requested",
            "detail_attempt_state": "not_requested", **row}


def _discovery_state(evidence: CaptureEvidence | None) -> dict:
    """Only the selected prior's retained crawl can establish live progress."""
    return (evidence.inherited_event("crs-discovery") or {}) if evidence is not None else {}


@receipt_builder
def build_crs_reports(
    output_dir: Path, *, since: date | None = None, evidence: CaptureEvidence | None = None,
    detail_limit: int = 0, detail_api_key: str | None = None, detail_transport=None, retry_completed: bool = False,
    retained_details: dict[str, tuple[Path, str]] | None = None, refresh_list: bool = True,
    reconcile: bool = False, coverage: dict | None = None,
    detail_reports: Sequence[str] | None = None, latest: bool = False,
    prior_subject: Path | None = None, prior_receipt_path: Path | None = None,
    prior_generation_id: str | None = None,
    max_prior_bytes: int = 128 * 1024 * 1024, max_prior_rows: int = 100000,
    max_requests: int = 10000, max_list_bytes: int = 128 * 1024 * 1024,
    max_list_rows: int = 100000, list_transport: httpx.BaseTransport | None = None,
) -> Path:
    """Build ``crs_reports.parquet`` (incremental merge with the prior table)."""
    if type(detail_limit) is not int or not 0 <= detail_limit <= 100:
        raise ValueError("detail_limit must be an integer between 0 and 100")
    if any(type(value) is not int or value < 1
           for value in (max_prior_bytes, max_prior_rows, max_list_bytes, max_list_rows)):
        raise ValueError("CRS table, receipt and list bounds must be positive integers")
    if type(max_requests) is not int or max_requests < 0:
        raise ValueError("CRS max_requests must be a nonnegative integer")
    if reconcile and (since is not None or not refresh_list):
        raise ValueError("CRS reconciliation requires a list refresh without a date window")
    if not refresh_list and since is not None:
        raise ValueError("CRS detail-only selection cannot also select a date window")
    if detail_reports is not None and (refresh_list or not detail_reports or len(detail_reports) > detail_limit):
        raise ValueError("An explicit detail cohort requires detail-only mode and a limit admitting every selected revision")
    if latest and detail_reports is None:
        raise ValueError("CRS latest selection requires an explicit report cohort")
    if (prior_receipt_path is None) != (prior_generation_id is None):
        raise ValueError("CRS explicit prior receipts require their generation identity")
    if (retry_completed or retained_details is not None) and not detail_limit:
        raise ValueError("CRS detail retry or retained originals require a positive detail limit")

    out_file = output_dir / OUTPUT
    prior_file = prior_subject or output_dir / "_crs_prior.parquet"
    prior_admission = None

    def admit_prior(subject: Path, receipts: Path | None) -> None:
        from spicy_regs.crs_file_preparation import admit_metadata_pair, select_revisions

        nonlocal prior_admission
        if receipts is None:
            _admit_literal_prior(subject, max_bytes=max_prior_bytes, max_rows=max_prior_rows)
        else:
            admission = admit_metadata_pair(subject, receipts, max_bytes=max_prior_bytes, max_rows=max_prior_rows)
            # Reuse admission's exact byte pins; published evidence keeps logical
            # member names and never copies local filesystem paths.
            prior_admission = {"decoded_bytes": admission["decoded_bytes"], "members": [
                {"name": path.name, **{key: member[key] for key in ("sha256", "bytes", "rows")}}
                for path, member in zip((subject, receipts), admission["members"], strict=True)]}
            if evidence is not None:
                evidence.event("crs-prior-admission", **prior_admission)
        # Selection errors must precede receipt restoration, not only HTTP reads.
        if detail_reports is not None:
            select_revisions(pq.read_table(subject, columns=["report_id", "update_date"]).to_pylist(),
                             detail_reports, latest=latest)

    # 1. Pull the prior table (best effort — absence just means full backfill).
    if prior_subject is not None and not prior_subject.is_file():
        raise ValueError("CRS explicitly selected prior subject is missing")
    have_prior = prior_file.exists() or (prior_subject is None and r2.download(OUTPUT, prior_file))
    if not refresh_list and (not have_prior or not detail_limit):
        raise ValueError("A detail-only build requires a selected local prior and positive detail_limit")
    if have_prior:
        prior_file = internal_prior("crs_reports", prior_file, receipt_path=prior_receipt_path,
                                    generation_id=prior_generation_id, admit=admit_prior, work_dir=output_dir)
        # Undated prior rows cannot be assigned revision identities. Preserve the
        # prior and refuse before merging, rather than inventing a date.
        if ("update_date" not in pq.read_schema(prior_file).names
                or pq.read_table(prior_file, columns=["update_date"])["update_date"].null_count):
            raise GovernmentShapeError("crs_reports: prior revisions require update_date; prior preserved")
    if have_prior:
        logger.info("CRS reports: merging against prior table {}", prior_file)
    else:
        logger.info("CRS reports: no prior table found — full backfill")

    # Capture before discovery. A crawl is observed pages, not an atomic snapshot.
    # The selected generation owns this cursor: a failed conversion cannot adopt it.
    discovered_at = datetime.now(UTC).isoformat()
    state = _discovery_state(evidence)
    explicit_window = since is not None
    if since is None and not reconcile and state.get("cursor"):
        since = datetime.fromisoformat(state["cursor"]).date() - timedelta(days=OVERLAP_DAYS)
    logger.info("CRS reports: list refresh={}, window starts {}", refresh_list, since or "the beginning")

    # 3. Fetch + shape into a "new rows" parquet.
    requests = _CrsRequests(max_requests, max_list_bytes)
    reader = CrsReportsReader(since=since, evidence=evidence, transport=requests.transport(list_transport, listing=True))
    records = []
    if refresh_list:
        for doc in reader.iter_records():
            if len(records) >= max_list_rows:
                raise CrsOperationLimitError("CRS list rows exceed the declared bound; prior preserved")
            records.append((_shape(doc), frozenset(doc)))
    list_rows = len(records)
    # One bounded table pass owns the source-specific merge. It distinguishes
    # unrequested detail from authoritative explicit null/empty values.
    merged = {(row["report_id"], row["update_date"]): {**dict.fromkeys(COLUMNS), **row}
              for row in pq.read_table(prior_file).to_pylist()} if have_prior else {}
    for row, fields in records:
        key = (row["report_id"], row["update_date"])
        witnesses = getattr(reader, "witnesses", {}).get(key)
        if witnesses:
            row["list_witness_json"] = json_text(witnesses)
        prior = merged.get(key)
        if prior:
            source = merge_list_observation(
                {field: prior.get(column) for column, field in LIST_SOURCE_FIELDS.items()},
                {field: row[column] for column, field in LIST_SOURCE_FIELDS.items() if field in fields},
            )
            row.update({column: source.get(field) for column, field in LIST_SOURCE_FIELDS.items()})
            for name in DETAIL_COLUMNS:
                row[name] = prior.get(name)
            old_witnesses = json.loads(prior.get("list_witness_json") or "[]")
            fresh_witnesses = getattr(reader, "witnesses", {}).get(key, [])
            for witness in fresh_witnesses:
                if witness not in old_witnesses:
                    old_witnesses.append(witness)
            row["list_witness_json"] = json_text(old_witnesses) if old_witnesses else None
        merged[key] = row
    if detail_limit:
        from spicy_regs.source_evidence import acquisition_evidence

        latest_revisions = {}
        for key, row in sorted(merged.items()):
            latest_revisions[row["report_id"]] = key
        from spicy_regs.crs_file_preparation import select_revisions

        candidates = (select_revisions(list(merged.values()), detail_reports, latest=latest)
                      if detail_reports is not None else [merged[key] for key in latest_revisions.values()])
        selected = [row for row in candidates if retry_completed
                    or row.get("detail_state") != "ok" or row.get("detail_attempt_state") != "ok"]
        selected = selected[:detail_limit]
        if retained_details is not None and any(row["report_id"] not in retained_details for row in selected):
            raise ValueError("CRS selected detail cohort has missing retained originals")
        context = nullcontext(evidence) if evidence is not None else acquisition_evidence("crs-reports", output_dir=output_dir)
        with context as detail_evidence:
            detail_reader = CrsReportDetailReader(evidence=detail_evidence, api_key=detail_api_key,
                                                 transport=requests.transport(detail_transport), retained=retained_details)
            for selected_row in selected:
                # The shared selector returns a copy. Mutate the retained revision.
                row = merged[(selected_row["report_id"], selected_row["update_date"])]
                outcome = detail_reader.read(row)
                if outcome["state"] == "revision_mismatch":
                    detail_evidence.event("crs-detail-revision-mismatch", report_id=row["report_id"],
                                          update_date=row["update_date"], witnesses=outcome["witnesses"])
                    raise GovernmentShapeError("CRS detail revision mismatch; prior preserved")
                if outcome["state"] == "ok":
                    if any(field in outcome["report"] and row.get(column) is not None
                           and outcome["report"][field] != row[column] for column, field in
                           (("title", "title"), ("report_type", "contentType"), ("status", "status"),
                            ("published_date", "publishDate"))):
                        raise GovernmentShapeError("CRS incompatible list/detail metadata; prior preserved")
                    check_detail_replacement(row, outcome["report"])
                    row.update(qualified_detail(outcome["report"], outcome["witnesses"]))
                else:
                    row.update(detail_attempt_state="failed", detail_error=outcome["error"],
                               detail_attempt_witness_json=json_text(outcome["witnesses"]))
    rows = sorted(merged.values(), key=lambda row: (row["update_date"] or "", row["report_id"] or ""), reverse=True)
    new_file = output_dir / "_crs_new.parquet"
    table = pa.Table.from_pylist(rows, schema=_SCHEMA) if rows else _SCHEMA.empty_table()
    pq.write_table(table, new_file, compression="zstd")
    logger.info("CRS reports: read {:,} list rows; prepared {:,} retained revisions", list_rows, len(rows))

    # Atomically replace only after every source-specific compatibility check.
    new_file.replace(out_file)

    # Housekeeping: drop scratch files so they aren't mistaken for outputs.
    for scratch in (prior_file, new_file):
        if scratch != prior_subject:
            scratch.unlink(missing_ok=True)

    total = pq.ParquetFile(out_file).metadata.num_rows
    logger.info("CRS reports: {:,} rows", total)
    failed_details = sum(row.get("detail_attempt_state") == "failed" for row in rows)
    status = {"discovery": "complete" if refresh_list else "not_requested",
              "scope": "detail-cohort" if not refresh_list else "native-inventory" if reconcile or since is None else "date-window",
              "since": str(since) if since is not None else None,
              "listed": list_rows, "detail_failed": failed_details,
              "table_generation": "prepared", "atomic_snapshot": False,
              "whole_history_complete": False, "origin_requests": requests.requests,
              "prior_inputs": prior_admission,
              "list_response_bytes": requests.received_list_bytes,
              "detail_selected": len(candidates) if detail_limit else 0,
              "detail_attempted": len(selected) if detail_limit else 0}
    if coverage is not None:
        coverage.update(status)
    if evidence is not None:
        evidence.event("acquisition-coverage", **status)
        # An explicit archive/window import or detail-only pass carries live state
        # forward unchanged. Metadata rows never create or move the live cursor.
        cursor = discovered_at if refresh_list and not explicit_window else state.get("cursor")
        evidence.event("crs-discovery", cursor=cursor, since=status["since"],
                       mode="reconcile" if reconcile else "explicit-window" if explicit_window else
                            "refresh" if refresh_list else "detail-only")
    return out_file
