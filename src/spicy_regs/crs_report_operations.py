"""Prepare bounded CRS detail or list candidates through the maintained builder."""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Literal

import pyarrow as pa
from cyclopts import App

from spicy_regs.etl_bulk import bulk_limits
from spicy_regs.source_evidence import acquisition_evidence
from spicy_regs.transforms.build_crs_reports import build_crs_reports

app = App(help=__doc__)


def _retained(path: Path | None) -> dict[str, tuple[Path, str]] | None:
    if path is None:
        return None
    if path.stat().st_size > 1024 * 1024:
        raise ValueError("CRS retained detail selection exceeds 1 MiB")
    entries = json.loads(path.read_text())
    if not isinstance(entries, list) or not entries or len(entries) > 100:
        raise ValueError("CRS retained details must name between 1 and 100 pinned originals")
    retained = {}
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"report_id", "path", "sha256"}:
            raise ValueError("CRS retained detail entries require report_id, path and sha256")
        report_id, original, digest = entry["report_id"], entry["path"], entry["sha256"]
        if (
            not isinstance(report_id, str)
            or not re.fullmatch(r"[A-Z0-9][A-Z0-9-]*", report_id)
            or report_id in retained
            or not isinstance(original, str)
            or not isinstance(digest, str)
            or not re.fullmatch(r"sha256:[0-9a-f]{64}", digest)
        ):
            raise ValueError("CRS retained details have an invalid or duplicate identity")
        source = path.parent / original
        if not source.is_file() or source.stat().st_size > 2 * 1024 * 1024:
            raise ValueError("CRS retained detail is missing or exceeds 2 MiB")
        retained[report_id] = source, digest
    return retained


@app.default
def prepare(
    output_dir: Path,
    *,
    operation: Literal["detail", "refresh", "reconcile"],
    prior_subject: Path,
    max_prior_bytes: int,
    max_prior_rows: int,
    max_requests: int,
    prior_receipts: Path | None = None,
    prior_generation_id: str | None = None,
    report: list[str] | None = None,
    latest: bool = False,
    max_details: int = 0,
    retained_details: Path | None = None,
    retry_completed: bool = False,
    since: date | None = None,
    max_list_bytes: int = 0,
    max_list_rows: int = 0,
) -> None:
    """Prepare a local candidate; preserve the complete selected prior.

    Report selections use ID@timestamp, or bare IDs with --latest. The explicit
    request budget includes retries. Retained detail reads spend no requests.
    Refresh uses the selected date window; reconcile uses no date predicate.
    """
    if operation not in {"detail", "refresh", "reconcile"}:
        raise ValueError("CRS operation must be detail, refresh or reconcile")
    detail = operation == "detail"
    if detail:
        if not report or not 1 <= max_details <= 100 or since is not None or max_list_bytes or max_list_rows:
            raise ValueError("CRS detail requires reports and max-details, without list/date options")
    elif report or latest or max_details or retained_details is not None or retry_completed:
        raise ValueError("CRS list operations cannot also select detail enrichment")
    elif max_requests < 1 or max_list_bytes < 1 or max_list_rows < 1:
        raise ValueError("CRS list operations require positive request, response-byte and row bounds")
    if operation == "reconcile" and since is not None:
        raise ValueError("CRS reconciliation cannot also select a date window")
    if operation == "refresh" and since is None:
        raise ValueError(
            "CRS local refresh requires an explicit date window; use reconcile for an unfiltered inventory"
        )
    if (prior_receipts is None) != (prior_generation_id is None):
        raise ValueError("CRS explicit prior receipts require their generation identity")
    if not prior_subject.is_file() or (prior_receipts is not None and not prior_receipts.is_file()):
        raise ValueError("CRS operations require a local selected prior subject (and matching receipts if supplied)")
    prior_dirs = {prior_subject.parent.resolve()}
    if prior_receipts is not None:
        prior_dirs.add(prior_receipts.parent.resolve())
    if output_dir.resolve() in prior_dirs:
        raise ValueError("CRS candidate output must be separate from the selected prior")
    retained = _retained(retained_details)
    output_dir.mkdir(parents=True, exist_ok=True)
    coverage: dict = {}
    pa.set_cpu_count(2)
    pa.set_io_thread_count(2)
    with (
        bulk_limits(threads=2, memory_limit="512MB"),
        acquisition_evidence("crs-reports", output_dir=output_dir) as evidence,
    ):
        # This command accepts a selected native pair, not a managed generation
        # root. It cannot derive qualified live discovery state from row dates.
        evidence.event(
            "crs-operation",
            operation=operation,
            discovery_inheritance="not_supplied",
            max_prior_bytes=max_prior_bytes,
            max_prior_rows=max_prior_rows,
            max_requests=max_requests,
            max_details=max_details,
            max_list_bytes=max_list_bytes,
            max_list_rows=max_list_rows,
        )
        build_crs_reports(
            output_dir,
            evidence=evidence,
            prior_subject=prior_subject,
            prior_receipt_path=prior_receipts,
            prior_generation_id=prior_generation_id,
            refresh_list=not detail,
            reconcile=operation == "reconcile",
            since=since,
            detail_reports=report,
            latest=latest,
            detail_limit=max_details,
            retained_details=retained,
            retry_completed=retry_completed,
            max_prior_bytes=max_prior_bytes,
            max_prior_rows=max_prior_rows,
            max_requests=max_requests,
            max_list_bytes=max_list_bytes or 1,
            max_list_rows=max_list_rows or 1,
            coverage=coverage,
        )
    # Local result metadata is deliberately separate from native source receipts.
    result = {
        "operation": operation,
        "candidate": str((output_dir / "crs_reports.parquet").resolve()),
        "prior_generation_id": prior_generation_id,
        "discovery_inheritance": "not_supplied",
        "coverage": coverage,
        "deployment": "not_requested",
    }
    (output_dir / "crs-operation.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, sort_keys=True))
    if coverage.get("detail_failed"):
        raise RuntimeError("CRS candidate prepared with failed details; see crs-operation.json")


def main() -> None:
    app()


if __name__ == "__main__":
    main()
