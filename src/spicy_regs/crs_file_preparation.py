"""Prepare bounded CRS file selections and evidence-scoped coverage, entirely offline."""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pyarrow.parquet as pq

MAX_SELECTION_BYTES = 2 * 1024 * 1024


def _sha(path: Path) -> str:
    with path.open("rb") as stream:
        return "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()


def admit_metadata_pair(subject: Path, receipt: Path, *, max_bytes: int, max_rows: int) -> dict:
    """Bound both Parquet members before restoration and record their exact identities."""
    if any(type(value) is not int or value <= 0 for value in (max_bytes, max_rows)):
        raise ValueError("CRS input bounds must be positive integers")
    paths = (subject, receipt)
    sizes = [path.stat().st_size for path in paths]
    if sum(sizes) > max_bytes:
        raise ValueError("CRS subject and receipt bytes exceed the declared bound")
    metadata = [pq.read_metadata(path) for path in paths]
    if any(item.num_rows > max_rows for item in metadata):
        raise ValueError("CRS subject or receipt rows exceed the declared bound")
    decoded = sum(item.row_group(i).column(j).total_uncompressed_size
                  for item in metadata for i in range(item.num_row_groups) for j in range(item.num_columns))
    if decoded > max_bytes:
        raise ValueError("CRS decoded subject and receipt bytes exceed the declared bound")
    return {"decoded_bytes": decoded, "members": [
        {"path": str(path.resolve()), "sha256": _sha(path), "bytes": size, "rows": item.num_rows}
        for path, size, item in zip(paths, sizes, metadata, strict=True)]}


def read_metadata_pair(subject: Path, receipt: Path, *, generation_id: str, max_bytes: int,
                       max_rows: int, subject_sha256: str | None = None,
                       receipt_sha256: str | None = None) -> tuple[list[dict], dict]:
    """Use the maintained shared receipt reader after complete resource admission."""
    from spicy_regs.etl_receipts import read_with_receipts
    from spicy_regs.transforms.government_receipts import POLICIES

    admission = admit_metadata_pair(subject, receipt, max_bytes=max_bytes, max_rows=max_rows)
    for member, expected in zip(admission["members"], (subject_sha256, receipt_sha256), strict=True):
        if expected is not None and member["sha256"] != expected:
            raise ValueError("CRS input differs from its expected SHA256")
    if not generation_id:
        raise ValueError("CRS metadata requires an explicit receipt generation")
    rows = [row["raw_record"] for row in
            read_with_receipts([subject], [receipt], POLICIES["crs_reports"], generation_id=generation_id)]
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError("CRS metadata receipt lacks its literal processing row")
    return rows, admission


def _instant(value: object) -> datetime:
    try:
        stamp = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    except (ValueError, TypeError):
        raise ValueError("CRS revision requires an ISO timestamp with timezone") from None
    if stamp.tzinfo is None or stamp.utcoffset() is None:
        raise ValueError("CRS revision requires an ISO timestamp with timezone")
    return stamp.astimezone(UTC)


def select_revisions(rows: Sequence[Mapping], reports: Sequence[str], *, latest: bool = False) -> list[dict]:
    """Select explicit native revisions; a bare report ID always requires ``latest``."""
    if not reports:
        raise ValueError("CRS selection requires explicit reports")
    indexed: dict[str, dict[datetime, dict]] = {}
    for row in rows:
        report_id = row.get("report_id")
        if not isinstance(report_id, str) or not re.fullmatch(r"[A-Z0-9][A-Z0-9-]*", report_id):
            raise ValueError("CRS metadata has an invalid report ID")
        instant = _instant(row.get("update_date"))
        revisions = indexed.setdefault(report_id, {})
        if instant in revisions:
            raise ValueError("CRS metadata repeats a native revision")
        revisions[instant] = dict(row)
    selected, seen = [], set()
    for report in reports:
        report_id, separator, stamp = report.partition("@")
        if not re.fullmatch(r"[A-Z0-9][A-Z0-9-]*", report_id):
            raise ValueError("CRS selection has an invalid report ID")
        if not separator and not latest:
            raise ValueError("CRS bare report IDs require explicit latest selection")
        if report_id not in indexed:
            raise ValueError("CRS selected report is unknown in the admitted metadata")
        instant = _instant(stamp) if separator else max(indexed[report_id])
        if instant not in indexed[report_id]:
            raise ValueError("CRS selected revision is unknown in the admitted metadata")
        key = report_id, instant
        if key in seen:
            raise ValueError("CRS selection repeats a native revision")
        seen.add(key)
        selected.append(indexed[report_id][instant])
    return selected


def _catalog_url(row: Mapping) -> str | None:
    facts = json.loads(row["data"]) if isinstance(row["data"], str) else row["data"]
    return ((facts.get("receipt_context") or {}).get("source_url") or facts.get("source_url")
            or facts.get("receipt_source_url"))


def _catalog_selection(path: Path, expected_sha256: str, *, max_bytes: int, max_rows: int) -> tuple[list[dict], dict]:
    """Admit exact exported catalog occurrences without scanning or hashing the catalog."""
    if path.stat().st_size > max_bytes:
        raise ValueError("CRS catalog selection bytes exceed the declared bound")
    body = path.read_bytes()
    pin = {"path": str(path.resolve()), "sha256": "sha256:" + hashlib.sha256(body).hexdigest(), "bytes": len(body)}
    if pin["sha256"] != expected_sha256:
        raise ValueError("CRS catalog selection differs from its expected SHA256")
    rows = json.loads(body)
    if not isinstance(rows, list) or len(rows) > max_rows or any(not isinstance(row, dict) for row in rows):
        raise ValueError("CRS catalog selection must be a bounded array of occurrences")
    return rows, pin


def _witnesses(row: Mapping, column: str) -> list[dict]:
    values = json.loads(row.get(column) or "[]")
    if not isinstance(values, list) or any(not isinstance(value, dict) for value in values):
        raise ValueError("CRS metadata witness must be an array of objects")
    return values


def _detail_state(row: Mapping) -> str:
    if row.get("detail_state") == "ok":
        return "qualified-detail"
    attempt = row.get("detail_attempt_state")
    if attempt == "revision_mismatch":
        return "historical-detail-not-retained"
    if attempt in {"failed", "unavailable"}:
        # Metadata records an attempt outcome, not its HTTP status. A failure
        # witness alone does not establish that a historical detail is absent.
        return "detail-attempt-failed"
    if attempt in (None, "not_requested"):
        return "detail-not-requested"
    return "detail-unknown"


def prepare_crs_report_files(
    subject: Path, receipt: Path, reports: Sequence[str], *, generation_id: str,
    subject_sha256: str, receipt_sha256: str, latest: bool = False,
    include_html: bool = False, prior_bundle: Path | None = None,
    prior_subject_sha256: str | None = None, prior_receipt_sha256: str | None = None,
    catalog_selection: Path | None = None, catalog_selection_sha256: str | None = None,
    max_input_bytes: int = 128 * 1024 * 1024, max_input_rows: int = 10000,
    max_reports: int = 10, max_file_bytes: int = 8 * 1024 * 1024,
    max_total_bytes: int = 64 * 1024 * 1024,
    max_catalog_rows: int = 1000,
) -> dict:
    """Read admitted native pairs; emit the existing collector input and bounded coverage.

    All metadata history remains in the input pair. Coverage describes only the
    requested revisions and supplied evidence. A missing catalog match is unmapped,
    never a claim that an original or historical edition does not exist.
    """
    from spicy_docs.sources.congress.crs_files import crs_file_locator, crs_html_locator
    from spicy_docs.sources.congress.crs_summaries import read_report
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    from spicy_regs.transforms import build_crs_report_files as collector

    limits = (max_reports, max_file_bytes, max_total_bytes, max_catalog_rows)
    if any(type(value) is not int or value <= 0 for value in limits):
        raise ValueError("CRS preparation bounds must be positive integers")
    if not reports or len(reports) > max_reports:
        raise ValueError("CRS requested report cohort exceeds the declared bound")
    rows, admission = read_metadata_pair(
        subject, receipt, generation_id=generation_id, max_bytes=max_input_bytes, max_rows=max_input_rows,
        subject_sha256=subject_sha256, receipt_sha256=receipt_sha256)
    selected = select_revisions(rows, reports, latest=latest)
    pins = list(admission["members"])
    prior_rows = []
    if prior_bundle is not None:
        if not prior_subject_sha256 or not prior_receipt_sha256:
            raise ValueError("CRS prior file bundle requires both expected SHA256 pins")
        prior_admission = admit_metadata_pair(prior_bundle / collector.KEY, prior_bundle / "etl_receipts.parquet",
                                               max_bytes=max_input_bytes, max_rows=max_input_rows)
        if [member["sha256"] for member in prior_admission["members"]] != [prior_subject_sha256, prior_receipt_sha256]:
            raise ValueError("CRS prior file bundle differs from its expected SHA256")
        pins.extend(prior_admission["members"])
        prior_rows = collector._read(prior_bundle, max_bytes=max_input_bytes, max_rows=max_input_rows)

    # Resolve the declared evidence set before opening any raw originals.
    catalog_rows = []
    if catalog_selection is not None:
        if not catalog_selection_sha256:
            raise ValueError("CRS catalog selection requires its expected SHA256")
        catalog_rows, catalog_pin = _catalog_selection(
            catalog_selection, catalog_selection_sha256, max_bytes=min(max_input_bytes, MAX_SELECTION_BYTES),
            max_rows=max_catalog_rows)
        pins.append(catalog_pin)
    have_catalog = catalog_selection is not None
    store = None
    qualified, retained, coverage = [], {}, []
    body_bytes = 0
    verified: set[tuple] = set()
    for row in selected:
        detail_state = _detail_state(row)
        detail_witnesses = _witnesses(row, "detail_witness_json")
        detail_sha = detail_witnesses[-1].get("sha256") if detail_witnesses else None
        catalog_matches = [item["id"] for item in catalog_rows if item.get("sha256") == detail_sha]
        base = {"report_id": row["report_id"], "update_date": row["update_date"],
                "detail_state": detail_state, "detail_attempt_state": row.get("detail_attempt_state"),
                "metadata_source_sha256": detail_sha,
                "catalog_state": "matched-by-witness" if catalog_matches else "unmapped" if have_catalog else "unknown",
                "catalog_occurrences": catalog_matches}
        if detail_state != "qualified-detail":
            coverage.append({**base, "disposition": "metadata-only", "format_state": "unknown",
                             "body_state": "unknown", "text_state": "unknown"})
            continue
        if not detail_sha:
            raise ValueError("CRS qualified detail lacks its exact original witness")
        if store is None:
            store = CentralRawArchiveStore(create=False)
        remaining = max_total_bytes - body_bytes
        if remaining <= 0:
            raise ValueError("CRS preparation decoded bodies exceed the declared total bound")
        with store.open(detail_sha) as stream:
            original = stream.read(min(2 * 1024 * 1024, remaining) + 1)
        if len(original) > 2 * 1024 * 1024 or collector._sha(original) != detail_sha:
            raise ValueError("CRS detail original differs from its digest/byte bound")
        body_bytes += len(original)
        if body_bytes > max_total_bytes:
            raise ValueError("CRS preparation decoded bodies exceed the declared total bound")
        native = read_report(original, row["report_id"], update_date=row["update_date"], version=row["version"])
        native_formats = native.get("formats")
        literal_formats = json.loads(row.get("formats_json") or "null")
        if native_formats != literal_formats:
            raise ValueError("CRS offered formats differ from the pinned detail original")
        if not isinstance(native_formats, list):
            coverage.append({**base, "disposition": "qualified-detail", "format_state": "unknown",
                             "body_state": "unknown", "text_state": "unknown"})
            continue
        if not any(isinstance(item, dict) and str(item.get("format", "")).upper() == "PDF"
                   for item in native_formats):
            coverage.append({**base, "disposition": "qualified-detail", "format_state": "no-pdf-offered",
                             "offered_formats": native_formats, "body_state": "unknown", "text_state": "unknown"})
            continue
        selection, digest = collector._qualified_report(row)
        qualified.append(row)
        renditions = [("pdf", crs_file_locator(selection.pdf), str(selection.pdf.version))]
        if selection.html is not None:
            renditions.append(("html", crs_html_locator(selection.html), None))
        for rendition, url, publisher_version in renditions:
            matching_prior = [item for item in prior_rows if item["report_id"] == row["report_id"]
                              and _instant(item["metadata_update_date"]) == _instant(row["update_date"])
                              and item["metadata_source_sha256"] == digest and item["source_url"] == url]
            candidates = [dict(item) for item in matching_prior if item.get("body_state") == "ok"]
            candidates.extend(collector.retained_file_pin(item) for item in catalog_rows
                              if item["status"] == "held" and _catalog_url(item) == url)
            identities = {(item["source_sha256"], item["source_bytes"]) for item in candidates}
            if len(identities) > 1:
                raise ValueError("CRS retained URL has conflicting body identities; select exact evidence")
            pin = candidates[0] if candidates else None
            if pin is not None:
                key = (pin["source_sha256"], pin["capture_receipt_sha256"], pin.get("capture_receipt_entry"), url)
                if key not in verified:
                    if body_bytes + pin["source_bytes"] > max_total_bytes:
                        raise ValueError("CRS preparation decoded bodies exceed the declared total bound")
                    body = collector._body(pin, store, max_file_bytes)
                    body_bytes += len(body)
                    verified.add(key)
                if rendition == "pdf" or include_html:
                    if url in retained and retained[url]["source_sha256"] != pin["source_sha256"]:
                        raise ValueError("CRS selected revisions require conflicting mutable bodies")
                    retained[url] = pin
            completed = next((item for item in matching_prior if item.get("body_state") == "ok"
                              and item.get("text_state") in {"ok", "empty"}), None)
            refusal = next((item for item in reversed(matching_prior)
                            if item.get("body_state") in {"unavailable", "refused"}), None)
            if refusal is not None:
                observed = collector._receipt(store, refusal["association_receipt_key"],
                                               refusal["association_receipt_sha256"])["receipt"]
                if (observed.get("kind") != "crs-file-refusal"
                        or observed.get("body_state") != refusal["body_state"]
                        or observed.get("selection") != [row["report_id"], row["update_date"], rendition, url, digest]):
                    raise ValueError("CRS file refusal differs from its pinned association receipt")
            body_state = "body-held" if pin else refusal["body_state"] if refusal else "unknown"
            prior_outcomes = [{"body_state": item["body_state"], "text_state": item["text_state"],
                               "source_sha256": item["source_sha256"]} for item in matching_prior]
            coverage.append({**base, "rendition": rendition, "source_url": url,
                             "publisher_version": publisher_version,
                             "selected": rendition == "pdf" or include_html,
                             "format_state": "format-offered", "body_state": body_state,
                             "text_state": completed["text_state"] if completed else "unknown",
                             "disposition": "empty-text" if completed and completed["text_state"] == "empty"
                             else "completed-text" if completed else "body-held" if pin
                             else "source-unavailable" if refusal and refusal["body_state"] == "unavailable"
                             else "source-refused" if refusal else "format-offered",
                             "source_sha256": pin["source_sha256"] if pin else None,
                             "prior_outcomes": prior_outcomes})
    for pin in pins:
        if _sha(Path(pin["path"])) != pin["sha256"]:
            raise ValueError("CRS admitted input changed during preparation")
    selection_input = {"reports": qualified, "retained_files": retained}
    if len(json.dumps(selection_input, indent=2, default=str).encode()) > MAX_SELECTION_BYTES:
        raise ValueError("CRS prepared selection exceeds the collector's 2 MiB input bound")
    return {"selection": selection_input,
            "coverage": {"scope": "explicit-admitted-revisions-and-supplied-evidence",
                         "input_rows": len(rows), "selected_revisions": len(selected), "inputs": pins,
                         "decoded_body_bytes": body_bytes, "revisions": coverage},
            "status": "prepared_offline_not_published"}


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("subject", "receipts", "selection-output", "coverage-output"):
        parser.add_argument("--" + name, type=Path, required=True)
    for name in ("generation-id", "subject-sha256", "receipt-sha256"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--report", action="append", required=True, dest="reports")
    parser.add_argument("--latest", action="store_true")
    parser.add_argument("--include-html", action="store_true")
    parser.add_argument("--prior-bundle", type=Path)
    parser.add_argument("--prior-subject-sha256")
    parser.add_argument("--prior-receipt-sha256")
    parser.add_argument("--catalog-selection", type=Path)
    parser.add_argument("--catalog-selection-sha256")
    for name, default in (("max-input-bytes", 128 * 1024 * 1024), ("max-input-rows", 10000),
                          ("max-reports", 10), ("max-file-bytes", 8 * 1024 * 1024),
                          ("max-total-bytes", 64 * 1024 * 1024),
                          ("max-catalog-rows", 1000)):
        parser.add_argument("--" + name, type=int, default=default)
    args = vars(parser.parse_args())
    selection_output, coverage_output = args.pop("selection_output"), args.pop("coverage_output")
    args["receipt"] = args.pop("receipts")
    inputs = {args["subject"].resolve(), args["receipt"].resolve()}
    if args["catalog_selection"] is not None:
        inputs.add(args["catalog_selection"].resolve())
    if args["prior_bundle"] is not None:
        inputs.update(args["prior_bundle"].joinpath(name).resolve() for name in
                       ("crs_report_files.parquet", "etl_receipts.parquet"))
    outputs = {selection_output.resolve(), coverage_output.resolve()}
    if len(outputs) != 2 or inputs & outputs:
        parser.error("selection and coverage outputs must be distinct from each other and all inputs")
    result = prepare_crs_report_files(**args)
    for path, value in ((selection_output, result["selection"]), (coverage_output, result["coverage"])):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, indent=2, default=str) + "\n")
    print(json.dumps({"status": result["status"], "selection_output": str(selection_output),
                      "coverage_output": str(coverage_output)}))


if __name__ == "__main__":
    main()
