"""Capture or replay one explicitly selected full USAspending award-transaction archive."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path
from urllib.parse import urlsplit

import pyarrow.parquet as pq

from spicy_regs.contract_types import described_schema
from spicy_regs.generations import build_generation
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_usaspending_awards import SCHEMAS, build_tables

FAMILY = "usaspending-awards"
DATASET = "declared:usaspending-award-downloads"
SELECTION = "usaspending-full-transactions/1"
MAX_BYTES = 8 * 1024 * 1024


def _archive_item(item, fiscal_year):
    if item["fiscal_year"] != fiscal_year or "_Full_" not in item["file_name"]:
        raise ValueError("Select the explicitly reviewed fiscal-year full archive; all-year/delta inputs refuse")
    url = urlsplit(item["url"])
    if (
        url.scheme != "https"
        or url.netloc != "files.usaspending.gov"
        or url.query
        or url.fragment
        or url.path != "/award_data_archive/" + item["file_name"]
    ):
        raise ValueError("Archive URL differs from the public monthly full-archive route")


def _read(store, pin, limit):
    from spicy_docs.sources.usaspending_awards import digest

    if type(pin["bytes"]) is not int or not 0 <= pin["bytes"] <= limit:
        raise ValueError("Retained USAspending input exceeds its byte budget")
    with store.open(pin["sha256"]) as stream:
        body = stream.read(limit + 1)
    if len(body) != pin["bytes"] or digest(body) != pin["sha256"]:
        raise ValueError("Retained USAspending input differs from its selected bytes")
    return body


def capture(evidence, *, agency_id, agency_code, fiscal_year, award_family, file_name, transport=None):
    """Resolve a caller-selected full filename in one exact inventory response.

    No archive pagination, all-year delta, or automatic newest-version choice.
    Reuse a retained selection instead when its exact version is already held.
    """
    import httpx
    from spicy_docs.reading.paged_json import PagedJsonBudget
    from spicy_docs.sources.usaspending import UsaspendingMonthlyFilesReader, monthly_files_request
    from spicy_docs.sources.usaspending_awards import digest, read_transaction_archive

    body = monthly_files_request(agency=agency_id, fiscal_year=fiscal_year, type=award_family)[1]
    observed = evidence.transport(transport, stage="usaspending-inventory", max_bytes=256 * 1024)
    with UsaspendingMonthlyFilesReader(budget=PagedJsonBudget(1, 256 * 1024, 30, 0), transport=observed) as reader:
        inventory = reader.monthly_files(body)
    (item,) = [r for r in json.loads(inventory.body)["monthly_files"] if r["file_name"] == file_name]
    _archive_item(item, fiscal_year)
    observed = evidence.transport(transport, stage="usaspending-full-archive", max_bytes=MAX_BYTES)
    with httpx.Client(transport=observed, timeout=30, headers={"Accept-Encoding": "identity"}) as client:
        response = client.get(item["url"])
        response.raise_for_status()
        if response.headers.get("content-encoding", "identity") != "identity":
            raise ValueError("USAspending full archive requires identity encoding")
        completed = response.extensions["spicy_docs.completed_capture"]
        pin = {**item, "sha256": completed.sha256, "bytes": completed.byte_size}
        read_transaction_archive(response.content, sha256=pin["sha256"], award_family=award_family)
    metadata = asdict(inventory)
    metadata.pop("body")
    metadata.pop("_digest")
    metadata.pop("request_body")
    selection = {
        "schema": SELECTION,
        "snapshot_kind": "full",
        "agency_id": agency_id,
        "agency_code": agency_code,
        "fiscal_year": fiscal_year,
        "award_family": award_family,
        "archive": pin,
        "inventory": {
            **metadata,
            "sha256": inventory.sha256,
            "bytes": inventory.byte_size,
            "request": body,
            "request_body": {"sha256": digest(inventory.request_body), "bytes": len(inventory.request_body)},
        },
    }
    evidence.event("usaspending-selection", selection=selection)
    return selection


def selected_archive(selection, store):
    """Renew the inventory request and exact archive identities before interpreting rows."""
    from spicy_docs.sources.usaspending import validate_monthly_files_capture
    from spicy_docs.sources.usaspending_awards import read_transaction_archive
    from spicy_docs.transport.captured import CapturedBodyResponse

    if selection["schema"] != SELECTION or selection["snapshot_kind"] != "full":
        raise ValueError("Expected a full USAspending transaction selection")
    pin, inv = selection["archive"], selection["inventory"]
    expected = {
        "agency": selection["agency_id"],
        "fiscal_year": selection["fiscal_year"],
        "type": selection["award_family"],
    }
    if inv["request"] != expected:
        raise ValueError("Retained inventory request differs from the selected population")
    capture = CapturedBodyResponse(
        **{
            key: inv[key]
            for key in (
                "requested_url",
                "resolved_url",
                "status_code",
                "content_type",
                "observed_at",
                "content_encoding",
                "method",
            )
        },
        body=_read(store, inv, 256 * 1024),
        request_body=_read(store, inv["request_body"], 1024),
    )
    inventory = validate_monthly_files_capture(capture, expected)
    offered = [{k: v for k, v in pin.items() if k not in {"sha256", "bytes"}}]
    if offered[0] not in inventory["monthly_files"] or pin["fiscal_year"] != selection["fiscal_year"]:
        raise ValueError("Selected archive is not the exact fiscal-year inventory item")
    _archive_item(pin, selection["fiscal_year"])
    original = _read(store, pin, MAX_BYTES)
    return (
        read_transaction_archive(original, sha256=pin["sha256"], award_family=selection["award_family"]),
        original,
        capture,
    )


def build(selection, output_dir: Path, *, archive_root: Path | None = None):
    """Build all declared tables for one finite, inventory-bound full cohort.

    Complete-family describes this build's declared tables and selected archive,
    not all USAspending agencies/years or known values for every monetary field.
    A refreshed full snapshot replaces the same cohort in a new generation;
    an unchanged replay performs no HTTP. Incomplete input or a dropped source
    record refuses before a complete selected-input declaration can be sealed.
    """
    output_dir = Path(output_dir)
    if (output_dir / "generation").exists():
        raise ValueError("Use a fresh candidate directory")
    evidence = CaptureEvidence(output_dir, FAMILY, archive_root=archive_root)
    try:
        archive, original, inventory_capture = selected_archive(selection, evidence.store)
        evidence.capture(inventory_capture, stage="usaspending-retained-inventory", dataset_id=DATASET)
        pin = selection["archive"]
        evidence.retain_bytes(
            original,
            stage="usaspending-retained-full-archive",
            raw_source=True,
            source_url=pin["url"],
            original_location=pin["url"],
            dataset_id=DATASET,
            native_id=pin["file_name"],
            source_version=pin["updated_date"],
            covered_period={
                "kind": "fiscal-year",
                "label": str(selection["fiscal_year"]),
                "basis": "retained fiscal-year inventory and complete native action-date validation",
            },
        )
        evidence.event("usaspending-selection", selection=selection)
        tables = build_tables(archive, output_dir, selection=selection)
        counts = {path.stem: pq.read_metadata(path).num_rows for path in tables}
        if counts["usaspending_transactions"] != len(archive.rows) or counts["usaspending_archive_members"] != len(
            archive.members
        ):
            raise ValueError("Full cohort build must retain every original transaction and CSV member")
        scope = {
            "id": (
                f"{FAMILY}:awarding-agency:{selection['agency_code']}:FY{selection['fiscal_year']}:"
                f"{selection['award_family']}:full:{archive.sha256}"
            ),
            "finite": True,
            "awarding_agency_database_id": selection["agency_id"],
            "awarding_agency_code": selection["agency_code"],
            "fiscal_year": selection["fiscal_year"],
            "award_family": selection["award_family"],
            "geographic_filter": None,
            "archive_sha256": archive.sha256,
            "archive_version": pin["updated_date"],
        }
        evidence.event(
            "usaspending-cohort",
            scope=scope,
            complete=True,
            completeness_basis="Every member and record in the selected full archive, with no row filtering or skipping",
            table_rows=counts,
            outlay_scope="Publisher lifetime amounts; blanks remain unknown, not a complete fiscal-year outlay total",
        )
        evidence.declare_used_inputs(
            [
                {"location": pin["url"], "sha256": pin["sha256"], "bytes": pin["bytes"], "asset_kind": "zip"},
                {
                    "location": inventory_capture.requested_url,
                    "sha256": inventory_capture.sha256,
                    "bytes": inventory_capture.byte_size,
                    "asset_kind": "json",
                },
            ],
            scope_id=scope["id"],
            complete=True,
        )
        artifact = build_generation(
            output_dir / "generation",
            family=FAMILY,
            files=tables,
            expected_keys=tuple(f"{name}.parquet" for name in SCHEMAS),
            schemas={name: described_schema(schema) for name, schema in SCHEMAS.items()},
            inputs=evidence.inputs(),
            publication_status="complete-family",
            owned_output_root=output_dir,
        )
    except BaseException as error:
        evidence.finish(error)
        raise
    evidence.finish()
    return artifact, evidence.artifact_dir


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact, evidence = build(json.loads(args.selection.read_text()), args.output)
    print(json.dumps({"generation": artifact.pin.as_dict(), "evidence": str(evidence)}, indent=2))


if __name__ == "__main__":
    main()
