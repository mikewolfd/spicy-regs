"""Source-faithful SAM public file observations, separate from entity registrations.

Rows retain every native CSV cell and its original locator. Repeated native IDs
and amendments survive. Snapshot replacement never edits the registration flag.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

COMMON = ["snapshot_id", "native_id", "source_url", "source_sha256", "observed_at", "snapshot_date",
          "member_name", "member_sha256", "source_locator_json", "raw_fields_json", "record_content_sha256"]
EXCLUSION_FIELDS = {
    "classification_raw": "Classification", "name_raw": "Name", "prefix_raw": "Prefix", "first_name_raw": "First",
    "middle_name_raw": "Middle", "last_name_raw": "Last", "suffix_raw": "Suffix", "uei": "Unique Entity ID",
    "cage_code": "CAGE", "npi": "NPI", "exclusion_program_raw": "Exclusion Program",
    "excluding_agency_raw": "Excluding Agency", "ct_code_raw": "CT Code", "exclusion_type_raw": "Exclusion Type",
    "additional_comments_raw": "Additional Comments", "active_date_raw": "Active Date",
    "termination_date_raw": "Termination Date", "record_status_raw": "Record Status",
    "cross_reference_raw": "Cross-Reference", "creation_date_raw": "Creation_Date",
}
NOTICE_FIELDS = {
    "title": "Title", "solicitation_number": "Sol#", "department_raw": "Department/Ind.Agency",
    "agency_code_raw": "CGAC", "sub_tier_raw": "Sub-Tier", "office_raw": "Office", "office_code_raw": "AAC Code",
    "posted_date_raw": "PostedDate", "notice_type_raw": "Type", "base_type_raw": "BaseType",
    "response_deadline_raw": "ResponseDeadLine", "set_aside_code_raw": "SetASideCode",
    "set_aside_raw": "SetASide", "naics_code_raw": "NaicsCode", "classification_code_raw": "ClassificationCode",
    "active_raw": "Active", "archive_date_raw": "ArchiveDate", "source_link": "Link",
    "additional_info_link": "AdditionalInfoLink", "description_raw": "Description",
}
TABLES = {"exclusions": "sam_exclusion_observations", "opportunities": "sam_opportunity_observations"}
FIELDS = {"exclusions": EXCLUSION_FIELDS, "opportunities": NOTICE_FIELDS}
SCHEMAS = {
    TABLES[kind]: pa.schema([*((name, pa.string()) for name in [*COMMON, *columns]),
                            ("source_ordinal", pa.int64()), ("source_line_start", pa.int64()),
                            ("source_line_end", pa.int64())])
    for kind, columns in FIELDS.items()
}
SNAPSHOTS = {"exclusions": "sam_exclusion_snapshots", "opportunities": "sam_opportunity_snapshots"}
SNAPSHOT_SCHEMA = pa.schema([
    *((name, pa.string()) for name in ["snapshot_id", "source_kind", "source_url", "source_sha256", "observed_at",
                                      "snapshot_date", "selection_json", "coverage_limit"]),
    ("source_rows", pa.int64()), ("selected_rows", pa.int64()), ("distinct_native_ids", pa.int64()),
])
SCHEMAS.update({name: SNAPSHOT_SCHEMA for name in SNAPSHOTS.values()})


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def validate_selection(kind, selection):
    if kind == "exclusions":
        if selection != {"population": "all-rows-in-public-extract"}:
            raise ValueError("SAM exclusions require the complete selected public extract")
        return
    if kind != "opportunities" or set(selection) != {"agency_code", "posted_from", "posted_to"}:
        raise ValueError("SAM notice selection requires agency_code, posted_from and posted_to")
    if not isinstance(selection["agency_code"], str) or re.fullmatch(r"[0-9]{3}", selection["agency_code"]) is None:
        raise ValueError("SAM public CSV agency_code must be a three-digit CGAC")
    first, last = (date.fromisoformat(selection[key]) for key in ("posted_from", "posted_to"))
    if last < first or (last - first).days > 365:
        raise ValueError("SAM notice date window is reversed or exceeds 365 days")


def _selected(fields, selection):
    if fields["CGAC"].zfill(3) != selection["agency_code"]:
        return False
    # The file's printed calendar date, with no guessed time zone conversion.
    try:
        posted = date.fromisoformat(fields["PostedDate"][:10])
    except ValueError as error:
        raise ValueError("Selected SAM agency has a missing or unrecognized posted date") from error
    return selection["posted_from"] <= posted.isoformat() <= selection["posted_to"]


def validate_capture(capture):
    if set(capture) != {"request_url", "response_sha256", "observed_at", "byte_size"}:
        raise ValueError("SAM capture descriptor fields differ")
    if re.fullmatch(r"sha256:[0-9a-f]{64}", capture["response_sha256"]) is None:
        raise ValueError("SAM capture needs an exact SHA-256")
    if type(capture["byte_size"]) is not int or capture["byte_size"] < 1:
        raise ValueError("SAM capture needs a positive byte size")
    if datetime.fromisoformat(capture["observed_at"].replace("Z", "+00:00")).utcoffset() is None:
        raise ValueError("SAM capture needs a time zone")


def build_sam_source(records, output_dir: Path, *, kind, capture, selection, snapshot_date=None):
    """Exhaust and atomically write one pinned original, plus its population summary.

    The caller checks original size/digest and journals the exact input before
    supplying records. A parse failure leaves any prior destination unchanged.
    """
    validate_selection(kind, selection)
    validate_capture(capture)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    snapshot_id = "sha256:" + hashlib.sha256(_json({"kind": kind, "capture": capture, "selection": selection}).encode()).hexdigest()
    source_count, selected_count, identities = 0, 0, set()

    def rows():
        nonlocal source_count, selected_count
        for item in records:
            if item["ordinal"] != source_count:
                raise ValueError("SAM original record ordinals are not contiguous")
            source_count += 1
            fields = item["fields"]
            if kind == "opportunities" and not _selected(fields, selection):
                continue
            selected_count += 1
            identities.add(item["native_id"])
            raw = _json(fields)
            yield {
                "snapshot_id": snapshot_id, "native_id": item["native_id"],
                "source_url": capture["request_url"], "source_sha256": capture["response_sha256"],
                "observed_at": capture["observed_at"], "snapshot_date": snapshot_date,
                "member_name": item.get("member_name"), "member_sha256": item.get("member_sha256"),
                "source_ordinal": item["ordinal"], "source_line_start": item["line_start"],
                "source_line_end": item["line_end"],
                "source_locator_json": _json({key: item[key] for key in ["ordinal", "line_start", "line_end"]}),
                "raw_fields_json": raw, "record_content_sha256": "sha256:" + hashlib.sha256(raw.encode()).hexdigest(),
                **{column: fields.get(native) for column, native in FIELDS[kind].items()},
            }

    table = TABLES[kind]
    path = write_rows(rows(), output_dir / (table + ".parquet"), SCHEMAS[table])
    summary = {
        "snapshot_id": snapshot_id, "source_kind": kind, "source_url": capture["request_url"],
        "source_sha256": capture["response_sha256"], "observed_at": capture["observed_at"],
        "snapshot_date": snapshot_date, "selection_json": _json(selection), "source_rows": source_count,
        "selected_rows": selected_count, "distinct_native_ids": len(identities),
        "coverage_limit": ("All rows of the selected public active-exclusions file; no inactive history or registration inference."
                           if kind == "exclusions" else
                           "Declared agency/posted-date rows in one public file; amendments retained, no all-version or attachment custody claim."),
    }
    summary_path = write_rows([summary], output_dir / (SNAPSHOTS[kind] + ".parquet"), SNAPSHOT_SCHEMA)
    return [path, summary_path], summary
