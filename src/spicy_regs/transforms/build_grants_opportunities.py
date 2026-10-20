"""Stream literal Grants.gov snapshot observations into one ordinary table."""

from __future__ import annotations

import json
import re
from datetime import date
from decimal import Decimal, localcontext
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

TABLE = "grants_opportunity_observations"
OUTPUT = TABLE + ".parquet"
NAMESPACE = "{http://apply.grants.gov/system/OpportunityDetail-V1.0}"
REFERENCE = json.loads((Path(__file__).resolve().parents[1] / "reference/refspec/grants-gov-codes.json").read_text())
TEXT_FIELDS = {
    "opportunity_number": "OpportunityNumber",
    "title": "OpportunityTitle",
    "agency_code": "AgencyCode",
    "agency_name": "AgencyName",
    "opportunity_category": "OpportunityCategory",
    "eligibility_text": "AdditionalInformationOnEligibility",
    "description": "Description",
    "close_date_explanation": "CloseDateExplanation",
    "estimated_synopsis_close_date_explanation": "EstimatedSynopsisCloseDateExplanation",
    "cost_sharing_or_matching_requirement": "CostSharingOrMatchingRequirement",
}
LIST_FIELDS = {
    "eligibility_codes_json": "EligibleApplicants",
    "funding_category_codes_json": "CategoryOfFundingActivity",
    "funding_instruments_json": "FundingInstrumentType",
    "assistance_listing_numbers_json": "CFDANumbers",
}
DATE_FIELDS = {
    "post_date": "PostDate",
    "close_date": "CloseDate",
    "last_updated_date": "LastUpdatedDate",
    "archive_date": "ArchiveDate",
    "estimated_synopsis_post_date": "EstimatedSynopsisPostDate",
    "estimated_synopsis_close_date": "EstimatedSynopsisCloseDate",
    "estimated_award_date": "EstimatedAwardDate",
    "estimated_project_start_date": "EstimatedProjectStartDate",
}
AMOUNT_FIELDS = {
    "award_ceiling": "AwardCeiling",
    "award_floor": "AwardFloor",
    "estimated_total_program_funding": "EstimatedTotalProgramFunding",
}
SCHEMA = pa.schema(
    [
        ("snapshot_date", pa.date32()),
        ("notice_type", pa.string()),
        ("opportunity_id", pa.string()),
        ("version", pa.string()),
        *((key, pa.string()) for key in TEXT_FIELDS),
        *((key, pa.string()) for key in LIST_FIELDS),
        ("eligibility_labels_json", pa.string()),
        ("funding_category_labels_json", pa.string()),
        *((key, pa.date32()) for key in DATE_FIELDS),
        *((key, pa.decimal128(38, 2)) for key in AMOUNT_FIELDS),
        ("amount_unit", pa.string()),
        ("value_statuses_json", pa.string()),
        ("source_record_id", pa.string()),
        ("source_release_logical_id", pa.string()),
        ("source_release_digest", pa.string()),
        ("source_ordinal", pa.int64()),
        ("source_locator_json", pa.string()),
        ("raw_fields_json", pa.string()),
        ("vocabulary_source_sha256", pa.string()),
    ]
)


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _date(raw):
    if raw is None or raw == "":
        return None, "missing" if raw is None else "blank"
    if re.fullmatch(r"[0-9]{8}", raw):
        try:
            return date(int(raw[4:]), int(raw[:2]), int(raw[2:4])), "value"
        except ValueError:
            pass
    return None, "unparsed"


def _amount(raw):
    if raw is None or raw == "":
        return None, "missing" if raw is None else "blank"
    if raw == "none":
        return None, "publisher_none"
    if not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", raw):
        return None, "unparsed"
    value = Decimal(raw)
    # Unknown future precision remains literal, never rounded into an amount.
    with localcontext() as context:
        context.prec = max(40, len(raw) + 2)
        if value >= Decimal("1e36") or value != value.quantize(Decimal(".01")):
            return None, "unrepresentable"
    return value, "value"


def _rows(readers):
    releases = set()
    for reader in readers:
        if reader.pin in releases:
            raise ValueError("A Grants source release was supplied twice")
        releases.add(reader.pin)
        outcome = reader.collection_outcome
        if (
            outcome["failedRecordCount"]
            or outcome["discardedObservationCount"]
            or outcome["discoveredRecordCount"] != outcome["publishedRecordCount"]
        ):
            raise ValueError("Grants domain requires the complete accepted snapshot population")
        scope = outcome["requestedScope"]
        snapshot_date = date.fromisoformat(scope["snapshot_date"])
        count = 0
        for item in reader.iter_records():
            record = item["record"]
            if record["snapshot_date"] != scope["snapshot_date"]:
                raise ValueError("Grants record differs from its declared snapshot")
            kind, opportunity_id = record["notice_type"], record["opportunity_id"]
            if kind not in {"synopsis", "forecast"}:
                raise ValueError("Unsupported Grants notice type")
            if item["sourceRecordId"] != f"grants-gov:{kind}:{opportunity_id}":
                raise ValueError("Grants native identity differs from its source record")
            fields = {}
            for field in record["fields"]:
                if field["tag"].startswith(NAMESPACE):
                    fields.setdefault(field["tag"][len(NAMESPACE) :], []).append(field["text"])

            def one(name):
                values = fields.get(name, [])
                if len(values) > 1:
                    raise ValueError(f"Grants scalar field occurs more than once: {name}")
                return values[0] if values else None

            if one("OpportunityID") != opportunity_id or one("Version") != record["version"]:
                raise ValueError("Grants identity/version differs from its literal fields")
            locator = record["source_locator"]
            if (
                locator["response_sha256"] != scope["capture"]["response_sha256"]
                or locator["member_name"] != scope["member_name"]
            ):
                raise ValueError("Grants record differs from its selected source archive")
            typed = {key: _date(one(field)) for key, field in DATE_FIELDS.items()}
            typed.update({key: _amount(one(field)) for key, field in AMOUNT_FIELDS.items()})
            yield {
                "snapshot_date": snapshot_date,
                "notice_type": kind,
                "opportunity_id": opportunity_id,
                "version": record["version"],
                **{key: one(field) for key, field in TEXT_FIELDS.items()},
                **{key: _json(fields.get(field, [])) for key, field in LIST_FIELDS.items()},
                "eligibility_labels_json": _json(
                    [REFERENCE["eligibilities"].get(code) for code in fields.get("EligibleApplicants", [])]
                ),
                "funding_category_labels_json": _json(
                    [REFERENCE["funding_categories"].get(code) for code in fields.get("CategoryOfFundingActivity", [])]
                ),
                **{key: value for key, (value, _) in typed.items()},
                # The selected XML/XSD does not establish a currency for every amount.
                "amount_unit": None,
                "value_statuses_json": _json({key: status for key, (_, status) in typed.items()}),
                "source_record_id": item["sourceRecordId"],
                "source_release_logical_id": reader.pin.logical_id,
                "source_release_digest": reader.pin.artifact_digest,
                "source_ordinal": record["ordinal"],
                "source_locator_json": _json(locator),
                "raw_fields_json": _json(record["fields"]),
                "vocabulary_source_sha256": REFERENCE["source_sha256"],
            }
            count += 1
        if count != outcome["publishedRecordCount"]:
            raise ValueError("Grants records differ from the admitted population")
    if not releases:
        raise ValueError("Declare at least one Grants snapshot release")


def build_grants_opportunities(readers, output_dir: Path) -> Path:
    """Replace the table only after all selected releases are fully consumed."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return write_rows(_rows(readers), output_dir / OUTPUT, SCHEMA)
