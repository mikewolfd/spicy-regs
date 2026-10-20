"""Part B fee-for-service spending at the publisher's HCPCS billing-code grain."""

from datetime import date
import hashlib
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows
from spicy_regs.transforms.cms_drug_spending import json_value, measurement

TABLE = "cms_part_b_drug_spending"
OUTPUT = TABLE + ".parquet"
MEASURES = {
    "total_spending_usd": "Tot_Spndng",
    "dosage_units": "Tot_Dsg_Unts",
    "claims": "Tot_Clms",
    "beneficiaries": "Tot_Benes",
    "spending_per_dosage_unit_usd": "Avg_Spndng_Per_Dsg_Unt",
    "spending_per_claim_usd": "Avg_Spndng_Per_Clm",
    "spending_per_beneficiary_usd": "Avg_Spndng_Per_Bene",
}
SCHEMA = pa.schema(
    [
        ("observation_id", pa.string()),
        ("program", pa.string()),
        ("benefit_year", pa.int32()),
        ("hcpcs_code", pa.string()),
        ("hcpcs_description", pa.string()),
        ("brand_name", pa.string()),
        ("generic_name", pa.string()),
        ("manufacturer_information_status", pa.string()),
        *((name, pa.decimal128(38, 12)) for name in MEASURES),
        ("average_sales_price_payment_usd", pa.decimal128(38, 12)),
        ("outlier_flag_raw", pa.string()),
        ("value_statuses_json", pa.string()),
        ("source_record_id", pa.string()),
        ("source_release_logical_id", pa.string()),
        ("source_release_digest", pa.string()),
        ("source_resources_url", pa.string()),
        ("source_selection_json", pa.string()),
        ("source_locator_json", pa.string()),
        ("raw_row_json", pa.string()),
    ]
)


def measure_fields(year):
    return {
        **{name: f"{prefix}_{year}" for name, prefix in MEASURES.items()},
        "average_sales_price_payment_usd": f"Avg_DY{year % 100:02d}_ASP_Price",
    }


def required_fields(year):
    return {
        "HCPCS_Cd",
        "HCPCS_Desc",
        "Brnd_Name",
        "Gnrc_Name",
        f"Outlier_Flag_{year}",
        *measure_fields(year).values(),
    }


def observation(item, pin, *, year):
    record, raw = item["record"], item["record"]["raw"]
    if not required_fields(year) <= raw.keys():
        raise ValueError("CMS Part B row lacks an identity or selected-year field")
    if any(value is not None and not isinstance(value, str) for value in raw.values()):
        raise ValueError("CMS Part B raw cells must remain literal strings or null")
    if not raw["HCPCS_Cd"]:
        raise ValueError("CMS Part B row lacks its literal HCPCS identity")
    source_id = item["sourceRecordId"]
    if not isinstance(source_id, str) or not source_id:
        raise ValueError("CMS Part B row lacks its admitted source record identity")
    values = {name: measurement(raw[field]) for name, field in measure_fields(year).items()}
    api_url = record["selection"]["api_url"]
    return {
        "observation_id": "sha256:"
        + hashlib.sha256(json_value([pin.artifact_digest, source_id, year]).encode()).hexdigest(),
        "program": "Medicare Part B",
        "benefit_year": year,
        "hcpcs_code": raw["HCPCS_Cd"],
        "hcpcs_description": raw["HCPCS_Desc"],
        "brand_name": raw["Brnd_Name"],
        "generic_name": raw["Gnrc_Name"],
        # The release-linked workbook lists multiple associations per HCPCS.
        # It does not allocate these spending totals to manufacturers.
        "manufacturer_information_status": "not_provided_in_main_table",
        **{name: pair[0] for name, pair in values.items()},
        "outlier_flag_raw": raw[f"Outlier_Flag_{year}"],
        "value_statuses_json": json_value({name: pair[1] for name, pair in values.items()}),
        "source_record_id": source_id,
        "source_release_logical_id": pin.logical_id,
        "source_release_digest": pin.artifact_digest,
        "source_resources_url": api_url.replace("/dataset/", "/dataset-resources/").removesuffix("/data"),
        "source_selection_json": json_value(record["selection"]),
        "source_locator_json": json_value(record["source_locator"]),
        "raw_row_json": json_value(raw),
    }


def observations(reader, *, year):
    outcome = reader.collection_outcome
    selection = outcome["requestedScope"]["selection"]
    if (
        type(year) is not int
        or selection["series_title"] != "Medicare Part B Spending by Drug"
        or date.fromisoformat(selection["release_date"]) != date(year, 1, 1)
    ):
        raise ValueError("Select the Part B release for the explicit benefit year")
    count = selection["total_rows"]
    if (
        type(count) is not int
        or count <= 0
        or outcome["publishedRecordCount"] != count
        or outcome["discoveredRecordCount"] != count
        or outcome["failedRecordCount"] != 0
        or outcome["discardedObservationCount"] != 0
    ):
        raise ValueError("CMS Part B requires a complete, nonempty admitted source population")
    seen_ids, codes = set(), set()
    for item in reader.iter_records():
        if item["record"]["selection"] != selection:
            raise ValueError("CMS Part B record differs from the selected source release")
        row = observation(item, reader.pin, year=year)
        if row["source_record_id"] in seen_ids or row["hcpcs_code"] in codes:
            raise ValueError("Repeated CMS Part B HCPCS or source occurrence")
        seen_ids.add(row["source_record_id"])
        codes.add(row["hcpcs_code"])
        yield row
    if len(seen_ids) != count:
        raise ValueError("CMS Part B source population differs from its declared row count")


def build_cms_part_b(reader, output_dir: Path, *, year: int) -> Path:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return write_rows(observations(reader, year=year), output_dir / OUTPUT, SCHEMA)
