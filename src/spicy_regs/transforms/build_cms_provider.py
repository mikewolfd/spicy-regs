"""Source-defined annual provider utilization and payments; no cross-grain rollup."""

import hashlib
import re
from datetime import date
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows
from spicy_regs.transforms.cms_drug_spending import json_value, measurement

TABLE = "cms_medicare_provider"
OUTPUT = TABLE + ".parquet"
CONTEXT = {
    "npi": "Rndrng_NPI",
    "last_or_organization_name": "Rndrng_Prvdr_Last_Org_Name",
    "first_name": "Rndrng_Prvdr_First_Name",
    "entity_type": "Rndrng_Prvdr_Ent_Cd",
    "specialty": "Rndrng_Prvdr_Type",
    "city": "Rndrng_Prvdr_City",
    "state": "Rndrng_Prvdr_State_Abrvtn",
    "state_fips": "Rndrng_Prvdr_State_FIPS",
    "zip5": "Rndrng_Prvdr_Zip5",
    "country": "Rndrng_Prvdr_Cntry",
    "ruca": "Rndrng_Prvdr_RUCA",
    "ruca_description": "Rndrng_Prvdr_RUCA_Desc",
    "medicare_participating_indicator": "Rndrng_Prvdr_Mdcr_Prtcptg_Ind",
}
# Distinct source-defined subsets; never append them as extra provider rows.
BASE = {
    "hcpcs_codes": "Tot_HCPCS_Cds",
    "beneficiaries": "Tot_Benes",
    "services": "Tot_Srvcs",
    "submitted_charges_usd": "Tot_Sbmtd_Chrg",
    "allowed_amount_usd": "Tot_Mdcr_Alowd_Amt",
    "medicare_payment_usd": "Tot_Mdcr_Pymt_Amt",
    "standardized_payment_usd": "Tot_Mdcr_Stdzd_Amt",
}
MEASURES = dict(BASE)
for prefix, source in (("drug", "Drug"), ("medical", "Med")):
    MEASURES.update(
        {
            prefix + "_" + name: source + "_" + field.removeprefix("Tot_")
            if field.startswith("Tot_") and name.endswith("_usd")
            else source + "_" + field
            for name, field in BASE.items()
        }
    )
SCHEMA = pa.schema(
    [
        ("observation_id", pa.string()),
        ("program", pa.string()),
        ("benefit_year", pa.int32()),
        ("aggregation_level", pa.string()),
        *((name, pa.string()) for name in CONTEXT),
        *((name, pa.decimal128(38, 12)) for name in MEASURES),
        ("drug_suppression_indicator", pa.string()),
        ("medical_suppression_indicator", pa.string()),
        ("value_statuses_json", pa.string()),
        ("source_selection_digest", pa.string()),
        ("source_selection_json", pa.string()),
        ("source_locator_json", pa.string()),
        ("raw_row_json", pa.string()),
    ]
)


def required_fields():
    return {*CONTEXT.values(), *MEASURES.values(), "Drug_Sprsn_Ind", "Med_Sprsn_Ind"}


def observation(record, *, year, selection_digest):
    raw = record["raw"]
    if not required_fields() <= raw.keys() or any(v is not None and not isinstance(v, str) for v in raw.values()):
        raise ValueError("CMS provider row lacks literal required fields")
    if not isinstance(raw["Rndrng_NPI"], str) or re.fullmatch(r"[0-9]{10}", raw["Rndrng_NPI"]) is None:
        raise ValueError("CMS provider row lacks its native NPI")
    indicators = {p: raw[f] for p, f in (("drug", "Drug_Sprsn_Ind"), ("medical", "Med_Sprsn_Ind"))}
    if any(flag not in ("", "*", "#", None) for flag in indicators.values()):
        raise ValueError("CMS provider suppression indicator changed")
    values = {}
    for name, field in MEASURES.items():
        group = name.split("_", 1)[0]
        flag = indicators.get(group)
        if flag in ("*", "#"):
            values[name] = (None, "suppressed_lt11" if flag == "*" else "counter_suppressed")
        else:
            value, status = measurement(raw[field])
            if status in {"non_numeric", "unrepresentable"}:
                raise ValueError("CMS provider measure syntax or precision changed")
            if (
                value is not None
                and name.endswith(("beneficiaries", "hcpcs_codes"))
                and value != value.to_integral_value()
            ):
                raise ValueError("CMS provider count is not integral")
            values[name] = value, status
    return {
        "observation_id": "sha256:"
        + hashlib.sha256(json_value([selection_digest, raw["Rndrng_NPI"], year]).encode()).hexdigest(),
        "program": "Original Medicare fee-for-service",
        "benefit_year": year,
        "aggregation_level": "provider",
        **{name: raw[field] for name, field in CONTEXT.items()},
        **{name: pair[0] for name, pair in values.items()},
        "drug_suppression_indicator": indicators["drug"],
        "medical_suppression_indicator": indicators["medical"],
        "value_statuses_json": json_value({name: pair[1] for name, pair in values.items()}),
        "source_selection_digest": selection_digest,
        "source_selection_json": json_value(record["selection"]),
        "source_locator_json": json_value(record["source_locator"]),
        "raw_row_json": json_value(raw),
    }


def build_cms_provider(records, output_dir: Path, *, scope, year: int):
    from spicy_docs.sources.cms import PROVIDER_SERIES
    from spicy_docs.sources.cms_provider import validate_scope

    validate_scope(scope)
    selection = scope["selection"]
    if (
        type(year) is not int
        or selection["series_title"] != PROVIDER_SERIES
        or date.fromisoformat(selection["release_date"]) != date(year, 1, 1)
    ):
        raise ValueError("Select the explicit CMS provider benefit year")
    digest = "sha256:" + hashlib.sha256(json_value(scope).encode()).hexdigest()

    def rows():
        seen = set()
        for record in records:
            if record["selection"] != selection:
                raise ValueError("CMS provider record selection differs")
            row = observation(record, year=year, selection_digest=digest)
            if row["npi"] in seen:
                raise ValueError("Repeated CMS provider NPI in one release")
            seen.add(row["npi"])
            yield row
        if len(seen) != selection["total_rows"]:
            raise ValueError("CMS provider population differs")

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    return write_rows(rows(), output_dir / OUTPUT, SCHEMA)
