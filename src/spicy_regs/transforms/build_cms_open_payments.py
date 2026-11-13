"""Source summary grains stay separate; amounts received never include associations."""

from __future__ import annotations

import json
import re
from decimal import Decimal
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

TABLE = "cms_open_payments_summaries"
SCHEMA = pa.schema(
    [
        ("observation_id", pa.string()),
        ("program_year", pa.int32()),
        ("release", pa.string()),
        ("aggregation_level", pa.string()),
        ("native_id", pa.string()),
        ("name", pa.string()),
        ("recipient_type", pa.string()),
        ("npi", pa.string()),
        ("state", pa.string()),
        ("country_code", pa.string()),
        ("country_name", pa.string()),
        ("state_basis", pa.string()),
        ("payment_category", pa.string()),
        *(
            (name, pa.decimal128(38, 2))
            for name in (
                "general_amount_usd",
                "research_amount_usd",
                "investment_amount_usd",
                "interest_amount_usd",
                "associated_research_amount_usd",
                "category_amount_usd",
            )
        ),
        *(
            (name, pa.int64())
            for name in (
                "general_payment_count",
                "research_payment_count",
                "investment_payment_count",
                "interest_payment_count",
                "associated_research_payment_count",
                "category_payment_count",
                "recipient_count",
            )
        ),
        ("value_statuses_json", pa.string()),
        ("source_record_id", pa.string()),
        ("dataset_id", pa.string()),
        ("source_download_url", pa.string()),
        ("source_catalog_sha256", pa.string()),
        ("source_locator_json", pa.string()),
        ("raw_row_json", pa.string()),
    ]
)
SUFFIXES = {
    "Covered Recipient Physician": "physician",
    "Covered Recipient Non-Physician Practitioner": "non_physician_practitioner",
    "Covered Recipient Teaching Hospital": "teaching_hospital",
}
CATEGORIES = ("General", "Research", "Investment", "Interest")


def json_value(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def measure(raw, *, count=False):
    if raw is None:
        return None, "source_null"
    if raw == "":
        return None, "source_empty"
    pattern = r"[0-9]+" if count else r"-?[0-9]+(?:\.[0-9]{1,2})?"
    if not isinstance(raw, str) or re.fullmatch(pattern, raw) is None:
        raise ValueError("Open Payments measure is not an exact source number")
    value = int(raw) if count else Decimal(raw)
    if (count and value > 2**63 - 1) or (not count and abs(value) >= Decimal(10) ** 36):
        raise ValueError("Open Payments measure exceeds its declared table type")
    return value, "reported"


def observation(item):
    raw, selected, locator = item["raw"], item["selection"], item["source_locator"]
    level = selected["aggregation_level"]
    result = dict.fromkeys(SCHEMA.names)
    result.update(
        observation_id=locator["source_record_id"],
        program_year=selected["program_year"],
        release=selected["release"],
        aggregation_level=level,
        source_record_id=locator["source_record_id"],
        dataset_id=selected["dataset_id"],
        source_download_url=selected["download_url"],
        source_catalog_sha256=selected["catalog_sha256"],
        source_locator_json=json_value(locator),
        raw_row_json=json_value(raw),
        payment_category="separate_columns",
    )
    values = {}
    if level == "entities":
        result.update(
            native_id=raw["amgpo_making_payment_id"],
            name=raw["amgpo_making_payment_name"],
            state=raw["amgpo_making_payment_state"],
            country_name=raw["amgpo_making_payment_country"],
            state_basis="reporting_entity_address",
        )
        values = {
            "general_amount_usd": "total_amount_general",
            "research_amount_usd": "total_amount_research",
            "investment_amount_usd": "total_amount_investment",
            "interest_amount_usd": "total_amount_interest",
            "general_payment_count": "trans_general",
            "research_payment_count": "trans_research",
            "investment_payment_count": "trans_invested",
        }
    elif level in {"recipients", "hospitals"}:
        if level == "recipients":
            result.update(
                native_id=raw["covered_recipient_profile_id"],
                name=" ".join(
                    raw[k]
                    for k in ("covered_recipient_profile_first_name", "covered_recipient_profile_last_name")
                    if raw[k]
                ),
                recipient_type=raw["covered_recipient_profile_type"],
                npi=raw["covered_recipient_npi"],
                state=raw["covered_recipient_profile_state"],
                country_name=raw["covered_recipient_profile_country_name"],
                state_basis="recipient_profile_address",
            )
            values = {
                "investment_amount_usd": "invested_total_amount",
                "interest_amount_usd": "interest_total_amount",
                "associated_research_amount_usd": "total_associated_research_payments",
                "investment_payment_count": "invested_total_transactions",
                "interest_payment_count": "interest_total_transactions",
                "associated_research_payment_count": "total_associated_research_transactions",
            }
        else:
            result.update(
                native_id=raw["teaching_hospital_ccn"],
                name=raw["teaching_hospital_name"],
                recipient_type="Covered Recipient Teaching Hospital",
                state=raw["teaching_hospital_state"],
                state_basis="teaching_hospital_address",
            )
        values.update(
            general_amount_usd="general_total_payment",
            research_amount_usd="research_total_payment",
            general_payment_count="general_total_transactions",
            research_payment_count="research_total_transactions",
        )
    elif level in {"states", "national"}:
        recipient_type = raw["recipient_type"]
        if recipient_type not in SUFFIXES or raw["payment_type"] not in CATEGORIES:
            raise ValueError("Unqualified Open Payments recipient type or payment category")
        suffix = SUFFIXES[recipient_type]
        count_suffix = {
            "physician": "physicians",
            "non_physician_practitioner": "non_physician_practitioners",
            "teaching_hospital": "teaching_hospitals",
        }[suffix]
        # CMS spells teachinghospital without an underscore in amount/count cells.
        amount_suffix = "teaching_hospital" if f"total_payment_amount_{suffix}" in raw else "teachinghospital"
        suffix = suffix if suffix != "teaching_hospital" else amount_suffix
        result.update(
            native_id=json_value([raw.get("country_code"), raw.get("state_code"), raw["payment_type"], recipient_type]),
            name=raw.get("state_name", raw.get("metric_level")),
            recipient_type=recipient_type,
            state=raw.get("state_code"),
            country_code=raw.get("country_code"),
            country_name=raw.get("country_name"),
            state_basis="publisher_recipient_state" if level == "states" else None,
            payment_category=raw["payment_type"],
        )
        values = {
            "category_amount_usd": f"total_payment_amount_{suffix}",
            "category_payment_count": f"total_payment_count_{suffix}",
            "recipient_count": f"total_number_of_{count_suffix}",
        }
    else:
        raise ValueError("Unsupported Open Payments summary level")
    statuses = {}
    for name, field in values.items():
        if field not in raw:
            raise ValueError(f"Open Payments row omitted required measure {field}")
        result[name], statuses[name] = measure(raw[field], count=name.endswith("_count"))
    result["value_statuses_json"] = json_value(statuses)
    return result


def build_cms_open_payments(summaries, output_dir: Path, *, year, release):
    """Write only after each selected complete family has passed its owning reader."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    def rows():
        for item in summaries:
            selected = item["selection"]
            if selected["program_year"] != year or selected["release"] != release:
                raise ValueError("Open Payments release or program year mismatch")
            yield observation(item)

    return write_rows(rows(), output_dir / (TABLE + ".parquet"), SCHEMA)


def compare_payments(connection, *, level, year, release, category, recipient_type=None, limit=10):
    """Compare one source grain/category only; refuse mismatched cohorts explicitly."""
    if level not in {"entities", "recipients", "hospitals", "states", "national"} or category not in CATEGORIES:
        raise ValueError("Unsupported Open Payments aggregation level or payment category")
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("Select a bounded comparison")
    found = connection.execute(f"SELECT DISTINCT program_year, release FROM {TABLE}").fetchall()
    if found != [(year, release)]:
        raise ValueError("Open Payments release or program year mismatch")
    clauses, params = ["aggregation_level = ?"], [level]
    amount = category.lower() + "_amount_usd"
    if level in {"states", "national"}:
        if recipient_type not in SUFFIXES:
            raise ValueError("Select one source recipient type for a state or national comparison")
        clauses.extend(["payment_category = ?", "recipient_type = ?"])
        params.extend([category, recipient_type])
        amount = "category_amount_usd"
    elif recipient_type is not None:
        clauses.append("recipient_type = ?")
        params.append(recipient_type)
    available = connection.execute(
        f"SELECT count({amount}) FROM {TABLE} WHERE " + " AND ".join(clauses), params
    ).fetchone()[0]
    if not available:
        raise ValueError("Requested Open Payments category/population is unavailable, not zero")
    return connection.execute(
        f"SELECT native_id, name, state, recipient_type, {amount} AS amount_usd, source_locator_json "
        f"FROM {TABLE} WHERE " + " AND ".join(clauses) + f" ORDER BY {amount} DESC NULLS LAST, native_id LIMIT ?",
        [*params, limit],
    ).fetchall()
