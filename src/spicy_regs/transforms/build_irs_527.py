"""Streaming IRS Form 8871/8872 (Section 527) transformation into typed Parquet tables.

Streams ``PolOrgsFullData.zip`` via ``spicy_docs.sources.irs_527.iter_archive_records``
without loading the full archive or decoded text into memory, preserving literal
date strings, amount strings, and ISO-8859-1 (latin-1) text verbatim alongside
parsed typed columns.
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, InvalidOperation, localcontext
from pathlib import Path
import re
from tempfile import TemporaryDirectory
from typing import BinaryIO

import pyarrow as pa
import pyarrow.parquet as pq

TRANSMISSIONS = "irs527_transmissions"
FORM8871 = "irs527_form8871"
FORM8872 = "irs527_form8872"
DIRECTORS = "irs527_directors"
RELATED_ENTITIES = "irs527_related_entities"
EAINS = "irs527_eains"
CONTRIBUTIONS = "irs527_contributions"
EXPENDITURES = "irs527_expenditures"

_PROVENANCE_FIELDS = [
    ("line_number", pa.int64()),
    ("source_sha256", pa.string()),
    ("source_url", pa.string()),
    ("observed_at", pa.string()),
]

_MONEY = pa.decimal128(38, 2)
_DECIMAL_TEXT = re.compile(r"[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)\Z")


def _str_fields(*names: str) -> list[tuple[str, pa.DataType]]:
    return [(name, pa.string()) for name in names]


TRANSMISSIONS_SCHEMA = pa.schema([
    *_str_fields("transmission_date_raw", "transmission_time_raw", "file_id_modifier"),
    ("transmission_date", pa.date32()),
    ("transmission_date_status", pa.string()),
    ("footer_transmission_date_raw", pa.string()),
    ("footer_transmission_time_raw", pa.string()),
    ("footer_record_count", pa.int64()),
    ("observed_data_records", pa.int64()),
    ("source_sha256", pa.string()),
    ("source_url", pa.string()),
    ("observed_at", pa.string()),
])

FORM8871_SCHEMA = pa.schema([
    *_str_fields(
        "form_type",
        "form_id_number",
        "initial_report_indicator",
        "amended_report_indicator",
        "final_report_indicator",
        "ein",
        "organization_name",
        "mailing_address_1",
        "mailing_address_2",
        "mailing_address_city",
        "mailing_address_state",
        "mailing_address_zip_code",
        "mailing_address_zip_ext",
        "email_address",
        "established_date_raw",
    ),
    ("established_date", pa.date32()),
    ("established_date_status", pa.string()),
    *_str_fields(
        "custodian_name",
        "custodian_address_1",
        "custodian_address_2",
        "custodian_address_city",
        "custodian_address_state",
        "custodian_address_zip_code",
        "custodian_address_zip_ext",
        "contact_person_name",
        "contact_address_1",
        "contact_address_2",
        "contact_address_city",
        "contact_address_state",
        "contact_address_zip_code",
        "contact_address_zip_ext",
        "business_address_1",
        "business_address_2",
        "business_address_city",
        "business_address_state",
        "business_address_zip_code",
        "business_address_zip_ext",
        "exempt_8872_indicator",
        "exempt_state",
        "exempt_990_indicator",
        "purpose",
        "material_change_date_raw",
    ),
    ("material_change_date", pa.date32()),
    ("material_change_date_status", pa.string()),
    *_str_fields("insert_datetime_raw", "related_entity_bypass", "eain_bypass"),
    *_PROVENANCE_FIELDS,
])

FORM8872_SCHEMA = pa.schema([
    *_str_fields("form_type", "form_id_number", "period_begin_date_raw"),
    ("period_begin_date", pa.date32()),
    ("period_begin_date_status", pa.string()),
    ("period_end_date_raw", pa.string()),
    ("period_end_date", pa.date32()),
    ("period_end_date_status", pa.string()),
    *_str_fields(
        "initial_report_indicator",
        "amended_report_indicator",
        "final_report_indicator",
        "change_of_address_indicator",
        "organization_name",
        "ein",
        "mailing_address_1",
        "mailing_address_2",
        "mailing_address_city",
        "mailing_address_state",
        "mailing_address_zip_code",
        "mailing_address_zip_ext",
        "email_address",
        "org_formation_date_raw",
    ),
    ("org_formation_date", pa.date32()),
    ("org_formation_date_status", pa.string()),
    *_str_fields(
        "custodian_name",
        "custodian_address_1",
        "custodian_address_2",
        "custodian_address_city",
        "custodian_address_state",
        "custodian_address_zip_code",
        "custodian_address_zip_ext",
        "contact_person_name",
        "contact_address_1",
        "contact_address_2",
        "contact_address_city",
        "contact_address_state",
        "contact_address_zip_code",
        "contact_address_zip_ext",
        "business_address_1",
        "business_address_2",
        "business_address_city",
        "business_address_state",
        "business_address_zip_code",
        "business_address_zip_ext",
        "qtr_indicator",
        "monthly_rpt_month",
        "pre_elect_type",
        "pre_or_post_elect_date_raw",
    ),
    ("pre_or_post_elect_date", pa.date32()),
    ("pre_or_post_elect_date_status", pa.string()),
    ("pre_or_post_elect_state", pa.string()),
    ("sched_a_ind", pa.string()),
    ("total_sched_a_raw", pa.string()),
    ("total_sched_a", _MONEY),
    ("total_sched_a_status", pa.string()),
    ("sched_b_ind", pa.string()),
    ("total_sched_b_raw", pa.string()),
    ("total_sched_b", _MONEY),
    ("total_sched_b_status", pa.string()),
    ("insert_datetime_raw", pa.string()),
    *_PROVENANCE_FIELDS,
])

DIRECTORS_SCHEMA = pa.schema([
    *_str_fields(
        "form_id_number",
        "director_id",
        "org_name",
        "ein",
        "entity_name",
        "entity_title",
        "entity_address_1",
        "entity_address_2",
        "entity_address_city",
        "entity_address_state",
        "entity_address_zip_code",
        "entity_address_zip_code_ext",
    ),
    *_PROVENANCE_FIELDS,
])

RELATED_ENTITIES_SCHEMA = pa.schema([
    *_str_fields(
        "form_id_number",
        "entity_id",
        "org_name",
        "ein",
        "entity_name",
        "entity_relationship",
        "entity_address_1",
        "entity_address_2",
        "entity_address_city",
        "entity_address_state",
        "entity_address_zip_code",
        "entity_address_zip_ext",
    ),
    *_PROVENANCE_FIELDS,
])

EAINS_SCHEMA = pa.schema([
    *_str_fields("form_id_number", "eain_id", "election_authority_id_number", "state_issued"),
    *_PROVENANCE_FIELDS,
])

CONTRIBUTIONS_SCHEMA = pa.schema([
    *_str_fields(
        "form_id_number",
        "sched_a_id",
        "org_name",
        "ein",
        "contributor_name",
        "contributor_address_1",
        "contributor_address_2",
        "contributor_address_city",
        "contributor_address_state",
        "contributor_address_zip_code",
        "contributor_address_zip_ext",
        "contributor_employer",
        "contribution_amount_raw",
    ),
    ("contribution_amount", _MONEY),
    ("contribution_amount_status", pa.string()),
    ("contributor_occupation", pa.string()),
    ("agg_contribution_ytd_raw", pa.string()),
    ("agg_contribution_ytd", _MONEY),
    ("agg_contribution_ytd_status", pa.string()),
    ("contribution_date_raw", pa.string()),
    ("contribution_date", pa.date32()),
    ("contribution_date_status", pa.string()),
    *_PROVENANCE_FIELDS,
])

EXPENDITURES_SCHEMA = pa.schema([
    *_str_fields(
        "form_id_number",
        "sched_b_id",
        "org_name",
        "ein",
        "recipient_name",
        "recipient_address_1",
        "recipient_address_2",
        "recipient_address_city",
        "recipient_address_state",
        "recipient_address_zip_code",
        "recipient_address_zip_ext",
        "recipient_employer",
        "expenditure_amount_raw",
    ),
    ("expenditure_amount", _MONEY),
    ("expenditure_amount_status", pa.string()),
    ("recipient_occupation", pa.string()),
    ("expenditure_date_raw", pa.string()),
    ("expenditure_date", pa.date32()),
    ("expenditure_date_status", pa.string()),
    ("expenditure_purpose", pa.string()),
    *_PROVENANCE_FIELDS,
])

SCHEMAS = {
    TRANSMISSIONS: TRANSMISSIONS_SCHEMA,
    FORM8871: FORM8871_SCHEMA,
    FORM8872: FORM8872_SCHEMA,
    DIRECTORS: DIRECTORS_SCHEMA,
    RELATED_ENTITIES: RELATED_ENTITIES_SCHEMA,
    EAINS: EAINS_SCHEMA,
    CONTRIBUTIONS: CONTRIBUTIONS_SCHEMA,
    EXPENDITURES: EXPENDITURES_SCHEMA,
}


def _parse_date(raw: str) -> tuple[date | None, str]:
    text = raw.strip()
    if not text:
        return None, "blank"
    if re.fullmatch(r"[0-9]{8}", text):
        try:
            return datetime.strptime(text, "%Y%m%d").date(), "parsed"
        except ValueError:
            return None, "unparsed"
    return None, "unparsed"


def _parse_amount(raw: str) -> tuple[Decimal | None, str]:
    text = raw.strip()
    if not text:
        return None, "blank"
    cleaned = text.replace(",", "")
    if _DECIMAL_TEXT.fullmatch(cleaned) is None:
        return None, "unparsed"
    try:
        value = Decimal(cleaned)
    except InvalidOperation:
        return None, "unparsed"
    with localcontext() as ctx:
        ctx.prec = 44
        if value != value.quantize(Decimal("0.01")):
            return None, "exceeds_scale"
    return value, "parsed"


def _row_for_1(fields: dict[str, str], provenance: dict) -> dict:
    est, est_status = _parse_date(fields["established_date"])
    mat, mat_status = _parse_date(fields["material_change_date"])
    return {
        **{k: v for k, v in fields.items() if k not in {"record_type", "established_date", "material_change_date", "insert_datetime"}},
        "established_date_raw": fields["established_date"],
        "established_date": est,
        "established_date_status": est_status,
        "material_change_date_raw": fields["material_change_date"],
        "material_change_date": mat,
        "material_change_date_status": mat_status,
        "insert_datetime_raw": fields["insert_datetime"],
        **provenance,
    }


def _row_for_2(fields: dict[str, str], provenance: dict) -> dict:
    p_begin, p_begin_status = _parse_date(fields["period_begin_date"])
    p_end, p_end_status = _parse_date(fields["period_end_date"])
    org_form, org_form_status = _parse_date(fields["org_formation_date"])
    elect, elect_status = _parse_date(fields["pre_or_post_elect_date"])
    tot_a, tot_a_status = _parse_amount(fields["total_sched_a"])
    tot_b, tot_b_status = _parse_amount(fields["total_sched_b"])
    excluded = {
        "record_type",
        "period_begin_date",
        "period_end_date",
        "org_formation_date",
        "pre_or_post_elect_date",
        "total_sched_a",
        "total_sched_b",
        "insert_datetime",
    }
    return {
        **{k: v for k, v in fields.items() if k not in excluded},
        "period_begin_date_raw": fields["period_begin_date"],
        "period_begin_date": p_begin,
        "period_begin_date_status": p_begin_status,
        "period_end_date_raw": fields["period_end_date"],
        "period_end_date": p_end,
        "period_end_date_status": p_end_status,
        "org_formation_date_raw": fields["org_formation_date"],
        "org_formation_date": org_form,
        "org_formation_date_status": org_form_status,
        "pre_or_post_elect_date_raw": fields["pre_or_post_elect_date"],
        "pre_or_post_elect_date": elect,
        "pre_or_post_elect_date_status": elect_status,
        "total_sched_a_raw": fields["total_sched_a"],
        "total_sched_a": tot_a,
        "total_sched_a_status": tot_a_status,
        "total_sched_b_raw": fields["total_sched_b"],
        "total_sched_b": tot_b,
        "total_sched_b_status": tot_b_status,
        "insert_datetime_raw": fields["insert_datetime"],
        **provenance,
    }


def _row_for_a(fields: dict[str, str], provenance: dict) -> dict:
    amt, amt_status = _parse_amount(fields["contribution_amount"])
    ytd, ytd_status = _parse_amount(fields["agg_contribution_ytd"])
    dt, dt_status = _parse_date(fields["contribution_date"])
    excluded = {"record_type", "contribution_amount", "agg_contribution_ytd", "contribution_date"}
    return {
        **{k: v for k, v in fields.items() if k not in excluded},
        "contribution_amount_raw": fields["contribution_amount"],
        "contribution_amount": amt,
        "contribution_amount_status": amt_status,
        "agg_contribution_ytd_raw": fields["agg_contribution_ytd"],
        "agg_contribution_ytd": ytd,
        "agg_contribution_ytd_status": ytd_status,
        "contribution_date_raw": fields["contribution_date"],
        "contribution_date": dt,
        "contribution_date_status": dt_status,
        **provenance,
    }


def _row_for_b(fields: dict[str, str], provenance: dict) -> dict:
    amt, amt_status = _parse_amount(fields["expenditure_amount"])
    dt, dt_status = _parse_date(fields["expenditure_date"])
    excluded = {"record_type", "expenditure_amount", "expenditure_date"}
    return {
        **{k: v for k, v in fields.items() if k not in excluded},
        "expenditure_amount_raw": fields["expenditure_amount"],
        "expenditure_amount": amt,
        "expenditure_amount_status": amt_status,
        "expenditure_date_raw": fields["expenditure_date"],
        "expenditure_date": dt,
        "expenditure_date_status": dt_status,
        **provenance,
    }


def _row_plain(fields: dict[str, str], provenance: dict) -> dict:
    return {**{k: v for k, v in fields.items() if k != "record_type"}, **provenance}


_TABLE_DISPATCH = {
    "1": (FORM8871, _row_for_1),
    "2": (FORM8872, _row_for_2),
    "D": (DIRECTORS, _row_plain),
    "R": (RELATED_ENTITIES, _row_plain),
    "E": (EAINS, _row_plain),
    "A": (CONTRIBUTIONS, _row_for_a),
    "B": (EXPENDITURES, _row_for_b),
}


def build_irs_527(
    archive_source: Path | str | BinaryIO | bytes,
    output_dir: Path,
    *,
    source_sha256: str,
    source_url: str,
    observed_at: str,
    batch_size: int = 2_000,
) -> tuple[Path, ...]:
    """Stream ``PolOrgsFullData.zip`` into all 8 typed Parquet tables atomically."""
    from spicy_docs.sources.irs_527 import Irs527MalformedRow, iter_archive_records

    if type(batch_size) is not int or batch_size <= 0:
        raise ValueError("batch_size must be a positive integer")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    header_fields: dict[str, str] | None = None
    footer_fields: dict[str, str] | None = None
    data_records = 0

    with TemporaryDirectory(dir=output_dir) as scratch:
        scratch_dir = Path(scratch)
        writers = {
            name: pq.ParquetWriter(scratch_dir / f"{name}.parquet", schema, compression="zstd")
            for name, schema in SCHEMAS.items()
        }
        buffers: dict[str, list[dict]] = {name: [] for name in SCHEMAS}
        try:
            def flush(name: str) -> None:
                if buffers[name]:
                    writers[name].write_table(pa.Table.from_pylist(buffers[name], schema=SCHEMAS[name]))
                    buffers[name].clear()

            for record in iter_archive_records(archive_source):
                if isinstance(record, Irs527MalformedRow):
                    raise ValueError(f"Malformed IRS 527 record at line {record.line_number}: {record.reason}")
                if record.record_type == "H":
                    header_fields = record.fields
                    continue
                if record.record_type == "F":
                    footer_fields = record.fields
                    continue
                table_name, row_fn = _TABLE_DISPATCH[record.record_type]
                provenance = {
                    "line_number": record.line_number,
                    "source_sha256": source_sha256,
                    "source_url": source_url,
                    "observed_at": observed_at,
                }
                buffers[table_name].append(row_fn(record.fields, provenance))
                data_records += 1
                if len(buffers[table_name]) >= batch_size:
                    flush(table_name)

            if header_fields is None or footer_fields is None:
                raise ValueError("IRS 527 archive omitted required H header or F footer")
            tx_date, tx_date_status = _parse_date(header_fields["transmission_date"])
            buffers[TRANSMISSIONS].append({
                "transmission_date_raw": header_fields["transmission_date"],
                "transmission_time_raw": header_fields["transmission_time"],
                "file_id_modifier": header_fields["file_id_modifier"],
                "transmission_date": tx_date,
                "transmission_date_status": tx_date_status,
                "footer_transmission_date_raw": footer_fields["transmission_date"],
                "footer_transmission_time_raw": footer_fields["transmission_time"],
                "footer_record_count": int(footer_fields["record_count"]),
                "observed_data_records": data_records,
                "source_sha256": source_sha256,
                "source_url": source_url,
                "observed_at": observed_at,
            })
            for name in SCHEMAS:
                flush(name)
        finally:
            for writer in writers.values():
                writer.close()

        destinations = []
        for name in SCHEMAS:
            dest = output_dir / f"{name}.parquet"
            (scratch_dir / f"{name}.parquet").replace(dest)
            destinations.append(dest)
        return tuple(destinations)
