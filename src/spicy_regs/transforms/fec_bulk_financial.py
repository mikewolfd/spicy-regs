"""Explicit consumer mappings for retained FEC bulk financial layouts.

These are source observations, not deduplicated transactions or current totals.
Selection proves the file's meaning; matching field names alone cannot do that.
SpicyDocs remains responsible for source decoding and native field evidence.
"""

from dataclasses import dataclass
from datetime import date, datetime
import json
import re

import pyarrow as pa

from .fec_query import AMOUNT_TYPE, CollectionSelection, exact_amount, observation_fields, record_evidence


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def financial_date(raw, spelling, *, year_bounds=None):
    """Parse an explicitly selected format, with no implicit two-digit-year pivot.

    A two-digit year remains unresolved unless the selection supplies evidenced
    year bounds admitting exactly one century. Raw dates also remain queryable.
    Bounds are not inferred from the transaction's election code or source cycle.
    """
    if raw is None:
        return None, "source_null"
    if raw == "":
        return None, "source_empty"
    formats = {
        "MMDDYYYY": (r"[0-9]{8}", "%m%d%Y"),
        "YYYYMMDD": (r"[0-9]{8}", "%Y%m%d"),
        "MM/DD/YYYY": (r"[0-9]{2}/[0-9]{2}/[0-9]{4}", "%m/%d/%Y"),
        "YYYY-MM-DD": (r"[0-9]{4}-[0-9]{2}-[0-9]{2}", "%Y-%m-%d"),
    }
    if spelling == "DD-MON-YY":
        if not isinstance(raw, str) or not re.fullmatch(r"[0-9]{2}-[A-Z]{3}-[0-9]{2}", raw):
            return None, "unsupported_spelling"
        months = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()
        if raw[3:6] not in months:
            return None, "invalid_date"
        if year_bounds is None:
            return None, "unresolved_century"
        low, high = year_bounds
        if type(low) is not int or type(high) is not int or not 1 <= low <= high <= 9999:
            raise ValueError("Date-year bounds must be an explicit valid inclusive range")
        years = [y for y in range(low, high + 1) if y % 100 == int(raw[-2:])]
        if len(years) != 1:
            return None, "unresolved_century" if years else "outside_selected_year_bounds"
        try:
            return date(years[0], months.index(raw[3:6]) + 1, int(raw[:2])), "exact_with_year_bounds"
        except ValueError:
            return None, "invalid_date"
    if spelling not in formats:
        raise ValueError("Unknown selected financial date format")
    pattern, fmt = formats[spelling]
    if not isinstance(raw, str) or not re.fullmatch(pattern, raw):
        return None, "unsupported_spelling"
    try:
        return datetime.strptime(raw, fmt).date(), "exact"
    except ValueError:
        return None, "invalid_date"


COMMON_TEXT = (
    "record_id identity_version mapping_version value_mapping_version mapping_status mapping_reason_json collection_id source_record_id "
    "source_sha256 source_locator_json source_authority selection_evidence_sha256 source_representation_role "
    "correction_operation correction_applicability_status current_record_status filing_key filing_link_status "
    "currency amount_kind source_namespace"
).split()


@dataclass(frozen=True)
class BulkMapping:
    """A finite reviewed mapping; each pair is (typed field, native field)."""

    key: str
    table: str
    text: tuple[tuple[str, str], ...]
    amounts: tuple[tuple[str, str], ...]
    dates: tuple[tuple[str, str, str], ...]
    constants: tuple[tuple[str, str], ...]

    @property
    def schema(self):
        text = COMMON_TEXT + [a for a, _ in self.text] + [a for a, _ in self.constants]
        text += [n + suffix for n, _ in self.amounts for suffix in ("_raw", "_status")]
        text += [n + suffix for n, _, _ in self.dates for suffix in ("_raw", "_status")]
        if len(text) != len(set(text)):
            raise ValueError("Duplicate field in typed FEC schema")
        return pa.schema(
            [(n, pa.string()) for n in text]
            + [("source_cycle", pa.int32())]
            + [(n, AMOUNT_TYPE) for n, _ in self.amounts]
            + [(n, pa.date32()) for n, _, _ in self.dates]
        )


def pairs(words):
    return tuple(tuple(p.split(":")) for p in words.split())


TRANSACTION_FIELDS = pairs(
    "reporting_committee_id:CMTE_ID amendment_indicator:AMNDT_IND report_type:RPT_TP "
    "transaction_election:TRANSACTION_PGI image_number:IMAGE_NUM transaction_type:TRANSACTION_TP "
    "counterparty_type:ENTITY_TP counterparty_name:NAME counterparty_city:CITY counterparty_state:STATE "
    "counterparty_zip:ZIP_CODE employer:EMPLOYER occupation:OCCUPATION other_native_id:OTHER_ID "
    "transaction_id:TRAN_ID report_number:FILE_NUM memo_indicator:MEMO_CD memo_text:MEMO_TEXT "
    "source_record_identifier:SUB_ID"
)

INTERCOMMITTEE = BulkMapping(
    "other-committee-transactions/1",
    "fec_intercommittee_transactions",
    TRANSACTION_FIELDS,
    (("amount", "TRANSACTION_AMT"),),
    (("transaction_date", "TRANSACTION_DT", "MMDDYYYY"),),
    (("reported_direction", "unresolved"), ("transaction_population", "other-committee-transactions")),
)

CANDIDATE_TRANSACTIONS = BulkMapping(
    "committee-to-candidate-transactions/1",
    "fec_intercommittee_transactions",
    TRANSACTION_FIELDS + (("candidate_id", "CAND_ID"),),
    (("amount", "TRANSACTION_AMT"),),
    (("transaction_date", "TRANSACTION_DT", "MMDDYYYY"),),
    (("reported_direction", "unresolved"), ("transaction_population", "committee-to-candidate-transactions")),
)

INDEPENDENT_EXPENDITURES = BulkMapping(
    "independent-expenditure-csv/1",
    "fec_independent_expenditures",
    pairs(
        "spender_native_id:spe_id spender_name:spe_nam payee_name:pay purpose:pur "
        "candidate_id:cand_id candidate_name:cand_name candidate_office:can_office "
        "candidate_state:can_office_state candidate_district:can_office_dis candidate_party:cand_pty_aff "
        "support_oppose_code:sup_opp election_type:ele_type candidate_election_year:fec_election_yr "
        "report_number:file_num previous_report_number:prev_file_num amendment_indicator:amndt_ind "
        "transaction_id:tran_id image_number:image_num"
    ),
    (("amount", "exp_amo"), ("reported_aggregate_amount", "agg_amo")),
    (
        ("expenditure_date", "exp_date", "DD-MON-YY"),
        ("dissemination_date", "dissem_dt", "DD-MON-YY"),
        ("receipt_date", "receipt_dat", "DD-MON-YY"),
    ),
    (("spender_identity_status", "native_identifier_unresolved"),),
)

COMMUNICATION_COSTS = BulkMapping(
    "communication-cost-csv/1",
    "fec_communication_costs",
    pairs(
        "reporting_entity_id:CMTE_ID reporting_entity_name:CMTE_NM candidate_id:CAND_ID "
        "candidate_name:CAND_NAME candidate_office:CAND_OFFICE candidate_state:CAND_STATE "
        "candidate_district:CAND_OFFICE_DISTRICT candidate_party:CAND_PTY_AFFILIATION "
        "candidate_state_description:CAND_STATE_DESCRIPTION candidate_party_description:CAND_PTY_AFFILIATION_DESCRIPTION "
        "transaction_type:TRANSACTION_TP communication_type:COMMUNICATION_TP communication_class:COMMUNICATION_CLASS "
        "support_oppose_code:SUPPORT_OPPOSE_IND image_number:IMAGE_NUM line_number:LINE_NUM form_type:FORM_TP_CD "
        "schedule_type:SCHED_TP_CD transaction_id:TRAN_ID source_record_identifier:SUB_ID report_number:FILE_NUM "
        "report_year:RPT_YR purpose:PURPOSE"
    ),
    (("amount", "TRANSACTION_AMT"),),
    (("transaction_date", "TRANSACTION_DT", "YYYYMMDD"),),
    (),
)

ELECTIONEERING = BulkMapping(
    "electioneering-candidate-disbursement-csv/1",
    "fec_electioneering_communications",
    pairs(
        "reporting_entity_id:COMMITTEE_ID reporting_entity_name:COMMITTEE_NAME candidate_id:CANDIDATE_ID "
        "candidate_name:CANDIDATE_NAME candidate_office:CANDIDATE_OFFICE candidate_state:CANDIDATE_STATE "
        "candidate_district:CANDIDATE_DISTRICT image_number:SB_IMAGE_NUM payee_name:PAYEE_NAME "
        "payee_street:PAYEE_STREET payee_city:PAYEE_CITY payee_state:PAYEE_STATE "
        "purpose:DISBURSEMENT_DESCRIPTION reported_candidate_count:NUMBER_OF_CANDIDATES"
    ),
    (("amount", "REPORTED_DISBURSEMENT_AMOUNT"), ("allocated_candidate_amount", "CALCULATED_CANDIDATE_SHARE")),
    (
        ("disbursement_date", "DISBURSEMENT_DATE", "DD-MON-YY"),
        ("communication_date", "COMMUNICATION_DATE", "DD-MON-YY"),
        ("public_disbursement_date", "PUBLIC_DISBURSEMENT_DATE", "DD-MON-YY"),
    ),
    (
        ("observation_grain", "candidate-associated-disbursement"),
        ("event_equivalence_status", "unresolved"),
        ("amount_aggregation_status", "requires-event-deduplication"),
    ),
)

OPPEXP_FIELDS = tuple(
    "CMTE_ID AMNDT_IND RPT_YR RPT_TP IMAGE_NUM LINE_NUM FORM_TP_CD SCHED_TP_CD NAME CITY STATE ZIP_CODE "
    "TRANSACTION_DT TRANSACTION_AMT TRANSACTION_PGI PURPOSE CATEGORY CATEGORY_DESC MEMO_CD MEMO_TEXT "
    "ENTITY_TP SUB_ID FILE_NUM TRAN_ID BACK_REF_TRAN_ID".split()
)

OPERATING_EXPENSES = BulkMapping(
    "operating-expense-ordered-header/1",
    "fec_disbursements",
    pairs(
        "reporting_committee_id:CMTE_ID amendment_indicator:AMNDT_IND report_year:RPT_YR report_type:RPT_TP "
        "image_number:IMAGE_NUM line_number:LINE_NUM form_type:FORM_TP_CD schedule_type:SCHED_TP_CD "
        "payee_name:NAME payee_city:CITY payee_state:STATE payee_zip:ZIP_CODE "
        "transaction_election:TRANSACTION_PGI purpose:PURPOSE category_code:CATEGORY category_description:CATEGORY_DESC "
        "memo_indicator:MEMO_CD memo_text:MEMO_TEXT payee_type:ENTITY_TP source_record_identifier:SUB_ID "
        "report_number:FILE_NUM transaction_id:TRAN_ID back_reference_transaction_id:BACK_REF_TRAN_ID"
    ),
    (("amount", "TRANSACTION_AMT"),),
    (("transaction_date", "TRANSACTION_DT", "MM/DD/YYYY"),),
    (("payment_type", "operating_expenditure"),),
)


def map_bulk_financial(row, selection: CollectionSelection, mapping: BulkMapping, *, year_bounds=None):
    """Map an explicitly selected native layout and retain all conversion states."""
    common = observation_fields(mapping.table, row, selection, "fec-bulk-" + mapping.key)
    native = json.loads(row["metadata_json"])
    required = {v for _, v in mapping.text} | {v for _, v in mapping.amounts} | {v for _, v, _ in mapping.dates}
    if not isinstance(native, dict) or not required <= native.keys():
        raise ValueError("Financial observation lacks selected native fields")
    if any(native[k] is not None and not isinstance(native[k], str) for k in required):
        raise ValueError("Bulk financial native fields must remain text or source null")
    result = {**common, **{k: native[v] for k, v in mapping.text}, **dict(mapping.constants)}
    result.update(
        currency="USD", amount_kind=mapping.key.split("/")[0], source_namespace="fec-bulk-" + mapping.key.split("/")[0]
    )
    refusals = {}
    for target, origin in mapping.amounts:
        result[target], status = exact_amount(native[origin])
        result[target + "_status"], result[target + "_raw"] = status, native[origin]
        if status not in {"exact", "source_null", "source_empty"}:
            refusals[target] = status
    for target, origin, fmt in mapping.dates:
        result[target], status = financial_date(native[origin], fmt, year_bounds=year_bounds)
        result[target + "_status"], result[target + "_raw"] = status, native[origin]
        if status not in {"exact", "exact_with_year_bounds", "source_null", "source_empty"}:
            refusals[target] = status
    result["mapping_status"] = "partial" if refusals else "mapped"
    result["mapping_reason_json"] = _json(
        {
            "field_refusals": refusals,
            "uninterpreted_native_fields": sorted(native.keys() - required),
            "date_year_bounds": year_bounds,
        }
    )
    return result, record_evidence(mapping.table, result["record_id"], row, selection.source_generation_pin)


def operating_expense(row, selection: CollectionSelection, header_row):
    """Interpret the known ordered header, preserving an unlabelled empty tail.

    The retained source has an additional positional value after the last header
    field. Only a single explicitly empty value is admitted; it remains visible
    in the mapping result. A nonempty tail or changed width requires investigation.
    """
    header = json.loads(header_row["metadata_json"])
    if header.get("kind") != "row" or tuple(header.get("fields", ())) != OPPEXP_FIELDS:
        raise ValueError("Operating-expense header differs from the selected ordered definition")
    native = json.loads(row["metadata_json"])
    fields = native.get("fields")
    if native.get("kind") != "row" or not isinstance(fields, list) or len(fields) not in {25, 26}:
        raise ValueError("Operating-expense row has an unqualified field count")
    if fields[25:] not in ([], [""]):
        raise ValueError("Operating-expense row has nonempty unmapped trailing positions")
    locator = json.loads(row["source_locator_json"])
    if locator.get("field_mapping") is not None:
        raise ValueError("Operating-expense observation already contains field mapping evidence")
    locator["field_mapping"] = {
        "collection_id": header_row["collection_id"],
        "source_record_id": header_row["source_record_id"],
        "source_sha256": header_row["source_sha256"],
        "source_locator": json.loads(header_row["source_locator_json"]),
    }
    mapped_source = {
        **row,
        "metadata_json": _json(dict(zip(OPPEXP_FIELDS, fields[:25], strict=True))),
        "source_locator_json": _json(locator),
    }
    result, evidence = map_bulk_financial(mapped_source, selection, OPERATING_EXPENSES)
    reasons = json.loads(result["mapping_reason_json"])
    reasons["unlabelled_positions"] = {str(i): fields[i] for i in range(25, len(fields))}
    result["mapping_reason_json"] = _json(reasons)
    if fields[25:]:
        result["mapping_status"] = "partial"
    return result, evidence
