"""Retained OpenFEC financial results with distinct flow, stock and total fields.

The source owner serializes decimal numbers as exact strings. Small JSON
integers also survive here without conversion through binary floats. Response
controls never become financial rows and single captures imply no traversal.
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import json
import re
from urllib.parse import urlsplit

from .fec_bulk_financial import BulkMapping, financial_date, pairs
from .fec_query import CollectionSelection, exact_amount, observation_fields, record_evidence


@dataclass(frozen=True)
class ApiMapping:
    route: str
    fields: BulkMapping


SHARED = pairs(
    "reporting_committee_id:committee_id report_number:file_number image_number:image_number "
    "report_type:report_type report_year:report_year form_type:filing_form schedule_type:schedule_type "
    "source_record_identifier:sub_id source_link_id:link_id"
)

NATIONAL_PARTY_RECEIPTS = ApiMapping(
    "/v1/national_party/schedule_a/",
    BulkMapping(
        "national-party-receipt/1",
        "fec_receipts",
        SHARED
        + pairs(
            "transaction_id:tran_id contributor_name:contributor_name contributor_native_id:contributor_id "
            "contributor_type:entity_type contributor_city:contributor_city contributor_state:contributor_state "
            "contributor_zip:contributor_zip employer:contributor_employer occupation:contributor_occupation "
            "receipt_type:receipt_type receipt_description:receipt_desc account_type:party_account_type "
            "memo_indicator:memo_cd memo_text:memo_text amendment_indicator:amendment_indicator "
            "reported_cycle:two_year_transaction_period line_number:line_num"
        ),
        (("amount", "contribution_receipt_amount"), ("contributor_aggregate_ytd", "contributor_aggregate_ytd")),
        (("transaction_date", "contribution_receipt_date", "YYYY-MM-DD"),),
        (("query_completeness", "not-asserted"),),
    ),
)

LOANS = ApiMapping(
    "/v1/schedules/schedule_c/",
    BulkMapping(
        "loan-state/1",
        "fec_loans",
        SHARED
        + pairs(
            "transaction_id:transaction_id lender_name:loan_source_name lender_city:loan_source_city "
            "lender_state:loan_source_state lender_zip:loan_source_zip entity_type:entity_type candidate_id:candidate_id "
            "candidate_name:candidate_name action_code:action_code original_source_record_id:original_sub_id "
            "due_date_terms:due_date_terms interest_rate_terms:interest_rate_terms secured_indicator:secured_ind "
            "personally_funded_indicator:personally_funded memo_indicator:memo_code memo_text:memo_text "
            "reported_cycle:cycle line_number:line_number"
        ),
        (
            ("original_loan_amount", "original_loan_amount"),
            ("outstanding_balance", "loan_balance"),
            ("payments_to_date", "payment_to_date"),
        ),
        (("incurred_date", "incurred_date", "YYYY-MM-DD"),),
        (("query_completeness", "not-asserted"), ("observation_grain", "reported-loan-state")),
    ),
)

DEBTS = ApiMapping(
    "/v1/schedules/schedule_d/",
    BulkMapping(
        "debt-state/1",
        "fec_debts",
        SHARED
        + pairs(
            "transaction_id:transaction_id creditor_debtor_name:creditor_debtor_name "
            "creditor_debtor_city:creditor_debtor_city creditor_debtor_state:creditor_debtor_state "
            "entity_type:entity_type purpose:nature_of_debt action_code:action_code "
            "original_source_record_id:original_sub_id reported_cycle:election_cycle line_number:line_number"
        ),
        (
            ("opening_balance", "outstanding_balance_beginning_of_period"),
            ("closing_balance", "outstanding_balance_close_of_period"),
            ("incurred_in_period", "amount_incurred_period"),
            ("paid_in_period", "payment_period"),
        ),
        (("period_start", "coverage_start_date", "YYYY-MM-DD"), ("period_end", "coverage_end_date", "YYYY-MM-DD")),
        (("query_completeness", "not-asserted"), ("observation_grain", "reported-obligation-state")),
    ),
)

ALLOCATED_DISBURSEMENTS = ApiMapping(
    "/v1/schedules/schedule_h4/",
    BulkMapping(
        "allocated-disbursement/1",
        "fec_allocated_disbursements",
        SHARED
        + pairs(
            "transaction_id:transaction_id payee_name:payee_name payee_city:payee_city payee_state:payee_state payee_zip:payee_zip "
            "purpose:disbursement_purpose memo_indicator:memo_code memo_text:memo_text reported_cycle:cycle "
            "line_number:line_number event_description:activity_or_event "
            "administrative_activity_indicator:administrative_activity_indicator "
            "administrative_voter_drive_indicator:administrative_voter_drive_activity_indicator "
            "direct_candidate_support_indicator:direct_candidate_support_activity_indicator "
            "exempt_activity_indicator:exempt_activity_indicator fundraising_activity_indicator:fundraising_activity_indicator "
            "general_voter_drive_indicator:general_voter_drive_activity_indicator public_communication_indicator:public_comm_indicator"
        ),
        (
            ("total_amount", "disbursement_amount"),
            ("federal_share", "federal_share"),
            ("nonfederal_share", "nonfederal_share"),
            ("event_amount_year_to_date", "event_amount_year_to_date"),
        ),
        (("event_purpose_date", "event_purpose_date", "YYYY-MM-DD"),),
        (("query_completeness", "not-asserted"), ("allocation_basis_status", "reported-shares-no-inferred-ratio")),
    ),
)

COORDINATED_EXPENDITURES = ApiMapping(
    "/v1/schedules/schedule_f/",
    BulkMapping(
        "coordinated-party-expenditure/1",
        "fec_coordinated_party_expenditures",
        SHARED
        + pairs(
            "transaction_id:transaction_id payee_name:payee_name purpose:expenditure_purpose_full "
            "expenditure_type:expenditure_type memo_indicator:memo_code memo_text:memo_text action_code:action_code "
            "candidate_id:candidate_id candidate_name:candidate_name candidate_office:candidate_office "
            "candidate_state:candidate_office_state candidate_district:candidate_office_district "
            "designated_committee_id:designated_committee_id designated_committee_name:designated_committee_name "
            "designation_indicator:committee_designated_coordinated_expenditure_indicator "
            "subordinate_committee_id:subordinate_committee_id reported_cycle:election_cycle line_number:line_number"
        ),
        (
            ("amount", "expenditure_amount"),
            ("aggregate_general_election_amount", "aggregate_general_election_expenditure"),
        ),
        (("expenditure_date", "expenditure_date", "ISO-local-datetime"),),
        (("query_completeness", "not-asserted"),),
    ),
)

INAUGURAL_AGGREGATES = ApiMapping(
    "/v1/totals/inaugural_committees/by_contributor/",
    BulkMapping(
        "inaugural-contributor-total/1",
        "fec_contribution_aggregates",
        pairs("reporting_entity_id:committee_id contributor_name:contributor_name reported_cycle:cycle"),
        (("amount", "total_donation"),),
        (),
        (
            ("query_completeness", "not-asserted"),
            ("observation_grain", "inaugural-contributor-total"),
            ("contributor_identity_status", "source-name-only-no-inferred-identity"),
        ),
    ),
)

MAPPINGS = (
    NATIONAL_PARTY_RECEIPTS,
    LOANS,
    DEBTS,
    ALLOCATED_DISBURSEMENTS,
    COORDINATED_EXPENDITURES,
    INAUGURAL_AGGREGATES,
)


def _scalar(value):
    if value is None or isinstance(value, str):
        return value
    if type(value) is int:
        return str(value)
    if type(value) is bool:
        return "true" if value else "false"
    raise ValueError("API scalar field must retain exact text, integer, boolean or null")


def _date(raw, fmt):
    if fmt != "ISO-local-datetime" or raw is None or raw == "":
        return financial_date(raw, "YYYY-MM-DD" if fmt == "ISO-local-datetime" else fmt)
    if not isinstance(raw, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}", raw):
        return None, "unsupported_spelling"
    try:
        return datetime.strptime(raw, "%Y-%m-%dT%H:%M:%S").date(), "exact_local_date_component"
    except ValueError:
        return None, "invalid_date"


def map_api_financial(row, selection: CollectionSelection, mapping: ApiMapping):
    """Map one source-owned API result; leave source flags uninterpreted."""
    if urlsplit(row["source_url"]).path != mapping.route:
        raise ValueError("API financial mapping differs from the source route")
    wrapped = json.loads(row["metadata_json"], parse_float=Decimal)
    if wrapped.get("kind") != "api-record-observation" or not isinstance(wrapped.get("metadata"), dict):
        raise ValueError("Expected an API result observation, not response controls")
    native = wrapped["metadata"]
    spec = mapping.fields
    result = observation_fields(spec.table, row, selection, "fec-api-" + spec.key)
    required = {v for _, v in spec.text} | {v for _, v in spec.amounts} | {v for _, v, _ in spec.dates}
    missing = sorted(required - native.keys())
    result.update({k: _scalar(native.get(v)) for k, v in spec.text})
    result.update(dict(spec.constants))
    result.update(
        currency="USD", amount_kind=spec.key.split("/")[0], source_namespace="fec-openfec-" + spec.key.split("/")[0]
    )
    refusals = {k: "source_missing" for k in missing}
    for target, origin in spec.amounts:
        raw = native.get(origin)
        # Reject bool and binary float as money. Decimal is the exact JSON token
        # type; source-owned decimal strings and integers are already lossless.
        text = str(raw) if isinstance(raw, Decimal) or type(raw) is int else raw
        amount, status = exact_amount(text)
        if origin not in native:
            status = "source_missing"
        result[target], result[target + "_status"] = amount, status
        result[target + "_raw"] = (
            str(raw)
            if isinstance(raw, Decimal) or type(raw) is int
            else raw
            if raw is None or isinstance(raw, str)
            else json.dumps(raw)
        )
        if status not in {"exact", "source_null", "source_empty"}:
            refusals[target] = status
    for target, origin, fmt in spec.dates:
        raw = native.get(origin)
        value, status = _date(raw, fmt)
        if origin not in native:
            status = "source_missing"
        result[target], result[target + "_status"], result[target + "_raw"] = value, status, _scalar(raw)
        if status not in {"exact", "exact_local_date_component", "source_null", "source_empty"}:
            refusals[target] = status
    result["mapping_status"] = "partial" if refusals else "mapped"
    result["mapping_reason_json"] = json.dumps(
        dict(
            field_refusals=refusals,
            missing_native_fields=missing,
            uninterpreted_native_fields=sorted(native.keys() - required),
        ),
        sort_keys=True,
        separators=(",", ":"),
    )
    return result, record_evidence(spec.table, result["record_id"], row, selection.source_generation_pin)
