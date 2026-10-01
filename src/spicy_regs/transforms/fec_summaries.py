"""Reported FEC summary editions and aggregates, without cross-period addition.

Source-specific monetary measures stay named by their native field. A derived
measure view can expose the list as rows without repeating stored provenance.
Candidate-like source IDs in aggregate files are not resolved candidate keys.
"""

from dataclasses import dataclass
import json

import pyarrow as pa

from .fec_bulk_financial import financial_date, pairs
from .fec_query import AMOUNT_TYPE, CollectionSelection, exact_amount, observation_fields, record_evidence

COMMON = (
    "record_id identity_version mapping_version value_mapping_version mapping_status mapping_reason_json collection_id source_record_id "
    "source_sha256 source_locator_json source_authority selection_evidence_sha256 source_representation_role "
    "correction_operation correction_applicability_status current_record_status filing_key filing_link_status "
    "summary_type source_namespace currency entity_reference_status candidate_native_id candidate_name "
    "committee_native_id committee_name period_start_raw period_start_status period_end_raw period_end_status "
    "attributes_json period_basis"
).split()
MEASURE_TYPE = pa.struct(
    [
        ("native_field", pa.string()),
        ("raw_value", pa.string()),
        ("value", AMOUNT_TYPE),
        ("value_status", pa.string()),
        ("unit", pa.string()),
    ]
)
SUMMARY_SCHEMA = pa.schema(
    [(n, pa.string()) for n in COMMON]
    + [
        ("source_cycle", pa.int32()),
        ("period_start", pa.date32()),
        ("period_end", pa.date32()),
        ("measures", pa.list_(MEASURE_TYPE)),
    ]
)


@dataclass(frozen=True)
class SummaryMapping:
    key: str
    identity_fields: tuple[tuple[str, str], ...]
    money_fields: tuple[str, ...]
    attributes: tuple[str, ...]
    period_start_field: str | None
    period_end_field: str | None
    date_format: str
    period_basis: str


CANDIDATE_CSV = SummaryMapping(
    "candidate-summary-csv/1",
    pairs("candidate_native_id:Cand_Id candidate_name:Cand_Name"),
    tuple(
        "Cand_Contribution Cand_Loan Cand_Loan_Repayment Cash_On_Hand_BOP Cash_On_Hand_COP Debt_Owe_To_Committee "
        "Debt_Owed_By_Committee Exempt_Legal_Accounting_Disbursement Fundraising_Disbursement Individual_Contribution "
        "Individual_Itemized_Contribution Individual_Refund Individual_Unitemized_Contribution Net_Contribution "
        "Net_Operating_Expenditure Offsets_To_Fundraising Offsets_To_Leagal_Accounting Offsets_To_Operating_Expenditure "
        "Operating_Expenditure Other_Committee_Contribution Other_Committee_Refund Other_Disbursements Other_Loan "
        "Other_Loan_Repayment Other_Receipts Party_Committee_Contribution Party_Committee_Refund Total_Contribution "
        "Total_Contribution_Refund Total_Disbursement Total_Loan Total_Loan_Repayment Total_Receipt "
        "Transfer_From_Other_Auth_Committee Transfer_To_Other_Auth_Committee".split()
    ),
    tuple(
        "Cand_City Cand_Incumbent_Challenger_Open_Seat Cand_Office Cand_Office_Dist Cand_Office_St Cand_Party_Affiliation "
        "Cand_State Cand_Street_1 Cand_Street_2 Cand_Zip Link_Image".split()
    ),
    "Coverage_Start_Date",
    "Coverage_End_Date",
    "MM/DD/YYYY",
    "source-reported-coverage",
)

COMMITTEE_CSV = SummaryMapping(
    "committee-summary-csv/1",
    pairs("committee_native_id:CMTE_ID committee_name:CMTE_NM candidate_native_id:CAND_ID"),
    tuple(
        "CAND_CNTB CAND_LOAN CAND_LOAN_REPYMNT COH_BOP COH_BOY COH_COP COH_COY COORD_EXP_BY_PTY_CMTE "
        "DEBTS_OWED_BY_CMTE DEBTS_OWED_TO_CMTE EXEMPT_LEGAL_ACCTG_DISB EXP_PRIOR_YRS_SUBJECT_LIM EXP_SUBJECT_LIMITS "
        "FED_CAND_CMTE_CONTB FED_CAND_CONTB_REF FED_FUNDS FNDRSG_DISB INDT_EXP INDV_CONTB INDV_ITEM_CONTB INDV_REF "
        "INDV_UNITEM_CONTB ITEM_CONVN_EXP_DISB ITEM_OTHER_DISB ITEM_OTHER_INCOME ITEM_OTHER_REF_REB_RET ITEM_REF_REB_RET "
        "LOANS_MADE LOAN_REPYMTS_RECEIVED NET_CONTB NET_OP_EXP NON_ALLOC_FED_ELECT_ACTVY OFFSETS_TO_FNDRSG "
        "OFFSETS_TO_LEGAL_ACCTG OFFSETS_TO_OP_EXP OP_EXP OTHER_DISB OTHER_FED_OP_EXP OTHER_RECEIPTS OTH_CMTE_CONTB "
        "OTH_CMTE_REF OTH_LOANS OTH_LOAN_REPYMTS POL_PTY_CMTE_REF PTY_CMTE_CONTB SHARED_FED_ACTVY_FED_SHR "
        "SHARED_FED_ACTVY_NONFED SHARED_FED_OP_EXP SHARED_NONFED_OP_EXP SUBTTL_CONVN_EXP_DISB SUBTTL_OTHER_REF_REB_RET "
        "SUBTTL_REF_REB_RET TRANF_FROM_NONFED_ACCT TRANF_FROM_NONFED_LEVIN TRANF_FROM_OTHER_AUTH_CMTE "
        "TRANF_TO_OTHER_AUTH_CMTE TTL_COMMUNICATION_COST TTL_CONTB TTL_CONTB_REF TTL_DISB TTL_EXP_SUBJECT_LIMITS "
        "TTL_FED_DISB TTL_FED_ELECT_ACTVY TTL_FED_RECEIPTS TTL_LOANS TTL_LOAN_REPYMTS TTL_NONFED_TRANF "
        "TTL_OFFSETS_TO_OP_EXP TTL_OP_EXP TTL_RECEIPTS UNITEM_CONVN_EXP_DISB UNITEM_OTHER_DISB UNITEM_OTHER_INCOME "
        "UNITEM_OTHER_REF_REB_RET UNITEM_REF_REB_RET".split()
    ),
    tuple(
        "CMTE_CITY CMTE_DSGN CMTE_FILING_FREQ CMTE_ST CMTE_ST1 CMTE_ST2 CMTE_TP CMTE_ZIP FEC_ELECTION_YR "
        "Link_Image ORG_TP TRES_NM".split()
    ),
    "CVG_START_DT",
    "CVG_END_DT",
    "YYYYMMDD",
    "source-reported-coverage",
)

CANDIDATE_WEB = SummaryMapping(
    "candidate-web-summary/1",
    pairs("candidate_native_id:CAND_ID candidate_name:CAND_NAME"),
    tuple(
        "CAND_CONTRIB CAND_LOANS CAND_LOAN_REPAY CMTE_REFUNDS COH_BOP COH_COP DEBTS_OWED_BY INDIV_REFUNDS "
        "OTHER_LOANS OTHER_LOAN_REPAY OTHER_POL_CMTE_CONTRIB POL_PTY_CONTRIB TRANS_FROM_AUTH TRANS_TO_AUTH "
        "TTL_DISB TTL_INDIV_CONTRIB TTL_RECEIPTS".split()
    ),
    tuple(
        "CAND_ICI CAND_OFFICE_DISTRICT CAND_OFFICE_ST CAND_PTY_AFFILIATION GEN_ELECTION GEN_ELECTION_PRECENT "
        "PRIM_ELECTION PTY_CD RUN_ELECTION SPEC_ELECTION".split()
    ),
    None,
    "CVG_END_DT",
    "MM/DD/YYYY",
    "selected-cycle-through-reported-end-start-not-supplied",
)

COMMITTEE_WEB = SummaryMapping(
    "committee-web-summary/1",
    pairs("committee_native_id:CMTE_ID committee_name:CMTE_NM"),
    tuple(
        "CAND_CONTRIB CAND_LOANS CAND_LOAN_REPAY COH_BOP COH_COP CONTRIB_TO_OTHER_CMTE DEBTS_OWED_BY "
        "INDV_CONTRIB INDV_REFUNDS IND_EXP LOAN_REPAY NONFED_SHARE_EXP NONFED_TRANS_RECEIVED OTHER_POL_CMTE_CONTRIB "
        "OTHER_POL_CMTE_REFUNDS PTY_COORD_EXP TRANF_TO_AFF TRANS_FROM_AFF TTL_DISB TTL_LOANS_RECEIVED TTL_RECEIPTS".split()
    ),
    tuple("CMTE_DSGN CMTE_FILING_FREQ CMTE_TP".split()),
    None,
    "CVG_END_DT",
    "MM/DD/YYYY",
    "selected-cycle-through-reported-end-start-not-supplied",
)

PRESIDENTIAL_OVERALL = SummaryMapping(
    "presidential-overall-summary/1",
    pairs("candidate_native_id:CAND_ID candidate_name:CAND_NM committee_native_id:CMTE_ID committee_name:CMTE_NM"),
    tuple(
        "CANDIDATE_CONTRIB CASH_ON_HAND_HAND_COP DEBTS_OWED_BY_CMTE DEBTS_OWED_TO_CMTE EXEMPT_LEGAL_ACCTG_DISB "
        "EXP_SUBJECT_LIMITS FEDERAL_FUNDS FNDRSG_DISB INDIVIDUAL_CONTRIBUTIONS INDIVIDUAL_ITEM_CONTRIB "
        "INDIVIDUAL_UNITEM_CONTRIB LOANS_FROM_CANDIDATE NET_CONTRIBUTIONS NET_OPERATING_EXP OFFSETS_TO_FNDRSG_EXP "
        "OFFSETS_TO_LEGAL_ACCTG OFFSETS_TO_OP_EXP OPERATING_EXP OTHER_CMTE_CONTRIB OTHER_DISBURSEMENTS "
        "OTHER_LOANS_RECEIVED OTHER_RECEIPTS POLITICAL_PARTY_CONTRB REF_INDV_CONTB REF_OTHER_POL_CMTE_CONTRIB "
        "REF_POL_PTY_CMTE_CONTRIB REPYMTS_LOANS_MADE_BY_CAND REPYMTS_OTHER_LOANS TOTAL_CONTRIBUTIONS TOTAL_DISBURSEMENTS "
        "TOTAL_LOANS_RECEIVED TOTAL_RECEIPTS TRANF_FROM_AFFILIATED_CMTE TRANF_TO_OTHER_AUTH_CMTE TTL_CONTRIB_REF_PER "
        "TTL_LOAN_REPYMTS_MADE TTL_OFFSETS_TO_OP_EXP".split()
    ),
    ("CAND_PTY_AFFILIATION", "ELECTION_YR"),
    None,
    None,
    "YYYYMMDD",
    "source-election-year-dates-not-supplied",
)

LEADERSHIP = SummaryMapping(
    "leadership-summary/1",
    pairs("committee_native_id:Committee_Id committee_name:Committee_Name"),
    ("Cash_on_Hand", "Total_Disbursement", "Total_Receipt"),
    ("Link_Image", "Sponsor_Name"),
    None,
    "Coverage_End_Date",
    "DD-MON-YY",
    "source-coverage-end-start-not-supplied",
)

BUNDLING_RECIPIENT = SummaryMapping(
    "bundling-recipient-period-summary/1",
    pairs("committee_native_id:Committee_Id committee_name:Committee_Name"),
    ("Quarterly_Contribution", "Semi_Annual_Contribution"),
    ("Committee_Election_District", "Committee_Election_State", "Link_Image", "Receipt_Date", "Report_Type"),
    "Coverage_Start_Date",
    "Coverage_End_Date",
    "DD-MON-YY",
    "quarterly-and-semiannual-measures-overlap",
)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def map_summary(row, selection: CollectionSelection, mapping: SummaryMapping, *, year_bounds=None):
    """Keep one summary observation and its distinct named monetary measures."""
    table = "fec_reported_financial_summaries"
    result = observation_fields(table, row, selection, "fec-" + mapping.key)
    native = json.loads(row["metadata_json"])
    expected = {v for _, v in mapping.identity_fields} | set(mapping.money_fields) | set(mapping.attributes)
    expected |= {v for v in (mapping.period_start_field, mapping.period_end_field) if v}
    if not isinstance(native, dict) or not expected <= native.keys():
        raise ValueError("Summary lacks fields required by the selected source layout")
    if any(native[k] is not None and not isinstance(native[k], str) for k in expected):
        raise ValueError("Bulk summary fields must retain native text or source null")
    result.update(
        summary_type=mapping.key,
        source_namespace="fec-bulk-" + mapping.key.split("/")[0],
        currency="USD",
        entity_reference_status="source_identifier_unresolved",
        period_basis=mapping.period_basis,
        candidate_native_id=None,
        candidate_name=None,
        committee_native_id=None,
        committee_name=None,
        attributes_json=_json({k: native[k] for k in mapping.attributes}),
    )
    result.update({k: native[v] for k, v in mapping.identity_fields})
    refusals = {}
    measures = []
    for field in mapping.money_fields:
        amount, status = exact_amount(native[field])
        measures.append(
            dict(native_field=field, raw_value=native[field], value=amount, value_status=status, unit="USD")
        )
        if status not in {"exact", "source_null", "source_empty"}:
            refusals[field] = status
    result["measures"] = measures
    for target, origin in (("period_start", mapping.period_start_field), ("period_end", mapping.period_end_field)):
        raw = native[origin] if origin else None
        value, status = (
            financial_date(raw, mapping.date_format, year_bounds=year_bounds) if origin else (None, "not_supplied")
        )
        result[target], result[target + "_raw"], result[target + "_status"] = value, raw, status
        if status not in {"exact", "exact_with_year_bounds", "source_null", "source_empty", "not_supplied"}:
            refusals[target] = status
    result["mapping_status"] = "partial" if refusals else "mapped"
    result["mapping_reason_json"] = _json(
        dict(
            field_refusals=refusals,
            uninterpreted_native_fields=sorted(native.keys() - expected),
            date_year_bounds=year_bounds,
        )
    )
    return result, record_evidence(table, result["record_id"], row, selection.source_generation_pin)


AGGREGATE_SCHEMA = pa.schema(
    [
        (n, pa.string())
        for n in (
            "record_id identity_version mapping_version value_mapping_version mapping_status mapping_reason_json collection_id source_record_id "
            "source_sha256 source_locator_json source_authority selection_evidence_sha256 source_representation_role "
            "correction_operation correction_applicability_status current_record_status filing_key filing_link_status "
            "aggregate_type candidate_native_id candidate_name entity_reference_status currency amount_raw amount_status "
            "dimensions_json source_namespace reported_count_status period_basis"
        ).split()
    ]
    + [("source_cycle", pa.int32()), ("amount", AMOUNT_TYPE), ("reported_count", pa.int64())]
)


def map_contribution_aggregate(row, selection: CollectionSelection, *, kind):
    dimensions = {"three-digit-zip": ("ZIP_3", "CONTRIB_STATE", "STATE_NAME"), "size-band": ("SIZE_OF_CONTRIBUTION",)}
    if kind not in dimensions:
        raise ValueError("Unknown selected contribution-aggregate layout")
    table = "fec_contribution_aggregates"
    result = observation_fields(table, row, selection, "fec-contribution-aggregate-" + kind + "/1")
    native = json.loads(row["metadata_json"])
    expected = {"CANDIDATE_ID", "CANDIDATE_NAME", "CONTRIB_RECEIPT_AMOUNT"} | set(dimensions[kind])
    if not isinstance(native, dict) or not expected <= native.keys():
        raise ValueError("Contribution aggregate lacks selected native fields")
    if any(native[k] is not None and not isinstance(native[k], str) for k in expected):
        raise ValueError("Contribution aggregate fields must remain text or source null")
    amount, status = exact_amount(native["CONTRIB_RECEIPT_AMOUNT"])
    result.update(
        aggregate_type=kind,
        candidate_native_id=native["CANDIDATE_ID"],
        candidate_name=native["CANDIDATE_NAME"],
        entity_reference_status="source_identifier_unresolved",
        currency="USD",
        amount=amount,
        amount_raw=native["CONTRIB_RECEIPT_AMOUNT"],
        amount_status=status,
        dimensions_json=_json({k: native[k] for k in dimensions[kind]}),
        source_namespace="fec-bulk-contribution-" + kind,
        reported_count=None,
        reported_count_status="not_reported",
        period_basis="selected-source-cycle",
        mapping_status="mapped" if status in {"exact", "source_null", "source_empty"} else "partial",
        mapping_reason_json=_json(dict(uninterpreted_native_fields=sorted(native.keys() - expected))),
    )
    return result, record_evidence(table, result["record_id"], row, selection.source_generation_pin)
