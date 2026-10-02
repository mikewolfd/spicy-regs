"""Finite source definitions and supported identities for financial policy /3.

This module is shared by bounded Python evaluation and lightweight SQL views.
It contains no source readers, Arrow types, filesystem reads or runtime pins.
"""

from dataclasses import dataclass

from spicy_regs.fec_versions import INDIVIDUAL_RECEIPT_MAPPING_VERSION

IDENTITY_VERSION = "fec-typed-observation/1"
VALUE_MAPPING_VERSION = "fec-exact-financial-values/2"

POLICY_VERSION = "fec-financial-meaning/3"
SOURCE_GENERATION = "sha256:9c289dfec822ff7e54f9d5719276579452a7b35cad573e701a51e0a15c2a06c0"


@dataclass(frozen=True)
class Definition:
    collection_id: str
    context_sha256: str
    pointer: str
    source_url: str
    source_generation_pin: str = SOURCE_GENERATION


def _definition(cid, digest, event, slug):
    return Definition(
        cid,
        "sha256:" + digest,
        f"/receiverDisposition/callerContext/facts/parsing/native_events/{event}",
        "https://www.fec.gov/campaign-finance-data/" + slug + "/",
    )


INDIVIDUAL_MEMO = _definition(
    "retained-financial-reference-fd9c380f4e2026098fda-000",
    "ae267793a16e9ac9d47c401b5d6824e6326dcf3ed830e96c90b285777d0d8b31",
    1558,
    "contributions-individuals-file-description",
)
INTERCOMMITTEE_MEMO = _definition(
    "retained-financial-reference-4edc6e0865e0ce3ed749-000",
    "ef6a287ec54198924eb94471d867d4229c8b2ebae7e6fa285247f6fee2de3696",
    1548,
    "any-transaction-one-committee-another-file-description",
)
CANDIDATE_TRANSACTION_MEMO = _definition(
    "retained-financial-reference-60765b1919f24830b3b2-000",
    "c3f0e23a6caac114748cd1ad5918abfcf32abaf0db58c456db9fcfb7748d1946",
    1559,
    "contributions-committees-candidates-file-description",
)
ELECTIONEERING_SHARE = _definition(
    "retained-financial-reference-01cb2822eb1eb1c9363d-000",
    "e21d440a92825e9dc831cd6dade519ce9c938a47d46fabaa81159054736ce8f1",
    825,
    "electioneering-communications-file-description",
)
SUMMARY_NET = _definition(
    "retained-financial-reference-6254bce6e950047892b4-000",
    "f9254af386f2a78cf395cdcec95988b96481094b0d4646f8cc5c62e7fe9bc349",
    1802,
    "committee-summary-file-description",
)
CANDIDATE_TRANSFERS = _definition(
    "retained-financial-reference-e33cdca5faef526b0e61-000",
    "4b9134051f8e979fcb722aac97d9de05159d1f90a3e16561f862e9a0480713db",
    827,
    "all-candidates-file-description",
)
QUALITY_NOTICE = _definition(
    "retained-financial-reference-9833ac78263e0759e1b1-000",
    "c2b3ada667b5df57358a54f287c9240d8a6f0dec37c57564c42d7f01da94c63f",
    793,
    "false-fictitious-filings-file-description",
)

_MEMO_DEFINITIONS = {
    "fec-bulk-individual-contributions": INDIVIDUAL_MEMO,
    "fec-bulk-other-committee-transactions": INTERCOMMITTEE_MEMO,
    "fec-bulk-committee-to-candidate-transactions": CANDIDATE_TRANSACTION_MEMO,
}
# These are exact retained layout identities, not assumptions about all versions.
_LOAN_LAYOUT = "sha256:465c41d9b3a8187fe82d05f8df2a35d3a258c0091779eaa857937f5fd2d1e66d"
_DEBT_LAYOUT = "sha256:f2f4bd9d9cec224d22b6bf515c73601546a9c87a53a2a066f1dc5a9f8d7cc388"
_ALLOCATION_LAYOUT = "sha256:9b2ac5e292955198ff2a796bb58101e28e09c3e5ecba8dde7f14d7ef2d3bad44"


_BULK_MAPPINGS = {
    "fec-bulk-individual-contributions": INDIVIDUAL_RECEIPT_MAPPING_VERSION,
    **{
        "fec-bulk-" + family: "fec-bulk-" + family + "/1"
        for family in (
            "other-committee-transactions",
            "committee-to-candidate-transactions",
            "independent-expenditure-csv",
            "communication-cost-csv",
            "electioneering-candidate-disbursement-csv",
        )
    },
}
_FILING_MAPPINGS = {
    _LOAN_LAYOUT: "fec-filing-electronic-8.5-Sch-C/1",
    _DEBT_LAYOUT: "fec-filing-electronic-8.5-Sch-D/1",
    _ALLOCATION_LAYOUT: "fec-filing-electronic-8.5-Sch-H4/1",
}


@dataclass(frozen=True)
class SummaryFields:
    money_fields: tuple[str, ...]


_SUMMARY_MAPPINGS = {
    "candidate-summary-csv/1": SummaryFields(
        (
            "Cand_Contribution",
            "Cand_Loan",
            "Cand_Loan_Repayment",
            "Cash_On_Hand_BOP",
            "Cash_On_Hand_COP",
            "Debt_Owe_To_Committee",
            "Debt_Owed_By_Committee",
            "Exempt_Legal_Accounting_Disbursement",
            "Fundraising_Disbursement",
            "Individual_Contribution",
            "Individual_Itemized_Contribution",
            "Individual_Refund",
            "Individual_Unitemized_Contribution",
            "Net_Contribution",
            "Net_Operating_Expenditure",
            "Offsets_To_Fundraising",
            "Offsets_To_Leagal_Accounting",
            "Offsets_To_Operating_Expenditure",
            "Operating_Expenditure",
            "Other_Committee_Contribution",
            "Other_Committee_Refund",
            "Other_Disbursements",
            "Other_Loan",
            "Other_Loan_Repayment",
            "Other_Receipts",
            "Party_Committee_Contribution",
            "Party_Committee_Refund",
            "Total_Contribution",
            "Total_Contribution_Refund",
            "Total_Disbursement",
            "Total_Loan",
            "Total_Loan_Repayment",
            "Total_Receipt",
            "Transfer_From_Other_Auth_Committee",
            "Transfer_To_Other_Auth_Committee",
        )
    ),
    "committee-summary-csv/1": SummaryFields(
        (
            "CAND_CNTB",
            "CAND_LOAN",
            "CAND_LOAN_REPYMNT",
            "COH_BOP",
            "COH_BOY",
            "COH_COP",
            "COH_COY",
            "COORD_EXP_BY_PTY_CMTE",
            "DEBTS_OWED_BY_CMTE",
            "DEBTS_OWED_TO_CMTE",
            "EXEMPT_LEGAL_ACCTG_DISB",
            "EXP_PRIOR_YRS_SUBJECT_LIM",
            "EXP_SUBJECT_LIMITS",
            "FED_CAND_CMTE_CONTB",
            "FED_CAND_CONTB_REF",
            "FED_FUNDS",
            "FNDRSG_DISB",
            "INDT_EXP",
            "INDV_CONTB",
            "INDV_ITEM_CONTB",
            "INDV_REF",
            "INDV_UNITEM_CONTB",
            "ITEM_CONVN_EXP_DISB",
            "ITEM_OTHER_DISB",
            "ITEM_OTHER_INCOME",
            "ITEM_OTHER_REF_REB_RET",
            "ITEM_REF_REB_RET",
            "LOANS_MADE",
            "LOAN_REPYMTS_RECEIVED",
            "NET_CONTB",
            "NET_OP_EXP",
            "NON_ALLOC_FED_ELECT_ACTVY",
            "OFFSETS_TO_FNDRSG",
            "OFFSETS_TO_LEGAL_ACCTG",
            "OFFSETS_TO_OP_EXP",
            "OP_EXP",
            "OTHER_DISB",
            "OTHER_FED_OP_EXP",
            "OTHER_RECEIPTS",
            "OTH_CMTE_CONTB",
            "OTH_CMTE_REF",
            "OTH_LOANS",
            "OTH_LOAN_REPYMTS",
            "POL_PTY_CMTE_REF",
            "PTY_CMTE_CONTB",
            "SHARED_FED_ACTVY_FED_SHR",
            "SHARED_FED_ACTVY_NONFED",
            "SHARED_FED_OP_EXP",
            "SHARED_NONFED_OP_EXP",
            "SUBTTL_CONVN_EXP_DISB",
            "SUBTTL_OTHER_REF_REB_RET",
            "SUBTTL_REF_REB_RET",
            "TRANF_FROM_NONFED_ACCT",
            "TRANF_FROM_NONFED_LEVIN",
            "TRANF_FROM_OTHER_AUTH_CMTE",
            "TRANF_TO_OTHER_AUTH_CMTE",
            "TTL_COMMUNICATION_COST",
            "TTL_CONTB",
            "TTL_CONTB_REF",
            "TTL_DISB",
            "TTL_EXP_SUBJECT_LIMITS",
            "TTL_FED_DISB",
            "TTL_FED_ELECT_ACTVY",
            "TTL_FED_RECEIPTS",
            "TTL_LOANS",
            "TTL_LOAN_REPYMTS",
            "TTL_NONFED_TRANF",
            "TTL_OFFSETS_TO_OP_EXP",
            "TTL_OP_EXP",
            "TTL_RECEIPTS",
            "UNITEM_CONVN_EXP_DISB",
            "UNITEM_OTHER_DISB",
            "UNITEM_OTHER_INCOME",
            "UNITEM_OTHER_REF_REB_RET",
            "UNITEM_REF_REB_RET",
        )
    ),
    "candidate-web-summary/1": SummaryFields(
        (
            "CAND_CONTRIB",
            "CAND_LOANS",
            "CAND_LOAN_REPAY",
            "CMTE_REFUNDS",
            "COH_BOP",
            "COH_COP",
            "DEBTS_OWED_BY",
            "INDIV_REFUNDS",
            "OTHER_LOANS",
            "OTHER_LOAN_REPAY",
            "OTHER_POL_CMTE_CONTRIB",
            "POL_PTY_CONTRIB",
            "TRANS_FROM_AUTH",
            "TRANS_TO_AUTH",
            "TTL_DISB",
            "TTL_INDIV_CONTRIB",
            "TTL_RECEIPTS",
        )
    ),
    "committee-web-summary/1": SummaryFields(
        (
            "CAND_CONTRIB",
            "CAND_LOANS",
            "CAND_LOAN_REPAY",
            "COH_BOP",
            "COH_COP",
            "CONTRIB_TO_OTHER_CMTE",
            "DEBTS_OWED_BY",
            "INDV_CONTRIB",
            "INDV_REFUNDS",
            "IND_EXP",
            "LOAN_REPAY",
            "NONFED_SHARE_EXP",
            "NONFED_TRANS_RECEIVED",
            "OTHER_POL_CMTE_CONTRIB",
            "OTHER_POL_CMTE_REFUNDS",
            "PTY_COORD_EXP",
            "TRANF_TO_AFF",
            "TRANS_FROM_AFF",
            "TTL_DISB",
            "TTL_LOANS_RECEIVED",
            "TTL_RECEIPTS",
        )
    ),
    "presidential-overall-summary/1": SummaryFields(
        (
            "CANDIDATE_CONTRIB",
            "CASH_ON_HAND_HAND_COP",
            "DEBTS_OWED_BY_CMTE",
            "DEBTS_OWED_TO_CMTE",
            "EXEMPT_LEGAL_ACCTG_DISB",
            "EXP_SUBJECT_LIMITS",
            "FEDERAL_FUNDS",
            "FNDRSG_DISB",
            "INDIVIDUAL_CONTRIBUTIONS",
            "INDIVIDUAL_ITEM_CONTRIB",
            "INDIVIDUAL_UNITEM_CONTRIB",
            "LOANS_FROM_CANDIDATE",
            "NET_CONTRIBUTIONS",
            "NET_OPERATING_EXP",
            "OFFSETS_TO_FNDRSG_EXP",
            "OFFSETS_TO_LEGAL_ACCTG",
            "OFFSETS_TO_OP_EXP",
            "OPERATING_EXP",
            "OTHER_CMTE_CONTRIB",
            "OTHER_DISBURSEMENTS",
            "OTHER_LOANS_RECEIVED",
            "OTHER_RECEIPTS",
            "POLITICAL_PARTY_CONTRB",
            "REF_INDV_CONTB",
            "REF_OTHER_POL_CMTE_CONTRIB",
            "REF_POL_PTY_CMTE_CONTRIB",
            "REPYMTS_LOANS_MADE_BY_CAND",
            "REPYMTS_OTHER_LOANS",
            "TOTAL_CONTRIBUTIONS",
            "TOTAL_DISBURSEMENTS",
            "TOTAL_LOANS_RECEIVED",
            "TOTAL_RECEIPTS",
            "TRANF_FROM_AFFILIATED_CMTE",
            "TRANF_TO_OTHER_AUTH_CMTE",
            "TTL_CONTRIB_REF_PER",
            "TTL_LOAN_REPYMTS_MADE",
            "TTL_OFFSETS_TO_OP_EXP",
        )
    ),
    "leadership-summary/1": SummaryFields(("Cash_on_Hand", "Total_Disbursement", "Total_Receipt")),
    "bundling-recipient-period-summary/1": SummaryFields(("Quarterly_Contribution", "Semi_Annual_Contribution")),
}
