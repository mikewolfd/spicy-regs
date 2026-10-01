"""Typed filing schedules using pinned source-owned definitions.

This maps reported observations, not validated submissions or current money.
Native versions, forms, short rows, extra positions and body references survive.
The original filing header and workbook facts are separate evidence endpoints.
"""

from dataclasses import dataclass
import hashlib
import json
import re

import pyarrow as pa

from .fec_bulk_financial import BulkMapping, financial_date
from .fec_query import CollectionSelection, _text, exact_amount, observation_fields, record_evidence

WORKBOOK = "sha256:c667016493df28c186445201cf8a2bcff43563bccc41b75b2bcc06366b69132b"
EXTRA_TEXT = (
    "definition_set_id declared_format_version filing_header_record_id filing_header_locator_json "
    "submission_conformance_status"
).split()


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _sha(value):
    return "sha256:" + hashlib.sha256(_json(value).encode()).hexdigest()


@dataclass(frozen=True)
class FilingMapping:
    form: str
    layout_pin: str
    record_pattern: str
    fields: BulkMapping
    family: str = "electronic"
    version: str = "8.5"
    declared_versions: tuple[str, ...] = ("8.5",)
    workbook_sha256: str = WORKBOOK

    @property
    def schema(self):
        return pa.schema(list(self.fields.schema) + [pa.field(n, pa.string()) for n in EXTRA_TEXT])


def _mapping(
    form,
    pin,
    pattern,
    table,
    names,
    amounts,
    dates,
    constants=(),
    *,
    family="electronic",
    version="8.5",
    declared_versions=("8.5",),
    workbook_sha256=WORKBOOK,
):
    """Name reviewed physical positions. No normalized-label matching is used."""
    ordered = names.split()
    amounts, dates = set(amounts.split()), set(dates.split())
    if len(ordered) != len(set(ordered)) or not amounts | dates <= set(ordered) or amounts & dates:
        raise ValueError("Invalid filing mapping positions")
    return FilingMapping(
        form,
        "sha256:" + pin,
        pattern,
        BulkMapping(
            family + "-" + version + "-" + form.replace(" ", "-") + "/1",
            table,
            tuple((n, str(i)) for i, n in enumerate(ordered) if n not in amounts | dates),
            tuple((n, str(i)) for i, n in enumerate(ordered) if n in amounts),
            tuple((n, str(i), "YYYYMMDD") for i, n in enumerate(ordered) if n in dates),
            constants,
        ),
        family=family,
        version=version,
        declared_versions=declared_versions,
        workbook_sha256=workbook_sha256,
    )


TRANSACTION = "form_type reporting_committee_id transaction_id back_reference_transaction_id back_reference_schedule "
CONTRIBUTOR = (
    "entity_type contributor_organization contributor_last_name contributor_first_name contributor_middle_name "
    "contributor_prefix contributor_suffix contributor_street1 contributor_street2 contributor_city "
    "contributor_state contributor_zip "
)
PAYEE = (
    "entity_type payee_organization payee_last_name payee_first_name payee_middle_name payee_prefix payee_suffix "
    "payee_street1 payee_street2 payee_city payee_state payee_zip "
)

RECEIPTS = _mapping(
    "Sch A",
    "017a7ea6153fba28d5c693a3b6209999c8e4ae0c1466a19d4559dc711e00ea5a",
    r"SA[0-9][A-Za-z0-9]*",
    "fec_receipts",
    TRANSACTION
    + CONTRIBUTOR
    + "election_code election_other_description transaction_date amount reported_aggregate_amount "
    "purpose employer occupation donor_committee_id donor_committee_name donor_candidate_id donor_candidate_last_name "
    "donor_candidate_first_name donor_candidate_middle_name donor_candidate_prefix donor_candidate_suffix "
    "donor_candidate_office donor_candidate_state donor_candidate_district conduit_name conduit_street1 conduit_street2 "
    "conduit_city conduit_state conduit_zip memo_indicator memo_text account_reference",
    "amount reported_aggregate_amount",
    "transaction_date",
)
DISBURSEMENTS = _mapping(
    "Sch B",
    "6c39ee2b62584fb243ba3a20239c544b874a3c2199b7fa6649a69d8e597bc0bd",
    r"SB[0-9][A-Za-z0-9]*",
    "fec_disbursements",
    TRANSACTION
    + PAYEE
    + "election_code election_other_description transaction_date amount refunded_bundled_semiannual_amount "
    "purpose category_code beneficiary_committee_id beneficiary_committee_name beneficiary_candidate_id "
    "beneficiary_candidate_last_name beneficiary_candidate_first_name beneficiary_candidate_middle_name "
    "beneficiary_candidate_prefix beneficiary_candidate_suffix beneficiary_candidate_office beneficiary_candidate_state "
    "beneficiary_candidate_district conduit_name conduit_street1 conduit_street2 conduit_city conduit_state conduit_zip "
    "memo_indicator memo_text account_reference",
    "amount refunded_bundled_semiannual_amount",
    "transaction_date",
)
LOANS = _mapping(
    "Sch C",
    "465c41d9b3a8187fe82d05f8df2a35d3a258c0091779eaa857937f5fd2d1e66d",
    r"SC/[0-9]+",
    "fec_loans",
    "form_type reporting_committee_id transaction_id receipt_line_number entity_type lender_organization lender_last_name "
    "lender_first_name lender_middle_name lender_prefix lender_suffix lender_street1 lender_street2 lender_city lender_state "
    "lender_zip election_code election_other_description original_loan_amount payments_to_date outstanding_balance "
    "incurred_date due_date_terms interest_rate_terms secured_indicator personally_funded_indicator lender_committee_id "
    "lender_candidate_id lender_candidate_last_name lender_candidate_first_name lender_candidate_middle_name "
    "lender_candidate_prefix lender_candidate_suffix lender_candidate_office lender_candidate_state lender_candidate_district "
    "memo_indicator memo_text",
    "original_loan_amount payments_to_date outstanding_balance",
    "incurred_date",
    (("observation_grain", "reported-loan-state"),),
)
GUARANTORS = _mapping(
    "Sch C2",
    "8b226a7c0fa8be5560ce338471edea71c9a05ef7720b834fb5a05a3ae597b69a",
    r"SC2/[0-9]+",
    "fec_loan_guarantors",
    "form_type reporting_committee_id transaction_id back_reference_transaction_id entity_type guarantor_organization "
    "guarantor_committee_id guarantor_last_name guarantor_first_name guarantor_middle_name guarantor_prefix guarantor_suffix "
    "guarantor_street1 guarantor_street2 guarantor_city guarantor_state guarantor_zip employer occupation guaranteed_amount",
    "guaranteed_amount",
    "",
    (("loan_link_status", "unresolved"),),
)
DEBTS = _mapping(
    "Sch D",
    "f2f4bd9d9cec224d22b6bf515c73601546a9c87a53a2a066f1dc5a9f8d7cc388",
    r"SD[0-9]+",
    "fec_debts",
    "form_type reporting_committee_id transaction_id entity_type creditor_debtor_organization creditor_debtor_last_name "
    "creditor_debtor_first_name creditor_debtor_middle_name creditor_debtor_prefix creditor_debtor_suffix "
    "creditor_debtor_street1 creditor_debtor_street2 creditor_debtor_city creditor_debtor_state creditor_debtor_zip "
    "purpose opening_balance incurred_in_period paid_in_period closing_balance",
    "opening_balance incurred_in_period paid_in_period closing_balance",
    "",
    (("observation_grain", "reported-obligation-state"), ("obligation_direction", "native-line-uninterpreted")),
)
INDEPENDENT_EXPENDITURES = _mapping(
    "Sch E",
    "70895c7e558af8d813c60386e33b9377023a4ee0dcde014d2a0c80f239a95dc9",
    "SE",
    "fec_independent_expenditures",
    TRANSACTION + PAYEE + "election_code election_other_description dissemination_date amount disbursement_date "
    "reported_aggregate_amount purpose category_code payee_committee_id support_oppose_code candidate_id candidate_last_name "
    "candidate_first_name candidate_middle_name candidate_prefix candidate_suffix candidate_office candidate_district "
    "candidate_state completing_last_name completing_first_name completing_middle_name completing_prefix completing_suffix "
    "signed_date memo_indicator memo_text",
    "amount reported_aggregate_amount",
    "dissemination_date disbursement_date signed_date",
)
COORDINATED_EXPENDITURES = _mapping(
    "Sch F",
    "bb1d965b312b6fe72b31a6a1ed7eb379df7137d199890c395f71ced5130cb921",
    "SF",
    "fec_coordinated_party_expenditures",
    TRANSACTION + "designation_indicator designating_committee_id designating_committee_name subordinate_committee_id "
    "subordinate_committee_name subordinate_street1 subordinate_street2 subordinate_city subordinate_state subordinate_zip "
    + PAYEE
    + "transaction_date amount reported_aggregate_amount purpose category_code payee_committee_id "
    "candidate_id candidate_last_name candidate_first_name candidate_middle_name candidate_prefix candidate_suffix "
    "candidate_office candidate_state candidate_district memo_indicator memo_text",
    "amount reported_aggregate_amount",
    "transaction_date",
)
ALLOCATED_DISBURSEMENTS = _mapping(
    "Sch H4",
    "9b2ac5e292955198ff2a796bb58101e28e09c3e5ecba8dde7f14d7ef2d3bad44",
    "H4",
    "fec_allocated_disbursements",
    TRANSACTION
    + PAYEE
    + "event_identifier transaction_date total_amount federal_share nonfederal_share event_amount_year_to_date "
    "purpose category_code administrative_activity_indicator fundraising_activity_indicator exempt_activity_indicator "
    "general_voter_drive_indicator direct_candidate_support_indicator public_communication_indicator memo_indicator memo_text",
    "total_amount federal_share nonfederal_share event_amount_year_to_date",
    "transaction_date",
    (("allocation_basis_status", "reported-shares-no-inferred-ratio"),),
)
INAUGURAL_DONATIONS = _mapping(
    "F132",
    "aa08daaa3813f082c49479b1bc057c8ff9abad3dea37e260ed438082c400b345",
    "F132",
    "fec_inaugural_donations",
    TRANSACTION + CONTRIBUTOR + "transaction_date amount reported_aggregate_amount memo_indicator memo_text",
    "amount reported_aggregate_amount",
    "transaction_date",
    (("activity_kind", "reported-donation"),),
)
INAUGURAL_REFUNDS = _mapping(
    "F133",
    "713df3d46d87e0fe18b6049085f65e17d86106cf6dab4bc99d3b5dbde8f0a1ce",
    "F133",
    "fec_inaugural_donations",
    TRANSACTION + CONTRIBUTOR + "transaction_date amount memo_indicator memo_text",
    "amount",
    "transaction_date",
    (("activity_kind", "reported-refund"),),
)
MAPPINGS = (
    RECEIPTS,
    DISBURSEMENTS,
    LOANS,
    GUARANTORS,
    DEBTS,
    INDEPENDENT_EXPENDITURES,
    COORDINATED_EXPENDITURES,
    ALLOCATED_DISBURSEMENTS,
    INAUGURAL_DONATIONS,
    INAUGURAL_REFUNDS,
)


def load_layout(mapping: FilingMapping):
    """Use the owner API once; fail if any reviewed definition has changed."""
    from spicy_docs.sources.fec.layouts import filing_layout

    layout = filing_layout(family=mapping.family, version=mapping.version, form=mapping.form)
    if _sha(layout) != mapping.layout_pin or layout["source"]["sha256"] != mapping.workbook_sha256:
        raise ValueError("Filing definition differs from the reviewed layout")
    fields = mapping.fields
    positions = [p for _, p in fields.text] + [p for _, p in fields.amounts] + [p for _, p, _ in fields.dates]
    if set(positions) != {str(i) for i in range(len(layout["fields"]))} or len(positions) != len(set(positions)):
        raise ValueError("Filing mapping must account for every defined position exactly once")
    return layout


def filing_amount(raw):
    """Interpret boundary ASCII padding without changing retained source text."""
    value, status = exact_amount(raw)
    if status == "unsupported_spelling" and isinstance(raw, str):
        unpadded = raw.strip(" \t")
        if unpadded and unpadded != raw:
            value, unpadded_status = exact_amount(unpadded)
            if unpadded_status == "exact":
                return value, "exact_after_ascii_padding"
    return value, status


def validate_filing_header(row, header_row, selection: CollectionSelection, *, declared_versions=None):
    """Validate source-owned header identity without parsing its native syntax.

    A field mapper supplies exact supported versions. Narrative observations may
    retain a source header without claiming a particular field dictionary.
    """
    header = json.loads(header_row["metadata_json"])
    header_locator = json.loads(header_row["source_locator_json"])
    row_locator = json.loads(row["source_locator_json"])
    if (
        header.get("kind") != "header"
        or (declared_versions is not None and header.get("format_version") not in declared_versions)
        or row["collection_id"] != selection.collection_id
        or row["source_sha256"] != selection.source_sha256
        or any(row[k] != header_row[k] for k in ("collection_id", "source_sha256"))
        or header_locator.get("ordinal") != 0
        or header_locator.get("member") != row_locator.get("member")
    ):
        raise ValueError("Filing header differs from the selected collection or exact version")
    for source, locator in ((row, row_locator), (header_row, header_locator)):
        if any(locator.get(k) != _text(source[k]) for k in ("collection_id", "source_record_id")):
            raise ValueError("Filing header or row locator differs from its source observation")
    return header


def map_filing_financial(row, selection: CollectionSelection, mapping: FilingMapping, *, layout, header_row):
    """Keep typed values and explicit absent/body/extra dispositions.

    A field map does not assert source conformance, current-record eligibility,
    amendment replacement, or a resolved report number. SA3L requires an exact
    bundling mapping; generic transaction mappings cannot consume bundling rows.
    """
    from spicy_docs.sources.fec.layouts import map_filing_fields

    if _sha(layout) != mapping.layout_pin:
        raise ValueError("Filing definition differs from the reviewed layout")
    header = validate_filing_header(row, header_row, selection, declared_versions=mapping.declared_versions)
    # The sealed source reader owns header syntax, including native paper and
    # comment-style headers. Consume its literal version and cite the original;
    # requiring electronic HDR fields here would reject valid retained inputs.
    # Validate the header's source address independently of the data row.
    record_evidence(mapping.fields.table, mapping.layout_pin, header_row, selection.source_generation_pin)
    native = json.loads(row["metadata_json"])
    raw_type = native.get("record_type")
    if (
        not isinstance(raw_type, str)
        or re.fullmatch(mapping.record_pattern, raw_type) is None
        or raw_type == "SB3L"
        or (
            raw_type == "SA3L"
            and not (mapping.record_pattern == "SA3L" and mapping.fields.table == "fec_bundled_contributions")
        )
        or native.get("fields", {}).get("0") != raw_type
    ):
        raise ValueError("Filing record type does not match the reviewed schedule")
    annotated = map_filing_fields(native, layout=layout, format_version=header["format_version"])
    fields = native["fields"]
    if any(not isinstance(v, str) for v in fields.values()):
        raise ValueError("Filing fields must retain source text")
    present = {str(a["index"]): a["presence"] for a in annotated["field_mapping"]["annotations"]}
    spec = mapping.fields
    result = observation_fields(spec.table, row, selection, "fec-filing-" + spec.key)
    result.update(dict(spec.constants))
    result.update(
        definition_set_id=mapping.layout_pin,
        declared_format_version=header["format_version"],
        filing_header_record_id=header_row["source_record_id"],
        filing_header_locator_json=_json(json.loads(header_row["source_locator_json"])),
        submission_conformance_status="not-qualified",
        currency="USD",
        amount_kind=spec.key.split("/")[0],
        source_namespace="fec-" + mapping.family + "-filing-schedule",
    )
    for target, origin in spec.text:
        result[target] = fields.get(origin)
    refusals = {}
    for target, origin in spec.amounts:
        value, status = filing_amount(fields.get(origin))
        if present[origin] != "value":
            status = "source_missing" if present[origin] == "absent" else "source_body_reference"
        result.update({target: value, target + "_raw": fields.get(origin), target + "_status": status})
        if status not in {"exact", "exact_after_ascii_padding", "source_empty"}:
            refusals[target] = status
    for target, origin, fmt in spec.dates:
        value, status = financial_date(fields.get(origin), fmt)
        if present[origin] != "value":
            status = "source_missing" if present[origin] == "absent" else "source_body_reference"
        result.update({target: value, target + "_raw": fields.get(origin), target + "_status": status})
        if status not in {"exact", "source_empty"}:
            refusals[target] = status
    limit = len(layout["fields"])
    reasons = dict(
        field_refusals=refusals,
        absent_positions=[int(p) for p, v in present.items() if v == "absent"],
        body_positions=[int(p) for p, v in present.items() if v == "body"],
        extra_positions={p: v for p, v in fields.items() if int(p) >= limit},
        width=annotated["field_mapping"]["width"],
    )
    result["mapping_status"] = "partial" if any(reasons[k] for k in reasons if k != "width") else "mapped"
    result["mapping_reason_json"] = _json(reasons)
    evidence = record_evidence(spec.table, result["record_id"], row, selection.source_generation_pin)
    header_evidence = record_evidence(spec.table, result["record_id"], header_row, selection.source_generation_pin)
    evidence += [{**e, "role": "filing_header"} for e in header_evidence]
    # Workbook endpoints are normalized once per definition set; the serving view
    # joins definition_set_id to them instead of duplicating them on every row.
    return result, evidence
