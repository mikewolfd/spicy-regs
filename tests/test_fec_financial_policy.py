"""Synthetic financial hard cases; real-source qualification has a separate receipt."""

from dataclasses import replace
from decimal import Decimal, localcontext
import hashlib

import pytest

from spicy_regs.transforms import fec_financial_policy as policy
from spicy_regs.transforms.fec_financial_selection import (
    FinancialRecord,
    InclusionDecision,
    membership_pin,
    select_scope,
)


def pin(value):
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def row(identity="observation", **values):
    namespace = values.get("source_namespace", "fec-bulk-individual-contributions")
    mapping = policy._BULK_MAPPINGS.get(namespace)
    if "summary_type" in values:
        mapping = "fec-" + values["summary_type"]
        namespace = "fec-bulk-" + values["summary_type"].split("/")[0]
    if "definition_set_id" in values:
        mapping = policy._FILING_MAPPINGS.get(values["definition_set_id"])
        namespace = "fec-electronic-filing-schedule"
        values.setdefault("declared_format_version", "8.5")
    if "notice_kind" in values:
        mapping = "fec-identity-observations/2"
        namespace = "fec-bulk-false-fictitious-notice"
    return dict(
        record_id=pin(identity),
        source_representation_role="snapshot",
        correction_operation="none",
        currency="USD",
        identity_version=policy.IDENTITY_VERSION,
        value_mapping_version=policy.VALUE_MAPPING_VERSION,
        mapping_version=mapping,
        mapping_status="mapped",
        source_authority="official-fec",
        **{**values, "source_namespace": namespace},
    )


def amount(name, value):
    return {name: Decimal(value), name + "_raw": value, name + "_status": "exact"}


def bulk(identity="bulk", **changes):
    result = row(
        identity,
        source_namespace=changes.pop("source_namespace", "fec-bulk-individual-contributions"),
        memo_indicator="",
        memo_text="",
        **amount("amount", "100.00"),
    )
    result.update(changes)
    return result


def summary(summary_type="committee-summary-csv/1", **measures):
    return row(
        summary_type,
        summary_type=summary_type,
        measures=[
            dict(native_field=name, raw_value=value, value=Decimal(value), value_status="exact", unit="USD")
            for name, value in measures.items()
        ],
    )


@pytest.mark.parametrize("namespace", list(policy._MEMO_DEFINITIONS))
def test_memo_x_depends_on_source_and_purpose(namespace):
    observation = bulk(source_namespace=namespace, memo_indicator="X")
    analysis = policy.bulk_observation_eligibility(observation)
    component = policy.bulk_observation_eligibility(observation, purpose="detailed_summary_component")
    assert analysis.status == "eligible" and analysis.value == Decimal("100")
    assert analysis.definitions[0] == policy._MEMO_DEFINITIONS[namespace]
    assert component.status == "excluded" and component.value is None
    assert not analysis.current_financial_total_qualified


def test_memo_text_is_not_exclusion_or_inferred_attribution():
    observation = bulk(memo_text="Reattributed from John Adams 10/5/18")
    assert policy.bulk_observation_eligibility(observation, purpose="detailed_summary_component").status == "eligible"
    assert (
        policy.bulk_observation_eligibility(observation, purpose="net_receipts").reason
        == "refund_return_attribution_and_direction_rules_unqualified"
    )


@pytest.mark.parametrize(
    "change",
    [
        {"memo_indicator": "Y"},
        {"source_namespace": "fec-electronic-filing-schedule"},
        {"currency": "EUR"},
        {"amount_status": "source_empty"},
        {"amount": 100.0},
    ],
)
def test_bulk_unsupported_values_never_become_eligible(change):
    assert policy.bulk_observation_eligibility(bulk(**change)).status == "refused"


def test_missing_memo_field_is_not_a_blank_source_flag():
    observation = bulk()
    del observation["memo_indicator"]
    assert policy.bulk_observation_eligibility(observation).reason == "memo_indicator_not_projected"


@pytest.mark.parametrize("role,operation", [("deletion", "delete"), ("insertion", "insert")])
def test_positive_correction_payload_does_not_become_new_receipt(role, operation):
    result = policy.bulk_observation_eligibility(bulk(source_representation_role=role, correction_operation=operation))
    assert result.reason == "correction_payload_is_not_an_additional_receipt"


def test_negative_source_amount_stays_negative_and_is_not_inferred_refund():
    result = policy.bulk_observation_eligibility(bulk(**amount("amount", "-12.500000001")))
    assert result.value == Decimal("-12.500000001")
    assert "negative_sign_preserved_refund_not_inferred" in result.warnings
    assert policy.bulk_observation_eligibility(bulk(), purpose="gross_receipts").status == "refused"


def test_current_membership_requires_selection_owner_and_exact_nonmultiplied_set():
    observation = bulk()
    record = FinancialRecord(observation["record_id"], "report/A", observation["amount"], pin("fields"), "report")
    selected = select_scope(
        "report",
        (record,),
        (),
        base_membership_sha256=membership_pin((record,)),
        base_evidence_sha256=pin("proof"),
        base_complete=True,
    )
    result = policy.current_membership_guard([observation], selected)
    assert result.status == "eligible" and result.value is None
    assert not result.current_financial_total_qualified
    assert policy.current_membership_guard([observation], None).status == "refused"
    assert policy.current_membership_guard([observation, observation], selected).status == "refused"
    assert policy.current_membership_guard([bulk("different")], selected).status == "refused"
    contradictory = InclusionDecision(record.record_id, "removed", "conflicting", pin("proof"))
    assert (
        policy.current_membership_guard(
            [observation], replace(selected, decisions=selected.decisions + (contradictory,))
        ).status
        == "refused"
    )
    assert policy.current_membership_guard([observation], replace(selected, qualified=False)).status == "refused"


def spend(identity="spend", value="100"):
    return row(identity, source_namespace="fec-bulk-independent-expenditure-csv", **amount("amount", value))


def test_three_targets_do_not_triple_one_spending_observation():
    observation = spend()
    links = [{"spending_record_id": observation["record_id"], "target_id": candidate} for candidate in ["A", "B", "C"]]
    result = policy.spending_observation_sum([observation], links)
    assert result.value == Decimal("100") and result.status == "eligible"
    assert not result.current_financial_total_qualified
    assert policy.spending_observation_sum([observation] * 3, links).reason == "spending_rows_multiplied"
    assert policy.spending_observation_sum([observation], links + [links[0]]).status == "refused"
    assert (
        policy.spending_observation_sum([observation], [{"spending_record_id": pin("other"), "target_id": "A"}]).status
        == "refused"
    )


def test_spending_exact_decimal_does_not_depend_on_ambient_precision():
    with localcontext() as context:
        context.prec = 4
        result = policy.spending_observation_sum([spend("A", "123456789012345.123456789"), spend("B", "0.000000001")])
    assert result.value == Decimal("123456789012345.123456790")
    assert (
        policy.spending_observation_sum(
            [spend("A", "90000000000000000000000000000"), spend("B", "90000000000000000000000000000")]
        ).reason
        == "aggregate_decimal_overflow"
    )
    assert policy.spending_observation_sum([]).status == "refused"


def test_publisher_share_is_not_a_filer_allocation_or_deduplicated_event():
    observation = row(
        source_namespace="fec-bulk-electioneering-candidate-disbursement-csv",
        observation_grain="candidate-associated-disbursement",
        reported_candidate_count="3",
        **amount("amount", "100"),
        **amount("allocated_candidate_amount", "33.33"),
    )
    result = policy.publisher_calculated_candidate_share(observation)
    assert result.query == "publisher_calculated_candidate_share" and result.value == Decimal("33.33")
    assert result.measure_basis == "publisher_calculated_equal_candidate_share"
    assert "not_filer_reported_allocation" in result.warnings
    assert "publisher_rounding_not_recomputed" in result.warnings
    assert policy.spending_observation_sum([observation]).reason == "candidate_rows_require_event_equivalence"
    assert (
        policy.publisher_calculated_candidate_share({**observation, "reported_candidate_count": "0"}).status
        == "refused"
    )


def test_loan_cumulative_amount_is_not_period_activity_or_addable_repeated_balance():
    observation = row(
        definition_set_id=policy._LOAN_LAYOUT,
        **amount("outstanding_balance", "90"),
        **amount("payments_to_date", "0.000000000"),
    )
    args = dict(table="fec_loans", field="outstanding_balance")
    assert policy.state_measure([observation], **args).value == Decimal("90")
    assert policy.state_measure([observation, observation], **args).status == "refused"
    assert policy.state_measure([observation], table="fec_loans", field="payments_to_date").value == Decimal(0)
    assert (
        policy.state_measure(
            [observation], table="fec_loans", field="payments_to_date", purpose="period_activity"
        ).status
        == "refused"
    )
    assert (
        policy.state_measure([{**observation, "definition_set_id": pin("another version")}], **args).status == "refused"
    )


def test_debt_period_activity_and_closing_stock_keep_their_different_meanings():
    observation = row(
        definition_set_id=policy._DEBT_LAYOUT, **amount("incurred_in_period", "25"), **amount("closing_balance", "125")
    )
    assert policy.state_measure(
        [observation], table="fec_debts", field="incurred_in_period", purpose="period_activity"
    ).value == Decimal("25")
    assert (
        policy.state_measure(
            [observation], table="fec_debts", field="closing_balance", purpose="period_activity"
        ).status
        == "refused"
    )


def test_allocation_uses_total_or_components_and_checks_reported_equation():
    observation = row(
        definition_set_id=policy._ALLOCATION_LAYOUT,
        **amount("total_amount", "100"),
        **amount("federal_share", "40"),
        **amount("nonfederal_share", "60"),
        **amount("event_amount_year_to_date", "900"),
    )
    assert policy.allocated_payment(observation).value == Decimal("100")
    assert policy.allocated_payment(observation, fields=("federal_share", "nonfederal_share")).value == Decimal("100")
    assert policy.allocated_payment(observation, fields=("federal_share",)).value == Decimal("40")
    for fields in [
        ("total_amount", "federal_share", "nonfederal_share"),
        ("event_amount_year_to_date",),
        ("total_amount", "total_amount"),
    ]:
        assert policy.allocated_payment(observation, fields=fields).status == "refused"
    assert (
        policy.allocated_payment(
            {**observation, **amount("total_amount", "101")}, fields=("federal_share", "nonfederal_share")
        ).status
        == "refused"
    )


@pytest.mark.parametrize(
    "gross,refund,net", [("100", "20", "80"), ("100", "-20", "120"), ("0.000000000", "0.000000000", "0.000000000")]
)
def test_summary_net_uses_source_equation_and_literal_signs(gross, refund, net):
    observation = summary(TTL_CONTB=gross, TTL_CONTB_REF=refund, NET_CONTB=net)
    result = policy.committee_summary_net_contributions(observation)
    assert result.status == "eligible" and result.value == Decimal(net)
    assert not result.current_financial_total_qualified


def test_reported_subtotal_is_useful_alone_but_not_added_to_itemizations():
    observation = summary(TTL_CONTB="100", TTL_CONTB_REF="20", NET_CONTB="79")
    assert policy.reported_summary_measure(observation, "NET_CONTB").value == Decimal("79")
    assert (
        policy.committee_summary_net_contributions(observation).reason == "reported_net_differs_from_defined_components"
    )
    assert policy.reported_summary_measure(observation, "absent").status == "refused"
    assert policy.summary_itemized_overlap_guard([observation], [bulk()]).status == "refused"
    comparison = policy.summary_itemized_overlap_guard([observation], [bulk()], operation="reconcile")
    assert comparison.reason == "comparison_period_population_and_unitemized_scope_evidence_required"
    assert "residual_is_not_automatically_an_error" in comparison.warnings
    observation["measures"].append(observation["measures"][0])
    assert policy.reported_summary_measure(observation, "TTL_CONTB").status == "refused"


def test_authorized_transfer_summary_rule_does_not_deduplicate_individual_sides():
    observation = summary(
        "candidate-web-summary/1", TTL_RECEIPTS="150", TTL_DISB="140", TRANS_FROM_AUTH="50", TRANS_TO_AUTH="40"
    )
    assert policy.candidate_summary_transfer_adjustment(observation, direction="receipts").value == Decimal("100")
    assert policy.candidate_summary_transfer_adjustment(observation, direction="disbursements").value == Decimal("100")
    zero_incoming = summary(
        "candidate-web-summary/1", TTL_RECEIPTS="150", TTL_DISB="140", TRANS_FROM_AUTH="0", TRANS_TO_AUTH="40"
    )
    assert policy.candidate_summary_transfer_adjustment(zero_incoming, direction="receipts").status == "refused"
    sides = [bulk("A", reported_direction="outgoing"), bulk("B", reported_direction="incoming")]
    result = policy.consolidate_reported_transfers(sides)
    assert result.status == "refused" and result.record_ids == tuple(r["record_id"] for r in sides)
    assert "preserve_both_reported_sides" in result.warnings


def test_quality_notice_never_excludes_records_or_infers_donor_identity():
    observation = bulk(reporting_committee_id="C12345678")
    notice = row(
        "notice",
        committee_id="C12345678",
        notice_kind="publisher_false_fictitious_filings_list",
        notice_scope="source_listed_committee",
        exclusion_status="no_automatic_exclusion",
    )
    result = policy.quality_notice_effect(observation, notice)
    assert result["association"] == "same_reported_committee_id"
    assert not result["automatic_exclusion"] and not result["donor_identity_inferred"]
    assert (
        policy.quality_notice_effect(bulk(contributor_native_id="C12345678"), notice)["association"]
        == "no_qualified_record_association"
    )
    assert policy.quality_notice_effect(observation, {**notice, "notice_kind": "unknown"})["definition"] is None


@pytest.mark.parametrize(
    "change",
    [
        {"source_authority": "unofficial-senate-retained"},
        {"source_authority": None},
        {"mapping_version": "unsupported/999"},
        {"mapping_version": None},
        {"identity_version": "unsupported/999"},
        {"value_mapping_version": None},
        {"value_mapping_version": "unsupported/999"},
        {"mapping_status": "refused"},
    ],
)
def test_every_eligible_financial_rule_refuses_unknown_producer_or_authority(change):
    loan = row(definition_set_id=policy._LOAN_LAYOUT, **amount("outstanding_balance", "90"))
    debt = row(definition_set_id=policy._DEBT_LAYOUT, **amount("incurred_in_period", "25"))
    allocated = row(definition_set_id=policy._ALLOCATION_LAYOUT, **amount("total_amount", "100"))
    share = row(
        source_namespace="fec-bulk-electioneering-candidate-disbursement-csv",
        reported_candidate_count="3",
        **amount("allocated_candidate_amount", "33.33"),
    )
    committee = summary(TTL_CONTB="100", TTL_CONTB_REF="20", NET_CONTB="80")
    candidate = summary(
        "candidate-web-summary/1", TTL_RECEIPTS="150", TTL_DISB="140", TRANS_FROM_AUTH="50", TRANS_TO_AUTH="40"
    )
    cases = [
        (bulk(), policy.bulk_observation_eligibility),
        (spend(), lambda r: policy.spending_observation_sum([r])),
        (share, policy.publisher_calculated_candidate_share),
        (loan, lambda r: policy.state_measure([r], table="fec_loans", field="outstanding_balance")),
        (
            debt,
            lambda r: policy.state_measure(
                [r], table="fec_debts", field="incurred_in_period", purpose="period_activity"
            ),
        ),
        (allocated, policy.allocated_payment),
        (committee, policy.committee_summary_net_contributions),
        (committee, lambda r: policy.reported_summary_measure(r, "NET_CONTB")),
        (candidate, lambda r: policy.candidate_summary_transfer_adjustment(r, direction="receipts")),
    ]
    for value, rule in cases:
        assert rule(value).status == "eligible"
        assert rule({**value, **change}).status == "refused"


def test_summary_layout_and_measure_must_both_belong_to_exact_mapper():
    observation = summary(TTL_CONTB="100", TTL_CONTB_REF="20", NET_CONTB="80")
    assert (
        policy.reported_summary_measure(
            {**observation, "source_namespace": "fec-bulk-candidate-summary-csv"}, "NET_CONTB"
        ).status
        == "refused"
    )
    assert policy.reported_summary_measure({**observation, "summary_type": "future/1"}, "NET_CONTB").status == "refused"
    observation["measures"].append(
        dict(native_field="invented_money", raw_value="10", value=Decimal("10"), value_status="exact", unit="USD")
    )
    assert policy.reported_summary_measure(observation, "invented_money").status == "refused"
    observation["measures"][0]["raw_value"] = "99"
    assert policy.reported_summary_measure(observation, "TTL_CONTB").status == "refused"


def test_filing_definition_version_and_native_value_are_not_labels_to_guess_from():
    loan = row(definition_set_id=policy._LOAN_LAYOUT, **amount("outstanding_balance", "90"))
    for change in [
        {"declared_format_version": "8.4"},
        {"source_namespace": "fec-paper-filing-schedule"},
        {"definition_set_id": policy._DEBT_LAYOUT},
        {"outstanding_balance_raw": "89"},
    ]:
        assert (
            policy.state_measure([{**loan, **change}], table="fec_loans", field="outstanding_balance").status
            == "refused"
        )
    assert policy.spending_observation_sum([{**spend(), "amount_raw": "99"}]).status == "refused"


def test_quality_notice_requires_its_mapper_and_a_supported_record_before_associating():
    observation = bulk(reporting_committee_id="C12345678")
    notice = row(
        "notice",
        committee_id="C12345678",
        notice_kind="publisher_false_fictitious_filings_list",
        notice_scope="source_listed_committee",
        exclusion_status="no_automatic_exclusion",
    )
    for change in [
        {"source_authority": "third-party"},
        {"mapping_version": "unknown/1"},
        {"source_namespace": "other"},
    ]:
        result = policy.quality_notice_effect(observation, {**notice, **change})
        assert result["association"] == "no_qualified_record_association" and result["definition"] is None
        result = policy.quality_notice_effect({**observation, **change}, notice)
        assert result["association"] == "no_qualified_record_association"


def test_union_null_column_does_not_invent_a_native_individual_amount_field():
    observation = bulk(amount_raw=None)
    assert policy.bulk_observation_eligibility(observation).value == Decimal("100")
    spending = spend()
    del spending["amount_raw"]
    assert policy.spending_observation_sum([spending]).status == "refused"


def test_other_known_mapper_cannot_authorize_summary_or_state_labels():
    other = {k: v for k, v in bulk().items() if k in {"mapping_version", "source_namespace"}}
    summary_row = summary(TTL_CONTB="100", TTL_CONTB_REF="20", NET_CONTB="80")
    assert policy.reported_summary_measure({**summary_row, **other}, "NET_CONTB").status == "refused"
    loan = row(definition_set_id=policy._LOAN_LAYOUT, **amount("outstanding_balance", "90"))
    assert policy.state_measure([{**loan, **other}], table="fec_loans", field="outstanding_balance").status == "refused"
    allocation = row(definition_set_id=policy._ALLOCATION_LAYOUT, **amount("total_amount", "100"))
    assert policy.allocated_payment({**allocation, **other}).status == "refused"


@pytest.mark.parametrize("version", ["fec-identity-observations/1", "fec-identity-observations/3", "unknown"])
def test_quality_notice_rejects_old_and_future_mapper_versions(version):
    observation = bulk(reporting_committee_id="C12345678")
    notice = row(
        "notice",
        committee_id="C12345678",
        notice_kind="publisher_false_fictitious_filings_list",
        notice_scope="source_listed_committee",
        exclusion_status="no_automatic_exclusion",
    )
    assert policy.quality_notice_effect(observation, notice)["association"] == "same_reported_committee_id"
    result = policy.quality_notice_effect(observation, {**notice, "mapping_version": version})
    assert result["association"] == "no_qualified_record_association" and result["automatic_exclusion"] is False
