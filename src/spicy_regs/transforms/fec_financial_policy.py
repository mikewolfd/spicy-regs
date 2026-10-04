"""Named financial query guards for retained FEC observations.

These rules distinguish a source observation, a reported measure and an economic
total. Retained definitions qualify the first two for specific purposes. Scope
selection can prove current membership; it cannot prove economic additivity.
No rule guesses a refund from its sign, an attribution from free text, or an
amendment/transfer relationship from matching amounts. Inputs are bounded query
selections, never whole-corpus Python collections.
"""

from dataclasses import dataclass
from decimal import Decimal, localcontext

from .fec_financial_selection import POLICY_VERSION as SELECTION_VERSION, ScopeSelection
from .fec_query import _digest, exact_amount
from spicy_regs.fec_financial_rules import (
    POLICY_VERSION,
    SOURCE_GENERATION as SOURCE_GENERATION,
    Definition,
    IDENTITY_VERSION,
    VALUE_MAPPING_VERSION,
    INDIVIDUAL_MEMO as INDIVIDUAL_MEMO,
    INTERCOMMITTEE_MEMO as INTERCOMMITTEE_MEMO,
    CANDIDATE_TRANSACTION_MEMO as CANDIDATE_TRANSACTION_MEMO,
    ELECTIONEERING_SHARE,
    SUMMARY_NET,
    CANDIDATE_TRANSFERS,
    QUALITY_NOTICE,
    _MEMO_DEFINITIONS,
    _LOAN_LAYOUT,
    _DEBT_LAYOUT,
    _ALLOCATION_LAYOUT,
    _BULK_MAPPINGS,
    _FILING_MAPPINGS,
    _SUMMARY_MAPPINGS,
)


def _source_identity(row, mapping_version, namespace):
    return (
        row.get("source_authority") == "official-fec"
        and row.get("identity_version") == IDENTITY_VERSION
        and row.get("mapping_version") == mapping_version
        and row.get("source_namespace") == namespace
        and row.get("mapping_status") in ("mapped", "partial")
    )


def _supported(row):
    """Require the current producer identity before assigning financial meaning.

    Pinned release admission verifies bytes and endpoints separately. It does
    not make an unknown mapper, a different authority, or an older value shape
    eligible for these particular rules. Partial rows can expose an exact
    selected measure while unrelated dates or fields remain unresolved.
    """
    if row.get("value_mapping_version") != VALUE_MAPPING_VERSION:
        return False
    namespace = row.get("source_namespace")
    if isinstance(namespace, str) and namespace in _BULK_MAPPINGS:
        return _source_identity(row, _BULK_MAPPINGS[namespace], namespace)
    if namespace == "fec-electronic-filing-schedule":
        layout = row.get("definition_set_id")
        return (
            isinstance(layout, str)
            and layout in _FILING_MAPPINGS
            and row.get("declared_format_version") == "8.5"
            and _source_identity(row, _FILING_MAPPINGS[layout], namespace)
        )
    summary_type = row.get("summary_type")
    if isinstance(summary_type, str) and summary_type in _SUMMARY_MAPPINGS:
        return _source_identity(row, "fec-" + summary_type, "fec-bulk-" + summary_type.split("/")[0])
    return False


@dataclass(frozen=True)
class FinancialDecision:
    query: str
    status: str
    reason: str
    record_ids: tuple[str, ...]
    value: Decimal | None = None
    measure_basis: str | None = None
    definitions: tuple[Definition, ...] = ()
    layout_pins: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    policy_version: str = POLICY_VERSION
    current_financial_total_qualified: bool = False


def _decision(query, rows, status, reason, **kwargs):
    ids = tuple(_digest(r["record_id"]) for r in rows)
    return FinancialDecision(query, status, reason, ids, **kwargs)


def _amount(row, field):
    value = row.get(field)
    if row.get(field + "_status") != "exact" or not isinstance(value, Decimal) or not value.is_finite():
        return None
    exact, status = exact_amount(format(value, "f"))
    if status != "exact":
        return None
    native, native_status = exact_amount(row.get(field + "_raw"))
    if native_status != "exact" or native != exact:
        return None
    return exact


def _arithmetic(values, signs=None):
    with localcontext() as context:
        context.prec = 80
        total = sum((value * sign for value, sign in zip(values, signs or [1] * len(values), strict=True)), Decimal(0))
    value, status = exact_amount(format(total, "f"))
    return value if status == "exact" else None


def _snapshot(row):
    return row.get("source_representation_role") == "snapshot" and row.get("correction_operation") == "none"


def current_membership_guard(rows, selection: ScopeSelection | None):
    """Require exact selected observation membership, without qualifying a sum.

    The scope-selection owner binds values and source proof into its membership
    pin. Callers must retain that evidence; this guard checks the returned set
    and decisions rather than pretending member IDs prove financial meaning.
    """
    query = "current_membership"
    if any(not _supported(row) for row in rows):
        return _decision(query, rows, "refused", "input_mapping_or_authority_unqualified")
    ids = [row["record_id"] for row in rows]
    if selection is None or not selection.qualified:
        return _decision(query, rows, "refused", "current_scope_unresolved")
    if selection.policy_version != SELECTION_VERSION or selection.membership_sha256 is None:
        return _decision(query, rows, "refused", "selection_policy_or_membership_pin_missing")
    _digest(selection.membership_sha256)
    if (
        len(ids) != len(set(ids))
        or set(ids) != set(selection.member_ids)
        or len(set(selection.member_ids)) != len(selection.member_ids)
    ):
        return _decision(query, rows, "refused", "selected_membership_differs_or_multiplies")
    decision_ids = [d.record_id for d in selection.decisions]
    included = {d.record_id for d in selection.decisions if d.status == "included"}
    if len(decision_ids) != len(set(decision_ids)) or included != set(ids):
        return _decision(query, rows, "refused", "selected_inclusion_decisions_differ")
    return _decision(
        query, rows, "eligible", "exact_current_membership_only", warnings=("economic_additivity_not_established",)
    )


def bulk_observation_eligibility(row, *, purpose="source_analysis"):
    """Apply retained bulk memo definitions only to their named source families.

    Source analysis includes X memos. Detailed-summary contribution excludes X;
    that exclusion alone does not reconstruct a complete reported/current total.
    A nonempty memo description is never an exclusion or attribution rule.
    """
    if purpose not in {"source_analysis", "detailed_summary_component", "gross_receipts", "net_receipts"}:
        raise ValueError("Unsupported bulk financial query purpose")
    query = "bulk_" + purpose
    if not _supported(row):
        return _decision(query, [row], "refused", "input_mapping_or_authority_unqualified")
    definition = _MEMO_DEFINITIONS.get(row.get("source_namespace"))
    if definition is None:
        return _decision(query, [row], "refused", "source_family_memo_rule_unavailable")
    if not _snapshot(row):
        return _decision(
            query, [row], "refused", "correction_payload_is_not_an_additional_receipt", definitions=(definition,)
        )
    if purpose in {"gross_receipts", "net_receipts"}:
        return _decision(
            query,
            [row],
            "refused",
            "refund_return_attribution_and_direction_rules_unqualified",
            definitions=(definition,),
        )
    value = _amount(row, "amount")
    if value is None or row.get("currency") != "USD":
        return _decision(query, [row], "refused", "amount_or_currency_unqualified", definitions=(definition,))
    if "memo_indicator" not in row:
        return _decision(query, [row], "refused", "memo_indicator_not_projected", definitions=(definition,))
    memo = row.get("memo_indicator")
    if memo not in {None, "", "X"}:
        return _decision(query, [row], "refused", "memo_code_meaning_unqualified", definitions=(definition,))
    if purpose == "detailed_summary_component" and memo == "X":
        return _decision(query, [row], "excluded", "publisher_memo_not_in_detailed_summary", definitions=(definition,))
    warnings = ["source_observation_not_current_net_money"]
    if memo == "X":
        warnings.append("memo_included_for_source_analysis_attribution_or_prior_reporting_unresolved")
    if value < 0:
        warnings.append("negative_sign_preserved_refund_not_inferred")
    return _decision(
        query,
        [row],
        "eligible",
        "source_defined_purpose",
        value=value,
        measure_basis="signed_reported_observation",
        definitions=(definition,),
        warnings=tuple(warnings),
    )


def publisher_calculated_candidate_share(row):
    """Expose the FEC's stored equal-share calculation; never call it reported allocation."""
    query = "publisher_calculated_candidate_share"
    if not _supported(row):
        return _decision(query, [row], "refused", "input_mapping_or_authority_unqualified")
    if row.get("source_namespace") != "fec-bulk-electioneering-candidate-disbursement-csv" or not _snapshot(row):
        return _decision(query, [row], "refused", "source_grain_not_supported", definitions=(ELECTIONEERING_SHARE,))
    value = _amount(row, "allocated_candidate_amount")
    count = row.get("reported_candidate_count")
    if (
        row.get("currency") != "USD"
        or value is None
        or not isinstance(count, str)
        or not count.isascii()
        or not count.isdigit()
        or int(count) < 1
    ):
        return _decision(
            query,
            [row],
            "refused",
            "calculated_share_or_candidate_count_not_exact",
            definitions=(ELECTIONEERING_SHARE,),
        )
    return _decision(
        query,
        [row],
        "eligible",
        "publisher_equal_share_value_preserved",
        value=value,
        measure_basis="publisher_calculated_equal_candidate_share",
        definitions=(ELECTIONEERING_SHARE,),
        warnings=(
            "not_filer_reported_allocation",
            "event_and_amendment_equivalence_unresolved",
            "publisher_rounding_not_recomputed",
        ),
    )


def spending_observation_sum(rows, target_links=()):
    """Sum distinct supplied spend observations before their one-to-many target join.

    This is explicitly an observation sum, not amendment-qualified event money.
    Candidate-associated electioneering rows need event equivalence first.
    Target links use spending_record_id/target_id; discovery never changes value.
    """
    query = "spending_record_observation_sum"
    if not rows:
        return _decision(query, rows, "refused", "empty_selection_not_source_qualified")
    if any(not _supported(row) for row in rows):
        return _decision(query, rows, "refused", "input_mapping_or_authority_unqualified")
    ids = [r["record_id"] for r in rows]
    if len(ids) != len(set(ids)):
        return _decision(query, rows, "refused", "spending_rows_multiplied")
    allowed = {"fec-bulk-independent-expenditure-csv", "fec-bulk-communication-cost-csv"}
    if any(r.get("observation_grain") == "candidate-associated-disbursement" for r in rows):
        return _decision(
            query, rows, "refused", "candidate_rows_require_event_equivalence", definitions=(ELECTIONEERING_SHARE,)
        )
    if any(r.get("source_namespace") not in allowed or not _snapshot(r) for r in rows):
        return _decision(query, rows, "refused", "spending_grain_or_source_role_unqualified")
    pairs = [(link["spending_record_id"], link["target_id"]) for link in target_links]
    if len(pairs) != len(set(pairs)) or any(rid not in ids or not target for rid, target in pairs):
        return _decision(query, rows, "refused", "target_link_multiplicity_or_endpoint_invalid")
    values = [_amount(row, "amount") for row in rows]
    if (
        any(value is None for value in values)
        or len({r.get("currency") for r in rows}) != 1
        or rows[0].get("currency") != "USD"
    ):
        return _decision(query, rows, "refused", "amount_or_currency_unqualified")
    total = _arithmetic(values)
    if total is None:
        return _decision(query, rows, "refused", "aggregate_decimal_overflow")
    return _decision(
        query,
        rows,
        "eligible",
        "base_observations_summed_before_targets",
        value=total,
        measure_basis="signed_source_observation_sum",
        warnings=("amendment_and_event_equivalence_not_qualified",),
    )


def state_measure(rows, *, table, field, purpose="reported_snapshot"):
    """Expose one loan/debt state or one explicitly named period activity field."""
    query = table + "_" + purpose
    rules = {
        "fec_loans": (_LOAN_LAYOUT, {"original_loan_amount", "payments_to_date", "outstanding_balance"}, set()),
        "fec_debts": (_DEBT_LAYOUT, {"opening_balance", "closing_balance"}, {"incurred_in_period", "paid_in_period"}),
    }
    if table not in rules or purpose not in {"reported_snapshot", "period_activity"}:
        raise ValueError("Unsupported loan/debt query")
    layout, stocks, activity = rules[table]
    if len(rows) != 1:
        return _decision(query, rows, "refused", "select_one_reported_state_do_not_sum_repeated_balances")
    row = rows[0]
    if not _supported(row):
        return _decision(query, rows, "refused", "input_mapping_or_authority_unqualified")
    if (
        row.get("source_namespace") != "fec-electronic-filing-schedule"
        or row.get("definition_set_id") != layout
        or not _snapshot(row)
        or row.get("currency") != "USD"
    ):
        return _decision(query, rows, "refused", "state_definition_or_source_role_unqualified", layout_pins=(layout,))
    if field not in (stocks if purpose == "reported_snapshot" else activity):
        return _decision(
            query, rows, "refused", "stock_cumulative_and_period_activity_are_distinct", layout_pins=(layout,)
        )
    value = _amount(row, field)
    if value is None:
        return _decision(query, rows, "refused", "state_measure_not_exact", layout_pins=(layout,))
    return _decision(
        query,
        rows,
        "eligible",
        "single_reported_state_measure",
        value=value,
        measure_basis=field,
        layout_pins=(layout,),
        warnings=("latest_or_period_end_selection_not_inferred",),
    )


def allocated_payment(row, *, fields=("total_amount",)):
    """Choose the payment or its shares, never total plus shares or YTD activity."""
    query = "allocated_payment_measure"
    if not _supported(row):
        return _decision(query, [row], "refused", "input_mapping_or_authority_unqualified")
    choices = {("total_amount",), ("federal_share",), ("nonfederal_share",), ("federal_share", "nonfederal_share")}
    if tuple(fields) not in choices:
        return _decision(query, [row], "refused", "total_components_and_ytd_cannot_be_added")
    if (
        row.get("source_namespace") != "fec-electronic-filing-schedule"
        or row.get("definition_set_id") != _ALLOCATION_LAYOUT
        or not _snapshot(row)
        or row.get("currency") != "USD"
    ):
        return _decision(
            query,
            [row],
            "refused",
            "allocation_definition_or_source_role_unqualified",
            layout_pins=(_ALLOCATION_LAYOUT,),
        )
    values = [_amount(row, name) for name in fields]
    if any(value is None for value in values):
        return _decision(query, [row], "refused", "allocation_value_not_exact", layout_pins=(_ALLOCATION_LAYOUT,))
    total = _arithmetic(values)
    if total is None:
        return _decision(query, [row], "refused", "aggregate_decimal_overflow")
    if len(fields) == 2 and total != _amount(row, "total_amount"):
        return _decision(
            query, [row], "refused", "reported_components_do_not_reconcile_to_total", layout_pins=(_ALLOCATION_LAYOUT,)
        )
    return _decision(
        query,
        [row],
        "eligible",
        "one_nonoverlapping_measure",
        value=total,
        measure_basis="+".join(fields),
        layout_pins=(_ALLOCATION_LAYOUT,),
        warnings=("reported_row_only",),
    )


def _summary_values(row, names):
    if not _supported(row) or not _snapshot(row) or row.get("currency") != "USD":
        return None
    summary_type = row.get("summary_type")
    if not isinstance(summary_type, str) or not _source_identity(
        row, "fec-" + summary_type, "fec-bulk-" + summary_type.split("/")[0]
    ):
        return None
    if summary_type not in _SUMMARY_MAPPINGS or any(
        name not in _SUMMARY_MAPPINGS[summary_type].money_fields for name in names
    ):
        return None
    values = []
    for name in names:
        matches = [m for m in row.get("measures", ()) if m["native_field"] == name]
        if len(matches) != 1 or matches[0].get("unit") != "USD":
            return None
        item = matches[0]
        value = _amount(
            {"value": item.get("value"), "value_status": item.get("value_status"), "value_raw": item.get("raw_value")},
            "value",
        )
        if value is None:
            return None
        values.append(value)
    return values


def reported_summary_measure(row, field):
    """One source-reported summary measure, including subtotals, remains useful as stated."""
    values = _summary_values(row, [field])
    if values is None:
        return _decision("reported_summary_measure", [row], "refused", "summary_measure_missing_ambiguous_or_inexact")
    return _decision(
        "reported_summary_measure",
        [row],
        "eligible",
        "single_source_reported_measure",
        value=values[0],
        measure_basis=field,
        warnings=("do_not_add_to_its_components_or_itemized_rows", "period_and_population_remain_as_reported"),
    )


def committee_summary_net_contributions(row):
    """Check the publisher's defined contributions-minus-refunds equation exactly."""
    query = "reported_committee_net_contributions"
    if row.get("summary_type") != "committee-summary-csv/1":
        return _decision(query, [row], "refused", "summary_layout_not_supported", definitions=(SUMMARY_NET,))
    values = _summary_values(row, ["TTL_CONTB", "TTL_CONTB_REF", "NET_CONTB"])
    if values is None:
        return _decision(
            query, [row], "refused", "summary_component_missing_ambiguous_or_inexact", definitions=(SUMMARY_NET,)
        )
    calculated = _arithmetic(values[:2], [1, -1])
    if calculated is None or calculated != values[2]:
        return _decision(
            query, [row], "refused", "reported_net_differs_from_defined_components", definitions=(SUMMARY_NET,)
        )
    return _decision(
        query,
        [row],
        "eligible",
        "source_reported_net_equation_reconciled",
        value=calculated,
        measure_basis="TTL_CONTB-TTL_CONTB_REF=NET_CONTB",
        definitions=(SUMMARY_NET,),
        warnings=("signs_preserved_without_absolute_value", "not_a_net_total_rebuilt_from_itemizations"),
    )


def candidate_summary_transfer_adjustment(row, *, direction):
    """Apply the all-candidates file's explicit authorized-committee transfer rule."""
    if direction not in {"receipts", "disbursements"}:
        raise ValueError("Candidate activity direction must be receipts or disbursements")
    query = "candidate_summary_transfer_adjusted_" + direction
    if row.get("summary_type") != "candidate-web-summary/1":
        return _decision(
            query, [row], "refused", "candidate_summary_layout_not_supported", definitions=(CANDIDATE_TRANSFERS,)
        )
    values = _summary_values(row, ["TTL_RECEIPTS", "TTL_DISB", "TRANS_FROM_AUTH", "TRANS_TO_AUTH"])
    if values is None or values[2] == 0 or values[3] == 0:
        return _decision(
            query,
            [row],
            "refused",
            "publisher_both_transfer_fields_condition_not_met",
            definitions=(CANDIDATE_TRANSFERS,),
        )
    indexes = (0, 2) if direction == "receipts" else (1, 3)
    value = _arithmetic([values[i] for i in indexes], [1, -1])
    if value is None:
        return _decision(query, [row], "refused", "aggregate_decimal_overflow")
    return _decision(
        query,
        [row],
        "eligible",
        "publisher_candidate_summary_transfer_rule",
        value=value,
        measure_basis="reported_total_minus_authorized_committee_transfer",
        definitions=(CANDIDATE_TRANSFERS,),
        warnings=("does_not_pair_or_deduplicate_transaction_rows",),
    )


def consolidate_reported_transfers(rows):
    """Retained bulk sides have no qualified cross-filer event/direction proof."""
    return _decision(
        "consolidated_transfer_flow",
        rows,
        "refused",
        "transfer_direction_pairing_and_scope_evidence_required",
        warnings=("preserve_both_reported_sides", "equal_amount_date_or_names_do_not_prove_a_transfer_pair"),
    )


def summary_itemized_overlap_guard(summary_rows, itemized_rows, *, operation="add"):
    """Keep reported and itemized totals separate until their scopes are qualified."""
    if operation not in {"add", "reconcile"}:
        raise ValueError("Unsupported summary/itemized operation")
    reason = (
        "summary_and_itemizations_are_overlapping_measures"
        if operation == "add"
        else "comparison_period_population_and_unitemized_scope_evidence_required"
    )
    return _decision(
        "summary_itemized_" + operation,
        [*summary_rows, *itemized_rows],
        "refused",
        reason,
        warnings=("residual_is_not_automatically_an_error",),
    )


def quality_notice_effect(row, notice):
    """A source-listed committee notice annotates; it does not identify or exclude donors."""
    committee = row.get("reporting_committee_id")
    supported = (
        _source_identity(notice, "fec-identity-observations/2", "fec-bulk-false-fictitious-notice")
        and notice.get("notice_kind") == "publisher_false_fictitious_filings_list"
        and notice.get("notice_scope") == "source_listed_committee"
        and notice.get("exclusion_status") == "no_automatic_exclusion"
    )
    matches = supported and _supported(row) and bool(committee) and committee == notice.get("committee_id")
    return dict(
        policy_version=POLICY_VERSION,
        record_id=_digest(row["record_id"]),
        notice_id=_digest(notice["record_id"]),
        association="same_reported_committee_id" if matches else "no_qualified_record_association",
        automatic_exclusion=False,
        donor_identity_inferred=False,
        reason="notice_reports_unverified_registration_not_a_transaction_exclusion"
        if supported
        else "notice_policy_unqualified",
        definition=QUALITY_NOTICE if supported else None,
    )
