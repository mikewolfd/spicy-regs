"""Exact member sets, not just totals, establish correction behavior."""

from dataclasses import replace
from decimal import Decimal
import hashlib
from typing import Any

import pytest

from spicy_regs.transforms.fec_financial_selection import (
    EquivalentRecord,
    FinancialRecord,
    SelectionStep,
    membership_pin,
    select_scope,
)


def pin(value):
    return "sha256:" + hashlib.sha256(value.encode()).hexdigest()


def row(identity, key, amount):
    return FinancialRecord(pin(identity), key, Decimal(amount), pin(f"{key}|{amount}"), "committee/report")


A = row("original-A", "committee/report/A", "100")
B = row("original-B", "committee/report/B", "200")
A2 = row("amendment-A", A.transaction_key, "150")
SCOPE = "committee/report"
EVIDENCE = pin("retained-source-proof")


def step(mode, *, records=(), removed=(), base=(A, B), **kwargs):
    kwargs.setdefault("operation_record_ids", (pin("deletion-stream-row"),) if removed else ())
    return replace(
        SelectionStep("amendment-1", SCOPE, 1, mode, membership_pin(base), EVIDENCE, True, records, removed),
        **kwargs,
    )


def select(steps=(), *, base=(A, B), equivalents=(), **kwargs):
    params: dict[str, Any] = dict(
        base_membership_sha256=membership_pin(base), base_evidence_sha256=EVIDENCE, base_complete=True
    )
    params.update(kwargs)
    return select_scope(SCOPE, base, steps, equivalents=equivalents, **params)


def decisions(result):
    return {r.record_id: r for r in result.decisions}


def test_complete_replacement_removes_omitted_b_and_retains_its_scope_evidence():
    result = select((step("complete-replacement", records=(A2,)),))
    assert result.require_members() == (A2.record_id,)
    d = decisions(result)
    assert d[A.record_id].status == "superseded" and d[A.record_id].replacing_record_id == A2.record_id
    assert d[B.record_id].status == "removed" and d[B.record_id].replacing_record_id is None
    assert d[B.record_id].reason == "omitted-from-complete-replacement"
    assert d[B.record_id].replacing_scope_step_id == "amendment-1"
    assert d[B.record_id].evidence_sha256 == EVIDENCE


def test_partial_amendment_preserves_unaffected_b():
    result = select((step("partial-amendment", records=(A2,)),))
    assert set(result.require_members()) == {A2.record_id, B.record_id}
    assert decisions(result)[B.record_id].reason == "selected-base"


def test_positive_amount_deletion_is_not_a_refund_or_negative_receipt():
    result = select((step("bulk-correction", removed=(A.record_id,)),))
    assert A.amount > 0 and result.require_members() == (B.record_id,)
    assert decisions(result)[A.record_id].reason == "explicit-source-deletion"
    witness = pin("deletion-stream-row")
    assert set(decisions(result)) == {A.record_id, B.record_id, witness}
    assert decisions(result)[witness].status == "outside-selection"
    assert decisions(result)[witness].reason == "source-correction-operation"
    assert decisions(result)[witness].evidence_sha256 == EVIDENCE
    assert decisions(result)[witness].replacing_scope_step_id == "amendment-1"


def test_source_proven_insertion_already_applied_does_not_increase_membership():
    result = select((step("already-applied", records=(A,)),))
    assert set(result.require_members()) == {A.record_id, B.record_id}
    assert result.membership_sha256 == membership_pin((A, B))


def test_equal_amount_and_native_key_do_not_establish_already_applied_identity():
    copy = row("another observation", A.transaction_key, "100")
    result = select((step("already-applied", records=(copy,)),))
    assert not result.qualified and result.reason == "already-applied-operation-not-proven-in-base"


def test_distinct_insertion_witness_already_in_snapshot_needs_proven_equivalence():
    insertion = replace(A, record_id=pin("insertion stream row"))
    change = step("already-applied", records=(insertion,))
    result = select((change,), equivalents=(EquivalentRecord(insertion, A.record_id, EVIDENCE),))
    assert set(result.require_members()) == {A.record_id, B.record_id}
    assert decisions(result)[insertion.record_id].status == "duplicate"
    assert decisions(result)[insertion.record_id].replacing_record_id == A.record_id
    assert result.operation_evidence == ((change.step_id, EVIDENCE),)


def test_two_representations_of_amendment_count_once_with_equivalence_proof():
    copy = replace(A2, record_id=pin("paper rendition"))
    result = select(
        (step("complete-replacement", records=(A2,)),),
        equivalents=(EquivalentRecord(copy, A2.record_id, pin("representation comparison")),),
    )
    assert result.require_members() == (A2.record_id,)
    assert decisions(result)[copy.record_id].status == "duplicate"
    assert decisions(result)[copy.record_id].replacing_record_id == A2.record_id


def test_equivalence_requires_every_qualified_financial_field_to_match():
    copy = replace(A2, record_id=pin("different memo flag"), financial_fields_sha256=pin("memo-X"))
    result = select(
        (step("partial-amendment", records=(A2,)),), equivalents=(EquivalentRecord(copy, A2.record_id, EVIDENCE),)
    )
    assert not result.qualified and result.reason == "representation-financial-values-differ"


def test_repeated_operation_is_idempotent_and_conflicting_identity_refuses():
    change = step("bulk-correction", removed=(A.record_id,))
    assert select((change, change)) == select((change,))
    result = select((change, replace(change, removed_record_ids=(B.record_id,))))
    assert not result.qualified and result.reason == "conflicting-repeated-operation"


def test_ordered_delete_then_insert_binds_each_intermediate_membership():
    delete = step("bulk-correction", removed=(A.record_id,))
    insert = step("bulk-correction", records=(A2,), base=(B,), sequence=2, step_id="insert-2")
    result = select((delete, insert))
    assert set(result.require_members()) == {A2.record_id, B.record_id}
    out_of_order = select((insert, delete))
    assert not out_of_order.qualified and out_of_order.reason == "unknown-or-conflicting-operation-order"


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"expected_prior_membership": None}, "operation-base-unavailable"),
        ({"expected_prior_membership": pin("wrong base")}, "operation-base-mismatch"),
        ({"sequence": None}, "unknown-or-conflicting-operation-order"),
        ({"evidence_sha256": None}, "operation-evidence-unavailable"),
        ({"mode": "unknown"}, "unknown-replacement-mode"),
        ({"scope_id": "different committee/report"}, "operation-scope-mismatch"),
        ({"membership_complete": False}, "affected-membership-incomplete"),
    ],
)
def test_incomplete_or_unsupported_scope_never_exposes_qualified_members(changes, reason):
    result = select((replace(step("complete-replacement", records=(A2,)), **changes),))
    assert not result.qualified and result.reason == reason
    assert result.member_ids == () and result.membership_sha256 is None
    assert set(decisions(result)) == {A.record_id, B.record_id, A2.record_id}
    assert all(d.status == "unresolved" for d in result.decisions)
    with pytest.raises(ValueError, match="Unresolved"):
        result.require_members()


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"base_membership_sha256": None}, "base-membership-unavailable"),
        ({"base_membership_sha256": pin("wrong")}, "base-membership-pin-mismatch"),
        ({"base_evidence_sha256": None}, "base-evidence-unavailable"),
        ({"base_complete": False}, "base-membership-incomplete"),
    ],
)
def test_unknown_base_refuses_even_without_corrections(changes, reason):
    result = select(**changes)
    assert not result.qualified and result.reason == reason


def test_absent_deletion_and_duplicate_native_identity_refuse():
    result = select((step("bulk-correction", removed=(pin("absent"),)),))
    assert result.reason == "absent-or-ambiguous-removal-target"
    duplicate_key = replace(A, record_id=pin("another same-key row"))
    assert select(base=(A, duplicate_key)).reason == "ambiguous-native-target-or-multiplicity"


def test_observations_from_another_reporting_scope_cannot_be_selected():
    outside = replace(A, scope_id="another committee/report")
    assert select(base=(outside,)).reason == "observation-scope-mismatch"
    amendment = replace(A2, scope_id="another committee/report")
    assert select((step("partial-amendment", records=(amendment,)),)).reason == "observation-scope-mismatch"


def test_unproven_duplicate_insertion_cannot_be_silently_dropped():
    result = select((step("bulk-correction", records=(A2,)),))
    assert result.reason == "insertion-target-already-present"


def test_complete_empty_replacement_is_distinct_from_incomplete_capture():
    complete = select((step("complete-replacement"),))
    assert complete.qualified and complete.require_members() == ()
    incomplete = select((step("complete-replacement", membership_complete=False),))
    assert not incomplete.qualified


def test_negative_amount_null_amount_and_decimal_scale_survive_selection():
    negative = row("reported negative", "other/negative", "-17.001")
    null = replace(B, amount=None)
    result = select(base=(negative, null))
    assert result.qualified and set(result.member_ids) == {negative.record_id, null.record_id}
    # Membership selection grants no aggregate meaning to a null amount.
    assert null.amount is None


def test_membership_pin_is_order_independent_but_sensitive_to_exact_member_values():
    assert membership_pin((A, B)) == membership_pin((B, A))
    assert membership_pin((A, B)) != membership_pin((A2, B))
    assert membership_pin((A, B)) != membership_pin((A,))


def test_invalid_numeric_inputs_are_never_implicitly_coerced():
    with pytest.raises(ValueError, match="exact finite"):
        FinancialRecord(pin("x"), "key", 1.0, EVIDENCE, SCOPE)  # ty: ignore[invalid-argument-type]
    with pytest.raises(ValueError, match="exact finite"):
        FinancialRecord(pin("x"), "key", Decimal("NaN"), EVIDENCE, SCOPE)


def test_deletion_requires_a_separate_operation_observation():
    result = select((step("bulk-correction", removed=(A.record_id,), operation_record_ids=()),))
    assert not result.qualified and result.reason == "deletion-operation-observation-unavailable"


def test_unresolved_scope_accounts_for_operation_witnesses_too():
    witness = pin("deletion-stream-row")
    result = select((step("bulk-correction", removed=(A.record_id,), sequence=None),))
    assert not result.qualified
    assert set(decisions(result)) == {A.record_id, B.record_id, witness}
    assert all(d.status == "unresolved" for d in result.decisions)


@pytest.mark.parametrize("witnesses", [(A.record_id,), (pin("delete"), pin("delete"))])
def test_colliding_or_repeated_action_identity_refuses(witnesses):
    result = select((step("bulk-correction", removed=(A.record_id,), operation_record_ids=witnesses),))
    assert result.reason == "ambiguous-operation-observation-identity"
    assert not result.member_ids


def test_one_action_observation_cannot_describe_two_distinct_operations():
    first = step("bulk-correction", removed=(A.record_id,))
    second = step("bulk-correction", removed=(B.record_id,), base=(B,), step_id="delete-2", sequence=2)
    assert select((first, second)).reason == "ambiguous-operation-observation-identity"
    assert select((first, first)) == select((first,))
