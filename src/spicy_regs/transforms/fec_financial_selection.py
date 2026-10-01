"""Select a bounded, evidenced reporting scope without inferring amendments.

The caller supplies source-qualified scope, order, completeness and exact prior
membership. This module checks and applies that evidence. It never derives these
facts from capture dates, format names, amount signs or an amendment indicator.
Unresolved membership provides no qualified current-record set. Selecting current
records does not establish that their amounts may be added for a given purpose.
"""

from dataclasses import asdict, dataclass
from decimal import Decimal
import hashlib
from typing import NoReturn

from .fec_query import _digest, _json, _text

POLICY_VERSION = "fec-evidenced-scope-selection/1"


@dataclass(frozen=True)
class FinancialRecord:
    record_id: str
    transaction_key: str
    amount: Decimal | None
    financial_fields_sha256: str
    scope_id: str

    def __post_init__(self):
        _digest(self.record_id)
        _text(self.transaction_key)
        _digest(self.financial_fields_sha256)
        _text(self.scope_id)
        if self.amount is not None and (not isinstance(self.amount, Decimal) or not self.amount.is_finite()):
            raise ValueError("Financial selection requires exact finite amounts or explicit null")


def membership_pin(records):
    """Bind exact observations, scoped native keys and interpreted value facts."""
    facts = sorted(
        (
            r.record_id,
            r.scope_id,
            r.transaction_key,
            str(r.amount) if r.amount is not None else None,
            r.financial_fields_sha256,
        )
        for r in records
    )
    return "sha256:" + hashlib.sha256(_json([POLICY_VERSION, facts]).encode()).hexdigest()


@dataclass(frozen=True)
class SelectionStep:
    step_id: str
    scope_id: str
    sequence: int | None
    mode: str
    expected_prior_membership: str | None
    evidence_sha256: str | None
    membership_complete: bool
    records: tuple[FinancialRecord, ...] = ()
    removed_record_ids: tuple[str, ...] = ()
    operation_record_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class EquivalentRecord:
    record: FinancialRecord
    canonical_record_id: str
    evidence_sha256: str


@dataclass(frozen=True)
class InclusionDecision:
    record_id: str
    status: str
    reason: str
    evidence_sha256: str | None
    replacing_record_id: str | None = None
    replacing_scope_step_id: str | None = None


@dataclass(frozen=True)
class ScopeSelection:
    scope_id: str
    qualified: bool
    reason: str | None
    member_ids: tuple[str, ...]
    membership_sha256: str | None
    decisions: tuple[InclusionDecision, ...]
    policy_version: str = POLICY_VERSION
    operation_evidence: tuple[tuple[str, str], ...] = ()

    def require_members(self):
        if not self.qualified:
            raise ValueError(f"Unresolved FEC current selection: {self.reason}")
        return self.member_ids


class _Unresolved(Exception):
    pass


def select_scope(
    scope_id: str,
    base: tuple[FinancialRecord, ...],
    steps: tuple[SelectionStep, ...],
    *,
    base_membership_sha256: str | None,
    base_evidence_sha256: str | None,
    base_complete: bool,
    equivalents: tuple[EquivalentRecord, ...] = (),
):
    """Return exact current membership and decisions, or refuse the whole scope.

    Complete replacements remove omitted prior records only with complete scope
    membership. Partial amendments replace matching native keys and retain others;
    deletion requires an explicit prior observation ID. Bulk corrections also use
    explicit removal IDs and reject insertions colliding with current native keys.
    Already-applied operations must cite the exact selected base and may only name
    identical records already in it. Repeated identical step IDs are idempotent.

    ``evidence_sha256`` identifies retained proof of operation identity, source
    rule, applicability window, sequence and scope. Release validation must check
    those artifacts; a syntactically valid digest alone is not source proof.
    """
    _text(scope_id)
    all_records = list(base) + [r for step in steps for r in step.records] + [e.record for e in equivalents]
    operation_ids = {rid for step in steps for rid in step.operation_record_ids}
    known = {}
    decisions = {}

    def refuse(reason) -> NoReturn:
        raise _Unresolved(reason)

    def proof(pin, reason):
        if pin is None:
            refuse(reason)
        _digest(pin)

    def indexed(records):
        result = {}
        ids = set()
        for record in records:
            if record.transaction_key in result or record.record_id in ids:
                refuse("ambiguous-native-target-or-multiplicity")
            result[record.transaction_key] = record
            ids.add(record.record_id)
        return result

    try:
        for record in all_records:
            if record.scope_id != scope_id:
                refuse("observation-scope-mismatch")
            if record.record_id in known and known[record.record_id] != record:
                refuse("conflicting-observation-identity")
            known[record.record_id] = record
        proof(base_evidence_sha256, "base-evidence-unavailable")
        proof(base_membership_sha256, "base-membership-unavailable")
        if base_complete is not True:
            refuse("base-membership-incomplete")
        if membership_pin(base) != base_membership_sha256:
            refuse("base-membership-pin-mismatch")
        current = indexed(base)
        equivalence_by_id = {}
        for equivalent in equivalents:
            proof(equivalent.evidence_sha256, "representation-equivalence-unavailable")
            canonical = known.get(equivalent.canonical_record_id)
            if canonical is None:
                refuse("representation-canonical-observation-unavailable")
            a, b = asdict(equivalent.record), asdict(canonical)
            del a["record_id"], b["record_id"]
            if a != b:
                refuse("representation-financial-values-differ")
            if equivalent.record.record_id in equivalence_by_id:
                refuse("ambiguous-representation-equivalence")
            equivalence_by_id[equivalent.record.record_id] = equivalent
        for record in base:
            decisions[record.record_id] = InclusionDecision(
                record.record_id, "included", "selected-base", base_evidence_sha256
            )
        seen = {}
        seen_operation_ids = set()
        sequence = 0
        for step in steps:
            _text(step.step_id)
            if step.step_id in seen:
                if step != seen[step.step_id]:
                    refuse("conflicting-repeated-operation")
                continue
            seen[step.step_id] = step
            for rid in step.operation_record_ids:
                _digest(rid)
                if rid in known or rid in seen_operation_ids:
                    refuse("ambiguous-operation-observation-identity")
                seen_operation_ids.add(rid)
            proof(step.evidence_sha256, "operation-evidence-unavailable")
            if step.scope_id != scope_id:
                refuse("operation-scope-mismatch")
            if type(step.sequence) is not int or step.sequence != sequence + 1:
                refuse("unknown-or-conflicting-operation-order")
            sequence = step.sequence
            proof(step.expected_prior_membership, "operation-base-unavailable")
            if step.expected_prior_membership != membership_pin(current.values()):
                refuse("operation-base-mismatch")
            if step.membership_complete is not True:
                refuse("affected-membership-incomplete")
            if step.removed_record_ids and not step.operation_record_ids:
                refuse("deletion-operation-observation-unavailable")
            for rid in step.operation_record_ids:
                decisions[rid] = InclusionDecision(
                    rid,
                    "outside-selection",
                    "source-correction-operation",
                    step.evidence_sha256,
                    replacing_scope_step_id=step.step_id,
                )
            new = indexed(step.records)
            if len(step.removed_record_ids) != len(set(step.removed_record_ids)):
                refuse("ambiguous-removal-multiplicity")
            current_ids = {r.record_id: key for key, r in current.items()}
            if step.mode == "already-applied":
                if step.removed_record_ids:
                    refuse("already-applied-operation-not-proven-in-base")
                for key, record in new.items():
                    if current.get(key) == record:
                        continue
                    equivalent = equivalence_by_id.get(record.record_id)
                    prior = current.get(key)
                    if equivalent is None or prior is None or equivalent.canonical_record_id != prior.record_id:
                        refuse("already-applied-operation-not-proven-in-base")
                    if record.record_id in decisions:
                        refuse("representation-observation-already-selected")
                    decisions[record.record_id] = InclusionDecision(
                        record.record_id,
                        "duplicate",
                        "proven-equivalent-representation",
                        equivalent.evidence_sha256,
                        prior.record_id,
                    )
                continue
            if step.mode not in {"complete-replacement", "partial-amendment", "bulk-correction"}:
                refuse("unknown-replacement-mode")
            if any(rid not in current_ids for rid in step.removed_record_ids):
                refuse("absent-or-ambiguous-removal-target")
            if step.mode == "complete-replacement":
                if step.removed_record_ids:
                    refuse("complete-replacement-has-conflicting-explicit-removals")
                removed = tuple(current.values())
            else:
                removed = tuple(current[current_ids[rid]] for rid in step.removed_record_ids)
            for previous in removed:
                replacement = new.get(previous.transaction_key)
                if replacement is not None and previous.record_id == replacement.record_id:
                    refuse("operation-reuses-prior-observation")
                decisions[previous.record_id] = InclusionDecision(
                    previous.record_id,
                    "superseded" if replacement else "removed",
                    "replaced-in-scope"
                    if replacement
                    else "omitted-from-complete-replacement"
                    if step.mode == "complete-replacement"
                    else "explicit-source-deletion",
                    step.evidence_sha256,
                    replacement.record_id if replacement else None,
                    step.step_id,
                )
                del current[previous.transaction_key]
            for key, record in new.items():
                prior = current.get(key)
                if prior is not None:
                    if step.mode != "partial-amendment":
                        refuse("insertion-target-already-present")
                    if prior.record_id == record.record_id:
                        refuse("operation-reuses-prior-observation")
                    decisions[prior.record_id] = InclusionDecision(
                        prior.record_id,
                        "superseded",
                        "explicit-partial-amendment",
                        step.evidence_sha256,
                        record.record_id,
                        step.step_id,
                    )
                if record.record_id in decisions:
                    refuse("operation-reuses-prior-observation")
                current[key] = record
                decisions[record.record_id] = InclusionDecision(
                    record.record_id, "included", "selected-operation-result", step.evidence_sha256
                )
        for equivalent in equivalents:
            canonical = known.get(equivalent.canonical_record_id)
            alias = equivalent.record
            if canonical is None or canonical.record_id not in decisions:
                refuse("representation-canonical-observation-unavailable")
            if alias.record_id in decisions:
                prior_decision = decisions[alias.record_id]
                if prior_decision.status == "duplicate" and prior_decision.replacing_record_id == canonical.record_id:
                    continue
                refuse("representation-observation-already-selected")
            decisions[alias.record_id] = InclusionDecision(
                alias.record_id,
                "duplicate",
                "proven-equivalent-representation",
                equivalent.evidence_sha256,
                canonical.record_id,
            )
        return ScopeSelection(
            scope_id,
            True,
            None,
            tuple(sorted(r.record_id for r in current.values())),
            membership_pin(current.values()),
            tuple(decisions[k] for k in sorted(decisions)),
            operation_evidence=tuple((s.step_id, _digest(s.evidence_sha256)) for s in seen.values()),
        )
    except _Unresolved as exc:
        return ScopeSelection(
            scope_id,
            False,
            str(exc),
            (),
            None,
            tuple(
                InclusionDecision(rid, "unresolved", str(exc), None)
                for rid in sorted({r.record_id for r in all_records} | operation_ids)
            ),
        )
