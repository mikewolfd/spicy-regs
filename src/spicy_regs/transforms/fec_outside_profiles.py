"""Reconciled publisher Schedule E aggregates; no raw-notice reconstruction."""

from collections import defaultdict
from decimal import Decimal, localcontext
import hashlib
import json
import re

import pyarrow as pa

from spicy_regs.transforms.fec_query import exact_amount

POLICY = "fec-publisher-outside-spending/1"
PROFILES = "fec_outside_spending_profiles"
SPENDERS = "fec_outside_spending_spenders"
COMMON = ("profile_id candidate_id candidate_name support_oppose source_evidence_json source_generation "
          "period_start period_end election_basis amount_basis policy_version coverage_note").split()
SCHEMAS = {
    PROFILES: pa.schema([(n, pa.string()) for n in COMMON + ["total", "reported_event_dates"]]
        + [("source_cycle", pa.int32()), ("reported_spender_count", pa.int32()), ("unknown_spender_rows", pa.int32())]),
    SPENDERS: pa.schema([(n, pa.string()) for n in COMMON + ["record_id", "spender_id", "spender_name", "spender_namespace", "total"]]
        + [("source_cycle", pa.int32()), ("reported_transaction_count", pa.int64())]),
}


def _json(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def _pin(value):
    return "sha256:" + hashlib.sha256(_json(value).encode()).hexdigest()


def _money(value):
    if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
        raise ValueError("Outside-spending aggregate requires an exact native amount")
    amount, status = exact_amount(str(value))
    if status != "exact":
        raise ValueError("Outside-spending aggregate amount is missing or refused")
    return amount


def outside_profiles(spenders, totals, *, candidate_id, cycle, evidence, election=None):
    """Require complete matching publisher groups and independently captured totals.

    The source groups all elections in a two-year cycle. A specific election
    request refuses rather than relabeling cycle totals. Source IDs count filers;
    transaction counts are never presented as spender or unique-person counts.
    """
    if election is not None:
        raise ValueError("Publisher cycle aggregate does not qualify an election-specific comparison")
    if type(cycle) is not int or cycle % 2 or not re.fullmatch(r"[HPS][A-Z0-9]{8}", candidate_id):
        raise ValueError("Select an explicit candidate ID and two-year cycle")
    selected = {}
    sums = defaultdict(lambda: Decimal(0))
    groups = defaultdict(list)
    for row in spenders:
        if row.get("candidate_id") != candidate_id or row.get("cycle") != cycle:
            raise ValueError("Spender aggregate differs from the selected candidate/cycle")
        indicator = row.get("support_oppose_indicator")
        if indicator not in {"S", "O"}:
            raise ValueError("Unresolved support/oppose aggregate cannot be folded into a total")
        key = (indicator, row.get("committee_id"))
        if key in selected:
            raise ValueError("Repeated spender aggregate key; select one source representation")
        if type(row.get("count")) is not int or row["count"] < 0:
            raise ValueError("Spender transaction count is missing or invalid")
        amount = _money(row.get("total"))
        selected[key] = row
        with localcontext() as context:
            context.prec = 50
            sums[indicator] += amount
        groups[indicator].append(row)
    expected = {}
    for row in totals:
        indicator = row.get("support_oppose_indicator")
        if row.get("candidate_id") != candidate_id or row.get("cycle") != cycle or indicator not in {"S", "O"}:
            raise ValueError("Official total differs from the selected candidate/cycle/indicator")
        if indicator in expected:
            raise ValueError("Repeated official total; no capture-time precedence")
        expected[indicator] = _money(row.get("total"))
    if set(expected) != set(groups) or any(expected[k] != sums[k] for k in expected):
        raise ValueError("Spender groups do not reconcile exactly to matching official totals")
    output = {PROFILES: [], SPENDERS: []}
    generation = _pin([POLICY, evidence])
    for indicator, rows in sorted(groups.items()):
        names = {r.get("candidate_name") for r in rows}
        if len(names) != 1:
            raise ValueError("Conflicting source candidate labels remain unresolved")
        pid = _pin([generation, candidate_id, cycle, indicator])
        common = dict(profile_id=pid, candidate_id=candidate_id, candidate_name=names.pop(),
            support_oppose=indicator, source_cycle=cycle, source_evidence_json=_json(evidence),
            source_generation=generation, period_start=f"{cycle-1}-01-01", period_end=f"{cycle}-12-31",
            election_basis="all_elections_in_selected_two_year_cycle", policy_version=POLICY,
            amount_basis="publisher_schedule_e_aggregate_reconciled_to_publisher_total",
            coverage_note="Observed publisher aggregate snapshot; memo items and 24/48-hour notices are excluded "
                          "by publisher methodology. No addition of raw transactions or direct contributions. "
                          "This is not a frozen or current all-outside-spending census.")
        known = {r["committee_id"] for r in rows if re.fullmatch(r"C[0-9]{8}", r.get("committee_id") or "")}
        unknown = sum(not re.fullmatch(r"C[0-9]{8}", r.get("committee_id") or "") for r in rows)
        output[PROFILES].append({**common, "total": format(expected[indicator], "f"),
            "reported_spender_count": len(known), "unknown_spender_rows": unknown,
            "reported_event_dates": "not_supplied_by_aggregate; period is requested cycle, not observed min/max dates"})
        for row in rows:
            sid = row.get("committee_id")
            namespace = "fec_form5_filer" if re.fullmatch(r"C9[0-9]{7}", sid or "") else (
                "fec_committee" if re.fullmatch(r"C[0-9]{8}", sid or "") else "unresolved")
            output[SPENDERS].append({**common, "record_id": _pin([pid, sid]), "spender_id": sid, "spender_name": row.get("committee_name"),
                "spender_namespace": namespace, "total": format(_money(row["total"]), "f"),
                "reported_transaction_count": row["count"]})
    return output
