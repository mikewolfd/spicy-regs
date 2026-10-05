"""Explicit earlier field policies, shared by source replay and receipt validation."""
from dataclasses import replace

# Exact columns promoted from receipts to subjects in government-sources/2.
EARLIER_COLUMNS = {
    "gao_decisions": ("b_numbers_truncated",),
    "gao_recommendations": ("first_seen", "last_seen"),
}


def earlier_policies(policy):
    if policy.policy_version != "government-sources/2" or policy.dataset not in EARLIER_COLUMNS:
        return ()
    schema = policy.subject_schema
    for column in EARLIER_COLUMNS[policy.dataset]:
        index = schema.get_field_index(column)
        if index < 0:
            raise ValueError("Current government policy omits a declared promoted column")
        schema = schema.remove(index)
    return (replace(policy, subject_schema=schema, policy_version="government-sources/1"),)
