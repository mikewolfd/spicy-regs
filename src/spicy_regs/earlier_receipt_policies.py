"""Explicit earlier field policies, shared by source replay and receipt validation."""

from dataclasses import replace
from importlib.resources import files
import json

# Exact columns promoted from receipts to subjects in government-sources/2.
EARLIER_COLUMNS = {
    "gao_reports": ("major_rule_agency", "major_rule_rins", "major_rule_fr_citations"),
    "gao_decisions": ("b_numbers_truncated",),
    "gao_recommendations": ("first_seen", "last_seen"),
}


def earlier_policies(policy):
    if policy.dataset == "scorecard_member_ratings" and policy.policy_version in {
        "scorecards-etl-ratings-v3",
        "scorecards-etl-ratings-v2",
    }:
        from spicy_regs.etl_receipts import DatasetPolicy

        historical = json.loads(files("spicy_regs.scorecards").joinpath("historical_rating_policies.json").read_text())
        versions = ("scorecards-etl-ratings-v2", "scorecards-etl-v1")
        if set(historical) != set(versions):
            raise ValueError("Historical scorecard policies differ from the declared versions")
        policies = {version: DatasetPolicy.from_descriptor(historical[version]) for version in versions}
        if any(p.dataset != policy.dataset or p.policy_version != version for version, p in policies.items()):
            raise ValueError("Historical scorecard policy identity differs")
        if policy.policy_version == "scorecards-etl-ratings-v3":
            current = json.loads(files("spicy_regs").joinpath("etl_policies/scorecard_member_ratings.json").read_text())
            expected = DatasetPolicy.from_descriptor(current)
            admitted = tuple(policies[version] for version in versions)
        else:
            expected = policies["scorecards-etl-ratings-v2"]
            admitted = (policies["scorecards-etl-v1"],)
        if policy.descriptor() != expected.descriptor():
            raise ValueError("Current scorecard policy differs from the exact declared policy")
        return admitted
    if policy.policy_version != "government-sources/2" or policy.dataset not in EARLIER_COLUMNS:
        return ()
    schema = policy.subject_schema
    for column in EARLIER_COLUMNS[policy.dataset]:
        index = schema.get_field_index(column)
        if index < 0:
            raise ValueError("Current government policy omits a declared promoted column")
        schema = schema.remove(index)
    return (replace(policy, subject_schema=schema, policy_version="government-sources/1"),)
