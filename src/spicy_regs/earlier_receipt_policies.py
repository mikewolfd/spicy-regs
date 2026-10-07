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
    from spicy_regs.etl_receipts import DatasetPolicy
    if policy.policy_version == "legislative-documents/2" and policy.dataset in {
        "native_legal_references", "native_legal_reference_reads",
    }:
        history = json.loads(files("spicy_regs").joinpath("native_legal_policy_history.json").read_text())
        earlier = DatasetPolicy.from_descriptor(history[policy.dataset])
        if earlier.dataset != policy.dataset or earlier.policy_version != "legislative-documents/1":
            raise ValueError("Native legal history differs from its declared policy")
        if not earlier.receipt_only and earlier.identity_fields != policy.identity_fields:
            raise ValueError("Native legal migration changes a historical observation identity")
        return (earlier,)

    if policy.policy_version in {"fec-subject-receipts/2", "fec-identity-context-receipts/2"}:
        fec_history = json.loads(files("spicy_regs").joinpath("fec_policy_history.json").read_text())
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/" + policy.dataset + ".json").read_text()))
        if policy.descriptor() != expected.descriptor() or policy.dataset not in fec_history:
            # Fixture or partial writer schemas do not acquire historical policy
            # authority merely by sharing the current version label.
            return ()
        return (DatasetPolicy.from_descriptor(fec_history[policy.dataset]),)
    if policy.dataset == "fcc_filings" and policy.policy_version == "government-sources/3":
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/fcc_filings.json").read_text()))
        if policy.descriptor() != expected.descriptor():
            raise ValueError("Current FCC policy differs from the exact declared policy")
        history = json.loads(files("spicy_regs").joinpath("navigation_policy_history.json").read_text())
        return tuple(DatasetPolicy.from_descriptor(history[key]) for key in ("fcc_filings_v2", "fcc_filings"))
    context_history = json.loads(files("spicy_regs").joinpath("source_context_policy_history.json").read_text())
    if policy.dataset in context_history and policy.policy_version in {
        "regulations-native-v2", "courts/2", "scorecards-etl-v2", "scorecards-etl-ratings-v4", "government-sources/3",
    }:
        expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
            "etl_policies/" + policy.dataset + ".json").read_text()))
        if policy.descriptor() != expected.descriptor():
            raise ValueError("Current source-context policy differs from the exact declared policy")
        prior = DatasetPolicy.from_descriptor(context_history[policy.dataset])
        if prior.dataset != policy.dataset or (not prior.receipt_only and prior.identity_fields != policy.identity_fields):
            raise ValueError("Source-context migration changes a historical business identity")
        return (prior, *earlier_policies(prior))
    history = json.loads(files("spicy_regs").joinpath("navigation_policy_history.json").read_text())
    if policy.dataset in history and policy.policy_version in {"congress-subjects/2", "government-sources/2", "navigation-read-outcomes/2"}:
        if policy.policy_version == "navigation-read-outcomes/2":
            expected = DatasetPolicy.from_descriptor(json.loads(files("spicy_regs").joinpath(
                "etl_policies/" + policy.dataset + ".json").read_text()))
            if policy.descriptor() != expected.descriptor():
                raise ValueError("Current House outcome policy differs from the exact declared policy")
        earlier = DatasetPolicy.from_descriptor(history[policy.dataset])
        if earlier.identity_fields != policy.identity_fields or earlier.dataset != policy.dataset:
            raise ValueError("Navigation migration changes a historical identity")
        if policy.policy_version == "navigation-read-outcomes/2" and earlier.policy_version != "navigation-read-outcomes/1":
            raise ValueError("House outcome history differs from its declared policy")
        return (earlier,)

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
            current = context_history[policy.dataset]
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
