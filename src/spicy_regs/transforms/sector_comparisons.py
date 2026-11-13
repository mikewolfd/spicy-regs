"""Compare qualified source measures without joining money through labels or issues."""

from collections import defaultdict
from datetime import date
from decimal import Decimal, localcontext
import hashlib

import pyarrow as pa

from spicy_regs.transforms.build_fec_financial_profiles import json_bytes
from spicy_regs.transforms.fec_query import exact_amount

POLICY = "fec-lda-sector-comparison/1"
COMPARISONS = "sector_financial_comparisons"
MAPPINGS = "sector_classification_evidence"
ISSUES = "lobbying_issue_financial_observations"
SCHEMAS = {
    COMPARISONS: pa.schema([(n, pa.string()) for n in (
        "record_id sector_code sector_label classification_scheme mapping_version source_family measure "
        "known_reported_amount amount_status amount_basis period_start period_end source_generation "
        "source_table_sha256 cutoff population_json evidence_json coverage_note policy_version"
    ).split()] + [(n, pa.int32()) for n in (
        "population_profiles group_profiles qualified_measure_profiles unresolved_measure_profiles "
        "classified_population_profiles unclassified_population_profiles uncertain_population_profiles"
    ).split()]),
    MAPPINGS: pa.schema([(n, pa.string()) for n in (
        "record_id source_namespace native_entity_id sector_codes_json classification_status classification_scheme "
        "mapping_version method applicable_start applicable_end observed_at evidence_json uncertainty association_json"
    ).split()]),
    ISSUES: pa.schema([(n, pa.string()) for n in (
        "record_id issue_code profile_id filing_uuid income expenses source_url period_start period_end "
        "source_generation amount_basis overlap_note"
    ).split()]),
}


def _pin(value):
    return "sha256:" + hashlib.sha256(json_bytes(value)).hexdigest()


def _date(value):
    return value.isoformat() if isinstance(value, date) else str(value)[:10]


def _amount(value):
    if not isinstance(value, str):
        raise ValueError("Sector comparison requires exact source decimal text")
    result, status = exact_amount(value)
    if status != "exact":
        raise ValueError("Sector source amount is missing or refused")
    return result


def classifications(mapping, *, start, end):
    """One source-local classification per entity; multiple labels stay uncertain.

    NAICS definitions name industries; they do not classify organizations. Each
    assignment needs separate source business evidence. An uncertain proposed
    identity association is stored separately and never joins the financial rows.
    """
    if not mapping.get("version") or mapping.get("scheme") != "NAICS-2022":
        raise ValueError("Select an explicit mapping revision and existing NAICS vintage")
    by_entity = defaultdict(list)
    evidence = []
    for entry in mapping["assignments"]:
        key = (entry["source_namespace"], entry["native_entity_id"])
        if entry["method"] in {"name_only", "issue_code", "code_definition_only"} or not entry.get("evidence"):
            raise ValueError("Organization industry needs business evidence, not a label or issue definition")
        if entry["applicable_start"] > start or entry["applicable_end"] < end:
            raise ValueError("Sector classification is outside its applicable reporting period")
        codes = sorted(set(entry["sector_codes"]))
        if not codes or any(code not in mapping["sectors"] for code in codes):
            raise ValueError("Sector assignment lacks an existing selected code definition")
        by_entity[key].append(entry)
        evidence.append(dict(record_id=_pin([mapping["version"], entry]), source_namespace=key[0],
            native_entity_id=key[1], sector_codes_json=json_bytes(codes).decode(),
            classification_status=entry["status"], classification_scheme=mapping["scheme"],
            mapping_version=mapping["version"], method=entry["method"], applicable_start=entry["applicable_start"],
            applicable_end=entry["applicable_end"], observed_at=entry["observed_at"],
            evidence_json=json_bytes(entry["evidence"]).decode(), uncertainty=entry["uncertainty"], association_json=None))
    selected = {}
    for key, entries in by_entity.items():
        codes = {code for entry in entries for code in entry["sector_codes"]}
        selected[key] = next(iter(codes)) if len(codes) == 1 and all(e["status"] == "supported_context" for e in entries) else "uncertain"
    for association in mapping.get("uncertain_associations", []):
        if association.get("status") != "unresolved":
            raise ValueError("Identity associations require their own qualified bridge before use")
        evidence.append(dict(record_id=_pin([mapping["version"], association]), source_namespace="cross_source_association",
            native_entity_id=None, sector_codes_json="[]", classification_status="unresolved_identity",
            classification_scheme=mapping["scheme"], mapping_version=mapping["version"], method="not_used_for_money",
            applicable_start=start, applicable_end=end, observed_at=mapping["observed_at"], evidence_json="[]",
            uncertainty=association["reason"], association_json=json_bytes(association).decode()))
    # Exact repeated curation entries are equivalent witnesses, not additional
    # public identities. Their original multiplicity remains in the pinned map.
    return selected, list({row["record_id"]: row for row in evidence}.values())


def compare(fec, lobbying, *, mapping, scope):
    """Compare explicitly selected 2025 observations, keeping every measure separate.

    Financial populations need not contain the same organizations. FEC rows
    must cover the exact requested window. Each LDA quarter must lie within it;
    unresolved or absent quarters remain coverage gaps, not estimated zeros.
    """
    start, end = scope["period_start"], scope["period_end"]
    if start != "2025-01-01" or end != "2025-12-31":
        raise ValueError("Only the declared aligned 2025 comparison window is qualified")
    assigned, evidence = classifications(mapping, start=start, end=end)
    for rows in (fec, lobbying):
        ids = [r["profile_id"] for r in rows]
        if len(ids) != len(set(ids)):
            raise ValueError("Repeated source profiles would duplicate money")
    if not fec or not lobbying:
        raise ValueError("Both declared source populations are required")
    if any(r.get("entity_kind") != "committee" or _date(r["period_start"]) != start or
           _date(r["period_end"]) != end for r in fec):
        raise ValueError("Unsupported FEC population or reporting-period comparison")
    if any(not start <= _date(r["period_start"]) <= _date(r["period_end"]) <= end for r in lobbying):
        raise ValueError("Unsupported lobbying reporting-period comparison")
    output = []
    sectors = {**mapping["sectors"], "unclassified": "Unclassified", "uncertain": "Uncertain classification"}
    for source, rows, measures, namespace, identity in (
        ("fec", fec, ("receipts", "disbursements"), "fec_committee", "entity_id"),
        ("lda", lobbying, ("income", "expenses"), "lda_client", "client_id"),
    ):
        # LDA client identifiers are scoped to their registrant. Neither a
        # matching client number nor a repeated name creates an organization join.
        codes = {r["profile_id"]: assigned.get((namespace, r[identity] if source == "fec" else
            r["registrant_id"] + "/" + r["client_id"]), "unclassified") for r in rows}
        classified = sum(code not in {"uncertain", "unclassified"} for code in codes.values())
        unknown = sum(code == "unclassified" for code in codes.values())
        uncertain = sum(code == "uncertain" for code in codes.values())
        for code, label in sectors.items():
            group = [r for r in rows if codes[r["profile_id"]] == code]
            for measure in measures:
                values = []
                for row in group:
                    status = "exact" if source == "fec" else "reported_estimate"
                    if (row.get("selection_status") == "selected" and row.get(measure + "_status") == status
                            and (source != "fec" or row.get("coverage_status") == "reported")):
                        values.append(_amount(row[measure]))
                with localcontext() as context:
                    context.prec = 50
                    amount = format(sum(values, Decimal(0)), "f") if values else None
                output.append(dict(record_id=_pin([POLICY, mapping["version"], scope, source, code, measure]),
                    sector_code=code, sector_label=label, classification_scheme=mapping["scheme"],
                    mapping_version=mapping["version"], source_family=source, measure=measure,
                    known_reported_amount=amount, amount_status="known_selected_observations" if values else "no_qualified_measure",
                    amount_basis="publisher_reported_period_flow" if source == "fec" else "selected_quarterly_reported_estimates",
                    period_start=start, period_end=end, source_generation=scope[source]["generation"],
                    source_table_sha256=scope[source]["table_sha256"], cutoff=scope[source]["cutoff"],
                    population_json=json_bytes(scope[source]["population"]).decode(), evidence_json=json_bytes(
                        [entry for entry in mapping["assignments"] if entry["source_namespace"] == namespace and code in entry["sector_codes"]]).decode(),
                    population_profiles=len(rows), group_profiles=len(group), qualified_measure_profiles=len(values),
                    unresolved_measure_profiles=len(group)-len(values), classified_population_profiles=classified,
                    unclassified_population_profiles=unknown, uncertain_population_profiles=uncertain,
                    policy_version=POLICY, coverage_note="Curated broad industry context as observed at mapping time, not publisher-assigned "
                        "NAICS or historical corporate ownership. FEC and LDA populations differ. LDA missing/competing quarters remain unresolved; "
                        "known amounts are not complete annual totals. Income, expenses, receipts and disbursements must never be added together. "
                        "No name-based cross-source identity join or causal inference."))
    return {COMPARISONS: output, MAPPINGS: evidence}


def issue_views(profiles, activities, *, source_generation):
    """Each selected profile appears once per native issue, never per activity.

    Whole-filing reported money is not allocated by issue. Filtered issue sets
    overlap and cannot be added together. Industry assignments are not consulted.
    """
    by_filing = defaultdict(set)
    for activity in activities:
        if activity.get("general_issue_code"):
            by_filing[activity["filing_uuid"]].add(activity["general_issue_code"])
    result = []
    for row in profiles:
        if row.get("selection_status") != "selected":
            continue
        for code in sorted(by_filing[row["selected_filing_uuid"]]):
            result.append(dict(record_id=_pin([source_generation, row["profile_id"], code]), issue_code=code,
                profile_id=row["profile_id"], filing_uuid=row["selected_filing_uuid"],
                income=row["income"] if row["income_status"] == "reported_estimate" else None,
                expenses=row["expenses"] if row["expenses_status"] == "reported_estimate" else None,
                source_url=row["selected_source_url"], period_start=_date(row["period_start"]),
                period_end=_date(row["period_end"]), source_generation=source_generation,
                amount_basis="whole_selected_filing_not_allocated_by_issue",
                overlap_note="Issue-filtered views overlap. Do not add across issues; repeated activities do not multiply money."))
    return result
