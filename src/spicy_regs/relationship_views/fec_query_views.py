"""Explicit qualified FEC view specifications, separate from MCP activation.

The factory binds trusted installed interpretation files and a selected source
scope. It creates no release receipt, production image pin or financial rows.
The containing release must retain the referenced immutable source generation
and validate the definition/context witnesses before admitting these views.
"""

from __future__ import annotations

import json
from pathlib import Path
import re
from typing import Mapping

from spicy_regs.fec_financial_rules import IDENTITY_VERSION, POLICY_VERSION as FINANCIAL_POLICY
from spicy_regs.fec_release import QualifiedView
from spicy_regs.fec_versions import INDIVIDUAL_SNAPSHOT_POLICY

from .core import literal
from .fec_filing_associations import (
    FILE_NUMBER_FILER_FIELDS,
    POLICY_VERSION as ASSOCIATION_POLICY,
    financial_header_association_sql,
    financial_number_association_sql,
)
from .fec_financial_meaning import financial_rule_sql
from .fec_typed import (
    FILING_REFERENCE_POLICY,
    filing_record_evidence,
    filing_reference_resolution,
    individual_snapshot_inclusion,
    reported_summary_measures,
    spending_targets,
    typed_record_evidence,
)
from .sql_views import SQLView

VIEW_VERSION = "fec-qualified-query-views/1"
_FINANCIAL_TABLES = (
    "fec_account_transfers",
    "fec_allocated_disbursements",
    "fec_allocation_bases",
    "fec_bundled_contributions",
    "fec_communication_costs",
    "fec_contribution_aggregates",
    "fec_coordinated_party_expenditures",
    "fec_debts",
    "fec_disbursements",
    "fec_electioneering_communications",
    "fec_historical_ie_statistics",
    "fec_inaugural_donations",
    "fec_independent_expenditures",
    "fec_intercommittee_transactions",
    "fec_loan_guarantors",
    "fec_loan_terms",
    "fec_loans",
    "fec_receipts",
    "fec_reported_financial_summaries",
    "fec_filing_report_observations",
)
_EVIDENCE_COLUMNS = ("record_id", "collection_id", "source_record_id", "source_sha256", "source_locator_json")
_DEFINITION_EVIDENCE_COLUMNS = (
    "target_record_id",
    "target_table",
    "witness_generation_scope",
    "witness_generation_pin",
    "role",
    "endpoint_kind",
    "collection_id",
    "source_record_id",
    "context_column",
    "context_pointer",
    "witness_sha256",
    "witness_locator_json",
)
_HEADER_COLUMNS = (
    "collection_id",
    "source_sha256",
    "source_authority",
    "header_record_id",
    "header_locator_json",
    "record_id",
    "policy_version",
    "association_basis",
    "filing_key",
    "referenced_filing_key",
    "association_status",
    "source_generation_pin",
)
_NUMBER_TARGET_COLUMNS = (
    "filing_key",
    "record_id",
    "source_authority",
    "source_namespace",
    "identity_version",
    "mapping_version",
    "report_number",
    "native_filer_id",
    "form_type",
)
_NUMBER_SOURCE_COLUMNS = (
    "record_id",
    "collection_id",
    "source_record_id",
    "source_sha256",
    "source_authority",
    "source_locator_json",
    "selection_evidence_sha256",
    "source_namespace",
    "mapping_version",
    "report_number",
)
_RULE_IDENTITY_COLUMNS = (
    "record_id",
    "identity_version",
    "mapping_version",
    "value_mapping_version",
    "source_authority",
    "source_namespace",
    "mapping_status",
    "definition_set_id",
    "declared_format_version",
    "summary_type",
    "source_representation_role",
    "correction_operation",
    "currency",
)


def _selected_scope(source_generation_pin, population, as_of, namespace_evidence):
    if (
        not isinstance(source_generation_pin, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", source_generation_pin) is None
    ):
        raise ValueError("FEC views require an explicit selected source-generation pin")
    if any(not isinstance(value, str) or not value.strip() for value in (population, as_of)):
        raise ValueError("FEC views require explicit population and as-of declarations")
    proofs = dict(namespace_evidence)
    if any(
        not isinstance(key, str)
        or not key
        or not isinstance(value, str)
        or re.fullmatch(r"sha256:[0-9a-f]{64}", value) is None
        for key, value in proofs.items()
    ):
        raise ValueError("FEC filing namespace witnesses require explicit SHA-256 pins")
    return proofs


def fec_query_views(
    *,
    source_generation_pin: str,
    population: str,
    as_of: str,
    namespace_evidence: Mapping[str, str],
    package_root: Path | None = None,
) -> tuple[QualifiedView, ...]:
    """Build trusted view declarations; leave image/receipt pins and activation unset.

    Namespace witnesses bind retained source definitions for native FILE_NUM
    equivalence. An empty explicit mapping produces unresolved association rules.
    ``package_root`` is a trusted installed-package location, not receipt input.
    Each resulting spec captures its SQL now; later caller mutation cannot
    change scope, columns or witnesses. No source processors are imported.
    """
    proofs = _selected_scope(source_generation_pin, population, as_of, namespace_evidence)
    root = package_root or Path(__file__).resolve().parent.parent
    dictionary = root / "table_metadata.json"
    metadata = json.loads(dictionary.read_text(encoding="utf-8"))
    schemas = {}
    for table, info in metadata.items():
        columns = tuple(item["column_name"] for item in info["columns"])
        if len(columns) != len(set(columns)):
            raise ValueError(f"Repeated installed dictionary column: {table}")
        schemas[table] = columns
    specs = []

    def required(table, columns):
        if table not in schemas or not set(columns) <= set(schemas[table]):
            raise ValueError(f"Installed FEC dictionary lacks required columns: {table}")
        return tuple(columns)

    def add(
        name, requirements, sql, meaning, keys, version, helper, *, definitions=(), identity_version=IDENTITY_VERSION
    ):
        checked = {table: required(table, columns) for table, columns in requirements.items()}
        policies = {VIEW_VERSION: root / "relationship_views/fec_query_views.py", version: root / helper}
        # Evidence interpretation uses the declared source locator/definition
        # semantics in the helper. Financial rules additionally bind finite
        # retained source definitions; these are actual files, not fake table reads.
        defs = {Path(path).stem: root / path for path in (definitions or (helper,))}
        spec = SQLView(name, checked, lambda _publication, sql=sql: sql, meaning, tuple(keys), version)
        specs.append(
            QualifiedView(
                spec,
                version,
                identity_version,
                {
                    "policies": policies,
                    "dictionaries": {"fec-installed-table-metadata": dictionary},
                    "definitions": defs,
                },
                population,
                as_of,
                {"fec-observations": source_generation_pin},
            )
        )

    for table in _FINANCIAL_TABLES:
        columns = schemas.get(table, ())
        if not set(_EVIDENCE_COLUMNS) <= set(columns):
            # Context-derived financial rows already have complete stored
            # evidence, including worksheet header and methodology witnesses.
            add(
                table + "_evidence",
                {"fec_record_evidence": schemas["fec_record_evidence"]},
                "SELECT * FROM fec_record_evidence WHERE target_table = " + literal(table),
                "Stored source-context and definition witnesses for retained financial observations. Endpoint existence and cardinality are verified by release acceptance.",
                (
                    "target_table",
                    "target_record_id",
                    "role",
                    "endpoint_kind",
                    "collection_id",
                    "source_record_id",
                    "context_pointer",
                ),
                "fec-stored-financial-evidence/1",
                "relationship_views/fec_query_views.py",
                definitions=("transforms/fec_research_context.py",),
                identity_version="fec-research-context/1",
            )
            continue
        filing = "filing_header_record_id" in columns
        needs: dict[str, tuple[str, ...]] = {table: _EVIDENCE_COLUMNS}
        if filing:
            needs[table] += ("filing_header_record_id", "filing_header_locator_json", "definition_set_id")
            needs["fec_filing_definition_evidence"] = _DEFINITION_EVIDENCE_COLUMNS
        add(
            table + "_evidence",
            needs,
            filing_record_evidence(table, source_generation_pin)
            if filing
            else typed_record_evidence(table, source_generation_pin),
            "Source-record and definition witnesses for each retained typed observation. Repeated witnesses remain; endpoint existence is verified by release acceptance, not inferred by this route.",
            (
                "target_table",
                "target_record_id",
                "role",
                "endpoint_kind",
                "collection_id",
                "source_record_id",
                "context_pointer",
            ),
            "fec-typed-evidence-routes/1",
            "relationship_views/fec_typed.py",
        )
        if filing:
            add(
                table + "_filing_associations",
                {
                    table: (
                        "record_id",
                        "collection_id",
                        "source_record_id",
                        "source_sha256",
                        "source_authority",
                        "filing_header_record_id",
                        "filing_header_locator_json",
                    ),
                    "fec_filing_header_associations": _HEADER_COLUMNS,
                },
                financial_header_association_sql(table, source_generation_pin),
                "One exact physical-header association decision per observation; missing, conflicting and unsupported header metadata stay unresolved. No amendment replacement or current money is inferred.",
                ("target_table", "target_record_id"),
                ASSOCIATION_POLICY,
                "relationship_views/fec_filing_associations.py",
            )

    for table in (
        "fec_receipts",
        "fec_intercommittee_transactions",
        "fec_disbursements",
        "fec_independent_expenditures",
        "fec_communication_costs",
    ):
        columns = required(table, _NUMBER_SOURCE_COLUMNS)
        columns += tuple(field for field in dict.fromkeys(FILE_NUMBER_FILER_FIELDS.values()) if field in schemas[table])
        add(
            table + "_native_filing_associations",
            {table: columns, "fec_filings": _NUMBER_TARGET_COLUMNS},
            financial_number_association_sql(table, source_generation_pin, proofs, columns=columns),
            "One native-file-number association per retained financial observation. All matching metadata versions remain counted; authority, mapper, filer or target conflicts refuse. Matching file number proves no amendment scope or total.",
            ("target_table", "target_record_id"),
            ASSOCIATION_POLICY,
            "relationship_views/fec_filing_associations.py",
        )

    add(
        "fec_filing_reference_resolution",
        {
            "fec_filing_links": schemas["fec_filing_links"],
            "fec_filings": (
                "record_id",
                "filing_key",
                "source_authority",
                "source_namespace",
                "report_number",
                "native_filer_id",
                "form_type",
            ),
        },
        filing_reference_resolution(),
        "One resolution decision per retained native filing reference; all retained target observations remain visible and amendment direction or replacement membership remains unknown.",
        ("record_id",),
        FILING_REFERENCE_POLICY,
        "relationship_views/fec_typed.py",
    )
    add(
        "fec_individual_snapshot_inclusion",
        {
            "fec_receipts": ("record_id", "collection_id", "source_sha256", "source_namespace"),
            "fec_collection_selection": (
                "collection_id",
                "source_sha256",
                "record_id",
                "policy_version",
                "purpose",
                "selection_status",
                "selection_reason",
                "selected_collection_id",
                "equivalence_evidence_sha256",
                "source_generation_pin",
            ),
        },
        individual_snapshot_inclusion(source_generation_pin),
        "One main/date representation decision per retained individual observation, requiring exact source-generation and collection policy. It qualifies representation choice only; correction applicability and current totals stay unresolved.",
        ("target_table", "target_record_id"),
        INDIVIDUAL_SNAPSHOT_POLICY,
        "relationship_views/fec_typed.py",
        definitions=("fec_versions.py",),
    )
    summary_columns = (
        "record_id",
        "summary_type",
        "mapping_version",
        "measures",
        "source_cycle",
        "period_start",
        "period_end",
        "period_basis",
        "candidate_native_id",
        "candidate_name",
        "committee_native_id",
        "committee_name",
        "entity_reference_status",
        "mapping_status",
        "selection_evidence_sha256",
        "collection_id",
        "source_record_id",
        "source_sha256",
        "source_locator_json",
    )
    add(
        "fec_reported_financial_summary_metrics",
        {"fec_reported_financial_summaries": summary_columns},
        reported_summary_measures(),
        "One literal source-named measure per reported summary. Names, units, raw text and missingness remain distinct; measures are not equated, added or selected as latest.",
        ("summary_record_id", "native_measure_name"),
        "fec-summary-measure/1",
        "relationship_views/fec_typed.py",
        identity_version="fec-summary-measure/1",
    )
    for table in (
        "fec_independent_expenditures",
        "fec_communication_costs",
        "fec_electioneering_communications",
        "fec_coordinated_party_expenditures",
    ):
        columns = (
            "record_id",
            "candidate_id",
            "candidate_name",
            "candidate_office",
            "candidate_state",
            "candidate_district",
            "currency",
            "collection_id",
            "source_record_id",
            "source_sha256",
            "source_locator_json",
            "mapping_version",
            "mapping_status",
            "selection_evidence_sha256",
        )
        if table in ("fec_independent_expenditures", "fec_communication_costs"):
            columns += ("support_oppose_code",)
        if table == "fec_electioneering_communications":
            columns += ("allocated_candidate_amount", "allocated_candidate_amount_status")
        add(
            table + "_targets",
            {table: columns},
            spending_targets(table),
            "Source-reported candidate associations at the spending-observation grain. Joining targets must not multiply event money; a stored publisher share is distinct from a reported allocation.",
            ("record_id",),
            "fec-spending-target/1",
            "relationship_views/fec_typed.py",
            identity_version="fec-spending-target/1",
        )

    def rule(table, suffix, query, *, field=None, fields=("total_amount",)):
        needed = set(_RULE_IDENTITY_COLUMNS)
        if query.startswith("bulk_"):
            needed.update(("amount", "amount_raw", "amount_status", "memo_indicator"))
        elif query == "publisher_calculated_candidate_share":
            needed.update(
                (
                    "allocated_candidate_amount",
                    "allocated_candidate_amount_raw",
                    "allocated_candidate_amount_status",
                    "reported_candidate_count",
                )
            )
        elif query.startswith(("fec_loans_", "fec_debts_")):
            if not isinstance(field, str):
                raise ValueError("A state rule requires its named source field")
            needed.update((field, field + "_raw", field + "_status"))
        elif query == "allocated_payment_measure":
            for name in {*fields, "total_amount"}:
                needed.update((name, name + "_raw", name + "_status"))
        else:
            needed.add("measures")
        columns = tuple(name for name in schemas[table] if name in needed)
        sql = financial_rule_sql(table, query, columns=columns, field=field, fields=fields)
        add(
            table + "_" + suffix,
            {table: columns},
            sql,
            "Per-observation financial policy /2 decision, with explicit refusal, exact source-defined amount, definitions and warnings. Eligibility is limited to the named purpose; current financial totals and group additivity are never qualified.",
            ("source_table", "target_record_id"),
            FINANCIAL_POLICY,
            "relationship_views/fec_financial_meaning.py",
            definitions=("fec_financial_rules.py",),
        )

    for table in ("fec_receipts", "fec_intercommittee_transactions"):
        for purpose in ("source_analysis", "detailed_summary_component", "gross_receipts", "net_receipts"):
            rule(table, purpose + "_decision", "bulk_" + purpose)
    rule(
        "fec_electioneering_communications",
        "publisher_candidate_share_decision",
        "publisher_calculated_candidate_share",
    )
    for table, stock, activity in (
        ("fec_loans", ("original_loan_amount", "payments_to_date", "outstanding_balance"), ()),
        ("fec_debts", ("opening_balance", "closing_balance"), ("incurred_in_period", "paid_in_period")),
    ):
        for purpose, names in (("reported_snapshot", stock), ("period_activity", activity)):
            for field in names:
                rule(table, field + "_decision", table + "_" + purpose, field=field)
    for fields in (("total_amount",), ("federal_share",), ("nonfederal_share",), ("federal_share", "nonfederal_share")):
        rule(
            "fec_allocated_disbursements",
            "_and_".join(fields) + "_decision",
            "allocated_payment_measure",
            fields=fields,
        )
    table = "fec_reported_financial_summaries"
    for field in ("NET_CONTB", "TTL_RECEIPTS", "TTL_DISB"):
        rule(table, field.lower() + "_decision", "reported_summary_measure", field=field)
    rule(table, "committee_net_contributions_decision", "reported_committee_net_contributions")
    for direction in ("receipts", "disbursements"):
        rule(
            table,
            "candidate_transfer_adjusted_" + direction + "_decision",
            "candidate_summary_transfer_adjusted_" + direction,
        )
    if len({spec.view.name for spec in specs}) != len(specs):
        raise ValueError("Repeated qualified FEC view name")
    return tuple(specs)
