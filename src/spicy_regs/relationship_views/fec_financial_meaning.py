"""Per-observation financial policy /2 decisions without stored row copies.

Serving admission binds the source tables and retained definition witnesses.
These SQL rules preserve refusals and never qualify a current financial total.
Group membership, sums, transfer pairing and notice joins are separate operations.
"""

from dataclasses import asdict
import json

from spicy_regs.fec_financial_rules import (
    POLICY_VERSION,
    IDENTITY_VERSION,
    VALUE_MAPPING_VERSION,
    ELECTIONEERING_SHARE,
    SUMMARY_NET,
    CANDIDATE_TRANSFERS,
    _BULK_MAPPINGS,
    _FILING_MAPPINGS,
    _SUMMARY_MAPPINGS,
    _MEMO_DEFINITIONS,
    _LOAN_LAYOUT,
    _DEBT_LAYOUT,
    _ALLOCATION_LAYOUT,
)

from .core import literal, quoted

_EMPTY = "[]::VARCHAR[]"
_NULL = "NULL::DECIMAL(38,9)"
_MEASURE_TYPE = (
    "STRUCT(native_field VARCHAR, raw_value VARCHAR, value DECIMAL(38,9), value_status VARCHAR, unit VARCHAR)[]"
)


def _list(values):
    return "[" + ",".join(literal(v) for v in values) + "]::VARCHAR[]" if values else _EMPTY


def _definitions(values):
    return literal(json.dumps([asdict(value) for value in values], sort_keys=True, separators=(",", ":")))


def _exact(value, status, raw=None):
    # TRY_CAST alone would round fractional digits. Require the Python spelling
    # and reject every nonzero digit past scale nine before interpreting bytes.
    typed = f"TRY_CAST({value} AS DECIMAL(38,9))"
    valid = (
        f"{status} = 'exact' AND regexp_full_match(typeof({value}), 'DECIMAL\\([0-9]+,[0-9]+\\)') AND {typed} = {value}"
    )
    if raw is not None:
        valid += f""" AND regexp_full_match({raw}, '[+-]?([0-9]+(\\.[0-9]+)?|\\.[0-9]+)')
            AND NOT regexp_matches({raw}, '\\.[0-9]{{9}}[0-9]*[1-9]')
            AND TRY_CAST({raw} AS DECIMAL(38,9)) = {typed}"""
    return f"CASE WHEN {valid} THEN {typed} END"


def financial_rule_sql(table, query, *, columns, field=None, fields=("total_amount",)):
    """One decision for each row, matching the supported Python policy rule.

    ``columns`` is the admitted table's column names. State queries evaluate each
    reported state independently; they do not select the latest or period end.
    Summary queries require the maintained typed measures array. The caller must
    retain definition admission and the containing generation's source pins.
    """
    names = set(columns)
    if any(name.lower().startswith("_fm_") for name in names):
        raise ValueError("Financial source columns cannot use reserved _fm_ names")
    if "record_id" not in names:
        raise ValueError("Financial decisions require typed observation identity")

    def col(name, default="NULL::VARCHAR"):
        return quoted(name) if name in names else default

    def supported_identity(mapping, namespace):
        return f"{col('mapping_version')} = {literal(mapping)} AND {col('source_namespace')} = {literal(namespace)}"

    identities = [supported_identity(m, ns) for ns, m in _BULK_MAPPINGS.items()]
    identities += [
        f"({supported_identity(m, 'fec-electronic-filing-schedule')} AND {col('definition_set_id')} = {literal(layout)} AND {col('declared_format_version')} = '8.5')"
        for layout, m in _FILING_MAPPINGS.items()
    ]
    identities += [
        f"({supported_identity('fec-' + key, 'fec-bulk-' + key.split('/')[0])} AND {col('summary_type')} = {literal(key)})"
        for key in _SUMMARY_MAPPINGS
    ]
    supported = f"""COALESCE({col("source_authority")} = 'official-fec'
        AND {col("identity_version")} = {literal(IDENTITY_VERSION)}
        AND {col("value_mapping_version")} = {literal(VALUE_MAPPING_VERSION)}
        AND {col("mapping_status")} IN ('mapped','partial') AND ({" OR ".join("(" + v + ")" for v in identities)}), FALSE)"""
    snapshot = (
        f"COALESCE({col('source_representation_role')} = 'snapshot' AND {col('correction_operation')} = 'none', FALSE)"
    )
    projections = [f"{supported} AS _fm_supported", f"{snapshot} AS _fm_snapshot"]
    branches = []

    def decision(status, reason, value=_NULL, basis=None, definitions="'[]'", layouts=_EMPTY, warnings=_EMPTY):
        return f"""struct_pack(query := {literal(query)}, status := {literal(status)}, reason := {literal(reason)},
            record_ids := [record_id], value := {value}, measure_basis := {literal(basis) if basis is not None else "NULL::VARCHAR"},
            definitions_json := {definitions}, layout_pins := {layouts}, warnings := {warnings},
            policy_version := {literal(POLICY_VERSION)}, current_financial_total_qualified := FALSE)"""

    def refuse(condition, reason, **kwargs):
        branches.append((condition, decision("refused", reason, **kwargs)))

    def amount(name, *, individual=False):
        index = len(projections)
        val = col(name, _NULL)
        if individual:
            expression = f"CASE WHEN {col('source_namespace')} = 'fec-bulk-individual-contributions' THEN {_exact(val, col(name + '_status'))} ELSE {_exact(val, col(name + '_status'), col(name + '_raw'))} END"
        else:
            expression = _exact(val, col(name + "_status"), col(name + "_raw"))
        alias = f"_fm_amount_{index}"
        projections.append(f"{expression} AS {alias}")
        return alias

    def summary_values(requested):
        exact_identity = (
            " OR ".join(
                f"({supported_identity('fec-' + key, 'fec-bulk-' + key.split('/')[0])} AND {col('summary_type')} = {literal(key)})"
                for key, rule in _SUMMARY_MAPPINGS.items()
                if all(name in rule.money_fields for name in requested)
            )
            or "FALSE"
        )
        valid = f"_fm_supported AND _fm_snapshot AND {col('currency')} = 'USD' AND ({exact_identity})"
        values = []
        for name in requested:
            selected = f"list_filter({col('measures', '[]::' + _MEASURE_TYPE)}, m -> m.native_field = {literal(name)})"
            entry = f"list_extract({selected},1)"
            expression = _exact(f"({entry}).value", f"({entry}).value_status", f"({entry}).raw_value")
            alias = f"_fm_amount_{len(projections)}"
            projections.append(
                f"CASE WHEN length({selected}) = 1 AND ({entry}).unit = 'USD' THEN {expression} END AS {alias}"
            )
            values.append(alias)
        return values, valid + " AND " + " AND ".join(value + " IS NOT NULL" for value in values)

    if query.startswith("bulk_"):
        purpose = query.removeprefix("bulk_")
        if purpose not in {"source_analysis", "detailed_summary_component", "gross_receipts", "net_receipts"}:
            raise ValueError("Unsupported per-observation bulk purpose")
        refuse("NOT _fm_supported", "input_mapping_or_authority_unqualified")
        definition = (
            "CASE "
            + " ".join(
                f"WHEN {col('source_namespace')} = {literal(ns)} THEN {_definitions((ref,))}"
                for ns, ref in _MEMO_DEFINITIONS.items()
            )
            + " ELSE '[]' END"
        )
        family = "(" + ",".join(map(literal, _MEMO_DEFINITIONS)) + ")"
        refuse(f"{col('source_namespace')} NOT IN {family}", "source_family_memo_rule_unavailable")
        refuse("NOT _fm_snapshot", "correction_payload_is_not_an_additional_receipt", definitions=definition)
        if purpose in {"gross_receipts", "net_receipts"}:
            final = decision(
                "refused", "refund_return_attribution_and_direction_rules_unqualified", definitions=definition
            )
        else:
            value = amount("amount", individual=True)
            refuse(
                f"{value} IS NULL OR {col('currency')} IS DISTINCT FROM 'USD'",
                "amount_or_currency_unqualified",
                definitions=definition,
            )
            refuse(
                "TRUE" if "memo_indicator" not in names else "FALSE",
                "memo_indicator_not_projected",
                definitions=definition,
            )
            memo = col("memo_indicator")
            refuse(
                f"{memo} IS NOT NULL AND {memo} NOT IN ('','X')",
                "memo_code_meaning_unqualified",
                definitions=definition,
            )
            if purpose == "detailed_summary_component":
                branches.append(
                    (
                        f"{memo} = 'X'",
                        decision("excluded", "publisher_memo_not_in_detailed_summary", definitions=definition),
                    )
                )
            warnings = (
                _list(("source_observation_not_current_net_money",))
                + f" || CASE WHEN {memo} = 'X' THEN {_list(('memo_included_for_source_analysis_attribution_or_prior_reporting_unresolved',))} ELSE {_EMPTY} END || CASE WHEN {value} < 0 THEN {_list(('negative_sign_preserved_refund_not_inferred',))} ELSE {_EMPTY} END"
            )
            final = decision(
                "eligible",
                "source_defined_purpose",
                value,
                "signed_reported_observation",
                definition,
                warnings=warnings,
            )
    elif query == "publisher_calculated_candidate_share":
        definition = _definitions((ELECTIONEERING_SHARE,))
        refuse("NOT _fm_supported", "input_mapping_or_authority_unqualified")
        refuse(
            f"{col('source_namespace')} IS DISTINCT FROM 'fec-bulk-electioneering-candidate-disbursement-csv' OR NOT _fm_snapshot",
            "source_grain_not_supported",
            definitions=definition,
        )
        value = amount("allocated_candidate_amount")
        count = col("reported_candidate_count")
        refuse(
            f"{col('currency')} IS DISTINCT FROM 'USD' OR {value} IS NULL OR NOT COALESCE(regexp_full_match({count}, '[0-9]*[1-9][0-9]*'), FALSE)",
            "calculated_share_or_candidate_count_not_exact",
            definitions=definition,
        )
        final = decision(
            "eligible",
            "publisher_equal_share_value_preserved",
            value,
            "publisher_calculated_equal_candidate_share",
            definition,
            warnings=_list(
                (
                    "not_filer_reported_allocation",
                    "event_and_amendment_equivalence_unresolved",
                    "publisher_rounding_not_recomputed",
                )
            ),
        )
    elif query in {
        "fec_loans_reported_snapshot",
        "fec_loans_period_activity",
        "fec_debts_reported_snapshot",
        "fec_debts_period_activity",
    }:
        loan = query.startswith("fec_loans_")
        layout = _LOAN_LAYOUT if loan else _DEBT_LAYOUT
        stock = (
            {"original_loan_amount", "payments_to_date", "outstanding_balance"}
            if loan
            else {"opening_balance", "closing_balance"}
        )
        activity = set() if loan else {"incurred_in_period", "paid_in_period"}
        layouts = _list((layout,))
        refuse("NOT _fm_supported", "input_mapping_or_authority_unqualified")
        refuse(
            f"{col('source_namespace')} IS DISTINCT FROM 'fec-electronic-filing-schedule' OR {col('definition_set_id')} IS DISTINCT FROM {literal(layout)} OR NOT _fm_snapshot OR {col('currency')} IS DISTINCT FROM 'USD'",
            "state_definition_or_source_role_unqualified",
            layouts=layouts,
        )
        if field not in (stock if query.endswith("reported_snapshot") else activity):
            final = decision("refused", "stock_cumulative_and_period_activity_are_distinct", layouts=layouts)
        else:
            value = amount(field)
            refuse(f"{value} IS NULL", "state_measure_not_exact", layouts=layouts)
            final = decision(
                "eligible",
                "single_reported_state_measure",
                value,
                field,
                layouts=layouts,
                warnings=_list(("latest_or_period_end_selection_not_inferred",)),
            )
    elif query == "allocated_payment_measure":
        refuse("NOT _fm_supported", "input_mapping_or_authority_unqualified")
        if tuple(fields) not in {
            ("total_amount",),
            ("federal_share",),
            ("nonfederal_share",),
            ("federal_share", "nonfederal_share"),
        }:
            final = decision("refused", "total_components_and_ytd_cannot_be_added")
        else:
            layouts = _list((_ALLOCATION_LAYOUT,))
            refuse(
                f"{col('source_namespace')} IS DISTINCT FROM 'fec-electronic-filing-schedule' OR {col('definition_set_id')} IS DISTINCT FROM {literal(_ALLOCATION_LAYOUT)} OR NOT _fm_snapshot OR {col('currency')} IS DISTINCT FROM 'USD'",
                "allocation_definition_or_source_role_unqualified",
                layouts=layouts,
            )
            values = [amount(name) for name in fields]
            refuse(" OR ".join(value + " IS NULL" for value in values), "allocation_value_not_exact", layouts=layouts)
            value = "TRY(" + " + ".join(values) + ")"
            refuse(f"{value} IS NULL", "aggregate_decimal_overflow")
            if len(fields) == 2:
                total = amount("total_amount")
                refuse(
                    f"{value} IS DISTINCT FROM {total}",
                    "reported_components_do_not_reconcile_to_total",
                    layouts=layouts,
                )
            final = decision(
                "eligible",
                "one_nonoverlapping_measure",
                value,
                "+".join(fields),
                layouts=layouts,
                warnings=_list(("reported_row_only",)),
            )
    elif query == "reported_summary_measure":
        if not isinstance(field, str):
            raise ValueError("A reported summary rule requires a named native field")
        values, valid = summary_values((field,))
        refuse(f"NOT COALESCE(({valid}), FALSE)", "summary_measure_missing_ambiguous_or_inexact")
        final = decision(
            "eligible",
            "single_source_reported_measure",
            values[0],
            field,
            warnings=_list(
                ("do_not_add_to_its_components_or_itemized_rows", "period_and_population_remain_as_reported")
            ),
        )
    elif query == "reported_committee_net_contributions":
        definition = _definitions((SUMMARY_NET,))
        refuse(
            f"{col('summary_type')} IS DISTINCT FROM 'committee-summary-csv/1'",
            "summary_layout_not_supported",
            definitions=definition,
        )
        values, valid = summary_values(("TTL_CONTB", "TTL_CONTB_REF", "NET_CONTB"))
        refuse(
            f"NOT COALESCE(({valid}), FALSE)", "summary_component_missing_ambiguous_or_inexact", definitions=definition
        )
        value = f"TRY({values[0]} - {values[1]})"
        refuse(
            f"{value} IS NULL OR {value} <> {values[2]}",
            "reported_net_differs_from_defined_components",
            definitions=definition,
        )
        final = decision(
            "eligible",
            "source_reported_net_equation_reconciled",
            value,
            "TTL_CONTB-TTL_CONTB_REF=NET_CONTB",
            definition,
            warnings=_list(("signs_preserved_without_absolute_value", "not_a_net_total_rebuilt_from_itemizations")),
        )
    elif query in {"candidate_summary_transfer_adjusted_receipts", "candidate_summary_transfer_adjusted_disbursements"}:
        definition = _definitions((CANDIDATE_TRANSFERS,))
        refuse(
            f"{col('summary_type')} IS DISTINCT FROM 'candidate-web-summary/1'",
            "candidate_summary_layout_not_supported",
            definitions=definition,
        )
        values, valid = summary_values(("TTL_RECEIPTS", "TTL_DISB", "TRANS_FROM_AUTH", "TRANS_TO_AUTH"))
        refuse(
            f"NOT COALESCE(({valid}), FALSE) OR {values[2]} = 0 OR {values[3]} = 0",
            "publisher_both_transfer_fields_condition_not_met",
            definitions=definition,
        )
        first, second = (0, 2) if query.endswith("receipts") else (1, 3)
        value = f"TRY({values[first]} - {values[second]})"
        refuse(f"{value} IS NULL", "aggregate_decimal_overflow")
        final = decision(
            "eligible",
            "publisher_candidate_summary_transfer_rule",
            value,
            "reported_total_minus_authorized_committee_transfer",
            definition,
            warnings=_list(("does_not_pair_or_deduplicate_transaction_rows",)),
        )
    else:
        raise ValueError("Unsupported per-observation financial rule; group operations require separate scope evidence")
    expression = (
        "CASE " + " ".join(f"WHEN {condition} THEN {result}" for condition, result in branches) + f" ELSE {final} END"
    )
    requested = tuple(fields) if query == "allocated_payment_measure" else ((field,) if field is not None else ())
    # Malformed observation IDs are invalid evidence in the Python owner too.
    # Refuse the query rather than return an apparently qualified anonymous row.
    checked_id = "CASE WHEN regexp_full_match(record_id, 'sha256:[0-9a-f]{64}') THEN record_id ELSE error('FEC evidence requires a SHA-256 pin') END"
    # Guard the decision itself: DuckDB can prune the target-ID projection
    # when callers select only status/value or aggregate a financial field.
    # count(*) alone interprets no money and may legitimately prune both guards.
    checked_decision = f"CASE WHEN regexp_full_match(record_id, 'sha256:[0-9a-f]{{64}}') THEN {expression} ELSE error('FEC evidence requires a SHA-256 pin') END"
    return f"""WITH facts AS (SELECT {", ".join(quoted(name) for name in sorted(names))}, {", ".join(projections)} FROM {quoted(table)}),
        decisions AS (SELECT {checked_id} AS target_record_id, {checked_decision} AS decision FROM facts)
        SELECT {literal(table)} AS source_table, {_list(requested)} AS requested_fields, target_record_id, decision.* FROM decisions"""
