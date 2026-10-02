"""SQL/Python parity for per-observation financial meaning; no corpus/PDF work."""

from dataclasses import asdict
from decimal import Decimal
import json
import subprocess
import sys

import duckdb
import pyarrow as pa
import pytest

from spicy_regs.fec_financial_rules import _SUMMARY_MAPPINGS
from spicy_regs.relationship_views.fec_financial_meaning import financial_rule_sql
from spicy_regs.transforms import fec_financial_policy as policy
from tests.test_fec_financial_policy import amount, bulk, row, summary


def evaluate(observations, query, **kwargs):
    table = pa.Table.from_pylist(observations)
    # Arrow cannot infer the type of an all-NULL synthetic column. Source tables
    # carry explicit VARCHAR fields; use the same physical type for these tests.
    table = table.cast(
        pa.schema([pa.field(f.name, pa.string() if pa.types.is_null(f.type) else f.type) for f in table.schema])
    )
    with duckdb.connect(config={"threads": 1, "memory_limit": "128MB", "max_temp_directory_size": "0B"}) as con:
        con.register("observations", table)
        cursor = con.execute(financial_rule_sql("observations", query, columns=table.column_names, **kwargs))
        names = [item[0] for item in cursor.description]
        return [dict(zip(names, values, strict=True)) for values in cursor.fetchall()]


def oracle(observation, query, **kwargs):
    if query.startswith("bulk_"):
        result = policy.bulk_observation_eligibility(observation, purpose=query.removeprefix("bulk_"))
    elif query == "publisher_calculated_candidate_share":
        result = policy.publisher_calculated_candidate_share(observation)
    elif query.startswith(("fec_loans_", "fec_debts_")):
        table = "fec_loans" if query.startswith("fec_loans_") else "fec_debts"
        result = policy.state_measure([observation], table=table, purpose=query.removeprefix(table + "_"), **kwargs)
    elif query == "allocated_payment_measure":
        result = policy.allocated_payment(observation, **kwargs)
    elif query == "reported_summary_measure":
        result = policy.reported_summary_measure(observation, kwargs["field"])
    elif query == "reported_committee_net_contributions":
        result = policy.committee_summary_net_contributions(observation)
    elif query.startswith("candidate_summary_transfer_adjusted_"):
        result = policy.candidate_summary_transfer_adjustment(observation, direction=query.rsplit("_", 1)[1])
    else:
        raise AssertionError(query)
    return json.loads(json.dumps(asdict(result), default=str), parse_float=Decimal)


def assert_parity(observation, query, **kwargs):
    actual = evaluate([observation], query, **kwargs)[0]
    assert actual.pop("source_table") == "observations"
    requested = (
        kwargs.get("fields", ("total_amount",))
        if query == "allocated_payment_measure"
        else ((kwargs["field"],) if "field" in kwargs else ())
    )
    assert actual.pop("requested_fields") == list(requested)
    assert actual.pop("target_record_id") == observation["record_id"]
    actual["definitions"] = json.loads(actual.pop("definitions_json"))
    expected = oracle(observation, query, **kwargs)
    if expected["value"] is not None:
        expected["value"] = Decimal(expected["value"])
    assert actual == expected
    assert not actual["current_financial_total_qualified"]


@pytest.mark.parametrize("purpose", ["source_analysis", "detailed_summary_component", "net_receipts", "gross_receipts"])
@pytest.mark.parametrize("memo", [None, "", "X", "Y"])
def test_memo_and_purpose(purpose, memo):
    assert_parity(bulk(memo_indicator=memo, **amount("amount", "-12.500000001")), "bulk_" + purpose)


@pytest.mark.parametrize(
    "change",
    [
        {"source_authority": "unofficial"},
        {"source_authority": None},
        {"identity_version": "unknown"},
        {"mapping_version": "unknown"},
        {"value_mapping_version": None},
        {"mapping_status": "unmapped"},
        {"source_namespace": "unknown"},
        {"currency": None},
        {"source_representation_role": "deletion", "correction_operation": "delete"},
        {"amount_status": "unread"},
        {"amount": 12.0},
        {"amount": "12.00"},
    ],
)
def test_bulk_refusal_parity(change):
    assert_parity(bulk(**change), "bulk_source_analysis")


def test_union_null_and_missing_memo():
    assert_parity(bulk(amount_raw=None), "bulk_source_analysis")
    observation = bulk()
    del observation["memo_indicator"]
    assert_parity(observation, "bulk_source_analysis")


@pytest.mark.parametrize("raw", [None, "", "99.99", "1e2", " 100", "100.0000000001"])
def test_individual_native_amount_is_required_for_financial_meaning(raw):
    observation = bulk(amount_raw=raw)
    actual = evaluate([observation], "bulk_source_analysis")[0]
    assert actual["status"] == "refused"
    assert actual["reason"] == "amount_or_currency_unqualified"
    assert actual["value"] is None
    assert not actual["current_financial_total_qualified"]
    assert_parity(observation, "bulk_source_analysis")


@pytest.mark.parametrize("version,status", [("fec-bulk-individual-receipt/1", "refused"), ("fec-bulk-individual-receipt/2", "eligible")])
def test_individual_mapper_version_has_no_legacy_financial_fallback(version, status):
    observation = bulk(mapping_version=version)
    actual = evaluate([observation], "bulk_source_analysis")[0]
    assert actual["status"] == status
    assert actual["value"] == (Decimal("100") if status == "eligible" else None)
    assert_parity(observation, "bulk_source_analysis")


@pytest.mark.parametrize(
    "raw", ["12.0000000000", "+12.0", "12.0000000001", "12.00000000001", "12e0", " 12", "12.", "12", "-12", None]
)
@pytest.mark.parametrize("namespace", ["fec-bulk-individual-contributions", "fec-bulk-other-committee-transactions"])
def test_exact_native_amount_without_rounding(raw, namespace):
    assert_parity(
        bulk(source_namespace=namespace, amount=Decimal("12"), amount_raw=raw),
        "bulk_source_analysis",
    )


def test_individual_missing_native_amount_column_refuses():
    observation = bulk()
    del observation["amount_raw"]
    actual = evaluate([observation], "bulk_source_analysis")[0]
    assert actual["reason"] == "amount_or_currency_unqualified"
    assert actual["value"] is None
    assert_parity(observation, "bulk_source_analysis")


@pytest.mark.parametrize("count", ["3", "0001", "0", "", None, "１２", "1e2", "-1"])
def test_candidate_share(count):
    observation = row(
        source_namespace="fec-bulk-electioneering-candidate-disbursement-csv",
        reported_candidate_count=count,
        **amount("allocated_candidate_amount", "33.330000001"),
    )
    assert_parity(observation, "publisher_calculated_candidate_share")


@pytest.mark.parametrize(
    "table,layout,names",
    [
        ("fec_loans", policy._LOAN_LAYOUT, ("original_loan_amount", "payments_to_date", "outstanding_balance")),
        (
            "fec_debts",
            policy._DEBT_LAYOUT,
            ("opening_balance", "incurred_in_period", "paid_in_period", "closing_balance"),
        ),
    ],
)
def test_each_state_field_and_purpose(table, layout, names):
    observation = row(definition_set_id=layout)
    for name in names:
        observation.update(amount(name, "123.123456789"))
    for name in [*names, "unknown"]:
        for purpose in ["reported_snapshot", "period_activity"]:
            assert_parity(observation, table + "_" + purpose, field=name)


@pytest.mark.parametrize(
    "fields",
    [
        ("total_amount",),
        ("federal_share",),
        ("nonfederal_share",),
        ("federal_share", "nonfederal_share"),
        ("total_amount", "federal_share"),
        ("event_amount_year_to_date",),
    ],
)
def test_allocated_choices(fields):
    observation = row(
        definition_set_id=policy._ALLOCATION_LAYOUT,
        **amount("total_amount", "100"),
        **amount("federal_share", "60"),
        **amount("nonfederal_share", "40"),
    )
    assert_parity(observation, "allocated_payment_measure", fields=fields)


def test_allocated_reconciliation_and_overflow():
    for total, federal, nonfederal in [
        ("101", "60", "40"),
        ("90000000000000000000000000000", "90000000000000000000000000000", "90000000000000000000000000000"),
    ]:
        observation = row(
            definition_set_id=policy._ALLOCATION_LAYOUT,
            **amount("total_amount", total),
            **amount("federal_share", federal),
            **amount("nonfederal_share", nonfederal),
        )
        assert_parity(observation, "allocated_payment_measure", fields=("federal_share", "nonfederal_share"))


@pytest.mark.parametrize(
    "change", [{}, {"mapping_version": "unknown"}, {"source_authority": None}, {"currency": "EUR"}]
)
def test_summary_rules(change):
    observation = {**summary(TTL_CONTB="123", TTL_CONTB_REF="-5", NET_CONTB="128"), **change}
    assert_parity(observation, "reported_committee_net_contributions")
    assert_parity(observation, "reported_summary_measure", field="NET_CONTB")
    assert_parity(observation, "reported_summary_measure", field="unknown")


def test_summary_duplicate_missing_inexact_measures():
    for mutation in ["duplicate", "missing", "inexact", "raw_precision"]:
        observation = summary(TTL_CONTB="123", TTL_CONTB_REF="5", NET_CONTB="118")
        if mutation == "duplicate":
            observation["measures"].append(observation["measures"][0])
        if mutation == "missing":
            observation["measures"].pop()
        if mutation == "inexact":
            observation["measures"][0]["value_status"] = "invalid"
        if mutation == "raw_precision":
            observation["measures"][0]["raw_value"] = "123.0000000001"
        assert_parity(observation, "reported_committee_net_contributions")


@pytest.mark.parametrize("transfer_from,transfer_to", [("10", "2"), ("0", "2"), ("-10", "2")])
def test_transfer_rule(transfer_from, transfer_to):
    observation = summary(
        "candidate-web-summary/1",
        TTL_RECEIPTS="100",
        TTL_DISB="80",
        TRANS_FROM_AUTH=transfer_from,
        TRANS_TO_AUTH=transfer_to,
    )
    for direction in ["receipts", "disbursements"]:
        assert_parity(observation, "candidate_summary_transfer_adjusted_" + direction)


def test_finite_summary_fields_equal_current_mappers():
    from spicy_regs.transforms import fec_summaries

    current = {
        value.key: value for value in vars(fec_summaries).values() if isinstance(value, fec_summaries.SummaryMapping)
    }
    assert set(_SUMMARY_MAPPINGS) <= set(current)
    for key, rule in _SUMMARY_MAPPINGS.items():
        assert set(rule.money_fields) == set(current[key].money_fields)


def test_runtime_import_has_no_source_processors_or_arrow():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; from spicy_regs.relationship_views.fec_financial_meaning import financial_rule_sql; assert not any(n == 'pyarrow' or n.startswith(('spicy_docs', 'spicy_regs.transforms')) for n in sys.modules)",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_group_operations_require_separate_scope_evidence():
    for query in [
        "current_membership",
        "spending_record_observation_sum",
        "consolidated_transfer_flow",
        "quality_notice",
    ]:
        with pytest.raises(ValueError, match="group operations"):
            financial_rule_sql("observations", query, columns=["record_id"])


def test_one_decision_per_physical_observation_no_grouping():
    observations = [bulk("A"), bulk("B"), bulk("A")]
    result = evaluate(observations, "bulk_source_analysis")
    assert [r["target_record_id"] for r in result] == [r["record_id"] for r in observations]


@pytest.mark.parametrize("identity", [None, "bad", "sha256:" + "A" * 64])
def test_malformed_record_identity_refuses_the_query(identity):
    observation = bulk(record_id=identity)
    with pytest.raises(ValueError, match="SHA-256"):
        policy.bulk_observation_eligibility(observation)
    with pytest.raises(duckdb.InvalidInputException, match="SHA-256"):
        evaluate([observation], "bulk_source_analysis")


def test_all_summary_allowlists_expose_only_native_money_fields():
    for key, rule in _SUMMARY_MAPPINGS.items():
        name = rule.money_fields[0]
        observation = summary(key, **{name: "123.123456789"})
        assert_parity(observation, "reported_summary_measure", field=name)
        assert_parity(observation, "reported_summary_measure", field="missing")


@pytest.mark.parametrize(
    "value", ["0.0000000001", "123.0000000000", "-0.000000000", "99999999999999999999999999999.999999999"]
)
def test_typed_amount_precision_and_boundary(value):
    assert_parity(bulk(**amount("amount", value)), "bulk_source_analysis")


def test_each_rule_refuses_wrong_producer_identity():
    cases = [
        (
            row(
                source_namespace="fec-bulk-electioneering-candidate-disbursement-csv",
                reported_candidate_count="1",
                **amount("allocated_candidate_amount", "20"),
            ),
            "publisher_calculated_candidate_share",
            {},
        ),
        (
            row(definition_set_id=policy._LOAN_LAYOUT, **amount("outstanding_balance", "20")),
            "fec_loans_reported_snapshot",
            {"field": "outstanding_balance"},
        ),
        (
            row(definition_set_id=policy._DEBT_LAYOUT, **amount("paid_in_period", "20")),
            "fec_debts_period_activity",
            {"field": "paid_in_period"},
        ),
        (
            row(definition_set_id=policy._ALLOCATION_LAYOUT, **amount("total_amount", "20")),
            "allocated_payment_measure",
            {},
        ),
        (
            summary(
                "candidate-web-summary/1", TTL_RECEIPTS="100", TTL_DISB="80", TRANS_FROM_AUTH="10", TRANS_TO_AUTH="2"
            ),
            "candidate_summary_transfer_adjusted_receipts",
            {},
        ),
    ]
    for observation, query, kwargs in cases:
        for key in [
            "mapping_version",
            "identity_version",
            "value_mapping_version",
            "source_authority",
            "source_namespace",
            "mapping_status",
        ]:
            for value in [None, "unknown"]:
                changed = {**observation, key: value}
                assert oracle(changed, query, **kwargs)["status"] == "refused"
                assert_parity(changed, query, **kwargs)


@pytest.mark.parametrize("projection", ["*", "status, value", "sum(value)"])
def test_invalid_evidence_guard_survives_financial_projection(projection):
    observation = bulk(record_id=None)
    table = pa.Table.from_pylist([observation])
    table = table.cast(
        pa.schema([pa.field(f.name, pa.string() if pa.types.is_null(f.type) else f.type) for f in table.schema])
    )
    with duckdb.connect(config={"threads": 1, "memory_limit": "64MB", "max_temp_directory_size": "0B"}) as con:
        con.register("observations", table)
        con.execute(
            "CREATE VIEW decisions AS "
            + financial_rule_sql("observations", "bulk_source_analysis", columns=table.column_names)
        )
        with pytest.raises(duckdb.InvalidInputException, match="SHA-256"):
            con.execute("SELECT " + projection + " FROM decisions").fetchall()


@pytest.mark.parametrize("name", ["_fm_supported", "_fm_snapshot", "_FM_SUPPORTED"])
def test_reserved_declared_source_columns_refuse(name):
    with pytest.raises(ValueError, match="reserved _fm_"):
        financial_rule_sql("observations", "bulk_source_analysis", columns=["record_id", name])


def test_undeclared_source_columns_cannot_override_rule_facts():
    observation = bulk(source_authority="unofficial")
    table = pa.Table.from_pylist([{**observation, "_fm_supported": True, "_fm_snapshot": True}])
    with duckdb.connect(config={"threads": 1, "memory_limit": "64MB", "max_temp_directory_size": "0B"}) as con:
        con.register("observations", table)
        cursor = con.execute(financial_rule_sql("observations", "bulk_source_analysis", columns=list(observation)))
        row = cursor.fetchone()
        assert row is not None
        actual = dict(zip([item[0] for item in cursor.description], row, strict=True))
    assert actual["status"] == "refused"
    assert actual["value"] is None
    assert actual["reason"] == oracle(observation, "bulk_source_analysis")["reason"]
