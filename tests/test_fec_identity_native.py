from datetime import date
from spicy_regs.transforms.fec_identity_native import native_list, native_scalar


def test_mixed_candidate_values_keep_invalid_strings_repeats_and_positions():
    values, diagnostics = native_list(
        '["H8CA12060","N/A",null,"C00694323","H8CA12060",27]', "string", candidate_ids=True
    )
    assert values == ["H8CA12060", "N/A", None, "C00694323", "H8CA12060", None]
    assert [d["ordinal"] for d in diagnostics] == [1, 3, 5]
    assert diagnostics[-1]["value"] == 27


def test_integer_array_does_not_accept_bool_coerce_text_drop_repeats_or_shift_nulls():
    values, diagnostics = native_list('[2024,true,"2026",null,2024,2147483648]', "integer")
    assert values == [2024, None, None, None, 2024, None]
    assert [d["ordinal"] for d in diagnostics] == [1, 2, 5]


def test_missing_null_empty_and_refused_are_distinct_in_input_and_diagnostics():
    assert native_list(None, "string") == (None, [])
    assert native_list("null", "string") == (None, [])
    assert native_list("[]", "string") == ([], [])
    assert native_list("{}", "string") == (None, [{"reason": "expected_array"}])
    assert native_list("", "string") == (None, [{"reason": "invalid_json"}])


def test_dates_links_and_exact_filing_references():
    assert native_list('["2025-02-25","2025-02-27"]', "date")[0] == [date(2025, 2, 25), date(2025, 2, 27)]
    assert native_list('["2025-02-30",null]', "date")[0] == [None, None]
    assert native_list(
        '[{"url":"https://www.fec.gov/a","href":"/a","body_status":"deferred_pdf","source_fact_index":12}]',
        "meeting_link",
    )[0] == [{"url": "https://www.fec.gov/a", "href": "/a"}]
    assert native_list("[-3,12,null,12]", "filing_number")[0] == ["-3", "12", None, "12"]


def test_scalar_never_guesses_century_or_timezone():
    assert native_scalar("01-APR-09", "date") == (None, "unsupported_date")
    assert native_scalar("2026-01-03T12:01:00", "timestamp") == (None, "unsupported_timestamp")
    assert native_scalar(True, "integer") == (None, "unsupported_integer")
    assert native_scalar("2026", "integer") == (2026, None)


def test_all_owned_fields_have_an_explicit_destination_and_native_schemas():
    from spicy_regs.transforms.fec_identity_context_fields import REGISTRY, subject_schema

    for table, rules in REGISTRY.items():
        subject = set(rules["subject_fields"])
        receipt = set(rules["receipt_fields"])
        assert not subject & receipt
        assert set(rules["input_fields"]) <= subject | receipt
        assert subject_schema(table).names == list(rules["subject_fields"])
    assert REGISTRY["fec_candidate_api_observations"]["subject_fields"]["candidate_status"] == "VARCHAR"
    assert "treasurer_name" in REGISTRY["fec_postgres_committee_history_observations"]["subject_fields"]
    assert "treasurer_text" not in REGISTRY["fec_postgres_committee_history_observations"]["subject_fields"]


def test_normalization_preserves_business_status_and_refuses_unclassified_field():
    import pytest
    from spicy_regs.transforms.fec_identity_context_fields import normalize_record

    row = {"record_id": "x", "candidate_status": "C", "cycles_json": "[2024,2024,null]", "election_years_json": "[]"}
    result = normalize_record("fec_candidate_api_observations", row)
    assert result["candidate_status"] == "C"
    assert result["cycles"] == [2024, 2024, None]
    assert result["election_years"] == []
    assert result["cycles_json"] == row["cycles_json"]
    with pytest.raises(ValueError, match="unclassified"):
        normalize_record("fec_candidate_api_observations", {**row, "surprise": 42})


def test_meeting_range_and_cancellation_are_domain_data():
    from spicy_regs.transforms.fec_identity_context_fields import normalize_record

    row = normalize_record(
        "fec_research_meeting_observations",
        {
            "record_id": "m",
            "title_raw": "February 25 and 27, 2025 (Canceled)",
            "reported_status": "canceled",
            "date_status": "source_listed_dates",
            "dates_json": '["2025-02-25","2025-02-27"]',
        },
    )
    assert row["reported_status"] == "canceled"
    assert row["date_kind"] == "listed_dates"
    assert row["dates"] == [date(2025, 2, 25), date(2025, 2, 27)]


def test_new_struct_business_fields_refuse_and_nonfinite_json_stays_diagnostic():
    import pytest

    assert native_list("[NaN]", "integer") == (None, [{"reason": "invalid_json"}])
    with pytest.raises(ValueError, match="Unclassified"):
        native_list('[{"sponsor_candidate_id":"H4NC05146","new_business_property":"x"}]', "sponsor_candidate")
