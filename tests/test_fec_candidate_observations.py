import json

import pytest

from spicy_regs.transforms import fec_candidate_observations as candidate
from spicy_regs.transforms.fec_typed_batch import typed_batch

PIN = "sha256:" + "a" * 64
GEN = "sha256:" + "b" * 64
URL = "https://api.open.fec.gov/v1/candidates/?candidate_id=H2AK01158&per_page=100"


def entry():
    return dict(
        collection_id="candidate-page",
        profile="document",
        scope=dict(
            format="api-json",
            api_mode="page",
            member=None,
            capture=dict(
                requestUrl=URL,
                responseSha256=PIN,
                byteSize=100,
                representation="opaque",
                observedAt="2026-09-12T00:00:00Z",
            ),
        ),
    )


def row(value, field=None):
    source = dict(sha256=PIN, response_mode="page")
    if field is None:
        source["pointer"] = "/results/0"
        native = dict(kind="api-record-observation", metadata=value, source=source)
    else:
        native = dict(kind="api-response-field", field=field, value=value, source=source)
    return dict(
        collection_id="candidate-page",
        source_record_id="native-record",
        source_sha256=PIN,
        source_url=URL,
        profile="document",
        metadata_json=json.dumps(native),
        source_locator_json=json.dumps(
            dict(**source, member=None, collection_id="candidate-page", source_record_id="native-record")
        ),
    )


def mapped(value, field=None):
    r = row(value, field)
    p = candidate.prepare_candidate_api(entry(), source_generation_pin=GEN)
    tables, evidence = candidate.map_candidate_api(r, p)
    table = next(t for t, v in tables.items() if v)
    result = tables[table][0]
    assert typed_batch([result], candidate.SCHEMAS[table]).to_pylist() == [result]
    assert evidence[0]["witness_sha256"] == PIN
    return result


def test_literal_candidate_preserves_arrays_null_missing_native_fields_and_no_cycle_inference():
    native = dict(
        candidate_id="H2AK01158",
        name="  Native name  ",
        cycles=[2022, 2024, 2026],
        election_years=[],
        inactive_election_years=None,
        has_raised_funds=False,
        extra={"integer": 0},
    )
    r = mapped(native)
    assert "native_metadata_json" not in r and "native_field_states_json" not in r
    assert r["has_raised_funds"] is False and r["has_raised_funds_status"] == "parsed"
    assert r["name"] == "  Native name  " and r["source_cycle"] is None
    assert r["current_record_status"] == "unqualified" and r["candidate_id_status"] == "source_id_shape"
    assert r["election_years_status"] == "source_empty"
    assert r["inactive_election_years_status"] == "source_null"
    assert r["election_districts_status"] == "source_missing"
    assert r["cycles_json"] == "[2022,2024,2026]" and r["election_districts_json"] is None


@pytest.mark.parametrize("value", [[], [{"candidate_id": "H2AK01158"}]])
def test_results_container_is_control_not_another_candidate(value):
    r = mapped(value, "results")
    assert r["response_field_role"] == "result-container"
    assert r["observed_count"] == len(value) and "candidate_id" not in r
    assert "value_json" not in r and "native_metadata_json" not in r


@pytest.mark.parametrize("value,state", [(None, "source_null"), ({}, "reported"), ({"count": 0}, "reported")])
def test_empty_query_controls_do_not_invent_absent_candidates(value, state):
    r = mapped(value, "pagination")
    assert r["value_status"] == state
    assert r["query_completeness"] == "not-asserted"


@pytest.mark.parametrize("case", ["digest", "locator-id", "pointer", "member", "profile", "url"])
def test_mismatched_source_refuses(case):
    r = row({"candidate_id": "H2AK01158"})
    p = candidate.prepare_candidate_api(entry(), source_generation_pin=GEN)
    loc = json.loads(r["source_locator_json"])
    if case == "digest":
        r["source_sha256"] = GEN
    if case == "locator-id":
        loc["source_record_id"] = "wrong"
    if case == "pointer":
        loc["pointer"] = "/results/1"
    if case == "member":
        loc["member"] = {"ordinal": 0, "name": "x"}
    if case == "profile":
        r["profile"] = "positional"
    if case == "url":
        r["source_url"] = URL.replace("candidates", "committees")
    r["source_locator_json"] = json.dumps(loc)
    with pytest.raises(ValueError):
        candidate.map_candidate_api(r, p)


def test_source_owned_request_route_validation():
    e = entry()
    e["scope"]["capture"]["requestUrl"] = URL.replace("candidates", "committees")
    with pytest.raises(ValueError):
        candidate.prepare_candidate_api(e, source_generation_pin=GEN)


def test_useful_candidate_fields_are_typed_once_and_identity_stays_stable():
    from datetime import date
    from spicy_regs.transforms.fec_query import observation_id

    native = dict(
        candidate_id="H2AK01158",
        active_through=2026,
        first_file_date="2022-04-01",
        candidate_inactive=False,
        has_raised_funds=True,
        last_file_date="24-11-08",
    )
    result = mapped(native)
    assert result["active_through"] == 2026 and result["first_file_date"] == date(2022, 4, 1)
    assert result["candidate_inactive"] is False and result["has_raised_funds"] is True
    assert result["last_file_date"] is None and result["last_file_date_status"] == "unsupported_date"
    assert result["record_id"] == observation_id(candidate.CANDIDATES, row(native), "official-fec")
    assert (
        not {"currency", "amount_kind", "value_mapping_version", "filing_key", "correction_operation"} & result.keys()
    )
