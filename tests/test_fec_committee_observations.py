"""Retained committee observations keep snapshot identity and native states."""

import json

import pytest

from spicy_regs.transforms import fec_committee_observations as committee
from spicy_regs.transforms.build_fec_committees import _shape
from spicy_regs.transforms.fec_query import CollectionSelection
from spicy_regs.transforms.fec_typed_batch import typed_batch

PIN = "sha256:" + "a" * 64
GEN = "sha256:" + "b" * 64
SELECTION = CollectionSelection("snapshot", PIN, GEN, "official-fec", None, "snapshot", PIN)


def source(value, *, control=None, ordinal=3):
    endpoint = dict(sha256=PIN, response_mode="page")
    native = dict(kind="api-response-field", field=control, value=value, source=endpoint)
    if control is None:
        endpoint["pointer"] = "/results/0"
        native = dict(kind="api-record-observation", metadata=value, source=endpoint)
    return dict(
        collection_id="snapshot",
        source_record_id=str(ordinal),
        source_sha256=PIN,
        source_url="https://api.open.fec.gov/v1/committees/?sort=committee_id&per_page=100&page=1",
        profile="document",
        metadata_json=json.dumps(native),
        source_locator_json=json.dumps(
            dict(collection_id="snapshot", source_record_id=str(ordinal), ordinal=ordinal, member=None, **endpoint)
        ),
    )


def mapped(value, **kwargs):
    row = source(value, **kwargs)
    tables, evidence = committee.map_committee_api(row, SELECTION)
    populated = [table for table, rows in tables.items() if rows]
    assert len(populated) == 1
    table = populated[0]
    result = tables[table][0]
    assert typed_batch([result], committee.SCHEMAS[table]).to_pylist() == [result]
    assert len(evidence) == 1 and evidence[0]["target_table"] == table
    return result


def test_core_fields_reuse_maintained_mapping_and_unknown_native_fields_survive():
    native = dict(
        committee_id="C00000000",
        name="  Exact name  ",
        cycles=[1976, 1978],
        candidate_ids=[],
        party=None,
        additional={"code": "001", "flag": False},
    )
    result = mapped(native)
    expected = _shape(native)
    for column in expected:
        if column.endswith("_json"):
            assert json.loads(result[column]) == json.loads(expected[column])
        else:
            assert result[column] == expected[column]
    assert "native_metadata_json" not in result
    assert result["source_pointer"] == "/metadata" and result["original_result_pointer"] == "/results/0"
    assert result["current_record_status"] == "unqualified" and result["source_cycle"] is None
    assert result["registry_scope_status"] == "retained-source-observations-only"
    assert result["query_completeness"] == "not-asserted"


@pytest.mark.parametrize(
    "present,value,expected,state",
    [
        (False, None, None, "source_missing"),
        (True, None, "null", "source_null"),
        (True, [], "[]", "source_empty"),
        (True, ["H00000001"], '["H00000001"]', "reported"),
    ],
)
def test_arrays_distinguish_missing_null_empty_and_reported(present, value, expected, state):
    native = {"committee_id": "C00000001"}
    if present:
        native["candidate_ids"] = value
    result = mapped(native)
    assert result["candidate_ids_json"] == expected
    assert result["candidate_ids_status"] == state
    assert "native_metadata_json" not in result


def test_unexpected_array_shape_stays_literal_with_explicit_partial_status():
    result = mapped(dict(committee_id="C00000001", cycles=""))
    assert result["cycles_json"] == '""'
    assert result["mapping_status"] == "partial"
    assert json.loads(result["mapping_reason_json"]) == {"cycles": "source_value_is_not_an_array"}


@pytest.mark.parametrize(
    "value,state",
    [
        (None, "source_null"),
        ("", "source_empty"),
        ([], "source_empty"),
        ({"pages": 897, "count": 89679, "page": 449}, "reported"),
    ],
)
def test_response_controls_are_separate_from_committee_and_financial_rows(value, state):
    result = mapped(value, control="pagination", ordinal=1)
    assert "committee_id" not in result
    assert "value_json" not in result
    assert result["value_status"] == state and result["response_field"] == "pagination"
    assert "currency" not in result and "amount_kind" not in result


def test_same_committee_in_different_snapshots_or_positions_remains_distinct():
    first = mapped({"committee_id": "C00000001"})
    second = mapped({"committee_id": "C00000001"}, ordinal=4)
    assert first["record_id"] != second["record_id"]
    row = source({"committee_id": "C00000001"})
    row["collection_id"] = "other-snapshot"
    row["source_locator_json"] = json.dumps(
        {**json.loads(row["source_locator_json"]), "collection_id": "other-snapshot"}
    )
    selection = CollectionSelection("other-snapshot", PIN, GEN, "official-fec", None, "snapshot", PIN)
    tables, _ = committee.map_committee_api(row, selection)
    assert tables[committee.COMMITTEES][0]["record_id"] != first["record_id"]


@pytest.mark.parametrize(
    "case", ["route", "host", "profile", "source-digest", "locator-id", "pointer", "collection", "kind"]
)
def test_wrong_source_selection_route_and_native_identity_refuse(case):
    row = source({"committee_id": "C00000001"})
    if case == "route":
        row["source_url"] = row["source_url"].replace("/committees/", "/candidates/")
    if case == "host":
        row["source_url"] = row["source_url"].replace("api.open.fec.gov", "example.com")
    if case == "profile":
        row["profile"] = "positional"
    if case == "collection":
        row["collection_id"] = "another"
    if case in {"source-digest", "kind"}:
        native = json.loads(row["metadata_json"])
        if case == "source-digest":
            native["source"]["sha256"] = GEN
        else:
            native["kind"] = "unknown"
        row["metadata_json"] = json.dumps(native)
    if case in {"locator-id", "pointer"}:
        locator = json.loads(row["source_locator_json"])
        locator["source_record_id" if case == "locator-id" else "pointer"] = "wrong"
        row["source_locator_json"] = json.dumps(locator)
    with pytest.raises(ValueError):
        committee.map_committee_api(row, SELECTION)


def test_native_results_container_is_preserved_without_creating_duplicate_entities():
    payload = [{"committee_id": "C00000001"}]
    result = mapped(payload, control="results")
    assert result["response_field_role"] == "result-container"
    assert result["observed_count"] == len(payload) and "value_json" not in result
    assert "committee_id" not in result


def test_committee_native_dates_affiliations_and_sponsors_are_promoted():
    from datetime import date

    result = mapped(
        dict(
            committee_id="C00000001",
            first_file_date="1975-07-08",
            first_f1_date="1976-03-11",
            affiliated_committee_name="Example",
            organization_type="L",
            sponsor_candidate_ids=["H2AK01158", "H2AK01158"],
        )
    )
    assert result["first_file_date"] == date(1975, 7, 8) and result["first_f1_date"] == date(1976, 3, 11)
    assert result["affiliated_committee_name"] == "Example" and result["organization_type"] == "L"
    assert json.loads(result["sponsor_candidate_ids_json"]) == ["H2AK01158", "H2AK01158"]
    assert result["sponsor_candidate_ids_status"] == "reported"
