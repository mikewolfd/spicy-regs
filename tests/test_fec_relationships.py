"""Reported relationships retain ambiguity, source roles and reversible evidence."""

import copy
import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.fec_relationships import (
    COLUMNS,
    api_relationships,
    bulk_relationships,
    statement_relationships,
    write_fec_relationship_rows,
)

REF = {
    "sha256": "sha256:" + "a" * 64,
    "url": "https://www.fec.gov/files/example",
    "observed_at": "2026-09-12T00:00:00Z",
    "locator": {"row": 1},
}


def test_api_current_fields_are_not_multiplied_into_history_or_name_matched():
    source = {
        "committee_id": "C00812388",
        "cycles": [2024, 2026],
        "candidate_ids": ["H2AK01158", "C00778159", "H2AK01158"],
        "affiliated_committee_name": "COMMITTEE WITH A MATCHING NAME",
        "sponsor_candidate_ids": ["H2AK01158"],
        "sponsor_candidate_list": [{"sponsor_candidate_id": "H2AK01158", "sponsor_candidate_name": "Name"}],
    }
    rows = list(api_relationships(source, evidence=REF))
    assert len(rows) == 6
    assert all(r["cycle"] is None for r in rows)
    assert rows[0]["object_id"] is None
    assert rows[2]["object_id"] == "C00778159"
    assert rows[2]["object_id_status"] == "invalid_source_id_shape"
    assert rows[1]["relationship_type"] == "committee_candidate"
    assert rows[-1]["object_name"] == "Name"
    assert json.loads(rows[-1]["source_locator_json"])["array_index"] == 0
    assert all(r["source_sha256"] == REF["sha256"] for r in rows)


@pytest.mark.parametrize(
    "field,value,status",
    [
        ("candidate_ids", [], "empty_list"),
        ("candidate_ids", None, "null"),
        ("affiliated_committee_name", "", "empty_string"),
        ("affiliated_committee_name", " NONE ", "reported_none"),
    ],
)
def test_empty_observations_are_not_edges(field, value, status):
    rows = list(api_relationships({"committee_id": "C00812388", field: value}, evidence=REF))
    actual = next(r for r in rows if field in json.loads(r["source_locator_json"])["fields"])
    assert actual["value_status"] == status
    assert all(r["value_status"] == "missing_field" for r in rows if r is not actual)
    assert json.loads(actual["source_fields_json"])[field] == value


@pytest.mark.parametrize("field", ["candidate_ids", "sponsor_candidate_ids", "sponsor_candidate_list"])
@pytest.mark.parametrize("state", ["missing_field", "null", "empty_list"])
def test_every_source_array_has_an_explicit_absence_observation(field, state):
    # Not dict[str, str]: the point of this case is the three shapes an array
    # field arrives in — absent, null and empty — so the value type is a union.
    source: dict[str, object] = {"committee_id": "C00812388"}
    if state != "missing_field":
        source[field] = None if state == "null" else []
    rows = list(api_relationships(source, evidence=REF))
    assert len(rows) == 4
    (actual,) = [r for r in rows if json.loads(r["source_locator_json"]).get("array_field") == field]
    locator = json.loads(actual["source_locator_json"])
    fields = json.loads(actual["source_fields_json"])
    assert actual["value_status"] == state
    assert actual["object_id"] is None and actual["object_name"] is None
    assert actual["object_id_status"] == "not_reported"
    assert locator["array_index"] is None
    assert (field in fields) == (state != "missing_field")
    if state != "missing_field":
        assert fields[field] == source[field]


@pytest.mark.parametrize("reverse", [False, True])
def test_sponsor_arrays_are_independent_assertions_with_exact_elements(reverse):
    sponsors = [
        {"sponsor_candidate_id": "S6OH00379", "sponsor_candidate_name": "First", "extra": {"literal": 1}},
        {"sponsor_candidate_id": "H2AK01158", "sponsor_candidate_name": "Second"},
        {"sponsor_candidate_name": "Unresolved"},
        {"sponsor_candidate_id": None, "sponsor_candidate_name": "Unresolved"},
        None,
        {},
        {"sponsor_candidate_name": " NONE "},
        {"sponsor_candidate_name": ""},
    ]
    if reverse:
        sponsors[:2] = reversed(sponsors[:2])
    source = {
        "committee_id": "C00812388",
        "sponsor_candidate_ids": ["S6OH00379", "H2AK01158", "S6OH00379"],
        "sponsor_candidate_list": sponsors,
    }
    original = copy.deepcopy(source)
    rows = list(api_relationships(source, evidence=REF))
    ids = [r for r in rows if json.loads(r["source_locator_json"]).get("array_field") == "sponsor_candidate_ids"]
    listed = [r for r in rows if json.loads(r["source_locator_json"]).get("array_field") == "sponsor_candidate_list"]
    assert [r["object_id"] for r in ids] == ["S6OH00379", "H2AK01158", "S6OH00379"]
    assert [r["object_id"] for r in listed] == (
        ["H2AK01158", "S6OH00379"] if reverse else ["S6OH00379", "H2AK01158"]
    ) + [None] * 6
    assert [r["object_name"] for r in listed] == (["Second", "First"] if reverse else ["First", "Second"]) + [
        "Unresolved",
        "Unresolved",
        None,
        None,
        " NONE ",
        "",
    ]
    assert [r["value_status"] for r in listed] == [
        "reported",
        "reported",
        "reported",
        "reported",
        "null",
        "null",
        "reported_none",
        "empty_string",
    ]
    for index, actual in enumerate(listed):
        assert json.loads(actual["source_fields_json"])["sponsor_candidate_list"] == sponsors[index]
        assert json.loads(actual["source_locator_json"])["array_index"] == index
        assert actual["relationship_type"] == "leadership_pac_sponsor"
    assert source == original


@pytest.mark.parametrize(
    "source",
    [
        {
            "committee_id": "C00872283",
            "committee_type": "I",
            "committee_type_full": "Independent expenditure filer (not a committee)",
        },
        {"committee_id": "C00894162", "organization_type": "I", "organization_type_full": None},
    ],
)
def test_identifier_role_preserves_source_entity_qualifier_and_missing_labels(source):
    rows = list(api_relationships(source, evidence=REF))
    for actual in rows:
        assert actual["subject_type"] == "committee"  # Source field role, not entity status.
        assert actual["subject_id_status"] == "source_id_shape"
        assert json.loads(actual["source_fields_json"]) == source
    without_label = {key: value for key, value in source.items() if key != "organization_type_full"}
    assert "organization_type_full" not in json.loads(
        next(api_relationships(without_label, evidence=REF))["source_fields_json"]
    )


def test_api_array_context_grows_linearly_and_keeps_parent_locator():
    def build(size):
        source = {
            "committee_id": "C00812388",
            "candidate_ids": ["H2AK01158"] * size,
            "cycles": list(range(size)),
            "affiliated_committee_name": "Literal",
        }
        ref = {**REF, "locator": {"json_pointer": "/results/7"}}
        rows = list(api_relationships(source, evidence=ref))
        members = [r for r in rows if r["relationship_type"] == "committee_candidate"]
        for index, actual in enumerate(members):
            locator = json.loads(actual["source_locator_json"])
            fields = json.loads(actual["source_fields_json"])
            assert locator["json_pointer"] == "/results/7"
            assert locator["array_field"] == "candidate_ids" and locator["array_index"] == index
            assert fields == {"committee_id": "C00812388", "candidate_ids": "H2AK01158"}
        assert len(members) == size
        scalar = rows[0]
        assert "array_field" not in json.loads(scalar["source_locator_json"])
        return sum(len(r["source_fields_json"].encode()) for r in members), scalar

    small_cost, small_scalar = build(16)
    large_cost, large_scalar = build(32)
    assert large_cost == 2 * small_cost  # Selected field bytes, not disk, time or memory.
    assert small_scalar == large_scalar  # Unrelated scalar does not copy either array.


@pytest.mark.parametrize("field", ["candidate_ids", "sponsor_candidate_ids", "sponsor_candidate_list"])
@pytest.mark.parametrize("value", ["unexpected scalar", 0, {}])
def test_malformed_source_arrays_refuse(field, value):
    with pytest.raises(ValueError, match="array"):
        list(api_relationships({"committee_id": "C00812388", field: value}, evidence=REF))


@pytest.mark.parametrize("value", [["not an object"], [{"sponsor_candidate_id": []}]])
def test_malformed_sponsor_elements_refuse(value):
    with pytest.raises(ValueError, match="source"):
        list(api_relationships({"committee_id": "C00812388", "sponsor_candidate_list": value}, evidence=REF))


def test_bulk_role_codes_and_election_year_are_source_values():
    source = {
        "CAND_ID": "C00778159",
        "CMTE_ID": "C00778159",
        "CMTE_DSGN": "D",
        "CAND_ELECTION_YR": "2022",
        "FEC_ELECTION_YR": "2026",
        "LINKAGE_ID": "001",
    }
    (row,) = bulk_relationships("linkage", source, evidence=REF, cycle=2026)
    assert row["subject_id_status"] == "invalid_source_id_shape"
    assert row["relationship_type"] == "candidate_committee_link"
    assert row["candidate_election_year"] == "2022" and row["cycle"] == "2026"
    assert json.loads(row["source_fields_json"]) == source
    source["CMTE_DSGN"] = "A"
    assert (
        next(bulk_relationships("linkage", source, evidence=REF, cycle=2026))["relationship_type"]
        == "authorized_committee"
    )


@pytest.mark.parametrize("version,start", [("8.3", 37), ("8.4", 39)])
@pytest.mark.parametrize(
    "code,role",
    [
        ("ORG", "connected_organization"),
        ("AFF", "affiliated_committee"),
        ("JFR", "joint_fundraising_participant"),
        ("LPS", "leadership_pac_sponsor"),
        ("UNKNOWN", "reported_affiliation"),
    ],
)
def test_statement_layout_shift_and_explicit_roles(version, start, code, role):
    fields = {str(i): "" for i in range(103)}
    fields.update(
        {
            "0": "F1A",
            "1": "C00812388",
            "21": "C",
            "22": "H2AK01158",
            str(start + 1): "Literal name",
            str(start + 13): code,
            "102": "UNKNOWN EXTRA FIELD",
        }
    )
    if code == "LPS":
        fields[str(start + 2)] = "H2AK01158"
    rows = list(statement_relationships({"fields": fields}, version=version, evidence=REF))
    assert rows[0]["relationship_type"] == "support_or_oppose_candidate"
    assert rows[1]["relationship_type"] == role
    assert rows[1]["object_name"] == "Literal name"
    assert rows[1]["object_id"] == ""  # A name alone is not an identity.
    assert str(start + 13) in json.loads(rows[1]["source_locator_json"])["fields"]
    assert fields["102"] == "UNKNOWN EXTRA FIELD"  # Input and original are not rewritten.


def test_supplementary_records_and_conflicting_targets_survive():
    fields = {
        "0": "F1S",
        "1": "C00812388",
        "2": "Participant",
        "3": "C00825034",
        "4": "C00817361",
        "5": "Affiliate",
        "6": "H2AK01158",
        "7": "Last",
        "8": "First",
        "17": "AFF",
    }
    rows = list(statement_relationships({"fields": fields}, version="8.4", evidence=REF))
    assert [(r["object_id"], r["object_type"]) for r in rows] == [
        ("C00825034", "committee"),
        ("C00817361", "committee"),
        ("H2AK01158", "candidate"),
    ]
    assert json.loads(rows[2]["source_fields_json"])["7"] == "Last"
    (row,) = statement_relationships(
        {"fields": {"0": "F2S", "1": "H2AK01158", "2": "C00817361", "3": "Named"}}, version="8.4", evidence=REF
    )
    assert row["relationship_type"] == "authorized_committee"
    assert row["candidate_election_year"] is None


def test_form2_dates_are_not_inferred_from_acquisition_cycle():
    fields = {
        "0": "F2A",
        "1": "H2AK01158",
        "22": "2024",
        "23": "C00812388",
        "24": "Principal",
        "30": "C00825034",
        "31": "Other",
        "42": "20221129",
    }
    rows = list(statement_relationships({"fields": fields}, version="8.4", evidence=REF))
    assert [r["relationship_type"] for r in rows] == ["principal_campaign_committee", "authorized_committee"]
    assert all(r["candidate_election_year"] == "2024" and r["cycle"] is None for r in rows)
    assert all(json.loads(r["source_fields_json"])["42"] == "20221129" for r in rows)


def test_unsupported_inputs_refuse_instead_of_guessing():
    with pytest.raises(ValueError, match="version"):
        list(statement_relationships({}, version="9.0", evidence=REF))
    with pytest.raises(ValueError, match="record"):
        list(statement_relationships({"fields": {"0": "F13N"}}, version="8.4", evidence=REF))
    with pytest.raises(ValueError, match="digest"):
        list(api_relationships({"committee_id": "C00812388"}, evidence={**REF, "sha256": "bad"}))
    with pytest.raises(ValueError, match="array"):
        list(api_relationships({"committee_id": "C00812388", "candidate_ids": "H2AK01158"}, evidence=REF))


def test_writer_preserves_previous_output_after_late_failure_and_all_columns(tmp_path):
    rows = list(api_relationships({"committee_id": "C00812388", "candidate_ids": []}, evidence=REF))
    target = write_fec_relationship_rows(rows, tmp_path / "relationships.parquet", batch_size=2)
    assert pq.read_table(target).column_names == list(COLUMNS)
    assert pq.read_table(target).to_pylist() == rows
    prior = target.read_bytes()

    def failed():
        yield from rows
        raise ValueError("source failure")

    with pytest.raises(ValueError, match="source failure"):
        write_fec_relationship_rows(failed(), target, batch_size=1)
    assert target.read_bytes() == prior
    assert list(tmp_path.iterdir()) == [target]
    with pytest.raises(ValueError, match="columns"):
        write_fec_relationship_rows([{**rows[0], "unexpected": "must not disappear"}], target)
    assert target.read_bytes() == prior
