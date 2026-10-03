"""Exact links read one immutable input set, including partitioned bill tables."""

from copy import deepcopy
import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.pipelines.rollups.scorecard_analysis import ScorecardAnalysisRollup
from spicy_regs.scorecards.resolution import OFFICIAL_COLUMNS
from spicy_regs.sources import publication as pub
from spicy_regs.transforms.build_scorecard_analysis import _read
from tests.generation_fakes import Store


def test_old_member_artifact_reports_missing_votesmart_column(tmp_path):
    path = tmp_path / "members.parquet"
    old_columns = [column for column in OFFICIAL_COLUMNS["members"] if column != "votesmart_id"]
    pq.write_table(pa.Table.from_pylist([], schema=pa.schema([(c, pa.string()) for c in old_columns])), path)
    with pytest.raises(ValueError, match=r"members\.parquet lacks required columns: votesmart_id"):
        _read([path], OFFICIAL_COLUMNS["members"])


def _inputs(tmp_path, *, row_overrides=None):
    source = tmp_path / "inputs"
    source.mkdir()
    provenance = {
        "scorecard_id": "test:2025",
        "snapshot_id": "s1",
        "capture_id": "c1",
        "source_url": "https://publisher.example/2025",
        "source_path": "/table/1",
    }
    rows = {
        "scorecards": [{"scorecard_id": "test:2025", "snapshot_id": "s1", "year_text": "2025"}],
        "scorecard_members": [
            {
                **provenance,
                "publisher_member_key": "p1",
                "member_name": "Alex Example",
                "state": "CA",
                "district": "1",
                "chamber_text": "House",
            }
        ],
        "scorecard_items": [
            {
                **provenance,
                "item_id": "i1",
                "item_kind_text": "cosponsorship",
                "congress_text": "119",
                "bill_citation_text": "H.R. 1",
            }
        ],
        "members": [{"bioguide_id": "X000001", "name_first": "Alex", "name_last": "Example"}],
        "member_terms": [
            {
                "bioguide_id": "X000001",
                "term_index": "0",
                "term_type": "rep",
                "term_start": "2025-01-03",
                "term_end": "2027-01-03",
                "term_state": "CA",
                "term_district": "1",
            }
        ],
        "amendments": [],
        "roll_call_votes": [],
    }
    rows.update(row_overrides or {})
    files = []
    for name, records in rows.items():
        columns = (
            OFFICIAL_COLUMNS[name]
            if name in OFFICIAL_COLUMNS
            else tuple(dict.fromkeys(column for record in records for column in record))
        )
        path = source / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(records, schema=pa.schema([(c, pa.string()) for c in columns])), path)
        files.append(path)
    for congress in ("118", "119"):
        directory = source / "congress_bills" / f"congress={congress}"
        directory.mkdir(parents=True)
        pq.write_table(
            pa.table({"bill_id": [f"{congress}-hr-1"], "congress": [congress]}), directory / "part-000000.parquet"
        )
    files.append(source / "congress_bills")
    artifact = tmp_path / "published-inputs"
    build_generation(
        artifact,
        family="test-inputs",
        files=files,
        expected_keys=ScorecardAnalysisRollup.inputs,
        partitioned={"congress_bills.parquet": ("congress",)},
    )
    store = Store()
    index = pub.publish_generation(artifact, client=store, bucket="test", prior_index=pub.empty_index())
    return store, index


def _serve(monkeypatch, store):
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example")
    calls = []

    def fetch(url, member, target, key):
        calls.append(member.path)
        target.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)
    return calls


def test_analysis_pins_every_partition_and_keeps_publisher_identities(tmp_path, monkeypatch):
    store, index = _inputs(tmp_path)
    calls = _serve(monkeypatch, store)
    output = tmp_path / "analysis"
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    parents = rollup._prime(output, index)
    assert len(calls) == len(rollup.inputs) + 1

    assert "tableDescriptorDigest" in parents["congress_bills.parquet"]
    assert "sha256" not in parents["congress_bills.parquet"]
    members, items = rollup.build(output)
    member = pq.read_table(members).to_pylist()[0]
    item = pq.read_table(items).to_pylist()[0]
    assert member["publisher_member_key"] == "p1"
    assert member["bioguide_id"] == "X000001"
    assert item["bill_id"] == "119-hr-1"
    assert item["vote_id"] is None
    assert json.loads(item["input_pins_json"])["congress_bills"] == parents["congress_bills.parquet"]
    sealed = tmp_path / "analysis-generation"
    build_generation(
        sealed,
        family="scorecard-analysis",
        files=[members, items],
        expected_keys=rollup.outputs,
        parents=parents,
        read_snapshot=index,
    )
    verify_generation(sealed)
    # A warm read validates bytes and needs no network.
    rollup._prime(output, index)
    assert len(calls) == len(rollup.inputs) + 1


def test_analysis_preserves_unresolved_people_and_independent_actions(tmp_path, monkeypatch):
    provenance = {
        "scorecard_id": "test:2025",
        "snapshot_id": "s1",
        "capture_id": "c1",
        "source_url": "https://publisher.example/2025",
        "source_path": "/table/1",
    }
    source_people = [
        {
            **provenance,
            "publisher_member_key": "named-participant",
            "member_name": "Unresolved named participant",
            "chamber_text": "Senate",
            "identifiers_json": '[{"scheme":"ocd-person","value":"publisher-supplied-value"}]',
        }
    ]
    items = [
        {
            **provenance,
            "item_id": "committee-motion",
            "item_kind_text": "committee_action",
            "title": "Senate Finance Committee: motion reported without a bill",
            "chamber_text": "Senate",
            "congress_text": "119",
            "session_text": "1",
            "roll_number_text": "1",
        },
        {
            **provenance,
            "item_id": "cosponsor",
            "item_kind_text": "cosponsorship",
            "congress_text": "119",
            "bill_citation_text": "H.R. 1",
        },
        *[
            {
                **provenance,
                "item_id": f"vote-{roll}",
                "item_kind_text": "roll_call",
                "congress_text": "119",
                "chamber_text": "House",
                "session_text": "1",
                "roll_number_text": str(roll),
                "bill_citation_text": "H.R. 1",
            }
            for roll in (1, 2)
        ],
    ]
    originals = deepcopy((source_people, items))
    store, index = _inputs(
        tmp_path,
        row_overrides={
            "scorecard_members": source_people,
            "scorecard_items": items,
            "roll_call_votes": [{"vote_id": f"119-house-1-{roll}", "bill_id": "119-hr-1"} for roll in (1, 2)],
        },
    )
    calls = _serve(monkeypatch, store)
    output = tmp_path / "analysis"
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    rollup._prime(output, index)
    acquired = list(calls)
    members, links = rollup.build(output)
    # Analysis uses only the selected inputs; it does not acquire official data again.
    assert calls == acquired
    person = pq.read_table(members).to_pylist()[0]
    assert person["bioguide_id"] is None
    assert person["resolution_status"] == "unresolved"
    observed = json.loads(person["source_context_json"])
    assert observed["member_name"] == source_people[0]["member_name"]
    assert observed["identifiers_json"] == source_people[0]["identifiers_json"]
    linked = {row["item_id"]: row for row in pq.read_table(links).to_pylist()}
    assert linked["committee-motion"]["vote_id"] is None
    assert linked["committee-motion"]["bill_id"] is None
    assert "committee_action_not_mapped_to_floor_roll" in linked["committee-motion"]["reason"]
    assert linked["cosponsor"]["bill_id"] == "119-hr-1"
    assert linked["cosponsor"]["vote_id"] is None
    assert {linked[f"vote-{roll}"]["vote_id"] for roll in (1, 2)} == {"119-house-1-1", "119-house-1-2"}
    assert {linked[f"vote-{roll}"]["bill_id"] for roll in (1, 2)} == {"119-hr-1"}
    assert (source_people, items) == originals


@pytest.mark.parametrize("corruption", ["bytes", "missing-family"])
def test_analysis_refuses_inputs_that_do_not_match_the_index(tmp_path, monkeypatch, corruption):
    store, index = _inputs(tmp_path)
    _serve(monkeypatch, store)
    output = tmp_path / "analysis"
    output.mkdir()
    rollup = ScorecardAnalysisRollup(output_dir=output)
    rollup._prime(output, index)
    if corruption == "bytes":
        (output / "congress_bills/congress=119/part-000000.parquet").write_bytes(b"different")
    else:
        index = pub.empty_index()
    with pytest.raises(pub.PublicationError):
        rollup._prime(output, index)
    with pytest.raises(pub.PublicationError, match="Prime verified"):
        rollup.build(output)
    assert not (output / "scorecard_member_links.parquet").exists()


def test_split_parent_pin_rejects_a_changed_member_or_size(tmp_path):
    _, index = _inputs(tmp_path)
    key = "congress_bills.parquet"
    pin = pub.table_pin(index, key)
    output = tmp_path / "derived.parquet"
    pq.write_table(pa.table({"id": ["one"]}), output)
    for number, field in enumerate(("member", "byteSize", "unmanaged")):
        changed = deepcopy(index)
        parent = deepcopy(pin)
        if field == "member":
            changed["families"]["test-inputs"]["tables"][key]["members"][0]["sha256"] = "sha256:" + "0" * 64
        elif field == "byteSize":
            parent["byteSize"] += 1
        else:
            parent.pop("family")
            parent.pop("artifactDigest")
        with pytest.raises(ValueError, match="[Pp]arent"):
            build_generation(
                tmp_path / f"refused-{number}",
                family="derived",
                files=[output],
                expected_keys=[output.name],
                parents={key: parent},
                read_snapshot=changed,
            )
