"""Source outcomes reach ordinary generation, publication rehearsal and SQL."""

import hashlib
import json
from copy import deepcopy

import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin
from spicy_docs.sources.census_geocoder import BENCHMARK, VINTAGE, GeocoderSelection, lookup_record, selection_id
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs import mcp_server
from spicy_regs.pipelines.census_geocoder import build
from spicy_regs.sources import publication
from spicy_regs.transforms import build_census_geocoder as module
from tests.generation_fakes import Store


class Reader:
    pin = ArtifactPin("urn:test:census-geocoder:" + "d" * 64, "sha256:" + "a" * 64)

    def __init__(self, records):
        self.records = records

    def iter_records(self):
        yield from deepcopy(self.records)


def reader(case="matched", suffix=""):
    selection = GeocoderSelection("onelineaddress", BENCHMARK, VINTAGE, address="1 Main St " + case + suffix)
    coordinates: dict[str, float | str] = {"x": -122.25, "y": 37.5}
    candidate = {
        "matchedAddress": "SYNTHETIC",
        "coordinates": coordinates,
        "geographies": {
            "Counties": [
                {"GEOID": "06001", "STATE": "06", "COUNTY": "001", "NAME": "Alameda", "unknown": [None, "", 0]}
            ]
        },
    }
    matches = [candidate]
    if case == "unmatched":
        matches = []
    elif case == "ambiguous":
        matches.append(deepcopy(candidate))
    elif case == "conversion_failed":
        coordinates["x"] = "malformed"
    elif case == "refused":
        candidate.pop("geographies")
    value = {
        "result": {
            "input": {
                "address": {"address": selection.address},
                "benchmark": {"benchmarkName": BENCHMARK},
                "vintage": {"vintageName": VINTAGE},
            },
            "addressMatches": matches,
        }
    }
    raw = json.dumps(value).replace("-122.25", "-122.250000123456789000").encode()
    capture = CapturedBodyResponse(selection.url, selection.url, 200, "application/json", "2026-11-13T00:00:00Z", raw)
    row = lookup_record(selection, capture)
    return Reader([{"sourceRecordId": selection_id(selection), "record": row}])


def test_generation_query_and_publication_preserve_all_outcomes(tmp_path, monkeypatch):
    readers = [reader(outcome) for outcome in ("matched", "unmatched", "ambiguous", "refused", "conversion_failed")]
    artifact, evidence = build(readers, tmp_path)
    lookup_rows = pq.read_table(tmp_path / (module.LOOKUPS + ".parquet")).to_pylist()
    assert {r["outcome"] for r in lookup_rows} == {"matched", "unmatched", "ambiguous", "refused", "conversion_failed"}
    geography_rows = pq.read_table(tmp_path / (module.GEOGRAPHIES + ".parquet")).to_pylist()
    assert len(geography_rows) == 3
    assert all(
        r["state_code"] == "06" and r["county_code"] == "001" and r["county_geoid"] == "06001" for r in geography_rows
    )
    assert all(r["x"] == "-122.250000123456789000" for r in geography_rows)
    assert all(r["unique_assignment"] == (r["outcome"] == "matched") for r in geography_rows)
    assert json.loads(geography_rows[0]["geography_json"])["unknown"] == [None, "", 0]
    journal = [json.loads(line) for line in (evidence / "journal.jsonl").read_text().splitlines()]
    captures = [e for e in journal if e["event"] == "capture"]
    assert len(captures) == 5
    assert all(e["source_artifact_digest"] == readers[0].pin.artifact_digest for e in captures)
    assert {e["sha256"] for e in captures} == {r["response_sha256"] for r in lookup_rows}
    store = Store()
    index = publication.publish_generation(
        tmp_path / "generation",
        client=store,
        bucket="test",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"]["census-geocoder"]["artifactDigest"] == artifact.pin.artifact_digest
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path / "generation")
    with mcp_server._build_connection() as con:
        answer = con.execute("""SELECT l.outcome,g.county_geoid,g.x,g.response_sha256
            FROM census_geocoder_lookups l LEFT JOIN census_geocoder_geographies g USING(lookup_id)
            WHERE l.outcome = 'matched' AND g.layer = 'Counties' AND g.unique_assignment""").fetchone()
        failures = con.execute("""SELECT outcome,count(*) FROM census_geocoder_lookups
            WHERE outcome <> 'matched' GROUP BY outcome ORDER BY outcome""").fetchall()
    assert answer is not None
    assert answer[:3] == ("matched", "06001", "-122.250000123456789000")
    assert answer[3].startswith("sha256:")
    assert failures == [("ambiguous", 1), ("conversion_failed", 1), ("refused", 1), ("unmatched", 1)]


def test_all_unmatched_still_produces_empty_typed_geography_table(tmp_path):
    build([reader("unmatched")], tmp_path)
    table = pq.read_table(tmp_path / (module.GEOGRAPHIES + ".parquet"))
    assert table.num_rows == 0 and table.schema == module.GEOGRAPHY_SCHEMA


@pytest.mark.parametrize("defect", ["duplicate", "missing", "changed-response", "changed-outcome", "wrong-id"])
def test_invalid_cohort_is_refused_before_any_table(tmp_path, defect):
    source = reader()
    cohort = [source]
    if defect == "duplicate":
        cohort.append(source)
    elif defect == "missing":
        source.records.clear()
    elif defect == "changed-response":
        source.records[0]["record"]["response_json"] += " "
    elif defect == "changed-outcome":
        source.records[0]["record"]["outcome"] = "unmatched"
    else:
        source.records[0]["sourceRecordId"] = "other"
    with pytest.raises(ValueError):
        module.build_census_geocoder(cohort, tmp_path)
    assert not list(tmp_path.glob("*.parquet"))


def test_rebuild_requires_fresh_output_and_preserves_existing_candidate(tmp_path):
    build([reader()], tmp_path)
    target = tmp_path / "generation" / (module.LOOKUPS + ".parquet")
    digest = hashlib.sha256(target.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="fresh"):
        build([reader("unmatched")], tmp_path)
    assert hashlib.sha256(target.read_bytes()).hexdigest() == digest
