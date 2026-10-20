"""Small synthetic source records exercise domain meaning and the existing consumers."""
from copy import deepcopy
from decimal import Decimal
import hashlib
import json

import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin

from spicy_regs import mcp_server
from spicy_regs.pipelines.census_acs import build
from spicy_regs.sources import publication
from spicy_regs.transforms import build_census_acs as module
from tests.generation_fakes import Store


class Reader:
    pin = ArtifactPin("urn:test:census-acs:" + "d" * 64, "sha256:" + "a" * 64)

    def __init__(self, records):
        self.records = records

    def iter_records(self):
        yield from deepcopy(self.records)

    def iter_record_evidence(self):
        for item in self.records:
            yield {"sourceRecordId": item["sourceRecordId"],
                   "evidenceBlobRef": "sha256:" + hashlib.sha256(b"synthetic source fixture").hexdigest()}

    def read_evidence(self, ref, *, max_bytes):
        return b"synthetic source fixture"


def record(county="001", income="80000.25"):
    variables = [base + suffix for base in module.MEASURES for suffix in ("E", "M", "EA", "MA")]
    values = {key: None if key.endswith("A") else "100" for key in variables}
    values["B19013_001E"] = income
    return {"sourceRecordId": "2024:10:" + county, "record": {
        "dataset": "acs/acs5", "vintage": 2024, "geography": "county", "state": "10", "county": county,
        "name": "County " + county, "geo_id": "0500000US10" + county, "values": values,
        "variables": {key: {"label": key, "predicateType": "int"} for key in variables},
        "value_sources": {key: {
            "request_url": "https://api.census.gov/data/2024/acs/acs5?for=county:*&in=state:10",
            "response_sha256": "sha256:" + "b" * 64, "observed_at": "2026-10-20T00:00:00Z",
            "row_index": 1, "column_index": index,
        } for index, key in enumerate(variables)},
        "metadata_sources": {key: {
            "request_url": "https://api.census.gov/data/2024/acs/acs5/variables.json",
            "response_sha256": "sha256:" + "c" * 64, "observed_at": "2026-10-20T00:00:00Z",
            "json_pointer": "/variables/" + key,
        } for key in variables},
    }}


@pytest.mark.parametrize(("raw", "annotation", "status"), [
    ("-999999999", "N", "insufficient_sample"),
    ("-888888888.0", "(X)", "not_applicable_or_unavailable"),
    ("-666666666.00", "-", "unavailable"),
    ("-555555555", "*****", "controlled_estimate"),
    ("-333333333", "***", "open_ended_median"),
    ("-222222222", "**", "insufficient_observations"),
    (None, None, "null"), ("", None, "blank"),
])
def test_missing_and_sentinel_cells_survive_without_becoming_numbers(tmp_path, raw, annotation, status):
    item = record()
    item["record"]["values"].update(B19013_001E=raw, B19013_001EA=annotation)
    path = module.build_census_acs(Reader([item]), tmp_path, vintage=2024, state="10", counties=("001",))
    row = pq.read_table(path).to_pylist()[1]
    assert row["estimate"] is None
    assert (row["estimate_status"], row["estimate_raw"], row["estimate_annotation"]) == (status, raw, annotation)
    assert row["county_code"] == "001" and row["county_geoid"] == "10001"


@pytest.mark.parametrize(("annotation", "status"), [
    ("250,000+", "lower_bound"), ("250000-", "upper_bound"), ("-", "annotated"), ("unknown", "annotated"),
])
def test_only_numeric_median_annotations_establish_bounds(tmp_path, annotation, status):
    item = record(income="250000")
    item["record"]["values"]["B19013_001EA"] = annotation
    row = pq.read_table(module.build_census_acs(
        Reader([item]), tmp_path, vintage=2024, state="10", counties=("001",),
    )).to_pylist()[1]
    assert row["estimate"] == Decimal("250000")
    assert row["estimate_status"] == status
    assert row["estimate_annotation"] == annotation


@pytest.mark.parametrize("value", ["NaN", "1e3", "1.0000001"])
def test_non_decimal_or_inexact_cells_are_refused(tmp_path, value):
    with pytest.raises(ValueError):
        module.build_census_acs(Reader([record(income=value)]), tmp_path,
                               vintage=2024, state="10", counties=("001",))
    assert not (tmp_path / module.OUTPUT).exists()


@pytest.mark.parametrize("defect", ["missing_county", "duplicate_county", "wrong_vintage", "wrong_geoid", "lost_metadata"])
def test_late_refusal_preserves_prior_table(tmp_path, monkeypatch, defect):
    original = [record(), record("003"), record("005")]
    target = module.build_census_acs(Reader(original), tmp_path, vintage=2024, state="10", counties=("001", "003", "005"))
    before = target.read_bytes()
    broken = deepcopy(original)
    if defect == "missing_county":
        broken.pop()
    elif defect == "duplicate_county":
        broken[-1] = record("001")
    elif defect == "wrong_vintage":
        broken[-1]["record"]["vintage"] = 2023
    elif defect == "wrong_geoid":
        broken[-1]["record"]["geo_id"] = "0500000US10003"
    else:
        broken[-1]["record"]["metadata_sources"].pop("B19013_001M")
    write_rows = module.write_rows
    monkeypatch.setattr(module, "write_rows", lambda rows, path, schema: write_rows(rows, path, schema, batch_size=1))
    with pytest.raises(ValueError):
        module.build_census_acs(Reader(broken), tmp_path, vintage=2024, state="10", counties=("001", "003", "005"))
    assert target.read_bytes() == before
    assert list(tmp_path.iterdir()) == [target]


def test_generation_publication_and_actual_local_sql_comparison(tmp_path, monkeypatch):
    reader = Reader([record(), record("003", "90000.50"), record("005", "85000")])
    artifact, evidence = build(reader, tmp_path, vintage=2024, state="10", counties=("001", "003", "005"))
    source_input, = artifact.root["inputs"]
    assert source_input["role"] == "source-evidence"
    journal = [json.loads(line) for line in (evidence / "journal.jsonl").read_text().splitlines()]
    retained, = [row for row in journal if row.get("stage") == "census-acs-source-release"]
    assert retained["source_artifact_digest"] == reader.pin.artifact_digest
    store = Store()
    index = publication.publish_generation(
        tmp_path / "generation", client=store, bucket="test", prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"]["census-acs"]["artifactDigest"] == artifact.pin.artifact_digest
    # Exercise the actual SQL consumer's local-file route, without a remote selection.
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path / "generation")
    with mcp_server._build_connection() as connection:
        rows = connection.execute(f"""
            SELECT county_code, estimate, margin_of_error, source_release_digest,
                json_extract_string(value_sources_json, '$.B19013_001E.request_url')
            FROM {module.TABLE}
            WHERE measure = 'median_household_income'
            ORDER BY estimate DESC
        """).fetchall()
    assert [row[0] for row in rows] == ["003", "005", "001"]
    assert rows[0][1:3] == (Decimal("90000.50"), Decimal("100"))
    assert rows[0][3] == reader.pin.artifact_digest
    assert rows[0][4].startswith("https://api.census.gov/data/2024/acs/acs5?")
    actual = pq.read_table(tmp_path / module.OUTPUT)
    assert actual.schema.equals(module.SCHEMA)
    row = actual.to_pylist()[1]
    assert json.loads(row["variable_metadata_json"])["B19013_001E"] == reader.records[0]["record"]["variables"]["B19013_001E"]
