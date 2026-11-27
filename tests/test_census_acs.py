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
def cd_record(state="10", cd="00", *, session="19", income="82000.00"):
    variables = [base + suffix for base in module.MEASURES for suffix in ("E", "M", "EA", "MA")]
    values = {key: None if key.endswith("A") else "100" for key in variables}
    values["B19013_001E"] = income
    return {"sourceRecordId": f"2024:{state}:{cd}", "record": {
        "dataset": "acs/acs5", "vintage": 2024, "geography": "congressional district",
        "state": state, "congressional_district": cd,
        "name": f"Congressional District {cd}, State {state}",
        "geo_id": f"500{session}00US{state}{cd}", "values": values,
        "variables": {key: {"label": key, "predicateType": "int"} for key in variables},
        "value_sources": {key: {
            "request_url": f"https://api.census.gov/data/2024/acs/acs5?for=congressional+district:*&in=state:{state}",
            "response_sha256": "sha256:" + "b" * 64, "observed_at": "2026-10-20T00:00:00Z",
            "row_index": 1, "column_index": index,
        } for index, key in enumerate(variables)},
        "metadata_sources": {key: {
            "request_url": "https://api.census.gov/data/2024/acs/acs5/variables.json",
            "response_sha256": "sha256:" + "c" * 64, "observed_at": "2026-10-20T00:00:00Z",
            "json_pointer": "/variables/" + key,
        } for key in variables},
    }}


def test_nationwide_counties_and_optional_allowlist(tmp_path):
    rec_de = record("001", "80000")
    rec_ca = record("001", "95000")
    rec_ca["sourceRecordId"] = "2024:06:001"
    rec_ca["record"].update(state="06", geo_id="0500000US06001", name="Alameda County, California")
    path = module.build_census_acs(Reader([rec_de, rec_ca]), tmp_path, vintage=2024, state="*")
    rows = pq.read_table(path).to_pylist()
    assert {(row["state_code"], row["county_geoid"]) for row in rows} == {("10", "10001"), ("06", "06001")}
    filtered = module.build_census_acs(
        Reader([rec_de, rec_ca]), tmp_path / "allowlist", vintage=2024, state="*", counties=("10001", "06001"),
    )
    assert len(pq.read_table(filtered).to_pylist()) == 4
    with pytest.raises(ValueError, match="unexpected or repeated"):
        module.build_census_acs(
            Reader([rec_de, rec_ca]), tmp_path / "missing", vintage=2024, state="*", counties=("10001",),
        )


def test_congressional_districts_nationwide_codes_and_session_guard(tmp_path, monkeypatch):
    records = [
        cd_record("10", "00", income="82000"),
        cd_record("11", "98", income="91000"),
        cd_record("06", "ZZ", income=None),
        cd_record("06", "12", income="125000.50"),
    ]
    reader = Reader(records)
    artifact, evidence = build(
        reader, tmp_path / "cd-build", vintage=2024, state="*", geography="congressional district",
    )
    assert artifact.pin.artifact_digest and (evidence / "journal.jsonl").exists()
    actual = pq.read_table(tmp_path / "cd-build" / module.CD_OUTPUT)
    assert actual.schema.equals(module.CD_SCHEMA)
    rows = [row for row in actual.to_pylist() if row["measure"] == "median_household_income"]
    assert [(r["district_geoid"], r["congress_session_code"], r["estimate"], r["estimate_status"]) for r in rows] == [
        ("1000", "19", Decimal("82000.000000"), "numeric"),
        ("1198", "19", Decimal("91000.000000"), "numeric"),
        ("06ZZ", "19", None, "null"),
        ("0612", "19", Decimal("125000.500000"), "numeric"),
    ]
    store = Store()
    index = publication.publish_generation(
        tmp_path / "cd-build" / "generation",
        client=store,
        bucket="test",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert module.CD_OUTPUT in index["families"]["census-acs"]["tables"]

    mixed = [cd_record("10", "00", session="18"), cd_record("06", "12", session="19")]
    with pytest.raises(ValueError, match="mixes Congress session GEO_ID prefixes"):
        module.build_census_acs(
            Reader(mixed), tmp_path / "mixed", vintage=2024, state="*", geography="congressional district",
        )

    with pytest.raises(ValueError, match="unexpected or repeated"):
        module.build_census_acs(
            Reader(records),
            tmp_path / "bad-allowlist",
            vintage=2024,
            state="*",
            geography="congressional district",
            districts=("1000",),
        )


def test_build_reads_evidence_from_real_source_native_release_reader(tmp_path):
    from urllib.parse import unquote_plus

    import httpx

    from spicy_docs.sources.census_acs import AcsAcquirer, AcsSelection
    from spicy_docs.sources.census_acs_profile import (
        ACS5_CONGRESSIONAL_DISTRICT_PROFILE,
        acs_scope,
        retained_page,
    )
    from spicy_docs.storage.blobs import LocalSourceNativeBlobStore
    from spicy_regs.pipelines.raw_releases import admit_release

    selection = AcsSelection(vintage=2024, state="*", geography="congressional district")
    variables = selection.variables
    groups_payloads = {
        selection.metadata_url(group): json.dumps({
            "variables": {
                key: {
                    "group": group,
                    "label": key,
                    "predicateType": "string" if key.endswith("A") else "int",
                }
                for key in variables
                if key.startswith(group + "_")
            }
        }).encode()
        for group in selection.groups
    }
    data_url = selection.data_url(selection.chunks[0])
    data_payload = json.dumps([
        ["NAME", "GEO_ID", *variables, "state", "congressional district"],
        ["Congressional District (at Large), Delaware", "5001900US1000", "1000000", "0", None, None, "82000", "1500", None, None, "10", "00"],
        ["Delegate District (at Large), District of Columbia", "5001900US1198", "670000", "0", None, None, "91000", "2000", None, None, "11", "98"],
        ["Congressional District (not defined), California", "5001900US06ZZ", "0", "0", None, None, "-666666666", "-222222222", "-", "**", "06", "ZZ"],
    ]).encode()
    responses = {unquote_plus(url): body for url, body in {**groups_payloads, data_url: data_payload}.items()}

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            stream=httpx.ByteStream(responses[unquote_plus(str(request.url.copy_remove_param("key")))]),
            headers={"content-type": "application/json"},
        )
    )
    retained = []
    with AcsAcquirer(api_key="testkey123456", transport=transport) as acq:
        captures = acq.acquire(selection, retain=retained.append)
    page = retained_page(selection, captures)
    scope = acs_scope(selection, captures)
    blobs = LocalSourceNativeBlobStore(tmp_path / "blobs")
    reader = admit_release(
        ACS5_CONGRESSIONAL_DISTRICT_PROFILE,
        [page],
        scope,
        tmp_path / "release",
        blobs=blobs,
        observed=[c.observed_at for c in captures],
    )

    artifact, evidence = build(
        reader,
        tmp_path / "build",
        vintage=2024,
        state="*",
        geography="congressional district",
    )
    assert artifact.pin.artifact_digest and (evidence / "journal.jsonl").exists()
    table = pq.read_table(tmp_path / "build" / "generation" / module.CD_OUTPUT)
    assert table.num_rows == 6
    income = {r["district_geoid"]: (r["estimate"], r["estimate_status"]) for r in table.to_pylist() if r["measure"] == "median_household_income"}
    assert income == {
        "1000": (Decimal("82000.000000"), "numeric"),
        "1198": (Decimal("91000.000000"), "numeric"),
        "06ZZ": (None, "unavailable"),
    }


def test_atomic_multi_table_county_and_congressional_district_generation(tmp_path, monkeypatch, ample_disk_space):
    from spicy_regs import local_publication
    from spicy_regs.explorer_metadata import build_bundle

    county_reader = Reader([record("001", "80000"), record("003", "90000")])
    cd_reader = Reader([cd_record("10", "00", income="82000")])

    # A late failure in congressional_district_reader preserves pre-existing county and CD tables.
    existing_dir = tmp_path / "atomic"
    county_path, cd_path = module.build_census_acs_tables(
        county_reader=county_reader,
        congressional_district_reader=cd_reader,
        output_dir=existing_dir,
        vintage=2024,
        state="10",
        counties=("001", "003"),
        districts=("00",),
    )
    county_before, cd_before = county_path.read_bytes(), cd_path.read_bytes()
    broken_cd_reader = Reader([cd_record("10", "00", session="18"), cd_record("10", "01", session="19")])
    with pytest.raises(ValueError, match="mixes Congress session GEO_ID prefixes"):
        module.build_census_acs_tables(
            county_reader=Reader([record("001", "99999"), record("003", "99999")]),
            congressional_district_reader=broken_cd_reader,
            output_dir=existing_dir,
            vintage=2024,
            state="10",
            counties=("001", "003"),
            districts=("00", "01"),
        )
    assert county_path.read_bytes() == county_before
    assert cd_path.read_bytes() == cd_before

    # Pipeline builds one atomic multi-table generation covering both county and congressional_district.
    out = tmp_path / "combined-build"
    artifact, evidence = build(
        county_reader,
        out,
        vintage=2024,
        state="10",
        counties=("001", "003"),
        congressional_district_reader=cd_reader,
        districts=("00",),
    )
    assert pq.read_schema(out / "generation" / module.OUTPUT).equals(module.SCHEMA)
    assert pq.read_schema(out / "generation" / module.CD_OUTPUT).equals(module.CD_SCHEMA)

    live = tmp_path / "live.json"
    live.write_text(json.dumps(publication.empty_index()))
    dry_out = tmp_path / "dry"
    local_publication.main([
        "--generation", str(out / "generation"),
        "--family", "census-acs",
        "--evidence", str(evidence),
        "--dry-run", "--scratch-raw",
        "--live-index", str(live),
        "--output", str(dry_out),
    ])
    receipt = json.loads((dry_out / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted"
    assert set(receipt["tables"]) == {module.OUTPUT, module.CD_OUTPUT}

    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors

    index = publication.empty_index()
    index["version"] = 2
    index["families"]["census-acs"] = publication.generation_entry(
        artifact, iter_member_descriptors(artifact, LocalMemberSource(out / "generation"))
    )
    bundle = build_bundle(index)
    for name in (module.TABLE, module.CD_TABLE):
        assert bundle["tables"][name]["metadataStatus"] == bundle["tables"][name]["sourceStatus"] == "documented"
