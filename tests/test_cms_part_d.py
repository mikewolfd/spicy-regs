"""Part D aggregation levels, exact values, atomic refusal and ordinary serving."""

from typing import Any
from copy import deepcopy
from decimal import Decimal
import hashlib
import json

import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import ArtifactPin

from spicy_regs import mcp_server
from spicy_regs.contract_types import described_schema
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.pipelines import cms_part_d as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_cms_part_d as module
from tests.generation_fakes import Store


def raw(manufacturer="Overall", **changes):
    return {
        "Brnd_Name": "Fixture drug*",
        "Gnrc_Name": "Fixture ingredient",
        "Mftr_Name": manufacturer,
        "Tot_Mftr": "1",
        "Tot_Spndng_2024": "900719925474099.01",
        "Tot_Dsg_Unts_2024": "50740",
        "Tot_Clms_2024": "378",
        "Tot_Benes_2024": "213",
        "Avg_Spnd_Per_Dsg_Unt_Wghtd_2024": "0.2059251004",
        "Avg_Spnd_Per_Clm_2024": "27.657671958",
        "Avg_Spnd_Per_Bene_2024": "49.082629108",
        "Outlier_Flag_2024": "1",
        "Tot_Spndng_2023": "historic source text",
        "UnknownFutureField": "keep me",
        **changes,
    }


class Reader:
    """Admitted-reader fixture; these synthetic observations are not live coverage."""

    def __init__(self, rows=None, *, digest="a"):
        rows = rows if rows is not None else [raw(), raw("Fixture manufacturer")]
        self.body = json.dumps(rows).encode()
        self.pin = ArtifactPin("urn:test:cms:" + digest, "sha256:" + digest * 64)
        self.ref = "sha256:" + hashlib.sha256(self.body).hexdigest()
        self.dictionary = json.dumps(
            {
                "data": {
                    "fields": [
                        {"name": name} for name in raw() if name not in {"UnknownFutureField", "Tot_Spndng_2023"}
                    ]
                }
            }
        ).encode()
        self.dictionary_ref = "sha256:" + hashlib.sha256(self.dictionary).hexdigest()
        self.selection: dict[str, Any] = {
            "series_title": "Medicare Part D Spending by Drug",
            "series_id": "urn:fixture:series",
            "dataset_id": "urn:fixture:release",
            "release_date": "2024-01-01",
            "modified": "2026-06-25",
            "api_url": "https://data.cms.gov/fixture/data",
            "dictionary_url": "https://data.cms.gov/fixture/dictionary",
            "catalog_sha256": "sha256:" + "b" * 64,
            "dictionary_sha256": self.dictionary_ref,
            "total_rows": len(rows),
        }
        locator = {
            "request_url": self.selection["api_url"] + "?offset=0&size=2",
            "response_sha256": self.ref,
            "observed_at": "2026-10-20T14:00:00Z",
        }
        self.collection_outcome: dict[str, Any] = {
            "requestedScope": {
                "selection": self.selection,
                "captures": [
                    {
                        **locator,
                        "request_url": self.selection["dictionary_url"],
                        "response_sha256": self.dictionary_ref,
                        "byte_size": len(self.dictionary),
                        "role": "dictionary",
                    },
                    {**locator, "byte_size": len(self.body), "role": "data"},
                ],
            },
            "publishedRecordCount": len(rows),
            "discoveredRecordCount": len(rows),
            "failedRecordCount": 0,
            "discardedObservationCount": 0,
        }
        self.records: list[dict] = [
            {
                "sourceRecordId": f"cms:fixture:{ordinal}",
                "record": {
                    "raw": row,
                    "selection": deepcopy(self.selection),
                    "source_locator": {**locator, "json_pointer": f"/{ordinal}", "ordinal": ordinal},
                },
            }
            for ordinal, row in enumerate(rows)
        ]

    def iter_records(self):
        yield from deepcopy(self.records)

    def read_evidence(self, ref, *, max_bytes):
        if ref == self.dictionary_ref:
            assert len(self.dictionary) <= max_bytes
            return self.dictionary
        assert ref == self.ref and len(self.body) <= max_bytes
        return self.body

    def iter_evidence(self, ref):
        yield self.dictionary if ref == self.dictionary_ref else self.body


def test_exact_values_and_alternative_aggregation_levels(tmp_path):
    reader = Reader()
    path = module.build_cms_part_d(reader, tmp_path, year=2024)
    overall, manufacturer = pq.read_table(path).to_pylist()
    assert overall["aggregation_level"] == "all_manufacturers"
    assert manufacturer["aggregation_level"] == "manufacturer"
    assert overall["total_spending_usd"] == Decimal("900719925474099.01")
    assert overall["weighted_spending_per_unit_usd"] == Decimal("0.2059251004")
    assert overall["outlier_flag_raw"] == "1" and overall["brand_name"].endswith("*")
    assert json.loads(overall["raw_row_json"]) == reader.records[0]["record"]["raw"]
    assert json.loads(overall["source_locator_json"])["json_pointer"] == "/0"
    assert overall["source_release_digest"] == reader.pin.artifact_digest
    assert len({overall["observation_id"], manufacturer["observation_id"]}) == 2
    assert pq.read_schema(path).equals(module.SCHEMA)


@pytest.mark.parametrize(
    ("raw_value", "expected", "status"),
    [
        ("0", Decimal(0), "value"),
        ("-0.012", Decimal("-0.012"), "value"),
        ("5.7405e-05", Decimal("0.000057405"), "value"),
        ("5.7405E-05", Decimal("0.000057405"), "value"),
        ("+5.7405e-05", Decimal("0.000057405"), "value"),
        ("-5.7405e-05", Decimal("-0.000057405"), "value"),
        ("1e-12", Decimal("0.000000000001"), "value"),
        ("-6.227546e-06", Decimal("-0.000006227546"), "value"),
        ("5.7405000000e-05", Decimal("0.000057405"), "value"),
        ("1e5", Decimal("100000"), "value"),
        ("1.23e+3", Decimal("1230"), "value"),
        ("0e+99", Decimal(0), "value"),
        ("-0e-15", Decimal(0), "value"),
        ("9.9999999999999999999999999999999999999e25", Decimal("99999999999999999999999999.999999999999"), "value"),
        (None, None, "null"),
        ("", None, "blank"),
        ("*", None, "non_numeric"),
        ("NaN", None, "non_numeric"),
        ("Infinity", None, "non_numeric"),
        ("-inf", None, "non_numeric"),
        ("1e", None, "non_numeric"),
        ("1e+", None, "non_numeric"),
        ("e5", None, "non_numeric"),
        (" 5.7405e-05", None, "non_numeric"),
        ("5.7405e-05 ", None, "non_numeric"),
        ("0.0000000000001", None, "unrepresentable"),
        ("1e-13", None, "unrepresentable"),
        ("8.2132926e-06", None, "unrepresentable"),
        ("1.0965515e-06", None, "unrepresentable"),
        ("1" + "0" * 26, None, "unrepresentable"),
        ("1e26", None, "unrepresentable"),
        ("-1e26", None, "unrepresentable"),
        ("1e99999999999999999999", None, "unrepresentable"),
        ("1e-99999999999999999999", None, "unrepresentable"),
        ("1" + "0" * 60 + "1e-61", None, "unrepresentable"),
    ],
)
def test_missing_markers_and_exact_decimal_limits(raw_value, expected, status):
    assert module.measurement(raw_value) == (expected, status)


def test_scientific_notation_roundtrips_through_parquet_without_rounding(tmp_path):
    rows = [
        raw(
            Tot_Spndng_2024="0e+99",
            Avg_Spnd_Per_Dsg_Unt_Wghtd_2024="5.7405e-05",
            Avg_Spnd_Per_Clm_2024="8.2132926e-06",
            Avg_Spnd_Per_Bene_2024="1.25E+2",
        ),
        raw(
            "Fixture manufacturer",
            Avg_Spnd_Per_Dsg_Unt_Wghtd_2024="-6.227546e-06",
            Avg_Spnd_Per_Clm_2024="1e26",
            Avg_Spnd_Per_Bene_2024="NaN",
        ),
    ]
    path = module.build_cms_part_d(Reader(rows), tmp_path, year=2024)
    overall, manufacturer = pq.read_table(path).to_pylist()
    assert overall["total_spending_usd"] == Decimal(0)
    assert overall["weighted_spending_per_unit_usd"] == Decimal("0.000057405")
    assert overall["spending_per_claim_usd"] is None
    assert overall["spending_per_beneficiary_usd"] == Decimal("125")
    overall_statuses = json.loads(overall["value_statuses_json"])
    assert overall_statuses["total_spending_usd"] == "value"
    assert overall_statuses["weighted_spending_per_unit_usd"] == "value"
    assert overall_statuses["spending_per_claim_usd"] == "unrepresentable"
    assert overall_statuses["spending_per_beneficiary_usd"] == "value"
    assert json.loads(overall["raw_row_json"])["Avg_Spnd_Per_Dsg_Unt_Wghtd_2024"] == "5.7405e-05"
    assert json.loads(overall["raw_row_json"])["Avg_Spnd_Per_Clm_2024"] == "8.2132926e-06"

    assert manufacturer["weighted_spending_per_unit_usd"] == Decimal("-0.000006227546")
    assert manufacturer["spending_per_claim_usd"] is None
    assert manufacturer["spending_per_beneficiary_usd"] is None
    mfg_statuses = json.loads(manufacturer["value_statuses_json"])
    assert mfg_statuses["weighted_spending_per_unit_usd"] == "value"
    assert mfg_statuses["spending_per_claim_usd"] == "unrepresentable"
    assert mfg_statuses["spending_per_beneficiary_usd"] == "non_numeric"


@pytest.mark.parametrize(
    "mutation",
    [
        lambda reader: reader.records.pop(),
        lambda reader: reader.records.append(deepcopy(reader.records[0])),
        lambda reader: reader.records[1]["record"]["raw"].update(Mftr_Name="Overall"),
        lambda reader: reader.records[0]["record"]["raw"].update(Mftr_Name="Missing overall"),
        lambda reader: reader.records[0]["record"]["raw"].pop("Tot_Spndng_2024"),
        lambda reader: reader.records[0]["record"]["selection"].update(modified="different"),
        lambda reader: reader.collection_outcome.update(failedRecordCount=1),
    ],
)
def test_incomplete_or_ambiguous_population_preserves_prior_output(tmp_path, mutation):
    destination = tmp_path / module.OUTPUT
    destination.write_bytes(b"previous output")
    reader = Reader()
    mutation(reader)
    with pytest.raises(ValueError):
        module.build_cms_part_d(reader, tmp_path, year=2024)
    assert destination.read_bytes() == b"previous output"


def test_wrong_year_or_program_refuses(tmp_path):
    with pytest.raises(ValueError, match="explicit benefit year"):
        module.build_cms_part_d(Reader(), tmp_path, year=2023)
    reader = Reader()
    reader.selection["series_title"] = "Medicaid Spending by Drug"
    with pytest.raises(ValueError, match="Part D"):
        module.build_cms_part_d(reader, tmp_path, year=2024)


def test_ordinary_generation_publication_metadata_and_comparison(tmp_path, monkeypatch):
    reader = Reader()
    output = tmp_path / "build"
    artifact, evidence = pipeline.build(reader, output, year=2024)
    index = publication.publish_generation(
        output / "generation",
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"]["cms-part-d"]["artifactDigest"] == artifact.pin.artifact_digest
    metadata = build_bundle(index)["tables"][module.TABLE]
    assert metadata["metadataStatus"] == metadata["sourceStatus"] == "documented"
    assert metadata["sources"][0]["id"] == "cms"
    assert metadata["publicationSchema"] == [list(pair) for pair in described_schema(module.SCHEMA)]
    assert metadata["recordIdentity"]["columns"] == ["observation_id"]
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        actual = connection.execute(
            f"""SELECT SUM(total_spending_usd), COUNT(*),
                       MIN(json_extract_string(source_locator_json, '$.request_url'))
                FROM {module.TABLE} WHERE aggregation_level='all_manufacturers'"""
        ).fetchone()
    assert actual == (Decimal("900719925474099.01"), 1, "https://data.cms.gov/fixture/data?offset=0&size=2")


def test_source_revision_has_distinct_observation_identity(tmp_path):
    old = module.observation(Reader().records[0], Reader().pin, year=2024)
    reader = Reader(digest="d")
    changed = module.observation(reader.records[0], reader.pin, year=2024)
    assert old["observation_id"] != changed["observation_id"]


def test_dictionary_missing_interpreted_field_prevents_generation(tmp_path):
    reader = Reader()
    dictionary = json.loads(reader.dictionary)
    dictionary["data"]["fields"] = [item for item in dictionary["data"]["fields"] if item["name"] != "Tot_Spndng_2024"]
    reader.dictionary = json.dumps(dictionary).encode()
    with pytest.raises(ValueError, match="dictionary lacks"):
        pipeline.build(reader, tmp_path / "build", year=2024)
    assert not (tmp_path / "build/generation").exists()
    assert not (tmp_path / "build" / module.OUTPUT).exists()
