"""Medicaid semantics and the shared CMS publication path remain distinct from Part D."""

from copy import deepcopy
from decimal import Decimal
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.contract_types import described_schema
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.pipelines import cms_medicaid as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_cms_medicaid as module
from spicy_regs.transforms import build_cms_part_d as part_d
from tests.generation_fakes import Store
from tests.test_cms_part_d import Reader as PartDReader, raw as part_d_raw

EXCERPTS = json.loads((Path(__file__).parent / "fixtures/cms_medicaid/selected-2024-excerpts.json").read_text())


class Reader(PartDReader):
    """Reuse the admitted-reader test boundary, with Medicaid's actual program/fields."""

    def __init__(self, rows=None):
        super().__init__(deepcopy(rows if rows is not None else [entry["raw"] for entry in EXCERPTS["first_drug"]]))
        self.selection["series_title"] = "Medicaid Spending by Drug"
        for record in self.records:
            record["record"]["selection"] = deepcopy(self.selection)


def test_actual_program_measures_and_overall_reconciliation(tmp_path):
    reader = Reader()
    path = module.build_cms_medicaid(reader, tmp_path, year=2024)
    rows = pq.read_table(path).to_pylist()
    overall, first, second = rows
    assert {row["program"] for row in rows} == {"Medicaid"}
    assert overall["total_spending_usd"] == Decimal("5918.97")
    assert first["total_spending_usd"] == Decimal("2686.17")
    assert second["total_spending_usd"] == Decimal("3232.8")
    for measure in ("total_spending_usd", "claims", "dosage_units"):
        assert overall[measure] == first[measure] + second[measure]
    assert overall["weighted_spending_per_unit_usd"] == Decimal("0.4664236761")
    assert overall["weighted_spending_per_unit_usd"] != overall["total_spending_usd"] / overall["dosage_units"]
    assert [row["aggregation_level"] for row in rows] == ["all_manufacturers", "manufacturer", "manufacturer"]
    assert pq.read_schema(path).equals(module.SCHEMA)
    assert "beneficiaries" not in module.SCHEMA.names and "spending_per_beneficiary_usd" not in module.SCHEMA.names
    for row, original in zip(rows, reader.records, strict=True):
        assert json.loads(row["raw_row_json"]) == original["record"]["raw"]
        assert row["source_release_digest"] == reader.pin.artifact_digest


def test_actual_older_year_blank_is_not_zero_or_a_current_year_missing_value():
    case = EXCERPTS["older_year_blank"]
    reader = Reader([case["raw"]])
    item = reader.records[0]
    item["sourceRecordId"] = case["source_record_id"]
    item["record"]["source_locator"] = case["source_locator"]
    row = module.observation(item, reader.pin, year=2024)
    assert row["brand_name"] == "Acetaminophen ER"
    assert row["total_spending_usd"] == Decimal("1024035.27")
    assert json.loads(row["raw_row_json"])["Tot_Spndng_2020"] == ""
    assert json.loads(row["source_locator_json"])["json_pointer"] == "/100"
    assert set(json.loads(row["value_statuses_json"]).values()) == {"value"}


def test_unknown_cells_and_brand_markers_survive_without_row_filtering(tmp_path):
    raw = part_d_raw(Brnd_Name="Fixture*^", Tot_Spndng_2024=None, Tot_Dsg_Unts_2024="", Tot_Clms_2024="suppressed?")
    reader = Reader([raw])
    row = pq.read_table(module.build_cms_medicaid(reader, tmp_path, year=2024)).to_pylist()[0]
    assert row["brand_name"] == "Fixture*^" and row["outlier_flag_raw"] == "1"
    assert row["total_spending_usd"] is row["dosage_units"] is row["claims"] is None
    statuses = json.loads(row["value_statuses_json"])
    assert statuses["total_spending_usd"] == "null"
    assert statuses["dosage_units"] == "blank" and statuses["claims"] == "non_numeric"
    assert json.loads(row["raw_row_json"]) == raw


@pytest.mark.parametrize("reader,year", [(PartDReader(), 2024), (Reader(), 2023)])
def test_wrong_program_or_year_refuses(tmp_path, reader, year):
    with pytest.raises(ValueError, match="Select the Medicaid release"):
        module.build_cms_medicaid(reader, tmp_path, year=year)
    assert not (tmp_path / module.OUTPUT).exists()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda reader: reader.records.pop(),
        lambda reader: reader.records[2]["record"]["raw"].update(Mftr_Name="Overall"),
        lambda reader: reader.records[0]["record"]["raw"].update(Mftr_Name="No Overall"),
    ],
)
def test_late_population_failure_preserves_prior_table(tmp_path, mutation):
    destination = tmp_path / module.OUTPUT
    destination.write_bytes(b"previous")
    reader = Reader()
    mutation(reader)
    with pytest.raises(ValueError):
        module.build_cms_medicaid(reader, tmp_path, year=2024)
    assert destination.read_bytes() == b"previous"


def test_dictionary_missing_selected_measure_refuses_before_table(tmp_path):
    reader = Reader()
    dictionary = json.loads(reader.dictionary)
    dictionary["data"]["fields"] = [field for field in dictionary["data"]["fields"] if field["name"] != "Tot_Clms_2024"]
    reader.dictionary = json.dumps(dictionary).encode()
    with pytest.raises(ValueError, match="Medicaid dictionary lacks"):
        pipeline.build(reader, tmp_path, year=2024)
    assert not (tmp_path / module.OUTPUT).exists()
    assert not (tmp_path / "generation").exists()


def test_ordinary_generation_metadata_and_program_labelled_query(tmp_path, monkeypatch):
    artifact, evidence = pipeline.build(Reader(), tmp_path, year=2024)
    index = publication.publish_generation(
        tmp_path / "generation",
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"]["cms-medicaid"]["artifactDigest"] == artifact.pin.artifact_digest
    metadata = build_bundle(index)["tables"][module.TABLE]
    assert metadata["metadataStatus"] == metadata["sourceStatus"] == "documented"
    assert metadata["recordIdentity"]["columns"] == ["observation_id"]
    assert metadata["sources"][0]["id"] == "cms"
    assert metadata["publicationSchema"] == [list(pair) for pair in described_schema(module.SCHEMA)]
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path / "generation")
    with mcp_server._build_connection() as connection:
        result = connection.execute(
            f"SELECT program, benefit_year, total_spending_usd FROM {module.TABLE} WHERE aggregation_level='all_manufacturers'"
        ).fetchall()
    assert result == [("Medicaid", 2024, Decimal("5918.97"))]


def test_part_d_schema_and_program_boundary_remain_distinct():
    assert "program" not in part_d.SCHEMA.names
    assert "beneficiaries" in part_d.SCHEMA.names
    with pytest.raises(ValueError, match="Select the Part D release"):
        list(part_d.observations(Reader(), year=2024))
