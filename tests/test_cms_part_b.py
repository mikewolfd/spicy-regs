"""Real Part B field shapes, precise values and ordinary publication boundaries."""

from copy import deepcopy
from decimal import Decimal
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.pipelines import cms_part_b as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_cms_part_b as module
from tests.generation_fakes import Store
from tests.test_cms_part_d import Reader as BaseReader

EXCERPTS = json.loads((Path(__file__).parent / "fixtures/cms_part_b/selected-2024-excerpts.json").read_text())


class Reader(BaseReader):
    def __init__(self, rows=None, *, digest="a"):
        super().__init__(deepcopy(EXCERPTS["rows"] if rows is None else rows), digest=digest)
        self.dictionary = json.dumps({"data": {"fields": [{"name": name} for name in EXCERPTS["rows"][0]]}}).encode()
        self.dictionary_ref = "sha256:" + hashlib.sha256(self.dictionary).hexdigest()
        self.selection.update(
            series_title="Medicare Part B Spending by Drug",
            api_url="https://data.cms.gov/data-api/v1/dataset/f48ced88-541a-4c51-947b-149197e5eaf3/data",
            dictionary_sha256=self.dictionary_ref,
        )
        self.collection_outcome["requestedScope"]["captures"][0].update(
            response_sha256=self.dictionary_ref, byte_size=len(self.dictionary)
        )
        for record in self.records:
            record["record"]["selection"] = deepcopy(self.selection)


def test_actual_hcpcs_values_source_markers_and_ordinary_query(tmp_path, monkeypatch):
    reader = Reader()
    artifact, evidence = pipeline.build(reader, tmp_path, year=2024)
    index = publication.publish_generation(
        tmp_path / "generation",
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    assert index["families"]["cms-part-b"]["artifactDigest"] == artifact.pin.artifact_digest
    metadata = build_bundle(index)["tables"][module.TABLE]
    assert metadata["metadataStatus"] == metadata["sourceStatus"] == "documented"
    assert metadata["recordIdentity"]["columns"] == ["observation_id"]
    path = tmp_path / "generation" / module.OUTPUT
    rows = pq.read_table(path).to_pylist()
    assert pq.read_schema(path).equals(module.SCHEMA)
    first, second = rows
    assert first["hcpcs_code"] == "90371" and first["brand_name"] == "HyperHEP B*"
    assert first["total_spending_usd"] == Decimal("53811.85")
    assert first["dosage_units"] == 389 and first["claims"] == 93 and first["beneficiaries"] == 52
    assert first["spending_per_dosage_unit_usd"] == Decimal("138.3338046")
    assert first["average_sales_price_payment_usd"] == Decimal("140.67725")
    assert first["spending_per_dosage_unit_usd"] != first["total_spending_usd"] / first["dosage_units"]
    assert first["manufacturer_information_status"] == "not_provided_in_main_table"
    assert first["source_resources_url"].endswith("/dataset-resources/f48ced88-541a-4c51-947b-149197e5eaf3")
    assert "manufacturer_name" not in first
    assert [json.loads(row["raw_row_json"]) for row in rows] == EXCERPTS["rows"]
    assert second["source_release_digest"] == reader.pin.artifact_digest
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path / "generation")
    with mcp_server._build_connection() as connection:
        actual = connection.execute(
            f"SELECT hcpcs_code, benefit_year, total_spending_usd FROM {module.TABLE} ORDER BY hcpcs_code"
        ).fetchall()
    assert actual == [("90371", 2024, Decimal("53811.85")), ("90375", 2024, Decimal("10531197.21"))]


def test_null_blank_unknown_outlier_and_revision_remain_explicit(tmp_path):
    raw = deepcopy(EXCERPTS["rows"][0])
    raw.update(
        Tot_Spndng_2024=None,
        Tot_Dsg_Unts_2024="",
        Tot_Clms_2024="*",
        Outlier_Flag_2024="1",
        UnknownFutureField="retained",
    )
    reader = Reader([raw])
    first = pq.read_table(module.build_cms_part_b(reader, tmp_path, year=2024)).to_pylist()[0]
    assert first["total_spending_usd"] is first["dosage_units"] is first["claims"] is None
    statuses = json.loads(first["value_statuses_json"])
    assert [statuses[name] for name in ("total_spending_usd", "dosage_units", "claims")] == [
        "null",
        "blank",
        "non_numeric",
    ]
    assert first["outlier_flag_raw"] == "1" and json.loads(first["raw_row_json"]) == raw
    revised = Reader([raw], digest="d")
    assert module.observation(revised.records[0], revised.pin, year=2024)["observation_id"] != first["observation_id"]


@pytest.mark.parametrize("change", ["duplicate", "short", "year", "program", "missing-column", "selection", "failed"])
def test_changed_or_incomplete_inputs_preserve_previous_output(tmp_path, change):
    reader = Reader()
    year = 2024
    if change == "duplicate":
        reader.records[1]["record"]["raw"]["HCPCS_Cd"] = reader.records[0]["record"]["raw"]["HCPCS_Cd"]
    elif change == "short":
        reader.records.pop()
    elif change == "year":
        year = 2023
    elif change == "program":
        reader.selection["series_title"] = "Medicare Part D Spending by Drug"
    elif change == "missing-column":
        reader.records[1]["record"]["raw"].pop("Tot_Clms_2024")
    elif change == "selection":
        reader.records[1]["record"]["selection"]["modified"] = "changed"
    else:
        reader.collection_outcome["failedRecordCount"] = 1
    path = tmp_path / module.OUTPUT
    path.write_bytes(b"prior")
    with pytest.raises(ValueError):
        module.build_cms_part_b(reader, tmp_path, year=year)
    assert path.read_bytes() == b"prior"


def test_changed_dictionary_refuses_before_table_or_generation(tmp_path):
    reader = Reader()
    dictionary = json.loads(reader.dictionary)
    dictionary["data"]["fields"] = [f for f in dictionary["data"]["fields"] if f["name"] != "HCPCS_Cd"]
    reader.dictionary = json.dumps(dictionary).encode()
    with pytest.raises(ValueError, match="dictionary lacks"):
        pipeline.build(reader, tmp_path, year=2024)
    assert not (tmp_path / module.OUTPUT).exists() and not (tmp_path / "generation").exists()
