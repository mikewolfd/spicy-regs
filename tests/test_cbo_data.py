"""End-to-end tests for US-CBO/cbo-data acquisition, Parquet conversion, generation replay, and MCP queries."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.cbo_data import PINNED_COMMIT, parse_catalog

from spicy_regs import data_dictionary, local_publication, mcp_server
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines import cbo_data as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms.build_cbo_data import DATASET_TO_TABLE, SCHEMAS
from tests.generation_fakes import Store
from tests.published_paths import local_paths

FIXTURES = Path(__file__).parent / "fixtures" / "cbo_data"


def _trimmed_catalog_bytes() -> bytes:
    """Restrict catalog.json distributions to only the bounded fixture CSVs present on disk."""
    raw = json.loads((FIXTURES / "catalog.json").read_bytes())
    for ds in raw["datasets"]:
        ds["distribution"] = [d for d in ds["distribution"] if (FIXTURES / d["downloadURL"]).exists()]
        ds["vintages"] = sorted({d["vintage"] for d in ds["distribution"]}, reverse=True)
    return json.dumps(raw).encode("utf-8")


def _mock_transport(trimmed_catalog: bytes) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        prefix = f"/US-CBO/cbo-data/{PINNED_COMMIT}/"
        assert request.url.path.startswith(prefix), f"unexpected path {request.url.path}"
        rel = request.url.path.removeprefix(prefix)
        if rel == "catalog.json":
            return httpx.Response(
                200, stream=httpx.ByteStream(trimmed_catalog), headers={"content-type": "application/json"}
            )
        path = FIXTURES / rel
        if not path.is_file():
            return httpx.Response(404, stream=httpx.ByteStream(b"missing"), headers={"content-type": "text/plain"})
        media = "application/json" if rel.endswith(".json") else "text/csv; charset=utf-8"
        return httpx.Response(200, stream=httpx.ByteStream(path.read_bytes()), headers={"content-type": media})

    return httpx.MockTransport(handler)


def test_acquire_build_publish_replay_and_query_all_13_datasets(tmp_path, monkeypatch):
    trimmed_catalog = _trimmed_catalog_bytes()
    catalog = parse_catalog(trimmed_catalog, commit=PINNED_COMMIT)
    expected_dist_count = sum(len(entry.distributions) for entry in catalog.datasets)
    assert len(catalog.datasets) == 13
    assert expected_dist_count >= 13

    selection = pipeline.acquire(
        tmp_path / "acquire",
        commit=PINNED_COMMIT,
        transport=_mock_transport(trimmed_catalog),
        min_request_interval_seconds=0.0,
    )
    assert selection["commit"] == PINNED_COMMIT
    assert len(selection["schemas"]) == 13
    assert len(selection["distributions"]) == expected_dist_count

    output = tmp_path / "candidate"
    artifact, evidence = pipeline.build(selection, output)
    assert verify_generation(output / "generation").pin == artifact.pin
    assert "cbo-data" in local_publication.LOCAL_FAMILIES
    assert set(local_publication._family_schemas("cbo-data") or {}) == set(SCHEMAS)

    for table_name, schema in SCHEMAS.items():
        parquet_path = output / "generation" / f"{table_name}.parquet"
        assert parquet_path.is_file()
        table = pq.read_table(parquet_path)
        assert table.schema == schema
        assert table.num_rows > 0

    snapshots = pq.read_table(output / "generation" / "cbo_data_snapshots.parquet").to_pylist()
    assert len(snapshots) == expected_dist_count
    assert {row["dataset"] for row in snapshots} == set(DATASET_TO_TABLE)

    # Verify dictionary, publication registration, and explorer metadata
    store = Store()
    index = publication.publish_generation(
        output / "generation",
        client=store,
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    metadata = build_bundle(index)
    expected = data_dictionary.expected_schemas()
    for name in SCHEMAS:
        assert name in expected
        assert metadata["tables"][name]["sourceStatus"] == "documented"
        assert metadata["tables"][name]["metadataStatus"] == "documented"

    # Verify DuckDB / MCP queries across fiscal, economic, revenue_detail, spending_detail, and demographic tables
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as con:
        tf_rows = con.execute(
            "SELECT publication_id, vintage, unit, count(*) FROM cbo_trust_fund GROUP BY 1, 2, 3"
        ).fetchall()
        assert tf_rows and tf_rows[0][0] == "51136" and tf_rows[0][1] == "2026-02"

        rev_iit = con.execute(
            "SELECT section, variable_note FROM cbo_revenue_detail WHERE section IS NOT NULL LIMIT 1"
        ).fetchall()
        assert rev_iit and rev_iit[0][0] == "Addendum"

        sp_rows = con.execute(
            "SELECT tin, date, disc_or_mand, category, unit FROM cbo_spending_detail WHERE tin = '166-8239-0-7-151' AND date = 'FY2026'"
        ).fetchall()
        assert len(sp_rows) == 2
        assert {r[3] for r in sp_rows} == {"Offsetting receipts", "Other Spending"}
        assert sp_rows[0][4] == "Millions of dollars"

        dem_domains = {
            r[0]
            for r in con.execute("SELECT DISTINCT domain FROM cbo_demographic").fetchall()
        }
        assert dem_domains == {
            "fertility",
            "migration",
            "mortality",
            "population_bls",
            "population_census",
            "ss_area_population",
        }
        mig_row = con.execute(
            "SELECT count(*) FROM cbo_demographic WHERE domain='migration' AND vintage='2026-01' AND value IS NOT NULL"
        ).fetchone()
        assert mig_row is not None and mig_row[0] > 0

        lte_lfp = con.execute(
            "SELECT sex, age_group, unit FROM cbo_long_term_economic WHERE file_type='lfp_rates' LIMIT 1"
        ).fetchall()
        assert lte_lfp and lte_lfp[0][2] == "Percent"

    # Verify byte-identical replay and absence of local filesystem leaks
    replay = tmp_path / "replay"
    mirror = tmp_path / "mirror"
    pinned_index = mirror / "publication.v2/index.json"
    pinned_index.parent.mkdir(parents=True)
    pinned_index.write_text(json.dumps(index))
    prior = mirror / index["families"][pipeline.FAMILY]["prefix"] / "artifact.json"
    prior.parent.mkdir(parents=True)
    prior.write_bytes((output / "generation/artifact.json").read_bytes())

    _, replay_evidence = pipeline.build(selection, replay, read_index=pinned_index)
    replay_root = json.loads((replay / "generation/artifact.json").read_bytes())
    assert any(item["role"] == "prior-generation" for item in replay_root["inputs"])
    for name in SCHEMAS:
        assert (output / "generation" / f"{name}.parquet").read_bytes() == (
            replay / "generation" / f"{name}.parquet"
        ).read_bytes()
    assert local_paths(output / "generation", evidence, also=(str(tmp_path),)) == []
    publication.publish_generation(
        replay / "generation",
        client=store,
        bucket="fixture",
        prior_index=index,
        evidence_directories=(replay_evidence,),
    )


def test_wrong_pin_refuses_without_a_generation(tmp_path):
    trimmed_catalog = _trimmed_catalog_bytes()
    selection = pipeline.acquire(
        tmp_path / "acquire",
        commit=PINNED_COMMIT,
        transport=_mock_transport(trimmed_catalog),
        min_request_interval_seconds=0.0,
    )
    selection["catalog"]["byte_size"] += 1
    output = tmp_path / "refused"
    with pytest.raises(ValueError, match="differs from pinned size/digest"):
        pipeline.build(selection, output)
    assert not (output / "generation").exists()
    assert not list(output.glob("*.parquet"))
