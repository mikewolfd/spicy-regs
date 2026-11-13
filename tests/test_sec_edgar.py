"""SEC native observation delivery and ordinary consumer failure controls."""

from copy import deepcopy
import hashlib
import json
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs import mcp_server
from spicy_regs.pipelines.sec_edgar import acquire, build
from spicy_regs.transforms.build_sec_edgar import FILINGS, SCHEMAS, select_fact

FIXTURES = Path(__file__).parent / "fixtures/sec_edgar"
CONCEPTS = [("us-gaap", "Assets"), ("us-gaap", "EarningsPerShareDiluted"), ("us-gaap", "MissingConcept")]


def sources(tmp_path, monkeypatch, *, mutate=None):
    raw = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw))
    store = CentralRawArchiveStore(raw)
    selection = {
        "format": "spicy-sec-edgar-captures",
        "version": 1,
        "filing_start": "2023-01-01",
        "filing_end": "2024-12-31",
        "sources": [],
    }
    for kind in ("submissions", "companyfacts"):
        value = json.loads((FIXTURES / (kind + ".json")).read_bytes())
        if mutate:
            mutate(kind, value)
        body = json.dumps(value).encode()
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        store.put_blob(digest, len(body), [body])
        path = "submissions/" if kind == "submissions" else "api/xbrl/companyfacts/"
        selection["sources"].append(
            {
                "cik": "0000320193",
                "kind": kind,
                "continuation": None,
                "sha256": digest,
                "bytes": len(body),
                "url": f"https://data.sec.gov/{path}CIK0000320193.json",
                "observed_at": "2026-11-13T12:00:00Z",
            }
        )
    return raw, selection


def connection(directory):
    con = duckdb.connect()
    for name in SCHEMAS:
        con.read_parquet(str(directory / (name + ".parquet"))).create_view(name)
    return con


def test_generation_and_ordinary_company_period_query(tmp_path, monkeypatch):
    raw, selection = sources(tmp_path, monkeypatch)
    output = tmp_path / "build"
    artifact, evidence = build(
        selection, CONCEPTS, output, fact_start="2023-01-01", fact_end="2024-12-31", archive_root=raw
    )
    assert artifact.pin.artifact_digest and (evidence / "artifact.json").exists()
    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors
    from spicy_regs.explorer_metadata import build_bundle
    from spicy_regs.local_data import selection_record
    from spicy_regs.sources.publication import empty_index, generation_entry

    index = empty_index()
    index["version"] = 2
    index["families"]["sec-edgar"] = generation_entry(
        artifact, iter_member_descriptors(artifact, LocalMemberSource(output / "generation"))
    )
    bundle = build_bundle(index)
    for name in SCHEMAS:
        assert bundle["tables"][name]["metadataStatus"] == bundle["tables"][name]["sourceStatus"] == "documented"
    query = output / "local-query"
    shutil.copytree(output / "generation", query)
    (query / "download.json").write_text(
        json.dumps(
            {
                "version": 1,
                "status": "complete",
                "publication": index,
                "selected": {name: selection_record(index, name) for name in SCHEMAS},
            }
        )
    )
    monkeypatch.setattr(mcp_server, "DATA_DIR", query)
    with mcp_server._build_connection() as con:
        result = select_fact(
            con,
            cik="320193",
            taxonomy="us-gaap",
            concept="Assets",
            unit="USD",
            end="2024-09-28",
            accession="0000320193-24-000123",
        )
        assert result["outcome"] == "selected"
        assert result["observations"][0]["value_text"] == "364980000000"
        assert (
            select_fact(con, cik="320193", taxonomy="us-gaap", concept="Assets", unit="USD", end="2024-09-28")[
                "outcome"
            ]
            == "competing_observations"
        )
        assert (
            select_fact(con, cik="320193", taxonomy="us-gaap", concept="MissingConcept", unit="USD", end="2024-09-28")[
                "outcome"
            ]
            == "concept_not_reported"
        )
        assert (
            select_fact(con, cik="789019", taxonomy="us-gaap", concept="Assets", unit="USD", end="2024-09-28")[
                "outcome"
            ]
            == "not_selected"
        )
        assert con.execute(f"select count(*) from {FILINGS} where form='10-K'").fetchone() == (2,)
    from tests.published_paths import local_paths

    assert local_paths(output / "generation", evidence) == []
    with (query / "sec_reported_facts.parquet").open("ab") as stream:
        stream.write(b"changed native member")
    with pytest.raises(RuntimeError, match="generation pin"):
        mcp_server._build_connection()


def test_changed_units_nulls_and_amendments_are_competing_native_observations(tmp_path, monkeypatch):
    def change(kind, value):
        if kind != "companyfacts":
            return
        units = value["facts"]["us-gaap"]["Assets"]["units"]
        row = deepcopy(next(r for r in units["USD"] if r["end"] == "2024-09-28"))
        row.update(accn="0000320193-24-999999", form="10-K/A", val=None, start=None)
        units["USD"].append(row)
        units["CAD"] = [{**row, "val": 999}]
        eps = value["facts"]["us-gaap"]["EarningsPerShareDiluted"]["units"]["USD/shares"]
        annual = next(r for r in eps if r.get("start") == "2023-10-01" and r["accn"] == "0000320193-24-000123")
        eps.append({**annual, "start": "2024-06-30", "val": 1.23})

    raw, selection = sources(tmp_path, monkeypatch, mutate=change)
    output = tmp_path / "build"
    build(selection, CONCEPTS, output, fact_start="2023-01-01", fact_end="2024-12-31", archive_root=raw)
    with connection(output / "generation") as con:
        args = dict(
            cik="320193", taxonomy="us-gaap", concept="Assets", end="2024-09-28", accession="0000320193-24-999999"
        )
        result = select_fact(con, unit="USD", **args)
        assert result["outcome"] == "reported_null"
        assert result["observations"][0]["is_amendment"] is True
        assert result["observations"][0]["period_kind"] == "unknown"
        assert select_fact(con, unit="CAD", **args)["observations"][0]["value_text"] == "999"
        assert select_fact(con, unit="EUR", **args)["outcome"] == "missing"
        duration = dict(
            cik="320193",
            taxonomy="us-gaap",
            concept="EarningsPerShareDiluted",
            unit="USD/shares",
            end="2024-09-28",
            accession="0000320193-24-000123",
        )
        assert select_fact(con, start="2023-10-01", **duration)["observations"][0]["value_text"] == "6.08"
        assert select_fact(con, start="2024-06-30", **duration)["observations"][0]["value_text"] == "1.23"
        assert select_fact(con, **duration)["outcome"] == "missing"


@pytest.mark.parametrize("failure", ["missing_continuation", "duplicate", "wrong_digest", "wrong_size"])
def test_incomplete_or_changed_original_refuses_without_generation(tmp_path, monkeypatch, failure):
    def change(kind, value):
        if failure == "missing_continuation" and kind == "submissions":
            value["filings"]["files"] = [
                {
                    "name": "CIK0000320193-submissions-001.json",
                    "filingCount": 1,
                    "filingFrom": "2023-01-01",
                    "filingTo": "2023-12-31",
                }
            ]

    raw, selection = sources(tmp_path, monkeypatch, mutate=change)
    if failure == "duplicate":
        selection["sources"].append(selection["sources"][0])
    if failure == "wrong_digest":
        selection["sources"][0]["sha256"] = selection["sources"][1]["sha256"]
    if failure == "wrong_size":
        selection["sources"][0]["bytes"] += 1
    output = tmp_path / "build"
    with pytest.raises(ValueError):
        build(selection, CONCEPTS, output, fact_start="2023-01-01", fact_end="2024-12-31", archive_root=raw)
    assert not (output / "generation").exists()
    assert not list(output.glob("candidate/*.parquet"))


def test_blank_report_date_and_schema_are_retained(tmp_path, monkeypatch):
    def change(kind, value):
        if kind == "submissions":
            value["filings"]["recent"]["filingDate"][2] = "2024-01-01"

    raw, selection = sources(tmp_path, monkeypatch, mutate=change)
    output = tmp_path / "build"
    build(selection, CONCEPTS, output, fact_start="2023-01-01", fact_end="2024-12-31", archive_root=raw)
    assert all(
        pq.read_schema(output / "generation" / (name + ".parquet")).equals(schema) for name, schema in SCHEMAS.items()
    )
    assert any(
        row["report_date_raw"] == ""
        for row in pq.read_table(output / "generation" / (FILINGS + ".parquet")).to_pylist()
    )


def test_complete_continuation_and_empty_fact_scope(tmp_path, monkeypatch):
    raw, selection = sources(tmp_path, monkeypatch)
    store = CentralRawArchiveStore(raw)
    root = json.loads((FIXTURES / "submissions.json").read_text())
    columns = root["filings"]["recent"]
    name = "CIK0000320193-submissions-001.json"
    root["filings"] = {
        "recent": {key: [] for key in columns},
        "files": [
            {
                "name": name,
                "filingCount": len(columns["form"]),
                "filingFrom": min(columns["filingDate"]),
                "filingTo": max(columns["filingDate"]),
            }
        ],
    }
    for value, descriptor in [
        (root, selection["sources"][0]),
        (columns, {**selection["sources"][0], "continuation": name, "url": "https://data.sec.gov/submissions/" + name}),
    ]:
        body = json.dumps(value).encode()
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        store.put_blob(digest, len(body), [body])
        descriptor.update(sha256=digest, bytes=len(body))
        if descriptor["continuation"]:
            selection["sources"].append(descriptor)
    output = tmp_path / "build"
    build(selection, CONCEPTS, output, fact_start="2000-01-01", fact_end="2000-12-31", archive_root=raw)
    with connection(output / "generation") as con:
        assert con.execute("select count(*) from sec_filings").fetchone()[0] == 2
        assert con.execute("select count(*) from sec_reported_facts").fetchone()[0] == 0
        assert (
            select_fact(con, cik="320193", taxonomy="us-gaap", concept="Assets", unit="USD", end="2000-01-01")[
                "outcome"
            ]
            == "no_observations_in_period"
        )


def test_acquisition_reuses_exact_originals_without_network(tmp_path, monkeypatch):
    import httpx

    _raw, selection = sources(tmp_path, monkeypatch)

    def unexpected(request):
        raise AssertionError("A retained exact original must not be downloaded again")

    actual = acquire(
        ["320193"],
        "2023-01-01",
        "2024-12-31",
        tmp_path / "acquire",
        contact_email="ops@example.org",
        transport=httpx.MockTransport(unexpected),
        reuse=selection["sources"],
    )
    assert actual == selection
