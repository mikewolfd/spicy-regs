"""Dated complete Senate summary through ordinary tables, queries and failure controls."""

import json
from copy import deepcopy
from pathlib import Path

import httpx
import pytest
from spicy_docs.sources.historical_vetoes import SOURCE_URL, read_veto_table
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs import data_dictionary, mcp_server
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines import historical_vetoes as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms.build_historical_vetoes import SCHEMAS, interpret_table
from tests.generation_fakes import Store
from tests.published_paths import local_paths

FIXTURES = Path(__file__).parent / "fixtures/historical_vetoes"
BODY = (FIXTURES / "vetoCounts-20261113.html").read_bytes()


def table():
    selection = json.loads((FIXTURES / "provenance.json").read_text())
    return read_veto_table(BODY, source_url=SOURCE_URL, observed_at=selection["observed_at"])


def test_complete_original_categories_nonconsecutive_spans_and_zero_rows():
    rows, snapshots = interpret_table(table())
    summary = snapshots[0]
    assert len(rows) == summary["administration_rows"] == 47
    assert [summary[k] for k in ("regular_vetoes", "pocket_vetoes", "total_vetoes", "overridden_vetoes")] == [
        1533,
        1066,
        2599,
        112,
    ]
    assert len([r for r in rows if r["president"] == "Grover Cleveland"]) == 2
    assert len([r for r in rows if r["president"] == "Donald J. Trump"]) == 2
    garfield = next(r for r in rows if r["president"] == "James A. Garfield")
    assert garfield["total_vetoes"] == 0 and garfield["years_raw"] == "1881"
    truman = next(r for r in rows if r["president"] == "Harry S. Truman")
    assert truman["years_raw"] == "1945-53"
    assert "numbered presidential vetoes" in summary["footnotes_json"]


@pytest.mark.parametrize("fault", ["blank", "row_total", "overridden", "dropped_row", "duplicate", "footer", "marker"])
def test_bad_or_missing_cells_never_produce_qualified_counts(fault):
    source = deepcopy(table())
    if fault == "blank":
        source["rows"][0]["cells"][2]["text"] = ""
    elif fault == "row_total":
        source["rows"][0]["cells"][4]["text"] = "3"
    elif fault == "overridden":
        source["rows"][0]["cells"][5]["text"] = "3"
    elif fault == "dropped_row":
        source["rows"].pop(0)
    elif fault == "duplicate":
        source["rows"].append(source["rows"][0])
    elif fault == "footer":
        source["total"]["cells"][5]["text"] = "113"
    else:
        source["rows"][0]["cells"][0]["text"] += "99"
    with pytest.raises(ValueError):
        interpret_table(source)


def test_capture_build_publication_registration_query_and_exact_replay(tmp_path, monkeypatch):
    requested = []

    def fetch(request):
        requested.append(str(request.url))
        return httpx.Response(200, content=BODY, headers={"Content-Type": "text/html"})

    selection = pipeline.acquire(tmp_path / "acquire", transport=httpx.MockTransport(fetch))
    assert requested == [SOURCE_URL]
    output = tmp_path / "candidate"
    artifact, evidence = pipeline.build(selection, output)
    assert verify_generation(output / "generation").pin == artifact.pin
    store = Store()
    index = publication.publish_generation(
        output / "generation",
        client=store,
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    metadata = build_bundle(index)
    for name in SCHEMAS:
        assert name in data_dictionary.expected_schemas()
        assert metadata["tables"][name]["sourceStatus"] == "documented"
        assert metadata["tables"][name]["metadataStatus"] == "documented"
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        actual = connection.execute("""
            SELECT president, years_raw, regular_vetoes, pocket_vetoes, total_vetoes, overridden_vetoes,
                   source_url, json_extract_string(source_locator_json, '$.locator')
            FROM historical_veto_administrations WHERE president='Grover Cleveland' ORDER BY years_raw
        """).fetchall()
        assert [(r[1], *r[2:6]) for r in actual] == [("1885-1889", 304, 110, 414, 2), ("1893-1897", 42, 128, 170, 5)]
        assert all(r[6] == SOURCE_URL and "/tbody[1]/tr[" in r[7] for r in actual)
        assert (
            connection.execute(
                "SELECT total_vetoes FROM historical_veto_administrations WHERE president='Unknown'"
            ).fetchall()
            == []
        )
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
        assert (output / "generation" / (name + ".parquet")).read_bytes() == (
            replay / "generation" / (name + ".parquet")
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
    selection = pipeline.acquire(
        tmp_path / "acquire",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, content=BODY, headers={"Content-Type": "text/html"})
        ),
    )
    selection["byte_size"] += 1
    output = tmp_path / "refused"
    with pytest.raises(ValueError, match="selected bytes/digest"):
        pipeline.build(selection, output)
    assert not (output / "generation").exists()
    assert not list(output.glob("*.parquet"))
    assert list(output.glob("source-evidence/*/artifact/journal.jsonl"))


@pytest.mark.parametrize(("status", "body"), [(403, b"refused"), (200, b"<html><h1>Continue</h1></html>"), (200, b"")])
def test_refusal_challenge_and_empty_response_leave_failed_evidence(tmp_path, status, body):
    with pytest.raises((ValueError, CredentialRefusedError)):
        pipeline.acquire(
            tmp_path / "failed",
            transport=httpx.MockTransport(
                lambda _: httpx.Response(status, content=body, headers={"Content-Type": "text/html"})
            ),
        )
    assert not (tmp_path / "failed/generation").exists()
    outcomes = list((tmp_path / "failed").glob("source-evidence/*/run-outcome.json"))
    assert outcomes and json.loads(outcomes[0].read_bytes())["outcome"] == "failed"
