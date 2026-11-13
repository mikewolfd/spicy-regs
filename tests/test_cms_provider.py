"""Provider measures, exact missing states, ordinary query and atomic refusal."""

import csv
import hashlib
import io
import json
from copy import deepcopy
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs import mcp_server
from spicy_regs.pipelines import cms_provider as pipeline
from spicy_regs.transforms import build_cms_provider as module

F = json.loads((Path(__file__).parent / "fixtures/cms_provider/selected-2024-excerpts.json").read_text())


def inputs(rows=None):
    from spicy_docs.sources.cms import CATALOG_URL, PROVIDER_SERIES, resolve_release
    from spicy_docs.sources.cms_provider import provider_scope
    from spicy_docs.transport.captured import CapturedBodyResponse

    rows = deepcopy(F["rows"] if rows is None else rows)
    selected = resolve_release(F["catalog"], series_title=PROVIDER_SERIES)

    def capture(url, data):
        return CapturedBodyResponse(
            url, url, 200, "application/json", "2026-11-13T12:00:00+00:00", json.dumps(data).encode()
        )

    controls = [
        capture(CATALOG_URL, F["catalog"]),
        capture(selected["dictionary_url"], F["dictionary"]),
        capture(selected["api_url"] + "/stats", {"found_rows": len(rows), "total_rows": len(rows)}),
    ]
    buffer = io.StringIO(newline="")
    w = csv.DictWriter(buffer, fieldnames=rows[0])
    w.writeheader()
    w.writerows(rows)
    body = buffer.getvalue().encode()
    url = next(d["downloadURL"] for d in F["catalog"]["dataset"][0]["distribution"] if d["format"] == "CSV")
    scope = provider_scope(
        *controls,
        {
            "request_url": url,
            "response_sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
            "byte_size": len(body),
            "observed_at": "2026-11-13T12:00:00+00:00",
        },
    )
    return scope, controls, body


def test_pipeline_and_ordinary_query_preserve_source(tmp_path, monkeypatch):
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    scope, controls, body = inputs()
    original = tmp_path / "original.csv"
    original.write_bytes(body)
    artifact, evidence = pipeline.build(scope, controls, original, tmp_path / "result", year=2024)
    assert artifact.pin.artifact_digest and evidence.exists()
    from spicy_regs.explorer_metadata import build_bundle
    from spicy_regs.sources import publication
    from tests.generation_fakes import Store

    index = publication.publish_generation(
        tmp_path / "result" / "generation",
        client=Store(),
        bucket="fixture",
        prior_index=publication.empty_index(),
        evidence_directories=(evidence,),
    )
    metadata = build_bundle(index)["tables"][module.TABLE]
    assert metadata["metadataStatus"] == metadata["sourceStatus"] == "documented"
    assert metadata["recordIdentity"]["columns"] == ["observation_id"]
    rows = pq.read_table(tmp_path / "result" / "generation" / module.OUTPUT).to_pylist()
    assert [json.loads(r["raw_row_json"]) for r in rows] == F["rows"]
    assert rows[0]["medicare_payment_usd"] == Decimal("35325.46")
    assert rows[0]["beneficiaries"] == 328 and rows[0]["services"] == 399
    monkeypatch.setattr(mcp_server, "DATA_DIR", tmp_path / "result" / "generation")
    with mcp_server._build_connection() as c:
        result = c.execute(
            "SELECT npi, specialty, state, medicare_payment_usd FROM cms_medicare_provider ORDER BY npi LIMIT 1"
        ).fetchone()
        unmatched = c.execute(
            "SELECT p.npi FROM cms_medicare_provider p LEFT JOIN (SELECT 'unmatched' AS npi) d USING(npi) WHERE d.npi IS NULL"
        ).fetchall()
    assert result == ("1003000126", "Internal Medicine", "MD", Decimal("35325.46"))
    assert len(unmatched) == len(rows)


def test_suppression_blank_null_and_revision():
    raw = deepcopy(F["rows"][0])
    raw.update(Drug_Sprsn_Ind="*", Med_Sprsn_Ind="#", Tot_Benes=None, Tot_Srvcs="")
    record = {"raw": raw, "selection": {}, "source_locator": {}}
    r = module.observation(record, year=2024, selection_digest="a")
    assert r["drug_medicare_payment_usd"] is r["medical_medicare_payment_usd"] is None
    statuses = json.loads(r["value_statuses_json"])
    assert statuses["beneficiaries"] == "null" and statuses["services"] == "blank"
    assert statuses["drug_services"] == "suppressed_lt11" and statuses["medical_services"] == "counter_suppressed"
    assert json.loads(r["raw_row_json"]) == raw
    assert module.observation(record, year=2024, selection_digest="b")["observation_id"] != r["observation_id"]


@pytest.mark.parametrize("change", ["duplicate", "short", "year", "marker", "measure", "missing-column", "hash"])
def test_failed_source_preserves_previous_table(tmp_path, change):
    from spicy_docs.sources.cms_provider import provider_records

    scope, controls, body = inputs()
    year = 2024
    if change in {"duplicate", "marker", "measure", "missing-column"}:
        rows = deepcopy(F["rows"])
        if change == "duplicate":
            rows[1]["Rndrng_NPI"] = rows[0]["Rndrng_NPI"]
        elif change == "marker":
            rows[1]["Drug_Sprsn_Ind"] = "?"
        elif change == "measure":
            rows[1]["Tot_Mdcr_Pymt_Amt"] = "NaN"
        else:
            # Source-level changed header fails before domain output.
            for r in rows:
                r.pop("Tot_Srvcs")
        scope, controls, body = inputs(rows)
    elif change == "short":
        body = body[:-10]
    elif change == "year":
        year = 2023
    else:
        scope["csv"]["response_sha256"] = "sha256:" + "0" * 64
    prior = tmp_path / module.OUTPUT
    prior.write_bytes(b"prior")
    with pytest.raises(ValueError):
        module.build_cms_provider(
            provider_records(io.BytesIO(body), scope=scope, dictionary=controls[1].body),
            tmp_path,
            scope=scope,
            year=year,
        )
    assert prior.read_bytes() == b"prior"
