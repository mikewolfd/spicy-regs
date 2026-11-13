"""Source/consumer boundary, failure atomicity, exact amounts and ordinary generation."""

import copy
import json
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.cms_open_payments import CATALOG_URL, TITLES, iter_summary, page_url, select_summary
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_regs.pipelines.cms_open_payments import build
from spicy_regs.transforms.build_cms_open_payments import (
    TABLE,
    build_cms_open_payments,
    compare_payments,
    measure,
    observation,
)

DATA = json.loads((Path(__file__).parent / "fixtures/cms_open_payments/source-excerpts.json").read_text())
NOW = "2026-11-13T11:30:00+00:00"
RELEASE = "P06302026_06032026"


def captured(url, data):
    return CapturedBodyResponse(url, url, 200, "application/json", NOW, json.dumps(data).encode())


def cohort(level):
    selected = select_summary(captured(CATALOG_URL, DATA["catalog"]), aggregation_level=level, program_year=2024)
    data = copy.deepcopy(DATA["examples"][level]["page"])
    # Explicit synthetic complete one-row population; raw fields are original excerpts.
    data["count"] = 1
    c = captured(page_url(selected), data)
    return selected, c, list(iter_summary(selected, [c]))


def test_exact_money_association_identity_and_missing_values():
    _, _, rows = cohort("recipients")
    item = rows[0]
    item["raw"]["general_total_payment"] = "-123456789012345.67"
    item["raw"]["total_associated_research_payments"] = "99.99"
    item["raw"]["covered_recipient_npi"] = ""
    item["raw"]["future_column"] = None
    row = observation(item)
    assert row["general_amount_usd"] == Decimal("-123456789012345.67")
    assert row["associated_research_amount_usd"] == Decimal("99.99")
    assert row["npi"] == ""
    assert json.loads(row["raw_row_json"])["future_column"] is None
    assert measure(None) == (None, "source_null") and measure("") == (None, "source_empty")
    assert measure("0.0") == (Decimal("0.0"), "reported")
    for malformed in ("1.001", "NaN", "1,000", "bad"):
        with pytest.raises(ValueError):
            measure(malformed)


def test_repeated_provider_associations_are_not_deduplicated_by_npi(tmp_path):
    _, _, rows = cohort("recipients")
    first = rows[0]
    second = copy.deepcopy(first)
    second["raw"]["covered_recipient_profile_id"] = "another-source-profile"
    second["source_locator"]["source_record_id"] = "second-source-occurrence"
    table = build_cms_open_payments([first, second], tmp_path, year=2024, release=RELEASE)
    data = pq.read_table(table).to_pylist()
    assert len(data) == 2 and data[0]["npi"] == data[1]["npi"]
    assert data[0]["native_id"] != data[1]["native_id"]


def test_source_failure_does_not_replace_existing_table(tmp_path):
    selected, c, rows = cohort("recipients")
    table = build_cms_open_payments(rows, tmp_path, year=2024, release=RELEASE)
    before = table.read_bytes()
    d = json.loads(c.body)
    d["count"] = 2
    incomplete = iter_summary(selected, [captured(c.requested_url, d)])
    with pytest.raises(ValueError):
        build_cms_open_payments(incomplete, tmp_path, year=2024, release=RELEASE)
    assert table.read_bytes() == before
    with pytest.raises(ValueError, match="release or program year mismatch"):
        build_cms_open_payments(rows, tmp_path, year=2024, release="wrong")
    assert table.read_bytes() == before


def test_ordinary_generation_and_consumer_refuse_mismatched_scope(tmp_path, monkeypatch):
    from spicy_regs.generations import verify_generation
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    store = CentralRawArchiveStore(tmp_path / "raw")
    catalog = captured(CATALOG_URL, DATA["catalog"])

    def save(c):
        store.put_blob(c.sha256, c.byte_size, [c.body])
        return {
            "request_url": c.requested_url,
            "sha256": c.sha256,
            "byte_size": c.byte_size,
            "observed_at": c.observed_at,
        }

    manifest = {
        "program_year": 2024,
        "complete": True,
        "catalog": save(catalog),
        "final_catalog": save(catalog),
        "families": {},
    }
    for level in TITLES:
        selected, c, _ = cohort(level)
        manifest["families"][level] = {"selection": selected.as_dict(), "pages": [save(c)], "count": 1}
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    artifact, evidence = build(path, tmp_path / "candidate")
    verified = verify_generation(tmp_path / "candidate/generation", expected_pin=artifact.pin)
    assert verified.pin == artifact.pin
    con = duckdb.connect(config={"threads": 1})
    con.read_parquet(str(tmp_path / "candidate/generation" / f"{TABLE}.parquet")).create_view(TABLE)
    row = compare_payments(con, level="recipients", year=2024, release=RELEASE, category="General")[0]
    assert row[4] == Decimal("121.96")
    assert json.loads(row[5])["json_pointer"] == "/results/0"
    for changes in ({"year": 2023}, {"release": "Pold"}, {"category": "All"}, {"category": "associated research"}):
        args = dict(level="recipients", year=2024, release=RELEASE, category="General")
        args.update(changes)
        with pytest.raises(ValueError):
            compare_payments(con, **args)
    with pytest.raises(ValueError, match="recipient type"):
        compare_payments(con, level="states", year=2024, release=RELEASE, category="Research")
    with pytest.raises(ValueError, match="unavailable"):
        compare_payments(con, level="hospitals", year=2024, release=RELEASE, category="Investment")
    manifest["families"].pop("recipients")
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="all declared complete"):
        build(path, tmp_path / "refused")


def test_supported_csv_suffix_and_resume_avoid_repeated_data_requests(tmp_path, monkeypatch):
    import csv
    import io
    import httpx
    from spicy_regs.pipelines.cms_open_payments import capture

    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    requested = []
    levels = {item["dataset"]["identifier"]: level for level, item in DATA["examples"].items()}

    def answer(request):
        requested.append(str(request.url))
        if request.url.path == "/data.json":
            return httpx.Response(200, json=DATA["catalog"])
        if request.url.path.endswith("/download"):
            raw = copy.deepcopy(DATA["examples"]["recipients"]["page"]["results"][0])
            raw["record_number"] = "501"
            text = io.StringIO(newline="")
            writer = csv.writer(text)
            writer.writerow(raw)
            writer.writerow(raw.values())
            writer.writerow([])
            return httpx.Response(200, content=text.getvalue().encode(), headers={"content-type": "text/csv"})
        level = levels[request.url.path.split("/")[-2]]
        data = copy.deepcopy(DATA["examples"][level]["page"])
        data["count"] = 501 if level == "recipients" else 1
        if request.url.params.get("results") == "false":
            data["results"] = []
        elif level == "recipients":
            data["results"] = [{**data["results"][0], "record_number": str(i + 1)} for i in range(500)]
        return httpx.Response(200, json=data)

    transport = httpx.MockTransport(answer)
    manifest = capture(tmp_path / "capture", year=2024, transport=transport)
    saved = json.loads(manifest.read_text())
    assert saved["families"]["recipients"]["count"] == 501
    assert len(saved["families"]["recipients"]["pages"]) == 1
    assert "csv_tail" in saved["families"]["recipients"]
    artifact, _ = build(manifest, tmp_path / "candidate")
    table = pq.read_table(tmp_path / "candidate/generation" / f"{TABLE}.parquet")
    assert table.num_rows == 505
    data = [row for row in table.to_pylist() if row["aggregation_level"] == "recipients"]
    assert len(data) == 501 and len({row["native_id"] for row in data}) == 1
    assert json.loads(data[-1]["source_locator_json"])["source_format"] == "csv"
    before = len(requested)
    capture(tmp_path / "capture", year=2024, transport=transport)
    repeated = requested[before:]
    assert len(repeated) == 2
    assert all("/data.json" in url or "results=false" in url for url in repeated)
