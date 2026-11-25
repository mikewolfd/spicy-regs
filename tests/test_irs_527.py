"""Streaming IRS 527 archive transform and pipeline tests."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from io import BytesIO
import json
from zipfile import ZIP_DEFLATED, ZipFile

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.irs_527 import FULL_DATA_MEMBER, FULL_DATA_URL, Irs527Error
from spicy_regs import local_publication
from spicy_regs.pipelines import irs_527 as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms.build_irs_527 import (
    CONTRIBUTIONS,
    DIRECTORS,
    EAINS,
    EXPENDITURES,
    FORM8871,
    FORM8872,
    RELATED_ENTITIES,
    SCHEMAS,
    TRANSMISSIONS,
)


def _pipe(*fields: str, trailing: bool = True) -> str:
    return "|".join(fields) + ("|" if trailing else "")


def _sample_zip_bytes(*, malformed: bool = False) -> bytes:
    lines = [
        _pipe("H", "20261124", "0830", "A"),
        _pipe(
            "1", "8871", "1001", "Y", "N", "N", "123456789", "Café Action PAC",
            "100 Main St", "", "Dover", "DE", "19901", "0001", "pac@example.org", "20200115",
            "Jane Custodian", "100 Main St", "", "Dover", "DE", "19901", "",
            "John Contact", "100 Main St", "", "Dover", "DE", "19901", "",
            "100 Main St", "", "Dover", "DE", "19901", "",
            "Y", "DE", "N", "Voter education", "", "2020-01-15 10:00:00", "N", "N",
            trailing=False,
        ),
        _pipe("D", "1001", "2001", "Café Action PAC", "123456789", "José García", "Chair",
              "10 Elm", "", "Dover", "DE", "19901", ""),
        _pipe("R", "1001", "3001", "Café Action PAC", "123456789", "Related Org", "Affiliate",
              "20 Oak", "", "Dover", "DE", "19901", ""),
        _pipe("E", "1001", "4001", "DE-12345", "DE"),
        _pipe(
            "2", "8872", "5001", "20240101", "20240331", "Y", "N", "N", "N",
            "Café Action PAC", "123456789",
            "100 Main St", "", "Dover", "DE", "19901", "", "pac@example.org", "20200115",
            "Jane Custodian", "100 Main St", "", "Dover", "DE", "19901", "",
            "John Contact", "100 Main St", "", "Dover", "DE", "19901", "",
            "100 Main St", "", "Dover", "DE", "19901", "",
            "Q1", "", "", "", "", "Y", "1250.50", "Y", "300.00", "2024-04-15 09:00:00",
        ),
        _pipe("A", "5001", "6001", "Café Action PAC", "123456789", "François Müller",
              "55 Pine", "", "Newark", "DE", "19711", "", "Self", "1250.50", "Engineer", "2500.00", "20240214"),
        _pipe("B", "5001", "7001", "Café Action PAC", "123456789", "Print Co",
              "99 Broad", "", "Wilmington", "DE", "19801", "", "", "300.00", "", "notadate", "Flyers"),
        _pipe("F", "20261124", "0830", "7"),
        "(return status = 0)",
    ]
    if malformed:
        lines[2] = "D|1001|2001|TRUNCATED"
    payload = ("\n".join(lines) + "\n").encode("latin-1")
    buf = BytesIO()
    with ZipFile(buf, "w", compression=ZIP_DEFLATED) as zf:
        zf.writestr(FULL_DATA_MEMBER, payload)
    return buf.getvalue()


def test_irs_527_acquire_build_preserves_literals_latin1_types_and_dry_run(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw))
    zip_bytes = _sample_zip_bytes()

    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            stream=httpx.ByteStream(zip_bytes),
            headers={"content-type": "application/zip", "content-length": str(len(zip_bytes))},
        )
    )
    selection = pipeline.acquire(tmp_path / "acq", archive_root=raw, transport=transport)
    assert selection["archive"]["url"] == FULL_DATA_URL
    assert selection["archive"]["bytes"] == len(zip_bytes)

    out = tmp_path / "build"
    artifact, evidence = pipeline.build(selection, out, archive_root=raw)
    assert artifact.pin.artifact_digest and (evidence / "journal.jsonl").exists()

    for name, schema in SCHEMAS.items():
        actual = pq.read_schema(out / "generation" / f"{name}.parquet")
        assert actual.equals(schema)

    tx = pq.read_table(out / "generation" / f"{TRANSMISSIONS}.parquet").to_pylist()[0]
    assert tx["transmission_date"] == date(2026, 11, 24)
    assert tx["footer_record_count"] == 7 and tx["observed_data_records"] == 7

    f1 = pq.read_table(out / "generation" / f"{FORM8871}.parquet").to_pylist()[0]
    assert f1["organization_name"] == "Café Action PAC"
    assert f1["established_date"] == date(2020, 1, 15) and f1["established_date_status"] == "parsed"
    assert f1["material_change_date"] is None and f1["material_change_date_status"] == "blank"

    f2 = pq.read_table(out / "generation" / f"{FORM8872}.parquet").to_pylist()[0]
    assert f2["period_begin_date"] == date(2024, 1, 1) and f2["total_sched_a"] == Decimal("1250.50")

    directors = pq.read_table(out / "generation" / f"{DIRECTORS}.parquet").to_pylist()
    assert directors[0]["entity_name"] == "José García"
    assert pq.read_table(out / "generation" / f"{RELATED_ENTITIES}.parquet").num_rows == 1
    assert pq.read_table(out / "generation" / f"{EAINS}.parquet").num_rows == 1

    contrib = pq.read_table(out / "generation" / f"{CONTRIBUTIONS}.parquet").to_pylist()[0]
    assert contrib["contributor_name"] == "François Müller"
    assert contrib["contribution_amount"] == Decimal("1250.50")
    assert contrib["contribution_amount_raw"] == "1250.50"
    assert contrib["contribution_date"] == date(2024, 2, 14)

    exp = pq.read_table(out / "generation" / f"{EXPENDITURES}.parquet").to_pylist()[0]
    assert exp["expenditure_amount"] == Decimal("300.00")
    assert exp["expenditure_date"] is None and exp["expenditure_date_raw"] == "notadate"
    assert exp["expenditure_date_status"] == "unparsed"

    live = tmp_path / "live.json"
    live.write_text(json.dumps(publication.empty_index()))
    dry_out = tmp_path / "dry"
    local_publication.main([
        "--generation", str(out / "generation"),
        "--family", pipeline.FAMILY,
        "--evidence", str(evidence),
        "--dry-run", "--scratch-raw",
        "--live-index", str(live),
        "--output", str(dry_out),
    ])
    assert json.loads((dry_out / "dry-run.json").read_text())["status"] == "dry_run_accepted"


def test_malformed_irs_527_row_refuses_without_writing_generation(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw))
    bad_zip = _sample_zip_bytes(malformed=True)
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, stream=httpx.ByteStream(bad_zip), headers={"content-type": "application/zip"})
    )
    selection = pipeline.acquire(tmp_path / "acq", archive_root=raw, transport=transport)
    out = tmp_path / "bad-build"
    with pytest.raises(Irs527Error, match="malformed or truncated"):
        pipeline.build(selection, out, archive_root=raw)
    assert not (out / "generation").exists()
