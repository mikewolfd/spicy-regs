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
    assert tx["footer_record_count"] == 7 and tx["observed_data_records"] == 7 and tx["malformed_row_count"] == 0

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


def test_irs_527_oversized_amount_preserves_raw_with_exceeds_precision():
    from spicy_regs.transforms.build_irs_527 import _parse_amount

    value, status = _parse_amount("9" * 37 + ".00")
    assert value is None and status == "exceeds_precision"
    assert _parse_amount("12.300") == (Decimal("12.300"), "parsed")
    assert _parse_amount("12.345") == (None, "exceeds_scale")
    assert _parse_amount("1,234.50") == (Decimal("1234.50"), "parsed")
    assert _parse_amount("-42.25") == (Decimal("-42.25"), "parsed")


def test_irs_527_bounded_publisher_truncated_rows_quarantined_and_non_truncation_refused(tmp_path, monkeypatch):
    raw = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw))

    def _build_large_zip(bad_lines: list[str]) -> bytes:
        valid_e = [_pipe("E", "1001", str(i), f"DE-{i}", "DE") for i in range(1, 2001)]
        lines = [
            _pipe("H", "20261124", "0830", "F"),
            *bad_lines,
            *valid_e,
            _pipe("F", "20261124", "0830", str(len(valid_e) + len(bad_lines))),
            "(return status = 0)",
        ]
        buf = BytesIO()
        with ZipFile(buf, "w", compression=ZIP_DEFLATED) as zf:
            zf.writestr(FULL_DATA_MEMBER, ("\n".join(lines) + "\n").encode("latin-1"))
        return buf.getvalue()

    truncated_type1 = _pipe(
        "1", "8871", "9623773", "0", "1", "0", "463735347", "Truncated PAC",
        *([""] * 31), "Truncated purpose at smart quote",
        trailing=False,
    )
    truncated_type_d = "D|9736261|242358|Vote PAC|991248053|Margaret O"
    zip_bytes = _build_large_zip([truncated_type1, truncated_type_d])
    transport = httpx.MockTransport(
        lambda request: httpx.Response(200, stream=httpx.ByteStream(zip_bytes), headers={"content-type": "application/zip"})
    )
    selection = pipeline.acquire(tmp_path / "acq-trunc", archive_root=raw, transport=transport)
    out = tmp_path / "build-trunc"
    artifact, evidence = pipeline.build(selection, out, archive_root=raw)
    assert artifact.pin.artifact_digest

    tx = pq.read_table(out / "generation" / f"{TRANSMISSIONS}.parquet").to_pylist()[0]
    assert tx["footer_record_count"] == 2002
    assert tx["observed_data_records"] == 2000
    assert tx["malformed_row_count"] == 2
    assert pq.read_table(out / "generation" / f"{EAINS}.parquet").num_rows == 2000

    quarantined = json.loads((out / "malformed-rows.json").read_text())
    assert quarantined["count"] == 2
    assert quarantined["by_record_type"] == {"1": 1, "D": 1}
    assert [(r["line_number"], r["record_type"], r["raw_line"]) for r in quarantined["rows"]] == [
        (2, "1", truncated_type1),
        (3, "D", truncated_type_d),
    ]

    events = [json.loads(line) for line in (evidence / "journal.jsonl").read_text().splitlines()]
    assert any(e.get("stage") == "irs-527-malformed-rows" for e in events)
    assert any(
        e.get("event") == "irs-527-malformed-rows-quarantined" and e.get("malformed_row_count") == 2
        for e in events
    )

    # Non-truncation corruption (unknown record type) MUST still refuse even at <0.1% ratio.
    unknown_type_zip = _build_large_zip(["Z|999|unexpected|"])
    bad_transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200, stream=httpx.ByteStream(unknown_type_zip), headers={"content-type": "application/zip"}
        )
    )
    bad_sel = pipeline.acquire(tmp_path / "acq-unknown", archive_root=raw, transport=bad_transport)
    bad_out = tmp_path / "build-unknown"
    with pytest.raises(Irs527Error, match="unknown record type"):
        pipeline.build(bad_sel, bad_out, archive_root=raw)
    assert not (bad_out / "generation").exists()


def test_irs_527_acquire_uses_evidence_transport_custody_and_records_403_refusal(tmp_path, monkeypatch):
    from spicy_docs.transport.credentials import CredentialRefusedError

    raw = tmp_path / "raw"
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw))
    zip_bytes = _sample_zip_bytes()
    acq_dir = tmp_path / "acq"
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            stream=httpx.ByteStream(zip_bytes),
            headers={"content-type": "application/zip", "content-length": str(len(zip_bytes))},
        )
    )
    selection = pipeline.acquire(acq_dir, archive_root=raw, transport=transport)
    journals = list((acq_dir / "source-evidence").glob("*/artifact/journal.jsonl"))
    assert len(journals) == 1
    events = [json.loads(line) for line in journals[0].read_text().splitlines()]
    captures = [e for e in events if e.get("event") == "capture" and e.get("stage") == "irs-527-full-data-zip"]
    assert len(captures) == 1
    assert captures[0]["blob_store"] == "central-raw"
    assert captures[0]["sha256"] == selection["archive"]["sha256"]
    assert captures[0]["byte_size"] == len(zip_bytes)

    refused_dir = tmp_path / "acq-403"
    refused_transport = httpx.MockTransport(
        lambda request: httpx.Response(403, content=b"forbidden", headers={"content-type": "text/plain"})
    )
    with pytest.raises(CredentialRefusedError):
        pipeline.acquire(refused_dir, archive_root=raw, transport=refused_transport)
    refused_journals = list((refused_dir / "source-evidence").glob("*/artifact/journal.jsonl"))
    assert len(refused_journals) == 1
    refused_events = [json.loads(line) for line in refused_journals[0].read_text().splitlines()]
    assert any(e.get("event") == "capture-incomplete" for e in refused_events)
