"""Offline preparation admits native inputs and preserves evidence-scoped outcomes."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import crs_file_preparation as prep
from spicy_regs.transforms import build_crs_report_files as files
from spicy_regs.transforms.build_crs_reports import _SCHEMA, COLUMNS
from spicy_regs.transforms.government_receipts import migrate_outputs
from tests.test_crs_report_files import HTML, PDF, report


def metadata_pair(tmp_path, records) -> dict[str, Any]:
    directory = tmp_path / "metadata"
    directory.mkdir()
    subject = directory / "crs_reports.parquet"
    pq.write_table(pa.Table.from_pylist([{**dict.fromkeys(COLUMNS), **row} for row in records], schema=_SCHEMA), subject)
    migrate_outputs((subject,), generation_id="prep-test")
    receipts = directory / "etl_receipts.parquet"
    return {"subject": subject, "receipt": receipts, "generation_id": "prep-test",
            "subject_sha256": prep._sha(subject), "receipt_sha256": prep._sha(receipts)}


def qualified():
    row = report()
    witness = json.loads(row["detail_witness_json"])[0]
    witness.update(source_id="congress.gov:crs-detail", source_uri=None, locator="/CRSReport", body_version=None)
    row["detail_witness_json"] = json.dumps([witness])
    row["title"] = "Test CRS report"
    return row


def catalog_occurrence(row):
    return {"id": "file:exact", "status": "held", "sha256": row["source_sha256"], "bytes": row["source_bytes"],
            "data": {"mapping": {"receipt_evidence_ref": {"manifest": row["capture_receipt_key"],
                      "sha256": row["capture_receipt_sha256"], "entry": 1}},
                     "receipt_context": {"source_url": row["source_url"]}}}


def capture(tmp_path, row, *, body=PDF, include_html=False):
    transport = httpx.MockTransport(lambda request: httpx.Response(200,
        stream=httpx.ByteStream(HTML if request.url.path.endswith(".html") else body),
        headers={"Content-Type": "text/html" if request.url.path.endswith(".html") else "application/pdf"}))
    result = files.build_crs_report_files(tmp_path / "capture", [row], transport=transport,
                                           allow_network=True, include_html=include_html)
    bundle = Path(result["bundle"])
    return bundle, pq.read_table(bundle / files.KEY).to_pylist()


def test_revision_selection_requires_explicit_latest_and_compares_instants():
    older = {"report_id": "R1", "update_date": "2026-01-01T00:00:00Z", "version": "1"}
    newer = {"report_id": "R1", "update_date": datetime(2026, 1, 2, tzinfo=UTC), "version": "2"}
    rows = [older, newer]
    assert prep.select_revisions(rows, ["R1"], latest=True) == [newer]
    assert prep.select_revisions(rows, ["R1@2025-12-31T19:00:00-05:00"]) == [older]
    assert rows == [older, newer]
    for reports, latest, message in ((["R1"], False, "explicit latest"),
        (["R2"], True, "unknown"), (["R1@2026-01-03T00:00:00Z"], False, "unknown"),
        (["R1@2026-01-01"], False, "timezone"), (["R1", "R1"], True, "repeats")):
        with pytest.raises(ValueError, match=message):
            prep.select_revisions(rows, reports, latest=latest)


def test_native_pair_admission_precedes_restoration(tmp_path, monkeypatch):
    pair = metadata_pair(tmp_path, [{"report_id": "R1", "update_date": "2026-01-01T00:00:00Z"}])
    monkeypatch.setattr("spicy_regs.etl_receipts.read_with_receipts",
                        lambda *_, **__: (_ for _ in ()).throw(AssertionError("restored before admission")))
    with pytest.raises(ValueError, match="bytes exceed"):
        prep.prepare_crs_report_files(**pair, reports=["R1"], latest=True, max_input_bytes=1)
    bad_pair = pair.copy()
    bad_pair["receipt_sha256"] = "sha256:" + "0" * 64
    with pytest.raises(ValueError, match="expected SHA256"):
        prep.prepare_crs_report_files(**bad_pair, reports=["R1"], latest=True)


def test_admission_bounds_receipt_rows_and_decoded_bytes(tmp_path):
    subject, receipts = tmp_path / "subject.parquet", tmp_path / "receipts.parquet"
    pq.write_table(pa.table({"a": ["one"]}), subject)
    pq.write_table(pa.table({"a": ["one", "two"]}), receipts)
    with pytest.raises(ValueError, match="receipt rows"):
        prep.admit_metadata_pair(subject, receipts, max_bytes=10000, max_rows=1)
    pq.write_table(pa.table({"a": ["x" * 100000]}), receipts, compression="zstd")
    assert sum(path.stat().st_size for path in (subject, receipts)) < 20000
    with pytest.raises(ValueError, match="decoded"):
        prep.admit_metadata_pair(subject, receipts, max_bytes=20000, max_rows=2)


def test_metadata_only_and_historical_unretained_are_distinct(tmp_path):
    records = [{"report_id": "R1", "update_date": "2026-01-01T00:00:00Z", "detail_state": "not_requested"},
               {"report_id": "R2", "update_date": "2026-01-01T00:00:00Z", "detail_state": "not_requested",
                "detail_attempt_state": "revision_mismatch"}]
    result = prep.prepare_crs_report_files(**metadata_pair(tmp_path, records), reports=["R1", "R2"], latest=True)
    assert result["selection"]["reports"] == []
    first, second = result["coverage"]["revisions"]
    assert first["detail_state"] == "detail-not-requested" and first["catalog_state"] == "unknown"
    assert second["detail_state"] == "historical-detail-not-retained"
    assert first["body_state"] == second["body_state"] == "unknown"


def test_exact_catalog_selection_feeds_collector_without_network(tmp_path, monkeypatch):
    row = qualified()
    _, captured = capture(tmp_path, row, include_html=True)
    catalog = tmp_path / "selected-catalog-rows.json"
    catalog.write_text(json.dumps([catalog_occurrence(item) for item in captured]))
    pair = metadata_pair(tmp_path, [row])
    result = prep.prepare_crs_report_files(**pair, reports=["RL12345"], latest=True, include_html=True,
        catalog_selection=catalog, catalog_selection_sha256=prep._sha(catalog))
    pdf, html = result["coverage"]["revisions"]
    assert pdf["disposition"] == html["disposition"] == "body-held"
    assert pdf["publisher_version"] == "2" and html["publisher_version"] is None
    assert pdf["catalog_state"] == "unmapped"  # No metadata witness match does not imply source absence.
    monkeypatch.setattr("spicy_docs.sources.congress.crs_files.CrsFileAcquirer.acquire_report",
                        lambda *_, **__: (_ for _ in ()).throw(AssertionError("unexpected download")))
    selection = result["selection"]
    built = files.build_crs_report_files(tmp_path / "consumer", selection["reports"],
        retained_files=selection["retained_files"], include_html=True)
    assert built["requests"] == 0 and built["reused_bodies"] == 2
    actual = files._read(Path(built["bundle"]), max_bytes=2**24, max_rows=10)
    assert [item["text_state"] for item in actual] == ["ok", "ok"]
    assert actual[1]["publisher_version"] is None


def test_prior_completed_and_empty_text_have_separate_coverage(tmp_path):
    from tests.pdf_fixtures import make_pdf

    row = qualified()
    prior, _ = capture(tmp_path, row, body=make_pdf([]))
    result = prep.prepare_crs_report_files(**metadata_pair(tmp_path, [row]), reports=["RL12345"], latest=True,
        prior_bundle=prior, prior_subject_sha256=prep._sha(prior / files.KEY),
        prior_receipt_sha256=prep._sha(prior / "etl_receipts.parquet"))
    pdf, html = result["coverage"]["revisions"]
    assert pdf["disposition"] == "empty-text" and pdf["text_state"] == "empty"
    assert html["disposition"] == "format-offered" and html["selected"] is False


def test_corrupt_retained_original_and_oversize_body_refuse(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    row = qualified()
    _, captured = capture(tmp_path, row)
    catalog = tmp_path / "selected.json"
    catalog.write_text(json.dumps([catalog_occurrence(captured[0])]))
    args: dict[str, Any] = {**metadata_pair(tmp_path, [row]), "reports": ["RL12345"], "latest": True,
            "catalog_selection": catalog, "catalog_selection_sha256": prep._sha(catalog)}
    with pytest.raises(ValueError, match="retained body"):
        prep.prepare_crs_report_files(**args, max_file_bytes=1)
    store = CentralRawArchiveStore(create=False)
    digest = captured[0]["source_sha256"].removeprefix("sha256:")
    (store.root / "objects/sha256" / digest[:2] / digest).write_bytes(b"corrupt")
    with pytest.raises(ValueError, match="retained original"):
        prep.prepare_crs_report_files(**args)


def test_invalid_catalog_pin_and_changed_formats_refuse(tmp_path):
    row = qualified()
    row["formats_json"] = row["formats_json"].replace("/RA/", "/RL/")
    pair = metadata_pair(tmp_path, [row])
    with pytest.raises(ValueError, match="offered formats differ"):
        prep.prepare_crs_report_files(**pair, reports=["RL12345"], latest=True)
    catalog = tmp_path / "selected.json"
    catalog.write_text("[]")
    with pytest.raises(ValueError, match="expected SHA256"):
        prep.prepare_crs_report_files(**pair, reports=["RL12345"], latest=True,
            catalog_selection=catalog, catalog_selection_sha256="sha256:" + "0" * 64)


def test_real_refusal_stays_unavailable_without_claiming_missing_original(tmp_path):
    row = qualified()
    transport = httpx.MockTransport(lambda _: httpx.Response(404,
        stream=httpx.ByteStream(b"missing"), headers={"Content-Type": "text/html"}))
    result = files.build_crs_report_files(tmp_path / "unavailable", [row], transport=transport, allow_network=True)
    prior = Path(result["bundle"])
    prepared = prep.prepare_crs_report_files(**metadata_pair(tmp_path, [row]), reports=["RL12345"], latest=True,
        prior_bundle=prior, prior_subject_sha256=prep._sha(prior / files.KEY),
        prior_receipt_sha256=prep._sha(prior / "etl_receipts.parquet"))
    pdf = prepared["coverage"]["revisions"][0]
    assert pdf["disposition"] == "source-unavailable" and pdf["body_state"] == "unavailable"
    assert pdf["source_sha256"] is None
    assert prepared["selection"]["retained_files"] == {}


def test_preparation_cli_emits_collector_shape_and_protects_input_pair(tmp_path, monkeypatch):
    import sys

    pair = metadata_pair(tmp_path, [{"report_id": "R1", "update_date": "2026-01-01T00:00:00Z"}])
    selection, coverage = tmp_path / "selection.json", tmp_path / "coverage.json"
    argv = ["prep", "--subject", str(pair["subject"]), "--receipts", str(pair["receipt"]),
            "--generation-id", pair["generation_id"], "--subject-sha256", pair["subject_sha256"],
            "--receipt-sha256", pair["receipt_sha256"], "--report", "R1", "--latest",
            "--selection-output", str(selection), "--coverage-output", str(coverage)]
    monkeypatch.setattr(sys, "argv", argv)
    prep.main()
    assert set(json.loads(selection.read_text())) == {"reports", "retained_files"}
    assert json.loads(coverage.read_text())["selected_revisions"] == 1
    argv[argv.index("--selection-output") + 1] = str(pair["subject"])
    with pytest.raises(SystemExit):
        prep.main()
    assert prep._sha(pair["subject"]) == pair["subject_sha256"]


def test_empty_offered_formats_remain_qualified_detail_without_file_selection(tmp_path):
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    row = qualified()
    native = {"CRSReport": {"id": row["report_id"], "updateDate": row["update_date"],
                            "currentVersion": 2, "formats": []}}
    body = json.dumps(native).encode()
    digest = files._sha(body)
    CentralRawArchiveStore().put_blob(digest, len(body), [body])
    witnesses = json.loads(row["detail_witness_json"])
    witnesses[0]["sha256"] = digest
    row.update(formats_json="[]", detail_witness_json=json.dumps(witnesses))
    result = prep.prepare_crs_report_files(**metadata_pair(tmp_path, [row]), reports=["RL12345"], latest=True)
    assert result["selection"]["reports"] == []
    [coverage] = result["coverage"]["revisions"]
    assert coverage["detail_state"] == "qualified-detail" and coverage["format_state"] == "no-pdf-offered"
    assert coverage["offered_formats"] == [] and coverage["body_state"] == "unknown"


def test_generated_selection_respects_existing_collector_byte_ceiling(tmp_path, monkeypatch):
    row = qualified()
    pair = metadata_pair(tmp_path, [row])
    monkeypatch.setattr(prep, "MAX_SELECTION_BYTES", 100)
    with pytest.raises(ValueError, match="collector's 2 MiB"):
        prep.prepare_crs_report_files(**pair, reports=["RL12345"], latest=True)
