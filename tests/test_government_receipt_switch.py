"""Parity and seam verification for retiring per-row receipts on the six government rollups (ss-vse3)."""

from __future__ import annotations

import hashlib
import importlib
import json
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.native_types import described_schema
from spicy_regs.pipelines.rollups.crs_reports import CrsReportsRollup
from spicy_regs.pipelines.rollups.fcc_filings import FccFilingsRollup
from spicy_regs.pipelines.rollups.gao_recommendations import GaoRecommendationsRollup
from spicy_regs.pipelines.rollups.gao_reports import GaoReportsRollup
from spicy_regs.pipelines.rollups.sam_entities import SamEntitiesRollup
from spicy_regs.pipelines.rollups.usaspending_recipients import UsaSpendingRecipientsRollup
from spicy_regs.receipt_switch import receipt_files, receipt_free_prior, table_differences
from spicy_regs.sources import publication
from spicy_regs.transforms.government_receipts import _legacy_schema, migrate_outputs
from spicy_regs.transforms.government_source_shapes import IDENTITY_FIELDS, SUBJECT_SCHEMAS, map_subject


def _receipt_route(directory: Path, dataset: str, rows: list[dict], *, generation_id: str = "switch-gen") -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{dataset}.parquet"
    schema = _legacy_schema(dataset)
    full = [{**dict.fromkeys(schema.names), **row} for row in rows]
    pq.write_table(pa.Table.from_pylist(full, schema=schema), path, compression="zstd")
    migrate_outputs((path,), generation_id=generation_id)
    return path


def _write_receipted_prior(prior_dir: Path, dataset: str, rows: list[dict]) -> Path:
    _receipt_route(prior_dir, dataset, rows, generation_id="prior-gen")
    assert (prior_dir / "etl_receipts.parquet").exists()
    return prior_dir


def test_sam_entities_matches_receipt_route_without_receipts(tmp_path, monkeypatch):
    mod = importlib.import_module("spicy_regs.transforms.build_sam_entities")
    prior_rows = [
        {"uei": "UEI000000001", "entity_eft_indicator": None, "legal_business_name": "Prior Corp",
         "registration_status": "Active", "registration_date": "2024-03-01",
         "registration_expiration_date": "2027-03-01"},
        {"uei": "UEI000000002", "entity_eft_indicator": "0001", "legal_business_name": "Retired Corp",
         "registration_status": "Active", "registration_date": "2025-05-01",
         "registration_expiration_date": "2026-05-01"},
    ]
    fresh_api = [
        {
            "entityRegistration": {
                "ueiSAM": "UEI000000003",
                "entityEFTIndicator": None,
                "legalBusinessName": "Fresh Corp",
                "registrationStatus": "Active",
                "registrationDate": "2025-08-15",
                "registrationExpirationDate": "2028-08-15",
            }
        },
    ]
    prior_src = _write_receipted_prior(tmp_path / "prior_src", "sam_entities", prior_rows)
    plain_prior = receipt_free_prior(prior_src, tmp_path / "plain_prior")
    build_dir = tmp_path / "plain"
    build_dir.mkdir()
    (build_dir / "_sam_prior.parquet").write_bytes((plain_prior / "sam_entities.parquet").read_bytes())

    monkeypatch.setattr(mod.r2, "download", lambda *_: False)
    monkeypatch.setattr(mod, "_iter_sam_entities", lambda **_: iter(fresh_api))
    out = mod.build_sam_entities(build_dir, mode="extract", since_year=2025, until_year=2025, max_records=None)

    assert receipt_files(build_dir) == []
    expected = _receipt_route(tmp_path / "expected", "sam_entities", [mod._shape(fresh_api[0]), prior_rows[0]])
    assert table_differences(out, expected, key=IDENTITY_FIELDS["sam_entities"]) == []


def test_usaspending_recipients_matches_receipt_route_without_receipts(tmp_path, monkeypatch):
    mod = importlib.import_module("spicy_regs.transforms.build_usaspending_recipients")
    prior_rows = [
        {"recipient_id": "A-R", "uei": "UEIA", "duns": None, "name": "Alpha", "recipient_level": "R",
         "total_award_amount": "100.25", "observed_at": "2026-01-01T00:00:00Z",
         "source_capture_sha256": "sha256:" + "1" * 64},
        {"recipient_id": "B-R", "uei": "UEIB", "duns": None, "name": "Beta Old", "recipient_level": "R",
         "total_award_amount": "50", "observed_at": "2026-01-01T00:00:00Z",
         "source_capture_sha256": "sha256:" + "1" * 64},
    ]
    fresh_api = [
        {"id": "B-R", "uei": "UEIB", "duns": None, "name": "Beta New", "recipient_level": "R", "amount": 250.5},
    ]
    prior_src = _write_receipted_prior(tmp_path / "prior_src", "usaspending_recipients", prior_rows)
    plain_prior = receipt_free_prior(prior_src, tmp_path / "plain_prior")
    build_dir = tmp_path / "plain"
    build_dir.mkdir()
    (build_dir / "_usaspending_prior.parquet").write_bytes((plain_prior / "usaspending_recipients.parquet").read_bytes())

    monkeypatch.setattr(mod.r2, "download", lambda *_: False)
    monkeypatch.setattr(mod, "_iter_recipient_rows", lambda **_: iter(fresh_api))
    out = mod.build_usaspending_recipients(build_dir)

    assert receipt_files(build_dir) == []
    expected = _receipt_route(tmp_path / "expected", "usaspending_recipients", [mod._shape(fresh_api[0]), prior_rows[0]])
    assert table_differences(out, expected, key=IDENTITY_FIELDS["usaspending_recipients"]) == []


def test_gao_recommendations_matches_receipt_route_without_receipts(tmp_path, monkeypatch):
    from spicy_docs.schemas.gao_recommendation_tables import shape_gao_recommendation
    from spicy_docs.sources.gao.recommendations import parse_recommendations_export
    from spicy_docs.transport.captured import CapturedBodyResponse

    mod = importlib.import_module("spicy_regs.transforms.build_gao_recommendations")
    body = (Path(__file__).parent / "fixtures/gao_recommendations/open-recs-2026-09-28-excerpt.csv").read_bytes()
    export = parse_recommendations_export(body)
    shaped = [
        shape_gao_recommendation(rec, status_as_of=export.status_as_of, as_of=export.as_of)
        for rec in export.recommendations
    ]
    prior_rows = [
        {**shaped[0], "first_seen": "2026-09-20", "last_seen": "2026-09-25", "status_as_of": "2026-09-25"},
        {**shaped[0], "recommendation_id": "REC-CLOSED", "report_id": "gao-25-999",
         "first_seen": "2026-09-20", "last_seen": "2026-09-25", "status_as_of": "2026-09-25"},
    ]
    prior_src = _write_receipted_prior(tmp_path / "prior_src", "gao_recommendations", prior_rows)
    plain_prior = receipt_free_prior(prior_src, tmp_path / "plain_prior")
    build_dir = tmp_path / "plain"
    build_dir.mkdir()
    (build_dir / "_gao_recommendations_prior.parquet").write_bytes(
        (plain_prior / "gao_recommendations.parquet").read_bytes()
    )

    capture = CapturedBodyResponse("https://gao.test/recs", "https://gao.test/recs", 200, "text/csv", "2026-09-28T00:00:00Z", body)
    monkeypatch.setattr(mod, "_acquire", lambda *a, **k: (export, capture))
    out = mod.build_gao_recommendations(build_dir, download_prior=lambda *_: False)

    assert receipt_files(build_dir) == []
    expected_rows, _ = mod.fold(prior_rows, shaped)
    expected = _receipt_route(tmp_path / "expected", "gao_recommendations", expected_rows)
    assert table_differences(out, expected, key=IDENTITY_FIELDS["gao_recommendations"]) == []


def test_gao_reports_and_decisions_match_receipt_route_and_soft_read_retired_recs(tmp_path, monkeypatch):
    mod = importlib.import_module("spicy_regs.transforms.build_gao_reports")
    from spicy_regs.sources import gao_product_pages

    prior_reports = [
        {"report_id": "gao-25-100", "title": "Prior Report", "report_type": "Report",
         "published_date": "2025-05-01", "abstract": "Summary", "agencies_json": '["DoD"]',
         "topics_json": '["Defense"]', "url": "https://www.gao.gov/products/gao-25-100",
         "source": "gao_rss", "report_number": "GAO-25-100"},
    ]
    prior_decisions = [
        {"decision_number": "B-400001", "b_numbers_json": '["B-400001"]', "decision_type": "Bid Protest Decision",
         "title": "Prior Decision", "released_date": "2025-06-01", "decided_date": "2025-05-25",
         "decision_status": "We deny the protest.", "outcome": "denied", "outcome_rule": "rule/1",
         "b_numbers_truncated": "false", "url": "https://www.gao.gov/products/b-400001",
         "listing_page": "https://www.gao.gov/legal/bid-protests", "source": "gao_listing"},
    ]
    fresh_feed = [
        {"product_id": "gao-26-200", "title": "Fresh Report", "pub_date": "Sun, 01 Mar 2026 00:00:00 +0000",
         "description": "What GAO Found.", "link": "https://www.gao.gov/products/gao-26-200"},
    ]
    prior_src = _write_receipted_prior(tmp_path / "prior_reports", "gao_reports", prior_reports)
    dec_src = _write_receipted_prior(tmp_path / "prior_decisions", "gao_decisions", prior_decisions)
    plain_rep = receipt_free_prior(prior_src, tmp_path / "plain_rep")
    plain_dec = receipt_free_prior(dec_src, tmp_path / "plain_dec")
    build_dir = tmp_path / "plain"
    build_dir.mkdir()
    (build_dir / "_gao_prior.parquet").write_bytes((plain_rep / "gao_reports.parquet").read_bytes())
    (build_dir / "_gao_decisions_prior.parquet").write_bytes((plain_dec / "gao_decisions.parquet").read_bytes())

    class FakeFeed:
        def __init__(self, **_):
            pass

        def iter_records(self):
            return iter(fresh_feed)

    monkeypatch.setattr(mod, "GaoReportsReader", FakeFeed)
    monkeypatch.setattr(mod.r2, "download", lambda *_: False)
    reports_out, decisions_out = mod.build_gao_reports(build_dir)

    assert receipt_files(build_dir) == []
    expected_reports = _receipt_route(
        tmp_path / "exp_rep",
        "gao_reports",
        [mod._shape(fresh_feed[0]), prior_reports[0]],
    )
    expected_decisions = _receipt_route(tmp_path / "exp_dec", "gao_decisions", prior_decisions)
    assert table_differences(reports_out, expected_reports, key=IDENTITY_FIELDS["gao_reports"]) == []
    assert table_differences(decisions_out, expected_decisions, key=IDENTITY_FIELDS["gao_decisions"]) == []

    # Cross-family soft read: gao-reports reads retired receiptless gao_recommendations.parquet cleanly.
    recs_dir = tmp_path / "retired_recs"
    recs_dir.mkdir()
    rec_row = map_subject(
        "gao_recommendations",
        {"recommendation_id": "REC-9", "report_id": "gao-26-200", "last_seen": "2026-03-10", "listed_open": "true"},
    )
    recs_file = recs_dir / "gao_recommendations.parquet"
    pq.write_table(pa.Table.from_pylist([rec_row], schema=SUBJECT_SCHEMAS["gao_recommendations"]), recs_file)

    def fake_download(key, target):
        if key == "gao_recommendations.parquet":
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(recs_file.read_bytes())
            return True
        return False

    monkeypatch.setattr(gao_product_pages.r2, "download", fake_download)
    recs = gao_product_pages.open_recommendations(tmp_path / "soft")
    assert recs is not None and recs.as_of == "2026-03-10" and recs.rows == {"gao-26-200": 1}
    assert receipt_files(recs_dir, tmp_path / "soft") == []


def test_fcc_filings_matches_receipt_route_without_receipts(tmp_path, monkeypatch):
    mod = importlib.import_module("spicy_regs.transforms.build_fcc_ecfs")
    prior_raw = {
        "id_submission": "1001", "proceedings": [{"name": "24-1", "id_proceeding": 99}],
        "submissiontype": {"description": "COMMENT"}, "express_comment": 1,
        "date_received": "2026-01-10T10:00:00Z", "filers": [{"name": "Alice"}],
        "documents": [{"filename": "a.pdf", "src": "https://docs.fcc.gov/a.pdf"}],
        "text_data": "Old", "total_page_count": 2,
    }
    fresh_raw = {
        "id_submission": "1002", "proceedings": [{"name": "24-1", "id_proceeding": 99}],
        "submissiontype": {"description": "COMMENT"}, "express_comment": 0,
        "date_received": "2026-01-12T10:00:00Z", "filers": [{"name": "Bob"}],
        "documents": [], "text_data": "Fresh", "total_page_count": 1,
    }
    prior_shaped = mod._shape_filing(prior_raw)
    prior_shaped["pdf_extraction_results_json"] = json.dumps([
        {"url": "https://docs.fcc.gov/a.pdf", "status": "ok", "page_count": 2, "source_sha256": None, "error": None}
    ])
    prior_src = _write_receipted_prior(tmp_path / "prior_src", "fcc_filings", [prior_shaped])
    plain_prior = receipt_free_prior(prior_src, tmp_path / "plain_prior")
    build_dir = tmp_path / "plain"
    build_dir.mkdir()
    (build_dir / "_fcc_filings_prior.parquet").write_bytes((plain_prior / "fcc_filings.parquet").read_bytes())

    monkeypatch.setattr(mod.r2, "download", lambda *_: False)
    monkeypatch.setattr(mod, "_fetch_fcc", lambda *a, **k: (r for r in [fresh_raw]))
    out = mod.build_fcc_filings(build_dir, since=date(2026, 1, 1), generation_id="switch-gen")

    assert receipt_files(build_dir) == []
    expected = _receipt_route(
        tmp_path / "expected", "fcc_filings", [mod._shape_filing(fresh_raw), prior_shaped], generation_id="switch-gen"
    )
    assert table_differences(out, expected, key=IDENTITY_FIELDS["fcc_filings"]) == []

    # Same-key refresh carries extraction_results when documents match, and invalidates when documents change.
    for changed in (False, True):
        refresh_dir = tmp_path / f"fcc_refresh_{changed}"
        refresh_dir.mkdir()
        (refresh_dir / "_fcc_filings_prior.parquet").write_bytes(out.read_bytes())
        refreshed = {**prior_raw, "text_data": "Refreshed"}
        if changed:
            refreshed["documents"] = []
        monkeypatch.setattr(mod, "_fetch_fcc", lambda *a, item=refreshed, **k: (r for r in [item]))
        refreshed_out = mod.build_fcc_filings(refresh_dir, since=date(2026, 1, 1), generation_id="refresh-gen")
        row = next(r for r in pq.read_table(refreshed_out).to_pylist() if r["id_submission"] == "1001")
        assert row["text_data"] == "Refreshed" and row["generation_id"] == "refresh-gen"
        assert (row["extraction_results"] is None) is changed


def test_corrupt_kv_metadata_missing_crs_witness_and_non_1_to_1_legacy_read_refuse(tmp_path):
    from spicy_regs.crs_report_detail import check_detail_replacement
    from spicy_regs.transforms.government_source_shapes import (
        CRS_DETAIL_WITNESSES_METADATA,
        GAO_READINGS_METADATA,
        GovernmentShapeError,
        native_to_legacy_rows,
    )

    for dataset, key, sample in (
        ("gao_reports", GAO_READINGS_METADATA, {"report_id": "gao-26-1", "title": "T"}),
        ("crs_reports", CRS_DETAIL_WITNESSES_METADATA, {"report_id": "R1", "update_date": "2026-01-01T00:00:00Z"}),
    ):
        bad_schema = SUBJECT_SCHEMAS[dataset].with_metadata({**(SUBJECT_SCHEMAS[dataset].metadata or {}), key: b"not-valid-b64!"})
        bad_path = tmp_path / f"corrupt_{dataset}.parquet"
        pq.write_table(pa.Table.from_pylist([map_subject(dataset, sample)], schema=bad_schema), bad_path)
        with pytest.raises(GovernmentShapeError, match="Corrupt Parquet table KV metadata"):
            native_to_legacy_rows(dataset, bad_path)

    with pytest.raises(GovernmentShapeError, match="lacks its pinned witness"):
        check_detail_replacement({"detail_state": "ok", "detail_witness_json": None}, {"id": "R1"})

    fcc_path = tmp_path / "fcc_filings.parquet"
    pq.write_table(
        pa.Table.from_pylist([map_subject("fcc_filings", {"id_submission": "1"})], schema=SUBJECT_SCHEMAS["fcc_filings"]),
        fcc_path,
    )
    with pytest.raises(ValueError, match="non-1:1"):
        native_to_legacy_rows("fcc_filings", fcc_path)


def test_crs_reports_matches_receipt_route_and_offline_seams_run_without_receipts(tmp_path, monkeypatch):
    from spicy_regs import crs_file_preparation as prep
    from spicy_regs.crs_report_operations import prepare as ops_prepare

    mod = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    prior_doc = {
        "id": "R100", "title": "Prior CRS", "contentType": "Reports", "status": "Active",
        "publishDate": "2026-01-01T00:00:00Z", "updateDate": "2026-01-02T00:00:00Z", "version": 1,
        "url": "https://api.congress.gov/v3/crsreport/R100",
    }
    fresh_doc = {
        "id": "R100", "title": "Fresh CRS", "contentType": "Reports", "status": "Active",
        "publishDate": "2026-02-01T00:00:00Z", "updateDate": "2026-02-02T00:00:00Z", "version": 2,
        "url": "https://api.congress.gov/v3/crsreport/R100",
    }
    prior_src = _write_receipted_prior(tmp_path / "prior_src", "crs_reports", [mod._shape(prior_doc)])
    plain_prior = receipt_free_prior(prior_src, tmp_path / "plain_prior")
    build_dir = tmp_path / "plain"
    build_dir.mkdir()
    (build_dir / "_crs_prior.parquet").write_bytes((plain_prior / "crs_reports.parquet").read_bytes())

    monkeypatch.setattr(mod.r2, "download", lambda *_: False)
    monkeypatch.setattr(mod.CrsReportsReader, "iter_records", lambda self: iter([fresh_doc]))
    out = mod.build_crs_reports(build_dir)

    assert receipt_files(build_dir) == []
    expected = _receipt_route(tmp_path / "expected", "crs_reports", [mod._shape(fresh_doc), mod._shape(prior_doc)])
    assert table_differences(out, expected, key=IDENTITY_FIELDS["crs_reports"]) == []

    # Offline CRS operations + file preparation work on the retired receiptless table alone.
    detail_payload = {
        "CRSReport": {
            "id": "R100", "updateDate": fresh_doc["updateDate"], "currentVersion": 2, "title": "Fresh CRS",
            "summary": "Summary", "authors": [{"author": "Analyst"}], "topics": [{"topic": "Law"}],
            "formats": [{"format": "PDF", "url": "https://www.congress.gov/crs_external_products/R/PDF/R100/R100.2.pdf"}],
            "relatedMaterials": [],
        }
    }
    detail_bytes = json.dumps(detail_payload, separators=(",", ":")).encode()
    detail_sha = "sha256:" + hashlib.sha256(detail_bytes).hexdigest()
    CentralRawArchiveStore().put_blob(detail_sha, len(detail_bytes), [detail_bytes])
    detail_file = tmp_path / "detail.json"
    detail_file.write_bytes(detail_bytes)
    retained_spec = tmp_path / "retained.json"
    retained_spec.write_text(json.dumps([{"report_id": "R100", "path": detail_file.name, "sha256": detail_sha}]))

    ops_out = tmp_path / "ops_out"
    ops_prepare(
        ops_out,
        operation="detail",
        prior_subject=out,
        max_prior_bytes=1024 * 1024,
        max_prior_rows=10,
        max_requests=0,
        report=["R100@2026-02-02T00:00:00Z"],
        max_details=1,
        retained_details=retained_spec,
    )
    enriched = ops_out / "crs_reports.parquet"
    assert receipt_files(ops_out) == []

    prepared = prep.prepare_crs_report_files(
        enriched,
        None,
        ["R100"],
        subject_sha256=prep._sha(enriched),
        latest=True,
    )
    assert len(prepared["selection"]["reports"]) == 1
    assert prepared["coverage"]["revisions"][0]["disposition"] == "format-offered"


@pytest.mark.parametrize(
    "rollup_cls,sample_rows",
    [
        (SamEntitiesRollup, {"sam_entities": [{"uei": "UEI000000001", "legal_business_name": "Acme"}]}),
        (UsaSpendingRecipientsRollup, {"usaspending_recipients": [{"recipient_id": "R-1", "name": "Acme", "total_award_amount": "10"}]}),
        (GaoRecommendationsRollup, {"gao_recommendations": [{"recommendation_id": "REC-1", "report_id": "gao-26-1"}]}),
        (GaoReportsRollup, {
            "gao_reports": [{"report_id": "gao-26-1", "title": "T"}],
            "gao_decisions": [{"decision_number": "B-1", "url": "https://www.gao.gov/products/b-1"}],
        }),
        (FccFilingsRollup, {"fcc_filings": [{"id_submission": "1001"}]}),
        (CrsReportsRollup, {"crs_reports": [{"report_id": "R1", "update_date": "2026-01-01T00:00:00Z"}]}),
    ],
)
def test_all_six_government_rollups_admit_receiptless_generations_and_enforce_archive_pins(
    tmp_path, monkeypatch, rollup_cls, sample_rows
):
    gen_dir = tmp_path / "gen"
    files = []
    for ds, rows in sample_rows.items():
        path = tmp_path / f"{ds}.parquet"
        mapped = [map_subject(ds, row, generation_id="g1" if ds == "fcc_filings" else None) for row in rows]
        pq.write_table(pa.Table.from_pylist(mapped, schema=SUBJECT_SCHEMAS[ds]), path)
        files.append(path)
    artifact = build_generation(
        gen_dir,
        family=rollup_cls.name,
        files=files,
        expected_keys=[p.name for p in files],
        schemas={p.stem: described_schema(SUBJECT_SCHEMAS[p.stem]) for p in files},
    )
    verified = verify_generation(gen_dir, expected_pin=artifact.pin)
    assert "etlReceipts" not in verified.root["spec"]
    assert receipt_files(gen_dir) == []

    # Start-of-run archive pin guard refuses if live generation with receipts does not match docs/pins/receipt-archive.json.
    stale_index = {
        "families": {
            rollup_cls.name: {
                "artifactDigest": "sha256:" + "0" * 64,
                "etlReceipts": {"generationId": "stale-live", "datasets": list(sample_rows)},
                "tables": {p.name: {} for p in files},
            }
        }
    }
    monkeypatch.setenv("R2_PUBLIC_URL", "https://pin-check.invalid")
    monkeypatch.setattr(publication, "current_index", lambda *a, **k: stale_index)
    pipeline = rollup_cls(output_dir=tmp_path / "run", skip_upload=False)
    with pytest.raises(publication.PublicationError, match="receipt-archive.json"):
        pipeline.run()
