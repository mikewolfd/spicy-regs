"""Explicit CRS operations through native receipts, CLI and consumer queries."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime

import duckdb
import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import read_with_receipts
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_crs_reports import _SCHEMA, CrsOperationLimitError, _shape, build_crs_reports
from spicy_regs.transforms.government_receipts import POLICIES, migrate_outputs
from spicy_regs.transforms.government_source_shapes import native_to_legacy_rows
from tests.test_crs_report_detail import DETAIL, LIST


@pytest.fixture
def prior(tmp_path):
    directory = tmp_path / "selected"
    directory.mkdir()
    path = directory / "crs_reports.parquet"
    reports = [
        LIST,
        {**LIST, "updateDate": "2026-10-02T00:00:00Z", "version": 3},
        {**LIST, "id": "R2", "title": "Unselected report"},
    ]
    pq.write_table(pa.Table.from_pylist([_shape(report) for report in reports], schema=_SCHEMA), path)
    migrate_outputs((path,), generation_id="selected")
    return path, directory / "etl_receipts.parquet"


def selected_build(prior, output, **kwargs):
    output.mkdir(exist_ok=True)
    return build_crs_reports(
        output,
        prior_subject=prior[0],
        prior_receipt_path=prior[1],
        prior_generation_id="selected",
        refresh_list=False,
        detail_limit=1,
        detail_reports=["R1@2026-10-01T00:00:00Z"],
        max_requests=0,
        **kwargs,
    )


@pytest.mark.parametrize("unselected_failed", [False, True])
def test_installed_local_command_enriches_exact_old_revision_and_repeats_without_requests(
    prior, tmp_path, unselected_failed
):
    failed_raw = None
    if unselected_failed:
        rows = [
            row["raw_record"]
            for row in read_with_receipts([prior[0]], [prior[1]], POLICIES["crs_reports"], generation_id="selected")
        ]
        failed_raw = next(row for row in rows if row["report_id"] == "R2")
        failed_raw.update(
            detail_attempt_state="failed",
            detail_error="prior source unavailable",
            detail_attempt_witness_json=json.dumps(
                [
                    {
                        "source_id": "congress.gov:crs-detail",
                        "source_uri": "https://api.congress.gov/v3/crsreport/R2",
                        "sha256": "sha256:" + hashlib.sha256(b"prior source unavailable").hexdigest(),
                        "locator": None,
                        "body_version": None,
                    }
                ]
            ),
        )
        failed_directory = tmp_path / "selected-with-failure"
        failed_directory.mkdir()
        prior = failed_directory / "crs_reports.parquet", failed_directory / "etl_receipts.parquet"
        pq.write_table(pa.Table.from_pylist(rows, schema=_SCHEMA), prior[0])
        migrate_outputs((prior[0],), generation_id="selected")
    original = tmp_path / "detail.json"
    body = json.dumps({"CRSReport": {**DETAIL, "topics": [{"topic": "Health"}, {"topic": "Health"}, None]}}).encode()
    original.write_bytes(body)
    selection = tmp_path / "retained.json"
    selection.write_text(
        json.dumps([{"report_id": "R1", "path": str(original), "sha256": "sha256:" + hashlib.sha256(body).hexdigest()}])
    )
    before = [path.read_bytes() for path in prior]
    output = tmp_path / "candidate"
    command = [
        sys.executable,
        "-m",
        "spicy_regs.crs_report_operations",
        str(output),
        "--operation",
        "detail",
        "--prior-subject",
        str(prior[0]),
        "--prior-receipts",
        str(prior[1]),
        "--prior-generation-id",
        "selected",
        "--max-prior-bytes",
        str(1024 * 1024),
        "--max-prior-rows",
        "10",
        "--max-requests",
        "0",
        "--max-details",
        "1",
        "--report",
        "R1@2026-10-01T00:00:00Z",
        "--retained-details",
        str(selection),
    ]
    result = subprocess.run(command, check=False, capture_output=True, text=True, env=dict(os.environ), timeout=90)
    assert result.returncode == 0, result.stderr
    status = json.loads(result.stdout)
    assert status["coverage"]["origin_requests"] == 0
    assert status["coverage"]["discovery"] == "not_requested"
    assert status["coverage"]["detail_attempted"] == 1
    assert status["coverage"]["detail_failed"] == 0
    assert status["coverage"]["retained_detail_failed"] == int(unselected_failed)
    assert status["coverage"]["prior_inputs"]["members"] == [
        {"name": path.name, "sha256": "sha256:" + hashlib.sha256(body).hexdigest(), "bytes": len(body), "rows": 3}
        for path, body in zip(prior, before, strict=True)
    ]
    assert [path.read_bytes() for path in prior] == before
    native = output / "crs_reports.parquet"
    with duckdb.connect(config={"threads": 2}) as con:
        assert con.execute(
            "SELECT report_id, update_date, version, summary, topics FROM read_parquet(?) "
            "ORDER BY report_id, update_date",
            [str(native)],
        ).fetchall() == [
            (
                "R1",
                datetime(2026, 10, 1, tzinfo=UTC),
                2,
                "Source summary",
                [{"topic": "Health"}, {"topic": "Health"}, None],
            ),
            ("R1", datetime(2026, 10, 2, tzinfo=UTC), 3, None, None),
            ("R2", datetime(2026, 10, 1, tzinfo=UTC), 2, None, None),
        ]
    assert not (output / "etl_receipts.parquet").exists()
    assert not (output / ".government-etl-build.json").exists()
    if unselected_failed:
        [failed] = [row for row in native_to_legacy_rows("crs_reports", native) if row["report_id"] == "R2"]
        assert failed["detail_attempt_state"] == "failed"
        assert failed["detail_error"] == "prior source unavailable"
    second = tmp_path / "unchanged"
    repeat = [
        sys.executable,
        "-m",
        "spicy_regs.crs_report_operations",
        str(second),
        "--operation",
        "detail",
        "--prior-subject",
        str(native),
        "--max-prior-bytes",
        str(1024 * 1024),
        "--max-prior-rows",
        "10",
        "--max-requests",
        "0",
        "--max-details",
        "1",
        "--report",
        "R1@2026-10-01T00:00:00Z",
        "--retained-details",
        str(selection),
    ]
    result = subprocess.run(repeat, check=False, capture_output=True, text=True, env=dict(os.environ), timeout=90)
    assert result.returncode == 0, result.stderr
    repeated_status = json.loads(result.stdout)["coverage"]
    assert repeated_status["detail_attempted"] == repeated_status["detail_failed"] == 0
    assert repeated_status["retained_detail_failed"] == int(unselected_failed)
    assert pq.read_table(second / "crs_reports.parquet").equals(pq.read_table(native))
    assert not (second / "etl_receipts.parquet").exists()


@pytest.mark.parametrize(
    "selection,latest",
    [(["R404"], True), (["R1"], False), (["R1@2026-01-01T00:00:00Z"], False), (["R1@bad"], False), (["../R1"], True)],
)
def test_invalid_selection_refuses_before_receipt_restoration_or_requests(
    prior, tmp_path, monkeypatch, selection, latest
):
    from spicy_regs.transforms import government_receipts as gr

    def forbidden(*args, **kwargs):
        pytest.fail("Invalid selection must not restore inherited receipts or request source data")

    monkeypatch.setattr(gr, "select_receipts", forbidden)
    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    monkeypatch.setattr(module, "CrsReportsReader", forbidden)
    before = [path.read_bytes() for path in prior]
    with pytest.raises(ValueError):
        build_crs_reports(
            tmp_path,
            prior_subject=prior[0],
            prior_receipt_path=prior[1],
            prior_generation_id="selected",
            refresh_list=False,
            detail_limit=1,
            detail_reports=selection,
            latest=latest,
            max_requests=0,
        )
    assert [path.read_bytes() for path in prior] == before
    assert not (tmp_path / "crs_reports.parquet").exists()


@pytest.mark.parametrize("bound", ["bytes", "rows", "decoded"])
def test_inherited_pair_is_admitted_before_receipt_selection_and_restore(prior, tmp_path, monkeypatch, bound):
    from spicy_regs.transforms import government_receipts as gr

    def forbidden(*args, **kwargs):
        pytest.fail("Over-limit inputs must not restore or select inherited receipts")

    monkeypatch.setattr(gr, "select_receipts", forbidden)
    size = sum(path.stat().st_size for path in prior)
    options = {"max_prior_bytes": size - 1} if bound == "bytes" else {"max_prior_rows": 2}
    if bound == "decoded":
        # Rewrite valid Parquet containers with a highly compressible diagnostic;
        # metadata admission must refuse before receipt content is interpreted.
        table = pq.read_table(prior[1]).append_column("extra", pa.array(["x" * 100000] * 3))
        pq.write_table(table, prior[1], compression="zstd")
        options = {"max_prior_bytes": sum(path.stat().st_size for path in prior)}
    with pytest.raises(ValueError, match="bytes exceed|rows exceed"):
        selected_build(prior, tmp_path / "candidate", **options)


def test_failed_explicit_retry_preserves_qualified_values_clocks_and_unselected_revisions(
    prior, tmp_path, monkeypatch
):
    body = json.dumps({"CRSReport": DETAIL}).encode()
    original = tmp_path / "detail.json"
    original.write_bytes(body)
    qualified = selected_build(
        prior, tmp_path / "qualified", retained_details={"R1": (original, "sha256:" + hashlib.sha256(body).hexdigest())}
    )
    output = tmp_path / "failed"
    output.mkdir()
    asked = []

    def failed(request):
        asked.append(request)
        return httpx.Response(404, json={"error": "gone"})

    evidence = CaptureEvidence(output, "crs-reports")
    cursor = "2026-11-05T13:00:00+00:00"
    monkeypatch.setattr(evidence, "inherited_event", lambda *args, **kwargs: {"cursor": cursor})
    path = build_crs_reports(
        output,
        prior_subject=qualified,
        refresh_list=False,
        detail_limit=1,
        detail_reports=["R1@2026-10-01T00:00:00Z"],
        retry_completed=True,
        detail_api_key="test-key",
        detail_transport=httpx.MockTransport(failed),
        max_requests=1,
        evidence=evidence,
    )
    assert len(asked) == 1
    rows = pq.read_table(path).to_pylist()
    old = pq.read_table(qualified).to_pylist()
    for row, previous in zip(rows, old, strict=True):
        assert {key: value for key, value in row.items() if key not in {"detail_attempt_state", "detail_error"}} == {
            key: value for key, value in previous.items() if key not in {"detail_attempt_state", "detail_error"}
        }
    assert sum(row["detail_attempt_state"] == "failed" for row in rows) == 1
    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    discovery = [event for event in journal if event["event"] == "crs-discovery"][-1]
    assert discovery["cursor"] == cursor and discovery["mode"] == "detail-only"
    assert not (output / "etl_receipts.parquet").exists()


@pytest.mark.parametrize("limit", ["requests", "bytes", "rows"])
def test_reconciliation_limit_preserves_prior_and_never_commits_cursor(prior, tmp_path, monkeypatch, limit):
    monkeypatch.setenv("API_GOV", "test-key")
    output = tmp_path / "candidate"
    output.mkdir()
    evidence = CaptureEvidence(output, "crs-reports")
    asked = []

    def listing(request):
        asked.append(request)
        return httpx.Response(200, json={"pagination": {"count": 2}, "CRSReports": [LIST, {**LIST, "id": "R2"}]})

    with pytest.raises(CrsOperationLimitError):
        build_crs_reports(
            output,
            prior_subject=prior[0],
            prior_receipt_path=prior[1],
            prior_generation_id="selected",
            reconcile=True,
            evidence=evidence,
            list_transport=httpx.MockTransport(listing),
            max_requests=0 if limit == "requests" else 10000,
            max_list_bytes=10 if limit == "bytes" else 128 * 1024 * 1024,
            max_list_rows=1 if limit == "rows" else 100000,
        )
    assert len(asked) == (0 if limit == "requests" else 1)
    assert not (output / "crs_reports.parquet").exists()
    journal = (evidence.artifact_dir / "journal.jsonl").read_text()
    assert '"event":"crs-discovery"' not in journal and '"event": "crs-discovery"' not in journal


@pytest.mark.parametrize(
    "kwargs",
    [
        {"reconcile": True, "refresh_list": False},
        {"reconcile": True, "since": datetime(2020, 1, 1).date()},
        {"detail_reports": ["R1"], "detail_limit": 1},
        {"latest": True},
        {"max_requests": -1},
    ],
)
def test_invalid_operation_combinations_refuse_before_reading_prior(tmp_path, monkeypatch, kwargs):
    module = importlib.import_module("spicy_regs.transforms.build_crs_reports")
    monkeypatch.setattr(module.r2, "download", lambda *_: pytest.fail("Invalid combinations must precede prior reads"))
    with pytest.raises(ValueError):
        build_crs_reports(tmp_path, **kwargs)


def test_intentional_reconciliation_uses_unfiltered_inventory_without_deleting_history(prior, tmp_path, monkeypatch):
    monkeypatch.setenv("API_GOV", "test-key")
    output = tmp_path / "reconcile"
    output.mkdir()
    evidence = CaptureEvidence(output, "crs-reports")
    asked = []

    def listing(request):
        asked.append(request)
        return httpx.Response(
            200,
            json={"pagination": {"count": 2}, "CRSReports": [LIST, {**LIST, "id": "R2", "title": "Unselected report"}]},
        )

    coverage = {}
    path = build_crs_reports(
        output,
        prior_subject=prior[0],
        prior_receipt_path=prior[1],
        prior_generation_id="selected",
        reconcile=True,
        evidence=evidence,
        list_transport=httpx.MockTransport(listing),
        max_requests=1,
        max_list_bytes=4096,
        max_list_rows=2,
        coverage=coverage,
    )
    assert len(asked) == 1 and "fromDateTime" not in asked[0].url.params
    assert coverage["scope"] == "native-inventory" and coverage["listed"] == 2

    # One later revision absent from the current inventory remains available.
    def key(row):
        return row["report_id"], row["update_date"]

    assert sorted(pq.read_table(path).to_pylist(), key=key) == sorted(pq.read_table(prior[0]).to_pylist(), key=key)
    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    discovery = [event for event in journal if event["event"] == "crs-discovery"][-1]
    assert discovery["mode"] == "reconcile" and discovery["cursor"] is not None


def test_local_command_records_failed_candidate_before_reporting_failure(prior, tmp_path, monkeypatch):
    from spicy_regs.crs_report_operations import prepare
    from spicy_regs.sources.crs_reports import CrsReportDetailReader

    monkeypatch.setattr(
        CrsReportDetailReader,
        "read",
        lambda *_: {"state": "failed", "error": "controlled source unavailability", "witnesses": []},
    )
    output = tmp_path / "failed-candidate"
    with pytest.raises(RuntimeError, match="candidate prepared with failed details"):
        prepare(
            output,
            operation="detail",
            prior_subject=prior[0],
            prior_receipts=prior[1],
            prior_generation_id="selected",
            max_prior_bytes=1024 * 1024,
            max_prior_rows=10,
            max_requests=0,
            report=["R1@2026-10-01T00:00:00Z"],
            max_details=1,
        )
    result = json.loads((output / "crs-operation.json").read_text())
    assert result["coverage"]["detail_failed"] == 1
    assert result["coverage"]["retained_detail_failed"] == 1
    assert pq.read_metadata(output / "crs_reports.parquet").num_rows == 3
