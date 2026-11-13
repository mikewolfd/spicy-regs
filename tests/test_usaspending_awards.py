"""Financial grain, full-snapshot replay and ordinary generation consumption."""

import csv
import io
import json
import zipfile

import duckdb
import httpx
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.usaspending_awards import digest, read_transaction_archive
from spicy_regs.generations import verify_generation
from spicy_regs.pipelines.usaspending_awards import build, capture, selected_archive
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_usaspending_awards import build_tables, money

NAME = "FY2025_513_Assistance_Full_test.zip"
URL = "https://files.usaspending.gov/award_data_archive/" + NAME
ROW = {
    "assistance_transaction_unique_key": "T1",
    "assistance_award_unique_key": "A1",
    "action_date": "2025-01-01",
    "action_date_fiscal_year": "2025",
    "federal_action_obligation": "100.01",
    "total_obligated_amount": "80.00",
    "total_outlayed_amount_for_overall_award": "",
    "awarding_agency_code": "513",
    "funding_agency_code": "513",
    "last_modified_date": "2026-01-01",
    "recipient_uei": "",
    "recipient_name": "EXAMPLE",
    "recipient_state_code": "MT",
    "primary_place_of_performance_state_name": "ALASKA",
    "modification_number": "0",
    "correction_delete_indicator_code": "",
    "record_type_code": "2",
    "assistance_type_code": "04",
}


def archive(rows):
    csv_body = io.StringIO(newline="")
    writer = csv.DictWriter(csv_body, fieldnames=list(rows[0]) if rows else list(ROW))
    writer.writeheader()
    writer.writerows(rows)
    body = io.BytesIO()
    with zipfile.ZipFile(body, "w", compression=zipfile.ZIP_DEFLATED) as z:
        z.writestr("records.csv", csv_body.getvalue())
    return body.getvalue()


def selection(body):
    return {
        "schema": "usaspending-full-transactions/1",
        "snapshot_kind": "full",
        "agency_id": 120,
        "agency_code": "513",
        "fiscal_year": 2025,
        "award_family": "assistance",
        "archive": {"sha256": digest(body), "url": URL, "updated_date": "2026-11-06"},
    }


def tables(rows, output):
    body = archive(rows)
    literal = read_transaction_archive(body, sha256=digest(body), award_family="assistance")
    return build_tables(literal, output, selection=selection(body))


def test_obligations_award_totals_outlays_and_locations_have_distinct_grains(tmp_path):
    files = tables(
        [
            ROW,
            {
                **ROW,
                "assistance_transaction_unique_key": "T2",
                "federal_action_obligation": "-20.01",
                "modification_number": "1",
            },
        ],
        tmp_path,
    )
    with duckdb.connect() as db:
        tx = str(files[0])
        total = db.execute("select sum(federal_action_obligation) from read_parquet(?)", [tx]).fetchone()
        assert total is not None and str(total[0]) == "80.00"
        assert (
            db.execute("select count(publisher_overall_award_outlayed_amount) from read_parquet(?)", [tx]).fetchone()
            == (0,)
        )
        award = pq.read_table(files[1]).to_pylist()[0]
        assert str(award["publisher_overall_award_obligated_amount"]) == "80.00"
        assert award["selected_transaction_count"] == 2
    raw = pq.read_table(files[0]).to_pylist()[0]
    assert raw["recipient_state_code"] == "MT" and raw["place_of_performance_state_name"] == "ALASKA"
    assert json.loads(raw["raw_cells_json"])["total_outlayed_amount_for_overall_award"] == ""


@pytest.mark.parametrize(
    "changed,reason",
    [
        ({"federal_action_obligation": "1.001"}, "decimal"),
        ({"federal_action_obligation": ""}, "decimal"),
        ({"action_date_fiscal_year": "2024"}, "outside"),
        ({"action_date": "2024-09-30"}, "outside"),
        ({"awarding_agency_code": "012"}, "outside"),
        ({"assistance_type_code": "A"}, "family"),
        ({"correction_delete_indicator_code": "D"}, "deleted"),
        ({"correction_delete_indicator_code": "unknown"}, "unknown correction"),
        ({"record_type_code": "1"}, "Aggregate"),
        ({"assistance_transaction_unique_key": ""}, "identity"),
    ],
)
def test_bad_values_population_or_delete_refuse_without_tables(tmp_path, changed, reason):
    with pytest.raises(ValueError, match=reason):
        tables([{**ROW, **changed}], tmp_path)
    assert not list(tmp_path.glob("*.parquet"))


@pytest.mark.parametrize(
    "other",
    [
        ROW,
        {**ROW, "assistance_transaction_unique_key": "T2", "total_obligated_amount": "81.00"},
        {**ROW, "assistance_transaction_unique_key": "T2", "total_outlayed_amount_for_overall_award": "0.00"},
    ],
)
def test_duplicate_revisions_and_conflicting_award_totals_refuse(tmp_path, other):
    with pytest.raises(ValueError):
        tables([ROW, other], tmp_path)
    assert not list(tmp_path.glob("*.parquet"))


def test_full_capture_replay_generation_empty_and_tamper_controls(tmp_path, monkeypatch):
    original = archive([ROW])
    item = {
        "agency_name": "Denali Commission",
        "agency_acronym": "DENALI",
        "fiscal_year": 2025,
        "type": "assistance",
        "file_name": NAME,
        "url": URL,
        "updated_date": "2026-11-06",
    }
    calls = []

    def response(request):
        calls.append(request)
        return (
            httpx.Response(200, json={"monthly_files": [item]})
            if request.method == "POST"
            else httpx.Response(200, content=original)
        )

    raw = tmp_path / "raw"
    evidence = CaptureEvidence(tmp_path / "acquire", "usaspending-awards", archive_root=raw)
    selected = capture(
        evidence,
        agency_id=120,
        agency_code="513",
        fiscal_year=2025,
        award_family="assistance",
        file_name=NAME,
        transport=httpx.MockTransport(response),
    )
    evidence.finish()
    assert len(calls) == 2
    first, first_evidence = build(selected, tmp_path / "first", archive_root=raw)
    second, _ = build(selected, tmp_path / "second", archive_root=raw)
    assert len(calls) == 2  # Offline unchanged replay performs no network requests.
    verify_generation(tmp_path / "first" / "generation", expected_pin=first.pin)
    verify_generation(tmp_path / "second" / "generation", expected_pin=second.pin)
    assert first.root["spec"]["publicationStatus"] == "complete-family"
    events = [json.loads(line) for line in (first_evidence / "journal.jsonl").read_text().splitlines()]
    (cohort,) = [event for event in events if event["event"] == "usaspending-cohort"]
    (declared,) = [event["manifest"] for event in events if event["event"] == "selected-inputs"]
    assert cohort["complete"] and cohort["scope"]["finite"]
    assert cohort["scope"]["awarding_agency_code"] == "513"
    assert cohort["scope"]["awarding_agency_database_id"] == 120
    assert cohort["scope"]["fiscal_year"] == 2025 and cohort["scope"]["award_family"] == "assistance"
    assert cohort["scope"]["geographic_filter"] is None
    assert cohort["scope"]["archive_sha256"] == digest(original)
    assert cohort["table_rows"] == dict.fromkeys(
        ("usaspending_transactions", "usaspending_awards", "usaspending_archive_members"), 1
    )
    assert declared["scope"] == {"id": cohort["scope"]["id"], "finite": True}
    assert declared["complete"] and len(declared["used"]) == 2
    assert digest(original) in {source["sha256"] for source in declared["used"]}
    assert (
        pq.read_table(tmp_path / "first/generation/usaspending_awards.parquet").to_pylist()[0][
            "publisher_overall_award_outlayed_amount"
        ]
        is None
    )
    for name in ("usaspending_transactions", "usaspending_awards", "usaspending_archive_members"):
        assert pq.read_table(tmp_path / "first/generation" / f"{name}.parquet").equals(
            pq.read_table(tmp_path / "second/generation" / f"{name}.parquet")
        )
    with pytest.raises(ValueError, match="request"):
        selected_archive({**selected, "fiscal_year": 2024}, evidence.store)
    with pytest.raises((ValueError, FileNotFoundError)):
        selected_archive(
            {**selected, "archive": {**selected["archive"], "sha256": "sha256:" + "0" * 64}}, evidence.store
        )
    with pytest.raises(ValueError, match="full"):
        selected_archive({**selected, "snapshot_kind": "delta"}, evidence.store)
    # A later converter bug cannot label a source cohort complete after dropping a native record.
    from spicy_regs.pipelines import usaspending_awards as pipeline

    original_builder = pipeline.build_tables

    def drop_record(literal, output, *, selection):
        paths = original_builder(literal, output, selection=selection)
        pq.write_table(pq.read_table(paths[0]).slice(0, 0), paths[0])
        return paths

    with monkeypatch.context() as broken:
        broken.setattr(pipeline, "build_tables", drop_record)
        with pytest.raises(ValueError, match="retain every original"):
            pipeline.build(selected, tmp_path / "dropped-record", archive_root=raw)
    assert not (tmp_path / "dropped-record/generation/artifact.json").exists()
    (failed_journal,) = (tmp_path / "dropped-record/source-evidence").glob("*/artifact/journal.jsonl")
    assert all(json.loads(line)["event"] != "selected-inputs" for line in failed_journal.read_text().splitlines())
    empty = tmp_path / "empty"
    paths = tables([], empty)
    assert pq.read_table(paths[0]).num_rows == pq.read_table(paths[1]).num_rows == 0
    assert pq.read_table(paths[2]).num_rows == 1


def test_exact_large_decimal_is_not_a_float():
    assert str(money("9007199254740993.01")) == "9007199254740993.01"
    assert money("") is None
    with pytest.raises(ValueError):
        money("NaN")


def test_refreshed_full_version_replaces_cohort_instead_of_appending(tmp_path):
    """An amended source with the same native key contributes only its new amount."""
    raw = tmp_path / "raw"
    candidates = []
    calls = []
    for version, amount in (("2026-10-06", "100.01"), ("2026-11-06", "75.00")):
        original = archive([{**ROW, "federal_action_obligation": amount, "total_obligated_amount": amount}])
        item = {
            "agency_name": "Denali Commission",
            "agency_acronym": "DENALI",
            "fiscal_year": 2025,
            "type": "assistance",
            "file_name": NAME,
            "url": URL,
            "updated_date": version,
        }

        def response(request):
            calls.append(request)
            return (
                httpx.Response(200, json={"monthly_files": [item]})
                if request.method == "POST"
                else httpx.Response(200, content=original)
            )

        evidence = CaptureEvidence(tmp_path / version, "usaspending-awards", archive_root=raw)
        selected = capture(
            evidence,
            agency_id=120,
            agency_code="513",
            fiscal_year=2025,
            award_family="assistance",
            file_name=NAME,
            transport=httpx.MockTransport(response),
        )
        evidence.finish()
        destination = tmp_path / (version + "-candidate")
        build(selected, destination, archive_root=raw)
        candidates.append(pq.read_table(destination / "generation/usaspending_transactions.parquet").to_pylist())
    assert len(calls) == 4
    assert len(candidates[0]) == len(candidates[1]) == 1
    assert candidates[0][0]["transaction_key"] == candidates[1][0]["transaction_key"]
    assert str(candidates[0][0]["federal_action_obligation"]) == "100.01"
    assert str(candidates[1][0]["federal_action_obligation"]) == "75.00"
    assert candidates[0][0]["archive_sha256"] != candidates[1][0]["archive_sha256"]
