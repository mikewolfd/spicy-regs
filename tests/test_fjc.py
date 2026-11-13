"""Source-backed judicial careers through generation, metadata and ordinary SQL consumers."""

from copy import deepcopy
import csv
from datetime import date
import hashlib
import io
import json
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest

from spicy_docs.sources.fjc import EXPORT_URL, fjc_scope
from spicy_docs.sources.fjc_profile import FJC_PROFILE, iter_retained_pages
from spicy_docs.storage.blobs import LocalSourceNativeBlobStore
from spicy_docs.transport.download import HttpRefusal
from spicy_regs import data_dictionary, mcp_server
from spicy_regs.explorer_metadata import build_bundle
from spicy_regs.pipelines import fjc as pipeline
from spicy_regs.pipelines.raw_releases import admit_release
from spicy_regs.sources import publication
from spicy_regs.transforms.build_fjc import SCHEMAS, build_fjc
from tests.generation_fakes import Store

RAW = (Path(__file__).parent / "fixtures/fjc/judges-excerpt.csv").read_bytes()


def reader(tmp_path, raw=RAW, *, observed="2026-11-13T11:37:19Z"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    scope = fjc_scope({"request_url": EXPORT_URL, "response_sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
                       "byte_size": len(raw), "observed_at": observed})
    blobs = LocalSourceNativeBlobStore(tmp_path / "blobs")
    blobs.put_blob(scope["capture"]["response_sha256"], len(raw), (raw,))
    admitted = admit_release(FJC_PROFILE, iter_retained_pages(scope, blob_source=blobs), scope, tmp_path / "release",
                             blobs=blobs, observed=[observed])
    original = tmp_path / "judges.csv"
    original.write_bytes(raw)
    return admitted, {scope["capture"]["response_sha256"]: original}


def mutated(**changes):
    rows = list(csv.DictReader(io.StringIO(RAW.decode())))
    for key, value in changes.items():
        rows[0][key] = value
    text = io.StringIO(newline="")
    writer = csv.DictWriter(text, fieldnames=list(rows[0]))
    writer.writeheader()
    writer.writerows(rows)
    return text.getvalue().encode()


def test_native_multiple_commissions_reassignment_recess_and_ordinary_consumer(tmp_path, monkeypatch):
    source, archives = reader(tmp_path / "source")
    output = tmp_path / "build"
    artifact, evidence = pipeline.build([source], output, archives=archives)
    index = publication.publish_generation(output / "generation", client=Store(), bucket="fixture",
                                           prior_index=publication.empty_index(), evidence_directories=(evidence,))
    assert index["families"]["fjc-judicial-careers"]["artifactDigest"] == artifact.pin.artifact_digest
    metadata = build_bundle(index)
    assert set(SCHEMAS) <= data_dictionary.MCP_QUERYABLE
    for name in SCHEMAS:
        assert metadata["tables"][name]["metadataStatus"] == "documented"
        assert metadata["tables"][name]["sources"][0]["id"] == "fjc"
    monkeypatch.setattr(mcp_server, "DATA_DIR", output / "generation")
    with mcp_server._build_connection() as connection:
        rows = connection.execute("""
            SELECT j.nid, a.service_ordinal, c.court_name_raw, a.appointment_kind,
                   a.appointing_president_raw, a.senate_vote_type_raw, a.ayes_nays_raw,
                   a.commission_date, a.recess_appointment_date, s.termination_date,
                   s.termination_reason_raw, s.vacancy_date, c.external_court_id
            FROM fjc_judges j JOIN fjc_appointments a USING(snapshot_id,nid)
            JOIN fjc_service_terms s USING(snapshot_id,nid,service_ordinal,appointment_id,court_label_id,seat_id_raw)
            JOIN fjc_courts c USING(snapshot_id,court_label_id)
            ORDER BY j.nid,a.service_ordinal
        """).fetchall()
    ackerman = [row for row in rows if row[0] == "1376996"]
    assert ackerman[0][2] == "U.S. District Court for the Southern District of Illinois"
    assert ackerman[1][2] == "U.S. District Court for the Central District of Illinois"
    assert ackerman[1][3:5] == ("reassignment", "None (reassignment)")
    assert ackerman[0][5:7] == ("Voice", "  ")
    ginsburg = [row for row in rows if row[0] == "1381271"]
    assert ginsburg[1][7] == date(1993, 8, 5) and ginsburg[0][9] == date(1993, 8, 10)
    rehnquist = [row for row in rows if row[0] == "1386831"]
    assert len(rehnquist) == 2 and rehnquist[0][2] == rehnquist[1][2]
    andrews = [row for row in rows if row[0] == "1390306"][0]
    assert andrews[7] is None and andrews[8] == date(1949, 10, 21)
    assert andrews[10] == "Recess Appointment-Not Confirmed"
    assert all(row[-2:] == (None, None) for row in rows)
    assert pq.read_table(output / "generation/fjc_snapshots.parquet").to_pylist()[0]["service_count"] == 7


def test_invalid_partial_dates_and_senior_status_are_preserved_without_imputation(tmp_path):
    raw = mutated(**{"Nomination Date (1)": "1976", "Senior Status Date (1)": "1982-02-03",
                     "Commission Date (1)": "7/2/1976"})
    source, _ = reader(tmp_path / "source", raw)
    paths = build_fjc([source], tmp_path / "tables")
    appointments = pq.read_table(paths[1]).to_pylist()
    services = pq.read_table(paths[2]).to_pylist()
    assert appointments[0]["nomination_date"] is None
    assert json.loads(appointments[0]["date_status_json"])["nomination_date"] == "unresolved-literal"
    assert json.loads(appointments[0]["raw_appointment_json"])["Nomination Date"] == "1976"
    assert appointments[0]["commission_date"] == date(1976, 7, 2)
    assert services[0]["senior_status_date"] == date(1982, 2, 3)
    assert services[0]["termination_date"] == date(1979, 3, 31)
    assert services[0]["vacancy_date"] is None


def test_selected_refresh_keeps_changed_and_absent_observations_separate(tmp_path):
    before, _ = reader(tmp_path / "before")
    raw = mutated(**{"Gender": "Unknown"})
    rows = list(csv.reader(io.StringIO(raw.decode())))
    text = io.StringIO(newline="")
    csv.writer(text).writerows(rows[:-1])
    after, _ = reader(tmp_path / "after", text.getvalue().encode(), observed="2026-11-14T11:00:00Z")
    paths = build_fjc([before, after], tmp_path / "tables")
    judges = pq.read_table(paths[0]).to_pylist()
    assert len(judges) == 7
    assert [row["gender_raw"] for row in judges if row["nid"] == "1376996"] == ["Male", "Unknown"]
    assert [row["snapshot_id"] for row in judges if row["nid"] == "1386831"] == [before.pin.artifact_digest]


@pytest.mark.parametrize("defect", ["failed", "short", "duplicate", "capture", "court", "stream"])
def test_refused_or_interrupted_input_installs_no_partial_tables(tmp_path, defect):
    source, _ = reader(tmp_path / "source")

    class Altered:
        pin = source.pin
        collection_outcome = deepcopy(source.collection_outcome)

        def iter_records(self):
            rows = list(source.iter_records())
            if defect == "capture":
                rows[0]["record"]["source_locator"]["response_sha256"] = "sha256:" + "0" * 64
            if defect == "court":
                rows[0]["record"]["fields"]["Court Name (1)"] = ""
            yield from rows
            if defect == "stream":
                raise OSError("source stream interrupted")

    altered = Altered()
    if defect == "failed":
        altered.collection_outcome["failedRecordCount"] = 1
    if defect == "short":
        altered.collection_outcome["discoveredRecordCount"] += 1
    with pytest.raises((ValueError, OSError)):
        build_fjc([altered, altered] if defect == "duplicate" else [altered], tmp_path / "tables")
    assert not list((tmp_path / "tables").glob("*.parquet"))


def test_modified_original_refuses_and_prior_outputs_are_preserved(tmp_path):
    source, archives = reader(tmp_path / "source")
    next(iter(archives.values())).write_bytes(b"!" * len(RAW))
    with pytest.raises(ValueError, match="digest"):
        pipeline.build([source], tmp_path / "failed", archives=archives)
    assert not (tmp_path / "failed/generation").exists()
    assert not list((tmp_path / "failed").glob("*.parquet"))
    output = tmp_path / "held"
    output.mkdir()
    held = output / "fjc_service_terms.parquet"
    held.write_bytes(b"foreign")
    with pytest.raises(ValueError, match="fresh"):
        build_fjc([source], output)
    assert held.read_bytes() == b"foreign"


def test_explicit_empty_and_credential_refusal_remain_distinct(tmp_path):
    header = RAW[:RAW.index(b"\n") + 1]
    source, _ = reader(tmp_path / "empty", header)
    paths = build_fjc([source], tmp_path / "tables")
    assert pq.read_table(paths[0]).num_rows == 0
    assert pq.read_table(paths[4]).to_pylist()[0]["judge_count"] == 0
    with pytest.raises(HttpRefusal):
        pipeline.acquire(tmp_path / "refused", transport=httpx.MockTransport(lambda request: httpx.Response(403)))
    journals = list((tmp_path / "refused/source-evidence").glob("*/artifact/journal.jsonl"))
    assert journals and any('403' in p.read_text() for p in journals)
