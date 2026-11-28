"""Parity and seam proof for retiring per-row ETL receipts on the five FEC identity/links families (ss-vse3)."""

from __future__ import annotations

from datetime import UTC, date, datetime
import io
import json
from pathlib import Path
import zipfile

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.pipelines.rollups.fec_candidate_history import FecCandidateHistoryRollup
from spicy_regs.pipelines.rollups.fec_committee_history import FecCommitteeHistoryRollup
from spicy_regs.pipelines.rollups.fec_committees import FecCommitteesRollup
from spicy_regs.pipelines.rollups.fec_source_catalog import FecSourceCatalogRollup
from spicy_regs.pipelines.rollups.fec_tables import FecTableRollup, published_as_fec_source
from spicy_regs.pipelines.rollups.org_committee_links import OrgCommitteeLinksRollup
from spicy_regs.receipt_switch import receipt_files, receipt_free_prior, table_differences
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms import build_fec_committees as committees_mod
from spicy_regs.transforms import build_fec_source_catalog as catalog_mod
from spicy_regs.transforms import fec_master_history
from spicy_regs.transforms.build_fec_identity_rollup import build_fec_identity_rollup
from spicy_regs.transforms.fec_identity_context_fields import subject_schema

_C1 = {
    "committee_id": "C00030718", "name": "NATIONAL ASSOCIATION OF REALTORS POLITICAL ACTION COMMITTEE",
    "committee_type": "Q", "committee_type_full": "PAC - Qualified", "designation": "U",
    "designation_full": "Unauthorized", "party": None, "party_full": None, "state": "DC",
    "treasurer_name": "DOE, JANE", "organization_type_full": "Trade Association", "filing_frequency": "M",
    "first_file_date": "1977-01-01", "last_file_date": "2026-09-25",
    "cycles": [2022, 2024, 2026], "candidate_ids": ["H0CA00001", "NONSTD"],
}
_C2 = {
    **_C1, "committee_id": "C00104802", "name": "DELTA AIR LINES PAC", "designation": "B",
    "designation_full": "Lobbyist/Registrant PAC", "organization_type_full": "Corporation",
    "first_file_date": "1980-03-01", "last_file_date": "2024-06-15", "cycles": [], "candidate_ids": [],
}
_C3 = {
    **_C1, "committee_id": "C00999999", "name": "NEW COMMITTEE PAC",
    "first_file_date": "2026-09-20", "last_file_date": "2026-09-26", "cycles": [2026], "candidate_ids": None,
}


def _zip(member: str, *rows: bytes) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as archive:
        archive.writestr(member, b"".join(row + b"\n" for row in rows))
    return buf.getvalue()


def _wire_master(monkeypatch, files: dict[str, bytes]) -> None:
    transport = CaptureEvidence.transport

    def respond(request):
        body = files.get(str(request.url))
        return httpx.Response(
            404 if body is None else 200,
            stream=httpx.ByteStream(b"missing" if body is None else body),
            headers={"Content-Type": "text/csv" if str(request.url).endswith(".csv") else "application/zip"},
        )

    monkeypatch.setattr(CaptureEvidence, "transport", lambda self, **kw: transport(self, httpx.MockTransport(respond), **kw))
    monkeypatch.setattr(fec_master_history, "current_cycle", lambda _: 1982)


def _specs(root: Path) -> list[dict]:
    return [json.loads(p.read_text())["spec"] for p in sorted((root / "generations").glob("*/artifact.json"))]


def test_fec_committees_full_walk_and_receiptless_incremental_parity(tmp_path, monkeypatch):
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    seen_since: list[object] = []

    def stub_walk(rows):
        def walk(*, capture_dir, resume=None, min_last_file_date=None):
            seen_since.append(min_last_file_date)
            Path(capture_dir).mkdir(parents=True, exist_ok=True)
            (Path(capture_dir) / "stub.json").write_text("{}")
            return (row for row in rows)

        return walk

    monkeypatch.setattr(committees_mod, "iter_fec_committee_records", stub_walk([_C1, _C2]))
    old_n1 = build_fec_identity_rollup("fec_committees", tmp_path / "old-n1", generation_id="g1", full_walk=True)
    plain_dir = tmp_path / "plain"
    monkeypatch.setenv("FEC_COMMITTEES_FULL_WALK", "true")
    FecCommitteesRollup(output_dir=plain_dir, skip_upload=True).run()
    assert table_differences(plain_dir / "fec_committees.parquet", old_n1 / "fec_committees.parquet", key=("committee_id",)) == []
    assert receipt_files(plain_dir) == [] and all("etlReceipts" not in s for s in _specs(plain_dir))

    night2 = [{**_C1, "name": "NAR UPDATED PAC", "last_file_date": "2026-09-26"}, _C3]
    monkeypatch.setattr(committees_mod, "iter_fec_committee_records", stub_walk(night2))
    old_n2 = build_fec_identity_rollup(
        "fec_committees", tmp_path / "old-n2", generation_id="g2", prior_bundle=old_n1, prior_generation_id="g1"
    )
    inc_dir = receipt_free_prior(old_n1, tmp_path / "plain-inc")
    seen_since.clear()
    monkeypatch.delenv("FEC_COMMITTEES_FULL_WALK", raising=False)
    FecCommitteesRollup(output_dir=inc_dir, skip_upload=True).run()
    assert [d.isoformat() if d is not None else None for d in seen_since] == ["2026-09-18"]
    assert table_differences(inc_dir / "fec_committees.parquet", old_n2 / "fec_committees.parquet", key=("committee_id",)) == []
    assert receipt_files(inc_dir) == []
    rows = {r["committee_id"]: r for r in pq.read_table(inc_dir / "fec_committees.parquet").to_pylist()}
    assert rows["C00030718"]["name"] == "NAR UPDATED PAC"
    assert rows["C00104802"]["cycles"] == [] and rows["C00999999"]["candidate_ids"] is None


def test_fec_master_histories_and_org_links_parity_and_receiptless_chaining(tmp_path, monkeypatch):
    from spicy_docs.sources.fec.candidate_master import CANDIDATE_MASTER_FIELDS, HEADER_URL as CN_HDR, candidate_master_url
    from spicy_docs.sources.fec.committee_master import COMMITTEE_MASTER_FIELDS, HEADER_URL as CM_HDR, committee_master_url

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    _wire_master(monkeypatch, {
        CN_HDR: (",".join(CANDIDATE_MASTER_FIELDS) + "\n").encode(),
        candidate_master_url(1980): _zip("cn.txt", b"H0AA00001|ALPHA|Rep|1980|AL|H|01|O|C|C00030718|ST1||CITY|AL|36685"),
        candidate_master_url(1982): _zip("cn.txt", b"H0AA00002|BETA|GOP|1982|CA|H|02|C|C|C00104802|ST1||CITY|CA|90001"),
        CM_HDR: (",".join(COMMITTEE_MASTER_FIELDS) + "\n").encode(),
        committee_master_url(1980): _zip("cm.txt", b"C00030718|NAR PAC|DOE, JANE|ST1||DC|DC|20001|U|Q|dem|M|T|NATIONAL ASSOCIATION OF REALTORS|"),
        committee_master_url(1982): _zip("cm.txt", b"C00104802|DELTA PAC|SMITH|ST1||ATL|GA|30320|B|Q|GOP|M|C|DELTA AIRLINES|"),
    })
    for table, rollup_cls, key, pattern in (
        ("fec_candidate_history", FecCandidateHistoryRollup, ("candidate_id", "cycle"), "fec-candidate-master-*"),
        ("fec_committee_history", FecCommitteeHistoryRollup, ("committee_id", "cycle"), "fec-committee-master-*"),
    ):
        plain = tmp_path / f"plain-{table}"
        rollup_cls(output_dir=plain, skip_upload=True).run()
        (selection,) = plain.glob(f".builds/*/native.inputs/raw/{pattern}/selection.json")
        old = build_fec_identity_rollup(table, tmp_path / f"old-{table}", generation_id=f"g-{table}", selection=selection)
        assert table_differences(plain / f"{table}.parquet", old / f"{table}.parquet", key=key) == []
        assert receipt_files(plain) == [] and all("etlReceipts" not in s for s in _specs(plain))

    monkeypatch.setattr(committees_mod, "iter_fec_committee_records", lambda **kw: (row for row in [_C1, _C2]))
    monkeypatch.setenv("FEC_COMMITTEES_FULL_WALK", "true")
    plain_comm = tmp_path / "plain-comm"
    FecCommitteesRollup(output_dir=plain_comm, skip_upload=True).run()

    comments_path = tmp_path / "comments.parquet"
    pq.write_table(pa.Table.from_pylist([
        {"comment_id": "C-1", "docket_id": "EPA-1", "agency_code": "EPA", "organization": "National Association of Realtors",
         "posted_date": "2026-03-01T12:00:00Z", "modify_date": "2026-03-01T12:00:00Z"},
        {"comment_id": "C-2", "docket_id": "DOT-2", "agency_code": "DOT", "organization": "Delta Air Lines, Inc.",
         "posted_date": "2026-04-01T00:00:00Z", "modify_date": "2026-04-01T00:00:00Z"},
    ]), comments_path)
    plain_cm = tmp_path / "plain-fec_committee_history"
    old_links = build_fec_identity_rollup(
        "org_committee_links", tmp_path / "old-links", generation_id="g-links",
        inputs=[plain_comm / "fec_committees.parquet", plain_cm / "fec_committee_history.parquet", comments_path],
    )
    plain_links = tmp_path / "plain-links"
    receipt_free_prior(plain_comm, plain_links)
    receipt_free_prior(plain_cm, plain_links)
    pq.write_table(pq.read_table(comments_path), plain_links / "comments.parquet")
    OrgCommitteeLinksRollup(output_dir=plain_links, skip_upload=True).run()
    assert table_differences(plain_links / "org_committee_links.parquet", old_links / "org_committee_links.parquet", key=("organization", "committee_id")) == []
    assert receipt_files(plain_links) == []
    [spec] = _specs(plain_links)
    assert "etlReceipts" not in spec
    assert set(spec["parents"]) == {"fec_committees.parquet", "fec_committee_history.parquet", "comments.parquet"}
    rows = {r["committee_id"]: r for r in pq.read_table(plain_links / "org_committee_links.parquet").to_pylist()}
    assert rows["C00030718"]["sponsor_name_match"] == "agrees" and rows["C00104802"]["sponsor_name_match"] == "agrees"


def test_fec_source_catalog_parity_and_stale_pin_refusal(tmp_path, monkeypatch):
    from spicy_regs import generation_retention
    from spicy_regs.sources import publication

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    fixed = datetime(2026, 11, 28, 12, 0, tzinfo=UTC)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed

    monkeypatch.setattr(catalog_mod, "datetime", FixedDatetime)
    old_cat = build_fec_identity_rollup("fec_source_catalog", tmp_path / "old-cat", generation_id="g-cat")
    plain_cat = tmp_path / "plain-cat"
    FecSourceCatalogRollup(output_dir=plain_cat, skip_upload=True).run()
    assert table_differences(plain_cat / "fec_source_catalog.parquet", old_cat / "fec_source_catalog.parquet", key=("observation_ordinal",)) == []
    assert receipt_files(plain_cat) == [] and all("etlReceipts" not in s for s in _specs(plain_cat))

    live_digest, stale_digest = "sha256:" + "1" * 64, "sha256:" + "2" * 64
    live_index = {"version": 2, "families": {"fec-source-catalog": {
        "artifactDigest": live_digest, "logicalId": "urn:spicy:artifact:spicy-regs-rollup-generation:" + "3" * 64,
        "prefix": f"generations/fec-source-catalog/{'1' * 64}", "publishedAt": "2026-10-10T18:26:53Z",
        "tables": {"fec_source_catalog.parquet": {"sha256": "sha256:" + "4" * 64, "byteSize": 100, "rows": 26}},
        "etlReceipts": {"key": "etl_receipts.parquet", "datasets": ["fec_source_catalog"]},
    }}}
    pins_path = tmp_path / "receipt-archive.json"
    pins_path.write_text(json.dumps({
        "format": generation_retention.RECEIPT_ARCHIVE_FORMAT, "version": 1,
        "archives": {"fec_source_catalog": {
            "family": "fec-source-catalog", "artifactDigest": stale_digest, "publishedAt": "2026-10-10T18:26:53Z",
            "receipts": f"generations/fec-source-catalog/{'2' * 64}/etl_receipts.parquet",
        }},
    }))
    monkeypatch.setattr(generation_retention, "RECEIPT_ARCHIVE", pins_path)
    monkeypatch.setattr(publication, "load_index", lambda _: live_index)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example.dev")
    built_called = []
    monkeypatch.setattr(FecSourceCatalogRollup, "build", lambda self, out: built_called.append(out))
    with pytest.raises(publication.PublicationError, match=f"fec_source_catalog leaves its receipts.*{live_digest}"):
        FecSourceCatalogRollup(output_dir=tmp_path / "refused", skip_upload=True).run()
    assert built_called == []


def test_fec_table_rollup_guards_and_published_prior_round_trip(tmp_path, monkeypatch):
    from spicy_regs import subject_catalog

    retired = subject_catalog.retired_receipt_datasets()
    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired - {"fec_source_catalog"})
    with pytest.raises(ValueError, match="still publishes receipts"):

        class UnretiredCatalog(FecTableRollup):
            name, output = "unretired-catalog", "fec_source_catalog.parquet"

            def build(self, output_dir: Path) -> Path:
                return output_dir / self.output

    monkeypatch.setattr(subject_catalog, "retired_receipt_datasets", lambda: retired)
    comm_in = tmp_path / "comm.parquet"
    pq.write_table(pa.Table.from_pylist([{
        "committee_id": "C1", "name": "PAC", "committee_type": "Q", "committee_type_full": None, "designation": "U",
        "designation_full": None, "party": "DEM", "party_full": None, "state": "DC", "treasurer_name": None,
        "organization_type_full": None, "filing_frequency": "M", "first_file_date": date(2020, 1, 2),
        "last_file_date": date(2026, 9, 25), "cycles": [2020, 2026], "candidate_ids": ["H0CA00001"],
    }], schema=subject_schema("fec_committees")), comm_in)
    row = pq.read_table(published_as_fec_source("fec_committees", comm_in, tmp_path / "prior.parquet")).to_pylist()[0]
    assert (row["first_file_date"], row["last_file_date"]) == ("2020-01-02", "2026-09-25")
    assert json.loads(row["cycles_json"]) == [2020, 2026] and json.loads(row["candidate_ids_json"]) == ["H0CA00001"]

    hist_in = tmp_path / "hist.parquet"
    pq.write_table(pa.Table.from_pylist([{
        "committee_id": "C1", "cycle": 2026, "name": "PAC", "treasurer_name": None, "street_1": None, "street_2": None,
        "city": None, "state": "DC", "zip": None, "designation": "U", "committee_type": "Q", "party": "DEM",
        "filing_frequency": "M", "organization_type": "T", "connected_organization_name": "ORG", "candidate_id": None,
    }], schema=subject_schema("fec_committee_history")), hist_in)
    hist_out = published_as_fec_source("fec_committee_history", hist_in, tmp_path / "hist_prior.parquet")
    assert pq.read_table(hist_out).to_pylist()[0]["cycle"] == "2026"
