"""fec_committee_history: every cycle's committee master read whole through SpicyDocs, projected and written."""

import hashlib
import io
import zipfile
from datetime import date

import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas.fec_committee_history import FEC_COMMITTEE_HISTORY
from spicy_docs.sources.fec import client as fec_client
from spicy_docs.sources.fec.committee_master import COMMITTEE_MASTER_FIELDS, HEADER_URL, committee_master_url

from spicy_regs.transforms import build_fec_committee_history as build

HEADER = (",".join(COMMITTEE_MASTER_FIELDS) + "\n").encode()


def _zip(*rows: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("cm.txt", b"".join(row + b"\n" for row in rows))
    return buffer.getvalue()


def _row(committee: str, name: str, party: str = "DEM") -> bytes:
    return f"{committee}|{name}|T|S1||CITY|ST|00000|P|H|{party}|Q|||H0XX00000".encode()


def _serve(monkeypatch, files: dict[str, bytes]) -> list:
    """Replace SpicyDocs' FEC client with one that files each body in its content-addressed store."""
    transports = []

    class Client:
        def __init__(self, *, store, max_requests, transport=None):
            self.store = store
            transports.append(transport)

        def __enter__(self):
            return self

        def __exit__(self, *error):
            return None

        def download(self, url, *, max_bytes):
            raw = files[url]
            digest = hashlib.sha256(raw).hexdigest()
            path = self.store / "sha256" / digest
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(raw)
            return {"url": url, "sha256": f"sha256:{digest}", "bytes": len(raw), "blob_path": f"sha256/{digest}",
                    "reused": False, "downloaded": True, "response": {"observed_at": "2026-09-27T01:00:00+00:00"}}

    monkeypatch.setattr(fec_client, "FecClient", Client)
    return transports


def test_every_cycle_is_read_whole_and_projected_onto_the_contract(tmp_path, monkeypatch):
    _serve(monkeypatch, {
        HEADER_URL: HEADER,
        committee_master_url(1980): _zip(_row("C1", "ONE")),
        committee_master_url(1982): _zip(_row("C1", "ONE AGAIN"), _row("C2", "TWO", party="")),
        committee_master_url(1984): _zip(_row("C2", "TWO")),
    })
    out = build.build_fec_committee_history(tmp_path, today=date(1983, 5, 1))
    table = pq.read_table(out)
    assert tuple(table.column_names) == FEC_COMMITTEE_HISTORY.columns
    rows = table.to_pylist()
    assert [(r["committee_id"], r["cycle"], r["name"]) for r in rows] == [
        ("C1", "1980", "ONE"), ("C1", "1982", "ONE AGAIN"), ("C2", "1982", "TWO"), ("C2", "1984", "TWO")]
    assert rows[2]["party"] is None and rows[0]["candidate_id"] == "H0XX00000"


def test_a_committee_repeated_within_a_cycle_refuses(tmp_path, monkeypatch):
    _serve(monkeypatch, {HEADER_URL: HEADER, committee_master_url(1980): _zip(_row("C1", "ONE"), _row("C1", "DUP"))})
    with pytest.raises(ValueError, match="repeats C1 in cycle 1980"):
        build.build_fec_committee_history(tmp_path, today=date(1980, 1, 1))
    assert not (tmp_path / build.OUTPUT).exists()


def test_every_response_goes_through_the_evidence_transport(tmp_path, monkeypatch):
    transports = _serve(monkeypatch, {HEADER_URL: HEADER, committee_master_url(1980): _zip(_row("C1", "ONE"))})
    marker, events = object(), []

    class Evidence:
        def transport(self, transport=None, *, stage, max_bytes):
            assert stage == "fec-committee-master-response" and max_bytes == build.MAX_FILE_BYTES
            return marker

        def event(self, event, **fields):
            events.append((event, fields))

    build.build_fec_committee_history(tmp_path, evidence=Evidence(), today=date(1980, 6, 1))  # ty: ignore[invalid-argument-type]
    assert transports == [marker]
    assert [event for event, _ in events] == ["selection", "cycle-rows"]
    assert events[1][1]["rows_by_cycle"] == {"1980": 1}


@pytest.mark.parametrize(("today", "cycle"), [(date(2026, 9, 27), 2026), (date(2027, 1, 2), 2028), (date(2028, 12, 31), 2028)])
def test_the_current_cycle_is_the_even_year_at_or_after_today(today, cycle):
    assert build.current_cycle(today) == cycle
