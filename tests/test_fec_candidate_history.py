"""fec_candidate_history: every cycle's candidate master read whole through SpicyDocs, projected and written."""

import hashlib
import io
import json
import zipfile
from datetime import date

import httpx
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas.fec_candidate_history import FEC_CANDIDATE_HISTORY
from spicy_docs.sources.fec import client as fec_client
from spicy_docs.sources.fec.candidate_master import CANDIDATE_MASTER_FIELDS, HEADER_URL, candidate_master_url

from spicy_regs.transforms import build_fec_candidate_history as build
from spicy_regs.pipelines.rollups.fec_candidate_history import FecCandidateHistoryRollup
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence

HEADER = (",".join(CANDIDATE_MASTER_FIELDS) + "\n").encode()


def _zip(*rows: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("cn.txt", b"".join(row + b"\n" for row in rows))
    return buffer.getvalue()


def _row(candidate: str, name: str, party: str = "DEM") -> bytes:
    return f"{candidate}|{name}|{party}|2026|AL|H|01|O|C|C00000000|PO BOX||MOBILE|AL|36685".encode()


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
            return {
                "url": url,
                "sha256": f"sha256:{digest}",
                "bytes": len(raw),
                "blob_path": f"sha256/{digest}",
                "reused": False,
                "downloaded": True,
                "response": {"observed_at": "2026-09-29T01:00:00+00:00"},
            }

    monkeypatch.setattr(fec_client, "FecClient", Client)
    return transports


def test_every_cycle_is_read_whole_and_projected_onto_the_contract(tmp_path, monkeypatch):
    _serve(
        monkeypatch,
        {
            HEADER_URL: HEADER,
            candidate_master_url(1980): _zip(_row("H0AA00001", "ONE")),
            candidate_master_url(1982): _zip(_row("H0AA00001", "ONE AGAIN"), _row("H0AA00002", "TWO", party="")),
            candidate_master_url(1984): _zip(_row("H0AA00002", "TWO")),
        },
    )
    out = build.build_fec_candidate_history(tmp_path, today=date(1983, 5, 1))
    table = pq.read_table(out)
    assert tuple(table.column_names) == FEC_CANDIDATE_HISTORY.columns
    rows = table.to_pylist()
    assert [(r["candidate_id"], r["cycle"], r["name"]) for r in rows] == [
        ("H0AA00001", "1980", "ONE"),
        ("H0AA00001", "1982", "ONE AGAIN"),
        ("H0AA00002", "1982", "TWO"),
        ("H0AA00002", "1984", "TWO"),
    ]
    assert rows[2]["party"] is None and rows[0]["principal_committee_id"] == "C00000000"


def test_a_candidate_repeated_within_a_cycle_refuses(tmp_path, monkeypatch):
    _serve(
        monkeypatch,
        {HEADER_URL: HEADER, candidate_master_url(1980): _zip(_row("H0AA00001", "ONE"), _row("H0AA00001", "DUP"))},
    )
    with pytest.raises(ValueError, match="repeats H0AA00001 in cycle 1980"):
        build.build_fec_candidate_history(tmp_path, today=date(1980, 1, 1))
    assert not (tmp_path / build.OUTPUT).exists()


def test_every_response_goes_through_the_evidence_transport(tmp_path, monkeypatch):
    transports = _serve(monkeypatch, {HEADER_URL: HEADER, candidate_master_url(1980): _zip(_row("H0AA00001", "ONE"))})
    marker, events = object(), []

    class Evidence:
        def transport(self, transport=None, *, stage, max_bytes):
            assert stage == "fec-candidate-master-response" and max_bytes == build.MAX_FILE_BYTES
            return marker

        def event(self, event, **fields):
            events.append((event, fields))

    build.build_fec_candidate_history(tmp_path, evidence=Evidence(), today=date(1980, 6, 1))  # ty: ignore[invalid-argument-type]
    assert transports == [marker]
    assert [event for event, _ in events] == ["selection", "cycle-rows"]
    assert events[1][1]["rows_by_cycle"] == {"1980": 1}


def test_the_current_cycle_is_the_even_year_at_or_after_today():
    assert build.current_cycle(date(2026, 9, 27)) == 2026
    assert build.current_cycle(date(2027, 1, 2)) == 2028
    assert build.current_cycle(date(2028, 12, 31)) == 2028


def _wire(monkeypatch, files, redirects=None):
    """Exercise the real client and evidence tee, replacing only the remote HTTP peer."""
    transport = CaptureEvidence.transport

    def respond(request):
        if redirects is not None and str(request.url) in redirects:
            return httpx.Response(302, headers={"Location": redirects[str(request.url)]}, stream=httpx.ByteStream(b""))
        body = files.get(str(request.url))
        return httpx.Response(
            404 if body is None else 200,
            stream=httpx.ByteStream(b"missing cycle" if body is None else body),
            headers={"Content-Type": "text/csv" if str(request.url) == HEADER_URL else "application/zip"},
        )

    monkeypatch.setattr(
        CaptureEvidence,
        "transport",
        lambda self, **kwargs: transport(self, httpx.MockTransport(respond), **kwargs),
    )
    monkeypatch.setattr(build, "current_cycle", lambda _: 1982)


@pytest.mark.parametrize("failure", ["missing", "empty", "malformed", "duplicate", "blank-id", "header", "redirect"])
def test_later_cycle_refusal_preserves_prior_and_never_publishes(tmp_path, monkeypatch, failure):
    # Cross a write_rows batch before the later cycle refuses; no partial replacement is allowed.
    first = _zip(*(_row(f"H0AA{i:05d}", "FIRST") for i in range(2001)))
    second = {
        "empty": _zip(),
        "malformed": _zip(b"H0AA00001|SHORT"),
        "duplicate": _zip(_row("H0AA00001", "ONE"), _row("H0AA00001", "AGAIN")),
        "blank-id": _zip(_row("", "NO ID")),
        "header": _zip(_row("H0AA00001", "ONE")),
    }.get(failure)
    files = {HEADER_URL: HEADER if failure != "header" else b"CAND_ID\n", candidate_master_url(1980): first}
    if second is not None:
        files[candidate_master_url(1982)] = second
    redirects = {candidate_master_url(1982): candidate_master_url(1980)} if failure == "redirect" else None
    _wire(monkeypatch, files, redirects)
    prior = tmp_path / build.OUTPUT
    prior.write_bytes(b"untouched prior output")
    pipeline = FecCandidateHistoryRollup(output_dir=tmp_path, skip_upload=False)
    from spicy_docs.transport.download import AcquisitionError
    from spicy_regs.sources import publication

    published = []
    monkeypatch.setattr(publication, "publish_generation", lambda *args, **kwargs: published.append(args))
    with pytest.raises((ValueError, AcquisitionError)):
        pipeline.run()
    assert prior.read_bytes() == b"untouched prior output"
    assert not (tmp_path / "generations").exists()
    assert not published
    evidence = pipeline.source_evidence
    assert evidence is not None
    assert json.loads((evidence.directory / "run-outcome.json").read_text())["outcome"] == "failed"
    captures = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    captured = {r["sha256"] for r in captures if r["event"] == "capture"}
    assert {"sha256:" + hashlib.sha256(body).hexdigest() for body in files.values()} <= captured
    assert list(evidence.artifact_dir.glob("blobs/sha256/*"))


def test_complete_generation_binds_exact_responses_and_refresh_retires_rows(tmp_path, monkeypatch):
    files = {
        HEADER_URL: HEADER,
        candidate_master_url(1980): _zip(_row("H0AA00001", "OLD"), _row("H0AA00002", "RETIRED")),
        candidate_master_url(1982): _zip(_row("H0AA00003", "OTHER CYCLE")),
    }
    _wire(monkeypatch, files)
    first = FecCandidateHistoryRollup(output_dir=tmp_path)
    first.run()
    files[candidate_master_url(1980)] = _zip(_row("H0AA00001", "CORRECTED"))
    second = FecCandidateHistoryRollup(output_dir=tmp_path)
    second.run()
    rows = pq.read_table(tmp_path / build.OUTPUT).to_pylist()
    assert [(r["candidate_id"], r["cycle"], r["name"]) for r in rows] == [
        ("H0AA00001", "1980", "CORRECTED"),
        ("H0AA00003", "1982", "OTHER CYCLE"),
    ]
    assert len(list((tmp_path / "generations").iterdir())) == 2
    evidence = second.source_evidence
    assert evidence is not None
    verified = verify_evidence(evidence.artifact_dir)
    for body in files.values():
        digest = hashlib.sha256(body).hexdigest()
        assert (evidence.artifact_dir / "blobs" / "sha256" / digest).read_bytes() == body
    generations = [json.loads(p.read_text()) for p in (tmp_path / "generations").glob("*/artifact.json")]
    assert any(verified.pin.artifact_digest in json.dumps(generation) for generation in generations)
