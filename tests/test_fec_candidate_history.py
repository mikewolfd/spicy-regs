"""fec_candidate_history through its rollup: refusals preserve the prior output and the raw originals survive.

The builder itself is tested for both masters in ``test_fec_master_history.py``.
"""

import hashlib
import io
import json
import zipfile

import httpx
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.fec.candidate_master import CANDIDATE_MASTER_FIELDS, HEADER_URL, candidate_master_url

from spicy_regs.pipelines.rollups.fec_candidate_history import FecCandidateHistoryRollup
from spicy_regs.source_evidence import CaptureEvidence, verify_evidence
from spicy_regs.transforms import build_fec_candidate_history as build
from spicy_regs.transforms import fec_master_history
from spicy_regs.transforms.fec_identity_receipts import dataset_policy

HEADER = (",".join(CANDIDATE_MASTER_FIELDS) + "\n").encode()


def _zip(*rows: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("cn.txt", b"".join(row + b"\n" for row in rows))
    return buffer.getvalue()


def _row(candidate: str, name: str, party: str = "DEM") -> bytes:
    return f"{candidate}|{name}|{party}|2026|AL|H|01|O|C|C00000000|PO BOX||MOBILE|AL|36685".encode()


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
    monkeypatch.setattr(fec_master_history, "current_cycle", lambda _: 1982)


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
    # The raw originals outlive the refused build, outside its staging directory.
    (attempt,) = tmp_path.glob(".builds/*/native.inputs/raw/fec-candidate-master-*")
    for body in files.values():
        assert (attempt / "blobs" / "sha256" / hashlib.sha256(body).hexdigest()).read_bytes() == body
    assert not list(tmp_path.glob(".builds/*/.fec-identity-producer-*")) and not list(
        tmp_path.glob(".fec-identity-producer-*")
    )


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
        ("H0AA00001", 1980, "CORRECTED"),
        ("H0AA00003", 1982, "OTHER CYCLE"),
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
    # The rows are written under the policy main declares for this table, not a version literal of this lane.
    version = dataset_policy("fec_candidate_history").policy_version
    assert version == "fec-identity-context-receipts/2"
    assert dataset_policy("fec_committee_history").policy_version == "fec-identity-context-receipts/2"
    written = {
        row["policy_version"]
        for receipts in (tmp_path / "generations").glob("*/etl_receipts.parquet")
        for row in pq.read_table(receipts).to_pylist()
        if row["dataset"] == "fec_candidate_history"
    }
    assert written == {version}
