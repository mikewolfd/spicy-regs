"""Separate wire-vs-decoded digest binding and mismatch refusal for compressed captures (ss-hm80 / REGS-05-08)."""

from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path
import tracemalloc
import zlib

import httpx
import pytest
from spicy_docs.storage.raw_archive import iter_acquisition_receipts
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.source_catalog.input_proof import input_proof
from spicy_regs.source_evidence import (
    _CHUNK,
    CaptureEvidence,
    EvidenceMemberSource,
    SourceEvidenceError,
    verify_evidence,
)
from spicy_regs.sources.unified_agenda import EDITION_REPORT_URL

AGENDA_LISTING_HTML = (
    b'<html><body><a href="XMLViewFileAction?f=REGINFO_RIN_DATA_202510.xml">Fall 2025</a>'
    b'<a href="XMLViewFileAction?f=REGINFO_RIN_DATA_202604.xml">Spring 2026</a></body></html>'
)
AGENDA_XML = (
    b'<?xml version="1.0" encoding="UTF-8"?>'
    b"<REGINFO_RIN_DATA><RIN_INFO><RIN>2060-AV12</RIN>"
    b"<PUBLICATION><PUBLICATION_ID>202510</PUBLICATION_ID></PUBLICATION>"
    b"</RIN_INFO></REGINFO_RIN_DATA>"
)


def _sha(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def _journal(evidence: CaptureEvidence) -> list[dict]:
    return [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]


def _stored_objects(evidence: CaptureEvidence) -> list[Path]:
    objects_dir = evidence.store.root / "objects"
    if not objects_dir.exists():
        return []
    return [path for path in objects_dir.rglob("*") if path.is_file()]


class _Chunks(httpx.SyncByteStream):
    def __init__(self, parts: list[bytes]):
        self.parts = parts

    def __iter__(self):
        yield from self.parts


@pytest.mark.parametrize("encoding", ["gzip", "x-gzip", "deflate", "deflate-raw", "gzip-concat"])
def test_transport_binds_separate_wire_and_decoded_digests_for_compressed_captures(tmp_path, encoding):
    decoded = AGENDA_LISTING_HTML * 40
    if encoding in ("gzip", "x-gzip"):
        wire = gzip.compress(decoded, mtime=0)
        header = encoding
    elif encoding == "gzip-concat":
        mid = len(decoded) // 2
        wire = gzip.compress(decoded[:mid], mtime=0) + gzip.compress(decoded[mid:], mtime=0)
        header = "gzip"
    elif encoding == "deflate":
        wire = zlib.compress(decoded)
        header = "deflate"
    else:
        comp = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        wire = comp.compress(decoded) + comp.flush()
        header = "deflate"

    assert wire != decoded and _sha(wire) != _sha(decoded)
    evidence = CaptureEvidence(tmp_path, "unified-agenda")
    parts = [wire[i : i + 17] for i in range(0, len(wire), 17)]
    transport = evidence.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/html", "content-encoding": header},
                stream=_Chunks(parts),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(wire) + 64,
        max_decoded_bytes=len(decoded) + 64,
        expected_wire_sha256=_sha(wire),
        expected_decoded_sha256=_sha(decoded),
    )
    with httpx.Client(transport=transport) as client:
        response = client.get(EDITION_REPORT_URL)
    # HTTPX decodes single-member gzip and deflate; x-gzip and concatenated gzip members are still decoded by CaptureEvidence.
    if encoding in ("gzip", "deflate", "deflate-raw"):
        assert response.content == decoded
    elif encoding == "x-gzip":
        assert response.content == wire
    else:
        assert response.content == decoded[: len(decoded) // 2]

    [event] = [row for row in _journal(evidence) if row["event"] == "capture"]
    assert event["content_encoding"] == header
    assert event["sha256"] == event["wire_sha256"] == _sha(wire)
    assert event["byte_size"] == event["wire_byte_size"] == len(wire)
    assert event["decoded_sha256"] == _sha(decoded)
    assert event["decoded_byte_size"] == len(decoded)
    assert event["wire_sha256"] != event["decoded_sha256"]
    assert "decoded_status" not in event

    # Central raw archive receipt also carries separate wire and decoded bindings.
    [(_, archive_receipt)] = [
        item
        for item in iter_acquisition_receipts(evidence.store.root)
        if item[1]["receipt"].get("event") == "capture"
    ]
    [file_entry] = archive_receipt["files"]
    assert file_entry["wire_sha256"] == _sha(wire)
    assert file_entry["decoded_sha256"] == _sha(decoded)
    assert file_entry["decoded_byte_size"] == len(decoded)

    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin).pin == artifact.pin


def test_direct_capture_binds_wire_and_decoded_digests_and_rejects_mismatches(tmp_path):
    wire = gzip.compress(AGENDA_XML, mtime=0)
    wire_sha, decoded_sha = _sha(wire), _sha(AGENDA_XML)
    captured = CapturedBodyResponse(
        requested_url="https://www.reginfo.gov/public/do/XMLViewFileAction?f=REGINFO_RIN_DATA_202510.xml",
        resolved_url="https://www.reginfo.gov/public/do/XMLViewFileAction?f=REGINFO_RIN_DATA_202510.xml",
        status_code=200,
        content_type="application/xml",
        observed_at="2026-11-27T00:00:00Z",
        body=wire,
        content_encoding="gzip",
    )

    ok = CaptureEvidence(tmp_path / "ok", "unified-agenda")
    receipt = ok.capture(
        captured,
        stage="unified-agenda-edition",
        expected_wire_sha256=wire_sha,
        expected_decoded_sha256=decoded_sha,
        edition="202510",
    )
    assert receipt["sha256"] == receipt["wire_sha256"] == wire_sha
    assert receipt["byte_size"] == receipt["wire_byte_size"] == len(wire)
    assert receipt["decoded_sha256"] == decoded_sha
    assert receipt["decoded_byte_size"] == len(AGENDA_XML)
    ok_artifact = ok.seal(outcome="build-complete")
    verify_evidence(ok.artifact_dir, expected_pin=ok_artifact.pin)

    # Passing decoded digest as wire digest (REGS-05-08 confusion) must refuse before retention.
    bad_wire = CaptureEvidence(tmp_path / "bad-wire", "unified-agenda")
    with pytest.raises(SourceEvidenceError, match="expected wire_sha256"):
        bad_wire.capture(captured, stage="unified-agenda-edition", expected_wire_sha256=decoded_sha)
    assert not any(row["event"] == "capture" for row in _journal(bad_wire))
    with pytest.raises(SourceEvidenceError, match="retention failed"):
        bad_wire.seal(outcome="build-complete")

    # Passing wire digest as decoded digest must refuse before retention.
    bad_decoded = CaptureEvidence(tmp_path / "bad-decoded", "unified-agenda")
    with pytest.raises(SourceEvidenceError, match="expected decoded_sha256"):
        bad_decoded.capture(captured, stage="unified-agenda-edition", expected_decoded_sha256=wire_sha)
    assert not any(row["event"] == "capture" for row in _journal(bad_decoded))
    with pytest.raises(SourceEvidenceError, match="retention failed"):
        bad_decoded.seal(outcome="build-complete")


@pytest.mark.parametrize("bad_kind", ["wire", "decoded"])
def test_transport_rejects_wire_or_decoded_expected_digest_mismatch(tmp_path, bad_kind):
    wire = gzip.compress(AGENDA_LISTING_HTML, mtime=0)
    wire_sha, decoded_sha = _sha(wire), _sha(AGENDA_LISTING_HTML)
    evidence = CaptureEvidence(tmp_path / bad_kind, "unified-agenda", archive_root=tmp_path / f"raw-{bad_kind}")
    transport = evidence.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/html", "content-encoding": "gzip"},
                stream=_Chunks([wire]),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(wire) + 64,
        expected_wire_sha256=decoded_sha if bad_kind == "wire" else wire_sha,
        expected_decoded_sha256=wire_sha if bad_kind == "decoded" else decoded_sha,
    )
    match = "expected wire_sha256" if bad_kind == "wire" else "expected decoded_sha256"
    with httpx.Client(transport=transport) as client, pytest.raises(SourceEvidenceError, match=match):
        client.get(EDITION_REPORT_URL)
    assert not any(row["event"] == "capture" for row in _journal(evidence))
    assert _stored_objects(evidence) == []
    with pytest.raises(SourceEvidenceError, match="retention failed"):
        evidence.seal(outcome="build-complete")


def test_used_input_proof_binds_compressed_wire_and_decoded_and_rejects_mismatches(tmp_path):
    """REGS-05-08: decoded listing digest cannot masquerade as retained compressed bytes."""
    wire = gzip.compress(AGENDA_LISTING_HTML, mtime=0)
    wire_sha, decoded_sha = _sha(wire), _sha(AGENDA_LISTING_HTML)

    def _capture_into(name: str) -> CaptureEvidence:
        ev = CaptureEvidence(tmp_path / name, "unified-agenda")
        transport = ev.transport(
            httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    headers={"content-type": "text/html", "content-encoding": "gzip"},
                    stream=_Chunks([wire]),
                )
            ),
            stage="unified-agenda-listing",
            max_bytes=len(wire) + 64,
        )
        with httpx.Client(transport=transport) as client:
            assert client.get(EDITION_REPORT_URL).content == AGENDA_LISTING_HTML
        return ev

    # 1. Valid binding: wire sha256/bytes + separate decoded_sha256/decoded_bytes succeeds end-to-end.
    ev_ok = _capture_into("ok")
    ev_ok.declare_used_inputs(
        [
            {
                "location": EDITION_REPORT_URL,
                "sha256": wire_sha,
                "bytes": len(wire),
                "wire_sha256": wire_sha,
                "wire_bytes": len(wire),
                "decoded_sha256": decoded_sha,
                "decoded_bytes": len(AGENDA_LISTING_HTML),
                "asset_kind": "unified-agenda-listing",
            }
        ],
        scope_id="unified-agenda-listing-2026-11-27",
    )
    ev_artifact = ev_ok.seal(outcome="build-complete")
    verify_evidence(ev_ok.artifact_dir, expected_pin=ev_artifact.pin)
    [selected] = [row for row in _journal(ev_ok) if row["event"] == "selected-inputs"]
    [used] = selected["manifest"]["used"]
    assert used["sha256"] == used["wire_sha256"] == wire_sha
    assert used["bytes"] == len(wire)
    assert used["decoded_sha256"] == decoded_sha
    assert used["decoded_bytes"] == len(AGENDA_LISTING_HTML)

    # Confirm input_proof consumes the generation + evidence cleanly without losing either digest.
    import pyarrow as pa
    import pyarrow.parquet as pq
    from rulespec_artifacts import LocalMemberSource
    from spicy_regs.generations import build_generation

    table_path = tmp_path / "unified_agenda.parquet"
    pq.write_table(pa.table({"rin": ["2060-AV12"]}), table_path)
    gen_dir = tmp_path / "generation"
    gen_artifact = build_generation(
        gen_dir,
        family="unified-agenda",
        files=(table_path,),
        expected_keys=("unified_agenda.parquet",),
        inputs=ev_ok.inputs(),
    )
    proof, _controls = input_proof(
        gen_artifact,
        LocalMemberSource(gen_dir),
        evidence=((EvidenceMemberSource(ev_ok.artifact_dir, archive_root=ev_ok.store.root), ev_artifact),),
    )
    [source_entry] = proof["generations"][0]["sources"]
    assert source_entry["sha256"] == source_entry["wire_sha256"] == wire_sha
    assert source_entry["decoded_sha256"] == decoded_sha
    assert source_entry["decoded_bytes"] == len(AGENDA_LISTING_HTML)

    # 2. Asserting decoded digest as retained wire sha256 must refuse!
    ev_decoded_as_wire = _capture_into("decoded-as-wire")
    ev_decoded_as_wire.declare_used_inputs(
        [
            {
                "location": EDITION_REPORT_URL,
                "sha256": decoded_sha,
                "bytes": len(AGENDA_LISTING_HTML),
                "asset_kind": "unified-agenda-listing",
            }
        ],
        scope_id="unified-agenda-listing-2026-11-27",
    )
    with pytest.raises(SourceEvidenceError, match="lacks a successful recorded capture"):
        ev_decoded_as_wire.seal(outcome="build-complete")

    # 3. Correct wire digest + mismatched decoded digest must refuse!
    ev_bad_decoded = _capture_into("bad-decoded")
    ev_bad_decoded.declare_used_inputs(
        [
            {
                "location": EDITION_REPORT_URL,
                "sha256": wire_sha,
                "bytes": len(wire),
                "decoded_sha256": _sha(b"tampered listing"),
                "decoded_bytes": len(AGENDA_LISTING_HTML),
                "asset_kind": "unified-agenda-listing",
            }
        ],
        scope_id="unified-agenda-listing-2026-11-27",
    )
    with pytest.raises(SourceEvidenceError, match="recorded decoded digest"):
        ev_bad_decoded.seal(outcome="build-complete")

    # 4. Contradictory wire_sha256 vs sha256 refuses immediately at declaration.
    ev_bad_wire = _capture_into("bad-wire")
    with pytest.raises(SourceEvidenceError, match="Invalid used-input descriptor"):
        ev_bad_wire.declare_used_inputs(
            [
                {
                    "location": EDITION_REPORT_URL,
                    "sha256": wire_sha,
                    "bytes": len(wire),
                    "wire_sha256": decoded_sha,
                    "asset_kind": "unified-agenda-listing",
                }
            ],
            scope_id="unified-agenda-listing-2026-11-27",
        )


@pytest.mark.parametrize(
    "tamper",
    [
        "wire_sha256",
        "decoded_sha256",
        "decoded_byte_size",
        "drop_decoded",
        "false_undecodable",
        "selected_decoded",
    ],
)
def test_verify_evidence_rejects_tampered_wire_or_decoded_bindings(tmp_path, tamper):
    wire = gzip.compress(AGENDA_LISTING_HTML, mtime=0)
    wire_sha, decoded_sha = _sha(wire), _sha(AGENDA_LISTING_HTML)
    evidence = CaptureEvidence(tmp_path, "unified-agenda")
    transport = evidence.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/html", "content-encoding": "gzip"},
                stream=_Chunks([wire]),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(wire) + 64,
    )
    with httpx.Client(transport=transport) as client:
        client.get(EDITION_REPORT_URL)
    evidence.declare_used_inputs(
        [
            {
                "location": EDITION_REPORT_URL,
                "sha256": wire_sha,
                "bytes": len(wire),
                "decoded_sha256": decoded_sha,
                "decoded_bytes": len(AGENDA_LISTING_HTML),
                "asset_kind": "unified-agenda-listing",
            }
        ],
        scope_id="unified-agenda-listing",
    )
    if tamper == "selected_decoded":
        # Seal first to produce the selected-inputs event, then tamper selected-inputs in a fresh seal check.
        evidence.seal(outcome="build-complete")
        rows = _journal(evidence)
        for row in rows:
            if row["event"] == "selected-inputs":
                row["manifest"]["used"][0]["decoded_sha256"] = _sha(b"forged")
        # Rebuild artifact manifest on tampered journal to isolate _verify_receipts semantic check.
        from rulespec_artifacts import (
            Producer,
            admit_artifact,
            build_artifact_root,
            canonical_json_bytes,
            describe_member,
            write_member_manifest,
        )
        from spicy_regs.source_evidence import _verify_receipts

        (evidence.artifact_dir / "artifact.json").unlink()
        (evidence.artifact_dir / "members.json").unlink()
        (evidence.artifact_dir / "journal.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
        source = EvidenceMemberSource(evidence.artifact_dir, archive_root=evidence.store.root)
        members = [
            describe_member(
                source,
                object_key=k,
                role="journal" if k == "journal.jsonl" else "source-body",
                media_type="application/octet-stream",
            )
            for k in sorted(source.keys())
        ]
        with (evidence.artifact_dir / "members.json").open("wb") as s:
            manifest = write_member_manifest(
                s,
                scope_kind="global",
                scope_id="unified-agenda",
                object_key="members.json",
                members=members,
            )
        from spicy_regs.generations import implementation_id

        impl = implementation_id()
        root = build_artifact_root(
            kind="spicy-regs-source-evidence",
            spec={"evidence_version": 2, "family": "unified-agenda", "outcome": "build-complete", "coverage": "test"},
            producer=Producer("spicy-regs", impl, "urn:spicy-regs:source-evidence", "1", impl),
            inputs=[],
            manifests=[manifest],
        )
        (evidence.artifact_dir / "artifact.json").write_bytes(canonical_json_bytes(root))
        admitted = admit_artifact(source)
        with pytest.raises(SourceEvidenceError, match="Selected used input decoded digest"):
            _verify_receipts(source, admitted)
        return

    rows = _journal(evidence)
    capture_row = next(row for row in rows if row["event"] == "capture")
    if tamper == "wire_sha256":
        capture_row["wire_sha256"] = _sha(b"different-wire")
    elif tamper == "decoded_sha256":
        capture_row["decoded_sha256"] = _sha(b"different-decoded")
    elif tamper == "decoded_byte_size":
        capture_row["decoded_byte_size"] = len(AGENDA_LISTING_HTML) + 1
    elif tamper == "drop_decoded":
        capture_row.pop("decoded_sha256")
        capture_row.pop("decoded_byte_size")
    elif tamper == "false_undecodable":
        capture_row.pop("decoded_sha256")
        capture_row.pop("decoded_byte_size")
        capture_row["decoded_status"] = "undecodable"
    (evidence.artifact_dir / "journal.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    with pytest.raises(SourceEvidenceError):
        evidence.seal(outcome="build-complete")


def test_truncated_gzip_is_marked_undecodable_and_cannot_become_used_input(tmp_path):
    truncated = gzip.compress(AGENDA_LISTING_HTML, mtime=0)[:-4]
    evidence = CaptureEvidence(tmp_path, "unified-agenda")
    transport = evidence.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/html", "content-encoding": "gzip"},
                stream=_Chunks([truncated]),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(truncated) + 64,
    )
    # HTTPX's GZipDecoder does not always check gzip EOF on truncated trailers,
    # so CaptureEvidence's own EOF check is what catches truncation and marks undecodable.
    with httpx.Client(transport=transport) as client:
        client.get(EDITION_REPORT_URL)
    [event] = [row for row in _journal(evidence) if row["event"] == "capture"]
    assert event["wire_sha256"] == event["sha256"] == _sha(truncated)
    assert event["wire_byte_size"] == len(truncated)
    assert event["decoded_status"] == "undecodable"
    assert "decoded_sha256" not in event and "decoded_byte_size" not in event

    # Undecodable compressed captures still pass evidence verification (preserving raw fault bytes),
    # but MUST refuse if declared as a successful used input.
    ev_valid = CaptureEvidence(tmp_path / "audit-only", "unified-agenda")
    t2 = ev_valid.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/html", "content-encoding": "gzip"},
                stream=_Chunks([truncated]),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(truncated) + 64,
    )
    with httpx.Client(transport=t2) as client:
        client.get(EDITION_REPORT_URL)
    verify_evidence(ev_valid.artifact_dir, expected_pin=ev_valid.seal(outcome="build-complete").pin)

    evidence.declare_used_inputs(
        [
            {
                "location": EDITION_REPORT_URL,
                "sha256": _sha(truncated),
                "bytes": len(truncated),
                "asset_kind": "unified-agenda-listing",
            }
        ],
        scope_id="unified-agenda-listing",
    )
    with pytest.raises(SourceEvidenceError, match="could not be decoded"):
        evidence.seal(outcome="build-complete")


def test_compressed_credential_echo_and_max_decoded_bytes_refuse_before_retention(tmp_path):
    secret = "compressed-secret-token-987654"
    payload = b"prefix-" + secret.encode() + b"-suffix"
    wire_secret = gzip.compress(payload, mtime=0)
    ev_cred = CaptureEvidence(tmp_path / "cred", "unified-agenda", archive_root=tmp_path / "raw-cred")
    ev_cred.credential = secret
    transport = ev_cred.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/html", "content-encoding": "gzip"},
                stream=_Chunks([wire_secret[:10], wire_secret[10:]]),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(wire_secret) + 64,
    )
    with httpx.Client(transport=transport) as client, pytest.raises(CredentialRefusedError):
        client.get(EDITION_REPORT_URL)
    assert not any(row["event"] == "capture" for row in _journal(ev_cred))
    assert _stored_objects(ev_cred) == []

    # Gzip bomb / oversized decoded payload refused by max_decoded_bytes.
    big = b"A" * (2 * 1024 * 1024)
    wire_big = gzip.compress(big, mtime=0)
    assert len(wire_big) < 10_000
    ev_bomb = CaptureEvidence(tmp_path / "bomb", "unified-agenda", archive_root=tmp_path / "raw-bomb")
    bomb_transport = ev_bomb.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "application/xml", "content-encoding": "gzip"},
                stream=_Chunks([wire_big]),
            )
        ),
        stage="unified-agenda-edition",
        max_bytes=len(wire_big) + 64,
        max_decoded_bytes=64 * 1024,
    )
    with httpx.Client(transport=bomb_transport) as client, pytest.raises(SourceEvidenceError, match="Decoded source response"):
        client.get(EDITION_REPORT_URL)
    assert not any(row["event"] == "capture" for row in _journal(ev_bomb))
    assert _stored_objects(ev_bomb) == []


@pytest.mark.parametrize("policy", ["hash_only", "metadata_only"])
def test_restricted_policies_respect_compressed_digest_visibility(tmp_path, policy):
    wire = gzip.compress(AGENDA_LISTING_HTML, mtime=0)
    wire_sha, decoded_sha = _sha(wire), _sha(AGENDA_LISTING_HTML)
    evidence = CaptureEvidence(tmp_path, "unified-agenda")
    source = evidence.for_source("reginfo", policy, parser_version="test/1", policy_decision_id="rights-v1")
    transport = source.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/html", "content-encoding": "gzip"},
                stream=_Chunks([wire]),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(wire) + 64,
    )
    with httpx.Client(transport=transport) as client:
        assert client.get(EDITION_REPORT_URL).content == AGENDA_LISTING_HTML
    [event] = [row for row in _journal(evidence) if row["event"] == "capture"]
    assert event["wire_byte_size"] == len(wire)
    assert event["decoded_byte_size"] == len(AGENDA_LISTING_HTML)
    if policy == "hash_only":
        assert event["wire_sha256"] == event["sha256"] == wire_sha
        assert event["decoded_sha256"] == decoded_sha
    else:
        assert "sha256" not in event and "wire_sha256" not in event and "decoded_sha256" not in event
        for path in evidence.directory.rglob("*"):
            if path.is_file():
                raw_bytes = path.read_bytes()
                assert wire_sha[7:].encode() not in raw_bytes
                assert decoded_sha[7:].encode() not in raw_bytes
    artifact = evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin)


def test_streaming_compressed_digest_bounds_memory_well_below_decoded_size(tmp_path):
    """Streaming _StreamCheck hashes a 16 MiB XML compressed to ~20 KiB in O(_CHUNK) memory."""
    chunk = b"<RIN_INFO><RIN>2060-AV12</RIN><ABSTRACT>" + b"x" * 960 + b"</ABSTRACT></RIN_INFO>\n"
    repeats = (16 * 1024 * 1024) // len(chunk)
    decoded_hash = hashlib.sha256()
    decoded_size = 0
    compressor = zlib.compressobj(level=6, wbits=16 + zlib.MAX_WBITS)
    wire_parts: list[bytes] = []
    for _ in range(repeats):
        decoded_hash.update(chunk)
        decoded_size += len(chunk)
        compressed = compressor.compress(chunk)
        if compressed:
            wire_parts.append(compressed)
    wire_parts.append(compressor.flush())
    wire = b"".join(wire_parts)
    assert len(wire) < 128 * 1024 and decoded_size >= 15 * 1024 * 1024

    evidence = CaptureEvidence(tmp_path, "unified-agenda")
    captured = CapturedBodyResponse(
        requested_url=EDITION_REPORT_URL,
        resolved_url=EDITION_REPORT_URL,
        status_code=200,
        content_type="application/xml",
        observed_at="2026-11-27T00:00:00Z",
        body=wire,
        content_encoding="gzip",
    )
    tracemalloc.start()
    try:
        receipt = evidence.capture(
            captured,
            stage="unified-agenda-edition",
            expected_wire_sha256=_sha(wire),
            expected_decoded_sha256="sha256:" + decoded_hash.hexdigest(),
        )
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert receipt["decoded_byte_size"] == decoded_size
    assert receipt["decoded_sha256"] == "sha256:" + decoded_hash.hexdigest()
    assert peak_bytes < 6 * 1024 * 1024, f"Expected streaming O(_CHUNK) peak (<6 MiB), got {peak_bytes / 1024**2:.2f} MiB"
    artifact = evidence.seal(outcome="build-complete")
    verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin)


def test_transport_refuses_credential_echo_across_1mib_decoded_chunk_boundary(tmp_path):
    """Decoded credential straddling the 1 MiB _CHUNK boundary refuses with zero blobs retained."""
    secret = "chunk-boundary-secret-token-123456"
    payload = b"A" * (_CHUNK - 5) + secret.encode() + b"B" * 100
    wire = gzip.compress(payload, mtime=0)
    evidence = CaptureEvidence(tmp_path / "boundary", "unified-agenda", archive_root=tmp_path / "raw-boundary")
    evidence.credential = secret
    transport = evidence.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/plain", "content-encoding": "gzip"},
                stream=_Chunks([wire]),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(wire) + 64,
    )
    with httpx.Client(transport=transport) as client, pytest.raises(CredentialRefusedError):
        client.get(EDITION_REPORT_URL)
    assert not any(row["event"] == "capture" for row in _journal(evidence))
    assert _stored_objects(evidence) == []


def test_retained_file_raw_source_with_decoded_digest_seals_and_verifies(tmp_path):
    """raw_source=True retained-file inputs accept matching decoded_sha256/decoded_bytes through seal()."""
    input_path = tmp_path / "input.xml"
    input_path.write_bytes(AGENDA_XML)
    digest = _sha(AGENDA_XML)
    raw_root = tmp_path / "raw-retained"

    ev = CaptureEvidence(tmp_path / "ok", "unified-agenda", archive_root=raw_root)
    ev.retain_file(input_path, stage="local", raw_source=True)
    ev.declare_used_inputs(
        [
            {
                "location": "input.xml",
                "sha256": digest,
                "bytes": len(AGENDA_XML),
                "decoded_sha256": digest,
                "decoded_bytes": len(AGENDA_XML),
                "asset_kind": "unified-agenda-edition",
            }
        ],
        scope_id="scope-retained-file",
    )
    artifact = ev.seal(outcome="build-complete")
    assert verify_evidence(ev.artifact_dir, expected_pin=artifact.pin, archive_root=raw_root).pin == artifact.pin

    # Mismatched decoded digest on a retained-file input still refuses.
    ev_bad = CaptureEvidence(tmp_path / "bad", "unified-agenda", archive_root=tmp_path / "raw-retained-bad")
    ev_bad.retain_file(input_path, stage="local", raw_source=True)
    ev_bad.declare_used_inputs(
        [
            {
                "location": "input.xml",
                "sha256": digest,
                "bytes": len(AGENDA_XML),
                "decoded_sha256": _sha(b"different"),
                "decoded_bytes": len(AGENDA_XML),
                "asset_kind": "unified-agenda-edition",
            }
        ],
        scope_id="scope-retained-file-bad",
    )
    with pytest.raises(SourceEvidenceError, match="recorded decoded digest"):
        ev_bad.seal(outcome="build-complete")


@pytest.mark.parametrize("split_mode", ["opening-1-byte", "all-1-byte"])
def test_raw_deflate_with_valid_zlib_cmf_byte_1byte_opening_chunk_seals_cleanly(tmp_path, split_mode):
    """Raw RFC 1951 deflate starting with 0x78 decodes identically regardless of 1-byte chunk splits."""
    wire = bytes([0x78, 0x00, 0x00, 0xFF, 0xFF, 0x01, 0x05, 0x00, 0xFA, 0xFF]) + b"hello"
    parts = [wire[:1], wire[1:]] if split_mode == "opening-1-byte" else [bytes([b]) for b in wire]
    raw_root = tmp_path / f"raw-{split_mode}"
    evidence = CaptureEvidence(tmp_path / split_mode, "unified-agenda", archive_root=raw_root)
    transport = evidence.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/plain", "content-encoding": "deflate"},
                stream=_Chunks(parts),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(wire) + 64,
        expected_wire_sha256=_sha(wire),
        expected_decoded_sha256=_sha(b"hello"),
    )
    with httpx.Client(transport=transport) as client:
        # Drain the retained stream through HTTPX (HTTPX's own DeflateDecoder may fail on 1-byte chunks).
        try:
            client.get(EDITION_REPORT_URL)
        except httpx.DecodingError:
            pass
    [event] = [row for row in _journal(evidence) if row["event"] == "capture"]
    assert event["wire_sha256"] == event["sha256"] == _sha(wire)
    assert event["decoded_sha256"] == _sha(b"hello")
    assert event["decoded_byte_size"] == 5
    assert "decoded_status" not in event
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin, archive_root=raw_root).pin == artifact.pin


def test_empty_gzip_member_binds_zero_byte_decoded_digest(tmp_path):
    wire = gzip.compress(b"", mtime=0)
    raw_root = tmp_path / "raw-empty-gzip"
    evidence = CaptureEvidence(tmp_path / "empty-gzip", "unified-agenda", archive_root=raw_root)
    captured = CapturedBodyResponse(
        requested_url=EDITION_REPORT_URL,
        resolved_url=EDITION_REPORT_URL,
        status_code=200,
        content_type="text/plain",
        observed_at="2026-11-27T00:00:00Z",
        body=wire,
        content_encoding="gzip",
    )
    receipt = evidence.capture(
        captured,
        stage="unified-agenda-listing",
        expected_wire_sha256=_sha(wire),
        expected_decoded_sha256=_sha(b""),
    )
    assert receipt["decoded_byte_size"] == 0
    assert receipt["decoded_sha256"] == _sha(b"")
    assert "decoded_status" not in receipt
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin, archive_root=raw_root).pin == artifact.pin


@pytest.mark.parametrize("bad_bound", [0, -1, True])
def test_capture_validates_max_decoded_bytes_without_poisoning_retention(tmp_path, bad_bound):
    wire = gzip.compress(AGENDA_XML, mtime=0)
    raw_root = tmp_path / f"raw-bad-{bad_bound}"
    evidence = CaptureEvidence(tmp_path / f"bad-{bad_bound}", "unified-agenda", archive_root=raw_root)
    captured = CapturedBodyResponse(
        requested_url=EDITION_REPORT_URL,
        resolved_url=EDITION_REPORT_URL,
        status_code=200,
        content_type="application/xml",
        observed_at="2026-11-27T00:00:00Z",
        body=wire,
        content_encoding="gzip",
    )
    with pytest.raises(ValueError, match="max_decoded_bytes must be positive"):
        evidence.capture(captured, stage="bad-bound", max_decoded_bytes=bad_bound)
    assert evidence.retention_failure is None
    assert _stored_objects(evidence) == []
    receipt = evidence.capture(captured, stage="valid-after-bad-bound", max_decoded_bytes=len(AGENDA_XML))
    assert receipt["decoded_sha256"] == _sha(AGENDA_XML)
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin, archive_root=raw_root).pin == artifact.pin


@pytest.mark.parametrize("via", ["capture", "transport"])
def test_unsupported_content_encoding_br_records_status_and_refuses_used_input(tmp_path, via):
    wire = b"\x8b\x02\x80hello\x03"
    raw_root = tmp_path / f"raw-br-{via}"
    evidence = CaptureEvidence(tmp_path / f"br-{via}", "unified-agenda", archive_root=raw_root)
    if via == "capture":
        captured = CapturedBodyResponse(
            requested_url=EDITION_REPORT_URL,
            resolved_url=EDITION_REPORT_URL,
            status_code=200,
            content_type="text/html",
            observed_at="2026-11-27T00:00:00Z",
            body=wire,
            content_encoding="br",
        )
        evidence.capture(captured, stage="unified-agenda-listing")
    else:
        transport = evidence.transport(
            httpx.MockTransport(
                lambda _: httpx.Response(
                    200,
                    headers={"content-type": "text/html", "content-encoding": "br"},
                    stream=_Chunks([wire]),
                )
            ),
            stage="unified-agenda-listing",
            max_bytes=len(wire) + 64,
        )
        with httpx.Client(transport=transport) as client:
            try:
                client.get(EDITION_REPORT_URL)
            except httpx.DecodingError:
                pass
    [event] = [row for row in _journal(evidence) if row["event"] == "capture"]
    assert event["wire_sha256"] == event["sha256"] == _sha(wire)
    assert event["wire_byte_size"] == len(wire)
    assert event["decoded_status"] == "unsupported_encoding"
    assert "decoded_sha256" not in event and "decoded_byte_size" not in event

    # Audit-only seal and verification succeed cleanly.
    artifact = evidence.seal(outcome="build-complete")
    assert verify_evidence(evidence.artifact_dir, expected_pin=artifact.pin, archive_root=raw_root).pin == artifact.pin

    # Declaring an unsupported_encoding capture as a used input refuses at seal().
    ev_used = CaptureEvidence(tmp_path / f"br-used-{via}", "unified-agenda", archive_root=tmp_path / f"raw-br-used-{via}")
    ev_used.capture(
        CapturedBodyResponse(
            requested_url=EDITION_REPORT_URL,
            resolved_url=EDITION_REPORT_URL,
            status_code=200,
            content_type="text/html",
            observed_at="2026-11-27T00:00:00Z",
            body=wire,
            content_encoding="br",
        ),
        stage="unified-agenda-listing",
    )
    ev_used.declare_used_inputs(
        [
            {
                "location": EDITION_REPORT_URL,
                "sha256": _sha(wire),
                "bytes": len(wire),
                "asset_kind": "unified-agenda-listing",
            }
        ],
        scope_id="scope-br-used",
    )
    with pytest.raises(SourceEvidenceError, match="could not be decoded"):
        ev_used.seal(outcome="build-complete")

    # Expecting a decoded digest on unsupported encoding refuses before retaining any blob.
    ev_exp = CaptureEvidence(tmp_path / f"br-exp-{via}", "unified-agenda", archive_root=tmp_path / f"raw-br-exp-{via}")
    exp_transport = ev_exp.transport(
        httpx.MockTransport(
            lambda _: httpx.Response(
                200,
                headers={"content-type": "text/html", "content-encoding": "br"},
                stream=_Chunks([wire]),
            )
        ),
        stage="unified-agenda-listing",
        max_bytes=len(wire) + 64,
        expected_decoded_sha256=_sha(b"hello"),
    )
    with httpx.Client(transport=exp_transport) as client, pytest.raises(SourceEvidenceError, match="could not be decoded"):
        client.get(EDITION_REPORT_URL)
    assert _stored_objects(ev_exp) == []


@pytest.mark.parametrize("case", ["identity_status", "metadata_only_bad_status", "metadata_only_both_size_and_status"])
def test_verify_evidence_rejects_invalid_decoded_status_on_identity_and_metadata_only(tmp_path, case):
    raw_root = tmp_path / f"raw-{case}"
    evidence = CaptureEvidence(tmp_path / case, "unified-agenda", archive_root=raw_root)
    if case == "identity_status":
        evidence.capture(
            CapturedBodyResponse(
                requested_url=EDITION_REPORT_URL,
                resolved_url=EDITION_REPORT_URL,
                status_code=200,
                content_type="text/html",
                observed_at="2026-11-27T00:00:00Z",
                body=AGENDA_LISTING_HTML,
                content_encoding="identity",
            ),
            stage="unified-agenda-listing",
        )
        rows = _journal(evidence)
        next(row for row in rows if row["event"] == "capture")["decoded_status"] = "undecodable"
    else:
        wire = gzip.compress(AGENDA_LISTING_HTML, mtime=0)
        source = evidence.for_source("reginfo", "metadata_only", parser_version="test/1", policy_decision_id="rights-v1")
        source.capture(
            CapturedBodyResponse(
                requested_url=EDITION_REPORT_URL,
                resolved_url=EDITION_REPORT_URL,
                status_code=200,
                content_type="text/html",
                observed_at="2026-11-27T00:00:00Z",
                body=wire,
                content_encoding="gzip",
            ),
            stage="unified-agenda-listing",
        )
        rows = _journal(evidence)
        cap_row = next(row for row in rows if row["event"] == "capture")
        if case == "metadata_only_bad_status":
            cap_row.pop("decoded_byte_size")
            cap_row["decoded_status"] = "bogus"
        else:
            cap_row["decoded_status"] = "undecodable"
    (evidence.artifact_dir / "journal.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    with pytest.raises(SourceEvidenceError):
        evidence.seal(outcome="build-complete")
