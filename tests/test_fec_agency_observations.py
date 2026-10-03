"""Agency originals and pinned releases retain every native record in the existing FEC tables."""

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from rulespec_artifacts import Producer
from spicy_docs.releases.format import VERIFIER_ID, VERIFIER_VERSION
from spicy_docs.source_native import SourceNativeReleaseBuild, SourceNativeReleasePublisher
from spicy_docs.sources.agency_reports.foia import parse_foia_annual_report
from spicy_docs.sources.agency_reports.oversight import parse_oversight_report
from spicy_docs.sources.fec.agency_profile import (
    FEC_AGENCY_REPORT_PROFILE,
    agency_report_scope,
    iter_retained_agency_originals,
)
from spicy_docs.storage.blobs import LocalSourceNativeBlobStore

from spicy_regs.transforms.build_fec_observations import build_fec_observations
from tests.test_fec_observations import _manifest, _rows

FIXTURES = Path(__file__).parent / "fixtures" / "fec-agency"
SOURCES = json.loads((FIXTURES / "sources.json").read_text())
IMPLEMENTATION = "git+https://example.com/fixture@" + "a" * 40


def _input(tmp_path, source):
    raw = (FIXTURES / source["file"]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == source["sha256"] and len(raw) == source["bytes"]
    capture = {
        "requestUrl": source["url"],
        "observedAt": "2026-09-30T00:00:00Z",
        "responseSha256": "sha256:" + source["sha256"],
        "byteSize": len(raw),
        "representation": "opaque",
    }
    store = LocalSourceNativeBlobStore(tmp_path / "blobs")
    store.put_blob(capture["responseSha256"], len(raw), [raw])
    scope = agency_report_scope(
        capture, format="foia-xml" if source["file"].endswith(".xml") else "oversight-html", max_records_per_page=3
    )
    return {
        "collection_id": source["file"],
        "source_family": "fec_agency_reports",
        "profile": "agency",
        "blob_root": "blobs",
        "scope": scope,
    }, raw


def _release(tmp_path, item):
    release = SourceNativeReleasePublisher(
        FEC_AGENCY_REPORT_PROFILE,
        blob_store=LocalSourceNativeBlobStore(tmp_path / "release-blobs"),
        clock=lambda: datetime(2026, 9, 30, tzinfo=UTC),
    ).publish(
        iter_retained_agency_originals(item["scope"], blob_source=LocalSourceNativeBlobStore(tmp_path / "blobs")),
        build=SourceNativeReleaseBuild(
            query_scope=item["scope"],
            started_at="2026-09-30T00:00:00Z",
            producer=Producer(
                product="spicy-docs",
                implementation_id=IMPLEMENTATION,
                verifier_id=VERIFIER_ID,
                verifier_version=VERIFIER_VERSION,
                verifier_implementation_id=IMPLEMENTATION,
            ),
        ),
        destination=tmp_path / "release",
    )
    return {key: value for key, value in item.items() if key not in {"scope", "blob_root"}} | {
        "blob_root": "release-blobs",
        "release_path": "release",
        "artifact_sha256": release.artifact.pin.artifact_digest,
        "verifier_implementation_id": IMPLEMENTATION,
    }


@pytest.mark.parametrize("source", SOURCES, ids=lambda s: s["file"])
@pytest.mark.parametrize("mode", ["scope", "release"])
def test_agency_inputs_preserve_every_native_record_without_invented_relationships(tmp_path, source, mode):
    item, raw = _input(tmp_path, source)
    if mode == "release":
        item = _release(tmp_path, item)
    records, collections, relationships = _rows(
        build_fec_observations(_manifest(tmp_path, [item]), tmp_path / "out", batch_size=2)
    )
    metadata = [json.loads(r["metadata_json"]) for r in records]
    native = (
        parse_foia_annual_report(raw)
        if source["file"].endswith(".xml")
        else parse_oversight_report(raw, url=source["url"])
    )
    assert metadata[0]["report"] == {"source": native["source"], "metadata": native["metadata"]}
    if source["file"].endswith(".xml"):
        assert [r["element"] for r in metadata[1:]] == native["elements"]
    else:
        assert [r["field"] for r in metadata if r["kind"] == "oversight-field"] == native["metadata"]["fields"]
        assert [r["body"] for r in metadata if r["kind"] == "oversight-body"] == native["bodies"]
        assert [r["asset"] for r in metadata if r["kind"] == "oversight-asset"] == native["assets"]
        assert [asset for r in records for asset in json.loads(r["assets_json"])] == native["assets"]
        assert [body for r in records for body in json.loads(r["embedded_bodies_json"])] == native["bodies"]
    assert not relationships
    assert all(r[key] is None for r in records for key in ("committee_id", "candidate_id", "filing_id"))
    assert [json.loads(r["source_locator_json"])["ordinal"] for r in records] == list(range(len(records)))
    assert all(r["source_sha256"] == "sha256:" + source["sha256"] for r in records)
    assert all(
        json.loads(r["source_record_json"])["record"]["record"] == native
        for r, native in zip(records, metadata, strict=True)
    )
    assert collections[0]["record_count"] == len(records)
    assert collections[0]["relationship_count"] == 0
    assert collections[0]["artifact_sha256"] == item.get("artifact_sha256")
    assert json.loads(collections[0]["coverage_limits_json"])[0].startswith("One selected retained original")


@pytest.mark.parametrize("failure", ["bytes", "pin", "flat-opc"])
def test_agency_failure_installs_no_partial_family(tmp_path, failure):
    good, _ = _input(tmp_path, SOURCES[0])
    bad, _ = _input(tmp_path, SOURCES[1])
    if failure == "pin":
        bad = _release(tmp_path, bad)
        bad["artifact_sha256"] = "sha256:" + "0" * 64
    elif failure == "bytes":
        digest = bad["scope"]["capture"]["responseSha256"][7:]
        (tmp_path / "blobs" / "sha256" / digest).write_bytes(b"changed original")
    else:
        raw = b'<pkg:package xmlns:pkg="http://schemas.microsoft.com/office/2006/xmlPackage"/>'
        digest = "sha256:" + hashlib.sha256(raw).hexdigest()
        LocalSourceNativeBlobStore(tmp_path / "blobs").put_blob(digest, len(raw), [raw])
        bad["scope"]["capture"].update(responseSha256=digest, byteSize=len(raw))
    with pytest.raises(ValueError):
        build_fec_observations(_manifest(tmp_path, [good, bad]), tmp_path / "out", batch_size=1)
    assert not (tmp_path / "out").exists()
