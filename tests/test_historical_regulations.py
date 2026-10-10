"""Offline admission and refusal do not confuse baselines with source replay."""

import hashlib
import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import validate_receipt_bundle
from spicy_regs.transforms.historical_regulations import POLICIES, build_historical_regulations

XML = b'<ECFR><DIV1 TYPE="TITLE" N="1"><HEAD>Title 1</HEAD><DIV5 TYPE="PART" N="18"><DIV8 TYPE="SECTION" N="18.1"><P>Literal text</P></DIV8></DIV5></DIV1></ECFR>'


def selection(tmp_path, *, body=XML, digest=None):
    source = tmp_path / "original.xml"
    source.write_bytes(body)
    manifest = tmp_path / "selection.json"
    manifest.write_text(json.dumps({"format": "spicy-historical-regulations", "version": 1, "sources": [{
        "kind": "ecfr-snapshot", "path": source.name, "bytes": len(body),
        "sha256": digest or "sha256:" + hashlib.sha256(body).hexdigest(),
        "selection": {"title": 1, "date": "2026-08-10"},
        "source_url": "https://www.ecfr.gov/api/versioner/v1/full/2026-08-10/title-1.xml"}]}))
    return manifest


def test_source_only_bundle_and_receipts(tmp_path):
    bundle = build_historical_regulations(selection(tmp_path), tmp_path / "bundle")
    rows = pq.read_table(bundle / "ecfr_snapshot_sections/ecfr_snapshot_sections.parquet").to_pylist()
    assert rows[0]["text"] == "Literal text" and rows[0]["part"] == "18"
    snapshots = pq.read_table(bundle / "ecfr_snapshots/ecfr_snapshots.parquet").to_pylist()
    assert rows[0]["snapshot_id"] == snapshots[0]["snapshot_id"]
    validate_receipt_bundle({name: [bundle / name / (name + ".parquet")] for name in POLICIES},
                           [bundle / "etl_receipts.parquet"], list(POLICIES.values()))
    with pytest.raises(FileExistsError):
        build_historical_regulations(tmp_path / "selection.json", bundle)


def test_failed_original_keeps_requested_destination_absent(tmp_path):
    manifest = selection(tmp_path, digest="sha256:" + "0" * 64)
    with pytest.raises(ValueError, match="digest"):
        build_historical_regulations(manifest, tmp_path / "refused")
    assert not (tmp_path / "refused").exists()


def test_same_scope_with_omitted_nulls_refuses_duplicates(tmp_path):
    manifest = selection(tmp_path)
    value = json.loads(manifest.read_text())
    repeat = dict(value["sources"][0])
    repeat["selection"] = {**repeat["selection"], "part": None, "section": None}
    value["sources"].append(repeat)
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="repeated dated snapshot"):
        build_historical_regulations(manifest, tmp_path / "duplicates")
    assert not (tmp_path / "duplicates").exists()
