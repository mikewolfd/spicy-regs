"""Offline admission and refusal do not confuse baselines with source replay."""

import hashlib
import json

import pyarrow.parquet as pq
import pytest

from spicy_regs.etl_receipts import validate_receipt_bundle, decode_exact_json
from spicy_regs.transforms.historical_regulations import POLICIES, build_historical_regulations

XML = b'<ECFR><DIV1 TYPE="TITLE" N="1"><HEAD>Title 1</HEAD><DIV5 TYPE="PART" N="18"><DIV8 TYPE="SECTION" N="18.1"><P>Literal text</P></DIV8></DIV5></DIV1></ECFR>'


def selection(tmp_path, *, body=XML, digest=None):
    source = tmp_path / "original.xml"
    source.write_bytes(body)
    manifest = tmp_path / "selection.json"
    manifest.write_text(
        json.dumps(
            {
                "format": "spicy-historical-regulations",
                "version": 1,
                "sources": [
                    {
                        "kind": "ecfr-snapshot",
                        "path": source.name,
                        "bytes": len(body),
                        "sha256": digest or "sha256:" + hashlib.sha256(body).hexdigest(),
                        "selection": {"title": 1, "date": "2026-08-10"},
                        "source_url": "https://www.ecfr.gov/api/versioner/v1/full/2026-08-10/title-1.xml",
                    }
                ],
            }
        )
    )
    return manifest


def test_source_only_bundle_and_receipts(tmp_path):
    bundle = build_historical_regulations(selection(tmp_path), tmp_path / "bundle")
    rows = pq.read_table(bundle / "ecfr_snapshot_sections/ecfr_snapshot_sections.parquet").to_pylist()
    assert rows[0]["text"] == "Literal text" and rows[0]["part"] == "18"
    snapshots = pq.read_table(bundle / "ecfr_snapshots/ecfr_snapshots.parquet").to_pylist()
    assert rows[0]["snapshot_id"] == snapshots[0]["snapshot_id"]
    validate_receipt_bundle(
        {name: [bundle / name / (name + ".parquet")] for name in POLICIES},
        [bundle / "etl_receipts.parquet"],
        list(POLICIES.values()),
    )
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


def test_public_receipts_allowlist_selection_metadata_and_content_address_originals(tmp_path):
    manifest = selection(tmp_path)
    selected = json.loads(manifest.read_text())
    selected["private"] = {"path": "/Users/private/manifest"}
    selected["sources"][0]["debug"] = {"path": "/Users/private/nested"}
    selected["sources"][0]["retained_renditions"] = [{"path": "/Users/private/unselected"}]
    manifest.write_text(json.dumps(selected))
    bundle = build_historical_regulations(manifest, tmp_path / "bundle")
    receipts = pq.read_table(bundle / "etl_receipts.parquet").to_pylist()
    encoded = json.dumps(receipts)
    assert "/Users/private" not in encoded and str(tmp_path) not in encoded
    raw = manifest.read_bytes()
    source_digest = selected["sources"][0]["sha256"]
    manifest_digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    for row in receipts:
        assert row["witnesses"][0]["source_uri"] == source_digest
        assert row["witnesses"][1]["source_uri"] == manifest_digest
        assert row["generation_id"] != manifest_digest
        assert row["policy_version"] == "historical-regulations-public-v2"
        processing = decode_exact_json(row["processing_json"])
        assert "debug" not in json.dumps(processing)
    assert (bundle / "selection.json").read_bytes() == raw


def test_nested_retained_rendition_paths_do_not_enter_subject_or_receipts(tmp_path):
    body = b"""<mods xmlns="http://www.loc.gov/mods/v3"><titleInfo><title>Federal Register</title></titleInfo>
<originInfo><dateIssued>1939-11-17</dateIssued></originInfo><extension><accessId>FR-1939-11-17</accessId>
<collectionCode>FR</collectionCode></extension><location><url>https://www.govinfo.gov/content/pkg/FR-1939-11-17/pdf/FR-1939-11-17.pdf</url></location></mods>"""
    (tmp_path / "mods.xml").write_bytes(body)
    rendition = b"%PDF retained selection test"
    (tmp_path / "file.pdf").write_bytes(rendition)
    spec = {
        "kind": "fr-issue",
        "path": "mods.xml",
        "bytes": len(body),
        "sha256": "sha256:" + hashlib.sha256(body).hexdigest(),
        "package_id": "FR-1939-11-17",
        "retained_renditions": [
            {
                "path": "file.pdf",
                "bytes": len(rendition),
                "sha256": "sha256:" + hashlib.sha256(rendition).hexdigest(),
                "source_url": "https://www.govinfo.gov/content/pkg/FR-1939-11-17/pdf/FR-1939-11-17.pdf",
                "private": {"path": "/Users/private/rendition"},
            }
        ],
    }
    manifest = tmp_path / "selection.json"
    manifest.write_text(json.dumps({"format": "spicy-historical-regulations", "version": 1, "sources": [spec]}))
    output = build_historical_regulations(manifest, tmp_path / "bundle")
    subject = pq.read_table(output / "federal_register_issues/federal_register_issues.parquet").to_pylist()
    receipts = pq.read_table(output / "etl_receipts.parquet").to_pylist()
    assert "/Users/private" not in json.dumps(subject + receipts)
    retained = json.loads(subject[0]["retained_renditions_json"])
    assert set(retained[0]) == {"source_url", "sha256", "bytes"}
    validate_receipt_bundle(
        {name: [output / name / (name + ".parquet")] for name in POLICIES},
        [output / "etl_receipts.parquet"],
        list(POLICIES.values()),
    )


def annual_selection(tmp_path):
    from tests.test_cfr_sections import RETAINED_XML, retained_mods

    sources = {}
    for name, body, url in (
        ("mods", retained_mods(), "https://www.govinfo.gov/metadata/pkg/CFR-2025-title1-vol1/mods.xml"),
        ("xml", RETAINED_XML, "https://www.govinfo.gov/bulkdata/CFR/2025/title-1/CFR-2025-title1-vol1.xml"),
    ):
        digest = "sha256:" + hashlib.sha256(body).hexdigest()
        (tmp_path / f"{name}.xml").write_bytes(body)
        record = {"url": url, "final_url": url, "status": 200, "sha256": digest, "bytes": len(body)}
        raw = json.dumps(record).encode()
        (tmp_path / f"{name}.capture.json").write_bytes(raw)
        sources[name] = {
            "path": f"{name}.xml",
            "sha256": digest,
            "bytes": len(body),
            "source_url": url,
            "capture": {
                "path": f"{name}.capture.json",
                "sha256": "sha256:" + hashlib.sha256(raw).hexdigest(),
                "bytes": len(raw),
                "expected_record": record,
            },
        }
    spec = {
        "kind": "annual-cfr",
        "selection": {"year": 2025, "title": 1, "volume": 1},
        **sources,
        "expected_constituents": 2,
        "expected_sections": 1,
        "expected_edition": {
            "year": 2025,
            "title": 1,
            "volume": 1,
            "date_issued": "2025-01-01",
            "original_date_issued": "2023-01-01",
            "is_cover_only": True,
            "edition_id": None,
            "is_current_edition": None,
            "is_fallback_title": None,
            "title_text": None,
        },
        "expected_native_identity": {
            "source": "annual-cfr",
            "title": 1,
            "volume": None,
            "part": None,
            "section": None,
            "root_tag": "CFRDOC",
            "title_text": "General Provisions",
            "stated_date": "2023-01-01",
            "revision_text": "Revised as of January 1, 2023",
            "amendment_dates": [],
            "identity_basis": ["title:native", "volume:request-url", "edition:request-url"],
        },
        "private": {"path": "/Users/private/unselected"},
    }
    manifest = tmp_path / "annual-selection.json"
    manifest.write_text(json.dumps({"format": "spicy-historical-regulations", "version": 1, "sources": [spec]}))
    return manifest


def test_retained_annual_bundle_links_originals_captures_and_all_row_contexts(tmp_path):
    manifest = annual_selection(tmp_path)
    bundle = build_historical_regulations(manifest, tmp_path / "annual-bundle")
    rows = pq.read_table(bundle / "cfr_sections/cfr_sections.parquet").to_pylist()
    assert [row["granule_id"] for row in rows] == ["CFR-2025-title1-vol1", "CFR-2025-title1-vol1-sec1-1"]
    assert rows[0]["cfr_ref"] is None and rows[1]["cfr_ref"] == "1-1.1"
    receipts = pq.read_table(bundle / "cfr_sections/etl_receipts.parquet").to_pylist()
    spec = json.loads(manifest.read_text())["sources"][0]
    assert len(receipts) == len(rows)
    for index, (row, receipt) in enumerate(zip(rows, receipts, strict=True)):
        assert receipt["policy_version"] == receipt["processor"] == "retained-annual-cfr-v1"
        assert receipt["outcome"] == "accepted"
        assert [witness["sha256"] for witness in receipt["witnesses"]] == [
            spec["mods"]["sha256"],
            spec["mods"]["capture"]["sha256"],
            spec["xml"]["sha256"],
            spec["xml"]["capture"]["sha256"],
            "sha256:" + hashlib.sha256(manifest.read_bytes()).hexdigest(),
        ]
        assert decode_exact_json(receipt["identity_json"]) == [["granule_id", row["granule_id"]]]
        processing = decode_exact_json(receipt["processing_json"])
        assert processing["source_context"]["constituent_position"] == index
        assert processing["source_context"]["granule_id"] == row["granule_id"]
        assert "annual_source_path" not in processing["source_context"]
        assert processing["selection_metadata"]["edition"]["date_issued"] == "2025-01-01"
        assert processing["selection_metadata"]["native_identity"]["stated_date"] == "2023-01-01"
    assert str(tmp_path) not in json.dumps(receipts) and "/Users/private" not in json.dumps(receipts)
    validate_receipt_bundle(
        {name: [bundle / name / (name + ".parquet")] for name in POLICIES},
        [bundle / "etl_receipts.parquet"],
        list(POLICIES.values()),
    )
    with pytest.raises(FileExistsError):
        build_historical_regulations(manifest, bundle)


@pytest.mark.parametrize("change", ["capture", "native", "duplicate"])
def test_retained_annual_refusal_keeps_destination_absent(tmp_path, change):
    manifest = annual_selection(tmp_path)
    value = json.loads(manifest.read_text())
    spec = value["sources"][0]
    if change == "capture":
        capture = spec["mods"]["capture"]
        capture["expected_record"]["final_url"] = "https://www.govinfo.gov/wrong"
        raw = json.dumps(capture["expected_record"]).encode()
        (tmp_path / capture["path"]).write_bytes(raw)
        capture.update(bytes=len(raw), sha256="sha256:" + hashlib.sha256(raw).hexdigest())
    elif change == "native":
        spec["expected_native_identity"]["stated_date"] = "2025-01-01"
    else:
        value["sources"].append(spec)
    manifest.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        build_historical_regulations(manifest, tmp_path / "refused")
    assert not (tmp_path / "refused").exists()
