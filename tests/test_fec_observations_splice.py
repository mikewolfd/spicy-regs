"""Tests for receipts-free streaming ``fec-observations`` cp1252 splice (ss-42zk / ss-42nr)."""

from __future__ import annotations

from collections import namedtuple
import hashlib
import io
import json
from pathlib import Path
import shutil
from zipfile import ZipFile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.fec.delimited import verified_archive_session
from spicy_docs.sources.fec.row_profile import positional_row_scope
from spicy_docs.storage.blobs import LocalSourceNativeBlobStore
from spicy_docs.storage.raw_archive import CentralRawArchiveStore

from spicy_regs.generations import admit_generation_for_publication
from spicy_regs.pipelines.fec_query_rebuild import (
    _safe_cleanup_staging,
    check_disk_headroom,
    splice_observations_cmd,
)
from spicy_regs.transforms.build_fec_observations import OUTPUTS, build_fec_observations
from spicy_regs.transforms.fec_identity_context_fields import normalize_record
from spicy_regs.transforms.fec_identity_receipts import dataset_policy
from spicy_regs.transforms.fec_observations_splice import (
    CP1252_SPLICE_COLLECTION_IDS,
    CollectionContextRepair,
    correct_manifest_encodings,
    splice_fec_observations,
)
from spicy_regs.transforms.fec_query_builder import make_filing_body_reader

CID_ZIP_1981885, CID_1998706, CID_1998705 = CP1252_SPLICE_COLLECTION_IDS
CID_UTF8 = "retained-filing-untouched-utf8"
CID_SENATE_NBSP = "senate-unofficial-nbsp"
CID_DOJ_CONTEXT = "research-capture-doj-test-0000"


def _put_raw(
    store: CentralRawArchiveStore,
    payload: bytes,
    legacy_store: LocalSourceNativeBlobStore | None = None,
) -> tuple[str, int]:
    digest = "sha256:" + hashlib.sha256(payload).hexdigest()
    store.put_blob(digest, len(payload), (payload,))
    if legacy_store is not None:
        legacy_store.put_blob(digest, len(payload), (payload,))
    return digest, len(payload)


def _fec_bytes(committee_id: str, narrative_bytes: bytes) -> bytes:
    return (
        b"HDR\x1cFEC\x1c8.5\x1cTEST\r\n"
        + b"F99\x1c"
        + committee_id.encode("ascii")
        + b"\r\n"
        + b"[BEGINTEXT]\r\n"
        + narrative_bytes
        + b"\r\n[ENDTEXT]\r\n"
    )


def _build_splice_fixture(tmp_path: Path, *, schema_mode: str = "subject", inject_doj_c1: bool = False):
    """Create a read-only raw store, a latin-1 manifest, and verified prior ``fec-observations`` Parquets."""
    raw_root = tmp_path / "raw_archive"
    store = CentralRawArchiveStore(raw_root)
    legacy_blobs = tmp_path / "legacy_blobs"
    legacy_store = LocalSourceNativeBlobStore(legacy_blobs)

    # 1. ZIP member 1981885.fec containing cp1252 0x92 ("AFC PAC’s")
    f1981885 = _fec_bytes("C00700001", b"Funds drawn from AFC PAC\x92s bank account.")
    zip_buf = io.BytesIO()
    with ZipFile(zip_buf, "w") as zf:
        zf.writestr("1981885.fec", f1981885)
    zip_bytes = zip_buf.getvalue()
    zip_sha, zip_size = _put_raw(store, zip_bytes, legacy_store)

    # 2. Opaque 1998705.fec and 1998706.fec containing cp1252 0x93/0x94 (“Agreement”)
    f1998705 = _fec_bytes("C00700002", b"Loan Agreement (this \x93Agreement\x94) dated May 1.")
    f1998705_sha, f1998705_size = _put_raw(store, f1998705, legacy_store)
    f1998706 = _fec_bytes("C00700003", b"Loan Agreement (this \x93Agreement\x94) Amendment.")
    f1998706_sha, f1998706_size = _put_raw(store, f1998706, legacy_store)

    # 3. Untouched clean UTF-8 filing + untouched latin-1 NBSP-only Senate filing (0xA0 is NOT C1)
    f_utf8 = _fec_bytes("C00700004", "Clean UTF-8 filing — untouched.".encode("utf-8"))
    f_utf8_sha, f_utf8_size = _put_raw(store, f_utf8, legacy_store)
    f_senate = _fec_bytes("C00700005", b"Senator\xa0Smith filing without C1 bytes.")
    f_senate_sha, f_senate_size = _put_raw(store, f_senate, legacy_store)

    def _scope(url: str, sha: str, size: int, encoding: str, member: dict | None = None) -> dict:
        return positional_row_scope(
            {
                "requestUrl": url,
                "observedAt": "2026-09-21T10:00:00Z",
                "responseSha256": sha,
                "byteSize": size,
                "representation": "zip" if member else "opaque",
            },
            format="fec",
            encoding=encoding,
            member=member,
        )

    from typing import Any

    collections: list[dict[str, Any]] = [
        {
            "collection_id": CID_UTF8,
            "source_family": "fec_reports",
            "profile": "positional",
            "blob_root": "legacy_blobs",
            "scope": _scope("https://docquery.fec.gov/dcdev/posted/1900001.fec", f_utf8_sha, f_utf8_size, "utf-8"),
        },
        {
            "collection_id": CID_ZIP_1981885,
            "source_family": "fec_reports",
            "profile": "positional",
            "blob_root": "legacy_blobs",
            "scope": _scope(
                "https://cg-519a459a-0ea3-42c2-b7bc-fa1143481f74.s3-us-gov-west-1.amazonaws.com/bulk-downloads/electronic/20260608.zip",
                zip_sha,
                zip_size,
                "latin-1",
                member={"ordinal": 0, "name": "1981885.fec"},
            ),
        },
        {
            "collection_id": CID_SENATE_NBSP,
            "source_family": "fec_reports",
            "profile": "positional",
            "blob_root": "legacy_blobs",
            "scope": _scope("https://sers.fec.gov/filings/999.fec", f_senate_sha, f_senate_size, "latin-1"),
        },
        {
            "collection_id": CID_1998705,
            "source_family": "fec_reports",
            "profile": "positional",
            "blob_root": "legacy_blobs",
            "scope": _scope("https://docquery.fec.gov/dcdev/posted/1998705.fec", f1998705_sha, f1998705_size, "latin-1"),
        },
        {
            "collection_id": CID_1998706,
            "source_family": "fec_reports",
            "profile": "positional",
            "blob_root": "legacy_blobs",
            "scope": _scope("https://docquery.fec.gov/dcdev/posted/1998706.fec", f1998706_sha, f1998706_size, "latin-1"),
        },
    ]

    manifest_obj = {"version": 1, "collections": collections}
    manifest_bytes = (json.dumps(manifest_obj, sort_keys=True) + "\n").encode("utf-8")
    manifest_sha, _ = _put_raw(store, manifest_bytes)

    # Current spicy-docs refuses latin-1 on cp1252 bytes (FilingEncodingError). Build the
    # historical 4cb0dffa-like prior Parquets via cp1252 first, then rewrite ONLY the 3 target
    # collections back to their pre-guard latin-1 metadata/locator state.
    cp1252_target_set = set(CP1252_SPLICE_COLLECTION_IDS)
    bootstrap_collections = [
        {**c, "scope": {**c["scope"], "encoding": "cp1252"}} if c["collection_id"] in cp1252_target_set else c
        for c in collections
    ]
    bootstrap_manifest_file = tmp_path / "bootstrap_manifest.json"
    bootstrap_manifest_file.write_bytes((json.dumps({"version": 1, "collections": bootstrap_collections}, sort_keys=True) + "\n").encode("utf-8"))

    full_prior_dir = tmp_path / "prior_full"
    build_fec_observations(bootstrap_manifest_file, full_prior_dir, batch_size=2)
    # Remove legacy_blobs after building prior Parquets to prove splice reads ONLY CentralRawArchiveStore(create=False).
    shutil.rmtree(legacy_blobs)

    # Revert ONLY the 3 target collections in full_prior_dir to historical latin-1 state (mimicking live 4cb0dffa).
    def _to_historical_latin1(value: str | None) -> str | None:
        if value is None:
            return None
        return value.replace('"cp1252"', '"latin-1"').replace("\u2019", "\x92").replace("\u201c", "\x93").replace("\u201d", "\x94")

    for filename in ("fec_source_records.parquet", "fec_collections.parquet"):
        path = full_prior_dir / filename
        tbl = pq.read_table(path)
        rows = tbl.to_pylist()
        rewritten = []
        for row in rows:
            if row["collection_id"] in cp1252_target_set:
                rewritten.append({k: _to_historical_latin1(v) if isinstance(v, str) else v for k, v in row.items()})
            else:
                rewritten.append(row)
        pq.write_table(pa.Table.from_pylist(rewritten, schema=tbl.schema), path, compression="zstd")

    doj_pin = "sha256:" + "d" * 64
    if schema_mode == "full":
        prior_dir = full_prior_dir
    else:
        prior_dir = tmp_path / "prior_subject"
        prior_dir.mkdir()
        for table_name, filename in (
            ("fec_source_records", "fec_source_records.parquet"),
            ("fec_collections", "fec_collections.parquet"),
            ("fec_relationships", "fec_relationships.parquet"),
        ):
            schema = dataset_policy(table_name).subject_schema
            rows = pq.read_table(full_prior_dir / filename).to_pylist()
            if table_name == "fec_source_records":
                normed = [normalize_record(table_name, r, observation_ordinal=i) for i, r in enumerate(rows)]
            else:
                normed = [normalize_record(table_name, r) for r in rows]
            aligned = [{name: r[name] for name in schema.names} for r in normed]
            pq.write_table(pa.Table.from_pylist(aligned, schema=schema), prior_dir / filename, compression="zstd")

    if inject_doj_c1:
        col_path = prior_dir / "fec_collections.parquet"
        col_table = pq.read_table(col_path)
        col_rows = col_table.to_pylist()
        doj_outcome = {
            "sourceStateScope": "retained_context",
            "recordOutcome": "retained_unparsed",
            "receiverDisposition": {
                "callerContext": {
                    "pin": {"sha256": doj_pin},
                    "teaser": "Following Judge Bell\x92s ruling on campaign contributions.",
                }
            },
        }
        doj_row = {
            **col_rows[0],
            "collection_id": CID_DOJ_CONTEXT,
            "source_family": "fec_research_context",
            "profile": "document",
            "record_count": 0,
            "relationship_count": 0,
            "record_outcome": "retained_unparsed",
            "collection_outcome_json": json.dumps(doj_outcome, ensure_ascii=False),
        }
        col_rows.append(doj_row)
        pq.write_table(pa.Table.from_pylist(col_rows, schema=col_table.schema), col_path, compression="zstd")

    ro_store = CentralRawArchiveStore(raw_root, create=False)
    return {
        "raw_root": raw_root,
        "store": ro_store,
        "prior_dir": prior_dir,
        "manifest_bytes": manifest_bytes,
        "manifest_sha": manifest_sha,
        "doj_pin": doj_pin,
    }


@pytest.mark.parametrize("schema_mode", ["subject", "full"])
def test_splice_cp1252_filings_preserves_untouched_collections_and_seals_receipts_free(tmp_path, schema_mode):
    fx = _build_splice_fixture(tmp_path, schema_mode=schema_mode)
    out_dir = tmp_path / f"spliced_{schema_mode}"

    result = splice_fec_observations(
        prior_dir=fx["prior_dir"],
        output_dir=out_dir,
        store=fx["store"],
        manifest_sha256=fx["manifest_sha"],
        target_collection_ids=CP1252_SPLICE_COLLECTION_IDS,
        batch_size=2,  # forces collections to straddle Arrow batch boundaries
        c1_threads=2,
    )

    # 1. Receipts-free publication admission: NO etlReceipts, NO etl_receipts.parquet.
    artifact = admit_generation_for_publication(result.generation_dir)
    assert artifact.pin.artifact_digest == result.artifact_digest
    assert "etlReceipts" not in artifact.root["spec"]
    assert not (result.generation_dir / "etl_receipts.parquet").exists()

    # 2. Untouched collections are bit/value-identical (including observation_ordinal in subject mode).
    assert result.telemetry.c1_control_matches == 0
    assert result.telemetry.untouched_collections_count == 2
    assert result.telemetry.zip_spool_opens == 3
    for filename in OUTPUTS:
        assert result.parity["tables"][filename]["prior_only_rows"] == 0
        assert result.parity["tables"][filename]["spliced_only_rows"] == 0
    assert result.parity["relationships_byte_copied_unchanged"] is True

    # 3. Verify cp1252-spliced collections declare encoding='cp1252' and resolve to clean UTF-8.
    cols = {
        row["collection_id"]: row
        for row in pq.read_table(result.generation_dir / "fec_collections.parquet").to_pylist()
    }
    for cid in CP1252_SPLICE_COLLECTION_IDS:
        assert json.loads(cols[cid]["requested_scope_json"])["encoding"] == "cp1252"
    assert json.loads(cols[CID_UTF8]["requested_scope_json"])["encoding"] == "utf-8"
    assert json.loads(cols[CID_SENATE_NBSP]["requested_scope_json"])["encoding"] == "latin-1"

    records = pq.read_table(result.generation_dir / "fec_source_records.parquet").to_pylist()
    if schema_mode == "subject":
        # Exact observation_ordinal preservation across untouched AND same-cardinality spliced rows.
        assert [r["observation_ordinal"] for r in records] == list(range(len(records)))

    resolved_texts: dict[str, str] = {}
    with verified_archive_session():
        for cid in CP1252_SPLICE_COLLECTION_IDS:
            scope = json.loads(cols[cid]["requested_scope_json"])
            reader = make_filing_body_reader(fx["store"], scope)
            for row in records:
                if row["collection_id"] != cid:
                    continue
                loc = json.loads(row["source_locator_json"])
                if "embedded_bodies" in loc:
                    body = loc["embedded_bodies"][0]
                    assert body["encoding"] == "cp1252"
                    resolved_texts[cid] = reader(body)

    assert "AFC PAC\u2019s" in resolved_texts[CID_ZIP_1981885]
    assert "\u201cAgreement\u201d" in resolved_texts[CID_1998705]
    assert "\u201cAgreement\u201d" in resolved_texts[CID_1998706]
    assert all(not any(0x80 <= ord(ch) <= 0x9F for ch in txt) for txt in resolved_texts.values())

    # 4. Splice receipt & corrected manifest match digest pins.
    receipt = json.loads(result.receipt_path.read_text(encoding="utf-8"))
    assert receipt["receipts_free"] is True
    assert receipt["manifest_diff"]["source_manifest_sha256"] == fx["manifest_sha"]
    corrected_bytes = result.corrected_manifest_path.read_bytes()
    assert "sha256:" + hashlib.sha256(corrected_bytes).hexdigest() == receipt["manifest_diff"]["corrected_manifest_sha256"]


def test_splice_refuses_c1_in_untouched_collection_unless_pin_verified_repair_provided(tmp_path):
    fx = _build_splice_fixture(tmp_path, schema_mode="subject", inject_doj_c1=True)

    # Default run MUST fail closed via refuse_c1_controls() and NOT seal artifact.json.
    failed_dir = tmp_path / "splice_fail_closed"
    with pytest.raises(ValueError, match="C1 control"):
        splice_fec_observations(
            prior_dir=fx["prior_dir"],
            output_dir=failed_dir,
            store=fx["store"],
            manifest_sha256=fx["manifest_sha"],
            target_collection_ids=CP1252_SPLICE_COLLECTION_IDS,
            c1_threads=2,
        )
    assert not (failed_dir / "build" / "generation" / "artifact.json").exists()

    # Wrong context pin MUST fail closed.
    bad_repair = CollectionContextRepair(
        collection_id=CID_DOJ_CONTEXT,
        prior_context_sha256="sha256:" + "0" * 64,
    )
    with pytest.raises(ValueError, match="pin mismatch"):
        splice_fec_observations(
            prior_dir=fx["prior_dir"],
            output_dir=tmp_path / "splice_bad_pin",
            store=fx["store"],
            manifest_sha256=fx["manifest_sha"],
            target_collection_ids=CP1252_SPLICE_COLLECTION_IDS,
            collection_context_repairs=(bad_repair,),
            c1_threads=2,
        )

    # Explicit pin-verified repair replaces U+0092 with U+2019, records provenance, and seals cleanly.
    good_repair = CollectionContextRepair(
        collection_id=CID_DOJ_CONTEXT,
        prior_context_sha256=fx["doj_pin"],
    )
    repaired_dir = tmp_path / "splice_repaired_doj"
    result = splice_fec_observations(
        prior_dir=fx["prior_dir"],
        output_dir=repaired_dir,
        store=fx["store"],
        manifest_sha256=fx["manifest_sha"],
        target_collection_ids=CP1252_SPLICE_COLLECTION_IDS,
        collection_context_repairs=(good_repair,),
        c1_threads=2,
    )
    admit_generation_for_publication(result.generation_dir)
    assert result.telemetry.c1_control_matches == 0
    assert len(result.manifest_diff["collection_context_repairs"]) == 1

    with duckdb.connect() as con:
        [(outcome_raw,)] = con.execute(
            "SELECT collection_outcome_json FROM read_parquet(?) WHERE collection_id = ?",
            [str(result.generation_dir / "fec_collections.parquet"), CID_DOJ_CONTEXT],
        ).fetchall()
    outcome = json.loads(outcome_raw)
    assert "Judge Bell\u2019s" in outcome["receiverDisposition"]["callerContext"]["teaser"]
    assert outcome["c1ContextRepair"]["priorContextSha256"] == fx["doj_pin"]
    assert outcome["c1ContextRepair"]["replacedCharacters"] == 1


def test_splice_refuses_non_cp1252_target_or_manifest_digest_mismatch(tmp_path):
    fx = _build_splice_fixture(tmp_path, schema_mode="subject")

    with pytest.raises(ValueError, match="digest differs"):
        correct_manifest_encodings(
            fx["manifest_bytes"],
            store=fx["store"],
            target_collection_ids=CP1252_SPLICE_COLLECTION_IDS,
            expected_manifest_sha256="sha256:" + "f" * 64,
        )

    with pytest.raises(ValueError, match="expected 'cp1252'"):
        correct_manifest_encodings(
            fx["manifest_bytes"],
            store=fx["store"],
            target_collection_ids=(CID_UTF8,),
            expected_manifest_sha256=fx["manifest_sha"],
        )


def test_disk_headroom_guard_safe_cleanup_and_cli_splice(tmp_path, monkeypatch):
    Usage = namedtuple("Usage", ["total", "used", "free"])
    gib = 1024**3

    monkeypatch.setattr("shutil.disk_usage", lambda _p: Usage(100 * gib, 85 * gib, 15 * gib))
    with pytest.raises(RuntimeError, match="aborted"):
        check_disk_headroom(tmp_path)

    monkeypatch.setattr("shutil.disk_usage", lambda _p: Usage(100 * gib, 78 * gib, 22 * gib))
    with pytest.raises(RuntimeError, match="refused"):
        check_disk_headroom(tmp_path)

    monkeypatch.setattr("shutil.disk_usage", lambda _p: Usage(100 * gib, 65 * gib, 35 * gib))
    info = check_disk_headroom(tmp_path)
    assert info["free_gib"] == 35.0

    # Safe cleanup must refuse raw roots and corpora/raw paths.
    raw_dir = tmp_path / "corpora" / "spicyregs" / "raw"
    raw_dir.mkdir(parents=True)
    with pytest.raises(ValueError, match="corpora raw"):
        _safe_cleanup_staging(raw_dir)
    custom_raw = tmp_path / "store_root"
    custom_raw.mkdir()
    with pytest.raises(ValueError, match="overlapping raw archive"):
        _safe_cleanup_staging(custom_raw / "sub", raw_root=custom_raw)

    # End-to-end CLI entrypoint with cleanup_prior=True.
    fx = _build_splice_fixture(tmp_path, schema_mode="subject")
    summary = splice_observations_cmd(
        prior_dir=fx["prior_dir"],
        output_dir=tmp_path / "cli_out",
        raw_root=fx["raw_root"],
        manifest_sha256=fx["manifest_sha"],
        target_collections=CP1252_SPLICE_COLLECTION_IDS,
        threads=2,
        cleanup_prior=True,
    )
    assert summary["phase"] == "splice-observations"
    assert summary["prior_cleaned"] is True
    assert not fx["prior_dir"].exists()
    admit_generation_for_publication(Path(summary["generation_dir"]))


def test_streaming_ordered_untouched_parity_cross_batch_and_fail_closed(tmp_path):
    from spicy_regs.transforms.fec_observations_splice import _verify_ordered_untouched_parity

    schema = pa.schema([("collection_id", pa.string()), ("val", pa.string())])
    prior_path = tmp_path / "prior.parquet"
    spliced_path = tmp_path / "spliced.parquet"

    prior_rows = [
        {"collection_id": "keep-1", "val": "a"},
        {"collection_id": "drop-1", "val": "old"},
        {"collection_id": "keep-2", "val": None},
        {"collection_id": "keep-3", "val": "b"},
        {"collection_id": "keep-4", "val": "c"},
    ]
    spliced_rows = [
        {"collection_id": "keep-1", "val": "a"},
        {"collection_id": "drop-1", "val": "new-1"},
        {"collection_id": "drop-1", "val": "new-2"},
        {"collection_id": "keep-2", "val": None},
        {"collection_id": "keep-3", "val": "b"},
        {"collection_id": "keep-4", "val": "c"},
    ]
    pq.write_table(pa.Table.from_pylist(prior_rows, schema=schema), prior_path)
    pq.write_table(pa.Table.from_pylist(spliced_rows, schema=schema), spliced_path)

    # Different batch boundaries align cleanly and match all 4 untouched rows.
    assert _verify_ordered_untouched_parity(prior_path, spliced_path, ("drop-1",), batch_size=2) == 4

    # Single mutated untouched cell must fail closed.
    mutated_rows = list(spliced_rows)
    mutated_rows[4] = {"collection_id": "keep-3", "val": "CORRUPTED"}
    mutated_path = tmp_path / "mutated.parquet"
    pq.write_table(pa.Table.from_pylist(mutated_rows, schema=schema), mutated_path)
    with pytest.raises(ValueError, match="value mismatch"):
        _verify_ordered_untouched_parity(prior_path, mutated_path, ("drop-1",), batch_size=2)

    # Dropped untouched row must fail closed.
    dropped_path = tmp_path / "dropped.parquet"
    pq.write_table(pa.Table.from_pylist(spliced_rows[:-1], schema=schema), dropped_path)
    with pytest.raises(ValueError, match="row count mismatch"):
        _verify_ordered_untouched_parity(prior_path, dropped_path, ("drop-1",), batch_size=2)
