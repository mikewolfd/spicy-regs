"""Tests for scripts/build_fec_release_receipt.py (VERSION = 2 FEC release receipt CLI)."""

from __future__ import annotations

import base64
from copy import deepcopy
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import build_fec_release_receipt as builder
from spicy_regs import data_dictionary as dd
from spicy_regs import fec_release, mcp_server
from spicy_regs.generations import build_generation
from spicy_regs.sources import publication
from spicy_regs.transforms.build_fec_observations import COLLECTION_SCHEMA, RECORD_COLUMNS
from spicy_regs.transforms.fec_relationships import COLUMNS as RELATIONSHIP_COLUMNS

pytestmark = pytest.mark.usefixtures("ample_disk_space")


def _digest(label: str) -> str:
    return fec_release.sha256(label.encode("utf-8"))


def _synthetic_v2_index(*, partitioned_receipts: bool = True) -> dict[str, Any]:
    """Synthetic receipts-free 50-table /2 fec-query + receipts-free fec-observations index."""
    scope = json.loads(Path(fec_release.__file__).with_name("fec_query_scope.json").read_text(encoding="utf-8"))
    obs_pin = scope["selected_observations_pin"]
    query_pin = _digest("synthetic-fec-query-v2-generation")

    typed_schemas = dd.fec_typed_schemas()
    assert len(typed_schemas) == 50

    query_tables: dict[str, Any] = {}
    for idx, (table, cols) in enumerate(sorted(typed_schemas.items()), start=1):
        col_list = [[name, dtype] for name, dtype in cols]
        if partitioned_receipts and table == "fec_receipts":
            members = [
                {
                    "key": f"fec_receipts/source_namespace=ns_{p:02d}/part-000000.parquet",
                    "sha256": _digest(f"fec_receipts-part-{p}"),
                    "byteSize": 1000 + p,
                    "rows": 10,
                    "partition": {"source_namespace": f"ns_{p:02d}"},
                }
                for p in range(26)
            ]
            query_tables[table + ".parquet"] = {
                "columns": col_list,
                "rows": sum(m["rows"] for m in members),
                "byteSize": sum(m["byteSize"] for m in members),
                "partitionColumns": ["source_namespace"],
                "members": members,
            }
        else:
            query_tables[table + ".parquet"] = {
                "sha256": _digest(f"table-{idx}-{table}"),
                "byteSize": 512 + idx,
                "rows": 1,
                "columns": col_list,
            }

    obs_schemas = dd.expected_schemas()
    obs_tables = {
        table + ".parquet": {
            "sha256": _digest(f"obs-{table}"),
            "byteSize": 256,
            "rows": 1,
            "columns": [[name, dtype] for name, dtype in obs_schemas[table]],
        }
        for table in ("fec_collections", "fec_relationships", "fec_source_records")
    }

    index = {
        "format": "spicy-regs-publication",
        "version": 2,
        "families": {
            "fec-observations": {
                "prefix": "generations/fec-observations/" + obs_pin.removeprefix("sha256:"),
                "logicalId": "urn:spicy:artifact:spicy-regs-rollup-generation:" + "a" * 64,
                "artifactDigest": obs_pin,
                "tables": obs_tables,
            },
            "fec-query": {
                "prefix": "generations/fec-query/" + query_pin.removeprefix("sha256:"),
                "logicalId": "urn:spicy:artifact:spicy-regs-rollup-generation:" + "b" * 64,
                "artifactDigest": query_pin,
                "tables": query_tables,
            },
        },
    }
    return publication.parse_index(json.dumps(index, sort_keys=True).encode("utf-8"))


def test_build_v2_release_receipt_roundtrip_and_all_71_views_compatible():
    index = _synthetic_v2_index(partitioned_receipts=True)
    consumer = fec_release.runtime_consumer(_digest("synthetic-container-image"))
    acceptance = [_digest("splice-acceptance"), _digest("query-build-acceptance"), _digest("parity-acceptance")]

    built = builder.build_and_verify_fec_release_receipt(
        index=index,
        consumer=consumer,
        acceptance_receipts=acceptance,
    )

    raw_bytes = built["receipt_bytes"]
    summary = built["summary"]
    compacted = built["receipt"]

    assert compacted["version"] == 2
    assert summary["version"] == 2
    assert summary["format"] == fec_release.FORMAT
    assert summary["bytes"] == len(raw_bytes)
    assert len(raw_bytes) < builder.DEFAULT_MAX_BYTES < fec_release.LIMIT
    assert summary["selected_view_mode"] == "v2"
    assert summary["output_tables_count"] == 50
    assert summary["view_count"] == len(mcp_server.FEC_QUALIFIED_VIEWS) == 71
    assert summary["compatible_views"] == 71
    assert summary["disabled_views"] == 0
    assert summary["fec_filing_reference_resolution_sql_sha256"].startswith("sha256:")
    assert set(summary["views_sql_sha256"]) == {s.view.name for s in mcp_server.FEC_QUALIFIED_VIEWS}

    # Verify default rollback receipts and retained generations are preserved.
    recovery = compacted["recovery"]
    assert recovery["rollback_receipts"] == list(builder.DEFAULT_ROLLBACK_RECEIPTS)
    assert index["families"]["fec-query"]["artifactDigest"] in recovery["retained_generations"]["fec-query"]
    assert builder.DEFAULT_PRIOR_QUERY_GENERATION in recovery["retained_generations"]["fec-query"]
    assert index["families"]["fec-observations"]["artifactDigest"] in recovery["retained_generations"]["fec-observations"]
    for prior_obs in builder.DEFAULT_PRIOR_OBSERVATIONS_GENERATIONS:
        assert prior_obs in recovery["retained_generations"]["fec-observations"]

    # Roundtrip parse_receipt verifies SHA-256, schema, and expansion parity.
    parsed = fec_release.parse_receipt(raw_bytes, summary["receipt_sha256"])
    assert parsed["version"] == 2
    assert set(parsed["views"]) == {s.view.name for s in mcp_server.FEC_QUALIFIED_VIEWS}
    assert parsed["views"]["fec_filing_reference_resolution"]["sql_sha256"] == summary[
        "fec_filing_reference_resolution_sql_sha256"
    ]


def test_cli_builds_from_local_sealed_receiptless_generation_dirs(tmp_path: Path):
    typed_doc = json.loads((dd.DICTIONARY_INPUTS / "fec_typed_schemas.json").read_text(encoding="utf-8"))["tables"]
    obs_src = tmp_path / "obs-src"
    obs_src.mkdir()
    pq.write_table(pa.Table.from_batches([], schema=COLLECTION_SCHEMA), obs_src / "fec_collections.parquet")
    pq.write_table(
        pa.Table.from_batches([], schema=pa.schema([(c, pa.string()) for c in RELATIONSHIP_COLUMNS])),
        obs_src / "fec_relationships.parquet",
    )
    pq.write_table(
        pa.Table.from_batches([], schema=pa.schema([(c, pa.string()) for c in RECORD_COLUMNS])),
        obs_src / "fec_source_records.parquet",
    )
    obs_gen_dir = tmp_path / "obs-gen"
    obs_artifact = build_generation(
        obs_gen_dir,
        family="fec-observations",
        files=[
            obs_src / "fec_collections.parquet",
            obs_src / "fec_relationships.parquet",
            obs_src / "fec_source_records.parquet",
        ],
        expected_keys=["fec_collections.parquet", "fec_relationships.parquet", "fec_source_records.parquet"],
        receipt_path=None,
    )

    query_src = tmp_path / "query-src"
    query_src.mkdir()
    query_files: list[Path] = []
    query_keys: list[str] = []
    receipt_namespaces = ("fec-bulk-individual-contributions", "fec-bulk-pas2")
    for table, item in sorted(typed_doc.items()):
        schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(item["arrow_schema_ipc_base64"], validate=True)))
        if table == "fec_receipts":
            split_root = query_src / table
            for ns in receipt_namespaces:
                part_dir = split_root / f"source_namespace={ns}"
                part_dir.mkdir(parents=True)
                cols = {
                    field.name: pa.array(
                        [ns if field.name == "source_namespace" else (None if field.nullable else "")],
                        type=field.type,
                    )
                    for field in schema
                }
                pq.write_table(pa.table(cols, schema=schema), part_dir / "part-000000.parquet")
            query_files.append(split_root)
            query_keys.append(f"{table}.parquet")
        else:
            out = query_src / f"{table}.parquet"
            pq.write_table(pa.Table.from_batches([], schema=schema), out)
            query_files.append(out)
            query_keys.append(f"{table}.parquet")
    query_gen_dir = tmp_path / "query-gen"
    query_artifact = build_generation(
        query_gen_dir,
        family="fec-query",
        files=query_files,
        expected_keys=query_keys,
        partitioned={"fec_receipts.parquet": ("source_namespace",)},
        receipt_path=None,
    )

    scope_path = tmp_path / "scope.json"
    base_scope = json.loads(Path(fec_release.__file__).with_name("fec_query_scope.json").read_text(encoding="utf-8"))
    base_scope["source_generation_pin"] = obs_artifact.pin.artifact_digest
    base_scope["selected_observations_pin"] = obs_artifact.pin.artifact_digest
    scope_path.write_text(json.dumps(base_scope), encoding="utf-8")

    consumer_path = tmp_path / "consumer.json"
    consumer = fec_release.runtime_consumer(_digest("local-gen-image"))
    consumer_path.write_text(json.dumps(consumer), encoding="utf-8")

    acceptance_file = tmp_path / "parity.json"
    acceptance_file.write_text('{"status":"passed"}\n', encoding="utf-8")

    out_receipt = tmp_path / "release-v2.json"
    out_summary = tmp_path / "release-v2.summary.json"
    rc = builder.main(
        [
            "--observations-generation",
            str(obs_gen_dir),
            "--query-generation",
            str(query_gen_dir),
            "--verify-member-bytes",
            "--consumer-json",
            str(consumer_path),
            "--acceptance-receipt-file",
            str(acceptance_file),
            "--scope",
            str(scope_path),
            "--output",
            str(out_receipt),
            "--summary",
            str(out_summary),
        ]
    )
    assert rc == 0
    assert out_receipt.is_file() and out_summary.is_file()
    summary = json.loads(out_summary.read_text(encoding="utf-8"))
    raw = out_receipt.read_bytes()
    assert summary["receipt_sha256"] == fec_release.sha256(raw)
    assert summary["version"] == 2
    assert summary["query_generation"] == query_artifact.pin.artifact_digest
    assert summary["observations_generation"] == obs_artifact.pin.artifact_digest
    assert summary["compatible_views"] == 71 and summary["disabled_views"] == 0
    parsed = fec_release.parse_receipt(raw, summary["receipt_sha256"])
    assert parsed["views"]["fec_filing_reference_resolution"]["acceptance_receipts"] == [
        fec_release.sha256(acceptance_file.read_bytes())
    ]
    receipt_members = parsed["output_membership"]["fec_receipts"]["descriptor"]["members"]
    assert [m["key"] for m in receipt_members] == [
        f"fec_receipts/source_namespace={ns}/part-000000.parquet" for ns in receipt_namespaces
    ]
    assert sum(m["rows"] for m in receipt_members) == len(receipt_namespaces)
    assert (
        parsed["views"]["fec_receipts_evidence"]["dependencies"]["fec_receipts"]["descriptor"]["members"]
        == receipt_members
    )


def test_refuses_undeclared_etl_receipts_member_in_local_generation_dir(tmp_path: Path):
    from rulespec_artifacts import (
        LocalMemberSource,
        canonical_json_bytes,
        describe_member,
        stamp_root,
        write_member_manifest,
    )

    src = tmp_path / "src"
    src.mkdir()
    pq.write_table(pa.Table.from_batches([], schema=COLLECTION_SCHEMA), src / "fec_collections.parquet")
    gen = tmp_path / "gen"
    build_generation(
        gen,
        family="fec-observations",
        files=[src / "fec_collections.parquet"],
        expected_keys=["fec_collections.parquet"],
        receipt_path=None,
    )
    pq.write_table(
        pa.table({"receipt_id": pa.array(["r1"], type=pa.string())}),
        gen / "etl_receipts.parquet",
    )
    source = LocalMemberSource(gen)
    members = [
        describe_member(
            source,
            object_key="etl_receipts.parquet",
            role="table",
            media_type="application/vnd.apache.parquet",
            record_count=1,
        ),
        describe_member(
            source,
            object_key="fec_collections.parquet",
            role="table",
            media_type="application/vnd.apache.parquet",
            record_count=0,
        ),
    ]
    with (gen / "members.json").open("wb") as stream:
        manifest_ref = write_member_manifest(
            stream, scope_kind="global", scope_id="fec-observations", object_key="members.json", members=members
        )
    art = json.loads((gen / "artifact.json").read_text(encoding="utf-8"))
    art["memberManifests"] = [manifest_ref.as_dict()]
    art["counts"] = {
        **art["counts"],
        "memberCount": manifest_ref.member_count,
        "totalMemberByteSize": manifest_ref.total_member_byte_size,
        "totalRecordCount": manifest_ref.total_record_count,
    }
    art = stamp_root(art)
    (gen / "artifact.json").write_bytes(canonical_json_bytes(art))

    with pytest.raises(ValueError, match="etlReceipts declaration disagrees with etl_receipts.parquet membership"):
        builder.load_generation_entry(gen, expected_family="fec-observations", verify_member_bytes=True)


def test_refuses_observations_digest_mismatch():
    index = _synthetic_v2_index(partitioned_receipts=False)
    wrong = _digest("wrong-observations-generation")
    index["families"]["fec-observations"]["artifactDigest"] = wrong
    index["families"]["fec-observations"]["prefix"] = "generations/fec-observations/" + wrong.removeprefix("sha256:")
    consumer = fec_release.runtime_consumer(_digest("img"))
    with pytest.raises(ValueError, match="disagrees with fec_query_scope"):
        builder.build_and_verify_fec_release_receipt(
            index=index,
            consumer=consumer,
            acceptance_receipts=[_digest("acc")],
        )


def test_refuses_missing_table_in_fec_query():
    index = _synthetic_v2_index(partitioned_receipts=False)
    del index["families"]["fec-query"]["tables"]["fec_filing_links.parquet"]
    consumer = fec_release.runtime_consumer(_digest("img"))
    with pytest.raises(ValueError, match="missing 1 table.*fec_filing_links"):
        builder.build_and_verify_fec_release_receipt(
            index=index,
            consumer=consumer,
            acceptance_receipts=[_digest("acc")],
        )


def test_refuses_dictionary_schema_mismatch():
    index = _synthetic_v2_index(partitioned_receipts=False)
    index["families"]["fec-query"]["tables"]["fec_receipts.parquet"]["columns"][0][1] = "BIGINT"
    consumer = fec_release.runtime_consumer(_digest("img"))
    with pytest.raises(ValueError, match="fec_receipts schema disagrees with installed table_metadata.json"):
        builder.build_and_verify_fec_release_receipt(
            index=index,
            consumer=consumer,
            acceptance_receipts=[_digest("acc")],
        )


def test_refuses_receipts_on_fec_query_or_observations_by_default():
    index = _synthetic_v2_index(partitioned_receipts=False)
    index["families"]["fec-query"]["etlReceipts"] = {
        "key": "etl_receipts.parquet",
        "sha256": _digest("receipts"),
        "byteSize": 100,
        "rows": 1,
        "columns": [["receipt_id", "VARCHAR"]],
        "generationId": "g-1",
        "datasets": ["fec_receipts"],
    }
    consumer = fec_release.runtime_consumer(_digest("img"))
    with pytest.raises(ValueError, match="violating receipts-free INVARIANT 1"):
        builder.build_and_verify_fec_release_receipt(
            index=index,
            consumer=consumer,
            acceptance_receipts=[_digest("acc")],
        )


def test_refuses_legacy_54_table_index_without_allow_legacy():
    index = _synthetic_v2_index(partitioned_receipts=False)
    historical = json.loads((dd.DICTIONARY_INPUTS / "fec_historical_definitions.json").read_text())["tables"]
    legacy_tables = sorted(
        t for t in historical if t not in {"fec_collections", "fec_relationships", "fec_source_records"}
    )
    index["families"]["fec-query"]["tables"] = {
        table + ".parquet": {
            "sha256": _digest(f"legacy-{table}"),
            "byteSize": 100,
            "rows": 1,
            "columns": historical[table]["columns"],
        }
        for table in legacy_tables
    }
    consumer = fec_release.runtime_consumer(_digest("img"))
    with pytest.raises(ValueError, match="does not match receipts-free 50-table /2 layout"):
        builder.build_and_verify_fec_release_receipt(
            index=index,
            consumer=consumer,
            acceptance_receipts=[_digest("acc")],
        )


def test_refuses_tampered_local_generation_artifact_digest(tmp_path: Path):
    src = tmp_path / "src"
    src.mkdir()
    pq.write_table(pa.Table.from_batches([], schema=COLLECTION_SCHEMA), src / "fec_collections.parquet")
    gen = tmp_path / "gen"
    build_generation(
        gen,
        family="fec-observations",
        files=[src / "fec_collections.parquet"],
        expected_keys=["fec_collections.parquet"],
        receipt_path=None,
    )
    art = json.loads((gen / "artifact.json").read_text(encoding="utf-8"))
    art["artifactDigest"] = _digest("tampered")
    (gen / "artifact.json").write_text(json.dumps(art), encoding="utf-8")
    with pytest.raises(ValueError, match="artifactDigest does not match"):
        builder.load_generation_entry(gen, expected_family="fec-observations")


def test_refuses_receipt_exceeding_max_bytes_and_sql_drift_in_roundtrip():
    index = _synthetic_v2_index(partitioned_receipts=False)
    consumer = fec_release.runtime_consumer(_digest("img"))
    with pytest.raises(ValueError, match="max_bytes must be between"):
        builder.build_and_verify_fec_release_receipt(
            index=index,
            consumer=consumer,
            acceptance_receipts=[_digest("acc")],
            max_bytes=True,  # type: ignore[arg-type]
        )
    with pytest.raises(ValueError, match="exceeds configured --max-bytes"):
        builder.build_and_verify_fec_release_receipt(
            index=index,
            consumer=consumer,
            acceptance_receipts=[_digest("acc")],
            max_bytes=1024,
        )

    # Verify tampering SQL digest in the produced receipt disables the affected view on check_view.
    built = builder.build_and_verify_fec_release_receipt(
        index=index,
        consumer=consumer,
        acceptance_receipts=[_digest("acc")],
    )
    tampered = deepcopy(built["receipt"])
    tampered["views"]["fec_filing_reference_resolution"]["sql_sha256"] = _digest("drifted-sql")
    raw = fec_release.serialize_receipt(tampered)
    pin = fec_release.sha256(raw)
    assert fec_release.parse_receipt(raw, pin)["version"] == 2
    spec = next(s for s in mcp_server.FEC_QUALIFIED_VIEWS if s.view.name == "fec_filing_reference_resolution")
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "tampered.json"
        p.write_bytes(raw)
        cfg = fec_release.capture_configuration(
            (spec,),
            receipt_digest=pin,
            image_digest=consumer["image_digest"],
            base_url="unused",
            local_path=p,
            local_mode=True,
            consumer=consumer,
        )
        checked = fec_release.check_view(
            spec,
            cfg,
            index,
            [k.removesuffix(".parquet") for k in index["families"]["fec-query"]["tables"]],
        )
        assert checked["status"] == "disabled"
        assert any(r["path"] == "sql_sha256" for r in checked["reasons"])
