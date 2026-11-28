"""Compact spec.readSnapshot schema, dual legacy/compact compatibility, downstream seams, and INDEX_LIMIT guard."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime, timedelta
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, ClassVar

from loguru import logger
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from rulespec_artifacts import (
    LocalMemberSource,
    canonical_json_bytes,
    parse_canonical_json,
    stamp_root,
)

from spicy_regs import (
    generation_audit,
    generation_retention,
    local_publication,
    remote_generations,
)
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.pipelines.rollups.base import RollupPipeline
from spicy_regs.scorecards.etl import SOURCE_NAMES, generation_options, write_family
from spicy_regs.scorecards.operations import publish as scorecard_publish
from spicy_regs.source_catalog.input_proof import input_proof
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.sources import publication as pub
from spicy_regs.transforms import build_court_pdf_extractions as court_pdf
from spicy_regs.transforms.pdf_text import PdfTextResult, PdfTextStatus
from tests.generation_fakes import Store
from tests.test_court_pdf_extractions import case as court_pdf_case
from tests.test_generation_audit import public_base
from tests.test_remote_generations import RemoteStore, SCHEMAS as REMOTE_SCHEMAS, staged as remote_staged
from tests.test_scorecard_refresh import edition as scorecard_edition, tables as scorecard_tables

pytestmark = pytest.mark.usefixtures("ample_disk_space")


def _sha256_label(label: str) -> str:
    return "sha256:" + hashlib.sha256(label.encode()).hexdigest()


def _family_entry(
    family: str,
    table_key: str,
    *,
    seed: str,
    column_count: int = 24,
    split_parts: int = 0,
    published_at: str = "2026-11-28T00:00:00Z",
) -> dict[str, Any]:
    digest = _sha256_label(f"{family}:{seed}:root")
    hex_digest = digest.removeprefix("sha256:")
    columns = [["id", "VARCHAR"], *[
        [f"col_{idx:02d}_value", "VARCHAR"] for idx in range(1, column_count)
    ]]
    if split_parts:
        columns = [["congress", "VARCHAR"], *columns]
        stem = table_key.removesuffix(".parquet")
        members = [
            {
                "key": f"{stem}/congress={100 + part}/part-000000.parquet",
                "sha256": _sha256_label(f"{family}:{seed}:{table_key}:{part}"),
                "byteSize": 4096 + part,
                "rows": 100 + part,
                "partition": {"congress": str(100 + part)},
            }
            for part in range(split_parts)
        ]
        table: dict[str, Any] = {
            "byteSize": sum(m["byteSize"] for m in members),
            "rows": sum(m["rows"] for m in members),
            "columns": columns,
            "partitionColumns": ["congress"],
            "members": members,
        }
    else:
        table = {
            "sha256": _sha256_label(f"{family}:{seed}:{table_key}"),
            "byteSize": 8192,
            "rows": 250,
            "columns": columns,
        }
    return {
        "prefix": f"generations/{family}/{hex_digest}",
        "logicalId": f"urn:spicy-regs:rollup-generation:{family}",
        "artifactDigest": digest,
        "publishedAt": published_at,
        "tables": {table_key: table},
    }


def _realistic_102_family_index() -> dict[str, Any]:
    """Synthetic 102-family publication.v2.json modeled on live catalog width (~400 KiB)."""
    families: dict[str, Any] = {}
    families["parent-single"] = _family_entry(
        "parent-single", "parent_single.parquet", seed="v1", column_count=20
    )
    families["parent-split"] = _family_entry(
        "parent-split", "parent_split.parquet", seed="v1", column_count=20, split_parts=8
    )
    families["sample-bill-family"] = _family_entry(
        "sample-bill-family", "sample_bills.parquet", seed="v1", column_count=60, split_parts=12
    )
    families["irs-form990"] = _family_entry(
        "irs-form990", "irs990_filings.parquet", seed="v1", column_count=20
    )
    for idx in range(98):
        name = f"catalog-family-{idx:03d}"
        # Mirror live proportions (~3 tables/family + wider/split families -> ~400 KiB).
        split_parts = 14 if idx < 10 else 0
        cols = 70 if idx < 15 else 36
        entry = _family_entry(
            name, f"catalog_table_{idx:03d}_a.parquet", seed="v1", column_count=cols, split_parts=split_parts
        )
        for suffix in ("b", "c"):
            extra = _family_entry(name, f"catalog_table_{idx:03d}_{suffix}.parquet", seed=f"v1-{suffix}", column_count=36)
            entry["tables"].update(extra["tables"])
        families[name] = entry
    index = {"format": "spicy-regs-publication", "version": 2, "families": families}
    raw = canonical_json_bytes(index)
    pub.parse_index(raw)
    assert len(index["families"]) == 102
    assert len(raw) > 350_000
    return index


def _write_parquet(path: Path, value: str = "a") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"id": [value]}), path, store_schema=False)
    return path


def _restamp_legacy_full_root(directory: Path, full_snapshot: dict[str, Any]) -> tuple[str, bytes]:
    """Rewrite a generation's artifact.json with a legacy full readSnapshot and restamp its digest."""
    root = parse_canonical_json((directory / "artifact.json").read_bytes())
    root["spec"]["readSnapshot"] = deepcopy(full_snapshot)
    root.pop("artifactDigest", None)
    stamped = stamp_root(root)
    raw = canonical_json_bytes(stamped)
    (directory / "artifact.json").write_bytes(raw)
    return stamped["artifactDigest"], raw


def _publish_legacy_generation(store: Store, directory: Path, prior_index: dict[str, Any]) -> dict[str, Any]:
    """Rewrite a built generation's root to legacy full-snapshot form and publish it into ``store``."""
    _restamp_legacy_full_root(directory, prior_index)
    artifact = verify_generation(directory)
    assert "scope" not in artifact.root["spec"]["readSnapshot"]
    return pub.publish_generation(directory, client=store, bucket="fixture", prior_index=prior_index)


def test_compact_read_snapshot_reduces_root_size_on_102_family_index(tmp_path, monkeypatch):
    index = _realistic_102_family_index()
    full_index_raw = canonical_json_bytes(index)
    expected_sha = "sha256:" + hashlib.sha256(full_index_raw).hexdigest()

    # 1. Direct build_generation with own prior + 2 managed parents (single + split).
    out_file = _write_parquet(tmp_path / "src" / "sample_bills.parquet", "bill-1")
    parents = {
        "parent_single.parquet": pub.table_pin(index, "parent_single.parquet"),
        "parent_split.parquet": pub.table_pin(index, "parent_split.parquet"),
    }
    gen_dir = tmp_path / "gen-compact"
    artifact = build_generation(
        gen_dir,
        family="sample-bill-family",
        files=[out_file],
        expected_keys=["sample_bills.parquet"],
        read_snapshot=index,
        parents=parents,
    )
    snapshot = artifact.root["spec"]["readSnapshot"]
    assert snapshot["format"] == "spicy-regs-publication"
    assert snapshot["version"] == 2
    assert snapshot["scope"] == pub.READ_SNAPSHOT_SCOPE
    assert snapshot["indexSha256"] == expected_sha
    assert snapshot["indexFamilyCount"] == 102
    assert set(snapshot["families"]) == {"sample-bill-family", "parent-single", "parent-split"}
    assert snapshot["families"]["sample-bill-family"] == index["families"]["sample-bill-family"]
    assert snapshot["families"]["parent-single"] == index["families"]["parent-single"]
    assert snapshot["families"]["parent-split"] == index["families"]["parent-split"]

    # Measure exact byte savings vs equivalent legacy full-snapshot root.
    compact_root_bytes = (gen_dir / "artifact.json").stat().st_size
    compact_snapshot_bytes = len(canonical_json_bytes(snapshot))
    legacy_dir = tmp_path / "gen-legacy-measure"
    legacy_dir.mkdir()
    for p in gen_dir.iterdir():
        (legacy_dir / p.name).write_bytes(p.read_bytes())
    _, legacy_root_raw = _restamp_legacy_full_root(legacy_dir, index)
    legacy_root_bytes = len(legacy_root_raw)
    assert compact_root_bytes < legacy_root_bytes * 0.15  # >85% root reduction
    assert compact_snapshot_bytes < len(full_index_raw) * 0.10  # >90% readSnapshot reduction

    # 2. Remote generation path also compacts automatically.
    rstore = RemoteStore()
    member, _ = remote_staged(rstore, prefix="stage/compact")
    remote_art = remote_generations.prepare_remote_generation(
        tmp_path / "remote-gen",
        family="sample-bill-family",
        client=rstore,
        bucket="fork",
        staging_prefix="stage/compact",
        members=[member],
        expected_keys=["body.parquet"],
        schemas=REMOTE_SCHEMAS,
        read_snapshot=index,
        parents=parents,
        extra_snapshot_families=("catalog-family-000",),
    )
    remote_snap = remote_art.root["spec"]["readSnapshot"]
    assert remote_snap["scope"] == pub.READ_SNAPSHOT_SCOPE
    assert remote_snap["indexSha256"] == expected_sha
    assert remote_snap["indexFamilyCount"] == 102
    assert set(remote_snap["families"]) == {
        "sample-bill-family", "parent-single", "parent-split", "catalog-family-000"
    }

    # 3. RollupPipeline path compacts automatically against a 102-family index.
    class _SingleRollup(RollupPipeline):
        name: ClassVar[str] = "sample-bill-family"
        output: ClassVar[str] = "sample_bills.parquet"
        retain_source_evidence: ClassVar[bool] = False

        def build(self, output_dir: Path) -> Path:
            return _write_parquet(output_dir / self.output, "rollup-bill")

    monkeypatch.setenv("R2_PUBLIC_URL", "https://fixture.example")
    monkeypatch.setattr(pub, "load_index", lambda _url: deepcopy(index))
    rollup_dir = tmp_path / "rollup-out"
    _SingleRollup(output_dir=rollup_dir, skip_upload=True).run()
    (built_gen,) = (rollup_dir / "generations").iterdir()
    rollup_art = verify_generation(built_gen)
    rollup_snap = rollup_art.root["spec"]["readSnapshot"]
    assert rollup_snap["scope"] == pub.READ_SNAPSHOT_SCOPE
    assert rollup_snap["indexSha256"] == expected_sha
    assert rollup_snap["indexFamilyCount"] == 102
    assert set(rollup_snap["families"]) == {"sample-bill-family"}

    # 4. local_publication.dry_run succeeds against the 102-family index with a compact root
    #    and still refuses when the live family moved after capture.
    dry_index = deepcopy(index)
    dry_index["families"].pop("irs-form990")
    live_path = tmp_path / "live-102.v2.json"
    live_path.write_bytes(canonical_json_bytes(dry_index))
    evidence = CaptureEvidence(tmp_path / "ev-dry", "irs-form990")
    evidence.retain_bytes(b"original-990", stage="fixture-original", raw_source=True)
    dry_table = _write_parquet(tmp_path / "dry-src" / "irs990_filings.parquet", "990-1")
    # Build matching declared schema for irs-form990 using local_publication helper.
    from tests.test_local_publication import _table_for_schema
    declared = local_publication._family_schemas("irs-form990") or {}
    if "irs990_filings" in declared:
        pq.write_table(_table_for_schema(declared["irs990_filings"], ["990-1"]), dry_table)
    dry_gen_dir = tmp_path / "dry-gen"
    dry_art = build_generation(
        dry_gen_dir,
        family="irs-form990",
        files=[dry_table],
        expected_keys=["irs990_filings.parquet"],
        read_snapshot=dry_index,
        inputs=evidence.inputs(),
    )
    evidence.finish()
    assert dry_art.root["spec"]["readSnapshot"]["scope"] == pub.READ_SNAPSHOT_SCOPE
    assert set(dry_art.root["spec"]["readSnapshot"]["families"]) == set()
    assert dry_art.root["spec"]["readSnapshot"]["indexFamilyCount"] == 101

    local_publication.main([
        "--generation", str(dry_gen_dir),
        "--family", "irs-form990",
        "--evidence", str(evidence.artifact_dir),
        "--output", str(tmp_path / "dry-receipt"),
        "--dry-run", "--scratch-raw", "--live-index", str(live_path),
    ])
    receipt = json.loads((tmp_path / "dry-receipt" / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted"
    assert receipt["other_family_changes"] == []

    moved_path = tmp_path / "live-102-moved.v2.json"
    moved_path.write_bytes(canonical_json_bytes(index))  # irs-form990 now exists on live!
    with pytest.raises(pub.PublicationError, match="Family changed since"):
        local_publication.main([
            "--generation", str(dry_gen_dir),
            "--family", "irs-form990",
            "--evidence", str(evidence.artifact_dir),
            "--output", str(tmp_path / "dry-stale"),
            "--dry-run", "--scratch-raw", "--live-index", str(moved_path),
            "--expect-prior", index["families"]["irs-form990"]["artifactDigest"].removeprefix("sha256:")[:12],
        ])


def test_parse_index_strict_boundary_and_parse_read_snapshot_validation(tmp_path):
    index = {
        "format": "spicy-regs-publication",
        "version": 2,
        "families": {
            "alpha": _family_entry("alpha", "alpha.parquet", seed="1", column_count=4),
            "beta": _family_entry("beta", "beta.parquet", seed="1", column_count=4),
        },
    }
    compact = pub.compact_read_snapshot(index, family="alpha")
    compact_raw = canonical_json_bytes(compact)

    # Strict live-pointer boundary: parse_index MUST refuse compact snapshots.
    with pytest.raises(pub.PublicationError, match="Invalid publication index"):
        pub.parse_index(compact_raw)

    # parse_read_snapshot accepts empty {}, legacy full index, and compact snapshot.
    assert pub.parse_read_snapshot(b"{}") == {}
    assert pub.parse_read_snapshot(canonical_json_bytes(index)) == index
    assert pub.parse_read_snapshot(compact_raw) == compact

    # parse_read_snapshot rejects invalid compact variants.
    bad_cases: list[bytes] = [
        b"[]",
        b"null",
        b'{"scope":"referenced-families"}',
        canonical_json_bytes({**compact, "scope": "all-families"}),
        canonical_json_bytes({**compact, "indexSha256": "sha256:not-hex"}),
        canonical_json_bytes({**compact, "indexFamilyCount": 0}),  # < len(families)==1
        canonical_json_bytes({**compact, "indexFamilyCount": True}),  # bool is not int
        canonical_json_bytes({**compact, "extra": 1}),
        b'{"format":"spicy-regs-publication","format":"spicy-regs-publication","version":2,"families":{}}',
    ]
    for raw in bad_cases:
        with pytest.raises(pub.PublicationError):
            pub.parse_read_snapshot(raw)

    with pytest.raises(pub.PublicationError, match="byte limit"):
        pub.parse_read_snapshot(b" " * (pub.INDEX_LIMIT + 1))

    # compact_read_snapshot on an already-compact snapshot preserves indexSha256 without re-hashing.
    recompacted = pub.compact_read_snapshot(compact, family="alpha")
    assert recompacted == compact
    assert recompacted is not compact

    # Missing managed parent family in an already-compact snapshot is refused.
    beta_pin = pub.table_pin(index, "beta.parquet")
    with pytest.raises(pub.PublicationError, match="omits required parent"):
        pub.compact_read_snapshot(compact, family="alpha", parents={"beta.parquet": beta_pin})

    # Passing a bare string as extra_families is refused (prevents character splitting).
    with pytest.raises(ValueError, match="extra_families"):
        pub.compact_read_snapshot(index, family="alpha", extra_families="beta")

    # verify_generation refuses a compact root missing a managed parent family or missing prior-generation entry.
    out_file = _write_parquet(tmp_path / "alpha.parquet", "x")
    gen_dir = tmp_path / "gen"
    build_generation(
        gen_dir,
        family="alpha",
        files=[out_file],
        expected_keys=["alpha.parquet"],
        read_snapshot=index,
        parents={"beta.parquet": beta_pin},
    )
    tampered = parse_canonical_json((gen_dir / "artifact.json").read_bytes())
    tampered["spec"]["readSnapshot"]["families"].pop("beta")
    tampered.pop("artifactDigest")
    (gen_dir / "artifact.json").write_bytes(canonical_json_bytes(stamp_root(tampered)))
    with pytest.raises(ValueError, match="Parent differs from its captured family"):
        verify_generation(gen_dir)


def test_retention_and_audit_traverse_mixed_legacy_and_compact_chain(tmp_path):
    store = Store()
    digests: list[str] = []
    table_shas: list[str] = []
    prior_index = pub.empty_index()

    # Build a sibling unrelated family so every full vs compact snapshot differs meaningfully.
    sibling_dir = tmp_path / "sibling"
    build_generation(
        sibling_dir,
        family="unrelated",
        files=[_write_parquet(tmp_path / "src-sib" / "unrelated.parquet", "u")],
        expected_keys=["unrelated.parquet"],
        read_snapshot=prior_index,
    )
    prior_index = pub.publish_generation(sibling_dir, client=store, bucket="fixture", prior_index=prior_index)

    # Gen 1 & Gen 2: legacy full-snapshot roots; Gen 3, 4, 5: compact roots.
    for step in range(1, 6):
        gen_dir = tmp_path / f"base-{step}"
        src = _write_parquet(tmp_path / f"src-{step}" / "base.parquet", f"v{step}")
        build_generation(
            gen_dir,
            family="base",
            files=[src],
            expected_keys=["base.parquet"],
            read_snapshot=prior_index,
        )
        if step <= 2:
            prior_index = _publish_legacy_generation(store, gen_dir, prior_index)
        else:
            art = verify_generation(gen_dir)
            snap = art.root["spec"]["readSnapshot"]
            assert snap["scope"] == pub.READ_SNAPSHOT_SCOPE
            assert set(snap["families"]) == {"base"}
            assert "unrelated" not in snap["families"]
            prior_index = pub.publish_generation(gen_dir, client=store, bucket="fixture", prior_index=prior_index)
        entry = prior_index["families"]["base"]
        digests.append(entry["artifactDigest"].removeprefix("sha256:"))
        table_shas.append(entry["tables"]["base.parquet"]["sha256"])

    # Also add a rulemaking snapshot pinning Gen 2's table sha256 (resolved via Gen 3's compact readSnapshot!).
    snapshot_id = "20261128t000000z-test"
    manifest_key = f"materialized/rulemaking/snapshots/{snapshot_id}/manifest.json"
    store.objects[pub.SNAPSHOT_POINTER] = json.dumps({
        "format_version": 2,
        "dataset": "rulemaking",
        "snapshot_id": snapshot_id,
        "manifest_key": manifest_key,
    }).encode()
    store.objects[manifest_key] = json.dumps({
        "format_version": 2,
        "dataset": "rulemaking",
        "snapshot_id": snapshot_id,
        "inputs": {"sources": {"base.parquet": {"sha256": table_shas[1].removeprefix("sha256:")}}},
    }).encode()

    # Retention keeps Gen 5, 4, 3 (KEEP_LAST=3) + Gen 2 (rulemaking input via compact Gen 3 readSnapshot);
    # Gen 1 is planned for deletion!
    later = datetime.now(UTC) + timedelta(days=30)
    plan = generation_retention.plan(store, "fixture", notes=[], docspec={}, now=later)
    kept = {g["digest"]: g["keep"] for g in plan["families"]["base"]["generations"] if g["keep"]}
    assert set(kept) == {digests[1], digests[2], digests[3], digests[4]}
    assert any("rulemaking" in reason for reason in kept[digests[1]])
    assert plan["delete"] == [f"generations/base/{digests[0]}"]

    # Audit chain walk from Gen 5 traverses Gen 4 -> Gen 3 -> Gen 2 (legacy) -> Gen 1 (legacy).
    base = public_base(store, tmp_path / "public")
    current_entry = prior_index["families"]["base"]
    current_root = parse_canonical_json(store.objects[f"{current_entry['prefix']}/artifact.json"])
    walked = [entry["artifactDigest"].removeprefix("sha256:")
              for entry in generation_audit._chain(base, current_root, "base")]
    assert walked == [digests[3], digests[2], digests[1], digests[0]]


def test_downstream_seams_input_proof_conversion_proof_and_exact_verifiers(tmp_path, monkeypatch):
    index = _realistic_102_family_index()
    single_pin = pub.table_pin(index, "parent_single.parquet")
    split_pin = pub.table_pin(index, "parent_split.parquet")
    parents = {"parent_single.parquet": single_pin, "parent_split.parquet": split_pin}

    # 1. input_proof resolves both single-file and split-table parent members from a compact root.
    gen_dir = tmp_path / "derived-gen"
    artifact = build_generation(
        gen_dir,
        family="sample-bill-family",
        files=[_write_parquet(tmp_path / "src" / "sample_bills.parquet", "b1")],
        expected_keys=["sample_bills.parquet"],
        read_snapshot=index,
        parents=parents,
    )
    receipt, _ = input_proof(artifact, LocalMemberSource(gen_dir))
    proof_parents = receipt["generations"][0]["parents"]
    assert len([p for p in proof_parents if p["table"] == "parent_single"]) == 1
    assert len([p for p in proof_parents if p["table"] == "parent_split"]) == 8

    # 2. read_snapshot_matches accepts both compact and legacy full roots, and rejects a 1-byte tamper
    #    in an UNRELATED family of the captured full index (caught by indexSha256!).
    compact_snap = artifact.root["spec"]["readSnapshot"]
    assert pub.read_snapshot_matches(compact_snap, index, family="sample-bill-family", parents=parents)
    assert pub.read_snapshot_matches(index, index, family="sample-bill-family", parents=parents)

    tampered_unrelated = deepcopy(index)
    tampered_unrelated["families"]["catalog-family-097"]["tables"]["catalog_table_097_a.parquet"]["rows"] += 1
    assert not pub.read_snapshot_matches(compact_snap, tampered_unrelated, family="sample-bill-family", parents=parents)
    assert not pub.read_snapshot_matches(index, tampered_unrelated, family="sample-bill-family", parents=parents)
    assert not pub.read_snapshot_matches({}, index, family="sample-bill-family", parents=parents)
    assert not pub.read_snapshot_matches(None, index, family="sample-bill-family", parents=parents)

    # 3. NativeConversionProof (_conversion_tables) accepts a compact root.
    prior_bill = index["families"]["sample-bill-family"]
    fake_artifact = SimpleNamespace(
        pin=artifact.pin,
        root={
            "spec": {
                "family": "sample-bill-family",
                "etlReceipts": {"key": "etl_receipts.parquet"},
                "readSnapshot": compact_snap,
            }
        },
    )
    proof = pub.NativeConversionProof(
        logical_id=artifact.pin.logical_id,
        artifact_digest=artifact.pin.artifact_digest,
        prior_entry=canonical_json_bytes(prior_bill),
        restored_tables=frozenset(prior_bill["tables"]),
    )
    assert pub._conversion_tables(proof, fake_artifact, prior_bill) == frozenset(prior_bill["tables"])

    # 4. Scorecard publish verifier accepts BOTH compact and legacy full-snapshot roots,
    #    and rejects a 1-byte tamper in an unrelated family of read_snapshot_path.
    sc_index: dict[str, Any] = {
        "format": "spicy-regs-publication",
        "version": 2,
        "families": {"unrelated": _family_entry("unrelated", "unrelated.parquet", seed="s1", column_count=4)},
    }
    for mode in ("compact", "legacy"):
        work = tmp_path / f"scorecard-{mode}"
        source = work / "source"
        files = write_family(source, scorecard_tables(scorecard_edition("2025")))
        sc_gen = work / "generation"
        sc_art = build_generation(
            sc_gen,
            family="scorecards",
            files=files,
            expected_keys=[p.name for p in files],
            read_snapshot=sc_index,
            **generation_options(source, SOURCE_NAMES),
        )
        if mode == "legacy":
            _restamp_legacy_full_root(sc_gen, sc_index)
            sc_art = verify_generation(sc_gen)
            assert "scope" not in sc_art.root["spec"]["readSnapshot"]
        else:
            assert sc_art.root["spec"]["readSnapshot"]["scope"] == pub.READ_SNAPSHOT_SCOPE
            assert sc_art.root["spec"]["readSnapshot"]["families"] == {}

        idx_path = work / "index.json"
        idx_path.write_bytes(canonical_json_bytes(sc_index))
        prep = work / "preparation.json"
        prep.write_text(json.dumps({
            "status": "prepared_not_published",
            "generation": sc_art.pin.as_dict(),
            "generation_directory": str(sc_gen),
            "read_snapshot_path": str(idx_path),
        }))
        store = Store()
        store.objects[pub.INDEX_V2_KEY] = canonical_json_bytes(sc_index)
        monkeypatch.setattr(scorecard_publish, "load_dotenv", lambda _p: None)
        monkeypatch.setattr(scorecard_publish, "implementation_id", lambda: "test-verifier")
        monkeypatch.setattr(
            store, "meta",
            SimpleNamespace(endpoint_url="https://174055408ff1560e60601c4d12c561c4.r2.cloudflarestorage.com"),
            raising=False,
        )
        monkeypatch.setattr(scorecard_publish.r2, "get_r2_client", lambda: store)
        monkeypatch.setenv("R2_BUCKET_NAME", "spicy-regs")
        scorecard_publish.main(["--preparation", str(prep), "--output", str(work / "published")])
        assert json.loads((work / "published" / "receipt.json").read_text())["status"] == "published_authenticated_readback"

        # 1-byte tamper in unrelated family must be refused.
        tampered_sc = deepcopy(sc_index)
        tampered_sc["families"]["unrelated"]["tables"]["unrelated.parquet"]["rows"] += 1
        idx_path.write_bytes(canonical_json_bytes(tampered_sc))
        with pytest.raises(ValueError, match="Preparation read snapshot differs"):
            scorecard_publish.main(["--preparation", str(prep), "--output", str(work / "refused")])

    # 5. local_publication dry_run also works with a legacy full-snapshot root (dual readability).
    legacy_local_dir = tmp_path / "legacy-local-gen"
    legacy_ev = CaptureEvidence(tmp_path / "legacy-local-ev", "irs-form990")
    legacy_ev.retain_bytes(b"legacy-990", stage="fixture-original", raw_source=True)
    legacy_file = _write_parquet(tmp_path / "legacy-local-src" / "irs990_filings.parquet", "990-legacy")
    from tests.test_local_publication import _table_for_schema
    declared = local_publication._family_schemas("irs-form990") or {}
    if "irs990_filings" in declared:
        pq.write_table(_table_for_schema(declared["irs990_filings"], ["990-legacy"]), legacy_file)
    legacy_idx = deepcopy(sc_index)
    build_generation(
        legacy_local_dir,
        family="irs-form990",
        files=[legacy_file],
        expected_keys=["irs990_filings.parquet"],
        read_snapshot=legacy_idx,
        inputs=legacy_ev.inputs(),
    )
    legacy_ev.finish()
    _restamp_legacy_full_root(legacy_local_dir, legacy_idx)
    legacy_idx_path = tmp_path / "legacy-live.json"
    legacy_idx_path.write_bytes(canonical_json_bytes(legacy_idx))
    local_publication.main([
        "--generation", str(legacy_local_dir),
        "--family", "irs-form990",
        "--evidence", str(legacy_ev.artifact_dir),
        "--output", str(tmp_path / "legacy-dry-receipt"),
        "--dry-run", "--scratch-raw", "--live-index", str(legacy_idx_path),
    ])
    assert json.loads((tmp_path / "legacy-dry-receipt" / "dry-run.json").read_text())["status"] == "dry_run_accepted"


def test_court_pdf_extractions_preserves_contextual_clusters_for_spicygov(tmp_path, monkeypatch):
    """Retain court-opinion-clusters beside court-opinions so spicygov/scripts/congress_coverage.py:463 stays green."""
    opinion, capture = court_pdf_case()
    capture.media_type = "application/pdf"
    snapshot = {
        "format": "spicy-regs-publication",
        "version": 2,
        "families": {
            "court-opinions": _family_entry("court-opinions", "court_opinions.parquet", seed="o1", column_count=6),
            "court-opinion-clusters": _family_entry(
                "court-opinion-clusters", "court_opinion_clusters.parquet", seed="c1", column_count=6
            ),
            "unrelated-family": _family_entry("unrelated-family", "unrelated.parquet", seed="u1", column_count=6),
        },
    }
    monkeypatch.setattr(court_pdf, "qualify_parent_opinions", lambda *_: None)
    original_shape = court_pdf.shape_captured_opinion
    monkeypatch.setattr(
        court_pdf,
        "shape_captured_opinion",
        lambda op, cap, *, parent: original_shape(
            op, cap, parent=parent, extract=lambda _: PdfTextResult(PdfTextStatus.OK, "extracted text", 1)
        ),
    )

    output = tmp_path / "court-pdf-candidate"
    result = court_pdf.prepare_captured_opinions(
        output,
        [(opinion, capture)],
        read_snapshot=snapshot,
        public_url="https://fixture.invalid",
    )
    artifact = verify_generation(Path(result["generation_directory"]))
    recorded = artifact.root["spec"]["readSnapshot"]
    assert recorded["scope"] == pub.READ_SNAPSHOT_SCOPE
    assert recorded["indexFamilyCount"] == 3
    assert set(recorded["families"]) == {"court-opinions", "court-opinion-clusters"}
    assert recorded["families"]["court-opinion-clusters"] == snapshot["families"]["court-opinion-clusters"]
    assert "unrelated-family" not in recorded["families"]
    assert pub.read_snapshot_matches(
        recorded,
        snapshot,
        family=court_pdf.FAMILY,
        parents=artifact.root["spec"]["parents"],
        extra_families=("court-opinion-clusters",),
    )


def test_merge_family_warns_at_80_percent_of_index_limit_and_refuses_above_limit():
    """INDEX_LIMIT stays 1 MiB; _merge_family logs a non-blocking warning at >=80% (838,861 B) and refuses >1 MiB."""
    assert pub.INDEX_LIMIT == 1024 * 1024
    assert pub.INDEX_WARNING_BYTES == 838_861

    warnings: list[str] = []
    sink_id = logger.add(lambda msg: warnings.append(str(msg)), level="WARNING")
    try:
        # Build an index whose serialized bytes sit just below 80% (~790 KiB).
        families: dict[str, Any] = {}
        idx = 0
        while True:
            name = f"fam-{idx:04d}"
            families[name] = _family_entry(name, f"tbl_{idx:04d}.parquet", seed="w", column_count=45, split_parts=10)
            size = len(canonical_json_bytes({"format": "spicy-regs-publication", "version": 2, "families": families}))
            if size >= pub.INDEX_WARNING_BYTES - 20_000:
                families.pop(name)
                break
            idx += 1

        base_index = {"format": "spicy-regs-publication", "version": 2, "families": families}
        small_entry = _family_entry("probe", "probe.parquet", seed="small", column_count=4)
        _, raw_below = pub._merge_family(base_index, "probe", small_entry)
        assert len(raw_below) < pub.INDEX_WARNING_BYTES
        assert warnings == []

        # Push across 80% while staying under 1 MiB -> non-blocking warning, returns valid merged index.
        big_entry = _family_entry("probe", "probe.parquet", seed="warn", column_count=45, split_parts=120)
        updated, raw_warn = pub._merge_family(base_index, "probe", big_entry)
        assert pub.INDEX_WARNING_BYTES <= len(raw_warn) <= pub.INDEX_LIMIT
        assert updated["families"]["probe"] == big_entry
        assert len(warnings) == 1
        assert "INDEX_LIMIT" in warnings[0]

        # Push above 1 MiB -> refuses with PublicationError and emits no extra non-blocking warning.
        warnings.clear()
        too_big_entry = _family_entry("probe", "probe.parquet", seed="over", column_count=45, split_parts=1300)
        with pytest.raises(pub.PublicationError, match="exceeds its byte limit"):
            pub._merge_family(base_index, "probe", too_big_entry)
        assert warnings == []
    finally:
        logger.remove(sink_id)
