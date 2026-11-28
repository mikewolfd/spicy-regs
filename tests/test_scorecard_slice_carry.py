"""Parity, invariant, and refusal tests for receiptless Arrow/DuckDB scorecard slice-carry."""

from __future__ import annotations

from copy import deepcopy
import hashlib
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
import yaml

from spicy_regs.contract_types import described_schema
import spicy_regs.data_dictionary as dictionary
from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.pipelines.rollups.scorecards import ScorecardsRollup
from spicy_regs.receipt_switch import receipt_files
from spicy_regs.scorecards.etl import POLICIES, SOURCE_NAMES, generation_schemas, read_family, write_family
from spicy_regs.scorecards.registry import REGISTRY
from spicy_regs.sources import publication as pub
from spicy_regs.transforms.build_scorecards import ScorecardRefreshError, build_scorecards
from tests.generation_fakes import Store
from tests.test_scorecard_refresh import (
    NOW,
    capture,
    edition,
    evidence,
    journal,
    no_fetch,
    provider,
    tables,
)


def _combine_multi(*bundles: dict) -> dict:
    result: dict[str, list[dict]] = {name: [] for name in bundles[0]}
    publishers: dict[str, dict] = {}
    for bundle in bundles:
        for name, rows in bundle.items():
            if name == "scorecard_publishers":
                for row in rows:
                    publishers[row["publisher_id"]] = row
            else:
                result[name].extend(rows)
    result["scorecard_publishers"] = list(publishers.values())
    return result


def _multi_publisher_registry(tmp_path: Path, publishers: tuple[str, ...]) -> Path:
    document = yaml.safe_load(REGISTRY.read_text())
    selected = []
    for publisher_id in publishers:
        entry = deepcopy(next(row for row in document["sources"] if row["publisher_id"] == publisher_id))
        entry.update(
            enabled=True,
            qualification_id=f"synthetic-qualified-{publisher_id}-v1",
            historical_backfill=True,
        )
        selected.append(entry)
    path = tmp_path / "sources.yaml"
    path.write_text(yaml.safe_dump(dict(version=1, sources=selected)))
    return path


def _multi_publisher_provider(editions_by_id: dict, outcomes: dict, *, prior_provider=None):
    base = prior_provider or provider([], {})

    class MultiAdapter:
        parser_version = "test-v1"

        def __init__(self, publisher_id: str) -> None:
            self.publisher_id = publisher_id

        def list_scorecards(self, context):
            return [e for e in editions_by_id.values() if e.publisher_id == self.publisher_id]

        def acquire_scorecard(self, selected, context):
            outcome = outcomes[selected.scorecard_id]
            if isinstance(outcome, Exception):
                raise outcome
            receipt = context.capture(capture(b"PRIVATE SOURCE PAYLOAD"), stage="test")
            rows = outcome(selected, receipt["capture_id"])
            return SimpleNamespace(
                scorecard_id=selected.scorecard_id,
                snapshot_id=rows["scorecard_snapshots"][0]["snapshot_id"],
                tables=rows,
                capture_ids=(receipt["capture_id"],),
                completeness_rule="Synthetic complete member table",
            )

    return SimpleNamespace(
        contracts=base.contracts,
        get_adapter=lambda name: MultiAdapter(
            next(
                e.publisher_id
                for e in editions_by_id.values()
                if e.publisher_id == name or name.startswith(e.publisher_id)
            )
        ),
        validate=base.validate,
        context=base.context,
        refusal=base.refusal,
    )


def _rich_bundle(selected, cap: str, *, members: int = 4, items: bool = True, escape: bool = False):
    rows = tables(selected, cap, members=members, items=items)
    if escape:
        rows["scorecard_members"][0]["member_name"] = "Member\x1b[0m"
    if items:
        rows["scorecard_metric_items"].append(
            {
                "scorecard_id": selected.scorecard_id,
                "metric_id": "annual",
                "item_id": "item",
                "participation_id": "p-tiny",
                "methodology_id": None,
                "publisher_position_text": "Yes",
                "position_basis": "explicit",
                "weight_text": "0.0000001",
                "weight_number": "0.0000001",
                "contribution_rule_text": "weighted",
                "counts_toward_metric": "true",
                "snapshot_id": rows["scorecard_snapshots"][0]["snapshot_id"],
                "capture_id": cap,
                "source_url": "https://source.test/body",
                "source_path": "synthetic fixture",
            }
        )
    return rows


def _publish_prior_native(tmp_path: Path, monkeypatch, initial_rows: dict):
    prior_dir = tmp_path / "prior_etl"
    files = write_family(prior_dir, initial_rows)
    generation = tmp_path / "prior_generation"
    build_generation(
        generation,
        family="scorecards",
        files=files,
        expected_keys=tuple(p.name for p in files),
        schemas=generation_schemas(SOURCE_NAMES),
    )
    store = Store()
    index = pub.publish_generation(generation, client=store, bucket="test", prior_index=pub.empty_index())

    def fetch(url, member, target, key=None):
        target.write_bytes(store.objects[member.path])
        return True

    monkeypatch.setattr(pub, "fetch_member", fetch)

    def download(key, target):
        return fetch(None, pub.single_member(index, key), target)

    return store, index, prior_dir, download


def _assert_parquet_except_all_equal(dir_a: Path, dir_b: Path, filenames: tuple[str, ...]) -> None:
    with duckdb.connect() as con:
        for filename in filenames:
            path_a, path_b = dir_a / filename, dir_b / filename
            with pq.ParquetFile(path_a) as pf_a, pq.ParquetFile(path_b) as pf_b:
                assert pf_a.schema_arrow.equals(pf_b.schema_arrow, check_metadata=False), (
                    f"Schema mismatch on {filename}: {pf_a.schema_arrow} vs {pf_b.schema_arrow}"
                )
                assert pf_a.metadata.num_rows == pf_b.metadata.num_rows, f"Row count mismatch on {filename}"
            lit_a = repr(str(path_a))
            lit_b = repr(str(path_b))
            row = con.execute(
                f"SELECT count(*) FROM ("
                f"(SELECT * FROM read_parquet({lit_a}, hive_partitioning=false) EXCEPT ALL "
                f"SELECT * FROM read_parquet({lit_b}, hive_partitioning=false)) "
                f"UNION ALL "
                f"(SELECT * FROM read_parquet({lit_b}, hive_partitioning=false) EXCEPT ALL "
                f"SELECT * FROM read_parquet({lit_a}, hive_partitioning=false)))"
            ).fetchone()
            assert row == (0,), f"EXCEPT ALL mismatch on {filename}: {row}"


def test_slice_carry_matches_full_rebuild_across_multiple_publishers_and_streams_without_receipts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    publishers = ("lcv", "afscme", "hrc", "nea", "afl_cio")
    reg_path = _multi_publisher_registry(tmp_path, publishers)
    editions = {
        "lcv:2025": edition("2025", publisher="lcv"),
        "lcv:2024": edition("2024", publisher="lcv", current=False),
        "afscme:2025": edition("2025", publisher="afscme"),
        "hrc:2025": edition("2025", publisher="hrc"),
        "nea:2025": edition("2025", publisher="nea"),
        "afl_cio:2025": edition("2025", publisher="afl_cio"),
    }
    initial = _combine_multi(
        _rich_bundle(editions["lcv:2025"], "cap-lcv-2025", members=4, escape=True),
        _rich_bundle(editions["lcv:2024"], "cap-lcv-2024", members=3),
        _rich_bundle(editions["afscme:2025"], "cap-afscme-2025", members=3),
        _rich_bundle(editions["hrc:2025"], "cap-hrc-2025", members=5, escape=True),
        _rich_bundle(editions["nea:2025"], "cap-nea-2025", members=2),
        _rich_bundle(editions["afl_cio:2025"], "cap-afl-2025", members=3),
    )
    store, index, _, download = _publish_prior_native(tmp_path, monkeypatch, initial)

    outcomes = {
        "lcv:2025": lambda sel, cap: _rich_bundle(sel, cap, members=2, items=False),
        "lcv:2024": provider([], {}).refusal("simulated edition refusal"),
        "afscme:2025": lambda sel, cap: _rich_bundle(sel, cap, members=4, escape=True),
    }

    def deterministic_provider():
        base = _multi_publisher_provider(editions, outcomes)

        class DetAdapter:
            parser_version = "test-v1"

            def __init__(self, pub_id: str) -> None:
                self._inner = base.get_adapter(pub_id)

            def list_scorecards(self, ctx):
                return self._inner.list_scorecards(ctx)

            def acquire_scorecard(self, sel, ctx):
                outcome = outcomes[sel.scorecard_id]
                if isinstance(outcome, Exception):
                    raise outcome
                receipt = ctx.capture(capture(b"PRIVATE SOURCE PAYLOAD"), stage="test")
                cap_id = receipt["capture_id"]
                rows = outcome(sel, cap_id)
                snap_id = f"scorecard-snapshot:fixed-{sel.scorecard_id}"
                for table_rows in rows.values():
                    for row in table_rows:
                        if "snapshot_id" in row:
                            row["snapshot_id"] = snap_id
                return SimpleNamespace(
                    scorecard_id=sel.scorecard_id,
                    snapshot_id=snap_id,
                    tables=rows,
                    capture_ids=(cap_id,),
                    completeness_rule="Synthetic complete member table",
                )

        return SimpleNamespace(
            contracts=base.contracts,
            get_adapter=lambda name: DetAdapter(name),
            validate=base.validate,
            context=base.context,
            refusal=base.refusal,
        )

    def deterministic_uuids():
        from uuid import UUID

        counter = 0

        def next_uuid():
            nonlocal counter
            counter += 1
            return UUID(int=counter)

        return next_uuid

    monkeypatch.setattr(
        pub,
        "_bounded_get",
        lambda url, *, allow_missing=False, headers=None, limit=pub.INDEX_LIMIT: store.objects.get(
            url.removeprefix("https://data.example/")
        ),
    )

    # 1. Full reference rebuild (validate_readback forces full materialization + write_family).
    with monkeypatch.context() as patch:
        patch.setattr("spicy_regs.source_evidence.uuid4", deterministic_uuids())
        full_ev = evidence(tmp_path / "full_run")
        full_ev.inherit(index, public_url="https://data.example")
        build_scorecards(
            tmp_path / "full_build",
            evidence=full_ev,
            registry=reg_path,
            publishers=("lcv", "afscme"),
            historical_backfill=True,
            force=True,
            provider=deterministic_provider(),
            fetch_factory=no_fetch,
            download_prior=download,
            now=NOW,
            validate_readback=lambda _prior, _current: None,
        )

    # 2. Incremental slice-carry rebuild (validate_readback=None) — prove streaming only & zero receipts.
    with monkeypatch.context() as patch:
        patch.setattr("spicy_regs.source_evidence.uuid4", deterministic_uuids())
        slice_ev = evidence(tmp_path / "slice_run")
        slice_ev.inherit(index, public_url="https://data.example")
        orig_pf = pq.ParquetFile

        class NoEagerReadParquetFile:
            def __init__(self, *args, **kwargs):
                self._inner = orig_pf(*args, **kwargs)

            def __getattr__(self, item):
                return getattr(self._inner, item)

            def __enter__(self):
                self._inner.__enter__()
                return self

            def __exit__(self, *args):
                return self._inner.__exit__(*args)

            def read(self, *args, **kwargs):
                pytest.fail("Slice-carry must stream batches, never eagerly materialize ParquetFile.read()")

        patch.setattr(pq, "ParquetFile", NoEagerReadParquetFile)
        build_scorecards(
            tmp_path / "slice_build",
            evidence=slice_ev,
            registry=reg_path,
            publishers=("lcv", "afscme"),
            historical_backfill=True,
            force=True,
            provider=deterministic_provider(),
            fetch_factory=no_fetch,
            download_prior=download,
            now=NOW,
        )

    assert receipt_files(tmp_path / "full_build", tmp_path / "slice_build") == []
    _assert_parquet_except_all_equal(
        tmp_path / "full_build",
        tmp_path / "slice_build",
        tuple(f"{n}.parquet" for n in SOURCE_NAMES),
    )
    assert read_family(tmp_path / "slice_build", SOURCE_NAMES) == read_family(tmp_path / "full_build", SOURCE_NAMES)

    full_retired = [
        {k: v for k, v in e.items() if k != "recorded_at"}
        for e in journal(full_ev)
        if e["event"] == "rows-retired"
    ]
    slice_retired = [
        {k: v for k, v in e.items() if k != "recorded_at"}
        for e in journal(slice_ev)
        if e["event"] == "rows-retired"
    ]
    assert slice_retired == full_retired and len(slice_retired) > 0

    slice_ev.finish()
    gen_dir = tmp_path / "slice_generation"
    artifact = build_generation(
        gen_dir,
        family="scorecards",
        files=tuple(tmp_path / "slice_build" / f"{n}.parquet" for n in SOURCE_NAMES),
        expected_keys=tuple(f"{n}.parquet" for n in SOURCE_NAMES),
        read_snapshot=index,
        inputs=slice_ev.inputs(),
        schemas=generation_schemas(SOURCE_NAMES),
    )
    admitted = verify_generation(gen_dir)
    assert admitted.pin == artifact.pin
    assert "etlReceipts" not in admitted.root["spec"]
    published = pub.publish_generation(
        gen_dir,
        client=store,
        bucket="test",
        prior_index=index,
        evidence_directories=(slice_ev.artifact_dir,),
    )
    assert published["families"]["scorecards"]["artifactDigest"] == artifact.pin.artifact_digest
    assert "etlReceipts" not in published["families"]["scorecards"]


def test_rollup_pipeline_end_to_end_slice_carry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify ScorecardsRollup builds a receiptless slice-carried generation end-to-end."""
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.setattr(
        dictionary,
        "expected_schemas",
        lambda: {
            name: described_schema(POLICIES[name].subject_schema)
            for name in SOURCE_NAMES
            if not POLICIES[name].receipt_only
        },
    )
    publishers = ("lcv", "afscme", "hrc")
    reg_path = _multi_publisher_registry(tmp_path, publishers)
    editions = {
        "lcv:2025": edition("2025", publisher="lcv"),
        "afscme:2025": edition("2025", publisher="afscme"),
        "hrc:2025": edition("2025", publisher="hrc"),
    }
    initial = _combine_multi(
        _rich_bundle(editions["lcv:2025"], "cap-1", members=3),
        _rich_bundle(editions["afscme:2025"], "cap-2", members=3),
        _rich_bundle(editions["hrc:2025"], "cap-3", members=3),
    )
    store, index, _, download = _publish_prior_native(tmp_path, monkeypatch, initial)
    monkeypatch.setenv("R2_PUBLIC_URL", "https://data.example")
    monkeypatch.setattr(pub, "load_index", lambda *args: index)
    monkeypatch.setattr(
        pub,
        "_bounded_get",
        lambda url, *, allow_missing=False, headers=None, limit=pub.INDEX_LIMIT: store.objects.get(
            url.removeprefix("https://data.example/")
        ),
    )

    out_dir = tmp_path / "rollup_out"
    p = _multi_publisher_provider(editions, {"lcv:2025": lambda sel, cap: _rich_bundle(sel, cap, members=5)})
    rollup = ScorecardsRollup(
        output_dir=out_dir,
        registry=reg_path,
        publishers=("lcv",),
        force=True,
        provider=p,
        fetch_factory=no_fetch,
    )
    from spicy_regs.sources import r2

    monkeypatch.setattr(r2, "download", download)
    rollup.run()
    gens = list((out_dir / "generations").iterdir())
    assert len(gens) == 1
    verified = verify_generation(gens[0])
    assert verified.root["spec"]["family"] == "scorecards"
    assert "etlReceipts" not in verified.root["spec"]
    assert receipt_files(out_dir) == []
    restored = read_family(gens[0], SOURCE_NAMES)
    assert len([r for r in restored["scorecard_members"] if r["scorecard_id"] == "lcv:2025"]) == 5
    assert len([r for r in restored["scorecard_members"] if r["scorecard_id"] == "afscme:2025"]) == 3
    assert len([r for r in restored["scorecard_members"] if r["scorecard_id"] == "hrc:2025"]) == 3


@pytest.mark.parametrize(
    "fault",
    [
        "index_missing_column",
        "parquet_missing_column",
        "parquet_wrong_type",
        "parquet_row_count_mismatch",
        "duplicate_kept_member",
        "duplicate_kept_publisher",
        "missing_kept_publisher_fk",
        "incomplete_kept_snapshot",
        "orphan_kept_child_snapshot",
    ],
)
def test_slice_carry_refuses_loudly_on_schema_mismatch_and_corrupt_slices(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    publishers = ("lcv", "afscme")
    reg_path = _multi_publisher_registry(tmp_path, publishers)
    editions = {
        "lcv:2025": edition("2025", publisher="lcv"),
        "afscme:2025": edition("2025", publisher="afscme"),
    }
    initial = _combine_multi(
        _rich_bundle(editions["lcv:2025"], "cap-lcv", members=3),
        _rich_bundle(editions["afscme:2025"], "cap-afscme", members=3),
    )
    _, index, prior_dir, _ = _publish_prior_native(tmp_path, monkeypatch, initial)
    bad_index = deepcopy(index)
    family_entry = bad_index["families"]["scorecards"]

    def re_pin_file(key: str, table: pa.Table) -> None:
        target = prior_dir / key
        pq.write_table(table, target)
        raw = target.read_bytes()
        family_entry["tables"][key]["sha256"] = "sha256:" + hashlib.sha256(raw).hexdigest()
        family_entry["tables"][key]["byteSize"] = len(raw)
        family_entry["tables"][key]["rows"] = table.num_rows

    if fault == "index_missing_column":
        family_entry["tables"]["scorecard_members.parquet"]["columns"].pop()
    elif fault == "parquet_missing_column":
        tbl = pq.read_table(prior_dir / "scorecard_members.parquet").drop(["notes_text"])
        re_pin_file("scorecard_members.parquet", tbl)
    elif fault == "parquet_wrong_type":
        tbl = pq.read_table(prior_dir / "scorecard_metric_items.parquet")
        idx = tbl.schema.get_field_index("weight_number")
        tbl = tbl.set_column(
            idx,
            "weight_number",
            pa.array([str(x) if x is not None else None for x in tbl["weight_number"].to_pylist()], type=pa.string()),
        )
        re_pin_file("scorecard_metric_items.parquet", tbl)
    elif fault == "parquet_row_count_mismatch":
        family_entry["tables"]["scorecard_members.parquet"]["rows"] += 1
    elif fault == "duplicate_kept_member":
        tbl = pq.read_table(prior_dir / "scorecard_members.parquet")
        kept_row = tbl.filter(pc.call_function("equal", [tbl["scorecard_id"], "afscme:2025"])).slice(0, 1)
        re_pin_file("scorecard_members.parquet", pa.concat_tables([tbl, kept_row]))
    elif fault == "duplicate_kept_publisher":
        tbl = pq.read_table(prior_dir / "scorecard_publishers.parquet")
        kept_row = tbl.filter(pc.call_function("equal", [tbl["publisher_id"], "afscme"])).slice(0, 1)
        re_pin_file("scorecard_publishers.parquet", pa.concat_tables([tbl, kept_row]))
    elif fault == "missing_kept_publisher_fk":
        tbl = pq.read_table(prior_dir / "scorecard_publishers.parquet")
        tbl = tbl.filter(pc.call_function("not_equal", [tbl["publisher_id"], "afscme"]))
        re_pin_file("scorecard_publishers.parquet", tbl)
    elif fault == "incomplete_kept_snapshot":
        tbl = pq.read_table(prior_dir / "scorecard_snapshots.parquet")
        rows = tbl.to_pylist()
        for row in rows:
            if row["scorecard_id"] == "afscme:2025":
                row["completeness_status"] = "incomplete"
        re_pin_file("scorecard_snapshots.parquet", pa.Table.from_pylist(rows, schema=tbl.schema))
    elif fault == "orphan_kept_child_snapshot":
        tbl = pq.read_table(prior_dir / "scorecard_members.parquet")
        rows = tbl.to_pylist()
        for row in rows:
            if row["scorecard_id"] == "afscme:2025":
                row["snapshot_id"] = "scorecard-snapshot:orphan"
        re_pin_file("scorecard_members.parquet", pa.Table.from_pylist(rows, schema=tbl.schema))

    def download_faulty(key: str, target: Path) -> bool:
        target.write_bytes((prior_dir / key).read_bytes())
        return True

    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    ev = evidence(tmp_path / "fault_run")
    ev.read_snapshot = bad_index
    p = _multi_publisher_provider(editions, {"lcv:2025": lambda sel, cap: _rich_bundle(sel, cap, members=2)})

    with pytest.raises((ValueError, ScorecardRefreshError)):
        build_scorecards(
            tmp_path / "fault_build",
            evidence=ev,
            registry=reg_path,
            publishers=("lcv",),
            force=True,
            provider=p,
            fetch_factory=no_fetch,
            download_prior=download_faulty,
            now=NOW,
        )
    assert not list((tmp_path / "fault_build").glob("score*.parquet"))
