"""court_opinion_texts: receipts-free streaming build, local pipeline, and MCP Hive partition pruning.

The export fixture is written in the publisher's dialect with records out of id order across three opinion blocks.
The converter is a stand-in (the real Converter 003 is tested in SpicyDocs and exercised in the local canary);
what is tested here is the receipts-free courts/2 build: 11-column schema, export vs per-rendering digests,
numeric id ordering, member layout, row-group and footer statistics bounds, duplicate-id refusal, exceptions file,
atomic commit and resume, parallel Phase 2 workers, local-publication pipeline + disk/raw guards, and HTTP
request-counting proof that mcp_server.py prunes opinion_block files via parquet_scan(partition_types=...).
"""

from __future__ import annotations

import bz2
from datetime import date
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import random
import shutil
import threading
from types import SimpleNamespace
from typing import NamedTuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import court_bulk, local_publication, mcp_server
from spicy_regs.court_subjects import SUBJECT_SCHEMAS
from spicy_regs.generations import build_generation
from spicy_regs.pipelines import court_opinion_texts as pipeline
from spicy_regs.sources import publication
from spicy_regs.transforms import build_court_opinion_texts as build
from tests.test_mcp_server import _records, _tool_data

WIDTH = build.OPINION_BLOCK_WIDTH
HEADER = ("id", "cluster_id", "date_modified", "html_with_citations", "html", "plain_text")


class _Text(NamedTuple):
    markdown: str
    text_source: str
    cites: tuple


def stand_in(row: dict) -> _Text:
    """Echo the first filled rendering; refuse one marked unconvertible, as the converter's error would."""
    for column in ("html_with_citations", "html", "plain_text"):
        value = row.get(column)
        if value and value.strip():
            if "UNCONVERTIBLE" in value:
                raise ValueError("unparseable rendering")
            return _Text(f"md:{value}", column, ())
    return _Text("", "none", ())


def _quoted(value: str | None) -> str:
    return "" if value is None else '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def _export(tmp_path: Path, rows: list[tuple], name: str = "opinions-2026-09-30.csv.bz2") -> Path:
    path = tmp_path / name
    lines = [",".join(f'"{col}"' for col in HEADER)] + [",".join(_quoted(v) for v in row) for row in rows]
    path.write_bytes(bz2.compress(("\n".join(lines) + "\n").encode()))
    return path


def _rows() -> list[tuple]:
    ids = [*range(5, 40), *range(WIDTH + 1, WIDTH + 30), *range(2 * WIDTH + 7, 2 * WIDTH + 20)]
    random.Random(7).shuffle(ids)
    rows = []
    for n in ids:
        text = "x" * (200 + n % 50)
        if n % 17 == 0:
            text += "\x1b"
        if n % 11 == 0:
            rows.append((str(n), str(n * 3), "2026-09-01T00:00:00Z", "", text, None))
        elif n % 13 == 0:
            rows.append((str(n), str(n * 3), "2026-09-01T00:00:00Z", None, None, text))
        elif n == WIDTH + 2:
            rows.append((str(n), str(n * 3), "2026-09-01T00:00:00Z", None, None, None))
        elif n == 2 * WIDTH + 9:
            rows.append((str(n), str(n * 3), "2026-09-01T00:00:00Z", "<p>UNCONVERTIBLE</p>", None, None))
        else:
            rows.append((str(n), str(n * 3), "2026-09-01T00:00:00Z", f'<p data-x="1">{text}</p>', None, None))
    return rows


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> tuple[Path, Path, dict, list[tuple]]:
    tmp = tmp_path_factory.mktemp("texts")
    rows = _rows()
    export = _export(tmp, rows)
    run = build.build_court_opinion_texts(
        export,
        tmp / "out",
        dump_date=date(2026, 9, 30),
        workers=0,
        convert=stand_in,
        parse_workers=1,
        row_group_text_bytes=2_000,
    )
    return tmp / "out", export, run, rows


def _members(out: Path) -> list[Path]:
    return sorted(
        (out / build.DATASET).glob("opinion_block=*/part-*.parquet"),
        key=lambda p: int(p.parent.name.partition("=")[2]),
    )


def test_members_are_id_blocks_sorted_numerically_disjoint_and_receipts_free(built) -> None:
    out, export, run, rows = built
    export_sha = "sha256:" + hashlib.sha256(export.read_bytes()).hexdigest()
    assert not (out / "etl_receipts.parquet").exists()
    members = _members(out)
    assert [m.parent.name for m in members] == ["opinion_block=0", "opinion_block=1", "opinion_block=2"]
    assert run["source_sha256"] == export_sha
    assert run["members"] == 3 and run["opinion_blocks"] == [0, 1, 2]

    columns = ("html_with_citations", "html", "plain_text")
    expected_rendering: dict[str, tuple[str | None, str | None]] = {}
    for n, _, _, *renderings in rows:
        col = next((c for c, v in zip(columns, renderings) if v and v.strip()), None)
        val = renderings[columns.index(col)] if col else None
        expected_rendering[n] = (col, None if val is None else "sha256:" + hashlib.sha256(val.encode()).hexdigest())

    previous = -1
    observed_rendering: dict[str, tuple[str | None, str | None]] = {}
    for member in members:
        table = pq.ParquetFile(member).read()
        assert table.schema.equals(SUBJECT_SCHEMAS[build.DATASET])
        ids = [int(v) for v in table["opinion_id"].to_pylist()]
        assert ids == sorted(ids) and ids[0] > previous
        assert {int(v) // WIDTH for v in ids} == {int(member.parent.name.partition("=")[2])}
        assert set(table["opinion_block"].to_pylist()) == {member.parent.name.partition("=")[2]}
        assert set(table["dump_date"].to_pylist()) == {"2026-09-30"}
        assert set(table["source_sha256"].to_pylist()) == {export_sha}
        for row in table.to_pylist():
            observed_rendering[row["opinion_id"]] = (row["source_column"], row["source_text_sha256"])
        previous = ids[-1]

    written = sum(pq.ParquetFile(m).metadata.num_rows for m in members)
    assert written == len(rows) - 1 == run["rows_written"]
    assert observed_rendering == {k: v for k, v in expected_rendering.items() if k != str(2 * WIDTH + 9)}
    assert observed_rendering[str(WIDTH + 2)] == (None, None)


def test_row_groups_compression_and_statistics_follow_the_layout(built) -> None:
    out, _, _, _ = built
    for member in _members(out):
        meta = pq.ParquetFile(member).metadata
        schema = meta.schema.to_arrow_schema()
        markdown_idx = schema.get_field_index("markdown")
        text_sha_idx = schema.get_field_index("source_text_sha256")
        assert meta.num_row_groups > 1
        for g in range(meta.num_row_groups):
            group = meta.row_group(g)
            for omitted_idx in (markdown_idx, text_sha_idx):
                col = group.column(omitted_idx)
                assert col.compression == "ZSTD"
                assert col.statistics is None or not col.statistics.has_min_max
            for short_col in build.STATISTICS:
                col_idx = schema.get_field_index(short_col)
                assert group.column(col_idx).statistics is not None
                assert group.column(col_idx).statistics.has_min_max, short_col
            text = pq.ParquetFile(member).read_row_group(g, columns=["markdown"])["markdown"]
            sizes = [len(v.encode()) for v in text.to_pylist() if v]
            assert sum(sizes) <= 2_000 or len(sizes) == 1


def test_every_record_is_counted_once_and_no_text_is_null_markdown(built) -> None:
    out, _, run, rows = built
    table = duckdb.sql(
        f"SELECT * FROM read_parquet('{out / build.DATASET}/*/*.parquet', hive_partitioning=false)"
    ).to_arrow_table()
    by_source = {s: table["text_source"].to_pylist().count(s) for s in set(table["text_source"].to_pylist())}
    assert run["rows_read"] == len(rows) == table.num_rows + run["refused"]
    assert run["refused"] == 1 and run["status"] == "complete" and run["completed_read"]
    assert run["rows_by_text_source"] == by_source
    assert sum(by_source.values()) + run["refused"] == run["rows_read"]
    assert by_source["none"] == 1 and set(by_source) == {"html_with_citations", "html", "plain_text", "none"}
    none_rows = [r for r in table.to_pylist() if r["text_source"] == "none"]
    assert len(none_rows) == 1
    assert none_rows[0]["markdown"] is None
    assert none_rows[0]["source_column"] is None
    assert none_rows[0]["source_text_sha256"] is None


def test_exceptions_point_at_the_rendering_and_never_copy_it(built, tmp_path) -> None:
    out, export, run, _ = built
    export_sha = "sha256:" + hashlib.sha256(export.read_bytes()).hexdigest()
    exc_path = out / f"{build.DATASET}.exceptions.parquet"
    assert exc_path.is_file() and run["exceptions_file"] == exc_path.name
    exc_table = pq.read_table(exc_path)
    assert exc_table.schema.equals(court_bulk.EXCEPTION_SCHEMA)
    rows = exc_table.to_pylist()
    assert len(rows) == 1
    refusal = rows[0]
    assert refusal["dataset"] == build.DATASET
    assert refusal["outcome"] == "refused"
    assert "ValueError: unparseable rendering" in refusal["reason"]
    assert "UNCONVERTIBLE" not in refusal["source_record"]
    assert "xxxxxxxx" not in refusal["source_record"]
    pointer = json.loads(refusal["source_record"])
    assert pointer["opinion_id"] == str(2 * WIDTH + 9)
    assert pointer["source_column"] == "html_with_citations"
    assert pointer["source_text_sha256"] == "sha256:" + hashlib.sha256(b"<p>UNCONVERTIBLE</p>").hexdigest()
    assert pointer["source_sha256"] == export_sha
    assert pointer["dump_date"] == "2026-09-30"

    # An export with zero refusals leaves no exceptions file.
    clean_export = _export(
        tmp_path,
        [("10", "30", "2026-09-01T00:00:00Z", "<p>hello</p>", None, None)],
        name="clean.csv.bz2",
    )
    clean_out = tmp_path / "clean-out"
    clean_run = build.build_court_opinion_texts(
        clean_export, clean_out, dump_date=date(2026, 9, 30), workers=0, convert=stand_in, parse_workers=1
    )
    assert clean_run["refused"] == 0 and clean_run["exceptions_file"] is None
    assert not (clean_out / f"{build.DATASET}.exceptions.parquet").exists()


def test_excessive_refusals_above_max_exceptions_abort_build(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(build, "MAX_EXCEPTIONS", 2)
    bad_rows = [
        (str(i), str(i * 3), "2026-09-01T00:00:00Z", "<p>UNCONVERTIBLE</p>", None, None)
        for i in range(1, 5)
    ]
    export = _export(tmp_path, bad_rows, name="bad.csv.bz2")
    with pytest.raises(RuntimeError, match="more than 2 rows refused"):
        build.build_court_opinion_texts(
            export, tmp_path / "out", dump_date=date(2026, 9, 30), workers=0, convert=stand_in, parse_workers=1
        )


def test_duplicate_opinion_id_is_refused_in_phase_2(tmp_path) -> None:
    dup_rows = [
        ("42", "100", "2026-09-01T00:00:00Z", "<p>first</p>", None, None),
        ("43", "101", "2026-09-01T00:00:00Z", "<p>second</p>", None, None),
        ("42", "100", "2026-09-01T00:00:00Z", "<p>duplicate</p>", None, None),
    ]
    export = _export(tmp_path, dup_rows, name="dup.csv.bz2")
    out = tmp_path / "dup-out"
    with pytest.raises(ValueError, match="duplicate opinion_id in opinion_block=0.*'42'"):
        build.build_court_opinion_texts(
            export, out, dump_date=date(2026, 9, 30), workers=0, convert=stand_in, parse_workers=1
        )
    assert not (out / build.DATASET / "opinion_block=0" / "part-000000.parquet").exists()
    assert not list((out / build.DATASET).rglob("*.tmp"))


def test_the_spill_is_one_file_per_bucket_and_refusals_are_counted_by_rendering(built) -> None:
    _, _, run, _ = built
    assert run["spill_files"] == run["spill_row_groups"] == 3
    assert run["spill_compression_level"] == build.DEFAULT_SPILL_COMPRESSION_LEVEL == 6
    assert run["refused_by_text_source"] == {"html_with_citations": 1} and not run["resumed"]
    assert run["max_rss_main_process_bytes"] > 1 << 20


def test_a_failed_phase_2_resumes_past_committed_blocks_and_unlinks_committed_spill(built, tmp_path, monkeypatch) -> None:
    out, export, run, _ = built
    calls = {"n": 0}
    orig_writer = build._member_writer

    def failing_writer(path: Path, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:  # block 1 fails after block 0 committed
            raise RuntimeError("interrupted")
        return orig_writer(path, **kwargs)

    monkeypatch.setattr(build, "_member_writer", failing_writer)
    again = tmp_path / "out"
    with pytest.raises(RuntimeError, match="interrupted"):
        build.build_court_opinion_texts(
            export,
            again,
            dump_date=date(2026, 9, 30),
            workers=0,
            convert=stand_in,
            parse_workers=1,
            row_group_text_bytes=2_000,
        )
    spill = again / f".{build.DATASET}-spill"
    assert (spill / "_complete.json").exists()
    # Block 0 committed and its spill bucket was deleted immediately; block 1 has no committed file or .tmp.
    assert (again / build.DATASET / "opinion_block=0" / "part-000000.parquet").exists()
    assert not (spill / "spill_bucket=0.parquet").exists()
    assert not (again / build.DATASET / "opinion_block=1" / "part-000000.parquet").exists()
    assert not list((again / build.DATASET).rglob("*.tmp"))
    assert (spill / "spill_bucket=3.parquet").exists()
    monkeypatch.setattr(build, "_member_writer", orig_writer)

    with pytest.raises(ValueError, match="another compression level"):
        build.build_court_opinion_texts(
            export, again, dump_date=date(2026, 9, 30), workers=0, convert=stand_in, spill_compression_level=12
        )

    def never(row):
        raise AssertionError("a resumed run converted again")

    with pytest.raises(ValueError, match="another export"):
        build.build_court_opinion_texts(export, again, dump_date=date(2026, 10, 1), workers=0, convert=never)

    resumed = build.build_court_opinion_texts(
        export,
        again,
        dump_date=date(2026, 9, 30),
        workers=0,
        convert=never,
        parse_workers=1,
        row_group_text_bytes=2_000,
    )
    assert resumed["resumed"] and not spill.exists()
    assert resumed["rows_written"] == run["rows_written"] and resumed["refused"] == run["refused"]
    for member in _members(out):
        rel = member.relative_to(out)
        assert pq.ParquetFile(again / rel).read().equals(pq.ParquetFile(member).read()), rel
    assert pq.read_table(again / f"{build.DATASET}.exceptions.parquet").equals(
        pq.read_table(out / f"{build.DATASET}.exceptions.parquet")
    )


def test_a_run_stopped_after_all_blocks_resumes_to_its_run_json(built, tmp_path, monkeypatch) -> None:
    out, export, run, _ = built
    again = tmp_path / "out"

    def rebuild() -> dict:
        return build.build_court_opinion_texts(
            export, again, dump_date=date(2026, 9, 30), workers=0, convert=stand_in, parse_workers=1,
            row_group_text_bytes=2_000,
        )

    rss = build._rss_bytes
    monkeypatch.setattr(build, "_rss_bytes", lambda: (_ for _ in ()).throw(RuntimeError("killed")))
    with pytest.raises(RuntimeError, match="killed"):
        rebuild()
    assert not (again / f"{build.DATASET}-run.json").exists()
    assert (again / f".{build.DATASET}-spill" / "_complete.json").exists()
    assert len(_members(again)) == 3
    monkeypatch.setattr(build, "_rss_bytes", rss)
    finished = rebuild()
    assert finished["resumed"] and finished["rows_written"] == run["rows_written"]
    assert json.loads((again / f"{build.DATASET}-run.json").read_text()) == json.loads(json.dumps(finished, default=str))
    with pytest.raises(FileExistsError, match="run.json"):
        rebuild()


def test_parallel_phase2_workers_and_precomputed_source_sha256(built, tmp_path, monkeypatch) -> None:
    out, export, _, _ = built
    digest = "sha256:" + hashlib.sha256(export.read_bytes()).hexdigest()
    monkeypatch.setattr(build, "_file_sha256", lambda _: (_ for _ in ()).throw(AssertionError("re-hashed export")))
    parallel_out = tmp_path / "parallel"
    run = build.build_court_opinion_texts(
        export,
        parallel_out,
        dump_date=date(2026, 9, 30),
        source_sha256=digest,
        workers=0,
        convert=stand_in,
        parse_workers=1,
        row_group_text_bytes=2_000,
        phase2_workers=2,
        spill_compression_level=12,
    )
    assert run["phase2_workers"] == 2 and run["spill_compression_level"] == 12
    assert run["source_sha256"] == digest
    for member in _members(out):
        rel = member.relative_to(out)
        assert pq.ParquetFile(parallel_out / rel).read().equals(pq.ParquetFile(member).read()), rel


def test_the_output_is_admitted_as_a_receipts_free_split_generation(built, tmp_path) -> None:
    """build_generation admits the receipts-free courts/2 split generation with zero-copy adoption."""
    out, _, _, _ = built
    root = tmp_path.resolve()
    staging = root / "staging"
    shutil.copytree(out / build.DATASET, staging / build.DATASET)
    gen_dir = root / "generation"
    artifact = build_generation(
        gen_dir,
        family=pipeline.FAMILY,
        files=[staging / build.DATASET],
        expected_keys=[pipeline.TABLE_KEY],
        partitioned={pipeline.TABLE_KEY: ["opinion_block"]},
        owned_output_root=root,
    )
    assert "etlReceipts" not in artifact.root["spec"]
    members = sorted(p.relative_to(gen_dir).as_posix() for p in gen_dir.rglob("part-*.parquet"))
    assert members == [m.relative_to(out).as_posix() for m in _members(out)]
    # Zero-copy adoption moved member files out of staging.
    assert not list((staging / build.DATASET).rglob("part-*.parquet"))


def test_pipeline_builds_generation_with_evidence_guards_and_dry_run_acceptance(built, tmp_path, monkeypatch) -> None:
    """End-to-end local-publication pipeline + publish-local-generation --dry-run --scratch-raw."""
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    _, export, _, rows = built
    raw_root = tmp_path / "raw"
    store = CentralRawArchiveStore(raw_root)
    raw_bytes = export.read_bytes()
    export_sha = "sha256:" + hashlib.sha256(raw_bytes).hexdigest()
    store.put_blob(export_sha, len(raw_bytes), [raw_bytes])

    # 1. Full build refuses when disk headroom < 75 GiB.
    with pytest.raises(RuntimeError, match="below the 75.0 GiB pre-flight floor"):
        pipeline.build_court_opinion_texts_generation(
            tmp_path / "low-disk",
            dump_date=date(2026, 9, 30),
            source_sha256=export_sha,
            archive_root=raw_root,
            workers=0,
            convert=stand_in,
            parse_workers=1,
            disk_usage=lambda _: SimpleNamespace(free=40 * 1024**3),
        )
    assert not (tmp_path / "low-disk").exists()

    # 2. Full build refuses when SPICYGOV_RAW_ROOT is set (unless scratch/canary).
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(raw_root))
    with pytest.raises(ValueError, match="Unset SPICYGOV_RAW_ROOT"):
        pipeline.build_court_opinion_texts_generation(
            tmp_path / "env-blocked",
            dump_date=date(2026, 9, 30),
            source_sha256=export_sha,
            disk_usage=lambda _: SimpleNamespace(free=100 * 1024**3),
        )
    monkeypatch.delenv("SPICYGOV_RAW_ROOT", raising=False)

    # 3. Full build with >= 75 GiB free succeeds, seals evidence (with exceptions + run.json), and passes dry-run.
    work_dir = tmp_path / "pipeline-out"
    result = pipeline.build_court_opinion_texts_generation(
        work_dir,
        dump_date=date(2026, 9, 30),
        source_sha256=export_sha,
        archive_root=raw_root,
        workers=0,
        convert=stand_in,
        parse_workers=1,
        row_group_text_bytes=2_000,
        phase2_workers=2,
        disk_usage=lambda _: SimpleNamespace(free=100 * 1024**3),
    )
    assert result.family == "court-opinion-texts"
    assert result.run["rows_read"] == len(rows) and result.run["rows_written"] == len(rows) - 1
    assert result.exceptions_path is not None and result.exceptions_path.is_file()
    journal_events = [json.loads(line) for line in (result.evidence_dir / "journal.jsonl").read_text().splitlines()]
    stages = {ev.get("stage") for ev in journal_events}
    assert {
        pipeline.EXPORT_STAGE,
        "courtlistener-opinion-texts-exceptions",
        "courtlistener-opinion-texts-run",
    } <= stages
    build_events = [ev for ev in journal_events if ev.get("event") == "courtlistener-opinion-texts-build"]
    assert len(build_events) == 1 and build_events[0]["rows_written"] == len(rows) - 1

    # Dry-run through publish-local-generation succeeds on the sealed generation + evidence.
    live_index = tmp_path / "publication.v2.json"
    live_index.write_text(json.dumps(publication.empty_index()))
    dry_run_dir = tmp_path / "dry-run-out"
    monkeypatch.setattr(shutil, "disk_usage", lambda _: SimpleNamespace(free=100 * 1024**3))
    local_publication.main(
        [
            "--generation",
            str(result.generation_dir),
            "--family",
            pipeline.FAMILY,
            "--evidence",
            str(result.evidence_dir),
            "--output",
            str(dry_run_dir),
            "--dry-run",
            "--scratch-raw",
            "--live-index",
            str(live_index),
        ]
    )
    receipt = json.loads((dry_run_dir / "dry-run.json").read_text())
    assert receipt["status"] == "dry_run_accepted"
    assert receipt["family"] == pipeline.FAMILY
    assert receipt["tables"][pipeline.TABLE_KEY]["rows"] == len(rows) - 1


def test_parquet_scan_hive_types_compatibility_and_fallbacks() -> None:
    urls = [
        "https://data.spicygov.ai/generations/court-opinion-texts/aaa/court_opinion_texts/opinion_block=0/part-000000.parquet",
        "https://data.spicygov.ai/generations/court-opinion-texts/aaa/court_opinion_texts/opinion_block=47/part-000000.parquet",
    ]
    assert "hive_partitioning = false" in publication.parquet_scan(urls)
    scan = publication.parquet_scan(urls, partition_types={"opinion_block": "VARCHAR"})
    assert "hive_partitioning = true, hive_types = {'opinion_block': 'VARCHAR'}" in scan

    multi = [
        "https://data.spicygov.ai/generations/house-disbursements/aaa/house_disbursements/report_year=2024/report_quarter=1/part-000000.parquet",
    ]
    multi_scan = publication.parquet_scan(multi, partition_types={"report_year": "INTEGER", "report_quarter": "INTEGER"})
    assert "hive_partitioning = true, hive_types = {'report_year': 'INTEGER', 'report_quarter': 'INTEGER'}" in multi_scan

    # Extraneous '=' in an ancestor directory falls back safely to hive_partitioning = false.
    tricky = ["/tmp/pytest[param=1]/court_opinion_texts/opinion_block=0/part-000000.parquet"]
    assert "hive_partitioning = false" in publication.parquet_scan(tricky, partition_types={"opinion_block": "VARCHAR"})
    # Flat paths without partition dirs fall back safely.
    flat = ["https://data.spicygov.ai/generations/x/aaa/court_opinion_texts.parquet"]
    assert "hive_partitioning = false" in publication.parquet_scan(flat, partition_types={"opinion_block": "VARCHAR"})
    # Hive null sentinels, percent-encoded values, and trailing '=' values fall back safely.
    for bad_val in ("NULL", "null", "__HIVE_DEFAULT_PARTITION__", "foo%20bar", "a=b"):
        bad_url = [f"https://data.spicygov.ai/generations/x/aaa/court_opinion_texts/opinion_block={bad_val}/part-000000.parquet"]
        assert "hive_partitioning = false" in publication.parquet_scan(
            bad_url, partition_types={"opinion_block": "VARCHAR"}
        ), bad_val


def test_mcp_parquet_scan_hive_prunes_split_table_over_http(tmp_path, monkeypatch) -> None:
    """Finding 2: mcp_server.py's parquet_scan view prunes 10 blocks to <= 2 files (<= 6 requests) on a point read."""
    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors

    # Build a real 10-block split generation including block 47 (opinion_id=7090690) and block 58 (opinion_id=8818687).
    root = tmp_path.resolve()
    src = root / "split-src" / build.DATASET
    export_sha = "sha256:" + "5" * 64
    blocks = (0, 1, 2, 3, 4, 5, 6, 7, 47, 58)
    for block in blocks:
        base_id = block * WIDTH
        if block == 47:
            ids = [str(base_id + 1), "7090690", str(base_id + 90_000)]
        elif block == 58:
            ids = [str(base_id + 1), "8818687", str(base_id + 120_000)]
        else:
            ids = [str(base_id + 1), str(base_id + 42), str(base_id + 99)]
        part_path = src / f"opinion_block={block}" / "part-000000.parquet"
        part_path.parent.mkdir(parents=True, exist_ok=True)
        table = pa.table(
            {
                "opinion_id": ids,
                "cluster_id": [str(int(i) + 10) for i in ids],
                "opinion_block": [str(block)] * 3,
                "date_modified": ["2026-09-30T00:00:00Z"] * 3,
                "text_source": ["html_with_citations"] * 3,
                "markdown": [f"Opinion body for {i}" for i in ids],
                "converter_version": ["003"] * 3,
                "source_column": ["html_with_citations"] * 3,
                "source_text_sha256": ["sha256:" + hashlib.sha256(f"raw-{i}".encode()).hexdigest() for i in ids],
                "dump_date": ["2026-09-30"] * 3,
                "source_sha256": [export_sha] * 3,
            },
            schema=SUBJECT_SCHEMAS[build.DATASET],
        )
        pq.write_table(table, part_path, compression="zstd", write_statistics=build.STATISTICS)

    gen_dir = root / "generation"
    artifact = build_generation(
        gen_dir,
        family=pipeline.FAMILY,
        files=[src],
        expected_keys=[pipeline.TABLE_KEY],
        partitioned={pipeline.TABLE_KEY: ["opinion_block"]},
        owned_output_root=root,
    )
    entry = publication.generation_entry(artifact, iter_member_descriptors(artifact, LocalMemberSource(gen_dir)))
    index = publication.empty_index()
    index["families"][pipeline.FAMILY] = entry

    # Serve the generation directory over local HTTP with proper byte-range support and request logging.
    prefix = entry["prefix"]
    requests_log: list[tuple[str, str]] = []
    lock = threading.Lock()

    class RangeHandler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):  # noqa: A002
            return

        def _resolve(self) -> Path | None:
            rel = self.path.lstrip("/")
            if not rel.startswith(prefix + "/"):
                return None
            candidate = gen_dir / rel.removeprefix(prefix + "/")
            return candidate if candidate.is_file() else None

        def do_HEAD(self) -> None:
            with lock:
                requests_log.append(("HEAD", self.path))
            target = self._resolve()
            if target is None:
                self.send_error(404)
                return
            size = target.stat().st_size
            self.send_response(200)
            self.send_header("Content-Length", str(size))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", f'"{size}"')
            self.send_header("Last-Modified", "Wed, 30 Sep 2026 00:00:00 GMT")
            self.end_headers()

        def do_GET(self) -> None:
            with lock:
                requests_log.append(("GET", self.path))
            target = self._resolve()
            if target is None:
                self.send_error(404)
                return
            data = target.read_bytes()
            total = len(data)
            range_header = self.headers.get("Range")
            if range_header and range_header.startswith("bytes="):
                spec = range_header.removeprefix("bytes=").split(",", 1)[0]
                start_s, end_s = spec.split("-", 1)
                start = int(start_s) if start_s else max(0, total - int(end_s))
                end = int(end_s) if end_s else total - 1
                end = min(end, total - 1)
                chunk = data[start : end + 1]
                self.send_response(206)
                self.send_header("Content-Range", f"bytes {start}-{end}/{total}")
                self.send_header("Content-Length", str(len(chunk)))
                self.send_header("Accept-Ranges", "bytes")
                self.send_header("ETag", f'"{total}"')
                self.send_header("Last-Modified", "Wed, 30 Sep 2026 00:00:00 GMT")
                self.end_headers()
                self.wfile.write(chunk)
                return
            self.send_response(200)
            self.send_header("Content-Length", str(total))
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("ETag", f'"{total}"')
            self.send_header("Last-Modified", "Wed, 30 Sep 2026 00:00:00 GMT")
            self.end_headers()
            self.wfile.write(data)

    httpd = ThreadingHTTPServer(("127.0.0.1", 0), RangeHandler)
    port = httpd.server_address[1]
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()

    monkeypatch.setattr(mcp_server, "DATA_DIR", None)
    monkeypatch.setattr(mcp_server, "R2_BASE_URL", f"http://127.0.0.1:{port}")
    monkeypatch.setattr(mcp_server, "TABLES", (build.DATASET,))
    monkeypatch.setattr(mcp_server, "_bind_archive", lambda _: (None, None))
    monkeypatch.setattr(mcp_server, "FEC_QUALIFIED_VIEWS", ())
    monkeypatch.setattr(mcp_server, "_read_publication", lambda: mcp_server._Publication(index, None, None))
    mcp_server._reset_connection_cache()
    try:
        # 1. Admit the MCP connection (view registration + schema check touches only 1 member file, not all 10).
        con = mcp_server._build_connection()
        try:
            admit_files = {p for _, p in requests_log if p.endswith(".parquet")}
            assert len(admit_files) == 1, f"View admission opened {len(admit_files)} files: {sorted(admit_files)}"
            # 2. Point read through the admitted MCP parquet_scan view: <= 2 files, <= 6 HTTP requests!
            requests_log.clear()
            rows = con.execute(
                "SELECT opinion_id, cluster_id, opinion_block, markdown, source_sha256 "
                "FROM court_opinion_texts "
                "WHERE opinion_block = '47' AND opinion_id = '7090690'"
            ).fetchall()
            assert rows == [("7090690", "7090700", "47", "Opinion body for 7090690", export_sha)]
            parquet_requests = [(m, p) for m, p in requests_log if p.endswith(".parquet")]
            touched_files = {p for _, p in parquet_requests}
            assert len(touched_files) <= 2, f"Expected <= 2 files, touched {sorted(touched_files)}"
            assert len(parquet_requests) <= 6, f"Expected <= 6 HTTP requests, saw {len(parquet_requests)}: {parquet_requests}"
            assert any("opinion_block=47/" in p for p in touched_files)
            assert not any(f"opinion_block={b}/" in p for b in (1, 2, 3, 4, 5, 6, 7, 58) for p in touched_files)
        finally:
            con.close()

        # 3. Also verify end-to-end through the MCP query_sql tool (cold + warm point read).
        requests_log.clear()
        mcp_server._reset_connection_cache()
        server = mcp_server.build_server()
        reply = _tool_data(
            server,
            "query_sql",
            {
                "sql": (
                    "SELECT opinion_id, cluster_id, opinion_block, markdown, source_sha256 "
                    "FROM court_opinion_texts "
                    "WHERE opinion_block = '47' AND opinion_id = '7090690'"
                )
            },
        )
        assert _records(reply) == [
            {
                "opinion_id": "7090690",
                "cluster_id": "7090700",
                "opinion_block": "47",
                "markdown": "Opinion body for 7090690",
                "source_sha256": export_sha,
            }
        ]
        cold_files = {p for _, p in requests_log if p.endswith(".parquet")}
        assert len(cold_files) <= 2, f"Cold MCP query_sql touched {sorted(cold_files)}"
        assert any("opinion_block=47/" in p for p in cold_files)

        requests_log.clear()
        reply2 = _tool_data(
            server,
            "query_sql",
            {
                "sql": (
                    "SELECT opinion_id, opinion_block, markdown "
                    "FROM court_opinion_texts "
                    "WHERE opinion_block = '58' AND opinion_id = '8818687'"
                )
            },
        )
        assert _records(reply2) == [
            {"opinion_id": "8818687", "opinion_block": "58", "markdown": "Opinion body for 8818687"}
        ]
        warm_requests = [(m, p) for m, p in requests_log if p.endswith(".parquet")]
        warm_files = {p for _, p in warm_requests}
        assert len(warm_files) <= 2 and len(warm_requests) <= 6, f"Warm MCP query_sql saw {warm_requests}"
        assert any("opinion_block=58/" in p for p in warm_files)
    finally:
        mcp_server._reset_connection_cache()
        httpd.shutdown()
        httpd.server_close()


def test_the_spill_schema_is_what_the_bulk_writer_maps() -> None:
    schema = pa.schema([f for f in build._SPILL_SCHEMA if f.name != build._BUCKET])
    assert schema.equals(SUBJECT_SCHEMAS[build.DATASET])
    assert court_bulk.eligible(build.DATASET, schema)
    assert not court_bulk.eligible(build.DATASET, schema.append(pa.field("html", pa.string())))


def test_byte_segments_bound_each_slice_except_a_single_oversized_row() -> None:
    table = pa.table({"markdown": ["a" * 5, "b" * 5, None, "c" * 20, "d" * 3]})
    slices = list(build.byte_segments(10)(table))
    assert [s.num_rows for s in slices] == [3, 1, 1]
    assert sum(s.num_rows for s in slices) == table.num_rows


def test_a_build_refuses_an_output_it_would_overwrite(built) -> None:
    out, _, _, _ = built
    with pytest.raises(FileExistsError):
        build.build_court_opinion_texts(Path("unused.csv.bz2"), out, dump_date=date(2026, 9, 30), convert=stand_in)


@pytest.mark.parametrize("bad_max_rows", [0, -5, False])
def test_max_rows_must_be_positive_integer(tmp_path, bad_max_rows) -> None:
    export = _export(tmp_path, [("1", "2", "2026-09-30", "<p>ok</p>", None, None)], name="one.csv.bz2")
    with pytest.raises(ValueError, match="max_rows must be a positive integer"):
        build.build_court_opinion_texts(
            export, tmp_path / "out", dump_date=date(2026, 9, 30), max_rows=bad_max_rows, workers=0, convert=stand_in
        )
    with pytest.raises(ValueError, match="max_rows must be a positive integer"):
        pipeline.build_court_opinion_texts_generation(
            tmp_path / "pipe-out",
            dump_date=date(2026, 9, 30),
            export_path=export,
            archive_root=tmp_path / "raw",
            max_rows=bad_max_rows,
            workers=0,
            convert=stand_in,
        )


def test_pipeline_refuses_mismatched_export_before_retain_and_canary_on_shared_raw(tmp_path, monkeypatch) -> None:
    from spicy_docs.storage.raw_archive import CentralRawArchiveStore

    export = _export(tmp_path, [("1", "2", "2026-09-30", "<p>ok</p>", None, None)], name="mismatch.csv.bz2")
    raw_root = tmp_path / "raw"
    CentralRawArchiveStore(raw_root)

    # 1. Mismatched --export-path digest is refused BEFORE retain_file touches CentralRawArchiveStore.
    wrong_sha = "sha256:" + "0" * 64
    with pytest.raises(ValueError, match="Export file digest .* differs from expected"):
        pipeline.build_court_opinion_texts_generation(
            tmp_path / "bad-digest",
            dump_date=date(2026, 9, 30),
            source_sha256=wrong_sha,
            export_path=export,
            archive_root=raw_root,
            workers=0,
            convert=stand_in,
            disk_usage=lambda _: SimpleNamespace(free=100 * 1024**3),
        )
    assert not list((raw_root / "objects").rglob("*")) if (raw_root / "objects").exists() else True
    assert not list((raw_root / "acquisitions").rglob("*")) if (raw_root / "acquisitions").exists() else True
    assert not (tmp_path / "bad-digest").exists()

    # 2. Bounded --max-rows canary refuses to run against the shared raw archive without --archive-root / SPICYGOV_RAW_ROOT.
    monkeypatch.delenv("SPICYGOV_RAW_ROOT", raising=False)
    with pytest.raises(ValueError, match="must pass --archive-root or set SPICYGOV_RAW_ROOT"):
        pipeline.build_court_opinion_texts_generation(
            tmp_path / "canary-shared-blocked",
            dump_date=date(2026, 9, 30),
            max_rows=10,
            workers=0,
            convert=stand_in,
        )
    assert not (tmp_path / "canary-shared-blocked").exists()


def test_write_block_slices_order_before_table_take(tmp_path, monkeypatch) -> None:
    """WARNING 2: _write_block slices ``order`` per row group before ``table.take`` so >2 GiB buckets never overflow."""
    bucket = tmp_path / "spill_bucket=0.parquet"
    export_sha = "sha256:" + "5" * 64
    # Write 12 out-of-order rows across 3 row groups (4 rows per group), 400 bytes of markdown each.
    ids = [12, 3, 9, 1, 11, 5, 7, 2, 10, 4, 8, 6]
    with pq.ParquetWriter(bucket, build.SCHEMA, compression="zstd") as writer:
        for start in range(0, len(ids), 4):
            chunk_ids = [str(i) for i in ids[start : start + 4]]
            tbl = pa.table(
                {
                    "opinion_id": chunk_ids,
                    "cluster_id": chunk_ids,
                    "opinion_block": ["0"] * 4,
                    "date_modified": ["2026-09-30"] * 4,
                    "text_source": ["html"] * 4,
                    "markdown": ["m" * 400] * 4,
                    "converter_version": ["003"] * 4,
                    "source_column": ["html"] * 4,
                    "source_text_sha256": [export_sha] * 4,
                    "dump_date": ["2026-09-30"] * 4,
                    "source_sha256": [export_sha] * 4,
                },
                schema=build.SCHEMA,
            )
            writer.write_table(tbl)

    take_lengths: list[int] = []
    real_read_table = pq.read_table

    class _TrackingTable:
        def __init__(self, inner: pa.Table):
            self._inner = inner

        def select(self, columns):
            return _TrackingTable(self._inner.select(columns))

        def take(self, indices):
            take_lengths.append(len(indices))
            return self._inner.take(indices)

        def __getitem__(self, key):
            return self._inner[key]

        def __getattr__(self, name):
            return getattr(self._inner, name)

    monkeypatch.setattr(build.pq, "read_table", lambda path: _TrackingTable(real_read_table(path)))
    table_dir = tmp_path / "out"
    written = build._write_block(0, [bucket], table_dir, row_group_text_bytes=1_000, compression_level=12)
    assert written == 12
    # Each row is 400 bytes, limit=1,000 -> 2 rows (800 B) per segment -> 6 chunked takes of 2 rows, never 12!
    assert take_lengths == [2, 2, 2, 2, 2, 2]
    assert not bucket.exists()
    out_path = table_dir / "opinion_block=0" / "part-000000.parquet"
    out_table = pq.ParquetFile(out_path).read()
    assert [int(v) for v in out_table["opinion_id"].to_pylist()] == list(range(1, 13))
    assert pq.ParquetFile(out_path).metadata.num_row_groups == 6
