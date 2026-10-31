"""court_opinion_texts: one pass over an opinions export, written id-sorted into id-block members with receipts.

The export fixture is written in the publisher's dialect with its records out of id order, across three opinion
blocks. The converter is a stand-in (the real one is SpicyDocs' and is tested there); what is tested here is the
build: order, member layout, row-group and footer bounds, receipts as pointers, refusals, resume and the point read
routed through the publication index.
"""

from __future__ import annotations

import bz2
import hashlib
import json
import random
import re
import shutil
from datetime import date
from pathlib import Path
from typing import NamedTuple

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import court_bulk
from spicy_regs.transforms import build_court_opinion_texts as build

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


def _export(tmp_path: Path, rows: list[tuple]) -> Path:
    path = tmp_path / "opinions-2026-06-30.csv.bz2"
    lines = [",".join(f'"{name}"' for name in HEADER)] + [",".join(_quoted(v) for v in row) for row in rows]
    path.write_bytes(bz2.compress(("\n".join(lines) + "\n").encode()))
    return path


def _rows() -> list[tuple]:
    ids = [*range(5, 40), *range(WIDTH + 1, WIDTH + 30), *range(2 * WIDTH + 7, 2 * WIDTH + 20)]
    random.Random(7).shuffle(ids)
    rows = []
    for n in ids:
        text = "x" * (200 + n % 50)
        if n % 17 == 0:  # a control character the SQL encoding spells differently: decided by the row function
            text += "\x1b"
        if n % 11 == 0:
            rows.append((str(n), str(n * 3), "2026-06-01T00:00:00Z", "", text, None))
        elif n % 13 == 0:
            rows.append((str(n), str(n * 3), "2026-06-01T00:00:00Z", None, None, text))
        elif n == WIDTH + 2:
            rows.append((str(n), str(n * 3), "2026-06-01T00:00:00Z", None, None, None))
        elif n == 2 * WIDTH + 9:
            rows.append((str(n), str(n * 3), "2026-06-01T00:00:00Z", "<p>UNCONVERTIBLE</p>", None, None))
        else:
            rows.append((str(n), str(n * 3), "2026-06-01T00:00:00Z", f'<p data-x="1">{text}</p>', None, None))
    return rows


@pytest.fixture(scope="module")
def built(tmp_path_factory) -> tuple[Path, dict, list[tuple]]:
    tmp = tmp_path_factory.mktemp("texts")
    rows = _rows()
    run = build.build_court_opinion_texts(
        _export(tmp, rows), tmp / "out", dump_date=date(2026, 6, 30), workers=0, convert=stand_in,
        parse_workers=1, row_group_text_bytes=2_000,
    )
    return tmp / "out", run, rows


def _members(out: Path) -> list[Path]:
    return sorted((out / build.DATASET).glob("opinion_block=*/part-*.parquet"),
                  key=lambda p: int(p.parent.name.partition("=")[2]))


def test_members_are_id_blocks_sorted_numerically_and_disjoint(built) -> None:
    out, _, rows = built
    members = _members(out)
    assert [m.parent.name for m in members] == ["opinion_block=0", "opinion_block=1", "opinion_block=2"]
    previous = -1
    for member in members:
        table = pq.ParquetFile(member).read()
        ids = [int(v) for v in table["opinion_id"].to_pylist()]
        assert ids == sorted(ids) and ids[0] > previous
        assert {int(v) // WIDTH for v in ids} == {int(member.parent.name.partition("=")[2])}
        assert set(table["opinion_block"].to_pylist()) == {member.parent.name.partition("=")[2]}
        previous = ids[-1]
    written = sum(pq.ParquetFile(m).metadata.num_rows for m in members)
    assert written == len(rows) - 1  # the refused record has a receipt, not a row


def test_row_groups_compression_and_statistics_follow_the_layout(built) -> None:
    out, _, _ = built
    for member in _members(out):
        meta = pq.ParquetFile(member).metadata
        markdown = meta.schema.to_arrow_schema().get_field_index("markdown")
        assert meta.num_row_groups > 1
        for g in range(meta.num_row_groups):
            group = meta.row_group(g)
            column = group.column(markdown)
            assert column.compression == "ZSTD"
            assert column.statistics is None or not column.statistics.has_min_max
            assert group.column(0).statistics.has_min_max  # opinion_id
            text = pq.ParquetFile(member).read_row_group(g, columns=["markdown"])["markdown"]
            sizes = [len(v.encode()) for v in text.to_pylist() if v]
            assert sum(sizes) <= 2_000 or len(sizes) == 1


def test_every_record_is_counted_once_and_no_text_is_null_markdown(built) -> None:
    out, run, rows = built
    table = duckdb.sql(f"SELECT * FROM read_parquet('{out / build.DATASET}/*/*.parquet')").to_arrow_table()
    by_source = {s: table["text_source"].to_pylist().count(s) for s in set(table["text_source"].to_pylist())}
    assert run["rows_read"] == len(rows) == table.num_rows + run["refused"]
    assert run["refused"] == 1 and run["status"] == "complete" and run["completed_read"]
    assert run["rows_routed_to_row_function"] == sum(1 for r in rows if int(r[0]) % 17 == 0) > 0
    assert run["rows_by_text_source"] == by_source
    assert sum(by_source.values()) + run["refused"] == run["rows_read"]
    assert by_source["none"] == 1 and set(by_source) == {"html_with_citations", "html", "plain_text", "none"}
    assert [r["markdown"] for r in table.to_pylist() if r["text_source"] == "none"] == [None]


def test_receipts_point_at_the_rendering_and_never_copy_it(built) -> None:
    out, _, rows = built
    receipts = pq.read_table(out / "etl_receipts.parquet").to_pylist()
    accepted = [r for r in receipts if r["outcome"] == "accepted"]
    refused = [r for r in receipts if r["outcome"] == "refused"]
    assert len(accepted) == len(rows) - 1 and len(refused) == 1
    assert receipts[-1]["outcome"] == "refused"  # refusals follow every accepted receipt
    witness = accepted[0]["witnesses"][0]
    assert witness["source_id"] == "opinions-2026-06-30.csv.bz2" and witness["sha256"].startswith("sha256:")
    for receipt in receipts:
        assert "xxxxxxxx" not in receipt["processing_json"]  # no rendering text in the receipt
    processing = json.loads(refused[0]["processing_json"])
    assert "UNCONVERTIBLE" not in refused[0]["processing_json"]
    assert processing and json.dumps(processing).count("sha256:") >= 1
    # Each pointer is the digest of the rendering the converter was given (none for a record without text).
    columns = ("html_with_citations", "html", "plain_text")
    rendered = {}
    for n, _, _, *renderings in rows:
        column = next((c for c, v in zip(columns, renderings) if v and v.strip()), None)
        value = renderings[columns.index(column)] if column else None
        rendered[n] = (column, None if value is None else "sha256:" + hashlib.sha256(value.encode()).hexdigest())

    def pointer(receipt: dict) -> tuple[str, tuple[str | None, str | None]]:
        """The four parts: the witness (above), the opinion id, the source column and its value's digest."""
        opinion_id = re.search(r'"opinion_id"\],\["str","(\d+)"', receipt["identity_json"])
        column = re.search(r'"source_column",\["str","(\w+)"', receipt["processing_json"])
        digest = re.search(r'"source_sha256",\["str","(sha256:[0-9a-f]{64})"', receipt["processing_json"])
        assert opinion_id is not None
        return opinion_id.group(1), (column and column.group(1), digest and digest.group(1))

    assert dict(map(pointer, accepted)) == {n: d for n, d in rendered.items() if n != str(2 * WIDTH + 9)}
    assert pointer(accepted[0])[1][1] is not None and rendered[str(WIDTH + 2)] == (None, None)
    assert '"source_column",["null",null]' in next(r["processing_json"] for r in accepted
                                                    if pointer(r)[0] == str(WIDTH + 2))
    refusal = refused[0]["processing_json"]  # no identity: the record's id and pointer ride in its raw fields
    assert f'"{2 * WIDTH + 9}"' in refusal and rendered[str(2 * WIDTH + 9)][1] in refusal
    assert '"source_column",["str","html_with_citations"]' in refusal


def test_the_spill_is_one_file_per_bucket_and_refusals_are_counted_by_rendering(built) -> None:
    _, run, _ = built
    assert run["spill_files"] == run["spill_row_groups"] == 3  # 77 rows in three buckets, none reopened
    assert run["refused_by_text_source"] == {"html_with_citations": 1} and not run["resumed"]
    assert run["max_rss_main_process_bytes"] > 1 << 20


def test_a_failed_phase_2_resumes_at_the_first_uncommitted_block(built, tmp_path, monkeypatch) -> None:
    out, run, _ = built
    export = out.parent / "opinions-2026-06-30.csv.bz2"  # the same bytes and path, so the same witness
    calls = {"n": 0}
    write = build._write_bulk_pair

    def failing(*args, **kwargs):
        calls["n"] += 1
        if calls["n"] == 2:  # the second block's only bucket
            raise RuntimeError("interrupted")
        return write(*args, **kwargs)

    monkeypatch.setattr(build, "_write_bulk_pair", failing)
    again = tmp_path / "out"
    with pytest.raises(RuntimeError, match="interrupted"):
        build.build_court_opinion_texts(export, again, dump_date=date(2026, 6, 30), workers=0, convert=stand_in,
                                        parse_workers=1, row_group_text_bytes=2_000, generation_id=run["generation_id"])
    spill = again / f".{build.DATASET}-spill"
    assert (spill / "_complete.json").exists() and not (again / "etl_receipts.parquet").exists()
    assert [p.name for p in (spill / "_receipts").glob("*.parquet")] == ["0.parquet"]  # block 1 uncommitted
    monkeypatch.setattr(build, "_write_bulk_pair", write)

    def never(row):
        raise AssertionError("a resumed run converted again")

    with pytest.raises(ValueError, match="another export"):
        build.build_court_opinion_texts(export, again, dump_date=date(2026, 7, 1), workers=0, convert=never)
    resumed = build.build_court_opinion_texts(export, again, dump_date=date(2026, 6, 30), workers=0, convert=never,
                                              parse_workers=1, row_group_text_bytes=2_000)
    assert resumed["resumed"] and resumed["generation_id"] == run["generation_id"] and not spill.exists()
    # Block 0's row-function decisions were made by the failed run; the resumed total still counts them.
    assert resumed["rows_routed_to_row_function"] == run["rows_routed_to_row_function"]
    for name in ("etl_receipts.parquet", *(m.relative_to(out).as_posix() for m in _members(out))):
        assert pq.ParquetFile(again / name).read().equals(pq.ParquetFile(out / name).read()), name


def test_a_run_stopped_after_the_receipts_join_resumes_to_its_run_receipt(built, tmp_path, monkeypatch) -> None:
    """The receipts are in place but the run receipt is not: a rerun finishes it rather than refusing the directory."""
    out, run, _ = built
    export = out.parent / "opinions-2026-06-30.csv.bz2"
    again = tmp_path / "out"

    def rebuild() -> dict:
        return build.build_court_opinion_texts(export, again, dump_date=date(2026, 6, 30), workers=0, convert=stand_in,
                                               parse_workers=1, row_group_text_bytes=2_000)

    rss = build._rss_bytes
    monkeypatch.setattr(build, "_rss_bytes", lambda: (_ for _ in ()).throw(RuntimeError("killed")))
    with pytest.raises(RuntimeError, match="killed"):
        rebuild()
    assert (again / "etl_receipts.parquet").exists() and not (again / f"{build.DATASET}-run.json").exists()
    assert (again / f".{build.DATASET}-spill" / "_complete.json").exists()  # the resume state outlives the join
    monkeypatch.setattr(build, "_rss_bytes", rss)
    finished = rebuild()
    assert finished["resumed"] and json.loads((again / f"{build.DATASET}-run.json").read_text()) == json.loads(
        json.dumps(finished, default=str))
    assert finished["rows_routed_to_row_function"] == run["rows_routed_to_row_function"]
    assert pq.read_table(again / "etl_receipts.parquet").num_rows == pq.read_table(out / "etl_receipts.parquet").num_rows
    with pytest.raises(FileExistsError, match="run.json"):
        rebuild()


def test_the_output_is_admitted_as_a_split_generation(built, tmp_path) -> None:
    """build_generation, with the full publication admission, accepts the members and receipts as written."""
    from spicy_regs.court_receipts import POLICIES
    from spicy_regs.generations import build_generation

    out, run, _ = built
    copy = tmp_path / "copy"
    shutil.copytree(out / build.DATASET, copy / build.DATASET)
    build_generation(tmp_path / "generation", family=build.DATASET, files=[copy / build.DATASET],
                     expected_keys=[f"{build.DATASET}.parquet"], partitioned={f"{build.DATASET}.parquet": ["opinion_block"]},
                     receipt_path=out / "etl_receipts.parquet", receipt_policies=[POLICIES[build.DATASET]],
                     receipt_generation_id=run["generation_id"])
    members = sorted(p.relative_to(tmp_path / "generation").as_posix()
                     for p in (tmp_path / "generation").rglob("part-*.parquet"))
    assert members == [m.relative_to(out).as_posix() for m in _members(out)]
    listed = json.loads((tmp_path / "generation" / "members.json").read_text())["members"]
    assert sorted(m["objectKey"] for m in listed if m["objectKey"] != "etl_receipts.parquet") == members


def test_a_point_read_is_routed_by_the_published_index_to_one_member(built, tmp_path) -> None:
    """The publication index lists each member with its opinion_block: the id-range manifest a point read uses."""
    from rulespec_artifacts import LocalMemberSource, iter_member_descriptors

    from spicy_regs.court_receipts import POLICIES
    from spicy_regs.generations import build_generation
    from spicy_regs.sources import publication

    out, run, _ = built
    copy = tmp_path / "copy"
    shutil.copytree(out / build.DATASET, copy / build.DATASET)
    artifact = build_generation(tmp_path / "generation", family=build.DATASET, files=[copy / build.DATASET],
                                expected_keys=[f"{build.DATASET}.parquet"],
                                partitioned={f"{build.DATASET}.parquet": ["opinion_block"]},
                                receipt_path=out / "etl_receipts.parquet", receipt_policies=[POLICIES[build.DATASET]],
                                receipt_generation_id=run["generation_id"])
    entry = publication.generation_entry(artifact, iter_member_descriptors(artifact, LocalMemberSource(tmp_path / "generation")))
    published = entry["tables"][f"{build.DATASET}.parquet"]["members"]
    for member in published:  # each member holds exactly its block's id range
        ids = [int(v) for v in pq.ParquetFile(tmp_path / "generation" / member["key"]).read(columns=["opinion_id"])["opinion_id"].to_pylist()]
        block = int(member["partition"]["opinion_block"])
        assert block * WIDTH <= min(ids) and max(ids) < (block + 1) * WIDTH and member["rows"] == len(ids)
    target = 2 * WIDTH + 12
    (member,) = [m for m in published if m["partition"] == {"opinion_block": str(target // WIDTH)}]
    row = duckdb.sql(f"SELECT opinion_id, markdown FROM read_parquet('{tmp_path / 'generation' / member['key']}') "
                     f"WHERE opinion_id = '{target}'").fetchall()
    assert [r[0] for r in row] == [str(target)] and row[0][1].startswith("md:")


def test_the_spill_schema_is_what_the_bulk_writer_maps() -> None:
    schema = pa.schema([f for f in build._SPILL_SCHEMA if f.name != build._BUCKET])
    assert court_bulk.eligible(build.DATASET, schema)
    assert not court_bulk.eligible(build.DATASET, schema.append(pa.field("html", pa.string())))


def test_a_point_read_by_id_and_block_opens_one_member_and_joins_its_cluster(built, tmp_path) -> None:
    out, _, _ = built
    target = WIDTH + 5
    clusters = tmp_path / "court_opinion_clusters.parquet"
    pq.write_table(pa.table({"cluster_id": [str(target * 3)], "court_is_federal": [True]}), clusters)
    profile = tmp_path / "profile.json"
    con = duckdb.connect()
    con.execute("PRAGMA enable_profiling='json'")
    con.execute(f"PRAGMA profiling_output='{profile}'")
    row = con.execute(
        f"""SELECT t.opinion_id, t.markdown, c.court_is_federal
            FROM read_parquet('{out / build.DATASET}/*/*.parquet') t JOIN read_parquet('{clusters}') c USING (cluster_id)
            WHERE t.opinion_block = {target // WIDTH} AND t.opinion_id = '{target}'"""
    ).fetchall()
    assert row == [(str(target), row[0][1], True)] and row[0][1].startswith("md:")
    files = []

    def walk(node):
        info = node.get("extra_info") or {}
        if "Total Files Read" in info and str(out) in info.get("Filename(s)", ""):
            files.append(int(info["Total Files Read"]))
        for child in node.get("children", []):
            walk(child)

    walk(json.loads(profile.read_text()))
    assert files == [1]


def test_byte_segments_bound_each_slice_except_a_single_oversized_row() -> None:
    table = pa.table({"markdown": ["a" * 5, "b" * 5, None, "c" * 20, "d" * 3]})
    slices = list(build.byte_segments(10)(table))
    assert [s.num_rows for s in slices] == [3, 1, 1]
    assert sum(s.num_rows for s in slices) == table.num_rows


def test_a_build_refuses_an_output_it_would_overwrite(built) -> None:
    out, _, _ = built
    with pytest.raises(FileExistsError):
        build.build_court_opinion_texts(Path("unused.csv.bz2"), out, dump_date=date(2026, 6, 30), convert=stand_in)
