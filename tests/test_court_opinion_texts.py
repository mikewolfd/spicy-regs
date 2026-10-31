"""court_opinion_texts: one pass over an opinions export, written id-sorted into id-block members with receipts.

The export fixture is written in the publisher's dialect with its records out of id order, across three opinion
blocks. The converter is a stand-in (the real one is SpicyDocs' and is tested there); what is tested here is the
build: order, member layout, row-group and footer bounds, receipts as pointers, refusals and the point read.
"""

from __future__ import annotations

import bz2
import json
import random
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


def test_court_opinions_at_courts_3_still_reads_its_courts_2_receipts(tmp_path) -> None:
    """date_modified moved from court_opinions receipts to court_opinion_texts (C21); a courts/2 bundle, which
    carries it, stays readable through the registered history rather than being relabelled."""
    from spicy_regs.court_receipts import POLICIES, read_court_rows
    from spicy_regs.court_subjects import normalize_court_row
    from spicy_regs.earlier_receipt_policies import earlier_policies
    from spicy_regs.etl_receipts import ReceiptContext, write_dataset

    current = POLICIES["court_opinions"]
    prior, original = earlier_policies(current)
    assert (current.policy_version, prior.policy_version, original.policy_version) == ("courts/3", "courts/2", "courts/1")
    assert "date_modified" not in current.receipt_fields and "date_modified" in prior.receipt_fields
    assert [f for f in prior.receipt_fields if f != "date_modified"] == list(current.receipt_fields)
    assert prior.subject_schema.equals(current.subject_schema) and prior.identity_fields == current.identity_fields
    assert "date_modified" in POLICIES[build.DATASET].subject_schema.names

    raw = dict(opinion_id="1", cluster_id="2", sha1="a" * 40, download_url="https://court.example/1.pdf",
               date_modified="2026-06-01T00:00:00Z", dump_date="2026-06-30")
    shaped = normalize_court_row("court_opinions", {k: v for k, v in raw.items() if k != "date_modified"})
    held = {name: raw["date_modified"] if name == "date_modified" else shaped.get(name) for name in prior.input_fields}
    context = ReceiptContext("courts-2", "row", "prior", [{"source_id": "opinions", "body_version": "courts-2"}])
    subject, receipt = write_dataset([(held, context)], tmp_path / "old", prior)
    assert subject is not None
    [row] = read_court_rows(subject, dataset="court_opinions", receipt_path=receipt, generation_id="courts-2")
    assert row["opinion_id"] == "1" and row["date_modified"] == "2026-06-01T00:00:00Z"
