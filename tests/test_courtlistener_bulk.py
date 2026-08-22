"""Hermetic tests for the CourtListener bulk-dump ingest (no network).

Covers the pieces with real logic: parsing the publisher's S3 listing into
dataset/date pairs (which is how coverage gets checked at all), the streaming
bzip2 CSV reader and its two bounds, the raw-row → published-schema mappings for
both new tables, and the disk-headroom guard that is supposed to refuse a
backfill rather than fill the volume.
"""

from __future__ import annotations

import bz2
from datetime import date
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from spicy_regs.sources.courtlistener_bulk import (
    BulkObject,
    CourtListenerBulkReader,
    find_dump,
    latest_dump_date,
)
from spicy_regs.transforms.build_court_opinion_bodies import (
    COLUMNS as BODY_COLUMNS,
)
from spicy_regs.transforms.build_court_opinion_bodies import (
    DISK_HEADROOM_FLOOR,
    check_headroom,
)
from spicy_regs.transforms.build_court_opinion_bodies import _shape as shape_body
from spicy_regs.transforms.build_court_opinion_clusters import (
    COLUMNS as CLUSTER_COLUMNS,
)
from spicy_regs.transforms.build_court_opinion_clusters import (
    _shape_bulk,
    _shape_search,
)

# One real row from the 2026-06-30 opinions dump's column set, trimmed to the
# fields the transform reads. `html_lawbox` is populated while `plain_text` is
# empty — the exact case that makes "no text" and "no plain_text" different
# questions.
_RAW_OPINION = {
    "id": "11422346",
    "cluster_id": "10954746",
    "type": "010combined",
    "author_str": "Amit P. Mehta",
    "author_id": "3227",
    "joined_by_str": "",
    "per_curiam": "f",
    "sha1": "f1b0a6170c709af85ed4f18f1ff2d7cbebcc697d",
    "page_count": "17",
    "download_url": "https://ecf.dcd.uscourts.gov/cgi-bin/show_public_doc?2025cv3854-20",
    "local_path": "pdf/2026/08/21/zeevi_v._united_states_department_of_state.pdf",
    "extracted_by_ocr": "f",
    "plain_text": "",
    "html": "",
    "html_lawbox": "<p>MEMORANDUM OPINION</p>",
    "html_with_citations": "",
    "date_created": "2026-08-21T14:02:11.000Z",
    "date_modified": "2026-08-21T14:02:11.000Z",
}

_RAW_CLUSTER = {
    "id": "10954746",
    "docket_id": "73299709",
    "case_name": "Zeevi v. United States Department of State",
    "case_name_short": "Zeevi",
    "case_name_full": "Yoav ZEEVI v. UNITED STATES DEPARTMENT OF STATE",
    "date_filed": "2026-08-21",
    "date_filed_is_approximate": "f",
    "judges": "Amit P. Mehta",
    "precedential_status": "Published",
    "citation_count": "0",
    "slug": "zeevi-v-united-states-department-of-state",
    "date_created": "2026-08-21T14:02:11.000Z",
}


def _csv_bz2(tmp_path: Path, name: str, header: str, rows: list[str]) -> Path:
    """Write a bzip2 CSV the reader can stream, exactly as the dumps are shaped."""
    path = tmp_path / name
    body = "\n".join([header, *rows]) + "\n"
    path.write_bytes(bz2.compress(body.encode("utf-8")))
    return path


# -- listing / enumeration ---------------------------------------------------


def test_bulk_object_splits_dataset_from_dump_date():
    """Coverage is checked per dataset per dump, so both must parse out of the key."""
    obj = BulkObject("bulk-data/opinion-clusters-2026-06-30.csv.bz2", 2_457_231_057, "2026-06-30T04:11:47.000Z")
    assert obj.dataset == "opinion-clusters"
    assert obj.dump_date == date(2026, 6, 30)
    assert obj.filename == "opinion-clusters-2026-06-30.csv.bz2"
    assert obj.url.endswith("/bulk-data/opinion-clusters-2026-06-30.csv.bz2")

    # The bucket also holds undated one-off exports; those must not masquerade
    # as a dated dump of some dataset.
    undated = BulkObject("bulk-data/scotus_network.csv", 7_000, "2024-04-04T00:00:00.000Z")
    assert undated.dump_date is None
    assert undated.dataset == "scotus_network"


def test_latest_dump_date_and_find_dump_pick_one_published_object():
    objects = [
        BulkObject("bulk-data/opinions-2026-03-31.csv.bz2", 54_190_000_000, ""),
        BulkObject("bulk-data/opinions-2026-06-30.csv.bz2", 54_561_543_156, ""),
        BulkObject("bulk-data/courts-2026-06-30.csv.bz2", 81_180, ""),
    ]
    assert latest_dump_date(objects, "opinions") == date(2026, 6, 30)
    assert latest_dump_date(objects, "nonexistent") is None

    found = find_dump(objects, "opinions", date(2026, 6, 30))
    assert found is not None and found.size == 54_561_543_156
    assert find_dump(objects, "opinions", date(2020, 1, 1)) is None


# -- streaming reader --------------------------------------------------------


def test_reader_streams_rows_and_normalizes_blanks(tmp_path: Path):
    path = _csv_bz2(
        tmp_path,
        "courts-2026-06-30.csv.bz2",
        "id,short_name,jurisdiction,notes",
        ["ca9,Ninth Circuit,F,", "scotus,Supreme Court,F,seat of last resort"],
    )
    rows = list(CourtListenerBulkReader("courts", local_file=path).iter_records())
    assert [r["id"] for r in rows] == ["ca9", "scotus"]
    # An empty CSV field is absence, not the empty string — downstream NULL
    # handling depends on that being decided here rather than per-transform.
    assert rows[0]["notes"] is None
    assert rows[1]["notes"] == "seat of last resort"


def test_reader_honors_the_record_bound_and_reports_it(tmp_path: Path):
    """A bounded run must be *recorded* as bounded, so coverage is not overclaimed."""
    path = _csv_bz2(
        tmp_path,
        "opinions-2026-06-30.csv.bz2",
        "id,cluster_id",
        [f"{i},{i * 10}" for i in range(1, 51)],
    )
    reader = CourtListenerBulkReader("opinions", local_file=path, max_records=7)
    rows = list(reader.iter_records())
    assert len(rows) == 7
    assert reader.rows_yielded == 7
    assert reader.stopped_early is True

    unbounded = CourtListenerBulkReader("opinions", local_file=path)
    assert len(list(unbounded.iter_records())) == 50
    assert unbounded.stopped_early is False


def test_reader_applies_the_row_filter_before_materializing(tmp_path: Path):
    """Filtering is what makes a targeted pass over a 50 GiB dump affordable."""
    path = _csv_bz2(
        tmp_path,
        "opinions-2026-06-30.csv.bz2",
        "id,cluster_id",
        ["1,100", "2,200", "3,100"],
    )
    wanted = {"100"}
    reader = CourtListenerBulkReader(
        "opinions", local_file=path, row_filter=lambda row: (row.get("cluster_id") or "") in wanted
    )
    rows = list(reader.iter_records())
    assert [r["id"] for r in rows] == ["1", "3"]
    assert reader.rows_scanned == 3
    assert reader.rows_yielded == 2


def test_reader_reads_the_dumps_backslash_escaped_quotes(tmp_path: Path):
    """The dumps escape an embedded quote as ``\\"``, not as the doubled ``""``.

    Read with the stdlib default dialect this does not raise — it *desyncs*, and
    the prose after the escaped quote becomes the next record's first column.
    Measured on the real 2026-06-30 opinion-clusters dump that corrupted 1,987 of
    the first 3,000 rows and dropped ``docket_id`` on two thirds of them, which
    would have silently destroyed the docket join. A regression here is a data
    corruption, not a parse error, so it is pinned.
    """
    path = _csv_bz2(
        tmp_path,
        "opinion-clusters-2026-06-30.csv.bz2",
        "id,case_name,docket_id",
        [r'"7290305","Ex parte \"Doe\", Inc.","64278691"', '"7290306","Plain v. Simple","64278692"'],
    )
    rows = list(CourtListenerBulkReader("opinion-clusters", local_file=path).iter_records())
    assert [r["id"] for r in rows] == ["7290305", "7290306"]
    assert rows[0]["case_name"] == 'Ex parte "Doe", Inc.'
    # The row after the escaped quote must still be a row, with its join key intact.
    assert rows[0]["docket_id"] == "64278691"
    assert rows[1]["docket_id"] == "64278692"


def test_reader_handles_embedded_newlines_in_opinion_text(tmp_path: Path):
    """Opinion bodies contain newlines inside quoted fields; a naive line split loses rows."""
    path = _csv_bz2(
        tmp_path,
        "opinions-2026-06-30.csv.bz2",
        "id,plain_text",
        ['1,"line one\nline two"', "2,short"],
    )
    rows = list(CourtListenerBulkReader("opinions", local_file=path).iter_records())
    assert len(rows) == 2
    assert rows[0]["plain_text"] == "line one\nline two"


# -- shaping -----------------------------------------------------------------


def test_body_shape_matches_the_published_schema_and_records_text_provenance():
    row = shape_body(_RAW_OPINION, dump_date=date(2026, 6, 30))
    assert set(row) == set(BODY_COLUMNS)
    assert row["opinion_id"] == "11422346"
    assert row["cluster_id"] == "10954746"
    assert row["dump_date"] == "2026-06-30"

    # plain_text is empty upstream, so it stays NULL rather than becoming "".
    assert row["plain_text"] is None
    assert row["html_with_citations"] is None
    # ...but the row is not textless, and the table must say which rendering exists.
    assert row["available_text_fields"] == "html_lawbox"
    assert row["text_char_count"] == str(len("<p>MEMORANDUM OPINION</p>"))


def test_body_shape_reports_a_genuinely_textless_opinion_as_zero():
    bare = {**_RAW_OPINION, "html_lawbox": ""}
    row = shape_body(bare, dump_date=None)
    assert row["available_text_fields"] is None
    assert row["text_char_count"] == "0"
    assert row["dump_date"] is None


def test_cluster_shape_matches_schema_and_renames_the_docket_join_key():
    row = _shape_bulk(_RAW_CLUSTER)
    assert set(row) == set(CLUSTER_COLUMNS)
    assert row["cluster_id"] == "10954746"
    # The dump calls it docket_id; court_dockets calls it cl_docket_id. The join
    # only reads the same on both sides if the rename happens here.
    assert row["cl_docket_id"] == "73299709"
    assert row["absolute_url"] == (
        "https://www.courtlistener.com/opinion/10954746/"
        "zeevi-v-united-states-department-of-state/"
    )
    assert row["ingest_source"] == "bulk"


def test_search_shape_leaves_dump_only_columns_null():
    """The catch-up API is narrower than the dump; absent prose must stay absent."""
    result = {
        "cluster_id": "10954746",
        "docket_id": "73299709",
        "caseName": "Zeevi v. United States Department of State",
        "dateFiled": "2026-08-21",
        "status": "Published",
        "absolute_url": "/opinion/10954746/zeevi/",
    }
    row = _shape_search(result)
    assert set(row) == set(CLUSTER_COLUMNS)
    assert row["cl_docket_id"] == "73299709"
    assert row["ingest_source"] == "search"
    assert row["absolute_url"] == "https://www.courtlistener.com/opinion/10954746/zeevi/"
    # Not supplied by /search/ — and not fabricated.
    assert row["syllabus"] is None
    assert row["headnotes"] is None
    assert row["headmatter"] is None


# -- the disk guard ----------------------------------------------------------


def test_check_headroom_refuses_an_ingest_that_would_cross_the_floor(tmp_path: Path, monkeypatch):
    """The floor exists so a backfill fails loudly instead of filling the volume."""
    import shutil as shutil_module

    free = DISK_HEADROOM_FLOOR + 10 * 2**30

    def fake_usage(_path):
        return shutil_module._ntuple_diskusage(total=free * 4, used=free * 3, free=free)

    # The transform calls ``shutil.disk_usage`` through the stdlib module, so
    # patching it there covers the real call site.
    monkeypatch.setattr(shutil_module, "disk_usage", fake_usage)

    # 5 GiB still leaves 5 GiB above the floor.
    check_headroom(5 * 2**30, path=tmp_path)

    # The real 2026-06-30 opinions dump does not fit, and must say so.
    with pytest.raises(RuntimeError, match="below the 100 GiB floor"):
        check_headroom(54_561_543_156, path=tmp_path)


# -- first build promotes rather than merges ---------------------------------


def test_first_build_promotes_the_staged_table_without_merging(tmp_path: Path, monkeypatch):
    """With no prior table the merge is a sort the machine cannot always afford.

    One dump, whose id column is the publisher's primary key, so the dedup can
    remove nothing. Running the COPY anyway held the staged and merged copies on
    disk at once and — on 250,000 rows carrying kilobytes of opinion text each —
    ran duckdb's memory budget out entirely. The first build promotes instead.
    """
    from spicy_regs.sources import r2
    from spicy_regs.transforms.build_court_opinion_bodies import build_court_opinion_bodies

    monkeypatch.setattr(r2, "download", lambda *_: False)
    dump = _csv_bz2(
        tmp_path,
        "opinions-2026-06-30.csv.bz2",
        "id,cluster_id,type,plain_text,html_with_citations",
        ['"11","101","010combined","body one",""', '"12","102","040dissent","","<p>body two</p>"'],
    )
    out = build_court_opinion_bodies(tmp_path, dump_date=date(2026, 6, 30), local_file=dump)

    stored = pq.read_table(out).to_pylist()
    assert [r["opinion_id"] for r in stored] == ["11", "12"]
    assert set(stored[0]) == set(BODY_COLUMNS)
    assert stored[0]["plain_text"] == "body one"
    assert stored[1]["html_with_citations"] == "<p>body two</p>"
    assert stored[1]["available_text_fields"] == "html_with_citations"
    # The staged file must not survive as a second copy of the same rows.
    assert not (tmp_path / "_bodies_new.parquet").exists()
