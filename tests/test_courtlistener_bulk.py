"""Hermetic tests for the CourtListener bulk-dump ingest (no network).

Covers the pieces with real logic: parsing the publisher's S3 listing into
dataset/date pairs (which is how coverage gets checked at all), the streaming
bzip2 CSV reader and its two bounds, the raw-row → published-schema mapping for
clusters, and the disk-headroom guard that is supposed to refuse a
backfill rather than fill the volume.
"""

from __future__ import annotations

import bz2
from datetime import date
from pathlib import Path

import pytest

from spicy_docs.sources.courtlistener.bulk import (
    BulkObject,
    CourtListenerBulkReader,
    find_dump,
    latest_dump_date,
)
from spicy_regs.transforms._courtlistener_writer import DISK_HEADROOM_FLOOR, check_headroom
from spicy_regs.transforms.build_court_opinion_clusters import (
    COLUMNS as CLUSTER_COLUMNS,
)
from spicy_regs.transforms.build_court_opinion_clusters import (
    _shape_bulk,
    _shape_search,
)

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
    obj = BulkObject(
        "bulk-data/opinion-clusters-2026-06-30.csv.bz2", 2_457_231_057, '"fixture-etag"', "2026-06-30T04:11:47.000Z"
    )
    assert obj.dataset == "opinion-clusters"
    assert obj.dump_date == date(2026, 6, 30)
    assert obj.filename == "opinion-clusters-2026-06-30.csv.bz2"
    assert obj.url.endswith("/bulk-data/opinion-clusters-2026-06-30.csv.bz2")

    # The bucket also holds undated one-off exports; those must not masquerade
    # as a dated dump of some dataset.
    undated = BulkObject("bulk-data/scotus_network.csv", 7_000, '"fixture-etag"', "2024-04-04T00:00:00.000Z")
    assert undated.dump_date is None
    assert undated.dataset == "scotus_network"


def test_published_object_pin_identifies_what_a_capture_read():
    """A receipt naming a filename has named a filename, not a thing.

    The publisher's listing carries the object's exact byte size and
    last-modified stamp, which is what makes two runs comparable and makes "the
    dump was re-cut under us" a detectable event rather than an unexplained
    difference in row counts. DocSpec pins the *population* at
    ``fixtures/courtlistener-bulk-v1/``; this pins the one object a run read,
    and lets that be checked against DocSpec's before the reading starts.
    """
    from spicy_docs.sources.courtlistener.bulk import published_object_pin

    listing = [
        BulkObject(
            "bulk-data/opinions-2026-06-30.csv.bz2",
            54_561_543_156,
            '"fixture-etag"',
            "2026-06-30T09:56:48.000Z",
        ),
        BulkObject("bulk-data/courts-2026-06-30.csv.bz2", 81_180, '"fixture-etag"', "2026-06-30T09:00:26.000Z"),
    ]

    pin = published_object_pin("opinions", date(2026, 6, 30), objects=listing)
    assert pin["bytes"] == 54_561_543_156
    assert pin["last_modified"] == "2026-06-30T09:56:48.000Z"
    assert pin["filename"] == "opinions-2026-06-30.csv.bz2"
    assert pin["listing_object_count"] == 2

    # Held against an expectation, it is a precondition rather than a note —
    # which is the only useful place to discover a changed object when reading
    # it costs 8.6 hours.
    published_object_pin(
        "opinions",
        date(2026, 6, 30),
        objects=listing,
        expect_bytes=54_561_543_156,
        expect_last_modified="2026-06-30T09:56:48.000Z",
    )
    with pytest.raises(RuntimeError, match="the publisher's object changed"):
        published_object_pin("opinions", date(2026, 6, 30), objects=listing, expect_bytes=1)
    with pytest.raises(RuntimeError, match="the publisher's object changed"):
        published_object_pin(
            "opinions",
            date(2026, 6, 30),
            objects=listing,
            expect_last_modified="2026-07-01T00:00:00.000Z",
        )
    with pytest.raises(RuntimeError, match="no opinions dump published"):
        published_object_pin("opinions", date(2026, 3, 31), objects=listing)


def test_latest_dump_date_and_find_dump_pick_one_published_object():
    objects = [
        BulkObject("bulk-data/opinions-2026-03-31.csv.bz2", 54_190_000_000, '"fixture-etag"', "2026-06-30T09:00:00Z"),
        BulkObject("bulk-data/opinions-2026-06-30.csv.bz2", 54_561_543_156, '"fixture-etag"', "2026-06-30T09:00:00Z"),
        BulkObject("bulk-data/courts-2026-06-30.csv.bz2", 81_180, '"fixture-etag"', "2026-06-30T09:00:00Z"),
    ]
    assert latest_dump_date(objects, "opinions") == date(2026, 6, 30)
    assert latest_dump_date(objects, "nonexistent") is None

    found = find_dump(objects, "opinions", date(2026, 6, 30))
    assert found is not None and found.size == 54_561_543_156
    assert find_dump(objects, "opinions", date(2020, 1, 1)) is None


# -- streaming reader --------------------------------------------------------


def test_reader_preserves_source_nulls_and_quoted_empty_strings(tmp_path: Path):
    path = _csv_bz2(
        tmp_path,
        "courts-2026-06-30.csv.bz2",
        "id,short_name,jurisdiction,notes",
        ["ca9,Ninth Circuit,F,", 'scotus,Supreme Court,F,""'],
    )
    rows = list(CourtListenerBulkReader("courts", local_file=path).iter_records())
    assert [r["id"] for r in rows] == ["ca9", "scotus"]
    # PostgreSQL CSV distinguishes an unquoted NULL from a quoted empty string.
    assert rows[0]["notes"] is None
    assert rows[1]["notes"] == ""


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


def test_cluster_shape_matches_schema_and_renames_the_docket_join_key():
    row = _shape_bulk(_RAW_CLUSTER)
    assert set(row) == set(CLUSTER_COLUMNS)
    assert row["cluster_id"] == "10954746"
    # The dump calls it docket_id; court_dockets calls it cl_docket_id. The join
    # only reads the same on both sides if the rename happens here.
    assert row["cl_docket_id"] == "73299709"
    assert row["absolute_url"] == (
        "https://www.courtlistener.com/opinion/10954746/zeevi-v-united-states-department-of-state/"
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
