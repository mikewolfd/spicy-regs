"""Tests for the derived-data comment-text backfill.

Exercises the in-place enrichment over a comments frame, the partition walker
(which must read agency_code from the directory name), and the incremental /
limit / overwrite semantics — all against the fake in-memory S3 resource.
"""

from __future__ import annotations

import json

import duckdb
import polars as pl

import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.mirrulations import MirrulationsAccessRefusedError, comments_extracted_prefix

from spicy_regs import backfill_derived_text
from spicy_regs.backfill_derived_text import (
    _backfill_agency_in_catalog,
    _derived_text_updates,
    backfill_comment_partitions,
    enrich_comments_with_derived_text,
)
from spicy_regs.schemas import COMMENT
from spicy_regs.sources import iceberg
from tests.conftest import COMMENT_SCHEMA

# Reuse the fake S3 surface from the derived-text unit tests.
from tests.test_derived_text import ACF, _FakeS3Resource, _store


def _factory(store: dict[str, bytes] | None = None, **kwargs):
    return lambda: _FakeS3Resource(_store() if store is None else store, **kwargs)


def _counts(selected: int, derived: int = 0, missing: int = 0, failed: int = 0, failed_dockets: int = 0) -> dict:
    return {
        "selected": selected,
        "derived": derived,
        "missing": missing,
        "failed": failed,
        "failed_dockets": failed_dockets,
    }


def _attach() -> str:
    return json.dumps([{"title": "x", "formats": [{"url": "https://x/a.pdf", "format": "pdf"}]}])


def _frame(rows: list[dict]) -> pl.DataFrame:
    base = {k: None for k in COMMENT_SCHEMA}
    return pl.DataFrame([{**base, **r} for r in rows], schema=COMMENT_SCHEMA)


def test_enrich_fills_from_derived_data_using_agency_column() -> None:
    df = _frame(
        [
            {
                "comment_id": "ACF-2025-0038-0004",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": _attach(),
            },
            {
                "comment_id": "ACF-2025-0038-0015",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": _attach(),
            },
        ]
    )
    out, stats = enrich_comments_with_derived_text(df, resource_factory=_factory())
    assert stats == _counts(2, derived=2)
    by_id = {r["comment_id"]: r for r in out.iter_rows(named=True)}
    assert by_id["ACF-2025-0038-0004"]["text_content"] == "Wisconsin DCF comment body"
    assert by_id["ACF-2025-0038-0015"]["text_content"] == "first attachment\n\nsecond attachment"
    assert all(r["text_extraction_status"] == "derived" for r in out.iter_rows(named=True))
    provenance = json.loads(by_id["ACF-2025-0038-0015"]["pdf_extraction_results_json"])
    assert [(a["attachment"], a["tool"]) for a in provenance["attachments"]] == [(1, "pypdf"), (2, "pypdf")]
    assert all(len(a["sha256"]) == 64 for a in provenance["attachments"])


def test_enrich_with_explicit_agency_override() -> None:
    # Partition files drop agency_code; the agency is supplied explicitly.
    df = _frame(
        [{"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "attachments_json": _attach()}]
    ).drop("agency_code")
    out, stats = enrich_comments_with_derived_text(df, agency="ACF", resource_factory=_factory())
    assert stats["derived"] == 1
    assert out.row(0, named=True)["text_content"] == "Wisconsin DCF comment body"


def test_enrich_skips_no_attachment_and_already_filled() -> None:
    df = _frame(
        [
            {
                "comment_id": "ACF-2025-0038-0004",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": None,
            },
            {
                "comment_id": "ACF-2025-0038-0015",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": _attach(),
                "text_extraction_status": "ok",
                "text_content": "kept",
            },
        ]
    )
    out, stats = enrich_comments_with_derived_text(df, resource_factory=_factory())
    assert stats["selected"] == 0
    assert out.filter(pl.col("comment_id") == "ACF-2025-0038-0015").row(0, named=True)["text_content"] == "kept"


def test_enrich_missing_derived_text_counts_as_missing() -> None:
    df = _frame(
        [
            {
                "comment_id": "ACF-2025-0038-9999",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": _attach(),
            }
        ]
    )
    out, stats = enrich_comments_with_derived_text(df, resource_factory=_factory())
    assert stats == _counts(1, missing=1)
    # Left NULL so the PDF-download fallback can still pick it up.
    assert out.row(0, named=True)["text_extraction_status"] is None


def test_enrich_respects_limit() -> None:
    df = _frame(
        [
            {
                "comment_id": "ACF-2025-0038-0004",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": _attach(),
            },
            {
                "comment_id": "ACF-2025-0038-0015",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": _attach(),
            },
        ]
    )
    _, stats = enrich_comments_with_derived_text(df, resource_factory=_factory(), limit=1)
    assert stats["selected"] == 1
    assert stats["derived"] == 1


def test_discover_from_derived_finds_rows_with_no_attachments_json() -> None:
    # Legacy row: no attachments_json recorded at all (ingested before that
    # column existed), yet the fake derived-data store has extracted text for
    # it. Default mode must skip it; discover_from_derived must find it.
    df = _frame(
        [
            {
                "comment_id": "ACF-2025-0038-0004",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": None,
            }
        ]
    )

    default_out, default_stats = enrich_comments_with_derived_text(df, resource_factory=_factory())
    assert default_stats == _counts(0)
    assert default_out.row(0, named=True)["text_content"] is None

    discover_out, discover_stats = enrich_comments_with_derived_text(
        df, resource_factory=_factory(), discover_from_derived=True
    )
    assert discover_stats == _counts(1, derived=1)
    assert discover_out.row(0, named=True)["text_content"] == "Wisconsin DCF comment body"
    assert discover_out.row(0, named=True)["text_extraction_status"] == "derived"


def test_discover_from_derived_is_incremental() -> None:
    # A second run over the already-filled frame must not reselect the row.
    df = _frame(
        [
            {
                "comment_id": "ACF-2025-0038-0004",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": None,
            }
        ]
    )
    filled, _ = enrich_comments_with_derived_text(df, resource_factory=_factory(), discover_from_derived=True)

    _, stats = enrich_comments_with_derived_text(filled, resource_factory=_factory(), discover_from_derived=True)
    assert stats == _counts(0)


def test_discover_from_derived_still_counts_true_misses() -> None:
    # No attachments_json AND no derived-data text available at all.
    df = _frame(
        [
            {
                "comment_id": "ACF-2025-0038-9999",
                "docket_id": "ACF-2025-0038",
                "agency_code": "ACF",
                "attachments_json": None,
            }
        ]
    )
    _, stats = enrich_comments_with_derived_text(df, resource_factory=_factory(), discover_from_derived=True)
    assert stats == _counts(1, missing=1)


def test_backfill_partitions_reads_agency_from_path(tmp_path) -> None:
    part_dir = tmp_path / "comments" / "agency" / "agency_code=ACF"
    part_dir.mkdir(parents=True)
    # Partition file has NO agency_code column (mirrors production layout).
    df = _frame(
        [{"comment_id": "ACF-2025-0038-0004", "docket_id": "ACF-2025-0038", "attachments_json": _attach()}]
    ).drop("agency_code")
    df.write_parquet(part_dir / "part-0.parquet")

    totals, changed = backfill_comment_partitions(tmp_path / "comments" / "agency", resource_factory=_factory())
    assert totals["derived"] == 1
    assert len(changed) == 1
    written = pl.read_parquet(part_dir / "part-0.parquet")
    assert "agency_code" not in written.columns  # schema preserved
    assert written.row(0, named=True)["text_content"] == "Wisconsin DCF comment body"
    assert written.row(0, named=True)["text_extraction_status"] == "derived"
    assert [p.name for p in part_dir.iterdir()] == ["part-0.parquet"]  # the staging directory is gone


def _seed_catalog(con, rows: list[dict]) -> None:
    """Insert comment rows into the local catalog table (all COMMENT columns)."""
    iceberg._ensure_table(con, COMMENT)
    frame = pl.DataFrame([{**{c: None for c in COMMENT.schema}, **r} for r in rows], schema=COMMENT.schema)
    col_list = ", ".join(f'"{c}"' for c in COMMENT.schema)
    con.register("_seed_src", frame.to_arrow())
    con.execute(f"INSERT INTO {iceberg._qualified(COMMENT)} ({col_list}) SELECT {col_list} FROM _seed_src;")
    con.unregister("_seed_src")


def test_catalog_backfill_upserts_filled_rows_in_place() -> None:
    # Local DuckDB standing in for the attached R2 catalog (same alias the
    # connector uses), so the DELETE+INSERT upsert is exercised without network.
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS};")
    try:
        _seed_catalog(
            con,
            [
                # found in the fake derived-data store → gets filled
                {
                    "comment_id": "ACF-2025-0038-0004",
                    "docket_id": "ACF-2025-0038",
                    "agency_code": "ACF",
                    "attachments_json": _attach(),
                    "modify_date": "2025-01-01",
                    "comment": "orig body",
                },
                # attachment but no derived text → counts as missing, stays NULL
                {
                    "comment_id": "ACF-2025-0038-9999",
                    "docket_id": "ACF-2025-0038",
                    "agency_code": "ACF",
                    "attachments_json": _attach(),
                    "modify_date": "2025-01-01",
                },
                # no attachment → not a candidate at all
                {
                    "comment_id": "ACF-2025-0038-0007",
                    "docket_id": "ACF-2025-0038",
                    "agency_code": "ACF",
                    "attachments_json": None,
                    "modify_date": "2025-01-01",
                },
            ],
        )

        stats = _backfill_agency_in_catalog(con, COMMENT, "ACF", resource_factory=_factory())
        assert stats == _counts(2, derived=1, missing=1)

        out = dict(
            con.execute(
                f"SELECT comment_id, text_content FROM {iceberg._qualified(COMMENT)} ORDER BY comment_id"
            ).fetchall()
        )
        assert out["ACF-2025-0038-0004"] == "Wisconsin DCF comment body"
        assert out["ACF-2025-0038-9999"] is None  # missing left NULL for the PDF fallback
        assert out["ACF-2025-0038-0007"] is None  # non-candidate untouched

        # Upsert preserves other columns and doesn't duplicate the row.
        row = con.execute(
            f"SELECT comment, text_extraction_status, pdf_extraction_results_json FROM {iceberg._qualified(COMMENT)} "
            f"WHERE comment_id = 'ACF-2025-0038-0004'"
        ).fetchall()
        assert len(row) == 1
        assert row[0][:2] == ("orig body", "derived")
        assert json.loads(row[0][2])["comment_id"] == "ACF-2025-0038-0004"
        count = con.execute(f"SELECT count(*) FROM {iceberg._qualified(COMMENT)}").fetchone()
        assert count is not None and count[0] == 3

        # Idempotent: a second run finds nothing new (the filled row now has a status).
        again = _backfill_agency_in_catalog(con, COMMENT, "ACF", resource_factory=_factory())
        assert again == _counts(1, missing=1)
    finally:
        con.close()


def test_catalog_backfill_discover_from_derived_finds_legacy_rows_without_attachments_json() -> None:
    # This is the exact trap the docstring warns about: rows ingested before
    # attachments_json was recorded have it NULL, so the default (attachment-
    # gated) catalog query never selects them even though Mirrulations has
    # extracted text ready and waiting. discover_from_derived must find them.
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS};")
    try:
        _seed_catalog(
            con,
            [
                # legacy row: attachments_json was never populated
                {
                    "comment_id": "ACF-2025-0038-0004",
                    "docket_id": "ACF-2025-0038",
                    "agency_code": "ACF",
                    "attachments_json": None,
                    "modify_date": "2025-01-01",
                },
                # legacy row with no derived-data text available either
                {
                    "comment_id": "ACF-2025-0038-9999",
                    "docket_id": "ACF-2025-0038",
                    "agency_code": "ACF",
                    "attachments_json": None,
                    "modify_date": "2025-01-01",
                },
            ],
        )

        default_stats = _backfill_agency_in_catalog(con, COMMENT, "ACF", resource_factory=_factory())
        assert default_stats == _counts(0), "default mode must not touch legacy rows"

        discover_stats = _backfill_agency_in_catalog(
            con, COMMENT, "ACF", resource_factory=_factory(), discover_from_derived=True
        )
        assert discover_stats == _counts(2, derived=1, missing=1)

        out = dict(
            con.execute(
                f"SELECT comment_id, text_content FROM {iceberg._qualified(COMMENT)} ORDER BY comment_id"
            ).fetchall()
        )
        assert out["ACF-2025-0038-0004"] == "Wisconsin DCF comment body"
        assert out["ACF-2025-0038-9999"] is None

        # Idempotent: a second discover run finds nothing new for the filled row.
        again = _backfill_agency_in_catalog(
            con, COMMENT, "ACF", resource_factory=_factory(), discover_from_derived=True
        )
        assert again == _counts(1, missing=1)
    finally:
        con.close()


def _acf(comment: str, **values) -> dict:
    return {
        "comment_id": f"ACF-2025-0038-{comment}",
        "docket_id": "ACF-2025-0038",
        "agency_code": "ACF",
        "attachments_json": _attach(),
        **values,
    }


def test_a_refused_docket_counts_as_failed_and_stays_pending() -> None:
    """A strict-listing refusal is never read as "no text": nothing is written and the rows are retried."""
    store = _store() | {comments_extracted_prefix(*ACF) + "unexpected.txt": b"?"}
    df = _frame([_acf("0004"), _acf("9999")])
    out, stats = enrich_comments_with_derived_text(df, resource_factory=_factory(store))
    assert stats == _counts(2, failed=2, failed_dockets=1)
    assert out.select("text_content", "text_extraction_status", "pdf_extraction_results_json").null_count().row(0) == (
        2,
        2,
        2,
    )
    _, again = enrich_comments_with_derived_text(out, resource_factory=_factory())
    assert again == _counts(2, derived=1, missing=1)


def test_an_access_refusal_ends_the_backfill() -> None:
    with pytest.raises(MirrulationsAccessRefusedError):
        enrich_comments_with_derived_text(_frame([_acf("0004")]), resource_factory=_factory(listing_status=401))


def test_fills_stream_to_staging_a_bounded_buffer_at_a_time(tmp_path, monkeypatch) -> None:
    """With a one-character buffer every fill is its own row group: none waits for the run to end."""
    monkeypatch.setattr(backfill_derived_text, "_FLUSH_CHARACTERS", 1)
    updates = tmp_path / "updates.parquet"
    stats = _derived_text_updates(
        _frame([_acf("0004"), _acf("0015"), _acf("0030")]),
        agency=None,
        resource_factory=_factory(),
        limit=None,
        max_workers=2,
        overwrite=False,
        updates_path=updates,
    )
    assert stats == _counts(3, derived=3)
    staged = pq.ParquetFile(updates)
    assert staged.metadata.num_row_groups == 3
    rows = {r["comment_id"]: r for r in staged.read().to_pylist()}
    assert rows["ACF-2025-0038-0030"]["_new_text"] == "part 1\n\npart 2\n\npart 10"
    assert {r["_new_status"] for r in rows.values()} == {"derived"}


def test_file_backfill_keeps_row_order_and_untouched_rows(tmp_path) -> None:
    rows = [_acf(c) for c in ("0030", "0001", "0004", "0002")]
    path = tmp_path / "comments.parquet"
    _frame([*rows, _acf("0015", text_extraction_status="ok", text_content="kept")]).write_parquet(path)
    stats = backfill_derived_text.backfill_comments_parquet(path, resource_factory=_factory())
    assert stats == _counts(4, derived=2, missing=2)
    written = pl.read_parquet(path)
    assert written["comment_id"].to_list() == [r["comment_id"] for r in rows] + ["ACF-2025-0038-0015"]
    assert written["text_extraction_status"].to_list() == ["derived", None, "derived", None, "ok"]
    assert written["text_content"][4] == "kept"


def test_overwrite_refills_derived_rows_and_never_pdf_fills() -> None:
    pdf_rows = [
        _acf("0015", text_extraction_status=status, text_content=text, pdf_extraction_results_json="[]")
        for status, text in (("ok", "extracted by spicy-regs"), ("empty", None), ("error", None))
    ]
    stale = _acf("0004", text_extraction_status="derived", text_content="stale", pdf_extraction_results_json="{}")
    for rows, expected in (([stale], _counts(1, derived=1)), (pdf_rows, _counts(0))):
        frame = _frame(rows)
        out, stats = enrich_comments_with_derived_text(frame, resource_factory=_factory(), overwrite=True)
        assert stats == expected
        if rows is pdf_rows:
            assert out.select(frame.columns).equals(frame)
        else:
            assert out["text_content"][0] == "Wisconsin DCF comment body"
            assert json.loads(out["pdf_extraction_results_json"][0])["tool"] == "pypdf"


def test_catalog_overwrite_refills_derived_rows_and_never_pdf_fills() -> None:
    con = duckdb.connect()
    con.execute(f"ATTACH ':memory:' AS {iceberg._CATALOG_ALIAS};")
    try:
        _seed_catalog(
            con,
            [
                _acf("0004", modify_date="2025-01-01", text_extraction_status="derived", text_content="stale"),
                _acf("0015", modify_date="2025-01-01", text_extraction_status="ok", text_content="extracted by us"),
            ],
        )
        stats = _backfill_agency_in_catalog(con, COMMENT, "ACF", resource_factory=_factory(), overwrite=True)
        assert stats == _counts(1, derived=1)
        rows = dict(con.execute(f"SELECT comment_id, text_content FROM {iceberg._qualified(COMMENT)}").fetchall())
        assert rows == {"ACF-2025-0038-0004": "Wisconsin DCF comment body", "ACF-2025-0038-0015": "extracted by us"}
    finally:
        con.close()


def test_a_failed_attachment_fails_only_its_comment() -> None:
    """A comment whose object changed since the listing is `failed` and pending; its docket's others fill."""

    class _Replaced(_FakeS3Resource):
        def Object(self, name, key):  # noqa: N802
            obj = super().Object(name, key)
            if key.endswith("-0015_attachment_2_extracted.txt"):
                obj.e_tag = '"replaced after listing"'
            return obj

    df = _frame([_acf("0004"), _acf("0015"), _acf("0030")])
    out, stats = enrich_comments_with_derived_text(df, resource_factory=lambda: _Replaced(_store()))
    assert stats == _counts(3, derived=2, failed=1)
    by_id = {r["comment_id"]: r for r in out.iter_rows(named=True)}
    assert by_id["ACF-2025-0038-0015"]["text_extraction_status"] is None
    assert by_id["ACF-2025-0038-0030"]["text_extraction_status"] == "derived"


def test_an_access_refusal_ends_the_run_while_other_dockets_are_in_flight(tmp_path) -> None:
    """The refusal propagates, queued dockets are never listed, and the in-flight ones finish into a closed file."""
    import threading

    from botocore.exceptions import ClientError

    from tests.test_derived_text import _FakeObjects, derived_key

    dockets = [f"ACF-2025-00{n}" for n in range(40, 45)]  # two workers: 40 refuses while 41 is in flight
    store = {derived_key("ACF", d, "pypdf", "0001", 1): b"text" for d in dockets}
    release = threading.Event()

    class _Gated(_FakeObjects):
        def filter(self, Prefix):  # noqa: N803
            if dockets[0] in Prefix:
                self._resource.listed.append(Prefix)
                threading.Timer(0.2, release.set).start()  # hold the others until the refusal has landed
                raise ClientError(
                    {"Error": {"Code": "403"}, "ResponseMetadata": {"HTTPStatusCode": 403}}, "ListObjects"
                )
            release.wait(5)
            yield from super().filter(Prefix)

    class _GatedResource(_FakeS3Resource):
        def Bucket(self, name):  # noqa: N802
            return type("_Bucket", (), {"objects": _Gated(self)})()

    resource = _GatedResource(store)
    frame = _frame([_acf("0001") | {"comment_id": f"{d}-0001", "docket_id": d} for d in dockets])
    updates = tmp_path / "updates.parquet"
    with pytest.raises(MirrulationsAccessRefusedError):
        _derived_text_updates(
            frame,
            agency=None,
            resource_factory=lambda: resource,
            limit=None,
            max_workers=2,
            overwrite=False,
            updates_path=updates,
        )
    listed = {prefix.split("/")[2] for prefix in resource.listed}
    # 41 was in flight; 42 may have started in the refused docket's freed worker before the cancel.
    assert set(dockets[:2]) <= listed <= set(dockets[:3])
    assert pq.read_table(updates).num_rows == len(listed) - 1  # the in-flight fills, in a closed file


@pytest.mark.parametrize(
    "failed,failed_dockets,message", [(0, 0, None), (2, 2, "2 docket listing"), (1, 0, "1 comment")]
)
def test_the_command_fails_after_writing_when_a_fetch_or_listing_failed(
    tmp_path, monkeypatch, failed, failed_dockets, message
) -> None:
    monkeypatch.setattr(backfill_derived_text, "load_dotenv", lambda: None)
    monkeypatch.setattr(
        backfill_derived_text,
        "backfill_comments_parquet",
        lambda *a, **k: _counts(3, failed=failed, failed_dockets=failed_dockets),
    )
    monkeypatch.setattr("sys.argv", ["backfill-comment-text", "--output-dir", str(tmp_path)])
    if message:
        with pytest.raises(SystemExit, match=message):
            backfill_derived_text.main()
    else:
        backfill_derived_text.main()


def test_workers_are_capped_at_the_s3_pool(monkeypatch, tmp_path) -> None:
    """More threads than the shared resource's 16 connections stall, so the pool bounds them."""
    from concurrent.futures import ThreadPoolExecutor

    from spicy_docs.sources.mirrulations import DEFAULT_DOWNLOAD_WORKERS

    sizes: list[int] = []

    class _Recording(ThreadPoolExecutor):
        def __init__(self, max_workers: int) -> None:
            sizes.append(max_workers)
            super().__init__(max_workers=max_workers)

    monkeypatch.setattr(backfill_derived_text, "ThreadPoolExecutor", _Recording)
    for requested in (4, 64):
        _derived_text_updates(
            _frame([_acf("0004")]),
            agency=None,
            resource_factory=_factory(),
            limit=None,
            max_workers=requested,
            overwrite=False,
            updates_path=tmp_path / f"{requested}.parquet",
        )
    assert sizes == [4, DEFAULT_DOWNLOAD_WORKERS]
