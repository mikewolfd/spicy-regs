"""GAO output field mapping, source labels and merge; provider feed validation is tested in test_reference_source_failures."""

from __future__ import annotations

from importlib import import_module

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.build_gao_reports import (
    COLUMNS,
    _published_date,
    _report_id,
    _shape,
    build_gao_reports,
)
from tests.test_gao_govinfo import ListingReader, _package

# ``spicy_regs.transforms`` re-exports the builder function under the module's name.
module = import_module("spicy_regs.transforms.build_gao_reports")

_RAW_ITEM = {
    "title": "Navy Ship Modernization",
    "link": "https://www.gao.gov/products/gao-26-107974",
    "description": "What GAO Found. The Navy is behind schedule.",
    "pub_date": "Fri, 17 Jul 2026 07:10:42 -0400",
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_ITEM)
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 9
    assert row["source"] == "gao_rss"


def test_shape_maps_fields():
    row = _shape(_RAW_ITEM)
    assert row["report_id"] == "gao-26-107974"
    assert row["title"] == "Navy Ship Modernization"
    assert row["report_type"] == "Report"
    assert row["published_date"] == "2026-07-17"
    assert row["abstract"].startswith("What GAO Found")
    # Reserved columns default to empty JSON arrays.
    assert row["agencies_json"] == "[]"
    assert row["topics_json"] == "[]"
    assert row["url"] == "https://www.gao.gov/products/gao-26-107974"


def test_report_id_extraction():
    assert _report_id("https://www.gao.gov/products/gao-26-107974") == "gao-26-107974"
    # Trailing slash and mixed case are normalized.
    assert _report_id("https://www.gao.gov/products/GAO-26-108520/") == "gao-26-108520"
    assert _report_id(None) is None
    assert _report_id("") is None


def test_published_date_parses_rfc822():
    assert _published_date("Fri, 17 Jul 2026 07:10:42 -0400") == "2026-07-17"
    # Unparseable / missing values degrade to None, not an exception.
    assert _published_date("not a date") is None
    assert _published_date(None) is None


#: The published columns before ``source``: what the live prior holds until a run with this code publishes.
_PRE_SOURCE = pa.schema([(c, pa.string()) for c in COLUMNS if c != "source"])


def _feed_row(report_id: str) -> dict:
    return {"report_id": report_id, "title": f"Feed {report_id}", "report_type": "Report",
            "published_date": "2026-09-20", "abstract": "What GAO Found.", "agencies_json": "[]",
            "topics_json": "[]", "url": f"https://www.gao.gov/products/{report_id}"}


def _repair_row(report_id: str) -> dict:
    return {**dict.fromkeys(_PRE_SOURCE.names), "report_id": report_id, "title": f"Repaired {report_id}",
            "published_date": "2008-07-11", "url": f"https://www.gao.gov/products/{report_id}"}


def _run(tmp_path, monkeypatch, *, prior=None, feed=(), history=None):
    """One build over a local prior and a stubbed feed; ``history`` is the GovInfo listing, or None for no walk."""
    if prior is not None:
        pq.write_table(prior, tmp_path / "_gao_prior.parquet")

    class Feed:
        def __init__(self, **_):
            pass

        def iter_records(self):
            return iter({"title": f"Feed {i}", "link": f"https://www.gao.gov/products/{i}",
                         "description": "What GAO Found.", "pub_date": "Mon, 21 Sep 2026 10:00:00 -0400"} for i in feed)

    monkeypatch.setattr(module, "GaoReportsReader", Feed)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    out = build_gao_reports(tmp_path, govinfo_history=history is not None,
                            govinfo=ListingReader(history) if history is not None else None)
    table = pq.read_table(out)
    assert table.column_names == list(COLUMNS)
    return {row["report_id"]: row for row in table.to_pylist()}


def test_a_prior_without_source_is_labelled_by_the_shape_each_route_writes(tmp_path, monkeypatch):
    prior = pa.Table.from_pylist([_feed_row("gao-26-1"), _repair_row("gao-17-317")], schema=_PRE_SOURCE)
    rows = _run(tmp_path, monkeypatch, prior=prior, feed=["gao-26-2"])
    assert {k: v["source"] for k, v in rows.items()} == {
        "gao-26-1": "gao_rss", "gao-17-317": "gao_repair", "gao-26-2": "gao_rss"}
    assert rows["gao-17-317"] == {**_repair_row("gao-17-317"), "source": "gao_repair"}


def test_upstream_copied_rows_are_labelled_by_their_window_only_when_they_are_the_reviewed_rows(tmp_path, monkeypatch):
    import hashlib
    import json

    copied = {**_feed_row("gao-26-107000"), "published_date": "2026-08-01"}
    prior = pa.Table.from_pylist([_feed_row("gao-26-1"), copied], schema=_PRE_SOURCE)
    with pytest.raises(ValueError, match="reviewed copied rows"):
        _run(tmp_path, monkeypatch, prior=prior)
    # The import's own digest, written out: one compact JSON array per row, in report_id order.
    line = json.dumps([copied[c] for c in _PRE_SOURCE.names], ensure_ascii=False, separators=(",", ":"))
    monkeypatch.setattr(module, "UPSTREAM_COPY_ROWS_SHA256", "sha256:" + hashlib.sha256(line.encode()).hexdigest())
    rows = _run(tmp_path, monkeypatch, prior=prior)
    assert {k: v["source"] for k, v in rows.items()} == {"gao-26-1": "gao_rss", "gao-26-107000": "upstream_copy"}


def test_a_prior_row_neither_route_writes_refuses_rather_than_being_guessed(tmp_path, monkeypatch):
    stranger = {**_feed_row("gao-26-9"), "agencies_json": '["EPA"]'}
    prior = pa.Table.from_pylist([_feed_row("gao-26-1"), stranger], schema=_PRE_SOURCE)
    with pytest.raises(ValueError, match="gao-26-9"):
        _run(tmp_path, monkeypatch, prior=prior)


def test_history_adds_govinfo_rows_and_never_replaces_another_route(tmp_path, monkeypatch):
    prior = pa.Table.from_pylist([_feed_row("gao-26-1"), _repair_row("gao-08-919r")], schema=_PRE_SOURCE)
    listing = [_package("GAOREPORTS-GAO-08-919R"), _package("GAOREPORTS-T-RCED-94-121"),
               _package("GAOREPORTS-GAO-26-2"), _package("GAOREPORTS-B-400379", "COMPTROLLERDECISION")]
    rows = _run(tmp_path, monkeypatch, prior=prior, feed=["gao-26-2"], history=listing)
    assert {k: v["source"] for k, v in rows.items()} == {
        "gao-26-1": "gao_rss", "gao-26-2": "gao_rss", "gao-08-919r": "gao_repair", "t-rced-94-121": "govinfo"}
    assert rows["gao-08-919r"]["title"] == "Repaired gao-08-919r"
    assert rows["t-rced-94-121"]["report_type"] == "Testimony"


def test_a_feed_run_carries_govinfo_rows_and_a_history_rerun_refreshes_them(tmp_path, monkeypatch):
    first = _run(tmp_path, monkeypatch, feed=["gao-26-1"], history=[_package("GAOREPORTS-T-RCED-94-121")])
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    carried = _run(tmp_path, monkeypatch, feed=["gao-26-2"])
    assert carried["t-rced-94-121"] == first["t-rced-94-121"]
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    retitled = _run(tmp_path, monkeypatch, history=[_package("GAOREPORTS-T-RCED-94-121", title="Retitled")])
    assert retitled["t-rced-94-121"]["title"] == "Retitled"
    assert set(retitled) == {"gao-26-1", "gao-26-2", "t-rced-94-121"}


def test_the_rollup_walks_govinfo_only_when_its_flag_says_so(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups import gao_reports as rollup

    calls = []
    monkeypatch.setattr(rollup, "build_gao_reports", lambda *_, **kwargs: calls.append(kwargs["govinfo_history"]))
    for value in ("", "true"):
        monkeypatch.setenv("GAO_GOVINFO_HISTORY", value)
        rollup.GaoReportsRollup(output_dir=tmp_path).build(tmp_path)
    assert calls == [False, True]
