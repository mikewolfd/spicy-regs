"""GAO output field mapping, source labels and merge; provider feed validation is tested in test_reference_source_failures."""

from __future__ import annotations

from importlib import import_module

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.build_gao_reports import (
    COLUMNS,
    _published_date,
    _report_id,
    _shape,
    build_gao_reports,
)
from tests.test_gao_govinfo import ListingReader, _package
from tests.test_gao_listing import _product
from tests.test_gao_listing import _run as _listing

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
    assert len(COLUMNS) == 11
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


def _feed_row(report_id: str) -> dict:
    return {"report_id": report_id, "title": f"Feed {report_id}", "report_type": "Report",
            "published_date": "2026-09-20", "abstract": "What GAO Found.", "agencies_json": "[]",
            "topics_json": "[]", "url": f"https://www.gao.gov/products/{report_id}", "source": "gao_rss",
            "product_type": None, "report_number": None}


def _repair_row(report_id: str) -> dict:
    return {**dict.fromkeys(COLUMNS), "report_id": report_id, "title": f"Repaired {report_id}",
            "published_date": "2008-07-11", "url": f"https://www.gao.gov/products/{report_id}", "source": "gao_repair"}


def _run(tmp_path, monkeypatch, *, prior=None, feed=(), history=None, listed=None, evidence=None):
    """One build over a local prior and a stubbed feed; ``history`` is the GovInfo listing and ``listed`` GAO's own
    listing's products, each None for no read."""
    if prior is not None:
        pq.write_table(prior, tmp_path / "_gao_prior.parquet")

    class Feed:
        def __init__(self, **_):
            pass

        def iter_records(self):
            return iter({"title": f"Feed {i}", "link": f"https://www.gao.gov/products/{i}",
                         "description": "What GAO Found.", "pub_date": "Mon, 21 Sep 2026 10:00:00 -0400"} for i in feed)

    def read_listing(directory, evidence):
        assert directory == tmp_path / "walk"
        run = _listing(*(listed or ()))
        rows, counts = module.gao_listing.listing_rows(run)
        return rows, counts, run

    monkeypatch.setattr(module, "GaoReportsReader", Feed)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    monkeypatch.setattr(module.gao_listing, "read_listing", read_listing)
    out = build_gao_reports(tmp_path, govinfo_history=history is not None,
                            govinfo=ListingReader(history) if history is not None else None,
                            listing_run=tmp_path / "walk" if listed is not None else None, evidence=evidence)
    table = pq.read_table(out)
    assert table.column_names == list(COLUMNS)
    return {row["report_id"]: row for row in table.to_pylist()}


def test_history_adds_govinfo_rows_and_never_replaces_another_route(tmp_path, monkeypatch):
    prior = pa.Table.from_pylist([_feed_row("gao-26-1"), _repair_row("gao-08-919r")], schema=module._SCHEMA)
    listing = [_package("GAOREPORTS-GAO-08-919R"), _package("GAOREPORTS-T-RCED-94-121"),
               _package("GAOREPORTS-GAO-26-2"), _package("GAOREPORTS-B-400379", "COMPTROLLERDECISION")]
    rows = _run(tmp_path, monkeypatch, prior=prior, feed=["gao-26-2"], history=listing)
    assert {k: v["source"] for k, v in rows.items()} == {
        "gao-26-1": "gao_rss", "gao-26-2": "gao_rss", "gao-08-919r": "gao_repair", "t-rced-94-121": "govinfo"}
    assert rows["gao-08-919r"]["title"] == "Repaired gao-08-919r"
    assert rows["t-rced-94-121"]["report_type"] == "Testimony"


def test_a_feed_run_carries_govinfo_rows_and_a_history_rerun_leaves_held_rows_alone(tmp_path, monkeypatch):
    first = _run(tmp_path, monkeypatch, feed=["gao-26-1"], history=[_package("GAOREPORTS-T-RCED-94-121")])
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    carried = _run(tmp_path, monkeypatch, feed=["gao-26-2"])
    assert carried["t-rced-94-121"] == first["t-rced-94-121"]
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    # A MODS read must survive a later history run, so a held id is never replaced.
    rerun = _run(tmp_path, monkeypatch, history=[_package("GAOREPORTS-T-RCED-94-121", title="Retitled"),
                                                  _package("GAOREPORTS-GAO-08-919R")])
    assert rerun["t-rced-94-121"] == first["t-rced-94-121"]
    assert set(rerun) == {"gao-26-1", "gao-26-2", "t-rced-94-121", "gao-08-919r"}


def test_the_listing_fills_only_ids_no_row_holds_this_runs_govinfo_rows_included(tmp_path, monkeypatch):
    prior = pa.Table.from_pylist([_feed_row("gao-26-1"), _repair_row("gao-17-317")], schema=module._SCHEMA)
    listed = [_product("gao-26-1"), _product("gao-17-317"), _product("gao-26-2"), _product("gao-09-431t"),
              _product("gao-08-919r")]
    rows = _run(tmp_path, monkeypatch, prior=prior, feed=["gao-26-2"], listed=listed,
                history=[_package("GAOREPORTS-GAO-08-919R")])
    assert {k: v["source"] for k, v in rows.items()} == {
        "gao-26-1": "gao_rss", "gao-17-317": "gao_repair", "gao-26-2": "gao_rss", "gao-08-919r": "govinfo",
        "gao-09-431t": "gao_listing"}
    assert rows["gao-17-317"]["title"] == "Repaired gao-17-317"
    listed_row = rows["gao-09-431t"]
    assert (listed_row["report_type"], listed_row["report_number"], listed_row["topics_json"]) == (
        "Testimony", "GAO-09-431T", '["Education"]')


def test_a_later_listing_read_adds_new_products_and_leaves_held_rows_alone(tmp_path, monkeypatch):
    first = _run(tmp_path, monkeypatch, listed=[_product("gao-12-100")])
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    carried = _run(tmp_path, monkeypatch, feed=["gao-26-2"])
    assert carried["gao-12-100"] == first["gao-12-100"]
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    again = _run(tmp_path, monkeypatch, listed=[_product("gao-12-100", title="Label: Retitled"), _product("gao-12-101")])
    assert again["gao-12-100"] == first["gao-12-100"] and again["gao-12-101"]["source"] == "gao_listing"


def test_a_listing_read_is_journaled_with_its_scopes_and_counts(tmp_path, monkeypatch):
    class Evidence:
        def __init__(self):
            self.events = []

        def event(self, name, **fields):
            self.events.append((name, fields))

    evidence = Evidence()
    _run(tmp_path, monkeypatch, feed=["gao-26-1"], listed=[_product("gao-26-1"), _product("gao-12-100")],
         evidence=evidence)
    (name, fields), = evidence.events
    assert name == "gao-listing" and fields["scopes_read"] == ["2026-08"] and fields["scopes_unfinished"] == []
    assert fields["rows"] == 2 and fields["already_held"] == 1 and fields["complete_scopes"] == 1


def test_the_rollup_reads_a_listing_walk_only_when_one_is_named(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups import gao_reports as rollup

    calls = []
    monkeypatch.setattr(rollup, "build_gao_reports", lambda *_, **kwargs: calls.append(kwargs["listing_run"]))
    for value in ("", str(tmp_path / "walk")):
        monkeypatch.setenv("GAO_LISTING_RUN", value)
        rollup.GaoReportsRollup(output_dir=tmp_path).build(tmp_path)
    assert calls == [None, tmp_path / "walk"]


def test_the_rollup_walks_govinfo_only_when_its_flag_says_so(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups import gao_reports as rollup

    calls = []
    monkeypatch.setattr(rollup, "build_gao_reports",
                        lambda *_, **kwargs: calls.append((kwargs["govinfo_history"], kwargs["govinfo_mods"])))
    for history, mods in (("", ""), ("true", ""), ("", "true")):
        monkeypatch.setenv("GAO_GOVINFO_HISTORY", history)
        monkeypatch.setenv("GAO_GOVINFO_MODS", mods)
        rollup.GaoReportsRollup(output_dir=tmp_path).build(tmp_path)
    assert calls == [(False, False), (True, False), (False, True)]
