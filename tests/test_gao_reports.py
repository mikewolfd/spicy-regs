"""GAO output field mapping, source labels and merge; provider feed validation is tested in test_reference_source_failures."""

from __future__ import annotations

from importlib import import_module

from spicy_docs.interpretation.gao_decisions import GAO_OUTCOME_RULE
from spicy_docs.schemas import TABLE_CONTRACTS

import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.transforms.build_gao_reports import (
    COLUMNS,
    _published_date,
    _shape,
    build_gao_reports,
)
from tests.test_gao_govinfo import ListingReader, _package
from tests.test_gao_listing import _product
from tests.test_gao_listing import _run as _listing

# ``spicy_regs.transforms`` re-exports the builder function under the module's name.
module = import_module("spicy_regs.transforms.build_gao_reports")

_RAW_ITEM = {
    "product_id": "gao-26-107974",
    "title": "Navy Ship Modernization",
    "link": "https://www.gao.gov/products/gao-26-107974",
    "description": "What GAO Found. The Navy is behind schedule.",
    "pub_date": "Fri, 17 Jul 2026 07:10:42 -0400",
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_ITEM)
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 18
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


def _run(tmp_path, monkeypatch, *, prior=None, feed=(), history=None, listed=None, evidence=None, decided=None,
         with_decisions=False, others=()):
    """One build over a local prior and a stubbed feed; ``history`` is the GovInfo listing and ``listed`` GAO's own
    listing's products, each None for no read."""
    if prior is not None:
        pq.write_table(prior, tmp_path / "_gao_prior.parquet")

    class Feed:
        def __init__(self, **_):
            pass

        def iter_records(self):
            return iter({"product_id": i, "title": f"Feed {i}", "link": f"https://www.gao.gov/products/{i}",
                         "description": "What GAO Found.", "pub_date": "Mon, 21 Sep 2026 10:00:00 -0400"} for i in feed)

    def read_listing(directory, evidence):
        assert directory == tmp_path / "walk"
        run = _listing(*(listed or ()), decisions=decided or (), others=others)
        rows, counts = module.gao_listing.listing_rows(run)
        return rows, counts, run

    monkeypatch.setattr(module, "GaoReportsReader", Feed)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    monkeypatch.setattr(module.gao_listing, "read_listing", read_listing)
    out = build_gao_reports(tmp_path, govinfo_history=history is not None,
                            govinfo=ListingReader(history) if history is not None else None,
                            listing_run=tmp_path / "walk" if listed is not None else None, evidence=evidence)
    out, decisions = out
    table = pq.read_table(out)
    assert table.column_names == list(COLUMNS)
    assert pq.read_table(decisions).column_names == list(TABLE_CONTRACTS["gao_decisions"].columns)
    rows = {row["report_id"]: row for row in table.to_pylist()}
    if with_decisions:
        return rows, pq.read_table(decisions).to_pylist()
    return rows


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


def test_the_listing_fills_a_held_rows_null_report_number_and_never_replaces_one(tmp_path, monkeypatch):
    numbered = {**_feed_row("gao-26-3"), "report_number": "GAO-26-3 AS PRINTED"}
    prior = pa.Table.from_pylist([_feed_row("gao-26-1"), numbered, _repair_row("gao-17-317")], schema=module._SCHEMA)
    rows = _run(tmp_path, monkeypatch, prior=prior, feed=["gao-26-2"],
                listed=[_product("gao-26-1"), _product("gao-26-2"), _product("gao-26-3"), _product("gao-17-317")])
    assert {k: v["report_number"] for k, v in rows.items()} == {
        "gao-26-1": "GAO-26-1", "gao-26-2": "GAO-26-2", "gao-26-3": "GAO-26-3 AS PRINTED", "gao-17-317": "GAO-17-317"}
    assert {k: v["source"] for k, v in rows.items()} == {
        "gao-26-1": "gao_rss", "gao-26-2": "gao_rss", "gao-26-3": "gao_rss", "gao-17-317": "gao_repair"}
    assert rows["gao-17-317"] == {**_repair_row("gao-17-317"), "report_number": "GAO-17-317"}


def test_a_feed_read_of_a_held_product_fills_cells_and_never_empties_one(tmp_path, monkeypatch):
    listed = {**_feed_row("gao-26-5"), "title": "Label: Listed", "report_type": "Testimony", "abstract": None,
              "agencies_json": None, "topics_json": '["Education"]', "source": "gao_listing",
              "product_type": "Correspondence", "report_number": "GAO-26-5"}
    fed = {**_feed_row("gao-26-6"), "title": "Old feed title", "report_number": "GAO-26-6"}
    prior = pa.Table.from_pylist([listed, fed], schema=module._SCHEMA)
    rows = _run(tmp_path, monkeypatch, prior=prior, feed=["gao-26-5", "gao-26-6"])
    # A listing row keeps everything it states; the feed fills only its NULL abstract.
    assert rows["gao-26-5"] == {**dict.fromkeys(COLUMNS), **listed, "abstract": "What GAO Found."}
    # The feed refreshes its own row's title and date, and keeps the number the listing filled.
    assert rows["gao-26-6"]["title"] == "Feed gao-26-6" and rows["gao-26-6"]["published_date"] == "2026-09-21"
    assert (rows["gao-26-6"]["report_number"], rows["gao-26-6"]["source"]) == ("GAO-26-6", "gao_rss")


def test_the_listing_writes_gao_decisions_and_a_run_without_it_carries_them(tmp_path, monkeypatch):
    from tests.test_gao_listing import _decision

    decided = [_decision("/products/b-424129.2", "B-424129.2"), _decision("/products/b-331093-0", "B-331093")]
    _, first = _run(tmp_path, monkeypatch, listed=[_product("gao-26-1")], decided=decided, with_decisions=True)
    assert sorted((row["decision_number"], row["url"]) for row in first) == [
        ("B-331093", "https://www.gao.gov/products/b-331093-0"), ("B-424129.2", "https://www.gao.gov/products/b-424129.2")]
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    (tmp_path / "gao_decisions.parquet").rename(tmp_path / "_gao_decisions_prior.parquet")
    _, carried = _run(tmp_path, monkeypatch, feed=["gao-26-2"], with_decisions=True)
    assert carried == first


def test_a_prior_published_as_decision_date_carries_its_dates_into_released_date(tmp_path, monkeypatch):
    """The owner's rename (2026-10-03): the column is the date GAO released the decision, so it says so.

    The published prior still spells it ``decision_date``. A run without a walk,
    the daily case, must carry every row's date across the rename from the
    prior's own values, never re-walk the listing for it and never leave it NULL.
    """
    from tests.test_gao_listing import _decision

    decided = [_decision("/products/b-424129.2", "B-424129.2"), _decision("/products/b-331093-0", "B-331093")]
    _, first = _run(tmp_path, monkeypatch, listed=[_product("gao-26-1")], decided=decided, with_decisions=True)
    assert {row["released_date"] for row in first} == {"2026-08-18"}
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    published = pq.read_table(tmp_path / "gao_decisions.parquet")
    published = published.rename_columns(["decision_date" if c == "released_date" else c for c in published.column_names])
    pq.write_table(published, tmp_path / "_gao_decisions_prior.parquet")
    (tmp_path / "gao_decisions.parquet").unlink()
    _, carried = _run(tmp_path, monkeypatch, feed=["gao-26-2"], with_decisions=True)
    assert carried == first


def test_decisions_publish_under_the_spicy_docs_contract_with_the_outcome_their_sentence_states(tmp_path, monkeypatch):
    """gao_decisions is spicy-docs' contract from 0.54.0 (DRY X1): shaped there, its outcome read there.

    A numbered other (a Contract Appeals Board docket) is a row with no B-numbers; an unnumbered one is left out.
    """
    from tests.test_gao_listing import LISTING_PAGE, _decision, _other

    decided = [_decision("/products/b-424129.2", "B-424129.2", status="We deny the protest.")]
    others = [_other("/products/2020-02-0", "2020-02"), _other("/products/p00459", None)]
    _, rows = _run(tmp_path, monkeypatch, listed=[], decided=decided, others=others, with_decisions=True)
    by_number = {row["decision_number"]: row for row in rows}
    assert set(by_number) == {"B-424129.2", "2020-02"}
    assert by_number["B-424129.2"] | {} == {
        "decision_number": "B-424129.2", "b_numbers_json": '["B-424129.2"]', "decision_type": "Bid Protest Decision",
        "title": "Acme Corp.", "released_date": "2026-08-18", "topics_json": '["Bid Protest Decision"]',
        "url": "https://www.gao.gov/products/b-424129.2", "listing_page": LISTING_PAGE, "source": "gao_listing",
        "decision_status": "We deny the protest.", "outcome": "denied", "outcome_rule": GAO_OUTCOME_RULE,
        # The listing's own values: its list is whole, and only a decision page states the decided day.
        "b_numbers_truncated": "false", "decided_date": None,
    }
    assert (by_number["2020-02"]["b_numbers_json"], by_number["2020-02"]["outcome"]) == ("[]", None)


def test_every_held_decision_has_its_outcome_read_again_each_run(tmp_path, monkeypatch):
    """The outcome is a reading of the stated sentence, applied to every merged row each run, never carried.

    So a table version reaches held rows without a re-read of the walk, and a prior's own outcome (here one an
    earlier table wrote, or none) is never what gets published.
    """
    from tests.test_gao_listing import _decision

    decided = [_decision("/products/b-424129.2", "B-424129.2", status="We sustain the protest."),
               _decision("/products/b-424130.1", "B-424130.1")]
    _, first = _run(tmp_path, monkeypatch, listed=[], decided=decided, with_decisions=True)
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    stale = pq.read_table(tmp_path / "gao_decisions.parquet").to_pylist()
    for row in stale:
        row.update(outcome="denied", outcome_rule="gao-decision-outcome/000")
    pq.write_table(pa.Table.from_pylist(stale), tmp_path / "_gao_decisions_prior.parquet")
    (tmp_path / "gao_decisions.parquet").unlink()
    _, carried = _run(tmp_path, monkeypatch, feed=["gao-26-2"], with_decisions=True)
    assert carried == first
    assert {row["decision_number"]: (row["outcome"], row["outcome_rule"]) for row in carried} == {
        "B-424129.2": ("sustained", GAO_OUTCOME_RULE), "B-424130.1": (None, None)}


def test_a_sentence_the_table_does_not_read_is_no_outcome_and_is_journaled(tmp_path, monkeypatch):
    class Evidence:
        def __init__(self):
            self.events = []

        def event(self, name, **fields):
            self.events.append((name, fields))

    from tests.test_gao_listing import _decision

    sentence = "We recommend that the agency reimburse the protester."
    decided = [_decision("/products/b-1.1", "B-1.1", status=sentence),
               _decision("/products/b-2.1", "B-2.1", status="We deny the protest.")]
    evidence = Evidence()
    _, rows = _run(tmp_path, monkeypatch, listed=[], decided=decided, with_decisions=True, evidence=evidence)
    assert {row["decision_number"]: row["outcome"] for row in rows} == {"B-1.1": None, "B-2.1": "denied"}
    [fields] = [fields for name, fields in evidence.events if name == "gao-decision-outcomes"]
    assert fields["rule"] == GAO_OUTCOME_RULE and fields["rows"] == 2 and fields["with_outcome"] == 1
    assert fields["unmapped_sentences"] == {sentence: 1}


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
    (name, fields), _outcomes = evidence.events
    assert name == "gao-listing" and fields["scopes_read"] == ["2026-08"] and fields["scopes_unfinished"] == []
    assert fields["rows"] == 2 and fields["already_held"] == 1 and fields["complete_scopes"] == 1


def test_the_rollup_reads_a_listing_walk_only_when_one_is_named(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups import gao_reports as rollup

    calls = []
    monkeypatch.setattr(rollup, "build_gao_reports", lambda *_, **kwargs: calls.append(kwargs["listing_run"]))
    assert rollup.GaoReportsRollup.outputs == ("gao_reports.parquet", "gao_decisions.parquet")
    assert rollup.GaoReportsRollup.added_tables == ("gao_decisions.parquet",)
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
