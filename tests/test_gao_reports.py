"""GAO output field mapping, source labels and merge; provider feed validation is tested in test_reference_source_failures."""

from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path

from spicy_docs.interpretation.gao_decisions import GAO_OUTCOME_RULE
from spicy_docs.schemas.gao_decision_tables import GAO_DECISIONS

import pyarrow as pa
import pyarrow.parquet as pq
from tests.government_fakes import literal_table
from spicy_regs.transforms.government_source_shapes import SUBJECT_SCHEMAS

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
    assert len(COLUMNS) == 23
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
         with_decisions=False, others=(), pages=None, stated=None, **more):
    """One build over a local prior and a stubbed feed; ``history`` is the GovInfo listing and ``listed`` GAO's own
    listing's products, each None for no read. ``stated`` is what the major-rule listings merged into ``listed``
    state beyond rows; ``more`` goes to the builder."""
    if prior is not None:
        pq.write_table(prior, tmp_path / "_gao_prior.parquet")

    class Feed:
        def __init__(self, **_):
            pass

        def iter_records(self):
            return iter({"product_id": i, "title": f"Feed {i}", "link": f"https://www.gao.gov/products/{i}",
                         "description": "What GAO Found.", "pub_date": "Mon, 21 Sep 2026 10:00:00 -0400"} for i in feed)

    def read_listing(directory, evidence, *, major_rules=None, old_index=None):
        assert directory == tmp_path / "walk"
        assert (major_rules, old_index) == (more.get("major_rule_run"), more.get("old_index_run"))
        run = _listing(*(listed or ()), decisions=decided or (), others=others)
        rows, counts = module.gao_listing.listing_rows(run)
        return rows, counts, run, stated or module.gao_listing.MajorRuleReports()

    monkeypatch.setattr(module, "GaoReportsReader", Feed)
    monkeypatch.setattr(module.r2, "download", lambda *_: False)
    monkeypatch.setattr(module.gao_listing, "read_listing", read_listing)
    out = build_gao_reports(tmp_path, govinfo_history=history is not None,
                            govinfo=ListingReader(history) if history is not None else None,
                            listing_run=tmp_path / "walk" if listed is not None else None, evidence=evidence,
                            decision_pages=pages, **more)
    out, decisions = out
    assert pq.read_schema(out).equals(SUBJECT_SCHEMAS['gao_reports'])
    assert pq.read_schema(decisions).equals(SUBJECT_SCHEMAS['gao_decisions'])
    table = literal_table(out).select(COLUMNS)
    rows = {row["report_id"]: row for row in table.to_pylist()}
    if with_decisions:
        return rows, literal_table(decisions).select(GAO_DECISIONS.columns).to_pylist()
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
    published = literal_table(tmp_path / "gao_decisions.parquet").select(GAO_DECISIONS.columns)
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
    stale = literal_table(tmp_path / "gao_decisions.parquet").select(GAO_DECISIONS.columns).to_pylist()
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

        def retain_file(self, path, **fields):
            pass

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

        def retain_file(self, path, **fields):
            assert path.is_file()

    evidence = Evidence()
    _run(tmp_path, monkeypatch, feed=["gao-26-1"], listed=[_product("gao-26-1"), _product("gao-12-100")],
         evidence=evidence)
    (name, fields), = [event for event in evidence.events if event[0] == "gao-listing"]
    assert name == "gao-listing" and fields["scopes_read"] == ["2026-08"] and fields["scopes_unfinished"] == []
    assert fields["rows"] == 2 and fields["already_held"] == 1 and fields["complete_scopes"] == 1


def test_the_rollup_reads_a_listing_walk_and_a_page_capture_only_when_one_is_named(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups import gao_reports as rollup

    calls = []
    monkeypatch.setattr(rollup, "build_gao_reports",
                        lambda *_, **kwargs: calls.append((kwargs["listing_run"], kwargs["decision_pages"])))
    assert rollup.GaoReportsRollup.outputs == ("gao_reports.parquet", "gao_decisions.parquet")
    assert rollup.GaoReportsRollup.added_tables == ("gao_decisions.parquet",)
    for walk, pages in (("", ""), (str(tmp_path / "walk"), ""), (str(tmp_path / "walk"), str(CAPTURE))):
        monkeypatch.setenv("GAO_LISTING_RUN", walk)
        monkeypatch.setenv("GAO_DECISION_PAGES", pages)
        rollup.GaoReportsRollup(output_dir=tmp_path).build(tmp_path)
    assert calls == [(None, None), (tmp_path / "walk", None), (tmp_path / "walk", CAPTURE)]


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


# Round 6 (L2 and the owner's instruction): every decision's page, read by reference from the local capture.

CAPTURE = Path(__file__).parent / "fixtures/gao_decision_pages"


def _captured_decisions():
    """The four captured decisions as GAO's listing states them (numbers split by spicy-docs, with its cut flag)."""
    from spicy_docs.sources.gao.month_in_review import decision_numbers

    from tests.test_gao_listing import _decision

    listed = []
    for line in (CAPTURE / "targets.jsonl").read_text().splitlines():
        target = json.loads(line)
        numbers, cut = decision_numbers(target["decision_number"])
        listed.append(_decision(target["url"].removeprefix("https://www.gao.gov"), target["decision_number"],
                                numbers=numbers, cut=cut, released=target["released_date"]))
    return listed


def _by_number(rows):
    return {row["decision_number"].split(",")[0]: row for row in rows}


def test_a_decision_takes_its_whole_list_and_decided_day_from_its_captured_page(tmp_path, monkeypatch):
    """The caption's File list completes a list the listing cut and its Date is the day GAO decided; a page with no
    caption, a refused page and a page never captured keep the listing's values."""
    _, rows = _run(tmp_path, monkeypatch, listed=[], decided=_captured_decisions(), with_decisions=True,
                   pages=CAPTURE)
    got = {number: (len(json.loads(row["b_numbers_json"])), row["b_numbers_truncated"], row["decided_date"])
           for number, row in _by_number(rows).items()}
    assert got == {
        "B-403174": (8, "false", "2010-10-07"),  # the listing kept 7; the page adds B-403649
        "B-412940": (22, "true", None),  # its page carries no decision text, so the list stays as cut
        "B-420269": (2, "false", None),  # File 'B-420269.1, 420269.2' is refused
        "B-419305.3": (1, "false", None),  # Zyte answered 520 on every attempt: no page held
    }
    assert json.loads(_by_number(rows)["B-403174"]["b_numbers_json"])[-1] == "B-403649"


def test_the_run_journal_names_the_capture_and_each_page_it_could_not_read(tmp_path, monkeypatch):
    from spicy_regs.source_evidence import CaptureEvidence

    evidence = CaptureEvidence(tmp_path / "evidence", "gao-reports")
    _run(tmp_path, monkeypatch, listed=[], decided=_captured_decisions(), with_decisions=True, pages=CAPTURE,
         evidence=evidence)
    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    [event] = [entry for entry in journal if entry["event"] == "gao-decision-pages"]
    assert (event["campaign"], event["read"], event["no_caption"], event["not_held"]) == (CAPTURE.name, 1, 1, 1)
    assert event["refused"] == [{"url": "https://www.gao.gov/products/b-420269%2Cb-420269.2",
                                 "reason": "unreadable-file-line"}]


def test_the_capture_is_an_input_of_the_generation_by_the_receipts_it_read(tmp_path, monkeypatch):
    """Recorded through the read ledger, as every other input is: its campaign, and a digest over the receipt lines
    of the pages read (so a later retry appending to receipts.jsonl does not move it)."""
    import hashlib

    held = sorted((json.loads(line)["url"], line) for line in (CAPTURE / "receipts.jsonl").read_text().splitlines()
                  if json.loads(line).get("status_code") == 200)
    read = "".join(line + "\n" for _, line in held).encode()
    with module.r2.recorded_reads() as reads:
        _run(tmp_path, monkeypatch, listed=[], decided=_captured_decisions(), with_decisions=True, pages=CAPTURE)
    assert reads == {CAPTURE.name: {"sha256": "sha256:" + hashlib.sha256(read).hexdigest(), "byteSize": len(read)}}


def test_without_the_capture_a_run_keeps_the_listings_values(tmp_path, monkeypatch):
    _, rows = _run(tmp_path, monkeypatch, listed=[], decided=_captured_decisions(), with_decisions=True)
    got = {number: (len(json.loads(row["b_numbers_json"])), row["b_numbers_truncated"], row["decided_date"])
           for number, row in _by_number(rows).items()}
    assert got == {"B-403174": (7, "true", None), "B-412940": (22, "true", None), "B-420269": (2, "false", None),
                   "B-419305.3": (1, "false", None)}


def test_a_walk_without_the_capture_keeps_what_an_earlier_run_read_from_a_page(tmp_path, monkeypatch):
    """A page's File list and Date are facts the listing never restates, so a later walk that lists the decision
    again without the capture keeps them rather than falling back to the listing's cut list and a NULL day."""
    _run(tmp_path, monkeypatch, listed=[], decided=_captured_decisions(), with_decisions=True, pages=CAPTURE)
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    (tmp_path / "gao_decisions.parquet").rename(tmp_path / "_gao_decisions_prior.parquet")
    _, rows = _run(tmp_path, monkeypatch, listed=[], decided=_captured_decisions(), with_decisions=True)
    row = _by_number(rows)["B-403174"]
    assert (len(json.loads(row["b_numbers_json"])), row["b_numbers_truncated"], row["decided_date"]) == (
        8, "false", "2010-10-07")
    assert _by_number(rows)["B-412940"]["b_numbers_truncated"] == "true"


# The major-rule reports before 2009 and the letter columns (owner decisions 2026-10-04).

MAJOR = "Federal Agency Major Rule Report"


def _listing_row(report_id: str, **cells) -> dict:
    return {**_feed_row(report_id), "abstract": None, "agencies_json": None, "topics_json": '["Health Care"]',
            "source": "gao_listing", "product_type": MAJOR, "report_number": report_id.upper(), **cells}


class _Journal:
    def __init__(self):
        self.events = {}

    def event(self, name, **fields):
        self.events[name] = fields

    def capture(self, capture, *, stage):
        pass

    def retain_file(self, path, **fields):
        pass


def test_the_listing_makes_whole_a_title_its_own_row_holds_cut_and_no_other(tmp_path, monkeypatch):
    """The Month in Review cuts a heading at about 200 characters; GAO's major-rule listing states it whole."""
    whole = f"{MAJOR}: Department of Health: A Rule Whose Heading Runs On Past the Cut"
    cut = whole[:50] + "..."
    prior = pa.Table.from_pylist([
        _listing_row("b-331093", title=cut),
        _listing_row("b-331094", title=cut),  # the listing states another heading: not this one's cut
        _listing_row("gao-09-707sp", title=cut, product_type=None),  # cut in the listing read again: still cut
        {**_feed_row("gao-26-1"), "title": cut},  # the feed's row, not the listing's
    ], schema=module._SCHEMA)
    journal = _Journal()
    rows = _run(tmp_path, monkeypatch, prior=prior, evidence=journal, listed=[
        _product("b-331093", title=whole), _product("b-331094", title=f"{MAJOR}: Another Heading Altogether"),
        _product("gao-09-707sp", title=cut), _product("gao-26-1", title=whole)])
    assert {key: row["title"] for key, row in rows.items()} == {
        "b-331093": whole, "b-331094": cut, "gao-09-707sp": cut, "gao-26-1": cut}
    assert rows["b-331093"] == {**dict.fromkeys(COLUMNS), **_listing_row("b-331093", title=whole)}
    assert journal.events["gao-listing"]["titles_made_whole"] == 1


def test_a_report_only_a_major_rule_listing_states_joins_and_takes_a_package_row_over_keeping_its_cells(
        tmp_path, monkeypatch):
    """The listing states no topic, so the R package's topic stands; so do its counts and its summary."""
    package = {**dict.fromkeys(COLUMNS), "report_id": "ogc-00-70", "title": "Social Security Administration: A Rule",
               "report_type": "Report", "published_date": "2000-09-26", "abstract": "Package summary.",
               "topics_json": '["Worker and Family Assistance"]', "url": "https://www.gao.gov/products/ogc-00-70",
               "source": "gao_r_package", "report_number": "OGC-00-70", "recommendation_count": 0, "page_count": 4}
    prior = pa.Table.from_pylist([package], schema=module._SCHEMA)
    listed = [
        _product("ogc-00-70", label=MAJOR, title=f"{MAJOR}: Social Security Administration: A Rule", topics=(),
                 released=None, published="2000-09-26", scopes=("majrule-index-2000-12-15",)),
        _product("gao-01-193r", label=MAJOR, topics=(), released=None, published="2000-11-27",
                 scopes=("reports-on-major-rules",)),
        _product("aimd-00-159r", label=None, title="An Audit Review", topics=(), released=None,
                 published="2000-05-05", scopes=("reports-on-major-rules",)),
    ]
    rows = _run(tmp_path, monkeypatch, prior=prior, listed=listed)
    taken = rows["ogc-00-70"]
    assert (taken["source"], taken["product_type"], taken["title"]) == (
        "gao_major_rule_index", MAJOR, f"{MAJOR}: Social Security Administration: A Rule")
    assert (taken["topics_json"], taken["abstract"], taken["recommendation_count"], taken["page_count"]) == (
        '["Worker and Family Assistance"]', "Package summary.", 0, 4)
    new = rows["gao-01-193r"]
    assert (new["source"], new["product_type"], new["published_date"], new["topics_json"]) == (
        "gao_major_rule_listing", MAJOR, "2000-11-27", None)
    assert (rows["aimd-00-159r"]["product_type"], rows["aimd-00-159r"]["title"]) == (None, "An Audit Review")


def test_a_report_gaos_major_rule_listings_state_is_labelled_whatever_route_holds_its_row(tmp_path, monkeypatch):
    """The owner's "trust GAO's listing": GovInfo's MODS calls GAO-01-1024R Correspondence, and GAO lists it among
    its reports on major rules. The row stays GovInfo's in every other cell."""
    held = {**dict.fromkeys(COLUMNS), "report_id": "gao-01-1024r", "title": "Accuracy of Information in the Agenda",
            "report_type": "Report", "published_date": "2001-07-27", "url": "https://www.govinfo.gov/x",
            "source": "govinfo", "product_type": "Correspondence", "report_number": "GAO-01-1024R"}
    other = {**held, "report_id": "gao-01-999r", "report_number": "GAO-01-999R"}
    prior = pa.Table.from_pylist([held, other], schema=module._SCHEMA)
    journal = _Journal()
    rows = _run(tmp_path, monkeypatch, prior=prior, evidence=journal,
                listed=[_product("gao-01-1024r", label=MAJOR), _product("gao-01-999r", label=MAJOR)],
                stated=module.gao_listing.MajorRuleReports(numbers={"gao-01-1024r": "GAO-01-1024R"}))
    assert rows["gao-01-1024r"] == {**held, "product_type": MAJOR}
    # A row the Month in Review alone lists keeps the type its own route gave it, as before.
    assert rows["gao-01-999r"] == other
    assert journal.events["gao-listing"]["major_rule_labelled"] == 1


def test_the_major_rule_listings_are_refused_without_a_month_in_review_walk(tmp_path, monkeypatch):
    """Their rows are merged into the Month in Review's, whose rows stand first; alone they would not be."""
    import pytest

    for walk in ("major_rule_run", "old_index_run"):
        with pytest.raises(ValueError, match="read with a Month in Review walk"):
            build_gao_reports(tmp_path, **{walk: tmp_path / "walk"})


def _letter_capture(tmp_path, *product_ids):
    from tests.test_gao_major_rule_letters import PRODUCTS, capture, page

    return capture(tmp_path / "letters-capture", {PRODUCTS + product_id: page(product_id) for product_id in product_ids})


def test_each_major_rule_report_takes_its_letters_columns_once_and_later_runs_carry_them(tmp_path, monkeypatch):
    letters = _letter_capture(tmp_path, "gao-04-193r", "gao-01-300r", "b-330560")
    listed = [_product(product_id, label=MAJOR) for product_id in ("gao-04-193r", "gao-01-300r", "b-330560")]
    listed.append(_product("gao-26-1"))  # no major-rule report: no letter is looked for
    listed.append(_product("gao-17-392r", label=MAJOR))  # a major-rule report whose page the capture lacks
    journal = _Journal()
    first = _run(tmp_path, monkeypatch, listed=listed, major_rule_letters=(letters,), evidence=journal)
    assert (first["gao-04-193r"]["major_rule_agency"], first["gao-04-193r"]["major_rule_rins_json"],
            first["gao-04-193r"]["major_rule_fr_citations_json"]) == (
        "Department of Health and Human Services, Food and Drug Administration (FDA)", '["0910-AC40"]', '["68-58894"]')
    # A blank is NULL, and the row's receipt field says why.
    assert first["gao-01-300r"]["major_rule_rins_json"] is None
    assert json.loads(first["gao-01-300r"]["major_rule_letter_json"])["rins"]["status"] == "not-stated"
    assert json.loads(first["b-330560"]["major_rule_letter_json"])["reason"] == "not-this-letter"
    assert first["gao-26-1"]["major_rule_letter_json"] is None and first["gao-17-392r"]["major_rule_letter_json"] is None
    event = journal.events["gao-major-rule-letters"]
    assert (event["reports"], event["read"], event["refused"], event["page_not_held"]) == (4, 2, 1, 1)
    assert (event["with_major_rule_agency"], event["with_major_rule_rins_json"], event["rins_not-stated"]) == (2, 1, 1)
    assert [entry["campaign"] for entry in event["captures"]] == [f"{tmp_path.name}/letters-capture"]

    # The published columns are native: one string and two lists, the readings only in the receipt.
    table = pq.read_table(tmp_path / "gao_reports.parquet")
    assert "major_rule_letter_json" not in table.column_names
    by_id = {row["report_id"]: row for row in table.to_pylist()}
    assert by_id["gao-04-193r"]["major_rule_rins"] == ["0910-AC40"]
    assert by_id["gao-04-193r"]["major_rule_fr_citations"] == ["68-58894"]
    assert by_id["gao-01-300r"]["major_rule_rins"] is None

    # The daily run, with no walk and no capture, carries every cell and reading forward.
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    carried = _run(tmp_path, monkeypatch, feed=["gao-26-2"])
    assert {key: carried[key] for key in first} == first

    # A later run with the capture reads only what the current rule has not read.
    (tmp_path / "gao_reports.parquet").rename(tmp_path / "_gao_prior.parquet")
    again = _Journal()
    rows = _run(tmp_path, monkeypatch, listed=listed, major_rule_letters=(letters,), evidence=again)
    event = again.events["gao-major-rule-letters"]
    assert (event["reports"], event["already_read"], event["page_not_held"], event.get("read", 0)) == (4, 3, 1, 0)
    assert {key: rows[key] for key in first} == first


def test_a_report_the_old_index_walked_is_read_from_the_walks_own_page_on_whatever_row_holds_it(tmp_path, monkeypatch):
    """The 1996-2000 reports' pages are the old index walk's; GovInfo holds a few of those reports' rows."""
    from tests.test_gao_major_rule_letters import PRODUCTS, page

    held = {**dict.fromkeys(COLUMNS), "report_id": "ogc-97-44", "title": "GAO Report from GAO/OGC-97-44",
            "report_type": "Report", "published_date": "1997-05-21", "url": "https://www.govinfo.gov/x",
            "source": "govinfo", "product_type": "Other Written Product", "report_number": "GAO/OGC-97-44"}
    prior = pa.Table.from_pylist([held], schema=module._SCHEMA)
    stated = module.gao_listing.MajorRuleReports(numbers={"ogc-97-44": "OGC-97-44"},
                                                 pages={PRODUCTS + "ogc-97-44": page("ogc-97-44")})
    rows = _run(tmp_path, monkeypatch, prior=prior, stated=stated,
                listed=[_product("ogc-97-44", label=MAJOR, scopes=("majrule-index-2000-12-15",))])
    row = rows["ogc-97-44"]
    # GovInfo's row, labelled on the listing's word; the letter is read under the number the listing states.
    assert (row["source"], row["title"], row["report_number"], row["product_type"]) == (
        "govinfo", "GAO Report from GAO/OGC-97-44", "GAO/OGC-97-44", MAJOR)
    assert json.loads(row["major_rule_fr_citations_json"]) == ["62-24746", "61-52190"]
    assert json.loads(row["major_rule_rins_json"]) == ["0579-AA83"]


def test_the_rollup_reads_the_major_rule_walks_and_letters_only_when_they_are_named(tmp_path, monkeypatch):
    import os

    from spicy_regs.pipelines.rollups import gao_reports as rollup

    calls = []
    monkeypatch.setattr(rollup, "build_gao_reports", lambda *_, **kwargs: calls.append(
        (kwargs["major_rule_run"], kwargs["old_index_run"], kwargs["major_rule_letters"], kwargs["product_pages"])))
    rollup.GaoReportsRollup(output_dir=tmp_path).build(tmp_path)
    monkeypatch.setenv("GAO_MAJOR_RULE_RUN", str(tmp_path / "cra"))
    monkeypatch.setenv("GAO_MAJOR_RULE_OLD_INDEX_RUN", str(tmp_path / "early"))
    monkeypatch.setenv("GAO_MAJOR_RULE_LETTERS", os.pathsep.join([str(tmp_path / "letters"), str(tmp_path / "pdfs")]))
    rollup.GaoReportsRollup(output_dir=tmp_path).build(tmp_path)
    assert calls == [(None, None, (), False),
                     (tmp_path / "cra", tmp_path / "early", (tmp_path / "letters", tmp_path / "pdfs"), False)]
