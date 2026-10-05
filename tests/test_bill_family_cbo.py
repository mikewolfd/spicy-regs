"""The bill family's host rules for spicy-docs 0.51.0 and 0.52.0: CBO's feed merged through the host, and stages.

``cbo_cost_estimates`` takes feed rows for each scoped Congress, merged so a BILLSTATUS row keeps its identity (this
run's or a prior one), a feed row carries its bill's own report citations (this run's status, else its published
rows), a law title reads through the published ``laws``, and a feed read replaces its Congress's prior feed rows.
A prior stage is re-derived under the running rule, so a rule change emits no ``stage_changed`` event.
"""

import importlib
import json
from pathlib import Path
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.cbo import parse_cbo_cost_estimates_feed
from spicy_docs.sources.congress.bill_status import BillIdentity, parse_bill_status

from tests.test_bill_family import (
    FIXTURES,
    StubBodyAcquirer,
    StubBulkAcquirer,
    _Archive,
    _Member,
    _priors,
    write_output,
    zip_entry,
)

build = importlib.import_module("spicy_regs.transforms.build_bill_family")
cbo = importlib.import_module("spicy_regs.transforms.bill_family_cbo")

H7643 = BillIdentity(118, "hr", 7643)


class Bill7643(StubBulkAcquirer):
    """Serves 118 H.R. 7643, whose BILLSTATUS lists CBO publication 60249 and two committee reports."""

    def acquire(self, congress, bill_type, **kwargs):
        result = super().acquire(congress, bill_type, **kwargs)
        if result.archive is not None and (congress, bill_type) == (118, "hr"):
            body = (FIXTURES / "status-118hr7643.xml").read_bytes()
            result.archive = _Archive([_Member(parse_bill_status(body, identity=H7643))])
        return result


def _item(key, title, publication, bill_number, date="Wed, 08 May 2024 16:00:00 -0400"):
    number = "" if bill_number is None else f"<Bill_Number>{bill_number}</Bill_Number>"
    return (
        f'<item key="{key}"><Title>{title}</Title><Date>{date}</Date>'
        f"<Link>https://www.cbo.gov/publication/{publication}</Link><Description>As ordered reported</Description>"
        f"{number}</item>"
    )


#: The BILLSTATUS estimate again (60249), one BILLSTATUS does not list (60300), a blank item titled by a law, and
#: an estimate of a bill this run does not read.
ITEMS = {
    "billstatus": _item(0, "H.R. 7643, Veterans Congressional Work Study Act of 2024", 60249, "H.R. 7643"),
    "feed_only": _item(1, "H.R. 7643, Veterans Congressional Work Study Act of 2024", 60300, "H.R. 7643",
                       date="Thu, 09 May 2024 16:00:00 -0400"),
    "law": _item(2, "Public Law 118-5, An act to extend a program", 60400, None),
    "other_bill": _item(3, "H.R. 12, An act about something else", 60500, "H.R. 12"),
}


class StubCbo:
    """Serves one Congress's feed from ``items``; ``requested`` records each Congress asked for."""

    def __init__(self, *items):
        self.items = items
        self.requested: list[int] = []

    def acquire_per_congress_feed(self, congress, **_arguments):
        self.requested.append(congress)
        body = ("<?xml version='1.0'?><response>" + "".join(self.items) + "</response>").encode()
        return SimpleNamespace(feed=parse_cbo_cost_estimates_feed(body))


@pytest.fixture
def scoped_118(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)


def _laws(directory: Path) -> None:
    """A published ``laws`` naming 118 H.R. 9999 as the bill that became P.L. 118-5."""
    directory.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"law_id": ["118-public-5"], "bill_id": ["118-hr-9999"]}), directory / "laws.parquet")


def run(directory: Path, *, prior: Path | None, feed, bulk=None) -> dict[str, Path]:
    directory.mkdir()
    paths = build.build_bill_family(
        directory,
        bulk_acquirer=bulk or Bill7643(),
        body_acquirer=StubBodyAcquirer(),
        cbo_acquirer=feed,
        max_version_fetches=0,
        **_priors(prior),
    )
    return {path.stem: path for path in paths}


def estimates(paths):
    return {(row["bill_id"], row["publication_id"]): row for row in pq.read_table(paths["cbo_cost_estimates"]).to_pylist()}


def test_feed_rows_merge_through_the_host(tmp_path, scoped_118):
    """BILLSTATUS keeps its identity; the feed adds what it alone states, with the bill's citations and the law map."""
    _laws(tmp_path / "prior")
    feed = StubCbo(*ITEMS.values())
    rows = estimates(run(tmp_path / "first", prior=tmp_path / "prior", feed=feed))
    assert feed.requested == [118], "each scoped Congress's feed is read once"
    assert set(rows) == {
        ("118-hr-7643", "60249"),
        ("118-hr-7643", "60300"),
        ("118-hr-9999", "60400"),
        ("118-hr-12", "60500"),
    }
    listed = rows[("118-hr-7643", "60249")]
    assert (listed["source"], listed["found_by"], listed["title_bill_id"]) == ("billstatus_bulk", "billstatus", "118-hr-7643")
    added = rows[("118-hr-7643", "60300")]
    assert (added["source"], added["found_by"]) == ("cbo_feed", "bill_number")
    assert added["report_citations_json"] == listed["report_citations_json"] and added["report_citation_count"] == "2"
    law = rows[("118-hr-9999", "60400")]
    assert (law["found_by"], law["title_bill_id"]) == ("title_law", "118-hr-9999"), "the published laws name the bill"
    unread = rows[("118-hr-12", "60500")]
    assert unread["report_citation_count"] is None, "a bill no status and no published row names has no citations"
    assert {row["title_bill_id_rule"] for row in rows.values()} == {"cbo_title_citation/1"}


def test_a_later_feed_read_keeps_billstatus_precedence_citations_and_withdraws(tmp_path, scoped_118):
    """No status is read: the prior BILLSTATUS row still wins, a feed row keeps its citations, a withdrawn item leaves."""
    _laws(tmp_path / "prior")
    run(tmp_path / "first", prior=tmp_path / "prior", feed=StubCbo(*ITEMS.values()))
    later = StubCbo(ITEMS["billstatus"], ITEMS["feed_only"], ITEMS["law"])
    rows = estimates(run(tmp_path / "second", prior=tmp_path / "first", feed=later))
    assert rows[("118-hr-7643", "60249")]["source"] == "billstatus_bulk", "a prior BILLSTATUS row keeps its identity"
    assert rows[("118-hr-7643", "60300")]["report_citation_count"] == "2", "citations from the bill's published rows"
    assert ("118-hr-12", "60500") not in rows, "an item the feed no longer lists leaves with the feed read"


def test_an_unread_feed_keeps_its_prior_feed_rows(tmp_path, scoped_118):
    _laws(tmp_path / "prior")
    run(tmp_path / "first", prior=tmp_path / "prior", feed=StubCbo(*ITEMS.values()))
    unavailable = importlib.import_module("tests.conftest")._NoCboFeed()
    rows = estimates(run(tmp_path / "second", prior=tmp_path / "first", feed=unavailable))
    assert ("118-hr-12", "60500") in rows and rows[("118-hr-12", "60500")]["source"] == "cbo_feed"


def _suspension(key: int) -> str:
    """CBO's weekly suspension notice: an empty Bill_Number and no bill in its title (62589, 2026-09-08).

    A feed item's key is its position in the document, so the caller places it.
    """
    return _item(key, "Legislation Considered Under Suspension of the Rules, September 8, 2026", 62589, "",
                 date="Tue, 08 Sep 2026 10:00:00 -0400")


def _items(paths):
    return {row["publication_id"]: row for row in pq.read_table(paths["cbo_feed_items"]).to_pylist()}


def test_every_item_of_a_read_feed_is_a_cbo_feed_items_row_whether_or_not_it_names_a_bill(tmp_path, scoped_118):
    """spicy-docs 0.54.0's cbo_feed_items: the feed as CBO lists it, published beside the estimates it yields.

    An item naming no bill (a suspension-calendar notice) has no cbo_cost_estimates row, so before this table it
    left no trace; the anti-join on publication_id now finds it.
    """
    _laws(tmp_path / "prior")
    paths = run(tmp_path / "first", prior=tmp_path / "prior", feed=StubCbo(*ITEMS.values(), _suspension(4)))
    items = _items(paths)
    assert set(items) == {"60249", "60300", "60400", "60500", "62589"}
    assert pq.read_schema(paths["cbo_feed_items"]).names == list(TABLE_CONTRACTS["cbo_feed_items"].columns)
    assert (items["62589"]["bill_number"], items["62589"]["congress"]) == ("", "118")
    named = {row["publication_id"] for row in pq.read_table(paths["cbo_cost_estimates"]).to_pylist()}
    assert set(items) - named == {"62589"}, "the anti-join gives exactly the bill-less item"


def test_a_later_feed_read_replaces_its_congresss_items_and_an_unread_feed_keeps_them(tmp_path, scoped_118):
    _laws(tmp_path / "prior")
    run(tmp_path / "first", prior=tmp_path / "prior", feed=StubCbo(*ITEMS.values(), _suspension(4)))
    later = run(tmp_path / "second", prior=tmp_path / "first", feed=StubCbo(ITEMS["billstatus"], _suspension(1)))
    assert set(_items(later)) == {"60249", "62589"}, "an item the feed no longer lists leaves with the feed read"
    unavailable = importlib.import_module("tests.conftest")._NoCboFeed()
    kept = run(tmp_path / "third", prior=tmp_path / "second", feed=unavailable)
    assert _items(kept) == _items(later)


def test_law_bills_reads_the_published_laws(tmp_path):
    _laws(tmp_path)
    assert cbo.law_bills_from(tmp_path / "laws.parquet") == {"118-public-5": "118-hr-9999"}
    assert cbo.law_bills_from(None) is None


# --------------------------------------------------------------------------- #
# Stage: a prior stage is read under the running rule before events compare.
# --------------------------------------------------------------------------- #


def _events(paths):
    return [row for row in pq.read_table(paths["public_activity_events"]).to_pylist() if row["event_type"] == "stage_changed"]


def _reread(prior: Path, *, stage: str | None = None, drop_first_action: bool = False) -> None:
    """Make the prior's 119 H.R. 6028 due for a status read, as a rule change or a new action would leave it."""
    bills = pq.read_table(prior / "congress_bills.parquet").to_pylist()
    for row in bills:
        row["update_date_including_text"] = "1999-01-01T00:00:00Z"
        if stage is not None:
            row["stage"] = stage
    write_output(prior / "congress_bills.parquet", pa.Table.from_pylist(bills, schema=pq.read_schema(prior / "congress_bills.parquet")))
    if drop_first_action:
        path = prior / "bill_actions.parquet"
        actions = [row for row in pq.read_table(path).to_pylist() if row["action_index"] != "0"]
        write_output(path, pa.Table.from_pylist(actions, schema=pq.read_schema(path)))


def moved() -> StubBulkAcquirer:
    """The folder's zip, moved since the last run, so its bills' stamps are compared and a changed one re-read."""
    return StubBulkAcquirer(entry=lambda c, t: zip_entry(c, t, size=31_658_670))


@pytest.fixture
def scoped_119(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)


def test_a_rule_only_stage_move_emits_no_event(tmp_path, scoped_119):
    first = run(tmp_path / "first", prior=None, feed=None, bulk=StubBulkAcquirer())
    published = pq.read_table(first["congress_bills"]).to_pylist()[0]["stage"]
    assert published != "passed_chamber"
    _reread(tmp_path / "first", stage="passed_chamber")
    second = run(tmp_path / "second", prior=tmp_path / "first", feed=None, bulk=moved())
    assert pq.read_table(second["congress_bills"]).to_pylist()[0]["stage"] == published
    assert _events(second) == [], "the running rule reads the same actions as the same stage"


def test_a_real_stage_move_emits_one_event_from_the_running_rules_prior_stage(tmp_path, scoped_119):
    first = run(tmp_path / "first", prior=None, feed=None, bulk=StubBulkAcquirer())
    now = pq.read_table(first["congress_bills"]).to_pylist()[0]["stage"]
    before = build.infer_stage(
        [{"text": "Motion to reconsider laid on the table Agreed to without objection.", "actionCode": "H38310",
          "type": "Floor", "actionDate": "2026-06-08"}],
        newest_first=True,
    ).stage
    assert before != now, "the fixture's newest action moves its stage"
    _reread(tmp_path / "first", stage="passed_chamber", drop_first_action=True)
    events = _events(run(tmp_path / "second", prior=tmp_path / "first", feed=None, bulk=moved()))
    assert [json.loads(event["event_data_json"])["from"] for event in events] == [before]
    assert [json.loads(event["event_data_json"])["to"] for event in events] == [now]


#: Two bills' rows as bill family e264e62b published them under the round-5 rule (``fixtures/bill_family_held``).
HELD = Path(__file__).parent / "fixtures" / "bill_family_held" / "e264e62b-rows.json"


def test_held_rows_take_the_running_rules_stage_and_signing_date_without_a_read(tmp_path, monkeypatch):
    """Round 6 (M3): a held row is re-read from its stored actions every run, not when its Congress is read again.

    The run reads 119 H.J.Res. only, so neither bill's status is read. S. 240 was published `conference` from the
    "Resolving differences" category, no conference matcher since round 6; the Senate's agreement to the House
    amendment above it clears it. H.R. 3377 became Private Law 119-1, which round 6 dates from its coded action.
    """
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hjres")
    held = json.loads(HELD.read_text())
    prior = tmp_path / "prior"
    prior.mkdir()
    for table in ("congress_bills", "bill_actions"):
        pq.write_table(pa.Table.from_pylist(held[table]), prior / f"{table}.parquet")
    paths = run(tmp_path / "next", prior=prior, feed=None, bulk=StubBulkAcquirer())
    bills = {row["bill_id"]: row for row in pq.read_table(paths["congress_bills"]).to_pylist()}
    stage = ("stage", "stage_rule", "stage_matcher", "stage_action_index", "stage_action_date")
    assert tuple(bills["119-s-240"][c] for c in stage) == (
        "cleared", "cleared", "agreed to the other chamber's amendment", "1", "2026-09-24")
    assert bills["119-s-240"]["stage_source_text"].startswith("Senate agreed to the House amendment to S. 240")
    signing = ("signed_date", "signed_date_rule", "signed_date_action_index", "signed_date_action_code")
    assert tuple(bills["119-hr-3377"][c] for c in signing) == (
        "2026-03-26", "private_law_and_became_law_action", "0", "E40000")
    assert bills["119-hr-3377"]["public_law_number"] is None, "a private law states no public law number"
    actions = {(row["bill_id"], row["action_index"]): (row["stage"], row["stage_rule"], row["stage_matcher"])
               for row in pq.read_table(paths["bill_actions"]).to_pylist()}
    assert actions[("119-s-240", "2")] == ("cleared", "cleared", "agreed to the other chamber's amendment")
    assert actions[("119-hr-3377", "8")] == ("cleared", "cleared", "passed senate without amendment")
    published = {(row["bill_id"], row["action_index"]): (row["stage"], row["stage_rule"], row["stage_matcher"])
                 for row in held["bill_actions"]}
    assert {key for key in published if actions[key] != published[key]} == {
        ("119-s-240", "1"), ("119-s-240", "2"), ("119-hr-3377", "7"), ("119-hr-3377", "8")}


def test_restaging_moves_each_held_row_once_and_leaves_a_bill_no_rule_read(tmp_path):
    """A second pass moves nothing and rewrites nothing; 104 H.R. 517 (Public Law 104-11), a detail-route row with no
    stored action, keeps its NULL stage and signing rule rather than gaining a rule that never read it."""
    held = json.loads(HELD.read_text())
    bills, actions = tmp_path / "congress_bills.parquet", tmp_path / "bill_actions.parquet"
    pq.write_table(pa.Table.from_pylist(held["congress_bills"]), bills)
    pq.write_table(pa.Table.from_pylist(held["bill_actions"]), actions)
    assert build.restage_held_rows(bills, actions) == (2, 4)
    written = (bills.stat().st_mtime_ns, actions.stat().st_mtime_ns)
    assert build.restage_held_rows(bills, actions) == (0, 0)
    assert (bills.stat().st_mtime_ns, actions.stat().st_mtime_ns) == written, "nothing moved, so nothing is rewritten"
    detail = next(row for row in pq.read_table(bills).to_pylist() if row["bill_id"] == "104-hr-517")
    assert detail == next(row for row in held["congress_bills"] if row["bill_id"] == "104-hr-517")
    assert pq.read_table(bills).column_names == list(held["congress_bills"][0]), "columns keep their order"


def test_rederived_stages_fold_published_actions_in_publisher_order(tmp_path):
    rows = [
        {"bill_id": "119-hr-1", "action_index": "1", "action_text": "Referred to the House Committee on Rules.",
         "action_code": "H11100", "action_type": "IntroReferral", "action_date": "2026-01-01", "action_time": None},
        {"bill_id": "119-hr-1", "action_index": "0", "action_text": "Passed/agreed to in House: On passage Passed by voice vote.",
         "action_code": "8000", "action_type": "Floor", "action_date": "2026-01-01", "action_time": None},
    ]
    path = tmp_path / "bill_actions.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    assert build.rederived_stages(path) == {"119-hr-1": "passed_chamber"}, "index 0 is the newest of one day"
    assert build.rederived_stages(path, {"119-hr-2"}) == {}
    assert build.rederived_stages(None) == {}


def test_rederived_stages_read_published_rows_as_the_build_reads_bills(tmp_path):
    """A published row passes as it is: its chamber's system and the bill's own type reach the stage rule (DRY A1).

    A Senate bill the House holds at the desk is in the other chamber only when the rule knows the bill is a
    Senate bill and that the House's system entered "Held at the desk." (it names neither chamber). Renaming the
    rows into BILLSTATUS keys and dropping both read it ``passed_chamber``, so every such bill raised a false
    ``stage_changed`` event against the stage the build publishes.
    """
    from spicy_docs.interpretation.bill_stage import infer_stage

    rows = [
        {"bill_id": "119-s-12", "action_index": "0", "action_text": "Held at the desk.", "action_code": None,
         "action_type": "Floor", "action_date": "2026-03-05", "action_time": None,
         "source_system_name": "House floor actions"},
        {"bill_id": "119-s-12", "action_index": "1",
         "action_text": "Passed Senate without amendment by Unanimous Consent.", "action_code": "17000",
         "action_type": "Floor", "action_date": "2026-03-04", "action_time": None,
         "source_system_name": "Library of Congress"},
        {"bill_id": "119-s-12", "action_index": "2", "action_text": "Introduced in Senate", "action_code": "10000",
         "action_type": "IntroReferral", "action_date": "2026-03-01", "action_time": None,
         "source_system_name": "Library of Congress"},
    ]
    path = tmp_path / "bill_actions.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    published = infer_stage([{k: v for k, v in row.items() if k != "bill_id"} for row in rows], newest_first=True,
                            bill_type="s").stage
    assert published == "other_chamber"
    assert build.rederived_stages(path) == {"119-s-12": published}


def test_a_bare_summary_digest_refuses_the_run_before_any_compare(tmp_path, scoped_119):
    first = run(tmp_path / "first", prior=None, feed=None, bulk=StubBulkAcquirer())
    schema = pq.read_schema(first["bill_summaries"])
    row = {name: None for name in schema.names} | {"bill_id": "119-hr-6028", "content_hash": "a" * 64}
    pq.write_table(pa.Table.from_pylist([row], schema=schema), tmp_path / "first" / "bill_summaries.parquet")
    with pytest.raises(ValueError, match="bare-hex content_hash"):
        run(tmp_path / "second", prior=tmp_path / "first", feed=None, bulk=StubBulkAcquirer())

