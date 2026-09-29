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


def test_a_bare_summary_digest_refuses_the_run_before_any_compare(tmp_path, scoped_119):
    first = run(tmp_path / "first", prior=None, feed=None, bulk=StubBulkAcquirer())
    schema = pq.read_schema(first["bill_summaries"])
    row = {name: None for name in schema.names} | {"bill_id": "119-hr-6028", "content_hash": "a" * 64}
    pq.write_table(pa.Table.from_pylist([row], schema=schema), tmp_path / "first" / "bill_summaries.parquet")
    with pytest.raises(ValueError, match="bare-hex content_hash"):
        run(tmp_path / "second", prior=tmp_path / "first", feed=None, bulk=StubBulkAcquirer())

