"""Hermetic tests for the per-bill subject enrichment (no network).

Covers the pieces with real logic: the route each Congress takes (the bill
family's own BILLSTATUS rows, a folder's bulk zip read through spicy-docs, the
Congress.gov page walk), and the three outcomes the transform depends on — an
answer, a definitive "not held", and a failure that must leave the bill for the
next run rather than pin an empty or truncated answer to it.
"""

from __future__ import annotations

import io
import json
import zipfile

import httpx

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from loguru import logger
from spicy_docs.sources.congress.bill_acquisition import BillSourceUnavailableError
from spicy_docs.sources.congress.bill_status import BillSourceError
from spicy_docs.sources.congress.bulk_status import read_bulk_status_archive
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.sources.bill_subjects import (
    API_HOURLY_BUDGET,
    CARRIER_API,
    CARRIER_BULKDATA,
    DELAY_SECONDS,
    BillSubjects,
    BillSubjectsFetcher,
    FetchCounts,
    assignment,
)
from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS
from spicy_regs.transforms.enrich_bill_subjects import (
    COLUMNS,
    MAX_API_BILLS_PER_RUN,
    _pending_bills,
    _shape,
    enrich_bill_subjects,
)

# A BILLSTATUS record in the shape GPO actually serves: <policyArea> appears
# both as a direct child of <bill> and again inside <subjects>, and a term can
# repeat across containers.
_BILLSTATUS = """<?xml version="1.0" encoding="UTF-8"?>
<billStatus>
  <version>3.0.0</version>
  <bill>
    <number>{number}</number>
    <title>Synthetic bill for subject receiver tests</title>
    <congress>{congress}</congress>
    <type>HR</type>
    <policyArea><name>Environmental Protection</name></policyArea>
    <subjects>
      <legislativeSubjects>
        <item><name>Air quality</name></item>
        <item><name>  Congressional  oversight </name></item>
        <item><name>Air quality</name></item>
      </legislativeSubjects>
      <policyArea>
        <name>Environmental Protection</name>
        <updateDate>2024-01-02T00:00:00Z</updateDate>
      </policyArea>
    </subjects>
  </bill>
</billStatus>
"""

# The superseded schema the publisher still serves for a few reserved numbers
# (117 hr 9 and 13-17 on 2026-09-23); the shared reader refuses it by name.
_BILLSTATUS_1_0_0 = """<billStatus><bill><billType>HR</billType><billNumber>{number}</billNumber>
<congress>{congress}</congress><title>Reserved for the Speaker.</title></bill></billStatus>"""

_FAMILY_COLUMNS = ("bill_id", "congress", "bill_type", "bill_number", "schema_version", "policy_area", "subjects_json")


def _keyless(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def _bill(bill_id, *, family=None):
    """One congress_bills row; ``family`` is the (policy_area, subjects) BILLSTATUS filled it with."""
    congress, bill_type, number = bill_id.split("-")
    row = dict(bill_id=bill_id, congress=congress, bill_type=bill_type, bill_number=number)
    if family is not None:
        policy_area, subjects = family
        row |= dict(schema_version="3.0.0", policy_area=policy_area, subjects_json=json.dumps(subjects))
    return row


def _write_bills(path, rows):
    """Write a congress_bills.parquet fixture with the family columns this transform reads."""
    schema = pa.schema([(c, pa.string()) for c in _FAMILY_COLUMNS])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _zip(congress, members):
    """A BILLSTATUS folder zip: ``members`` maps bill number to XML template."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for number, template in members.items():
            archive.writestr(f"BILLSTATUS-{congress}hr{number}.xml", template.format(congress=congress, number=number))
    return buffer.getvalue()


class _Folders:
    """Serves retained folder zips through the real spicy-docs archive reader, counting reads."""

    def __init__(self, zips=None, errors=None):
        self.zips = zips or {}
        self.errors = errors or {}
        self.read: list[tuple[int, str]] = []

    def __call__(self, congress, bill_type):
        self.read.append((congress, bill_type))
        if (congress, bill_type) in self.errors:
            raise self.errors[(congress, bill_type)]
        return read_bulk_status_archive(self.zips[(congress, bill_type)], congress=congress, bill_type=bill_type)


class _StubFetcher:
    """Answers the API route from a canned map; anything unmapped is a transport failure."""

    def __init__(self, answers=None):
        self.answers = answers or {}
        self.asked: list[str] = []

    def subjects_for(self, congress, bill_type, bill_number):
        key = f"{congress}-{bill_type}-{bill_number}"
        self.asked.append(key)
        return self.answers.get(key)

    def close(self):
        pass


def _rows(path):
    return {row["bill_id"]: row for row in pq.read_table(path).to_pylist()}


# -- the published shape and cleanup ------------------------------------------


def test_shape_produces_exact_schema():
    row = _shape("118-hr-1", "Health", ("Medicare",), CARRIER_API, "2026-08-22T00:00:00+00:00")
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 6
    assert row["subjects_json"] == '["Medicare"]'
    assert row["subject_count"] == "1"
    assert row["carrier"] == CARRIER_API


def test_assignment_trims_drops_blanks_and_dedups_in_first_seen_order():
    result = assignment("  Health ", ["B", " A  a ", None, "", "B", "A a"], CARRIER_BULKDATA)
    assert result == BillSubjects("Health", ("B", "A a"), CARRIER_BULKDATA)
    assert assignment(" ", [], CARRIER_API).policy_area is None


def test_every_result_lands_in_exactly_one_bucket():
    counts = FetchCounts()
    for result in (
        BillSubjects("Health", ("Medicare",), CARRIER_API),
        BillSubjects(None, ("Medicare",), CARRIER_API),
        BillSubjects(None, (), CARRIER_API),
        BillSubjects(None, (), CARRIER_API, held=False),
        BillSubjects("Health", (), CARRIER_BULKDATA),
        None,
    ):
        counts.record(result)
    assert (counts.with_policy_area, counts.subjects_only, counts.unassigned, counts.not_held) == (2, 1, 1, 1)
    assert (counts.answered, counts.failed) == (5, 1)
    assert counts.policy_areas == {"Health": 2}


# -- routes: the family's rows, a folder zip, or waiting ----------------------


def test_family_rows_are_projected_without_any_request(tmp_path):
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [_bill("119-hr-1", family=("Health", [" Medicare ", "Drug safety", "Medicare"]))],
    )
    folders, fetcher = _Folders(), _StubFetcher()
    out = enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=fetcher)
    row = _rows(out)["119-hr-1"]
    assert (row["policy_area"], json.loads(row["subjects_json"]), row["subject_count"], row["carrier"]) == (
        "Health",
        ["Medicare", "Drug safety"],
        "2",
        CARRIER_BULKDATA,
    )
    assert folders.read == [] and fetcher.asked == []


def test_a_folder_the_family_has_not_read_comes_from_its_zip_once(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill(f"118-hr-{n}") for n in (1, 2, 3, 4)])
    folders = _Folders({(118, "hr"): _zip(118, {1: _BILLSTATUS, 2: _BILLSTATUS, 3: _BILLSTATUS_1_0_0})})
    out = enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=_StubFetcher())
    rows = _rows(out)
    assert folders.read == [(118, "hr")]
    assert json.loads(rows["118-hr-1"]["subjects_json"]) == ["Air quality", "Congressional oversight"]
    assert rows["118-hr-2"]["policy_area"] == "Environmental Protection"
    # A member the reader refuses is no answer; a bill the zip does not list is not held.
    assert "118-hr-3" not in rows
    assert (rows["118-hr-4"]["policy_area"], rows["118-hr-4"]["subjects_json"]) == (None, "[]")


def test_a_list_level_bill_in_a_folder_the_family_reads_waits_for_the_family(tmp_path):
    _write_bills(
        tmp_path / "congress_bills.parquet",
        [_bill("119-hr-1", family=("Health", [])), _bill("119-hr-2")],
    )
    folders = _Folders()
    out = enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=_StubFetcher())
    assert set(_rows(out)) == {"119-hr-1"}
    assert folders.read == []


def test_folders_are_read_newest_first_up_to_the_cap(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("117-hr-1"), _bill("118-hr-1")])
    zips = {(congress, "hr"): _zip(congress, {1: _BILLSTATUS}) for congress in (117, 118)}
    first = _Folders(zips)
    enrich_bill_subjects(tmp_path, max_folders=1, read_folder=first, fetcher=_StubFetcher())
    assert first.read == [(118, "hr")]
    second = _Folders(zips)
    out = enrich_bill_subjects(tmp_path, max_folders=1, read_folder=second, fetcher=_StubFetcher())
    assert second.read == [(117, "hr")]
    assert set(_rows(out)) == {"117-hr-1", "118-hr-1"}


def _unavailable():
    url = "https://www.govinfo.gov/bulkdata/BILLSTATUS/118/hr/BILLSTATUS-118-hr.zip"
    return BillSourceUnavailableError(
        CapturedBodyResponse(url, url, 404, "text/html", "2026-09-23T00:00:00Z", body=b"")
    )


@pytest.mark.parametrize(
    "error,held_rows",
    [
        (_unavailable(), True),
        (BillSourceError("BILLSTATUS archive exceeds its entry bound"), False),
        (httpx.ConnectError("private transport detail"), False),
    ],
)
def test_a_folder_that_is_unpublished_is_not_held_and_one_that_failed_is_retried(tmp_path, error, held_rows):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("118-hr-1")])
    out = enrich_bill_subjects(tmp_path, read_folder=_Folders(errors={(118, "hr"): error}), fetcher=_StubFetcher())
    rows = _rows(out)
    assert ("118-hr-1" in rows) is held_rows
    if held_rows:
        assert (rows["118-hr-1"]["policy_area"], rows["118-hr-1"]["carrier"]) == (None, CARRIER_BULKDATA)


def test_a_credential_refusal_from_a_folder_stops_the_run(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("118-hr-1")])
    refused = _Folders(errors={(118, "hr"): CredentialRefusedError("refused")})
    with pytest.raises(CredentialRefusedError):
        enrich_bill_subjects(tmp_path, read_folder=refused, fetcher=_StubFetcher())


# -- routes: the Congress.gov API below the 108th -----------------------------


def test_only_bills_below_the_108th_congress_reach_the_api(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("107-hr-1"), _bill("108-hr-1")])
    fetcher = _StubFetcher({"107-hr-1": BillSubjects("Health", (), CARRIER_API)})
    folders = _Folders({(108, "hr"): _zip(108, {1: _BILLSTATUS})})
    out = enrich_bill_subjects(tmp_path, read_folder=folders, fetcher=fetcher)
    assert fetcher.asked == ["107-hr-1"]
    assert folders.read == [(108, "hr")]
    assert {bill_id: row["carrier"] for bill_id, row in _rows(out).items()} == {
        "107-hr-1": CARRIER_API,
        "108-hr-1": CARRIER_BULKDATA,
    }


def test_without_a_key_api_bills_wait_rather_than_fail(tmp_path, monkeypatch):
    _keyless(monkeypatch)
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("107-hr-1")])
    out = enrich_bill_subjects(tmp_path, read_folder=_Folders())
    assert _rows(out) == {}


def test_a_capped_api_run_resumes_where_it_stopped(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill(f"100-hr-{n}") for n in range(1, 6)])
    answers = {f"100-hr-{n}": BillSubjects("Health", ("Medicare",), CARRIER_API) for n in range(1, 6)}
    first = _StubFetcher(answers)
    enrich_bill_subjects(tmp_path, max_bills=2, fetcher=first)
    second = _StubFetcher(answers)
    out = enrich_bill_subjects(tmp_path, max_bills=2, fetcher=second)
    assert len(first.asked) == len(second.asked) == 2
    assert set(second.asked).isdisjoint(first.asked)
    assert pq.ParquetFile(out).metadata.num_rows == 4
    assert pq.ParquetFile(out).schema_arrow.names == list(COLUMNS)


def test_a_failed_fetch_leaves_the_bill_un_enriched_for_the_next_run(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("100-hr-1"), _bill("100-hr-2")])
    out = enrich_bill_subjects(tmp_path, fetcher=_StubFetcher({"100-hr-1": BillSubjects("Health", (), CARRIER_API)}))
    assert set(_rows(out)) == {"100-hr-1"}
    recovered = _StubFetcher({"100-hr-2": BillSubjects("Taxation", (), CARRIER_API)})
    out = enrich_bill_subjects(tmp_path, fetcher=recovered)
    assert recovered.asked == ["100-hr-2"]
    assert set(_rows(out)) == {"100-hr-1", "100-hr-2"}


def test_a_definitive_miss_is_recorded_and_not_asked_again(tmp_path):
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("100-hr-1")])
    enrich_bill_subjects(tmp_path, fetcher=_StubFetcher({"100-hr-1": BillSubjects(None, (), CARRIER_API, held=False)}))
    second = _StubFetcher({"100-hr-1": BillSubjects("Health", (), CARRIER_API)})
    enrich_bill_subjects(tmp_path, fetcher=second)
    assert second.asked == []


def test_an_empty_answer_from_the_other_carrier_is_asked_again(tmp_path):
    """A 108th-plus bill the API once left empty is re-asked from BILLSTATUS; one BILLSTATUS answered is not."""
    prior = pa.Table.from_pylist(
        [
            _shape("118-hr-1", None, (), CARRIER_API, "2026-08-22T00:00:00+00:00"),
            _shape("118-hr-2", None, (), CARRIER_BULKDATA, "2026-08-22T00:00:00+00:00"),
        ],
        schema=pa.schema([(c, pa.string()) for c in COLUMNS]),
    )
    pq.write_table(prior, tmp_path / "_bill_subjects_prior.parquet")
    _write_bills(tmp_path / "congress_bills.parquet", [_bill("118-hr-1"), _bill("118-hr-2")])
    pending = _pending_bills(
        tmp_path / "congress_bills.parquet", tmp_path / "_bill_subjects_prior.parquet", have_prior=True
    )
    assert [bill.bill_id for bill in pending] == ["118-hr-1"]


def test_bills_below_the_api_floor_or_without_a_number_are_never_selected(tmp_path):
    bills = tmp_path / "congress_bills.parquet"
    _write_bills(bills, [_bill("92-hr-1"), _bill("93-hr-1"), dict(_bill("118-hr-2"), bill_number=None)])
    pending = _pending_bills(bills, tmp_path / "_absent_prior.parquet", have_prior=False)
    assert [bill.bill_id for bill in pending] == ["93-hr-1"]


def test_a_missing_bill_table_fails_loudly(tmp_path):
    with pytest.raises(RuntimeError, match="congress_bills.parquet"):
        enrich_bill_subjects(tmp_path, fetcher=_StubFetcher())


def test_a_run_cannot_outspend_the_documented_hourly_budget():
    """Congress.gov states 5,000 requests an hour; a capped run must fit inside it and the 30-minute job."""
    assert 3600 / DELAY_SECONDS < API_HOURLY_BUDGET
    assert MAX_API_BILLS_PER_RUN * DELAY_SECONDS < 30 * 60


# -- the Congress.gov fetch ---------------------------------------------------


def test_the_api_fetcher_refuses_to_run_without_a_key(monkeypatch):
    _keyless(monkeypatch)
    with pytest.raises(ValueError, match="api.data.gov key"):
        BillSubjectsFetcher()


def _api_fetcher(monkeypatch, pages):
    fetcher = BillSubjectsFetcher(api_key="test", delay=0)
    calls: list[int] = []

    def fake_get_json(url, *, params):
        calls.append(params["offset"])
        return pages[params["offset"]]

    monkeypatch.setattr(fetcher, "_get_json", fake_get_json)
    return fetcher, calls


def _page(count, names, policy_area=None):
    subjects: dict[str, object] = {"legislativeSubjects": [{"name": name} for name in names]}
    if policy_area:
        subjects["policyArea"] = {"name": policy_area}
    return {"pagination": {"count": count}, "subjects": subjects}


def test_one_call_carries_both_fields(monkeypatch):
    """policyArea and legislativeSubjects arrive together — no second request."""
    fetcher, calls = _api_fetcher(monkeypatch, {0: _page(2, ["Medicare", "Drug safety"], "Health")})
    assert fetcher.subjects_for("100", "HR", "1") == BillSubjects("Health", ("Medicare", "Drug safety"), CARRIER_API)
    assert calls == [0]


def test_a_fat_bill_walks_offsets_until_the_count_is_met(monkeypatch):
    fetcher, calls = _api_fetcher(monkeypatch, {0: _page(3, ["A", "B"], "Taxation"), 2: _page(3, ["C"])})
    result = fetcher.subjects_for("100", "HR", "1")
    assert result == BillSubjects("Taxation", ("A", "B", "C"), CARRIER_API)
    assert calls == [0, 2]


def test_a_failed_later_page_publishes_nothing_rather_than_a_truncated_list(monkeypatch):
    fetcher, _ = _api_fetcher(monkeypatch, {0: _page(400, ["A"], "Taxation"), 1: None})
    assert fetcher.subjects_for("100", "HR", "1") is None


def test_the_page_cap_is_a_refusal_not_a_silent_stop(monkeypatch):
    pages = {offset: _page(1_001, [f"S{offset + n}" for n in range(250)]) for offset in (0, 250, 500, 750)}
    fetcher, calls = _api_fetcher(monkeypatch, pages)
    messages: list[str] = []
    sink = logger.add(messages.append, level="ERROR", format="{message}")
    try:
        assert fetcher.subjects_for("100", "hr", "1") is None
    finally:
        logger.remove(sink)
    assert calls == [0, 250, 500, 750]
    assert any("100-hr-1" in m and "1001 subjects stated" in m for m in messages)


def test_an_empty_page_before_the_count_is_met_is_a_refusal(monkeypatch):
    fetcher, calls = _api_fetcher(monkeypatch, {0: _page(3, ["A"]), 1: _page(3, [])})
    assert fetcher.subjects_for("100", "hr", "1") is None
    assert calls == [0, 1]


_KEY = "fixture-congress-key-0123456789"


@pytest.fixture
def api_transport(monkeypatch):
    """The API carrier over a MockTransport of ``httpx.Response`` kwargs; the last repeats, backoff does not sleep."""
    monkeypatch.setattr("spicy_regs.sources.bill_subjects.time.sleep", lambda _seconds: None)
    clients = []

    def create(*actions):
        calls = []

        def respond(request):
            calls.append(request)
            return httpx.Response(**actions[min(len(calls), len(actions)) - 1])

        client = httpx.Client(transport=httpx.MockTransport(respond))
        clients.append(client)
        return BillSubjectsFetcher(api_key=_KEY, delay=0, client=client), calls

    yield create
    for client in clients:
        client.close()


def test_the_api_key_travels_only_in_the_header(api_transport):
    fetcher, calls = api_transport({"status_code": 200, "json": _page(1, ["Medicare"])})
    assert fetcher.subjects_for("100", "HR", "1") == BillSubjects(None, ("Medicare",), CARRIER_API)
    [request] = calls
    assert "api_key" not in request.url.params
    assert _KEY not in str(request.url)
    assert request.headers["X-Api-Key"] == _KEY


def test_a_404_is_a_definitive_miss(api_transport):
    fetcher, calls = api_transport({"status_code": 404, "json": {}})
    assert fetcher.subjects_for("100", "hr", "1") == BillSubjects(None, (), CARRIER_API, held=False)
    assert len(calls) == 1


@pytest.mark.parametrize("status", [401, 403])
def test_api_access_refusal_stops_without_an_empty_result_or_retry(api_transport, status):
    fetcher, calls = api_transport({"status_code": status, "json": {"error": "refused"}})
    with pytest.raises(CredentialRefusedError) as refused:
        fetcher.subjects_for("100", "hr", "1")
    assert len(calls) == 1
    assert _KEY not in str(refused.value)


def test_a_failed_request_logs_neither_the_key_nor_the_query(api_transport):
    """Checks the query string too: the key is header-only, so its absence alone cannot catch a URL in a log line."""
    fetcher, calls = api_transport({"status_code": 400, "json": {"error": "bad request"}})
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        assert fetcher.subjects_for("100", "hr", "1") is None
    finally:
        logger.remove(sink)
    assert len(messages) == len(calls) > 1
    assert all("api.congress.gov/v3/bill/100/hr/1/subjects" in m and "HTTP 400" in m for m in messages)
    assert not any(_KEY in m or "?" in m or "offset=" in m for m in messages)


def test_a_redirect_is_not_followed_so_the_key_header_stays_on_the_api_host(api_transport):
    fetcher, calls = api_transport({"status_code": 302, "headers": {"location": "https://elsewhere.example/collect"}})
    assert fetcher.subjects_for("100", "hr", "1") is None
    assert {request.url.host for request in calls} == {"api.congress.gov"}
