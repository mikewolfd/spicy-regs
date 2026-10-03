"""Recently active dockets are listed at Regulations.gov, and a held document the publisher no longer serves is flagged.

The publisher is the real keyed reader over a stub transport serving the answers the round-6 audit recorded
(``tests/fixtures/regulations_gov_reconcile``): FNA-2026-0301 lists -0001, -0002, -0003 and -0005, and -0004, the Utah
notice the mirror still holds, answers 404.
"""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import httpx
import polars as pl
import pyarrow.parquet as pq
import pytest
from spicy_docs.reading.paged_json import PagedJsonBudget
from spicy_docs.sources.regulations_gov.api import RegulationsGovApiReader

import spicy_regs.pipelines.docket_reconcile as reconcile
from spicy_regs.sources import r2

FIXTURES = Path(__file__).parent / "fixtures" / "regulations_gov_reconcile"
NOW = datetime(2026, 10, 3, 12, 40, tzinfo=UTC)
UTAH = "FNA-2026-0301-0004"
NOT_FOUND = (FIXTURES / "document-FNA-2026-0301-0004.404.json").read_bytes()

#: The working copy's FNA-2026-0301 rows on 2026-10-03 (documents generation 066964e8): -0005 was modified on
#: 2026-09-29, so the docket is active in the week before NOW. FNS-2025-0401 was last touched in May.
HELD = [
    ("FNA-2026-0301-0001", "FNA-2026-0301", "2026-09-08T04:00:00Z", "2026-09-08T21:48:52Z"),
    ("FNA-2026-0301-0002", "FNA-2026-0301", "2026-09-08T04:00:00Z", "2026-09-08T21:49:06Z"),
    ("FNA-2026-0301-0003", "FNA-2026-0301", "2026-09-08T04:00:00Z", "2026-09-08T21:49:08Z"),
    (UTAH, "FNA-2026-0301", "2026-09-15T04:00:00Z", "2026-09-16T09:00:44Z"),
    ("FNA-2026-0301-0005", "FNA-2026-0301", "2026-09-15T04:00:00Z", "2026-09-29T09:00:26Z"),
    ("FNA-2026-0313-0006", "FNA-2026-0313", "2026-09-18T04:00:00Z", "2026-10-02T09:00:33Z"),
    ("FNS-2025-0401-0001", "FNS-2025-0401", "2026-05-08T04:00:00Z", "2026-05-08T21:01:50Z"),
]


def _listing(ids: list[str], *, total: int | None = None, has_next: bool = False) -> bytes:
    rows = [{"id": i, "type": "documents", "attributes": {"docketId": i.rsplit("-", 1)[0]}} for i in ids]
    meta = {"hasNextPage": has_next, "lastPage": not has_next, "numberOfElements": len(rows), "pageNumber": 1,
            "pageSize": 250, "totalElements": len(rows) if total is None else total, "totalPages": 1}
    return json.dumps({"data": rows, "meta": meta}).encode()


def _recorded_listing() -> bytes:
    """The audit's FNA-2026-0301 page, its page size restated for the step's own request (see the README)."""
    page = json.loads((FIXTURES / "documents-docket-FNA-2026-0301.json").read_bytes())
    page["meta"]["pageSize"] = 250
    return json.dumps(page).encode()


def _response(status: int, body: bytes, media_type: str) -> httpx.Response:
    return httpx.Response(status, stream=httpx.ByteStream(body), headers={"content-type": media_type})


class Publisher:
    """Serves each docket's listing and each document's detail; records every request, in order."""

    def __init__(self, listings=None, details=None):
        self.listings = {"FNA-2026-0301": _recorded_listing(), "FNA-2026-0313": _listing(["FNA-2026-0313-0006"])}
        self.listings |= listings or {}
        self.details = {UTAH: 404} | (details or {})
        self.asked: list[str] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        url = urlsplit(str(request.url))
        if url.path == "/v4/documents":
            docket = parse_qs(url.query)["filter[docketId]"][0]
            self.asked.append(docket)
            answer = self.listings[docket]
            if isinstance(answer, BaseException):
                raise answer
            if isinstance(answer, int):
                return httpx.Response(answer, request=request)
            return _response(200, answer, "application/json")
        document = url.path.rsplit("/", 1)[1]
        self.asked.append(document)
        answer = self.details[document]
        if isinstance(answer, BaseException):
            raise answer
        if answer == 200:
            body = json.dumps({"data": {"id": document, "type": "documents", "attributes": {}}}).encode()
            return _response(200, body, "application/vnd.api+json")
        return _response(answer, NOT_FOUND, "application/json")


def _run(working: Path, publisher: Publisher, now: datetime = NOW, **options) -> dict[str, int]:
    budget = PagedJsonBudget(max_requests=1, max_page_bytes=1 << 20, timeout_seconds=5, min_request_interval_seconds=0)
    with RegulationsGovApiReader(budget=budget, api_key="test-key", transport=httpx.MockTransport(publisher)) as reader:
        return reconcile.reconcile(working, reader, now=lambda: now, **options)


def _outcomes(working: Path) -> dict[str, dict]:
    return {row["document_id"]: row for row in pl.read_parquet(working / reconcile.OUTCOMES).iter_rows(named=True)}


@pytest.fixture
def working(tmp_path, monkeypatch):
    pl.DataFrame(HELD, schema=["document_id", "docket_id", "posted_date", "modify_date"], orient="row").write_parquet(
        tmp_path / "documents.parquet")
    monkeypatch.setattr(r2, "download_working_copy", lambda key, path: False)
    return tmp_path


@pytest.mark.parametrize("status", [404, 410])
def test_a_held_document_its_docket_no_longer_lists_and_that_answers_404_or_410_is_removed(working, status):
    publisher = Publisher(details={UTAH: status})
    counts = _run(working, publisher)
    outcomes = _outcomes(working)
    assert {d: row["publisher_status"] for d, row in outcomes.items()} == {
        "FNA-2026-0301-0001": "listed", "FNA-2026-0301-0002": "listed", "FNA-2026-0301-0003": "listed",
        UTAH: "removed", "FNA-2026-0301-0005": "listed", "FNA-2026-0313-0006": "listed"}
    utah = outcomes[UTAH]
    assert (utah["http_status"], utah["removed_observed_at"], utah["observed_at"]) == (
        status, NOW.isoformat(), NOW.isoformat())
    assert utah["body_sha256"] == hashlib.sha256(NOT_FOUND).hexdigest()
    # One listing per active docket and one read by id; FNS-2025-0401, last touched in May, is not asked.
    assert publisher.asked == ["FNA-2026-0301", UTAH, "FNA-2026-0313"]
    assert (counts["requests"], counts["removed"], counts["left"]) == (3, 1, 0)


def test_a_document_the_publisher_still_serves_by_id_is_listed(working):
    _run(working, Publisher(details={UTAH: 200}))
    utah = _outcomes(working)[UTAH]
    assert (utah["publisher_status"], utah["http_status"], utah["removed_observed_at"]) == ("listed", 200, None)


@pytest.mark.parametrize(
    "answer", [429, httpx.ConnectError("connection reset")], ids=["429", "transport failure"]
)
@pytest.mark.parametrize("where", ["the second docket's listing", "the first docket's read by id"])
def test_a_429_or_a_transport_failure_fails_the_step_and_records_nothing_for_its_docket(working, answer, where):
    if where == "the first docket's read by id":
        publisher = Publisher(details={UTAH: answer})
    else:
        publisher = Publisher(listings={"FNA-2026-0313": answer})
    with pytest.raises((httpx.HTTPStatusError, ConnectionError)):
        _run(working, publisher)
    recorded = {row["docket_id"] for row in _outcomes(working).values()}
    # The docket in flight records nothing, not even the documents its listing named; one done before it is kept.
    assert recorded == (set() if where == "the first docket's read by id" else {"FNA-2026-0301"})


def test_a_listing_cut_at_the_page_cap_never_implies_removal(working):
    """40 pages of 250 reach 10,000 documents: past that the listing cannot be complete."""
    cut = _listing(["FNA-2026-0301-0001", "FNA-2026-0301-0002"], total=10_001, has_next=True)
    publisher = Publisher(listings={"FNA-2026-0301": cut})
    counts = _run(working, publisher)
    outcomes = _outcomes(working)
    assert {d for d in outcomes if d.startswith("FNA-2026-0301")} == {"FNA-2026-0301-0001", "FNA-2026-0301-0002"}
    assert publisher.asked == ["FNA-2026-0301", "FNA-2026-0313"]  # no read by id, no second page
    assert counts["cut"] == 1


def test_the_run_stops_at_its_cap_and_the_next_day_resumes_where_it_left_off(working):
    publisher = Publisher(details={UTAH: 200})
    first = _run(working, publisher, max_requests=1)
    assert publisher.asked == ["FNA-2026-0301"] and first["left"] == 2
    assert _outcomes(working) == {}, "the docket in flight when the cap was reached records nothing"
    second = _run(working, publisher, max_requests=2)
    assert publisher.asked[1:] == ["FNA-2026-0301", UTAH] and second["left"] == 1
    # The same day, a docket already done is not asked again; the one left waits for the next run.
    third = _run(working, publisher, max_requests=5)
    assert publisher.asked[3:] == ["FNA-2026-0313"] and third["left"] == 0
    # The next day the longest-unreconciled lead.
    publisher.asked.clear()
    _run(working, publisher, now=NOW + timedelta(days=1), max_requests=1)
    assert publisher.asked == ["FNA-2026-0301"]


def test_a_removal_is_not_read_again_until_its_retry_window_passes(working):
    publisher = Publisher()
    _run(working, publisher)
    publisher.asked.clear()
    _run(working, publisher, now=NOW + timedelta(days=1))
    assert publisher.asked == ["FNA-2026-0301", "FNA-2026-0313"], "the listing's silence leaves the 404 standing"
    later = NOW + reconcile.RETRY_AFTER + timedelta(days=1)
    publisher.asked.clear()
    _run(working, publisher, now=later, window_days=60)
    assert UTAH in publisher.asked
    utah = _outcomes(working)[UTAH]
    assert (utah["observed_at"], utah["removed_observed_at"]) == (later.isoformat(), NOW.isoformat())


def test_a_named_docket_is_reconciled_however_long_ago_it_was_touched(working):
    """The first live check need not wait for FNA-2026-0301 to be active: two requests settle it."""
    publisher = Publisher()
    counts = _run(working, publisher, now=NOW + timedelta(days=60), dockets=["FNA-2026-0301"])
    assert publisher.asked == ["FNA-2026-0301", UTAH] and counts["requests"] == 2
    assert _outcomes(working)[UTAH]["publisher_status"] == "removed"


def test_the_documents_family_reads_each_documents_last_answer_and_null_where_none(tmp_path):
    documents = tmp_path / "documents.parquet"
    rows = [{"document_id": d, "docket_id": k, "title": None} for d, k, *_ in HELD]
    pl.DataFrame(rows).write_parquet(documents, row_group_size=3)
    outcomes = tmp_path / reconcile.OUTCOMES
    pl.DataFrame(
        [{"document_id": UTAH, "docket_id": "FNA-2026-0301", "publisher_status": "removed",
          "observed_at": NOW.isoformat(), "removed_observed_at": NOW.isoformat(), "http_status": 404,
          "body_sha256": None},
         {"document_id": "FNA-2026-0301-0005", "docket_id": "FNA-2026-0301", "publisher_status": "listed",
          "observed_at": NOW.isoformat(), "removed_observed_at": None, "http_status": None, "body_sha256": None}],
        schema=reconcile.OUTCOME_SCHEMA,
    ).write_parquet(outcomes)
    before = pq.ParquetFile(documents).metadata
    reconcile.with_publisher_status(documents, outcomes)
    after = pq.ParquetFile(documents)
    assert [after.metadata.row_group(i).num_rows for i in range(after.metadata.num_row_groups)] == [
        before.row_group(i).num_rows for i in range(before.num_row_groups)]
    table = {row["document_id"]: row for row in after.read().to_pylist()}
    assert (table[UTAH]["publisher_status"], table[UTAH]["removed_observed_at"]) == ("removed", NOW.isoformat())
    assert table["FNA-2026-0301-0005"]["publisher_status"] == "listed"
    assert table["FNS-2025-0401-0001"]["publisher_status"] is None
    # Never reconciled: the columns are there and NULL.
    bare = tmp_path / "bare.parquet"
    pl.DataFrame(rows).write_parquet(bare)
    reconcile.with_publisher_status(bare, None)
    assert {row["publisher_status"] for row in pq.read_table(bare).to_pylist()} == {None}


def test_the_daily_step_paces_one_attempt_per_request_and_runs_clear_of_the_sweeps(monkeypatch, tmp_path):
    """Owner decision 2026-10-03: about 1,100 keyed requests a day on a key that allows 1,000 an hour, shared."""
    import re
    import tomllib
    from contextlib import nullcontext

    import yaml

    opened, ran = [], []
    monkeypatch.setattr(reconcile, "keyed_reader", lambda **budget: opened.append(budget) or nullcontext("reader"))
    monkeypatch.setattr(reconcile, "reconcile", lambda *args, **options: ran.append(options))
    monkeypatch.setattr(reconcile, "load_dotenv", lambda *args, **kwargs: None)
    with pytest.raises(SystemExit) as exited:
        reconcile.app(["--output-dir", str(tmp_path)])
    assert exited.value.code == 0
    assert opened == [{"max_requests": 1, "min_request_interval_seconds": reconcile.REQUEST_INTERVAL_SECONDS}]
    assert ran[0]["max_requests"] == reconcile.DEFAULT_MAX_REQUESTS == 1_100 and ran[0]["dockets"] == ()
    assert 3_600 / reconcile.REQUEST_INTERVAL_SECONDS < 1_000

    root = Path(__file__).resolve().parents[1]
    scripts = tomllib.loads((root / "pyproject.toml").read_text())["project"]["scripts"]
    assert scripts["reconcile-dockets"] == "spicy_regs.pipelines.docket_reconcile:app"
    workflow = yaml.safe_load((root / ".github/workflows/reconcile-dockets.yml").read_text())
    (cron,) = [entry["cron"] for entry in workflow[True]["schedule"]]
    minute, hour = (int(part) for part in cron.split()[:2])
    sweeps = [6 * 60 + 25, 18 * 60 + 25]
    assert all(abs(hour * 60 + minute - sweep) >= 3 * 60 for sweep in sweeps)
    step = workflow["jobs"]["reconcile"]["steps"][-1]
    assert re.search(r"uv run --frozen reconcile-dockets", step["run"])
    assert "DATA_GOV_API_KEY" in step["env"] and "R2_CATALOG_TOKEN" not in step["env"]
    timeout = workflow["jobs"]["reconcile"]["timeout-minutes"]
    assert timeout * 60 > reconcile.DEFAULT_MAX_REQUESTS * reconcile.REQUEST_INTERVAL_SECONDS
