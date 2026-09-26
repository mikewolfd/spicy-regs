"""Hermetic tests for the Senate LDA lobbying-filings ingest (no network).

The request walk itself is spicy-docs' ``LdaFilingsReader``; its retries,
credential headers, page-shape refusals, count checks and traversal bounds are
pinned by spicy-docs' own ``test_lda_and_courtlistener_search`` and
``test_paged_json``. What remains here is what this repository owns on top of
it: the raw-filing → published-schema mapping (``_shape``, including the nested
activity/government-entity projections), the filtered-request semantics
(bounded date window, cold-start upper bound, ``max_records``) and that a
failed or malformed read aborts the rollup before anything is written or
published.
"""

from __future__ import annotations

import json
from datetime import date
from importlib import import_module

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.reading.paged_json import PagedJsonSourceError
from spicy_docs.sources.lda import API
from spicy_docs.transport import retry as spicy_retry

from spicy_regs.transforms.build_lobbying_filings import (
    COLUMNS,
    LobbyingFilingsError,
    _bounded_until,
    _shape,
)

# ``transforms/__init__`` shadows the module name with the public function, so
# import the module itself for attribute access (its ``MAX_REQUESTS_PER_PAGE``).
bld = import_module("spicy_regs.transforms.build_lobbying_filings")

_RAW_FILING = {
    "filing_uuid": "7866327b-c892-4430-b9f0-1f0f679c58c6",
    "filing_type": "Q1",
    "filing_year": 2024,
    "filing_period": "first_quarter",
    "dt_posted": "2024-01-02T10:13:41-05:00",
    "income": "30000.00",
    "expenses": None,
    "filing_document_url": "https://lda.senate.gov/filings/public/filing/7866327b/print/",
    "registrant": {"id": 35707, "name": "SMITH GARSON"},
    "client": {"id": 58116, "client_id": 58116, "name": "E-COM 9-1-1 DISPATCH CENTER"},
    "lobbying_activities": [
        {
            "general_issue_code": "TEC",
            "general_issue_code_display": "Telecommunications",
            "description": "Emergency dispatch technology funding.",
            "government_entities": [{"id": 2, "name": "HOUSE OF REPRESENTATIVES"}],
        },
        {
            "general_issue_code": "BUD",
            "general_issue_code_display": "Budget/Appropriations",
            "description": "Appropriations.",
            # Same chamber lobbied again — should dedup to one entity.
            "government_entities": [
                {"id": 2, "name": "HOUSE OF REPRESENTATIVES"},
                {"id": 1, "name": "SENATE"},
            ],
        },
    ],
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_FILING)
    assert set(row) == set(COLUMNS)


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_FILING)
    assert row["filing_uuid"] == "7866327b-c892-4430-b9f0-1f0f679c58c6"
    assert row["filing_type"] == "Q1"
    # Integer scalars stringify (schema is all-VARCHAR).
    assert row["filing_year"] == "2024"
    assert row["registrant_id"] == "35707"
    assert row["client_id"] == "58116"
    assert row["registrant_name"] == "SMITH GARSON"
    assert row["client_name"] == "E-COM 9-1-1 DISPATCH CENTER"
    assert row["income"] == "30000.00"
    assert row["expenses"] is None
    assert row["url"] == "https://lda.senate.gov/filings/public/filing/7866327b/print/"
    # Activities project to issue codes + descriptions.
    acts = json.loads(row["lobbying_activities_json"])
    assert [a["general_issue_code"] for a in acts] == ["TEC", "BUD"]
    assert acts[0]["general_issue_code_display"] == "Telecommunications"
    # Government entities flatten + dedup across activities.
    ents = json.loads(row["government_entities_json"])
    assert {e["name"] for e in ents} == {"HOUSE OF REPRESENTATIVES", "SENATE"}
    assert len(ents) == 2


def test_shape_handles_missing_nested():
    row = _shape({"filing_uuid": "x"})
    assert row["registrant_name"] is None
    assert row["client_id"] is None
    assert row["lobbying_activities_json"] == "[]"
    assert row["government_entities_json"] == "[]"


def _page(uuids: list[str], next_url: str | None, *, count: int | None = None) -> dict:
    return {
        "count": len(uuids) if count is None else count,
        "next": next_url,
        "previous": None,
        "results": [{"filing_uuid": u} for u in uuids],
    }


def _json_response(payload, *, status: int = 200) -> httpx.Response:
    """A streamed JSON response — the owner's capture reads bodies via ``iter_raw``."""
    return httpx.Response(
        status,
        stream=httpx.ByteStream(json.dumps(payload).encode()),
        headers={"content-type": "application/json"},
    )


def _mock_http(monkeypatch, handler):
    """Route every httpx client (spicy-docs' capture builds one per reader) through ``handler``."""
    original = httpx.Client
    calls = []

    def record(request):
        calls.append(request)
        return handler(request, len(calls))

    def make_client(**kwargs):
        # spicy-docs' capture always passes its own ``transport`` (None here);
        # drop it so the mock transport is the one the requests flow through.
        kwargs.pop("transport", None)
        return original(transport=httpx.MockTransport(record), **kwargs)

    monkeypatch.setattr(httpx, "Client", make_client)
    return calls


def _no_retry_delay(monkeypatch):
    """Deterministic retries: no jitter, sleeps recorded. Returns the sleep log."""
    sleeps = []
    monkeypatch.setattr(spicy_retry.random, "uniform", lambda *_: 0.0)
    monkeypatch.setattr(spicy_retry.time, "sleep", sleeps.append)
    # Page pacing waits on the same clock; these tests are about retries, so run unpaced.
    monkeypatch.setattr(bld, "KEYLESS_INTERVAL_SECONDS", 0.0)
    return sleeps


def _no_prior(monkeypatch):
    from spicy_regs.sources import r2

    monkeypatch.setattr(r2, "download", lambda *args: False)


def test_lagging_window_is_capped_to_thirty_days():
    assert _bounded_until(
        date(2026, 4, 1),
        None,
        today=date(2026, 7, 20),
    ) == date(2026, 5, 1)
    assert _bounded_until(
        date(2026, 7, 1),
        None,
        today=date(2026, 7, 20),
    ) == date(2026, 7, 20)


def test_cold_start_uses_today_filter_without_archive_year_floor(tmp_path, monkeypatch):
    _no_prior(monkeypatch)

    def respond(request, number):
        assert request.url.params["filing_dt_posted_before"] == date.today().isoformat()
        assert "filing_year" not in request.url.params
        assert "filing_dt_posted_after" not in request.url.params
        assert request.url.params["ordering"] == "dt_posted"
        return _json_response(_page([], None))

    calls = _mock_http(monkeypatch, respond)
    bld.build_lobbying_filings(tmp_path)
    assert len(calls) == 1


def test_fetch_sends_bounded_date_window(tmp_path, monkeypatch):
    _no_prior(monkeypatch)

    def respond(request, number):
        assert request.url.params["filing_dt_posted_after"] == "2026-04-01"
        assert request.url.params["filing_dt_posted_before"] == "2026-05-01"
        assert request.url.params["ordering"] == "dt_posted"
        return _json_response(_page([], None))

    _mock_http(monkeypatch, respond)
    bld.build_lobbying_filings(tmp_path, since=date(2026, 4, 1), until=date(2026, 5, 1))


def test_a_filing_year_reads_the_whole_year_whatever_the_prior_watermark(tmp_path, monkeypatch):
    """The history backfill: the stored posted-date watermark must not narrow a year to its last days."""
    from spicy_regs.transforms.build_lobbying_filings import _SCHEMA

    _no_prior(monkeypatch)
    pq.write_table(pa.Table.from_pylist([_shape(_RAW_FILING)], schema=_SCHEMA), tmp_path / "_lda_prior.parquet")

    def respond(request, number):
        assert request.url.params["filing_year"] == "2010"
        assert "filing_dt_posted_after" not in request.url.params
        assert "filing_dt_posted_before" not in request.url.params
        return _json_response(_page([], None))

    calls = _mock_http(monkeypatch, respond)
    bld.build_lobbying_filings(tmp_path, filing_year=2010)
    assert len(calls) == 1


def test_max_records_stops_the_walk_mid_page(tmp_path, monkeypatch):
    """An explicit record cap is a prefix of the walk: the next page is never requested."""
    _no_prior(monkeypatch)
    calls = _mock_http(
        monkeypatch,
        lambda request, number: _json_response(
            _page(["a", "b", "c", "d", "e"], f"{API}/filings/?page=2", count=100)
        ),
    )
    out, *_ = bld.build_lobbying_filings(tmp_path, max_records=3)
    assert len(calls) == 1
    assert [row["filing_uuid"] for row in pq.read_table(out).to_pylist()] == ["a", "b", "c"]


@pytest.mark.parametrize(
    ("failure_page", "failure", "have_prior"),
    [(1, 400, False), (1, 400, True), (2, 400, False), (2, 400, True), (2, 503, True), (2, "transport", True)],
)
def test_reader_failure_aborts_actual_rollup_before_output_or_publication(
    tmp_path, monkeypatch, failure_page, failure, have_prior
):
    from spicy_regs.pipelines.rollups.lobbying_filings import LobbyingFilingsRollup
    from spicy_regs.sources import publication, r2
    from spicy_regs.transforms.build_lobbying_filings import OUTPUT, _SCHEMA

    prior = tmp_path / OUTPUT
    if have_prior:
        pq.write_table(pa.Table.from_pylist([_shape(_RAW_FILING)], schema=_SCHEMA), prior)
    original = prior.read_bytes() if have_prior else None

    def download(key, target):
        assert key == OUTPUT
        if original is None:
            return False
        target.write_bytes(original)
        return True

    monkeypatch.setenv("R2_PUBLIC_URL", "https://fixture.example")
    monkeypatch.setenv("R2_ACCESS_KEY_ID", "synthetic-test-key")
    monkeypatch.setattr(r2, "download", download)
    monkeypatch.setattr(publication, "load_index", lambda base: publication.empty_index())
    monkeypatch.setattr(
        publication, "publish_generation", lambda *args, **kwargs: pytest.fail("failed read was published")
    )
    # Three requests per page: the initial attempt plus two retries.
    monkeypatch.setattr(bld, "MAX_REQUESTS_PER_PAGE", 3)
    sleeps = _no_retry_delay(monkeypatch)

    def respond(request, number):
        if failure_page == 2 and number == 1:
            return _json_response({"count": 2, "next": f"{API}/filings/?page=2", "results": [_RAW_FILING]})
        if failure == "transport":
            raise httpx.ReadTimeout("interrupted", request=request)
        return _json_response({"detail": "failure"}, status=failure)

    calls = _mock_http(monkeypatch, respond)
    retried = failure in {503, "transport"}
    expected_error = httpx.HTTPError if failure == 503 else ConnectionError if retried else PagedJsonSourceError
    with pytest.raises(expected_error):
        LobbyingFilingsRollup(output_dir=tmp_path, skip_upload=False).run()
    assert len(calls) == failure_page + (2 if retried else 0)
    assert sleeps == ([0.0, 0.0] if retried else [])
    assert not (tmp_path / "generations").exists()
    assert not list((tmp_path / ".builds").rglob(OUTPUT))
    assert not list(tmp_path.rglob("_*_new.parquet"))
    if have_prior:
        assert prior.read_bytes() == original
        assert pq.read_table(prior).to_pylist()[0]["filing_uuid"] == _RAW_FILING["filing_uuid"]
        assert next((tmp_path / ".builds").rglob("_lda_prior.parquet")).read_bytes() == original
    else:
        assert not prior.exists()


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ({"detail": "not a result page"}, PagedJsonSourceError),
        ({"count": 0, "results": [], "next": 2}, PagedJsonSourceError),
        ({"count": -1, "results": [], "next": None}, PagedJsonSourceError),
        ({"count": 1, "results": [{}], "next": None}, LobbyingFilingsError),
        ({"count": 1, "results": [{"filing_uuid": ""}], "next": None}, LobbyingFilingsError),
        ({"count": 12, "results": [], "next": None}, PagedJsonSourceError),
    ],
)
def test_malformed_success_cannot_become_empty_or_partial_output(tmp_path, monkeypatch, payload, error):
    _no_prior(monkeypatch)
    _mock_http(monkeypatch, lambda request, number: _json_response(payload))
    with pytest.raises(error):
        bld.build_lobbying_filings(tmp_path)
    assert not list(tmp_path.glob("*.parquet"))


_FILING_WITH_ACTIVITIES = {
    "filing_uuid": "f-1",
    "lobbying_activities": [
        {
            "general_issue_code": "SCI",
            "general_issue_code_display": "Science/Technology",
            "description": "Quantum computing policy, including S. 3597.",
            "foreign_entity_issues": "Bluefors OY has interest in this issue area.",
            "lobbyists": [
                {"lobbyist": {"id": 67914, "first_name": "GARY", "last_name": "GALLANT"},
                 "covered_position": None, "new": False},
                {"lobbyist": {"id": 70001, "first_name": "ANA", "last_name": "RUIZ"},
                 "covered_position": "Legislative Assistant, Sen. X", "new": True},
            ],
            "government_entities": [{"id": 2, "name": "HOUSE OF REPRESENTATIVES"}, {"id": 1, "name": "SENATE"}],
        },
        {"general_issue_code": "TRD", "description": "Export controls.", "lobbyists": [], "government_entities": []},
    ],
}


def test_each_activity_and_each_named_lobbyist_is_a_row_at_its_position(tmp_path, monkeypatch):
    """Decision 47: the lobbyists the filings table dropped become rows, keyed by position in the filing."""
    _no_prior(monkeypatch)
    _mock_http(monkeypatch, lambda request, number: _json_response(
        {"count": 1, "next": None, "previous": None, "results": [_FILING_WITH_ACTIVITIES]}))
    filings, activities, lobbyists = bld.build_lobbying_filings(tmp_path, since=date(2026, 9, 1), until=date(2026, 9, 2))

    assert [r["filing_uuid"] for r in pq.read_table(filings).to_pylist()] == ["f-1"]
    acts = pq.read_table(activities).to_pylist()
    assert [(a["activity_index"], a["general_issue_code"]) for a in acts] == [("0", "SCI"), ("1", "TRD")]
    assert json.loads(acts[0]["government_entities_json"]) == [{"id": 2, "name": "HOUSE OF REPRESENTATIVES"},
                                                               {"id": 1, "name": "SENATE"}]
    people = pq.read_table(lobbyists).to_pylist()
    assert [(p["activity_index"], p["lobbyist_index"], p["lobbyist_id"], p["last_name"], p["new"]) for p in people] == [
        ("0", "0", "67914", "GALLANT", "False"), ("0", "1", "70001", "RUIZ", "True")]
    assert people[1]["covered_position"] == "Legislative Assistant, Sen. X"


def test_the_rollup_declares_its_two_new_tables_as_an_explicit_migration():
    from spicy_regs.pipelines.rollups.lobbying_filings import LobbyingFilingsRollup

    assert set(LobbyingFilingsRollup.outputs) - set(LobbyingFilingsRollup.added_tables) == {"lobbying_filings.parquet"}
