"""Hermetic tests for the Senate LDA lobbying-filings ingest (no network).

Covers the pieces with real logic: the raw-filing → published-schema mapping
(``_shape``, including the nested activity/government-entity projections) and the
reader's DRF ``next``-following pagination + ``max_records`` bound.
"""

from __future__ import annotations

import json
from datetime import date

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.sources import lobbying_filings as source
from spicy_regs.sources.lobbying_filings import API_BASE, LobbyingFilingsError, LobbyingFilingsReader
from spicy_regs.transforms.build_lobbying_filings import COLUMNS, _bounded_until, _shape

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


def test_uses_post_sunset_lda_api_host():
    assert API_BASE == "https://lda.gov/api/v1"


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


def test_pagination_follows_next(monkeypatch):
    """The reader must follow the DRF ``next`` URL until it is null."""
    reader = LobbyingFilingsReader()
    pages = {
        None: _page(["a", "b"], "PAGE2", count=4),  # first request (params, url ignored by stub)
        "PAGE2": _page(["c", "d"], None, count=4),
    }
    calls: list[str | None] = []

    def fake_get(url: str, params: dict | None) -> dict | None:
        # First call carries params (url is the base); later calls pass the next url.
        key = None if params is not None else url
        calls.append(key)
        return pages[key]

    monkeypatch.setattr(reader, "_get", fake_get)
    got = [f["filing_uuid"] for f in reader._paginate()]
    assert got == ["a", "b", "c", "d"]
    assert calls == [None, "PAGE2"]


def test_pagination_respects_max_records(monkeypatch):
    reader = LobbyingFilingsReader(max_records=3)

    def fake_get(url: str, params: dict | None) -> dict | None:
        # One big page; max_records must stop iteration mid-page.
        return _page(["a", "b", "c", "d", "e"], None)

    monkeypatch.setattr(reader, "_get", fake_get)
    got = [f["filing_uuid"] for f in reader._paginate()]
    assert got == ["a", "b", "c"]


def test_pagination_sends_bounded_date_window(monkeypatch):
    reader = LobbyingFilingsReader(since=date(2026, 4, 1), until=date(2026, 5, 1))
    seen_params: dict[str, object] = {}

    def fake_get(url: str, params: dict | None) -> dict | None:
        assert params is not None
        seen_params.update(params)
        return _page([], None)

    monkeypatch.setattr(reader, "_get", fake_get)
    assert list(reader._paginate()) == []
    assert seen_params["filing_dt_posted_after"] == "2026-04-01"
    assert seen_params["filing_dt_posted_before"] == "2026-05-01"
    assert seen_params["ordering"] == "dt_posted"


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


def _mock_http(monkeypatch, handler):
    original = httpx.Client
    calls = []

    def record(request):
        calls.append(request)
        return handler(request, len(calls))

    monkeypatch.setattr(
        source.httpx, "Client", lambda **kwargs: original(transport=httpx.MockTransport(record), **kwargs)
    )
    return calls


def test_cold_start_uses_today_filter_without_archive_year_floor(monkeypatch):
    def respond(request, number):
        assert request.url.params["filing_dt_posted_before"] == date.today().isoformat()
        assert "filing_year" not in request.url.params
        assert "filing_dt_posted_after" not in request.url.params
        return httpx.Response(200, json=_page([], None))

    _mock_http(monkeypatch, respond)
    assert list(LobbyingFilingsReader().iter_records()) == []


@pytest.mark.parametrize("status", [400, 401, 403])
def test_permanent_request_or_credential_refusal_is_not_retried(monkeypatch, status):
    calls = _mock_http(monkeypatch, lambda request, number: httpx.Response(status, json={"detail": "refused"}))
    monkeypatch.setattr(source.time, "sleep", lambda seconds: pytest.fail("permanent refusal was retried"))
    with pytest.raises(LobbyingFilingsError, match=f"HTTP {status}"):
        list(LobbyingFilingsReader(api_key="synthetic-test-key").iter_records())
    assert len(calls) == 1
    assert calls[0].headers["Authorization"] == "Token synthetic-test-key"


@pytest.mark.parametrize("failure", [429, 503, "transport"])
def test_transient_requests_retry_and_recover(monkeypatch, failure):
    def respond(request, number):
        if number == 1:
            if failure == "transport":
                raise httpx.ReadTimeout("interrupted", request=request)
            return httpx.Response(failure)
        return httpx.Response(200, json=_page(["recovered"], None))

    calls = _mock_http(monkeypatch, respond)
    sleeps = []
    monkeypatch.setattr(source.time, "sleep", sleeps.append)
    assert [row["filing_uuid"] for row in LobbyingFilingsReader().iter_records()] == ["recovered"]
    assert len(calls) == 2 and sleeps == [2]


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
    monkeypatch.setattr(source, "_MAX_RETRIES", 3)
    sleeps = []
    monkeypatch.setattr(source.time, "sleep", sleeps.append)

    def respond(request, number):
        if failure_page == 2 and number == 1:
            return httpx.Response(
                200, json={"count": 2, "next": f"{API_BASE}/filings/?page=2", "results": [_RAW_FILING]}
            )
        if failure == "transport":
            raise httpx.ReadTimeout("interrupted", request=request)
        return httpx.Response(failure, json={"detail": "failure"})

    calls = _mock_http(monkeypatch, respond)
    with pytest.raises(LobbyingFilingsError):
        LobbyingFilingsRollup(output_dir=tmp_path, skip_upload=False).run()
    assert len(calls) == failure_page + (2 if failure in {503, "transport"} else 0)
    assert sleeps == ([2, 4] if failure in {503, "transport"} else [])
    assert not (tmp_path / "generations").exists()
    assert not list((tmp_path / ".builds").rglob(OUTPUT))
    assert not list(tmp_path.rglob("_lda_new.parquet"))
    if have_prior:
        assert prior.read_bytes() == original
        assert pq.read_table(prior).to_pylist()[0]["filing_uuid"] == _RAW_FILING["filing_uuid"]
        assert next((tmp_path / ".builds").rglob("_lda_prior.parquet")).read_bytes() == original
    else:
        assert not prior.exists()


@pytest.mark.parametrize(
    "payload",
    [
        {"detail": "not a result page"},
        {"count": 0, "results": [], "next": 2},
        {"count": -1, "results": [], "next": None},
        {"count": 1, "results": [{}], "next": None},
        {"count": 1, "results": [{"filing_uuid": ""}], "next": None},
        {"count": 12, "results": [], "next": None},
    ],
)
def test_malformed_success_cannot_become_empty_or_partial_output(tmp_path, monkeypatch, payload):
    from spicy_regs.sources import r2
    from spicy_regs.transforms.build_lobbying_filings import build_lobbying_filings

    monkeypatch.setattr(r2, "download", lambda *args: False)
    _mock_http(monkeypatch, lambda request, number: httpx.Response(200, json=payload))
    with pytest.raises(LobbyingFilingsError):
        build_lobbying_filings(tmp_path)
    assert not list(tmp_path.glob("*.parquet"))


def test_invalid_json_refuses_instead_of_exhausting(monkeypatch):
    _mock_http(monkeypatch, lambda request, number: httpx.Response(200, content=b"not json"))
    with pytest.raises(LobbyingFilingsError, match="invalid JSON"):
        list(LobbyingFilingsReader().iter_records())


def test_changed_count_refuses(monkeypatch):
    def respond(request, number):
        return httpx.Response(200, json=_page([str(number)], f"{API_BASE}/filings/?page=2", count=number + 1))

    _mock_http(monkeypatch, respond)
    with pytest.raises(LobbyingFilingsError, match="count changed"):
        list(LobbyingFilingsReader().iter_records())


def test_explicit_sample_can_stop_early(monkeypatch):
    _mock_http(monkeypatch, lambda request, number: httpx.Response(200, json=_page(["sample"], "unused", count=100)))
    assert list(LobbyingFilingsReader(max_records=1).iter_records()) == [{"filing_uuid": "sample"}]
