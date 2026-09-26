"""Hermetic tests for the CourtListener APA-litigation ingest (no network).

Covers the pieces with real logic: the raw-search-result → published-schema
mapping (``_shape``, including array-field JSON serialization and URL
absolutization) and the reader's cursor ``next``-following pagination +
``max_records`` bound.
"""

from __future__ import annotations

import json
import importlib

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_docs.transport.credentials import CredentialRefusedError
from spicy_regs.sources import courtlistener as source
from spicy_regs.sources.courtlistener import CourtListenerReader, CourtListenerOpinionSearchReader
from spicy_regs.transforms.build_courtlistener import COLUMNS, _shape

_RAW_DOCKET = {
    "docket_id": 73613631,
    "caseName": "HENNEPIN COUNTY, MINNESOTA v. U.S. DEPARTMENT OF HEALTH AND HUMAN SERVICES",
    "case_name_full": "",
    "court_id": "dcd",
    "court": "District Court, District of Columbia",
    "court_citation_string": "D.D.C.",
    "docketNumber": "1:26-cv-02460",
    "dateFiled": "2026-07-14",
    "dateTerminated": None,
    "dateArgued": None,
    "suitNature": "899 Other Statutes: Administrative Procedures Act/Review or Appeal of Agency Decision",
    "cause": "05:551 Administrative Procedure Act",
    "jurisdictionType": "U.S. Government Defendant",
    "juryDemand": "None",
    "assignedTo": "Christopher Reid Cooper",
    "referredTo": None,
    "party": [
        "HENNEPIN COUNTY, MINNESOTA",
        "U.S. DEPARTMENT OF HEALTH AND HUMAN SERVICES",
        None,  # blanks are dropped
    ],
    "attorney": ["Skye Perryman", "Allison Marcy Zieve"],
    "firm": ["Democracy Forward", "Public Citizen Litigation Group"],
    "pacer_case_id": "294455",
    "docket_absolute_url": "/docket/73613631/hennepin-county-minnesota-v-us-dept-of-hhs/",
    "meta": {"date_created": "2026-07-14T17:38:04.836183Z"},
    # Intentionally dropped by _shape (document-level, not docket-level):
    "recap_documents": [{"docket_entry_id": 471006633}],
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_DOCKET)
    assert set(row) == set(COLUMNS)


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_DOCKET)
    # Integer id stringifies (schema is all-VARCHAR).
    assert row["cl_docket_id"] == "73613631"
    assert row["case_name"].startswith("HENNEPIN COUNTY")
    # Empty full caption normalizes to NULL, not "".
    assert row["case_name_full"] is None
    assert row["court_id"] == "dcd"
    assert row["court_citation_string"] == "D.D.C."
    assert row["docket_number"] == "1:26-cv-02460"
    assert row["date_filed"] == "2026-07-14"
    assert row["date_terminated"] is None
    assert row["nature_of_suit"].startswith("899 ")
    assert row["cause"] == "05:551 Administrative Procedure Act"
    assert row["jurisdiction_type"] == "U.S. Government Defendant"
    assert row["assigned_to"] == "Christopher Reid Cooper"
    assert row["pacer_case_id"] == "294455"
    assert row["date_created"] == "2026-07-14T17:38:04.836183Z"
    # docket_absolute_url is absolutized against the CourtListener host.
    assert row["absolute_url"] == (
        "https://www.courtlistener.com/docket/73613631/hennepin-county-minnesota-v-us-dept-of-hhs/"
    )
    # Array fields serialize to JSON, dropping blank/None entries.
    parties = json.loads(row["parties_json"])
    assert parties == [
        "HENNEPIN COUNTY, MINNESOTA",
        "U.S. DEPARTMENT OF HEALTH AND HUMAN SERVICES",
    ]
    assert json.loads(row["attorneys_json"]) == ["Skye Perryman", "Allison Marcy Zieve"]
    assert json.loads(row["firms_json"]) == ["Democracy Forward", "Public Citizen Litigation Group"]


def test_shape_handles_missing_fields():
    row = _shape({"docket_id": 42})
    assert row["cl_docket_id"] == "42"
    assert row["case_name"] is None
    assert row["parties_json"] == "[]"
    assert row["attorneys_json"] == "[]"
    assert row["firms_json"] == "[]"
    assert row["absolute_url"] is None
    assert row["date_created"] is None


def _page(ids, next_url=None, *, total=None, kind="r"):
    """A search-response envelope; ``kind`` selects docket (``r``) or cluster (``o``) ids."""
    return {
        "count": len(ids) if total is None else total,
        "next": next_url,
        "previous": None,
        "results": [{"docket_id" if kind == "r" else "cluster_id": i} for i in ids],
    }


class Transport(httpx.MockTransport):
    """Queued-response mock transport; an ``Exception`` or ``int`` entry is raised or served as that status."""

    def __init__(self, *responses):
        self.responses = iter(responses)
        self.calls = []
        super().__init__(self.handle)

    def handle(self, request):
        self.calls.append(request)
        response = next(self.responses)
        if isinstance(response, Exception):
            raise response
        return httpx.Response(
            response if isinstance(response, int) else 200,
            stream=httpx.ByteStream(json.dumps(response).encode()),
            headers={"content-type": "application/json"},
        )


@pytest.fixture(autouse=True)
def bounded_requests(monkeypatch):
    """Cap the reader's per-page retry budget so a transport failure surfaces immediately; run unpaced."""
    monkeypatch.setattr(source, "_MAX_REQUESTS_PER_PAGE", 1)
    monkeypatch.setattr(source, "MIN_REQUEST_INTERVAL_SECONDS", 0.0)


NEXT = f"{source.API_BASE}/search/?cursor=second"


def test_pagination_follows_cursor_next():
    """Pins the bearer-style ``Authorization`` header and that the token never enters the URL."""
    transport = Transport(_page([1, 2], NEXT, total=4), _page([3, 4], total=4))
    reader = CourtListenerReader(transport=transport, api_token="fixture-token")
    assert [row["docket_id"] for row in reader.iter_records()] == [1, 2, 3, 4]
    assert str(transport.calls[1].url) == NEXT
    assert all(r.headers["Authorization"] == "Token fixture-token" for r in transport.calls)
    assert all("fixture-token" not in str(r.url) for r in transport.calls)


def test_pagination_respects_explicit_record_cap():
    transport = Transport(_page([1, 2, 3, 4, 5], NEXT, total=9))
    assert [row["docket_id"] for row in CourtListenerReader(max_records=3, transport=transport).iter_records()] == [
        1,
        2,
        3,
    ]
    assert len(transport.calls) == 1


def test_since_sets_filed_after_param():
    from datetime import date

    transport = Transport(_page([1]))
    list(CourtListenerReader(since=date(2024, 7, 1), transport=transport).iter_records())
    params = transport.calls[0].url.params
    assert params["filed_after"] == "07/01/2024"
    assert params["type"] == "r"
    assert params["nature_of_suit"] == "899"


def test_opinion_selection_uses_cluster_identity_and_court():
    transport = Transport(_page([4], kind="o"))
    assert list(CourtListenerOpinionSearchReader(court="dcd", transport=transport).iter_records()) == [
        {"cluster_id": 4}
    ]
    params = transport.calls[0].url.params
    assert params["type"] == "o" and params["court"] == "dcd"
    assert "nature_of_suit" not in params


def test_opinion_selection_above_an_id_ignores_filing_date():
    transport = Transport(_page([11], kind="o"))
    list(CourtListenerOpinionSearchReader(above=10, transport=transport).iter_records())
    params = transport.calls[0].url.params
    assert params["q"] == "cluster_id:[11 TO *]" and "filed_after" not in params
    with pytest.raises(ValueError, match="non-negative integer"):
        CourtListenerOpinionSearchReader(above=-1)


@pytest.mark.parametrize("status", [401, 403, 404, 429, 500])
def test_source_refusal_is_not_empty_success(status):
    with pytest.raises((ValueError, ConnectionError, httpx.HTTPError, CredentialRefusedError)):
        list(CourtListenerReader(transport=Transport(status)).iter_records())


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"results": []},
        _page([None]),
        _page([True]),
        _page([0]),
        _page([1, 1]),
        _page([], total=1),
        _page([], NEXT, total=1),
        {"count": 1, "results": "not-a-list", "next": None},
        _page([1], "https://untrusted.example/search?cursor=x", total=2),
    ],
)
def test_malformed_response_refuses(payload):
    with pytest.raises(ValueError):
        list(CourtListenerReader(transport=Transport(payload)).iter_records())


@pytest.mark.parametrize(
    "second",
    [
        500,
        _page([1], total=2),
        _page([2], total=3),
        _page([], total=2),
        {"count": 2, "results": [{"docket_id": 2}]},
        _page([], NEXT, total=2),
    ],
)
def test_later_page_failure_preserves_prior_and_existing_output(monkeypatch, tmp_path, second):
    module = importlib.import_module("spicy_regs.transforms.build_courtlistener")
    table = pa.Table.from_pylist([module._shape(_RAW_DOCKET)], schema=module._SCHEMA)
    prior, output = tmp_path / "_cl_prior.parquet", tmp_path / "court_dockets.parquet"
    pq.write_table(table, prior)
    pq.write_table(table, output)
    before = prior.read_bytes(), output.read_bytes()
    reader = CourtListenerReader(transport=Transport(_page([1], NEXT, total=2), second))
    monkeypatch.setattr(module, "CourtListenerReader", lambda **kwargs: reader)
    with pytest.raises((ValueError, ConnectionError, httpx.HTTPError, CredentialRefusedError)):
        module.build_courtlistener(tmp_path)
    assert (prior.read_bytes(), output.read_bytes()) == before
    assert not (tmp_path / "_cl_new.parquet").exists()


def test_repeated_cursor_and_page_budget_refuse(monkeypatch):
    with pytest.raises(ValueError, match="repeated its continuation"):
        list(
            CourtListenerReader(
                transport=Transport(_page([1], NEXT, total=3), _page([2], NEXT, total=3))
            ).iter_records()
        )
    monkeypatch.setattr(source, "_MAX_PAGES", 1)
    with pytest.raises(ValueError, match="page bound"):
        list(CourtListenerReader(transport=Transport(_page([1], NEXT, total=3))).iter_records())


def test_valid_empty_selection_and_cap_page_validation():
    assert list(CourtListenerReader(transport=Transport(_page([]))).iter_records()) == []
    with pytest.raises(ValueError):
        list(CourtListenerReader(max_records=1, transport=Transport(_page([1, None]))).iter_records())


@pytest.mark.parametrize("bound", [0, -1, True, 1.5])
def test_invalid_cap_refuses(bound):
    with pytest.raises(ValueError):
        CourtListenerReader(max_records=bound)


@pytest.mark.parametrize("counts", [(2100, 2110), (2001, 2010)])
def test_docket_estimated_count_does_not_replace_terminal_cursor(counts):
    """A later total that disagrees with the first page's estimate must not invalidate the walk."""
    transport = Transport(
        _page(list(range(1, 1001)), NEXT, total=counts[0]), _page(list(range(1001, 2051)), total=counts[1])
    )
    rows = list(CourtListenerReader(transport=transport).iter_records())
    assert [row["docket_id"] for row in rows] == list(range(1, 2051))
    assert len(transport.calls) == 2


@pytest.mark.parametrize("cap", [None, 1])
@pytest.mark.parametrize("kind,reader", [("r", CourtListenerReader), ("o", CourtListenerOpinionSearchReader)])
def test_exact_terminal_count_still_refuses_before_capped_yield(cap, kind, reader):
    transport = Transport(_page([1], total=2, kind=kind))
    with pytest.raises(ValueError, match="terminal page disagrees"):
        list(reader(max_records=cap, transport=transport).iter_records())


def test_docket_ids_pack_in_order_into_queries_within_the_limit():
    ids = [str(100_000_000 + n) for n in range(100)]
    queries = source.docket_id_queries(ids)
    assert len(queries) == 3 and all(len(q) <= source.QUERY_LIMIT for q in queries)
    assert [i for q in queries for i in q.removeprefix("docket_id:(").removesuffix(")").split(" OR ")] == ids
    assert source.docket_id_queries([]) == []
    with pytest.raises(ValueError, match="does not fit"):
        source.docket_id_queries(["9" * 600])


def test_every_page_request_is_paced(monkeypatch):
    import time

    monkeypatch.setattr(source, "MIN_REQUEST_INTERVAL_SECONDS", 0.2)
    started = time.monotonic()
    list(CourtListenerReader(transport=Transport(_page([1], NEXT, total=2), _page([2], total=2))).iter_records())
    assert time.monotonic() - started >= 0.2


def test_a_run_fills_a_bounded_slice_of_unnamed_dockets_and_flags_case_type(monkeypatch, tmp_path):
    module = importlib.import_module("spicy_regs.transforms.build_courtlistener")
    named = {**module._shape(_RAW_DOCKET), "cl_docket_id": "1"}
    unnamed = [{**named, "cl_docket_id": str(100_000_000 + n), "parties_json": None,
                "docket_number": "22-16094" if n == 0 else f"2:19-cr-{n:05d}"} for n in range(50)]
    pq.write_table(pa.Table.from_pylist([named, *unnamed], schema=module._SCHEMA), tmp_path / "_cl_prior.parquet")
    asked = []

    class Named:
        def __init__(self, *, query, evidence=None):
            asked.append(query)
            self.ids = query.removeprefix("docket_id:(").removesuffix(")").split(" OR ")

        def iter_records(self):
            return iter({"docket_id": int(i), "party": [f"party of {i}"], "docketNumber": "22-16094"} for i in self.ids)

    monkeypatch.setattr(module, "CourtListenerReader", lambda **kwargs: CourtListenerReader(transport=Transport(_page([]))))
    monkeypatch.setattr(module, "CourtListenerDocketIdReader", Named)
    monkeypatch.setattr(module, "FILL_QUERIES_PER_RUN", 1)

    rows = {r["cl_docket_id"]: r for r in pq.read_table(module.build_courtlistener(tmp_path)).to_pylist()}
    filled = asked[0].removeprefix("docket_id:(").removesuffix(")").split(" OR ")
    assert len(asked) == 1 and filled == [row["cl_docket_id"] for row in unnamed[:len(filled)]]
    assert all(json.loads(rows[i]["parties_json"]) == [f"party of {i}"] for i in filled)
    assert all(rows[row["cl_docket_id"]]["parties_json"] is None for row in unnamed[len(filled):])
    assert rows["1"]["case_type"] == "cv" and rows[filled[0]]["case_type"] is None
    assert rows[unnamed[-1]["cl_docket_id"]]["case_type"] == "cr"
    assert list(rows["1"]) == list(module.PUBLISHED_COLUMNS)
