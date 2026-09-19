"""Hermetic tests for the Congress.gov bill ingest (no network).

Covers the pieces with real logic: the raw-bill → published-schema mapping
(``_shape`` / ``_bill_id``), the API-key resolution fallback chain, the fetch
window this repo still computes (``_bounded_until``), and — at the bottom —
an end-to-end ``build_congress_bills()`` run against a seeded prior table and
a stubbed fetch, plus the actual delegation to spicy-docs'
``CongressListingReader`` over an ``httpx.MockTransport`` (gap D2 / SR01:
this repo no longer hand-rolls the ``offset``/``limit`` walk; see
``spicy_regs.sources.congress_bills``'s module docstring for what moved and
why). The unit tests above ``test_build_congress_bills_merges_prior_and_fresh_rows``
never call ``build_congress_bills()`` or read the merged Parquet, so only that
test exercises the SQL merge end to end.

The last section covers the retry *policy* this reader's absolute refusal
made necessary: ``_fetch_bills`` in ``build_congress_bills.py`` retries an
identical window a bounded number of times when the reader refuses on a
mid-walk publisher drift, and gives up loudly — never publishing a partial
table — once that bound is spent. ``test_walk_refuses_when_the_declared_count_changes_mid_walk``
pins the exact refusal shape (a two-page walk whose declared count shrinks
between pages) that policy exists for.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from importlib import import_module

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.reading.paged_json import PagedJsonSourceError

from spicy_regs.sources.congress_bills import (
    API_KEY_ENV_VARS,
    CongressBillsReader,
    _resolve_api_key,
)
from spicy_regs.transforms.build_congress_bills import (
    COLUMNS,
    MAX_WINDOW_DAYS,
    _bill_id,
    _bounded_until,
    _shape,
)

# spicy_regs.transforms/__init__.py does `from .build_congress_bills import
# build_congress_bills`, which rebinds the *package* attribute
# `spicy_regs.transforms.build_congress_bills` to that function — so
# `import spicy_regs.transforms.build_congress_bills as bcb` would silently
# bind `bcb` to the function, not the submodule. import_module() reads
# sys.modules directly and is not fooled by that shadowing.
bcb = import_module("spicy_regs.transforms.build_congress_bills")

_RAW_BILL = {
    "congress": 118,
    "type": "HR",
    "number": 1234,
    "title": "A Bill To Do A Thing",
    "originChamber": "House",
    "latestAction": {"actionDate": "2024-03-01", "text": "Referred to committee."},
    "updateDate": "2024-03-05",
    "url": "https://api.congress.gov/v3/bill/118/hr/1234?format=json",
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_BILL)
    # Every published column present, and nothing extra (10-column schema).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 10


def test_shape_maps_and_serializes_fields():
    row = _shape(_RAW_BILL)
    # bill_id is built from congress + lowercased type + number.
    assert row["bill_id"] == "118-hr-1234"
    assert row["congress"] == "118"
    assert row["bill_type"] == "hr"  # lowercased
    assert row["bill_number"] == "1234"  # int stringified
    assert row["title"] == "A Bill To Do A Thing"
    assert row["origin_chamber"] == "House"
    # Nested latestAction is flattened.
    assert row["latest_action_date"] == "2024-03-01"
    assert row["latest_action_text"] == "Referred to committee."
    assert row["update_date"] == "2024-03-05"


def test_shape_handles_missing_nested_and_scalars():
    row = _shape({"congress": 117, "type": "S", "number": 5})
    assert row["bill_id"] == "117-s-5"
    # Missing nested objects degrade to null, not KeyError.
    assert row["latest_action_date"] is None
    assert row["latest_action_text"] is None
    assert row["origin_chamber"] is None


def test_bill_id_requires_all_parts():
    assert _bill_id({"congress": 118, "type": "hr", "number": 1}) == "118-hr-1"
    # Any missing component yields None (row is dropped downstream).
    assert _bill_id({"type": "hr", "number": 1}) is None
    assert _bill_id({"congress": 118, "number": 1}) is None
    assert _bill_id({"congress": 118, "type": "hr"}) is None


# -- API-key resolution ------------------------------------------------------


def test_resolve_api_key_prefers_first_env_var(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setenv("DATA_GOV_API_KEY", "data-gov-key")
    monkeypatch.setenv("CONGRESS_GOV_API_KEY", "congress-key")
    assert _resolve_api_key() == "data-gov-key"


def test_resolve_api_key_falls_back_in_order(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    # Only the last one set — the fallback chain should still find it.
    monkeypatch.setenv("REGULATIONS_GOV_API_KEY", "regs-key")
    assert _resolve_api_key() == "regs-key"


def test_resolve_api_key_returns_none_when_unset(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    assert _resolve_api_key() is None


def test_reader_yields_nothing_without_key(monkeypatch):
    for var in API_KEY_ENV_VARS:
        monkeypatch.delenv(var, raising=False)
    reader = CongressBillsReader()
    # No key configured: a keyless run is a no-op, not a crash — and this
    # must not require spicy-docs' optional source-readers extra, since
    # iter_records() returns before ever importing spicy_docs.
    assert list(reader.iter_records()) == []


# -- the walk, delegated to spicy-docs' CongressListingReader ---------------


def _bill(number: int, day: str) -> dict:
    return {"congress": 118, "type": "hr", "number": number, "updateDate": day}


def test_reader_walks_the_bill_route_and_yields_raw_dicts():
    """The window this repo computes reaches the wire as fromDateTime/toDateTime
    on the measured ``bill`` route; the key travels as a header, never the query
    string — the whole walk is spicy-docs' CongressListingReader, not a local loop."""
    bills = [_bill(1, "2025-04-07"), _bill(2, "2025-04-06")]

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.host == "api.congress.gov"
        assert request.url.path == "/v3/bill"
        assert request.headers["X-Api-Key"] == "test-key"
        assert "api_key" not in request.url.params  # header-only, never the URL
        assert request.url.params["sort"] == "updateDate desc"
        assert request.url.params["fromDateTime"] == "2025-04-04T00:00:00Z"
        assert request.url.params["toDateTime"] == "2025-04-08T00:00:00Z"
        body = {"bills": bills, "pagination": {"count": len(bills)}}
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps(body).encode()), headers={"content-type": "application/json"}
        )

    reader = CongressBillsReader(
        since=date(2025, 4, 4),
        until=date(2025, 4, 8),
        api_key="test-key",
        transport=httpx.MockTransport(respond),
    )
    assert list(reader.iter_records()) == bills


def test_no_since_or_until_sends_no_window_bound():
    """A full backfill (no watermark yet) must not accidentally bound itself."""

    def respond(request: httpx.Request) -> httpx.Response:
        assert "fromDateTime" not in request.url.params
        assert "toDateTime" not in request.url.params
        body = {"bills": [], "pagination": {"count": 0}}
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps(body).encode()), headers={"content-type": "application/json"}
        )

    reader = CongressBillsReader(api_key="test-key", transport=httpx.MockTransport(respond))
    assert list(reader.iter_records()) == []


def test_walk_refuses_when_declared_and_observed_counts_disagree():
    """Surfaces spicy-docs' own walk-consistency refusal here: this repo no
    longer hand-rolls pagination, so the discipline against an inconsistent or
    truncated walk — the shape of the 510-day freeze — now lives in, and is
    enforced by, the shared spicy-docs reader rather than a local completeness
    warning."""

    def respond(request: httpx.Request) -> httpx.Response:
        # The publisher declares 5 but the (sole, terminal) page only carries 1.
        body = {"bills": [_bill(1, "2025-04-07")], "pagination": {"count": 5}}
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps(body).encode()), headers={"content-type": "application/json"}
        )

    reader = CongressBillsReader(api_key="test-key", transport=httpx.MockTransport(respond))
    with pytest.raises(PagedJsonSourceError, match="differ"):
        list(reader.iter_records())


def test_walk_refuses_when_the_declared_count_changes_mid_walk():
    """The operational risk ``_fetch_bills`` retries for: a nightly window that
    closes at "now" is walked over several minutes, and a bill the publisher
    edits *during* that walk can move its own ``updateDate`` past
    ``toDateTime`` and drop out of the window — the declared count shrinks
    between page one and page two of the *same* walk, not only at the end.
    The reader refuses immediately rather than publish a shrunk result;
    retrying the identical window is ``build_congress_bills.py``'s job, not
    this reader's (see ``test_congress_bills.py``'s retry-policy tests)."""
    page_one_bills = [_bill(1, "2025-04-07"), _bill(2, "2025-04-06")]
    page_two_bills = [_bill(3, "2025-04-05")]
    next_url = "https://api.congress.gov/v3/bill?format=json&limit=250&offset=250&sort=updateDate+desc"
    calls = {"n": 0}

    def respond(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            # Page one declares 3 bills total and points at a next page.
            body = {"bills": page_one_bills, "pagination": {"count": 3, "next": next_url}}
        else:
            # Page two, the walk's terminal page: the publisher now declares
            # only 2 — one fewer than page one stated, mid-walk.
            assert str(request.url) == next_url
            body = {"bills": page_two_bills, "pagination": {"count": 2}}
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps(body).encode()), headers={"content-type": "application/json"}
        )

    reader = CongressBillsReader(api_key="test-key", transport=httpx.MockTransport(respond))
    with pytest.raises(PagedJsonSourceError, match="declared count changed during the traversal"):
        list(reader.iter_records())
    assert calls["n"] == 2


# -- catch-up windowing ------------------------------------------------------


def test_window_is_capped_so_a_deep_backfill_converges():
    """An unbounded catch-up publishes nothing and retries forever; cap it.

    The 510-day freeze needed 238k bills in one walk. That run hit the job
    timeout at 190k and persisted nothing. Capping the window makes each run
    publish and advance the watermark.
    """
    since = date(2025, 4, 4)
    until = _bounded_until(since, None, today=date(2026, 8, 30))
    assert until == since + timedelta(days=MAX_WINDOW_DAYS)


def test_explicit_until_is_still_capped():
    since = date(2025, 4, 4)
    # An operator asking for the whole 510-day span still gets one window.
    assert _bounded_until(since, date(2026, 8, 30)) == since + timedelta(days=MAX_WINDOW_DAYS)


def test_short_explicit_window_is_left_alone():
    since, until = date(2026, 8, 1), date(2026, 8, 10)
    assert _bounded_until(since, until) == until


def test_caught_up_run_stops_at_today():
    """Steady state: the cap must not push the window past now."""
    since, today = date(2026, 8, 27), date(2026, 8, 30)
    assert _bounded_until(since, None, today=today) == today


def test_backwards_window_is_rejected():
    with pytest.raises(ValueError, match="precedes"):
        _bounded_until(date(2026, 8, 10), date(2026, 8, 1))


def test_full_backfill_has_no_upper_bound():
    """No prior table means no watermark to window from."""
    assert _bounded_until(None, None) is None


# -- end-to-end merge (build_congress_bills) ----------------------------------

_PRIOR_ROWS = [
    {
        "bill_id": "118-hr-1",
        "congress": "118",
        "bill_type": "hr",
        "bill_number": "1",
        "title": "Old Title",
        "origin_chamber": "House",
        "latest_action_date": "2024-01-01",
        "latest_action_text": "Old action",
        "update_date": "2024-01-01",
        "url": "https://api.congress.gov/v3/bill/118/hr/1?format=json",
    },
    {
        "bill_id": "118-s-2",
        "congress": "118",
        "bill_type": "s",
        "bill_number": "2",
        "title": "Prior Only",
        "origin_chamber": "Senate",
        "latest_action_date": "2024-01-02",
        "latest_action_text": "Prior action",
        "update_date": "2024-01-02",
        "url": "https://api.congress.gov/v3/bill/118/s/2?format=json",
    },
]

_FRESH_RAW = [
    # Repeats 118-hr-1's identity with a later update_date — must win over the
    # seeded prior row.
    {
        "congress": 118,
        "type": "HR",
        "number": 1,
        "title": "New Title",
        "originChamber": "House",
        "latestAction": {"actionDate": "2024-03-01", "text": "New action"},
        "updateDate": "2024-03-01",
        "url": "https://api.congress.gov/v3/bill/118/hr/1?format=json",
    },
    # A bill absent from the prior table entirely.
    {
        "congress": 119,
        "type": "HR",
        "number": 99,
        "title": "Brand New",
        "originChamber": "House",
        "latestAction": {"actionDate": "2024-02-01", "text": "Introduced"},
        "updateDate": "2024-02-01",
        "url": "https://api.congress.gov/v3/bill/119/hr/99?format=json",
    },
]


class _FakeReader:
    """Stands in for CongressBillsReader: no network, no API key needed."""

    def __init__(self, *, since=None, until=None):
        self.since = since
        self.until = until

    def iter_records(self):
        return iter(_FRESH_RAW)


def test_build_congress_bills_merges_prior_and_fresh_rows(tmp_path, monkeypatch):
    """Seed a prior Parquet, stub the fetch and the R2 download, and assert on
    the actual merged output — fresh wins on a repeated ``bill_id``, a
    prior-only row survives, and the result is ordered by ``update_date`` then
    ``bill_id``. This is the behavior-preservation proof for routing
    ``build_congress_bills`` through ``table_merge.merge_table``: the unit
    tests above never call ``build_congress_bills()`` or read merged Parquet,
    so only this test actually exercises the SQL end to end.
    """

    def fake_download(remote_key, local_path):
        assert remote_key == bcb.OUTPUT
        schema = pa.schema([(c, pa.string()) for c in COLUMNS])
        table = pa.Table.from_pylist(_PRIOR_ROWS, schema=schema)
        pq.write_table(table, local_path)
        return True

    monkeypatch.setattr(bcb.r2, "download", fake_download)
    monkeypatch.setattr(bcb, "CongressBillsReader", _FakeReader)

    out_path = bcb.build_congress_bills(tmp_path)

    rows = pq.read_table(out_path).to_pylist()
    by_id = {row["bill_id"]: row for row in rows}

    # Fresh wins on the repeated identity: title/update_date come from the
    # freshly fetched row, not the seeded prior one.
    assert by_id["118-hr-1"]["title"] == "New Title"
    assert by_id["118-hr-1"]["update_date"] == "2024-03-01"

    # The prior-only row survives the merge untouched.
    assert by_id["118-s-2"]["title"] == "Prior Only"

    # The fresh-only row is present too.
    assert "119-hr-99" in by_id

    # Ordered by update_date DESC, then bill_id.
    assert [row["bill_id"] for row in rows] == ["118-hr-1", "119-hr-99", "118-s-2"]


# -- retrying a mid-walk publisher drift (build_congress_bills._fetch_bills) --


def _flaky_reader_factory(*, fail_times: int) -> tuple[type, dict]:
    """A CongressBillsReader stand-in whose first ``fail_times`` constructions
    raise from ``iter_records()`` (the shape of a mid-walk declared-count
    drift), then succeed. Returns ``(cls, calls)``: the counter dict is
    shared across instances — each retry attempt in ``_fetch_bills`` builds a
    brand-new reader, exactly like a real retry would — so a test can assert
    exactly how many attempts were made without needing a class attribute
    (a class body cannot close over this factory's locals the way a method
    can, so the counter travels back to the caller instead).
    """
    calls = {"n": 0}

    class _FlakyReader:
        def __init__(self, *, since=None, until=None):
            self.since = since
            self.until = until

        def iter_records(self):
            calls["n"] += 1
            if calls["n"] <= fail_times:
                error = PagedJsonSourceError("Congress.gov declared count changed during the traversal")
                # Mirrors spicy-docs' own paged_json.py, which attaches this
                # context via __dict__ rather than a declared attribute.
                error.__dict__["paged_json_acquisition"] = {"declaredCount": 3, "observedCount": 2}
                raise error
            return iter(_FRESH_RAW)

    return _FlakyReader, calls


def test_fetch_retries_a_walk_refusal_and_succeeds(tmp_path, monkeypatch):
    """First attempt raises (the mid-walk drift), second succeeds — the retry
    this reader's absolute refusal exists for; the reader itself never
    weakens, but ``build_congress_bills`` asks the same window again."""
    monkeypatch.setattr(bcb.r2, "download", lambda remote_key, local_path: False)
    monkeypatch.setattr(bcb.time, "sleep", lambda seconds: None)
    reader_cls, calls = _flaky_reader_factory(fail_times=1)
    monkeypatch.setattr(bcb, "CongressBillsReader", reader_cls)

    out_path = bcb.build_congress_bills(tmp_path)

    rows = pq.read_table(out_path).to_pylist()
    assert {row["bill_id"] for row in rows} == {"118-hr-1", "119-hr-99"}
    assert calls["n"] == 2  # one refusal, one successful retry


def test_fetch_gives_up_after_max_attempts(tmp_path, monkeypatch):
    """A refusal that never clears exhausts every attempt and propagates —
    the job fails loudly instead of publishing a table it never fully walked."""
    monkeypatch.setattr(bcb.r2, "download", lambda remote_key, local_path: False)
    monkeypatch.setattr(bcb.time, "sleep", lambda seconds: None)
    reader_cls, calls = _flaky_reader_factory(fail_times=bcb.FETCH_ATTEMPTS + 10)
    monkeypatch.setattr(bcb, "CongressBillsReader", reader_cls)

    with pytest.raises(PagedJsonSourceError, match="declared count changed during the traversal"):
        bcb.build_congress_bills(tmp_path)

    # Exactly FETCH_ATTEMPTS attempts — no more (it must stop), no fewer (it
    # must actually retry rather than propagate the first refusal).
    assert calls["n"] == bcb.FETCH_ATTEMPTS
