"""Hermetic tests for the shared Congress.gov helpers in ``spicy_regs.sources.congress_bills`` (no network).

The api.data.gov key's fallback chain, and one bill's detail record fetched
through the ``bill`` route's reader and proven to be the bill asked for. The
archive-wide list writer these tests once also covered was retired by plan A1
(decision 31); the bill family owns ``congress_bills``.
"""

from __future__ import annotations

import json

import httpx
import pytest
from spicy_docs.reading.paged_json import PagedJsonSourceError

from spicy_regs.sources.congress_bills import API_KEY_ENV_VARS, _resolve_api_key


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


# --------------------------------------------------------------------------- #
# bill_detail: one detail record through the same reader, proven to be the bill
# asked for (the bill family's pre-BILLSTATUS backfill builds status from it).
# --------------------------------------------------------------------------- #


def _detail_reader(body: dict):
    """A listing reader over a MockTransport answering the 92/hr/2185 detail route with ``body``."""
    from spicy_regs.sources.congress_bills import listing_reader

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v3/bill/92/hr/2185"
        assert request.headers["X-Api-Key"] == "test-key"
        assert "api_key" not in request.url.params
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps(body).encode()), headers={"content-type": "application/json"}
        )

    return listing_reader("test-key", httpx.MockTransport(respond))


def test_bill_detail_returns_the_record_and_its_capture():
    from spicy_docs.sources.congress.bill_status import BillIdentity

    from spicy_regs.sources.congress_bills import bill_detail, bill_detail_url

    identity = BillIdentity(congress=92, bill_type="hr", number=2185)
    record = {"congress": 92, "type": "HR", "number": "2185", "title": "An Act", "laws": []}
    with _detail_reader({"bill": record, "request": {"format": "json"}}) as reader:
        bill, capture = bill_detail(reader, identity)
    assert dict(bill) == record
    assert capture.status_code == 200
    assert capture.requested_url == bill_detail_url(identity)
    assert "api_key" not in capture.requested_url


def test_bill_detail_refuses_a_200_for_a_different_bill():
    """A success naming another bill is not this bill's record — refused, never shaped."""
    from spicy_docs.sources.congress.bill_status import BillIdentity

    from spicy_regs.sources.congress_bills import bill_detail

    identity = BillIdentity(congress=92, bill_type="hr", number=2185)
    other = {"congress": 92, "type": "HR", "number": "2186", "title": "Another act"}
    with _detail_reader({"bill": other}) as reader, pytest.raises(PagedJsonSourceError, match="identity differs"):
        bill_detail(reader, identity)


def test_bill_detail_refuses_an_empty_success():
    """An empty ``200`` is not absence and not a record: it omits the bill object, so it is refused."""
    from spicy_docs.sources.congress.bill_status import BillIdentity

    from spicy_regs.sources.congress_bills import bill_detail

    identity = BillIdentity(congress=92, bill_type="hr", number=2185)
    with _detail_reader({}) as reader, pytest.raises(PagedJsonSourceError, match="omitted its bill object"):
        bill_detail(reader, identity)
