"""USAspending field mapping; page failure and selection bound tests are in test_reference_source_failures."""

from __future__ import annotations

import pytest

from spicy_regs.transforms.build_usaspending_recipients import COLUMNS, _shape

_RAW_RECIPIENT = {
    "id": "b97d19b0-833c-8d8f-3a2c-157d04ea55ef-P",
    "duns": "834951691",
    "uei": "ZFN2JJXBLZT3",
    "name": "LOCKHEED MARTIN CORP",
    "recipient_level": "P",
    "amount": 63465270734.15,
}


def test_shape_produces_exact_schema():
    row = _shape(_RAW_RECIPIENT)
    # Every published column present, and nothing extra (including observation metadata).
    assert set(row) == set(COLUMNS)
    assert len(COLUMNS) == 8


def test_shape_maps_and_stringifies_fields():
    row = _shape(_RAW_RECIPIENT)
    assert row["recipient_id"] == "b97d19b0-833c-8d8f-3a2c-157d04ea55ef-P"
    assert row["uei"] == "ZFN2JJXBLZT3"
    assert row["duns"] == "834951691"
    assert row["name"] == "LOCKHEED MARTIN CORP"
    assert row["recipient_level"] == "P"
    # Numeric amount is coerced to a string, not left as a float.
    assert row["total_award_amount"] == "63465270734.15"


def test_shape_handles_missing_fields():
    row = _shape({"id": "x-R"})
    assert row["recipient_id"] == "x-R"
    # Missing scalars degrade to None, not KeyError.
    assert row["uei"] is None
    assert row["duns"] is None
    assert row["name"] is None
    assert row["recipient_level"] is None
    # Missing amount stays None (not the string "None").
    assert row["total_award_amount"] is None


def _ranking(*pages: list[float]):
    """A MockTransport serving the recipient ranking page by page, each amount one recipient."""
    import json

    import httpx

    served = []

    def respond(request: httpx.Request) -> httpx.Response:
        page = len(served) + 1  # the walk is sequential: the nth request asks for page n
        served.append(page)
        amounts = pages[page - 1]
        results = [{"id": f"r-{page}-{i}", "uei": None, "name": "R", "recipient_level": "R", "amount": a}
                   for i, a in enumerate(amounts)]
        more = page < len(pages)
        total = sum(len(amounts) for amounts in pages)
        body = {"results": results, "page_metadata": {"total": total, "next": page + 1 if more else None, "hasNext": more}}
        raw = json.dumps(body).encode()
        return httpx.Response(200, stream=httpx.ByteStream(raw), headers={"content-type": "application/json"})

    return httpx.MockTransport(respond), served


def test_the_funded_walk_reads_past_the_top_pages_and_stops_at_the_unfunded_tail(monkeypatch):
    """Decision 48: every recipient with a positive trailing-12-month amount, not just the top pages."""
    import importlib

    usa = importlib.import_module("spicy_regs.transforms.build_usaspending_recipients")

    monkeypatch.setattr(usa, "DEFAULT_MAX_PAGES", 1)
    transport, served = _ranking([9.0, 8.0], [7.0, 6.0], [5.0, 0.0], [0.0, 0.0])
    rows = list(usa._iter_recipient_rows(per_page=2, transport=transport, every_funded=True))
    assert [row["amount"] for row in rows] == [9.0, 8.0, 7.0, 6.0, 5.0]
    assert served == [1, 2, 3]  # the tail page ends the walk; the page after it is never read

    transport, served = _ranking([9.0, 8.0], [7.0, 6.0])
    assert len(list(usa._iter_recipient_rows(per_page=2, transport=transport))) == 2 and served == [1]


def test_a_funded_ranking_still_positive_at_the_bound_refuses(monkeypatch):
    from spicy_docs.reading.paged_json import PagedJsonSourceError

    import importlib

    usa = importlib.import_module("spicy_regs.transforms.build_usaspending_recipients")

    monkeypatch.setattr(usa, "FUNDED_WALK_PAGE_BOUND", 2)
    transport, _ = _ranking([9.0], [8.0], [7.0])
    with pytest.raises(PagedJsonSourceError, match="had not ended after 2 pages"):
        list(usa._iter_recipient_rows(per_page=1, transport=transport, every_funded=True))
