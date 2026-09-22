"""The whole-route walk reads past exactly one refusal: the publisher over-declaring at its terminal page."""

from __future__ import annotations

import pytest
from spicy_docs.reading.paged_json import PagedJsonSourceError

from spicy_regs.transforms.congress_walk import COUNT_MISMATCH, MAX_OVER_DECLARATION, TRAVERSAL_CONTEXT, walk_route


class _Page:
    """One page: its records and the route's declared count."""

    def __init__(self, records, declared):
        self.records = tuple(records)
        self.declared_count = declared


def _refusal(message: str, *, declared: int, observed: int) -> PagedJsonSourceError:
    """Build a paged-JSON error carrying the walk's ``declaredCount``/``observedCount`` context."""
    error = PagedJsonSourceError(f"Congress.gov {message}")
    error.__dict__[TRAVERSAL_CONTEXT] = {"operation": "traversal", "declaredCount": declared, "observedCount": observed}
    return error


class Reader:
    """Yields the given pages, then raises ``refusal`` when one is set."""

    def __init__(self, pages, refusal=None):
        self.pages = pages
        self.refusal = refusal

    def records(self, route, url, *, max_pages=100):
        yield from self.pages
        if self.refusal is not None:
            raise self.refusal


def test_an_over_declared_terminal_page_is_published_with_both_numbers():
    records = [{"systemCode": f"c{i}"} for i in range(4)]
    reader = Reader([_Page(records, 6)], _refusal(COUNT_MISMATCH, declared=6, observed=4))
    walk = walk_route(reader, object(), "u", max_pages=5, label="t")
    assert walk.records == tuple(records)
    assert (walk.declared, walk.over_declared) == (6, True)


def test_a_walk_whose_counts_agree_is_not_over_declared():
    walk = walk_route(Reader([_Page([{"a": 1}], 1)]), object(), "u", max_pages=5, label="t")
    assert (len(walk.records), walk.declared, walk.over_declared) == (1, 1, False)


@pytest.mark.parametrize(
    "refusal",
    [
        _refusal("repeated its continuation", declared=6, observed=4),
        _refusal("declared count changed during the traversal", declared=6, observed=4),
        # The right words, but the observed count is not what this walk served: not this walk's terminal page.
        _refusal(COUNT_MISMATCH, declared=6, observed=3),
        # The right words and the served count, but a page's worth short: a short walk, not the over-declaration.
        _refusal(COUNT_MISMATCH, declared=238, observed=4),
        _refusal(COUNT_MISMATCH, declared=4 + MAX_OVER_DECLARATION + 1, observed=4),
        PagedJsonSourceError(f"Congress.gov {COUNT_MISMATCH}"),  # no traversal context at all
    ],
)
def test_every_other_refusal_propagates(refusal):
    reader = Reader([_Page([{"a": 1}] * 4, 6)], refusal)
    with pytest.raises(PagedJsonSourceError):
        walk_route(reader, object(), "u", max_pages=5, label="t")


def test_the_shortfall_bound_is_inclusive_and_the_measured_delta_is_inside_it():
    served = [{"a": 1}] * 4
    at_bound = 4 + MAX_OVER_DECLARATION
    walk = walk_route(
        Reader([_Page(served, at_bound)], _refusal(COUNT_MISMATCH, declared=at_bound, observed=4)),
        object(),
        "u",
        max_pages=5,
        label="t",
    )
    assert (walk.declared, walk.over_declared, len(walk.records)) == (at_bound, True, 4)
    assert 238 - 236 <= MAX_OVER_DECLARATION, "the 2026-09-19 committee/119 finding must stay inside the bound"
