"""The whole-route walk reads past exactly one refusal: the publisher over-declaring at its terminal page."""

from __future__ import annotations

import pytest
from spicy_docs.reading.paged_json import DeclaredCountChanged, DeclaredCountMismatch, PagedJsonSourceError

from spicy_regs.transforms.congress_walk import MAX_OVER_DECLARATION, walk_route


class _Page:
    """One page: its records and the route's declared count."""

    def __init__(self, records, declared):
        self.records = tuple(records)
        self.declared_count = declared


def _mismatch(*, declared: int, observed: int) -> DeclaredCountMismatch:
    """The reader's terminal refusal, carrying both numbers."""
    return DeclaredCountMismatch(
        "Congress.gov declared and observed record counts differ", declared=declared, observed=observed
    )


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
    reader = Reader([_Page(records, 6)], _mismatch(declared=6, observed=4))
    walk = walk_route(reader, object(), "u", max_pages=5, label="t")
    assert walk.records == tuple(records)
    assert (walk.declared, walk.over_declared) == (6, True)


def test_a_walk_whose_counts_agree_is_not_over_declared():
    walk = walk_route(Reader([_Page([{"a": 1}], 1)]), object(), "u", max_pages=5, label="t")
    assert (len(walk.records), walk.declared, walk.over_declared) == (1, 1, False)


@pytest.mark.parametrize(
    "refusal",
    [
        PagedJsonSourceError("Congress.gov repeated its continuation"),
        DeclaredCountChanged("Congress.gov declared count changed during the traversal", declared=6, changed_to=4),
        # The right kind, but the observed count is not what this walk served: not this walk's terminal page.
        _mismatch(declared=6, observed=3),
        # The right kind and the served count, but a page's worth short: a short walk, not the over-declaration.
        _mismatch(declared=238, observed=4),
        _mismatch(declared=4 + MAX_OVER_DECLARATION + 1, observed=4),
        # More served than declared is not an over-declaration either.
        _mismatch(declared=3, observed=4),
        # The mismatch's words without its type: the message is no longer read.
        PagedJsonSourceError("Congress.gov declared and observed record counts differ"),
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
        Reader([_Page(served, at_bound)], _mismatch(declared=at_bound, observed=4)),
        object(),
        "u",
        max_pages=5,
        label="t",
    )
    assert (walk.declared, walk.over_declared, len(walk.records)) == (at_bound, True, 4)
    assert 238 - 236 <= MAX_OVER_DECLARATION, "the 2026-09-19 committee/119 finding must stay inside the bound"
