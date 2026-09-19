"""Hermetic end-to-end test for the press-release transform's bill linkage.

No network: the acquirer is stubbed with the two captured feeds
(``tests/fixtures/press_releases/README.md``), and the published
``congress_bills`` table the matcher reads is seeded where the transform's
best-effort download looks for it. What this establishes is that the transform
drives ``release_matching`` — one compiled pattern per published bill in
scope, matched field by field — and fills the four linkage columns from the
result. Whether a pattern is right (that ``H.R. 650`` does not match bill 6500,
say) is spicy-docs' own test's job and is not re-asserted here.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.congress.press_releases import PRESS_RELEASE_FEEDS, parse_press_release_feed
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.transforms.build_press_releases import build_press_releases
from spicy_regs.transforms.table_merge import prior_scratch_path

FIXTURES = Path(__file__).parent / "fixtures" / "press_releases"
FEED_FILES = {"house": "house-rss.xml", "senate": "senate-rss-press.xml"}
OBSERVED_AT = "2026-09-19T00:00:00Z"

#: The bills the House feed's titles name, as captured, plus one no title names.
PUBLISHED_BILLS = [("119", "hr", "6500"), ("119", "hr", "9770"), ("119", "hr", "8595"), ("119", "s", "2503")]


class _Acquisition:
    def __init__(self, channel, capture):
        self.channel = channel
        self.capture = capture


class StubAcquirer:
    """Serves each chamber's captured feed, with one optional byte-level edit."""

    def __init__(self, edits: dict[str, tuple[str, str]] | None = None):
        self.edits = edits or {}

    def acquire_press_releases(self, feed):
        body = (FIXTURES / FEED_FILES[feed.chamber]).read_bytes()
        if feed.chamber in self.edits:
            old, new = self.edits[feed.chamber]
            assert old.encode() in body, f"the fixture no longer carries {old!r}"
            body = body.replace(old.encode(), new.encode(), 1)
        capture = CapturedBodyResponse(
            requested_url=feed.url,
            resolved_url=feed.url,
            status_code=200,
            content_type="application/rss+xml",
            observed_at=OBSERVED_AT,
            body=body,
        )
        return _Acquisition(parse_press_release_feed(body, feed), capture)


def _no_prior(remote_key: str, local_path: Path) -> bool:
    return False


def _seed_bills(output_dir: Path, bills=PUBLISHED_BILLS) -> None:
    """A published congress_bills table where the transform's download will find it."""
    contract = TABLE_CONTRACTS["congress_bills"]
    rows = [
        {c: None for c in contract.columns}
        | {"bill_id": f"{congress}-{kind}-{number}", "congress": congress, "bill_type": kind, "bill_number": number}
        for congress, kind, number in bills
    ]
    pq.write_table(
        pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in contract.columns])),
        prior_scratch_path(output_dir, "congress_bills"),
    )


def _rows(output_dir: Path, acquirer=None) -> list[dict]:
    path = build_press_releases(output_dir, acquirer=acquirer or StubAcquirer(), download_prior=_no_prior)
    return pq.read_table(path).to_pylist()


@pytest.fixture
def scoped(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")


def test_a_house_item_naming_a_bill_is_linked_on_its_title(tmp_path, scoped):
    _seed_bills(tmp_path)
    rows = _rows(tmp_path)
    assert len(rows) == 25, "ten House items and fifteen Senate items"

    by_bill = {}
    for row in rows:
        if row["bill_id"]:
            by_bill.setdefault(row["bill_id"], []).append(row)
    assert set(by_bill) == {"119-hr-6500", "119-hr-9770", "119-hr-8595"}
    assert len(by_bill["119-hr-9770"]) == 2, "two House titles name H.R. 9770"
    for linked in by_bill.values():
        for row in linked:
            assert row["chamber"] == "house"
            assert row["match_rule"] == "bill_number_in_title"
            assert row["matched_field"] == "title"
            assert row["matched_text"] in row["title"], "the matched text is quoted from the field it was found in"

    # The pass ran over every release, so the rest say so rather than staying NULL.
    unmatched = [row for row in rows if not row["bill_id"]]
    assert len(unmatched) == 21
    assert {row["match_rule"] for row in unmatched} == {"unmatched"}
    assert all(row["matched_field"] is None and row["matched_text"] is None for row in unmatched)


def test_a_senate_item_can_only_match_on_its_title(tmp_path, scoped):
    """The Senate feed carries no description, so a match there is a title match by construction.

    As captured, no Senate title names a bill; one is edited to, which is the
    smallest input that reaches the Senate branch of the match without a
    second synthetic feed.
    """
    _seed_bills(tmp_path)
    edit = {"senate": ("Senator Murray on Passage of CR", "Senator Murray on Passage of S. 2503")}
    rows = _rows(tmp_path, StubAcquirer(edit))

    senate = [row for row in rows if row["chamber"] == "senate"]
    assert len(senate) == 15
    assert all(row["description"] is None for row in senate), "the fixture's Senate items carry no description"
    linked = [row for row in senate if row["bill_id"]]
    assert [(row["bill_id"], row["match_rule"], row["matched_field"]) for row in linked] == [
        ("119-s-2503", "bill_number_in_title", "title")
    ]
    assert linked[0]["matched_text"] == "S. 2503"


def test_only_bills_in_the_congresses_in_scope_are_matched_against(tmp_path, monkeypatch):
    """The scope rule bounds the matcher's releases-times-bills cost, and is the same one the family uses."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118")
    _seed_bills(tmp_path)
    rows = _rows(tmp_path)
    assert all(row["bill_id"] is None for row in rows)
    assert {row["match_rule"] for row in rows} == {"unmatched"}, "the pass ran; there was simply nothing to match"


def test_no_published_bills_means_no_matching_pass_not_a_false_unmatched(tmp_path, scoped):
    """NULL and `unmatched` are different claims: the first says no pass ran, the second that one found nothing."""
    rows = _rows(tmp_path)
    assert len(rows) == 25
    assert all(row["bill_id"] is None and row["match_rule"] is None for row in rows)
    assert not prior_scratch_path(tmp_path, "congress_bills").exists()


def test_the_bills_scratch_file_is_not_left_beside_the_outputs(tmp_path, scoped):
    _seed_bills(tmp_path)
    _rows(tmp_path)
    assert not prior_scratch_path(tmp_path, "congress_bills").exists()
    assert (tmp_path / "press_releases.parquet").exists()


def test_the_prior_table_is_merged_with_the_match_columns_kept(tmp_path, scoped):
    """A release seen again takes the fresh row, match and all; one that rotated off keeps its old one."""
    _seed_bills(tmp_path)
    first = _rows(tmp_path)
    assert any(row["bill_id"] for row in first)

    # The published table becomes the next run's prior; one extra, rotated-off
    # release is added to it so the append-only merge is visible.
    published = pq.read_table(tmp_path / "press_releases.parquet").to_pylist()
    rotated = dict(published[0]) | {"release_id": "rotated-off", "link": "https://example.invalid/old", "bill_id": None}
    contract = TABLE_CONTRACTS["press_releases"]
    pq.write_table(
        pa.Table.from_pylist([*published, rotated], schema=pa.schema([(c, pa.string()) for c in contract.columns])),
        prior_scratch_path(tmp_path, "press_releases"),
    )
    _seed_bills(tmp_path)
    second = _rows(tmp_path)
    assert len(second) == len(first) + 1
    assert sum(1 for row in second if row["bill_id"]) == sum(1 for row in first if row["bill_id"])


def test_every_feed_is_still_asked_for(tmp_path, scoped):
    """The linkage is a merge-time join; it changes nothing about what is fetched."""
    asked: list[str] = []

    class Counting(StubAcquirer):
        def acquire_press_releases(self, feed):
            asked.append(feed.chamber)
            return super().acquire_press_releases(feed)

    _rows(tmp_path, Counting())
    assert asked == list(PRESS_RELEASE_FEEDS)


def test_the_seeded_prior_is_read_through_download_prior_when_absent_locally(tmp_path, scoped):
    """The transform asks R2 for the bills table the way it asks for its own prior: once, best effort."""
    source = tmp_path / "remote"
    source.mkdir()
    _seed_bills(source)
    shutil.move(prior_scratch_path(source, "congress_bills"), source / "congress_bills.parquet")
    asked: list[str] = []

    def download(remote_key: str, local_path: Path) -> bool:
        asked.append(remote_key)
        candidate = source / remote_key
        if not candidate.exists():
            return False
        shutil.copyfile(candidate, local_path)
        return True

    path = build_press_releases(tmp_path, acquirer=StubAcquirer(), download_prior=download)
    rows = pq.read_table(path).to_pylist()
    assert asked == ["congress_bills.parquet", "press_releases.parquet"]
    assert sum(1 for row in rows if row["bill_id"]) == 4
