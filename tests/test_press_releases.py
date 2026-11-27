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
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.congress.press_releases import PRESS_RELEASE_FEEDS, parse_press_release_feed
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.transforms.build_press_releases import build_press_releases, decode_title_entities
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
    """Serves each chamber's captured feed, with optional byte-level edits."""

    def __init__(self, edits: dict[str, list[tuple[str, str]]] | None = None):
        self.edits = edits or {}

    def acquire_press_releases(self, feed):
        body = (FIXTURES / FEED_FILES[feed.chamber]).read_bytes()
        for old, new in self.edits.get(feed.chamber, ()):
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


def test_a_house_item_naming_a_bill_is_linked_on_its_title(tmp_path):
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


def test_a_senate_item_can_only_match_on_its_title(tmp_path):
    """The Senate feed carries no description, so a match there is a title match by construction.

    As captured, no Senate title names a bill; one is edited to, which is the
    smallest input that reaches the Senate branch of the match without a
    second synthetic feed.
    """
    _seed_bills(tmp_path)
    edit = {"senate": [("Senator Murray on Passage of CR", "Senator Murray on Passage of S. 2503")]}
    rows = _rows(tmp_path, StubAcquirer(edit))

    senate = [row for row in rows if row["chamber"] == "senate"]
    assert len(senate) == 15
    assert all(row["description"] is None for row in senate), "the fixture's Senate items carry no description"
    linked = [row for row in senate if row["bill_id"]]
    assert [(row["bill_id"], row["match_rule"], row["matched_field"]) for row in linked] == [
        ("119-s-2503", "bill_number_in_title", "title")
    ]
    assert linked[0]["matched_text"] == "S. 2503"


def test_a_release_is_scoped_by_its_own_publication_date(tmp_path):
    """The January case: a December release still matches the outgoing Congress's bills.

    A feed is a rotating window roughly two months deep, so through January it
    carries both sides of the flip. Here one House item is moved to December
    2026 (the 119th) and another to January 2027 (the 120th); both are retitled
    to name H.R. 6500, and only the 119th has published bills. A run-wide scope
    could serve at most one of them — and on the wrong side of the flip it
    would serve neither, while still stamping `unmatched` over what an earlier
    run had correctly published.
    """
    _seed_bills(tmp_path)
    edits = {
        "house": [
            (
                "<title>Cole Remarks at Hearing on Funding Lapses: Analyzing Shutdown Reform</title>",
                "<title>Cole Remarks on H.R. 6500 in December</title>",
            ),
            (
                "<pubDate>Wed, 22 Jul 2026 14:20:44 +0000</pubDate>",
                "<pubDate>Tue, 15 Dec 2026 14:20:44 +0000</pubDate>",
            ),
            (
                "<title>Joyce Remarks at Oversight Hearing on the Economy Act</title>",
                "<title>Joyce Remarks on H.R. 6500 in January</title>",
            ),
            (
                "<pubDate>Tue, 15 Sep 2026 14:00:15 +0000</pubDate>",
                "<pubDate>Fri, 15 Jan 2027 14:00:15 +0000</pubDate>",
            ),
        ]
    }
    rows = {row["title"]: row for row in _rows(tmp_path, StubAcquirer(edits))}

    december = rows["Cole Remarks on H.R. 6500 in December"]
    assert december["bill_id"] == "119-hr-6500", "the 119th's bills are what a December release names"
    assert december["match_rule"] == "bill_number_in_title"

    january = rows["Joyce Remarks on H.R. 6500 in January"]
    assert january["bill_id"] is None
    assert january["match_rule"] is None, (
        "the 120th has no published bills, so no pass ran for it — this must not read as `unmatched`"
    )


def test_a_congress_with_no_published_bills_never_asserts_unmatched(tmp_path):
    """The blocker: an empty-but-present bill set must not be published as a measurement.

    `compile_bill_patterns([])` is `()`, which is truthy-distinct from None, so
    an empty scope used to run the matcher against zero patterns and stamp
    every release `unmatched` — a false claim, and one that overwrote the
    correct `bill_id` an earlier run had published, because this table merges
    row-wise and a fresh row wins whole.
    """
    # A real table, with bills — but none in the Congress the releases fall in.
    _seed_bills(tmp_path, [("118", "hr", "6500")])
    rows = _rows(tmp_path)
    assert len(rows) == 25
    assert all(row["bill_id"] is None for row in rows)
    assert all(row["match_rule"] is None for row in rows), "no bills for this Congress means no pass ran"


def test_a_release_with_no_readable_pub_date_is_not_matched_against_a_guess(tmp_path):
    """Nothing scopes it, so nothing matches it; the columns say so rather than naming a Congress for it."""
    _seed_bills(tmp_path)
    edits = {"house": [("<pubDate>Tue, 01 Sep 2026 16:27:43 +0000</pubDate>", "")]}
    rows = _rows(tmp_path, StubAcquirer(edits))
    undated = [row for row in rows if row["pub_date"] is None]
    assert len(undated) == 1
    assert "H.R. 6500" in undated[0]["title"], "the text names a bill; only the date is missing"
    assert undated[0]["bill_id"] is None and undated[0]["match_rule"] is None


def test_no_published_bills_means_no_matching_pass_not_a_false_unmatched(tmp_path):
    """NULL and `unmatched` are different claims: the first says no pass ran, the second that one found nothing."""
    rows = _rows(tmp_path)
    assert len(rows) == 25
    assert all(row["bill_id"] is None and row["match_rule"] is None for row in rows)
    assert not prior_scratch_path(tmp_path, "congress_bills").exists()


def test_the_bills_scratch_file_is_not_left_beside_the_outputs(tmp_path):
    _seed_bills(tmp_path)
    _rows(tmp_path)
    assert not prior_scratch_path(tmp_path, "congress_bills").exists()
    assert (tmp_path / "press_releases.parquet").exists()


def test_the_prior_table_is_merged_with_the_match_columns_kept(tmp_path):
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


def test_every_feed_is_still_asked_for(tmp_path):
    """The linkage is a merge-time join; it changes nothing about what is fetched."""
    asked: list[str] = []

    class Counting(StubAcquirer):
        def acquire_press_releases(self, feed):
            asked.append(feed.chamber)
            return super().acquire_press_releases(feed)

    _rows(tmp_path, Counting())
    assert asked == list(PRESS_RELEASE_FEEDS)


def test_the_seeded_prior_is_read_through_download_prior_when_absent_locally(tmp_path):
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


def test_rollup_retains_source_evidence_and_passes_it_to_builder(tmp_path, monkeypatch):
    from spicy_regs.pipelines.rollups.press_releases import PressReleasesRollup

    assert PressReleasesRollup.retain_source_evidence is True
    seen = {}

    def stub_build(output_dir: Path, **kwargs):
        seen.update(kwargs)
        return build_press_releases(output_dir, acquirer=StubAcquirer(), **kwargs)

    monkeypatch.setattr("spicy_regs.pipelines.rollups.press_releases.build_press_releases", stub_build)
    pipeline = PressReleasesRollup(output_dir=tmp_path, skip_upload=True)
    pipeline.run()
    assert seen["evidence"] is pipeline.source_evidence and pipeline.source_evidence is not None
    assert list((tmp_path / "generations").iterdir())


def test_decode_title_entities_handles_double_encoded_entities_without_corrupting_valid_text():
    assert decode_title_entities(None) is None
    # Senate RSS feed double-encoded numeric hex entities + literal ampersands.
    assert (
        decode_title_entities(
            "Murray & Kaptur Call for Review of DOE&#x2019;s &#x201c;Rule&#x201d; &#x2014; &#x2018;Unlawful&#x2019;"
        )
        == "Murray & Kaptur Call for Review of DOE\u2019s \u201cRule\u201d \u2014 \u2018Unlawful\u2019"
    )
    # Decimal numeric and named entities.
    assert (
        decode_title_entities("Bill &amp; Melinda Gates&#8217;s &ldquo;Grant&rdquo; &#039;Plan&#039;")
        == "Bill & Melinda Gates\u2019s \u201cGrant\u201d 'Plan'"
    )
    # Bare ampersands, semicolon-less HTML5 legacy tokens, and unknown entities stay untouched.
    untouched = "Watchdog Concludes Trump & Vought, AT&T, A&reg B&copy C&para D&not=1 &notanentity;"
    assert decode_title_entities(untouched) == untouched
    # NUL, surrogate, and out-of-range numeric references stay untouched rather than turning into U+FFFD.
    invalid = "Keep &#0; and &#xD800; and &#x110000; verbatim"
    assert decode_title_entities(invalid) == invalid
    # Single decode pass only: does not recursively strip intentional entity text.
    assert decode_title_entities("&amp;#x2019;") == "&#x2019;"


def test_senate_fixture_double_encoded_titles_and_rotated_prior_rows_are_decoded_with_evidence(tmp_path):
    """Fresh Senate RSS double-encoded titles and rotated-off prior rows both decode cleanly and journal originals."""
    import json
    import re

    from spicy_regs.source_evidence import CaptureEvidence

    _seed_bills(tmp_path)
    # Seed one rotated-off prior row whose title still has raw entities (as on live R2).
    contract = TABLE_CONTRACTS["press_releases"]
    prior_raw_title = "Sen. Collins Calls Administration&#x2019;s Pocket Rescissions Package &#x2018;Unlawful&#x2019;"
    prior_row = {c: None for c in contract.columns} | {
        "release_id": "rotated-senate-entity-row",
        "chamber": "senate",
        "title": prior_raw_title,
        "link": "http://www.appropriations.senate.gov/news/majority/sen-collins-calls-administrations-pocket-rescissions-package-unlawful",
        "pub_date": "Fri, 25 Sep 2026 13:00:00 EST",
        "pub_date_instant": "2026-09-25T13:00:00-05:00",
        "observed_at": OBSERVED_AT,
        "match_rule": "unmatched",
    }
    pq.write_table(
        pa.Table.from_pylist([prior_row], schema=pa.schema([(c, pa.string()) for c in contract.columns])),
        prior_scratch_path(tmp_path, "press_releases"),
    )

    evidence = CaptureEvidence(tmp_path, "press-releases")
    path = build_press_releases(tmp_path, acquirer=StubAcquirer(), download_prior=_no_prior, evidence=evidence)
    rows = pq.read_table(path).to_pylist()

    entity_re = re.compile(r"&(?:#[0-9]+|#[xX][0-9a-fA-F]+|[A-Za-z][A-Za-z0-9]+);")
    assert len(rows) == 26
    assert [row["title"] for row in rows if entity_re.search(row["title"] or "")] == []

    by_id = {row["release_id"]: row for row in rows}
    assert (
        by_id["rotated-senate-entity-row"]["title"]
        == "Sen. Collins Calls Administration\u2019s Pocket Rescissions Package \u2018Unlawful\u2019"
    )
    # Confirm literal ampersands survived intact alongside decoded entities in the Senate fixture.
    senate_titles = {row["title"] for row in rows if row["chamber"] == "senate"}
    assert (
        "Murray, Baldwin Sound Alarm on Trump Admin\u2019s Plans to Siphon Away & Likely Impound Healthcare Research Funds"
        in senate_titles
    )
    assert (
        "\u201cIt\u2019s not 1950\u201d: Senator Murray on Trump\u2019s Outrageous and Backwards Move to Siphon Child Care Funding Away from Single Parents and Working Families"
        in senate_titles
    )
    assert (
        "NEW: DHS Deploys Freshly Acquired Jets to Shuttle Trump Officials\u2014including Kash Patel & Tom Homan\u2014Around in Luxury"
        in senate_titles
    )

    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    (decoded_event,) = (e for e in events if e["event"] == "press-release-title-decoded")
    # 5 double-encoded titles in senate-rss-press.xml + 1 rotated-off prior row.
    assert decoded_event["count"] == 6
    origins = {c["origin"] for c in decoded_event["changes"]}
    assert origins == {"fresh", "prior"}
    prior_change = next(c for c in decoded_event["changes"] if c["release_id"] == "rotated-senate-entity-row")
    assert prior_change["original_title"] == prior_raw_title
    assert prior_change["decoded_title"] == by_id["rotated-senate-entity-row"]["title"]


def test_bill_matching_succeeds_on_double_encoded_quoted_bill_number_in_title(tmp_path):
    """Decoding entities before matching lets quoted bill references match cleanly and keeps matched_text in title."""
    _seed_bills(tmp_path)
    edit = {"senate": [("Senator Murray on Passage of CR", "Senator Murray on &amp;#x201c;S. 2503&amp;#x201d; Passage")]}
    rows = _rows(tmp_path, StubAcquirer(edit))
    linked = [row for row in rows if row["chamber"] == "senate" and row["bill_id"]]
    assert len(linked) == 1
    assert linked[0]["bill_id"] == "119-s-2503"
    assert linked[0]["title"] == "Senator Murray on \u201cS. 2503\u201d Passage"
    assert linked[0]["matched_text"] in linked[0]["title"]


def test_press_releases_rollup_admits_generation_and_carries_receipts_when_prior_title_decodes(tmp_path, monkeypatch):
    """End-to-end SubjectReceiptRollup proof: decoding a rotated-off prior title passes receipt carry and admission."""
    import re

    from spicy_regs.congress_receipts import CongressBuild, CongressInput, write_congress_dataset
    from spicy_regs.source_evidence import CaptureEvidence

    # 1. Build a prior native subject + receipt pair whose rotated-off row carries raw &#x2019; / &#x2018; entities.
    contract = TABLE_CONTRACTS["press_releases"]
    prior_raw = {c: None for c in contract.columns} | {
        "release_id": "rotated-senate-entity-row",
        "chamber": "senate",
        "title": "Sen. Collins Calls Administration&#x2019;s Pocket Rescissions Package &#x2018;Unlawful&#x2019;",
        "link": "http://www.appropriations.senate.gov/news/majority/sen-collins-calls-administrations-pocket-rescissions-package-unlawful",
        "pub_date": "Fri, 25 Sep 2026 13:00:00 EST",
        "pub_date_instant": "2026-09-25T13:00:00-05:00",
        "observed_at": OBSERVED_AT,
        "match_rule": "unmatched",
    }
    prior_source = tmp_path / "prior-source.parquet"
    pq.write_table(
        pa.Table.from_pylist([prior_raw], schema=pa.schema([(c, pa.string()) for c in contract.columns])),
        prior_source,
    )
    prior_subject, prior_receipts = write_congress_dataset(
        prior_source, tmp_path / "prior-native", dataset="press_releases", generation_id="prior-gen-1"
    )
    assert prior_subject is not None

    # 2. Run a full CongressBuild with that native prior + StubAcquirer (which also has 5 double-encoded Senate titles).
    build = CongressBuild(
        "next-gen-2",
        inputs={"press_releases": CongressInput(prior_subject, prior_receipts, "prior-gen-1")},
    )
    evidence = CaptureEvidence(tmp_path / "run", "press-releases")
    subject = build_press_releases(
        tmp_path / "run",
        acquirer=StubAcquirer(),
        receipt_build=build,
        evidence=evidence,
    )
    evidence.finish()
    build.admit(tmp_path / "admitted", family="press-releases", subjects=[subject])

    rows = pq.read_table(tmp_path / "admitted" / "press_releases.parquet").to_pylist()
    entity_re = re.compile(r"&(?:#[0-9]+|#[xX][0-9a-fA-F]+|[A-Za-z][A-Za-z0-9]+);")
    assert len(rows) == 26
    assert [r["title"] for r in rows if entity_re.search(r["title"] or "")] == []
    rotated = next(r for r in rows if r["release_id"] == "rotated-senate-entity-row")
    assert rotated["title"] == "Sen. Collins Calls Administration\u2019s Pocket Rescissions Package \u2018Unlawful\u2019"
