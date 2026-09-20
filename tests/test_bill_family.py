"""Hermetic end-to-end test for the bill-family transform.

No network and no model: the two acquirers are stubbed with the fixture bytes
copied from spicy-docs (see ``tests/fixtures/govinfo_bills/README.md``), and
both model seams stay unwired because no key is set. What this establishes is
that the transform drives ``build_bill_family`` correctly and publishes all
thirteen tables plus its own archive state — not that any rule inside it is
right, which is spicy-docs' own test's job.

119 HR 6028 is used because it offers two consecutive printings, which is the
smallest input that reaches the section and diff tables as well as the
status-derived ones.

``StubBulkAcquirer`` reproduces ``BulkStatusAcquirer``'s ``unchanged_since``
contract rather than just recording the argument: reading the listing only when
given something to compare against, refusing an entry that names a different
file, and returning ``skipped_unchanged`` with no archive and no zip capture.
A stub that always served the zip could not tell a wired skip from an unwired
one, which is the whole point of the second-run test below.
"""

from __future__ import annotations

import json
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pyarrow.parquet as pq
import pytest
from loguru import logger
from spicy_docs.extraction import gemini
from spicy_docs.interpretation.bill_summaries import DIFF_SUMMARY_ANSWER_SCHEMA, SUMMARY_ANSWER_SCHEMA
from spicy_docs.interpretation.section_classification import CLASSIFICATION_ANSWER_SCHEMA, LABEL_NAMES
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.congress.bill_status import BillIdentity, parse_bill_status
from spicy_docs.sources.congress.bill_status import BillSourceError
from spicy_docs.sources.congress.bulk_status import BulkListingEntry
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_docs.transport.credentials import CredentialRefusedError

from spicy_regs.transforms.build_bill_family import (
    ARCHIVE_COLUMNS,
    ARCHIVES_TABLE,
    BACKFILL_COLUMNS,
    BACKFILL_WALK_COLUMNS,
    BACKFILL_WALKS_TABLE,
    BACKFILLS_TABLE,
    BACKFILL_UNSUBSTANTIATED,
    FAMILY_TABLES,
    VOTE_REFERENCE_COLUMNS,
    VOTE_REFERENCES_TABLE,
    build_bill_family,
    engine_stamp,
)
from tests.pdf_fixtures import make_multiline_pdf

FIXTURES = Path(__file__).parent / "fixtures" / "govinfo_bills"
IDENTITY = BillIdentity(congress=119, bill_type="hr", number=6028)
OBSERVED_AT = "2026-09-19T00:00:00Z"
BULKDATA = "https://www.govinfo.gov/bulkdata/BILLSTATUS"

#: The printings the fixture bill offers, by the package id each resolves to.
TEXT_FIXTURES = {
    "BILLS-119hr6028ih": "text-119hr6028ih.xml",
    "BILLS-119hr6028eh": "text-119hr6028eh.xml",
}


def _capture(url: str, body: bytes) -> CapturedBodyResponse:
    return CapturedBodyResponse(
        requested_url=url,
        resolved_url=url,
        status_code=200,
        content_type="text/xml",
        observed_at=OBSERVED_AT,
        body=body,
    )


class _Member:
    def __init__(self, status):
        self.status = status
        self.identity = status.identity
        self.refusal = None


class _Archive:
    def __init__(self, members):
        self.members = tuple(members)
        self.parsed_count = len(self.members)
        self.refused_count = 0


class _Acquisition:
    def __init__(self, archive, capture, *, skipped_unchanged=False, listing_capture=None, listing_entry=None):
        self.archive = archive
        self.capture = capture
        self.skipped_unchanged = skipped_unchanged
        self.listing_capture = listing_capture
        self.listing_entry = listing_entry


class _Listing:
    def __init__(self, zip_entry):
        self.zip_entry = zip_entry


class _ListingAcquisition:
    def __init__(self, listing, capture):
        self.listing = listing
        self.capture = capture


def zip_entry(congress: int, bill_type: str, *, size: int = 31_656_886) -> BulkListingEntry:
    """The folder's own zip entry as GovInfo's bulkdata listing states it."""
    name = f"BILLSTATUS-{congress}-{bill_type}.zip"
    return BulkListingEntry(
        name=name,
        display_label=name,
        just_file_name=name,
        link=f"{BULKDATA}/{congress}/{bill_type}/{name}",
        folder=False,
        formatted_last_modified_time="18-Sep-2026 20:26",
        modified_at=datetime(2026, 9, 18, 20, 26, tzinfo=UTC),
        mime_type="application/zip",
        file_extension="zip",
        formatted_size="30 MB",
        size=size,
    )


class StubBulkAcquirer:
    """Serves the one fixture bill for (119, hr) and an empty archive otherwise.

    Honors ``unchanged_since`` the way ``BulkStatusAcquirer`` does, so
    ``zip_downloads`` records only the folders whose zip bytes were really
    read and ``listings`` records the small listing requests.
    """

    def __init__(self, entry=zip_entry, status: bytes | None = None):
        self.calls: list[tuple[int, str]] = []
        self.zip_downloads: list[tuple[int, str]] = []
        self.listings: list[tuple[int, str]] = []
        self._entry = entry
        self._status = status

    def _listing_capture(self, congress: int, bill_type: str) -> CapturedBodyResponse:
        return _capture(f"{BULKDATA.replace('/bulkdata/', '/bulkdata/json/')}/{congress}/{bill_type}", b"{}")

    def list_archives(self, congress: int, bill_type: str):
        self.listings.append((congress, bill_type))
        return _ListingAcquisition(
            _Listing(self._entry(congress, bill_type)), self._listing_capture(congress, bill_type)
        )

    def acquire(self, congress: int, bill_type: str, *, unchanged_since: BulkListingEntry | None = None):
        self.calls.append((congress, bill_type))
        entry = self._entry(congress, bill_type)
        listing_capture = None
        if unchanged_since is not None:
            self.listings.append((congress, bill_type))
            listing_capture = self._listing_capture(congress, bill_type)
            if (unchanged_since.name, unchanged_since.link) != (entry.name, entry.link):
                raise BillSourceError("unchanged_since names a different file than this folder's own zip entry")
            if (unchanged_since.modified_at, unchanged_since.size) == (entry.modified_at, entry.size):
                return _Acquisition(
                    None, None, skipped_unchanged=True, listing_capture=listing_capture, listing_entry=entry
                )

        self.zip_downloads.append((congress, bill_type))
        body = self._status if self._status is not None else (FIXTURES / "status-119hr6028.xml").read_bytes()
        capture = _capture(entry.link, body)
        members = [_Member(parse_bill_status(body, identity=IDENTITY))] if (congress, bill_type) == (119, "hr") else []
        return _Acquisition(
            _Archive(members),
            capture,
            listing_capture=listing_capture,
            listing_entry=entry if unchanged_since is not None else None,
        )


class _BodyIdentity:
    def __init__(self, media_type: str, byte_size: int):
        self.media_type = media_type
        self.byte_size = byte_size


class _Package:
    """The three facts `body_text` reads off a fetched body, plus the capture."""

    def __init__(self, fmt: str, capture: CapturedBodyResponse, *, media_type: str = "text/xml"):
        self.format = fmt
        self.body_capture = capture
        self.body = _BodyIdentity(media_type, len(capture.body))


class StubBodyAcquirer:
    """Serves a printing's XML from the fixtures; refuses a package it has none for."""

    def __init__(self):
        self.requested: list[str] = []

    def acquire(self, package_id: str, *, max_bytes=None):
        self.requested.append(package_id)
        name = TEXT_FIXTURES.get(package_id)
        if name is None:
            raise LookupError(f"no fixture for {package_id}")
        url = f"https://www.govinfo.gov/content/pkg/{package_id}/xml/{package_id}.xml"
        return _Package("xml", _capture(url, (FIXTURES / name).read_bytes()))


def _no_prior(remote_key: str, local_path: Path) -> bool:
    return False


def _prior_from(published: Path):
    """A ``download_prior`` that serves an earlier run's published files."""

    def download(remote_key: str, local_path: Path) -> bool:
        source = published / remote_key
        if not source.exists():
            return False
        shutil.copyfile(source, local_path)
        return True

    return download


@pytest.fixture
def family(tmp_path, monkeypatch):
    """One bill-family run over the fixture bill, keyless and offline."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    # No model key: the three model-backed tables must come back empty.
    for name in ("GEMINI_API_KEY", "GOOGLE_API_KEY"):
        monkeypatch.delenv(name, raising=False)

    paths = build_bill_family(
        tmp_path,
        bulk_acquirer=StubBulkAcquirer(),
        body_acquirer=StubBodyAcquirer(),
        download_prior=_no_prior,
    )
    return {path.stem: path for path in paths}


OWN_TABLES = {
    ARCHIVES_TABLE: ARCHIVE_COLUMNS,
    VOTE_REFERENCES_TABLE: VOTE_REFERENCE_COLUMNS,
    BACKFILLS_TABLE: BACKFILL_COLUMNS,
    BACKFILL_WALKS_TABLE: BACKFILL_WALK_COLUMNS,
}


def test_every_family_table_is_published(family):
    expected = {contract for contract, _ in FAMILY_TABLES} | {"public_activity_events", *OWN_TABLES}
    assert set(family) == expected
    assert len(family) == 17


def test_each_published_table_matches_its_contract_schema_or_its_own(family):
    """The thirteen contracts are spicy-docs'; the last four are this transform's."""
    for name, columns in OWN_TABLES.items():
        assert pq.read_table(family[name]).schema.names == list(columns), name


def test_each_published_table_matches_its_contract_schema(family):
    for name, path in family.items():
        if name in OWN_TABLES:
            continue
        contract = TABLE_CONTRACTS[name]
        assert pq.read_table(path).schema.names == list(contract.columns), name


def test_a_bill_with_no_recorded_votes_publishes_no_references(family):
    """The fixture bill's two actions record no roll call, so the table is empty rather than absent."""
    assert pq.read_table(family[VOTE_REFERENCES_TABLE]).to_pylist() == []


def test_the_bill_and_its_printings_are_there(family):
    bills = pq.read_table(family["congress_bills"]).to_pylist()
    assert [row["bill_id"] for row in bills] == ["119-hr-6028"]
    # The frozen prefix is filled from the same status as the appended columns.
    assert bills[0]["congress"] == "119"
    assert bills[0]["bill_type"] == "hr"

    versions = pq.read_table(family["bill_versions"]).to_pylist()
    # version_slug yields the full slug; the GovInfo suffix is what the
    # package id uses, which is why TEXT_FIXTURES is keyed the other way.
    assert {row["version_code"] for row in versions} == {"introduced-in-house", "engrossed-in-house"}
    assert all(row["bill_id"] == "119-hr-6028" for row in versions)
    # Bodies were fetched, so the capture columns are real rather than NULL.
    assert all(row["sha256"] and row["byte_size"] for row in versions)


def test_sections_are_parsed_and_every_parent_exists(family):
    sections = pq.read_table(family["bill_sections"]).to_pylist()
    assert sections, "the XML printings must yield sections"
    parents = {
        (row["bill_id"], row["version_code"], row["source"])
        for row in pq.read_table(family["bill_versions"]).to_pylist()
    }
    for row in sections:
        assert (row["bill_id"], row["version_code"], row["source"]) in parents


def test_the_consecutive_pair_is_actually_compared(family):
    """The reason the fixture is a pair: the diff tables must be non-empty.

    ``financial_changes`` is deliberately not asserted here — this pair changes
    no dollar figure, so it yields none, and asserting it would be asserting
    the fixture rather than the transform.
    """
    diffs = pq.read_table(family["section_diffs"]).to_pylist()
    assert len(diffs) == 1, "one consecutive pair means one comparison"
    assert diffs[0]["bill_id"] == "119-hr-6028"
    assert diffs[0]["engine_revision"], "every diff row names the engine that produced it"
    assert pq.read_table(family["section_diff_items"]).to_pylist(), "the comparison must settle items"


def test_the_model_tables_are_empty_without_a_key(family):
    for name in ("section_classifications", "bill_summaries", "diff_summaries"):
        assert pq.read_table(family[name]).to_pylist() == [], name


# --- The model seams, stubbed at the client -------------------------------
#
# ``build_bill_family`` builds its own ``GeminiClient`` from the environment,
# so the seam a test reaches is the client: a stub in its place sits behind
# spicy-docs' ``gemini_call.model_call``, all three readers and every check
# they make, which is the whole path a live answer takes.
#
# **Which prompt arrived is decided by ``response_schema``** — the request's
# own copy of the answer declaration, which spicy-docs 0.22.0 derives from the
# same ``AnswerField`` tuple as the prompt and the reader — compared against
# the three constants the generators send. That is exact where matching a
# substring of the prompt was not, and it fails if the schema stops reaching
# the request at all.
#
# **The answer keys are literal spellings on purpose.** Deriving them from
# ``field.key for field in SUMMARY_FIELDS`` would make a stub that silently
# follows a key rename, and a test asking whether a real answer is read would
# then be asking whether this file agrees with itself. Spelled out, a
# divergence fails here loudly, which is the whole lesson of C1.

LABEL = LABEL_NAMES[0]
#: Long enough for the reader's 60-character floor and short of its 1200 cap.
GOOD_SUMMARY = (
    "This bill directs the named agency to carry out the program it describes, "
    "and says who is covered, for how long, and what has to be reported back."
)
GOOD_ANSWER = {"summary": GOOD_SUMMARY, "audience": "people the program serves", "topThreeProvisions": ["one"]}
#: What the first live call actually answered (spicy-docs receipt
#: ``c1-provenance.json``, 2026-09-19): the summary key right and the other
#: two invented. Not a hypothetical — it is the measured answer the declared
#: key set exists to refuse.
V1_ERA_ANSWER = {
    "summary": GOOD_SUMMARY,
    "most_affected_audience": "people the program serves",
    "notable_provisions": ["the first one", "the second one"],
}
GOOD_DIFF_ANSWER = {
    "headline": "the later printing rewrites the program section and adds a funding section",
    "keyChanges": ["the program section is rewritten", "a funding section is added"],
    "sectionsAdded": ["Funding"],
    "sectionsRemoved": [],
    "dollarChanges": ["$1,400,000 authorized"],
}


def _section_ids(prompt: str) -> list[str]:
    """The section ids the classification prompt itself lists, in its order.

    Read off the prompt rather than recomputed here: ``_read_row`` refuses a
    row naming a section outside the batch it was sent, so answering from the
    request is what a stub must do to reach the row shapers at all. Stripping
    the brackets is what the ``v3`` field asks for in so many words — under
    ``v2`` it said "copied exactly as given below", and the live model
    returned the brackets too and was refused, twice.
    """
    body = prompt.split("Sections:\n", 1)[1]
    return [chunk.split("]", 1)[0].removeprefix("[") for chunk in body.split("\n\n---\n\n")]


class StubGemini:
    """A ``GenerationClient`` answering in the shape the request it was handed states.

    One client serves every prompt, as the real one does. ``summary_answer``
    replaces the summary answer alone, which is how a refusal is fed to one
    table without disturbing the others.
    """

    def __init__(self, *, summary_answer: dict | None = None):
        self.summary_answer = summary_answer
        self.calls: list[tuple[str, object]] = []

    def generate(self, model: str, body: dict) -> dict:
        prompt = body["contents"][0]["parts"][0]["text"]
        config = body["generationConfig"]
        assert config["responseMimeType"] == "application/json"
        schema = config.get("responseJsonSchema")
        assert schema is not None, "since 0.22.0 every generator states its answer's shape on the request"
        self.calls.append((prompt, schema))
        return {"candidates": [{"content": {"parts": [{"text": json.dumps(self._answer(prompt, schema))}]}}]}

    def _answer(self, prompt: str, schema: object):
        if schema == CLASSIFICATION_ANSWER_SCHEMA:
            return [{"sectionId": ref, "label": LABEL, "confidence": 0.9} for ref in _section_ids(prompt)]
        if schema == DIFF_SUMMARY_ANSWER_SCHEMA:
            return dict(GOOD_DIFF_ANSWER)
        assert schema == SUMMARY_ANSWER_SCHEMA, "a fourth request schema is a prompt this stub does not answer"
        return dict(self.summary_answer) if self.summary_answer is not None else dict(GOOD_ANSWER)


def _changed_eh() -> bytes:
    """The engrossed fixture with its one section rewritten and a second added.

    The committed pair settles as entirely ``unchanged``, so ``summarize_diff``
    declines before asking anything and the diff reader is never reached — the
    gap this exists to close. Derived from the fixture bytes rather than
    committed as a third file, so what differs from the real printing is
    stated in one place and cannot drift from it.
    """
    fixture = (FIXTURES / TEXT_FIXTURES["BILLS-119hr6028eh"]).read_text()
    rewritten = fixture.replace(
        "<header>Short title</header>",
        "<header>Short title and purpose</header>",
    ).replace(
        "</legis-body>",
        "    <section id=\"HADDEDSECTION0000000000000000001\">\n"
        "      <enum>2.</enum>\n"
        "      <header>Funding</header>\n"
        "      <text display-inline=\"no-display-inline\">There is authorized to be appropriated "
        "$1,400,000 to carry out this Act.</text>\n"
        "    </section>\n"
        "  </legis-body>",
    )
    assert rewritten != fixture, "the changed printing must actually differ from the fixture"
    return rewritten.encode()


class StubChangedBodyAcquirer(StubBodyAcquirer):
    """``StubBodyAcquirer`` serving a changed engrossed printing, so the pair has a diff."""

    def acquire(self, package_id: str, *, max_bytes=None):
        package = super().acquire(package_id, max_bytes=max_bytes)
        if package_id != "BILLS-119hr6028eh":
            return package
        url = package.body_capture.requested_url
        return _Package("xml", _capture(url, _changed_eh()))


def _modelled_run(tmp_path, monkeypatch, *, summary_answer: dict | None = None, body_acquirer=None):
    """One run with the model seams wired to ``StubGemini``; returns tables, warnings and the client."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    # Not a credential: `GeminiClient` is replaced below and never sends it.
    # It is only what `resolve_gemini_key` reads to decide the seams are wired.
    monkeypatch.setenv("GEMINI_API_KEY", "stub")
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    client = StubGemini(summary_answer=summary_answer)
    monkeypatch.setattr(gemini, "GeminiClient", lambda *, api_key: client)

    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        paths = build_bill_family(
            tmp_path,
            bulk_acquirer=StubBulkAcquirer(),
            body_acquirer=body_acquirer or StubBodyAcquirer(),
            download_prior=_no_prior,
        )
    finally:
        logger.remove(sink)
    return {path.stem: path for path in paths}, messages, client


def test_a_produced_row_is_stamped_with_the_prompt_version_that_asked_for_it(tmp_path, monkeypatch):
    """A row must say which prompt produced it: summaries ``v2``, labels ``v3``.

    Asserted as literals, not against each module's ``PROMPT_VERSION``:
    reading back the constant the column is filled from would agree with
    itself whatever it said, and this column is how a stored row is told apart
    from one an earlier prompt produced (``needs_regeneration`` reads it).
    """
    tables, _messages, _client = _modelled_run(tmp_path, monkeypatch)

    summaries = pq.read_table(tables["bill_summaries"]).to_pylist()
    assert summaries, "a wired seam over two text printings must produce summary rows"
    assert {row["prompt_version"] for row in summaries} == {"v2"}
    assert all(row["bill_id"] == "119-hr-6028" for row in summaries)
    assert all(row["summary"] == GOOD_SUMMARY for row in summaries)

    classifications = pq.read_table(tables["section_classifications"]).to_pylist()
    assert classifications, "the classifier answered every section it was sent"
    assert {row["prompt_version"] for row in classifications} == {"v3"}
    assert {row["label"] for row in classifications} == {LABEL}


def test_a_v1_era_answer_is_refused_by_name_and_the_run_completes(tmp_path, monkeypatch):
    """The measured failure, end to end: one refusal record, and the rest of the run.

    ``most_affected_audience`` and ``notable_provisions`` are what the first
    live call answered. The reader refuses it naming **both** absent keys at
    once — ``v1`` refused the same answer but stopped at the first — and since
    spicy-docs 0.22.0 that refusal is a ``FamilyRefusal`` filed against the
    printing, not an exception out of the rollup. It must cost the summary
    rows only: the status-derived tables, the diff and the classifications
    this same client answered correctly are all still published.
    """
    tables, warnings, client = _modelled_run(tmp_path, monkeypatch, summary_answer=V1_ERA_ANSWER)

    assert pq.read_table(tables["bill_summaries"]).to_pylist() == [], "a refused answer must not be stored"
    # The wording is the family's own refusal reason, not this rollup's prose.
    refusals = [line for line in warnings if "bill_summaries" in line and "the model's answer was refused" in line]
    assert refusals, f"the refusal record must reach the run log: {warnings}"
    for line in refusals:
        assert "audience" in line and "topThreeProvisions" in line, line
        assert "most_affected_audience" not in line, "the answer's own spellings are not the reader's to offer"

    # The run completed: everything that does not depend on that answer is here.
    assert [row["bill_id"] for row in pq.read_table(tables["congress_bills"]).to_pylist()] == ["119-hr-6028"]
    assert pq.read_table(tables["bill_versions"]).to_pylist()
    assert pq.read_table(tables["section_diffs"]).to_pylist()
    classifications = pq.read_table(tables["section_classifications"]).to_pylist()
    assert classifications, "the classifier's own answers were fine and must still be stored"
    assert {row["prompt_version"] for row in classifications} == {"v3"}
    assert len(client.calls) > 1, "the run kept asking after the first refusal"


def test_a_changed_pair_runs_the_diff_reader_and_publishes_its_row(tmp_path, monkeypatch):
    """``summarize_diff`` and ``_read_diff_answer`` on a pair that actually changed.

    On the committed fixtures every settled correspondence is ``unchanged``,
    so the generator declines before asking and this reader is never reached —
    which is why `diff_summaries` had no coverage here at all. With one
    section rewritten and one added, the diff text is non-empty, the call is
    made, and the five-key answer is read into a row.
    """
    tables, _messages, client = _modelled_run(tmp_path, monkeypatch, body_acquirer=StubChangedBodyAcquirer())

    assert any(schema == DIFF_SUMMARY_ANSWER_SCHEMA for _prompt, schema in client.calls), (
        "a changed pair must reach the diff-summary generator"
    )
    rows = pq.read_table(tables["diff_summaries"]).to_pylist()
    assert len(rows) == 1, "one compared pair, one diff summary"
    row = rows[0]
    assert row["bill_id"] == "119-hr-6028"
    assert (row["from_version_code"], row["to_version_code"]) == ("introduced-in-house", "engrossed-in-house")
    assert row["headline"] == GOOD_DIFF_ANSWER["headline"]
    assert row["prompt_version"] == "v2"
    # The four list-valued keys survive the reader and the shaper as JSON.
    assert json.loads(row["sections_added_json"]) == GOOD_DIFF_ANSWER["sectionsAdded"]
    assert json.loads(row["sections_removed_json"]) == [], "an empty array is a stated answer, not a NULL"
    assert json.loads(row["dollar_changes_json"]) == GOOD_DIFF_ANSWER["dollarChanges"]


def test_the_first_run_reports_every_bill_as_added(family):
    """With no prior table, every bill is new — and that is what the events say."""
    events = pq.read_table(family["public_activity_events"]).to_pylist()
    assert events, "a first run over one bill must detect it"
    kinds = {row["event_type"] for row in events}
    assert "bill_added" in kinds
    assert all(row["bill_id"] == "119-hr-6028" for row in events)
    assert all(row["detected_at"] for row in events)


def test_only_the_scoped_archive_is_fetched(tmp_path, monkeypatch):
    """The scope env vars bound the walk; nothing outside them is requested."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr,s")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    bulk = StubBulkAcquirer()
    build_bill_family(tmp_path, bulk_acquirer=bulk, body_acquirer=StubBodyAcquirer(), download_prior=_no_prior)
    assert bulk.calls == [(119, "hr"), (119, "s")]


def test_the_version_fetch_cap_is_honored(tmp_path, monkeypatch):
    """Past the cap a printing still gets a row, with its body columns NULL."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    body = StubBodyAcquirer()
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path,
            bulk_acquirer=StubBulkAcquirer(),
            body_acquirer=body,
            max_version_fetches=1,
            download_prior=_no_prior,
        )
    }
    assert len(body.requested) == 1
    versions = pq.read_table(paths["bill_versions"]).to_pylist()
    assert len(versions) == 2, "both printings are still published"
    assert sum(1 for row in versions if row["sha256"]) == 1
    assert sum(1 for row in versions if row["sha256"] is None) == 1


def test_the_engine_stamp_carries_the_vendored_revision():
    """A wheel install states no commit, so the vendored one must be supplied."""
    stamp = engine_stamp()
    assert stamp.name == "deltatrack"
    assert stamp.version
    assert len(stamp.revision) == 40, "the pinned DeltaTrack commit belongs in every diff row"


# --------------------------------------------------------------------------- #
# Incremental behavior: a steady-state run must not re-fetch what it holds.
# --------------------------------------------------------------------------- #
def _seed_prior(output_dir: Path, table: str, rows: list[dict]) -> None:
    """Write a prior published table where the merge and the index will find it."""
    import pyarrow as pa
    from spicy_regs.transforms.table_merge import prior_scratch_path

    contract = TABLE_CONTRACTS[table]
    filled = [{c: None for c in contract.columns} | row for row in rows]
    pq.write_table(
        pa.Table.from_pylist(filled, schema=pa.schema([(c, pa.string()) for c in contract.columns])),
        prior_scratch_path(output_dir, table),
    )


def _status_text_date() -> str | None:
    from spicy_docs.sources.congress.bill_status import parse_bill_status

    body = (FIXTURES / "status-119hr6028.xml").read_bytes()
    return parse_bill_status(body, identity=IDENTITY).update_date_including_text


@pytest.fixture
def scoped(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)


def test_an_unchanged_bill_costs_no_requests(tmp_path, scoped):
    """The steady state: the publisher's text stamp is unchanged, so nothing is fetched."""
    _seed_prior(
        tmp_path,
        "congress_bills",
        [{"bill_id": "119-hr-6028", "update_date_including_text": _status_text_date()}],
    )
    body = StubBodyAcquirer()
    build_bill_family(tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=body, download_prior=_no_prior)
    assert body.requested == [], "an unchanged bill must not fetch a single printing"


def test_a_changed_bill_is_rebuilt(tmp_path, scoped):
    """A different text stamp means the publisher changed something; re-read it."""
    _seed_prior(
        tmp_path,
        "congress_bills",
        [{"bill_id": "119-hr-6028", "update_date_including_text": "1999-01-01T00:00:00Z"}],
    )
    body = StubBodyAcquirer()
    build_bill_family(tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=body, download_prior=_no_prior)
    assert len(body.requested) == 2


def test_printings_already_held_are_not_refetched(tmp_path, scoped):
    """Both printings published: a changed bill re-reads its status, not its bodies."""
    _seed_prior(
        tmp_path,
        "congress_bills",
        [{"bill_id": "119-hr-6028", "update_date_including_text": "1999-01-01T00:00:00Z"}],
    )
    _seed_prior(
        tmp_path,
        "bill_versions",
        [
            {"bill_id": "119-hr-6028", "version_code": code, "source": "govinfo", "sha256": "sha256:x"}
            for code in ("introduced-in-house", "engrossed-in-house")
        ],
    )
    body = StubBodyAcquirer()
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=body, download_prior=_no_prior
        )
    }
    assert body.requested == [], "a held printing needs no second fetch"
    # And the published rows are the prior ones, not degraded re-emissions.
    versions = pq.read_table(paths["bill_versions"]).to_pylist()
    assert len(versions) == 2
    assert all(row["sha256"] == "sha256:x" for row in versions), "a held row must not be overwritten with NULLs"


def test_a_new_printing_pulls_its_neighbour_so_the_diff_still_happens(tmp_path, scoped):
    """Only the predecessor is held; a diff needs both sides, so both are fetched."""
    _seed_prior(
        tmp_path,
        "congress_bills",
        [{"bill_id": "119-hr-6028", "update_date_including_text": "1999-01-01T00:00:00Z"}],
    )
    _seed_prior(
        tmp_path,
        "bill_versions",
        [{"bill_id": "119-hr-6028", "version_code": "introduced-in-house", "source": "govinfo"}],
    )
    body = StubBodyAcquirer()
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=body, download_prior=_no_prior
        )
    }
    assert set(body.requested) == {"BILLS-119hr6028ih", "BILLS-119hr6028eh"}
    assert pq.read_table(paths["section_diffs"]).to_pylist(), "the new pair must still be compared"


def test_needed_printings_picks_the_unheld_and_their_neighbours():
    from spicy_regs.transforms.build_bill_family import _needed_printings

    codes = ["a", "b", "c", "d"]
    assert _needed_printings(codes, held=set(codes)) == set()
    assert _needed_printings(codes, held={"a", "b", "c"}) == {2, 3}
    assert _needed_printings(codes, held=()) == {0, 1, 2, 3}


# --------------------------------------------------------------------------- #
# The bulk-listing skip: a folder whose zip has not moved is not downloaded.
# --------------------------------------------------------------------------- #
def _run(output_dir: Path, bulk: StubBulkAcquirer, download_prior) -> dict[str, Path]:
    output_dir.mkdir(exist_ok=True)
    paths = build_bill_family(
        output_dir, bulk_acquirer=bulk, body_acquirer=StubBodyAcquirer(), download_prior=download_prior
    )
    return {path.stem: path for path in paths}


def test_a_cold_folder_retains_the_listing_entry_it_did_not_need(tmp_path, scoped):
    """`acquire` reads no listing with nothing to compare, so the run asks for one itself.

    Without this, the retained table would stay empty and the skip could never
    fire on any later run — the failure would look exactly like a working
    pipeline that just never saves anything.
    """
    bulk = StubBulkAcquirer()
    paths = _run(tmp_path / "run", bulk, _no_prior)

    assert bulk.zip_downloads == [(119, "hr")], "a cold folder downloads its zip"
    assert bulk.listings == [(119, "hr")], "and asks for the listing once, to retain the entry"
    rows = pq.read_table(paths[ARCHIVES_TABLE]).to_pylist()
    assert len(rows) == 1
    expected = zip_entry(119, "hr")
    assert rows[0]["name"] == expected.name
    assert rows[0]["link"] == expected.link
    assert rows[0]["size"] == str(expected.size)
    assert rows[0]["modified_at"] == expected.modified_at.isoformat()
    assert rows[0]["congress"] == "119"
    assert rows[0]["bill_type"] == "hr"


def test_a_second_run_over_an_unchanged_listing_makes_no_zip_request(tmp_path, scoped):
    """The whole point of the retained entry: the zip is proved unchanged, not re-read."""
    first = tmp_path / "run1"
    _run(first, StubBulkAcquirer(), _no_prior)

    second = StubBulkAcquirer()
    paths = _run(tmp_path / "run2", second, _prior_from(first))

    assert second.calls == [(119, "hr")], "the folder is still visited"
    assert second.listings == [(119, "hr")], "through its listing, which is the cheap half"
    assert second.zip_downloads == [], "and the zip itself is never requested"
    # The entry is retained again, so a third run can skip on the same evidence.
    assert len(pq.read_table(paths[ARCHIVES_TABLE]).to_pylist()) == 1


def test_a_moved_zip_is_downloaded_again(tmp_path, scoped):
    """The comparison has to be able to say no, or the skip is just a cache that never expires."""
    first = tmp_path / "run1"
    _run(first, StubBulkAcquirer(), _no_prior)

    # The publisher rebuilt the zip: same name and link, different size.
    moved = StubBulkAcquirer(entry=lambda c, t: zip_entry(c, t, size=31_658_670))
    _run(tmp_path / "run2", moved, _prior_from(first))

    assert moved.zip_downloads == [(119, "hr")]
    assert moved.listings == [(119, "hr")], "the listing is read once, inside acquire"


def test_a_retained_entry_naming_another_file_does_not_wedge_the_rollup(tmp_path, scoped):
    """A stale row is recognised before a request, so the fallback costs one listing read.

    ``acquire`` would refuse this entry by name — but only after reading the
    folder listing, and its refusal carries no listing to reuse, so passing it
    anyway would buy the same cold download for two reads instead of one.
    """
    first = tmp_path / "run1"
    _run(first, StubBulkAcquirer(entry=lambda c, t: zip_entry(c, "sres")), _no_prior)

    renamed = StubBulkAcquirer()
    paths = _run(tmp_path / "run2", renamed, _prior_from(first))

    assert renamed.zip_downloads == [(119, "hr")], "the zip is fetched rather than the run failing"
    assert renamed.listings == [(119, "hr")], "and the folder listing is read exactly once"
    rows = pq.read_table(paths[ARCHIVES_TABLE]).to_pylist()
    assert {row["name"] for row in rows} == {"BILLSTATUS-119-hr.zip"}, "and the bad row is replaced"


# --------------------------------------------------------------------------- #
# Recorded votes: the fifteenth output, read off the bill's own actions.
# --------------------------------------------------------------------------- #
#: chamber, session, roll number, date, url, and the optional seventh field.
#: The Senate entry carries a `<fullActionName>`; the House one does not, which
#: is the pair the publisher's own measurement found (absent on 58 of 58 JSON
#: entries, documented by the BILLSTATUS guide, so both states are real).
RECORDED_VOTES = {
    # The Senate action (index 0 in the fixture) and the House floor action (index 1).
    "Received in the Senate.": (
        "Senate",
        "2",
        "00312",
        "2026-06-09T20:14:02Z",
        "https://www.senate.gov/legislative/LIS/roll_call_votes/vote1192/vote_119_2_00312.xml",
        "MOTION TO CONSIDER",
    ),
    "Motion to reconsider laid on the table Agreed to without objection.": (
        "House",
        "2",
        "306",
        "2026-06-08T19:48:09Z",
        "https://clerk.house.gov/evs/2026/roll306.xml",
        None,
    ),
}


def _voted_status() -> bytes:
    """The fixture bill with one recorded vote on each of its two actions.

    Derived from the fixture rather than committed beside it, the way the
    PDF-only status is: only a `<recordedVotes>` block is added under each
    action's `<text>`, in the guide's own element spelling, so the parser
    reads it exactly as it reads a real BILLSTATUS. One vote per chamber, so
    both URL grammars and both chamber spellings are exercised.
    """
    raw = (FIXTURES / "status-119hr6028.xml").read_text()
    for action_text, (chamber, session, roll, date, url, full_name) in RECORDED_VOTES.items():
        named = "" if full_name is None else f"<fullActionName>{full_name}</fullActionName>"
        block = (
            f"<text>{action_text}</text>\n        <recordedVotes><recordedVote>"
            f"<chamber>{chamber}</chamber><congress>119</congress><date>{date}</date>{named}"
            f"<rollNumber>{roll}</rollNumber><sessionNumber>{session}</sessionNumber><url>{url}</url>"
            f"</recordedVote></recordedVotes>"
        )
        assert f"<text>{action_text}</text>" in raw
        raw = raw.replace(f"<text>{action_text}</text>", block, 1)
    return raw.encode()


def test_recorded_votes_on_a_bills_actions_are_published_as_references(tmp_path, scoped):
    """Each `recordedVotes` entry becomes one row naming the bill, the roll call and the action it sat on."""
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path,
            bulk_acquirer=StubBulkAcquirer(status=_voted_status()),
            body_acquirer=StubBodyAcquirer(),
            download_prior=_no_prior,
        )
    }
    rows = pq.read_table(paths[VOTE_REFERENCES_TABLE]).to_pylist()
    # Keyed by the action each entry sat on, because the merge publishes in
    # identity order (chamber sorts before session), not publisher order.
    by_action = {row["action_index"]: row for row in rows}
    assert [
        (by_action[i]["chamber"], by_action[i]["congress"], by_action[i]["session"], by_action[i]["roll_number"])
        for i in ("0", "1")
    ] == [
        ("senate", "119", "2", "312"),
        ("house", "119", "2", "306"),
    ], "chamber lowercased, numbers read as integers (the Senate's leading zeros go)"
    for row in rows:
        assert row["bill_id"] == "119-hr-6028"
        assert row["url"] and row["date"] and row["observed_at"] == OBSERVED_AT

    # The optional seventh field, both ways: `_full_action_name` finds the entry
    # again on its action by the key the reference states, so a publisher that
    # resumes sending it is carried rather than silently dropped, and one that
    # does not send it yields NULL rather than a guess.
    assert by_action["0"]["full_action_name"] == "MOTION TO CONSIDER"
    assert by_action["1"]["full_action_name"] is None


def test_references_are_keyed_by_action_so_one_roll_call_on_two_actions_is_two_rows(tmp_path, scoped):
    """The publisher attaches a passage vote to every floor action it settled; both are kept."""
    house = RECORDED_VOTES["Motion to reconsider laid on the table Agreed to without objection."]
    same_on_both = {text: house for text in RECORDED_VOTES}
    raw = (FIXTURES / "status-119hr6028.xml").read_text()
    for action_text, (chamber, session, roll, date, url, _full_name) in same_on_both.items():
        block = (
            f"<text>{action_text}</text><recordedVotes><recordedVote><chamber>{chamber}</chamber>"
            f"<congress>119</congress><date>{date}</date><rollNumber>{roll}</rollNumber>"
            f"<sessionNumber>{session}</sessionNumber><url>{url}</url></recordedVote></recordedVotes>"
        )
        raw = raw.replace(f"<text>{action_text}</text>", block, 1)
    paths = {
        p.stem: p
        for p in build_bill_family(
            tmp_path,
            bulk_acquirer=StubBulkAcquirer(status=raw.encode()),
            body_acquirer=StubBodyAcquirer(),
            download_prior=_no_prior,
        )
    }
    rows = pq.read_table(paths[VOTE_REFERENCES_TABLE]).to_pylist()
    assert sorted(r["action_index"] for r in rows) == ["0", "1"]
    assert {r["roll_number"] for r in rows} == {"306"}


# --------------------------------------------------------------------------- #
# The PDF rendition: reachable since 0.21.1 put PDF last in the preference
# rather than outside it, and the one branch that fills the cleanup_* columns.
# --------------------------------------------------------------------------- #
def _pdf_only_status() -> bytes:
    """The fixture bill with every printing offered only as PDF.

    The real case this stands in for is the pre-113th corpus, which offers no
    XML (spicy-docs `docs/research/pdf-only-corpus-2026-09-19.md`). Derived
    from the fixture rather than committed beside it so the two cannot drift:
    only the format URLs move, and `format_name` reads the rendition from the
    URL folder exactly as it does for the XML original.
    """
    raw = (FIXTURES / "status-119hr6028.xml").read_text()
    return re.sub(r"<url>(\S+?)/xml/(\S+?)\.xml</url>", r"<url>\1/pdf/\2.pdf</url>", raw).encode()


#: Two pages, each three real physical lines with its own gutter number
#: (1-3, a consecutive run starting at 1) and a genuine hyphen-wrap on the
#: first line, corroborated by its gutter number — the shape a real GPO
#: gutter-numbered page has under PyMuPDF's line-grouped extraction (a
#: content line immediately followed by its own bare digit line; see
#: ``spicy_docs.extraction.gpo_normalize``'s module docstring). No real fixture
#: from ``spicy-docs``' own ``tests/fixtures/gpo_pdf_text/README.md``
#: provenance is both under 200 KB and gutter-numbered — its two documents
#: under 200 KB (the ENR bill at 196,785 bytes, the committee report at
#: 199,803 bytes) are never GPO line-numbered by GPO's own print convention,
#: and its two gutter-numbered documents (223,439 and 206,513 bytes) both
#: exceed 200 KB — so this synthetic PDF stands in, built to exercise
#: ``is_gpo_layout`` for real rather than merely asserting its output is not
#: None.
PDF_GPO_PAGES = [
    [
        "Introduc-",
        "1",
        "ing this measure to amend the Act.",
        "2",
        "Additional provision text follows here today.",
        "3",
    ],
    [
        "Further findings support the measure describ-",
        "1",
        "ed in section one, presented here today now.",
        "2",
        "Enacted this day pursuant to the authority granted.",
        "3",
    ],
]


class StubPdfBodyAcquirer:
    """Serves each printing as a real, minimal, genuinely GPO-gutter-numbered PDF."""

    def __init__(self):
        self.requested: list[str] = []

    def acquire(self, package_id: str, *, max_bytes=None):
        self.requested.append(package_id)
        url = f"https://www.govinfo.gov/content/pkg/{package_id}/pdf/{package_id}.pdf"
        body = make_multiline_pdf(PDF_GPO_PAGES)
        capture = CapturedBodyResponse(
            requested_url=url,
            resolved_url=url,
            status_code=200,
            content_type="application/pdf",
            observed_at=OBSERVED_AT,
            body=body,
        )
        return _Package("pdf", capture, media_type="application/pdf")


@pytest.fixture
def pdf_family(tmp_path, scoped):
    """One run over a bill whose printings are offered only as PDF, with the warnings it logged."""
    messages: list[str] = []
    sink = logger.add(messages.append, level="WARNING", format="{message}")
    try:
        paths = build_bill_family(
            tmp_path,
            bulk_acquirer=StubBulkAcquirer(status=_pdf_only_status()),
            body_acquirer=StubPdfBodyAcquirer(),
            download_prior=_no_prior,
        )
    finally:
        logger.remove(sink)
    return {path.stem: path for path in paths}, messages


def test_a_pdf_printing_is_fetched_and_published_as_one(pdf_family):
    """PDF is last in the preference, not outside it: the body columns are real."""
    paths, _ = pdf_family
    versions = pq.read_table(paths["bill_versions"]).to_pylist()
    assert len(versions) == 2
    assert {row["format_name"] for row in versions} == {"pdf"}
    for row in versions:
        assert row["source"] == "govinfo"
        assert row["sha256"] and row["byte_size"] and row["observed_at"] and row["resolved_url"]


def test_the_pdf_cleanup_record_reaches_the_cleanup_columns(pdf_family):
    """`body_text`'s PDF branch is the GPO normalizer, and its record is what these columns are.

    They were NULL on every row before the 0.21.1 adoption: the transform
    never ran `body_text` at all. They went silently null again, in a
    different way, when this repository's PDF branch ran through
    `PypdfPageExtractor` instead of `body_text`'s default PyMuPDF extractor:
    pypdf glues a GPO gutter number onto the end of its content line
    ("Representa-1") rather than emitting it as PyMuPDF does — its own
    physical line immediately after — so `is_gpo_layout`'s adjacency
    detector never fires on a pypdf-read page, `cleanup_line_numbers` stays
    `False` on a genuinely numbered document, and `hyphen_rejoin_count` stays
    `0` since rejoin is gated on the layout verdict (measured
    `docs/research/gpo-normalizer-vs-upstream-2026-09-19.md` in spicy-docs:
    0 of 6, then 0 of 12, real gutter numbers rejoined under pypdf, vs 6 of 6
    and 12 of 12 under PyMuPDF). `PDF_GPO_PAGES` is genuinely gutter-numbered
    (see its own docstring), so asserting the layout verdict and the rejoin
    count here — not just that the columns are non-null — is what would catch
    a regression back to an extractor that defeats the normalizer.
    """
    paths, _ = pdf_family
    versions = pq.read_table(paths["bill_versions"]).to_pylist()
    for row in versions:
        assert row["cleanup_line_numbers"] == "true", "PDF_GPO_PAGES is gutter-numbered; the layout must be detected"
        assert row["cleanup_gpo_footers"] is not None
        assert row["cleanup_spacing_normalized"] is not None
        assert row["cleanup_hyphen_rejoins"] == "2", "one gutter-corroborated hyphen wrap per page, two pages"
        pages = json.loads(row["cleanup_json"])
        assert [page["page"] for page in pages] == [1, 2], "one entry per PDF page, one-based"
        # The two fields 0.21.1 added to GpoPageCleanup, serialized upstream.
        assert all("running_footer_lines" in page and "content_lines" in page for page in pages)


def test_a_pdf_only_pair_is_refused_by_name_not_silently_skipped(pdf_family):
    """No XML means no section tree, so the pair cannot be diffed — and says so."""
    paths, messages = pdf_family
    assert pq.read_table(paths["bill_sections"]).to_pylist() == [], "a PDF printing has no section tree"
    assert pq.read_table(paths["section_diffs"]).to_pylist() == [], "so the consecutive pair yields no diff"
    refusals = [line for line in messages if "refusals by table" in line]
    assert refusals, "the refusal must be reported, not left as an empty table"
    assert "section_diffs" in refusals[0]


# --------------------------------------------------------------------------- #
# The pre-BILLSTATUS backfill (gap A11): the 82nd-107th Congresses from the
# API ``bill`` route, hermetically — a stubbed walk, no key, no network.
# --------------------------------------------------------------------------- #

#: A detail record shaped like the retained 92nd-Congress response the A11
#: receipt kept (``detail-bill-92-hr-2185.json``): laws and the latest action
#: inline, every other BILLSTATUS-rich field a sub-route count with no items,
#: and no ``sponsors`` or ``cosponsors`` at all.
_DETAIL_92_HR_2185 = {
    "actions": {"count": 7, "url": "https://api.congress.gov/v3/bill/92/hr/2185/actions?format=json"},
    "committees": {"count": 2, "url": "https://api.congress.gov/v3/bill/92/hr/2185/committees?format=json"},
    "congress": 92,
    "introducedDate": "1971-01-25",
    "latestAction": {"actionDate": "1972-09-29", "text": "Became Public Law No. 92-441"},
    "laws": [{"number": "92-441", "type": "Public Law"}],
    "legislationUrl": "https://www.congress.gov/bill/92th-congress/house-bill/2185",
    "number": "2185",
    "originChamber": "House",
    "originChamberCode": "H",
    "textVersions": {"count": 1, "url": "https://api.congress.gov/v3/bill/92/hr/2185/text?format=json"},
    "title": "An Act to declare that certain federally owned land is held by the United States in trust",
    "titles": {"count": 3, "url": "https://api.congress.gov/v3/bill/92/hr/2185/titles?format=json"},
    "type": "HR",
    "updateDate": "2026-09-08T18:09:59Z",
    "updateDateIncludingText": "2026-09-08T18:09:59Z",
}

#: The richer shape the receipt's ``detail-bill-107-hr-3162.json`` has: one
#: sponsor in ``sponsors`` and a separate ``cosponsors`` sub-route declaring 1.
_DETAIL_107_HR_3162 = {
    **_DETAIL_92_HR_2185,
    "congress": 107,
    "number": "3162",
    "title": "USA PATRIOT Act",
    "sponsors": [{"bioguideId": "S000244", "fullName": "Rep. Sensenbrenner, F. James, Jr."}],
    "cosponsors": {"count": 1, "url": "https://api.congress.gov/v3/bill/107/hr/3162/cosponsors?format=json"},
    "policyArea": {"name": "Crime and Law Enforcement"},
}


class _Page:
    """One list page: its records, the route's declared total, a capture."""

    def __init__(self, records, declared_count):
        self.records = tuple(records)
        self.declared_count = declared_count
        self.capture = _capture("https://api.congress.gov/v3/bill/92/hr?format=json", b"{}")


class StubListSource:
    """Pages and details from fixtures, recording everything the walk asked for.

    A unit with no pages given answers one empty page declaring 0 — what the
    route does for a bill type a Congress has none of. ``detail_error`` is a
    callable consulted before every detail: the exception it returns is
    raised for that bill, so a test can refuse one bill and let the rest
    through.
    """

    def __init__(self, pages_by_unit, details_by_key, *, detail_error=None):
        self._pages = pages_by_unit
        self._details = details_by_key
        self._detail_error = detail_error
        self.paged: list[tuple[int, str]] = []
        self.requested: list[BillIdentity] = []

    def __enter__(self) -> "StubListSource":
        return self

    def __exit__(self, *_error: object) -> None:
        return None

    def pages(self, congress: int, bill_type: str):
        self.paged.append((congress, bill_type))
        return iter(self._pages.get((congress, bill_type), [_Page([], 0)]))

    def detail(self, identity: BillIdentity):
        self.requested.append(identity)
        if self._detail_error is not None:
            error = self._detail_error(identity)
            if error is not None:
                raise error
        return self._details[(identity.congress, identity.bill_type, identity.number)], OBSERVED_AT


def _stub_source(records: list[dict], *, detail_error=None, congress: int = 92) -> StubListSource:
    """One page per bill type declaring that type's records, one detail per record."""
    by_type: dict[str, list[dict]] = {}
    details = {}
    for record in records:
        bill_type = str(record["type"]).lower()
        by_type.setdefault(bill_type, []).append(record)
        details[(congress, bill_type, int(str(record["number"])))] = record
    return StubListSource(
        {(congress, bill_type): [_Page(rows, len(rows))] for bill_type, rows in by_type.items()},
        details,
        detail_error=detail_error,
    )


def _two_bills() -> list[dict]:
    second = {**_DETAIL_92_HR_2185, "number": "2190", "title": "A second act"}
    return [dict(_DETAIL_92_HR_2185), second]


def _numbers(identities) -> list[int]:
    return [identity.number for identity in identities]


def _walk_row(tmp_path: Path, bill_type: str = "hr") -> dict:
    rows = pq.read_table(tmp_path / f"{BACKFILL_WALKS_TABLE}.parquet").to_pylist()
    return next(row for row in rows if row["bill_type"] == bill_type)


def _unsettle(tmp_path: Path) -> None:
    """Rewrite the published walks table so its one unit reads as never having reached its terminal page."""
    import pyarrow as pa

    path = tmp_path / f"{BACKFILL_WALKS_TABLE}.parquet"
    rows = [{**row, "list_completed": "false"} for row in pq.read_table(path).to_pylist()]
    pq.write_table(pa.Table.from_pylist(rows, schema=pa.schema([(c, pa.string()) for c in BACKFILL_WALK_COLUMNS])), path)


def _state(tmp_path: Path) -> list[dict]:
    return sorted(pq.read_table(tmp_path / f"{BACKFILLS_TABLE}.parquet").to_pylist(), key=lambda r: int(r["number"]))


@pytest.fixture
def scoped_92(monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "92")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")


def test_a_backfilled_bill_publishes_what_the_detail_record_states(tmp_path, scoped_92):
    paths = build_bill_family(tmp_path, list_source=_stub_source([dict(_DETAIL_92_HR_2185)]), download_prior=_no_prior)
    assert {path.stem for path in paths} >= {BACKFILLS_TABLE, BACKFILL_WALKS_TABLE}
    row = pq.read_table(tmp_path / "congress_bills.parquet").to_pylist()[0]
    assert row["bill_id"] == "92-hr-2185"
    assert row["title"] == _DETAIL_92_HR_2185["title"]
    assert row["latest_action_text"] == "Became Public Law No. 92-441"
    assert row["public_law_number"] == "92-441"
    assert row["law_type"] == "Public Law"
    assert row["update_date_including_text"] == "2026-09-08T18:09:59Z"
    assert row["url"] == _DETAIL_92_HR_2185["legislationUrl"]
    # No actions are in the record, so nothing about the bill's stage or its
    # signing action was examined: neither is stated, not even as a default.
    # The sample row in the A11 receipt became public law; "introduced" would
    # have been a false statement, and so would a signing rule that claims to
    # have looked for a became-law action.
    assert row["stage"] is None and row["stage_rule"] is None
    assert row["signed_date"] is None and row["signed_date_rule"] is None
    # A count the record states only as a sub-route count is NULL, not zero —
    # the route declared seven actions, and a published zero would deny that.
    for column in BACKFILL_UNSUBSTANTIATED:
        assert row[column] is None, column
    assert row["cosponsor_count"] is None  # the record states no cosponsors sub-route
    assert _state(tmp_path) == [
        {
            "congress": "92",
            "bill_type": "hr",
            "number": "2185",
            "list_update_date_including_text": "2026-09-08T18:09:59Z",
            "refusal": None,
            "observed_at": OBSERVED_AT,
        }
    ]
    assert _walk_row(tmp_path) == {
        "congress": "92",
        "bill_type": "hr",
        "declared_count": "1",
        "records_walked": "1",
        "pages_walked": "1",
        "list_completed": "true",
        "unwalkable_count": "0",
        "repeated_count": "0",
        "backfilled_count": "1",
        "observed_at": OBSERVED_AT,
    }


def test_cosponsor_count_is_the_sub_routes_own_count_never_the_sponsor_arithmetic(tmp_path, monkeypatch):
    """The receipt's 107/hr/3162: one sponsor, ``cosponsors.count`` 1 — the shaper's ``len(sponsors) - 1`` says 0."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "107")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    build_bill_family(
        tmp_path, list_source=_stub_source([dict(_DETAIL_107_HR_3162)], congress=107), download_prior=_no_prior
    )
    row = pq.read_table(tmp_path / "congress_bills.parquet").to_pylist()[0]
    assert row["sponsor_bioguide_id"] == "S000244"
    assert row["cosponsor_count"] == "1"
    assert row["policy_area"] == "Crime and Law Enforcement"


def test_the_cap_charges_pages_and_details_and_the_next_run_fills_the_rest(tmp_path, scoped_92):
    first = _stub_source(_two_bills())
    # One page and one detail fit; the second detail does not.
    build_bill_family(tmp_path, list_source=first, max_version_fetches=2, download_prior=_no_prior)
    assert _numbers(first.requested) == [2185]
    walk = _walk_row(tmp_path)
    assert walk["records_walked"] == "2" and walk["backfilled_count"] == "1" and walk["list_completed"] == "false"

    # Not settled, so the unit is walked again (its page charged again) and
    # only the bill without a matching state row is requested.
    second = _stub_source(_two_bills())
    build_bill_family(tmp_path, list_source=second, download_prior=_prior_from(tmp_path))
    assert second.paged == [(92, "hr")]
    assert _numbers(second.requested) == [2190]
    bills = pq.read_table(tmp_path / "congress_bills.parquet").to_pylist()
    assert sorted(row["bill_id"] for row in bills) == ["92-hr-2185", "92-hr-2190"]
    walk = _walk_row(tmp_path)
    assert walk["backfilled_count"] == "2" and walk["list_completed"] == "true"


def test_a_capped_run_records_declared_versus_walked(tmp_path, scoped_92):
    """A cap that stops the walk after its first page must not let it read as an empty unit."""
    build_bill_family(tmp_path, list_source=_stub_source(_two_bills()), max_version_fetches=1, download_prior=_no_prior)
    assert _state(tmp_path) == []
    walk = _walk_row(tmp_path)
    assert walk["declared_count"] == "2"
    assert walk["records_walked"] == "1"  # the cap stopped the walk on the first record needing a detail
    assert walk["list_completed"] == "false"
    assert walk["backfilled_count"] == "0"


def test_a_credential_refusal_aborts_the_run(tmp_path, scoped_92):
    with pytest.raises(CredentialRefusedError):
        build_bill_family(
            tmp_path,
            list_source=_stub_source(
                [dict(_DETAIL_92_HR_2185)], detail_error=lambda _identity: CredentialRefusedError("401")
            ),
            download_prior=_no_prior,
        )


def _respond_92_hr(records: list[dict], *, fail: set[int]):
    """A Congress.gov stand-in: the hr list page for the 92nd, and each bill's detail unless it is in ``fail``."""

    def respond(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-Api-Key"] == "test-key"
        assert "api_key" not in request.url.params
        path = request.url.path
        if path == "/v3/bill/92/hr":
            body = {"bills": records, "pagination": {"count": len(records)}}
        else:
            number = int(path.rsplit("/", 1)[1])
            if number in fail:
                raise httpx.ConnectError("connection reset")
            body = {"bill": next(record for record in records if int(record["number"]) == number)}
        return httpx.Response(
            200, stream=httpx.ByteStream(json.dumps(body).encode()), headers={"content-type": "application/json"}
        )

    return respond


@pytest.fixture
def one_attempt(monkeypatch):
    """spicy-docs' reader retries a transport failure up to the budget's ``max_requests``, with jitter
    that climbs to a minute; one attempt and no pacing is what a hermetic test can afford."""
    from spicy_regs.sources import congress_bills

    monkeypatch.setattr(congress_bills, "_MAX_REQUESTS_PER_PAGE", 1)
    monkeypatch.setattr(congress_bills, "_MIN_REQUEST_INTERVAL_SECONDS", 0.0)


def test_a_transport_failure_is_not_a_record(tmp_path, scoped_92, one_attempt):
    """Through the real seam: a connection that fails after the reader's retries fills nothing.

    spicy-docs raises its own ``ConnectionError`` subclass then — not an
    ``httpx`` error — and it must be that one bill's gap, recorded as refused
    and retried next run, never the whole run's failure.
    """
    from spicy_regs.transforms.build_bill_family import CongressListBackfill

    transport = httpx.MockTransport(_respond_92_hr(_two_bills(), fail={2185}))
    build_bill_family(tmp_path, list_source=CongressListBackfill("test-key", transport), download_prior=_no_prior)
    bills = pq.read_table(tmp_path / "congress_bills.parquet").to_pylist()
    assert [row["bill_id"] for row in bills] == ["92-hr-2190"]
    state = _state(tmp_path)
    assert [(row["number"], row["refusal"]) for row in state] == [("2185", "_RetryableTransportError"), ("2190", None)]
    assert _walk_row(tmp_path)["list_completed"] == "true"


def test_a_refused_bill_is_retried_first_and_a_settled_unit_is_not_walked_again(tmp_path, scoped_92, one_attempt):
    from spicy_regs.transforms.build_bill_family import CongressListBackfill

    transport = httpx.MockTransport(_respond_92_hr(_two_bills(), fail={2185}))
    build_bill_family(tmp_path, list_source=CongressListBackfill("test-key", transport), download_prior=_no_prior)

    # Every record is accounted for (one filled, one refused), so the next run
    # makes no list request at all: the refusal is retried directly.
    second = _stub_source(_two_bills())
    build_bill_family(tmp_path, list_source=second, download_prior=_prior_from(tmp_path))
    assert second.paged == []
    assert _numbers(second.requested) == [2185]
    assert [(row["number"], row["refusal"]) for row in _state(tmp_path)] == [("2185", None), ("2190", None)]
    bills = pq.read_table(tmp_path / "congress_bills.parquet").to_pylist()
    assert sorted(row["bill_id"] for row in bills) == ["92-hr-2185", "92-hr-2190"]


def test_a_permanent_gap_costs_one_request_a_run_not_a_walk(tmp_path, scoped_92):
    always = lambda identity: ConnectionError("still down") if identity.number == 2185 else None  # noqa: E731
    build_bill_family(tmp_path, list_source=_stub_source(_two_bills(), detail_error=always), download_prior=_no_prior)
    third = _stub_source(_two_bills(), detail_error=always)
    build_bill_family(tmp_path, list_source=third, download_prior=_prior_from(tmp_path))
    assert third.paged == [] and _numbers(third.requested) == [2185]
    assert [(row["number"], row["refusal"]) for row in _state(tmp_path)] == [("2185", "ConnectionError"), ("2190", None)]


def test_a_moved_stamp_is_refetched_once_and_counted_once(tmp_path, scoped_92):
    build_bill_family(tmp_path, list_source=_stub_source([dict(_DETAIL_92_HR_2185)]), download_prior=_no_prior)
    moved = {**_DETAIL_92_HR_2185, "updateDateIncludingText": "2026-09-10T00:00:00Z", "title": "Retitled"}
    # The unit was settled, but a stamp only moves when the publisher edits
    # the bill; naming the Congress again with a widened scope walks it.
    second = _stub_source([moved])
    build_bill_family(tmp_path, list_source=second, download_prior=_prior_from(tmp_path))
    assert _numbers(second.requested) == []  # settled: the moved stamp is not seen without a walk
    # Force the walk by leaving the unit unsettled: a prior walk row that never completed.
    _unsettle(tmp_path)
    third = _stub_source([moved])
    build_bill_family(tmp_path, list_source=third, download_prior=_prior_from(tmp_path))
    assert _numbers(third.requested) == [2185]
    assert [row["list_update_date_including_text"] for row in _state(tmp_path)] == ["2026-09-10T00:00:00Z"]
    assert _walk_row(tmp_path)["backfilled_count"] == "1"
    bills = pq.read_table(tmp_path / "congress_bills.parquet").to_pylist()
    assert [(row["bill_id"], row["title"]) for row in bills] == [("92-hr-2185", "Retitled")]


def test_an_unwalkable_record_is_counted_and_settles_the_unit(tmp_path, scoped_92):
    stray = {**_DETAIL_92_HR_2185, "type": "S", "number": "9"}  # a Senate bill on the hr walk
    source = StubListSource({(92, "hr"): [_Page([dict(_DETAIL_92_HR_2185), stray], 2)]}, {(92, "hr", 2185): _DETAIL_92_HR_2185})
    build_bill_family(tmp_path, list_source=source, download_prior=_no_prior)
    walk = _walk_row(tmp_path)
    assert walk["unwalkable_count"] == "1" and walk["backfilled_count"] == "1" and walk["declared_count"] == "2"
    second = _stub_source([dict(_DETAIL_92_HR_2185)])
    build_bill_family(tmp_path, list_source=second, download_prior=_prior_from(tmp_path))
    assert second.paged == [] and second.requested == []


def test_a_repeated_list_entry_is_requested_once_and_counted_so_the_unit_settles(tmp_path, scoped_92):
    """The route repeats a bill across a page boundary when its stamp ties (the 92nd's hr 4634, live)."""
    record = dict(_DETAIL_92_HR_2185)
    source = StubListSource({(92, "hr"): [_Page([record], 2), _Page([record], 2)]}, {(92, "hr", 2185): record})
    build_bill_family(tmp_path, list_source=source, download_prior=_no_prior)
    assert _numbers(source.requested) == [2185]
    walk = _walk_row(tmp_path)
    assert (walk["declared_count"], walk["records_walked"], walk["repeated_count"], walk["backfilled_count"]) == (
        "2",
        "2",
        "1",
        "1",
    )
    assert len(_state(tmp_path)) == 1
    second = _stub_source([record])
    build_bill_family(tmp_path, list_source=second, download_prior=_prior_from(tmp_path))
    assert second.paged == [] and second.requested == []


def test_a_refusal_retried_at_the_start_is_not_asked_for_again_by_the_walk(tmp_path, scoped_92):
    """One attempt per bill per run: an unsettled unit's walk reaches a bill the retry pass already tried."""
    always = lambda identity: ConnectionError("still down") if identity.number == 2185 else None  # noqa: E731
    # A cap of 2 (one page, one detail) leaves the unit unsettled with 2185 refused.
    build_bill_family(
        tmp_path, list_source=_stub_source(_two_bills(), detail_error=always), max_version_fetches=2, download_prior=_no_prior
    )
    second = _stub_source(_two_bills(), detail_error=always)
    build_bill_family(tmp_path, list_source=second, download_prior=_prior_from(tmp_path))
    assert second.paged == [(92, "hr")]
    assert _numbers(second.requested) == [2185, 2190]  # the retry first, then the walk fills the rest, 2185 once


def test_the_walk_is_narrowed_by_the_scoped_bill_types(tmp_path, monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "92")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "s,hjres")
    source = _stub_source([{**_DETAIL_92_HR_2185, "type": "S", "number": "493"}])
    build_bill_family(tmp_path, list_source=source, download_prior=_no_prior)
    assert source.paged == [(92, "s"), (92, "hjres")]
    rows = pq.read_table(tmp_path / f"{BACKFILL_WALKS_TABLE}.parquet").to_pylist()
    assert {(row["bill_type"], row["declared_count"]) for row in rows} == {("s", "1"), ("hjres", "0")}


def test_a_pre_floor_congress_never_reaches_the_bulk_acquirer(tmp_path, monkeypatch):
    """One scope input, two routes: the split is on the publisher's floor."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "92,119")
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr")
    bulk = StubBulkAcquirer()
    build_bill_family(
        tmp_path,
        bulk_acquirer=bulk,
        list_source=_stub_source([dict(_DETAIL_92_HR_2185)]),
        download_prior=_no_prior,
    )
    # The bulk half is asked for the 119th only — never for a Congress below the floor.
    assert bulk.calls == [(119, "hr")]
    assert bulk.zip_downloads == [(119, "hr")]
    bills = pq.read_table(tmp_path / "congress_bills.parquet").to_pylist()
    assert {row["bill_id"] for row in bills} == {"92-hr-2185", "119-hr-6028"}  # both routes, one table


def test_a_congress_below_the_route_floor_is_refused(tmp_path, monkeypatch):
    """The route reaches the 82nd and nothing older; an empty walk would read as absence."""
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "81")
    with pytest.raises(ValueError, match="82nd"):
        build_bill_family(tmp_path, list_source=StubListSource({}, {}), download_prior=_no_prior)


def test_a_printing_with_no_govinfo_suffix_is_a_row_not_a_dead_run():
    """`Private Law` resolves as a slug and has no GovInfo package; that is one printing's problem.

    Measured on the first cold-start walk of the 119th
    (`~/Work/corpora/supply-2026-09-02/receipts/d1-measured-run-2026-09-19/`,
    attempt 1): `bill_version_package_id` raised `VersionCodeError` for
    `private-law` from outside any refusal boundary, and all seventeen outputs
    were lost to it after 532 seconds and 1,802 requests. The wheel answers the
    same refusal in `version_code_is_reprint_ambiguous` and `_sorted_versions`
    for exactly this reason; this holds the third place to the same rule.

    Without the fix this raises instead of returning, so the assertion below is
    reached only when the refusal is caught.
    """
    from spicy_docs.sources.congress.bill_versions import VersionCodeError, govinfo_suffix, version_slug

    from spicy_regs.transforms.build_bill_family import _version_captures

    # The premise the test rests on, re-derived rather than assumed: the slug
    # resolves and its GovInfo suffix does not.
    assert version_slug("Private Law") == "private-law"
    with pytest.raises(VersionCodeError):
        govinfo_suffix("private-law")

    class _Printing:
        type = "Private Law"
        formats = ()
        package_id = None

    class _Status:
        identity = IDENTITY
        text_versions = (_Printing(),)

    captures = _version_captures(_Status(), acquirer=None, budget=[10])

    assert [c.version_code for c in captures] == ["private-law"]
    assert captures[0].package_id is None, "unaddressable, and the row says so rather than guessing"
    assert captures[0].body is None and captures[0].document is None


def test_an_unaddressable_printing_is_never_fetched_and_spends_no_budget():
    """The `package_id is not None` guard, with a printing that gets past the other two.

    The sibling test above passes no acquirer and no formats, so the guard on
    `package_id` is never evaluated there and deleting it would survive. This
    one gives the printing a real offered format, so `chosen` is not None and an
    acquirer *is* present — the only remaining thing standing between the run
    and `acquire(None)` is the guard under test.

    The format's URL is a Congress.gov link rather than a canonical GovInfo one,
    so `bill_package_id_from_url` declines it (it recognizes only the canonical
    form) and the derivation below it raises for `private-law`. That is the real
    shape: the publisher offers somewhere to read the printing, and nothing in
    it addresses a GovInfo package.
    """
    from spicy_docs.sources.congress.bill_status import BillTextFormat, BillTextVersion

    from spicy_regs.transforms.build_bill_family import _version_captures

    class _RecordingAcquirer:
        def __init__(self) -> None:
            self.requested: list[str | None] = []

        def acquire(self, package_id, *, max_bytes=None):
            self.requested.append(package_id)
            raise AssertionError(f"acquire must not be called for an unaddressable printing: {package_id!r}")

    printing = BillTextVersion(
        type="Private Law",
        date="2026-09-01",
        formats=(
            BillTextFormat(url="https://www.congress.gov/119/bills/hr6028/BILLS-119hr6028.htm", type="HTML", package_id=None),
        ),
        package_id=None,
    )

    class _Status:
        identity = IDENTITY
        text_versions = (printing,)

    acquirer = _RecordingAcquirer()
    budget = [10]
    captures = _version_captures(_Status(), acquirer=acquirer, budget=budget)

    # The premise: the other two conditions are both satisfied, so only the
    # package_id guard can be what stops the call.
    from spicy_docs.sources.congress.bill_versions import DEFAULT_FORMAT_PREFERENCE, choose_format

    assert choose_format(printing.formats, prefer=DEFAULT_FORMAT_PREFERENCE) is not None
    assert acquirer is not None

    assert acquirer.requested == [], "an unaddressable printing must not be handed to the acquirer"
    assert budget == [10], "and must not spend the run's per-run fetch budget"
    assert [c.version_code for c in captures] == ["private-law"]
    assert captures[0].package_id is None
    assert captures[0].source == "congress", "not fetched, so the row does not claim a GovInfo source"
