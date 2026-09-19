"""Hermetic end-to-end test for the bill-family transform.

No network and no model: the two acquirers are stubbed with the fixture bytes
copied from spicy-docs (see ``tests/fixtures/govinfo_bills/README.md``), and
both model seams stay unwired because no key is set. What this establishes is
that the transform drives ``build_bill_family`` correctly and publishes all
thirteen tables — not that any rule inside it is right, which is spicy-docs'
own test's job.

119 HR 6028 is used because it offers two consecutive printings, which is the
smallest input that reaches the section and diff tables as well as the
status-derived ones.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.congress.bill_status import BillIdentity, parse_bill_status
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.transforms.build_bill_family import FAMILY_TABLES, build_bill_family, engine_stamp

FIXTURES = Path(__file__).parent / "fixtures" / "govinfo_bills"
IDENTITY = BillIdentity(congress=119, bill_type="hr", number=6028)
OBSERVED_AT = "2026-09-19T00:00:00Z"

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
    def __init__(self, archive, capture):
        self.archive = archive
        self.capture = capture


class StubBulkAcquirer:
    """Serves the one fixture bill for (119, hr) and an empty archive otherwise."""

    def __init__(self):
        self.calls: list[tuple[int, str]] = []

    def acquire(self, congress: int, bill_type: str):
        self.calls.append((congress, bill_type))
        body = (FIXTURES / "status-119hr6028.xml").read_bytes()
        capture = _capture("https://www.govinfo.gov/bulkdata/BILLSTATUS/119/hr/BILLSTATUS-119-hr.zip", body)
        if (congress, bill_type) != (119, "hr"):
            return _Acquisition(_Archive([]), capture)
        status = parse_bill_status(body, identity=IDENTITY)
        return _Acquisition(_Archive([_Member(status)]), capture)


class _Package:
    def __init__(self, fmt: str, capture: CapturedBodyResponse):
        self.format = fmt
        self.body_capture = capture


class StubBodyAcquirer:
    """Serves a printing's XML from the fixtures; refuses a package it has none for."""

    def __init__(self):
        self.requested: list[str] = []

    def acquire(self, package_id: str, *, prefer=(), max_bytes=None):
        self.requested.append(package_id)
        name = TEXT_FIXTURES.get(package_id)
        if name is None:
            raise LookupError(f"no fixture for {package_id}")
        url = f"https://www.govinfo.gov/content/pkg/{package_id}/xml/{package_id}.xml"
        return _Package("xml", _capture(url, (FIXTURES / name).read_bytes()))


def _no_prior(remote_key: str, local_path: Path) -> bool:
    return False


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


def test_every_family_table_is_published(family):
    expected = {contract for contract, _ in FAMILY_TABLES} | {"public_activity_events"}
    assert set(family) == expected
    assert len(family) == 13


def test_each_published_table_matches_its_contract_schema(family):
    for name, path in family.items():
        contract = TABLE_CONTRACTS[name]
        assert pq.read_table(path).schema.names == list(contract.columns), name


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


def test_the_model_tables_are_empty_without_a_key(family):
    for name in ("section_classifications", "bill_summaries", "diff_summaries"):
        assert pq.read_table(family[name]).to_pylist() == [], name


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
