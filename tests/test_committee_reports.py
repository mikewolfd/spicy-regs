"""Hermetic test for the committee-report transform's body reading.

No network: the discovery reader and the body acquirer are stubbed, and the
``htm`` body is the shape GovInfo actually serves for a CRPT package — GPO's
plain text inside ``<html><title>..</title><body><pre>``, measured
2026-09-19 in spicy-docs ``docs/sources/govinfo-bodies.md``.

What this establishes is the two things the 0.21.1 adoption changed here: the
text comes from ``extraction.body_text`` rather than a local decoder, and a
rendition that states no page boundary publishes a NULL ``page_count`` instead
of a count invented from a separator that never occurs. The window arithmetic
is covered in ``test_incremental_rollups.py``.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from spicy_docs.sources.govinfo.bodies import (
    PackageBodyIdentity,
    PackageModsIdentity,
    PackageSummary,
    parse_package_id,
)
from spicy_docs.sources.govinfo.body_acquisition import GovInfoBodyBudget, GovInfoPackageBody
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.transforms.build_committee_reports import build_committee_reports
from tests.pdf_fixtures import make_pdf

OBSERVED_AT = "2026-09-19T00:00:00Z"

#: One agency header the report parser matches, so `report_sections` is reached.
REPORT_HTML = b"""<html><title>Committee Report</title><body><pre>
  DEPARTMENT OF THE TREASURY

  The Committee recommends an appropriation of $1,000,000 for salaries
  and expenses.
</pre></body></html>
"""

BUDGET = GovInfoBodyBudget(
    max_requests=8,
    max_body_bytes=1 << 20,
    max_metadata_bytes=1 << 18,
    timeout_seconds=10.0,
    min_request_interval_seconds=0.0,
)


#: The folder and extension GovInfo serves each rendition from; not the format
#: name (`spicy-docs/docs/sources/govinfo-bodies.md`).
_ROUTES = {"htm": ("html", "htm"), "pdf": ("pdf", "pdf")}


def _package(package_id: str, *, fmt: str = "htm", media_type: str = "text/html", body: bytes = REPORT_HTML):
    identity = parse_package_id(package_id)
    folder, extension = _ROUTES[fmt]
    url = f"https://www.govinfo.gov/content/pkg/{package_id}/{folder}/{package_id}.{extension}"
    capture = CapturedBodyResponse(
        requested_url=url,
        resolved_url=url,
        status_code=200,
        content_type=media_type,
        observed_at=OBSERVED_AT,
        body=body,
    )
    return GovInfoPackageBody(
        identity=identity,
        format=fmt,
        preference=("xml", "htm", "txt", "pdf"),
        offered_formats=("htm", "pdf"),
        summary=PackageSummary(
            identity=identity,
            collection_code=identity.collection,
            date_issued="2026-01-05",
            last_modified="2026-09-18T12:00:00Z",
            title=f"Report {package_id}",
            download_links=(),
        ),
        mods=PackageModsIdentity(
            identity=identity,
            access_ids=(package_id,),
            collection_code=identity.collection,
            offered_formats=("htm", "pdf"),
            moved_renditions=(),
            other_renditions=(),
        ),
        body=PackageBodyIdentity(
            identity=identity,
            format=fmt,
            media_type=media_type,
            final_url=url,
            byte_size=len(body),
        ),
        summary_capture=capture,
        mods_capture=capture,
        body_capture=capture,
        request_count=3,
        budget=BUDGET,
    )


class _Page:
    def __init__(self, records):
        self.records = records


class StubDiscovery:
    """One CRPT package and one CHRG package, the smallest input reaching all three tables."""

    IDS = {"CRPT": ["CRPT-119hrpt1"], "CHRG": ["CHRG-119hhrg64242"]}

    def packages(self, url: str, *, max_pages: int = 1):
        collection = next(name for name in self.IDS if f"/{name}/" in url or url.endswith(name))
        yield _Page([{"packageId": package_id} for package_id in self.IDS[collection]])


class StubBodyAcquirer:
    """Serves the htm rendition, and records that no preference was asked for."""

    def __init__(self):
        self.requested: list[str] = []

    def acquire(self, package_id: str, *, max_bytes=None):
        self.requested.append(package_id)
        return _package(package_id)


def _no_prior(remote_key: str, local_path: Path) -> bool:
    return False


@pytest.fixture
def reports(tmp_path, monkeypatch):
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    paths = build_committee_reports(
        tmp_path, reader=StubDiscovery(), acquirer=StubBodyAcquirer(), download_prior=_no_prior
    )
    return {path.stem: path for path in paths}


def test_a_non_pdf_rendition_publishes_no_page_count(reports):
    """No GovInfo body carries a form feed or a `[[Page N]]` marker, so htm states no pages.

    The old reader split on a form feed that never occurred and published
    `page_count = 1` for every report — a count of the separator's absence,
    not of pages.
    """
    for table in ("committee_reports", "hearing_transcripts"):
        rows = pq.read_table(reports[table]).to_pylist()
        assert len(rows) == 1, table
        assert rows[0]["page_count"] is None, table
        assert rows[0]["format"] == "htm", table
        assert rows[0]["media_type"] == "text/html"


def test_the_text_is_the_markup_readers_text_not_the_raw_html(reports):
    """`body_text` reads the htm body structurally; the wrapper never reaches the digest."""
    rows = pq.read_table(reports["committee_reports"]).to_pylist()
    assert rows[0]["text_sha256"].startswith("sha256:")
    assert rows[0]["byte_size"] == str(len(REPORT_HTML))

    sections = pq.read_table(reports["report_sections"]).to_pylist()
    assert sections, "the agency header must produce a block"
    assert all("<pre>" not in (row["body"] or "") for row in sections)
    assert all(row["package_id"] == "CRPT-119hrpt1" for row in sections)


def test_the_acquirer_is_asked_with_no_preference(tmp_path, monkeypatch):
    """The rendition order is the acquirer's sealed default; passing one here would fork it."""
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    acquirer = StubBodyAcquirer()
    build_committee_reports(tmp_path, reader=StubDiscovery(), acquirer=acquirer, download_prior=_no_prior)
    assert acquirer.requested == ["CRPT-119hrpt1", "CHRG-119hhrg64242"]


class StubPdfAcquirer:
    """The fallback: a package offered only as PDF, which the sealed preference now reaches."""

    def acquire(self, package_id: str, *, max_bytes=None):
        body = make_pdf([f"{package_id} page one", "page two"])
        return _package(package_id, fmt="pdf", media_type="application/pdf", body=body)


def test_the_pdf_fallback_is_extracted_and_states_its_page_count(tmp_path, monkeypatch):
    """The one rendition that does state page boundaries, and the one that needs an extractor.

    `body_text`'s default extractor opens PDFs with PyMuPDF, which this
    repository does not install — it pins the narrow `pdf-pypdf` provider — so
    without `PypdfPageExtractor` every PDF-only package would be counted
    refused and publish no row at all.
    """
    monkeypatch.delenv("COMMITTEE_REPORTS_SINCE", raising=False)
    paths = {
        path.stem: path
        for path in build_committee_reports(
            tmp_path, reader=StubDiscovery(), acquirer=StubPdfAcquirer(), download_prior=_no_prior
        )
    }
    for table in ("committee_reports", "hearing_transcripts"):
        rows = pq.read_table(paths[table]).to_pylist()
        assert len(rows) == 1, f"{table}: the PDF fallback must publish a row, not a refusal"
        assert rows[0]["format"] == "pdf"
        assert rows[0]["page_count"] == "2", "a paginated rendition states its pages"
        assert rows[0]["text_sha256"].startswith("sha256:")
