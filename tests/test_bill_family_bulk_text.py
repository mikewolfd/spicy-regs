"""Bill text from the BILLS bulk folders: one zip read for every printing it holds, and a second run that does nothing.

The folders are built here from the native printings of 119 HR 6028 and read
by spicy-docs' own listing and archive readers, so the host is exercised
against the provider's real member checks; only the transport is replaced. The
fixture bytes and their provenance are in ``tests/fixtures/govinfo_bills/README.md``.
"""

from __future__ import annotations

import hashlib
import importlib
import io
import json
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pyarrow.parquet as pq
from spicy_docs.sources.congress.bulk_bills import (
    bulk_bills_locator,
    read_bulk_bills_archive,
    read_bulk_bills_listing,
)
from spicy_docs.transport.captured import CapturedBodyResponse

from tests.test_bill_family import FIXTURES, TEXT_FIXTURES, StubBodyAcquirer, StubBulkAcquirer, _no_prior, _prior_from
from tests.test_bill_family import scoped as fixture_scope

scoped = fixture_scope

build = importlib.import_module("spicy_regs.transforms.build_bill_family")
bodies = importlib.import_module("spicy_regs.transforms.bill_family_bodies")

OBSERVED_AT = "2026-09-26T12:00:00Z"
BOTH = {package: (FIXTURES / name).read_bytes() for package, name in TEXT_FIXTURES.items()}


class FolderBills:
    """BILLS folders served from memory: ``{(congress, session, type): {package id: bytes}}``.

    ``stamps`` sets each folder zip's listed Last-Modified, so a test can move a
    zip between runs; ``downloads`` and ``listings`` record what was asked for.
    """

    def __init__(self, folders, stamps=None):
        self.folders = folders
        self.stamps = stamps or {}
        self.listings: list[tuple[int, int, str]] = []
        self.downloads: list[tuple[int, int, str]] = []

    def _zip(self, folder) -> bytes:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            for package, data in self.folders[folder].items():
                archive.writestr(f"{package}.xml", data)
        return buffer.getvalue()

    def list_folder(self, congress, session, bill_type):
        folder = (congress, session, bill_type)
        self.listings.append(folder)
        base = f"https://www.govinfo.gov/bulkdata/BILLS/{congress}/{session}/{bill_type}"
        stamp = self.stamps.get(folder, "26-Sep-2026 09:47")

        def entry(name, size, mime):
            return {
                "name": name,
                "displayLabel": name,
                "justFileName": name,
                "link": f"{base}/{name}",
                "folder": False,
                "formattedLastModifiedTime": stamp,
                "mimeType": mime,
                "size": size,
            }

        members = self.folders.get(folder, {})
        files = [entry(f"{package}.xml", len(data), "application/xml") for package, data in members.items()]
        files.append(
            entry(
                f"BILLS-{congress}-{session}-{bill_type}.zip",
                len(self._zip(folder)) if members else 22,
                "application/zip",
            )
        )
        listing = read_bulk_bills_listing(
            json.dumps({"files": files}).encode(), congress=congress, session=session, bill_type=bill_type
        )
        return SimpleNamespace(listing=listing)

    def acquire(self, listing, *, keep=None):
        folder = (listing.congress, listing.session, listing.bill_type)
        self.downloads.append(folder)
        url = bulk_bills_locator(*folder)
        capture = CapturedBodyResponse(url, url, 200, "application/zip", OBSERVED_AT, self._zip(folder))
        return SimpleNamespace(archive=read_bulk_bills_archive(capture, listing=listing, keep=keep), capture=capture)


def run(directory: Path, bills, *, prior: Path | None = None, body=None, budget: int = 600):
    directory.mkdir()
    body = body or StubBodyAcquirer()
    paths = build.build_bill_family(
        directory,
        bulk_acquirer=StubBulkAcquirer(),
        body_acquirer=body,
        bills_acquirer=bills,
        max_version_fetches=budget,
        download_prior=_prior_from(prior) if prior else _no_prior,
    )
    return {path.stem: path for path in paths}, body


def rows(paths, table):
    return pq.read_table(paths[table]).to_pylist()


def test_one_zip_read_gives_every_printing_it_holds_and_the_comparison(tmp_path, scoped):
    bills = FolderBills({(119, 1, "hr"): BOTH})
    paths, body = run(tmp_path / "run", bills)
    assert bills.downloads == [(119, 1, "hr")], "one download for both printings"
    assert body.requested == [], "no per-package request for what the bulk folder holds"
    versions = rows(paths, "bill_versions")
    assert sorted((row["version_code"], row["source"]) for row in versions) == [
        ("engrossed-in-house", "govinfo"),
        ("introduced-in-house", "govinfo"),
    ], "one row per printing: the listed placeholder is superseded by its body"
    zip_url = "https://www.govinfo.gov/bulkdata/BILLS/119/1/hr/BILLS-119-1-hr.zip"
    for row in versions:
        data = BOTH[row["package_id"]]
        assert (row["requested_url"], row["resolved_url"], row["content_type"]) == (zip_url, zip_url, "application/xml")
        assert (row["byte_size"], row["sha256"]) == (str(len(data)), "sha256:" + hashlib.sha256(data).hexdigest())
        assert row["format_name"] == "xml"
    assert len(rows(paths, "section_diffs")) == 1


def test_bulk_sections_equal_the_per_package_sections_element_for_element(tmp_path, scoped):
    """The same bytes by either route give the same section rows, every column."""
    bulk, _ = run(tmp_path / "bulk", FolderBills({(119, 1, "hr"): BOTH}))
    package, body = run(tmp_path / "package", FolderBills({}))
    assert sorted(body.requested) == sorted(BOTH), "the empty folder leaves both to the per-package route"
    assert rows(bulk, "bill_sections") and rows(bulk, "bill_sections") == rows(package, "bill_sections")
    assert rows(bulk, "section_diff_items") == rows(package, "section_diff_items")


def test_a_second_run_reads_no_listing_no_zip_and_rewrites_nothing(tmp_path, scoped):
    first, _ = run(tmp_path / "first", FolderBills({(119, 1, "hr"): BOTH}))
    bills = FolderBills({(119, 1, "hr"): BOTH})
    second, body = run(tmp_path / "second", bills, prior=tmp_path / "first")
    assert (bills.listings, bills.downloads, body.requested) == ([], [], [])
    for table in ("bill_versions", "bill_sections", "section_diffs", "section_diff_items"):
        assert second[table].read_bytes() == first[table].read_bytes(), f"{table} is byte-identical"


def test_a_printing_in_the_second_session_is_compared_with_its_first_session_neighbour(tmp_path, scoped):
    folders = {
        (119, 1, "hr"): {"BILLS-119hr6028ih": BOTH["BILLS-119hr6028ih"]},
        (119, 2, "hr"): {"BILLS-119hr6028eh": BOTH["BILLS-119hr6028eh"]},
    }

    class Unpublished(StubBodyAcquirer):
        """The engrossed printing is not published anywhere yet."""

        def acquire(self, package_id, *, max_bytes=None):
            self.requested.append(package_id)
            raise LookupError(f"not yet published: {package_id}")

    first, _ = run(
        tmp_path / "first",
        FolderBills({(119, 1, "hr"): folders[(119, 1, "hr")], (119, 2, "hr"): {}}),
        body=Unpublished(),
    )
    assert [row["version_code"] for row in rows(first, "bill_versions") if row["source"] == "govinfo"] == [
        "introduced-in-house"
    ]
    bills = FolderBills(folders)
    second, body = run(tmp_path / "second", bills, prior=tmp_path / "first")
    assert sorted(bills.downloads) == [(119, 1, "hr"), (119, 2, "hr")], "the held neighbour is read for the comparison"
    assert body.requested == []
    assert len(rows(second, "section_diffs")) == 1
    assert rows(second, "bill_sections") == rows(
        run(tmp_path / "whole", FolderBills({(119, 1, "hr"): BOTH}))[0], "bill_sections"
    )


def test_an_unchanged_zip_is_not_read_again_for_a_printing_it_refused(tmp_path, scoped, monkeypatch):
    broken = {"BILLS-119hr6028ih": BOTH["BILLS-119hr6028ih"], "BILLS-119hr6028eh": b"<bill>"}
    first, _ = run(tmp_path / "first", FolderBills({(119, 1, "hr"): broken}))
    refused = json.loads(pq.read_schema(first[build.ARCHIVES_TABLE]).metadata[bodies.TEXT_REFUSALS_KEY.encode()])
    link = "https://www.govinfo.gov/bulkdata/BILLS/119/1/hr/BILLS-119-1-hr.zip"
    assert refused[link]["packages"] == ["BILLS-119hr6028eh"]

    same = FolderBills({(119, 1, "hr"): broken})
    run(tmp_path / "second", same, prior=tmp_path / "first")
    assert same.listings and same.downloads == [], "the listing proves the zip unchanged: nothing new to read"

    moved = FolderBills({(119, 1, "hr"): BOTH}, stamps={(119, 1, "hr"): "27-Sep-2026 09:47"})
    third, _ = run(tmp_path / "third", moved, prior=tmp_path / "first")
    assert moved.downloads == [(119, 1, "hr")], "a moved zip is read again"
    assert len(rows(third, "section_diffs")) == 1
    metadata = pq.read_schema(third[build.ARCHIVES_TABLE]).metadata[bodies.TEXT_REFUSALS_KEY.encode()]
    assert json.loads(metadata) == {}


def test_the_bulk_pass_is_bounded_by_whole_folders(tmp_path, scoped, monkeypatch):
    monkeypatch.setattr(bodies, "MAX_BULK_PRINTINGS", 0)
    bills = FolderBills({(119, 1, "hr"): BOTH})
    paths, body = run(tmp_path / "run", bills)
    assert (bills.listings, bills.downloads, body.requested) == ([], [], [])
    assert {row["source"] for row in rows(paths, "bill_versions")} == {"congress"}, "left pending for the next run"


def test_an_enrolled_bill_and_its_public_law_are_both_read_from_their_one_member(tmp_path, scoped):
    """119 HR 983's enrolled printing and the law it became are one package, ``BILLS-119hr983enr``."""
    from tests.test_bill_family_order import BODIES, HR983, NativeBulk

    folder = {package: (FIXTURES / name).read_bytes() for package, name in BODIES.items() if "983" in package}
    bills = FolderBills({(119, 1, "hr"): folder})
    directory = tmp_path / "run"
    directory.mkdir()
    paths = {
        path.stem: path
        for path in build.build_bill_family(
            directory,
            bulk_acquirer=NativeBulk(HR983),
            body_acquirer=StubBodyAcquirer(),
            bills_acquirer=bills,
            download_prior=_no_prior,
        )
    }
    acquired = {row["version_code"]: row for row in rows(paths, "bill_versions") if row["source"] == "govinfo"}
    assert acquired["enrolled-bill"]["sha256"] == acquired["public-law"]["sha256"]
    assert ("enrolled-bill", "public-law") in {
        (row["from_version_code"], row["to_version_code"]) for row in rows(paths, "section_diffs")
    }
    assert bills.downloads == [(119, 1, "hr")]


def test_no_zip_is_read_for_a_neighbour_whose_new_printing_was_not(tmp_path, scoped):
    """The engrossed printing is in no listing and the per-package cap is spent: its held neighbour stays unread."""

    class Unpublished(StubBodyAcquirer):
        def acquire(self, package_id, *, max_bytes=None):
            self.requested.append(package_id)
            raise LookupError(f"not yet published: {package_id}")

    only_introduced = {(119, 1, "hr"): {"BILLS-119hr6028ih": BOTH["BILLS-119hr6028ih"]}}
    run(tmp_path / "first", FolderBills(only_introduced), body=Unpublished())
    bills = FolderBills(only_introduced)
    _, body = run(tmp_path / "second", bills, prior=tmp_path / "first", body=Unpublished(), budget=0)
    assert body.requested == [] and bills.downloads == [], "nothing to compare the held printing with this run"
    assert bills.listings, "the listings still answer whether bulk holds the pending printing"
