"""Numbered reprints in the bill family: each is its own printing, and what was published before is repaired.

119 HR 6644 went back and forth between the chambers, and GovInfo holds two
Senate engrossed amendments, ``BILLS-119hr6644eas`` (2026-03-12) and ``…eas2``
(2026-06-22), both typed "Engrossed Amendment Senate". Before spicy-docs 0.37.0
both took the code ``engrossed-amendment-senate``, and the live generation
``d380cdc0`` published the result: one printing whose sections mix both
documents, a congress row describing ``eas2`` under the first printing's key,
and two comparisons into and out of ``eas2`` whose items name sections never
published (receipt ``fork-execution-2026-09-21/repeated-printings-2026-09-26/``).

The end-to-end case first rebuilds that published shape from the same native
bytes, through the provider of the time fed every printing the host then
fetched, then runs as the next scheduled run would and checks the repair. The
fixtures and their provenance are in ``tests/fixtures/govinfo_bills/README.md``.
"""

from __future__ import annotations

import importlib
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.interpretation import bill_family as family_provider
from spicy_docs.schemas import TABLE_CONTRACTS
from spicy_docs.sources.congress.bill_status import BillIdentity
from spicy_docs.sources.congress.bill_tree import parse_bill_tree
from spicy_docs.sources.congress.bill_versions import version_slug

from spicy_regs.transforms.table_merge import prior_scratch_path
from tests.test_bill_family import FIXTURES, StubBodyAcquirer, _capture, _Package, _seed_prior
from tests.test_bill_family import scoped as fixture_scope
from tests.test_bill_family_order import NativeBulk, _pairs, _rows, _run, _status, _unresolved_items

scoped = fixture_scope

build = importlib.import_module("spicy_regs.transforms.build_bill_family")

HR6644 = BillIdentity(congress=119, bill_type="hr", number=6644)
HR3426 = BillIdentity(congress=119, bill_type="hr", number=3426)
HR7643 = BillIdentity(congress=118, bill_type="hr", number=7643)
EAS = "engrossed-amendment-senate"
EAH = "engrossed-amendment-house"


class PackageBodies(StubBodyAcquirer):
    """Serves ``text-{package stem}.xml`` for every package that has a fixture, and refuses the rest."""

    def acquire(self, package_id, *, max_bytes=None):
        self.requested.append(package_id)
        path = FIXTURES / f"text-{package_id.removeprefix('BILLS-')}.xml"
        if not path.exists():
            raise LookupError(f"not served: {package_id}")
        url = f"https://www.govinfo.gov/content/pkg/{package_id}/xml/{package_id}.xml"
        return _Package("xml", _capture(url, path.read_bytes()))


def _elements(stem: str) -> list[str | None]:
    """The element ids of a fixture printing's sections, in document order."""
    return [node.element_id for node in parse_bill_tree((FIXTURES / f"text-{stem}.xml").read_bytes()).sections]


@contextmanager
def _published_before_0_37():
    """Host and provider key printings as they did when generation d380cdc0 was published.

    The host coded every printing by its stage name, and the provider refused a
    repeated printing's rows one at a time rather than the printing whole.
    """
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(build, "printing_version_code", lambda version: version_slug(version.type))
        patch.setattr(family_provider, "_repeated_printings", lambda ordered: set())
        yield


def _published_by_the_old_host(directory: Path, identity: BillIdentity) -> dict[str, Path]:
    """The tables generation d380cdc0 published for one bill, from the same native bytes.

    The host of the time read every listed printing's body into one
    ``build_bill_family`` call, keyed by stage name, so both Senate
    engrossments arrived under one code; a printing with no fixture is the
    metadata-only row it published for an unread body.
    """
    directory.mkdir()
    status = _status(identity)
    bodies = PackageBodies()
    with _published_before_0_37():
        versions = []
        for listed in build._listed_captures(status):
            try:
                package = bodies.acquire(listed.package_id or "")
            except LookupError:
                versions.append(listed)
                continue
            document = parse_bill_tree(package.body_capture.body, version=listed.version_code)
            versions.append(replace(listed, source="govinfo", body=package.body_capture, document=document))
        tables = family_provider.build_bill_family(
            family_provider.BillFamilyCapture(status=status, versions=tuple(versions)), engine=build.engine_stamp()
        )
    paths = {}
    for contract, attr in build.FAMILY_TABLES:
        columns = TABLE_CONTRACTS[contract].columns
        schema = pa.schema([(column, pa.string()) for column in columns])
        paths[contract] = directory / f"{contract}.parquet"
        pq.write_table(pa.Table.from_pylist(list(getattr(tables, attr)), schema=schema), paths[contract])
    return paths


def _with_stale_listing(paths: dict[str, Path]) -> None:
    """Add the congress row an earlier generation left: ``eas2``'s listing under the first printing's code."""
    table = pq.read_table(paths["bill_versions"])
    stale = {name: None for name in table.schema.names} | {
        "bill_id": "119-hr-6644",
        "version_code": EAS,
        "source": "congress",
        "label": "Engrossed Amendment Senate",
        "version_date": "2026-06-22T04:00:00Z",
        "package_id": "BILLS-119hr6644eas2",
    }
    pq.write_table(pa.Table.from_pylist([*table.to_pylist(), stale], schema=table.schema), paths["bill_versions"])


def _sections(paths: dict[str, Path], code: str) -> list[str | None]:
    rows = [row for row in _rows(paths, "bill_sections") if row["version_code"] == code]
    return [row["element_id"] for row in sorted(rows, key=lambda row: int(row["seq"]))]


def _versions(paths: dict[str, Path], code: str) -> list[tuple[str, str]]:
    return sorted(
        (row["source"], row["package_id"]) for row in _rows(paths, "bill_versions") if row["version_code"] == code
    )


# --------------------------------------------------------------------------- #
# Identity: a numbered reprint the publisher addresses by package is its own printing.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize(
    ("identity", "reprint", "stage"),
    [(HR6644, "eas2", EAS), (HR3426, "rfs2", "rfs"), (HR7643, "rh2", "reported-in-house")],
)
def test_the_host_keys_a_numbered_reprint_as_its_own_printing(identity, reprint, stage):
    codes = [code for code, _ in build._printings(_status(identity))]
    assert reprint in codes and stage in codes
    assert len(set(codes)) == len(codes), "every listed printing has its own key"


# --------------------------------------------------------------------------- #
# Repair: the next run over the published d380cdc0 shape.
# --------------------------------------------------------------------------- #
def test_the_next_run_separates_a_mixed_printing_and_its_comparisons(tmp_path, scoped):
    audited = _published_by_the_old_host(tmp_path / "audited", HR6644)
    _with_stale_listing(audited)
    eas, eas2 = _elements("119hr6644eas"), _elements("119hr6644eas2")
    assert len(eas) < len(eas2), "the fixture keeps the published shape: the reprint has more sections"
    assert _sections(audited, EAS) == eas + eas2[len(eas) :], "reproduces the mixed printing"
    assert {(EAH, EAS), (EAS, "enrolled-bill")} <= _pairs(audited)
    assert _unresolved_items(audited), "and the items naming the reprint's withheld sections"

    bodies = PackageBodies()
    repaired = _run(tmp_path / "repaired", NativeBulk(HR6644), bodies, prior=tmp_path / "audited")
    assert _sections(repaired, EAS) == eas, "the first printing's own sections, whole"
    assert _sections(repaired, "eas2") == eas2, "the reprint under its own code"
    assert _versions(repaired, EAS) == [("govinfo", "BILLS-119hr6644eas")], "the stale listing row is retired"
    assert _versions(repaired, "eas2") == [("govinfo", "BILLS-119hr6644eas2")]
    assert not {(EAH, EAS), (EAS, "enrolled-bill")} & _pairs(repaired), "both stale comparisons retire"
    assert {("placed-on-calendar-senate", EAS), (EAS, EAH), (EAH, "eas2"), ("eas2", "enrolled-bill")} <= _pairs(
        repaired
    )
    assert _unresolved_items(repaired) == []
    # The re-read scope: the two unheld printings (the first printing's sections
    # no longer match its row) and the neighbours of the comparisons not yet
    # published; placed-on-calendar -> eas is published whole, so pcs is not read.
    assert sorted(bodies.requested) == [f"BILLS-119hr6644{suffix}" for suffix in ("eah", "eas", "eas2", "enr")]

    steady = PackageBodies()
    again = _run(tmp_path / "steady", NativeBulk(HR6644), steady, prior=tmp_path / "repaired")
    assert steady.requested == [], "a repaired bill is complete: no request loop"
    assert _pairs(again) == _pairs(repaired)


def test_a_cold_run_publishes_each_referral_as_its_own_printing(tmp_path, scoped):
    """119 HR 3426 was referred to the Senate twice; ``rfs2`` was once refused as a repeat of ``rfs``."""
    paths = _run(tmp_path / "run", NativeBulk(HR3426), PackageBodies())
    for code, stem in (("rfs", "119hr3426rfs"), ("rfs2", "119hr3426rfs2"), ("engrossed-in-house", "119hr3426eh1s")):
        assert _sections(paths, code) == _elements(stem)
    assert {
        ("rfs", "returned-to-the-house-by-unanimous-consent"),
        ("returned-to-the-house-by-unanimous-consent", "rfs2"),
    } <= (_pairs(paths))
    assert _unresolved_items(paths) == []


def test_a_row_keyed_under_another_printings_code_alone_reopens_its_bill(tmp_path):
    """118 HR 7643's published congress row ``reported-in-house`` carries ``rh2``'s package.

    Everything else about the seeded bill is complete -- both printings held with
    their sections, their one comparison published -- so only that row reopens it.
    """
    bill = "118-hr-7643"
    held = [
        {
            "bill_id": bill,
            "version_code": code,
            "source": "govinfo",
            "label": label,
            "version_date": date,
            "package_id": f"BILLS-118hr7643{suffix}",
            "sha256": "sha256:x",
            "byte_size": "1",
            "format_name": "xml",
            "content_type": "text/xml",
            "section_count": "1",
        }
        for code, label, date, suffix in (
            ("introduced-in-house", "Introduced in House", "2024-03-12T04:00:00Z", "ih"),
            ("reported-in-house", "Reported in House", "2024-07-18T04:00:00Z", "rh"),
        )
    ]
    stale = held[1] | {"source": "congress", "version_date": "2024-08-27T04:00:00Z", "package_id": "BILLS-118hr7643rh2"}
    _seed_prior(tmp_path, "bill_versions", [*held, stale])
    _seed_prior(
        tmp_path,
        "bill_sections",
        [{"bill_id": bill, "version_code": row["version_code"], "source": "govinfo", "seq": "0"} for row in held],
    )
    pair = {
        "bill_id": bill,
        "from_version_code": "introduced-in-house",
        "from_source": "govinfo",
        "to_version_code": "reported-in-house",
        "to_source": "govinfo",
    }
    _seed_prior(tmp_path, "section_diffs", [pair | {"item_count": "1"}])
    _seed_prior(tmp_path, "section_diff_items", [pair | {"seq": "0"}])
    names = ("bill_versions", "bill_sections", "section_diffs", "section_diff_items")
    index = build._prior_index({name: prior_scratch_path(tmp_path, name) for name in names})
    assert index.xml_codes(bill) == {"introduced-in-house", "reported-in-house"}
    assert index.miskeyed_rows == {bill: {("reported-in-house", "congress", "rh2")}}
    assert bill in index.pending_bills
