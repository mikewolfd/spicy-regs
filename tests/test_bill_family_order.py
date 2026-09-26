"""Printing order, stale comparisons and partial section sets in the bill family, on native bytes.

The 2026-09-26 qualification (``drift-qualification-2026-09-26/bills-citations/``)
found two defects in the published bill family, both fixed in spicy-docs 0.35.0.
BILLSTATUS states no date for an enrolled printing, so the provider paired it
first and diffed it *into* the introduced text (119-hr-983, enrolled ->
introduced), and the last printing -> enrolled comparison was never made. And the
old ``bill_sections`` key could not tell two native sections apart, so the merge
dropped one (119-hr-5334 enrolled, 84 -> 83), leaving a diff item naming a
section that was never published.

The provider now orders printings itself, and this host reads the same public
``printing_order``/``consecutive_pairs`` to decide which comparisons are missing.
What stays here is the repair of what was already published: each end-to-end case
first builds the prior the old provider published, from the same native bytes,
then runs as the next scheduled run would and checks the tables are repaired.
The fixtures and their provenance are in ``tests/fixtures/govinfo_bills/README.md``.
"""

from __future__ import annotations

import importlib
from collections.abc import Sequence
from contextlib import contextmanager
from itertools import pairwise
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from spicy_docs.interpretation import bill_family as family_provider
from spicy_docs.sources.congress.bill_status import BillIdentity, parse_bill_status
from spicy_docs.sources.congress.bill_versions import VERSION_CODES

from spicy_regs.transforms.table_merge import prior_scratch_path
from tests.test_bill_family import (
    FIXTURES,
    StubBodyAcquirer,
    StubBulkAcquirer,
    _Archive,
    _capture,
    _Member,
    _no_prior,
    _Package,
    _prior_from,
    _seed_prior,
)
from tests.test_bill_family import scoped as fixture_scope

scoped = fixture_scope

build = importlib.import_module("spicy_regs.transforms.build_bill_family")

HR983 = BillIdentity(congress=119, bill_type="hr", number=983)
HR5334 = BillIdentity(congress=119, bill_type="hr", number=5334)

#: Every body the two native bills are served with, by the package id it answers.
BODIES = {
    "BILLS-119hr983ih": "text-119hr983ih.xml",
    "BILLS-119hr983eh": "text-119hr983eh.xml",
    "BILLS-119hr983rfs": "text-119hr983rfs.xml",
    "BILLS-119hr983enr": "text-119hr983enr.xml",
    "BILLS-119hr5334ih": "text-119hr5334ih.xml",
    "BILLS-119hr5334enr": "text-119hr5334enr.xml",
}


def _status(identity: BillIdentity):
    name = f"status-{identity.congress}{identity.bill_type}{identity.number}.xml"
    return parse_bill_status((FIXTURES / name).read_bytes(), identity=identity)


class NativeBulk(StubBulkAcquirer):
    """The (119, hr) folder holding one native BILLSTATUS, with the stub's listing and skip behaviour."""

    def __init__(self, identity: BillIdentity):
        super().__init__()
        self.status = _status(identity)

    def acquire(self, congress, bill_type, **kwargs):
        result = super().acquire(congress, bill_type, **kwargs)
        if result.archive is not None and (congress, bill_type) == (119, "hr"):
            result.archive = _Archive([_Member(self.status)])
        return result


class NativeBodies(StubBodyAcquirer):
    """Serves the named packages' native XML and refuses every other package."""

    def __init__(self, *served: str):
        super().__init__()
        self.served = {package: BODIES[package] for package in served}

    def acquire(self, package_id, *, max_bytes=None):
        self.requested.append(package_id)
        name = self.served.get(package_id)
        if name is None:
            raise LookupError(f"not served: {package_id}")
        url = f"https://www.govinfo.gov/content/pkg/{package_id}/xml/{package_id}.xml"
        return _Package("xml", _capture(url, (FIXTURES / name).read_bytes()))


def _date_first_order(printings: Sequence[tuple[str, str | None]]) -> list[int]:
    """spicy-docs' order before 0.35.0: an empty date sorted first."""
    rank = {entry.slug: index for index, entry in enumerate(VERSION_CODES)}
    return sorted(range(len(printings)), key=lambda i: (printings[i][1] or "", rank.get(printings[i][0], len(rank))))


def _date_first_pairs(printings: Sequence[tuple[str, str | None]]) -> list[tuple[int, int]]:
    return list(pairwise(_date_first_order(printings)))


@contextmanager
def _published_before_0_35():
    """Provider and host pair printings as they did when the audited tables were published."""
    with pytest.MonkeyPatch.context() as patch:
        for module in (family_provider, build):
            patch.setattr(module, "printing_order", _date_first_order)
            patch.setattr(module, "consecutive_pairs", _date_first_pairs)
        yield


def _run(directory: Path, bulk, bodies, prior: Path | None = None) -> dict[str, Path]:
    directory.mkdir()
    paths = build.build_bill_family(
        directory,
        bulk_acquirer=bulk,
        body_acquirer=bodies,
        download_prior=_prior_from(prior) if prior else _no_prior,
    )
    return {path.stem: path for path in paths}


def _rows(paths: dict[str, Path], table: str) -> list[dict]:
    return pq.read_table(paths[table]).to_pylist()


def _pairs(paths: dict[str, Path]) -> set[tuple[str, str]]:
    return {(row["from_version_code"], row["to_version_code"]) for row in _rows(paths, "section_diffs")}


def _unresolved_items(paths: dict[str, Path]) -> list[tuple]:
    """Diff-item sides whose element or text digest no published section carries."""
    sections = {
        (row["bill_id"], row["version_code"], row["source"], row["element_id"]): row["body_sha256"]
        for row in _rows(paths, "bill_sections")
    }
    unresolved = []
    for item in _rows(paths, "section_diff_items"):
        for side in ("from", "to"):
            element = item[f"{side}_element_id"]
            if element is None:
                continue
            key = (item["bill_id"], item[f"{side}_version_code"], item[f"{side}_source"], element)
            if sections.get(key, "absent") != item[f"{side}_text_sha256"]:
                unresolved.append((*key, item["seq"]))
    return unresolved


# --------------------------------------------------------------------------- #
# Order: the host expects exactly the comparisons the provider makes.
# --------------------------------------------------------------------------- #
def test_the_host_orders_a_dateless_enrolled_printing_as_the_provider_does():
    status = _status(HR983)
    assert {version.type: version.date for version in status.text_versions}["Enrolled Bill"] == "", (
        "the fixture's own claim: BILLSTATUS states <date/> for the enrolled bill"
    )
    codes = [build.version_slug(version.type) for version in build._ordered_printings(status)]
    assert codes == ["introduced-in-house", "engrossed-in-house", "rfs", "enrolled-bill", "public-law"]
    assert build._code_pairs(build._printings(status)) == set(pairwise(codes))


def test_the_enrolled_comparison_is_made_from_the_last_printing(tmp_path, scoped):
    paths = _run(tmp_path / "run", NativeBulk(HR983), NativeBodies(*BODIES))
    assert _pairs(paths) == build._code_pairs(build._printings(_status(HR983)))
    assert ("rfs", "enrolled-bill") in _pairs(paths)
    assert _unresolved_items(paths) == []


# --------------------------------------------------------------------------- #
# Repair: what the old provider published is replaced on the next run.
# --------------------------------------------------------------------------- #
def test_the_next_run_retires_published_backward_pairs_and_makes_the_missing_ones(tmp_path, scoped):
    with _published_before_0_35():
        audited = _run(tmp_path / "audited", NativeBulk(HR983), NativeBodies(*BODIES))
    assert ("enrolled-bill", "introduced-in-house") in _pairs(audited), "reproduces the audited pair"
    assert ("rfs", "enrolled-bill") not in _pairs(audited)
    assert [row for row in _rows(audited, "section_diff_items") if row["from_version_code"] == "enrolled-bill"]

    bodies = NativeBodies(*BODIES)
    repaired = _run(tmp_path / "repaired", NativeBulk(HR983), bodies, prior=tmp_path / "audited")
    assert _pairs(repaired) == build._code_pairs(build._printings(_status(HR983)))
    assert not [
        row
        for row in _rows(repaired, "section_diff_items")
        if (row["from_version_code"], row["to_version_code"]) == ("enrolled-bill", "introduced-in-house")
    ]
    assert _unresolved_items(repaired) == []
    # The re-read scope: the printings the two new neighbour pairs need -- the
    # enrolled body once for itself and once for the law it became.
    assert sorted(bodies.requested) == ["BILLS-119hr983enr", "BILLS-119hr983enr", "BILLS-119hr983rfs"]

    steady = NativeBodies(*BODIES)
    again = _run(tmp_path / "steady", NativeBulk(HR983), steady, prior=tmp_path / "repaired")
    assert steady.requested == [], "a repaired bill is complete: no request loop"
    assert _pairs(again) == _pairs(repaired)


def test_a_published_backward_pair_alone_reopens_its_bill(tmp_path):
    """Every established pair is published, yet a stale one is too: the bill is still pending."""
    bill = "119-hr-983"
    versions = [
        {
            "bill_id": bill,
            "version_code": code,
            "source": "govinfo",
            "sha256": "sha256:x",
            "byte_size": "1",
            "format_name": "xml",
            "content_type": "text/xml",
            "section_count": "1",
            "version_date": date,
        }
        for code, date in (("introduced-in-house", "2025-02-05T05:00:00Z"), ("enrolled-bill", ""))
    ]
    _seed_prior(tmp_path, "bill_versions", versions)
    _seed_prior(
        tmp_path,
        "bill_sections",
        [{"bill_id": bill, "version_code": row["version_code"], "source": "govinfo", "seq": "0"} for row in versions],
    )
    pairs = [("introduced-in-house", "enrolled-bill"), ("enrolled-bill", "introduced-in-house")]
    key = {"from_source": "govinfo", "to_source": "govinfo", "bill_id": bill}
    _seed_prior(
        tmp_path,
        "section_diffs",
        [{**key, "from_version_code": a, "to_version_code": b, "item_count": "1"} for a, b in pairs],
    )
    _seed_prior(
        tmp_path,
        "section_diff_items",
        [{**key, "from_version_code": a, "to_version_code": b, "seq": "0"} for a, b in pairs],
    )
    paths = {
        name: prior_scratch_path(tmp_path, name)
        for name in ("bill_versions", "bill_sections", "section_diffs", "section_diff_items")
    }
    index = build._prior_index(paths)
    assert index.xml_codes(bill) == {"introduced-in-house", "enrolled-bill"}
    assert (bill, "introduced-in-house", "enrolled-bill") in index.pairs
    assert bill in index.pending_bills


def test_the_next_run_republishes_a_silently_partial_printing_whole(tmp_path, scoped):
    """The audited table lacked one of two sections the old key could not tell apart; seq keys both."""
    served = ("BILLS-119hr5334ih", "BILLS-119hr5334enr")
    with _published_before_0_35():
        audited = _run(tmp_path / "audited", NativeBulk(HR5334), NativeBodies(*served))
    # The old key's merge kept one of Division A's and Division B's "Sec. 1";
    # the audited 119-hr-5334 enrolled printing kept seq 3 and lost seq 80.
    sections = pq.read_table(audited["bill_sections"])
    dropped = [row for row in sections.to_pylist() if (row["version_code"], row["seq"]) == ("enrolled-bill", "6")]
    assert dropped and dropped[0]["match_path"] == "sec. 1"
    pq.write_table(
        sections.filter([(row["version_code"], row["seq"]) != ("enrolled-bill", "6") for row in sections.to_pylist()]),
        audited["bill_sections"],
    )
    assert ("enrolled-bill", "introduced-in-house") in _pairs(audited)
    assert _unresolved_items(audited), "the diff item naming the dropped section"

    repaired = _run(tmp_path / "repaired", NativeBulk(HR5334), NativeBodies(*served), prior=tmp_path / "audited")
    enrolled = [row for row in _rows(repaired, "bill_sections") if row["version_code"] == "enrolled-bill"]
    (declared,) = [
        row["section_count"]
        for row in _rows(repaired, "bill_versions")
        if row["version_code"] == "enrolled-bill" and row["source"] == "govinfo"
    ]
    assert sorted(int(row["seq"]) for row in enrolled) == list(range(int(declared))) == list(range(10))
    assert ("enrolled-bill", "introduced-in-house") not in _pairs(repaired)
    assert _unresolved_items(repaired) == []


# --------------------------------------------------------------------------- #
# Identity: a repeated key is refused by name, never collapsed by the merge.
# --------------------------------------------------------------------------- #
def test_a_repeated_key_is_refused_by_name_not_left_to_the_merge():
    section: dict[str, str | None] = {column: "x" for column in build.TABLE_CONTRACTS["bill_sections"].columns}
    other = section | {"seq": "1"}
    tables = build.BillFamilyTables(bill_sections=(section, other, dict(section)))
    admitted = build._refuse_repeated_keys(tables)
    assert admitted.bill_sections == (section, other)
    key = tuple(section[column] or "" for column in build.TABLE_CONTRACTS["bill_sections"].identity)
    assert admitted.refusals == (build.FamilyRefusal("bill_sections", key, build.REPEATED_KEY_REASON),)
    assert build._refuse_repeated_keys(admitted) is admitted, "nothing repeated, nothing refused"
