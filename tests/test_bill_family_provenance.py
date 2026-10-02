"""Printing attachment and held-state repair against native, independently identified printings."""

from __future__ import annotations

import importlib
import json

import pyarrow as pa
import pytest

from tests.test_bill_family import _priors, read_output, write_output
from tests.test_bill_family import scoped as fixture_scope
from tests.test_bill_family_order import BODIES, HR983, NativeBodies, NativeBulk, _run

scoped = fixture_scope
build = importlib.import_module("spicy_regs.transforms.build_bill_family")


def _replace(paths, table, rows):
    write_output(paths[table], pa.Table.from_pylist(rows, schema=read_output(paths[table]).schema))


def _rows(paths, table):
    return sorted(read_output(paths[table]).to_pylist(), key=lambda row: json.dumps(row, sort_keys=True))


def _seed_alias(paths):
    """Seed the former published association explicitly; never ask the new selector to recreate it."""
    versions = _rows(paths, "bill_versions")
    enrolled = next(row for row in versions if row["version_code"] == "enrolled-bill")
    law = next(row for row in versions if row["version_code"] == "public-law")
    wrong = enrolled | {key: law[key] for key in ("version_code", "label", "version_date", "offered_formats_json")}
    _replace(paths, "bill_versions", [row for row in versions if row["version_code"] != "public-law"] + [wrong])
    sections = [row for row in _rows(paths, "bill_sections") if row["version_code"] != "public-law"]
    _replace(paths, "bill_sections", sections + [
        row | {"version_code": "public-law"} for row in sections if row["version_code"] == "enrolled-bill"
    ])
    pair = {
        "bill_id": "119-hr-983", "from_version_code": "enrolled-bill", "from_source": "govinfo",
        "to_version_code": "public-law", "to_source": "govinfo",
    }
    for table in ("section_diffs", "section_diff_items", "financial_changes", "diff_summaries"):
        kept = [row for row in _rows(paths, table)
                if "public-law" not in (row["from_version_code"], row["to_version_code"])]
        _replace(paths, table, kept + [pair | {"item_count": "1", "seq": "0"}])
    for table in ("bill_summaries", "section_classifications"):
        _replace(paths, table, [
            {"bill_id": "119-hr-983", "version_code": code, "source": "govinfo", "seq": "0", "label": "test"}
            for code in ("public-law", "enrolled-bill")
        ])


@pytest.mark.parametrize("orphaned", [False, True])
def test_unchanged_status_repairs_held_law_and_only_its_dependents(tmp_path, scoped, orphaned):
    prior = _run(tmp_path / "prior", NativeBulk(HR983), NativeBodies(*BODIES))
    _seed_alias(prior)
    if orphaned:
        _replace(prior, "section_diffs", [row for row in _rows(prior, "section_diffs")
                                        if row["to_version_code"] != "public-law"])
    enrolled = [row for row in _rows(prior, "bill_sections") if row["version_code"] == "enrolled-bill"]
    repaired = _run(tmp_path / "repaired", NativeBulk(HR983), NativeBodies(*BODIES), tmp_path / "prior")
    law = [row for row in _rows(repaired, "bill_versions") if row["version_code"] == "public-law"]
    assert len(law) == 1 and law[0]["source"] == "congress"
    assert all(law[0][key] is None for key in (
        "sha256", "package_id", "section_count", "publisher_stage", "format_name", "format_type",
        "requested_url", "resolved_url", "content_type", "byte_size", "observed_at",
    ))
    assert "PLAW-119publ55" in law[0]["offered_formats_json"]
    assert [row for row in _rows(repaired, "bill_sections") if row["version_code"] == "enrolled-bill"] == enrolled
    for table in ("bill_sections", "bill_summaries", "section_classifications"):
        assert not any(row["version_code"] == "public-law" for row in _rows(repaired, table)), table
        assert any(row["version_code"] == "enrolled-bill" for row in _rows(repaired, table)), table
    for table in ("section_diffs", "section_diff_items", "financial_changes", "diff_summaries"):
        assert not any("public-law" in (row["from_version_code"], row["to_version_code"])
                       for row in _rows(repaired, table)), table
    body = NativeBodies(*BODIES)
    again = _run(tmp_path / "again", NativeBulk(HR983), body, tmp_path / "repaired")
    assert body.requested == []
    for table in ("bill_versions", "bill_sections", "section_diffs", "section_diff_items",
                  "financial_changes", "diff_summaries", "bill_summaries", "section_classifications"):
        assert _rows(again, table) == _rows(repaired, table), table


def test_out_of_scope_held_printings_are_preserved(tmp_path, scoped, monkeypatch):
    prior = _run(tmp_path / "prior", NativeBulk(HR983), NativeBodies(*BODIES))
    _seed_alias(prior)
    monkeypatch.setenv("BILL_FAMILY_CONGRESSES", "118")
    out = tmp_path / "out"
    out.mkdir()
    paths = {path.stem: path for path in build.build_bill_family(
        out, bulk_acquirer=NativeBulk(HR983), body_acquirer=NativeBodies(*BODIES), **_priors(tmp_path / "prior")
    )}
    assert _rows(paths, "bill_versions") == _rows(prior, "bill_versions")
    assert _rows(paths, "bill_sections") == _rows(prior, "bill_sections")


@pytest.mark.parametrize("wrong", ["BILLS-119hr6028eh", "BILLS-119hr5334enr"])
@pytest.mark.parametrize("redirect_only", [False, True])
def test_a_package_response_for_another_printing_is_never_attached(tmp_path, scoped, wrong, redirect_only):
    from tests.test_bill_family import FIXTURES, _Package, _capture, _no_prior, StubBulkAcquirer, StubBodyAcquirer

    class WrongBody(StubBodyAcquirer):
        def acquire(self, package_id, *, max_bytes=None):
            from dataclasses import replace

            url = f"https://www.govinfo.gov/content/pkg/{wrong}/xml/{wrong}.xml"
            capture = _capture(url, (FIXTURES / "text-119hr6028ih.xml").read_bytes())
            if redirect_only:
                capture = replace(capture, requested_url=(
                    f"https://www.govinfo.gov/content/pkg/{package_id}/xml/{package_id}.xml"
                ))
            return _Package("xml", capture)

    paths = {path.stem: path for path in build.build_bill_family(
        tmp_path, bulk_acquirer=StubBulkAcquirer(), body_acquirer=WrongBody(), download_prior=_no_prior
    )}
    assert not any(row["version_code"] == "introduced-in-house" and row["source"] == "govinfo"
                   for row in _rows(paths, "bill_versions"))


def test_bulk_attachment_rejects_a_stale_law_target_even_if_the_member_parses():
    from tests.test_bill_family import FIXTURES
    from tests.test_bill_family_bulk_text import FolderBills
    from tests.test_bill_family_order import _status

    bodies = importlib.import_module("spicy_regs.transforms.bill_family_bodies")
    package = "BILLS-119hr983enr"
    source = FolderBills({(119, 1, "hr"): {package: (FIXTURES / "text-119hr983enr.xml").read_bytes()}})
    versions = {version.type: version for version in _status(HR983).text_versions}
    work = bodies.BillWork("119-hr-983", HR983, [
        bodies.Printing(code, versions[label], package, False, False, None, True)
        for code, label in (("enrolled-bill", "Enrolled Bill"), ("public-law", "Public Law"))
    ], {"enrolled-bill", "public-law"}, [])
    outcome = bodies.BodyPass(
        bills_source=source, body_source=None, remaining=[0], engine=build.engine_stamp(),
        classify=None, summarize_diff=None, refusals={},
    ).run([work])
    acquired = [row for tables in outcome.families for row in tables.bill_versions if row["source"] == "govinfo"]
    assert [row["version_code"] for row in acquired] == ["enrolled-bill"]
    assert acquired[0]["sha256"] == "sha256:b95447fdcbe46553d563f2a656e32f4b3a9861b92e4d75e57ba0812c5eb3a12a"
