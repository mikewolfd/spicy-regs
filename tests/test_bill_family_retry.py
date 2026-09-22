"""A capped or failed body pass stays retryable despite unchanged BILLSTATUS."""

import importlib
import json
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml

from spicy_docs.sources.congress.bill_status import parse_bill_status
from spicy_docs.transport.credentials import CredentialRefusedError
from tests.test_bill_family import (
    FIXTURES,
    IDENTITY,
    StubBodyAcquirer,
    StubBulkAcquirer,
    StubPdfBodyAcquirer,
    _no_prior,
    _pdf_only_status,
    _prior_from,
    scoped as fixture_scope,
)

scoped = fixture_scope

build = importlib.import_module("spicy_regs.transforms.build_bill_family")
rollup = importlib.import_module("spicy_regs.pipelines.rollups.bill_family")


def run(directory, *, prior=None, budget=600, bulk=None, body=None):
    directory.mkdir()
    bulk = bulk or StubBulkAcquirer()
    body = body or StubBodyAcquirer()
    paths = build.build_bill_family(
        directory,
        bulk_acquirer=bulk,
        body_acquirer=body,
        max_version_fetches=budget,
        download_prior=_prior_from(prior) if prior else _no_prior,
    )
    return {path.stem: path for path in paths}, bulk, body


def acquired(paths):
    return {
        row["version_code"]: row
        for row in pq.read_table(paths["bill_versions"]).to_pylist()
        if row["source"] == "govinfo"
    }


def completed_scopes(paths):
    metadata = pq.read_schema(paths[build.ARCHIVES_TABLE]).metadata or {}
    return json.loads(metadata[build.ARCHIVE_COMPLETION_KEY.encode()])


def test_one_body_caps_resume_then_retry_pending_pair_without_metadata_change(tmp_path, scoped):
    first, _, first_body = run(tmp_path / "first", budget=1)
    assert first_body.requested == ["BILLS-119hr6028ih"]
    assert len(acquired(first)) == 1 and completed_scopes(first) == []
    held = acquired(first)["introduced-in-house"]

    second, second_bulk, second_body = run(tmp_path / "second", prior=tmp_path / "first", budget=1)
    assert second_bulk.zip_downloads == [(119, "hr")]
    assert second_body.requested == ["BILLS-119hr6028eh"], "missing body must precede its held neighbour"
    assert len(acquired(second)) == 2
    assert acquired(second)["introduced-in-house"] == held
    assert pq.read_table(second["section_diffs"]).num_rows == 0
    assert completed_scopes(second) == [], "a missing XML comparison must remain unfinished"

    third, _, third_body = run(tmp_path / "third", prior=tmp_path / "second", budget=2)
    assert set(third_body.requested) == {"BILLS-119hr6028ih", "BILLS-119hr6028eh"}
    assert pq.read_table(third["section_diffs"]).num_rows == 1
    assert completed_scopes(third) == [["119", "hr"]]

    fourth, fourth_bulk, fourth_body = run(tmp_path / "fourth", prior=tmp_path / "third", budget=1)
    assert fourth_bulk.zip_downloads == [] and fourth_body.requested == []
    assert acquired(fourth) == acquired(third)


def test_failed_xml_parse_is_not_a_completed_govinfo_body(tmp_path, scoped, monkeypatch):
    with monkeypatch.context() as failed:
        failed.setattr(build, "parse_bill_tree", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad XML")))
        first, _, _ = run(tmp_path / "first")
    assert all(row["sha256"] and row["section_count"] is None for row in acquired(first).values())
    assert completed_scopes(first) == []
    second, _, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert len(body.requested) == 2
    assert all(row["section_count"] for row in acquired(second).values())
    assert completed_scopes(second) == [["119", "hr"]]


def test_missing_published_sections_invalidate_a_completed_archive_skip(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    sections = pq.read_table(first["bill_sections"])
    pq.write_table(sections.slice(0, 0), first["bill_sections"])
    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")] and len(body.requested) == 2
    assert pq.read_table(second["bill_sections"]).num_rows == sections.num_rows


def test_failed_pdf_cleanup_retries_unchanged_status_then_qualifies_skip(tmp_path, scoped, monkeypatch):
    with monkeypatch.context() as failed:
        failed.setattr(build, "body_text", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad PDF")))
        first, _, _ = run(
            tmp_path / "first", bulk=StubBulkAcquirer(status=_pdf_only_status()), body=StubPdfBodyAcquirer()
        )
    assert all(row["sha256"] and row["cleanup_json"] is None for row in acquired(first).values())
    assert completed_scopes(first) == []
    second, _, body = run(
        tmp_path / "second",
        prior=tmp_path / "first",
        bulk=StubBulkAcquirer(status=_pdf_only_status()),
        body=StubPdfBodyAcquirer(),
    )
    assert len(body.requested) == 2
    assert all(row["cleanup_json"] for row in acquired(second).values())
    assert completed_scopes(second) == [["119", "hr"]]


def test_missing_published_diff_invalidate_a_completed_archive_skip(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    diffs = pq.read_table(first["section_diffs"])
    pq.write_table(diffs.slice(0, 0), first["section_diffs"])
    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")] and len(body.requested) == 2
    assert pq.read_table(second["section_diffs"]).num_rows == 1


def test_legacy_archive_stamp_without_completion_evidence_is_rechecked(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    path = first[build.ARCHIVES_TABLE]
    pq.write_table(pq.read_table(path).replace_schema_metadata(None), path)
    _, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")]
    assert body.requested == [], "the verified body/diff rows still qualify a completed bill skip"


def test_refused_archive_member_is_not_remembered_as_complete(tmp_path, scoped):
    class RefusedMember(StubBulkAcquirer):
        def acquire(self, *args, **kwargs):
            result = super().acquire(*args, **kwargs)
            result.archive.refused_count = 1
            return result

    first, _, _ = run(tmp_path / "first", bulk=RefusedMember())
    assert completed_scopes(first) == []
    _, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")]


def test_failed_held_neighbour_refresh_preserves_its_complete_row(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first", budget=1)
    held = acquired(first)["introduced-in-house"]

    class FailedNeighbour(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            if package_id.endswith("ih"):
                self.requested.append(package_id)
                raise ValueError("temporary body refusal")
            return super().acquire(package_id, **kwargs)

    second, _, body = run(tmp_path / "second", prior=tmp_path / "first", budget=2, body=FailedNeighbour())
    assert body.requested == ["BILLS-119hr6028eh", "BILLS-119hr6028ih"]
    assert acquired(second)["introduced-in-house"] == held
    assert completed_scopes(second) == []


def test_missing_middle_body_does_not_create_a_nonconsecutive_diff(scoped):
    status = parse_bill_status((FIXTURES / "status-119hr6028.xml").read_bytes(), identity=IDENTITY)
    introduced, engrossed = build._ordered_printings(status)
    enrolled = replace(engrossed, type="Enrolled Bill", date="2026-09-20", package_id="BILLS-119hr6028enr")
    status = replace(status, text_versions=(introduced, engrossed, enrolled))

    class SyntheticThird(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            return super().acquire(package_id.replace("enr", "eh"), **kwargs)

    entries = build._version_captures(status, SyntheticThird(), [2], held={"engrossed-in-house"})
    assert len(entries) == 3
    middle = next(entry for entry in entries if entry.version_code == "engrossed-in-house")
    assert middle.body is None and middle.source == "congress"
    tables = build.build_family(
        build.BillFamilyCapture(status=status, versions=tuple(entries)), engine=build.engine_stamp()
    )
    assert tables.section_diffs == (), "the held middle placeholder must prevent an introduced-to-enrolled shortcut"


def test_credential_refusal_preserves_every_existing_output(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first", budget=1)
    retry = tmp_path / "retry"
    retry.mkdir()
    for path in first.values():
        (retry / path.name).write_bytes(path.read_bytes())
    before = {path.name: path.read_bytes() for path in retry.glob("*.parquet")}

    class Denied(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            raise CredentialRefusedError("denied")

    with pytest.raises(CredentialRefusedError):
        build.build_bill_family(
            retry,
            bulk_acquirer=StubBulkAcquirer(),
            body_acquirer=Denied(),
            download_prior=_prior_from(tmp_path / "first"),
        )
    assert {name: (retry / name).read_bytes() for name in before} == before


@pytest.mark.parametrize("budget", [-1, 1.5, True, "600"])
def test_invalid_budget_refuses_before_acquisition_or_output(tmp_path, budget):
    bulk = StubBulkAcquirer()
    with pytest.raises(ValueError, match="nonnegative integer"):
        build.build_bill_family(tmp_path, max_version_fetches=budget, bulk_acquirer=bulk, download_prior=_no_prior)
    assert bulk.calls == [] and list(tmp_path.iterdir()) == []


def test_zero_budget_is_metadata_only_and_keeps_body_work_retryable(tmp_path, scoped):
    first, _, body = run(tmp_path / "first", budget=0)
    assert body.requested == []
    assert pq.read_table(first["congress_bills"]).num_rows == 1
    assert not acquired(first) and completed_scopes(first) == []
    _, _, later = run(tmp_path / "second", prior=tmp_path / "first", budget=2)
    assert len(later.requested) == 2


@pytest.mark.parametrize(("raw", "expected"), [(None, 600), ("", 600), ("0", 0), ("17", 17)])
def test_rollup_forwards_explicit_budget(monkeypatch, tmp_path, raw, expected):
    if raw is None:
        monkeypatch.delenv("BILL_FAMILY_MAX_VERSION_FETCHES", raising=False)
    else:
        monkeypatch.setenv("BILL_FAMILY_MAX_VERSION_FETCHES", raw)
    calls = []
    monkeypatch.setattr(rollup, "build_bill_family", lambda output_dir, **kwargs: calls.append(kwargs) or ())
    assert rollup.BillFamilyRollup().build(tmp_path) == ()
    assert calls == [{"max_version_fetches": expected}]


@pytest.mark.parametrize("raw", ["-1", "1.0", "unlimited"])
def test_rollup_refuses_invalid_budget(monkeypatch, tmp_path, raw):
    monkeypatch.setenv("BILL_FAMILY_MAX_VERSION_FETCHES", raw)
    with pytest.raises(ValueError, match="nonnegative integer"):
        rollup.BillFamilyRollup().build(tmp_path)


def test_dispatch_forwards_zero_as_a_string_without_unlimited_semantics():
    root = Path(__file__).resolve().parents[1]
    dedicated = yaml.safe_load((root / ".github/workflows/rollup-bill-family.yml").read_text())
    shared = yaml.safe_load((root / ".github/workflows/_rollup.yml").read_text())
    inputs = dedicated[True]["workflow_dispatch"]["inputs"]
    assert inputs["max_version_fetches"]["type"] == "string"
    assert inputs["max_version_fetches"]["default"] == "600"
    assert (
        dedicated["jobs"]["run"]["with"]["bill_family_max_version_fetches"]
        == "${{ inputs.max_version_fetches || '600' }}"
    )
    assert shared[True]["workflow_call"]["inputs"]["bill_family_max_version_fetches"]["default"] == "600"
    assert (
        "BILL_FAMILY_MAX_VERSION_FETCHES: ${{ inputs.bill_family_max_version_fetches }}"
        in (root / ".github/workflows/_rollup.yml").read_text()
    )
