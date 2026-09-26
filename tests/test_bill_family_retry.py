"""Bill-family retry contracts for build_bill_family and its rollup.

A capped or failed body stays pending by its own printing's state, so the next
run reads it without reading its BILLSTATUS again; a status folder is complete
once its bills are shaped, and skips only while its published evidence
verifies; an invalid budget is refused before any acquisition or output.
"""

import importlib
import json
from dataclasses import replace
from pathlib import Path

import pyarrow.parquet as pq
import pyarrow as pa
import pytest
import yaml

from spicy_docs.sources.congress.bill_status import parse_bill_status
from spicy_docs.transport.credentials import CredentialRefusedError
from tests.test_bill_family import (
    FIXTURES,
    IDENTITY,
    _Archive,
    _Member,
    StubBodyAcquirer,
    StubBulkAcquirer,
    StubChangedBodyAcquirer,
    StubPdfBodyAcquirer,
    _no_prior,
    _pdf_only_status,
    _prior_from,
    scoped as fixture_scope,
)

scoped = fixture_scope

build = importlib.import_module("spicy_regs.transforms.build_bill_family")
bodies = importlib.import_module("spicy_regs.transforms.bill_family_bodies")
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


class SenateToo(StubBulkAcquirer):
    """Also serves the fixture bill as 119 S 6028, so a second folder has body work to leave unfinished."""

    def acquire(self, congress, bill_type, **kwargs):
        result = super().acquire(congress, bill_type, **kwargs)
        if (congress, bill_type) == (119, "s") and result.archive is not None:
            body = (FIXTURES / "status-119hr6028.xml").read_bytes()
            body = body.replace(b"<type>HR</type>", b"<type>S</type>").replace(b"119hr6028", b"119s6028")
            result.archive = _Archive([_Member(parse_bill_status(body, identity=replace(IDENTITY, bill_type="s")))])
        return result


def archive_scopes(paths):
    return {
        (row["congress"], row["bill_type"], row["name"])
        for row in pq.read_table(paths[build.ARCHIVES_TABLE]).to_pylist()
    }


def test_a_capped_body_pass_leaves_status_complete_and_resumes_by_printing(tmp_path, scoped, monkeypatch):
    """Run 35946820267 left every folder unfinished over the cap; a body is no longer a folder's to finish."""
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr,s")
    first, _, body = run(tmp_path / "first", budget=1, bulk=SenateToo())
    assert body.requested == ["BILLS-119hr6028ih"], "the cap is hit on the first folder's first printing"
    assert archive_scopes(first) == {("119", "hr", "BILLSTATUS-119-hr.zip"), ("119", "s", "BILLSTATUS-119-s.zip")}
    assert completed_scopes(first) == [["119", "hr"], ["119", "s"]], "both folders' status is shaped"
    _, bulk, body = run(tmp_path / "second", prior=tmp_path / "first", budget=1, bulk=SenateToo())
    assert bulk.zip_downloads == [], "no BILLSTATUS zip is read again to reach a body"
    assert body.requested == ["BILLS-119hr6028eh"], "the pending printing, by its own state"


def test_a_folder_is_complete_whatever_its_bodies(tmp_path, scoped, monkeypatch):
    monkeypatch.setenv("BILL_FAMILY_BILL_TYPES", "hr,s")
    first, _, body = run(tmp_path / "first", budget=2, bulk=SenateToo())
    assert body.requested == ["BILLS-119hr6028ih", "BILLS-119hr6028eh"]
    assert completed_scopes(first) == [["119", "hr"], ["119", "s"]], "119 S 6028's refused bodies do not reopen it"
    _, bulk, _ = run(tmp_path / "second", prior=tmp_path / "first", budget=0, bulk=SenateToo())
    assert bulk.zip_downloads == []


def test_one_body_caps_resume_then_retry_pending_pair_without_metadata_change(tmp_path, scoped):
    first, _, first_body = run(tmp_path / "first", budget=1)
    assert first_body.requested == ["BILLS-119hr6028ih"]
    assert len(acquired(first)) == 1 and completed_scopes(first) == [["119", "hr"]]
    held = acquired(first)["introduced-in-house"]

    second, second_bulk, second_body = run(tmp_path / "second", prior=tmp_path / "first", budget=1)
    assert second_bulk.zip_downloads == []
    assert second_body.requested == ["BILLS-119hr6028eh"], "missing body must precede its held neighbour"
    assert len(acquired(second)) == 2
    assert acquired(second)["introduced-in-house"] == held
    assert pq.read_table(second["section_diffs"]).num_rows == 0, "the comparison waits for both documents"

    third, _, third_body = run(tmp_path / "third", prior=tmp_path / "second", budget=2)
    assert set(third_body.requested) == {"BILLS-119hr6028ih", "BILLS-119hr6028eh"}
    assert pq.read_table(third["section_diffs"]).num_rows == 1
    assert acquired(third) == acquired(second), "both sides were read only to be compared"

    fourth, fourth_bulk, fourth_body = run(tmp_path / "fourth", prior=tmp_path / "third", budget=1)
    assert fourth_bulk.zip_downloads == [] and fourth_body.requested == []
    assert acquired(fourth) == acquired(third)


def test_failed_xml_parse_is_not_a_completed_govinfo_body(tmp_path, scoped, monkeypatch):
    with monkeypatch.context() as failed:
        failed.setattr(
            bodies, "parse_bill_tree", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad XML"))
        )
        first, _, _ = run(tmp_path / "first")
    assert all(row["sha256"] and row["section_count"] is None for row in acquired(first).values())
    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [] and len(body.requested) == 2
    assert all(row["section_count"] for row in acquired(second).values())


def test_missing_published_sections_make_their_printings_pending_again(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    sections = pq.read_table(first["bill_sections"])
    pq.write_table(sections.slice(0, 0), first["bill_sections"])
    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [] and len(body.requested) == 2
    assert pq.read_table(second["bill_sections"]).num_rows == sections.num_rows


def test_failed_pdf_cleanup_retries_by_printing_state(tmp_path, scoped, monkeypatch):
    with monkeypatch.context() as failed:
        failed.setattr(bodies, "body_text", lambda *_args, **_kwargs: (_ for _ in ()).throw(ValueError("bad PDF")))
        first, _, _ = run(
            tmp_path / "first", bulk=StubBulkAcquirer(status=_pdf_only_status()), body=StubPdfBodyAcquirer()
        )
    assert all(row["sha256"] and row["cleanup_json"] is None for row in acquired(first).values())
    second, _, body = run(
        tmp_path / "second",
        prior=tmp_path / "first",
        bulk=StubBulkAcquirer(status=_pdf_only_status()),
        body=StubPdfBodyAcquirer(),
    )
    assert len(body.requested) == 2
    assert all(row["cleanup_json"] for row in acquired(second).values())


def test_missing_published_diff_is_computed_again(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    diffs = pq.read_table(first["section_diffs"])
    pq.write_table(diffs.slice(0, 0), first["section_diffs"])
    second, bulk, body = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [] and len(body.requested) == 2
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
    assert pq.read_table(second["section_diffs"]).num_rows == 0, "the comparison stays pending"


def test_missing_middle_body_does_not_create_a_nonconsecutive_diff(scoped):
    """A held middle printing that cannot be read again stays in the order, so its neighbours are not paired."""
    status = parse_bill_status((FIXTURES / "status-119hr6028.xml").read_bytes(), identity=IDENTITY)
    introduced, engrossed = build._ordered_printings(status)
    enrolled = replace(engrossed, type="Enrolled Bill", date="2026-09-20", package_id="BILLS-119hr6028enr")

    def row(version, code, source):
        return {
            "bill_id": "119-hr-6028",
            "version_code": code,
            "source": source,
            "label": version.type,
            "version_date": version.date,
            "package_id": version.package_id,
            "offered_formats_json": json.dumps(
                [{"url": item.url, "type": item.type, "package_id": item.package_id} for item in engrossed.formats]
            ),
            "sha256": "sha256:held" if source == "govinfo" else None,
        }

    class SyntheticThird(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            if package_id.endswith("eh"):
                self.requested.append(package_id)
                raise ValueError("the held middle cannot be read again")
            return super().acquire(package_id.replace("enr", "eh"), **kwargs)

    rows = [
        row(introduced, "introduced-in-house", "congress"),
        row(engrossed, "engrossed-in-house", "govinfo"),
        row(enrolled, "enrolled-bill", "congress"),
    ]
    held = {"engrossed-in-house"}
    work, _, _ = bodies.plan_work(
        rows, held=lambda bill: held, xml=lambda bill: held, complete_pairs=(), published_pairs={}
    )
    acquirer = SyntheticThird()
    outcome = bodies.BodyPass(
        bills_source=None,
        body_source=acquirer,
        remaining=[5],
        engine=build.engine_stamp(),
        classify=None,
        summarize_diff=None,
        refusals={},
    ).run(work)
    assert len(acquirer.requested) == 3, "both new printings, and the held middle for their comparisons"
    [tables] = outcome.families
    assert {row["version_code"] for row in tables.bill_versions} == {"introduced-in-house", "enrolled-bill"}
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
    assert not acquired(first) and completed_scopes(first) == [["119", "hr"]]
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
    assert calls == [{"max_version_fetches": expected, "evidence": None}]


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


@pytest.mark.parametrize("missing", ["all", "one", "schema"])
def test_missing_source_versions_reopen_completed_archive(tmp_path, scoped, missing):
    first, _, _ = run(tmp_path / "first")
    versions = pq.read_table(first["bill_versions"])
    damaged = versions.slice(0, 0) if missing == "all" else versions.slice(0, 1)
    if missing == "schema":
        damaged = versions.drop(["sha256"])
    pq.write_table(damaged, first["bill_versions"])

    second, bulk, bodies = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")]
    assert bodies.requested
    assert set(acquired(second)) == {"introduced-in-house", "engrossed-in-house"}
    assert completed_scopes(second) == [["119", "hr"]]
    _, next_bulk, next_bodies = run(tmp_path / "third", prior=tmp_path / "second")
    assert next_bulk.zip_downloads == [] and next_bodies.requested == []


def test_uploaded_version_does_not_conceal_missing_source_version(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    versions = pq.read_table(first["bill_versions"])
    rows = versions.to_pylist()
    rows[0]["source"] = "upload"
    pq.write_table(pa.Table.from_pylist(rows, schema=versions.schema), first["bill_versions"])
    second, bulk, bodies = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")] and bodies.requested
    assert set(acquired(second)) == {"introduced-in-house", "engrossed-in-house"}
    assert any(row["source"] == "upload" for row in pq.read_table(second["bill_versions"]).to_pylist())


@pytest.mark.parametrize("missing", ["all", "one", "file", "schema"])
def test_missing_diff_children_reopen_completed_archive(tmp_path, scoped, missing):
    first, _, _ = run(tmp_path / "first", body=StubChangedBodyAcquirer())
    items = pq.read_table(first["section_diff_items"])
    assert items.num_rows > 1
    if missing == "file":
        first["section_diff_items"].unlink()
    else:
        damaged = items.slice(0, 0) if missing == "all" else items.slice(0, 1)
        if missing == "schema":
            damaged = items.drop(["seq"])
        pq.write_table(damaged, first["section_diff_items"])
    second, bulk, bodies = run(tmp_path / "second", prior=tmp_path / "first", body=StubChangedBodyAcquirer())
    assert bulk.zip_downloads == [] and len(bodies.requested) == 2
    assert pq.read_table(second["section_diff_items"]).to_pylist() == items.to_pylist()
    assert completed_scopes(second) == [["119", "hr"]]
    _, next_bulk, next_bodies = run(tmp_path / "third", prior=tmp_path / "second")
    assert next_bulk.zip_downloads == [] and next_bodies.requested == []


def test_missing_source_version_count_is_requalified_from_status(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first")
    bills = pq.read_table(first["congress_bills"])
    pq.write_table(bills.drop(["version_count"]), first["congress_bills"])
    second, bulk, bodies = run(tmp_path / "second", prior=tmp_path / "first")
    assert bulk.zip_downloads == [(119, "hr")] and bodies.requested == []
    assert pq.read_table(second["congress_bills"])["version_count"].to_pylist() == ["2"]
    _, next_bulk, _ = run(tmp_path / "third", prior=tmp_path / "second")
    assert next_bulk.zip_downloads == []


def test_corrected_retry_replaces_removed_sections_and_diff_items_only_in_successful_scopes(tmp_path, scoped):
    first, _, _ = run(tmp_path / "first", body=StubChangedBodyAcquirer())
    preserved = {}
    for name in ("bill_sections", "section_diffs", "section_diff_items"):
        table = pq.read_table(first[name])
        rows = table.to_pylist()
        preserved[name] = [{**row, "bill_id": "118-hr-99"} for row in rows]
        pq.write_table(pa.Table.from_pylist(rows + preserved[name], schema=table.schema), first[name])

    # A missing child makes the existing XML pair retryable. The source now
    # returns the original short bill, which has fewer sections and diff items.
    items = pq.read_table(first["section_diff_items"])
    rows = [row for row in items.to_pylist() if row["bill_id"] == "118-hr-99" or row["seq"] != "0"]
    pq.write_table(pa.Table.from_pylist(rows, schema=items.schema), first["section_diff_items"])
    second, _, bodies = run(tmp_path / "second", prior=tmp_path / "first")
    assert len(bodies.requested) == 2
    for name in preserved:
        rows = pq.read_table(second[name]).to_pylist()
        assert [row for row in rows if row["bill_id"] == "118-hr-99"] == preserved[name]
    sections = [row for row in pq.read_table(second["bill_sections"]).to_pylist() if row["bill_id"] == "119-hr-6028"]
    # Each original printing has a masthead, enacting clause and short-title node.
    assert len(sections) == 6 and all(row["heading"] != "Funding" for row in sections)
    items = [row for row in pq.read_table(second["section_diff_items"]).to_pylist() if row["bill_id"] == "119-hr-6028"]
    assert len(items) == 3 and all(row["heading"] != "Funding" for row in items)
    parent = next(row for row in pq.read_table(second["section_diffs"]).to_pylist() if row["bill_id"] == "119-hr-6028")
    assert parent["item_count"] == "3"
    assert completed_scopes(second) == [["119", "hr"]]
    _, next_bulk, next_bodies = run(tmp_path / "third", prior=tmp_path / "second")
    assert next_bulk.zip_downloads == [] and next_bodies.requested == []


@pytest.mark.parametrize("budget", [0, 2])
def test_failed_or_unattempted_retry_preserves_retained_child_scopes(tmp_path, scoped, budget):
    first, _, _ = run(tmp_path / "first", body=StubChangedBodyAcquirer())
    # Force a retry without destroying the retained body/section evidence.
    path = first["section_diffs"]
    parents = pq.read_table(path)
    pq.write_table(parents.slice(0, 0), path)
    before_sections = pq.read_table(first["bill_sections"]).to_pylist()
    before_items = pq.read_table(first["section_diff_items"]).to_pylist()

    class Refused(StubBodyAcquirer):
        def acquire(self, package_id, **kwargs):
            self.requested.append(package_id)
            raise ValueError("temporary source refusal")

    second, _, bodies = run(tmp_path / "second", prior=tmp_path / "first", body=Refused(), budget=budget)
    assert len(bodies.requested) == budget
    assert pq.read_table(second["bill_sections"]).to_pylist() == before_sections
    assert pq.read_table(second["section_diff_items"]).to_pylist() == before_items
    assert pq.read_table(second["section_diffs"]).num_rows == 0, "the comparison stays pending"
