"""Corrected report readers and named re-reads refresh unchanged sources and replace their blocks.

A report whose MODS names only its part 1 is held instead: refused, and its prior rows removed.
"""

import json
import shutil

from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.interpretation.cbo_estimates import CBO_ESTIMATE_RULE_VERSION
from spicy_docs.schemas.committee_report_tables import REPORT_SECTIONS
from spicy_docs.transport.captured import CapturedBodyResponse

from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms.build_committee_reports import build_committee_reports
from spicy_regs.transforms.committee_report_reads import READ_COLUMNS, READS_TABLE, RULE_VERSIONS, complete
from spicy_regs.transforms.table_merge import prior_scratch_path
from tests.test_committee_reports import (
    CHRG_ID,
    CRPT_ID,
    OBSERVED_AT,
    HearingBodyAcquirer,
    StubHearings,
    _no_prior,
    _package,
)


class NoDiscovery:
    """A discovery reader that lists nothing, so only retained evidence can trigger a read."""

    def packages(self, url, *, max_pages=40):
        return iter(())


class RetainedReport:
    """Serves the retained report body, optionally failing or returning an empty body."""

    def __init__(self, fail=False, empty=False):
        self.requested = []
        self.fail = fail
        self.empty = empty

    def acquire(self, package_id, *, max_bytes=None):
        self.requested.append(package_id)
        if self.fail:
            raise ConnectionError("retained source unavailable")
        assert package_id == CRPT_ID
        if self.empty:
            return _package(package_id, body=b"<html><body><pre></pre></body></html>")
        return _package(package_id)


def _write_prior(directory, name, columns, rows):
    """Write a prior published table where the merge and the reads checkpoint will find it."""
    schema = pa.schema([(column, pa.string()) for column in columns])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), prior_scratch_path(directory, name))


@pytest.mark.parametrize("fail,empty", [(False, False), (False, True), (True, False)])
def test_rule_change_refreshes_outside_window_and_replaces_only_successful_report(tmp_path, fail, empty):
    modified = "2026-09-18T12:00:00Z"
    _write_prior(
        tmp_path,
        "committee_reports",
        ("package_id", "last_modified"),
        [
            {"package_id": CRPT_ID, "last_modified": modified},
        ],
    )
    _write_prior(
        tmp_path,
        READS_TABLE,
        READ_COLUMNS,
        [
            {
                "package_id": CRPT_ID,
                "last_modified": modified,
                "outcome": "complete",
                "rule_version": CBO_ESTIMATE_RULE_VERSION,
                "observed_at": "2026-09-20",
            },
        ],
    )
    old = {
        "package_id": CRPT_ID,
        "seq": "99",
        "agency_label": "CONTENTS",
        "agency_key": "CONTENTS",
        "body": "obsolete block",
        "last_modified": modified,
    }
    unread = {"package_id": "CRPT-119hrpt2", "seq": "0", "body": "unread report"}
    _write_prior(tmp_path, "report_sections", REPORT_SECTIONS.columns, [old, unread])
    acquirer = RetainedReport(fail, empty)
    paths = build_committee_reports(
        tmp_path,
        reader=NoDiscovery(),
        acquirer=acquirer,
        hearings=StubHearings(),
        download_prior=_no_prior,
    )
    assert acquirer.requested == [CRPT_ID]
    files = {path.stem: path for path in paths}
    rows = pq.read_table(files["report_sections"]).to_pylist()
    assert next(row for row in rows if row["package_id"] == unread["package_id"])["body"] == "unread report"
    changed = [row for row in rows if row["package_id"] == CRPT_ID]
    [state] = pq.read_table(files[READS_TABLE]).to_pylist()
    if fail:
        assert changed[0]["agency_label"] == "CONTENTS"
        assert changed[0]["seq"] == "99"
        assert state["outcome"] == "refused"
    else:
        assert all(row["seq"] != "99" for row in changed)
        assert all(row["agency_label"] is None and row["agency_key"] is None for row in changed)
        if empty:
            assert changed == []
        else:
            assert any(row["heading"] == "DEPARTMENT OF THE TREASURY" for row in changed)
        assert state["outcome"] == "complete"
        assert state["rule_version"] == RULE_VERSIONS["CRPT"] != CBO_ESTIMATE_RULE_VERSION
        assert state["last_modified"] == modified
    # A successful current read skips; a failed read remains eligible even
    # with no publisher change and no listing in the discovery window.
    for name in ("committee_reports", "report_sections", READS_TABLE):
        shutil.copyfile(files[name], prior_scratch_path(tmp_path, name))
    again = RetainedReport()
    build_committee_reports(
        tmp_path,
        reader=NoDiscovery(),
        acquirer=again,
        hearings=StubHearings(),
        download_prior=_no_prior,
    )
    assert again.requested == ([CRPT_ID] if fail else [])


class NoListing:
    """A discovery reader a named re-read must never reach."""

    def packages(self, url, *, max_pages=40):
        raise AssertionError("a named re-read lists nothing")


HELD, REFUSED = "CHRG-119hhrg64431", "CRPT-119hrpt2"


def _reread_priors(directory, *, unchecked=()):
    """Two checkpointed hearings and a refused report; ``unchecked`` hearings have a row but no checkpoint."""
    hearings = [
        {"package_id": package_id, "last_modified": "2026-09-18T12:00:00Z", "observed_at": "2026-09-20T00:00:00Z"}
        for package_id in (CHRG_ID, HELD, *unchecked)
    ]
    _write_prior(directory, "hearing_transcripts", ("package_id", "last_modified", "observed_at"), hearings)
    reads = [
        {
            "package_id": package_id,
            "last_modified": "2026-09-18T12:00:00Z",
            "outcome": outcome,
            "rule_version": RULE_VERSIONS[package_id[:4]],
            "observed_at": "2026-09-20T00:00:00+00:00",
        }
        for package_id, outcome in ((CHRG_ID, "complete"), (HELD, "complete"), (REFUSED, "refused"))
    ]
    _write_prior(directory, READS_TABLE, READ_COLUMNS, reads)
    return hearings, reads


def _reread(directory, reread, *, acquirer=None, hearings=None):
    return build_committee_reports(
        directory,
        reader=NoListing(),
        acquirer=acquirer or HearingBodyAcquirer(),
        hearings=hearings or StubHearings(),
        download_prior=_no_prior,
        reread=reread,
    )


def test_a_named_reread_reads_only_those_packages_and_carries_every_other_checkpoint(tmp_path):
    """A complete checkpoint is forced pending by name; a refused one not named stays unread."""
    prior_hearings, prior_reads = _reread_priors(tmp_path)
    acquirer = HearingBodyAcquirer()
    files = {path.stem: path for path in _reread(tmp_path, [CHRG_ID], acquirer=acquirer)}
    assert acquirer.requested == [CHRG_ID]
    reads = {row["package_id"]: row for row in pq.read_table(files[READS_TABLE]).to_pylist()}
    assert [reads[row["package_id"]] for row in prior_reads[1:]] == prior_reads[1:]
    assert reads[CHRG_ID]["outcome"] == "complete"
    assert reads[CHRG_ID]["observed_at"] != prior_reads[0]["observed_at"]
    hearings = {row["package_id"]: row for row in pq.read_table(files["hearing_transcripts"]).to_pylist()}
    assert hearings[HELD]["observed_at"] == prior_hearings[1]["observed_at"]
    assert hearings[CHRG_ID]["observed_at"] == OBSERVED_AT, "the re-read row carries its new capture time"


#: (named ids, whether priors exist, a prior hearing without a checkpoint, the refusal)
UNSAFE_REREADS = {
    "a report would move the discovery watermark": ([REFUSED], True, (), "hearings only"),
    "an id outside both collections": (["PLAW-119publ1"], True, (), "hearings only"),
    "a typo'd hearing": (["CHRG-119hhrg99999"], True, (), "no prior checkpoint"),
    "no prior at all": ([CHRG_ID], False, (), "no prior checkpoint"),
    "a prior row without a checkpoint": ([CHRG_ID], True, ("CHRG-119hhrg63968",), "run discovery first"),
}


@pytest.mark.parametrize(("reread", "priors", "unchecked", "refusal"), UNSAFE_REREADS.values(), ids=UNSAFE_REREADS)
def test_an_unsafe_named_reread_is_refused_before_any_source_request(tmp_path, reread, priors, unchecked, refusal):
    if priors:
        _reread_priors(tmp_path, unchecked=unchecked)
    acquirer, hearings = HearingBodyAcquirer(), StubHearings()
    with pytest.raises(ValueError, match=refusal):
        _reread(tmp_path, reread, acquirer=acquirer, hearings=hearings)
    assert acquirer.requested == [] and hearings.requested == []


class RefusingAcquirer(HearingBodyAcquirer):
    def acquire(self, package_id, *, max_bytes=None):
        self.requested.append(package_id)
        raise ConnectionError("stub: body unavailable")


@pytest.mark.parametrize(
    ("acquirer", "hearings"),
    [(RefusingAcquirer(), StubHearings()), (HearingBodyAcquirer(), StubHearings(details={}))],
    ids=["the body is refused", "the hearing detail is refused"],
)
def test_a_named_read_that_does_not_complete_fails_the_run_before_any_merge(tmp_path, acquirer, hearings):
    """A refused body would publish a retried-forever checkpoint; a refused detail would drop the prior event_id."""
    _reread_priors(tmp_path)
    with pytest.raises(RuntimeError, match="did not complete"):
        _reread(tmp_path, [CHRG_ID], acquirer=acquirer, hearings=hearings)
    assert acquirer.requested == [CHRG_ID]
    assert not (tmp_path / "hearing_transcripts.parquet").exists()
    assert not (tmp_path / f"{READS_TABLE}.parquet").exists()


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(None, []), ("", []), (" CHRG-1, ,CHRG-2,", ["CHRG-1", "CHRG-2"])],
    ids=["unset", "blank", "list"],
)
def test_the_rollup_reads_its_reread_list_from_the_environment(tmp_path, monkeypatch, raw, expected):
    from spicy_regs.pipelines.rollups import committee_reports

    if raw is None:
        monkeypatch.delenv("COMMITTEE_REPORTS_REREAD", raising=False)
    else:
        monkeypatch.setenv("COMMITTEE_REPORTS_REREAD", raw)
    calls = []
    monkeypatch.setattr(committee_reports, "build_committee_reports", lambda *args, **kwargs: calls.append(kwargs))
    committee_reports.CommitteeReportsRollup().build(tmp_path)
    assert [call["reread"] for call in calls] == [expected]


#: Its MODS (``tests/fixtures/govinfo_bodies/mods-CRPT-119hrpt811.xml``, the
#: bytes refused on 2026-09-23) names only ``-pt1``, which spicy-docs 0.31.0
#: reads at that part's stem.
PART_ONLY = "CRPT-119hrpt811"
PART_ONLY_MODS = "sha256:aba0227068f60977bb1ee98d58add159064bb2b38e7b235e70b8673de7b3e125"


class PartOnlyDiscovery:
    """Lists the part-only report beside the ordinary one, as a retained listing page."""

    def packages(self, url, *, max_pages=40):
        ids = [PART_ONLY, CRPT_ID] if "/CRPT/" in url else []
        records = [{"packageId": key, "lastModified": "2026-09-18T12:00:00Z"} for key in ids]
        page = CapturedBodyResponse(
            requested_url=url,
            resolved_url=url,
            status_code=200,
            observed_at=OBSERVED_AT,
            content_type="application/json",
            body=json.dumps(records).encode(),
        )
        yield SimpleNamespace(records=records, capture=page)


class ReportsAcquirer:
    """Serves each report from its retained MODS; ``REFUSED`` fails the way an unavailable source does."""

    def __init__(self):
        self.requested = []

    def acquire(self, package_id, *, max_bytes=None):
        self.requested.append(package_id)
        if package_id == REFUSED:
            raise ConnectionError("retained source unavailable")
        return _package(package_id)


def _tables(paths):
    return {path.stem: pq.read_table(path).to_pylist() for path in paths}


def test_a_report_whose_mods_names_only_its_part_1_is_held_and_refused(tmp_path):
    evidence = CaptureEvidence(tmp_path, "committee-reports")
    tables = _tables(
        build_committee_reports(
            tmp_path,
            reader=PartOnlyDiscovery(),
            acquirer=ReportsAcquirer(),
            hearings=StubHearings(),
            download_prior=_no_prior,
            evidence=evidence,
        )
    )
    assert [row["package_id"] for row in tables["committee_reports"]] == [CRPT_ID]
    assert {row["package_id"] for row in tables["report_sections"]} == {CRPT_ID}
    reads = {row["package_id"]: row for row in tables[READS_TABLE]}
    assert reads[CRPT_ID]["outcome"] == "complete"
    assert reads[PART_ONLY]["outcome"] == "refused" and not complete(reads[PART_ONLY], "CRPT")
    events = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_bytes().splitlines()]
    [refusal] = [row for row in events if row["event"] == "refusal"]
    assert refusal["stage"] == PART_ONLY and refusal["error_type"] == "ReportPartHeld"
    assert f"{PART_ONLY}-pt1" in refusal["message"] and "decision 29" in refusal["message"]
    assert refusal["response"]["sha256"] == PART_ONLY_MODS, "the refusal retains the MODS that names the part"
    assert not any(row.get("stage") == PART_ONLY + ":used" for row in events)


def test_a_held_report_loses_its_prior_part_1_rows_and_a_refused_one_keeps_its_own(tmp_path):
    """The live state of 2026-09-24: ``CRPT-119hrpt811`` complete at 0.31.0 with Part 1's row and sections."""
    before_hold = "spicy-docs=0.31.0;cbo=cf790f0f814a;sections=report-headings-001"
    part_url = f"https://www.govinfo.gov/content/pkg/{PART_ONLY}/html/{PART_ONLY}-pt1.htm"
    _write_prior(
        tmp_path,
        "committee_reports",
        ("package_id", "last_modified", "requested_url", "observed_at"),
        [
            {"package_id": key, "last_modified": "2026-09-18T12:00:00Z", "requested_url": url, "observed_at": "prior"}
            for key, url in ((PART_ONLY, part_url), (CRPT_ID, None), (REFUSED, None))
        ],
    )
    _write_prior(
        tmp_path,
        "report_sections",
        REPORT_SECTIONS.columns,
        [{"package_id": key, "seq": "0", "body": "prior block"} for key in (PART_ONLY, CRPT_ID, REFUSED)],
    )
    _write_prior(
        tmp_path,
        READS_TABLE,
        READ_COLUMNS,
        [
            {
                "package_id": key,
                "last_modified": "2026-09-18T12:00:00Z",
                "outcome": outcome,
                "rule_version": before_hold,
                "observed_at": "2026-09-24T03:47:22+00:00",
            }
            for key, outcome in ((PART_ONLY, "complete"), (CRPT_ID, "complete"), (REFUSED, "refused"))
        ],
    )
    acquirer = ReportsAcquirer()
    paths = build_committee_reports(
        tmp_path, reader=NoDiscovery(), acquirer=acquirer, hearings=StubHearings(), download_prior=_no_prior
    )
    assert sorted(acquirer.requested) == sorted([PART_ONLY, CRPT_ID, REFUSED]), "the hold is a rule change"
    tables = _tables(paths)
    reports = {row["package_id"]: row for row in tables["committee_reports"]}
    assert set(reports) == {CRPT_ID, REFUSED}
    assert reports[CRPT_ID]["observed_at"] == OBSERVED_AT and reports[REFUSED]["observed_at"] == "prior"
    sections = tables["report_sections"]
    assert {row["package_id"] for row in sections} == {CRPT_ID, REFUSED}
    assert [row["body"] for row in sections if row["package_id"] == REFUSED] == ["prior block"]
    assert "prior block" not in [row["body"] for row in sections if row["package_id"] == CRPT_ID]
    reads = {row["package_id"]: row for row in tables[READS_TABLE]}
    assert {key: row["outcome"] for key, row in reads.items()} == {
        PART_ONLY: "refused",
        CRPT_ID: "complete",
        REFUSED: "refused",
    }
    # Retried as before, and still held.
    for name in ("committee_reports", "report_sections", READS_TABLE):
        shutil.copyfile(tmp_path / f"{name}.parquet", prior_scratch_path(tmp_path, name))
    again = ReportsAcquirer()
    tables = _tables(
        build_committee_reports(
            tmp_path, reader=NoDiscovery(), acquirer=again, hearings=StubHearings(), download_prior=_no_prior
        )
    )
    assert sorted(again.requested) == sorted([PART_ONLY, REFUSED])
    assert PART_ONLY not in {row["package_id"] for row in tables["committee_reports"]}
