"""Corrected report readers and named re-reads refresh unchanged sources and replace their blocks."""

import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.interpretation.cbo_estimates import CBO_ESTIMATE_RULE_VERSION
from spicy_docs.schemas.committee_report_tables import REPORT_SECTIONS

from spicy_regs.transforms.build_committee_reports import build_committee_reports
from spicy_regs.transforms.committee_report_reads import READ_COLUMNS, READS_TABLE, RULE_VERSIONS
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
