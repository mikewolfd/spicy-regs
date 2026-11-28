"""Wave 1 tests for committee-reports: ss-uep5 (CBO letters & CRPT reread) and ss-xzy2 (retained hearing bundle merge)."""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
from pathlib import Path
from unittest.mock import patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from spicy_docs.interpretation.cbo_estimates import CBO_ESTIMATE_RULE_VERSION
from spicy_docs.schemas.committee_report_tables import (
    COMMITTEE_REPORT_CBO_LETTERS,
    COMMITTEE_REPORTS,
    HEARING_TRANSCRIPTS,
    REPORT_SECTIONS,
)
from spicy_docs.schemas.hearing_bill_link_tables import HEARING_BILL_LINKS

from spicy_regs.generations import build_generation, verify_generation
from spicy_regs.govinfo_revisions import latest_package_rows
from spicy_regs import legislative_tables
from spicy_regs.navigation_read_outcomes import FILE_TABLES, READ_TABLES
from spicy_regs.pipelines.rollups import committee_reports as rollup_module
from spicy_regs.source_evidence import CaptureEvidence
from spicy_regs.transforms import build_committee_reports as workflow
from spicy_regs.transforms.build_committee_reports import (
    build_committee_reports,
    load_retained_hearing_bundle,
)
from spicy_regs.transforms.committee_report_reads import READ_COLUMNS, READS_TABLE, RULE_VERSIONS
from spicy_regs.transforms.table_merge import prior_scratch_path
from tests.test_committee_reports import CHRG_ID, NoBodies, StubHearings, _no_prior, _parts

CRPT_830 = "CRPT-119hrpt830"
CRPT_139 = "CRPT-119srpt139"
CRPT_758 = "CRPT-119hrpt758"
CRPT_HELD = "CRPT-119hrpt1"

WITHDRAWN_64154 = "CHRG-118hhrg64154"
WITHDRAWN_64598 = "CHRG-118hhrg64598"
WITHDRAWN_64599 = "CHRG-118hhrg64599"


def _admit_plain_generation(outputs: tuple[Path, ...], stage_dir: Path, gen_dir: Path, *, generation_id: str) -> None:
    """Convert raw committee-reports builder outputs to receiptless native Parquet and admit a generation."""
    stage_dir.mkdir(parents=True, exist_ok=True)
    mapped: dict[str, list[str]] = {}
    subjects: list[Path] = []
    source_outputs: list[tuple[str, Path]] = []
    for path in outputs:
        dataset = path.stem
        source_outputs.append((dataset, path))
        if dataset == READS_TABLE:
            mapped[dataset] = []
            outcome_name = READ_TABLES[dataset]
            outcome_target = stage_dir / f"{outcome_name}.parquet"
            legislative_tables.write_read_outcomes(dataset, path, outcome_target, generation_id=generation_id)
            mapped[outcome_name] = [outcome_target.name]
            subjects.append(outcome_target)
        else:
            target = stage_dir / path.name
            mapped[dataset] = legislative_tables.to_public(dataset, path, target)
            subjects.append(target)
    file_table = FILE_TABLES["committee-reports"]
    file_target = stage_dir / f"{file_table}.parquet"
    legislative_tables.write_file_outcomes(
        file_table, source_outputs, mapped, stage_dir, file_target, generation_id=generation_id
    )
    subjects.append(file_target)
    artifact = build_generation(
        gen_dir,
        family="committee-reports",
        files=subjects,
        expected_keys=list(rollup_module.CommitteeReportsRollup.outputs),
        schemas=rollup_module.CommitteeReportsRollup().generation_schemas(),
    )
    assert "etlReceipts" not in artifact.root["spec"]
    assert not (gen_dir / "etl_receipts.parquet").exists()
    verify_generation(gen_dir, expected_pin=artifact.pin)

# Exact GPO absence reason from retained CRPT-119hrpt758 (422 chars).
ABSENCE_758 = (
    "1. Cost of Legislation and the Congressional Budget Act. Pursuant\n"
    "to clause 3(c)(2) of House rule XIII and section 308(a) of the\n"
    "Congressional Budget Act of 1974, and pursuant to clause 3(c)(3) of\n"
    "House rule XIII and section 402 of the Congressional Budget Act of\n"
    "1974, the Committee has requested but not received from the Director of\n"
    "the Congressional Budget Office a budgetary analysis and a cost\n"
    "estimate of this bill."
)

HTML_830 = b"""<html><title>House Report 119-830</title><body><pre>
[House Report 119-830]
[From the U.S. Government Publishing Office]

                        [To accompany H.R. 5517]
      [Including cost estimate of the Congressional Budget Office]

  CONGRESSIONAL BUDGET OFFICE ESTIMATE, NEW BUDGET AUTHORITY,
          ENTITLEMENT AUTHORITY, AND TAX EXPENDITURES

    With respect to the requirements of clause 3(c)(2) of rule
XIII and section 308(a) of the Congressional Budget Act of 1974,
the Committee provides the following cost estimate.

    CBO estimates that implementing H.R. 5517 would cost less
than $500,000 over the 2026-2030 period.

                                         Phillip L. Swagel,
                             Director, Congressional Budget Office.

                       FEDERAL MANDATES STATEMENT

    The Committee adopts as its own the estimate of Federal mandates.
</pre></body></html>
"""

HTML_139 = b"""<html><title>Senate Report 119-139</title><body><pre>
[Senate Report 119-139]
[From the U.S. Government Publishing Office]

                        [To accompany S. 3257]
      [Including cost estimate of the Congressiona Budget Office]

                            Estimated Costs

    In accordance with paragraph 11(a) of rule XXVI of the
Standing Rules of the Senate and section 403 of the
Congressional Budget Act of 1974, the Committee provides the
following cost estimate.

    CBO estimates that implementing S. 3257 would have no
significant net effect on the federal deficit.

                                         Phillip L. Swagel,
                             Director, Congressional Budget Office.

                      Regulatory Impact Statement

    In accordance with paragraph 11(b) of rule XXVI, the Committee evaluates regulatory impact.
</pre></body></html>
"""

HTML_758 = (
    "<html><title>House Report 119-758</title><body><pre>\n"
    "[House Report 119-758]\n"
    "[From the U.S. Government Publishing Office]\n\n"
    "                        [To accompany H.R. 3276]\n\n"
    "                       COMMITTEE COST ESTIMATE\n\n"
    f"    {ABSENCE_758}\n"
    "</pre></body></html>\n"
).encode()


def _crpt_mods(package_id: str, *, bill_type: str, bill_number: str) -> bytes:
    return (
        '<mods xmlns="http://www.loc.gov/mods/v3"><extension>'
        f"<accessId>{package_id}</accessId><collectionCode>CRPT</collectionCode>"
        f'<bill congress="119" context="PRIMARY" number="{bill_number}" type="{bill_type}"/>'
        "</extension></mods>"
    ).encode()


def _sha256(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def _tables(paths) -> dict[str, list[dict]]:
    return {path.stem: pq.read_table(path).to_pylist() for path in paths}


def _prior_download(directory: Path):
    def download(key: str, target: Path) -> bool:
        source = directory / key
        if not source.is_file():
            return False
        shutil.copyfile(source, target)
        return True

    return download


class _NoListing:
    def packages(self, url: str, *, max_pages: int = 40):
        raise AssertionError(f"discovery must not run during reread or retained-bundle merge: {url}")


class _CboCaseAcquirer(NoBodies):
    def __init__(self, payloads: dict[str, tuple[bytes, bytes]], *, fail: set[str] | None = None):
        self.payloads = payloads
        self.fail = fail or set()
        self.requested: list[str] = []

    def acquire_parts(self, package_id: str, *, max_bytes=None, prefer=()):
        self.requested.append(package_id)
        if package_id in self.fail:
            raise ConnectionError(f"stub: refused {package_id}")
        mods_bytes, html_bytes = self.payloads[package_id]
        return _parts(package_id, mods_bytes=mods_bytes, body=html_bytes)


def _cbo_payloads() -> dict[str, tuple[bytes, bytes]]:
    return {
        CRPT_830: (_crpt_mods(CRPT_830, bill_type="HR", bill_number="5517"), HTML_830),
        CRPT_139: (_crpt_mods(CRPT_139, bill_type="S", bill_number="3257"), HTML_139),
        CRPT_758: (_crpt_mods(CRPT_758, bill_type="HR", bill_number="3276"), HTML_758),
    }


def _seed_crpt_priors(directory: Path, *, old_rule: str = "code=old000000000;cbo=f8b3cdc069a9") -> None:
    stamp = "2026-09-18T12:00:00Z"
    report_rows = [
        dict.fromkeys(COMMITTEE_REPORTS.columns)
        | {
            "package_id": pkg,
            "part_id": pkg,
            "collection": "CRPT",
            "congress": "119",
            "report_type": "srpt" if "srpt" in pkg else "hrpt",
            "report_number": pkg.rsplit("rpt", 1)[1],
            "chamber": "senate" if "srpt" in pkg else "house",
            "last_modified": stamp,
            "observed_at": "2026-10-01T00:00:00Z",
            "title": f"Prior {pkg}",
            "format": "htm",
            "media_type": "text/html",
            "requested_url": f"https://www.govinfo.gov/content/pkg/{pkg}/html/{pkg}.htm",
            "resolved_url": f"https://www.govinfo.gov/content/pkg/{pkg}/html/{pkg}.htm",
            "byte_size": "100",
            "sha256": _sha256(f"prior-{pkg}".encode()),
            "text_sha256": _sha256(f"prior-text-{pkg}".encode()),
            "report_states_estimate": "false",
            "cbo_letter_count": "0",
        }
        for pkg in (CRPT_830, CRPT_139, CRPT_758, CRPT_HELD)
    ]
    hearing_rows = [
        dict.fromkeys(HEARING_TRANSCRIPTS.columns)
        | {
            "package_id": CHRG_ID,
            "collection": "CHRG",
            "congress": "119",
            "hearing_type": "hhrg",
            "jacket_number": "63127",
            "chamber": "house",
            "last_modified": stamp,
            "observed_at": "2026-10-01T00:00:00Z",
            "title": "Prior held hearing",
            "format": "htm",
            "media_type": "text/html",
            "requested_url": f"https://www.govinfo.gov/content/pkg/{CHRG_ID}/html/{CHRG_ID}.htm",
            "resolved_url": f"https://www.govinfo.gov/content/pkg/{CHRG_ID}/html/{CHRG_ID}.htm",
            "byte_size": "100",
            "sha256": _sha256(b"prior-chrg"),
            "text_sha256": _sha256(b"Held hearing transcript body"),
            "body_text": "Held hearing transcript body",
            "body_completeness": "not_flagged",
            "text_derivation": "markup-reader",
            "mods_event_ids_json": "[]",
            "hearing_detail_json": json.dumps({"state": "missing", "relationship": "not-stated"}),
        }
    ]
    read_rows = [
        {
            "package_id": pkg,
            "last_modified": stamp,
            "outcome": "complete",
            "rule_version": RULE_VERSIONS["CRPT"] if pkg == CRPT_HELD else old_rule,
            "observed_at": "2026-10-01T00:00:00+00:00",
        }
        for pkg in (CRPT_830, CRPT_139, CRPT_758, CRPT_HELD)
    ] + [
        {
            "package_id": CHRG_ID,
            "last_modified": stamp,
            "outcome": "complete",
            "rule_version": RULE_VERSIONS["CHRG"],
            "observed_at": "2026-10-01T00:00:00+00:00",
        }
    ]
    for name, cols, rows in (
        ("committee_reports", COMMITTEE_REPORTS.columns, report_rows),
        ("report_sections", REPORT_SECTIONS.columns, []),
        ("committee_report_cbo_letters", COMMITTEE_REPORT_CBO_LETTERS.columns, []),
        ("hearing_transcripts", HEARING_TRANSCRIPTS.columns, hearing_rows),
        ("hearing_bill_links", HEARING_BILL_LINKS.columns, []),
        (READS_TABLE, READ_COLUMNS, read_rows),
    ):
        schema = pa.schema([(col, pa.string()) for col in cols])
        pq.write_table(pa.Table.from_pylist(rows, schema=schema), prior_scratch_path(directory, name))


# --------------------------------------------------------------------------- #
# ss-uep5: CBO rule version bump, targeted CRPT reread, and letter regressions
# --------------------------------------------------------------------------- #


def test_crpt_rule_version_automatically_tracks_new_cbo_estimate_rule():
    """committee_report_reads.RULE_VERSIONS['CRPT'] shifts automatically via CBO_ESTIMATE_RULE_VERSION."""
    assert CBO_ESTIMATE_RULE_VERSION == "7967093c912f"
    assert f"cbo={CBO_ESTIMATE_RULE_VERSION}" in RULE_VERSIONS["CRPT"]
    assert "cbo=f8b3cdc069a9" not in RULE_VERSIONS["CRPT"]
    assert "cbo=" not in RULE_VERSIONS["CHRG"]


def test_targeted_crpt_reread_extracts_830_139_and_preserves_758_absence_and_cursor(tmp_path, monkeypatch):
    """COMMITTEE_REPORTS_REREAD=CRPT-119hrpt830,CRPT-119srpt139,CRPT-119hrpt758 re-reads only those three reports."""
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    _seed_crpt_priors(tmp_path)

    evidence = CaptureEvidence(tmp_path / "ev", "committee-reports")
    inherited_cursor = "2026-11-26T03:40:00+00:00"
    monkeypatch.setattr(
        evidence,
        "inherited_event",
        lambda name, **match: {"event": "committee-discovery", "cursor": inherited_cursor}
        if name == "committee-discovery"
        else None,
    )

    acquirer = _CboCaseAcquirer(_cbo_payloads())
    coverage: dict = {}
    outputs = build_committee_reports(
        tmp_path,
        reader=_NoListing(),
        acquirer=acquirer,
        hearings=StubHearings(),
        download_prior=_no_prior,
        evidence=evidence,
        reread=(CRPT_830, CRPT_139, CRPT_758),
        coverage=coverage,
    )
    assert acquirer.requested == [CRPT_830, CRPT_139, CRPT_758]
    assert coverage["discovery"] == "not_requested"
    assert coverage["nothing_read"] == []

    tables = _tables(outputs)
    reports = {row["package_id"]: row for row in tables["committee_reports"]}
    assert set(reports) == {CRPT_830, CRPT_139, CRPT_758, CRPT_HELD}
    assert reports[CRPT_HELD]["title"] == f"Prior {CRPT_HELD}", "untouched prior CRPT stays intact"
    assert reports[CRPT_830]["report_states_estimate"] == "true"
    assert reports[CRPT_830]["cbo_letter_count"] == "1"
    assert reports[CRPT_139]["report_states_estimate"] == "true"
    assert reports[CRPT_139]["cbo_letter_count"] == "1"
    assert reports[CRPT_758]["report_states_estimate"] == "false"
    assert reports[CRPT_758]["cbo_letter_count"] == "0"
    assert reports[CRPT_758]["estimate_absence_reason"] == ABSENCE_758
    assert len(reports[CRPT_758]["estimate_absence_reason"]) == 422

    letters_by_pkg: dict[str, list[dict]] = {}
    for row in tables["committee_report_cbo_letters"]:
        letters_by_pkg.setdefault(row["package_id"], []).append(row)
    assert len(letters_by_pkg.get(CRPT_830, [])) == 1
    assert letters_by_pkg[CRPT_830][0]["estimate_heading_rule"] == "cbo_estimate_compound"
    assert letters_by_pkg[CRPT_830][0]["letter_signatory"] == "Phillip L. Swagel"
    assert len(letters_by_pkg.get(CRPT_139, [])) == 1
    assert letters_by_pkg[CRPT_139][0]["estimate_heading_rule"] == "estimated_costs"
    assert letters_by_pkg[CRPT_139][0]["letter_signatory"] == "Phillip L. Swagel"
    assert CRPT_758 not in letters_by_pkg

    reads = {row["package_id"]: row for row in tables[READS_TABLE]}
    for pkg in (CRPT_830, CRPT_139, CRPT_758):
        assert reads[pkg]["outcome"] == "complete"
        assert reads[pkg]["rule_version"] == RULE_VERSIONS["CRPT"]
    assert reads[CHRG_ID]["outcome"] == "complete"
    assert [row["package_id"] for row in tables["hearing_transcripts"]] == [CHRG_ID]

    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    [discovery_event] = [event for event in journal if event["event"] == "committee-discovery"]
    assert discovery_event["cursor"] == inherited_cursor
    assert discovery_event["mode"] == "reread"


def test_failed_named_crpt_reread_aborts_before_merging(tmp_path):
    """A failed named CRPT reread raises before writing any output tables."""
    _seed_crpt_priors(tmp_path)
    acquirer = _CboCaseAcquirer(_cbo_payloads(), fail={CRPT_139})
    with pytest.raises(RuntimeError, match="reread did not complete"):
        build_committee_reports(
            tmp_path,
            reader=_NoListing(),
            acquirer=acquirer,
            hearings=StubHearings(),
            download_prior=_no_prior,
            reread=(CRPT_830, CRPT_139),
        )
    assert not (tmp_path / "committee_reports.parquet").exists()
    assert not (tmp_path / f"{READS_TABLE}.parquet").exists()


def test_rollup_forwards_crpt_reread_and_retained_bundle_env_vars(tmp_path, monkeypatch):
    """CommitteeReportsRollup reads COMMITTEE_REPORTS_REREAD and COMMITTEE_REPORTS_RETAINED_BUNDLE*."""
    monkeypatch.setenv("COMMITTEE_REPORTS_REREAD", f"{CRPT_830},{CRPT_139},{CRPT_758}")
    monkeypatch.setenv("COMMITTEE_REPORTS_RETAINED_BUNDLE", str(tmp_path / "pins.json"))
    monkeypatch.setenv("COMMITTEE_REPORTS_RETAINED_BUNDLE_ROOT", str(tmp_path / "bundle"))
    monkeypatch.setenv("COMMITTEE_REPORTS_RETAINED_RAW_ARCHIVE", str(tmp_path / "raw"))
    calls: list[dict] = []
    monkeypatch.setattr(rollup_module, "build_committee_reports", lambda *args, **kwargs: calls.append(kwargs) or ())
    monkeypatch.setattr(
        rollup_module.CommitteeReportsRollup,
        "build_tables",
        lambda self, directory, builder, **kwargs: builder(directory, **kwargs),
    )
    rollup_module.CommitteeReportsRollup().build(tmp_path)
    assert len(calls) == 1
    assert calls[0]["reread"] == [CRPT_830, CRPT_139, CRPT_758]
    assert calls[0]["retained_bundle"] == tmp_path / "pins.json"
    assert calls[0]["retained_bundle_root"] == tmp_path / "bundle"
    assert calls[0]["retained_raw_archive"] == tmp_path / "raw"


def _local_crpt_originals() -> dict[str, tuple[Path, str, str]]:
    from spicy_docs.storage.raw_archive import default_raw_archive_root

    raw_root = default_raw_archive_root()
    return {
        CRPT_830: (
            raw_root / "objects/sha256/5d/5d903da3db098538f16931d4119bb239982c20ec775ddfb3e40ae906ce3742b2.gz",
            "HR",
            "5517",
        ),
        CRPT_139: (
            raw_root / "objects/sha256/9e/9ee28d8cc79cf20efca5c8f3b39c6bbca0501eb2ec2e96536a30d9dbb39d9483.gz",
            "S",
            "3257",
        ),
        CRPT_758: (
            raw_root / "objects/sha256/bf/bf716d85bc680819ae654c2cd3f0b4e1d2fd309dab1e0bb28f30b63722d052e6",
            "HR",
            "3276",
        ),
    }


_LOCAL_CRPT_ORIGINALS = _local_crpt_originals()


@pytest.mark.skipif(
    not all(path.is_file() for path, _, _ in _LOCAL_CRPT_ORIGINALS.values()),
    reason="local raw CRPT originals not present on runner",
)
def test_ss_uep5_real_retained_crpt_originals_through_reread(tmp_path):
    """Verify the three actual retained CRPT originals through build_committee_reports(..., reread=...)."""
    _seed_crpt_priors(tmp_path)
    payloads: dict[str, tuple[bytes, bytes]] = {}
    for pkg, (raw_path, bill_type, bill_number) in _LOCAL_CRPT_ORIGINALS.items():
        raw = raw_path.read_bytes()
        html = gzip.decompress(raw) if raw_path.suffix == ".gz" else raw
        payloads[pkg] = (_crpt_mods(pkg, bill_type=bill_type, bill_number=bill_number), html)

    outputs = build_committee_reports(
        tmp_path,
        reader=_NoListing(),
        acquirer=_CboCaseAcquirer(payloads),
        hearings=StubHearings(),
        download_prior=_no_prior,
        reread=(CRPT_830, CRPT_139, CRPT_758),
    )
    tables = _tables(outputs)
    reports = {row["package_id"]: row for row in tables["committee_reports"]}
    letters = {row["package_id"]: row for row in tables["committee_report_cbo_letters"]}
    assert reports[CRPT_830]["report_states_estimate"] == "true"
    assert reports[CRPT_830]["cbo_letter_count"] == "1"
    assert reports[CRPT_139]["report_states_estimate"] == "true"
    assert reports[CRPT_139]["cbo_letter_count"] == "1"
    assert reports[CRPT_758]["report_states_estimate"] == "false"
    assert reports[CRPT_758]["cbo_letter_count"] == "0"
    assert len(reports[CRPT_758]["estimate_absence_reason"]) == 422
    assert set(letters) == {CRPT_830, CRPT_139}
    assert letters[CRPT_830]["estimate_heading_rule"] == "cbo_estimate_compound"
    assert letters[CRPT_139]["estimate_heading_rule"] == "estimated_costs"


# --------------------------------------------------------------------------- #
# ss-xzy2: Audited offline retained-bundle merge for withdrawn 118th hearings
# --------------------------------------------------------------------------- #


def _synthetic_hearing_mods(package_id: str, title: str) -> bytes:
    return (
        '<mods xmlns="http://www.loc.gov/mods/v3">'
        f"<titleInfo><title>{title}</title></titleInfo>"
        "<location>"
        f'<url displayLabel="HTML rendition" access="raw object">https://www.govinfo.gov/content/pkg/{package_id}/html/{package_id}.htm</url>'
        "</location>"
        "<extension>"
        f"<accessId>{package_id}</accessId>"
        "<collectionCode>CHRG</collectionCode>"
        '<congCommittee authorityId="hsba00" chamber="H" congress="118">'
        '<name type="authority-standard">Committee on Financial Services</name>'
        "</congCommittee>"
        "</extension></mods>"
    ).encode("utf-8")


def _build_synthetic_retained_bundle(root: Path) -> Path:
    """Build a self-contained, cryptographically pinned multi-revision withdrawn-hearing bundle."""
    originals_dir = root / "originals"
    originals_dir.mkdir(parents=True)

    def store_blob(payload: bytes) -> tuple[str, int]:
        digest = _sha256(payload)
        (originals_dir / digest.removeprefix("sha256:")).write_bytes(payload)
        return digest, len(payload)

    body_64154 = b"<html><body><pre>SAFEGUARDING MAIN STREET hearing transcript text.</pre></body></html>"
    body_64598 = b"<html><body><pre>[ERRATA] It should read: ONE HUNDRED NINETEENTH CONGRESS (for 64154).</pre></body></html>"
    body_64599 = b"<html><body><pre>[ERRATA] It should read: ONE HUNDRED NINETEENTH CONGRESS (for 63968).</pre></body></html>"

    specs = [
        (WITHDRAWN_64154, "64154", "2026-10-06T19:26:55Z", "SAFEGUARDING MAIN STREET", body_64154, "ev-oct07", "2026-10-07T03:55:34.131830Z"),
        (WITHDRAWN_64154, "64154", "2026-10-08T20:29:52Z", "SAFEGUARDING MAIN STREET", body_64154, "ev-oct09", "2026-10-09T03:56:30.308592Z"),
        (WITHDRAWN_64598, "64598", "2026-10-06T17:13:05Z", "[ERRATA]", body_64598, "ev-oct07", "2026-10-07T03:55:42.468196Z"),
        (WITHDRAWN_64598, "64598", "2026-10-07T12:20:10Z", "[ERRATA]", body_64598, "ev-oct08", "2026-10-08T03:54:20.248232Z"),
        (WITHDRAWN_64598, "64598", "2026-10-08T14:17:49Z", "[ERRATA] SAFEGUARDING MAIN STREET", body_64598, "ev-oct09", "2026-10-09T03:56:35.482133Z"),
        (WITHDRAWN_64599, "64599", "2026-10-06T17:13:39Z", "\u00a0", body_64599, "ev-oct07", "2026-10-07T03:55:40.865324Z"),
        (WITHDRAWN_64599, "64599", "2026-10-07T12:17:45Z", "\u00a0", body_64599, "ev-oct08", "2026-10-08T03:54:21.064446Z"),
        (WITHDRAWN_64599, "64599", "2026-10-08T14:24:40Z", "[ERRATA] THE ROLE OF SELF-REGULATORY ORGANIZATIONS", body_64599, "ev-oct09", "2026-10-09T03:56:34.676564Z"),
    ]

    wrapper_lines: list[dict] = []
    selected_rows: list[dict] = []

    def make_capture_event(url: str, content_type: str, digest: str, size: int, observed_at: str, stage: str) -> dict:
        return {
            "blob_member": "blobs/sha256/" + digest.removeprefix("sha256:"),
            "body_retained": True,
            "byte_size": size,
            "content_encoding": "identity",
            "content_type": content_type,
            "event": "capture",
            "method": "GET",
            "observed_at": observed_at,
            "requested_url": url,
            "resolved_url": url,
            "response_complete": True,
            "sha256": digest,
            "stage": stage,
            "status_code": 200,
        }

    for pkg, jacket, modified, title, body_bytes, ev_id, obs_at in specs:
        summary_url = f"https://api.govinfo.gov/packages/{pkg}/summary"
        mods_url = f"https://api.govinfo.gov/packages/{pkg}/mods"
        body_url = f"https://www.govinfo.gov/content/pkg/{pkg}/html/{pkg}.htm"
        detail_url = f"https://api.congress.gov/v3/hearing/118/house/{jacket}?format=json&limit=1"

        summary_bytes = json.dumps(
            {
                "packageId": pkg,
                "collectionCode": "CHRG",
                "title": title,
                "dateIssued": "2026-09-22",
                "lastModified": modified,
                "download": {"txtLink": body_url},
            },
            ensure_ascii=False,
        ).encode("utf-8")
        mods_bytes = _synthetic_hearing_mods(pkg, title)
        detail_bytes = json.dumps(
            {
                "hearing": {
                    "congress": 118,
                    "chamber": "House",
                    "jacketNumber": int(jacket),
                    "title": title,
                    "updateDate": modified,
                }
            },
            ensure_ascii=False,
        ).encode("utf-8")

        cap_events = []
        for url, ctype, raw, stage in (
            (summary_url, "application/json", summary_bytes, "govinfo-response"),
            (mods_url, "application/xml", mods_bytes, "govinfo-response"),
            (body_url, "text/html", body_bytes, "govinfo-response"),
        ):
            digest, size = store_blob(raw)
            ev = make_capture_event(url, ctype, digest, size, obs_at, stage)
            wrapper_lines.append({"event": ev, "evidence": ev_id, "generation": f"gen-{ev_id}"})
            cap_events.append(ev)

        detail_digest, detail_size = store_blob(detail_bytes)
        detail_ev = make_capture_event(detail_url, "application/json", detail_digest, detail_size, obs_at, f"{pkg}:hearing-detail")
        detail_wrapper = {"event": detail_ev, "evidence": ev_id, "generation": f"gen-{ev_id}"}
        wrapper_lines.append(detail_wrapper)
        detail_line = len(wrapper_lines)

        selected_rows.append(
            {
                "package_id": pkg,
                "last_modified": modified,
                "source_evidence_artifact": ev_id,
                "original_source_captures": cap_events,
                "detail_original_event_reference": {
                    "source_file": {},  # filled once wrapper file is hashed
                    "wrapper_line": detail_line,
                    "wrapper": detail_wrapper,
                },
                "detail_relationship_state": "missing",
            }
        )

    wrapper_path = root / "evidence-events.jsonl"
    wrapper_bytes = ("\n".join(json.dumps(item, ensure_ascii=False) for item in wrapper_lines) + "\n").encode("utf-8")
    wrapper_path.write_bytes(wrapper_bytes)
    wrapper_pin = {"path": str(wrapper_path), "sha256": _sha256(wrapper_bytes), "bytes": len(wrapper_bytes)}
    for row in selected_rows:
        row["detail_original_event_reference"]["source_file"] = wrapper_pin

    pins_path = root / "selected-capture-pins.json"
    pins_path.write_text(
        json.dumps(
            {
                "status": "test-retained-bundle",
                "retained_bundle_metadata": {"evidence-events.jsonl": wrapper_pin},
                "selected": selected_rows,
            },
            indent=2,
            ensure_ascii=False,
        )
        + "\n"
    )
    return pins_path


def test_retained_bundle_merges_withdrawn_hearings_onto_prior_crpt_generation_offline(tmp_path, monkeypatch):
    """End-to-end: merge 8 withdrawn 118th hearing revisions onto a prior containing CRPT-119hrpt830/139/758."""
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    monkeypatch.delenv("DATA_GOV_API_KEY", raising=False)
    monkeypatch.delenv("API_GOV", raising=False)
    monkeypatch.delenv("CONGRESS_API_KEY", raising=False)
    monkeypatch.delenv("GOVINFO_API_KEY", raising=False)

    # Stage 1: Build a prior generation containing ss-uep5 CRPT-119hrpt830/139/758 + held CHRG.
    prior_dir = tmp_path / "prior_crpt"
    prior_dir.mkdir()
    _seed_crpt_priors(prior_dir)
    build_committee_reports(
        prior_dir,
        reader=_NoListing(),
        acquirer=_CboCaseAcquirer(_cbo_payloads()),
        hearings=StubHearings(),
        download_prior=_no_prior,
        reread=(CRPT_830, CRPT_139, CRPT_758),
    )

    # Stage 2: Build self-contained retained bundle (moved to a separate root to verify portable bundle_root override).
    bundle_dir = tmp_path / "bundle_src"
    _build_synthetic_retained_bundle(bundle_dir)
    relocated = tmp_path / "bundle_relocated"
    shutil.copytree(bundle_dir, relocated)
    # Remove the original directory so resolution MUST use retained_bundle_root, never the stale path inside JSON.
    shutil.rmtree(bundle_dir)
    relocated_pins = relocated / "selected-capture-pins.json"

    # Stage 3: Merge offline (no API key, no discovery, body_text runs once per unique body blob).
    merged_dir = tmp_path / "merged"
    merged_dir.mkdir()
    evidence = CaptureEvidence(tmp_path / "ev_merge", "committee-reports")
    inherited_cursor = "2026-11-27T03:40:00+00:00"
    monkeypatch.setattr(
        evidence,
        "inherited_event",
        lambda name, **match: {"event": "committee-discovery", "cursor": inherited_cursor}
        if name == "committee-discovery"
        else None,
    )

    extract_calls: list[str] = []
    real_body_text = workflow.body_text

    def counting_body_text(body):
        extract_calls.append(body.body_capture.sha256)
        return real_body_text(body)

    coverage: dict = {}
    with patch.object(workflow, "body_text", side_effect=counting_body_text):
        outputs = build_committee_reports(
            merged_dir,
            download_prior=_prior_download(prior_dir),
            evidence=evidence,
            coverage=coverage,
            retained_bundle=relocated_pins,
            retained_bundle_root=relocated,
        )

    assert len(extract_calls) == 3, "identical body blobs across revisions are extracted once per unique body"
    assert coverage["discovery"] == "retained_bundle"
    assert coverage["nothing_read"] == []
    assert coverage["pending"] == 0

    tables = _tables(outputs)
    # Prior CRPT-119hrpt830, 139, 758, and held CRPT-119hrpt1 survive untouched.
    reports = {row["package_id"]: row for row in tables["committee_reports"]}
    assert set(reports) == {CRPT_830, CRPT_139, CRPT_758, CRPT_HELD}
    assert reports[CRPT_830]["report_states_estimate"] == "true"
    assert reports[CRPT_139]["report_states_estimate"] == "true"
    assert len(reports[CRPT_758]["estimate_absence_reason"]) == 422
    assert {row["package_id"] for row in tables["committee_report_cbo_letters"]} == {CRPT_830, CRPT_139}

    # Prior CHRG + all 8 withdrawn historical revisions coexist cleanly.
    hearings = tables["hearing_transcripts"]
    assert len(hearings) == 1 + 8
    by_pkg: dict[str, list[dict]] = {}
    for row in hearings:
        by_pkg.setdefault(row["package_id"], []).append(row)
    assert len(by_pkg[CHRG_ID]) == 1
    assert len(by_pkg[WITHDRAWN_64154]) == 2
    assert len(by_pkg[WITHDRAWN_64598]) == 3
    assert len(by_pkg[WITHDRAWN_64599]) == 3

    # Verify literal U+00A0 early title history on 64599 and empty associatedMeeting / native congress 118.
    revs_64599 = sorted(by_pkg[WITHDRAWN_64599], key=lambda r: r["last_modified"])
    assert [row["title"] for row in revs_64599] == [
        "\u00a0",
        "\u00a0",
        "[ERRATA] THE ROLE OF SELF-REGULATORY ORGANIZATIONS",
    ]
    for pkg in (WITHDRAWN_64154, WITHDRAWN_64598, WITHDRAWN_64599):
        for row in by_pkg[pkg]:
            assert row["congress"] == "118"
            assert row["event_id"] is None
            assert json.loads(row["mods_event_ids_json"]) == []
            detail = json.loads(row["hearing_detail_json"])
            assert detail["state"] == "missing"
            assert detail["relationship"] == "not-stated"
            assert row["body_text"]
    assert tables["hearing_bill_links"] == []

    # Checkpoints settle at each withdrawn package's latest revision under current CHRG rule.
    reads = {row["package_id"]: row for row in tables[READS_TABLE]}
    assert reads[WITHDRAWN_64154]["last_modified"] == "2026-10-08T20:29:52Z"
    assert reads[WITHDRAWN_64598]["last_modified"] == "2026-10-08T14:17:49Z"
    assert reads[WITHDRAWN_64599]["last_modified"] == "2026-10-08T14:24:40Z"
    for pkg in (WITHDRAWN_64154, WITHDRAWN_64598, WITHDRAWN_64599):
        assert reads[pkg]["outcome"] == "complete"
        assert reads[pkg]["rule_version"] == RULE_VERSIONS["CHRG"]

    # Evidence journal seals cleanly with zero local paths and preserves inherited discovery cursor.
    evidence.finish()
    journal = [json.loads(line) for line in (evidence.artifact_dir / "journal.jsonl").read_text().splitlines()]
    [merge_event] = [event for event in journal if event["event"] == "retained-bundle-merge"]
    assert merge_event["selected_revisions"] == 8
    assert merge_event["new_acquisition"] is False
    [discovery_event] = [event for event in journal if event["event"] == "committee-discovery"]
    assert discovery_event["cursor"] == inherited_cursor
    assert discovery_event["mode"] == "retained-bundle"

    # Full native legislative-documents/4 conversion & receiptless generation admission passes.
    _admit_plain_generation(outputs, tmp_path / "native", tmp_path / "generation", generation_id="test-wave1-merged")

    # Next night's live discovery run sees all 3 withdrawn packages as settled and asks GovInfo for none of them.
    next_dir = tmp_path / "next_night"
    next_dir.mkdir()

    class EmptyDiscovery:
        def packages(self, url: str, *, max_pages: int = 40):
            return iter(())

    next_acquirer = _CboCaseAcquirer({})
    build_committee_reports(
        next_dir,
        reader=EmptyDiscovery(),
        acquirer=next_acquirer,
        hearings=StubHearings(),
        download_prior=_prior_download(merged_dir),
    )
    assert next_acquirer.requested == []


def test_committee_reports_rollup_runs_offline_retained_bundle_end_to_end(tmp_path, monkeypatch):
    """CommitteeReportsRollup.run(skip_upload=True) produces a verified local generation from a retained bundle."""
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    monkeypatch.delenv("R2_PUBLIC_URL", raising=False)
    monkeypatch.delenv("DATA_GOV_API_KEY", raising=False)
    monkeypatch.delenv("API_GOV", raising=False)
    monkeypatch.delenv("COMMITTEE_REPORTS_REREAD", raising=False)
    monkeypatch.delenv("COMMITTEE_REPORTS_RETAINED_BUNDLE", raising=False)

    bundle_dir = tmp_path / "bundle"
    pins_path = _build_synthetic_retained_bundle(bundle_dir)
    out_dir = tmp_path / "rollup_out"

    rollup = rollup_module.CommitteeReportsRollup(output_dir=out_dir, skip_upload=True)
    rollup.retained_bundle = pins_path
    rollup.retained_bundle_root = bundle_dir
    rollup.run()

    gen_dirs = [p for p in (out_dir / "generations").iterdir() if p.is_dir()]
    assert len(gen_dirs) == 1
    rows = pq.read_table(gen_dirs[0] / "hearing_transcripts.parquet").to_pylist()
    assert len(rows) == 8
    assert {row["package_id"] for row in rows} == {WITHDRAWN_64154, WITHDRAWN_64598, WITHDRAWN_64599}


def test_retained_bundle_refuses_tampered_wrapper_blob_or_reread_combination(tmp_path):
    """Any digest mismatch, missing blob, or reread+retained_bundle combination refuses before touching output."""
    bundle_dir = tmp_path / "bundle"
    pins_path = _build_synthetic_retained_bundle(bundle_dir)

    # 1. Mutual exclusion with reread.
    with pytest.raises(ValueError, match="cannot be combined"):
        build_committee_reports(tmp_path / "out1", reread=(CHRG_ID,), retained_bundle=pins_path)

    # 2. Tampered wrapper SHA-256 in pins refuses before extraction.
    bad_pins = tmp_path / "bad-wrapper-pins.json"
    data = json.loads(pins_path.read_text())
    data["retained_bundle_metadata"]["evidence-events.jsonl"]["sha256"] = "sha256:" + "0" * 64
    bad_pins.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="differs from its selected SHA-256"):
        load_retained_hearing_bundle(bad_pins, bundle_root=bundle_dir)

    # 3. Tampered original blob byte refuses before extraction.
    tampered_dir = tmp_path / "tampered_bundle"
    shutil.copytree(bundle_dir, tampered_dir)
    first_blob = next((tampered_dir / "originals").iterdir())
    original_bytes = first_blob.read_bytes()
    first_blob.write_bytes(bytes([original_bytes[0] ^ 0xFF]) + original_bytes[1:])
    with pytest.raises(ValueError, match="differs from its pinned SHA-256"):
        load_retained_hearing_bundle(tampered_dir / "selected-capture-pins.json", bundle_root=tampered_dir)


def test_retained_hearing_details_falls_back_across_joint_sibling_chamber_urls():
    from spicy_docs.reading.paged_json import PagedJsonUnavailableError
    from spicy_docs.sources.congress.listing import LIST_ROUTES
    from spicy_docs.sources.govinfo.bodies import parse_package_id
    from spicy_docs.transport.captured import CapturedBodyResponse
    from spicy_regs.sources.hearing_detail import hearing_detail_urls
    from spicy_regs.transforms.build_committee_reports import _RetainedHearingDetails

    identity = parse_package_id("CHRG-119jhrg61392")
    urls = hearing_detail_urls(identity)
    assert len(urls) == 3
    senate_url = urls[1]
    body = json.dumps({"hearing": {"chamber": "Senate", "congress": 119, "jacketNumber": 61392}}).encode()
    capture = CapturedBodyResponse(
        requested_url=senate_url,
        resolved_url=senate_url,
        status_code=200,
        content_type="application/json",
        observed_at="2026-11-08T00:00:00Z",
        body=body,
    )
    reader = _RetainedHearingDetails(capture, urls)
    with pytest.raises(PagedJsonUnavailableError):
        list(reader.records(LIST_ROUTES["hearing-detail"], urls[0], max_pages=1))
    pages = list(reader.records(LIST_ROUTES["hearing-detail"], senate_url, max_pages=1))
    assert len(pages) == 1 and pages[0].capture.requested_url == senate_url
    with pytest.raises(ValueError, match="Unselected offline hearing-detail route"):
        list(reader.records(LIST_ROUTES["hearing-detail"], "https://api.congress.gov/v3/hearing/119/house/99999", max_pages=1))


_LOCAL_CORPORA_ROOT = Path.home() / "Work/corpora"
_REAL_PINS = _LOCAL_CORPORA_ROOT / "supply-2026-09-02/receipts/crs-followup-20261114/selected-capture-pins.json"
_REAL_BUNDLE_ROOT = _LOCAL_CORPORA_ROOT / "spicyregs/output/committee-reports/history/govinfo-withdrawn-20261108"


@pytest.mark.skipif(
    not (_REAL_PINS.is_file() and _REAL_BUNDLE_ROOT.is_dir()),
    reason="local withdrawn-hearing bundle not present on runner",
)
def test_ss_xzy2_real_withdrawn_bundle_merges_onto_ss_uep5_prior_and_passes_wave1_oracles(tmp_path, monkeypatch):
    """Replay the actual 2026-11-14 accepted pins onto an ss-uep5 prior and check arch_wave1.md §7.4 SQL oracles."""
    monkeypatch.setenv("SPICYGOV_RAW_ROOT", str(tmp_path / "raw"))
    monkeypatch.delenv("DATA_GOV_API_KEY", raising=False)
    monkeypatch.delenv("API_GOV", raising=False)
    monkeypatch.delenv("CONGRESS_API_KEY", raising=False)
    monkeypatch.delenv("GOVINFO_API_KEY", raising=False)

    # Step 1: Build an ss-uep5 prior holding CRPT-119hrpt830, CRPT-119srpt139, CRPT-119hrpt758.
    prior_dir = tmp_path / "prior"
    prior_dir.mkdir()
    _seed_crpt_priors(prior_dir)
    build_committee_reports(
        prior_dir,
        reader=_NoListing(),
        acquirer=_CboCaseAcquirer(_cbo_payloads()),
        hearings=StubHearings(),
        download_prior=_no_prior,
        reread=(CRPT_830, CRPT_139, CRPT_758),
    )

    # Step 2: Merge the real retained 118th withdrawn cohort via build_committee_reports.
    merged_dir = tmp_path / "merged"
    merged_dir.mkdir()
    evidence = CaptureEvidence(tmp_path / "ev", "committee-reports")
    outputs = build_committee_reports(
        merged_dir,
        download_prior=_prior_download(prior_dir),
        evidence=evidence,
        retained_bundle=_REAL_PINS,
        retained_bundle_root=_REAL_BUNDLE_ROOT,
    )
    evidence.finish()

    # Step 3: Convert to native legislative-documents/4 and admit receiptless generation.
    gen_dir = tmp_path / "generation"
    _admit_plain_generation(outputs, tmp_path / "native", gen_dir, generation_id="test-ss-xzy2-real-merge")

    # Step 4: Run exact arch_wave1.md §7.4 SQL oracles for BOTH ss-uep5 and ss-xzy2.
    with duckdb.connect() as db:
        for table in ("committee_reports", "committee_report_cbo_letters", "hearing_transcripts", "hearing_bill_links"):
            db.read_parquet(str(gen_dir / f"{table}.parquet")).create_view(table)

        xzy2_rows = db.sql(
            """
            SELECT package_id, count(*) AS revs, min(length(body_text)) AS min_chars, max(length(body_text)) AS max_chars
            FROM hearing_transcripts
            WHERE package_id IN ('CHRG-118hhrg64154', 'CHRG-118hhrg64598', 'CHRG-118hhrg64599')
            GROUP BY 1 ORDER BY 1
            """
        ).fetchall()
        assert xzy2_rows == [
            (WITHDRAWN_64154, 2, 118482, 118482),
            (WITHDRAWN_64598, 3, 1618, 1618),
            (WITHDRAWN_64599, 3, 1634, 1634),
        ]

        titles_64599 = db.sql(
            """
            SELECT last_modified, title, event_id, detail_state, detail_relationship
            FROM hearing_transcripts
            WHERE package_id = 'CHRG-118hhrg64599'
            ORDER BY last_modified
            """
        ).fetchall()
        assert [row[1] for row in titles_64599[:2]] == ["\u00a0", "\u00a0"]
        assert titles_64599[2][1].startswith("[ERRATA] THE ROLE OF SELF-REGULATORY")
        assert all(row[2] is None and row[3] == "missing" and row[4] == "not-stated" for row in titles_64599)

        current_withdrawn = db.sql(
            f"""
            SELECT package_id, length(body_text)
            FROM ({latest_package_rows('hearing_transcripts')})
            WHERE package_id IN ('CHRG-118hhrg64154', 'CHRG-118hhrg64598', 'CHRG-118hhrg64599')
            ORDER BY package_id
            """
        ).fetchall()
        assert current_withdrawn == [
            (WITHDRAWN_64154, 118482),
            (WITHDRAWN_64598, 1618),
            (WITHDRAWN_64599, 1634),
        ]

        # Serialization oracle: ss-uep5 CRPT letters and 758 absence reason remain intact in the same generation.
        uep5_letters = db.sql(
            """
            SELECT package_id, count(*) AS letters
            FROM committee_report_cbo_letters
            WHERE package_id IN ('CRPT-119hrpt830', 'CRPT-119srpt139', 'CRPT-119hrpt758')
            GROUP BY 1 ORDER BY 1
            """
        ).fetchall()
        assert uep5_letters == [(CRPT_830, 1), (CRPT_139, 1)]
        uep5_reports = db.sql(
            """
            SELECT package_id, report_states_estimate, cbo_letter_count, length(estimate_absence_reason)
            FROM committee_reports
            WHERE package_id IN ('CRPT-119hrpt830', 'CRPT-119srpt139', 'CRPT-119hrpt758')
            ORDER BY 1
            """
        ).fetchall()
        assert uep5_reports == [
            (CRPT_758, False, 0, 422),
            (CRPT_830, True, 1, None),
            (CRPT_139, True, 1, None),
        ]
