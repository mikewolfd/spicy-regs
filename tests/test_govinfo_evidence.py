import hashlib
import json
from dataclasses import replace
from pathlib import Path

import pytest

from spicy_docs.sources.govinfo.discovery import package_granules_url
from spicy_docs.sources.govinfo.bodies import package_mods_locator, package_summary_locator
from spicy_docs.sources.govinfo.preservation import package_premis_locator, read_package_preservation
from spicy_docs.storage.blobs import BlobIntegrityError
from spicy_docs.storage.raw_archive import CentralRawArchiveStore
from spicy_docs.transport.captured import CapturedBodyResponse
from spicy_regs.govinfo_evidence import JournalEntry, budget_facts, budget_granules, preservation_facts
from spicy_regs.source_evidence import CaptureEvidence

PACKAGE = "BUDGET-2026-TAB"
URL = package_granules_url(PACKAGE)
FILE = f"https://www.govinfo.gov/content/pkg/{PACKAGE}/csv/native.csv"


def _page(url, **data):
    return CapturedBodyResponse(url, url, 200, "application/json", "2026-11-07T12:00:00Z", json.dumps(data).encode())


def test_complete_empty_differs_from_no_page_and_incomplete_continuation():
    empty = _page(URL, count=0, granules=[])
    assert budget_granules(PACKAGE, (), page_size=100)["status"] == "unrequested"
    assert budget_granules(PACKAGE, (empty,), page_size=100) == {
        "status": "complete",
        "pages": [
            {
                "url": URL,
                "sha256": empty.sha256,
                "page": 1,
                "state": "retained",
                "status_code": 200,
                "byte_size": empty.byte_size,
                "observed_at": empty.observed_at,
                "declared_count": 0,
            }
        ],
        "granules": [],
    }
    next_url = URL.replace("offsetMark=*", "offsetMark=next")
    first = _page(URL, count=2, granules=[{"granuleId": "a"}], nextPage=next_url)
    with pytest.raises(ValueError, match="before a terminal response"):
        budget_granules(PACKAGE, (first,), page_size=100)
    duplicate = _page(next_url, count=2, granules=[{"granuleId": "a"}])
    with pytest.raises(ValueError, match="repeats"):
        budget_granules(PACKAGE, (first, duplicate), page_size=100)
    last = _page(next_url, count=2, granules=[{"granuleId": "b"}])
    assert len(budget_granules(PACKAGE, (first, last), page_size=100)["granules"]) == 2
    unavailable = budget_granules(PACKAGE, (replace(empty, status_code=404),), page_size=100)
    assert unavailable["status"] == "unavailable" and unavailable["pages"][0]["status_code"] == 404
    with pytest.raises(ValueError, match="different package"):
        budget_granules(
            PACKAGE, (_page(URL, count=1, granules=[{"granuleId": "a", "packageId": "OTHER"}]),), page_size=100
        )


def _budget(*, file, last_modified="2026-04-02T18:37:00Z", fixity="match", repeat=False, changed=False):
    first = _page(
        package_summary_locator(PACKAGE),
        packageId=PACKAGE,
        collectionCode="GPO",
        lastModified=last_modified,
        dateIssued="2025-06-20",
    )
    location = f'<location><url access="raw object">{FILE}</url></location>'
    mods_url = package_mods_locator(PACKAGE)
    mods = replace(
        _page(mods_url),
        body=(
            f'<mods xmlns="http://www.loc.gov/mods/v3"><extension><accessId>{PACKAGE}</accessId></extension>'
            f"{location}{location if repeat else ''}</mods>"
        ).encode(),
    )
    digest = file.sha256.removeprefix("sha256:") if fixity == "match" else "0" * 64
    statement = (
        f"<fixity><messageDigestAlgorithm>SHA-256</messageDigestAlgorithm><messageDigest>{digest}</messageDigest></fixity>"
        if fixity != "missing"
        else ""
    )
    obj = (
        f'<object xsi:type="file"><objectCharacteristics><compositionLevel>0</compositionLevel>{statement}'
        f"</objectCharacteristics><storage><contentLocation><contentLocationType>URI</contentLocationType>"
        f"<contentLocationValue>{FILE}</contentLocationValue></contentLocation></storage></object>"
    )
    premis = (
        None
        if fixity == "unavailable"
        else replace(
            _page(package_premis_locator(PACKAGE)),
            body=(
                f'<premis xmlns="info:lc/xmlns/premis-v2" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">'
                f"{obj}{obj if fixity == 'ambiguous' else ''}</premis>"
            ).encode(),
        )
    )
    closing = replace(first, body=first.body + (b" " if changed else b""), observed_at="2026-11-07T12:01:00Z")
    return read_package_preservation(PACKAGE, summary=first, mods=mods, premis=premis, closing_summary=closing)


@pytest.mark.parametrize(
    ("fixity", "reason"),
    [
        ("match", "sha256-match"),
        ("mismatch", "sha256-mismatch"),
        ("missing", "missing-fixity"),
        ("ambiguous", "ambiguous-file"),
        ("unavailable", "premis-unavailable"),
    ],
)
def test_budget_consumer_qualifies_only_exact_files(fixity, reason):
    file = replace(_page(FILE), body=b"Total,Note,\n0,,\n")
    observation = _budget(file=file, fixity=fixity)
    result = budget_facts(observation, pages=(), file=file)
    assert result["file"]["comparison"]["reason"] == reason
    if fixity == "match":
        assert result["table_status"] == "read" and result["table"] == (("Total", "Note", ""), ("0", "", ""))
    else:
        assert result["table_status"] == "fixity-unproved" and "table" not in result


def test_budget_consumer_holds_mixed_observations_and_repeated_offers():
    file = replace(_page(FILE), body=b"Total\n0\n")
    for observation, selected in (
        (_budget(file=file, changed=True), file),
        (_budget(file=file, last_modified="2026-04-02"), file),
        (_budget(file=file), replace(file, observed_at="2026-11-07T12:02:00Z")),
    ):
        result = budget_facts(observation, pages=(), file=selected)
        assert result["table_status"] == "revision-unproved" and "table" not in result
    with pytest.raises(ValueError, match="more than one"):
        budget_facts(_budget(file=file, repeat=True), pages=(), file=file)
    assert (
        budget_facts(_budget(file=file), pages=(), file=replace(file, status_code=404))["table_status"] == "unavailable"
    )
    facts = preservation_facts(_budget(file=file, last_modified="2026-04-02"))
    assert facts["last_modified"] is None
    assert next(date for date in facts["native_dates"] if date["field"] == "SUMMARY/lastModified")["state"] == "invalid"


def test_pinned_capture_refuses_journal_and_original_corruption(tmp_path):
    raw = tmp_path / "raw"
    evidence = CaptureEvidence(tmp_path, "govinfo-test", archive_root=raw)
    capture = _page(URL, count=0, granules=[])
    evidence.capture(capture, stage="granules")
    path = evidence.artifact_dir / "journal.jsonl"
    journal = path.read_bytes()
    sha = "sha256:" + hashlib.sha256(journal).hexdigest()
    pin = JournalEntry(path, sha, 2)
    store = CentralRawArchiveStore(raw, create=False)
    assert pin.read(store, max_bytes=1024) == capture
    path.write_bytes(journal + b"\n")
    with pytest.raises(ValueError, match="pinned SHA"):
        pin.read(store, max_bytes=1024)
    path.write_bytes(journal)
    digest = capture.sha256.removeprefix("sha256:")
    original = raw / Path("objects", "sha256", digest[:2], digest)
    original.write_bytes(b"x" * capture.byte_size)
    with pytest.raises(BlobIntegrityError):
        pin.read(store, max_bytes=1024)
