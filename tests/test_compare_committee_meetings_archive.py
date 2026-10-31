"""The committee_meetings-to-archive comparison measures both directions on a two-chamber fixture (ss-ikn9)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb
import pytest

from spicy_regs import archive_release

_spec = importlib.util.spec_from_file_location(
    "compare_committee_meetings_archive",
    Path(__file__).parents[1] / "scripts" / "compare_committee_meetings_archive.py")
assert _spec and _spec.loader
compare = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(compare)

DOC = "https://www.docs.house.gov/meetings/AP/1/HHRG-1.pdf"
#: The archive's own spelling of DOC: no www, so only the DATA-MODEL 8.2 lookup key matches it.
ARCHIVED = "https://docs.house.gov/meetings/AP/1/HHRG-1.pdf/"
UNHELD = "https://docs.house.gov/meetings/AP/1/missing.pdf"


def _write(paths: dict[str, Path], name: str, sql: str) -> None:
    paths[name] = paths["dir"] / f"{name}.parquet"
    duckdb.execute(f"COPY ({sql}) TO '{paths[name]}' (FORMAT parquet)")


def _two_chamber_fixture(tmp_path: Path) -> dict[str, Path]:
    """House meeting 1 is in both, with one held and one unheld document; Senate 2 is spicy-regs only; 3 is archive only."""
    paths = {"dir": tmp_path}
    doc_struct = "{'description': NULL, 'documentType': 'Hearing', 'format': 'PDF', 'name': NULL, 'url': ?}"
    _write(paths, "committee_meetings", f"""SELECT * FROM (VALUES
        ('118', 'house', '1', 'T', 'Hearing', 'Scheduled', '2024-01-10T15:00:00Z', 1, 1, 2,
         ['{DOC}', '{UNHELD}'], [{doc_struct.replace('?', repr(DOC))}], []::STRUCT(documentType VARCHAR,
         format VARCHAR, url VARCHAR)[],
         [{{'name': 'Approps', 'systemCode': 'HSAP00', 'url': NULL}}], ['1', '2'],
         [{{'name': 'Ann Lee', 'position': 'Director', 'organization': 'GAO'}}],
         [{{'name': 'Video', 'url': 'https://youtu.be/x'}}], ['118-hr-1']),
        ('118', 'senate', '2', 'S', 'Hearing', 'Scheduled', '2024-02-10T15:00:00Z', 0, 0, 0, [], [], [],
         [], [], [], [], []))
        AS t(congress, chamber, event_id, title, meeting_type, meeting_status, meeting_date, witness_count,
             witness_document_count, meeting_document_count, document_urls, meeting_documents, witness_documents,
             committees, hearing_jackets, witnesses, videos, bill_ids)""")
    _write(paths, "meetings", """SELECT * FROM (VALUES (1::BIGINT, 'house', 'T', DATE '2024-01-10', 'HHRG',
        'Scheduled', [{'package_id': 'CHRG-118hhrg1', 'list_row': 0}], [{'system_code': 'hsap00', 'role': 'host'}],
        [{'name': 'Video', 'url': 'https://youtu.be/x', 'video_key': 'yt:x'}], ['118-hr-1', '118-pn-5-1', '118-treaty-2']),
        (3::BIGINT, 'senate', 'Old', DATE '2015-01-01', 'Hearing', 'Scheduled', [], [], [], []))
        AS t(event_id, chamber, title, meeting_date, meeting_type, status, transcripts, committees, videos,
             subject_keys)""")
    _write(paths, "witnesses", """SELECT * FROM (VALUES ('w1', 3::BIGINT, 'Old Witness', NULL, NULL),
        ('w2', 1::BIGINT, 'Ann Lee', 'Director', 'Government Accountability Office'))
        AS t(witness_key, event_id, name, position, organization)""")
    _write(paths, "list", """SELECT * FROM (VALUES (['1'], []::VARCHAR[], ['meeting-support'], 'pdf', NULL, 118::SMALLINT),
        ([], ['w1'], ['witness-statement'], 'pdf', 'Statement', NULL))
        AS t(meeting_keys, witness_keys, document_kind, format, title, congress)""")
    _write(paths, "detail", "SELECT * FROM (VALUES ('CHRG-1', 'ab'::BLOB), (NULL, 'cd'::BLOB)) AS t(package_id, saved_body)")
    _write(paths, "sources", f"SELECT 0 AS list_row, '{ARCHIVED}' AS url")
    key = archive_release.lookup_key(ARCHIVED)
    assert key is not None
    _write(paths, "urls", f"SELECT from_hex('{key.hex()}') AS lookup_key, 0::INTEGER AS list_row")
    return paths


def test_coverage_is_measured_in_both_directions(tmp_path):
    report = compare.compare(_two_chamber_fixture(tmp_path))
    by_chamber = {row["chamber"]: row for row in report["coverage_committee_meetings_to_archive"]}
    assert by_chamber["house"]["missing"] == 0
    assert by_chamber["senate"]["missing_event_ids"] == ["2"]
    archive_side = {row["chamber"]: row for row in report["coverage_archive_to_committee_meetings"]}
    assert archive_side["senate"]["absent_before_window"] == 1
    assert report["field_disagreement_on_matched_rows"]["meeting_type"] == 1
    witnesses = {row["chamber"]: row for row in report["witness_meetings_both_ways"]}
    assert witnesses["house"]["only_committee_meetings"] == 0


def test_mapped_columns_are_compared_by_value_on_matched_meetings(tmp_path):
    agreement = compare.compare(_two_chamber_fixture(tmp_path))["mapped_value_agreement"]
    counts = {name: {k: v for k, v in row.items() if k != "examples"} for name, row in agreement.items()}
    one = {"either_nonempty": 1, "equal": 0, "archive_superset": 0, "committee_meetings_superset": 0, "differ": 0,
           "only_committee_meetings": 0, "only_archive": 0}
    # System codes agree once case is normalized; the archive holds one of two jackets, spelled as a package id.
    assert counts["committees"] == {**one, "equal": 1}
    assert counts["hearing_jackets"] == {**one, "committee_meetings_superset": 1}
    assert counts["videos"] == {**one, "equal": 1}
    assert counts["witness_names"] == {**one, "equal": 1}
    assert counts["witness_name_position"] == {**one, "equal": 1}
    # A spelling difference is a disagreement with an example, never silent agreement.
    assert counts["witness_name_organization"] == {**one, "differ": 1}
    # Nominations and treaties in subject_keys are not bills, so they do not make the archive a superset.
    assert counts["bill_ids"] == {**one, "equal": 1}
    assert agreement["hearing_jackets"]["examples"] == [{"event_id": 1, "committee_meetings": ["1", "2"],
                                                         "archive": ["1"]}]


def test_offered_urls_resolve_by_the_lookup_key_not_only_by_exact_spelling(tmp_path):
    report = compare.compare(_two_chamber_fixture(tmp_path))
    (house,) = report["offered_urls"]
    assert house == {"chamber": "house", "distinct_urls": 2, "exact_in_sources": 0, "resolved_by_lookup_key": 1,
                     "resolved_to_a_row_naming_the_meeting": 1}
    assert report["lookup_key_self_check"] == {"source_urls": 1, "found_in_urls": 1}
    assert report["meeting_linked_document_fields"]["detail_package_id"] == 1


def test_inputs_from_an_earlier_report_reread_its_generation_and_release(tmp_path, monkeypatch):
    files = {stem: f"{stem}-sha" for stem in compare.ARCHIVE}
    pinned = {"committee_meetings": {"artifactDigest": "sha256:abc", "sha256": "sha256:def", "publishedAt": "t"},
              "archive": {"release_id": "r1", "files": files}}
    manifest = {"files": [{"name": f"{stem}.parquet", "sha256": sha} for stem, sha in files.items()]}
    monkeypatch.setattr(compare.archive_release, "read_manifest", lambda release: manifest)
    monkeypatch.setattr(compare.publication, "load_index", lambda base: pytest.fail("read the live index"))
    fetched = []
    monkeypatch.setattr(compare, "_held", lambda cache, name, url, sha: fetched.append((url, sha)) or cache / name)
    pins, _ = compare.inputs(tmp_path, pinned)
    assert pins == pinned
    assert fetched[0] == (f"{compare.PUBLICATION}/generations/committee-meetings/abc/committee_meetings.parquet", "def")
    manifest["files"][0]["sha256"] = "moved"
    with pytest.raises(SystemExit, match="differ from the pinned report"):
        compare.inputs(tmp_path, pinned)
