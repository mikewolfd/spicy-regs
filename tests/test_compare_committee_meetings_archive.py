"""The committee_meetings-to-archive comparison measures both directions on a two-chamber fixture (ss-ikn9)."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import duckdb
import pytest

from spicy_regs import archive_release

_spec = importlib.util.spec_from_file_location(
    "compare_committee_meetings_archive",
    Path(__file__).parents[1] / "scripts" / "compare_committee_meetings_archive.py",
)
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
    _write(
        paths,
        "committee_meetings",
        f"""SELECT * FROM (VALUES
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
             committees, hearing_jackets, witnesses, videos, bill_ids)""",
    )
    _write(
        paths,
        "meetings",
        """SELECT * FROM (VALUES (1::BIGINT, 'house', 'T', DATE '2024-01-10', 'HHRG',
        'Scheduled', [{'package_id': 'CHRG-118hhrg1', 'list_row': 0}], [{'system_code': 'hsap00', 'role': 'host'}],
        [{'name': 'Video', 'url': 'https://youtu.be/x', 'video_key': 'yt:x'}], ['118-hr-1', '118-pn-5-1', '118-treaty-2']),
        (3::BIGINT, 'senate', 'Old', DATE '2015-01-01', 'Hearing', 'Scheduled', [], [], [], []))
        AS t(event_id, chamber, title, meeting_date, meeting_type, status, transcripts, committees, videos,
             subject_keys)""",
    )
    _write(
        paths,
        "witnesses",
        """SELECT * FROM (VALUES ('w1', 3::BIGINT, 'Old Witness', NULL, NULL),
        ('w2', 1::BIGINT, 'Ann Lee', 'Director', 'Government Accountability Office'))
        AS t(witness_key, event_id, name, position, organization)""",
    )
    _write(
        paths,
        "list",
        """SELECT * FROM (VALUES (['1'], []::VARCHAR[], ['meeting-support'], 'pdf', NULL, 118::SMALLINT),
        ([], ['w1'], ['witness-statement'], 'pdf', 'Statement', NULL))
        AS t(meeting_keys, witness_keys, document_kind, format, title, congress)""",
    )
    _write(paths, "detail", "SELECT * FROM (VALUES ('CHRG-1', 'ab'::BLOB), (NULL, 'cd'::BLOB)) AS t(package_id, saved_body)")
    _write(paths, "sources", f"SELECT 0 AS list_row, '{ARCHIVED}' AS url")
    key = archive_release.lookup_key(ARCHIVED)
    assert key is not None
    _write(paths, "urls", f"SELECT from_hex('{key.hex()}') AS lookup_key, 0::INTEGER AS list_row")
    return paths


def _full_parity_fixture(
    tmp_path: Path, *, unresolved_person: bool = False, orphan_doc_witness: bool = False
) -> dict[str, Path]:
    """Full-parity two-chamber fixture covering nominations, treaties, canonical witnesses, and Senate links."""
    paths = {"dir": tmp_path}
    doc_struct = "{'description': NULL, 'documentType': 'Hearing', 'format': 'PDF', 'name': NULL, 'url': ?}"
    sen_doc_url = "https://www.veterans.senate.gov/w_sen.pdf"
    _write(
        paths,
        "committee_meetings",
        f"""SELECT * FROM (VALUES
        ('118', 'house', '1', 'House Hearing', 'Hearing', 'Scheduled', '2024-01-10T15:00:00Z',
         '2024-01-09T14:00:00Z', 'RHOB', '2118', '[]', '[]',
         1, 0, 1, ['{DOC}'], [{doc_struct.replace('?', repr(DOC))}],
         []::STRUCT(documentType VARCHAR, format VARCHAR, url VARCHAR)[],
         [{{'name': 'Approps', 'systemCode': 'HSAP00', 'url': NULL}}], ['1'],
         [{{'name': 'Mr. Jane Doe, Ph.D.', 'position': 'Director', 'organization': 'GAO'}}],
         [{{'name': 'Video', 'url': 'https://youtu.be/x'}}], ['118-hr-1']),
        ('118', 'senate', '2', 'Senate Hearing', 'Hearing', 'Scheduled', '2024-02-10T15:00:00Z',
         '2024-02-09T14:00:00Z', 'SD', '106',
         '[{{"congress":118,"number":5,"part":"01"}},{{"congress":118,"number":7,"part":"00"}}]',
         '[{{"congress":118,"number":2}}]',
         0, 0, 0, []::VARCHAR[],
         []::STRUCT(description VARCHAR, documentType VARCHAR, format VARCHAR, name VARCHAR, url VARCHAR)[],
         []::STRUCT(documentType VARCHAR, format VARCHAR, url VARCHAR)[],
         [{{'name': 'VA', 'systemCode': 'SSVA00', 'url': NULL}}], []::VARCHAR[],
         []::STRUCT(name VARCHAR, position VARCHAR, organization VARCHAR)[],
         []::STRUCT(name VARCHAR, url VARCHAR)[], []::VARCHAR[]))
        AS t(congress, chamber, event_id, title, meeting_type, meeting_status, meeting_date,
             update_date, location_building, location_room, nomination_references_json, treaty_references_json,
             witness_count, witness_document_count, meeting_document_count, document_urls, meeting_documents,
             witness_documents, committees, hearing_jackets, witnesses, videos, bill_ids)""",
    )
    _write(
        paths,
        "meetings",
        """SELECT * FROM (VALUES
        (1::BIGINT, 'house', 'House Hearing', DATE '2024-01-10', 'HHRG', 'Scheduled',
         118::INTEGER, 'Hearing', TIMESTAMPTZ '2024-01-09T14:00:00Z', '2024-01-09T09:00:00',
         [{'start': TIMESTAMPTZ '2024-01-10T15:00:00Z', 'building': 'RHOB', 'room': '2118'}],
         [{'package_id': 'CHRG-118hhrg1', 'list_row': 0}], [{'system_code': 'hsap00', 'role': 'host'}],
         [{'name': 'Video', 'url': 'https://youtu.be/x', 'video_key': 'yt:x'}], ['118-hr-1']),
        (2::BIGINT, 'senate', 'Senate Hearing', DATE '2024-02-10', 'Hearing', 'Scheduled',
         118::INTEGER, 'Hearing', TIMESTAMPTZ '2024-02-09T14:00:00Z', NULL::VARCHAR,
         [{'start': TIMESTAMPTZ '2024-02-10T15:00:00Z', 'building': 'SD', 'room': '106'}],
         []::STRUCT(package_id VARCHAR, list_row INTEGER)[],
         [{'system_code': 'ssva00', 'role': 'host'}],
         []::STRUCT(name VARCHAR, url VARCHAR, video_key VARCHAR)[], ['118-pn-5-1', '118-pn-7', '118-treaty-2']))
        AS t(event_id, chamber, title, meeting_date, meeting_type, status, congress, congress_gov_meeting_type,
             congress_gov_updated, house_xml_updated, sittings, transcripts, committees, videos, subject_keys)""",
    )
    sen_person = "p_missing" if unresolved_person else "p_sen"
    _write(
        paths,
        "witnesses",
        f"""SELECT * FROM (VALUES
        ('w_h', 1::BIGINT, 'house_xml', 'Jane Doe', NULL::VARCHAR, 'Ph.D.', 'Director', 'GAO', 'witness', NULL::VARCHAR, 'p_h'),
        ('w_h_dup', 1::BIGINT, 'mods', 'Jane Q. Doe', NULL::VARCHAR, NULL::VARCHAR, 'Director', 'GAO', 'witness', 'w_h', 'p_h'),
        ('w_sen', 2::BIGINT, 'senate_page', 'Denis McDonough', NULL::VARCHAR, NULL::VARCHAR, 'Secretary', 'VA', 'witness', NULL::VARCHAR, '{sen_person}'),
        ('w_sen_page_only', NULL::BIGINT, 'senate_page', 'Page Only', NULL::VARCHAR, NULL::VARCHAR, 'Witness', 'Org', 'witness', NULL::VARCHAR, 'p_page'))
        AS t(witness_key, event_id, source, name, rank, credentials, position, organization, witness_type, superseded_by, person_key)""",
    )
    _write(
        paths,
        "people",
        """SELECT * FROM (VALUES
        ('p_h', NULL::VARCHAR, 'name_organization'),
        ('p_sen', NULL::VARCHAR, 'name_organization'),
        ('p_page', NULL::VARCHAR, 'name_organization'))
        AS t(person_key, bioguide_id, grouping_basis)""",
    )
    extra_wkey = ["w_orphan"] if orphan_doc_witness else []
    _write(
        paths,
        "list",
        f"""SELECT * FROM (VALUES
        (['1'], []::VARCHAR[], ['meeting-support'], 'pdf', NULL::VARCHAR, 118::SMALLINT),
        ([]::VARCHAR[], ['w_sen'{', ' + repr(extra_wkey[0]) if extra_wkey else ''}], ['witness-statement'], 'pdf', 'Statement', 118::SMALLINT),
        ([]::VARCHAR[], ['w_sen_page_only'], ['witness-statement'], 'pdf', 'Page Statement', 118::SMALLINT))
        AS t(meeting_keys, witness_keys, document_kind, format, title, congress)""",
    )
    _write(
        paths,
        "detail",
        "SELECT * FROM (VALUES ('CHRG-1', 'ab'::BLOB), (NULL, 'cd'::BLOB), (NULL, 'ef'::BLOB)) AS t(package_id, saved_body)",
    )
    _write(paths, "sources", f"SELECT * FROM (VALUES (0, '{ARCHIVED}'), (1, '{sen_doc_url}')) t(list_row, url)")
    key_h = archive_release.lookup_key(ARCHIVED)
    key_s = archive_release.lookup_key(sen_doc_url)
    assert key_h is not None and key_s is not None
    _write(
        paths,
        "urls",
        f"SELECT * FROM (VALUES (from_hex('{key_h.hex()}'), 0::INTEGER), (from_hex('{key_s.hex()}'), 1::INTEGER)) t(lookup_key, list_row)",
    )
    return paths


def test_coverage_is_measured_in_both_directions(tmp_path):
    report = compare.compare(_two_chamber_fixture(tmp_path))
    by_chamber = {row["chamber"]: row for row in report["coverage_committee_meetings_to_archive"]}
    assert by_chamber["house"]["missing"] == 0
    assert by_chamber["senate"]["missing_event_ids"] == ["2"]
    archive_side = {row["chamber"]: row for row in report["coverage_archive_to_committee_meetings"]}
    assert archive_side["senate"]["absent_before_window"] == 1
    assert report["field_disagreement_on_matched_rows"]["meeting_type"] is None
    assert report["column_destinations"]["meeting_type"] is None
    witnesses = {row["chamber"]: row for row in report["witness_meetings_both_ways"]}
    assert witnesses["house"]["only_committee_meetings"] == 0
    assert report["residuals"]["has_residuals"] is True
    assert report["residuals"]["missing_archive_event_ids"] == ["2"]


def test_meeting_details_compare_the_congress_gov_label_and_matching_sitting(tmp_path):
    paths = _two_chamber_fixture(tmp_path)
    _write(
        paths,
        "upstream_details",
        f"""SELECT *,
        '2024-01-09T10:00:00-05:00' AS update_date,
        ' RHOB' AS location_building, '----------' AS location_room
        FROM read_parquet('{paths["committee_meetings"]}')""",
    )
    _write(
        paths,
        "archive_details",
        f"""SELECT *,
        118 AS congress, 'Hearing' AS congress_gov_meeting_type,
        TIMESTAMPTZ '2024-01-09T14:00:00Z' AS congress_gov_updated,
        '2024-01-09T09:00:00' AS house_xml_updated,
        [{{'start': TIMESTAMPTZ '2024-01-10T13:00:00Z', 'room': '101', 'building': 'Other'}},
         {{'start': TIMESTAMPTZ '2024-01-10T15:00:00Z', 'room': NULL, 'building': 'RHOB'}}] AS sittings
        FROM read_parquet('{paths["meetings"]}')""",
    )
    paths["committee_meetings"] = paths.pop("upstream_details")
    paths["meetings"] = paths.pop("archive_details")
    report = compare.compare(paths)
    detail = report["meeting_detail_agreement"]
    assert report["field_disagreement_on_matched_rows"]["meeting_type"] == 0
    assert report["column_destinations"]["meeting_type"] == "meetings.congress_gov_meeting_type"
    assert detail["counts"] == {
        "matched": 1,
        "congress": 0,
        "update_date": 1,
        "meeting_type": 0,
        "location_building": 0,
        "location_room": 0,
    }
    assert detail["source_and_sitting_states"] == [
        {"version_state": "upstream_newer", "sitting_state": "matched", "rows": 1}
    ]
    assert detail["residuals"][0]["archive_sitting_count"] == 2
    assert detail["residuals"][0]["archive_room"] is None
    assert detail["location_normalization_differences"] == [
        {"event_id": 1, "upstream_building": " RHOB", "archive_building": "RHOB",
         "upstream_room": "----------", "archive_room": None}
    ]


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
    assert agreement["hearing_jackets"]["examples"] == [{"event_id": 1, "committee_meetings": ["1", "2"], "archive": ["1"]}]


def test_offered_urls_resolve_by_the_lookup_key_not_only_by_exact_spelling(tmp_path):
    paths = _two_chamber_fixture(tmp_path)
    _write(paths, "nullable_sources", f"SELECT 0 AS list_row, '{ARCHIVED}' AS url UNION ALL SELECT 0, NULL")
    paths["sources"] = paths.pop("nullable_sources")
    report = compare.compare(paths)
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


def test_zero_residual_parity_passes_and_senate_links_match(tmp_path, monkeypatch):
    paths = _full_parity_fixture(tmp_path)
    out = tmp_path / "clean-report.json"
    monkeypatch.setattr(compare, "inputs", lambda cache, pinned=None: ({"committee_meetings": {}, "archive": {}}, paths))
    rc = compare.main(["--cache", str(tmp_path / "cache"), "--output", str(out)])
    assert rc == 0
    report = json.loads(out.read_text())
    assert report["residuals"]["has_residuals"] is False
    assert report["columns_without_destination"] == []
    assert report["mapped_value_agreement"]["nominations"]["equal"] == 1
    assert report["mapped_value_agreement"]["treaties"]["equal"] == 1
    assert report["mapped_value_agreement"]["canonical_witness_names"]["equal"] == 1
    sen = report["senate_witness_links"]
    assert sen["orphan_document_witness_keys"] == 0
    assert sen["senate_meeting_witness_links"]["witnesses_with_doc_and_person"] == 1
    assert sen["senate_page_overall"]["witnesses_with_doc_and_person"] == 2
    assert sen["senate_scope_overall"]["distinct_people_with_documents"] == 2


def test_senate_witness_integrity_residuals_fail_nonzero_and_still_write_report(tmp_path, monkeypatch):
    paths = _full_parity_fixture(tmp_path, unresolved_person=True, orphan_doc_witness=True)
    out = tmp_path / "residual-report.json"
    monkeypatch.setattr(compare, "inputs", lambda cache, pinned=None: ({"committee_meetings": {}, "archive": {}}, paths))
    rc = compare.main(["--cache", str(tmp_path / "cache"), "--output", str(out)])
    assert rc == 1
    report = json.loads(out.read_text())
    assert report["residuals"]["has_residuals"] is True
    assert report["residuals"]["senate_witness_integrity_failures"] == {
        "orphan_document_witness_keys": 1,
        "unresolved_person_keys": 1,
        "unlinked_missing_meeting": 0,
    }
