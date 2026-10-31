"""The committee_meetings-to-archive comparison measures both directions on a two-chamber fixture (ss-ikn9)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb

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
         format VARCHAR, url VARCHAR)[]),
        ('118', 'senate', '2', 'S', 'Hearing', 'Scheduled', '2024-02-10T15:00:00Z', 0, 0, 0, [], [], []))
        AS t(congress, chamber, event_id, title, meeting_type, meeting_status, meeting_date, witness_count,
             witness_document_count, meeting_document_count, document_urls, meeting_documents, witness_documents)""")
    _write(paths, "meetings", """SELECT * FROM (VALUES (1::BIGINT, 'house', 'T', DATE '2024-01-10', 'HHRG',
        'Scheduled', [{'package_id': 'CHRG-1', 'list_row': 0}]),
        (3::BIGINT, 'senate', 'Old', DATE '2015-01-01', 'Hearing', 'Scheduled', []))
        AS t(event_id, chamber, title, meeting_date, meeting_type, status, transcripts)""")
    _write(paths, "witnesses", "SELECT 'w1' AS witness_key, 3::BIGINT AS event_id")
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
    assert witnesses["house"]["only_committee_meetings"] == 1


def test_offered_urls_resolve_by_the_lookup_key_not_only_by_exact_spelling(tmp_path):
    report = compare.compare(_two_chamber_fixture(tmp_path))
    (house,) = report["offered_urls"]
    assert house == {"chamber": "house", "distinct_urls": 2, "exact_in_sources": 0, "resolved_by_lookup_key": 1,
                     "resolved_to_a_row_naming_the_meeting": 1}
    assert report["lookup_key_self_check"] == {"source_urls": 1, "found_in_urls": 1}
    assert report["meeting_linked_document_fields"]["detail_package_id"] == 1
