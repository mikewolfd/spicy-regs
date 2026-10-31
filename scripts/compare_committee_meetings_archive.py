#!/usr/bin/env python3
"""Compare the published committee_meetings generation with the pinned archive release, both ways (ss-ikn9).

Retiring spicy-regs' committee_meetings capture needs evidence that the
archive release carries its rows and columns. Each input is read by digest:
committee_meetings by the sha256 the publication index states, the archive
files by the sha256 in the release manifest, whose id is recomputed from its
files (``archive_release.read_manifest``). A file already in ``--cache`` is
reused when its digest matches; otherwise it is downloaded there once. Every
query projects only the columns it reads. The report is JSON; it decides
nothing.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa

from spicy_regs import archive_release
from spicy_regs.sources import publication

PUBLICATION = "https://data.spicygov.ai"
FAMILY, TABLE = "committee-meetings", "committee_meetings.parquet"
ARCHIVE = ("meetings", "witnesses", "list", "detail", "sources", "urls")
#: Where each committee_meetings column lands in the archive release; None is a column the release does not carry.
DESTINATIONS = {
    "congress": None,
    "chamber": "meetings.chamber (NULL for nochamber)",
    "event_id": "meetings.event_id (BIGINT)",
    "title": "meetings.title",
    "meeting_type": "meetings.meeting_type (House XML codes for House rows)",
    "meeting_status": "meetings.status",
    "meeting_date": "meetings.meeting_date (local DATE) and sittings[].start",
    "location_building": None,
    "location_room": None,
    "committee_system_code": "meetings.committees[].system_code",
    "committee_count": "len(meetings.committees)",
    "committees": "meetings.committees[].system_code; name and url through the committees table by system code",
    "hearing_jacket": "meetings.transcripts[].package_id",
    "hearing_jacket_count": "len(meetings.transcripts)",
    "hearing_jackets": "meetings.transcripts[].package_id",
    "bill_count": "count(DISTINCT list.measure_keys) on the meeting's documents (document-level)",
    "bill_ids": "list.measure_keys on the meeting's documents (document-level, not meeting-level)",
    "witness_count": "count(witnesses) by event_id",
    "witnesses": "witnesses (name, position, organization)",
    "witness_document_count": "count(list rows) by witnesses.witness_key in list.witness_keys",
    "witness_documents": "list rows by witness_key: document_kind, format; no source URL column (urls.parquet resolves a URL)",
    "meeting_document_count": "count(list rows) by event_id in list.meeting_keys",
    "meeting_documents": "list rows by meeting key: title, document_kind, format; detail.package_id, saved_body",
    "document_urls": "sources.url and urls.lookup_key -> list_row",
    "videos": "meetings.videos",
    "update_date": None,
    "nomination_references_json": None,
    "treaty_references_json": None,
}


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def _held(cache: Path, name: str, url: str, sha256: str) -> Path:
    """The file at ``cache/name`` with the stated digest, downloading it there only when absent or different."""
    path = cache / name
    if not path.exists() or _sha256(path) != sha256:
        raw = publication._bounded_get(url, allow_missing=False, limit=1 << 30)
        assert raw is not None
        if hashlib.sha256(raw).hexdigest() != sha256:
            raise SystemExit(f"{url} does not match its stated sha256 {sha256[:8]}")
        path.write_bytes(raw)
    return path


def inputs(cache: Path) -> tuple[dict, dict[str, Path]]:
    index = publication.load_index(PUBLICATION)
    family = index["families"][FAMILY]
    table = family["tables"][TABLE]
    release = archive_release.load_pins()["bound"]
    manifest = archive_release.read_manifest(release)
    entries = {item["name"]: item for item in manifest["files"]}
    cache = cache / release
    cache.mkdir(parents=True, exist_ok=True)
    paths = {"committee_meetings": _held(cache, f"{family['artifactDigest'][7:]}-{TABLE}",
                                         f"{PUBLICATION}/{family['prefix']}/{TABLE}", table["sha256"].removeprefix("sha256:"))}
    for stem in ARCHIVE:
        name = f"{stem}.parquet"
        paths[stem] = _held(cache, name, f"{archive_release.ORIGIN}/releases/{release}/{name}", entries[name]["sha256"])
    pins = {"committee_meetings": {"artifactDigest": family["artifactDigest"], "sha256": table["sha256"],
                                   "publishedAt": family["publishedAt"]},
            "archive": {"release_id": release, "files": {stem: entries[f"{stem}.parquet"]["sha256"] for stem in ARCHIVE}}}
    return pins, paths


def _rows(connection, sql: str) -> list[dict]:
    cursor = connection.execute(sql)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


def compare(paths: dict[str, Path]) -> dict:
    c = duckdb.connect()
    c.execute("SET memory_limit='2GB'")
    p = {name: str(path).replace("'", "''") for name, path in paths.items()}
    # One projected pass per file into temp tables; every measure below reads these.
    c.execute(f"""CREATE TEMP TABLE cm AS SELECT congress, chamber, try_cast(event_id AS BIGINT) AS event_id, event_id AS
        event_text, title, meeting_type, meeting_status, meeting_date, witness_count, witness_document_count,
        meeting_document_count, document_urls, meeting_documents, witness_documents
        FROM read_parquet('{p['committee_meetings']}')""")
    c.execute(f"""CREATE TEMP TABLE m AS SELECT event_id, chamber, title, meeting_type, status, meeting_date,
        len(transcripts) AS transcripts FROM read_parquet('{p['meetings']}')""")
    c.execute(f"CREATE TEMP TABLE w AS SELECT witness_key, event_id FROM read_parquet('{p['witnesses']}')")
    # list_row is virtual: the zero-based row position (DATA-MODEL 8.4); the file is unsharded.
    c.execute(f"""CREATE TEMP TABLE l AS SELECT file_row_number::INTEGER AS list_row, meeting_keys, witness_keys,
        document_kind, format, title, congress FROM read_parquet('{p['list']}', file_row_number=true)""")
    # Unnest each key list once into (key, list_row) pairs; a per-meeting list_contains would scan list per meeting.
    c.execute("CREATE TEMP TABLE lm AS SELECT DISTINCT unnest(meeting_keys) AS key, list_row FROM l")
    c.execute("CREATE TEMP TABLE lw AS SELECT DISTINCT unnest(witness_keys) AS key, list_row FROM l")
    report: dict = {}
    report["rows"] = _rows(c, """SELECT (SELECT count(*) FROM cm) AS committee_meetings,
        (SELECT count(*) FROM cm WHERE event_id IS NULL) AS committee_meetings_non_numeric_event_id,
        (SELECT count(DISTINCT event_text) FROM cm) AS committee_meetings_distinct_event_id,
        (SELECT count(*) FROM m) AS archive_meetings, (SELECT count(DISTINCT event_id) FROM m) AS archive_distinct_event_id""")[0]
    report["coverage_committee_meetings_to_archive"] = _rows(c, """SELECT cm.chamber, count(*) AS rows,
        count(m.event_id) AS in_archive, count(*) - count(m.event_id) AS missing,
        list(cm.event_text ORDER BY cm.event_text) FILTER (WHERE m.event_id IS NULL) AS missing_event_ids,
        count(*) FILTER (WHERE m.event_id IS NOT NULL AND m.chamber IS NULL) AS archive_chamber_null
        FROM cm LEFT JOIN m USING (event_id) GROUP BY ALL ORDER BY 1""")
    window = c.execute("SELECT min(meeting_date)::DATE, max(meeting_date)::DATE FROM cm").fetchall()[0]
    report["coverage_archive_to_committee_meetings"] = _rows(c, f"""SELECT m.chamber, count(*) AS rows,
        count(cm.event_id) AS in_committee_meetings,
        count(*) FILTER (WHERE cm.event_id IS NULL AND m.meeting_date BETWEEN '{window[0]}' AND '{window[1]}')
            AS absent_within_committee_meetings_date_window,
        count(*) FILTER (WHERE cm.event_id IS NULL AND m.meeting_date < '{window[0]}') AS absent_before_window,
        count(*) FILTER (WHERE m.meeting_date IS NULL) AS no_meeting_date
        FROM m LEFT JOIN cm USING (event_id) GROUP BY ALL ORDER BY 1""")
    report["committee_meetings_date_window"] = [str(window[0]), str(window[1])]
    report["field_disagreement_on_matched_rows"] = _rows(c, """SELECT
        count(*) AS matched,
        count(*) FILTER (WHERE m.chamber IS DISTINCT FROM nullif(cm.chamber, 'nochamber')) AS chamber,
        count(*) FILTER (WHERE m.title IS DISTINCT FROM cm.title) AS title,
        count(*) FILTER (WHERE m.meeting_type IS DISTINCT FROM cm.meeting_type) AS meeting_type,
        count(*) FILTER (WHERE m.status IS DISTINCT FROM cm.meeting_status) AS status,
        count(*) FILTER (WHERE m.meeting_date IS DISTINCT FROM try_cast(cm.meeting_date AS TIMESTAMPTZ)::DATE)
            AS meeting_date_vs_utc_date
        FROM cm JOIN m USING (event_id)""")[0]
    report["meeting_type_pairs"] = _rows(c, """SELECT cm.chamber, cm.meeting_type AS committee_meetings,
        m.meeting_type AS archive, count(*) AS rows FROM cm JOIN m USING (event_id)
        WHERE m.meeting_type IS DISTINCT FROM cm.meeting_type GROUP BY ALL ORDER BY rows DESC""")
    report["witness_meetings_both_ways"] = _rows(c, """WITH a AS (SELECT DISTINCT event_id FROM w WHERE event_id IS NOT NULL)
        SELECT cm.chamber, count(*) FILTER (WHERE cm.witness_count > 0) AS committee_meetings_with_witnesses,
        count(a.event_id) AS archive_with_witnesses,
        count(*) FILTER (WHERE cm.witness_count > 0 AND a.event_id IS NULL) AS only_committee_meetings,
        count(*) FILTER (WHERE coalesce(cm.witness_count, 0) = 0 AND a.event_id IS NOT NULL) AS only_archive
        FROM cm LEFT JOIN a USING (event_id) GROUP BY ALL ORDER BY 1""")
    report["meeting_documents_per_event"] = _rows(c, """WITH a AS (SELECT try_cast(key AS BIGINT) AS event_id,
            count(*) AS archive_documents FROM lm GROUP BY 1)
        SELECT cm.chamber, count(*) FILTER (WHERE cm.meeting_document_count + cm.witness_document_count > 0)
            AS committee_meetings_with_documents,
        count(*) FILTER (WHERE a.archive_documents > 0) AS archive_with_meeting_key_documents,
        count(*) FILTER (WHERE cm.meeting_document_count + cm.witness_document_count > 0 AND a.event_id IS NULL)
            AS documents_only_in_committee_meetings,
        sum(cm.meeting_document_count + cm.witness_document_count) AS committee_meetings_documents,
        sum(a.archive_documents) AS archive_documents
        FROM cm LEFT JOIN a USING (event_id) GROUP BY ALL ORDER BY 1""")
    # Offered URLs, three ways: exact sources.url, the DATA-MODEL 8.2 lookup key in urls.parquet, and whether
    # the list row found by that key names the offering meeting in meeting_keys (directly or via a witness).
    offered = c.execute("""SELECT DISTINCT cm.chamber, cm.event_id, u AS url FROM cm,
        unnest(list_concat(coalesce(cm.document_urls, []),
                           [d.url FOR d IN coalesce(cm.meeting_documents, [])],
                           [d.url FOR d IN coalesce(cm.witness_documents, [])])) AS t(u)
        WHERE u IS NOT NULL""").fetchall()
    # Keys are computed in Python once per URL and handed over as one Arrow batch (row inserts took a minute).
    c.register("offered_keys", pa.table({"chamber": [row[0] for row in offered], "event_id": [row[1] for row in offered],
                             "url": [row[2] for row in offered],
                             "lookup_key": [archive_release.lookup_key(row[2]) for row in offered]}))
    c.execute("CREATE TEMP TABLE offered AS SELECT * FROM offered_keys")
    c.execute(f"CREATE TEMP TABLE s AS SELECT DISTINCT url FROM read_parquet('{p['sources']}')")
    c.execute(f"CREATE TEMP TABLE u AS SELECT lookup_key, list_row FROM read_parquet('{p['urls']}')")
    report["offered_urls"] = _rows(c, """WITH k AS (SELECT DISTINCT o.chamber, o.event_id, o.url, u.list_row FROM offered o
            JOIN u USING (lookup_key)),
        linked AS (SELECT DISTINCT k.chamber, k.url FROM k WHERE EXISTS (SELECT 1 FROM lm
            WHERE lm.list_row = k.list_row AND lm.key = k.event_id::VARCHAR)
            OR EXISTS (SELECT 1 FROM lw JOIN w ON w.witness_key = lw.key
            WHERE lw.list_row = k.list_row AND w.event_id = k.event_id))
        SELECT o.chamber, count(DISTINCT o.url) AS distinct_urls,
        count(DISTINCT o.url) FILTER (WHERE s.url IS NOT NULL) AS exact_in_sources,
        count(DISTINCT o.url) FILTER (WHERE o.lookup_key IN (SELECT lookup_key FROM u)) AS resolved_by_lookup_key,
        (SELECT count(*) FROM linked WHERE linked.chamber = o.chamber) AS resolved_to_a_row_naming_the_meeting
        FROM offered o LEFT JOIN s USING (url) GROUP BY o.chamber ORDER BY 1""")
    # The lookup key spelled here must reproduce the archive's own: key every sources.url and look it up.
    c.register("source_keys", pa.table({"lookup_key": [archive_release.lookup_key(url) for (url,) in
                                                       c.execute("SELECT url FROM s").fetchall()]}))
    c.execute("CREATE TEMP TABLE sk AS SELECT * FROM source_keys")
    report["lookup_key_self_check"] = _rows(c, """SELECT count(*) AS source_urls,
        count(*) FILTER (WHERE lookup_key IN (SELECT lookup_key FROM u)) AS found_in_urls FROM sk""")[0]
    c.execute(f"""CREATE TEMP TABLE d AS SELECT file_row_number::INTEGER AS list_row, package_id, saved_body
        FROM read_parquet('{p['detail']}', file_row_number=true)""")
    report["meeting_linked_document_fields"] = _rows(c, """WITH r AS (SELECT DISTINCT list_row FROM lm
            UNION SELECT DISTINCT list_row FROM lw)
        SELECT count(*) AS list_rows, count(l.document_kind) FILTER (WHERE len(l.document_kind) > 0) AS document_kind,
        count(l.format) AS format, count(l.title) AS title, count(l.congress) AS congress,
        count(d.package_id) AS detail_package_id, count(d.saved_body) AS detail_saved_body,
        (SELECT count(*) FROM lm) AS meeting_key_pairs, (SELECT count(DISTINCT key) FROM lm) AS meeting_keys
        FROM r JOIN l USING (list_row) JOIN d USING (list_row)""")[0]
    report["column_destinations"] = DESTINATIONS
    report["state_without_destination"] = [
        "committee_meetings_detail_reads (the per-event detail-read table) and the detail_read / list-only state",
        "Congress.gov meeting_type labels for House rows (the archive states House XML codes)",
        "meeting_documents description, name and documentType (list.title is set on few meeting-linked rows)"]
    report["columns_without_destination"] = sorted(name for name, where in DESTINATIONS.items() if where is None)
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--cache", type=Path, required=True, help="Directory holding (or to receive) the inputs")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    pins, paths = inputs(args.cache)
    report = {"inputs": pins, **compare(paths)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
