#!/usr/bin/env python3
"""Compare the published committee_meetings generation with the pinned archive release, both ways (ss-ikn9).

Retiring spicy-regs' committee_meetings capture needs evidence that the
archive release carries its rows and columns. Each input is read by digest:
committee_meetings by the sha256 the publication index states, the archive
files by the sha256 in the release manifest, whose id is recomputed from its
files (``archive_release.read_manifest``). A file already in ``--cache`` is
reused when its digest matches; otherwise it is downloaded there once. Every
query projects only the columns it reads. The script writes a JSON report and
exits 0 only when ``residuals.has_residuals`` is false (exiting 1 otherwise).
"""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs import archive_release
from spicy_regs.sources import publication

PUBLICATION = "https://data.spicygov.ai"
FAMILY, TABLE = "committee-meetings", "committee_meetings.parquet"
ARCHIVE = ("meetings", "witnesses", "people", "list", "detail", "sources", "urls")
MEETING_DETAIL_DESTINATIONS = {
    "congress": "meetings.congress",
    "meeting_type": "meetings.congress_gov_meeting_type",
    "location_building": "meetings.sittings[].building at the matching start",
    "location_room": "meetings.sittings[].room at the matching start",
    "update_date": "meetings.congress_gov_updated",
}
SUBJECT_REFERENCE_DESTINATIONS = {
    "nomination_references_json": (
        "meetings.subject_keys ({congress}-pn-{number}[-{int(part)}], omitting zero/absent part)"
    ),
    "treaty_references_json": "meetings.subject_keys ({congress}-treaty-{number})",
}
#: Where each committee_meetings column lands in the archive release; None is a column the release does not carry.
DESTINATIONS = {
    "chamber": "meetings.chamber (NULL for nochamber)",
    "event_id": "meetings.event_id (BIGINT)",
    "title": "meetings.title",
    "meeting_status": "meetings.status",
    "meeting_date": "meetings.meeting_date (local DATE) and sittings[].start",
    "committee_system_code": "meetings.committees[].system_code",
    "committee_count": "len(meetings.committees)",
    "committees": "meetings.committees[].system_code; name and url through the committees table by system code",
    "hearing_jacket": "meetings.transcripts[].package_id (the jacket is its trailing digits: CHRG-118hhrg56769 -> 56769)",
    "hearing_jacket_count": "len(meetings.transcripts)",
    "hearing_jackets": "meetings.transcripts[].package_id (the jacket is its trailing digits)",
    "bill_count": "len(meetings.subject_keys) less nominations and treaties",
    "bill_ids": "meetings.subject_keys less nominations (-pn-) and treaties (-treaty-); list.measure_keys per document",
    "witness_count": "count(witnesses) by event_id",
    "witnesses": "witnesses (name, rank, credentials, position, organization, person_key -> people)",
    "witness_document_count": "count(list rows) by witnesses.witness_key in list.witness_keys",
    "witness_documents": "list rows by witness_key: document_kind, format; no source URL column (urls.parquet resolves a URL)",
    "meeting_document_count": "count(list rows) by event_id in list.meeting_keys",
    "meeting_documents": "list rows by meeting key: title, document_kind, format; detail.package_id, saved_body",
    "document_urls": "sources.url and urls.lookup_key -> list_row",
    "videos": "meetings.videos",
    **SUBJECT_REFERENCE_DESTINATIONS,
    **MEETING_DETAIL_DESTINATIONS,
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


def inputs(cache: Path, pinned: dict | None = None) -> tuple[dict, dict[str, Path]]:
    """The input pins and local paths: the live generation and bound release, or exactly ``pinned``.

    ``pinned`` is the ``inputs`` block of an earlier report, so that report can be regenerated from the
    same bytes after live moves on (the generation must still be served or held in ``--cache``).
    """
    if pinned is None:
        family = publication.load_index(PUBLICATION)["families"][FAMILY]
        meetings = {"artifactDigest": family["artifactDigest"], "sha256": family["tables"][TABLE]["sha256"],
                    "publishedAt": family["publishedAt"]}
        release = archive_release.load_pins()["bound"]
    else:
        meetings, release = pinned["committee_meetings"], pinned["archive"]["release_id"]
    digest = meetings["artifactDigest"].removeprefix("sha256:")
    manifest = archive_release.read_manifest(release)
    entries = {item["name"]: item for item in manifest["files"]}
    if pinned is not None and any(entries[f"{stem}.parquet"]["sha256"] != sha for stem, sha in pinned["archive"]["files"].items()):
        raise SystemExit(f"Release {release[:8]} files differ from the pinned report")
    cache = cache / release
    cache.mkdir(parents=True, exist_ok=True)
    paths = {"committee_meetings": _held(cache, f"{digest}-{TABLE}", f"{PUBLICATION}/generations/{FAMILY}/{digest}/{TABLE}",
                                         meetings["sha256"].removeprefix("sha256:"))}
    for stem in ARCHIVE:
        name = f"{stem}.parquet"
        paths[stem] = _held(cache, name, f"{archive_release.ORIGIN}/releases/{release}/{name}", entries[name]["sha256"])
    pins = {"committee_meetings": meetings,
            "archive": {"release_id": release, "files": {stem: entries[f"{stem}.parquet"]["sha256"] for stem in ARCHIVE}}}
    return pins, paths


def _rows(connection, sql: str) -> list[dict]:
    cursor = connection.execute(sql)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names, row)) for row in cursor.fetchall()]


#: Each mapped list column on a matched meeting as two normalized sets (committee_meetings, archive).
#: Values are compared as trimmed lowercase text; a spelling difference shows up in ``examples``, not as agreement.
#: Two columns are brought to one key first, as each side spells it on live data: committee_meetings states the
#: GPO jacket number ('56769') where the archive states the package id ('CHRG-118hhrg56769'), and the archive's
#: subject_keys also carry nominations ('119-pn-12', '119-pn-12-1') and treaties ('119-treaty-3'), which bill_ids
#: never holds (committee_meetings states those in nomination/treaty_references_json).
_JACKET = "regexp_extract({}, '([0-9]+)$', 1)"
VALUE_PAIRS = {
    "committees": ("[c.systemCode FOR c IN cm.committees]", "[c.system_code FOR c IN m.committees]"),
    "hearing_jackets": (f"[{_JACKET.format('j')} FOR j IN cm.hearing_jackets]",
                        f"[{_JACKET.format('t.package_id')} FOR t IN m.transcripts]"),
    "videos": ("[v.url FOR v IN cm.videos]", "[v.url FOR v IN m.videos]"),
    "bill_ids": ("cm.bill_ids", "list_filter(m.subject_keys, k -> NOT regexp_matches(k, '-(pn|treaty)-'))"),
    "witness_names": ("[x.name FOR x IN cm.witnesses]", "a.names"),
    "witness_name_position": ("[concat_ws('|', x.name, x.position) FOR x IN cm.witnesses]", "a.name_positions"),
    "witness_name_organization": ("[concat_ws('|', x.name, x.organization) FOR x IN cm.witnesses]",
                                  "a.name_organizations"),
}
_HONORIFIC_RE = (
    r"^((the )?(honorable|hon)|mr|ms|mrs|miss|dr|doctor|prof|professor|rev|rabbi|fr|father|"
    r"sr|sister|rep|representative|sen|senator|del|commish|ambassador|secretary|judge|"
    r"justice|governor|gov|mayor)\s+"
)
_CM_NOMINATION_LIST = (
    "[concat(json_extract_string(r, '$.congress'), '-pn-', json_extract_string(r, '$.number'), "
    "CASE WHEN try_cast(json_extract_string(r, '$.part') AS INTEGER) > 0 "
    "THEN '-' || try_cast(json_extract_string(r, '$.part') AS INTEGER)::VARCHAR ELSE '' END) "
    "FOR r IN CAST(coalesce(nullif(trim(cm.nomination_references_json), ''), '[]') AS JSON[])]"
)
_CM_TREATY_LIST = (
    "[concat(json_extract_string(r, '$.congress'), '-treaty-', json_extract_string(r, '$.number')) "
    "FOR r IN CAST(coalesce(nullif(trim(cm.treaty_references_json), ''), '[]') AS JSON[])]"
)
_CM_CANONICAL_WITNESS_LIST = (
    "[trim(regexp_replace(trim(regexp_replace(regexp_replace(lower(coalesce(x.name, '')), "
    r"'[,.;]+', ' ', 'g'), '\s+', ' ', 'g')), "
    f"'{_HONORIFIC_RE}', '')) FOR x IN coalesce(cm.witnesses, [])]"
)
REFERENCE_VALUE_PAIRS = {
    "nominations": (_CM_NOMINATION_LIST, "list_filter(m.subject_keys, k -> regexp_matches(k, '-pn-'))"),
    "treaties": (_CM_TREATY_LIST, "list_filter(m.subject_keys, k -> regexp_matches(k, '-treaty-'))"),
}
CANONICAL_WITNESS_PAIR = {
    "canonical_witness_names": (_CM_CANONICAL_WITNESS_LIST, "a.canonical_names"),
}


def mapped_value_agreement(c, *, has_reference_columns: bool = False, has_witness_parts: bool = False) -> dict:
    """Per mapped list column, matched meetings whose sets agree, are contained either way, or are one-sided."""
    pairs = dict(VALUE_PAIRS)
    if has_reference_columns:
        pairs.update(REFERENCE_VALUE_PAIRS)
    if has_witness_parts:
        pairs.update(CANONICAL_WITNESS_PAIR)
    norm = "list_sort(list_distinct(list_filter([lower(trim(CAST(v AS VARCHAR))) FOR v IN coalesce({}, [])], v -> v <> '')))"
    columns = ", ".join(f"{norm.format(left)} AS {name}_cm, {norm.format(right)} AS {name}_archive"
                        for name, (left, right) in pairs.items())
    canonical_expr = (
        ", list(trim(regexp_replace(regexp_replace(lower(coalesce(concat_ws(' ', nullif(rank, ''), name, nullif(credentials, '')), '')), "
        r"'[,.;]+', ' ', 'g'), '\s+', ' ', 'g'))) FILTER (WHERE superseded_by IS NULL) AS canonical_names"
        if has_witness_parts else ""
    )
    c.execute(f"""CREATE TEMP TABLE agreement AS
        WITH a AS (SELECT event_id, list(name) AS names, list(concat_ws('|', name, position)) AS name_positions,
                   list(concat_ws('|', name, organization)) AS name_organizations{canonical_expr}
                   FROM w WHERE event_id IS NOT NULL GROUP BY 1)
        SELECT cm.event_id, {columns} FROM cm JOIN m USING (event_id) LEFT JOIN a USING (event_id)""")
    report = {}
    for name in pairs:
        x, y = f"{name}_cm", f"{name}_archive"
        report[name] = _rows(c, f"""SELECT
            count(*) FILTER (WHERE len({x}) > 0 OR len({y}) > 0) AS either_nonempty,
            count(*) FILTER (WHERE len({x}) > 0 AND {x} = {y}) AS equal,
            count(*) FILTER (WHERE len({x}) > 0 AND {x} <> {y} AND list_has_all({y}, {x})) AS archive_superset,
            count(*) FILTER (WHERE len({y}) > 0 AND {x} <> {y} AND list_has_all({x}, {y})) AS committee_meetings_superset,
            count(*) FILTER (WHERE len({x}) > 0 AND len({y}) > 0 AND NOT list_has_all({y}, {x})
                AND NOT list_has_all({x}, {y})) AS differ,
            count(*) FILTER (WHERE len({x}) > 0 AND len({y}) = 0) AS only_committee_meetings,
            count(*) FILTER (WHERE len({x}) = 0 AND len({y}) > 0) AS only_archive,
            (SELECT list({{'event_id': event_id, 'committee_meetings': {x}, 'archive': {y}}} ORDER BY event_id)[:3]
                FROM agreement WHERE {x} <> {y}) AS examples
            FROM agreement""")[0]
    return report


def meeting_detail_agreement(c) -> dict:
    """Compare locations only when the upstream start identifies one archive sitting."""
    c.execute("""CREATE TEMP MACRO stated_location(value) AS
        CASE WHEN regexp_full_match(trim(value), '-+') OR lower(trim(value)) = 'select one'
             THEN NULL ELSE nullif(trim(value), '') END""")
    c.execute("""CREATE TEMP TABLE meeting_details AS
        WITH paired AS (
            SELECT m.event_id, cm.chamber, try_cast(cm.congress AS INTEGER) AS upstream_congress,
                m.congress AS archive_congress, cm.meeting_type AS upstream_type,
                m.congress_gov_meeting_type AS archive_type,
                try_cast(cm.update_date AS TIMESTAMPTZ) AS upstream_updated,
                m.congress_gov_updated AS archive_updated, m.house_xml_updated,
                CASE WHEN regexp_matches(cm.meeting_date::VARCHAR, '[Tt ][0-9]{2}:.*([Zz]|[+-][0-9]{2}:[0-9]{2})$')
                     THEN try_cast(cm.meeting_date AS TIMESTAMPTZ) END AS upstream_start,
                cm.location_building AS upstream_building, cm.location_room AS upstream_room,
                m.sittings
            FROM cm JOIN m USING (event_id)
        ), matched AS (
            SELECT *, list_filter(sittings, s -> s.start = upstream_start) AS same_start,
                list_count(list_transform(sittings, s -> s.start)) AS known_starts
            FROM paired
        ), comparison AS (
            SELECT * EXCLUDE (sittings, same_start, known_starts),
            len(sittings) AS archive_sitting_count,
            CASE
                WHEN upstream_updated IS NULL AND archive_updated IS NULL THEN 'both_updates_absent'
                WHEN upstream_updated IS NULL THEN 'upstream_update_absent'
                WHEN archive_updated IS NULL THEN 'archive_update_absent'
                WHEN archive_updated > upstream_updated THEN 'archive_newer'
                WHEN archive_updated < upstream_updated THEN 'upstream_newer'
                ELSE 'same_update'
            END AS version_state,
            CASE
                WHEN upstream_start IS NULL THEN 'upstream_start_absent'
                WHEN coalesce(known_starts, 0) = 0 THEN 'archive_start_absent'
                WHEN len(same_start) = 0 THEN 'different_start'
                WHEN len(same_start) > 1 THEN 'ambiguous_start'
                ELSE 'matched'
            END AS sitting_state,
            CASE WHEN len(same_start) = 1 THEN same_start[1].building END AS archive_building,
            CASE WHEN len(same_start) = 1 THEN same_start[1].room END AS archive_room,
            list_transform(sittings, s -> s.start) AS archive_starts
            FROM matched
        )
        SELECT *,
            sitting_state = 'matched' AND
                stated_location(upstream_building) IS DISTINCT FROM stated_location(archive_building)
                AS building_disagreement,
            sitting_state = 'matched' AND
                stated_location(upstream_room) IS DISTINCT FROM stated_location(archive_room)
                AS room_disagreement
        FROM comparison""")
    return {
        "counts": _rows(c, """SELECT count(*) AS matched,
            count(*) FILTER (WHERE upstream_congress IS DISTINCT FROM archive_congress) AS congress,
            count(*) FILTER (WHERE upstream_updated IS DISTINCT FROM archive_updated) AS update_date,
            count(*) FILTER (WHERE upstream_type IS DISTINCT FROM archive_type) AS meeting_type,
            count(*) FILTER (WHERE building_disagreement) AS location_building,
            count(*) FILTER (WHERE room_disagreement) AS location_room
            FROM meeting_details""")[0],
        "source_and_sitting_states": _rows(c, """SELECT version_state, sitting_state, count(*) AS rows
            FROM meeting_details GROUP BY ALL ORDER BY 1,2"""),
        "residuals": _rows(c, """SELECT * FROM meeting_details
            WHERE version_state <> 'same_update' OR sitting_state <> 'matched'
                OR upstream_congress IS DISTINCT FROM archive_congress
                OR upstream_type IS DISTINCT FROM archive_type
                OR building_disagreement OR room_disagreement
            ORDER BY event_id"""),
        "location_normalization_differences": _rows(c, """SELECT event_id,
            upstream_building, archive_building, upstream_room, archive_room FROM meeting_details
            WHERE sitting_state = 'matched' AND (
                (NOT building_disagreement AND upstream_building IS DISTINCT FROM archive_building) OR
                (NOT room_disagreement AND upstream_room IS DISTINCT FROM archive_room))
            ORDER BY event_id"""),
    }


def senate_witness_links_check(c, *, has_witness_link_cols: bool) -> dict:
    """Measure Senate witness-to-meeting, witness-to-document, and witness-to-person integrity via one unnest."""
    orphan_doc_keys = c.execute(
        "SELECT count(DISTINCT lw.key) FROM lw LEFT JOIN w ON w.witness_key = lw.key WHERE w.witness_key IS NULL"
    ).fetchone()[0]
    if not has_witness_link_cols:
        senate_rows = _rows(c, """
            WITH wd AS (SELECT key AS witness_key, count(*) AS doc_links FROM lw GROUP BY 1)
            SELECT
                count(*) AS total_witnesses,
                count(DISTINCT w.witness_key) FILTER (WHERE wd.doc_links > 0) AS witnesses_with_documents,
                coalesce(sum(wd.doc_links), 0)::BIGINT AS document_links,
                count(DISTINCT w.event_id) FILTER (WHERE wd.doc_links > 0) AS senate_meetings_with_witness_docs
            FROM w JOIN m ON m.event_id = w.event_id LEFT JOIN wd ON wd.witness_key = w.witness_key
            WHERE m.chamber = 'senate'""")[0]
        return {
            "schema_supports_person_links": False,
            "orphan_document_witness_keys": orphan_doc_keys,
            "senate_meeting_witness_links": senate_rows,
            "by_chamber_and_source": [],
        }

    c.execute("""CREATE TEMP TABLE witness_link_detail AS
        WITH wd AS (
            SELECT lw.key AS witness_key, count(*) AS doc_links
            FROM lw
            GROUP BY lw.key
        )
        SELECT
            w.witness_key,
            w.source,
            w.event_id,
            m.chamber,
            w.superseded_by IS NULL AS is_canonical,
            w.event_id IS NOT NULL AND m.event_id IS NULL AS unlinked_missing_meeting,
            w.person_key IS NOT NULL AS has_person_key,
            w.person_key,
            w.person_key IS NOT NULL AND pe.person_key IS NOT NULL AS person_resolved,
            w.person_key IS NOT NULL AND pe.person_key IS NULL AS unresolved_person_key,
            coalesce(wd.doc_links, 0) AS doc_links
        FROM w
        LEFT JOIN m ON m.event_id = w.event_id
        LEFT JOIN p AS pe ON pe.person_key = w.person_key
        LEFT JOIN wd ON wd.witness_key = w.witness_key""")
    summary_sql = """
        SELECT
            count(*) AS total_witnesses,
            count(*) FILTER (WHERE is_canonical) AS canonical_witnesses,
            count(*) FILTER (WHERE unlinked_missing_meeting) AS unlinked_missing_meeting,
            count(*) FILTER (WHERE has_person_key) AS with_person_key,
            count(*) FILTER (WHERE person_resolved) AS person_key_resolved,
            count(*) FILTER (WHERE unresolved_person_key) AS unresolved_person_keys,
            count(*) FILTER (WHERE is_canonical AND NOT has_person_key) AS canonical_without_person_key,
            count(DISTINCT witness_key) FILTER (WHERE doc_links > 0) AS witnesses_with_documents,
            coalesce(sum(doc_links), 0)::BIGINT AS document_links,
            count(DISTINCT witness_key) FILTER (WHERE doc_links > 0 AND person_resolved) AS witnesses_with_doc_and_person,
            count(DISTINCT person_key) FILTER (WHERE doc_links > 0 AND person_resolved) AS distinct_people_with_documents,
            count(DISTINCT event_id) FILTER (WHERE doc_links > 0 AND event_id IS NOT NULL) AS distinct_meetings_with_witness_docs
        FROM witness_link_detail
    """
    senate_meeting = _rows(c, summary_sql + " WHERE chamber = 'senate'")[0]
    senate_page = _rows(c, summary_sql + " WHERE source = 'senate_page'")[0]
    senate_scope = _rows(c, summary_sql + " WHERE chamber = 'senate' OR source = 'senate_page'")[0]
    all_witnesses = _rows(c, summary_sql)[0]
    senate_docs = _rows(c, """
        SELECT
            count(DISTINCT lw.list_row) FILTER (WHERE m.chamber = 'senate') AS senate_meeting_distinct_documents,
            count(DISTINCT lw.list_row) FILTER (WHERE w.source = 'senate_page') AS senate_page_distinct_documents,
            count(DISTINCT lw.list_row) FILTER (
                WHERE m.chamber = 'senate' OR w.source = 'senate_page'
            ) AS senate_scope_distinct_documents
        FROM lw
        LEFT JOIN w ON w.witness_key = lw.key
        LEFT JOIN m ON m.event_id = w.event_id
    """)[0]
    by_group = _rows(c, """
        SELECT
            chamber,
            source,
            count(*) AS total_witnesses,
            count(*) FILTER (WHERE is_canonical) AS canonical_witnesses,
            count(*) FILTER (WHERE unlinked_missing_meeting) AS unlinked_missing_meeting,
            count(*) FILTER (WHERE has_person_key) AS with_person_key,
            count(*) FILTER (WHERE person_resolved) AS person_key_resolved,
            count(*) FILTER (WHERE unresolved_person_key) AS unresolved_person_keys,
            count(*) FILTER (WHERE is_canonical AND NOT has_person_key) AS canonical_without_person_key,
            count(DISTINCT witness_key) FILTER (WHERE doc_links > 0) AS witnesses_with_documents,
            coalesce(sum(doc_links), 0)::BIGINT AS document_links,
            count(DISTINCT witness_key) FILTER (WHERE doc_links > 0 AND person_resolved) AS witnesses_with_doc_and_person,
            count(DISTINCT person_key) FILTER (WHERE doc_links > 0 AND person_resolved) AS distinct_people_with_documents
        FROM witness_link_detail
        WHERE chamber = 'senate' OR source = 'senate_page'
        GROUP BY chamber, source
        ORDER BY chamber NULLS LAST, source NULLS LAST
    """)
    return {
        "schema_supports_person_links": True,
        "orphan_document_witness_keys": orphan_doc_keys,
        "senate_meeting_witness_links": {
            **senate_meeting,
            "distinct_documents": senate_docs["senate_meeting_distinct_documents"],
        },
        "senate_page_overall": {
            **senate_page,
            "distinct_documents": senate_docs["senate_page_distinct_documents"],
        },
        "senate_scope_overall": {
            **senate_scope,
            "distinct_documents": senate_docs["senate_scope_distinct_documents"],
        },
        "all_witnesses_integrity": {
            "total_witnesses": all_witnesses["total_witnesses"],
            "unlinked_missing_meeting": all_witnesses["unlinked_missing_meeting"],
            "unresolved_person_keys": all_witnesses["unresolved_person_keys"],
        },
        "by_chamber_and_source": by_group,
    }


def summarize_residuals(report: dict) -> dict:
    """Explicit residual ledger used to decide exit status (zero residuals == exit 0)."""
    missing_meetings = sum(row["missing"] for row in report["coverage_committee_meetings_to_archive"])
    missing_event_ids = sorted(
        eid for row in report["coverage_committee_meetings_to_archive"] for eid in (row.get("missing_event_ids") or [])
    )
    absent_in_window = sum(
        row["absent_within_committee_meetings_date_window"]
        for row in report["coverage_archive_to_committee_meetings"]
    )
    field_diffs = {
        k: v
        for k, v in report["field_disagreement_on_matched_rows"].items()
        if k != "matched" and v is not None and v > 0
    }
    detail_residuals = len(report["meeting_detail_agreement"]["residuals"]) if report.get("meeting_detail_agreement") else 0
    mapped_breakdown = {
        name: {
            "differ": row["differ"],
            "only_committee_meetings": row["only_committee_meetings"],
            "committee_meetings_superset": row["committee_meetings_superset"],
            "archive_superset": row["archive_superset"],
            "only_archive": row["only_archive"],
        }
        for name, row in report["mapped_value_agreement"].items()
    }
    witness_comparison_key = (
        "canonical_witness_names" if "canonical_witness_names" in mapped_breakdown else "witness_names"
    )
    primary_mapped_keys = [
        key for key in ("committees", "hearing_jackets", "videos", "bill_ids", "nominations", "treaties", witness_comparison_key)
        if key in mapped_breakdown
    ]
    mapped_disagreement_counts = {
        key: mapped_breakdown[key]["differ"]
        + mapped_breakdown[key]["only_committee_meetings"]
        + mapped_breakdown[key]["committee_meetings_superset"]
        for key in primary_mapped_keys
    }
    witness_only_cm = sum(row["only_committee_meetings"] for row in report["witness_meetings_both_ways"])
    docs_only_cm = sum(row["documents_only_in_committee_meetings"] for row in report["meeting_documents_per_event"])
    offered_urls_unresolved = sum(
        row["distinct_urls"] - row["resolved_by_lookup_key"] for row in report["offered_urls"]
    )
    lookup_self_failures = (
        report["lookup_key_self_check"]["source_urls"] - report["lookup_key_self_check"]["found_in_urls"]
    )
    senate = report["senate_witness_links"]
    senate_integrity = {
        "orphan_document_witness_keys": senate["orphan_document_witness_keys"],
        "unresolved_person_keys": senate["all_witnesses_integrity"]["unresolved_person_keys"]
        if senate["schema_supports_person_links"] else 0,
        "unlinked_missing_meeting": senate["all_witnesses_integrity"]["unlinked_missing_meeting"]
        if senate["schema_supports_person_links"] else 0,
    }
    columns_without_dest = list(report["columns_without_destination"])
    total_count = (
        missing_meetings
        + absent_in_window
        + sum(field_diffs.values())
        + detail_residuals
        + sum(mapped_disagreement_counts.values())
        + witness_only_cm
        + docs_only_cm
        + offered_urls_unresolved
        + lookup_self_failures
        + sum(senate_integrity.values())
        + len(columns_without_dest)
    )
    return {
        "has_residuals": total_count > 0,
        "total_residual_count": total_count,
        "missing_archive_meetings": missing_meetings,
        "missing_archive_event_ids": missing_event_ids,
        "absent_archive_meetings_in_cm_window": absent_in_window,
        "field_disagreements": field_diffs,
        "meeting_detail_residuals": detail_residuals,
        "mapped_value_disagreements": {k: v for k, v in mapped_disagreement_counts.items() if v > 0},
        "mapped_value_breakdown": mapped_breakdown,
        "witness_meetings_only_in_committee_meetings": witness_only_cm,
        "document_meetings_only_in_committee_meetings": docs_only_cm,
        "offered_urls_unresolved": offered_urls_unresolved,
        "lookup_key_self_check_failures": lookup_self_failures,
        "senate_witness_integrity_failures": senate_integrity,
        "columns_without_destination_count": len(columns_without_dest),
        "columns_without_destination": columns_without_dest,
    }


def compare(paths: dict[str, Path]) -> dict:
    c = duckdb.connect()
    c.execute("SET memory_limit='2GB'")
    c.execute("SET TimeZone='UTC'")
    p = {name: str(path).replace("'", "''") for name, path in paths.items()}
    m_schema = pq.read_schema(paths["meetings"])
    cm_schema = pq.read_schema(paths["committee_meetings"])
    w_schema = pq.read_schema(paths["witnesses"])

    has_meeting_details = {"congress", "congress_gov_updated", "congress_gov_meeting_type",
                           "house_xml_updated", "sittings"}.issubset(m_schema.names) and {
                               "room", "building"
                           }.issubset(f.name for f in m_schema.field("sittings").type.value_type)
    has_reference_columns = {"nomination_references_json", "treaty_references_json"}.issubset(cm_schema.names)
    has_witness_parts = {"superseded_by", "rank", "credentials"}.issubset(w_schema.names)
    has_witness_link_cols = (
        "people" in paths and {"source", "person_key", "superseded_by", "witness_type"}.issubset(w_schema.names)
    )

    cm_extra_columns = "".join([
        ", update_date, location_building, location_room" if has_meeting_details else "",
        ", nomination_references_json, treaty_references_json" if has_reference_columns else "",
    ])
    m_detail_columns = (", congress, congress_gov_updated, congress_gov_meeting_type, house_xml_updated, sittings"
                        if has_meeting_details else "")
    w_extra_columns = "".join([
        ", source" if "source" in w_schema.names else "",
        ", rank" if "rank" in w_schema.names else "",
        ", credentials" if "credentials" in w_schema.names else "",
        ", witness_type" if "witness_type" in w_schema.names else "",
        ", superseded_by" if "superseded_by" in w_schema.names else "",
        ", person_key" if "person_key" in w_schema.names else "",
    ])
    # One projected pass per file into temp tables; every measure below reads these.
    c.execute(f"""CREATE TEMP TABLE cm AS SELECT congress, chamber, try_cast(event_id AS BIGINT) AS event_id, event_id AS
        event_text, title, meeting_type, meeting_status, meeting_date, witness_count, witness_document_count,
        meeting_document_count, document_urls, meeting_documents, witness_documents, committees, hearing_jackets,
        witnesses, videos, bill_ids{cm_extra_columns} FROM read_parquet('{p['committee_meetings']}')""")
    c.execute(f"""CREATE TEMP TABLE m AS SELECT event_id, chamber, title, status, meeting_date,
        committees, transcripts, videos, subject_keys{m_detail_columns} FROM read_parquet('{p['meetings']}')""")
    c.execute(f"""CREATE TEMP TABLE w AS SELECT witness_key, event_id, name, position, organization{w_extra_columns}
        FROM read_parquet('{p['witnesses']}')""")
    if has_witness_link_cols:
        c.execute(f"CREATE TEMP TABLE p AS SELECT person_key, bioguide_id, grouping_basis FROM read_parquet('{p['people']}')")
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
        count(*) FILTER (WHERE m.status IS DISTINCT FROM cm.meeting_status) AS status,
        count(*) FILTER (WHERE m.meeting_date IS DISTINCT FROM try_cast(cm.meeting_date AS TIMESTAMPTZ)::DATE)
            AS meeting_date_vs_utc_date
        FROM cm JOIN m USING (event_id)""")[0]
    report["mapped_value_agreement"] = mapped_value_agreement(
        c, has_reference_columns=has_reference_columns, has_witness_parts=has_witness_parts,
    )
    details = meeting_detail_agreement(c) if has_meeting_details else None
    report["meeting_detail_agreement"] = details
    report["field_disagreement_on_matched_rows"]["meeting_type"] = details["counts"]["meeting_type"] if details else None
    report["meeting_type_pairs"] = _rows(c, """SELECT cm.chamber, cm.meeting_type AS committee_meetings,
        m.congress_gov_meeting_type AS archive, count(*) AS rows FROM cm JOIN m USING (event_id)
        WHERE m.congress_gov_meeting_type IS DISTINCT FROM cm.meeting_type
        GROUP BY ALL ORDER BY rows DESC, cm.chamber, committee_meetings, archive""") if has_meeting_details else []
    report["witness_meetings_both_ways"] = _rows(c, """WITH a AS (SELECT DISTINCT event_id FROM w WHERE event_id IS NOT NULL)
        SELECT cm.chamber, count(*) FILTER (WHERE cm.witness_count > 0) AS committee_meetings_with_witnesses,
        count(a.event_id) AS archive_with_witnesses,
        count(*) FILTER (WHERE cm.witness_count > 0 AND a.event_id IS NULL) AS only_committee_meetings,
        count(*) FILTER (WHERE coalesce(cm.witness_count, 0) = 0 AND a.event_id IS NOT NULL) AS only_archive
        FROM cm LEFT JOIN a USING (event_id) GROUP BY ALL ORDER BY 1""")
    report["senate_witness_links"] = senate_witness_links_check(c, has_witness_link_cols=has_witness_link_cols)
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
                                                       c.execute("SELECT url FROM s WHERE url IS NOT NULL").fetchall()]}))
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
    destinations = DESTINATIONS if has_meeting_details else DESTINATIONS | dict.fromkeys(MEETING_DETAIL_DESTINATIONS)
    if not has_reference_columns:
        destinations = destinations | dict.fromkeys(SUBJECT_REFERENCE_DESTINATIONS)
    report["column_destinations"] = destinations
    report["state_without_destination"] = [
        "committee_meetings_detail_reads (the per-event detail-read table) and the detail_read / list-only state",
        "meeting_documents description, name and documentType (list.title is set on few meeting-linked rows)"]
    report["columns_without_destination"] = sorted(name for name, where in destinations.items() if where is None)
    report["residuals"] = summarize_residuals(report)
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").split("\n\n")[0])
    parser.add_argument("--cache", type=Path, required=True, help="Directory holding (or to receive) the inputs")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--inputs-from", type=Path,
                        help="An earlier report whose committee_meetings generation and archive release to re-read")
    args = parser.parse_args(argv)
    pins, paths = inputs(args.cache, json.loads(args.inputs_from.read_text())["inputs"] if args.inputs_from else None)
    report = {"inputs": pins, **compare(paths)}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, default=str) + "\n")
    return 1 if report["residuals"]["has_residuals"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
