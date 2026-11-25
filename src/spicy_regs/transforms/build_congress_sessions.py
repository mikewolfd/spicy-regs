"""Congressional sessions and chamber party control from reconciled House and Senate history pages.

SpicyDocs owns HTML parsing, session reconciliation, and majority rules.
This transform materializes exact Arrow-typed Parquet tables while preserving
printed dates, ``adjournment_status``, ``correction_reason``, and ``None``
``majority_party`` on plurality Congresses where no organizing caucus is stated.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

SESSIONS = "congress_sessions"
PARTY_CONTROL = "congress_party_control"

SESSIONS_SCHEMA = pa.schema([
    ("congress", pa.int32()),
    ("session", pa.string()),
    ("begins", pa.date32()),
    ("adjourned", pa.date32()),
    ("adjournment_status", pa.string()),
    ("house_session", pa.string()),
    ("house_begins", pa.date32()),
    ("house_adjourned", pa.date32()),
    ("senate_session", pa.string()),
    ("senate_begins", pa.date32()),
    ("senate_adjourned", pa.date32()),
    ("disagreement_kind", pa.string()),
    ("correction_reason", pa.string()),
    ("house_source_url", pa.string()),
    ("house_source_sha256", pa.string()),
    ("house_observed_at", pa.string()),
    ("senate_source_url", pa.string()),
    ("senate_source_sha256", pa.string()),
    ("senate_observed_at", pa.string()),
])

PARTY_CONTROL_SCHEMA = pa.schema([
    ("chamber", pa.string()),
    ("congress", pa.int32()),
    ("effective_from", pa.date32()),
    ("effective_to", pa.date32()),
    ("majority_party", pa.string()),
    ("majority_seats", pa.int32()),
    ("minority_party", pa.string()),
    ("minority_seats", pa.int32()),
    ("other", pa.string()),
    ("vacancies", pa.int32()),
    ("total_seats", pa.int32()),
    ("majority_basis", pa.string()),
    ("source_url", pa.string()),
    ("printed_effective_from", pa.date32()),
    ("correction_reason", pa.string()),
    ("source_sha256", pa.string()),
    ("observed_at", pa.string()),
])

SCHEMAS = {
    SESSIONS: SESSIONS_SCHEMA,
    PARTY_CONTROL: PARTY_CONTROL_SCHEMA,
}


def build_congress_sessions(
    *,
    house_sessions_body: bytes,
    house_divisions_body: bytes,
    senate_division_body: bytes,
    house_sessions_source: dict,
    house_divisions_source: dict,
    senate_division_source: dict,
    output_dir: Path,
) -> tuple[Path, Path]:
    """Parse, reconcile, and atomically write ``congress_sessions.parquet`` and ``congress_party_control.parquet``."""
    from spicy_docs.sources.congress.party_divisions import (
        HOUSE_PARTY_DIVISIONS_URL,
        HOUSE_SESSION_DATES_URL,
        SENATE_PARTY_DIVISION_URL,
        parse_house_party_divisions,
        parse_house_session_dates,
        parse_senate_party_division,
        party_control,
        reconcile_sessions,
        senate_sessions,
    )

    house_sessions = parse_house_session_dates(house_sessions_body)
    house_divisions = parse_house_party_divisions(house_divisions_body)
    senate_congresses = parse_senate_party_division(senate_division_body)
    reconciled = reconcile_sessions(house_sessions, senate_sessions(senate_congresses))
    controls = party_control(house_divisions, senate_congresses, house_sessions=house_sessions)

    session_rows = [
        {
            "congress": item.congress,
            "session": item.session,
            "begins": item.begins,
            "adjourned": item.adjourned,
            "adjournment_status": item.adjournment_status,
            "house_session": item.house_session,
            "house_begins": item.house_begins,
            "house_adjourned": item.house_adjourned,
            "senate_session": item.senate_session,
            "senate_begins": item.senate_begins,
            "senate_adjourned": item.senate_adjourned,
            "disagreement_kind": item.disagreement_kind,
            "correction_reason": item.correction_reason,
            "house_source_url": house_sessions_source.get("url", HOUSE_SESSION_DATES_URL),
            "house_source_sha256": house_sessions_source["sha256"],
            "house_observed_at": house_sessions_source["observed_at"],
            "senate_source_url": senate_division_source.get("url", SENATE_PARTY_DIVISION_URL),
            "senate_source_sha256": senate_division_source["sha256"],
            "senate_observed_at": senate_division_source["observed_at"],
        }
        for item in reconciled
    ]

    by_url = {
        HOUSE_PARTY_DIVISIONS_URL: house_divisions_source,
        SENATE_PARTY_DIVISION_URL: senate_division_source,
    }
    control_rows = [
        {
            "chamber": row.chamber,
            "congress": row.congress,
            "effective_from": row.effective_from,
            "effective_to": row.effective_to,
            "majority_party": row.majority_party,
            "majority_seats": row.majority_seats,
            "minority_party": row.minority_party,
            "minority_seats": row.minority_seats,
            "other": row.other,
            "vacancies": row.vacancies,
            "total_seats": row.total_seats,
            "majority_basis": row.majority_basis,
            "source_url": row.source_url,
            "printed_effective_from": row.printed_effective_from,
            "correction_reason": row.correction_reason,
            "source_sha256": by_url[row.source_url]["sha256"],
            "observed_at": by_url[row.source_url]["observed_at"],
        }
        for row in controls
    ]

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    sessions_path = write_rows(session_rows, output_dir / f"{SESSIONS}.parquet", SESSIONS_SCHEMA)
    control_path = write_rows(control_rows, output_dir / f"{PARTY_CONTROL}.parquet", PARTY_CONTROL_SCHEMA)
    return sessions_path, control_path


def json_summary(sessions: tuple, controls: tuple) -> str:
    return json.dumps(
        {"sessions": len(sessions), "party_control": len(controls)},
        sort_keys=True,
        separators=(",", ":"),
    )
