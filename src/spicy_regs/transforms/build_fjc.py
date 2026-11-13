"""FJC judges, court labels, appointment observations and service terms without identity inference."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

TEXT = pa.string()
COMMON = [("snapshot_id", TEXT), ("nid", TEXT), ("source_record_id", TEXT), ("source_locator_json", TEXT)]
APPOINTMENT_DATES = {
    "Recess Appointment Date": "recess_appointment_date", "Nomination Date": "nomination_date",
    "Committee Referral Date": "committee_referral_date", "Hearing Date": "hearing_date",
    "Committee Action Date": "committee_action_date", "Confirmation Date": "confirmation_date",
    "Commission Date": "commission_date",
}
APPOINTMENT_TEXT = {
    "Appointment Title": "appointment_title_raw", "Appointing President": "appointing_president_raw",
    "Party of Appointing President": "appointing_president_party_raw",
    "Reappointing President": "reappointing_president_raw",
    "Party of Reappointing President": "reappointing_president_party_raw", "ABA Rating": "aba_rating_raw",
    "Statute Authorizing New Seat": "new_seat_statute_raw", "Judiciary Committee Action": "committee_action_raw",
    "Senate Vote Type": "senate_vote_type_raw", "Ayes/Nays": "ayes_nays_raw",
}
SCHEMAS = {
    "fjc_judges": pa.schema([
        *COMMON, ("jid_raw", TEXT), ("last_name", TEXT), ("first_name", TEXT), ("middle_name_raw", TEXT),
        ("suffix_raw", TEXT), ("gender_raw", TEXT), ("race_or_ethnicity_raw", TEXT),
        ("birth_month_raw", TEXT), ("birth_day_raw", TEXT), ("birth_year_raw", TEXT),
        ("death_month_raw", TEXT), ("death_day_raw", TEXT), ("death_year_raw", TEXT),
        ("biography_url", TEXT), ("external_judge_id", TEXT), ("external_identity_status", TEXT),
        ("raw_fields_json", TEXT),
    ]),
    "fjc_appointments": pa.schema([
        *COMMON, ("service_ordinal", pa.int32()), ("appointment_id", TEXT), ("appointment_kind", TEXT),
        ("court_label_id", TEXT), ("seat_id_raw", TEXT),
        *((column, TEXT) for column in APPOINTMENT_TEXT.values()),
        *((column, pa.date32()) for column in APPOINTMENT_DATES.values()),
        ("date_status_json", TEXT), ("raw_appointment_json", TEXT),
    ]),
    "fjc_service_terms": pa.schema([
        *COMMON, ("service_ordinal", pa.int32()), ("appointment_id", TEXT), ("court_label_id", TEXT),
        ("seat_id_raw", TEXT), ("senior_status_date", pa.date32()), ("termination_date", pa.date32()),
        ("termination_reason_raw", TEXT), ("vacancy_date", pa.date32()), ("vacancy_date_status", TEXT),
        ("chief_judge_periods_json", TEXT), ("date_status_json", TEXT), ("raw_service_json", TEXT),
    ]),
    "fjc_courts": pa.schema([
        ("snapshot_id", TEXT), ("court_label_id", TEXT), ("court_type_raw", TEXT), ("court_name_raw", TEXT),
        ("external_court_id", TEXT), ("external_identity_status", TEXT),
        ("source_record_id", TEXT), ("source_locator_json", TEXT),
    ]),
    "fjc_snapshots": pa.schema([
        ("snapshot_id", TEXT), ("observed_at", TEXT), ("source_sha256", TEXT), ("source_byte_size", pa.int64()),
        ("judge_count", pa.int64()), ("service_count", pa.int64()), ("source_release_logical_id", TEXT),
        ("source_selection_json", TEXT),
    ]),
}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _date(raw):
    """A full native date or explicit missing/invalid status; never guess a day or shorten to a year."""
    if raw == "":
        return None, "empty"
    if not raw.strip():
        return None, "whitespace"
    for pattern in ("%Y-%m-%d", "%m/%d/%Y"):
        try:
            parsed = datetime.strptime(raw, pattern).date()
            # strptime accepts year-less padding variations; these still explicitly supply every component.
            return parsed, "full-date"
        except ValueError:
            pass
    return None, "unresolved-literal"


def _court_key(kind, name):
    return "fjc-court-label:" + hashlib.sha256(_json([kind, name]).encode()).hexdigest()


def _collect(readers):
    from spicy_docs.sources.fjc import PERSON_FIELDS, REQUIRED_HEADERS, SERVICE_FIELDS, validate_scope

    tables = {name: [] for name in SCHEMAS}
    snapshots = set()
    for reader in readers:
        snapshot_id = reader.pin.artifact_digest
        if snapshot_id in snapshots:
            raise ValueError("An FJC source release was supplied twice")
        snapshots.add(snapshot_id)
        outcome = reader.collection_outcome
        if (outcome["failedRecordCount"] or outcome["discardedObservationCount"]
                or outcome["discoveredRecordCount"] != outcome["publishedRecordCount"]):
            raise ValueError("FJC requires a complete accepted snapshot")
        scope = validate_scope(outcome["requestedScope"])
        capture = scope["capture"]
        seen, courts, services = set(), {}, 0
        for expected_ordinal, item in enumerate(reader.iter_records()):
            record = item["record"]
            fields, locator = record["fields"], record["source_locator"]
            nid = record["nid"]
            if (record["ordinal"] != expected_ordinal or record["id"] != item["sourceRecordId"]
                    or record["id"] != f"fjc:{capture['response_sha256']}:{expected_ordinal:020d}"
                    or nid != fields.get("nid") or not nid or nid in seen
                    or not set(REQUIRED_HEADERS) <= fields.keys()
                    or any(locator.get(key) != capture[key] for key in capture)
                    or locator.get("csv_record_number") != expected_ordinal + 2):
                raise ValueError("FJC observation differs from its selected capture or native identity")
            seen.add(nid)
            common = {"snapshot_id": snapshot_id, "nid": nid, "source_record_id": item["sourceRecordId"],
                      "source_locator_json": _json(locator)}
            person = {name: fields[name] for name in PERSON_FIELDS}
            tables["fjc_judges"].append({
                **common, "jid_raw": person["jid"], "last_name": person["Last Name"],
                "first_name": person["First Name"], "middle_name_raw": person["Middle Name"],
                "suffix_raw": person["Suffix"], "gender_raw": person["Gender"],
                "race_or_ethnicity_raw": person["Race or Ethnicity"],
                **{f"{prefix}_{part}_raw": person[f"{prefix.title()} {part.title()}"]
                   for prefix in ("birth", "death") for part in ("month", "day", "year")},
                "biography_url": "https://www.fjc.gov/node/" + nid, "external_judge_id": None,
                "external_identity_status": "unresolved", "raw_fields_json": _json(fields),
            })
            for ordinal in range(1, 7):
                group = {name: fields[f"{name} ({ordinal})"] for name in SERVICE_FIELDS}
                if not any(value.strip() for value in group.values()):
                    continue
                if not group["Court Name"].strip() or not group["Court Type"].strip():
                    raise ValueError("FJC occupied service group lacks its native court relationship")
                services += 1
                court_key = _court_key(group["Court Type"], group["Court Name"])
                courts.setdefault(court_key, {
                    "snapshot_id": snapshot_id, "court_label_id": court_key, "court_type_raw": group["Court Type"],
                    "court_name_raw": group["Court Name"], "external_court_id": None,
                    "external_identity_status": "unresolved", "source_record_id": item["sourceRecordId"],
                    "source_locator_json": _json(locator),
                })
                key = {**common, "service_ordinal": ordinal, "appointment_id": f"fjc:{nid}:service:{ordinal}",
                       "court_label_id": court_key, "seat_id_raw": group["Seat ID"]}
                president = group["Appointing President"]
                kind = {"None (reassignment)": "reassignment", "None (assignment)": "assignment"}.get(
                    president, "presidential-appointment" if president.strip() else "unresolved"
                )
                dates = {column: _date(group[native]) for native, column in APPOINTMENT_DATES.items()}
                tables["fjc_appointments"].append({
                    **key, "appointment_kind": kind,
                    **{column: group[native] for native, column in APPOINTMENT_TEXT.items()},
                    **{column: result[0] for column, result in dates.items()},
                    "date_status_json": _json({column: result[1] for column, result in dates.items()}),
                    "raw_appointment_json": _json(group),
                })
                senior, senior_status = _date(group["Senior Status Date"])
                ended, ended_status = _date(group["Termination Date"])
                tables["fjc_service_terms"].append({
                    **key, "senior_status_date": senior, "termination_date": ended,
                    "termination_reason_raw": group["Termination"], "vacancy_date": None,
                    "vacancy_date_status": "not-provided-in-export",
                    "chief_judge_periods_json": _json([
                        {"begin_year_raw": group[f"{prefix}Service as Chief Judge, Begin"],
                         "end_year_raw": group[f"{prefix}Service as Chief Judge, End"]}
                        for prefix in ("", "2nd ")
                    ]),
                    "date_status_json": _json({"senior_status_date": senior_status, "termination_date": ended_status}),
                    "raw_service_json": _json(group),
                })
        if len(seen) != outcome["publishedRecordCount"]:
            raise ValueError("FJC selected source population is incomplete")
        tables["fjc_courts"].extend(courts.values())
        tables["fjc_snapshots"].append({
            "snapshot_id": snapshot_id, "observed_at": capture["observed_at"],
            "source_sha256": capture["response_sha256"], "source_byte_size": capture["byte_size"],
            "judge_count": len(seen), "service_count": services, "source_release_logical_id": reader.pin.logical_id,
            "source_selection_json": _json(scope),
        })
    if not snapshots:
        raise ValueError("Declare at least one FJC snapshot release")
    return tables


def build_fjc(readers, output_dir: Path):
    """Read each bounded snapshot once and install all tables only after complete source consumption."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    destinations = [output_dir / (name + ".parquet") for name in SCHEMAS]
    if any(path.exists() for path in destinations):
        raise ValueError("Use fresh output paths for each FJC generation")
    tables = _collect(readers)
    installed = []
    with TemporaryDirectory(dir=output_dir) as temporary:
        staging = Path(temporary)
        for name, schema in SCHEMAS.items():
            write_rows(tables[name], staging / (name + ".parquet"), schema, batch_size=256)
        try:
            for destination in destinations:
                destination.hardlink_to(staging / destination.name)
                installed.append(destination)
        except BaseException:
            for destination in installed:
                destination.unlink()
            raise
    return destinations
