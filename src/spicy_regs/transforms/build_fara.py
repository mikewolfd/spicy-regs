"""FARA native observations and explicitly limited filing-to-principal label associations."""

import json
from collections import defaultdict
from datetime import datetime
from pathlib import Path

import pyarrow as pa

from spicy_regs.parquet_rows import write_rows

COMMON = [("source_record_id", pa.string()), ("source_release_digest", pa.string()),
          ("source_locator_json", pa.string()), ("raw_fields_json", pa.string())]
SCHEMAS = {
    "fara_registrants": pa.schema([
        ("registration_number", pa.string()), ("registrant_name", pa.string()),
        ("registration_date", pa.date32()), ("termination_date", pa.date32()),
        ("registration_status", pa.string()), ("date_diagnostics_json", pa.string()), *COMMON]),
    "fara_foreign_principals": pa.schema([
        ("registration_number", pa.string()), ("native_principal_id", pa.string()),
        ("principal_name", pa.string()), ("country", pa.string()),
        ("registration_date", pa.date32()), ("termination_date", pa.date32()),
        ("relationship_status", pa.string()), ("date_diagnostics_json", pa.string()), *COMMON]),
    "fara_filings": pa.schema([
        ("registration_number", pa.string()), ("document_id", pa.string()), ("document_url", pa.string()),
        ("document_type", pa.string()), ("stamped_date", pa.date32()),
        ("date_diagnostic", pa.string()), ("date_after_capture", pa.bool_()),
        ("principal_name", pa.string()), ("principal_country", pa.string()),
        ("foreign_principal_observation_id", pa.string()), ("association_status", pa.string()),
        ("principal_candidates_json", pa.string()), ("document_status", pa.string()),
        ("document_sha256", pa.string()), ("document_bytes", pa.int64()),
        ("document_review_status", pa.string()), ("document_review_json", pa.string()), *COMMON]),
    "fara_snapshots": pa.schema([
        ("kind", pa.string()), ("capture_sha256", pa.string()), ("observed_at", pa.string()),
        ("source_release_digest", pa.string()), ("scope_json", pa.string()),
        ("scanned_rows", pa.int64()), ("selected_rows", pa.int64()), ("omitted_rows", pa.int64()),
        ("registrations_without_rows_json", pa.string())]),
}


def _json(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _date(value):
    if value is None:
        return None, "absent"
    if value == "":
        return None, "blank"
    try:
        return datetime.strptime(value, "%m/%d/%Y").date(), "value"
    except ValueError:
        return None, "unparsed"


def _status(value):
    # A malformed termination date is not evidence of an active relationship.
    return "no_termination_reported" if value in (None, "") else "terminated" if _date(value)[1] == "value" else "unresolved"


def build_fara(readers, output_dir: Path, *, populations, documents):
    """Consume complete source releases; no fuzzy matching, financial extraction or history truncation."""
    from spicy_docs.sources.fara import HEADERS, validate_scope

    selected, scopes, snapshots = {}, {}, []
    cohort = None
    for reader in readers:
        outcome = reader.collection_outcome
        scope = validate_scope(outcome["requestedScope"])
        kind = scope["kind"]
        if kind in selected or outcome["failedRecordCount"] or outcome["discardedObservationCount"]:
            raise ValueError("FARA requires one complete accepted release per export")
        if cohort is None:
            cohort = scope["registration_numbers"]
        if scope["registration_numbers"] != cohort:
            raise ValueError("FARA exports select different registrants")
        selected[kind] = []
        scopes[kind] = scope
        for item in reader.iter_records():
            row = item["record"]
            if row["kind"] != kind or row["fields"]["Registration Number"] not in cohort:
                raise ValueError("FARA record differs from its declared cohort")
            selected[kind].append((item, reader.pin.artifact_digest))
        count = len(selected[kind])
        population = populations[kind]
        if (count != outcome["publishedRecordCount"] or count != outcome["discoveredRecordCount"]
                or count != population["selected"] or population["scanned"] != count + population["omitted"]):
            raise ValueError("FARA source population differs from the admitted rows")
        present = {item["record"]["fields"]["Registration Number"] for item, _pin in selected[kind]}
        missing = sorted(set(cohort) - present)
        if kind == "Registrants" and missing:
            raise ValueError("FARA selected registrants are missing from the observed export")
        snapshots.append({"kind": kind, "capture_sha256": scope["capture"]["response_sha256"],
                          "observed_at": scope["capture"]["observed_at"],
                          "source_release_digest": reader.pin.artifact_digest, "scope_json": _json(scope),
                          "scanned_rows": population["scanned"], "selected_rows": count,
                          "omitted_rows": population["omitted"], "registrations_without_rows_json": _json(missing)})
    if set(selected) != set(HEADERS):
        raise ValueError("FARA needs registrants, foreign principals and filing metadata exports")

    def common(item, pin):
        row = item["record"]
        return {"source_record_id": item["sourceRecordId"], "source_release_digest": pin,
                "source_locator_json": _json(row["source_locator"]), "raw_fields_json": _json(row["native_fields"])}

    registrants, principals, filings = [], [], []
    candidates = defaultdict(list)
    for item, pin in selected["Registrants"]:
        fields = item["record"]["fields"]
        start, end = _date(fields["Registration Date"]), _date(fields["Termination Date"])
        registrants.append({"registration_number": fields["Registration Number"], "registrant_name": fields["Name"],
                            "registration_date": start[0], "termination_date": end[0],
                            "registration_status": _status(fields["Termination Date"]),
                            "date_diagnostics_json": _json({"registration": start[1], "termination": end[1]}),
                            **common(item, pin)})
    for item, pin in selected["ForeignPrincipals"]:
        fields = item["record"]["fields"]
        start = _date(fields["Foreign Principal Registration Date"])
        end = _date(fields["Foreign Principal Termination Date"])
        number, name, country = (fields[key] for key in
                                 ("Registration Number", "Foreign Principal", "Country/Location Represented"))
        candidates[(number, name, country)].append(item["sourceRecordId"])
        principals.append({"registration_number": number, "native_principal_id": None, "principal_name": name,
                           "country": country, "registration_date": start[0], "termination_date": end[0],
                           "relationship_status": _status(fields["Foreign Principal Termination Date"]),
                           "date_diagnostics_json": _json({"registration": start[1], "termination": end[1]}),
                           **common(item, pin)})
    used_documents = set()
    for item, pin in selected["RegistrantDocs"]:
        fields = item["record"]["fields"]
        number, name, country, url = (fields[key] for key in
                                     ("Registration Number", "Foreign Principal Name", "Foreign Principal Country", "URL"))
        matches = candidates[(number, name, country)] if name and country else []
        status = ("registrant_only" if not name else "export_label_unique" if len(matches) == 1 else
                  "ambiguous_label" if matches else "unresolved_label")
        document = documents.get(url)
        if document:
            used_documents.add(url)
        stamped, diagnostic = _date(fields["Date Stamped"])
        observed = datetime.fromisoformat(item["record"]["source_locator"]["observed_at"]).date()
        filings.append({"registration_number": number, "document_id": url.rsplit("/", 1)[-1], "document_url": url,
                        "document_type": fields["Document Type"], "stamped_date": stamped,
                        "date_diagnostic": diagnostic, "date_after_capture": stamped > observed if stamped else None,
                        "principal_name": name, "principal_country": country,
                        "foreign_principal_observation_id": matches[0] if len(matches) == 1 else None,
                        "association_status": status, "principal_candidates_json": _json(matches),
                        "document_status": "retained" if document else "not_selected",
                        "document_sha256": document["response_sha256"] if document else None,
                        "document_bytes": document["byte_size"] if document else None,
                        "document_review_status": document.get("review", {}).get("status", "unreviewed") if document
                        else "not_selected",
                        "document_review_json": _json(document["review"]) if document and "review" in document else None,
                        **common(item, pin)})
    if used_documents != set(documents):
        raise ValueError("FARA selected document is not listed in the selected registrant filing population")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    tables = dict(zip(SCHEMAS, (registrants, principals, filings, snapshots), strict=True))
    return [write_rows(tables[name], output_dir / f"{name}.parquet", schema) for name, schema in SCHEMAS.items()]
