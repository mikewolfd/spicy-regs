"""Typed read outcomes derived only from recorded source attempts.

The private acquisition journal remains the complete original evidence. These
main rows expose its keys and states without turning cached subjects, caps, or
an absent checkpoint into a read. Each Congress producer owns its own table.
"""
from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from urllib.parse import parse_qsl, urlsplit

import pyarrow as pa

from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, read_with_receipts, selected_subject_policy, write_dataset

DETAIL_TABLES = {
    "nominations": "nominations_detail_reads",
    "committee_meetings": "committee_meetings_detail_reads",
    "house_communications": "house_communications_detail_reads",
}
S, INT = pa.string(), pa.int64()
FIELD_STATE = pa.list_(pa.struct([("field", S), ("state", S)]))
SOURCE_WITNESS = pa.list_(pa.struct([("source_url", S), ("sha256", S), ("capture_id", S)]))
DETAIL_SCHEMA = pa.schema([
    ("generation_id", S), ("attempt_id", S), ("source_table", S),
    ("congress", INT), ("citation", S), ("number", S), ("part_number", S),
    ("chamber", S), ("event_id", S), ("communication_id", S),
    ("source_url", S), ("read_field", S), ("outcome", S),
    ("error_type", S), ("recorded_at", S), ("shape_version", S),
    ("field_states", FIELD_STATE), ("source_witnesses", SOURCE_WITNESS),
])
READ_TABLES = {"committee_report_reads": "committee_report_read_outcomes",
               "document_citation_reads": "document_citation_read_outcomes"}
HOUSE_TABLE = "house_record_enrichment_results"
HOUSE_KEY_FIELDS = [("stated_communication_id", S), ("communication_key_status", S),
                   ("communication_key_rule", S), ("communication_key_reason", S)]
HOUSE_DETAIL_SCHEMA = pa.schema([*DETAIL_SCHEMA, pa.field("communication_type", S),
                                *(pa.field(name, kind) for name, kind in HOUSE_KEY_FIELDS)])
HOUSE_SCHEMA = pa.schema([
    ("generation_id", S), ("attempt_id", S), ("communication_id", S),
    ("congress", INT), ("communication_type", S), ("number", S),
    ("congressional_record_date", S), ("record_calendar_day", S),
    ("outcome", S), ("qualified_occurrences", INT), ("complete_scope", pa.bool_()),
    ("conflict_status", S), ("rule_version", S), ("recorded_at", S),
    ("reason", S),
    ("scope_packages", pa.list_(S)),
    ("scope_read_results", pa.list_(pa.struct([
        ("package_id", S), ("outcome", S), ("marker", S), ("rule_version", S),
        ("input_generation", S), ("input_processing_sha256", S),
        ("body_witnesses", pa.list_(pa.struct([("granule_id", S), ("sha256", S), ("source_url", S)]))),
    ]))),
    ("witnesses", pa.list_(pa.struct([
        ("record_package_id", S), ("record_granule_id", S),
        ("body_sha256", S), ("body_url", S), ("marker", S),
        ("input_generation", S), ("input_processing_sha256", S),
    ]))),
    *HOUSE_KEY_FIELDS,
])
POLICIES: dict[str, DatasetPolicy] = {name: DatasetPolicy(name, DETAIL_SCHEMA, ("generation_id", "attempt_id"),
                              ("recorded_event",), policy_version="navigation-read-outcomes/1")
            for name in DETAIL_TABLES.values()}
POLICIES[DETAIL_TABLES["house_communications"]] = DatasetPolicy(
    DETAIL_TABLES["house_communications"], HOUSE_DETAIL_SCHEMA, ("generation_id", "attempt_id"),
    ("recorded_event",), policy_version="navigation-read-outcomes/2")
POLICIES[HOUSE_TABLE] = DatasetPolicy(HOUSE_TABLE, HOUSE_SCHEMA, ("generation_id", "attempt_id"),
                                   ("recorded_event",), policy_version="navigation-read-outcomes/2")


REPORT_READ_SCHEMA = pa.schema([
    ("generation_id", S), ("package_id", S), ("last_modified", S), ("outcome", S),
    ("rule_version", S), ("observed_at", S),
])
CITATION_READ_SCHEMA = pa.schema([
    ("generation_id", S), ("document_kind", S), ("document_key", S), ("body_version_id", S),
    ("text_sha256", S), ("rule_set_version", S), ("read_at", S), ("citation_rows", INT),
    ("source_table", S), ("input_family", S), ("input_generation", S), ("input_sha256", S),
])
POLICIES[READ_TABLES["committee_report_reads"]] = DatasetPolicy(
    READ_TABLES["committee_report_reads"], REPORT_READ_SCHEMA, ("generation_id", "package_id"),
    ("recorded_event",), policy_version="navigation-read-outcomes/1")
POLICIES[READ_TABLES["document_citation_reads"]] = DatasetPolicy(
    READ_TABLES["document_citation_reads"], CITATION_READ_SCHEMA,
    ("generation_id", "document_kind", "document_key", "body_version_id"),
    ("recorded_event",), policy_version="navigation-read-outcomes/1")

FILE_TABLES = {family: family.replace("-", "_") + "_document_file_outcomes" for family in (
    "bill-family", "committee-reports", "print-citations", "laws", "native-legal-references", "senate-expenditures",
)}
FILE_SCHEMA = pa.schema([
    ("generation_id", S), ("source_dataset", S), ("member_ordinal", INT), ("relative_path", S),
    ("partitioned", pa.bool_()), ("row_count", INT), ("state", S), ("original_byte_sha256", S),
    ("state_witness_sha256", S),
    ("source_schema", pa.list_(pa.struct([("name", S), ("type", S), ("nullable", pa.bool_())]))),
    ("source_metadata", pa.list_(pa.struct([("key_base64", S), ("value_base64", S)]))),
    ("native_members", pa.list_(pa.struct([
        ("relative_path", S), ("sha256", S), ("byte_size", INT), ("row_count", INT),
    ]))),
])
FILE_POLICIES: dict[str, DatasetPolicy] = {name: DatasetPolicy(name, FILE_SCHEMA, ("generation_id", "source_dataset", "member_ordinal"),
                                  ("recorded_event",), policy_version="navigation-read-outcomes/1")
                 for name in FILE_TABLES.values()}
POLICIES.update(FILE_POLICIES)


def checkpoint_rows(dataset, source, *, generation_id):
    """Retain completed zero-finding reads and their exact native document/body keys."""
    import pyarrow.parquet as pq
    from spicy_regs.legislative_documents import body_version_id, native_document_key

    for batch in pq.ParquetFile(source).iter_batches():
        for raw in batch.to_pylist():
            table = READ_TABLES[dataset]
            result = {name: raw.get(name) for name in POLICIES[table].subject_schema.names}
            result["generation_id"] = generation_id
            if dataset == "document_citation_reads":
                result["document_key"] = native_document_key(raw.get("document_kind"), raw.get("document_key"))
                result["body_version_id"] = body_version_id(raw.get("text_sha256"))
                if raw.get("citation_rows") is not None:
                    result["citation_rows"] = _native_congress(raw["citation_rows"])
            yield table, result, raw


def file_rows(states, subjects, stage, *, generation_id):
    """Only exact producer-to-native mappings qualify; empty states have no byte hash."""
    import base64
    import pyarrow.parquet as pq
    from spicy_regs.etl_receipts import exact_json

    ordinals = {}
    seen = set()
    accounted = {}
    for values, context in states:
        state = values["file_state"]
        dataset, relative = state["dataset"], state["relative_path"]
        if (dataset, relative) in seen:
            raise ValueError("Duplicate original member mapping")
        seen.add((dataset, relative))
        ordinal = ordinals.get(dataset, 0)
        ordinals[dataset] = ordinal + 1
        members = subjects.get(dataset)
        if members is None:
            raise ValueError("Missing native dataset mapping")
        if relative is None:
            if state["rows"] != 0 or not state["partitioned"] or members:
                raise ValueError("Empty state has an invented physical member")
            state_digest = "sha256:" + hashlib.sha256(exact_json(state).encode()).hexdigest()
            if not any(w.get("sha256") == state_digest for w in context.witnesses):
                raise ValueError("Empty-state witness digest differs")
            original_digest, schema, metadata, matched = None, None, None, []
        else:
            if Path(relative).is_absolute() or ".." in Path(relative).parts:
                raise ValueError("Producer member mapping requires a relative path")
            native_dataset = READ_TABLES.get(dataset, dataset)
            mapped_members = subjects.get(native_dataset, members)
            from spicy_regs.legislative_documents import field_registry
            if not mapped_members and (native_dataset != dataset or not field_registry()[dataset]["processing_only"]):
                raise ValueError("Missing native member mapping")
            expected = f"{native_dataset}/{relative}" if state["partitioned"] else f"{native_dataset}.parquet"
            if mapped_members and (mapped_members.count(expected) != 1 or ordinal >= len(mapped_members)):
                raise ValueError("Missing or duplicate native member mapping")
            if not any(w.get("sha256") == state["sha256"] for w in context.witnesses):
                raise ValueError("Original producer byte digest differs from its recorded witness")
            member = stage / expected
            if mapped_members and pq.read_metadata(member).num_rows != state["rows"]:
                raise ValueError("Native member row count differs from original mapping")
            original_digest, state_digest = state["sha256"], None
            original_schema = pa.ipc.read_schema(pa.BufferReader(state["schema"]))
            schema = [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in original_schema]
            metadata = [{"key_base64": base64.b64encode(k).decode(), "value_base64": base64.b64encode(v).decode()}
                        for k, v in state["metadata"]]
            matched = [] if not mapped_members else [{"relative_path": expected,
                "sha256": "sha256:" + hashlib.sha256(member.read_bytes()).hexdigest(),
                "byte_size": member.stat().st_size, "row_count": state["rows"]}]
            accounted.setdefault(native_dataset, set()).update(item["relative_path"] for item in matched)
        result = {"generation_id": generation_id, "source_dataset": dataset, "member_ordinal": ordinal,
            "relative_path": relative, "partitioned": state["partitioned"], "row_count": state["rows"],
            "state": "successful-empty-partition" if relative is None else
                     "converted-member" if matched else "retained-processing-member",
            "original_byte_sha256": original_digest, "state_witness_sha256": state_digest,
            "source_schema": schema, "source_metadata": metadata, "native_members": matched}
        yield result, values, context
    for dataset, members in accounted.items():
        if members != set(subjects[dataset]) or len(members) != len(subjects[dataset]):
            raise ValueError("Unmatched or duplicate native member mapping")


def family_tables(source_tables):
    names = [DETAIL_TABLES[table] for table in source_tables if table in DETAIL_TABLES]
    if "house_communications" in source_tables:
        names.append(HOUSE_TABLE)
    return tuple(names)


def _native_congress(value):
    # JSON journals from historical source shapers retain Congress as integer text.
    if type(value) is int and value >= 0:
        return value
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        return int(value)
    raise ValueError("Recorded Congress outcome has no complete native Congress")


def _house_communication_key(identity):
    """Validate stated IDs and native triples using the maintained source route."""
    from spicy_docs.schemas.tables import natural_key
    from spicy_docs.sources.congress.listing import LIST_ROUTES, list_route_url

    stated = identity.get("communication_id")
    result = {"communication_id": None, "stated_communication_id": stated if isinstance(stated, str) else None,
              "communication_key_status": "incomplete", "communication_key_rule": "house-communication-native-key/1",
              "communication_key_reason": "Missing native Congress, communication type, or number", "congress": None,
              "communication_type": identity.get("communication_type") if isinstance(identity.get("communication_type"), str) else None,
              "number": str(identity["number"]) if type(identity.get("number")) is int else
                        identity.get("number") if isinstance(identity.get("number"), str) else None}

    def positive(value):
        if type(value) is int and value > 0:
            return value
        if isinstance(value, str) and re.fullmatch(r"[1-9][0-9]*", value, flags=re.ASCII):
            return int(value)
        raise ValueError("Expected a canonical positive integer")

    def validated(congress, kind, number):
        list_route_url(LIST_ROUTES["house-communication-detail"], congress=congress,
                       communication_type=kind, number=number)
        return natural_key(congress, kind, number)

    try:
        parts = []
        for name in ("congress", "communication_type", "number"):
            value = identity.get(name)
            parts.append(None if value is None else
                         value.lower() if name == "communication_type" and isinstance(value, str) else
                         positive(value) if name != "communication_type" else value)
        congress, kind, number = parts
        # The maintained route validates even partially stated components.
        validated(congress if congress is not None else 1, kind if kind is not None else "ec",
                  number if number is not None else 1)
        result["congress"] = congress
        constructed = validated(congress, kind, number) if all(p is not None for p in parts) else None
        if stated is not None:
            if not isinstance(stated, str) or not re.fullmatch(r"[1-9][0-9]*-[a-z]+-[1-9][0-9]*", stated, flags=re.ASCII):
                raise ValueError("Stated communication ID is not canonical")
            c, k, n = stated.split("-")
            stated_parts = [positive(c), k, positive(n)]
            validated(*stated_parts)
            if any(p is not None and p != s for p, s in zip(parts, stated_parts)):
                result.update(communication_key_status="conflict",
                              communication_key_reason="Stated communication ID disagrees with native fields")
                return result
            result.update(communication_id=stated, communication_key_status="stated", communication_key_reason=None)
        elif constructed is not None:
            result.update(communication_id=constructed, communication_key_status="constructed", communication_key_reason=None)
    except (ValueError, TypeError) as error:
        result.update(communication_key_status="malformed", communication_key_reason=str(error))
    return result


def _source_request_key(value):
    """Same source request scope despite credential redaction and page offset."""
    if not isinstance(value, str):
        return None
    from spicy_docs.transport.credentials import CREDENTIAL_PARAMETERS
    parts = urlsplit(value)
    query = tuple(sorted((name, value) for name, value in parse_qsl(parts.query, keep_blank_values=True)
                         if name not in (*CREDENTIAL_PARAMETERS, "offset")))
    return parts.scheme, parts.netloc, parts.path, query


def recorded_rows(journal: Path, *, generation_id: str):
    """Yield exact keyed attempts; unknown old event fields stay unknown."""
    captures = {}
    package_reads = {}
    for ordinal, line in enumerate(journal.read_text().splitlines()):
        event = json.loads(line)
        if event.get("event") == "capture":
            key = _source_request_key(event.get("requested_url"))
            if key is not None:
                captures.setdefault(key, []).append(event)
            continue
        if event.get("event") == "house-record-package":
            package_reads[event.get("package_id")] = event
            continue
        common = {"generation_id": generation_id, "attempt_id": f"journal:{ordinal}",
                  "recorded_at": event.get("recorded_at")}
        if event.get("event") == "congress-detail-result" and event.get("table") in DETAIL_TABLES:
            identity = event.get("source_identity") or event
            if event["table"] == "house_communications":
                identity = event | identity
            row = common | {name: identity.get(name) for name in
                            ("citation", "number", "part_number", "chamber", "event_id", "communication_id")}
            if event["table"] == "house_communications":
                row.update(_house_communication_key(identity))
            else:
                row["congress"] = _native_congress(identity.get("congress"))
            row.update(source_table=event["table"],
                       source_url=event.get("source_url"), read_field=event.get("read_field") or "detail",
                       outcome=event["read_outcome"], error_type=event.get("error_type"),
                       shape_version=event.get("shape_version"),
                       field_states=None if event.get("field_states") is None else
                       [{"field": name, "state": state} for name, state in event["field_states"].items()],
                       source_witnesses=[{"source_url": c.get("resolved_url"), "sha256": c.get("sha256"),
                                          "capture_id": c.get("capture_id")}
                                         for c in captures.pop(_source_request_key(event.get("source_url")), [])])
            yield DETAIL_TABLES[event["table"]], row, event
        elif event.get("event") == "house-record-result":
            row = common | {name: event.get(name) for name in HOUSE_SCHEMA.names if name not in common}
            row.update(_house_communication_key(event))
            row["scope_read_results"] = []
            for package in event.get("scope_packages") or []:
                scope = package_reads.get(package)
                if scope is not None:
                    pin = scope.get("input") or {}
                    row["scope_read_results"].append({"package_id": package, "outcome": scope.get("outcome"),
                        "marker": scope.get("marker"), "rule_version": scope.get("rule"),
                        "input_generation": pin.get("generationId"),
                        "input_processing_sha256": pin.get("processing", {}).get("sha256"),
                        "body_witnesses": [{"granule_id": body.get("granule_id"), "sha256": body.get("sha256"),
                                           "source_url": body.get("locator")} for body in scope.get("bodies", [])]})
            row["witnesses"] = []
            for witness in event.get("witnesses", []):
                entry, pin = witness["entry"], witness.get("input", {})
                matching = [body for body in witness.get("bodies", [])
                            if body.get("package_id") == entry.get("record_package_id")
                            and body.get("granule_id") == entry.get("record_granule_id")]
                for body in matching:
                    row["witnesses"].append({"record_package_id": entry.get("record_package_id"),
                        "record_granule_id": entry.get("record_granule_id"),
                        "body_sha256": body.get("sha256"), "body_url": body.get("locator"),
                        "marker": witness.get("marker"), "input_generation": pin.get("generationId"),
                        "input_processing_sha256": pin.get("processing", {}).get("sha256")})
            yield HOUSE_TABLE, row, event


def write_recorded_outcomes(journal, directory, *, generation_id, tables, priors=None):
    """Write bounded observed status tables with original events in their receipts."""
    tables = tuple(tables)
    rows = {name: [] for name in tables}
    for name, selection in (priors or {}).items():
        if name not in rows or selection is None:
            continue
        with selection.receipts.open("rb") as stream:
            prior_digest = "sha256:" + hashlib.file_digest(stream, "sha256").hexdigest()
        prior_policy = selected_subject_policy(POLICIES[name], selection.subjects)
        for recorded in read_with_receipts(selection.subjects, [selection.receipts], prior_policy,
                                            generation_id=selection.generation_id):
            if name in (HOUSE_TABLE, DETAIL_TABLES["house_communications"]):
                event = recorded["recorded_event"]
                identity = event.get("source_identity") or event
                identity = event | identity
                recorded.update(_house_communication_key(identity))
            context = ReceiptContext(generation_id,
                "inherited:" + recorded["generation_id"] + ":" + recorded["attempt_id"],
                "navigation-read-outcomes/1", [{"source_id": "selected-outcomes", "source_uri": None,
                    "sha256": prior_digest,
                    "locator": recorded["attempt_id"], "body_version": None}])
            rows[name].append((recorded, context))
    digest = None
    if journal is not None and journal.exists():
        digest = "sha256:" + hashlib.sha256(journal.read_bytes()).hexdigest()
        for dataset, row, event in recorded_rows(journal, generation_id=generation_id):
            if dataset in rows:
                context = ReceiptContext(generation_id, row["attempt_id"], "navigation-read-outcomes/1",
                    [{"source_id": "source-journal", "source_uri": None, "sha256": digest,
                      "locator": row["attempt_id"], "body_version": None}])
                rows[dataset].append((row | {"recorded_event": event}, context))
    subjects, receipts = [], []
    for name in tables:
        subject, receipt = write_dataset(rows[name], directory / name, POLICIES[name])
        subjects.append(subject)
        receipts.append(receipt)
    return subjects, receipts
