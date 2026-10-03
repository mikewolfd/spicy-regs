"""Flat committee history facts with exact evidence for every source snapshot.

Known dates, flags and years are typed once at build time. Original PostgreSQL
spellings, extraction details and database internals remain in source evidence.
"""

from dataclasses import dataclass
import hashlib
import json
import re

import pyarrow as pa
from spicy_docs.schemas.fec_committee_history import FEC_COMMITTEE_HISTORY, project_committee_master_row
from spicy_docs.sources.fec.committee_master import COMMITTEE_MASTER_FIELDS, HEADER_URL, committee_master_url
from spicy_docs.sources.fec.postgres_profile import FEC_POSTGRES_ROWS_PROFILE
from spicy_docs.sources.fec.row_profile import FEC_POSITIONAL_ROWS_PROFILE

from .fec_identity_shape import array_values, typed_columns, typed_values
from .fec_query import CollectionSelection, SOURCE_TEXT, _json, subject_observation_fields, record_evidence

MASTER = "fec_committee_master_observations"
POSTGRES = "fec_postgres_committee_history_observations"
MAPPING_VERSION = "fec-retained-committee-history/2"

TYPED = {
    **{n: "date" for n in ("qualifying_date", "first_file_date", "last_file_date", "last_f1_date", "first_f1_date")},
    **{n: "boolean" for n in ("is_active", "convert_to_pac_flag")},
    **{
        n: "year"
        for n in ("cycle", "last_cycle_has_financial", "last_cycle_has_activity", "former_candidate_election_year")
    },
}
ARRAYS = {"cycles", "candidate_ids", "sponsor_candidate_ids", "cycles_has_financial", "cycles_has_activity"}
SOURCE_ONLY = {"idx", "treasurer_text"}


def _columns(names):
    selected = [n for n in names if n not in SOURCE_ONLY]
    return (
        [(n, pa.string()) for n in selected if n not in TYPED and n not in ARRAYS]
        + typed_columns({n: TYPED[n] for n in selected if n in TYPED})
        + [(n + suffix, pa.string()) for n in selected if n in ARRAYS for suffix in ("_json", "_status")]
    )


def _history_values(native):
    result = {n: v for n, v in native.items() if n not in SOURCE_ONLY and n not in TYPED and n not in ARRAYS}
    values, problems = typed_values(native, {n: TYPED[n] for n in native if n in TYPED}, postgres=True)
    result.update(values)
    for name in native.keys() & ARRAYS:
        raw = native[name]
        if raw is None:
            value = None
        elif raw == "{}":
            value = []
        elif (
            isinstance(raw, str)
            and re.fullmatch(r"\{[0-9]+(?:,[0-9]+)*\}", raw)
            and name != "candidate_ids"
            and name != "sponsor_candidate_ids"
        ):
            value = [int(part) for part in raw[1:-1].split(",")]
        elif (
            name in {"candidate_ids", "sponsor_candidate_ids"}
            and isinstance(raw, str)
            and re.fullmatch(r'\{[^,"\\{}\s]+(?:,[^,"\\{}\s]+)*\}', raw)
            and all(part.upper() != "NULL" for part in raw[1:-1].split(","))
        ):
            # Decode array syntax independently of identifier validity. Actual
            # history includes committee IDs and N/A beside valid candidate IDs.
            value = raw[1:-1].split(",")
        else:
            value = raw
            problems[name] = "unsupported_array_spelling"
        result.update(array_values({name: value}, name))
    return result, problems


@dataclass(frozen=True)
class HistorySelection:
    """A source-validated immutable scope and its reviewed header evidence."""

    selection: CollectionSelection
    table: str
    scope_json: str
    header_endpoint_json: str | None
    schema: pa.Schema


def prepare_history(entry, *, source_generation_pin, header_row=None):
    """Validate one manifest selection without acquiring or decoding new bytes."""
    if entry.get("source_family") != "fec_committees":
        raise ValueError("Committee history requires the selected committee source family")
    profile = entry.get("profile")
    if profile == "positional":
        scope = FEC_POSITIONAL_ROWS_PROFILE.validate_query_scope(entry["scope"])
        mapping = entry.get("field_mapping", {})
        cycle = mapping.get("cycle")
        expected_url = committee_master_url(cycle)
        if (
            mapping.get("relationship_family") != "committee_master"
            or mapping.get("data_has_header") is not False
            or scope.get("format") != "delimited"
            or scope.get("encoding") != "utf-8"
            or scope.get("delimiter") != "|"
            or scope.get("quoting") != "literal"
            or scope.get("member") != {"ordinal": 0, "name": "cm.txt"}
            or scope["capture"]["requestUrl"] != expected_url
            or scope["capture"].get("resolvedUrl", expected_url) != expected_url
        ):
            raise ValueError("Committee master selection differs from its native cycle/layout")
        if header_row is None:
            raise ValueError("Committee master requires its retained header observation")
        header = json.loads(header_row["metadata_json"])
        locator = json.loads(header_row["source_locator_json"])
        if (
            header.get("kind") != "row"
            or header.get("fields") != list(COMMITTEE_MASTER_FIELDS)
            or header_row["source_url"] != HEADER_URL
            or header_row["collection_id"] != mapping.get("header_collection_id")
            or locator.get("ordinal") != mapping.get("header_row_ordinal")
            or locator.get("member") is not None
            or header.get("source", {}).get("sha256") != header_row["source_sha256"]
            or locator.get("sha256") != header_row["source_sha256"]
            or any(locator.get(k) != header_row[k] for k in ("collection_id", "source_record_id"))
        ):
            raise ValueError("Committee master header differs from its exact native definition")
        endpoint = {k: header_row[k] for k in ("collection_id", "source_record_id", "source_sha256", "source_url")}
        endpoint["source_locator"] = locator
        header_endpoint = _json(endpoint)
        table = MASTER
        names = list(COMMITTEE_MASTER_FIELDS)
        columns = _columns(FEC_COMMITTEE_HISTORY.columns)
    elif profile == "postgres":
        scope = FEC_POSTGRES_ROWS_PROFILE.validate_query_scope(entry["scope"])
        names = [c["name"] for c in scope["derivation"]["columns"]]
        if not {"committee_id", "cycle"} <= set(names):
            raise ValueError("Committee history requires its native committee and cycle columns")
        table, cycle, header_endpoint = POSTGRES, None, None
        columns = _columns(names)
    else:
        raise ValueError("Unsupported committee history source profile")
    schema = pa.schema(
        [(n, pa.string()) for n in SOURCE_TEXT + ["source_url", "history_scope_status"]]
        + [("source_cycle", pa.int32())]
        + columns
    )
    selection = CollectionSelection(
        entry["collection_id"],
        scope["capture"]["responseSha256"],
        source_generation_pin,
        "official-fec",
        cycle,
        "snapshot",
        "sha256:" + hashlib.sha256(_json(entry).encode()).hexdigest(),
    )
    return HistorySelection(selection, table, _json(scope), header_endpoint, schema)


def map_history(row, prepared: HistorySelection):
    """Map one already qualified native observation and its exact evidence."""
    scope = json.loads(prepared.scope_json)
    capture = scope["capture"]
    table = prepared.table
    result = dict.fromkeys(prepared.schema.names)
    result.update(subject_observation_fields(table, row, prepared.selection, MAPPING_VERSION))
    locator = json.loads(row["source_locator_json"])
    native = json.loads(row["metadata_json"])
    member = scope.get("member")
    ordinal = locator.get("ordinal")
    member_id = "original" if member is None else str(member["ordinal"])
    if (
        row["source_url"] != capture["requestUrl"]
        or locator.get("member") != member
        or type(ordinal) is not int
        or ordinal < 0
        or row["source_record_id"] != f"{capture['responseSha256']}/{member_id}/{ordinal:020d}"
    ):
        raise ValueError("Committee history record differs from its selected source coordinates")
    if table == MASTER:
        if (
            row.get("profile") != "positional"
            or set(native) != set(COMMITTEE_MASTER_FIELDS)
            or any(not isinstance(v, str) for v in native.values())
            or _json(locator.get("field_mapping")) != prepared.header_endpoint_json
        ):
            raise ValueError("Committee master row differs from its qualified fields/header")
        values, problems = _history_values(
            project_committee_master_row({"fields": native, "cycle": prepared.selection.source_cycle})
        )
        result.update(values)
    elif table == POSTGRES:
        derivation = scope["derivation"]
        names = [c["name"] for c in derivation["columns"]]
        source = native.get("source", {})
        values, fields = native.get("values"), native.get("fields")
        if (
            row.get("profile") != "postgres"
            or native.get("kind") != "postgres-copy"
            or not isinstance(values, list)
            or not isinstance(fields, list)
            or len(values) != len(names)
            or len(fields) != len(names)
            or any(v is not None and not isinstance(v, str) for v in values)
            or any(not isinstance(v, str) for v in fields)
            or source.get("original_sha256") != capture["responseSha256"]
            or source.get("sha256") != derivation["outputs"]["data"]["sha256"]
            or source.get("table") != derivation["table"]
            or any(
                locator.get(k) != source.get(k)
                for k in ("sha256", "original_sha256", "table", "byte_offset", "byte_length", "encoding")
            )
            or locator.get("column_schema_sha256") != derivation["outputs"]["schema"]["sha256"]
            or native.get("named_fields") != dict(zip(names, values, strict=True))
        ):
            raise ValueError("PostgreSQL history row differs from its qualified native values/derivation")
        values, problems = _history_values(native["named_fields"])
        result.update(values)
    else:
        raise ValueError("Unsupported prepared committee history table")
    result.update(
        mapping_status="partial" if problems else "mapped",
        mapping_reason_json=_json(problems),
        source_namespace="fec-retained-committee-history",
        source_url=row["source_url"],
        history_scope_status="retained-source-observations-no-current-or-unique-cycle-assertion",
    )
    return result, record_evidence(table, result["record_id"], row, prepared.selection.source_generation_pin)
