"""Retained committee history observations, preserving every snapshot and field.

Master names reuse the source owner's mapping. PostgreSQL columns remain source
text or SQL NULL: array/date/boolean spellings are not converted or made current.
These tables deliberately have source-observation identities, not unique
committee/cycle keys. The original dump and its derived SQL stay distinct.
"""

from dataclasses import dataclass
import hashlib
import json

import pyarrow as pa
from spicy_docs.schemas.fec_committee_history import FEC_COMMITTEE_HISTORY, project_committee_master_row
from spicy_docs.sources.fec.committee_master import COMMITTEE_MASTER_FIELDS, HEADER_URL, committee_master_url
from spicy_docs.sources.fec.postgres_profile import FEC_POSTGRES_ROWS_PROFILE
from spicy_docs.sources.fec.row_profile import FEC_POSITIONAL_ROWS_PROFILE

from .fec_bulk_financial import COMMON_TEXT
from .fec_query import CollectionSelection, _json, observation_fields, record_evidence

MASTER = "fec_committee_master_observations"
POSTGRES = "fec_postgres_committee_history_observations"
MAPPING_VERSION = "fec-retained-committee-history/1"


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
        columns = [(n, pa.string()) for n in FEC_COMMITTEE_HISTORY.columns]
    elif profile == "postgres":
        scope = FEC_POSTGRES_ROWS_PROFILE.validate_query_scope(entry["scope"])
        names = [c["name"] for c in scope["derivation"]["columns"]]
        if not {"committee_id", "cycle"} <= set(names):
            raise ValueError("Committee history requires its native committee and cycle columns")
        table, cycle, header_endpoint = POSTGRES, None, None
        columns = [(n, pa.string()) for n in ("committee_id", "cycle", "derivation_json")]
        columns += [("raw_copy_fields", pa.list_(pa.string()))]
    else:
        raise ValueError("Unsupported committee history source profile")
    schema = pa.schema(
        [(n, pa.string()) for n in COMMON_TEXT + ["source_url", "capture_json", "history_scope_status"]]
        + [("source_cycle", pa.int32())]
        + columns
        + [("native_fields", pa.struct([(n, pa.string()) for n in names]))]
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
    result.update(observation_fields(table, row, prepared.selection, MAPPING_VERSION))
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
        result.update(project_committee_master_row({"fields": native, "cycle": prepared.selection.source_cycle}))
        result["native_fields"] = native
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
        result.update(
            native_fields=native["named_fields"],
            raw_copy_fields=fields,
            committee_id=native["named_fields"]["committee_id"],
            cycle=native["named_fields"]["cycle"],
            derivation_json=_json(derivation),
        )
    else:
        raise ValueError("Unsupported prepared committee history table")
    result.update(
        mapping_status="mapped",
        mapping_reason_json="{}",
        source_namespace="fec-retained-committee-history",
        source_url=row["source_url"],
        capture_json=_json(capture),
        history_scope_status="retained-source-observations-no-current-or-unique-cycle-assertion",
    )
    return result, record_evidence(table, result["record_id"], row, prepared.selection.source_generation_pin)
