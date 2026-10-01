"""Retained committee API observations, separate from the current registry.

Core committee fields reuse the maintained retained-record mapping. Each source
observation keeps its own identity and full native metadata. Response controls
have their own rows; three selected pages do not imply a completed traversal.
"""

import json
from urllib.parse import urlsplit

import pyarrow as pa

from .build_fec_committees import COLUMNS, _shape
from .fec_bulk_financial import COMMON_TEXT
from .fec_identity_observations import _json, _scalar, _state
from .fec_query import CollectionSelection, observation_fields, record_evidence
from .fec_relationships import _id_status

COMMITTEES = "fec_committee_observations"
CONTROLS = "fec_api_response_controls"
MAPPING_VERSION = "fec-retained-committee-api/1"
_EXTRA = "source_url source_pointer original_result_pointer query_completeness registry_scope_status native_metadata_json native_field_states_json".split()
SCHEMAS = {
    COMMITTEES: pa.schema(
        [(n, pa.string()) for n in COMMON_TEXT + list(COLUMNS) + _EXTRA + ["committee_id_status"]]
        + [("source_cycle", pa.int32())]
    ),
    CONTROLS: pa.schema(
        [
            (n, pa.string())
            for n in COMMON_TEXT + _EXTRA + ["response_field", "response_field_role", "value_json", "value_status"]
        ]
        + [("source_cycle", pa.int32())]
    ),
}


def map_committee_api(row, selection: CollectionSelection):
    """Return one snapshot observation or response control and exact evidence.

    No grouping by committee ID, cycle selection, prior-table merge or current
    registry replacement occurs. Native arrays preserve null/missing/empty and
    unsupported shapes independently of the historical committee table defaults.
    """
    url = urlsplit(row["source_url"])
    if (
        url.scheme != "https"
        or url.hostname != "api.open.fec.gov"
        or url.path != "/v1/committees/"
        or url.port not in (None, 443)
        or url.username is not None
        or url.password is not None
        or url.fragment
        or row.get("profile") != "document"
    ):
        raise ValueError("Expected the selected source-owned committee API route")
    native = json.loads(row["metadata_json"])
    locator = json.loads(row["source_locator_json"])
    source = native.get("source", {})
    if (
        source.get("sha256") != row["source_sha256"]
        or locator.get("sha256") != row["source_sha256"]
        or source.get("response_mode") != "page"
        or locator.get("response_mode") != "page"
        or locator.get("member") is not None
    ):
        raise ValueError("Committee API observation differs from its source response")
    tables = {COMMITTEES: [], CONTROLS: []}
    kind = native.get("kind")
    if kind == "api-record-observation" and isinstance(native.get("metadata"), dict):
        table = COMMITTEES
        doc = native["metadata"]
        pointer = source.get("pointer")
        if not isinstance(pointer, str) or not pointer.startswith("/results/") or pointer != locator.get("pointer"):
            raise ValueError("Committee result differs from its native result pointer")
        suffix = pointer.removeprefix("/results/")
        if not suffix.isascii() or not suffix.isdigit() or str(int(suffix)) != suffix:
            raise ValueError("Committee result requires its exact array position")
        result = dict.fromkeys(SCHEMAS[table].names)
        shaped = _shape(doc)
        problems = {}
        for column in COLUMNS:
            if column in {"candidate_ids_json", "cycles_json"}:
                field = column.removesuffix("_json")
                shaped[column] = _json(doc[field]) if field in doc else None
                if field in doc and doc[field] is not None and not isinstance(doc[field], list):
                    problems[field] = "source_value_is_not_an_array"
            else:
                shaped[column] = _scalar(shaped[column])
        result.update(shaped)
        expected = {n.removesuffix("_json") for n in COLUMNS}
        result.update(
            source_pointer="/metadata",
            original_result_pointer=pointer,
            native_metadata_json=_json(doc),
            native_field_states_json=_json({field: _state(doc, field) for field in sorted(set(doc) | expected)}),
            committee_id_status=_id_status(result["committee_id"], "committee"),
        )
    elif (
        kind == "api-response-field" and isinstance(native.get("field"), str) and native["field"] and "value" in native
    ):
        table = CONTROLS
        result = dict.fromkeys(SCHEMAS[table].names)
        problems = {}
        result.update(
            source_pointer="/value",
            response_field=native["field"],
            response_field_role="result-container" if native["field"] == "results" else "response-control",
            value_json=_json(native["value"]),
            value_status=_state(native, "value"),
            native_metadata_json=_json(native),
            native_field_states_json=_json({"value": _state(native, "value")}),
        )
    else:
        raise ValueError("Expected a source-owned committee result or response control")
    result.update(observation_fields(table, row, selection, MAPPING_VERSION))
    result.update(
        mapping_status="partial" if problems else "mapped",
        mapping_reason_json=_json(problems),
        source_namespace="fec-retained-committee-api",
        source_url=row["source_url"],
        query_completeness="not-asserted",
        registry_scope_status="retained-source-observations-only",
    )
    tables[table].append(result)
    return tables, record_evidence(table, result["record_id"], row, selection.source_generation_pin)
