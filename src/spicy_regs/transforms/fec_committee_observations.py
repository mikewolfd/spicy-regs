"""Retained committee API observations, separate from the current registry.

Core committee fields reuse the maintained source mapping. Each observation
keeps its own identity and links to exact native evidence. Response controls
keep typed counts without copying results; captures do not imply a full traversal.
"""

import json
from urllib.parse import urlsplit

import pyarrow as pa

from .build_fec_committees import COLUMNS, _shape
from .fec_identity_observations import _json, _scalar
from .fec_identity_shape import array_values, typed_columns, typed_values
from .fec_context_shape import API_CONTROL_FIELDS, api_control_values
from .fec_query import CollectionSelection, SOURCE_TEXT, subject_observation_fields, record_evidence
from .fec_relationships import _id_status

COMMITTEES = "fec_committee_observations"
CONTROLS = "fec_api_response_controls"
MAPPING_VERSION = "fec-retained-committee-api/2"
_EXTRA = "source_url source_pointer original_result_pointer query_completeness registry_scope_status".split()
TYPED = {name: "date" for name in ("first_file_date", "last_file_date", "first_f1_date", "last_f1_date")}
ARRAYS = ("cycles", "candidate_ids", "sponsor_candidate_ids")
SCHEMAS = {
    COMMITTEES: pa.schema(
        [
            (n, pa.string())
            for n in SOURCE_TEXT
            + [n for n in COLUMNS if n not in TYPED and not n.endswith("_json")]
            + _EXTRA
            + ["committee_id_status", "affiliated_committee_name", "organization_type"]
            + [name + suffix for name in ARRAYS for suffix in ("_json", "_status")]
        ]
        + typed_columns(TYPED)
        + [("source_cycle", pa.int32())]
    ),
    CONTROLS: pa.schema(
        [(n, pa.string()) for n in SOURCE_TEXT + [c for c in _EXTRA if c != "original_result_pointer"]]
        + API_CONTROL_FIELDS
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
            if column not in TYPED and not column.endswith("_json"):
                result[column] = _scalar(shaped[column])
        for name in ARRAYS:
            result.update(array_values(doc, name))
            if result[name + "_status"] == "unsupported_shape":
                problems[name] = "source_value_is_not_an_array"
        for name in ("affiliated_committee_name", "organization_type"):
            result[name] = _scalar(doc.get(name))
        values, invalid = typed_values(doc, TYPED)
        result.update(values)
        problems.update(invalid)
        result.update(
            source_pointer="/metadata",
            original_result_pointer=pointer,
            committee_id_status=_id_status(result["committee_id"], "committee"),
        )
    elif (
        kind == "api-response-field" and isinstance(native.get("field"), str) and native["field"] and "value" in native
    ):
        table = CONTROLS
        result = dict.fromkeys(SCHEMAS[table].names)
        problems = {}
        result.update(api_control_values(native))
        result["source_pointer"] = "/value"
    else:
        raise ValueError("Expected a source-owned committee result or response control")
    result.update(subject_observation_fields(table, row, selection, MAPPING_VERSION))
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
