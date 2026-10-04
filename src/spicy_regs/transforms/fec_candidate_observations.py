"""Literal retained candidate API observations, separate from bulk histories."""

from dataclasses import dataclass
import hashlib
import json

import pyarrow as pa
from spicy_docs.sources.fec.candidate_profile import candidate_query_scope
from spicy_docs.sources.fec.originals import original_capture

from .fec_committee_observations import CONTROLS, SCHEMAS as COMMITTEE_SCHEMAS
from .fec_identity_observations import _json, _scalar
from .fec_identity_shape import array_values, typed_columns, typed_values
from .fec_context_shape import api_control_values
from .fec_query import CollectionSelection, SOURCE_TEXT, subject_observation_fields, record_evidence
from .fec_relationships import _id_status

CANDIDATES = "fec_candidate_api_observations"
MAPPING_VERSION = "fec-retained-candidate-api/2"
FIELDS = "candidate_id name party party_full office office_full state district candidate_status incumbent_challenge incumbent_challenge_full".split()
ARRAYS = "cycles election_years election_districts inactive_election_years".split()
TYPED = {
    "district_number": "integer",
    "active_through": "year",
    "candidate_inactive": "boolean",
    "has_raised_funds": "boolean",
    "federal_funds_flag": "boolean",
    "first_file_date": "date",
    "last_file_date": "date",
    "last_f2_date": "date",
}
SCHEMAS = {
    CANDIDATES: pa.schema(
        [
            (n, pa.string())
            for n in SOURCE_TEXT
            + FIELDS
            + [n + suffix for n in ARRAYS for suffix in ("_json", "_status")]
            + "source_url source_pointer original_result_pointer candidate_id_status query_completeness".split()
        ]
        + typed_columns(TYPED)
        + [("source_cycle", pa.int32())]
    ),
    CONTROLS: COMMITTEE_SCHEMAS[CONTROLS],
}


@dataclass(frozen=True)
class CandidateSelection:
    selection: CollectionSelection
    request_url: str


def prepare_candidate_api(entry, *, source_generation_pin):
    """Bind an exact source-owned candidates request without inferring a cycle."""
    scope = entry["scope"]
    if (
        entry.get("profile") != "document"
        or scope.get("format") != "api-json"
        or scope.get("api_mode") != "page"
        or scope.get("member") is not None
    ):
        raise ValueError("Expected the selected native candidate API response")
    original = original_capture(scope["capture"])
    if original["representation"] != "opaque":
        raise ValueError("Candidate API response must be a native JSON original")
    capture = candidate_query_scope(
        [
            {
                k: v
                for k, v in original.items()
                if k in {"requestUrl", "observedAt", "responseSha256", "byteSize", "resolvedUrl", "mediaType", "via"}
            }
        ]
    )["captures"][0]
    return CandidateSelection(
        CollectionSelection(
            entry["collection_id"],
            capture["responseSha256"],
            source_generation_pin,
            "official-fec",
            None,
            "snapshot",
            "sha256:" + hashlib.sha256(_json(entry).encode()).hexdigest(),
        ),
        capture["requestUrl"],
    )


def map_candidate_api(row, prepared: CandidateSelection):
    """Expose useful facts with exact links to native evidence, without new entities."""
    selection = prepared.selection
    native = json.loads(row["metadata_json"])
    locator = json.loads(row["source_locator_json"])
    source = native.get("source", {})
    if (
        row.get("profile") != "document"
        or row["source_url"] != prepared.request_url
        or source.get("sha256") != row["source_sha256"]
        or locator.get("sha256") != row["source_sha256"]
        or source.get("response_mode") != "page"
        or locator.get("response_mode") != "page"
        or locator.get("member") is not None
    ):
        raise ValueError("Candidate observation differs from its selected native response")
    problems = {}
    if native.get("kind") == "api-record-observation" and isinstance(native.get("metadata"), dict):
        table, doc = CANDIDATES, native["metadata"]
        pointer = source.get("pointer")
        if not isinstance(pointer, str) or pointer != locator.get("pointer") or not pointer.startswith("/results/"):
            raise ValueError("Candidate observation differs from its native result pointer")
        ordinal = pointer.removeprefix("/results/")
        if not ordinal.isascii() or not ordinal.isdigit() or str(int(ordinal)) != ordinal:
            raise ValueError("Candidate observation requires its exact result array position")
        result = dict.fromkeys(SCHEMAS[table].names)
        result.update({name: _scalar(doc.get(name)) for name in FIELDS})
        for name in ARRAYS:
            result.update(array_values(doc, name))
            if name in doc and doc[name] is not None and not isinstance(doc[name], list):
                problems[name] = "source_value_is_not_an_array"
        result.update(
            source_pointer="/metadata",
            original_result_pointer=pointer,
            candidate_id_status=_id_status(result["candidate_id"], "candidate"),
        )
        values, invalid = typed_values(doc, TYPED)
        result.update(values)
        problems.update(invalid)
    elif (
        native.get("kind") == "api-response-field"
        and isinstance(native.get("field"), str)
        and native["field"]
        and "value" in native
    ):
        table = CONTROLS
        result = dict.fromkeys(SCHEMAS[table].names)
        result.update(api_control_values(native))
        result.update(source_pointer="/value", registry_scope_status="retained-source-observations-only")
    else:
        raise ValueError("Expected a source-owned candidate result or response control")
    result.update(subject_observation_fields(table, row, selection, MAPPING_VERSION))
    result.update(
        mapping_status="partial" if problems else "mapped",
        mapping_reason_json=_json(problems),
        source_namespace="fec-retained-candidate-api",
        source_url=row["source_url"],
        query_completeness="not-asserted",
    )
    return {name: [result] if name == table else [] for name in SCHEMAS}, record_evidence(
        table, result["record_id"], row, selection.source_generation_pin
    )
