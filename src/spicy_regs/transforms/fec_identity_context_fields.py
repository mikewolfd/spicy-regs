"""Reviewed per-field FEC identity/context rules, separate from receipt storage."""

from __future__ import annotations

from functools import cache
import hashlib
from importlib.resources import files
import json

import pyarrow as pa
from spicy_regs.contract_types import arrow_type

from .fec_identity_native import native_list, native_scalar

VERSION = "fec-identity-context-receipts/1"
REGISTRY = json.loads(files("spicy_regs").joinpath("fec_identity_context_fields.json").read_text())

_native_type = cache(arrow_type)


@cache
def subject_schema(table):
    return pa.schema([(name, _native_type(kind)) for name, kind in REGISTRY[table]["subject_fields"].items()])


def normalize_record(table, row, *, source_input=None):
    """Classify all input fields; preserve exact conversion inputs separately."""
    rules = REGISTRY[table]
    extra = set(row) - set(rules["input_fields"])
    if extra:
        raise ValueError(f"{table}: unclassified mapper fields {sorted(extra)}")
    result = dict(row)
    diagnostics, originals = {}, {}
    for raw, (name, kind) in rules["native_lists"].items():
        value = row.get(raw)
        list_kind = (
            "integer"
            if kind == "INTEGER[]"
            else "date"
            if kind == "DATE[]"
            else "sponsor_candidate"
            if name == "sponsor_candidate_list"
            else "meeting_link"
            if table == "fec_research_meeting_observations" and name == "links"
            else "page_link"
            if name == "links"
            else "filing_number"
            if name == "amendment_chain"
            else "string"
        )
        result[name], problems = native_list(
            value, list_kind, candidate_ids=name in {"candidate_ids", "sponsor_candidate_ids"}
        )
        if problems:
            diagnostics[name] = problems
    for name, kind in rules["native_scalars"].items():
        if name in row:
            originals[name] = row[name]
        result[name], problem = native_scalar(row.get(name), kind)
        if problem:
            diagnostics[name] = [{"reason": problem, "value": row.get(name)}]
    if table == "fec_relationships":
        # The source coordinate includes array ordinal, preserving repeated relationships.
        identity = [row.get(k) for k in ("source_family", "source_sha256", "source_locator_json", "source_fields_json")]
        result["record_id"] = (
            "sha256:"
            + hashlib.sha256(json.dumps(identity, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()
        )
    elif table == "fec_research_meeting_observations":
        result["date_kind"] = {
            "source_single_date": "single_date",
            "source_date_range": "range",
            "source_listed_dates": "listed_dates",
        }.get(row.get("date_status"))
    elif table == "org_committee_links":
        result["association_kind"] = "name_similarity_candidate"
    for name in rules["subject_fields"]:
        result.setdefault(name, None)
    result.update(conversion_inputs=originals, conversion_diagnostics=diagnostics, source_input=source_input)
    return result
