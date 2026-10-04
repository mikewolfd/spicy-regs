"""Explicit native conversions for FEC identity/context subject fields.

Conversion evidence is returned separately; invalid elements retain their source
position. Candidate identifier validity never deletes a source-reported string.
"""

from __future__ import annotations

from datetime import date, datetime
import json
import re
from typing import Any, cast


def _invalid_constant(value):
    raise ValueError(f"Nonfinite JSON constant: {value}")


def native_list(raw, kind, *, candidate_ids=False):
    """Return (native values, diagnostics), keeping repeats, nulls and order."""
    if raw is None:
        return None, []
    try:
        values = json.loads(raw, parse_constant=_invalid_constant) if isinstance(raw, str) else raw
    except (ValueError, TypeError):
        return None, [{"reason": "invalid_json"}]
    if values is None:
        return None, []
    if not isinstance(values, list):
        return None, [{"reason": "expected_array"}]
    output, diagnostics = [], []
    for ordinal, value in enumerate(values):
        parsed, problem = value, None
        if value is not None:
            if kind == "integer":
                if type(value) is not int or not -(2**31) <= value < 2**31:
                    parsed, problem = None, "unsupported_integer"
            elif kind == "string":
                if not isinstance(value, str):
                    parsed, problem = None, "unsupported_string"
                elif candidate_ids and not re.fullmatch(r"[HSP][0-9A-Z]{8}", value):
                    problem = "invalid_candidate_identifier_preserved"
            elif kind == "filing_number":
                if type(value) is int:
                    parsed = str(value)
                elif not isinstance(value, str):
                    parsed, problem = None, "unsupported_filing_number"
            elif kind == "date":
                try:
                    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
                        raise ValueError()
                    parsed = date.fromisoformat(value)
                except ValueError:
                    parsed, problem = None, "unsupported_date"
            elif kind in {"meeting_link", "page_link", "sponsor_candidate"}:
                keys = (
                    ("sponsor_candidate_id", "sponsor_candidate_name")
                    if kind == "sponsor_candidate"
                    else ("url", "href", "label")
                    if kind == "page_link"
                    else ("url", "href")
                )
                if not isinstance(value, dict):
                    parsed, problem = None, "unsupported_link"
                else:
                    allowed = set(keys) | (
                        set() if kind == "sponsor_candidate" else {"body_status", "source_fact_index", "label_status"}
                    )
                    if set(value) - allowed:
                        raise ValueError(f"Unclassified native {kind} fields: {sorted(set(value) - allowed)}")
                    value = cast(dict[str, Any], value)
                    parsed = {key: value.get(key) if isinstance(value.get(key), str) else None for key in keys}
                    if any(value.get(key) is not None and not isinstance(value.get(key), str) for key in keys):
                        problem = "unsupported_link_property"
            else:
                raise ValueError(f"Unsupported FEC native list kind: {kind}")
        output.append(parsed)
        if problem:
            diagnostics.append({"ordinal": ordinal, "reason": problem, "value": value})
    return output, diagnostics


def native_scalar(value, kind):
    """Convert reviewed exact spellings; never infer a missing year or timezone."""
    if value is None or value == "":
        return None, None
    if kind == "integer":
        if type(value) is int and -(2**31) <= value < 2**31:
            return value, None
        if isinstance(value, str) and re.fullmatch(r"[0-9]+", value) and int(value) < 2**31:
            return int(value), None
    elif kind == "date":
        if type(value) is date:
            return value, None
        if isinstance(value, str) and re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value):
            try:
                return date.fromisoformat(value), None
            except ValueError:
                pass
    elif kind == "timestamp":
        if isinstance(value, datetime) and value.tzinfo is not None:
            return value, None
        if isinstance(value, str):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
                if parsed.tzinfo is not None:
                    return parsed, None
            except ValueError:
                pass
    else:
        raise ValueError(f"Unsupported FEC scalar kind: {kind}")
    return None, "unsupported_" + kind
