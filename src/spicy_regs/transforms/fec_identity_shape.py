"""Build-time scalar interpretation; originals remain in exact source evidence."""

from datetime import date
import json
import re

import pyarrow as pa


def raw_text(value):
    if value is None or isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def typed_columns(specs):
    kinds = {"date": pa.date32(), "boolean": pa.bool_(), "year": pa.int32(), "integer": pa.int64()}
    return [(name, kinds[kind]) for name, kind in specs.items()] + [
        (name + suffix, pa.string()) for name in specs for suffix in ("_raw", "_status")
    ]


def typed_values(native, specs, *, postgres=False):
    """Interpret only exact source spellings, preserving refusals and raw values."""
    result, problems = {}, {}
    for name, kind in specs.items():
        raw = native.get(name)
        value = None
        if name not in native:
            status = "source_missing"
        elif raw is None:
            status = "source_null"
        elif raw == "":
            status = "source_empty"
        else:
            status = "unsupported_" + kind
            if kind == "date" and isinstance(raw, str):
                pattern = r"[0-9]{4}-[0-9]{2}-[0-9]{2}" + (r"(?: 00:00:00)?" if postgres else "")
                if re.fullmatch(pattern, raw):
                    try:
                        value = date.fromisoformat(raw[:10])
                    except ValueError:
                        pass
            elif kind == "boolean":
                if type(raw) is bool:
                    value = raw
                elif postgres and raw in ("t", "f"):
                    value = raw == "t"
            elif kind in {"year", "integer"} and (type(raw) is int or isinstance(raw, str)):
                text = str(raw)
                pattern = r"[0-9]{4}" if kind == "year" else r"[0-9]+"
                if re.fullmatch(pattern, text):
                    parsed = int(text)
                    if parsed < 2**63:
                        value = parsed
            if value is not None:
                status = "parsed"
            else:
                problems[name] = status
        result.update({name: value, name + "_raw": raw_text(raw), name + "_status": status})
    return result, problems


def array_values(native, name):
    if name not in native:
        return {name + "_json": None, name + "_status": "source_missing"}
    value = native[name]
    status = (
        "source_null"
        if value is None
        else "source_empty"
        if value == []
        else "reported"
        if isinstance(value, list)
        else "unsupported_shape"
    )
    return {
        name + "_json": json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        name + "_status": status,
    }
