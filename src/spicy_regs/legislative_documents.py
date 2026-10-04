"""Explicit subject shapes for legislative documents and their retained ETL inputs.

SpicyDocs continues to return source observations. These mappings run at the
SpicyRegs writer boundary, after source qualification and before receipt split.
The field registry documents every admitted input; an unreviewed field refuses.
"""

from __future__ import annotations

import hashlib
import json
import re
from decimal import Decimal, InvalidOperation, localcontext
from functools import cache
from importlib.resources import files
from typing import Any, Mapping

import pyarrow as pa


class LegislativeShapeError(ValueError):
    """An input cannot be represented without losing its stated meaning."""


@cache
def field_registry() -> dict[str, dict]:
    return json.loads(files("spicy_regs").joinpath("legislative_document_fields.json").read_text())


S = pa.string()
INT = pa.int64()
MONEY = pa.decimal128(38, 2)
TYPES = {
    "string": S,
    "int64": INT,
    "bool": pa.bool_(),
    "decimal128(38,2)": MONEY,
    "list<string>": pa.list_(S),
    "list<decimal128(38,2)>": pa.list_(MONEY),
}


def _struct_list(spelling: str, fields: list[tuple[str, pa.DataType]]) -> None:
    TYPES[spelling] = pa.list_(pa.struct(fields))


_struct_list("list<struct<bill_id:string,context:string>>", [("bill_id", S), ("context", S)])
_struct_list("list<struct<element:string,publisher_id:string,publisher_identifier:string,number:string,heading:string>>",
             [(name, S) for name in ("element", "publisher_id", "publisher_identifier", "number", "heading")])
_struct_list("list<struct<usc_key:string,detail:string>>", [("usc_key", S), ("detail", S)])
_struct_list(
    "list<struct<chamber:string,congress:int64,name:string,system_code:string>>",
    [("chamber", S), ("congress", INT), ("name", S), ("system_code", S)],
)
_struct_list(
    "list<struct<estimate_index:int64,title:string,description:string,pub_date:string>>",
    [("estimate_index", INT), ("title", S), ("description", S), ("pub_date", S)],
)
_struct_list(
    "list<struct<citation:string,congress:string,number:string,part:string,report_type:string>>",
    [(n, S) for n in ("citation", "congress", "number", "part", "report_type")],
)
TYPES["list<list<struct<amount:decimal128(38,2),text:string>>>"] = pa.list_(
    pa.list_(pa.struct([("amount", MONEY), ("text", S)]))
)
_struct_list(
    "list<struct<cite_kind:string,normalized_key:string,matched_text:string,span_start:int64,span_end:int64,target_kind:string,target_key:string,target_grain:string>>",
    [(n, INT if n in {"span_start", "span_end"} else S) for n in (
        "cite_kind", "normalized_key", "matched_text", "span_start", "span_end", "target_kind", "target_key", "target_grain"
    )],
)

# Lookup diagnostics are retained inside the original receipt input. Only the
# candidate's legal target and literal occurrence enter the subject list.
CANDIDATE_RECEIPT_FIELDS = frozenset({
    "candidate_keys", "derivation_rule", "derivation_version", "document_key", "document_kind",
    "expected_cardinality", "match_count", "occurrence_key", "reason", "resolution_rule",
    "source_record_key", "source_status", "target_resolved", "target_snapshot", "target_status",
    "target_table_selected", "text_sha256",
})


@cache
def subject_schema(dataset: str) -> pa.Schema:
    spec = field_registry()[dataset]
    fields = [(f["target"], TYPES[f["target_type"]]) for f in spec["fields"] if f["target"]]
    fields += [(name, TYPES[f["type"]]) for name, f in spec.get("derived_fields", {}).items()]
    if len({n for n, _ in fields}) != len(fields):
        raise LegislativeShapeError(f"{dataset}: duplicate subject column")
    return pa.schema(fields)


def _object(pairs):
    result = {}
    for name, value in pairs:
        if name in result:
            raise ValueError("Repeated JSON object property")
        result[name] = value
    return result


def _json(value: Any, field: str) -> Any:
    if value is None:
        return None
    if not isinstance(value, str):
        raise LegislativeShapeError(f"{field}: expected retained JSON text")
    try:
        return json.loads(value, parse_float=Decimal, object_pairs_hook=_object,
                          parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
    except (ValueError, TypeError) as error:
        raise LegislativeShapeError(f"{field}: invalid retained JSON") from error


def _typed(value: Any, dtype: pa.DataType, field: str) -> Any:
    if value is None:
        return None
    if pa.types.is_string(dtype):
        if not isinstance(value, str):
            raise LegislativeShapeError(f"{field}: expected string")
        return value
    if pa.types.is_boolean(dtype):
        if type(value) is bool:
            return value
        if value in ("true", "false"):
            return value == "true"
        raise LegislativeShapeError(f"{field}: expected true or false")
    if pa.types.is_integer(dtype):
        if type(value) is int:
            return value
        if isinstance(value, str) and re.fullmatch(r"-?[0-9]+", value):
            return int(value)
        raise LegislativeShapeError(f"{field}: expected integer without rounding")
    if pa.types.is_decimal(dtype):
        if isinstance(value, bool) or not isinstance(value, (str, int, Decimal)):
            raise LegislativeShapeError(f"{field}: money must be exact decimal text or a JSON number")
        try:
            amount = Decimal(value)
            with localcontext() as ctx:
                ctx.prec = 80
                if not amount.is_finite() or amount != amount.quantize(Decimal("0.01")):
                    raise LegislativeShapeError(f"{field}: money cannot be rounded")
            return amount
        except InvalidOperation as error:
            raise LegislativeShapeError(f"{field}: invalid decimal amount") from error
    if pa.types.is_list(dtype):
        if not isinstance(value, list):
            raise LegislativeShapeError(f"{field}: expected ordered list")
        return [_typed(v, dtype.value_type, field + "[]") for v in value]
    if pa.types.is_struct(dtype):
        if not isinstance(value, dict) or set(value) - set(dtype.names):
            raise LegislativeShapeError(f"{field}: unreviewed struct property")
        return {f.name: _typed(value.get(f.name), f.type, field + "." + f.name) for f in dtype}
    raise LegislativeShapeError(f"{field}: unsupported type {dtype}")


def _key(kind: str, values: list[Any]) -> str:
    if any(not isinstance(v, str) or not v for v in values):
        raise LegislativeShapeError(f"{kind}: every identity component must be a nonempty string")
    encoded = json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    return kind + ":" + hashlib.sha256(encoded).hexdigest()


def body_version_id(value: Any) -> str | None:
    """An explicit content-version key, consistent across occurrences and readers."""
    if value is None:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"(?:sha256:)?[0-9a-f]{64}", value):
        raise LegislativeShapeError("body version requires a complete SHA-256 digest")
    return "body:sha256:" + value.removeprefix("sha256:")


def bill_section_document_key(value: Any) -> str:
    """Translate an exact legacy held-field key to the native section identity."""
    try:
        parts = json.loads(value)
    except (ValueError, TypeError) as error:
        raise LegislativeShapeError("bill-section document key must be a JSON key tuple") from error
    if not isinstance(parts, list) or len(parts) != 4 or any(not isinstance(p, str) or not p for p in parts):
        raise LegislativeShapeError("bill-section document key requires bill, printing, source, and sequence")
    bill, version, source, seq = parts
    if re.fullmatch(r"printing:[0-9a-f]{64}", source):
        return json.dumps(parts, ensure_ascii=False, separators=(",", ":"))
    return json.dumps([bill, version, _key("printing", [bill, version, source]), seq], ensure_ascii=False, separators=(",", ":"))


def _identity_value(column: str, row: Mapping[str, Any]) -> str | None:
    value = row.get(column)
    if column in {"text_sha256", "input_sha256"}:
        return body_version_id(value)
    if column == "link_source":
        return _key("hearing-link", [row.get("package_id"), row.get("bill_id"), value])
    if column == "equivalent_xml_source":
        if value is None and row.get("equivalent_xml_version_code") is None:
            return None
        return _key("printing", [row.get("bill_id"), row.get("equivalent_xml_version_code"), value])
    prefix = column.removesuffix("source")
    return _key("printing", [row.get("bill_id"), row.get(prefix + "version_code"), value])


def map_subject(dataset: str, row: Mapping[str, Any]) -> dict[str, Any] | None:
    """Map one qualified source-owner row; keep the untouched input for its receipt."""
    spec = field_registry()[dataset]
    allowed = {f["name"] for f in spec["fields"]}
    if set(row) - allowed:
        raise LegislativeShapeError(f"{dataset}: unreviewed input columns: {sorted(set(row) - allowed)}")
    if spec["processing_only"]:
        for key in spec["identity_fields"]:
            if not isinstance(row.get(key), str) or not row[key]:
                raise LegislativeShapeError(f"{dataset}: missing processing scope {key}")
        return None
    subject = {}
    for field in spec["fields"]:
        name, target = field["name"], field["target"]
        if target is None:
            continue
        value = row.get(name)
        if field["decision"] == "stable-key":
            value = _identity_value(name, row)
        elif field["decision"] == "native-list":
            if name in {"match_path", "billstatus_action_code"}:
                value = None if value is None else ([] if value == "" else value.split("\x1f"))
            else:
                value = _json(value, name)
            if dataset == "law_sections" and name == "hierarchy_json" and value is not None:
                if not isinstance(value, list) or any(not isinstance(v, dict) for v in value):
                    raise LegislativeShapeError("hierarchy_json: expected list of ancestor objects")
                value = [{k: x for k, x in v.items() if k != "native_path"} for v in value]
            if name in {"target_candidates_json", "restatements_json"} and value is not None:
                excluded = CANDIDATE_RECEIPT_FIELDS if name == "target_candidates_json" else {"url"}
                if not isinstance(value, list) or any(v is not None and not isinstance(v, dict) for v in value):
                    raise LegislativeShapeError(f"{name}: expected list of objects")
                value = [None if v is None else {k: x for k, x in v.items() if k not in excluded} for v in value]
        subject[target] = _typed(value, TYPES[field["target_type"]], name)
    if dataset == "document_citations" and row.get("document_kind") == "bill_section":
        subject["document_key"] = bill_section_document_key(row.get("document_key"))
    for name, derived in spec.get("derived_fields", {}).items():
        value = row.get(derived["from"])
        if "property" in derived:
            obj = _json(value, derived["from"])
            if obj is not None and not isinstance(obj, dict):
                raise LegislativeShapeError("native attributes must be an object")
            value = None if obj is None else obj.get(derived["property"])
            subject[name] = _typed(value, S, name)
        else:
            subject[name] = body_version_id(value)
    for key in spec["identity_fields"]:
        if subject.get(key) is None or subject.get(key) == "":
            raise LegislativeShapeError(f"{dataset}: missing subject identity {key}")
    try:
        # Arrow enforces integer/decimal bounds as well as the complete shape.
        pa.Table.from_pylist([subject], schema=subject_schema(dataset))
    except (pa.ArrowException, OverflowError) as error:
        raise LegislativeShapeError(f"{dataset}: subject cannot be represented exactly") from error
    return subject
