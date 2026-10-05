"""SQL that reproduces the receipt writer's exact encoding and digests, a column at a time.

``etl_receipts.exact_json`` encodes one Python value. These build the DuckDB expression that yields the same text for a
column of an Arrow type, so a table's identities, versions and receipt ids come from one scan instead of one call per
row (2026-10-05: 26M comments took about 20 hours by row, and a million of them 82 seconds this way).

The row functions stay the reference. DuckDB spells one class of text differently: a control character with no short
JSON escape is written ``\\u001B`` where Python writes ``\\u001b``. :func:`needs_reference_sql` marks every text that
holds such an escape, at any depth, and a marked row belongs to the row writer.
"""

from __future__ import annotations

import json
from collections.abc import Iterable

import pyarrow as pa

from spicy_regs.etl_receipts import RECEIPT_SCHEMA, DatasetPolicy

#: An upper-case hex letter in a control character's escape: the backslash run before ``u`` is odd, so it is an escape
#: and not a doubled literal backslash followed by the letters.
_UPPER_HEX_ESCAPE = r"(^|[^\\])(\\\\)*\\u00[01][A-F]"


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def exact_json_sql(expression: str, dtype: pa.DataType) -> str:
    """SQL text equal to ``exact_json`` of ``expression``'s value: the tagged encoding with keys in sorted order.

    Covers the types whose DuckDB text equals Python's: strings, integers, booleans, and structs and lists of them.
    Any other type refuses, so its table stays with the row writer until a test here proves its spelling.
    """
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        body = f"'[\"str\",' || CAST(to_json({expression}) AS VARCHAR) || ']'"
    elif pa.types.is_integer(dtype):
        body = f"'[\"int\",' || CAST({expression} AS VARCHAR) || ']'"
    elif pa.types.is_boolean(dtype):
        body = f"'[\"bool\",' || CASE WHEN {expression} THEN 'true' ELSE 'false' END || ']'"
    elif pa.types.is_struct(dtype):
        body = record_json_sql(((field.name, field.type) for field in dtype), qualifier=expression + ".")
    elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        item = exact_json_sql("item", dtype.value_type)
        body = f"'[\"list\",[' || array_to_string(list_transform({expression}, item -> {item}), ',') || ']]'"
    else:
        raise NotImplementedError(f"No proven SQL spelling for {dtype}")
    return f"CASE WHEN {expression} IS NULL THEN '[\"null\",null]' ELSE {body} END"


def record_json_sql(fields: Iterable[tuple[str, pa.DataType]], *, qualifier: str = "") -> str:
    """SQL text equal to ``exact_json`` of a mapping of the named columns (or of a struct's fields)."""
    pairs = [
        _literal("[" + json.dumps(name, ensure_ascii=False) + ",")
        + " || "
        + exact_json_sql(qualifier + '"' + name.replace('"', '""') + '"', dtype)
        + " || ']'"
        for name, dtype in sorted(fields)
    ]
    return "'[\"dict\",[' || " + " || ',' || ".join(pairs) + " || ']]'" if pairs else "'[\"dict\",[]]'"


def digest_sql(text: str) -> str:
    """SQL equal to ``etl_receipts._digest`` of a value whose exact encoding is ``text``."""
    return f"'sha256:' || sha256({text})"


def needs_reference_sql(text: str) -> str:
    """Whether an encoding built here differs from the reference's: it holds an upper-case hex control escape."""
    return f"regexp_matches({text}, {_literal(_UPPER_HEX_ESCAPE)})"


def identity_sql(policy: DatasetPolicy) -> tuple[str, str, str]:
    """SQL for ``subject_identity`` over a table of the policy's subject columns: record id, version, identity text."""
    types = {field.name: field.type for field in policy.subject_schema}
    pairs = " || ',' || ".join(
        _literal('["list",[["str",' + json.dumps(key, ensure_ascii=False) + "],")
        + " || "
        + exact_json_sql('"' + key.replace('"', '""') + '"', types[key])
        + " || ']]'"
        for key in policy.identity_fields
    )
    identity = "'[\"list\",[' || " + pairs + " || ']]'"
    head = _literal('["list",[["str",' + json.dumps(policy.dataset, ensure_ascii=False) + "],")
    subject = record_json_sql((field.name, field.type) for field in policy.subject_schema)
    return (
        digest_sql(f"{head} || {identity} || ']]'"),
        digest_sql(f"{head} || {subject} || ']]'"),
        identity,
    )


def receipt_json_sql() -> str:
    """SQL text of a receipt row without its id: what ``receipt_id`` is the digest of."""
    return record_json_sql((field.name, field.type) for field in RECEIPT_SCHEMA if field.name != "receipt_id")
