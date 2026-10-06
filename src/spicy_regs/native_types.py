"""Native Arrow, Polars and SQL descriptions without a scalar-only lookup."""
from __future__ import annotations

import re
from collections.abc import Mapping
from functools import lru_cache

from duckdb import connect as _connect
import pyarrow as pa


@lru_cache(maxsize=256)
def arrow_type(name: str) -> pa.DataType:
    """Parse declared SQL scalars, decimals, lists and nested structures.

    The accepted grammar contains types and quoted field names only. DuckDB
    supplies native nested-type parsing; source values never enter the SQL.
    """
    tokens = re.findall(r'"(?:[^"]|"")*"|[A-Za-z_][A-Za-z_0-9]*|[0-9]+|[(),\[\]]', name)
    if ''.join(tokens) != re.sub(r'\s+', '', name):
        # Whitespace inside a quoted field name must be preserved.
        cleaned = re.sub(r'"(?:[^"]|"")*"|\s+', lambda m: m[0] if m[0].startswith('"') else '', name)
        if ''.join(tokens) != cleaned:
            raise ValueError(f"Invalid declared native type: {name!r}")
    with _connect() as con:
        table = con.execute(f"SELECT CAST(NULL AS {name}) AS value").to_arrow_table()
    dtype = table.schema.field(0).type
    # Preserve the existing UTC spelling, independent of machine timezone.
    if pa.types.is_timestamp(dtype) and dtype.tz:
        return pa.timestamp("us", tz="UTC")
    return dtype


@lru_cache(maxsize=256)
def described_schema(schema: pa.Schema) -> list[tuple[str, str]]:
    """Describe exactly the native types a Parquet reader observes."""
    with _connect() as con:
        con.register("native_schema", schema.empty_table())
        return [(row[0], row[1]) for row in con.execute("DESCRIBE SELECT * FROM native_schema").fetchall()]


def polars_sql_type(dtype) -> str:
    import polars as pl
    schema = pl.DataFrame(schema={"value": dtype}).to_arrow().schema
    return described_schema(schema)[0][1]


def reject_extra_fields(row, schema, *, label="row") -> None:
    """Refuse extra record/struct fields before Arrow or Polars can discard them."""
    _check_extra_fields(row, pa.struct(schema), label)


def extra_field_checker(schema, *, label="row"):
    """Prepare the same field checks once for a batch writer's immutable schema."""
    dtype = pa.struct(schema)
    flat = all(not (pa.types.is_struct(f.type) or pa.types.is_list(f.type)
                    or pa.types.is_large_list(f.type)) for f in dtype)
    names = frozenset(schema.names) if flat else None

    def check(row):
        if names is not None and type(row) is dict:
            if extra := set(row) - names:
                raise ValueError(f"{label}: unclassified fields {sorted(extra)}")
        else:
            _check_extra_fields(row, dtype, label)
    return check


def _check_extra_fields(value, dtype, path):
    if value is None:
        return
    if pa.types.is_struct(dtype) and isinstance(value, Mapping):
        if extra := set(value) - {f.name for f in dtype}:
            raise ValueError(f"{path}: unclassified fields {sorted(extra)}")
        for f in dtype:
            _check_extra_fields(value.get(f.name), f.type, f"{path}.{f.name}")
    elif (pa.types.is_list(dtype) or pa.types.is_large_list(dtype)) and isinstance(value, (list, tuple)):
        for item in value:
            _check_extra_fields(item, dtype.value_type, path)
