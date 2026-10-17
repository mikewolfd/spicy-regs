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
    if "".join(tokens) != re.sub(r"\s+", "", name):
        # Whitespace inside a quoted field name must be preserved.
        cleaned = re.sub(r'"(?:[^"]|"")*"|\s+', lambda m: m[0] if m[0].startswith('"') else "", name)
        if "".join(tokens) != cleaned:
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


def extra_field_checker(schema, *, label="row"):
    """Prepare, once for an immutable schema, the check that refuses undeclared record and struct fields.

    Refuses before Arrow or Polars can discard them. The declared names and every nested check are built here, so a
    row costs one set test per struct it holds and one call per nested value, never a rebuild of the schema.
    """
    return _checker(pa.struct(schema), label)


def _declared(value) -> None:
    """The check of a type that cannot hold a field: scalars, and lists of scalars."""


def _checker(dtype, path):
    """The prepared check of one declared type, or ``_declared`` when no value of it can hold an undeclared field."""
    if pa.types.is_struct(dtype):
        names = frozenset(field.name for field in dtype)
        fields = tuple((field.name, _checker(field.type, f"{path}.{field.name}")) for field in dtype)
        nested = tuple((name, check) for name, check in fields if check is not _declared)

        def check_struct(value):
            exact = type(value) is dict
            if exact or isinstance(value, Mapping):
                if not names.issuperset(value):
                    raise ValueError(f"{path}: unclassified fields {sorted(set(value) - names)}")
                # A plain dict has no side-effecting ``get``, so only the fields that can nest need reading; any other
                # mapping may define ``get``, so each declared field is read as the unprepared check did.
                for name, check in nested if exact else fields:
                    check(value.get(name))

        return check_struct
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        check_item = _checker(dtype.value_type, path)
        if check_item is _declared:
            return _declared

        def check_list(value):
            if isinstance(value, (list, tuple)):
                for item in value:
                    check_item(item)

        return check_list
    return _declared
