"""SpicyDocs' contract column types as this repository writes and describes them.

A contract spells a column's type with DocSpec's table-profile name (``spicy_docs.schemas.COLUMN_TYPES``); VARCHAR is
the default. Here each name maps to the Arrow type the writers use and to DuckDB's ``DESCRIBE`` spelling, which the
publication descriptors and the dictionary carry and DocSpec's admission compares with the footer. Timestamps are
microsecond precision, as admission requires. A writer with its own Arrow schema (the rulemaking lifecycle tables) is
spelled the same way.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa

#: The DuckDB ``DESCRIBE`` spelling of each contract type's Parquet column.
DESCRIBED: dict[str, str] = {
    "VARCHAR": "VARCHAR",
    "BOOLEAN": "BOOLEAN",
    "INTEGER": "INTEGER",
    "BIGINT": "BIGINT",
    "DOUBLE": "DOUBLE",
    "DATE": "DATE",
    "TIMESTAMP": "TIMESTAMP",
    "TIMESTAMPTZ": "TIMESTAMP WITH TIME ZONE",
    "VARCHAR[]": "VARCHAR[]",
}


def arrow_type(name: str) -> pa.DataType:
    """The Arrow type a contract column of type ``name`` is written as."""
    import pyarrow as pa

    return {
        "VARCHAR": pa.string(),
        "BOOLEAN": pa.bool_(),
        "INTEGER": pa.int32(),
        "BIGINT": pa.int64(),
        "DOUBLE": pa.float64(),
        "DATE": pa.date32(),
        "TIMESTAMP": pa.timestamp("us"),
        "TIMESTAMPTZ": pa.timestamp("us", tz="UTC"),
        "VARCHAR[]": pa.list_(pa.string()),
    }[name]


def arrow_schema(contract) -> pa.Schema:
    """The Arrow schema a contract table's Parquet is written with."""
    import pyarrow as pa

    return pa.schema([(column, arrow_type(contract.column_type(column))) for column in contract.columns])


def described_columns(contract) -> list[tuple[str, str]]:
    """``[(column, DuckDB type)]`` as a written member's footer describes it."""
    return [(column, DESCRIBED[contract.column_type(column)]) for column in contract.columns]


def described_schema(schema: pa.Schema) -> list[tuple[str, str]]:
    """``[(column, DuckDB type)]`` for a writer's own Arrow schema; a type no contract names raises KeyError."""
    spelled = {arrow_type(name): DESCRIBED[name] for name in DESCRIBED}
    return [(field.name, spelled[field.type]) for field in schema]
