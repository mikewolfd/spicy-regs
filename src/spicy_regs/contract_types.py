"""SpicyDocs' contract column types as this repository writes and describes them.

A source declaration spells native SQL column types. Arrow writes their values;
DuckDB supplies the exact footer spelling, including recursive lists/structs
and decimals. DocSpec's older row profile requires a separately qualified
extension before admitting these new types.
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
    from spicy_regs.native_types import arrow_type as native_arrow_type

    return native_arrow_type(name)


def arrow_schema(contract) -> pa.Schema:
    """The Arrow schema a contract table's Parquet is written with."""
    import pyarrow as pa

    return pa.schema([(column, arrow_type(contract.column_type(column))) for column in contract.columns])


def described_columns(contract) -> list[tuple[str, str]]:
    """``[(column, DuckDB type)]`` as a written member's footer describes it."""
    return described_schema(arrow_schema(contract))


def described_schema(schema: pa.Schema) -> list[tuple[str, str]]:
    """``[(column, DuckDB type)]`` for a writer's complete native Arrow schema."""
    from spicy_regs.native_types import described_schema as native_described_schema

    return native_described_schema(schema)
