"""Reject silent field loss while turning reviewed FEC mappings into Arrow."""

from collections.abc import Mapping

import pyarrow as pa


def typed_batch(rows: list[dict], schema: pa.Schema) -> pa.RecordBatch:
    """Check every emitted field, including nested fields, before Arrow coercion.

    Arrow ignores dictionary keys absent from an explicit schema. That behavior
    would silently discard new mapped facts. Missing schema fields remain NULL
    so source-specific maps can share a larger combined table without inventing
    values. Numeric/date interpretation stays with each reviewed mapping.
    """
    if not isinstance(rows, list):
        raise TypeError("FEC typed batches require an already bounded list of rows")
    names = frozenset(schema.names)
    if len(names) != len(schema):
        raise ValueError("FEC output schema contains duplicate fields")

    def check_type(dtype):
        if pa.types.is_struct(dtype):
            if len({field.name for field in dtype}) != len(dtype):
                raise ValueError("FEC output schema contains duplicate struct fields")
            for field in dtype:
                check_type(field.type)
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
            check_type(dtype.value_type)
        elif pa.types.is_map(dtype):
            check_type(dtype.key_type)
            check_type(dtype.item_type)
        elif pa.types.is_nested(dtype) or pa.types.is_dictionary(dtype) or isinstance(dtype, pa.BaseExtensionType):
            raise ValueError(f"Unsupported FEC output type for field-preservation checks: {dtype}")

    for field in schema:
        check_type(field.type)

    def check(value, dtype, location):
        if value is None:
            return
        if pa.types.is_struct(dtype):
            if not isinstance(value, Mapping):
                raise ValueError(f"FEC struct is not a named field mapping: {location}")
            unknown = value.keys() - {field.name for field in dtype}
            if unknown:
                raise ValueError(f"FEC output contains fields absent from its schema at {location}: {sorted(unknown)}")
            for field in dtype:
                check(value.get(field.name), field.type, f"{location}.{field.name}")
        elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype) or pa.types.is_fixed_size_list(dtype):
            for index, item in enumerate(value):
                check(item, dtype.value_type, f"{location}[{index}]")
        elif pa.types.is_map(dtype):
            pairs = value.items() if isinstance(value, Mapping) else value
            for index, (key, item) in enumerate(pairs):
                check(key, dtype.key_type, f"{location}[{index}].key")
                check(item, dtype.item_type, f"{location}[{index}].value")

    nested = [field for field in schema if pa.types.is_nested(field.type)]
    for index, row in enumerate(rows):
        unknown = row.keys() - names
        if unknown:
            raise ValueError(f"FEC output row {index} contains fields absent from its schema: {sorted(unknown)}")
        for field in nested:
            check(row.get(field.name), field.type, f"row[{index}].{field.name}")
    return pa.RecordBatch.from_pylist(rows, schema=schema)
