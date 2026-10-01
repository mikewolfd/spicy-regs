from decimal import Decimal

import pyarrow as pa
import pytest

from spicy_regs.transforms.fec_typed_batch import typed_batch


def test_new_field_is_never_silently_lost_from_published_values():
    rows = [{"value": None, "bound_value": Decimal("1"), "operator": "<"}]
    incomplete = pa.schema([("value", pa.decimal128(38, 9)), ("operator", pa.string())])
    with pytest.raises(ValueError, match="bound_value"):
        typed_batch(rows, incomplete)
    complete = incomplete.append(pa.field("bound_value", pa.decimal128(38, 9)))
    assert typed_batch(rows, complete).to_pylist() == rows


def test_nested_source_measure_cannot_disappear_from_list_of_structs():
    schema = pa.schema([("measures", pa.list_(pa.struct([("name", pa.string())])))])
    with pytest.raises(ValueError, match="amount"):
        typed_batch([{"measures": [{"name": "receipts", "amount": Decimal("1.12")}]}], schema)


def test_omitted_and_null_fields_remain_null_in_combined_schema():
    schema = pa.schema([("a", pa.string()), ("b", pa.string())])
    assert typed_batch([{"a": "", "b": None}, {"b": "source"}], schema).to_pylist() == [
        {"a": "", "b": None},
        {"a": None, "b": "source"},
    ]


def test_duplicate_schema_field_refuses_even_for_empty_input():
    with pytest.raises(ValueError, match="duplicate"):
        typed_batch([], pa.schema([("a", pa.string()), ("a", pa.string())]))


@pytest.mark.parametrize("as_pairs", [False, True])
def test_map_values_cannot_silently_lose_nested_fields(as_pairs):
    schema = pa.schema([("by_name", pa.map_(pa.string(), pa.struct([("amount", pa.int64())])))])
    value = {"donor": {"amount": 1, "new_fact": "must survive"}}
    rows = [{"by_name": list(value.items()) if as_pairs else value}]
    with pytest.raises(ValueError, match="new_fact"):
        typed_batch(rows, schema)
    value["donor"].pop("new_fact")
    assert typed_batch(rows, schema).to_pylist() == [{"by_name": [("donor", {"amount": 1})]}]


def test_unsupported_nested_shape_is_refused_even_when_empty():
    schema = pa.schema([("value", pa.dense_union([pa.field("a", pa.int64())]))])
    with pytest.raises(ValueError, match="Unsupported"):
        typed_batch([], schema)
