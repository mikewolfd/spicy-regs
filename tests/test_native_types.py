from collections import OrderedDict
from collections.abc import Mapping

import polars as pl
import pyarrow as pa
import pytest
from spicy_regs.native_types import arrow_type, described_schema, polars_sql_type


@pytest.mark.parametrize(
    "name,expected",
    [
        ("INTEGER[]", pa.list_(pa.int32())),
        ("DECIMAL(20,2)", pa.decimal128(20, 2)),
        (
            'STRUCT("name" VARCHAR, "cycles" INTEGER[])[]',
            pa.list_(pa.struct([("name", pa.string()), ("cycles", pa.list_(pa.int32()))])),
        ),
    ],
)
def test_recursive_types(name, expected):
    assert arrow_type(name) == expected
    assert arrow_type(described_schema(pa.schema([("value", expected)]))[0][1]) == expected


def test_polars_nested_and_decimal():
    assert polars_sql_type(pl.List(pl.Int32)) == "INTEGER[]"
    assert polars_sql_type(pl.Decimal(20, 2)) == "DECIMAL(20,2)"


@pytest.mark.parametrize("name", ["VARCHAR); SELECT 1; --", "VARCHAR /* comment */", "VARCHAR'", ""])
def test_not_sql_input(name):
    with pytest.raises(Exception):
        arrow_type(name)


def test_arrow_writer_rejects_extra_nested_fields_without_replacing(tmp_path):
    from spicy_regs.transforms.parquet_rows import write_rows

    path = tmp_path / "rows.parquet"
    schema = pa.schema([("items", pa.list_(pa.struct([("name", pa.string())])))])
    write_rows([{"items": [{"name": "original"}]}], path, schema)
    prior = path.read_bytes()
    with pytest.raises(ValueError, match="unclassified"):
        write_rows([{"items": [{"name": "new", "provenance": "would disappear"}]}], path, schema)
    assert path.read_bytes() == prior


def test_polars_staging_rejects_extra_fields(tmp_path):
    from spicy_regs.transforms.write_staging import write_staging

    with pytest.raises(ValueError, match="unclassified"):
        write_staging("agency", "items", [{"id": "x", "provenance": "would disappear"}], tmp_path, {"id": pl.String})
    assert not list(tmp_path.rglob("*.parquet"))


def test_prepared_checker_rechecks_nested_and_top_level_fields():
    from spicy_regs.native_types import extra_field_checker

    schema = pa.schema([("items", pa.large_list(pa.struct([("name", pa.string())])))])
    check = extra_field_checker(schema, label="record")
    check({"items": [{"name": "original"}, None]})
    with pytest.raises(ValueError, match=r"record.items: unclassified fields \['provenance'\]"):
        check({"items": [{"name": "new", "provenance": "retained"}]})
    with pytest.raises(ValueError, match=r"record: unclassified fields \['source'\]"):
        check({"items": None, "source": "retained"})


def test_prepared_flat_writer_refuses_extra_field_after_a_complete_batch(tmp_path):
    from spicy_regs.parquet_rows import write_rows

    schema = pa.schema([("id", pa.string())])
    target = tmp_path / "rows.parquet"
    write_rows([{"id": "original"}], target, schema)
    prior = target.read_bytes()
    rows = [{"id": str(number)} for number in range(2000)]
    rows.append({"id": "later", "source": "would disappear"})
    with pytest.raises(ValueError, match=r"row: unclassified fields \['source'\]"):
        write_rows(rows, target, schema)
    assert target.read_bytes() == prior


def _unprepared_refusal(value, dtype, path):
    """The recursive check as it read before the schema was prepared: the oracle the prepared checker must equal."""
    if value is None:
        return
    if pa.types.is_struct(dtype) and isinstance(value, Mapping):
        if extra := set(value) - {f.name for f in dtype}:
            raise ValueError(f"{path}: unclassified fields {sorted(extra)}")
        for f in dtype:
            _unprepared_refusal(value.get(f.name), f.type, f"{path}.{f.name}")
    elif (pa.types.is_list(dtype) or pa.types.is_large_list(dtype)) and isinstance(value, (list, tuple)):
        for item in value:
            _unprepared_refusal(item, dtype.value_type, path)


class _RefusingGet(dict):
    def get(self, key, default=None):
        raise ValueError("declared field cannot be read")


class _CountingGet(OrderedDict):
    reads: list

    def get(self, key, default=None):
        self.reads.append(key)
        return super().get(key, default)


_INNER = pa.struct([("name", pa.string()), ("tags", pa.list_(pa.string()))])
_SCHEMAS = {
    "flat": pa.schema([("id", pa.string()), ("count", pa.int64())]),
    "struct": pa.schema([("id", pa.string()), ("inner", _INNER)]),
    "list_struct": pa.schema([("items", pa.list_(_INNER))]),
    "large_list_struct": pa.schema([("items", pa.large_list(_INNER))]),
    "list_list_struct": pa.schema([("items", pa.list_(pa.list_(_INNER)))]),
    "struct_in_struct": pa.schema([("outer", pa.struct([("inner", _INNER), ("id", pa.string())]))]),
    "list_scalar": pa.schema([("tags", pa.list_(pa.string())), ("id", pa.string())]),
}
_ROWS = [
    None,
    {},
    [],
    "text",
    {"id": "a"},
    {"id": "a", "count": 1},
    {"id": "a", "extra": 1},
    {"zzz": 1, "aaa": 2, "id": 1},
    {"id": "a", "inner": None},
    {"id": "a", "inner": {"name": "n", "tags": ["x"]}},
    {"id": "a", "inner": {"name": "n", "extra": 1}},
    {"inner": {"tags": ["x", None], "other": 1}},
    {"inner": "not a mapping"},
    {"inner": ["not", "a", "mapping"]},
    {"items": None},
    {"items": []},
    {"items": [None, {"name": "n"}]},
    {"items": ({"name": "n"},)},
    {"items": [{"name": "n"}, {"name": "m", "extra": 1}]},
    {"items": [{"extra": 1}, {"name": "n", "also": 2}]},
    {"items": [[{"name": "n"}], [None, {"extra": 1}]]},
    {"items": "not a list"},
    {"items": {"name": "n"}},
    {"outer": {"inner": {"name": "n", "extra": 1}, "id": "a"}},
    {"outer": {"inner": None, "extra": 1}},
    {"outer": {"id": "a", "inner": {"tags": ["x"]}}},
    {"tags": ["x", "y"], "id": "a"},
    {"tags": "x", "extra": 1},
    OrderedDict(id="a", count=1),
    OrderedDict(id="a", extra=1),
    _RefusingGet(id="a"),
    _RefusingGet(),
    OrderedDict(inner=OrderedDict(name="n", extra=1)),
    {"items": [OrderedDict(name="n", extra=1)]},
]


def _outcome(check, *args):
    try:
        check(*args)
    except ValueError as error:
        return str(error)
    return None


@pytest.mark.parametrize("name", _SCHEMAS)
@pytest.mark.parametrize("label", ["row", "record"])
def test_prepared_checker_equals_the_unprepared_check_for_every_shape(name, label):
    from spicy_regs.native_types import extra_field_checker

    schema = _SCHEMAS[name]
    prepared = extra_field_checker(schema, label=label)
    refusals = 0
    for row in _ROWS:
        expected = _outcome(_unprepared_refusal, row, pa.struct(schema), label)
        assert _outcome(prepared, row) == expected, row
        refusals += expected is not None
    assert refusals  # the battery reaches refusals for this schema, not only acceptances


def test_prepared_checker_names_each_unknown_field_exactly():
    from spicy_regs.native_types import extra_field_checker

    check = extra_field_checker(_SCHEMAS["struct_in_struct"])
    assert _outcome(check, {"outer": {"id": "a"}, "z": 1, "a": 2}) == "row: unclassified fields ['a', 'z']"
    assert _outcome(check, {"outer": {"inner": {"b": 1, "a": 2}}}) == "row.outer.inner: unclassified fields ['a', 'b']"
    assert (
        _outcome(extra_field_checker(_SCHEMAS["list_struct"], label="record"), {"items": [{"name": "n"}, {"x": 1}]})
        == "record.items: unclassified fields ['x']"
    )


def test_prepared_checker_still_reads_every_declared_field_of_a_non_dict_mapping():
    from spicy_regs.native_types import extra_field_checker

    schema = _SCHEMAS["flat"]
    check = extra_field_checker(schema)
    with pytest.raises(ValueError, match="declared field cannot be read"):
        check(_RefusingGet(id="x"))
    row = _CountingGet(id="a", count=1)
    row.reads = []
    check(row)
    assert row.reads == ["id", "count"]


def test_prepared_checker_does_no_schema_work_per_row(monkeypatch):
    """Everything about the schema is built in ``extra_field_checker``; rows only meet sets and prepared closures."""
    import spicy_regs.native_types as native_types

    class NoPyarrow:
        def __getattr__(self, name):
            raise AssertionError(f"pyarrow.{name} used while checking a row")

    check = native_types.extra_field_checker(_SCHEMAS["list_list_struct"])
    monkeypatch.setattr(native_types, "pa", NoPyarrow())
    for number in range(50):
        check({"items": [[{"name": str(number)}, None], [{"tags": ["x"]}]]})
    with pytest.raises(ValueError, match="unclassified"):
        check({"items": [[{"name": "n", "provenance": "retained"}]]})


def test_write_rows_prepares_the_checker_once_per_write(tmp_path, monkeypatch):
    import spicy_regs.native_types as native_types
    from spicy_regs.parquet_rows import write_rows

    builds = []
    real = native_types.extra_field_checker
    monkeypatch.setattr(native_types, "extra_field_checker", lambda *a, **k: builds.append(a) or real(*a, **k))
    schema = pa.schema([("id", pa.string())])
    write_rows(({"id": str(number)} for number in range(25)), tmp_path / "rows.parquet", schema, batch_size=4)
    assert len(builds) == 1


@pytest.mark.parametrize(
    "schema,row,message",
    [
        (
            pa.schema([("id", pa.string())]),
            {"id": "x", "source": "s", "also": "a"},
            "row: unclassified fields ['also', 'source']",
        ),
        (
            pa.schema([("inner", _INNER)]),
            {"inner": {"name": "n", "provenance": "p"}},
            "row.inner: unclassified fields ['provenance']",
        ),
        (
            pa.schema([("items", pa.list_(_INNER))]),
            {"items": [{"name": "n"}, {"x": 1}]},
            "row.items: unclassified fields ['x']",
        ),
    ],
)
def test_write_rows_refuses_unknown_fields_with_the_same_message_and_keeps_the_destination(
    tmp_path, schema, row, message
):
    from spicy_regs.parquet_rows import write_rows

    target = tmp_path / "rows.parquet"
    write_rows([], target, schema)
    prior = target.read_bytes()
    clean = {field.name: None for field in schema}
    with pytest.raises(ValueError) as refused:
        write_rows([clean] * 5000 + [row] + [clean] * 5000, target, schema)
    assert str(refused.value) == message
    assert target.read_bytes() == prior
    assert [path.name for path in tmp_path.iterdir()] == ["rows.parquet"]


def test_bulk_restore_refuses_decoded_fields_outside_the_retained_schema(tmp_path):
    import pyarrow.parquet as pq
    from spicy_regs import congress_bulk

    schema = pa.schema([("k", pa.string())])
    receipts = tmp_path / "receipts.parquet"
    pq.write_table(
        pa.table({"dataset": ["d"] * 3, "processing_json": ["{}"] * 3, "diagnostic_json": ["{}"] * 3}), receipts
    )
    destination = tmp_path / "restored.parquet"
    assert (
        congress_bulk.restore_input(
            receipts, destination, dataset="d", schema=schema, row_fields=lambda processing, diagnostic: {"k": "v"}
        )
        == 3
    )
    prior = destination.read_bytes()
    with pytest.raises(ValueError, match=r"row: unclassified fields \['note'\]"):
        congress_bulk.restore_input(
            receipts,
            destination,
            dataset="d",
            schema=schema,
            row_fields=lambda processing, diagnostic: {"k": "v", "note": "would disappear"},
        )
    assert destination.read_bytes() == prior
