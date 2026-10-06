import polars as pl
import pyarrow as pa
import pytest
from spicy_regs.native_types import arrow_type, described_schema, polars_sql_type


@pytest.mark.parametrize("name,expected", [
    ("INTEGER[]", pa.list_(pa.int32())),
    ("DECIMAL(20,2)", pa.decimal128(20, 2)),
    ('STRUCT("name" VARCHAR, "cycles" INTEGER[])[]',
     pa.list_(pa.struct([("name", pa.string()), ("cycles", pa.list_(pa.int32()))]))),
])
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
        write_staging("agency", "items", [{"id": "x", "provenance": "would disappear"}],
                      tmp_path, {"id": pl.String})
    assert not list(tmp_path.rglob("*.parquet"))


def test_prepared_flat_checker_preserves_custom_mapping_get_refusal():
    from spicy_regs.native_types import extra_field_checker, reject_extra_fields

    class RefusingMapping(dict):
        def get(self, key, default=None):
            raise ValueError("declared field cannot be read")

    schema = pa.schema([("id", pa.string())])
    row = RefusingMapping(id="x")
    for check in (lambda value: reject_extra_fields(value, schema), extra_field_checker(schema)):
        with pytest.raises(ValueError, match="declared field cannot be read"):
            check(row)


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
