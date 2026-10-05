"""Exercise the live dictionary assertion locally, without publication or network reads."""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms.build_proceedings import build_proceedings
from spicy_regs.transforms.regulations_shape import shape_record, subject_schema
from tests import test_chaos_r4_dictionary_live as live
from tests.test_proceedings import _empty_rulemaking_inputs, _fr_rows


def _audit(path, *, table="proceedings", column="authority_refs", value="`[]`"):
    with duckdb.connect() as con:
        live.test_a_column_described_as_always_empty_is_empty({table: [str(path)]}, con, table, column, value)


def test_actual_proceedings_builder_and_native_mapping_satisfy_the_same_claim(tmp_path):
    _empty_rulemaking_inputs(tmp_path)
    _fr_rows(
        tmp_path,
        [
            {
                "document_number": "2024-12345",
                "publication_date": "2024-02-01",
                "document_type": "Rule",
                "title": "A final rule",
                "regulation_id_numbers_json": "[]",
            }
        ],
    )
    legacy = build_proceedings(tmp_path)
    rows = pq.read_table(legacy).to_pylist()
    assert len(rows) == 1
    assert rows[0]["authority_refs_json"] == "[]"
    _audit(legacy)

    schema = subject_schema("proceedings")
    shaped = [shape_record("proceedings", row) for row in rows]
    native_rows = [{field.name: row[field.name] for field in schema} for row in shaped]
    assert native_rows[0]["authority_refs"] == []
    native = tmp_path / "native.parquet"
    pq.write_table(pa.Table.from_pylist(native_rows, schema=schema), native)
    _audit(native)


@pytest.mark.parametrize(
    "name,dtype,empty,nonempty",
    [
        ("authority_refs_json", pa.string(), "[]", '["5 U.S.C. 301"]'),
        ("authority_refs", pa.list_(pa.string()), [], ["5 U.S.C. 301"]),
    ],
)
@pytest.mark.parametrize("kind", ["empty", "nonempty", "null", "empty_and_nonempty", "empty_and_null"])
def test_empty_claim_still_rejects_nonempty_and_null_rows(tmp_path, name, dtype, empty, nonempty, kind):
    values = {
        "empty": [empty],
        "nonempty": [nonempty],
        "null": [None],
        "empty_and_nonempty": [empty, nonempty],
        "empty_and_null": [empty, None],
    }[kind]
    path = tmp_path / "proceedings.parquet"
    pq.write_table(pa.table({name: pa.array(values, type=dtype)}), path)
    if kind == "empty":
        _audit(path)
    else:
        with pytest.raises(AssertionError, match="1 rows contradict"):
            _audit(path)


@pytest.mark.parametrize("value", ["null", "[null]", "[ ]", "malformed"])
def test_legacy_literal_assertion_does_not_coerce_other_json_or_strings(tmp_path, value):
    path = tmp_path / "proceedings.parquet"
    pq.write_table(pa.table({"authority_refs_json": [value]}), path)
    with pytest.raises(AssertionError, match="1 rows contradict"):
        _audit(path)


@pytest.mark.parametrize(
    "columns",
    [
        {"unrelated": pa.array(["[]"])},
        {"authority_refs": pa.array(["[]"])},
        {"authority_refs_json": pa.array([[]], type=pa.list_(pa.string()))},
        {"authority_refs": pa.array([[]], type=pa.list_(pa.int64()))},
        {"authority_refs": pa.array([[]], type=pa.list_(pa.string())), "authority_refs_json": pa.array(["[]"])},
    ],
)
def test_missing_ambiguous_or_undeclared_representations_refuse(tmp_path, columns):
    path = tmp_path / "proceedings.parquet"
    pq.write_table(pa.table(columns), path)
    with pytest.raises(ValueError, match="proceedings.authority_refs"):
        _audit(path)


def test_legacy_alias_is_specific_to_the_declared_table_and_field(tmp_path):
    path = tmp_path / "other.parquet"
    pq.write_table(pa.table({"authority_refs_json": ["[]"]}), path)
    with pytest.raises(ValueError, match="other.authority_refs"):
        _audit(path, table="other")


@pytest.mark.parametrize("value", [None, "2026-10-05"])
def test_existing_always_null_claim_keeps_its_assertion(tmp_path, value):
    path = tmp_path / "federal_register.parquet"
    pq.write_table(pa.table({"modify_date": pa.array([value], type=pa.string())}), path)
    if value is None:
        _audit(path, table="federal_register", column="modify_date", value="NULL")
    else:
        with pytest.raises(AssertionError, match="1 rows contradict"):
            _audit(path, table="federal_register", column="modify_date", value="NULL")
