"""Combined FEC bytes preserve exact fields, membership and source partitions."""

from decimal import Decimal
import hashlib

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs.transforms import assemble_fec_query as assembly


def source(tmp_path, name, rows, schema, namespace=None):
    path = tmp_path / name
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    return assembly.TypedInput(path, digest, len(rows), namespace)


def build(inputs, schema, output, **kwargs):
    return assembly.assemble_table(inputs, schema=schema, output=output, check_resources=lambda: None,
                                   read_batch_rows=2, row_group_rows=4, part_rows=8, **kwargs)


def test_exact_nested_fields_decimals_and_null_states_survive_schema_union(tmp_path):
    first_schema = pa.schema([("id", pa.string()), ("amount", pa.decimal128(38, 9)),
                              ("facts", pa.list_(pa.struct([("raw", pa.string())])))])
    second_schema = pa.schema([("id", pa.string()), ("extra", pa.string()),
                               ("facts", pa.list_(pa.struct([("raw", pa.string()), ("state", pa.string())])))])
    first = source(tmp_path, "first.parquet", [
        dict(id="a", amount=Decimal("-0.000000001"), facts=[dict(raw=""), None]),
        dict(id="b", amount=Decimal("0"), facts=None),
    ], first_schema)
    second = source(tmp_path, "second.parquet", [dict(id="c", extra="", facts=[dict(raw=None, state="null")])], second_schema)
    schema = pa.unify_schemas([first_schema, second_schema])
    output = tmp_path / "combined.parquet"
    result = build([first, second], schema, output)
    assert result["rows"] == 3 and result["every_stored_cell_compared"]
    assert pq.read_table(output).to_pylist() == [
        dict(id="a", amount=Decimal("-0.000000001"), facts=[dict(raw="", state=None), None], extra=None),
        dict(id="b", amount=Decimal("0"), facts=None, extra=None),
        dict(id="c", amount=None, facts=[dict(raw=None, state="null")], extra=""),
    ]


def test_source_partitions_and_shard_boundaries_preserve_every_row(tmp_path):
    schema = pa.schema([("id", pa.int64()), ("source_namespace", pa.string())])
    a = source(tmp_path, "a.parquet", [dict(id=i, source_namespace="fec-a") for i in range(11)], schema, "fec-a")
    b = source(tmp_path, "b.parquet", [dict(id=99, source_namespace="fec-b")], schema, "fec-b")
    c = source(tmp_path, "c.parquet", [dict(id=i, source_namespace="fec-a") for i in range(11, 18)], schema, "fec-a")
    output = tmp_path / "fec_receipts"
    result = build([a, b, c], schema, output, partitioned=True)
    assert result["partition_columns"] == ["source_namespace"]
    paths = sorted(output.rglob("*.parquet"))
    assert [pq.ParquetFile(p).metadata.num_rows for p in paths] == [8, 8, 2, 1]
    actual = [r for p in paths for r in pq.ParquetFile(p).read().to_pylist()]
    assert [r["id"] for r in actual] == [*range(18), 99]
    for p in paths:
        assert all("source_namespace=" + r["source_namespace"] == p.parent.name
                   for r in pq.ParquetFile(p).read().to_pylist())


def test_empty_requires_a_verified_zero_row_source(tmp_path):
    schema = pa.schema([("id", pa.string())])
    empty = source(tmp_path, "empty.parquet", [], schema)
    result = build([empty], schema, tmp_path / "empty-output.parquet")
    assert result["rows"] == 0 and len(result["members"]) == 1
    with pytest.raises(ValueError, match="selected inputs"):
        build([], schema, tmp_path / "absent.parquet")


@pytest.mark.parametrize("case", ["field-loss", "nested-loss", "type-change", "digest", "rows", "repeat"])
def test_invalid_membership_or_schema_refuses_before_writing(tmp_path, case):
    schema = pa.schema([("id", pa.string()), ("native", pa.struct([("exact", pa.string())]))])
    item = source(tmp_path, "input.parquet", [dict(id="001", native=dict(exact=""))], schema)
    inputs, target = [item], schema
    if case == "field-loss":
        target = pa.schema([schema.field("id")])
    elif case == "nested-loss":
        target = pa.schema([schema.field("id"), ("native", pa.struct([]))])
    elif case == "type-change":
        target = pa.schema([("id", pa.int64()), schema.field("native")])
    elif case == "digest":
        inputs = [assembly.TypedInput(item.path, "sha256:" + "0" * 64, 1)]
    elif case == "rows":
        inputs = [assembly.TypedInput(item.path, item.sha256, 2)]
    else:
        inputs *= 2
    output = tmp_path / "bad.parquet"
    with pytest.raises((ValueError, pa.ArrowException)):
        build(inputs, target, output)
    assert not output.exists() and not output.with_name(output.name + ".partial").exists()


def test_wrong_partition_values_do_not_become_a_successful_output(tmp_path):
    schema = pa.schema([("source_namespace", pa.string())])
    item = source(tmp_path, "source.parquet", [dict(source_namespace="fec-actual")], schema, "fec-wrong")
    output = tmp_path / "table"
    with pytest.raises(ValueError, match="declared source namespace"):
        build([item], schema, output, partitioned=True)
    assert not output.exists()


def test_changed_source_bytes_are_rejected_during_assembly(tmp_path):
    schema = pa.schema([("source_namespace", pa.string()), ("amount", pa.int64())])
    item = source(tmp_path, "source.parquet", [dict(source_namespace="fec-a", amount=10)], schema, "fec-a")
    output = tmp_path / "table"
    modified = False

    def resources():
        nonlocal modified
        if output.with_name("table.partial").exists() and not modified:
            pq.write_table(pa.Table.from_pylist([dict(source_namespace="fec-a", amount=99)], schema=schema), item.path)
            modified = True

    with pytest.raises(ValueError, match="changed during assembly"):
        assembly.assemble_table([item], schema=schema, output=output, partitioned=True, check_resources=resources)
    assert modified and not output.exists()


def test_full_value_readback_refuses_same_size_population_with_changed_value(tmp_path, monkeypatch):
    schema = pa.schema([("amount", pa.int64())])
    item = source(tmp_path, "input.parquet", [dict(amount=10), dict(amount=20)], schema)
    output = tmp_path / "table.parquet"
    original = assembly._equal_streams

    def tampered(expected, actual):
        pq.write_table(pa.Table.from_pylist([dict(amount=10), dict(amount=21)], schema=schema),
                       output.with_name("table.parquet.partial"))
        return original(expected, actual)

    monkeypatch.setattr(assembly, "_equal_streams", tampered)
    with pytest.raises(ValueError, match="differs from selected input values"):
        build([item], schema, output)
    assert not output.exists() and output.with_name("table.parquet.partial").exists()


def test_resource_stop_preserves_existing_inputs_and_refuses_final_output(tmp_path):
    schema = pa.schema([("amount", pa.int64())])
    item = source(tmp_path, "input.parquet", [dict(amount=10)], schema)
    output = tmp_path / "table.parquet"

    def stop():
        raise RuntimeError("resource budget exceeded")

    with pytest.raises(RuntimeError, match="resource budget"):
        assembly.assemble_table([item], schema=schema, output=output, check_resources=stop)
    assert assembly._sha(item.path) == item.sha256 and not output.exists()


@pytest.mark.parametrize("partitioned", [False, True])
def test_late_destination_creation_is_never_replaced(tmp_path, monkeypatch, partitioned):
    schema = pa.schema([("amount", pa.int64()), ("source_namespace", pa.string())])
    item = source(tmp_path, "input.parquet", [dict(amount=10, source_namespace="fec-a")], schema, "fec-a")
    output = tmp_path / "table"
    original = assembly._equal_streams

    def intervening_destination(expected, actual):
        count = original(expected, actual)
        if partitioned:
            output.mkdir()
            (output / "sentinel").write_text("preserve")
        else:
            output.write_text("preserve")
        return count

    monkeypatch.setattr(assembly, "_equal_streams", intervening_destination)
    with pytest.raises(FileExistsError):
        build([item], schema, output, partitioned=partitioned)
    assert (output / "sentinel" if partitioned else output).read_text() == "preserve"


def test_late_staging_creation_is_never_truncated(tmp_path):
    schema = pa.schema([("amount", pa.int64())])
    item = source(tmp_path, "input.parquet", [dict(amount=10)], schema)
    output = tmp_path / "table.parquet"
    staging = output.with_name(output.name + ".partial")

    def competing_writer():
        if not staging.exists():
            staging.write_text("other writer")

    with pytest.raises(FileExistsError):
        assembly.assemble_table([item], schema=schema, output=output, check_resources=competing_writer)
    assert staging.read_text() == "other writer" and not output.exists()


@pytest.mark.parametrize("phase", ["after-readback", "promotion"])
def test_changed_compared_bytes_never_receive_a_success_receipt(tmp_path, monkeypatch, phase):
    schema = pa.schema([("amount", pa.int64())])
    item = source(tmp_path, "input.parquet", [dict(amount=10)], schema)
    output = tmp_path / "table.parquet"
    staging = output.with_name(output.name + ".partial")

    def change():
        pq.write_table(pa.Table.from_pylist([dict(amount=99)], schema=schema), staging)

    if phase == "after-readback":
        original = assembly._equal_streams

        def tampered(expected, actual):
            count = original(expected, actual)
            change()
            return count

        monkeypatch.setattr(assembly, "_equal_streams", tampered)
    else:
        original = assembly.os.link

        def tampered(source, destination):
            original(source, destination)
            change()

        monkeypatch.setattr(assembly.os, "link", tampered)
    with pytest.raises(ValueError, match="(changed during readback|differs from its compared bytes)"):
        build([item], schema, output)
    assert staging.exists()  # Retain failed bytes; no admission receipt was returned.
