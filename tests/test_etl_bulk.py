"""The SQL encodings equal the receipt writer's, or mark the text for the row writer."""

import json
import random
import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import etl_bulk
from spicy_regs.etl_receipts import DatasetPolicy, ReceiptContext, _unpack, exact_json, subject_identity, write_dataset

#: The control characters with no short JSON escape whose ``\\u00XX`` holds a hex letter.
UPPER_HEX = {0x0B, 0x0E, 0x0F, *range(0x1A, 0x20)}
NESTED = pa.list_(pa.struct([("title", pa.string()), ("formats", pa.list_(pa.struct([("url", pa.string()), ("size", pa.int64())]))),
                             ("kept", pa.bool_())]))


def _encode(values: list, dtype: pa.DataType) -> list[tuple[str, bool]]:
    """Each value's SQL encoding and whether SQL marks it for the reference."""
    con = duckdb.connect()
    con.register("t", pa.table({"i": list(range(len(values))), "v": pa.array(values, type=dtype)}))
    text = etl_bulk.exact_json_sql('"v"', dtype)
    return con.execute(f"SELECT {text}, {etl_bulk.needs_reference_sql(text)} FROM t ORDER BY i").fetchall()


def test_every_code_point_encodes_like_the_reference_or_is_marked() -> None:
    points = [p for p in range(sys.maxunicode + 1) if not 0xD800 <= p <= 0xDFFF]
    values = ["a" + chr(p) + "b" for p in points]
    marked = set()
    for point, value, (text, flagged) in zip(points, values, _encode(values, pa.string())):
        if flagged:
            marked.add(point)
            assert _unpack(json.loads(text)) == value  # spelled differently, the same value: the row code can read it back
        else:
            assert text == exact_json(value), hex(point)
    assert marked == UPPER_HEX


@pytest.mark.parametrize("value", [
    "\\u001B", "\\\\u001B", "back\\slash", 'quote"', "\\", "\\\\", "x\\\\\\u000E", "\x00\x01\x12\x19", "\t\n\r\b\f", "\x7f  ﻿",
])
def test_text_that_only_looks_like_an_escape_is_not_marked(value: str) -> None:
    [(text, flagged)] = _encode([value], pa.string())
    assert not flagged and text == exact_json(value)


@pytest.mark.parametrize("value", ["\x1b", "\x1b\x1b", "\\\x0b", "\\\\\x1f", "a\x0eb\x0fc", "\x1a" * 5])
def test_an_upper_hex_escape_is_marked_wherever_it_stands(value: str) -> None:
    [(text, flagged)] = _encode([value], pa.string())
    assert flagged and text != exact_json(value) and text.lower() == exact_json(value).lower()


def test_nested_values_integers_and_nulls() -> None:
    rng = random.Random(20261005)
    alphabet = ['a', 'é', '"', '\\', '\n', '\x00', '😀', ' ', "'", '<', '\x1b']

    def text():
        return None if rng.random() < 0.2 else "".join(rng.choice(alphabet) for _ in range(rng.randrange(6)))

    def formats():
        return None if rng.random() < 0.2 else [None if rng.random() < 0.1 else {"url": text(), "size": rng.choice([None, 0, -7, 2**62])}
                                                for _ in range(rng.randrange(3))]

    values = [None if rng.random() < 0.1 else [None if rng.random() < 0.1 else {"title": text(), "formats": formats(), "kept": rng.choice([None, True, False])}
                                               for _ in range(rng.randrange(3))] for _ in range(2000)]
    stored = pa.array(values, type=NESTED).to_pylist()  # as Arrow holds them, every struct key present
    seen = set()
    for value, (text_sql, flagged) in zip(stored, _encode(values, NESTED)):
        seen.add(flagged)
        assert flagged == ("\\u001b" in exact_json(value))
        if not flagged:
            assert text_sql == exact_json(value)
    assert seen == {True, False}
    for dtype, column in ((pa.int32(), [None, 0, -1, 2**31 - 1]), (pa.bool_(), [None, True, False])):
        assert [text for text, _ in _encode(column, dtype)] == [exact_json(v) for v in column]


def test_an_unproven_type_refuses() -> None:
    with pytest.raises(NotImplementedError, match="double"):
        etl_bulk.exact_json_sql('"v"', pa.float64())


def test_identities_and_receipt_ids_of_a_written_bundle(tmp_path) -> None:
    schema = pa.schema([("id", pa.string()), ("n", pa.int32()), ("tags", pa.list_(pa.string())), ("note", pa.string())])
    policy = DatasetPolicy("things", schema, ("id",), ("raw",), policy_version="things/1")
    rows = [{"id": f"k{i}", "n": None if i % 3 == 0 else i - 5, "tags": None if i % 4 == 0 else ["a", 'q"'][: i % 3],
             "note": [None, "plain", "tab\there", "é😀"][i % 4], "raw": {"i": i}} for i in range(50)]
    witness = {"source_id": "s", "source_uri": None, "sha256": "ab" * 32, "locator": "row", "body_version": None}
    records = ((row, ReceiptContext("g1", f"a{i}", "test/1", [witness], {"ordinal": i})) for i, row in enumerate(rows))
    subject, receipts = write_dataset(records, tmp_path / "bundle", policy)
    con = duckdb.connect()
    record_id, version, identity = etl_bulk.identity_sql(policy)
    got = con.execute(f"SELECT {record_id}, {version}, {identity} FROM read_parquet('{subject}') ORDER BY id").fetchall()
    want = [subject_identity(policy, row) for row in sorted(pq.read_table(subject).to_pylist(), key=lambda row: row["id"])]
    assert got == want
    recomputed = con.execute(f"SELECT count(*), count(*) FILTER ({etl_bulk.digest_sql(etl_bulk.receipt_json_sql())} = receipt_id) "
                             f"FROM read_parquet('{receipts}')").fetchone()
    assert recomputed == (len(rows), len(rows))
