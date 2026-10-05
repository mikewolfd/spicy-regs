"""A flat Congress dataset's bulk bundle and restored table equal the row writer's, row for row."""

import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from spicy_regs import congress_bulk
from spicy_regs.congress_receipts import PROCESSOR, CongressInput, policy, restore_processing_input, write_congress_dataset
from spicy_regs.congress_subjects import INPUT_COLUMNS, map_record
from spicy_regs.etl_receipts import RECEIPT_SCHEMA, ReceiptContext, exact_json, failure_receipt

GENERATION = "generation-1"
WITNESS = {"source_id": "capture", "source_uri": "https://example.test/votes", "sha256": "ab" * 32, "locator": "page 1"}


def _member_votes(count: int) -> list[dict]:
    rows = [
        {name: None for name in INPUT_COLUMNS["member_votes"]}
        | {"vote_id": f"101-house-1-{i // 400}", "member_key": f"name:Member {i}", "congress": "101", "chamber": "house",
           "member_name": ["Smith (OR)", "O'Brien", 'Quote "Q"', "Peña"][i % 4], "position": "Yea" if i % 3 else None}
        for i in range(count)
    ]
    for index in range(7, count, 900):
        rows[index]["member_name"] = "Escape \x1b here"  # SQL spells this escape differently: the row code's
    for index in range(11, count, 1300):
        rows[index]["vote_id"] = None                    # no identity: the row code refuses it
    rows[-1] = {name: None for name in INPUT_COLUMNS["member_votes"]} | {"vote_id": "v", "member_key": "k"}
    return rows


def _member_vote_terms(count: int) -> list[dict]:
    rows = []
    for i in range(count):
        matched = i % 3 != 0
        rows.append({"vote_id": f"101-house-1-{i // 400}", "member_key": f"name:Member {i}", "chamber": "house",
                     "bioguide_id": f"B{i:06d}" if matched else None, "vote_day": "1990-01-23",
                     "term_match": "half_open" if matched else "unresolved_member", "term_index": str(i % 7) if matched else None,
                     "term_start": "1989-01-03" if matched else None, "term_end": "1991-01-03" if matched else None})
    rows[4]["term_index"] = "007"       # digits with leading zeros read as 7
    for index in range(5, count, 600):
        rows[index]["term_index"] = "x1"    # not an integer: the row code refuses it
    rows[8]["term_index"] = "9" * 19    # past int64: the row code refuses it
    for index in range(10, count, 750):
        rows[index]["chamber"] = "house\x0b"  # the row code's spelling
    rows[13]["term_match"] = None       # no term and no reason: a subject with a NULL term
    rows[13]["term_index"] = None
    return rows


def _source(tmp_path, dataset, rows, metadata=None):
    schema = pa.schema([(name, pa.string()) for name in INPUT_COLUMNS[dataset]], metadata=metadata)
    path = tmp_path / f"{dataset}.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    return path


def _taken(monkeypatch, name: str) -> list:
    """Record each call of a bulk function, so an equal result cannot come from the row code running twice."""
    calls, real = [], getattr(congress_bulk, name)

    def spy(*args, **kwargs):
        calls.append(name)
        return real(*args, **kwargs)

    monkeypatch.setattr(congress_bulk, name, spy)
    return calls


def _both(tmp_path, monkeypatch, dataset, rows, **kwargs):
    """The row writer's bundle and the bulk writer's, from one source file."""
    source = _source(tmp_path, dataset, rows, kwargs.pop("metadata", None))
    taken = _taken(monkeypatch, "write_bundle")
    bulk = write_congress_dataset(source, tmp_path / "bulk", dataset=dataset, generation_id=GENERATION, **kwargs)
    assert taken == ["write_bundle"]
    row = write_congress_dataset(source, tmp_path / "row", dataset=dataset, generation_id=GENERATION, bulk=False, **kwargs)
    assert taken == ["write_bundle"]
    return source, row, bulk


def _restored(tmp_path, monkeypatch, dataset, subject, receipts):
    """The row restore's table and the bulk restore's, from one bundle; or the error each raised."""
    def run(name, **kwargs):
        try:
            return pq.read_table(restore_processing_input(
                subject, receipts, tmp_path / name, dataset=dataset, generation_id=GENERATION, **kwargs))
        except Exception as error:  # the two paths are held to the same refusal, whatever it is
            return type(error)

    taken = _taken(monkeypatch, "restore_input")
    bulk = run("bulk.parquet")
    assert taken == ["restore_input"]
    row = run("row.parquet", bulk=False)
    assert taken == ["restore_input"]
    return row, bulk


@pytest.fixture(params=[100_000, 700], ids=["one batch", "several batches"])
def batch(request, monkeypatch) -> int:
    """A scan that spans several batches, each with rows for the row code, as every real table does."""
    monkeypatch.setattr(congress_bulk, "_BATCH", request.param)
    return request.param


@pytest.mark.parametrize(("dataset", "build"), [("member_votes", _member_votes), ("member_vote_terms", _member_vote_terms)])
def test_bulk_bundle_equals_the_row_writers(tmp_path, monkeypatch, batch, dataset, build) -> None:
    rows = build(4500)
    source, (row_subject, row_receipts), (bulk_subject, bulk_receipts) = _both(
        tmp_path, monkeypatch, dataset, rows, witnesses=[WITNESS], metadata={b"scope": b"101"})
    assert congress_bulk.eligible(dataset, pq.read_schema(source))
    expected = pq.read_table(row_receipts).to_pylist()
    assert len(expected) == len(rows) + 1
    assert pq.read_table(bulk_receipts).to_pylist() == expected
    routed = congress_bulk.routed_ordinals(source, dataset=dataset, policy=policy(dataset))
    assert len(routed) > 4500 // 700 and {expected[ordinal + 1]["outcome"] for ordinal in routed} == {"accepted", "refused"}
    assert {receipt["outcome"] for receipt in expected} >= {"accepted", "refused", "observed"}
    assert pq.read_table(bulk_subject).to_pylist() == pq.read_table(row_subject).to_pylist()
    written = pq.ParquetFile(bulk_receipts)
    assert written.schema_arrow.equals(RECEIPT_SCHEMA)
    assert [written.metadata.row_group(i).num_rows for i in range(written.metadata.num_row_groups)] == [2000, 2000, 501]


def test_rows_without_a_subject_are_the_mappers(tmp_path) -> None:
    """NO_SUBJECT restates map_record's rule; hold it to the mapper's answer on every row shape it distinguishes."""
    rows = _member_vote_terms(60)
    source = _source(tmp_path, "member_vote_terms", rows)
    stated = [flag for (flag,) in duckdb.sql(
        f"SELECT ({congress_bulk.NO_SUBJECT['member_vote_terms']}) FROM read_parquet('{source}', file_row_number = true) ORDER BY file_row_number"
    ).fetchall()]
    mapped = []
    for row in rows:
        try:
            mapped.append(map_record("member_vote_terms", row).subject is None)
        except (ValueError, OverflowError):
            mapped.append(False)  # refused, not subjectless: the row code's
    assert stated == mapped and True in stated and False in stated


@pytest.mark.parametrize(("dataset", "build"), [("member_votes", _member_votes), ("member_vote_terms", _member_vote_terms)])
def test_bulk_restore_equals_the_row_restore(tmp_path, monkeypatch, batch, dataset, build) -> None:
    rows = build(4500)
    _, (row_subject, row_receipts), _ = _both(tmp_path, monkeypatch, dataset, rows, metadata={b"scope": b"101"})
    expected, restored = _restored(tmp_path, monkeypatch, dataset, row_subject, row_receipts)
    assert restored.to_pylist() == expected.to_pylist()
    assert restored.schema.equals(expected.schema, check_metadata=True)
    refused = sum(receipt["outcome"] == "refused" for receipt in pq.read_table(row_receipts).to_pylist())
    assert refused > 2 and restored.num_rows == len(rows) - refused  # every row but the refused conversions


def test_a_refresh_writes_fresh_receipts_with_no_lineage(tmp_path, monkeypatch) -> None:
    """The interim rule: a bulk dataset's refresh names no predecessor; ``bulk=False`` still inherits."""
    source, _, (subject, receipts) = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    prior = CongressInput(subject, receipts, GENERATION)
    fresh = write_congress_dataset(source, tmp_path / "next", dataset="member_votes", generation_id="generation-2", prior=prior)
    row = write_congress_dataset(source, tmp_path / "next-row", dataset="member_votes", generation_id="generation-2", prior=prior, bulk=False)
    first = write_congress_dataset(source, tmp_path / "first", dataset="member_votes", generation_id="generation-2")
    assert pq.read_table(fresh[1]).to_pylist() == pq.read_table(first[1]).to_pylist()
    assert not any("prior_receipts" in receipt["diagnostic_json"] for receipt in pq.read_table(fresh[1]).to_pylist())
    assert any("prior_receipts" in receipt["diagnostic_json"] for receipt in pq.read_table(row[1]).to_pylist())


def test_a_scan_cut_short_is_refused(tmp_path, monkeypatch) -> None:
    """A stream that ends early leaves a bundle consistent with itself; only the file's own count can tell."""
    source, (subject, receipts), _ = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    real = congress_bulk._rows_in
    monkeypatch.setattr(congress_bulk, "_rows_in", lambda *args: real(*args) + 1)
    with pytest.raises(RuntimeError, match="read 40 of the source's 41 rows"):
        write_congress_dataset(source, tmp_path / "short", dataset="member_votes", generation_id=GENERATION)
    assert not (tmp_path / "short").exists()
    with pytest.raises(RuntimeError, match="read 41 of the file's 42 receipts"):
        restore_processing_input(subject, receipts, tmp_path / "short.parquet", dataset="member_votes", generation_id=GENERATION)
    assert not (tmp_path / "short.parquet").exists()


def test_the_files_own_count_is_of_the_datasets_receipts(tmp_path, monkeypatch) -> None:
    source, (subject, receipts), _ = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    assert congress_bulk._rows_in(source) == 40
    assert congress_bulk._rows_in(receipts, "member_votes") == 41 and congress_bulk._rows_in(receipts, "member_vote_terms") == 0


def _with_attempts(receipts, attempts) -> None:
    """Append failed attempts, built by the row code, to a written bundle's receipts."""
    selected = policy("member_votes")
    witness = {"source_id": "another-writer", "source_uri": None, "sha256": "cd" * 32, "locator": None, "body_version": None}
    added = [
        failure_receipt(selected, ReceiptContext(GENERATION, f"other:{i}", PROCESSOR, [witness], diagnostics), outcome="rejected", raw_fields=fields)
        for i, (fields, diagnostics) in enumerate(attempts)
    ]
    pq.write_table(pa.Table.from_pylist(pq.read_table(receipts).to_pylist() + added, schema=RECEIPT_SCHEMA), receipts)


def test_restore_of_receipts_another_writer_could_have_written(tmp_path, monkeypatch) -> None:
    """A valid bundle is wider than this writer's: a failed attempt may hold other keys, fewer columns, other reasons."""
    full = {name: None for name in INPUT_COLUMNS["member_votes"]} | {"vote_id": "v-other", "member_key": "k-other", "position": "Nay"}
    _, (subject, receipts), _ = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    _with_attempts(receipts, [
        ({"chamber": "house", "entry_kind": "row", "source_fields": full | {"member_name": "a key before entry_kind"}}, {}),
        ({"entry_kind": "row", "source_fields": {"vote_id": "v-few", "member_key": "k-few"}}, {}),
        ({"entry_kind": "row", "source_fields": full | {"member_name": "mentions it"}}, {"reason": "other", "note": "conversion_refused"}),
        ({"entry_kind": "row", "source_fields": full | {"member_name": "refused"}}, {"reason": "conversion_refused"}),
        ({"entry_kind": "note", "source_fields": full | {"member_name": "not a row"}}, {}),
        ({"source_fields": full | {"member_name": "no kind"}}, {}),
    ])
    expected, restored = _restored(tmp_path, monkeypatch, "member_votes", subject, receipts)
    assert restored.to_pylist() == expected.to_pylist()
    names = restored["member_name"].to_pylist()
    assert {"a key before entry_kind", "mentions it"} <= set(names) and not {"refused", "not a row", "no kind"} & set(names)
    assert "v-few" in restored["vote_id"].to_pylist()


_FULL = {name: None for name in INPUT_COLUMNS["member_votes"]} | {"vote_id": "v", "member_key": "k"}


@pytest.mark.parametrize("fields", [
    {"vote_id": "v", "member_key": "k", "not_a_column": "x"},
    {"vote_id": "v", "member_key": 7},
    {name: value for name, value in _FULL.items() if name != "position"} | {"not_a_column": "x"},  # as many keys as columns
    _FULL | {"member_key": 7},
    _FULL | {"position": ["Yea"]},
], ids=["extra key", "integer", "renamed key", "integer in a full row", "list in a full row"])
def test_a_retained_row_the_schema_cannot_hold_is_refused_by_both(tmp_path, monkeypatch, fields) -> None:
    _, (subject, receipts), _ = _both(tmp_path, monkeypatch, "member_votes", _member_votes(40))
    _with_attempts(receipts, [({"entry_kind": "row", "source_fields": fields}, {})])
    expected, restored = _restored(tmp_path, monkeypatch, "member_votes", subject, receipts)
    assert isinstance(expected, type) and restored is expected


def test_the_decode_reads_every_code_point_the_row_code_writes() -> None:
    points = [p for p in range(sys.maxunicode + 1) if not 0xD800 <= p <= 0xDFFF]
    values = ["a" + chr(p) + "b" for p in points] + ["", "\\u001B", "\\\\", '"', "\x00", "\x00\x00x", "null", " x ", None]
    schema = pa.schema([("k", pa.string()), ("n", pa.string())])
    texts = [exact_json({"entry_kind": "row", "source_fields": {"k": value, "n": None}}) for value in values]
    columns, plain = congress_bulk.source_fields_sql(schema)
    con = duckdb.connect()
    con.register("t", pa.table({"want": pa.array(values, type=pa.string()), "processing_json": pa.array(texts, type=pa.string())}))
    counted = con.execute(
        f"SELECT count(*), count(*) FILTER (k IS NOT DISTINCT FROM want AND n IS NULL AND plain) "
        f"FROM (SELECT want, {columns}, ({plain}) AS plain FROM t)"
    ).fetchone()
    assert counted == (len(values), len(values))


@pytest.mark.parametrize("dataset", ["roll_call_votes", "nominations", "bill_family_archives", "public_activity_events", "unknown"])
def test_a_dataset_with_other_rules_stays_with_the_row_code(dataset) -> None:
    names = INPUT_COLUMNS.get(dataset, ["a"])
    assert not congress_bulk.eligible(dataset, pa.schema([(name, pa.string()) for name in names]))


def test_a_typed_or_unknown_source_column_stays_with_the_row_code() -> None:
    names = INPUT_COLUMNS["member_votes"]
    assert congress_bulk.eligible("member_votes", pa.schema([(name, pa.string()) for name in names[:5]]))
    assert not congress_bulk.eligible("member_votes", pa.schema([(names[0], pa.int64())]))
    assert not congress_bulk.eligible("member_votes", pa.schema([("not_declared", pa.string())]))


@pytest.mark.parametrize("dataset", ["member_votes", "member_vote_terms"])
def test_partial_source_uses_the_same_missing_columns(tmp_path, dataset):
    source = tmp_path / "partial.parquet"
    pq.write_table(pa.table({"vote_id": ["v"], "member_key": ["k"]}), source)
    row = write_congress_dataset(source, tmp_path / "row", dataset=dataset, generation_id=GENERATION, bulk=False)
    bulk = write_congress_dataset(source, tmp_path / "bulk", dataset=dataset, generation_id=GENERATION)
    for reference, actual in zip(row, bulk):
        assert pq.read_table(reference).equals(pq.read_table(actual))


@pytest.mark.parametrize("field", ["locator", "source_uri", "source_id", "body_version"])
def test_control_character_in_receipt_context_routes_to_reference(tmp_path, monkeypatch, field):
    witness = {**WITNESS, field: "context\x1b"}
    _, row, bulk = _both(tmp_path, monkeypatch, "member_votes", _member_votes(5), witnesses=[witness])
    for reference, actual in zip(row, bulk):
        assert pq.read_table(reference).equals(pq.read_table(actual))
