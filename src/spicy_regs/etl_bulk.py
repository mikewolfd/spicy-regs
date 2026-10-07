"""SQL that reproduces the receipt writer's exact encoding and digests, a column at a time, and a validator built on it.

``etl_receipts.exact_json`` encodes one Python value. These build the DuckDB expression that yields the same text for a
column of an Arrow type, so a table's identities, versions and receipt ids come from one scan instead of one call per
row (2026-10-05: 26M comments took about 20 hours by row, and a million of them 82 seconds this way).

The row functions stay the reference. DuckDB spells one class of text differently: a control character with no short
JSON escape is written ``\\u001B`` where Python writes ``\\u001b``. :func:`needs_reference_sql` marks every text that
holds such an escape, at any depth, and a marked row belongs to the row writer.

:func:`validate_bundle` is ``etl_receipts.validate_receipt_bundle`` over whole columns. SQL may only prove a row good:
a row it cannot prove is read again by the row reader and decided by the row functions themselves.
"""

from __future__ import annotations

import io
import json
import sqlite3
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import closing, contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from spicy_regs.runtime_bounds import bounded_connection, checkpoint
from spicy_regs.etl_receipts import (
    RECEIPT_SCHEMA,
    DatasetPolicy,
    ParquetInput,
    _bundle_policies,
    _load_receipts,
    _parquet,
    _rows,
    _subjects,
    _unpack,
    subject_identity,
    receipt_policies,
    validate_publisher_generation,
)

#: An upper-case hex letter in a control character's escape: the backslash run before ``u`` is odd, so it is an escape
#: and not a doubled literal backslash followed by the letters.
_UPPER_HEX_ESCAPE = r"(^|[^\\])(\\\\)*\\u00[01][A-F]"
#: A JSON string as ``json.loads`` reads one: no raw control character, and only the escapes JSON defines.
_STRING = r'"(?:[^"\\\x00-\x1f]|\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4}))*"'
#: The encoded values with nothing inside them. An integer is held to 18 digits, below where ``int`` refuses a text.
_SCALAR = r'\["null",null\]|\["bool",(?:true|false)\]|\["int",-?(?:0|[1-9][0-9]{0,17})\]|\["str",' + _STRING + r"\]"
#: A list or a mapping whose members have each already been replaced by the ``chr(1)`` that stands for one value.
_NESTED = (
    r'\["list",\[(?:\x01(?:,\x01)*)?\]\]|\["dict",\[(?:\[' + _STRING + r",\x01\](?:,\[" + _STRING + r",\x01\])*)?\]\]"
)
#: One member of a mapping whose value is replaced; the key is taken only where its text is its JSON spelling.
_MEMBER = r'\["([^"\\\x00-\x1f]*)",\x01\]'
#: How many levels of list and mapping the proof follows. A deeper value is the row reader's to decide.
_DEPTH = 8
#: ``ReceiptContext``'s rule for a witness digest.
_WITNESS_SHA256 = r"(?:sha256:)?[0-9a-f]{64}"
_BATCH = 2_000


class NotBulkEligible(Exception):
    """SQL has no proven way to decide this bundle, so ``validate_receipt_bundle`` must."""


@contextmanager
def bulk_connection(directory: Path | None = None):
    """A private disk-backed DuckDB with bounded CPU, RAM and spill.

    Each operation owns its files. A second statement must never run while a
    result from this connection is still being consumed.
    """
    with TemporaryDirectory(prefix="etl-bulk-", dir=directory) as temp:
        root = Path(temp)
        with closing(duckdb.connect(str(root / "work.duckdb"), config={
            "threads": 4, "memory_limit": "4GB", "max_temp_directory_size": "32GB",
            "temp_directory": str(root / "spill"),
        })) as con, bounded_connection(con):
            yield con, root


def _ordinal_file(con, query: str, parameters, path: Path) -> Iterator[int]:
    """Finish the SQL query before streaming its bounded ordinal batches."""
    con.execute(f"COPY ({query}) TO {_literal(str(path))} (FORMAT PARQUET)", parameters)
    with pq.ParquetFile(path) as source:
        for batch in source.iter_batches(batch_size=_BATCH):
            checkpoint()
            yield from batch.column(0).to_pylist()


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def exact_json_sql(expression: str, dtype: pa.DataType) -> str:
    """SQL text equal to ``exact_json`` of ``expression``'s value: the tagged encoding with keys in sorted order.

    Covers strings, integers, booleans, fixed-point decimal128 scales 0 through 6,
    and structs and lists of them whose DuckDB text equals Python's.
    Any other type refuses, so its table stays with the row writer until a test here proves its spelling.
    """
    if pa.types.is_string(dtype) or pa.types.is_large_string(dtype):
        body = f"'[\"str\",' || CAST(to_json({expression}) AS VARCHAR) || ']'"
    elif pa.types.is_integer(dtype):
        body = f"'[\"int\",' || CAST({expression} AS VARCHAR) || ']'"
    elif pa.types.is_boolean(dtype):
        body = f"'[\"bool\",' || CASE WHEN {expression} THEN 'true' ELSE 'false' END || ']'"
    elif pa.types.is_decimal128(dtype) and 0 <= dtype.scale <= 6:
        # Arrow's Decimal and DuckDB both retain fixed-point spelling at these
        # scales. Higher scales can use Python's exponent spelling and remain
        # with the reference validator until their exact encoding is proven.
        # Widen precision without changing scale: DECIMAL(1,1) otherwise writes
        # '.0' in DuckDB while Arrow's Decimal spells it '0.0'.
        text = f"CAST(CAST({expression} AS DECIMAL(38,{dtype.scale})) AS VARCHAR)"
        body = f"'[\"decimal\",' || CAST(to_json({text}) AS VARCHAR) || ']'"
    elif pa.types.is_struct(dtype):
        body = record_json_sql(((field.name, field.type) for field in dtype), qualifier=expression + ".")
    elif pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        item = exact_json_sql("item", dtype.value_type)
        body = f"'[\"list\",[' || array_to_string(list_transform({expression}, item -> {item}), ',') || ']]'"
    else:
        raise NotImplementedError(f"No proven SQL spelling for {dtype}")
    return f"CASE WHEN {expression} IS NULL THEN '[\"null\",null]' ELSE {body} END"


def _concat_sql(parts: list[str]) -> str:
    """Balance nonempty ordered fragments, retaining ``||`` and its NULL propagation."""
    while len(parts) > 1:
        parts = [f"({parts[i]} || {parts[i + 1]})" if i + 1 < len(parts) else parts[i]
                 for i in range(0, len(parts), 2)]
    return parts[0]


def record_json_sql(fields: Iterable[tuple[str, pa.DataType]], *, qualifier: str = "") -> str:
    """SQL text equal to ``exact_json`` of a mapping of the named columns (or of a struct's fields).

    DuckDB matches a name without regard to case, so two fields that differ only by case would both read the first
    one; they refuse like an unproven type.
    """
    fields = list(fields)
    if len({name.casefold() for name, _ in fields}) != len(fields):
        raise NotImplementedError("No proven SQL spelling for field names that differ only by case")
    if not fields:
        return "'[\"dict\",[]]'"
    parts = ["'[\"dict\",['"]
    for index, (name, dtype) in enumerate(sorted(fields, key=lambda field: field[0])):
        if index:
            parts.append("','")
        parts.extend((
            _literal("[" + json.dumps(name, ensure_ascii=False) + ","),
            exact_json_sql(qualifier + '"' + name.replace('"', '""') + '"', dtype),
            "']'",
        ))
    parts.append("']]'")
    return _concat_sql(parts)


def digest_sql(text: str) -> str:
    """SQL equal to ``etl_receipts._digest`` of a value whose exact encoding is ``text``."""
    return f"'sha256:' || sha256({text})"


def needs_reference_sql(text: str) -> str:
    """Whether an encoding built here differs from the reference's: it holds an upper-case hex control escape."""
    return f"regexp_matches({text}, {_literal(_UPPER_HEX_ESCAPE)})"


def identity_sql(policy: DatasetPolicy) -> tuple[str, str, str]:
    """SQL for ``subject_identity`` over a table of the policy's subject columns: record id, version, identity text."""
    types = {field.name: field.type for field in policy.subject_schema}
    pairs = " || ',' || ".join(
        _literal('["list",[["str",' + json.dumps(key, ensure_ascii=False) + "],")
        + " || "
        + exact_json_sql('"' + key.replace('"', '""') + '"', types[key])
        + " || ']]'"
        for key in policy.identity_fields
    )
    identity = "'[\"list\",[' || " + pairs + " || ']]'"
    head = _literal('["list",[["str",' + json.dumps(policy.dataset, ensure_ascii=False) + "],")
    subject = record_json_sql((field.name, field.type) for field in policy.subject_schema)
    return (
        digest_sql(f"{head} || {identity} || ']]'"),
        digest_sql(f"{head} || {subject} || ']]'"),
        identity,
    )


def receipt_json_sql() -> str:
    """SQL text of a receipt row without its id: what ``receipt_id`` is the digest of."""
    return record_json_sql((field.name, field.type) for field in RECEIPT_SCHEMA if field.name != "receipt_id")


def reduced_sql(text: str) -> str:
    """``text`` with each value of the tagged encoding, to ``_DEPTH`` levels, replaced by ``chr(1)``.

    Every replacement stands for a text ``_unpack(json.loads(...))`` returns from without raising, so a text that
    reduces to one ``chr(1)`` is one such value. That holds only for a text with no ``chr(1)`` of its own, which the
    caller checks. A float, decimal, date or bytes value is never replaced: its row goes to the reference.
    """
    reduced = f"regexp_replace({text}, {_literal(_SCALAR)}, chr(1), 'g')"
    for _ in range(_DEPTH):
        reduced = f"regexp_replace({reduced}, {_literal(_NESTED)}, chr(1), 'g')"
    return reduced


def decodes_sql(text: str) -> str:
    """Whether SQL proves ``_unpack(json.loads(text))`` returns. False or NULL proves nothing either way."""
    return f"(NOT contains({text}, chr(1)) AND {reduced_sql(text)} = chr(1))"


def mapping_keys_sql(text: str) -> str:
    """The keys of the mapping ``text`` decodes to, or NULL where SQL cannot prove it decodes to one.

    A mapping is its members between a fixed head and tail, so only the members are reduced. A key spelled with an
    escape is not proven: its text would have to be decoded as ``json.loads`` decodes it.
    """
    members = reduced_sql(f"substr({text}, 10, length({text}) - 11)")
    return (
        f"CASE WHEN NOT contains({text}, chr(1)) AND length({text}) >= 11 AND starts_with({text}, '[\"dict\",[')"
        f" AND ends_with({text}, ']]') THEN list_transform([{members}], members -> CASE WHEN"
        f" regexp_full_match(members, {_literal(f'(?:{_MEMBER}(?:,{_MEMBER})*)?')})"
        f" THEN regexp_extract_all(members, {_literal(_MEMBER)}, 1) END)[1] END"
    )


def _names_sql(names: Iterable[str]) -> str:
    return "[" + ", ".join(_literal(name) for name in names) + "]::VARCHAR[]"


def _by_dataset_sql(policies: Mapping[str, DatasetPolicy], each: Callable[[DatasetPolicy], str], other: str) -> str:
    """``each`` policy's expression for a receipt of its dataset, and ``other`` for a dataset with no policy."""
    cases = " ".join(
        f"WHEN {_literal(name)} THEN CASE policy_version "
        + " ".join(f"WHEN {_literal(p.policy_version)} THEN {each(p)}" for p in receipt_policies(policy))
        + f" ELSE {other} END" for name, policy in policies.items()
    )
    return f"CASE dataset {cases} ELSE {other} END" if cases else other


def _receipt_insert_sql(policies: Mapping[str, DatasetPolicy], scoped: bool, *, retain_processing: bool = False) -> str:
    """Keep each receipt's join fields and whether SQL proved every check ``_load_receipts`` makes of one row.

    The conditions follow that function's order. One that is false or NULL leaves the row unproven; nothing here
    refuses a row. ``scoped`` leaves out the receipts ``select_receipts`` would not have copied.
    """
    witness = (
        "coalesce(w.source_id <> '', false) AND (coalesce(w.sha256 <> '', false) OR coalesce(w.body_version <> '', false))"
        f" AND (w.sha256 IS NULL OR regexp_full_match(w.sha256, {_literal(_WITNESS_SHA256)}))"
    )
    failed = _by_dataset_sql(policies, lambda policy: _names_sql(sorted(policy.input_fields)), "NULL")
    passed = _by_dataset_sql(policies, lambda policy: _names_sql(policy.receipt_fields), "NULL")
    proven = " AND ".join(
        (
            _by_dataset_sql(policies, lambda policy: f"policy_version = {_literal(policy.policy_version)}", "false"),
            f"receipt_id = {digest_sql('body')} AND NOT {needs_reference_sql('body')}",
            decodes_sql("diagnostic_json"),
            "generation_id <> '' AND attempt_id <> '' AND processor <> ''",
            f"len(witnesses) > 0 AND list_bool_and(list_transform(witnesses, w -> {witness}))",
            "CASE WHEN outcome = 'accepted' THEN subject_version IS NOT NULL AND record_id <> '' AND identity_json <> ''"
            f" AND {_by_dataset_sql(policies, lambda policy: 'false' if policy.receipt_only else 'true', 'false')}"
            " WHEN outcome IN ('rejected', 'refused', 'error', 'observed') THEN subject_version IS NULL ELSE false END",
            f"list_has_all(CASE WHEN outcome IN ('rejected', 'refused', 'error') THEN {failed} ELSE {passed} END, fields)",
        )
    )
    selected = f" WHERE dataset IN ({', '.join(map(_literal, policies)) or 'NULL'})" if scoped else ""
    return (
        "INSERT INTO receipts SELECT n, dataset, generation_id, receipt_id, outcome,"
        " CASE WHEN outcome = 'accepted' THEN record_id END, CASE WHEN outcome = 'accepted' THEN subject_version END,"
        f" CASE WHEN outcome = 'accepted' THEN identity_json END, ({proven}) IS NOT TRUE"
        + (", processing_json, diagnostic_json" if retain_processing else "") + " FROM (SELECT *,"
        f" {receipt_json_sql()} AS body, {mapping_keys_sql('processing_json')} AS fields FROM source{selected})"
    )


def _subject_insert_sql(policy: DatasetPolicy, number: str) -> str:
    """Keep each subject's identity as ``subject_identity`` gives it, and whether the row functions must give it instead.

    They must where the encoding holds an escape SQL spells differently, and where an identity field is NULL without
    leave, which ``subject_identity`` refuses.
    """
    record_id, version, identity = identity_sql(policy)
    return (
        f'INSERT INTO subjects SELECT "{number}", {_literal(policy.dataset)}, {record_id}, {version}, {identity},'
        f" ({_subject_reference_sql(policy)}) IS NOT FALSE FROM source"
    )


def _subject_reference_sql(policy: DatasetPolicy) -> str:
    """Subjects whose exact encoding or null validation belongs to the row reference."""
    subject = record_json_sql((field.name, field.type) for field in policy.subject_schema)
    missing = " OR ".join(
        '"' + key.replace('"', '""') + '" IS NULL'
        for key in dict.fromkeys([*(field.name for field in policy.subject_schema if not field.nullable),
                                  *(key for key in policy.identity_fields if key not in policy.nullable_identity_fields)])
    )
    return f"{needs_reference_sql(subject)} OR {missing or 'false'}"


def _numbered(
    parquet: pq.ParquetFile, start: int, number: str, wanted: frozenset[str] | None, read: list[int]
) -> pa.RecordBatchReader:
    """The file's rows as the row reader decodes them, with each row's number from ``start`` in a column ``number``.

    Every batch is validated first: DuckDB hashes an Arrow string without checking that it is UTF-8, where the row
    reader refuses to decode one that is not. ``wanted`` names the datasets a scoped read keeps; a batch holding none
    of them is still decoded and validated, as ``select_receipts`` would have read it, and then left out. ``read``
    counts the rows decoded and the rows SQL is to keep, so the caller can hold DuckDB to both.
    """
    schema = parquet.schema_arrow.append(pa.field(number, pa.int64()))

    def batches() -> Iterator[pa.RecordBatch]:
        first = start
        for batch in parquet.iter_batches(batch_size=_BATCH):
            checkpoint()
            batch.validate(full=True)
            last = first + batch.num_rows
            kept = batch.num_rows
            if wanted is not None:
                held = batch.column("dataset").value_counts().to_pylist()
                kept = sum(pair["counts"] for pair in held if pair["values"] in wanted)
            read[0] += batch.num_rows
            read[1] += kept
            if kept:
                yield pa.RecordBatch.from_arrays([*batch.columns, pa.arange(first, last)], schema=schema)
            first = last

    return pa.RecordBatchReader.from_batches(schema, batches())


Span = tuple[int, int, ParquetInput]


def _scan(
    con: duckdb.DuckDBPyConnection,
    paths: Iterable[ParquetInput],
    start: int,
    insert: str,
    *,
    number: str = "n",
    wanted: frozenset[str] | None = None,
) -> list[Span]:
    """Run ``insert`` once over each file, numbering rows on from ``start``; each file's row numbers and its input.

    A stream that ends early loses rows without an error, so the rows decoded and the rows inserted are both counted.
    """
    spans = []
    for path in paths:
        with _parquet(path) as parquet:
            read = [0, 0]
            con.register("source", _numbered(parquet, start, number, wanted, read))
            [(inserted,)] = con.execute(insert).fetchall()
            con.unregister("source")
            if read != [parquet.metadata.num_rows, inserted]:
                raise NotBulkEligible("DuckDB did not take every row the reader decoded")
            spans.append((start, start + parquet.metadata.num_rows, path))
            start += parquet.metadata.num_rows
    return spans


def _readable(
    paths: Sequence[ParquetInput], schema: pa.Schema, differs: str
) -> tuple[Sequence[ParquetInput], Exception | None]:
    """The inputs before the first the row validator stops at for its schema, and what it raises there."""
    for index, path in enumerate(paths):
        try:
            with _parquet(path) as parquet:
                if parquet.schema_arrow.equals(schema):
                    continue
        except Exception as error:
            return paths[:index], error
        return paths[:index], ValueError(differs)
    return paths, None


def _held(spans: Iterable[Span], numbers: Iterable[int]) -> Iterator[tuple[list[int], pa.Table]]:
    """The rows with these ascending numbers, read again as the row reader decodes them, a row group at a time.

    A row group is read once however many of its rows are asked for, and only when one is, so this costs one row
    group's decode for each row at worst and the row reader's own pass at most.
    """
    numbers = iter(numbers)
    wanted = next(numbers, None)
    for first, stop, path in spans:
        if wanted is None or wanted >= stop:
            continue
        with _parquet(path) as parquet:
            for group in range(parquet.num_row_groups):
                stop_group = first + parquet.metadata.row_group(group).num_rows
                if wanted is not None and wanted < stop_group:
                    for batch in parquet.iter_batches(batch_size=_BATCH, row_groups=[group]):
                        last = first + batch.num_rows
                        picked = []
                        while wanted is not None and wanted < last:
                            picked.append(wanted)
                            wanted = next(numbers, None)
                        if picked:
                            yield picked, pa.Table.from_batches([batch]).take([number - first for number in picked])
                        first = last
                first = stop_group


def _member(table: pa.Table) -> Callable[[], io.BytesIO]:
    sink = io.BytesIO()
    pq.write_table(table, sink)
    return lambda: io.BytesIO(sink.getvalue())


def _repeated_sql(key: str, where: str) -> str:
    """The first row that repeats an earlier row's ``key``: where the row loader's unique index refuses an insert."""
    return (
        f"SELECT min(n) FROM (SELECT n, row_number() OVER (PARTITION BY {key} ORDER BY n) AS turn FROM receipts"
        f" WHERE {where} AND ({key}) IN (SELECT {key} FROM receipts WHERE {where} GROUP BY ALL HAVING count(*) > 1))"
        " WHERE turn > 1"
    )


def _check_receipts(
    con: duckdb.DuckDBPyConnection,
    temp: Path,
    receipt_paths: Sequence[ParquetInput],
    policies: Mapping[str, DatasetPolicy],
    generation_id: str | None,
    scoped: bool,
    *,
    retain_processing: bool = False,
) -> None:
    """``_load_receipts`` over whole columns: raise what it would at the first row, or schema, it refuses."""
    validate_publisher_generation(generation_id)
    paths, stopped = _readable(receipt_paths, RECEIPT_SCHEMA, "Receipt schema differs from the shared schema")
    if scoped and stopped is not None:
        raise NotBulkEligible("select_receipts decides a receipt file that is not the shared schema")
    con.execute(
        "CREATE TABLE receipts (n BIGINT, dataset VARCHAR, generation_id VARCHAR, receipt_id VARCHAR, outcome VARCHAR,"
        " record_id VARCHAR, subject_version VARCHAR, identity_json VARCHAR, unproven BOOLEAN"
        + (", processing_json VARCHAR, diagnostic_json VARCHAR" if retain_processing else "") + ")"
    )
    wanted = frozenset(policies) if scoped else None
    spans = _scan(con, paths, 0, _receipt_insert_sql(policies, scoped, retain_processing=retain_processing), wanted=wanted)

    repeats = (
        con.execute(_repeated_sql("receipt_id", "true")).fetchall()[0][0],
        con.execute(_repeated_sql("dataset, record_id", "outcome = 'accepted' AND record_id IS NOT NULL")).fetchall()[0][0],
    )
    # No row after the first repeated receipt can change the row loader's refusal.
    last = min((number for number in repeats if number is not None), default=None)
    # Read to the end before anything else runs on this database: a second statement would end the stream.
    unproven = "SELECT n FROM receipts WHERE unproven AND n <= coalesce(?, n) ORDER BY n"
    numbers = _ordinal_file(con, unproven, [last], temp / "unproven-receipts.parquet")
    with closing(sqlite3.connect(str(temp / "reference.db"))) as reference:
        _load_receipts(
            reference,
            (_member(table) for _, table in _held(spans, numbers)),
            policies,
            generation_id,
            retain_processing=False,
        )
    if last is not None:
        raise ValueError("Duplicate or ambiguous receipt join")
    if stopped is not None:
        raise stopped


def _check_subjects(
    con: duckdb.DuckDBPyConnection, subjects: Mapping[str, Sequence[ParquetInput]], policies: Mapping[str, DatasetPolicy], temp: Path
) -> None:
    """``_joined_subjects`` over whole columns: raise what it would at the first subject, or schema, it refuses."""
    con.execute(
        "CREATE TABLE subjects (n BIGINT, dataset VARCHAR, record_id VARCHAR, version VARCHAR, identity VARCHAR,"
        " unproven BOOLEAN)"
    )
    start, stopped, before = 0, None, None
    for dataset, paths in subjects.items():
        policy = policies[dataset]
        if policy.receipt_only and paths:
            stopped = ValueError("Processing-only dataset cannot publish a subject table")
            break
        if not paths:
            continue
        number = "n"
        while number in {name.casefold() for name in policy.subject_schema.names}:
            number += "_"
        paths, stopped = _readable(paths, policy.subject_schema, f"Subject schema differs from policy: {dataset}")
        spans = _scan(con, paths, start, _subject_insert_sql(policy, number), number=number)
        start = spans[-1][1] if spans else start
        unproven = "SELECT n FROM subjects WHERE unproven AND dataset = ? ORDER BY n"
        numbers = _ordinal_file(con, unproven, [dataset], temp / "unproven-subjects.parquet")
        for picked, table in _held(spans, numbers):
            given = []
            for held, raw in zip(picked, table.to_pylist()):
                try:
                    try:
                        subject = _subjects(policy, [raw])[0]
                    except ValueError as error:
                        raise NotBulkEligible("The row normalizer must decide subject validation order") from error
                    given.append((held, dataset, *subject_identity(policy, subject), False))
                except ValueError as error:
                    # The row validator raises this only if every subject before it has its receipt.
                    stopped, before = error, held
                    break
            if given:
                con.executemany("INSERT INTO subjects VALUES (?, ?, ?, ?, ?, ?)", given)
            if before is not None:
                break
        if stopped is not None:
            break
    joined = (
        "FROM (SELECT * FROM subjects WHERE NOT unproven AND n < coalesce(?, n + 1)) AS s"
        " LEFT JOIN (SELECT * FROM receipts WHERE outcome = 'accepted') AS r ON s.dataset = r.dataset"
        " AND s.record_id = r.record_id AND s.version = r.subject_version AND s.identity = r.identity_json"
    )
    [(total, matched, used)] = con.execute(
        f"SELECT count(*), count(r.n), count(DISTINCT r.n) {joined}", [before]
    ).fetchall()
    if not total == matched == used:
        # The first subject with no accepted receipt, or whose receipt an earlier subject already took.
        [(dataset,)] = con.execute(
            "SELECT arg_min(dataset, n) FROM (SELECT s.n, s.dataset, r.n AS receipt,"
            f" row_number() OVER (PARTITION BY r.n ORDER BY s.n) AS turn {joined}) WHERE receipt IS NULL OR turn > 1",
            [before],
        ).fetchall()
        raise ValueError(f"Missing, ambiguous or reused subject receipt: {dataset}")
    if stopped is not None:
        raise stopped
    if [(used,)] != con.execute("SELECT count(*) FROM receipts WHERE outcome = 'accepted'").fetchall():
        raise ValueError("Accepted receipt has no matching subject")


def _nested_nullable(dtype: pa.DataType) -> bool:
    """Whether nested nulls need no extra validation beyond Arrow decoding."""
    if pa.types.is_struct(dtype):
        return all(field.nullable and _nested_nullable(field.type) for field in dtype)
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype):
        return dtype.value_field.nullable and _nested_nullable(dtype.value_type)
    return True


def validate_bundle(
    subjects: Mapping[str, Sequence[ParquetInput]],
    receipt_paths: Sequence[ParquetInput],
    policies: Sequence[DatasetPolicy],
    *,
    generation_id: str | None = None,
    scoped: bool = False,
) -> None:
    """``validate_receipt_bundle`` in bulk: accept and refuse the same bundles, raising what it raises.

    Each file is read once, through the row reader's own Parquet decoder, and each row is encoded, hashed and checked
    once in DuckDB. A row SQL cannot prove (an escape it spells differently, a value type it does not follow, anything
    malformed) is read again and given to ``_load_receipts`` or ``subject_identity``, so every refusal of one row is
    the row code's own. The joins the row validator makes through a SQLite index are set operations over a DuckDB
    file in a temporary directory, which spills there under memory pressure.

    Raises :class:`NotBulkEligible` when SQL cannot decide: a subject type with no proven spelling, or anything DuckDB
    or the decoder cannot read. The caller then runs the row validator, which decides.

    ``scoped`` ignores receipts of datasets with no policy here, as if ``select_receipts`` had first copied each
    policy's dataset out of a shared file; the receipts then keep their order in that file.
    """
    with _validated_bundle(subjects, receipt_paths, policies, generation_id=generation_id, scoped=scoped):
        pass


@contextmanager
def _validated_bundle(subjects, receipt_paths, policies, *, generation_id, scoped=False, retain_processing=False):
    """Keep the maintained admission tables alive for replay only after complete admission."""
    registered = _bundle_policies(subjects, receipt_paths, policies)
    if scoped and len(registered) > 1:
        raise NotBulkEligible("Multiple scoped policies require grouped row selection to preserve first-error order")
    try:
        for policy in registered.values():
            if not policy.receipt_only and subjects[policy.dataset]:
                if not all(_nested_nullable(field.type) for field in policy.subject_schema):
                    raise NotBulkEligible("Nested nonnullable fields require the row normalizer")
                _subject_insert_sql(policy, "n")
    except NotImplementedError as error:
        raise NotBulkEligible(str(error)) from error
    with bulk_connection() as (con, temp):
        try:
            con.execute("SET preserve_insertion_order = false")
            _check_receipts(con, Path(temp), receipt_paths, registered, generation_id, scoped,
                            retain_processing=retain_processing)
            _check_subjects(con, subjects, registered, temp)
        except duckdb.Error as error:
            raise NotBulkEligible(f"DuckDB could not decide the bundle: {error}") from error
        yield con


def read_with_receipts(subject_paths, receipt_paths, policy, *, generation_id):
    """Replay completely admitted subjects in source order, with the exact stored processing JSON.

    Admission uses the same SQL checks and row-reference fallbacks as validate_bundle.
    Replay never falls back after yielding: any truncated stream or changed input fails
    the read, so atomic processing writers cannot accept an incomplete population.
    """
    if not receipt_paths:
        raise NotBulkEligible("The row reader decides absent receipt inputs")
    with _validated_bundle({policy.dataset: subject_paths}, receipt_paths, [policy],
                           generation_id=generation_id, retain_processing=True) as con:
        yield from _replay_admitted_subjects(con, subject_paths, policy)


def _replay_admitted_subjects(con, subject_paths, policy, *, start=0):
    """Shared replay over already completely admitted state; no second admission."""
    if not subject_paths:
        return
    number = "n"
    while number in {field.name.casefold() for field in policy.subject_schema}:
        number += "_"
    fields = ["\"" + field.name.replace('"', '""') + "\"" for field in policy.subject_schema]
    subject_sql = "struct_pack(" + ", ".join(f"{name} := source.{name}" for name in fields) + ")"
    record_id, version, identity = identity_sql(policy)
    aliases = []
    occupied = {field.name.casefold() for field in policy.subject_schema} | {number}
    for name in ("_replay_id", "_replay_version", "_replay_identity", "_replay_reference"):
        while name in occupied:
            name += "_"
        aliases.append(name)
        occupied.add(name)
    key_name, version_name, identity_name, reference_name = aliases
    same = (f'CASE WHEN source."{reference_name}" IS NOT FALSE THEN NULL ELSE'
            f' source."{key_name}" = s.record_id AND source."{version_name}" = s.version'
            f' AND source."{identity_name}" = s.identity END')
    for path in subject_paths:
        with _parquet(path) as parquet:
            if not parquet.schema_arrow.equals(policy.subject_schema):
                raise ValueError(f"Subject schema differs from policy: {policy.dataset}")
            read = [0, 0]
            con.register("source", _numbered(parquet, start, number, None, read))
            count = 0
            try:
                query = (
                    f'SELECT {subject_sql}, r.processing_json, {same}, s.record_id, s.version, s.identity'
                    f' FROM (SELECT *, {record_id} AS "{key_name}", {version} AS "{version_name}",'
                    f' {identity} AS "{identity_name}", {_subject_reference_sql(policy)} AS "{reference_name}"'
                    f' FROM source) source JOIN subjects s ON source."{number}" = s.n AND NOT s.unproven'
                    " JOIN receipts r ON s.dataset = r.dataset AND s.record_id = r.record_id"
                    " AND s.version = r.subject_version AND s.identity = r.identity_json"
                    f" AND r.outcome = 'accepted' ORDER BY source.\"{number}\""
                )
                for batch in con.execute(query).to_arrow_reader(_BATCH):
                    raw = batch.column(0).to_pylist()
                    subjects = _subjects(policy, raw)
                    for subject, processing, matched, key, expected_version, expected_identity in zip(
                        subjects, *(batch.column(i).to_pylist() for i in range(1, 6))
                    ):
                        if matched is False or matched is None and subject_identity(policy, subject) != (key, expected_version, expected_identity):
                            raise ValueError(f"Missing, ambiguous or reused subject receipt: {policy.dataset}")
                        count += 1
                        yield subject | _unpack(json.loads(processing))
            finally:
                con.unregister("source")
            if read != [parquet.metadata.num_rows] * 2 or count != parquet.metadata.num_rows:
                raise ValueError("Receipt replay did not reconstruct every subject row")
            start += count


def read_attempts(receipt_paths, policy, *, generation_id, outcomes):
    """Admit every receipt before replaying its exact fields, without rebuilding the row index."""
    with bulk_connection() as (con, temp):
        try:
            _check_receipts(con, temp, receipt_paths, {policy.dataset: policy}, generation_id, False)
        except duckdb.Error as error:
            raise NotBulkEligible(f"DuckDB could not decide the receipts: {error}") from error
        for path in receipt_paths:
            for receipt in _rows(path):
                if outcomes is None or receipt["outcome"] in outcomes:
                    yield {
                        **receipt,
                        "processing_fields": _unpack(json.loads(receipt["processing_json"])),
                        "diagnostics": _unpack(json.loads(receipt["diagnostic_json"])),
                    }
