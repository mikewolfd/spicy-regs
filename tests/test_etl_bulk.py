"""The SQL encodings equal the receipt writer's, or mark the text for the row writer."""

import json
import os
import random
import signal
import subprocess
import sys
import threading
import time
from decimal import Decimal, localcontext
from pathlib import Path

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
    assert marked == UPPER_HEX - {0x1F}


@pytest.mark.parametrize("value", [
    "\\u001B", "\\\\u001B", "back\\slash", 'quote"', "\\", "\\\\", "x\\\\\\u000E", "\x00\x01\x12\x19", "\t\n\r\b\f", "\x7f  ﻿",
])
def test_text_that_only_looks_like_an_escape_is_not_marked(value: str) -> None:
    [(text, flagged)] = _encode([value], pa.string())
    assert not flagged and text == exact_json(value)


@pytest.mark.parametrize("value", ["\x1b", "\x1b\x1b", "\\\x0b", "\\\\\x1e", "a\x0eb\x0fc", "\x1a" * 5, "\x1f\x1b\x1f"])
def test_an_upper_hex_escape_is_marked_wherever_it_stands(value: str) -> None:
    [(text, flagged)] = _encode([value], pa.string())
    assert flagged and text != exact_json(value) and text.lower() == exact_json(value).lower()


@pytest.mark.parametrize("dtype", [pa.string(), pa.large_string()])
def test_unit_separators_preserve_literal_escapes_and_backslash_runs(dtype):
    values = [None, "", "\x1f", "\x1f\x1f", "\x1fstart", "end\x1f", 'a\x1f"b"\x1fc']
    for count in range(9):
        slashes = "\\" * count
        values.extend([slashes + "u001F", slashes + "\x1f", "\x1f" + slashes,
                       slashes + "u001F\x1f" + slashes + "u001f",
                       slashes + "\x1f\x1f" + slashes + "u001F"])
    assert _encode(values, dtype) == [(exact_json(value), False) for value in values]


def test_nested_unit_separator_values_keep_exact_spelling():
    dtype = pa.list_(pa.struct([("path", pa.list_(pa.string()))]))
    values = [None, [], [None], [{"path": None}], [{"path": []}],
              [{"path": [None, "", "a\x1fb", "\\u001F\x1f\\\\", "\x1f\x1f"]}]]
    assert _encode(values, dtype) == [(exact_json(value), False) for value in values]


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
    with pytest.raises(NotImplementedError, match="decimal128"):
        etl_bulk.exact_json_sql('"v"', pa.decimal128(12, 8))


@pytest.mark.parametrize(("precision", "scale"), [(precision, scale) for precision in (1, 9, 18, 38)
                                                for scale in range(7) if scale <= precision])
def test_fixed_point_decimal_spelling_matches_arrow_reference(precision, scale):
    rng = random.Random(20261006)
    integers = [0, 1, -1, 10**precision - 1, -(10**precision - 1)]
    integers += [rng.randrange(-(10**precision - 1), 10**precision) for _ in range(100)]
    values = [None] + [Decimal((int(n < 0), tuple(map(int, str(abs(n)))), -scale)) for n in integers]
    dtype = pa.decimal128(precision, scale)
    stored = pa.array(values, type=dtype).to_pylist()
    assert _encode(values, dtype) == [(exact_json(value), False) for value in stored]


@pytest.mark.parametrize("capitals", [0, 1])
def test_scorecard_decimal_fixed_values_match_and_exponent_values_route(capitals):
    dtype = pa.decimal128(38, 18)
    fixed = [None, *map(Decimal, [
        "99999999999999999999.999999999999999999", "-99999999999999999999.999999999999999999",
        "1.200000000000000000", "-1.200000000000000000", "0.000001", "-0.000001",
        "0.000001000000000001", "-0.000001000000000001",
    ])]
    routed = list(map(Decimal, ["0", "-0", "0.000000999999999999", "-0.000000999999999999", "1e-18", "-1e-18"]))
    with localcontext() as context:
        context.capitals = capitals
        stored = pa.array(fixed, type=dtype).to_pylist()
        expected = [(exact_json(value), False) for value in stored] + [(etl_bulk._ROUTE, True)] * len(routed)
        assert _encode(fixed + routed, dtype) == expected


def test_nested_decimal_lists_preserve_nulls_and_exact_versions():
    dtype = pa.list_(pa.struct([("amounts", pa.list_(pa.decimal128(38, 2)))]))
    values = [None, [], [None], [{"amounts": None}], [{"amounts": []}],
              [{"amounts": [None, Decimal("0.00"), Decimal("-0.01"), Decimal("999999999999999999999999999999999999.99")]}]]
    stored = pa.array(values, type=dtype).to_pylist()
    assert _encode(values, dtype) == [(exact_json(value), False) for value in stored]


def test_nested_scorecard_decimal_retains_nulls_and_routes_only_exponent_values():
    dtype = pa.list_(pa.struct([("amounts", pa.list_(pa.decimal128(38, 18)))]))
    fixed = [None, [], [None], [{"amounts": None}], [{"amounts": []}],
             [{"amounts": [None, Decimal("-0.000001"), Decimal("99999999999999999999.999999999999999999")]}]]
    routed = [[{"amounts": [Decimal("0")]}], [{"amounts": [Decimal("1e-18")]}]]
    encoded = _encode(fixed + routed, dtype)
    assert encoded[:len(fixed)] == [(exact_json(value), False) for value in pa.array(fixed, type=dtype).to_pylist()]
    assert all(marked and etl_bulk._ROUTE in text for text, marked in encoded[len(fixed):])


@pytest.mark.parametrize("dtype", [pa.decimal128(38, 7), pa.decimal128(37, 18), pa.decimal128(38, -2), pa.decimal256(76, 2)])
def test_unproven_decimal_spelling_remains_reference_only(dtype):
    with pytest.raises(NotImplementedError, match="No proven SQL spelling"):
        etl_bulk.exact_json_sql('"v"', dtype)


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


def _wait_for_file(path: Path, proc: subprocess.Popen[str], timeout: float = 15.0) -> str:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists():
            text = path.read_text().strip()
            if text:
                return text
        if proc.poll() is not None:
            out, err = proc.communicate(timeout=5)
            raise AssertionError(f"Subprocess exited early with rc={proc.returncode}: out={out!r}, err={err!r}")
        time.sleep(0.02)
    proc.kill()
    out, err = proc.communicate(timeout=5)
    raise AssertionError(f"Timed out waiting for {path}: out={out!r}, err={err!r}")


def test_bulk_connection_sigterm_cleans_owned_spill_and_preserves_evidence(tmp_path: Path) -> None:
    work_dir = tmp_path / "work"
    work_dir.mkdir()
    sibling_file = work_dir / "preserved.json"
    sibling_file.write_text('{"keep": true}')
    recovery_dir = work_dir / "recovery"
    recovery_dir.mkdir()
    recovery_checkpoint = recovery_dir / "checkpoint.json"
    recovery_checkpoint.write_text('{"phase": "admitted"}')

    evidence_dir = tmp_path / "retained-evidence"
    evidence_dir.mkdir()
    evidence_file = evidence_dir / "failure.json"
    evidence_file.write_text('{"status": "retained"}')

    ready = tmp_path / "ready.txt"
    script = """
import os
from pathlib import Path
from spicy_regs.etl_bulk import bulk_connection

work_dir = Path(os.environ["WORK_DIR"])
ready = Path(os.environ["READY_FILE"])
with bulk_connection(work_dir, threads=4, memory_limit="32MB") as (con, root):
    con.execute("SET preserve_insertion_order = false")
    con.execute("CREATE TABLE t AS SELECT i, repeat('x', 500) AS s FROM range(100000) t(i)")
    ready.write_text(str(root))
    # Force active multi-threaded spill into root / "spill" while awaiting SIGTERM.
    while True:
        con.execute("SELECT count(*) FROM (SELECT * FROM t ORDER BY md5(CAST(i AS VARCHAR)), s)").fetchall()
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        env={**os.environ, "WORK_DIR": str(work_dir), "READY_FILE": str(ready)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        root = Path(_wait_for_file(ready, proc))
        assert root.parent == work_dir and root.name.startswith("etl-bulk-")
        assert (root / "work.duckdb").exists()
        spill_dir = root / "spill"
        deadline = time.monotonic() + 15.0
        while time.monotonic() < deadline:
            if spill_dir.exists() and any(spill_dir.iterdir()):
                break
            assert proc.poll() is None, f"Subprocess exited before spilling: rc={proc.returncode}"
            time.sleep(0.02)
        assert spill_dir.exists() and any(spill_dir.iterdir()), "Expected real DuckDB spill files before SIGTERM"

        proc.send_signal(signal.SIGTERM)
        # Send a second immediate SIGTERM to verify cleanup is not aborted mid-removal.
        time.sleep(0.002)
        if proc.poll() is None:
            try:
                proc.send_signal(signal.SIGTERM)
            except ProcessLookupError:
                pass
        out, err = proc.communicate(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    assert proc.returncode == -signal.SIGTERM, f"Expected SIGTERM exit, got {proc.returncode}: out={out!r}, err={err!r}"
    assert not root.exists()
    assert not list(work_dir.glob("etl-bulk-*"))
    assert sibling_file.read_text() == '{"keep": true}'
    assert recovery_checkpoint.read_text() == '{"phase": "admitted"}'
    assert evidence_file.read_text() == '{"status": "retained"}'


def test_bulk_connection_sigterm_default_tmpdir_and_nested_connections(tmp_path: Path) -> None:
    custom_tmpdir = tmp_path / "tmpdir"
    custom_tmpdir.mkdir()
    unrelated_dir = custom_tmpdir / "unrelated-evidence"
    unrelated_dir.mkdir()
    unrelated_file = unrelated_dir / "keep.txt"
    unrelated_file.write_text("do not delete")

    inner_parent = tmp_path / "inner-parent"
    inner_parent.mkdir()
    inner_sibling = inner_parent / "manifest.json"
    inner_sibling.write_text('{"intact": true}')

    ready = tmp_path / "ready-nested.txt"
    script = """
import json
import os
import time
from pathlib import Path
from spicy_regs.etl_bulk import bulk_connection

inner_parent = Path(os.environ["INNER_PARENT"])
ready = Path(os.environ["READY_FILE"])
with bulk_connection(threads=1, memory_limit="32MB") as (outer_con, outer_root):
    outer_con.execute("CREATE TABLE outer_t AS SELECT i FROM range(1000) t(i)")
    (outer_root / "spill").mkdir(exist_ok=True)
    (outer_root / "spill" / "outer.tmp").write_bytes(b"outer-spill")
    try:
        with bulk_connection(inner_parent, threads=1, memory_limit="32MB") as (inner_con, inner_root):
            inner_con.execute("CREATE TABLE inner_t AS SELECT i FROM range(1000) t(i)")
            (inner_root / "spill").mkdir(exist_ok=True)
            (inner_root / "spill" / "inner.tmp").write_bytes(b"inner-spill")
            ready.write_text(json.dumps({"outer": str(outer_root), "inner": str(inner_root)}))
            try:
                inner_con.execute("SELECT sum(hash(i)) FROM range(10000000000) AS t(i)").fetchall()
            except Exception:
                # Even if caller code catches DuckDB's Query interrupted RuntimeError,
                # bulk_connection must still unwind and re-deliver SIGTERM rather than exiting 0.
                pass
    except Exception:
        pass
    time.sleep(5)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        env={**os.environ, "TMPDIR": str(custom_tmpdir), "INNER_PARENT": str(inner_parent), "READY_FILE": str(ready)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        roots = json.loads(_wait_for_file(ready, proc))
        outer_root = Path(roots["outer"])
        inner_root = Path(roots["inner"])
        assert outer_root.parent == custom_tmpdir
        assert inner_root.parent == inner_parent
        assert (outer_root / "work.duckdb").exists() and (outer_root / "spill" / "outer.tmp").exists()
        assert (inner_root / "work.duckdb").exists() and (inner_root / "spill" / "inner.tmp").exists()
        time.sleep(0.1)
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    assert proc.returncode == -signal.SIGTERM, f"Expected SIGTERM exit, got {proc.returncode}: out={out!r}, err={err!r}"
    assert not outer_root.exists()
    assert not inner_root.exists()
    assert not list(custom_tmpdir.glob("etl-bulk-*"))
    assert not list(inner_parent.glob("etl-bulk-*"))
    assert unrelated_file.read_text() == "do not delete"
    assert inner_sibling.read_text() == '{"intact": true}'


def test_bulk_connection_sigterm_suspended_generator_cleans_owned_dir(tmp_path: Path) -> None:
    work_dir = tmp_path / "gen-work"
    work_dir.mkdir()
    evidence = work_dir / "evidence.json"
    evidence.write_text('{"retained": true}')
    ready = tmp_path / "ready-gen.txt"
    script = """
import os
import time
from pathlib import Path
from spicy_regs.etl_bulk import bulk_connection

work_dir = Path(os.environ["WORK_DIR"])
ready = Path(os.environ["READY_FILE"])

def stream():
    with bulk_connection(work_dir, threads=1, memory_limit="32MB") as (con, root):
        con.execute("CREATE TABLE t AS SELECT 1 AS v")
        (root / "spill").mkdir(exist_ok=True)
        (root / "spill" / "gen.tmp").write_bytes(b"spill")
        yield root

rows = stream()
root = next(rows)
ready.write_text(str(root))
time.sleep(30)
"""
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        env={**os.environ, "WORK_DIR": str(work_dir), "READY_FILE": str(ready)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        root = Path(_wait_for_file(ready, proc))
        assert (root / "work.duckdb").exists() and (root / "spill" / "gen.tmp").exists()
        proc.send_signal(signal.SIGTERM)
        out, err = proc.communicate(timeout=10)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)

    assert proc.returncode == -signal.SIGTERM, f"Expected SIGTERM exit, got {proc.returncode}: out={out!r}, err={err!r}"
    assert not root.exists()
    assert evidence.read_text() == '{"retained": true}'


def test_bulk_connection_preserves_and_chains_custom_sigterm_handler(tmp_path: Path) -> None:
    original = signal.getsignal(signal.SIGTERM)
    try:
        # 1. Non-raising custom handler: chained during nested calls and restored only after outer exits.
        seen: list[int] = []

        def custom_flag(signum: int, _frame) -> None:
            seen.append(signum)

        signal.signal(signal.SIGTERM, custom_flag)
        with etl_bulk.bulk_connection(tmp_path, threads=1, memory_limit="32MB") as (outer_con, outer_root):
            with etl_bulk.bulk_connection(tmp_path, threads=1, memory_limit="32MB") as (inner_con, inner_root):
                assert outer_con.execute("SELECT 1").fetchone() == (1,)
                assert inner_con.execute("SELECT 2").fetchone() == (2,)
                signal.raise_signal(signal.SIGTERM)
            assert not inner_root.exists()
            assert outer_root.exists()
            # Outer is still active, so bulk_connection's wrapper remains installed until outer exits.
            assert signal.getsignal(signal.SIGTERM) is etl_bulk._handle_sigterm
            signal.raise_signal(signal.SIGTERM)
        assert not outer_root.exists()
        assert seen == [signal.SIGTERM, signal.SIGTERM]
        assert signal.getsignal(signal.SIGTERM) is custom_flag

        # 2. Raising custom handler that switches SIGTERM to SIG_IGN (as operational_runner does):
        #    unwinds cleanly, removes owned temp dir, propagates exception, and does not clobber SIG_IGN.
        def ignore_and_raise(_signum: int, _frame) -> None:
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            raise InterruptedError("caller interrupted")

        signal.signal(signal.SIGTERM, ignore_and_raise)
        leaked_root: Path | None = None
        with pytest.raises(InterruptedError, match="caller interrupted"):
            with etl_bulk.bulk_connection(tmp_path, threads=1, memory_limit="32MB") as (_con, root):
                leaked_root = root
                signal.raise_signal(signal.SIGTERM)
        assert leaked_root is not None and not leaked_root.exists()
        assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN

        # 3. Pre-existing SIG_IGN stays untouched during and after bulk_connection.
        with etl_bulk.bulk_connection(tmp_path, threads=1, memory_limit="32MB") as (con, _root):
            assert con.execute("SELECT 3").fetchone() == (3,)
            assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN
        assert signal.getsignal(signal.SIGTERM) == signal.SIG_IGN
    finally:
        signal.signal(signal.SIGTERM, original)


def test_bulk_connection_works_in_non_main_thread_without_clobbering_sigterm(tmp_path: Path) -> None:
    original = signal.getsignal(signal.SIGTERM)

    def sentinel(_signum: int, _frame) -> None:
        pass

    signal.signal(signal.SIGTERM, sentinel)
    try:
        # Standalone worker thread: never calls signal.signal, never crashes, never touches main SIGTERM.
        errors: list[BaseException] = []
        roots: list[Path] = []

        def worker(sub: Path) -> None:
            try:
                with etl_bulk.bulk_connection(sub, threads=1, memory_limit="32MB") as (con, root):
                    roots.append(root)
                    assert con.execute("SELECT 42").fetchone() == (42,)
                    assert (root / "work.duckdb").exists()
            except BaseException as exc:
                errors.append(exc)

        t1 = threading.Thread(target=worker, args=(tmp_path,))
        t1.start()
        t1.join(timeout=10)
        assert not t1.is_alive()
        assert not errors
        assert len(roots) == 1 and not roots[0].exists()
        assert signal.getsignal(signal.SIGTERM) is sentinel

        # Worker thread running concurrently with main-thread bulk_connection.
        worker_entered = threading.Event()
        main_exited = threading.Event()
        concurrent_roots: list[Path] = []

        def overlapping_worker() -> None:
            try:
                with etl_bulk.bulk_connection(tmp_path, threads=1, memory_limit="32MB") as (con, root):
                    concurrent_roots.append(root)
                    worker_entered.set()
                    assert main_exited.wait(timeout=10)
                    assert con.execute("SELECT 99").fetchone() == (99,)
            except BaseException as exc:
                errors.append(exc)

        t2 = threading.Thread(target=overlapping_worker)
        with etl_bulk.bulk_connection(tmp_path, threads=1, memory_limit="32MB") as (main_con, main_root):
            t2.start()
            assert worker_entered.wait(timeout=10)
            assert main_con.execute("SELECT 7").fetchone() == (7,)
        assert not main_root.exists()
        # Main thread restored sentinel cleanly even while the worker thread is still active.
        assert signal.getsignal(signal.SIGTERM) is sentinel
        main_exited.set()
        t2.join(timeout=10)
        assert not t2.is_alive()
        assert not errors
        assert len(concurrent_roots) == 1 and not concurrent_roots[0].exists()
        assert signal.getsignal(signal.SIGTERM) is sentinel
    finally:
        signal.signal(signal.SIGTERM, original)
