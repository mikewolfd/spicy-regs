#!/usr/bin/env python3
"""Hold the live published tables to their declared cross-table joins (``spicy_regs.table_joins``).

Each declared join counts its distinct non-null child keys and those absent from
the parent. A join fails below its floor, the baseline rate truncated to four
decimals, so a new orphan or a narrowed parent shows as a failing number. A
join reports a few orphans beyond those its floor admits as LAG
(``Join.lag_allowance``) rather than failing, since a growing child can name a
key its parent publishes a run later. A declared-empty child that now publishes keys fails too, until its
baseline is recorded. Each key projection is scanned once and retained in
DuckDB only until its last join. HTTP 404 is reported per join, never as zero
keys. Reads the publisher the output ledger names, or an explicit
``--index-url``, never a default. It reads Parquet inputs and writes nothing
but an optional receipt, except that ``--record`` also records each named join's
measurement as its baseline in ``join_measurements.json``.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import date
from functools import cache
from pathlib import Path
from tempfile import TemporaryDirectory

import duckdb
import httpx

from spicy_regs import output_ledger, table_joins
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.sources import publication

EXIT_UNREACHABLE = 3
FAILING = ("BELOW", "UNBASELINED", "EMPTIED", "MULTIPLICITY")
REPO_ROOT = Path(__file__).resolve().parents[1]
MEASUREMENTS = REPO_ROOT / "src/spicy_regs/join_measurements.json"
#: Fields describing the declaration before this measurement, not the measurement itself.
_DECLARATION_FIELDS = ("status", "floor_pct", "baseline_keys", "baseline_missing")


def previous_record(commit: str) -> dict:
    """Read the comparison commit's generated registry, never execute its code."""
    if not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", commit):
        raise ValueError("--changed-since requires a full Git commit SHA")
    result = subprocess.run(
        ["git", "show", f"{commit}:src/spicy_regs/table_joins.json"], cwd=REPO_ROOT,
        capture_output=True, text=True, check=False,
    )
    if result.returncode:
        raise ValueError("Cannot read the comparison commit's table_joins.json; fetch the base commit first")
    return json.loads(result.stdout)


def changed_joins(joins: Iterable[table_joins.Join], previous: dict) -> tuple[list[table_joins.Join], list[str]]:
    """Select every new or changed declaration, including baseline and evidence changes.

    This bounds PR reads; a run without it checks the whole registry.
    Removed declarations are reported separately rather than treated as checks.
    """
    if (previous.get("format") != table_joins.RECORD_FORMAT or previous.get("version") != 1
            or not isinstance(previous.get("joins"), list)):
        raise ValueError("Comparison registry has an unsupported format")
    before = {}
    for record in previous["joins"]:
        if not isinstance(record, dict):
            raise ValueError("Comparison registry has an invalid join")
        for side in ("child", "parent"):
            columns = record.get(f"{side}_columns")
            if (not isinstance(record.get(side), str) or not isinstance(columns, list) or not columns
                    or not all(isinstance(column, str) and column for column in columns)):
                raise ValueError("Comparison registry has an invalid join identity")
        name = (f"{record['child']}.{'+'.join(record['child_columns'])} -> "
                f"{record['parent']}.{'+'.join(record['parent_columns'])}")
        if name in before:
            raise ValueError("Comparison registry repeats a join identity")
        before[name] = record
    current = list(joins)
    selected = [join for join in current if before.get(join.name) != table_joins.record(join)]
    removed = sorted(set(before) - {join.name for join in current})
    return selected, removed


@dataclass(frozen=True)
class TableURLs:
    base: str
    selected: dict[str, list[str]]

    def __call__(self, table: str) -> list[str]:
        return self.selected.get(table, [f"{self.base}/{table}.parquet"])


def table_urls(base_url: str) -> TableURLs:
    """Resolve each table once: through the publication index, then the rulemaking pointer, else its legacy key."""
    base = base_url.rstrip("/")
    return TableURLs(base, publication.published_urls(base))


def connect() -> duckdb.DuckDBPyConnection:
    """Bound public reads: reuse HTTP connections, and read only column pages of large files."""
    con = duckdb.connect(config={"custom_user_agent": "spicy-regs-join-check/1"})
    load_public_http(con)
    con.execute("SET threads = 8; SET httpfs_connection_caching = true")
    # A small immutable file is cheaper as one GET than as many row-group
    # requests. Never fall back to downloading an entire large Parquet file.
    con.execute("SET force_download_threshold = 33554432; SET auto_fallback_to_full_download = false")
    return con


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


# The table and ordered key columns identify a scan. Different child filters are
# counted in the same grouped projection, not another read of the Parquet.
Key = tuple[str, tuple[str, ...]]
ChildFilter = tuple[tuple[str, str], ...]
_METRICS = ("child_input_rows", "child_nonnull_rows", "parent_input_rows", "parent_distinct_keys",
            "parent_duplicate_keys", "max_parent_multiplicity", "max_matched_parent_multiplicity",
            "inner_join_rows", "left_join_nonnull_rows")


def _keys(join: table_joins.Join) -> tuple[Key, Key]:
    return (join.measured_via or join.child, join.child_columns), (join.parent, join.parent_columns)


def _key_names(key: Key) -> list[str]:
    return [f"k{i}" for i in range(len(key[1]))]


def _grouped(key: Key, urls: list[str], filters: Sequence[ChildFilter]) -> str:
    columns = [f"{_quote(col)} AS {alias}" for col, alias in zip(key[1], _key_names(key), strict=True)]
    for i, conditions in enumerate(filters):
        predicate = " AND ".join(f"{_quote(col)} = {_literal(val)}" for col, val in conditions)
        count = f"count(*) FILTER (WHERE {predicate})" if predicate else "count(*)"
        columns.append(f"{count} AS n{i}")
    # Keep the null-key groups for raw input counts, but never join them.
    group_by = ", ".join(str(i + 1) for i in range(len(key[1])))
    return f"SELECT {', '.join(columns)} FROM {publication.parquet_scan(urls)} GROUP BY {group_by}"


def _join_sql(key: Key, child: str, parent: str, child_count: str, parent_count: str) -> str:
    names = _key_names(key)
    cols = ", ".join(names)
    nonnull = " AND ".join(f"{col} IS NOT NULL" for col in names)
    return f"""
        WITH c0 AS ({child}), p0 AS ({parent}),
             c AS (SELECT {cols}, {child_count} AS n FROM c0 WHERE {child_count}>0 AND {nonnull}),
             p AS (SELECT {cols}, {parent_count} AS n FROM p0 WHERE {parent_count}>0 AND {nonnull}),
             j AS (SELECT {cols}, c.n AS child_rows, coalesce(p.n, 0) AS parent_rows
                   FROM c LEFT JOIN p USING ({cols}))
        SELECT count(*) AS keys, count(*) FILTER (WHERE parent_rows=0) AS missing,
               arg_min(concat_ws('|', {cols}), row({cols}), 3) FILTER (WHERE parent_rows=0) AS examples,
               (SELECT coalesce(sum({child_count}),0) FROM c0) AS child_input_rows,
               coalesce(sum(child_rows),0) AS child_nonnull_rows,
               (SELECT coalesce(sum({parent_count}),0) FROM p0) AS parent_input_rows,
               (SELECT count(*) FROM p) AS parent_distinct_keys,
               (SELECT count(*) FROM p WHERE n>1) AS parent_duplicate_keys,
               coalesce((SELECT max(n) FROM p),0) AS max_parent_multiplicity,
               coalesce(max(parent_rows),0) AS max_matched_parent_multiplicity,
               coalesce(sum(child_rows*parent_rows),0) AS inner_join_rows,
               coalesce(sum(child_rows*greatest(parent_rows,1)),0) AS left_join_nonnull_rows
        FROM j"""


@dataclass(frozen=True)
class ReadFailure:
    status: str
    reason: str


@dataclass(frozen=True)
class Projection:
    name: str
    filters: tuple[ChildFilter, ...]

    @property
    def sql(self) -> str:
        return f"SELECT * FROM {self.name}"

    def count_column(self, conditions: ChildFilter) -> str:
        return f"n{self.filters.index(conditions)}"


class KeyCache:
    """Lazy, run-local distinct-key tables; drop each one after its final consumer."""

    def __init__(self, con: duckdb.DuckDBPyConnection, joins: Sequence[table_joins.Join],
                 url_of: Callable[[str], list[str]]):
        self.con = con
        self.urls = cache(url_of)
        filters: dict[Key, dict[ChildFilter, None]] = {}
        self.remaining: Counter[Key] = Counter()
        for join in joins:
            child, parent = _keys(join)
            self.remaining.update((child, parent))
            filters.setdefault(child, {})[join.child_filter] = None
            filters.setdefault(parent, {})[()] = None
        self.projections = {
            key: Projection(f"temp.main._join_keys_{i}", tuple(scopes))
            for i, (key, scopes) in enumerate(filters.items())
        }
        self.loaded: set[Key] = set()
        self.failures: dict[Key, ReadFailure] = {}
        self.absent: dict[str, ReadFailure] = {}

    def get(self, key: Key) -> Projection | ReadFailure:
        table = key[0]
        projection = self.projections[key]
        if table in self.absent:
            return self.absent[table]
        if key in self.failures:
            return self.failures[key]
        if key not in self.loaded:
            sql = _grouped(key, self.urls(table), projection.filters)
            try:
                self.con.execute(f"CREATE TEMP TABLE {projection.name} AS {sql}")
            except duckdb.HTTPException as exc:
                if exc.status_code == 404:
                    failure = ReadFailure("NOT_PUBLISHED", "HTTP 404")
                    self.absent[table] = failure
                    return failure
                self.failures[key] = ReadFailure("UNREACHABLE", str(exc))
                return self.failures[key]
            except (duckdb.IOException, duckdb.BinderException) as exc:
                self.failures[key] = ReadFailure("UNREACHABLE", str(exc))
                return self.failures[key]
            self.loaded.add(key)
        return projection

    def drop(self, key: Key) -> None:
        if key in self.loaded:
            self.con.execute(f"DROP TABLE {self.projections[key].name}")
            self.loaded.remove(key)

    def release(self, key: Key) -> None:
        self.remaining[key] -= 1
        if self.remaining[key] == 0:
            self.drop(key)


def _unreadable(join: table_joins.Join, failure: ReadFailure, side: str, table: str, pending: bool) -> dict:
    reason = f"{side} table {table}: {failure.reason}"
    if pending:
        reason += "; live join qualification remains pending"
    return {**_declaration(join), "status": failure.status, "keys": None, "missing": None,
            "resolved_pct": None, "examples": [],
            "scope": "input_not_published" if failure.status == "NOT_PUBLISHED" else "input_unreachable",
            "reason": reason, "qualification_pending": pending}


def _measure(keys: KeyCache, join: table_joins.Join, unpublished_allowed: bool,
             selected: dict[str, list[str]] | None) -> dict:
    child_key, parent_key = _keys(join)
    child_urls, parent_urls = keys.urls(child_key[0]), keys.urls(parent_key[0])
    common: dict = {"child_urls": child_urls, "parent_urls": parent_urls,
              "expected_cardinality": join.expected_cardinality}
    if join.child_filter:
        common["child_filter"] = dict(join.child_filter)
    child = keys.get(child_key)
    if isinstance(child, ReadFailure):
        pending = (child.status == "NOT_PUBLISHED" and unpublished_allowed
                   and selected is not None and child_key[0] not in selected)
        return {**_unreadable(join, child, "child", child_key[0], pending), **common}
    parent = keys.get(parent_key)
    if isinstance(parent, ReadFailure):
        return {**_unreadable(join, parent, "parent", parent_key[0], False), **common}

    query = _join_sql(child_key, child.sql, parent.sql, child.count_column(join.child_filter), parent.count_column(()))
    row = keys.con.execute(query).fetchone()
    assert row is not None
    result = verdict(join, row[0], row[1], row[2] or [])
    result.update(zip(_METRICS, row[3:], strict=True))
    # The receipt replays from the recorded URLs, independent of the run's temp tables.
    sql = _join_sql(child_key, _grouped(child_key, child_urls, [join.child_filter]),
                    _grouped(parent_key, parent_urls, [()]), "n0", "n0")
    result.update(common, scope="full_selected_inputs", sql=sql)
    if join.expected_cardinality == "one" and result["max_parent_multiplicity"] > 1:
        result["status"] = "MULTIPLICITY"
    return result


def measure(con: duckdb.DuckDBPyConnection, join: table_joins.Join, url_of: Callable[[str], list[str]],
            *, allow_unpublished: bool = False) -> dict:
    """Measure one join, preserving parent multiplicity and raw-row amplification."""
    return check(con, [join], url_of, unpublished_new=frozenset((join.name,)) if allow_unpublished else frozenset())[0]


def _declaration(join: table_joins.Join) -> dict:
    return {"join": join.name, "kind": join.kind, "floor_pct": join.floor_pct,
            "baseline_keys": join.baseline_keys, "baseline_missing": join.baseline_missing}


def verdict(join: table_joins.Join, keys: int, missing: int, examples: Sequence[str] = ()) -> dict:
    floor = join.floor_pct
    pct = None if not keys else 100 * (keys - missing) / keys
    if not keys:
        status = "EMPTY" if floor is None else "EMPTIED"
    elif floor is None:
        status = "UNBASELINED"
    elif pct is not None and pct >= floor:
        status = "OK"
    else:
        status = "LAG" if missing <= join.admitted_missing(keys) + join.lag_allowance(keys) else "BELOW"
    return {**_declaration(join), "status": status, "keys": keys, "missing": missing,
            "resolved_pct": None if pct is None else round(pct, 4), "examples": list(examples)}


def check(con: duckdb.DuckDBPyConnection, joins: Iterable[table_joins.Join],
          url_of: Callable[[str], list[str]], *, unpublished_new: frozenset[str] = frozenset(),
          on_result: Callable[[dict], None] | None = None) -> list[dict]:
    declarations = list(joins)
    keys = KeyCache(con, declarations, url_of)
    selected = url_of.selected if isinstance(url_of, TableURLs) else None
    results = []
    try:
        for join in declarations:
            allowed = join.kind == "empty" or (join.name in unpublished_new and join.kind == "unmeasured"
                                             and join.baseline_keys == join.baseline_missing == 0)
            result = _measure(keys, join, allowed, selected)
            results.append(result)
            for key in _keys(join):
                keys.release(key)
            if on_result is not None:
                on_result(result)
    finally:
        for key in tuple(keys.loaded):
            keys.drop(key)
    return results


def record_baselines(joins: Sequence[table_joins.Join], results: Sequence[dict], receipt: Path,
                     measured_on: str, path: Path) -> None:
    """Record each measurement as its join's baseline, replacing one recorded under the same name.

    An ``unmeasured`` join becomes ``complete`` when every key resolves and ``scope``
    otherwise; other kinds keep their declaration. A join with no key, or with more
    parent rows than its declared cardinality admits, has no baseline to record.
    """
    record = json.loads(path.read_text())
    entries = {entry["join"]: entry for entry in record["results"]}
    for join, result in zip(joins, results, strict=True):
        if result["status"] in ("EMPTY", "EMPTIED", "MULTIPLICITY", "NOT_PUBLISHED", "UNREACHABLE"):
            raise ValueError(f"{join.name}: {result['status']} has no baseline to record")
        kind = join.kind if join.kind != "unmeasured" else ("scope" if result["missing"] else "complete")
        entries[join.name] = {
            **{key: value for key, value in result.items() if key not in _DECLARATION_FIELDS},
            "kind": kind, "measured_on": measured_on, "receipt": receipt.resolve().relative_to(REPO_ROOT).as_posix(),
            "measured_expected_cardinality": "one" if result["max_parent_multiplicity"] <= 1 else "many",
        }
    record["results"] = list(entries.values())
    path.write_text(json.dumps(record, indent=2, ensure_ascii=False) + "\n")


def _line(result: dict) -> str:
    if result["status"] in ("NOT_PUBLISHED", "UNREACHABLE"):
        return f"{result['status']} {result['join']}  {result['reason']}"
    pct = "n/a" if result["resolved_pct"] is None else f"{result['resolved_pct']:.4f}%"
    floor = "none" if result["floor_pct"] is None else f"{result['floor_pct']:.4f}%"
    detail = f"  e.g. {result['examples']}" if result["examples"] else ""
    return (f"{result['status']:<11} {result['join']}  {pct} (floor {floor}, {result['kind']})  "
            f"keys={result['keys']:,} missing={result['missing']:,}{detail}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=output_ledger.LEDGER)
    parser.add_argument("--index-url", help="Public base URL to check instead of the ledger's stated destination")
    parser.add_argument("--receipt", type=Path, help="Write every measurement to this JSON file")
    parser.add_argument("--changed-since", help="Check only declarations added or changed since this full Git commit SHA")
    parser.add_argument("--allow-unpublished-new", action="store_true",
                        help="With --changed-since, report newly declared, unmeasured absent children as NOT_PUBLISHED; "
                             "does not qualify live joins or waive missing previously declared/selected tables")
    parser.add_argument("--aggregate", action="append", help="Run this named aggregate check instead of join checks; repeatable")
    parser.add_argument("--timeout-seconds", type=float, default=90, help="Aggregate query timeout, at most 90 seconds")
    parser.add_argument("--record", action="append", metavar="JOIN",
                        help="Measure this declared join (its full name) and record the result as its baseline in "
                             "join_measurements.json, citing --receipt; repeatable")
    args = parser.parse_args(argv)
    if args.allow_unpublished_new and not args.changed_since:
        parser.error("--allow-unpublished-new requires --changed-since")
    if args.changed_since and args.aggregate:
        parser.error("--changed-since selects joins and cannot be used with --aggregate")
    if args.record and (args.changed_since or args.aggregate or not args.receipt
                        or not args.receipt.resolve().is_relative_to(REPO_ROOT)):
        parser.error("--record needs a --receipt inside the repository it cites and cannot be combined with "
                     "--changed-since or --aggregate")
    joins, removed = list(table_joins.JOINS), []
    unpublished_new = frozenset()
    if args.record:
        declared = {join.name: join for join in joins}
        if unknown := [name for name in args.record if name not in declared]:
            parser.error(f"Not a declared join: {unknown[0]}")
        joins = [declared[name] for name in dict.fromkeys(args.record)]
    if args.changed_since:
        try:
            previous = previous_record(args.changed_since)
            joins, removed = changed_joins(joins, previous)
            if args.allow_unpublished_new:
                before_names = {f"{r['child']}.{'+'.join(r['child_columns'])} -> "
                                f"{r['parent']}.{'+'.join(r['parent_columns'])}" for r in previous["joins"]}
                before_children = {name for r in previous["joins"] for name in (r["child"], r.get("measured_via")) if name}
                unpublished_new = frozenset(join.name for join in joins if join.name not in before_names
                                            and (join.measured_via or join.child) not in before_children)
        except (ValueError, OSError) as exc:
            print(f"Cannot select changed joins: {exc}", file=sys.stderr)
            return 2
        print(f"Selected {len(joins)} added or changed joins against {args.changed_since}")
        for name in removed:
            print(f"REMOVED     {name}")
        if not joins:
            if args.receipt:
                args.receipt.write_text(json.dumps({"changed_since": args.changed_since, "selected": [],
                                                  "removed": removed, "results": []}, indent=2) + "\n")
            print("No added or changed join declarations; no public data reads needed.")
            return 0
    if not args.record and not args.changed_since:
        unpublished_new = frozenset(join.name for join in joins if join.kind == "unmeasured")
    if args.index_url:
        base, source = args.index_url.rstrip("/"), "an explicit --index-url, not the ledger's destination"
    else:
        try:
            base = output_ledger.ledger_destination(args.ledger.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            print(f"Cannot name the publisher to check: {exc}", file=sys.stderr)
            return 2
        source = "the ledger's stated destination"
    if args.aggregate:
        from spicy_regs.aggregate_checks import check_public
        try:
            with connect() as con:
                receipt = check_public(con, base, args.aggregate, timeout_seconds=args.timeout_seconds)
        except (httpx.HTTPError, duckdb.Error, publication.PublicationError, ValueError) as exc:
            print(f"Aggregate selection failed: {exc}", file=sys.stderr)
            return EXIT_UNREACHABLE
        if args.receipt:
            args.receipt.write_text(json.dumps(receipt, indent=2) + "\n")
        for result in receipt["results"]:
            print(f"{result['status']:<14} {result['check']}: {result['reason']}")
        if any(r["status"] == "MISMATCH" for r in receipt["results"]):
            return 1
        return 0 if all(r["status"] in ("OK", "EMPTY") for r in receipt["results"]) else EXIT_UNREACHABLE
    print(f"Checking {len(joins)} declared joins against {base} ({source}); baseline {table_joins.BASELINE_DATE}")
    try:
        with TemporaryDirectory(prefix="spicy-regs-join-") as spill, connect() as con:
            con.execute("SET temp_directory = ?", [spill])
            con.execute("SET memory_limit = '3GB'; SET preserve_insertion_order = false")
            results = check(con, joins, table_urls(base), unpublished_new=unpublished_new,
                            on_result=lambda result: print(_line(result), flush=True))
    except (httpx.HTTPError, duckdb.Error, publication.PublicationError) as exc:
        if args.receipt:
            args.receipt.write_text(json.dumps({"base": base, "changed_since": args.changed_since,
                                              "selected": [join.name for join in joins], "removed": removed,
                                              "error": str(exc), "status": "UNREACHABLE"}, indent=2) + "\n")
        print(f"Live tables could not be read; joins were NOT checked: {exc}", file=sys.stderr)
        return EXIT_UNREACHABLE
    if args.receipt:
        args.receipt.write_text(json.dumps({"base": base, "changed_since": args.changed_since,
                                          "selected": [join.name for join in joins], "removed": removed,
                                          "results": results}, indent=1) + "\n")
    if args.record:
        try:
            record_baselines(joins, results, args.receipt, date.today().isoformat(), MEASUREMENTS)
        except ValueError as exc:
            print(f"Baselines NOT recorded: {exc}", file=sys.stderr)
            return 1
        print(f"Recorded {len(joins)} baselines in {MEASUREMENTS.relative_to(REPO_ROOT)}; "
              "run `spicy-regs-dict generate` to refresh table_joins.json")
        return 0
    failed = [result for result in results if result["status"] in FAILING]
    counts = {status: sum(r["status"] == status for r in results) for status in dict.fromkeys(r["status"] for r in results)}
    print(" ".join(f"{status}={count}" for status, count in counts.items()))
    if any(result["status"] == "UNREACHABLE" or
           (result["status"] == "NOT_PUBLISHED" and not result["qualification_pending"]) for result in results):
        return EXIT_UNREACHABLE
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
