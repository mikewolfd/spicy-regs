#!/usr/bin/env python3
"""Hold the live published tables to their declared cross-table joins (``spicy_regs.table_joins``).

Each declared join counts its distinct non-null child keys and those absent from
the parent. A join fails below its floor, the baseline rate truncated to four
decimals, so a new orphan or a narrowed parent shows as a failing number. A
join reports a few orphans beyond those its floor admits as LAG
(``Join.lag_allowance``) rather than failing, since a growing child can name a
key its parent publishes a run later. A declared-empty child that now publishes keys fails too, until its
baseline is recorded. Reads the publisher the output ledger names, or an explicit
``--index-url``, never a default. Read-only: it downloads column pages and
writes nothing but an optional receipt.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path

import duckdb
import httpx

from spicy_regs import output_ledger, table_joins
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.sources import publication

EXIT_UNREACHABLE = 3
FAILING = ("BELOW", "UNBASELINED", "EMPTIED", "MULTIPLICITY")


def table_urls(base_url: str) -> Callable[[str], list[str]]:
    """Resolve each table once: through the publication index, then the rulemaking pointer, else its legacy key."""
    base = base_url.rstrip("/")
    urls = publication.published_urls(base)
    return lambda table: urls.get(table, [f"{base}/{table}.parquet"])


def connect() -> duckdb.DuckDBPyConnection:
    """Modest concurrency and patient retries: the public r2.dev endpoint answers 429 under load."""
    con = duckdb.connect()
    load_public_http(con)
    con.execute("SET threads = 2")
    return con


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def measure(con: duckdb.DuckDBPyConnection, join: table_joins.Join, url_of: Callable[[str], list[str]]) -> dict:
    """Measure full selected inputs, preserving parent multiplicity and raw-row amplification."""
    child = [_quote(column) for column in join.child_columns]
    parent = [_quote(column) for column in join.parent_columns]
    child_urls, parent_urls = url_of(join.measured_via or join.child), url_of(join.parent)
    keys_sql = ", ".join(f"c{i}" for i in range(len(child)))
    on = " AND ".join(f"c.c{i} = p.p{i}" for i in range(len(child)))
    sql = f"""
        WITH child_input AS (SELECT {', '.join(child)} FROM {publication.parquet_scan(child_urls)}),
             parent_input AS (SELECT {', '.join(parent)} FROM {publication.parquet_scan(parent_urls)}),
             c AS (SELECT {', '.join(f'{col} AS c{i}' for i, col in enumerate(child))}, count(*) AS child_rows
                   FROM child_input WHERE {' AND '.join(f'{col} IS NOT NULL' for col in child)}
                   GROUP BY ALL),
             p AS (SELECT {', '.join(f'{col} AS p{i}' for i, col in enumerate(parent))}, count(*) AS parent_rows
                   FROM parent_input WHERE {' AND '.join(f'{col} IS NOT NULL' for col in parent)}
                   GROUP BY ALL),
             j AS (SELECT {keys_sql}, child_rows, coalesce(parent_rows, 0) AS parent_rows
                   FROM c LEFT JOIN p ON {on})
        SELECT count(*) AS keys, count(*) FILTER (WHERE parent_rows=0) AS missing,
               (list(concat_ws('|', {keys_sql}) ORDER BY {keys_sql}) FILTER (WHERE parent_rows=0))[1:3] AS examples,
               (SELECT count(*) FROM child_input) AS child_input_rows,
               coalesce(sum(child_rows),0) AS child_nonnull_rows,
               (SELECT count(*) FROM parent_input) AS parent_input_rows,
               (SELECT count(*) FROM p) AS parent_distinct_keys,
               (SELECT count(*) FROM p WHERE parent_rows>1) AS parent_duplicate_keys,
               coalesce((SELECT max(parent_rows) FROM p),0) AS max_parent_multiplicity,
               coalesce(max(parent_rows),0) AS max_matched_parent_multiplicity,
               coalesce(sum(child_rows*parent_rows),0) AS inner_join_rows,
               coalesce(sum(child_rows*greatest(parent_rows,1)),0) AS left_join_nonnull_rows
        FROM j"""
    row = con.execute(sql).fetchone()
    assert row is not None
    result = verdict(join, row[0], row[1], row[2] or [])
    names = ("child_input_rows", "child_nonnull_rows", "parent_input_rows", "parent_distinct_keys",
             "parent_duplicate_keys", "max_parent_multiplicity", "max_matched_parent_multiplicity",
             "inner_join_rows", "left_join_nonnull_rows")
    result.update(zip(names, row[3:]))
    result.update({"scope": "full_selected_inputs", "child_urls": child_urls, "parent_urls": parent_urls,
                   "sql": sql, "expected_cardinality": join.expected_cardinality})
    if join.expected_cardinality == "one" and result["max_parent_multiplicity"] > 1:
        result["status"] = "MULTIPLICITY"
    return result


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
    return {"join": join.name, "kind": join.kind, "status": status, "keys": keys, "missing": missing,
            "resolved_pct": None if pct is None else round(pct, 4), "floor_pct": floor,
            "baseline_keys": join.baseline_keys, "baseline_missing": join.baseline_missing,
            "examples": list(examples)}


def check(con: duckdb.DuckDBPyConnection, joins: Iterable[table_joins.Join],
          url_of: Callable[[str], list[str]]) -> list[dict]:
    return [measure(con, join, url_of) for join in joins]


def _line(result: dict) -> str:
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
    parser.add_argument("--aggregate", action="append", help="Run this named aggregate check instead of join checks; repeatable")
    parser.add_argument("--timeout-seconds", type=float, default=90, help="Aggregate query timeout, at most 90 seconds")
    args = parser.parse_args(argv)
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
    print(f"Declared joins against {base} ({source}); baseline {table_joins.BASELINE_DATE}")
    try:
        results = check(connect(), table_joins.JOINS, table_urls(base))
    except (httpx.HTTPError, duckdb.IOException, duckdb.HTTPException, publication.PublicationError) as exc:
        print(f"Live tables could not be read; joins were NOT checked: {exc}", file=sys.stderr)
        return EXIT_UNREACHABLE
    for result in results:
        print(_line(result))
    if args.receipt:
        args.receipt.write_text(json.dumps({"base": base, "results": results}, indent=1) + "\n")
    failed = [result for result in results if result["status"] in FAILING]
    counts = {status: sum(r["status"] == status for r in results) for status in dict.fromkeys(r["status"] for r in results)}
    print(" ".join(f"{status}={count}" for status, count in counts.items()))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
