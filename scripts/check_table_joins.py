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
from spicy_regs.sources import publication

EXIT_UNREACHABLE = 3
SNAPSHOT_POINTER = "materialized/rulemaking/latest.json"
FAILING = ("BELOW", "UNBASELINED", "EMPTIED")


def _get_json(url: str) -> dict:
    response = httpx.get(url, headers={"Cache-Control": "no-cache"}, follow_redirects=True, timeout=60)
    response.raise_for_status()
    return response.json()


def table_urls(base_url: str) -> Callable[[str], str]:
    """Resolve each table once: managed tables through the publication index, rulemaking through its pointer."""
    base = base_url.rstrip("/")
    index = publication.load_index(base)
    pointer = _get_json(f"{base}/{SNAPSHOT_POINTER}")
    artifacts = _get_json(f"{base}/{pointer['manifest_key']}")["artifacts"]

    def url_of(table: str) -> str:
        if table in table_joins.MATERIALIZED_TABLES:
            return f"{base}/{artifacts[f'{table}.parquet']['remote_key']}"
        location, _ = publication.table_location(index, f"{table}.parquet")
        return f"{base}/{location}"

    return url_of


def connect() -> duckdb.DuckDBPyConnection:
    """Modest concurrency and patient retries: the public r2.dev endpoint answers 429 under load."""
    con = duckdb.connect()
    for statement in ("INSTALL httpfs", "LOAD httpfs", "SET threads = 2", "SET http_retries = 8",
                      "SET http_retry_wait_ms = 2000", "SET http_retry_backoff = 2"):
        con.execute(statement)
    return con


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def measure(con: duckdb.DuckDBPyConnection, join: table_joins.Join, url_of: Callable[[str], str]) -> dict:
    """Distinct non-null child keys, how many the parent lacks, a few examples, and the verdict, in one pass."""
    child = [_quote(column) for column in join.child_columns]
    parent = [_quote(column) for column in join.parent_columns]
    keys_sql = ", ".join(f"c{i}" for i in range(len(child)))
    on = " AND ".join(f"c.c{i} = p.p{i}" for i in range(len(child)))
    keys, missing, examples = con.execute(f"""
        WITH c AS (SELECT DISTINCT {', '.join(f'{col} AS c{i}' for i, col in enumerate(child))}
                   FROM read_parquet({_literal(url_of(join.measured_via or join.child))})
                   WHERE {' AND '.join(f'{col} IS NOT NULL' for col in child)}),
             p AS (SELECT DISTINCT {', '.join(f'{col} AS p{i}' for i, col in enumerate(parent))}, true AS present
                   FROM read_parquet({_literal(url_of(join.parent))})),
             j AS (SELECT {keys_sql}, p.present IS NULL AS missing FROM c LEFT JOIN p ON {on})
        SELECT count(*), count(*) FILTER (WHERE missing),
               (list(concat_ws('|', {keys_sql})) FILTER (WHERE missing))[1:3]
        FROM j""").fetchall()[0]
    return verdict(join, keys, missing, examples or [])


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


def check(con: duckdb.DuckDBPyConnection, joins: Iterable[table_joins.Join], url_of: Callable[[str], str]) -> list[dict]:
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
