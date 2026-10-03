#!/usr/bin/env python3
"""List the status columns that hold one value on every published row while their description does not say so.

A status stamped when a row is mapped reads like an outcome nobody computed
(round 6: ``filing_link_status`` was ``unresolved`` on every row of 27 FEC tables
while its sentence promised "the relationship to a submitted filing version").
This reads each published file's Parquet footer statistics, never its rows, for
every column of ``table_metadata.json`` whose name ends in status, outcome,
disposition, completeness or mode. A column is constant when every row group
states the same minimum and maximum. Its description says so when it names that
value and says "always" or "every row"; otherwise the column is listed.

It cannot see: a column whose footer states no statistics (a row group of long
strings, or a writer that omits them), a column that varies only because its
statistics are truncated, a value repeated on all but a few rows, a column that
is a function of another (``target_resolution_status`` follows
``target_filing_key``), or whether a sentence that says "always" is otherwise
true. It lists; a person decides. Non-hermetic: it reads the publisher the output
ledger names, or ``--index-url``, like ``check_table_joins.py``. Exit 0 when the
footers were read (whatever it lists), 3 when they could not be.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Callable, Mapping, Sequence
from importlib.resources import files
from pathlib import Path

import duckdb
import httpx

from spicy_regs import output_ledger
from spicy_regs.duckdb_settings import load_public_http
from spicy_regs.sources import publication

EXIT_UNREACHABLE = 3
#: A column named for a state or a result rather than a value: the names the round-6 sweep read (379 columns).
STATUS_NAME = re.compile(r"(?:^|_)(?:status|outcome|disposition|completeness|mode)$")
_SAYS_CONSTANT = re.compile(r"\balways\b|\bevery row\b", re.IGNORECASE)


def status_columns(metadata: Mapping[str, Mapping]) -> dict[str, dict[str, str]]:
    """``{table: {column: description}}`` for every status-named column the dictionary describes."""
    return {
        table: found
        for table, entry in metadata.items()
        if (found := {column["column_name"]: column["description"] or "" for column in entry["columns"]
                      if STATUS_NAME.search(column["column_name"])})
    }


def footer_values(con: duckdb.DuckDBPyConnection, urls: Sequence[str], columns: Sequence[str]) -> dict[str, dict]:
    """Per column: the smallest minimum, largest maximum, NULL count and value count its footers state.

    ``complete`` is false when a row group holding non-NULL values states no minimum, so its range is unknown.
    """
    paths = ", ".join("'" + url.replace("'", "''") + "'" for url in urls)
    names = ", ".join("'" + column.replace("'", "''") + "'" for column in columns)
    rows = con.execute(f"""
        SELECT path_in_schema, min(stats_min_value), max(stats_max_value), sum(stats_null_count), sum(num_values),
               bool_and(stats_min_value IS NOT NULL OR stats_null_count = num_values)
        FROM parquet_metadata([{paths}]) WHERE path_in_schema IN ({names}) GROUP BY 1""").fetchall()
    return {name: {"min": low, "max": high, "nulls": int(nulls or 0), "values": int(values or 0), "complete": bool(ok)}
            for name, low, high, nulls, values, ok in rows}


def unstated_constants(described: Mapping[str, Mapping[str, str]],
                       footers: Mapping[str, Mapping[str, Mapping]]) -> list[dict]:
    """Each constant status column whose description does not both name its value and say "always" or "every row"."""
    found = []
    for table, columns in described.items():
        for column, text in columns.items():
            stats = footers.get(table, {}).get(column)
            if not stats or not stats["complete"] or stats["min"] is None or stats["min"] != stats["max"]:
                continue
            value = stats["min"]
            if value in text and _SAYS_CONSTANT.search(text):
                continue
            found.append({"table": table, "column": column, "value": value, "nulls": stats["nulls"],
                          "rows": stats["values"], "named": value in text})
    return found


def report_lines(listed: Sequence[Mapping]) -> list[str]:
    """One line per column name and value, naming every table it holds them on: one template, one fix."""
    groups: dict[tuple[str, str, bool], list[str]] = {}
    for item in listed:
        groups.setdefault((item["column"], item["value"], item["named"]), []).append(item["table"])
    return [
        f"UNSTATED  {column} = {value!r} ({'value named, not said to be the only one' if named else 'value not named'}) "
        f"on {len(tables)} table{'s' if len(tables) > 1 else ''}: {', '.join(sorted(tables))}"
        for (column, value, named), tables in sorted(groups.items())
    ]


def check(con: duckdb.DuckDBPyConnection, metadata: Mapping[str, Mapping],
          url_of: Callable[[str], list[str] | None]) -> list[dict]:
    """Read the footers of every published table with a status column; unpublished tables are skipped."""
    described = status_columns(metadata)
    footers = {table: footer_values(con, urls, list(columns))
               for table, columns in described.items() if (urls := url_of(table))}
    return unstated_constants(described, footers)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ledger", type=Path, default=output_ledger.LEDGER)
    parser.add_argument("--index-url", help="Public base URL to check instead of the ledger's stated destination")
    parser.add_argument("--receipt", type=Path, help="Write every listed column to this JSON file")
    args = parser.parse_args(argv)
    try:
        base = (args.index_url or output_ledger.ledger_destination(args.ledger.read_text(encoding="utf-8"))).rstrip("/")
    except (OSError, ValueError) as exc:
        print(f"Cannot name the publisher to check: {exc}", file=sys.stderr)
        return 2
    metadata = json.loads(files("spicy_regs").joinpath("table_metadata.json").read_text(encoding="utf-8"))
    try:
        urls = publication.published_urls(base)
        con = duckdb.connect()
        load_public_http(con)
        listed = check(con, metadata, urls.get)
    except (httpx.HTTPError, duckdb.IOException, duckdb.HTTPException, publication.PublicationError) as exc:
        print(f"Published footers could not be read; nothing was checked: {exc}", file=sys.stderr)
        return EXIT_UNREACHABLE
    print(f"Status columns holding one value on every row, against {base}, whose description does not say so:")
    for line in report_lines(listed):
        print(line)
    print(f"UNSTATED={len(listed)} columns (a list for the release operator, not a failure)")
    if args.receipt:
        args.receipt.write_text(json.dumps({"base": base, "listed": listed}, indent=1) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
